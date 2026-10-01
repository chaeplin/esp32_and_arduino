/*
 * Adafruit HUZZAH32 ESP32 Feather — Victron ADV + GATT → MQTT, DEEP-SLEEP version (sleep3)
 *
 * Every minute (wake aligned to the minute boundary from the RTC clock):
 *   BLE scan until ADV_TARGET decoded ADV packets per device (or ADV_SCAN_TIMEOUT_MS)
 *   → averages → victron/mppt, victron/sense, victron/status (+ board VBAT) → deep sleep.
 * minute % 10 == 0: GATT day0 history → victron/mppt/hist; if MPPT charge state != off also
 *   GATT pv → victron/mppt/pv.  A failed 10-min job is retried on the next wakes (JOB_MAX_TRIES).
 * 00:10 (and later until done): day1 (yesterday) too, retry flag/date kept in RTC memory.
 * MQTT commands (victron/gatt/cmd, QoS1 + persistent session → queued while asleep):
 *   {"cmd":"hist"} | {"cmd":"hist","days":N} | {"cmd":"pv"} | {"cmd":"unpair"}
 * Retained victron/lwt = JSON {"tok","state":"sleep","ts","next",...} (no MQTT will).
 * Clock: NTP on first boot, then every NTP_RESYNC_S; RTC keeps time across deep sleep. TZ KST-9.
 * Protocol logic (pv PV_SEQ 0x01, EDBC push wait, hist 81-concat on 0003, NVS day cache) unchanged.
 *
 * sleep2 — history day labels by the device day sequence number (record byte 32, 0..364):
 *   The Victron closes its history day when PV goes dark in the evening, NOT at midnight. sleep1
 *   labelled ymd by the wall clock, so the fresh day0 (seq+1, all zeros) overwrote today's date.
 *   Now the last day0 seq + its ymd are kept in RTC memory and NVS ("d0seq"/"d0ymd"):
 *   - day0 seq == stored seq            → stored ymd (also after midnight, wall date ±1 accepted)
 *   - day0 seq > stored seq (by 1..7):  wall date == stored ymd (evening rollover) → stored ymd + 1
 *                                       otherwise stored ymd + diff (if within wall-1..wall+1)
 *     and the previous day is closed: day1 is read over GATT in the same job (resume session / the
 *     next wake if this wake's budget runs out) and published as kind "yesterday" with the right ymd.
 *     That marks yday done for that ymd; the 00:10 job is then a no-op (kept as a fallback).
 *   - no stored seq (first boot) / seq jump > 7 / no seq → wall-clock date. First-boot heuristic:
 *     local hour >= 18 and day0 looks fresh (yield 0, pmax 0, bulk/abs/float <= 5 min) → the
 *     device already rolled this evening → tomorrow's date.
 *   - ymd(day k) = ymd(day0) - k. hist JSON has "label":"seq"|"clock" (how the day0 base was found).
 *
 * sleep3 — clock + ADV:
 *   - The ts_offset sawtooth (+~1 s/min, reset on the 10-min job wakes) = deep-sleep RTC drift
 *     (internal 150 kHz RC, ~1.7 % fast) that was silently reset by retained yard/time messages,
 *     received during the long MQTT-connected job wakes (persistent session keeps an old
 *     subscription). The hourly NTP step then only showed the drift since that reset (~0.75 s).
 *     Now: yard/time is unsubscribed + ignored while NTP is fresh (< YARD_MAX_NTP_AGE_S);
 *     a one-packet UDP NTP query (~50-150 ms) resyncs every NTP_RESYNC_S (600 s, SNTP fallback);
 *     each NTP step updates a drift estimate (ppm of deep-sleep time, RTC + NVS "drift") and
 *     every deep-sleep wake subtracts lastSleep * ppm from the clock.
 *   - Wake alignment was already absolute (next minute boundary from the clock at sleep time,
 *     no carry-over from a long wake); unchanged.
 *   - ADV: 100 % scan duty (window = interval), ADV_TARGET 5, timeout 6 s (was 50 %, 10, 9 s).
 *   - status: dec_ok = new decoded packets, dec_dup = repeats (same nonce), dec_other = Victron
 *     non-readout records (not 0x10, harmless), dec_fail = real failures (key/length/invalid);
 *     + drift_ppm, clk ("ntp"|"yard"|"sntp"), ntp_ms, yard_ign.
 *
 * Arduino: Adafruit ESP32 Feather (esp32 core 3.3.x), NimBLE-Arduino 2.x, PubSubClient
 */

#include <WiFi.h>
#include <WiFiUdp.h>
#include <PubSubClient.h>
#include <NimBLEDevice.h>
#include <Preferences.h>
#include <mbedtls/aes.h>
#include <ctype.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include <math.h>
#include <time.h>
#include <sys/time.h>
#include <esp_system.h>
#include <esp_sleep.h>
#include <esp_timer.h>
#include <esp_sntp.h>
#include <esp_wifi.h>

/* ============================ USER CONFIG ============================ */
/* credentials / device identity: secrets.h (git-ignored). copy secrets.h.example → secrets.h */
#if __has_include("secrets.h")
#include "secrets.h"
#else
#error "secrets.h missing: copy secrets.h.example to secrets.h and fill in your values"
#endif
#ifndef SECRET_MQTT_PORT
#define SECRET_MQTT_PORT 1883
#endif
#ifndef SECRET_MQTT_CLIENT
#define SECRET_MQTT_CLIENT "huzzah-victron-gatt"
#endif
#ifndef SECRET_USE_STATIC_IP
#define SECRET_USE_STATIC_IP 0
#endif
static const char *WIFI_SSID    = SECRET_WIFI_SSID;
static const char *WIFI_PASS    = SECRET_WIFI_PASS;
static const char *MQTT_HOST    = SECRET_MQTT_HOST;
static const uint16_t MQTT_PORT = SECRET_MQTT_PORT;
static const char *MQTT_USER    = SECRET_MQTT_USER;
static const char *MQTT_PASS    = SECRET_MQTT_PASS;
static const char *MQTT_CLIENT  = SECRET_MQTT_CLIENT;
#define VICTRON_PIN   SECRET_VICTRON_PIN       /* BLE pairing PIN, 6 digits */
/* Victron "encryption key" (VictronConnect → Product info → Instant readout), 16 bytes each */
#define MPPT_AES_KEY  SECRET_MPPT_AES_KEY
#define SENSE_AES_KEY SECRET_SENSE_AES_KEY
#define MPPT_SN       SECRET_MPPT_SN           /* serial: "sn" in the JSON payloads */
#define SENSE_SN      SECRET_SENSE_SN
#define MPPT_MAC      SECRET_MPPT_MAC          /* 12 lowercase hex chars, no colons */
#define SENSE_MAC     SECRET_SENSE_MAC

/* optional static IP (saves DHCP time, ~0.3–1 s per wake) — values in secrets.h */
#define USE_STATIC_IP SECRET_USE_STATIC_IP
#if USE_STATIC_IP
static const IPAddress STATIC_IP(SECRET_STATIC_IP);
static const IPAddress STATIC_GW(SECRET_STATIC_GW);
static const IPAddress STATIC_MASK(SECRET_STATIC_MASK);
static const IPAddress STATIC_DNS(SECRET_STATIC_DNS);
#endif

/* schedule / budgets */
#define WAKE_PERIOD_S         60      /* wake on every minute boundary */
#define ADV_TARGET            5       /* sleep3: decoded ADV packets per device to average (was 10) */
#define ADV_SCAN_TIMEOUT_MS   6000    /* sleep3: publish whatever arrived by then (was 9000) */
#define ADV_SENSE_GRACE_MS    2000    /* after MPPT has ADV_TARGET, wait at most this long for the sense */
#define SCAN_INTERVAL         160     /* 0.625 ms units: 100 ms */
#define SCAN_WINDOW           160     /* sleep3: = interval → 100 % duty (was 80 = 50 %) */
#define HIST_EVERY_MIN        10      /* day0 hist (+pv) when minute % this == 0 */
#define YDAY_HOUR             0       /* day1 (yesterday) from 00:10 on */
#define YDAY_MIN              10
#define JOB_MAX_TRIES         3       /* 10-min job: first try + retries on the next minute wakes */
#define YDAY_FAST_TRIES       3       /* day1: retry on every wake this many times ... */
#define YDAY_MAX_TRIES        12      /* ... then only on 10-min wakes, at most this many per day */
#define PV_AFTER_HIST_MS      2500    /* gap between the hist and the pv GATT connection */
#define AWAKE_MAX_MIN_MS      50000UL /* watchdog: forced deep sleep on a plain minute wake */
#define AWAKE_MAX_JOB_MS      110000UL/* ... and on a wake with GATT jobs */
#define JOB_RESERVE_MS        8000UL  /* stop starting GATT work this long before the watchdog */
#define NTP_RESYNC_S          600     /* sleep3: quick UDP NTP every 10 min (was SNTP hourly) */
#define NTP_QUICK_TIMEOUT_MS  800     /* one UDP NTP request/reply */
#define YARD_MAX_NTP_AGE_S    21600   /* yard/time only if no NTP for 6 h (or no clock at all) */
#define DRIFT_MIN_SLEEP_S     240     /* min deep-sleep time between two NTP steps to estimate drift */
#define DRIFT_MAX_PPM         50000   /* clamp (5 %) */
#define NTP_FIRST_TIMEOUT_MS  10000
#define NTP_RESYNC_TIMEOUT_MS 3000
#define WIFI_FAST_TIMEOUT_MS  3000    /* cached BSSID/channel attempt, then normal connect */
#define WIFI_TIMEOUT_MS       12000
#define MQTT_PERSISTENT       1       /* clean_session=false + QoS1 cmd sub → commands queued while asleep */
#define MQTT_RX_WINDOW_MS     300     /* after connect: receive queued commands */
#define MQTT_FLUSH_MS         1500    /* wait for our own retained state message to come back */
#define BOOT_COMP_MS          150     /* wake timer → setup() latency, subtracted from the sleep */
#define MIN_SLEEP_MS          3000
static const char *NTP_SERVER1 = "pool.ntp.org";
static const char *NTP_SERVER2 = "time.google.com";
static const char *TZ_INFO     = "KST-9";
/* ===================================================================== */

static const char *TOPIC_MPPT   = "victron/mppt";
static const char *TOPIC_SENSE  = "victron/sense";
static const char *TOPIC_STATUS = "victron/status";
static const char *TOPIC_HIST   = "victron/mppt/hist";
static const char *TOPIC_PV     = "victron/mppt/pv";
static const char *TOPIC_LOG    = "victron/gatt";
static const char *TOPIC_CMD    = "victron/gatt/cmd";
static const char *TOPIC_LWT    = "victron/lwt";   /* sleep1: retained state JSON, no MQTT will */
static const char *TOPIC_TIME   = "yard/time";

static const uint32_t g_pin = VICTRON_PIN;

static const int PIN_LED  = 13;
static const int PIN_VBAT = 35;
static const uint16_t VICTRON_CID = 0x02E1;
static const uint32_t LIVE_MS = 10000;
static const uint32_t STALE_MS = 8000;


/* VE.Direct HEX / VREG (BlueSolar-HEX-protocol.pdf):
 *   0x1050..0x106E  daily history record, 34 bytes (0x1050=today, 0x1051=yesterday, ...)
 *   0x10A0..0x10BE  daily MPPT (per-tracker) history — doc: only on multi-tracker units (MPPT RS).
 *                   VictronConnect asks 1050+10A0 in pairs, so we do the same by default. */
static const uint16_t REG_HIST_DAY0   = 0x1050;
static const uint16_t REG_HIST_MPPT0  = 0x10A0;
static const uint16_t REG_EDD1 = 0xEDD1; /* yield yesterday */
static const uint16_t REG_EDD0 = 0xEDD0; /* pmax yesterday */
static const uint16_t REG_EDD3 = 0xEDD3; /* yield today */
static const uint16_t REG_EDD2 = 0xEDD2; /* pmax today */
static const uint16_t REG_PV_V = 0xEDBB;
static const uint16_t REG_PV_W = 0xEDBC;
static const uint16_t REG_PV_A = 0xEDBD;

#define HIST_MAX_DAYS 30            /* 0x1050+30 = 0x106E, device keeps 30 days + today */
#ifndef HIST_DAYS
#define HIST_DAYS 7                 /* default N: read day0..day N */
#endif
#ifndef HIST_ASK_MPPT_X
#define HIST_ASK_MPPT_X 0           /* rev5: off. 0x10A0 answered 09 flag 01 (unknown id) on this MPPT */
#endif
#ifndef HIST_DEBUG_FRAMES
#define HIST_DEBUG_FRAMES 2         /* 0=off 1=every 08/09 frame 2=skip unsolicited live frames (s=03) */
#endif
#ifndef HIST_DAYS_PER_SESSION
#define HIST_DAYS_PER_SESSION 16    /* device drops the link at ~10 s → read K days, reconnect, resume */
#endif
#ifndef HIST_WRITE_CHARS
#define HIST_WRITE_CHARS 1          /* first hist attempt target: 1=0003 2=0004 3=both. rev4 log: writing
                                       both made the device answer twice (dup page ~1.5 s later) */
#endif
#ifndef PV_WRITE_CHARS
#define PV_WRITE_CHARS HIST_WRITE_CHARS  /* rev6: pv GETs go to 0003 only (replies arrive on 0003; both = dup reply) */
#endif
#ifndef PV_SEQ
#define PV_SEQ 0x01                 /* rev10: fixed seq for every pv get (incl. retries). EDBB only ever
                                       answered s=01; s=02/s=04 got no reply */
#endif
#ifndef PV_EDBC_WAIT_MS
#define PV_EDBC_WAIT_MS 3000        /* rev11: EDBC is never requested (get → 09 flag 01); wait this long
                                       after EDBB is done for an unsolicited s=03 push (capped at PV_SESSION_MS) */
#endif
#ifndef INIT_BULK_TRIM
#define INIT_BULK_TRIM 1            /* drop dangling "05 00" at the end of the 20-byte 0004 init chunk */
#endif
static const uint32_t HIST_DAY_TIMEOUT_MS = 4000;  /* per attempt; history page took >1.5 s in rev4 */
static const uint32_t HIST_GAP_MS         = 150;   /* gap after a day closes before the next request */
static const uint32_t HIST_QUIET_MS       = 150;   /* no *reply* frames (live s=03 ignored) for this long */
static const uint32_t HIST_KEEPALIVE_MS   = 0;     /* f941 keepalive (rev3 guess) — off: drop came 260 ms after it */
static const uint32_t HIST_SESSION_MS     = 6500;  /* stop sending new requests after this (from session start) */
static const uint32_t HIST_SESSION_END_MS = 7500;  /* ... and disconnect cleanly at this point */
static const uint32_t HIST_RECONNECT_MS   = 2500;  /* gap before the next resume session */
static const uint8_t  HIST_MAX_SESSIONS   = 20;    /* per hist command */
static const uint32_t PV_REPLY_TIMEOUT_MS = 1000;  /* rev6: wait for the 08/09 of the current pv reg */
static const uint8_t  PV_RETRIES          = 1;     /* extra attempts per pv reg after a timeout */
static const uint32_t PV_GAP_MS           = 100;   /* gap after a pv reg closes before the next request */
static const uint32_t PV_SESSION_MS       = 6500;  /* no pv request whose wait would end after this (from connect) */
static const uint32_t PV_FB_WAIT_MS       = 1000;  /* seq=00 fallback wait */
static const uint8_t  HIST_DAY_RETRIES    = 2;     /* extra attempts per day (forms: 82 pair, 81 concat s=03, 81 single) */
static const uint8_t  HIST_EMPTY_STOP     = 2;     /* stop after N consecutive empty/failed days */
static const uint32_t HIST_FB_WAIT_MS     = 1500;  /* EDDx fallback wait */
static const uint8_t  HIST_CACHE_VER      = 3;     /* rev9: + ibat max (rev8 = 2) — bump on layout change */
static const int      HIST_CACHE_SLOTS    = 40;    /* NVS ring h00..h39, keyed by day number */
static const uint32_t SCAN_RESTART_MS     = 600000UL; /* clear scan state every 10 min */
static const uint8_t  CONN_FAIL_RESET     = 3;     /* host reset after N consecutive GATT failures */
static const int      MAX_GET_REGS        = 8;     /* regs per request packet (<=23, CBOR array head) */

static const char *SVC_APP = "306b0001-b081-4037-83dc-e59fcc3cdfd0";
static const char *CHR_2   = "306b0002-b081-4037-83dc-e59fcc3cdfd0";
static const char *CHR_3   = "306b0003-b081-4037-83dc-e59fcc3cdfd0";
static const char *CHR_4   = "306b0004-b081-4037-83dc-e59fcc3cdfd0";


enum { DEV_MPPT = 0, DEV_SENSE = 1, DEV_N = 2 };
enum { MODE_ADV = 0, MODE_GATT = 1 };
enum { JOB_NONE = 0, JOB_HIST = 1, JOB_PV = 2 };
/* per-day state inside one hist session */
enum { DAY_PENDING = 0, DAY_OK = 1, DAY_EMPTY = 2, DAY_FAIL = 3, DAY_CACHED = 4, DAY_SKIP = 5 };

struct VictronDev {
    const char *id, *label, *macHex;
    uint8_t key[16];
};
static VictronDev g_dev[DEV_N] = {
    { "mppt", MPPT_SN, MPPT_MAC, MPPT_AES_KEY },
    { "sense", SENSE_SN, SENSE_MAC, SENSE_AES_KEY }
};

struct DevState {
    bool seen, ok, hasTemp;
    int rssi;
    uint16_t model, nonce;
    uint32_t lastMs;
    char addr[20], name[28];
    uint8_t addrType, chargeState, chargerErr, recType;
    float vbat, ibat, yieldWh, pvW, loadA, tempC;
};
struct Acc {
    double vbat, ibat, pvW, yieldWh, loadA, tempC;
    long rssi;
    int n, nTemp;
    uint8_t state, err;
};
struct DayRec {
    bool ok, hasYield, hasPmax, hasSeq;
    int ymd;
    uint16_t seq;          /* day sequence number (record byte 32), 0..364 */
    float yieldKwh, consumedKwh, pmaxW, vpvMax, vbatMax, vbatMin;
    /* rev8: from the history page only (EDDx fallback leaves hasPage=false → null in JSON) */
    bool hasPage;
    uint8_t cacheVer;      /* HIST_CACHE_VER when written to NVS; older entries = miss */
    uint8_t err[4];        /* record bytes 14..17 (error 0..3) */
    uint16_t tBulk, tAbs, tFloat;   /* minutes, record bytes 18/20/22; 0xFFFF = unknown */
    uint16_t iBatMax;      /* rev9: battery max current, record byte 28, 0.1 A; 0xFFFF = unknown */
};

static DevState g_st[DEV_N];
static Acc g_acc[DEV_N];
static DayRec g_day[HIST_MAX_DAYS + 1];
static uint8_t g_dayState[HIST_MAX_DAYS + 1];
static uint8_t g_dayErr[HIST_MAX_DAYS + 1];  /* last 0x09 flag value for this day (0 = none) */
static uint8_t g_dayTries[HIST_MAX_DAYS + 1];/* attempts across sessions */
static bool g_dayPub[HIST_MAX_DAYS + 1];     /* already published in this hist job */
static DayRec &g_today = g_day[0];
static DayRec &g_yday  = g_day[1];

WiFiClient wifiClient;
PubSubClient mqtt(wifiClient);
Preferences prefs;

static uint32_t g_bootMs, g_lastScanRst;
static uint32_t g_scanHits, g_decOk, g_decFail, g_gattStartMs, g_histNextMs;
static uint32_t g_decDup, g_decOther;    /* sleep3: repeats (same nonce) / non-readout Victron records */
static uint32_t g_ntpMs = 0;            /* sleep3: duration of this wake's NTP sync, 0 = none */
static uint8_t g_connFails;
static int g_mode = MODE_ADV, g_job = JOB_NONE;
static volatile bool g_wantHist, g_wantPv, g_wantUnpair, g_enc, g_sawNotify, g_histPubPending;
static volatile bool g_histQueued = false;
static bool g_clockOk = false;
static int g_pvPhase = 0;                /* rev7: 0..1 = index into PV_REGS being read, PV_NREGS = done */
static bool g_pvFbSent = false;
static uint8_t g_pvTry = 0;              /* requests sent for the current pv reg (0 = not sent yet) */
static uint8_t g_pvErr = 0;              /* 09 flag received for the current pv reg (0 = none) */
static uint32_t g_pvDeadline = 0;
static bool g_pvEdbcWait = false;        /* rev11: waiting for the unsolicited EDBC push */
static bool g_pvEdbcDone = false;        /* rev11: EDBC wait finished (got it or timed out) */
static uint32_t g_pvEdbcStart = 0, g_pvEdbcUntil = 0;
/* rev7: EDBD (PV current) dropped — this SmartSolar never answers it (no 08, no 09). An EDBD 08
 * that arrives anyway is still accepted as ipv in parseFrames; otherwise pubPv derives ipv. */
/* rev11: EDBC (PV power) is not requested either — the get is rejected (09 flag 01). It only
 * arrives as an unsolicited s=03 push (~+1115 ms); pvTick waits for it after EDBB. */
static const uint16_t PV_REGS[] = { REG_PV_V };
static const int PV_NREGS = (int)(sizeof(PV_REGS) / sizeof(PV_REGS[0]));
/* hist session */
static int g_histReqDays = HIST_DAYS;   /* from cmd */
static int g_histN = HIST_DAYS;         /* this session: day0..g_histN */
static time_t g_histBase;               /* epoch at session start */
/* sleep2: ymd of day0 for this job (day k = g_base0Ymd - k). predicted at job start from the
 * stored seq/ymd, fixed when the day0 page (with its seq) arrives */
static int g_base0Ymd = 0;
static bool g_lblSeq = false;           /* base came from the seq logic → "label":"seq" */
static int32_t g_prevSeq = -1, g_prevYmd = 0;   /* stored d0 seq/ymd snapshot at job start */
static bool g_day0Labeled = false;
static int g_histDay = -1;              /* day currently requested, -1 = not started */
static uint8_t g_histTry, g_histEmptyRun;
static bool g_histNeedSend, g_histFbSent;
static uint32_t g_lastRxMs, g_lastTxMs;           /* loop-side timestamps for pacing */
static uint32_t g_lastReplyMs;                    /* last non-live frame (reply / error / hist page) */
static bool g_histCont;                           /* next hist session resumes the current job */
static uint8_t g_histSessions, g_histSessDays;    /* sessions in this job, days closed this session */
static bool g_histSessStop;                       /* no new requests in this session */
static uint8_t g_wrMask = 3;                      /* wrBoth targets: bit0 0003, bit1 0004 */
static uint8_t g_lastTxSeq;                       /* seq of the last getRegs request */
static uint32_t g_rxBytesCh[3];                   /* notify bytes per char 0002/0003/0004 */
static uint32_t g_histDeadline, g_histFbUntil, g_histBudgetMs = 28000;
/* disconnect is only flagged in the NimBLE host task and handled in loop() */
static volatile bool g_peerDrop = false;
static volatile int g_dropReason = 0;
static NimBLEClient *volatile g_dropCli = nullptr;

static NimBLEClient *g_cli = nullptr;
static NimBLERemoteCharacteristic *g_chCtrl, *g_chCmd, *g_chBulk;
static uint8_t g_seq = 1;
static int g_seqFixed = -1;              /* rev10: >=0 → getRegs uses this seq and leaves g_seq alone */
/* notify → loop hand-off. onNotify runs in the NimBLE host task (core 0), loop on core 1:
 * noInterrupts() only masks the local core, so use a spinlock. */
static const uint16_t RX_CAP = 2048;
static const uint16_t PARSE_CAP = 3072;
static portMUX_TYPE g_rxMux = portMUX_INITIALIZER_UNLOCKED;
static uint8_t g_rx[RX_CAP];
static uint16_t g_rxLen;               /* guarded by g_rxMux */
static uint32_t g_rxOvf, g_parseOvf;      /* g_rxOvf under g_rxMux, g_parseOvf loop only */
static uint8_t g_parse[PARSE_CAP];     /* loop task only */
static uint16_t g_parseLen;
static bool g_havePvV, g_havePvW, g_havePvA;
static float g_pvV, g_pvWatt, g_pvA;

/* ---- sleep1: state kept in RTC slow memory across deep sleep (lost on power-on / reset) ---- */
static const uint32_t RTC_MAGIC = 0x56534C33;   /* "VSL3" (sleep3: + clock drift fields) */
enum { JOBB_HIST0 = 1, JOBB_PV = 2 };
struct RtcState {
    uint32_t magic;
    uint32_t boots;            /* wakes since power-on */
    uint32_t wdtTrips;         /* forced sleeps by the awake watchdog */
    uint32_t prevAwakeMs;      /* awake time of the previous wake */
    bool timeValid;            /* RTC clock was set (NTP or yard/time) */
    time_t lastNtp;            /* epoch of the last NTP sync */
    int32_t lastNtpAdjMs;      /* clock step at the last resync (RTC drift since the one before) */
    uint8_t lastState;         /* last MPPT charge state from ADV, 255 = unknown */
    bool wifiCacheOk;
    uint8_t wifiBssid[6];
    uint8_t wifiCh;
    uint32_t jobSlot;          /* epoch/600 of the pending 10-min job */
    uint8_t jobPend, jobTries; /* JOBB_* bits still to do, attempts so far */
    int32_t ydayDoneYmd;       /* yesterday's history published for this date */
    int32_t ydayTryYmd;
    uint8_t ydayTries;
    int32_t day0DoneYmd;
    uint32_t day0DoneSlot;
    /* sleep2: seq-based day labels */
    int32_t d0Seq;             /* last seen day0 seq (0..364), -1 = unknown (mirrored in NVS d0seq) */
    int32_t d0Ymd;             /* ymd assigned to that seq                      (NVS d0ymd) */
    int32_t rollYdayYmd;       /* day closed by a detected rollover whose day1 is still to publish */
    uint8_t rollTries;
    /* sleep3: clock */
    uint64_t lastSleepUs;      /* requested deep-sleep length of the last sleep */
    uint64_t sleepUsSinceNtp;  /* deep-sleep time accumulated since the last NTP step */
    float driftPpm;            /* RTC gain during deep sleep (+ = clock runs fast), compensated */
    bool driftValid;
    bool otherClockSet;        /* clock set by yard/time since the last NTP → skip drift estimate */
    uint8_t clkSrc;            /* 0 none 1 ntp(udp) 2 yard 3 sntp */
    bool timeUnsub;            /* yard/time unsubscribed in the persistent session */
    uint32_t yardIgnored;
};
RTC_DATA_ATTR static RtcState rtc;

static uint32_t g_wifiStartMs, g_wifiMs;
static bool g_wifiFast, g_wifiFallback, g_wifiCached;
static volatile bool g_ntpSynced = false;
static volatile bool g_flushOk = false;
static char g_flushTok[24];
static bool g_pvPublished = false;
static int g_slotMin = -1, g_slotHour = -1;
static uint8_t g_jobsRun = 0;          /* bit0 hist0, bit1 pv, bit2 yday, bit3 cmd */
static int g_advN[DEV_N];              /* decoded ADV packets averaged this wake */
static float g_boardV = NAN;
static esp_timer_handle_t g_awakeWdt = nullptr;

static void led(bool on) { digitalWrite(PIN_LED, on ? HIGH : LOW); }
/* HUZZAH32 VBAT = A13 / GPIO35 (ADC1), 1:2 divider. calibrated mV, 8-sample average */
static float boardV() {
    uint32_t mv = 0;
    for (int i = 0; i < 8; i++) mv += analogReadMilliVolts(PIN_VBAT);
    return (mv / 8) * 2.0f / 1000.0f;
}

static void tryHist();
static void pubDay(const DayRec &r, int day, const char *src);

static void logln(const char *s) {
    /* GATT STEP/RX/TX 는 시리얼만. MQTT 는 mppt/sense/status/hist/pv */
    Serial.println(s);
}
static void logf(const char *fmt, ...) {
    char buf[200];
    va_list ap; va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    logln(buf);
}
/* JSON number or null. snprintf %f of NAN is "nan" — invalid JSON. */
static void jsonNum(char *out, size_t n, bool have, double v, int prec) {
    if (!out || n < 5) return;
    if (!have || isnan(v) || isinf(v)) {
        snprintf(out, n, "null");
        return;
    }
    if (prec <= 0) snprintf(out, n, "%.0f", v);
    else if (prec == 1) snprintf(out, n, "%.1f", v);
    else snprintf(out, n, "%.2f", v);
}

static int tmYmd(const struct tm *t) {
    return (t->tm_year + 1900) * 10000 + (t->tm_mon + 1) * 100 + t->tm_mday;
}
static bool clockReady() {
    return g_clockOk && time(nullptr) >= 1700000000;
}
static int todayYmd() {
    if (!clockReady()) return 0;
    time_t now = time(nullptr);
    struct tm t; localtime_r(&now, &t);
    return tmYmd(&t);
}
static int yesterdayYmd() {
    if (!clockReady()) return 0;
    time_t now = time(nullptr) - 86400;
    struct tm t; localtime_r(&now, &t);
    return tmYmd(&t);
}
static void applyEpoch(time_t epoch) {
    if (epoch < 1700000000 || epoch > 2000000000) return;
    struct timeval tv; tv.tv_sec = epoch; tv.tv_usec = 0;
    settimeofday(&tv, nullptr);
    setenv("TZ", TZ_INFO, 1);
    tzset();
    g_clockOk = true;
    rtc.timeValid = true;                 /* sleep1: fallback clock source (NTP preferred) */
    rtc.otherClockSet = true;             /* sleep3: next NTP step is not pure RTC drift */
    rtc.clkSrc = 2;
    logf("STEP time mqtt ymd=%d", todayYmd());
    tryHist();
}
/* sleep1: blocking NTP sync with timeout. logs/stores the clock step = RTC drift since last sync */
static void ntpCb(struct timeval *) { g_ntpSynced = true; }
static int64_t nowUs() { struct timeval tv; gettimeofday(&tv, nullptr); return (int64_t)tv.tv_sec * 1000000LL + tv.tv_usec; }
static void setUs(int64_t us) {
    struct timeval tv; tv.tv_sec = (time_t)(us / 1000000LL); tv.tv_usec = (suseconds_t)(us % 1000000LL);
    settimeofday(&tv, nullptr);
}
/* sleep3: one SNTPv4 client request over UDP. returns true and the clock step applied (us). */
static bool ntpQuick(uint32_t timeoutMs, int64_t *stepOut) {
    IPAddress ip;
    if (!WiFi.hostByName(NTP_SERVER1, ip) && !WiFi.hostByName(NTP_SERVER2, ip)) return false;
    WiFiUDP udp;
    if (!udp.begin(4123)) return false;
    uint8_t pkt[48]; memset(pkt, 0, sizeof(pkt));
    pkt[0] = 0x23;                                    /* LI 0, VN 4, mode 3 (client) */
    while (udp.parsePacket() > 0) udp.clear();
    int64_t t1 = nowUs();
    udp.beginPacket(ip, 123); udp.write(pkt, sizeof(pkt));
    if (!udp.endPacket()) { udp.stop(); return false; }
    uint32_t t0 = millis();
    bool got = false;
    while (millis() - t0 < timeoutMs) {
        if (udp.parsePacket() >= 48 && udp.read(pkt, 48) == 48) { got = true; break; }
        delay(2);
    }
    int64_t t4 = nowUs();
    udp.stop();
    if (!got || (pkt[0] & 0x07) != 4 || pkt[1] == 0 || pkt[1] > 15) return false;
    auto ts = [&](int o) -> int64_t {
        uint32_t s = ((uint32_t)pkt[o] << 24) | ((uint32_t)pkt[o + 1] << 16) | ((uint32_t)pkt[o + 2] << 8) | pkt[o + 3];
        uint32_t f = ((uint32_t)pkt[o + 4] << 24) | ((uint32_t)pkt[o + 5] << 16) | ((uint32_t)pkt[o + 6] << 8) | pkt[o + 7];
        return ((int64_t)s - 2208988800LL) * 1000000LL + (int64_t)(((uint64_t)f * 1000000ULL) >> 32);
    };
    int64_t t2 = ts(32), t3 = ts(40);
    if (t3 < 1700000000LL * 1000000LL) return false;
    int64_t off = ((t2 - t1) + (t3 - t4)) / 2;         /* server - local */
    setUs(nowUs() + off);
    *stepOut = off;
    return true;
}
/* sleep3: drift estimate from an NTP step. step = server - local (local fast → negative). */
static void driftUpdate(int64_t stepUs) {
    uint64_t sl = rtc.sleepUsSinceNtp;
    bool use = !rtc.otherClockSet && sl >= (uint64_t)DRIFT_MIN_SLEEP_S * 1000000ULL;
    if (use) {
        double resid = -(double)stepUs * 1e6 / (double)sl;    /* ppm still uncorrected */
        float old = rtc.driftPpm;
        float np = rtc.driftValid ? old + 0.7f * (float)resid : (float)resid;
        if (np > DRIFT_MAX_PPM) np = DRIFT_MAX_PPM;
        if (np < -DRIFT_MAX_PPM) np = -DRIFT_MAX_PPM;
        rtc.driftPpm = np;
        rtc.driftValid = true;
        int32_t stored = prefs.getInt("drift", INT32_MIN);
        if (stored == INT32_MIN || labs((long)stored - (long)np) >= 200) prefs.putInt("drift", (int32_t)np);
        logf("STEP drift step=%ldms over %lus sleep → resid %.0f ppm, comp %.0f → %.0f ppm",
             (long)(stepUs / 1000), (unsigned long)(sl / 1000000ULL), resid, (double)old, (double)np);
    } else {
        logf("STEP drift skip (sleep %lus, other_set=%d)", (unsigned long)(sl / 1000000ULL), (int)rtc.otherClockSet);
    }
    rtc.sleepUsSinceNtp = 0;
    rtc.otherClockSet = false;
}
static bool ntpSync(uint32_t timeoutMs) {
    if (WiFi.status() != WL_CONNECTED) return false;
    {   /* sleep3: quick UDP NTP first */
        uint32_t q0 = millis();
        bool hadClock = clockReady();
        int64_t step = 0;
        if (ntpQuick(NTP_QUICK_TIMEOUT_MS, &step)) {
            time_t nowS = time(nullptr);
            if (hadClock) {
                rtc.lastNtpAdjMs = (int32_t)(step / 1000);
                driftUpdate(step);
            } else {
                rtc.sleepUsSinceNtp = 0;
                rtc.otherClockSet = false;
            }
            long since = hadClock && rtc.lastNtp ? (long)(nowS - rtc.lastNtp) : 0;
            rtc.lastNtp = nowS;
            rtc.timeValid = true;
            rtc.clkSrc = 1;
            setenv("TZ", TZ_INFO, 1);
            tzset();
            g_clockOk = true;
            g_ntpMs = millis() - q0;
            logf("STEP ntp(udp) ok %lums today=%d step=%ldms over %lds", (unsigned long)g_ntpMs, todayYmd(),
                 hadClock ? (long)(step / 1000) : 0L, since);
            return true;
        }
        logf("NOTE ntp(udp) failed %lums — SNTP", (unsigned long)(millis() - q0));
    }
    uint32_t s0 = millis();
    struct timeval before; gettimeofday(&before, nullptr);
    int64_t m0 = esp_timer_get_time();
    bool hadClock = clockReady();
    g_ntpSynced = false;
    sntp_set_time_sync_notification_cb(ntpCb);
    configTzTime(TZ_INFO, NTP_SERVER1, NTP_SERVER2);
    uint32_t t0 = millis();
    while (!g_ntpSynced && millis() - t0 < timeoutMs) delay(20);
    if (!g_ntpSynced || time(nullptr) < 1700000000) {
        logf("FAIL ntp %lums", (unsigned long)(millis() - t0));
        return false;
    }
    struct timeval after; gettimeofday(&after, nullptr);
    int64_t el = esp_timer_get_time() - m0;
    int64_t stepUs = ((int64_t)(after.tv_sec - before.tv_sec) * 1000000LL + (after.tv_usec - before.tv_usec)) - el;
    if (hadClock) { rtc.lastNtpAdjMs = (int32_t)(stepUs / 1000); driftUpdate(stepUs); }
    else { rtc.sleepUsSinceNtp = 0; rtc.otherClockSet = false; }
    long since = hadClock ? (long)(before.tv_sec - rtc.lastNtp) : 0;
    rtc.lastNtp = after.tv_sec;
    rtc.timeValid = true;
    rtc.clkSrc = 3;
    g_ntpMs = millis() - s0;
    setenv("TZ", TZ_INFO, 1);
    tzset();
    g_clockOk = true;
    logf("STEP ntp ok %lums today=%d step=%ldms over %lds", (unsigned long)(millis() - t0), todayYmd(),
         hadClock ? (long)(stepUs / 1000) : 0L, since);
    return true;
}

/* sleep2: ymdForDay(wall clock, d) removed — day ymds come from dayYmd() (seq-based base) */
/* days since 1970-01-01 for a yyyymmdd (civil calendar) */
static long ymdDays(int ymd) {
    int y = ymd / 10000, m = (ymd / 100) % 100, d = ymd % 100;
    y -= m <= 2;
    long era = (y >= 0 ? y : y - 399) / 400;
    unsigned yoe = (unsigned)(y - era * 400);
    unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (long)doe - 719468;
}
/* sleep2: yyyymmdd for days since 1970-01-01 (inverse of ymdDays, civil calendar) */
static int ymdFromDays(long z) {
    z += 719468;
    long era = (z >= 0 ? z : z - 146096) / 146097;
    unsigned doe = (unsigned)(z - era * 146097);
    unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    long y = (long)yoe + era * 400;
    unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    unsigned mp = (5 * doy + 2) / 153;
    unsigned d = doy - (153 * mp + 2) / 5 + 1;
    unsigned m = mp < 10 ? mp + 3 : mp - 9;
    y += (m <= 2);
    return (int)(y * 10000 + m * 100 + d);
}
static int ymdAdd(int ymd, int n) {
    if (!ymd) return 0;
    return ymdFromDays(ymdDays(ymd) + n);
}
static const int SEQ_MOD = 365;            /* day seq wraps 0..364 (see histSeqCheck) */
static const int SEQ_MAX_STEP = 7;         /* larger forward jumps = device reset → wall clock */
static const int FRESH_EVENING_HOUR = 18;  /* first boot: fresh day0 after this hour = rolled today */
static const uint16_t FRESH_STAGE_MIN = 5; /* "tiny" bulk/abs/float minutes for a fresh day0 */
/* ymd of day d in the current job */
static int dayYmd(int d) { return g_base0Ymd ? ymdAdd(g_base0Ymd, -d) : 0; }
/* day0 ymd expected before the page is read: stored label if it is within wall-1..wall+1 */
static int predictBase0(bool *bySeq) {
    *bySeq = false;
    int w = todayYmd();
    if (!w) return 0;
    if (rtc.d0Seq >= 0 && rtc.d0Ymd) {
        long g = ymdDays(rtc.d0Ymd) - ymdDays(w);
        if (g >= -1 && g <= 1) { *bySeq = true; return rtc.d0Ymd; }
    }
    return w;
}
static void d0Load() {
    int32_t s = prefs.getInt("d0seq", -1), y = prefs.getInt("d0ymd", 0);
    if (s >= 0 && s < SEQ_MOD && y >= 20200101 && y <= 20991231) { rtc.d0Seq = s; rtc.d0Ymd = y; }
    else { rtc.d0Seq = -1; rtc.d0Ymd = 0; }
    logf("STEP d0 nvs seq=%ld ymd=%ld", (long)rtc.d0Seq, (long)rtc.d0Ymd);
}
static void d0Store(int seq, int ymd) {
    bool chg = (rtc.d0Seq != seq || rtc.d0Ymd != ymd);
    rtc.d0Seq = seq; rtc.d0Ymd = ymd;
    if (!chg) return;                           /* NVS write ~once a day */
    if (prefs.getInt("d0seq", -1) != seq) prefs.putInt("d0seq", seq);
    if (prefs.getInt("d0ymd", 0) != ymd) prefs.putInt("d0ymd", ymd);
}
static bool stageTiny(uint16_t v) { return v == 0xFFFF || v <= FRESH_STAGE_MIN; }

/* NVS ring cache of closed days. key = "hNN", NN = dayNumber % 40; ymd inside validates. */
static void cacheKey(int ymd, char *k, size_t n) {
    long dn = ymdDays(ymd);
    snprintf(k, n, "h%02ld", ((dn % HIST_CACHE_SLOTS) + HIST_CACHE_SLOTS) % HIST_CACHE_SLOTS);
}
static void cacheSave(const DayRec &r) {
    if (!r.ok || !r.ymd) return;
    char k[8]; cacheKey(r.ymd, k, sizeof(k));
    DayRec rec;
    memcpy(&rec, &r, sizeof(rec));        /* byte copy so the memcmp below also sees padding */
    rec.cacheVer = HIST_CACHE_VER;
    DayRec old;
    if (prefs.getBytes(k, &old, sizeof(old)) == sizeof(old) && !memcmp(&old, &rec, sizeof(rec))) return;
    prefs.putBytes(k, &rec, sizeof(rec));
}
static bool cacheLoad(int ymd, DayRec &out) {
    if (!ymd) return false;
    char k[8]; cacheKey(ymd, k, sizeof(k));
    DayRec tmp;
    /* rev7 entries are shorter (size mismatch) → miss; cacheVer guards future same-size changes */
    if (prefs.getBytesLength(k) != sizeof(tmp)) return false;
    if (prefs.getBytes(k, &tmp, sizeof(tmp)) != sizeof(tmp)) return false;
    if (tmp.cacheVer != HIST_CACHE_VER) return false;
    if (!tmp.ok || tmp.ymd != ymd) return false;
    out = tmp;
    return true;
}
static void cacheClear() {
    char k[8];
    for (int i = 0; i < HIST_CACHE_SLOTS; i++) {
        snprintf(k, sizeof(k), "h%02d", i);
        prefs.remove(k);
    }
}

/* hist cmd: NTP/yard-time 날짜가 있어야 NVS 비교.
 * day1..N 이 모두 NVS 캐시에 있으면 GATT skip. 없으면 GATT 로 day0 + 빠진 날. */
static void tryHist() {
    if (!g_histQueued) return;
    if (g_mode == MODE_GATT) return;
    if (!clockReady()) {
        logln("WAIT clock — hist queued (NTP or yard/time)");
        return;
    }
    g_histQueued = false;
    int n = g_histReqDays;
    if (n < 0) n = 0;
    if (n > HIST_MAX_DAYS) n = HIST_MAX_DAYS;
    bool bySeq;
    int base = predictBase0(&bySeq);              /* sleep2: day k = predicted day0 ymd - k */
    int missing = 0;
    DayRec r;
    for (int d = 1; d <= n; d++)
        if (!cacheLoad(ymdAdd(base, -d), r)) missing++;
    if (n >= 1 && !missing) {
        /* rev10: day1..N from NVS right away, then GATT for day0 only so today is published too */
        logf("NVS hit day1..%d (base %d %s) — GATT day0 only", n, base, bySeq ? "seq" : "clock");
        g_lblSeq = bySeq;
        for (int d = 1; d <= n; d++)
            if (cacheLoad(ymdAdd(base, -d), r)) pubDay(r, d, "nvs");
        g_histN = 0;
        g_histCont = false;
        g_wantHist = true;
        return;
    }
    logf("NVS miss %d/%d — GATT day0..%d", missing, n, n);
    g_histN = n;
    g_histCont = false;                       /* new job (a running resume chain is replaced) */
    g_wantHist = true;
}

static void macNorm(const char *in, char *out12) {
    int n = 0;
    for (const char *p = in; *p && n < 12; ++p) {
        if (isxdigit((unsigned char)*p)) {
            char c = *p;
            if (c >= 'A' && c <= 'F') c = (char)(c - 'A' + 'a');
            out12[n++] = c;
        }
    }
    out12[n] = 0;
}
static int findDev(const char *macRaw) {
    char norm[16]; macNorm(macRaw, norm);
    if (strlen(norm) != 12) return -1;
    for (int i = 0; i < DEV_N; i++) if (!strcmp(norm, g_dev[i].macHex)) return i;
    return -1;
}

/* sleep1: WiFi with cached BSSID/channel (RTC) → fallback to a normal scan+connect */
static void wifiStart() {
    WiFi.persistent(false);
    WiFi.mode(WIFI_STA);
    WiFi.setSleep(false);
#if USE_STATIC_IP
    WiFi.config(STATIC_IP, STATIC_GW, STATIC_MASK, STATIC_DNS);
#endif
    WiFi.setHostname(MQTT_CLIENT);
    g_wifiFast = rtc.wifiCacheOk && rtc.wifiCh >= 1 && rtc.wifiCh <= 14;
    if (g_wifiFast) WiFi.begin(WIFI_SSID, WIFI_PASS, rtc.wifiCh, rtc.wifiBssid, true);
    else WiFi.begin(WIFI_SSID, WIFI_PASS);
    g_wifiStartMs = millis();
    g_wifiFallback = g_wifiCached = false;
}
/* non-blocking step; true when connected */
static bool wifiPoll() {
    if (WiFi.status() == WL_CONNECTED) {
        if (!g_wifiCached) {
            g_wifiCached = true;
            g_wifiMs = millis() - g_wifiStartMs;
            const uint8_t *b = WiFi.BSSID();
            if (b) { memcpy(rtc.wifiBssid, b, 6); rtc.wifiCh = (uint8_t)WiFi.channel(); rtc.wifiCacheOk = true; }
            logf("STEP wifi up %lums fast=%d fb=%d ch=%u rssi=%d ip=%s", (unsigned long)g_wifiMs,
                 (int)g_wifiFast, (int)g_wifiFallback, (unsigned)rtc.wifiCh, WiFi.RSSI(),
                 WiFi.localIP().toString().c_str());
        }
        return true;
    }
    if (g_wifiFast && !g_wifiFallback && millis() - g_wifiStartMs > WIFI_FAST_TIMEOUT_MS) {
        logln("STEP wifi fast connect failed — normal connect");
        rtc.wifiCacheOk = false;
        g_wifiFallback = true;
        WiFi.disconnect();
        WiFi.begin(WIFI_SSID, WIFI_PASS);
    }
    return false;
}
static bool wifiWait(uint32_t timeoutMs) {
    while (millis() - g_wifiStartMs < timeoutMs) {
        if (wifiPoll()) return true;
        delay(20);
    }
    logf("FAIL wifi st=%d after %lums", (int)WiFi.status(), (unsigned long)(millis() - g_wifiStartMs));
    return false;
}

static void mqttCb(char *topic, byte *payload, unsigned int len) {
    if (!topic || !len) return;
    char tmp[96];
    if (len >= sizeof(tmp)) len = sizeof(tmp) - 1;
    memcpy(tmp, payload, len); tmp[len] = 0;
    if (!strcmp(topic, TOPIC_LWT)) {          /* sleep1: echo of our own state msg = broker has everything */
        if (g_flushTok[0] && strstr(tmp, g_flushTok)) g_flushOk = true;
        return;
    }
    if (!strcmp(topic, TOPIC_TIME)) {
        /* sleep3: only a fallback clock — a retained/periodic yard/time used to reset the drifting
         * RTC on long (job) wakes, which made ts_offset a 10-min sawtooth */
        if (clockReady() && rtc.lastNtp && time(nullptr) - rtc.lastNtp < YARD_MAX_NTP_AGE_S) {
            rtc.yardIgnored++;
            return;
        }
        time_t epoch = 0;
        if (tmp[0] == '{') {
            const char *p = strstr(tmp, "\"epoch\"");
            if (p && (p = strchr(p, ':'))) epoch = (time_t)strtol(p + 1, nullptr, 10);
        } else epoch = (time_t)strtol(tmp, nullptr, 10);
        applyEpoch(epoch);
        return;
    }
    if (strcmp(topic, TOPIC_CMD) != 0) return;
    for (char *p = tmp; *p; ++p) if (*p >= 'A' && *p <= 'Z') *p = (char)(*p - 'A' + 'a');
    logf("CMD %s", tmp);
    if (strstr(tmp, "unpair")) g_wantUnpair = true;
    else if (strstr(tmp, "pv")) g_wantPv = true;
    else if (strstr(tmp, "hist")) {
        /* optional "days":N (0..30). default HIST_DAYS */
        int days = HIST_DAYS;
        const char *pd = strstr(tmp, "\"days\"");
        if (pd && (pd = strchr(pd, ':'))) days = (int)strtol(pd + 1, nullptr, 10);
        if (days < 0) days = 0;
        if (days > HIST_MAX_DAYS) days = HIST_MAX_DAYS;
        g_histReqDays = days;
        g_histQueued = true;
        tryHist();
    }
}

static void mqttConnect() {
    if (mqtt.connected() || WiFi.status() != WL_CONNECTED) return;
    mqtt.setServer(MQTT_HOST, MQTT_PORT);
    mqtt.setCallback(mqttCb);
    mqtt.setKeepAlive(30);
    mqtt.setSocketTimeout(3);
    mqtt.setBufferSize(1280); /* status JSON ~900 B + topic + header */
    /* sleep1: no will (a sleeping node is not "offline"); persistent session queues QoS1 commands */
    if (mqtt.connect(MQTT_CLIENT, MQTT_USER, MQTT_PASS, nullptr, 0, false, nullptr, !MQTT_PERSISTENT)) {
        mqtt.subscribe(TOPIC_CMD, 1);
        mqtt.subscribe(TOPIC_LWT, 0);         /* flush check */
        bool ntpFresh = clockReady() && rtc.lastNtp && time(nullptr) - rtc.lastNtp < YARD_MAX_NTP_AGE_S;
        if (!ntpFresh) {                            /* retained time only as a fallback clock */
            mqtt.subscribe(TOPIC_TIME);
            rtc.timeUnsub = false;
        } else if (!rtc.timeUnsub) {
            /* sleep3: persistent session keeps an old yard/time subscription → drop it once */
            if (mqtt.unsubscribe(TOPIC_TIME)) { rtc.timeUnsub = true; logln("STEP yard/time unsubscribed"); }
        }
        logln("STEP mqtt up");
    } else {
        logf("FAIL mqtt rc=%d", mqtt.state());
    }
}
static void mqttKeep() {
    static uint32_t lastTry;
    if (WiFi.status() != WL_CONNECTED) return;
    /* no blocking reconnect while a GATT session is timing replies */
    if (!mqtt.connected() && g_mode != MODE_GATT && millis() - lastTry > 3000) { lastTry = millis(); mqttConnect(); }
    mqtt.loop();
}

static bool aes128_ctr(const uint8_t key[16], uint16_t iv,
                       const uint8_t *ct, size_t ctLen, uint8_t *pt) {
    mbedtls_aes_context ctx;
    mbedtls_aes_init(&ctx);
    if (mbedtls_aes_setkey_enc(&ctx, key, 128) != 0) { mbedtls_aes_free(&ctx); return false; }
    uint8_t nonce[16]; memset(nonce, 0, 16);
    nonce[0] = (uint8_t)(iv & 0xFF); nonce[1] = (uint8_t)(iv >> 8);
    uint8_t stream[16]; size_t nc_off = 0;
    int rc = mbedtls_aes_crypt_ctr(&ctx, ctLen, &nc_off, nonce, stream, ct, pt);
    mbedtls_aes_free(&ctx);
    return rc == 0;
}
static uint32_t brU(const uint8_t *d, size_t len, int *idx, int nbits) {
    uint32_t v = 0;
    for (int i = 0; i < nbits; i++) {
        int bit = *idx + i, by = bit / 8, bi = bit % 8;
        if ((size_t)by >= len) break;
        if (d[by] & (1u << bi)) v |= (1u << i);
    }
    *idx += nbits;
    return v;
}
static int32_t brS(const uint8_t *d, size_t len, int *idx, int nbits) {
    uint32_t u = brU(d, len, idx, nbits);
    uint32_t sign = 1u << (nbits - 1);
    if (u & sign) return (int32_t)(u | (~0u << nbits));
    return (int32_t)u;
}
static const char *chargeName(uint8_t s) {
    switch (s) {
        case 0: return "off"; case 2: return "fault"; case 3: return "bulk";
        case 4: return "abs"; case 5: return "float"; case 6: return "storage";
        case 7: return "equalize"; case 245: return "starting"; default: return "unknown";
    }
}

/* sleep3: result codes (was bool). OTHER = Victron record that is not an Instant Readout (0x10) */
enum { DEC_FAIL = 0, DEC_OK = 1, DEC_DUP = 2, DEC_OTHER = 3 };
static int parseVictron(int di, const uint8_t *mfg, size_t mlen, int rssi,
                        const char *addr, const char *name, uint8_t addrType, bool accumulate = true) {
    size_t off = 0;
    uint16_t cid = (uint16_t)mfg[0] | ((uint16_t)mfg[1] << 8);
    if (cid == VICTRON_CID) off = 2;
    if (mlen <= off || mfg[off] != 0x10) return DEC_OTHER;
    const uint8_t *p = mfg + off;
    size_t n = mlen - off;
    if (n < 10) return DEC_FAIL;
    if (p[7] != g_dev[di].key[0]) return DEC_FAIL;
    uint16_t model = (uint16_t)p[2] | ((uint16_t)p[3] << 8);
    uint8_t rec = p[4];
    uint16_t iv = (uint16_t)p[5] | ((uint16_t)p[6] << 8);
    size_t ctLen = n - 8;
    if (ctLen < 8 || ctLen > 24) return DEC_FAIL;
    uint8_t pt[32]; memset(pt, 0, sizeof(pt));
    if (!aes128_ctr(g_dev[di].key, iv, p + 8, ctLen, pt)) return DEC_FAIL;
    DevState &s = g_st[di];
    if (s.seen && s.nonce == iv && (millis() - s.lastMs) < 400) return DEC_DUP;
    s.rssi = rssi; s.model = model; s.recType = rec; s.nonce = iv;
    s.lastMs = millis(); s.seen = true; s.addrType = addrType;
    strncpy(s.addr, addr, sizeof(s.addr) - 1);
    strncpy(s.name, name && name[0] ? name : g_dev[di].label, sizeof(s.name) - 1);
    if (rec == 0x01 || di == DEV_MPPT) {
        int idx = 0;
        s.chargeState = (uint8_t)brU(pt, ctLen, &idx, 8);
        s.chargerErr  = (uint8_t)brU(pt, ctLen, &idx, 8);
        int32_t vv = brS(pt, ctLen, &idx, 16);
        int32_t ii = brS(pt, ctLen, &idx, 16);
        uint32_t yld = brU(pt, ctLen, &idx, 16);
        uint32_t pw = brU(pt, ctLen, &idx, 16);
        uint32_t ld = brU(pt, ctLen, &idx, 9);
        s.vbat = (vv == 0x7FFF) ? NAN : vv / 100.0f;
        s.ibat = (ii == 0x7FFF) ? NAN : ii / 10.0f;
        s.yieldWh = (yld == 0xFFFF) ? NAN : yld * 10.0f;
        s.pvW = (pw == 0xFFFF) ? NAN : (float)pw;
        s.loadA = (ld == 0x1FF) ? NAN : ld / 10.0f;
        s.ok = !isnan(s.vbat);
    } else {
        int idx = 0;
        (void)brU(pt, ctLen, &idx, 16);
        int32_t vv = brS(pt, ctLen, &idx, 16);
        (void)brU(pt, ctLen, &idx, 16);
        uint32_t aux = brU(pt, ctLen, &idx, 16);
        uint32_t auxMode = brU(pt, ctLen, &idx, 2);
        s.vbat = (vv == 0x7FFF) ? NAN : vv / 100.0f;
        s.hasTemp = (auxMode == 2);
        s.tempC = s.hasTemp ? aux / 100.0f - 273.15f : NAN;
        s.ok = !isnan(s.vbat);
    }
    if (s.ok && accumulate) {
        Acc &a = g_acc[di];
        if (!isnan(s.vbat)) { a.vbat += s.vbat; a.n++; }
        a.rssi += s.rssi;
        if (di == DEV_MPPT) {
            if (!isnan(s.ibat)) a.ibat += s.ibat;
            if (!isnan(s.pvW)) a.pvW += s.pvW;
            if (!isnan(s.yieldWh)) a.yieldWh += s.yieldWh;
            if (!isnan(s.loadA)) a.loadA += s.loadA;
            a.state = s.chargeState; a.err = s.chargerErr;
        } else if (s.hasTemp && !isnan(s.tempC)) { a.tempC += s.tempC; a.nTemp++; }
    }
    return s.ok ? DEC_OK : DEC_FAIL;
}

static int findDevByKeyCheck(const uint8_t *mfg, size_t mlen) {
    for (int i = 0; i < DEV_N; i++) {
        DevState tmp = g_st[i];
        /* sleep3: probe without accumulating (the real call below accumulated it a second time) */
        int r = parseVictron(i, mfg, mlen, 0, "", "", 0, false);
        g_st[i] = tmp;
        if (r == DEC_OK || r == DEC_DUP) return i;
    }
    return -1;
}


class ScanCB : public NimBLEScanCallbacks {
    void onResult(const NimBLEAdvertisedDevice *dev) override {
        if (g_mode != MODE_ADV) return;
        if (!dev->haveManufacturerData()) return;
        std::string md = dev->getManufacturerData();
        if (md.size() < 4) return;
        const uint8_t *raw = (const uint8_t *)md.data();
        uint16_t cid = (uint16_t)raw[0] | ((uint16_t)raw[1] << 8);
        if (!(cid == VICTRON_CID || raw[0] == 0x10)) return;
        String addr = String(dev->getAddress().toString().c_str());
        int di = findDev(addr.c_str());
        if (di < 0) di = findDevByKeyCheck(raw, md.size());
        if (di < 0) return;
        g_scanHits++;
        int r = parseVictron(di, raw, md.size(), dev->getRSSI(),
                             addr.c_str(), dev->getName().c_str(),
                             dev->getAddress().getType());
        if (r == DEC_OK) g_decOk++;
        else if (r == DEC_DUP) g_decDup++;
        else if (r == DEC_OTHER) g_decOther++;
        else g_decFail++;
    }
};
static ScanCB g_scanCb;

static void bleSec() {
    NimBLEDevice::setMTU(185);
    NimBLEDevice::setPower(ESP_PWR_LVL_P9);
    NimBLEDevice::setSecurityIOCap(BLE_HS_IO_KEYBOARD_ONLY);
    NimBLEDevice::setSecurityAuth(true, true, true);
    NimBLEDevice::setSecurityInitKey(BLE_SM_PAIR_KEY_DIST_ENC | BLE_SM_PAIR_KEY_DIST_ID);
    NimBLEDevice::setSecurityRespKey(BLE_SM_PAIR_KEY_DIST_ENC | BLE_SM_PAIR_KEY_DIST_ID);
}
static void bleStackReset() {
    logln("STEP ble host reset");
    NimBLEDevice::deinit(true);
    delay(250);
    NimBLEDevice::init("vt-br");
    bleSec();
    g_connFails = 0;
}
static void bleScanStart() {

    NimBLEScan *scan = NimBLEDevice::getScan();
    scan->setScanCallbacks(&g_scanCb, false);
    scan->setMaxResults(0);    /* callbacks only — default 0xFF stores every address forever */
    scan->setActiveScan(true); /* bleak-like; connectable ADV */
    scan->setInterval(SCAN_INTERVAL);
    scan->setWindow(SCAN_WINDOW);        /* sleep3: 100 % duty */
    scan->setDuplicateFilter(false);
    scan->start(0, false);
    g_lastScanRst = millis();
}
static void bleScanStop() {
    NimBLEScan *s = NimBLEDevice::getScan();
    if (s->isScanning()) s->stop();
}

static void accReset(Acc &a) { memset(&a, 0, sizeof(a)); }

/* sleep1: called once per wake after the ADV collection (no interval gate) */
static void publishLive() {
    g_advN[DEV_MPPT] = g_acc[DEV_MPPT].n;
    g_advN[DEV_SENSE] = g_acc[DEV_SENSE].n;
    if (!mqtt.connected()) { accReset(g_acc[0]); accReset(g_acc[1]); return; }
    long ts = clockReady() ? (long)time(nullptr) : 0;
    char buf[400];
    Acc &m = g_acc[DEV_MPPT];
    if (m.n > 0) {
        snprintf(buf, sizeof(buf),
                 "{\"id\":\"mppt\",\"src\":\"victron_ble\",\"sn\":\"" MPPT_SN "\","
                 "\"model\":\"MPPT 75/15\",\"mac\":\"%s\",\"rssi\":%d,"
                 "\"vbat\":%.2f,\"ibat\":%.2f,\"power\":%.0f,\"yield_wh\":%.0f,"
                 "\"load_a\":%.1f,\"state\":\"%s\",\"state_n\":%u,\"error\":%u,\"n\":%d,\"ts\":%ld}",
                 g_st[DEV_MPPT].addr, m.n ? (int)(m.rssi / m.n) : 0,
                 m.vbat / m.n, m.n ? m.ibat / m.n : NAN,
                 m.n ? m.pvW / m.n : NAN, m.n ? m.yieldWh / m.n : NAN,
                 m.n ? m.loadA / m.n : NAN,
                 chargeName(m.state), (unsigned)m.state, (unsigned)m.err, m.n, ts);
        mqtt.publish(TOPIC_MPPT, buf, false);
    }
    Acc &s = g_acc[DEV_SENSE];
    if (s.n > 0) {
        if (s.nTemp)
            snprintf(buf, sizeof(buf),
                     "{\"id\":\"sense\",\"src\":\"victron_ble\",\"sn\":\"" SENSE_SN "\","
                     "\"model\":\"SmartBatterySense\",\"mac\":\"%s\",\"rssi\":%d,"
                     "\"vbat\":%.2f,\"temp\":%.1f,\"n\":%d,\"ts\":%ld}",
                     g_st[DEV_SENSE].addr, (int)(s.rssi / s.n),
                     s.vbat / s.n, s.tempC / s.nTemp, s.n, ts);
        else
            snprintf(buf, sizeof(buf),
                     "{\"id\":\"sense\",\"src\":\"victron_ble\",\"sn\":\"" SENSE_SN "\","
                     "\"model\":\"SmartBatterySense\",\"mac\":\"%s\",\"rssi\":%d,"
                     "\"vbat\":%.2f,\"n\":%d,\"ts\":%ld}",
                     g_st[DEV_SENSE].addr, (int)(s.rssi / s.n), s.vbat / s.n, s.n, ts);
        mqtt.publish(TOPIC_SENSE, buf, false);
    }
    accReset(g_acc[0]); accReset(g_acc[1]);
}

/* sleep1: one status per wake. "vbat" = HUZZAH32 VBAT pin (board battery), read before the radios */
static void publishStatus() {
    if (!mqtt.connected()) return;
    char buf[1100];
    long now = clockReady() ? (long)time(nullptr) : 0;
    int n = snprintf(buf, sizeof(buf),
             "{\"src\":\"victron_ble\",\"board\":\"feather\",\"fw\":\"sleep3\",\"ts\":%ld,"
             "\"boot\":%lu,\"wake\":\"%02d:%02d\",\"awake_ms\":%lu,\"prev_awake_ms\":%lu,\"wdt\":%lu,"
             "\"adv_mppt_n\":%d,\"adv_sense_n\":%d,"
             "\"mppt_seen\":%s,\"sense_seen\":%s,\"mppt_rssi\":%d,\"sense_rssi\":%d,"
             "\"hits\":%lu,\"dec_ok\":%lu,\"dec_fail\":%lu,\"dec_dup\":%lu,\"dec_other\":%lu,"
             "\"clock\":\"%s\",\"clk\":\"%s\",\"ntp_ms\":%lu,\"drift_ppm\":%ld,\"yard_ign\":%lu,"
             "\"ntp_age\":%ld,\"ntp_adj_ms\":%ld,\"today\":%d,\"yday\":%d,"
             "\"day0_done\":%ld,\"yday_done\":%ld,\"job_pend\":%u,\"jobs\":%u,"
             "\"wifi_rssi\":%d,\"wifi_ms\":%lu,\"wifi_fast\":%s,\"vbat\":%.2f,\"ip\":\"%s\","
             "\"heap\":%lu,\"heap_min\":%lu,\"rst\":%d}",
             now, (unsigned long)rtc.boots, g_slotHour < 0 ? 0 : g_slotHour, g_slotMin < 0 ? 0 : g_slotMin,
             (unsigned long)millis(), (unsigned long)rtc.prevAwakeMs, (unsigned long)rtc.wdtTrips,
             g_advN[DEV_MPPT], g_advN[DEV_SENSE],
             g_st[DEV_MPPT].seen ? "true" : "false",
             g_st[DEV_SENSE].seen ? "true" : "false",
             g_st[DEV_MPPT].seen ? g_st[DEV_MPPT].rssi : 0,
             g_st[DEV_SENSE].seen ? g_st[DEV_SENSE].rssi : 0,
             (unsigned long)g_scanHits, (unsigned long)g_decOk, (unsigned long)g_decFail,
             (unsigned long)g_decDup, (unsigned long)g_decOther,
             g_clockOk ? "ok" : "wait",
             rtc.clkSrc == 1 ? "ntp" : rtc.clkSrc == 2 ? "yard" : rtc.clkSrc == 3 ? "sntp" : "none",
             (unsigned long)g_ntpMs, rtc.driftValid ? (long)lroundf(rtc.driftPpm) : 0L,
             (unsigned long)rtc.yardIgnored,
             (rtc.lastNtp && now) ? (long)(now - rtc.lastNtp) : -1L,
             (long)rtc.lastNtpAdjMs, todayYmd(), yesterdayYmd(),
             (long)rtc.day0DoneYmd, (long)rtc.ydayDoneYmd, (unsigned)rtc.jobPend, (unsigned)g_jobsRun,
             WiFi.RSSI(), (unsigned long)g_wifiMs, g_wifiFast && !g_wifiFallback ? "true" : "false",
             isnan(g_boardV) ? 0.0 : (double)g_boardV,
             WiFi.isConnected() ? WiFi.localIP().toString().c_str() : "",
             (unsigned long)ESP.getFreeHeap(), (unsigned long)ESP.getMinFreeHeap(),
             (int)esp_reset_reason());
    if (n > 0 && n < (int)sizeof(buf)) mqtt.publish(TOPIC_STATUS, buf, false);
    else logf("FAIL status json %d", n);
}

/* ---- CBOR (RFC 8949) item sizing. Victron VREG-over-BLE frames: op seq 19 HI LO <item> ---- */
#define CBOR_INCOMPLETE (-1)
#define CBOR_BAD        (-2)
/* head: initial byte + argument. returns head length, CBOR_INCOMPLETE or CBOR_BAD. */
static int cborHead(const uint8_t *p, int n, uint64_t *val) {
    if (n < 1) return CBOR_INCOMPLETE;
    uint8_t ai = p[0] & 0x1F;
    if (ai < 24) { *val = ai; return 1; }
    int extra = (ai == 24) ? 1 : (ai == 25) ? 2 : (ai == 26) ? 4 : (ai == 27) ? 8 : -1;
    if (extra < 0) return CBOR_BAD;            /* 28..30 reserved, 31 indefinite — not used here */
    if (n < 1 + extra) return CBOR_INCOMPLETE;
    uint64_t v = 0;
    for (int i = 0; i < extra; i++) v = (v << 8) | p[1 + i];   /* CBOR argument is big-endian */
    *val = v;
    return 1 + extra;
}
/* total length of one item: ints (major 0/1), byte/text strings (2/3, incl. 0x58/0x59/0x5a),
 * simple/float (7, incl. f16/f32/f64). arrays/maps/tags (4/5/6): head only. */
static int cborLen(const uint8_t *p, int n) {
    uint64_t v = 0;
    int h = cborHead(p, n, &v);
    if (h < 0) return h;
    switch (p[0] >> 5) {
        case 2: case 3: {
            if (v > (uint64_t)(PARSE_CAP - 8)) return CBOR_BAD;   /* can never fit → garbage */
            int need = h + (int)v;
            return n >= need ? need : CBOR_INCOMPLETE;
        }
        default:
            return h;
    }
}
/* numeric value: CBOR uint / negint, or Victron little-endian byte string of 1/2/4 bytes */
static bool cborNum(const uint8_t *p, int n, double *out) {
    uint64_t v = 0;
    int h = cborHead(p, n, &v);
    if (h < 0) return false;
    uint8_t mt = p[0] >> 5;
    if (mt == 0) { *out = (double)v; return true; }
    if (mt == 1) { *out = -1.0 - (double)v; return true; }
    if (mt == 2) {
        if (n < h + (int)v) return false;
        const uint8_t *b = p + h;
        if (v == 1) { *out = b[0]; return true; }
        if (v == 2) { *out = (double)((uint16_t)b[0] | ((uint16_t)b[1] << 8)); return true; }
        if (v == 4) {
            *out = (double)((uint32_t)b[0] | ((uint32_t)b[1] << 8) | ((uint32_t)b[2] << 16) | ((uint32_t)b[3] << 24));
            return true;
        }
    }
    return false;
}
static uint32_t u32le(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}
static uint16_t u16le(const uint8_t *p) { return (uint16_t)p[0] | ((uint16_t)p[1] << 8); }

static int histDayOfReg(uint16_t reg) {
    if (reg >= REG_HIST_DAY0 && reg <= REG_HIST_DAY0 + HIST_MAX_DAYS) return reg - REG_HIST_DAY0;
    return -1;
}

/* sleep2: ymd for the day0 record r from its seq vs the stored (snapshot) seq/ymd. */
static int labelDay0(const DayRec &r, bool *bySeq, bool *roll) {
    *bySeq = false; *roll = false;
    int w = todayYmd();
    if (!w) return g_base0Ymd;
    if (!r.hasSeq || r.seq >= SEQ_MOD) return w;
    if (g_prevSeq < 0 || !g_prevYmd) {
        /* first boot / no stored seq: wall clock, unless it is evening and day0 is fresh
         * (= the device already closed today's history day) → tomorrow */
        time_t now = time(nullptr);
        struct tm t; localtime_r(&now, &t);
        bool fresh = r.yieldKwh == 0 && r.pmaxW == 0 &&
                     stageTiny(r.tBulk) && stageTiny(r.tAbs) && stageTiny(r.tFloat);
        if (t.tm_hour >= FRESH_EVENING_HOUR && fresh) {
            logf("NOTE d0 first seq=%u: evening + fresh day0 → tomorrow", (unsigned)r.seq);
            return ymdAdd(w, 1);
        }
        return w;
    }
    int diff = ((int)r.seq - (int)g_prevSeq + SEQ_MOD) % SEQ_MOD;
    long gap = ymdDays(g_prevYmd) - ymdDays(w);           /* stored label - wall date */
    if (diff == 0) {
        if (gap >= -1 && gap <= 1) { *bySeq = true; return g_prevYmd; }
        return w;                                          /* stale stored label */
    }
    if (diff > SEQ_MAX_STEP) {
        logf("WARN d0 seq %ld → %u jump — wall clock label", (long)g_prevSeq, (unsigned)r.seq);
        return w;
    }
    *roll = true;                                          /* previous device day is closed */
    int lab;
    if (gap == 0) lab = ymdAdd(g_prevYmd, 1);              /* evening rollover */
    else lab = ymdAdd(g_prevYmd, diff);
    long g2 = ymdDays(lab) - ymdDays(w);
    if (g2 < -1 || g2 > 1) {                               /* implausible vs wall clock */
        logf("WARN d0 seq label %d far from wall %d — wall clock", lab, w);
        *roll = false;
        return w;
    }
    *bySeq = true;
    return lab;
}
/* sleep2: called once per job when the day0 page arrives */
static void day0Label(const DayRec &r) {
    if (g_day0Labeled) return;
    g_day0Labeled = true;
    bool bySeq, roll;
    int lab = labelDay0(r, &bySeq, &roll);
    if (!lab) return;
    int old = g_base0Ymd;
    g_base0Ymd = lab;
    g_lblSeq = bySeq;
    logf("STEP d0 seq=%u prev=%ld@%ld wall=%d → ymd=%d (%s)%s", (unsigned)r.seq, (long)g_prevSeq,
         (long)g_prevYmd, todayYmd(), lab, bySeq ? "seq" : "clock", roll ? " ROLLOVER" : "");
    if (r.hasSeq && r.seq < SEQ_MOD) d0Store(r.seq, lab);
    /* day1..N slots planned with the predicted base: re-check cached days against the final base */
    for (int d = 1; d <= g_histN && d <= HIST_MAX_DAYS; d++) {
        if (g_dayState[d] != DAY_CACHED) continue;
        DayRec &c = g_day[d];
        int want = ((int)r.seq - d) % SEQ_MOD;
        if (want < 0) want += SEQ_MOD;
        bool seqBad = r.hasSeq && c.hasSeq && c.seq != want;
        if (c.ymd == dayYmd(d) && !seqBad) continue;
        if (!seqBad && cacheLoad(dayYmd(d), c)) continue;
        logf("STEP day%d cache %d seq=%u stale for base %d — re-read", d, c.ymd, (unsigned)c.seq, lab);
        memset(&c, 0, sizeof(c));
        g_dayState[d] = DAY_PENDING;
        g_dayPub[d] = false;
    }
    if (old && old != lab) logf("STEP d0 base %d → %d", old, lab);
    if (roll) {
        /* the previous device day just closed: read day1 now (final values) → "yesterday" */
        rtc.rollYdayYmd = ymdAdd(lab, -1);
        rtc.rollTries = 0;
        if (g_histN < 1) g_histN = 1;
        g_dayState[1] = DAY_PENDING;
        g_dayTries[1] = 0;
        g_dayPub[1] = false;
        memset(&g_day[1], 0, sizeof(g_day[1]));
        logf("STEP rollover → day1 (%d) queued", rtc.rollYdayYmd);
    }
}

/* History day record (34 bytes):
 *  0 version(0; 255=unknown) 1 yield u32 .01kWh 5 consumed u32 9 vbat max u16 .01V 11 vbat min
 *  13 err db 14..17 errors 18 t bulk 20 t abs 22 t float 24 pmax u32 W 28 ibat max u16 .1A
 *  30 vpv max u16 .01V 32 day seq u16 */
static void parseDayPage(int day, const uint8_t *p, int n) {
    if (day < 0 || day > HIST_MAX_DAYS) return;
    DayRec &r = g_day[day];
    if (n < 34 || p[0] == 255) {
        logf("NOTE day%d page empty n=%d v=%u", day, n, n ? (unsigned)p[0] : 0);
        if (g_dayState[day] == DAY_PENDING) g_dayState[day] = DAY_EMPTY;
        return;
    }
#if HIST_DEBUG_FRAMES
    {
        char hx[34 * 2 + 1];
        for (int b = 0; b < 34; b++) snprintf(hx + 2 * b, 3, "%02X", p[b]);
        logf("PAGE day%d %s", day, hx);
    }
#endif
    uint32_t yld = u32le(p + 1);
    uint32_t cns = u32le(p + 5);
    uint32_t pmx = u32le(p + 24);
    uint16_t vbM = u16le(p + 9), vbm = u16le(p + 11), vpv = u16le(p + 30), seq = u16le(p + 32);
    r.yieldKwh = (yld == 0xFFFFFFFFu) ? 0 : yld / 100.0f;
    r.consumedKwh = (cns == 0xFFFFFFFFu) ? 0 : cns / 100.0f;
    r.vbatMax  = (vbM == 0xFFFF) ? 0 : vbM / 100.0f;
    r.vbatMin  = (vbm == 0xFFFF) ? 0 : vbm / 100.0f;
    r.pmaxW    = (pmx == 0xFFFFFFFFu) ? 0 : (float)pmx;
    r.vpvMax   = (vpv == 0xFFFF) ? 0 : vpv / 100.0f;
    r.hasYield = (yld != 0xFFFFFFFFu);
    r.hasPmax  = (pmx != 0xFFFFFFFFu);
    r.hasSeq   = (seq != 0xFFFF);
    r.seq      = seq;
    memcpy(r.err, p + 14, 4);             /* rev8: error 0..3 */
    r.tBulk    = u16le(p + 18);
    r.tAbs     = u16le(p + 20);
    r.tFloat   = u16le(p + 22);
    r.iBatMax  = u16le(p + 28);           /* rev9: 0.1 A */
    r.hasPage  = true;
    r.ok = r.hasYield || r.hasPmax;
    if (day == 0 && r.ok) day0Label(r);   /* sleep2: may move g_base0Ymd and extend the job to day1 */
    r.ymd = dayYmd(day);
    g_dayState[day] = r.ok ? DAY_OK : DAY_EMPTY;
    if (r.ok && day >= 1) cacheSave(r);   /* day0 is still open — not cached */
    logf("RX day%d ymd=%d seq=%u y=%.2f cons=%.2f pmax=%.0f ibat=%.1f bulk=%u abs=%u float=%u err=%u,%u,%u,%u",
         day, r.ymd, (unsigned)seq, (double)r.yieldKwh, (double)r.consumedKwh, (double)r.pmaxW,
         r.iBatMax == 0xFFFF ? -1.0 : r.iBatMax / 10.0,
         (unsigned)r.tBulk, (unsigned)r.tAbs, (unsigned)r.tFloat,
         (unsigned)r.err[0], (unsigned)r.err[1], (unsigned)r.err[2], (unsigned)r.err[3]);
}

static void parseFrames() {
    uint16_t i = 0;
    while (i < g_parseLen) {
        uint8_t op = g_parse[i];
        if (op == 0xF9) { if (i + 2 > g_parseLen) break; i += 2; continue; }
        if (op == 0xF7) { if (i + 3 > g_parseLen) break; i += 3; continue; }
        if (op == 0x07) { if (i + 4 > g_parseLen) break; i += 4; continue; }
        if (op != 0x08 && op != 0x09) { i++; continue; }
        if (i + 5 > g_parseLen) break;
        if (g_parse[i + 2] != 0x19) { i++; continue; }
        uint16_t reg = ((uint16_t)g_parse[i + 3] << 8) | g_parse[i + 4];
        int cl = cborLen(g_parse + i + 5, g_parseLen - (i + 5));
        if (cl == CBOR_INCOMPLETE) break;          /* wait for the next notification */
        if (cl == CBOR_BAD) { i++; continue; }     /* resync */
        const uint8_t *pay = g_parse + i + 5;
        bool live = (g_parse[i + 1] == 0x03 && op == 0x08 && histDayOfReg(reg) < 0 &&
                     (reg < REG_HIST_MPPT0 || reg > REG_HIST_MPPT0 + HIST_MAX_DAYS));
        if (!live) g_lastReplyMs = millis();   /* live stream must not block pacing */
#if HIST_DEBUG_FRAMES
        if (g_mode == MODE_GATT && (HIST_DEBUG_FRAMES == 1 || !live)) {
            /* FRM op seq reg len +ms | first payload bytes. seq = byte after op (compare with TX seq) */
            char hx[3 * 12 + 1]; int k = 0;
            for (int b = 0; b < cl && b < 12; b++) k += snprintf(hx + k, sizeof(hx) - k, "%02X ", pay[b]);
            hx[k] = 0;
            logf("FRM %02X s=%02X reg=0x%04X cl=%d +%lums | %s", (unsigned)op, (unsigned)g_parse[i + 1],
                 (unsigned)reg, cl, (unsigned long)(millis() - g_gattStartMs), hx);
        }
#endif
        int hd = histDayOfReg(reg);
        if (g_job == JOB_HIST && hd >= 0 && hd <= g_histN && op == 0x08 &&
            g_dayState[hd] != DAY_PENDING) {
            /* duplicate / late reply for a day that is already closed (each request goes to
             * 0003 and 0004, retries add more) — do not re-parse */
            logf("DUP day%d reg=0x%04X ignored (state=%u)", hd, (unsigned)reg, (unsigned)g_dayState[hd]);
            hd = -1;
        }
        if (g_job == JOB_HIST && hd >= 0 && hd <= g_histN) {
            uint64_t blen = 0;
            int h = cborHead(pay, cl, &blen);
            if (hd != g_histDay && op == 0x08)
                logf("NOTE reply for day%d while waiting day%d (accepted by reg id)", hd, g_histDay);
            if (op == 0x09) {
                /* 0x09 = error/status reply. value looks like VE.Direct HEX flags (guess):
                 * 0x01 unknown id, 0x02 not supported, 0x04 parameter error / empty history record.
                 * 0x04 closes the day as empty; 01/02 make histTick move to the next request form now. */
                uint8_t fl = (pay[0] <= 0x17) ? pay[0] : 0xFF;
                logf("RX day%d err 09 s=%02X flag=0x%02X", hd, (unsigned)g_parse[i + 1], (unsigned)fl);
                if (g_dayState[hd] == DAY_PENDING) {
                    if (fl == 0x04) g_dayState[hd] = DAY_EMPTY;
                    else g_dayErr[hd] = fl ? fl : 0xFF;
                }
            } else if ((pay[0] >> 5) == 2 && h > 0) {
                parseDayPage(hd, pay + h, (int)blen);
            } else if (g_dayState[hd] == DAY_PENDING) {
                /* 0x09 is ACK, not a missing page. only 0x08 non-page can mark empty. */
                logf("NOTE day%d op=%02X non-page reply t=%02X", hd, (unsigned)op, (unsigned)pay[0]);
                g_dayState[hd] = DAY_EMPTY;
            }
        }
        if (g_job == JOB_PV && g_pvPhase < PV_NREGS && reg == PV_REGS[g_pvPhase] && op == 0x09 && g_pvTry) {
            uint8_t fl = (pay[0] <= 0x17) ? pay[0] : 0xFF;
            logf("RX pv reg=0x%04X err 09 s=%02X flag=0x%02X", (unsigned)reg, (unsigned)g_parse[i + 1], (unsigned)fl);
            if (!g_pvErr) g_pvErr = fl ? fl : 0xFF;
        }
        bool pvDup = false;
        if (g_job == JOB_PV && op == 0x08 &&
            ((reg == REG_PV_V && g_havePvV) || (reg == REG_PV_W && g_havePvW) || (reg == REG_PV_A && g_havePvA))) {
            /* rev6: duplicate / stale reply for a pv reg we already have — ignore, do not advance */
            if (!live) logf("DUP pv reg=0x%04X s=%02X ignored +%lums", (unsigned)reg, (unsigned)g_parse[i + 1],
                            (unsigned long)(millis() - g_gattStartMs));   /* live s=03 repeats: silent */
            pvDup = true;
        }
        if (op == 0x08 && !pvDup) {
            double num = NAN;
            if (cborNum(pay, cl, &num)) {
                if (reg == REG_PV_V) { g_pvV = (float)(num * 0.01); g_havePvV = true; }
                if (reg == REG_PV_W) { g_pvWatt = (float)(num * 0.01); g_havePvW = true; }
                if (reg == REG_PV_A) { g_pvA = (float)(num * 0.1); g_havePvA = true; }
                if (g_job == JOB_PV && (reg == REG_PV_V || reg == REG_PV_W || reg == REG_PV_A))
                    logf("RX pv reg=0x%04X s=%02X raw=%.0f +%lums", (unsigned)reg, (unsigned)g_parse[i + 1],
                         num, (unsigned long)(millis() - g_gattStartMs));   /* rev10: where the value came from */
                if (g_job == JOB_HIST) {
                    /* EDDx fallback only fills fields the day page did not deliver */
                    if (reg == REG_EDD1 && g_histN >= 1 && !g_yday.hasYield) {
                        g_yday.yieldKwh = (float)(num * 0.01);
                        g_yday.hasYield = g_yday.ok = true;
                        g_yday.ymd = dayYmd(1);
                        logf("RX yday EDD1 ymd=%d y=%.2f", g_yday.ymd, (double)g_yday.yieldKwh);
                    }
                    if (reg == REG_EDD0 && g_histN >= 1 && !g_yday.hasPmax) {
                        g_yday.pmaxW = (float)num;
                        g_yday.hasPmax = g_yday.ok = true;
                        g_yday.ymd = dayYmd(1);
                        logf("RX yday EDD0 ymd=%d pmax=%.0f", g_yday.ymd, (double)g_yday.pmaxW);
                    }
                    if (reg == REG_EDD3 && !g_today.hasYield) {
                        g_today.yieldKwh = (float)(num * 0.01);
                        g_today.hasYield = g_today.ok = true;
                        g_today.ymd = dayYmd(0);
                        logf("RX today EDD3 ymd=%d y=%.2f", g_today.ymd, (double)g_today.yieldKwh);
                    }
                    if (reg == REG_EDD2 && !g_today.hasPmax) {
                        g_today.pmaxW = (float)num;
                        g_today.hasPmax = g_today.ok = true;
                        g_today.ymd = dayYmd(0);
                        logf("RX today EDD2 ymd=%d pmax=%.0f", g_today.ymd, (double)g_today.pmaxW);
                    }
                }
            }
        }
        i += 5 + cl;
    }
    if (i) {
        uint16_t left = g_parseLen - i;
        if (left) memmove(g_parse, g_parse + i, left);
        g_parseLen = left;
    }
}
static void rxReset() {
    portENTER_CRITICAL(&g_rxMux);
    g_rxLen = 0;
    portEXIT_CRITICAL(&g_rxMux);
    g_parseLen = 0;
}
static void drainRx() {
    portENTER_CRITICAL(&g_rxMux);
    uint16_t n = g_rxLen;
    if (n) {
        uint16_t space = PARSE_CAP - g_parseLen;
        if (n > space) n = space;              /* rest stays in g_rx for the next pass */
        memcpy(g_parse + g_parseLen, g_rx, n);
        g_parseLen += n;
        if (n < g_rxLen) memmove(g_rx, g_rx + n, g_rxLen - n);
        g_rxLen -= n;
    }
    portEXIT_CRITICAL(&g_rxMux);
    if (n) g_lastRxMs = millis();
    if (g_parseLen) parseFrames();
    if (g_parseLen >= PARSE_CAP) {             /* full and nothing consumable → drop 1 byte, resync */
        memmove(g_parse, g_parse + 1, --g_parseLen);
        g_parseOvf++;
    }
}
static void onNotify(NimBLERemoteCharacteristic *ch, uint8_t *data, size_t len, bool) {
    if (!data || !len) return;
    g_sawNotify = true;
    int ci = (ch == g_chCtrl) ? 0 : (ch == g_chCmd) ? 1 : (ch == g_chBulk) ? 2 : -1;
    portENTER_CRITICAL(&g_rxMux);
    if (ci >= 0) g_rxBytesCh[ci] += len;
    if ((size_t)g_rxLen + len <= RX_CAP) {
        memcpy(g_rx + g_rxLen, data, len);
        g_rxLen += (uint16_t)len;
    } else {
        g_rxOvf++;                             /* keep what we have, drop this chunk */
    }
    portEXIT_CRITICAL(&g_rxMux);
}

class ClientCB : public NimBLEClientCallbacks {
    void onConnect(NimBLEClient *) override {
        logln("STEP onConnect");
    }
    /* host task: only flag. cleanup (deleteClient, scan restart, publish) happens in loop(). */
    void onDisconnect(NimBLEClient *c, int reason) override {
        g_dropReason = reason;
        g_dropCli = c;
        g_peerDrop = true;
    }
    void onPassKeyEntry(NimBLEConnInfo &info) override {
        logln("STEP passkey inject");
        NimBLEDevice::injectPassKey(info, g_pin);
    }
    void onConfirmPasskey(NimBLEConnInfo &info, uint32_t) override {
        NimBLEDevice::injectConfirmPasskey(info, true);
    }
    void onAuthenticationComplete(NimBLEConnInfo &info) override {
        g_enc = info.isEncrypted();
        logf("STEP auth enc=%d bonded=%d", (int)info.isEncrypted(), (int)info.isBonded());
    }
};
static ClientCB g_clientCb;

static bool wrHex(NimBLERemoteCharacteristic *ch, const char *hex) {
    if (!ch) return false;
    uint8_t buf[64]; int n = 0;
    for (const char *p = hex; *p && *(p + 1) && n < 64; p += 2) {
        char t[3] = { p[0], p[1], 0 };
        buf[n++] = (uint8_t)strtoul(t, nullptr, 16);
    }
    return ch->writeValue(buf, n, false);
}
/* Handshake GET that worked on this MPPT: 05 seq 82 19 REG [19 REG ...]
 * lone 05 seq 81 19 REG was ignored (no 08 and no 09). */
static bool wrBoth(const uint8_t *pkt, int n) {
    if (g_mode == MODE_GATT && g_job == JOB_HIST) {
        char hx[3 * 24 + 1]; int k = 0;
        for (int b = 0; b < n && b < 24; b++) k += snprintf(hx + k, sizeof(hx) - k, "%02X ", pkt[b]);
        hx[k] = 0;
        logf("TXHEX m=%u n=%d +%lums | %s", (unsigned)g_wrMask, n,
             (unsigned long)(millis() - g_gattStartMs), hx);
    }
    bool ok = false;
    if (g_chCmd && (g_wrMask & 1)) ok = g_chCmd->writeValue(pkt, n, false);
    if (g_chBulk && (g_wrMask & 2)) {
        bool ok2 = g_chBulk->writeValue(pkt, n, false);
        if (!(g_wrMask & 1)) ok = ok2;
    }
    return ok;
}
/* 05 seq 8n 19 REG [19 REG ...]
 * The byte after seq looks like a CBOR array head (0x81 = 1 item, 0x82 = 2 items; same in the
 * captured init strings), so it is derived from n here. The original code sent 0x82 with 4
 * regs → device would only see the first 2. seq is kept 1..23 so it stays a 1-byte CBOR uint
 * (0x18+ would be read as "uint8 follows"). Refuse instead of silently truncating. */
static bool getRegs(const uint16_t *regs, int n, uint8_t flagHint) {
    uint8_t pkt[3 + 3 * MAX_GET_REGS];
    if (n <= 0 || n > MAX_GET_REGS) { logf("FAIL getRegs n=%d (max %d)", n, MAX_GET_REGS); return false; }
    uint8_t flag = (uint8_t)(0x80 | n);       /* CBOR array(n), n <= 23 */
    if (flagHint && flagHint != flag) logf("NOTE getRegs flag 0x%02X -> 0x%02X (n=%d)", flagHint, flag, n);
    int i = 0;
    pkt[i++] = 0x05;
    if (g_seqFixed >= 0) {
        pkt[i++] = (uint8_t)g_seqFixed;     /* rev10: pv uses PV_SEQ, running counter untouched */
        pkt[i++] = flag;
    } else {
    pkt[i++] = g_seq;
    pkt[i++] = flag;
    do { g_seq = (uint8_t)(g_seq % 0x17 + 1); } while (g_seq == 0x03);  /* 1..23, never 03 (live stream id) */
    }
    for (int k = 0; k < n; k++) {
        pkt[i++] = 0x19;
        pkt[i++] = (uint8_t)(regs[k] >> 8);
        pkt[i++] = (uint8_t)regs[k];
    }
    bool ok = wrBoth(pkt, i);
    g_lastTxSeq = pkt[1];
    g_lastTxMs = millis();
    logf("TX get s=%02X flag=0x%02X n=%d first=0x%04X ok=%d +%lums",
         (unsigned)pkt[1], (unsigned)flag, n, regs[0], (int)ok,
         (unsigned long)(g_mode == MODE_GATT ? millis() - g_gattStartMs : 0));
    return ok;
}
/* Mac working hist: concat 05 03 81 19 HI LO per reg, write 0003+0004 */
static bool getRegs81Concat(const uint16_t *regs, int n) {
    uint8_t pkt[6 * MAX_GET_REGS];
    if (n <= 0 || n > MAX_GET_REGS) { logf("FAIL hist81 n=%d (max %d)", n, MAX_GET_REGS); return false; }
    int i = 0;
    for (int k = 0; k < n; k++) {
        pkt[i++] = 0x05;
        pkt[i++] = 0x03;
        pkt[i++] = 0x81;
        pkt[i++] = 0x19;
        pkt[i++] = (uint8_t)(regs[k] >> 8);
        pkt[i++] = (uint8_t)regs[k];
    }
    bool ok = wrBoth(pkt, i);
    g_lastTxMs = millis();
    logf("TX hist81-concat s=03 n=%d first=0x%04X bytes=%d ok=%d +%lums", n, regs[0], i, (int)ok,
         (unsigned long)(g_mode == MODE_GATT ? millis() - g_gattStartMs : 0));
    return ok;
}
static bool getReg(uint16_t reg) {
    uint16_t one = reg;
    return getRegs(&one, 1, 0);   /* 05 seq 81 19 REG, seq 1..23 — lone seq=0 81 was ignored */
}

/* rev10: pv get with the fixed PV_SEQ */
static bool pvGetReg(uint16_t reg) {
    g_seqFixed = PV_SEQ;
    bool ok = getReg(reg);
    g_seqFixed = -1;
    return ok;
}

static void gattDrop() {
    if (g_cli) {
        /* connected: deleteClient disconnects and deletes on the disconnect event.
         * already dropped by peer: deleted right here (this is what fixes the client leak). */
        NimBLEDevice::deleteClient(g_cli);
        g_cli = nullptr;
    }
    g_chCtrl = g_chCmd = g_chBulk = nullptr;
    g_mode = MODE_ADV;
    g_job = JOB_NONE;
    accReset(g_acc[0]); accReset(g_acc[1]);
    bleScanStart();
    logln("STEP back_to_adv");
}
static void cleanupStaleClients() {
    for (int k = 0; k < 4; k++) {
        NimBLEClient *old = NimBLEDevice::getDisconnectedClient();
        if (!old) break;
        logln("STEP delete stale client");
        if (!NimBLEDevice::deleteClient(old)) break;
    }
}

static void pubDay(const DayRec &r, int day, const char *src) {
    if (!r.ok || !mqtt.connected()) return;
    const char *kind = (day == 0) ? "today" : (day == 1) ? "yesterday" : "day";
    char seq[8];
    if (r.hasSeq) snprintf(seq, sizeof(seq), "%u", (unsigned)r.seq);
    else snprintf(seq, sizeof(seq), "null");
    /* rev8: charge-stage minutes + error 0..3 from the page (null for EDDx-only / unknown 0xFFFF) */
    char tb[8], ta[8], tf[8], er[24];
    const uint16_t tv[3] = { r.tBulk, r.tAbs, r.tFloat };
    char *to[3] = { tb, ta, tf };
    for (int t = 0; t < 3; t++) {
        if (r.hasPage && tv[t] != 0xFFFF) snprintf(to[t], 8, "%u", (unsigned)tv[t]);
        else snprintf(to[t], 8, "null");
    }
    if (r.hasPage) snprintf(er, sizeof(er), "[%u,%u,%u,%u]",
                            (unsigned)r.err[0], (unsigned)r.err[1], (unsigned)r.err[2], (unsigned)r.err[3]);
    else snprintf(er, sizeof(er), "null");
    char ib[12];                          /* rev9: ibat_max A, 1 decimal */
    if (r.hasPage && r.iBatMax != 0xFFFF) snprintf(ib, sizeof(ib), "%.1f", r.iBatMax / 10.0);
    else snprintf(ib, sizeof(ib), "null");
    char js[480];
    int n = snprintf(js, sizeof(js),
             "{\"id\":\"mppt\",\"sn\":\"" MPPT_SN "\",\"src\":\"%s\","
             "\"kind\":\"%s\",\"day\":%d,\"ymd\":%d,\"seq\":%s,\"label\":\"%s\","
             "\"yield_kwh\":%.2f,\"consumed_kwh\":%.2f,\"pmax_w\":%.0f,\"vpv_max\":%.2f,"
             "\"vbat_max\":%.2f,\"vbat_min\":%.2f,\"ibat_max\":%s,"
             "\"bulk_min\":%s,\"abs_min\":%s,\"float_min\":%s,\"err\":%s}",
             src, kind, day, r.ymd, seq, g_lblSeq ? "seq" : "clock",
             (double)r.yieldKwh, (double)r.consumedKwh, (double)r.pmaxW, (double)r.vpvMax,
             (double)r.vbatMax, (double)r.vbatMin, ib, tb, ta, tf, er);
    if (n <= 0 || n >= (int)sizeof(js)) { logf("FAIL hist json day=%d n=%d", day, n); return; }
    bool ok = mqtt.publish(TOPIC_HIST, js, false);
    logf("PUB hist %s day=%d ymd=%d y=%.2f pmax=%.0f ok=%d",
         kind, day, r.ymd, (double)r.yieldKwh, (double)r.pmaxW, (int)ok);
}
static void pubHistAll() {
    for (int d = 0; d <= g_histN && d <= HIST_MAX_DAYS; d++) {
        if (!g_day[d].ok || g_dayPub[d]) continue;
        if (g_dayState[d] == DAY_PENDING) continue;   /* EDDx partial for a day still being read */
        pubDay(g_day[d], d, g_dayState[d] == DAY_CACHED ? "nvs" : "gatt");
        g_dayPub[d] = true;
        mqtt.loop();
    }
}
static void pubPv() {
    char vs[16], ws[16], as[16], js[220];
    jsonNum(vs, sizeof(vs), g_havePvV, g_pvV, 2);
    jsonNum(ws, sizeof(ws), g_havePvW, g_pvWatt, 2);
    /* rev7: no EDBD reply → ipv = ppv / vpv (vpv > 1 V), 0 if ppv == 0 at low/invalid vpv, else null */
    bool haveA = g_havePvA && !isnan(g_pvA);
    float ipv = g_pvA;
    bool calc = false;
    bool vOk = g_havePvV && !isnan(g_pvV), wOk = g_havePvW && !isnan(g_pvWatt);
    if (!haveA && wOk) {
        if (vOk && g_pvV > 1.0f) { ipv = roundf(g_pvWatt / g_pvV * 100.0f) / 100.0f; haveA = calc = true; }
        else if (g_pvWatt == 0.0f) { ipv = 0.0f; haveA = calc = true; }
    }
    jsonNum(as, sizeof(as), haveA, ipv, 2);
    snprintf(js, sizeof(js),
             "{\"id\":\"mppt\",\"sn\":\"" MPPT_SN "\",\"src\":\"gatt\","
             "\"vpv\":%s,\"ppv\":%s,\"ipv\":%s%s}",
             vs, ws, as, calc ? ",\"ipv_calc\":true" : "");
    if (mqtt.connected()) mqtt.publish(TOPIC_PV, js, false);
    logf("PUB pv %s", js);
}

static void histSeqCheck() {
    if (!g_today.ok || !g_today.hasSeq) return;
    for (int d = 1; d <= g_histN; d++) {
        const DayRec &r = g_day[d];
        if (!r.ok || !r.hasSeq || g_dayState[d] == DAY_CACHED) continue;
        int want = ((int)g_today.seq - d) % 365;
        if (want < 0) want += 365;
        if (r.seq != want)
            logf("WARN day%d seq=%u expected %d (device day boundary != calendar?)", d, (unsigned)r.seq, want);
    }
}
static int histPendingCount() {
    int n = 0;
    for (int d = 0; d <= g_histN; d++) if (g_dayState[d] == DAY_PENDING) n++;
    return n;
}
/* end of one GATT session. resume=true: disconnect and reconnect later for the remaining days */
static void histSessionEnd(bool resume) {
    int pend = histPendingCount();
    if (resume && pend > 0 && g_histSessions < HIST_MAX_SESSIONS) {
        logf("STEP hist session %u end: closed=%u pending=%d ms=%lu rx02=%lu rx03=%lu rx04=%lu — resume",
             (unsigned)g_histSessions, (unsigned)g_histSessDays, pend,
             (unsigned long)(millis() - g_gattStartMs),
             (unsigned long)g_rxBytesCh[0], (unsigned long)g_rxBytesCh[1], (unsigned long)g_rxBytesCh[2]);
        if (g_histSessDays) g_connFails = 0;
        gattDrop();
        pubHistAll();                       /* publish what we have so far */
        g_histCont = true;
        g_wantHist = true;                  /* maybeGatt reconnects after HIST_RECONNECT_MS */
        return;
    }
    /* job end: anything still pending is a failure */
    for (int d = 0; d <= g_histN; d++) if (g_dayState[d] == DAY_PENDING) g_dayState[d] = DAY_FAIL;
    g_histCont = false;
    g_wantHist = false;
    int ok = 0, empty = 0, fail = 0, cached = 0;
    for (int d = 0; d <= g_histN; d++) {
        if (g_dayState[d] == DAY_OK) ok++;
        else if (g_dayState[d] == DAY_EMPTY) empty++;
        else if (g_dayState[d] == DAY_FAIL) fail++;
        else if (g_dayState[d] == DAY_CACHED) cached++;
    }
    logf("STEP hist done N=%d ok=%d cached=%d empty=%d fail=%d sessions=%u ms=%lu rx02=%lu rx03=%lu rx04=%lu",
         g_histN, ok, cached, empty, fail, (unsigned)g_histSessions,
         (unsigned long)(millis() - g_gattStartMs),
         (unsigned long)g_rxBytesCh[0], (unsigned long)g_rxBytesCh[1], (unsigned long)g_rxBytesCh[2]);
    histSeqCheck();
    if (g_mode == MODE_GATT) gattDrop();
    if (ok || g_today.ok || g_yday.ok) g_connFails = 0;
    pubHistAll();
    if (!ok && !cached && !g_today.ok && !g_yday.ok) logln("NOTE hist empty");
}
static void histFinish() { histSessionEnd(false); }

static bool gattFail(NimBLEClient *c, const char *why) {
    logln(why);
    if (c) NimBLEDevice::deleteClient(c);   /* disconnects first if needed */
    if (g_connFails < 255) g_connFails++;
    bleScanStart();
    return false;
}

static bool gattBurst(int job) {
    DevState &m = g_st[DEV_MPPT];
    if (!m.seen || (millis() - m.lastMs) > 15000 || !m.addr[0]) {
        logln("FAIL no_mppt_adv — 먼저 ADV 가 보여야 함");
        return false;
    }
    logf("STEP gatt_connect %s type=%u rssi=%d age=%lu job=%d",
         m.addr, (unsigned)m.addrType, m.rssi,
         (unsigned long)(millis() - m.lastMs), job);
    cleanupStaleClients();
    logf("STEP clients=%d heap=%lu maxblk=%lu",
         (int)NimBLEDevice::getCreatedClientCount(),
         (unsigned long)ESP.getFreeHeap(), (unsigned long)ESP.getMaxAllocHeap());
    NimBLEClient *c = NimBLEDevice::createClient();
    if (!c) {
        logln("FAIL createClient — host reset");
        g_connFails = CONN_FAIL_RESET;      /* force recovery in maybeGatt */
        return false;
    }
    c->setClientCallbacks(&g_clientCb, false);
    c->setConnectTimeout(8000);
    /* address/type come from our own ADV decode — no scan-results lookup
     * (results are no longer stored, and reading them while scanning races the host task) */
    bleScanStop();
    delay(80);
    NimBLEAddress use(std::string(m.addr), m.addrType);
    logf("STEP peer %s type=%u", use.toString().c_str(), (unsigned)use.getType());
    g_peerDrop = false;
    uint32_t tConn = millis();
    bool linked = c->connect(use, true, false, true);
    uint32_t cms = millis() - tConn;
    logf("STEP connect()=%d ms=%lu", (int)linked, (unsigned long)cms);
    if (!linked) return gattFail(c, "FAIL connect");
    g_enc = false;
    c->secureConnection(true);
    uint32_t t0 = millis();
    while (!g_enc && millis() - t0 < 5000 && c->isConnected()) { delay(40); mqtt.loop(); }
    logf("STEP enc=%d", (int)g_enc);
    if (!c->isConnected()) return gattFail(c, "FAIL dropped during auth");
    if (!c->discoverAttributes()) return gattFail(c, "FAIL discover");
    NimBLERemoteService *svc = c->getService(SVC_APP);
    if (!svc) return gattFail(c, "FAIL no_306b");
    g_chCtrl = svc->getCharacteristic(CHR_2);
    g_chCmd  = svc->getCharacteristic(CHR_3);
    g_chBulk = svc->getCharacteristic(CHR_4);
    if (job == JOB_PV) {                  /* rev10: clear before subscribe — "already received" = this connection only */
        g_havePvV = g_havePvW = g_havePvA = false;
        g_pvV = g_pvWatt = g_pvA = NAN;
    }
    rxReset();
    if (g_chCtrl && g_chCtrl->canNotify()) g_chCtrl->subscribe(true, onNotify);
    if (g_chCmd && g_chCmd->canNotify()) g_chCmd->subscribe(true, onNotify);
    if (g_chBulk && g_chBulk->canNotify()) g_chBulk->subscribe(true, onNotify);
    g_cli = c;
    g_mode = MODE_GATT;
    g_job = job;
    g_gattStartMs = millis();
    g_sawNotify = false;
    g_pvPhase = 0;
    g_pvFbSent = false;
    g_pvTry = 0;
    g_pvErr = 0;
    g_pvEdbcWait = g_pvEdbcDone = false;
    g_wrMask = 3;
    if (job == JOB_HIST) {
        portENTER_CRITICAL(&g_rxMux);
        memset(g_rxBytesCh, 0, sizeof(g_rxBytesCh));
        portEXIT_CRITICAL(&g_rxMux);
        g_lastRxMs = g_lastTxMs = g_lastReplyMs = millis();
        int pending = 0;
        if (!g_histCont) {
            /* new hist job: per-day slots, ymd from job start. closed days in NVS are not re-read. */
            g_histBase = time(nullptr);
            g_base0Ymd = predictBase0(&g_lblSeq);   /* sleep2 */
            g_prevSeq = rtc.d0Seq;
            g_prevYmd = rtc.d0Ymd;
            g_day0Labeled = false;
            memset(g_day, 0, sizeof(g_day));
            memset(g_dayState, 0, sizeof(g_dayState));
            memset(g_dayErr, 0, sizeof(g_dayErr));
            memset(g_dayTries, 0, sizeof(g_dayTries));
            memset(g_dayPub, 0, sizeof(g_dayPub));
            for (int d = g_histN + 1; d <= HIST_MAX_DAYS; d++) g_dayState[d] = DAY_SKIP;
            for (int d = 0; d <= g_histN; d++)
                if (d >= 1 && cacheLoad(dayYmd(d), g_day[d])) g_dayState[d] = DAY_CACHED;
            g_histSessions = 0;
            g_histEmptyRun = 0;
            g_histFbSent = false;
        }
        for (int d = 0; d <= g_histN; d++) if (g_dayState[d] == DAY_PENDING) pending++;
        g_histSessions++;
        g_histSessDays = 0;
        g_histSessStop = false;
        g_histDay = -1;
        g_histTry = 0;
        g_histNeedSend = false;
        g_histBudgetMs = HIST_SESSION_END_MS;
        logf("STEP hist %s day0..%d pending=%d session=%u/%u",
             g_histCont ? "resume" : "plan", g_histN, pending,
             (unsigned)g_histSessions, (unsigned)HIST_MAX_SESSIONS);
    }
    wrHex(g_chCtrl, "fa80ff"); delay(100);
    wrHex(g_chCtrl, "f980");   delay(100);
    wrHex(g_chCmd,  "01");     delay(80);
    wrHex(g_chCmd,  "0300");   delay(100);
    wrHex(g_chCmd,  "060082189342102705008219ec6619ec6503010303");
    delay(250);
    /* Mac vereg extra init — without these, 1050/1051 stayed silent on Feather */
    wrHex(g_chCmd,  "05008119ec7d050081189005008119ec3f05008119ec12");
    delay(250);
#if INIT_BULK_TRIM
    /* the captured 0004 write was a 20-byte chunk ending in an incomplete "05 00" GET; that
     * dangling prefix glued onto our next 0004 write (replies seen one request late) */
    wrHex(g_chBulk, "05008119ec0f05008119ec0e05008119010c");
#else
    wrHex(g_chBulk, "05008119ec0f05008119ec0e05008119010c0500");
#endif
    delay(200);
    wrHex(g_chCtrl, "f941");
    /* hist: first request right away (device drops the link ~10 s after connect) */
    g_histNextMs = millis() + 150;   /* rev6: pv starts at the same point as hist (was +2000 for pv) */
    if (job == JOB_HIST) g_wrMask = HIST_WRITE_CHARS;
    if (job == JOB_PV) g_wrMask = PV_WRITE_CHARS;
    logln(job == JOB_HIST ? "OK gatt hist" : "OK gatt pv_burst");
    if (job == JOB_PV) {
        g_havePvV = g_havePvW = g_havePvA = false;
        g_pvV = g_pvWatt = g_pvA = NAN;
    }
    return true;
}

static int histNextPending(int from) {
    for (int d = from; d <= g_histN; d++)
        if (g_dayState[d] == DAY_PENDING) return d;
    return -1;
}
/* one request for day d. rev4 log: ONLY "05 03 81 19 HI LO" (channel/seq byte 03, the Mac capture
 * form) returned history pages. 05 s 82 / 05 s 81 with s != 03 got 09 flag 01 or no reply.
 * The register encoding (big-endian after 0x19) is correct — same as the init capture "19 ec 66"
 * and the reply decoder. Attempts differ only in the target characteristic:
 *   try0: HIST_WRITE_CHARS (default 0003)  try1: 0004  try2: both */
static void histSendDay(int d) {
    uint16_t regs[2];
    int n = 0;
    regs[n++] = (uint16_t)(REG_HIST_DAY0 + d);
#if HIST_ASK_MPPT_X
    regs[n++] = (uint16_t)(REG_HIST_MPPT0 + d);
#endif
    uint8_t t = g_dayTries[d] % 3;
    g_wrMask = (t == 0) ? (uint8_t)HIST_WRITE_CHARS : (t == 1) ? 2 : 3;
    g_dayErr[d] = 0;
    getRegs81Concat(regs, n);
    wrHex(g_chCtrl, "f941");
    if (g_dayTries[d] < 255) g_dayTries[d]++;
    g_histDeadline = millis() + HIST_DAY_TIMEOUT_MS;
}
/* after the day loop: EDD1/EDD0/EDD3/EDD2 once if day0/day1 page is missing */
static void histDone() {
    if (!g_histFbSent && (!g_today.ok || (g_histN >= 1 && !g_yday.ok))) {
        static const uint16_t fb[] = { REG_EDD1, REG_EDD0, REG_EDD3, REG_EDD2 };
        g_wrMask = HIST_WRITE_CHARS;
        getRegs81Concat(fb, 4);                 /* 05 03 81 per reg — the only form answered */
        wrHex(g_chCtrl, "f941");
        g_histFbSent = true;
        g_histFbUntil = millis() + HIST_FB_WAIT_MS;
        logln("TX hist EDDx fallback");
        return;
    }
    histFinish();
}

static void histTick() {
    if (g_mode != MODE_GATT || g_job != JOB_HIST) return;
    uint32_t age = millis() - g_gattStartMs;
    if (age > HIST_SESSION_END_MS) {        /* clean disconnect before the device's ~10 s drop */
        histSessionEnd(true);
        return;
    }
    if (g_histFbSent) {
        if ((int32_t)(millis() - g_histFbUntil) >= 0) histFinish();
        return;
    }
    if ((int32_t)(millis() - g_histNextMs) < 0) return;
    if (!g_sawNotify && age < 2000) {
        wrHex(g_chCtrl, "f941");
        g_histNextMs = millis() + 300;
        return;
    }
    if (HIST_KEEPALIVE_MS && millis() - g_lastTxMs > HIST_KEEPALIVE_MS) {
        wrHex(g_chCtrl, "f941");
        g_lastTxMs = millis();
    }
    if (g_histSessStop) return;              /* waiting for late replies until SESSION_END */
    if (age > HIST_SESSION_MS) {
        g_histSessStop = true;
        /* rev5: no flush GET — init trim removed the one-write lag, and s!=03 GETs are ignored */
        logf("STEP hist session time up at +%lums — draining", (unsigned long)age);
        return;
    }
    if (g_histDay < 0) {                     /* first day of this session */
        g_histDay = histNextPending(0);
        if (g_histDay < 0) { histDone(); return; }
        histSendDay(g_histDay);
        return;
    }
    if (g_histNeedSend) {
        if (millis() - g_lastReplyMs < HIST_QUIET_MS) return;   /* replies still arriving */
        g_histNeedSend = false;
        histSendDay(g_histDay);
        return;
    }
    int d = g_histDay;
    if (g_dayState[d] == DAY_PENDING) {
        bool err = g_dayErr[d] != 0;
        if (!err && (int32_t)(millis() - g_histDeadline) < 0) return;   /* still waiting */
        if (!err && millis() - g_lastReplyMs < HIST_QUIET_MS) return;
        if (g_dayTries[d] <= HIST_DAY_RETRIES) {
            logf("RETRY day%d try=%u%s", d, (unsigned)g_dayTries[d], err ? " (after 09)" : "");
            histSendDay(d);
            return;
        }
        if (err) {
            logf("NOTE day%d only 09 flag=0x%02X after %u tries — empty", d,
                 (unsigned)g_dayErr[d], (unsigned)g_dayTries[d]);
            g_dayState[d] = DAY_EMPTY;
        } else {
            logf("FAIL day%d no reply after %u tries", d, (unsigned)g_dayTries[d]);
            g_dayState[d] = DAY_FAIL;
        }
    }
    /* day d finished (ok / empty / fail) */
    g_histSessDays++;
    if (g_dayState[d] == DAY_OK) g_histEmptyRun = 0;
    else if (++g_histEmptyRun >= HIST_EMPTY_STOP && d >= 1) {
        logf("NOTE %u empty/failed days in a row at day%d — stop", (unsigned)g_histEmptyRun, d);
        for (int k = d + 1; k <= g_histN; k++) if (g_dayState[k] == DAY_PENDING) g_dayState[k] = DAY_SKIP;
        histDone();
        return;
    }
    int nx = histNextPending(d + 1);
    if (nx < 0) { histDone(); return; }
    if (g_histSessDays >= HIST_DAYS_PER_SESSION) {   /* K days done: reconnect for the rest */
        histSessionEnd(true);
        return;
    }
    g_histDay = nx;
    g_histNeedSend = true;
    g_histNextMs = millis() + HIST_GAP_MS;
}

static bool pvHave(int k) {
    return k == 0 ? g_havePvV : k == 1 ? g_havePvW : g_havePvA;
}
static void pvNext() {                    /* current pv reg closed (ok / 09 / failed) */
    g_pvPhase++;
    g_pvTry = 0;
    g_pvErr = 0;
    /* rev7: after the last reg go straight on (rev11: EDBC push wait, then publish + disconnect) */
    g_histNextMs = millis() + (g_pvPhase < PV_NREGS ? PV_GAP_MS : 0);
}
/* rev6: sequential like hist — one getRegs 81 n=1 at a time, the next reg only after the
 * 08 reply for the current reg (matched by reg id, not seq), a 09 for it, or a timeout.
 * timeout → one retry. stale / duplicate frames are dropped in parseFrames. */
static void pvTick() {
    if (g_mode != MODE_GATT || g_job != JOB_PV) return;
    if ((int32_t)(millis() - g_histNextMs) < 0) return;
    uint32_t age = millis() - g_gattStartMs;
    if (g_pvPhase < PV_NREGS) {
        int k = g_pvPhase;
        uint16_t reg = PV_REGS[k];
        if (pvHave(k)) {                  /* 08 for this reg arrived (possibly before we asked) */
            if (g_pvTry) logf("RX pv reg=0x%04X ok try=%u +%lums", (unsigned)reg, (unsigned)g_pvTry, (unsigned long)age);
            else logf("STEP pv reg=0x%04X already received — skip", (unsigned)reg);
            pvNext();
            return;
        }
        if (g_pvTry && g_pvErr) {
            logf("NOTE pv reg=0x%04X rejected 09 flag=0x%02X — next", (unsigned)reg, (unsigned)g_pvErr);
            pvNext();
            return;
        }
        if (g_pvTry && (int32_t)(millis() - g_pvDeadline) < 0) return;   /* still waiting */
        if (g_pvTry > PV_RETRIES) {
            logf("FAIL pv reg=0x%04X no reply after %u tries", (unsigned)reg, (unsigned)g_pvTry);
            pvNext();
            return;
        }
        if (age + PV_REPLY_TIMEOUT_MS > PV_SESSION_MS) {
            logf("STEP pv time up at +%lums — reg=0x%04X not sent", (unsigned long)age, (unsigned)reg);
            g_pvPhase = PV_NREGS;
            g_pvTry = 0;
            return;
        }
        if (g_pvTry) logf("RETRY pv reg=0x%04X try=%u", (unsigned)reg, (unsigned)g_pvTry);
        g_pvErr = 0;
        pvGetReg(reg);
        logf("TX pv %04X try=%u s=%02X", (unsigned)reg, (unsigned)g_pvTry, (unsigned)PV_SEQ);
        g_pvTry++;
        g_pvDeadline = millis() + PV_REPLY_TIMEOUT_MS;
        return;
    }
    /* rev11: EDBB handled — EDBC comes only as an unsolicited push. publish as soon as it is in,
     * or after PV_EDBC_WAIT_MS from here (never past PV_SESSION_MS from connect) */
    if (!g_pvEdbcDone) {
        if (g_havePvW) {
            if (g_pvEdbcWait) logf("RX pv EDBC push after %lums wait +%lums",
                                   (unsigned long)(millis() - g_pvEdbcStart), (unsigned long)age);
            else logf("STEP pv EDBC already pushed — no wait +%lums", (unsigned long)age);
            g_pvEdbcDone = true;
        } else if (!g_pvEdbcWait) {
            g_pvEdbcWait = true;
            g_pvEdbcStart = millis();
            uint32_t w = PV_EDBC_WAIT_MS;
            if (age + w > PV_SESSION_MS) w = (age < PV_SESSION_MS) ? PV_SESSION_MS - age : 0;
            g_pvEdbcUntil = millis() + w;
            logf("STEP pv wait EDBC push up to %lums +%lums", (unsigned long)w, (unsigned long)age);
            return;
        } else if ((int32_t)(millis() - g_pvEdbcUntil) < 0) {
            return;                       /* still waiting (polled every loop) */
        } else {
            logf("NOTE pv EDBC no push within %lums", (unsigned long)(millis() - g_pvEdbcStart));
            g_pvEdbcDone = true;
        }
    }
    /* nothing came back for 05 seq(1..23) 81: one fallback with the original seq=0 form
     * (that is what pv used before, and the init strings use seq 00 too) */
    if (!g_havePvV && !g_havePvW && !g_havePvA && !g_pvFbSent && age + PV_FB_WAIT_MS <= PV_SESSION_MS) {
        for (int k = 0; k < PV_NREGS; k++) {
            const uint16_t *pv = PV_REGS;
            uint8_t pkt[6] = { 0x05, 0x00, 0x81, 0x19, (uint8_t)(pv[k] >> 8), (uint8_t)pv[k] };
            wrBoth(pkt, 6);
            delay(30);
        }
        logln("TX pv fallback seq=00");
        g_pvFbSent = true;
        g_histNextMs = millis() + PV_FB_WAIT_MS;
        return;
    }
    g_pvPhase = 0;
    g_wantPv = false;
    pubPv();
    g_pvPublished = true;                 /* sleep1: 10-min pv job done */
    logf("STEP pv_done disconnect +%lums", (unsigned long)age);
    gattDrop();
}

static void handlePeerDrop() {
    if (!g_peerDrop) return;
    g_peerDrop = false;
    NimBLEClient *c = g_dropCli;
    logf("STEP drop 0x%03x", g_dropReason);   /* 0x208 sup. timeout, 0x213 remote, 0x216 local, 0x23e est. fail */
    if (g_mode != MODE_GATT || c != g_cli) return;   /* our own gattDrop / failure path */
    logf("STEP peer dropped session -> ADV age=%lums lastTx s=%02X +%lums lastRx +%lums rx02=%lu rx03=%lu rx04=%lu",
         (unsigned long)(millis() - g_gattStartMs), (unsigned)g_lastTxSeq,
         (unsigned long)(g_lastTxMs - g_gattStartMs), (unsigned long)(g_lastRxMs - g_gattStartMs),
         (unsigned long)g_rxBytesCh[0], (unsigned long)g_rxBytesCh[1], (unsigned long)g_rxBytesCh[2]);
    bool wasHist = (g_job == JOB_HIST);
    if (g_connFails < 255) g_connFails++;
    if (wasHist) {
        histSessionEnd(true);                 /* gattDrop + resume remaining days in a new session */
        return;
    }
    gattDrop();                               /* deletes the dead client (was leaked before) */
}

/* ======================= sleep1: wake cycle ======================= */

/* sleep length to the next WAKE_PERIOD_S boundary of the RTC clock (no drift from awake time) */
static uint64_t sleepUsToBoundary(time_t *nextOut) {
    struct timeval tv; gettimeofday(&tv, nullptr);
    if (clockReady()) {
        int64_t nowUs = (int64_t)tv.tv_sec * 1000000LL + tv.tv_usec;
        int64_t next = ((int64_t)tv.tv_sec / WAKE_PERIOD_S + 1) * WAKE_PERIOD_S;
        int64_t us = next * 1000000LL - nowUs - (int64_t)BOOT_COMP_MS * 1000LL;
        while (us < (int64_t)MIN_SLEEP_MS * 1000LL) { us += (int64_t)WAKE_PERIOD_S * 1000000LL; next += WAKE_PERIOD_S; }
        if (nextOut) *nextOut = (time_t)next;
        return (uint64_t)us;
    }
    /* no clock yet: one period from boot */
    int64_t us = (int64_t)WAKE_PERIOD_S * 1000000LL - (int64_t)millis() * 1000LL;
    if (us < (int64_t)MIN_SLEEP_MS * 1000LL) us = (int64_t)MIN_SLEEP_MS * 1000LL;
    if (nextOut) *nextOut = 0;
    return (uint64_t)us;
}

/* last resort: runs in the esp_timer task even if loop code is stuck in a blocking call */
static void awakeWdtCb(void *) {
    rtc.wdtTrips++;
    rtc.prevAwakeMs = millis();
    uint64_t us = sleepUsToBoundary(nullptr);
    rtc.lastSleepUs = us;                         /* sleep3: drift compensation on the next wake */
    Serial.printf("WDT awake limit hit at %lums — forced deep sleep %llums\n",
                  (unsigned long)millis(), (unsigned long long)(us / 1000));
    Serial.flush();
    esp_sleep_enable_timer_wakeup(us);
    esp_deep_sleep_start();
}
static void awakeWdtArm(uint32_t limitMs) {
    if (!g_awakeWdt) {
        esp_timer_create_args_t a = {};
        a.callback = awakeWdtCb;
        a.name = "awake_wdt";
        if (esp_timer_create(&a, &g_awakeWdt) != ESP_OK) { g_awakeWdt = nullptr; return; }
    } else {
        esp_timer_stop(g_awakeWdt);
    }
    uint32_t up = millis();
    uint32_t left = limitMs > up + 1000 ? limitMs - up : 1000;   /* limit counts from boot */
    esp_timer_start_once(g_awakeWdt, (uint64_t)left * 1000ULL);
}
static uint32_t g_awakeLimitMs = AWAKE_MAX_MIN_MS;

/* one service pass for the (unchanged) GATT state machines — what loop() used to do */
static void gattService() {
    mqttKeep();
    drainRx();
    handlePeerDrop();
    if (g_histPubPending) {
        g_histPubPending = false;
        pubHistAll();
    }
    if (g_mode == MODE_GATT) {
        uint32_t lim = (g_job == JOB_HIST) ? g_histBudgetMs + 3000UL : 28000UL;
        if (millis() - g_gattStartMs > lim) {
            logln("FAIL gatt timeout");
            if (g_connFails < 255) g_connFails++;
            if (g_job == JOB_HIST) histSessionEnd(true);
            else gattDrop();
        }
    }
    histTick();
    pvTick();
}
static void serviceFor(uint32_t ms) {
    uint32_t t0 = millis();
    while (millis() - t0 < ms) { gattService(); delay(5); }
}

/* run one GATT job (hist incl. resume sessions, or pv) until done or untilMs */
static bool runGatt(int job, uint32_t untilMs, int maxAttempts) {
    int attempts = 0;
    bool wasGatt = false;
    uint32_t nextTry = millis(), rescanMs = 0;
    while ((int32_t)(untilMs - millis()) > 0) {
        gattService();
        bool want = (job == JOB_HIST) ? g_wantHist : g_wantPv;
        if (g_mode == MODE_GATT) { wasGatt = true; delay(5); continue; }
        if (wasGatt) {                                     /* session just ended: gap before a resume */
            wasGatt = false;
            nextTry = millis() + HIST_RECONNECT_MS;
        }
        {
            if (!want) return true;
            /* gattBurst needs an MPPT ADV < 15 s old; the ADV scan was stopped after collection */
            DevState &m = g_st[DEV_MPPT];
            if (!m.seen || millis() - m.lastMs > 10000) {
                if (!rescanMs) rescanMs = millis();
                if (millis() - rescanMs > 6000) { logln("FAIL no fresh mppt adv for GATT"); break; }
                if (!NimBLEDevice::getScan()->isScanning()) { logln("STEP rescan for a fresh mppt adv"); bleScanStart(); }
                delay(20);
                continue;
            }
            rescanMs = 0;
            if (g_connFails >= CONN_FAIL_RESET) {          /* same recovery as the old maybeGatt */
                logf("STEP %u consecutive GATT failures", (unsigned)g_connFails);
                bleStackReset();
                bleScanStart();
                nextTry = millis() + 1500;                 /* let a fresh ADV arrive */
            }
            if (attempts >= maxAttempts) break;
            if ((int32_t)(millis() - nextTry) >= 0) {
                attempts++;
                logf("STEP gatt job=%d attempt %d/%d", job, attempts, maxAttempts);
                if (!gattBurst(job)) nextTry = millis() + 2000;
            }
        }
        delay(5);
    }
    logf("FAIL gatt job=%d not finished (attempts=%d, +%lums)", job, attempts, (unsigned long)millis());
    if (job == JOB_HIST) {
        histSessionEnd(false);              /* drops GATT if needed, closes days, publishes what we have */
    } else {
        if (g_mode == MODE_GATT) gattDrop();
        g_wantPv = false;
    }
    return false;
}

/* ADV phase: BLE scan until ADV_TARGET packets per device or timeout. WiFi connects meanwhile. */
static void advCollect() {
    accReset(g_acc[0]); accReset(g_acc[1]);
    uint32_t t0 = millis(), mpptDone = 0;
    while (millis() - t0 < ADV_SCAN_TIMEOUT_MS) {
        wifiPoll();
        int nm = g_acc[DEV_MPPT].n, ns = g_acc[DEV_SENSE].n;
        if (nm >= ADV_TARGET && ns >= ADV_TARGET) break;
        if (nm >= ADV_TARGET) {
            if (!mpptDone) mpptDone = millis();
            else if (millis() - mpptDone > ADV_SENSE_GRACE_MS) break;
        }
        delay(20);
    }
    bleScanStop();                          /* freeze the accumulators; gattBurst/gattDrop restart scans */
    delay(30);
    logf("STEP adv %lums mppt n=%d sense n=%d hits=%lu", (unsigned long)(millis() - t0),
         g_acc[DEV_MPPT].n, g_acc[DEV_SENSE].n, (unsigned long)g_scanHits);
}

/* decide this wake's GATT work from the (rounded) wake minute */
static void planJobs() {
    if (g_acc[DEV_MPPT].n > 0) rtc.lastState = g_acc[DEV_MPPT].state;
    if (!clockReady()) { logln("NOTE no clock — GATT schedule skipped"); return; }
    time_t now = time(nullptr);
    time_t wakeT = now - (time_t)(millis() / 1000);
    time_t slotT = ((wakeT + 20) / 60) * 60;          /* tolerate waking a bit early/late */
    struct tm t; localtime_r(&slotT, &t);
    g_slotMin = t.tm_min; g_slotHour = t.tm_hour;
    uint32_t slot10 = (uint32_t)(slotT / 600);        /* KST offset is a multiple of 600 s */
    bool tenMin = (t.tm_min % HIST_EVERY_MIN) == 0;
    if (tenMin) {
        rtc.jobSlot = slot10;
        rtc.jobPend = JOBB_HIST0 | (rtc.lastState != 0 ? JOBB_PV : 0);   /* 255 (unknown) → try pv */
        rtc.jobTries = 0;
    }
    bool jobNow = rtc.jobPend && rtc.jobSlot == slot10 && rtc.jobTries < JOB_MAX_TRIES;
    if (rtc.jobSlot != slot10 && rtc.jobPend) {
        logf("NOTE 10-min job slot %lu dropped (pend=%u)", (unsigned long)rtc.jobSlot, (unsigned)rtc.jobPend);
        rtc.jobPend = 0;
    }
    int yd = yesterdayYmd();
    if (rtc.ydayTryYmd != yd) { rtc.ydayTryYmd = yd; rtc.ydayTries = 0; }
    /* sleep2: done if a day >= yesterday was published (a seq rollover in the evening already
     * published the day that ends at midnight → the 00:10 fallback is a no-op) */
    bool ydayDue = (t.tm_hour > YDAY_HOUR || (t.tm_hour == YDAY_HOUR && t.tm_min >= YDAY_MIN)) &&
                   rtc.ydayDoneYmd < yd;
    bool ydayNow = ydayDue && rtc.ydayTries < YDAY_MAX_TRIES &&
                   (rtc.ydayTries < YDAY_FAST_TRIES || tenMin);
    /* sleep2: rollover seen but its day1 not published yet (time budget ran out) → retry */
    if (rtc.rollYdayYmd && rtc.ydayDoneYmd >= rtc.rollYdayYmd) rtc.rollYdayYmd = 0;
    bool rollNow = rtc.rollYdayYmd && rtc.rollTries < YDAY_MAX_TRIES &&
                   (rtc.rollTries < YDAY_FAST_TRIES || tenMin);
    if (rollNow) { rtc.rollTries++; ydayNow = true; logf("STEP rollover day1 %ld pending (try %u)",
                                                         (long)rtc.rollYdayYmd, (unsigned)rtc.rollTries); }
    bool histNow = (jobNow && (rtc.jobPend & JOBB_HIST0)) || ydayNow;
    int need = ydayNow ? 1 : 0;
    if (g_wantHist) {                                 /* hist cmd already queued a job: widen it */
        if (g_histN < need) g_histN = need;
        g_jobsRun |= 8;
    } else if (histNow || g_histQueued) {
        if (g_histQueued) { g_jobsRun |= 8; if (g_histReqDays < need) g_histReqDays = need; }
        else g_histReqDays = need;
        g_histQueued = true;
        tryHist();
    }
    if (g_wantPv) g_jobsRun |= 8;
    if (jobNow && (rtc.jobPend & JOBB_PV)) g_wantPv = true;
    if (jobNow) { rtc.jobTries++; g_jobsRun |= (rtc.jobPend & JOBB_HIST0 ? 1 : 0) | (rtc.jobPend & JOBB_PV ? 2 : 0); }
    if (ydayNow) { rtc.ydayTries++; g_jobsRun |= 4; }
    logf("STEP plan %02d:%02d slot10=%lu pend=%u tries=%u yday_due=%d tries=%u -> hist=%d(N=%d) pv=%d state=%u",
         t.tm_hour, t.tm_min, (unsigned long)slot10, (unsigned)rtc.jobPend, (unsigned)rtc.jobTries,
         (int)ydayDue, (unsigned)rtc.ydayTries, (int)g_wantHist, g_histN, (int)g_wantPv, (unsigned)rtc.lastState);
}

static void runJobs() {
    if (g_wantUnpair) {
        g_wantUnpair = false;
        if (g_mode == MODE_GATT) gattDrop();
        NimBLEDevice::deleteAllBonds();
        memset(g_day, 0, sizeof(g_day));
        cacheClear();
        logln("STEP unpair + hist nvs reset");
    }
    if (!g_wantHist && !g_wantPv) return;
    g_awakeLimitMs = AWAKE_MAX_JOB_MS;
    awakeWdtArm(g_awakeLimitMs);
    uint32_t until = g_awakeLimitMs - JOB_RESERVE_MS;          /* millis() since boot */
    bool histRan = false;
    if (g_wantHist) {
        histRan = true;
        runGatt(JOB_HIST, until, 4);
        if (g_today.ok) { rtc.day0DoneYmd = g_today.ymd; rtc.jobPend &= ~JOBB_HIST0; }
        /* sleep2: day1 of this job (seq-labelled) or the cached day1 / wall-clock yesterday */
        int yd = yesterdayYmd(), y1 = dayYmd(1), done = 0;
        DayRec tmp;
        if (g_histN >= 1 && g_yday.ok && g_yday.ymd) done = g_yday.ymd;
        else if (y1 && cacheLoad(y1, tmp)) done = y1;
        else if (yd && cacheLoad(yd, tmp)) done = yd;
        if (done > rtc.ydayDoneYmd) {
            logf("STEP yday %d done", done);
            rtc.ydayDoneYmd = done;
        }
        if (rtc.rollYdayYmd && rtc.ydayDoneYmd >= rtc.rollYdayYmd) rtc.rollYdayYmd = 0;
    }
    if (g_wantPv) {
        if (histRan) serviceFor(PV_AFTER_HIST_MS);
        g_pvPublished = false;
        runGatt(JOB_PV, until, 2);
        if (g_pvPublished) rtc.jobPend &= ~JOBB_PV;
    }
}

/* publish the retained state message and wait until it comes back = everything before it was
 * delivered to the broker (TCP + broker order), then disconnect cleanly */
static void mqttFlushAndClose(time_t nextWake) {
    if (mqtt.connected()) {
        snprintf(g_flushTok, sizeof(g_flushTok), "%lu-%lu", (unsigned long)rtc.boots, (unsigned long)millis());
        char js[256];
        long now = clockReady() ? (long)time(nullptr) : 0;
        char iso[32] = "";
        if (now) { struct tm t; time_t tt = now; localtime_r(&tt, &t); strftime(iso, sizeof(iso), "%Y-%m-%dT%H:%M:%S+09:00", &t); }
        snprintf(js, sizeof(js),
                 "{\"tok\":\"%s\",\"state\":\"sleep\",\"ts\":%ld,\"time\":\"%s\",\"next\":%ld,"
                 "\"boot\":%lu,\"awake_ms\":%lu,\"wdt\":%lu}",
                 g_flushTok, now, iso, (long)nextWake, (unsigned long)rtc.boots,
                 (unsigned long)millis(), (unsigned long)rtc.wdtTrips);
        g_flushOk = false;
        mqtt.publish(TOPIC_LWT, js, true);
        uint32_t t0 = millis();
        while (!g_flushOk && mqtt.connected() && millis() - t0 < MQTT_FLUSH_MS) { mqtt.loop(); delay(5); }
        logf("STEP mqtt flush %s %lums", g_flushOk ? "ok" : "timeout", (unsigned long)(millis() - t0));
        mqtt.disconnect();
        delay(20);
    }
    wifiClient.stop();
}

static void goSleep() {
    time_t nextWake = 0;
    sleepUsToBoundary(&nextWake);
    mqttFlushAndClose(nextWake);
    WiFi.disconnect(true);
    WiFi.mode(WIFI_OFF);
    if (g_mode == MODE_GATT) gattDrop();
    bleScanStop();
    NimBLEDevice::deinit(true);
    if (g_awakeWdt) esp_timer_stop(g_awakeWdt);
    prefs.end();
    led(false);
    rtc.prevAwakeMs = millis();
    uint64_t us = sleepUsToBoundary(nullptr);          /* recomputed after the shutdown work */
    rtc.lastSleepUs = us;                              /* sleep3: drift compensation on the next wake */
    logf("STEP sleep %llums (awake %lums)", (unsigned long long)(us / 1000), (unsigned long)millis());
    Serial.flush();
    esp_sleep_enable_timer_wakeup(us);
    esp_deep_sleep_start();
}

void setup() {
    Serial.begin(115200);
    g_bootMs = millis();
    esp_reset_reason_t rr = esp_reset_reason();
    bool warm = (rr == ESP_RST_DEEPSLEEP) && rtc.magic == RTC_MAGIC;
    if (!warm) {
        memset(&rtc, 0, sizeof(rtc));
        rtc.magic = RTC_MAGIC;
        rtc.lastState = 255;
        rtc.d0Seq = -1;
    }
    rtc.boots++;
    /* sleep3: RTC drift compensation. During deep sleep the clock advanced by ~lastSleepUs as
     * measured by the RC slow clock; driftPpm (from the NTP steps) is how much too fast that is. */
    int64_t driftCorrUs = 0;
    if (warm && rtc.lastSleepUs) {
        if (rtc.driftValid && rtc.timeValid && time(nullptr) >= 1700000000) {
            driftCorrUs = (int64_t)((double)rtc.lastSleepUs * (double)rtc.driftPpm / 1e6);
            if (driftCorrUs) setUs(nowUs() - driftCorrUs);
        }
        rtc.sleepUsSinceNtp += rtc.lastSleepUs;
    }
    rtc.lastSleepUs = 0;
    awakeWdtArm(g_awakeLimitMs);
    setenv("TZ", TZ_INFO, 1);
    tzset();
    if (warm && rtc.timeValid && time(nullptr) >= 1700000000) g_clockOk = true;
    pinMode(PIN_LED, OUTPUT);
    led(false);
    g_boardV = boardV();                    /* before the radios start (less ADC noise / droop) */
    prefs.begin("vtgatt", false);
    if (!warm) {
        prefs.remove("today");              /* old single-slot keys, replaced by ring h00..h39 */
        prefs.remove("yday");
        Serial.println("\n=== HUZZAH32 Victron ADV + GATT, deep-sleep 1-min (sleep3) ===");
    }
    if (!warm || rtc.d0Seq < 0) d0Load();   /* sleep2: last day0 seq/ymd survive power loss */
    if (!warm) {                            /* sleep3: last drift estimate survives power loss */
        int32_t dp = prefs.getInt("drift", INT32_MIN);
        if (dp != INT32_MIN && dp >= -DRIFT_MAX_PPM && dp <= DRIFT_MAX_PPM) { rtc.driftPpm = (float)dp; rtc.driftValid = true; }
    }
    if (driftCorrUs) logf("STEP drift comp %ldms (%.0f ppm)", (long)(driftCorrUs / 1000), (double)rtc.driftPpm);
    logf("STEP wake #%lu rst=%d warm=%d clock=%d vboard=%.2f prev_awake=%lums wdt=%lu",
         (unsigned long)rtc.boots, (int)rr, (int)warm, (int)g_clockOk, (double)g_boardV,
         (unsigned long)rtc.prevAwakeMs, (unsigned long)rtc.wdtTrips);

    wifiStart();                            /* connects in the background during the BLE scan */
    NimBLEDevice::init("vt-br");
    bleSec();
    bleScanStart();
    advCollect();

    if (wifiWait(WIFI_TIMEOUT_MS)) {
        if (!clockReady()) ntpSync(NTP_FIRST_TIMEOUT_MS);
        else if (time(nullptr) - rtc.lastNtp > NTP_RESYNC_S) ntpSync(NTP_RESYNC_TIMEOUT_MS);
        mqttConnect();
        uint32_t t0 = millis();
        while (mqtt.connected() && millis() - t0 < MQTT_RX_WINDOW_MS) { mqtt.loop(); delay(5); }   /* queued cmds */
    }
    planJobs();
    publishLive();
    publishStatus();
    runJobs();
    goSleep();
}

void loop() {
    goSleep();                              /* not reached: setup() always ends in deep sleep */
}

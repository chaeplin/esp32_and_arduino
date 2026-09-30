/*
 * Adafruit HUZZAH32 ESP32 Feather
 * Victron ADV IR 10s + GATT cmd
 *
 * 평소: ADV 10초 평균 → victron/mppt, victron/sense
 * {"cmd":"hist"} day0=오늘 0x1050, day1=어제 0x1051.
 *   앱과 같이 0부터 읽음. 어제 = day1.
 *   NTP(KST) 날짜로 NVS ymd 비교. 같으면 GATT 없이 NVS → MQTT.
 *   다르거나 없으면 GATT 0x1050 → 0x1051 → EDD1 → EDD0 → NVS → 끊고 MQTT.
 * {"cmd":"pv"} 현재 PV V/W/A → disconnect → victron/mppt/pv (없는 값은 null)
 * PIN 은 VICTRON_PIN. NTP + yard/time 보조.
 *
 * Arduino IDE: Adafruit ESP32 Feather, NimBLE-Arduino 2.x + PubSubClient
 * MQTT / Wi-Fi / PIN / MAC / AES 키는 아래 플레이스홀더를 로컬 값으로 바꿔 쓴다.
 */

#include <WiFi.h>
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

static const char *WIFI_SSID    = "YOUR_WIFI_SSID";
static const char *WIFI_PASS    = "YOUR_WIFI_PASSWORD";
static const char *MQTT_HOST    = "YOUR_MQTT_HOST";
static const uint16_t MQTT_PORT = 1883;
static const char *MQTT_USER    = "YOUR_MQTT_USER";
static const char *MQTT_PASS    = "YOUR_MQTT_PASSWORD";
static const char *MQTT_CLIENT  = "victron-gatt-bridge";

static const char *TOPIC_MPPT   = "victron/mppt";
static const char *TOPIC_SENSE  = "victron/sense";
static const char *TOPIC_STATUS = "victron/status";
static const char *TOPIC_HIST   = "victron/mppt/hist";
static const char *TOPIC_PV     = "victron/mppt/pv";
static const char *TOPIC_LOG    = "victron/gatt";
static const char *TOPIC_CMD    = "victron/gatt/cmd";
static const char *TOPIC_LWT    = "victron/lwt";
static const char *TOPIC_TIME   = "yard/time";

#define VICTRON_PIN 000000u
static const uint32_t g_pin = VICTRON_PIN;

static const int PIN_LED  = 13;
static const int PIN_VBAT = 35;
static const uint16_t VICTRON_CID = 0x02E1;
static const uint32_t LIVE_MS = 10000;
static const uint32_t STALE_MS = 8000;

static const uint16_t REG_TODAY = 0x1050; /* HISTORY_DAY00 = today */
static const uint16_t REG_YDAY  = 0x1051; /* HISTORY_DAY01 = yesterday */
static const uint16_t REG_TODAY_X = 0x10A0; /* app pairs 1050+10A0 */
static const uint16_t REG_YDAY_X  = 0x10A1;
static const uint16_t REG_EDD1 = 0xEDD1; /* yield yesterday */
static const uint16_t REG_EDD0 = 0xEDD0; /* pmax yesterday */
static const uint16_t REG_EDD3 = 0xEDD3; /* yield today */
static const uint16_t REG_EDD2 = 0xEDD2; /* pmax today */
static const uint16_t REG_PV_V = 0xEDBB;
static const uint16_t REG_PV_W = 0xEDBC;
static const uint16_t REG_PV_A = 0xEDBD;

static const char *SVC_APP = "306b0001-b081-4037-83dc-e59fcc3cdfd0";
static const char *CHR_2   = "306b0002-b081-4037-83dc-e59fcc3cdfd0";
static const char *CHR_3   = "306b0003-b081-4037-83dc-e59fcc3cdfd0";
static const char *CHR_4   = "306b0004-b081-4037-83dc-e59fcc3cdfd0";

enum { DEV_MPPT = 0, DEV_SENSE = 1, DEV_N = 2 };
enum { MODE_ADV = 0, MODE_GATT = 1 };
enum { JOB_NONE = 0, JOB_HIST = 1, JOB_PV = 2 };

struct VictronDev {
    const char *id, *label, *macHex;
    uint8_t key[16];
};
static VictronDev g_dev[DEV_N] = {
    { "mppt", "YOUR_MPPT_SN", "aabbccddeeff",
      { 0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00 } },
    { "sense", "YOUR_SENSE_SN", "112233445566",
      { 0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00 } }
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
    bool ok, hasYield, hasPmax;
    int ymd;
    float yieldKwh, consumedKwh, pmaxW, vpvMax, vbatMax, vbatMin;
};

static DevState g_st[DEV_N];
static Acc g_acc[DEV_N];
static DayRec g_today, g_yday;

WiFiClient wifiClient;
PubSubClient mqtt(wifiClient);
Preferences prefs;

static uint32_t g_bootMs, g_lastLive, g_lastWifi, g_lastStatus;
static uint32_t g_scanHits, g_decOk, g_decFail, g_gattStartMs, g_histNextMs;
static uint8_t g_connFails;
static int g_mode = MODE_ADV, g_job = JOB_NONE;
static volatile bool g_wantHist, g_wantPv, g_wantUnpair, g_enc, g_sawNotify, g_histPubPending;
static volatile bool g_histQueued = false;
static bool g_clockOk = false;
static bool g_ntpStarted = false;
static uint8_t g_histPhase = 0;
static int g_pvPhase = 0;
static NimBLEClient *g_cli = nullptr;
static NimBLERemoteCharacteristic *g_chCtrl, *g_chCmd, *g_chBulk;
static uint8_t g_seq = 1;
static uint8_t g_rx[512];
static volatile uint16_t g_rxLen;
static uint8_t g_parse[768];
static uint16_t g_parseLen;
static bool g_havePvV, g_havePvW, g_havePvA;
static float g_pvV, g_pvWatt, g_pvA;

static void led(bool on) { digitalWrite(PIN_LED, on ? HIGH : LOW); }
static float boardV() { return analogRead(PIN_VBAT) * (3.3f / 4095.0f) * 2.0f; }

static void pubYday();
static void tryHist();

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
    setenv("TZ", "KST-9", 1);
    tzset();
    g_clockOk = true;
    logf("STEP time mqtt ymd=%d", todayYmd());
    tryHist();
}
static void ntpStart() {
    if (g_ntpStarted || WiFi.status() != WL_CONNECTED) return;
    setenv("TZ", "KST-9", 1);
    tzset();
    configTime(0, 0, "pool.ntp.org", "time.google.com");
    g_ntpStarted = true;
    logln("STEP ntp start KST");
}
static void ntpPoll() {
    if (g_clockOk) return;
    time_t now = time(nullptr);
    if (now < 1700000000) return;
    setenv("TZ", "KST-9", 1);
    tzset();
    g_clockOk = true;
    logf("STEP ntp ok today=%d yday=%d", todayYmd(), yesterdayYmd());
    tryHist();
}

/* hist cmd: NTP/yard-time 날짜가 있어야 NVS 비교.
 * 어제 NVS가 있으면 GATT skip. 없으면 day0+day1 GET. */
static void tryHist() {
    if (!g_histQueued) return;
    if (g_mode == MODE_GATT) return;
    int y = yesterdayYmd();
    if (!y) {
        logln("WAIT clock — hist queued (NTP or yard/time)");
        return;
    }
    /* today 슬롯 ymd가 어제면 마감 전 스냅샷이다. 승격 금지, GATT 다시. */
    if (g_today.ok && g_today.ymd == y) {
        logf("NVS drop open-today %d — not closed yesterday", y);
        memset(&g_today, 0, sizeof(g_today));
        prefs.remove("today");
        if (g_yday.ymd == y) {
            memset(&g_yday, 0, sizeof(g_yday));
            prefs.remove("yday");
        }
    }
    g_histQueued = false;
    if (g_yday.ok && g_yday.ymd == y) {
        logf("NVS hit yesterday %d — GATT skip", y);
        pubYday();
        return;
    }
    logf("NVS miss yday=%d want=%d — GATT day0+day1", g_yday.ymd, y);
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

static void wifiConnect() {
    if (WiFi.status() == WL_CONNECTED) return;
    static bool started;
    if (!started) {
        WiFi.persistent(false); WiFi.mode(WIFI_STA); WiFi.setSleep(false);
        WiFi.setHostname(MQTT_CLIENT);
        WiFi.begin(WIFI_SSID, WIFI_PASS);
        started = true;
        return;
    }
    WiFi.begin(WIFI_SSID, WIFI_PASS);
}

static void mqttCb(char *topic, byte *payload, unsigned int len) {
    if (!topic || !len) return;
    char tmp[96];
    if (len >= sizeof(tmp)) len = sizeof(tmp) - 1;
    memcpy(tmp, payload, len); tmp[len] = 0;
    if (!strcmp(topic, TOPIC_TIME)) {
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
    else if (strstr(tmp, "\"cmd\":\"pv\"") || !strcmp(tmp, "pv")) g_wantPv = true;
    else if (strstr(tmp, "hist")) {
        g_histQueued = true;
        tryHist();
    }
}

static void mqttConnect() {
    if (mqtt.connected()) return;
    mqtt.setServer(MQTT_HOST, MQTT_PORT);
    mqtt.setCallback(mqttCb);
    mqtt.setKeepAlive(30);
    mqtt.setBufferSize(768);
    if (mqtt.connect(MQTT_CLIENT, MQTT_USER, MQTT_PASS, TOPIC_LWT, 0, true, "offline")) {
        mqtt.publish(TOPIC_LWT, "online", true);
        mqtt.subscribe(TOPIC_CMD);
        mqtt.subscribe(TOPIC_TIME);
        logln("STEP mqtt up");
    }
}
static void mqttKeep() {
    if (WiFi.status() != WL_CONNECTED) {
        if (millis() - g_lastWifi > 4000) { g_lastWifi = millis(); wifiConnect(); }
        return;
    }
    ntpStart();
    ntpPoll();
    if (!mqtt.connected()) mqttConnect();
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

static bool parseVictron(int di, const uint8_t *mfg, size_t mlen, int rssi,
                         const char *addr, const char *name, uint8_t addrType) {
    size_t off = 0;
    uint16_t cid = (uint16_t)mfg[0] | ((uint16_t)mfg[1] << 8);
    if (cid == VICTRON_CID) off = 2;
    if (mlen <= off || mfg[off] != 0x10) return false;
    const uint8_t *p = mfg + off;
    size_t n = mlen - off;
    if (n < 10) return false;
    if (p[7] != g_dev[di].key[0]) return false;
    uint16_t model = (uint16_t)p[2] | ((uint16_t)p[3] << 8);
    uint8_t rec = p[4];
    uint16_t iv = (uint16_t)p[5] | ((uint16_t)p[6] << 8);
    size_t ctLen = n - 8;
    if (ctLen < 8 || ctLen > 24) return false;
    uint8_t pt[32]; memset(pt, 0, sizeof(pt));
    if (!aes128_ctr(g_dev[di].key, iv, p + 8, ctLen, pt)) return false;
    DevState &s = g_st[di];
    if (s.seen && s.nonce == iv && (millis() - s.lastMs) < 400) return true;
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
    if (s.ok) {
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
    return s.ok;
}

static int findDevByKeyCheck(const uint8_t *mfg, size_t mlen) {
    for (int i = 0; i < DEV_N; i++) {
        DevState tmp = g_st[i];
        bool ok = parseVictron(i, mfg, mlen, 0, "", "", 0);
        g_st[i] = tmp;
        if (ok) return i;
    }
    return -1;
}

static bool fresh(int di) {
    return g_st[di].seen && (millis() - g_st[di].lastMs) < STALE_MS;
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
        if (parseVictron(di, raw, md.size(), dev->getRSSI(),
                         addr.c_str(), dev->getName().c_str(),
                         dev->getAddress().getType()))
            g_decOk++;
        else
            g_decFail++;
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
    scan->setActiveScan(true); /* bleak-like; connectable ADV */
    scan->setInterval(160);
    scan->setWindow(80);
    scan->setDuplicateFilter(false);
    scan->start(0, false);
}
static void bleScanStop() {
    NimBLEScan *s = NimBLEDevice::getScan();
    if (s->isScanning()) s->stop();
}

static void accReset(Acc &a) { memset(&a, 0, sizeof(a)); }

static void publishLive() {
    if (g_mode != MODE_ADV) return;
    if (millis() - g_lastLive < LIVE_MS) return;
    g_lastLive = millis();
    if (!mqtt.connected()) { accReset(g_acc[0]); accReset(g_acc[1]); return; }
    char buf[400];
    Acc &m = g_acc[DEV_MPPT];
    if (m.n > 0) {
        snprintf(buf, sizeof(buf),
                 "{\"id\":\"mppt\",\"src\":\"victron_ble\",\"sn\":\"%s\","
                 "\"model\":\"MPPT 75/15\",\"mac\":\"%s\",\"rssi\":%d,"
                 "\"vbat\":%.2f,\"ibat\":%.2f,\"power\":%.0f,\"yield_wh\":%.0f,"
                 "\"load_a\":%.1f,\"state\":\"%s\",\"state_n\":%u,\"error\":%u,\"n\":%d}",
                 g_dev[DEV_MPPT].label, g_st[DEV_MPPT].addr, m.n ? (int)(m.rssi / m.n) : 0,
                 m.vbat / m.n, m.n ? m.ibat / m.n : NAN,
                 m.n ? m.pvW / m.n : NAN, m.n ? m.yieldWh / m.n : NAN,
                 m.n ? m.loadA / m.n : NAN,
                 chargeName(m.state), (unsigned)m.state, (unsigned)m.err, m.n);
        mqtt.publish(TOPIC_MPPT, buf, false);
    }
    Acc &s = g_acc[DEV_SENSE];
    if (s.n > 0) {
        if (s.nTemp)
            snprintf(buf, sizeof(buf),
                     "{\"id\":\"sense\",\"src\":\"victron_ble\",\"sn\":\"%s\","
                     "\"model\":\"SmartBatterySense\",\"mac\":\"%s\",\"rssi\":%d,"
                     "\"vbat\":%.2f,\"temp\":%.1f,\"n\":%d}",
                     g_dev[DEV_SENSE].label, g_st[DEV_SENSE].addr, (int)(s.rssi / s.n),
                     s.vbat / s.n, s.tempC / s.nTemp, s.n);
        else
            snprintf(buf, sizeof(buf),
                     "{\"id\":\"sense\",\"src\":\"victron_ble\",\"sn\":\"%s\","
                     "\"model\":\"SmartBatterySense\",\"mac\":\"%s\",\"rssi\":%d,"
                     "\"vbat\":%.2f,\"n\":%d}",
                     g_dev[DEV_SENSE].label, g_st[DEV_SENSE].addr, (int)(s.rssi / s.n), s.vbat / s.n, s.n);
        mqtt.publish(TOPIC_SENSE, buf, false);
    }
    accReset(g_acc[0]); accReset(g_acc[1]);
}

static void publishStatus() {
    if (millis() - g_lastStatus < LIVE_MS) return;
    g_lastStatus = millis();
    if (!mqtt.connected()) return;
    char buf[400];
    snprintf(buf, sizeof(buf),
             "{\"src\":\"victron_ble\",\"board\":\"feather\",\"mode\":\"%s\","
             "\"mppt_seen\":%s,\"sense_seen\":%s,\"mppt_rssi\":%d,\"sense_rssi\":%d,"
             "\"hits\":%lu,\"dec_ok\":%lu,\"dec_fail\":%lu,"
             "\"clock\":\"%s\",\"today\":%d,\"yday\":%d,\"nvs_today\":%d,\"nvs_yday\":%d,"
             "\"today_ok\":%s,\"yday_ok\":%s,\"wifi_rssi\":%d,\"vbat\":%.2f,\"ip\":\"%s\",\"uptime\":%lu}",
             g_mode == MODE_GATT ? "gatt" : "adv",
             fresh(DEV_MPPT) ? "true" : "false",
             fresh(DEV_SENSE) ? "true" : "false",
             fresh(DEV_MPPT) ? g_st[DEV_MPPT].rssi : 0,
             fresh(DEV_SENSE) ? g_st[DEV_SENSE].rssi : 0,
             (unsigned long)g_scanHits, (unsigned long)g_decOk, (unsigned long)g_decFail,
             g_clockOk ? "ok" : "wait", todayYmd(), yesterdayYmd(),
             g_today.ymd, g_yday.ymd,
             g_today.ok ? "true" : "false",
             g_yday.ok ? "true" : "false",
             WiFi.RSSI(), boardV(),
             WiFi.isConnected() ? WiFi.localIP().toString().c_str() : "",
             (unsigned long)((millis() - g_bootMs) / 1000));
    mqtt.publish(TOPIC_STATUS, buf, false);
    led(fresh(DEV_MPPT) || fresh(DEV_SENSE));
}

static int cborLen(const uint8_t *p, int n) {
    if (n < 1) return -1;
    uint8_t t = p[0];
    if (t <= 0x17) return 1;
    if (t == 0x18) return n >= 2 ? 2 : -1;
    if (t == 0x19) return n >= 3 ? 3 : -1;
    if (t >= 0x40 && t <= 0x57) { int need = 1 + (t - 0x40); return n >= need ? need : -1; }
    if (t == 0x58) { if (n < 2) return -1; int need = 2 + p[1]; return n >= need ? need : -1; }
    if (t == 0x41) return n >= 2 ? 2 : -1;
    if (t == 0x42) return n >= 3 ? 3 : -1;
    if (t == 0x44) return n >= 5 ? 5 : -1;
    return 1;
}
static bool cborNum(const uint8_t *p, int n, double *out) {
    if (n < 1) return false;
    if (p[0] <= 0x17) { *out = p[0]; return true; }
    if (p[0] == 0x41 && n >= 2) { *out = p[1]; return true; }
    if (p[0] == 0x42 && n >= 3) { *out = p[1] | (p[2] << 8); return true; }
    if (p[0] == 0x44 && n >= 5) {
        *out = (uint32_t)p[1] | ((uint32_t)p[2] << 8) | ((uint32_t)p[3] << 16) | ((uint32_t)p[4] << 24);
        return true;
    }
    return false;
}
static uint32_t u32le(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}
static uint16_t u16le(const uint8_t *p) { return (uint16_t)p[0] | ((uint16_t)p[1] << 8); }

static void recSave(const char *key, const DayRec &r) {
    prefs.putBytes(key, &r, sizeof(r));
}
static void recLoad(const char *key, DayRec &r) {
    DayRec tmp;
    if (prefs.getBytes(key, &tmp, sizeof(tmp)) == sizeof(tmp) && tmp.ok)
        r = tmp;
}

static void parseDayPage(DayRec &r, const uint8_t *p, int n, int ymd, const char *tag) {
    if (n < 34 || p[0] == 255) {
        logf("NOTE %s page skip n=%d v=%u", tag, n, n ? (unsigned)p[0] : 0);
        return;
    }
    uint32_t yld = u32le(p + 1);
    uint32_t cns = u32le(p + 5);
    uint32_t pmx = u32le(p + 24);
    uint16_t vbM = u16le(p + 9), vbm = u16le(p + 11), vpv = u16le(p + 30);
    r.yieldKwh = (yld == 0xFFFFFFFFu) ? 0 : yld / 100.0f;
    r.consumedKwh = (cns == 0xFFFFFFFFu) ? 0 : cns / 100.0f;
    r.vbatMax  = (vbM == 0xFFFF) ? 0 : vbM / 100.0f;
    r.vbatMin  = (vbm == 0xFFFF) ? 0 : vbm / 100.0f;
    r.pmaxW    = (pmx == 0xFFFFFFFFu) ? 0 : (float)pmx;
    r.vpvMax   = (vpv == 0xFFFF) ? 0 : vpv / 100.0f;
    r.hasYield = (yld != 0xFFFFFFFFu);
    r.hasPmax  = (pmx != 0xFFFFFFFFu);
    r.ok = r.hasYield || r.hasPmax;
    if (ymd) r.ymd = ymd;
    recSave(tag, r);
    logf("NVS %s page ymd=%d y=%.2f cons=%.2f pmax=%.0f",
         tag, r.ymd, (double)r.yieldKwh, (double)r.consumedKwh, (double)r.pmaxW);
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
        if (cl < 0) break;
        const uint8_t *pay = g_parse + i + 5;
        if (op == 0x08 && (reg == REG_TODAY || reg == REG_YDAY)) {
            DayRec &slot = (reg == REG_TODAY) ? g_today : g_yday;
            const char *tag = (reg == REG_TODAY) ? "today" : "yday";
            int ymd = (reg == REG_TODAY) ? todayYmd() : yesterdayYmd();
            if (pay[0] == 0x58 && cl >= 2) parseDayPage(slot, pay + 2, pay[1], ymd, tag);
            else if (cl >= 34) parseDayPage(slot, pay, cl, ymd, tag);
        }
        if (op == 0x08) {
            double num = NAN;
            if (cborNum(pay, cl, &num)) {
                if (reg == REG_PV_V) { g_pvV = (float)(num * 0.01); g_havePvV = true; }
                if (reg == REG_PV_W) { g_pvWatt = (float)(num * 0.01); g_havePvW = true; }
                if (reg == REG_PV_A) { g_pvA = (float)(num * 0.1); g_havePvA = true; }
                if (reg == REG_EDD1) {
                    g_yday.yieldKwh = (float)(num * 0.01);
                    g_yday.hasYield = g_yday.ok = true;
                    if (yesterdayYmd()) g_yday.ymd = yesterdayYmd();
                    recSave("yday", g_yday);
                    logf("NVS yday EDD1 ymd=%d y=%.2f", g_yday.ymd, (double)g_yday.yieldKwh);
                }
                if (reg == REG_EDD0) {
                    g_yday.pmaxW = (float)num;
                    g_yday.hasPmax = g_yday.ok = true;
                    if (yesterdayYmd()) g_yday.ymd = yesterdayYmd();
                    recSave("yday", g_yday);
                    logf("NVS yday EDD0 ymd=%d pmax=%.0f", g_yday.ymd, (double)g_yday.pmaxW);
                }
                if (reg == REG_EDD3) {
                    g_today.yieldKwh = (float)(num * 0.01);
                    g_today.hasYield = g_today.ok = true;
                    if (todayYmd()) g_today.ymd = todayYmd();
                    recSave("today", g_today);
                    logf("NVS today EDD3 ymd=%d y=%.2f", g_today.ymd, (double)g_today.yieldKwh);
                }
                if (reg == REG_EDD2) {
                    g_today.pmaxW = (float)num;
                    g_today.hasPmax = g_today.ok = true;
                    if (todayYmd()) g_today.ymd = todayYmd();
                    recSave("today", g_today);
                    logf("NVS today EDD2 ymd=%d pmax=%.0f", g_today.ymd, (double)g_today.pmaxW);
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
static void drainRx() {
    noInterrupts();
    uint16_t n = g_rxLen;
    if (n) {
        if (g_parseLen + n > sizeof(g_parse)) g_parseLen = 0;
        if (g_parseLen + n <= sizeof(g_parse)) {
            memcpy(g_parse + g_parseLen, g_rx, n);
            g_parseLen += n;
        }
        g_rxLen = 0;
    }
    interrupts();
    if (g_parseLen) parseFrames();
}
static void onNotify(NimBLERemoteCharacteristic *, uint8_t *data, size_t len, bool) {
    if (!data || !len) return;
    g_sawNotify = true;
    noInterrupts();
    if (g_rxLen + len <= sizeof(g_rx)) {
        memcpy(g_rx + g_rxLen, data, len);
        g_rxLen += (uint16_t)len;
    } else g_rxLen = 0;
    interrupts();
}

class ClientCB : public NimBLEClientCallbacks {
    void onConnect(NimBLEClient *) override {
        logln("STEP onConnect");
    }
    void onDisconnect(NimBLEClient *, int reason) override {
        logf("STEP drop 0x%02x -> ADV", reason);
        if (g_job == JOB_HIST && (g_today.ok || g_yday.ok))
            g_histPubPending = true;
        g_wantHist = false;
        g_mode = MODE_ADV;
        g_job = JOB_NONE;
        g_cli = nullptr;
        g_chCtrl = g_chCmd = g_chBulk = nullptr;
        bleScanStart();
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
    bool ok = false;
    if (g_chCmd) ok = g_chCmd->writeValue(pkt, n, false);
    if (g_chBulk) g_chBulk->writeValue(pkt, n, false);
    return ok;
}
/* 05 seq flag 19 REG [19 REG ...] */
static bool getRegs(const uint16_t *regs, int n, uint8_t flag) {
    if (n <= 0) return false;
    uint8_t pkt[48];
    int i = 0;
    pkt[i++] = 0x05;
    pkt[i++] = g_seq;
    pkt[i++] = flag;
    g_seq = (uint8_t)(g_seq + 1); if (!g_seq) g_seq = 1;
    for (int k = 0; k < n && i + 3 <= (int)sizeof(pkt); k++) {
        pkt[i++] = 0x19;
        pkt[i++] = (uint8_t)(regs[k] >> 8);
        pkt[i++] = (uint8_t)regs[k];
    }
    bool ok = wrBoth(pkt, i);
    logf("TX get flag=0x%02X n=%d first=0x%04X ok=%d",
         (unsigned)flag, n, regs[0], (int)ok);
    return ok;
}
/* Mac working hist: concat 05 03 81 19 HI LO per reg, write 0003+0004 */
static bool getRegs81Concat(const uint16_t *regs, int n) {
    uint8_t pkt[64];
    int i = 0;
    for (int k = 0; k < n && i + 6 <= (int)sizeof(pkt); k++) {
        pkt[i++] = 0x05;
        pkt[i++] = 0x03;
        pkt[i++] = 0x81;
        pkt[i++] = 0x19;
        pkt[i++] = (uint8_t)(regs[k] >> 8);
        pkt[i++] = (uint8_t)regs[k];
    }
    bool ok = wrBoth(pkt, i);
    logf("TX hist81-concat n=%d bytes=%d ok=%d", n, i, (int)ok);
    return ok;
}
static bool getReg(uint16_t reg) {
    uint8_t pkt[6] = { 0x05, 0x00, 0x81, 0x19, (uint8_t)(reg >> 8), (uint8_t)reg };
    return wrBoth(pkt, 6);
}

static void gattDrop() {
    if (g_cli) {
        if (g_cli->isConnected()) g_cli->disconnect();
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

static void pubDay(const DayRec &r, const char *kind, int day) {
    if (!r.ok || !mqtt.connected()) return;
    int want = (day == 0) ? todayYmd() : yesterdayYmd();
    char js[300];
    snprintf(js, sizeof(js),
             "{\"id\":\"mppt\",\"sn\":\"%s\",\"src\":\"%s\","
             "\"kind\":\"%s\",\"day\":%d,\"ymd\":%d,"
             "\"yield_kwh\":%.2f,\"consumed_kwh\":%.2f,\"pmax_w\":%.0f,\"vpv_max\":%.2f,"
             "\"vbat_max\":%.2f,\"vbat_min\":%.2f}",
             g_dev[DEV_MPPT].label,
             (r.ymd && want && r.ymd == want) ? "nvs" : "gatt",
             kind, day, r.ymd,
             (double)r.yieldKwh, (double)r.consumedKwh, (double)r.pmaxW, (double)r.vpvMax,
             (double)r.vbatMax, (double)r.vbatMin);
    mqtt.publish(TOPIC_HIST, js, false);
    logf("PUB hist %s day=%d ymd=%d y=%.2f pmax=%.0f",
         kind, day, r.ymd, (double)r.yieldKwh, (double)r.pmaxW);
}
static void pubYday() { pubDay(g_yday, "yesterday", 1); }
static void pubPv() {
    char vs[16], ws[16], as[16], js[220];
    jsonNum(vs, sizeof(vs), g_havePvV, g_pvV, 2);
    jsonNum(ws, sizeof(ws), g_havePvW, g_pvWatt, 2);
    jsonNum(as, sizeof(as), g_havePvA, g_pvA, 2);
    snprintf(js, sizeof(js),
             "{\"id\":\"mppt\",\"sn\":\"%s\",\"src\":\"gatt\","
             "\"vpv\":%s,\"ppv\":%s,\"ipv\":%s}",
             g_dev[DEV_MPPT].label, vs, ws, as);
    if (mqtt.connected()) mqtt.publish(TOPIC_PV, js, false);
    logf("PUB pv %s", js);
}

static void histFinish() {
    g_wantHist = false;
    gattDrop();
    if (g_today.ok) pubDay(g_today, "today", 0);
    if (g_yday.ok) pubDay(g_yday, "yesterday", 1);
    if (!g_today.ok && !g_yday.ok) logln("NOTE hist empty");
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
    NimBLEScan *sc = NimBLEDevice::getScan();
    bool scanOn = sc && sc->isScanning();
    logf("STEP scanOn=%d clients=%d",
         (int)scanOn, (int)NimBLEDevice::getCreatedClientCount());
    NimBLEAddress peer(m.addr, m.addrType);
    NimBLEClient *c = NimBLEDevice::createClient();
    if (!c) { logln("FAIL createClient"); return false; }
    c->setClientCallbacks(&g_clientCb, false);
    c->setConnectTimeout(8000);
    const NimBLEAdvertisedDevice *ad = nullptr;
    if (sc) {
        NimBLEScanResults rs = sc->getResults();
        for (int i = 0; i < rs.getCount(); i++) {
            const NimBLEAdvertisedDevice *d = rs.getDevice(i);
            if (d && String(d->getAddress().toString().c_str()) == String(m.addr)) {
                ad = d; break;
            }
        }
    }
    String aStr = ad ? String(ad->getAddress().toString().c_str()) : String(m.addr);
    uint8_t aType = ad ? ad->getAddress().getType() : m.addrType;
    bleScanStop();
    delay(80);
    NimBLEAddress use(aStr.c_str(), aType);
    logf("STEP peer %s type=%u", use.toString().c_str(), (unsigned)use.getType());
    uint32_t tConn = millis();
    bool linked = c->connect(use, true, false, true);
    uint32_t cms = millis() - tConn;
    logf("STEP connect()=%d ms=%lu", (int)linked, (unsigned long)cms);
    if (!linked) {
        logln("FAIL connect");
        NimBLEDevice::deleteClient(c);
        bleScanStart();
        return false;
    }
    g_enc = false;
    c->secureConnection(true);
    uint32_t t0 = millis();
    while (!g_enc && millis() - t0 < 5000) { delay(40); mqtt.loop(); }
    logf("STEP enc=%d", (int)g_enc);
    if (!c->discoverAttributes()) {
        logln("FAIL discover");
        c->disconnect(); NimBLEDevice::deleteClient(c); bleScanStart();
        return false;
    }
    NimBLERemoteService *svc = c->getService(SVC_APP);
    if (!svc) {
        logln("FAIL no_306b");
        c->disconnect(); NimBLEDevice::deleteClient(c); bleScanStart();
        return false;
    }
    g_chCtrl = svc->getCharacteristic(CHR_2);
    g_chCmd  = svc->getCharacteristic(CHR_3);
    g_chBulk = svc->getCharacteristic(CHR_4);
    if (g_chCtrl && g_chCtrl->canNotify()) g_chCtrl->subscribe(true, onNotify);
    if (g_chCmd && g_chCmd->canNotify()) g_chCmd->subscribe(true, onNotify);
    if (g_chBulk && g_chBulk->canNotify()) g_chBulk->subscribe(true, onNotify);
    g_cli = c;
    g_mode = MODE_GATT;
    g_job = job;
    g_gattStartMs = millis();
    g_sawNotify = false;
    g_rxLen = g_parseLen = 0;
    g_histPhase = 0;
    g_pvPhase = 0;
    wrHex(g_chCtrl, "fa80ff"); delay(100);
    wrHex(g_chCtrl, "f980");   delay(100);
    wrHex(g_chCmd,  "01");     delay(80);
    wrHex(g_chCmd,  "0300");   delay(100);
    wrHex(g_chCmd,  "060082189342102705008219ec6619ec6503010303");
    delay(250);
    /* Mac vereg extra init — without these, 1050/1051 stayed silent on Feather */
    wrHex(g_chCmd,  "05008119ec7d050081189005008119ec3f05008119ec12");
    delay(250);
    wrHex(g_chBulk, "05008119ec0f05008119ec0e05008119010c0500");
    delay(200);
    wrHex(g_chCtrl, "f941");
    g_histNextMs = millis() + 2000;
    logln(job == JOB_HIST ? "OK gatt hist day0+day1" : "OK gatt pv_burst");
    if (job == JOB_HIST) {
        int y = yesterdayYmd();
        int t = todayYmd();
        if (!y || !g_yday.ok || g_yday.ymd != y)
            memset(&g_yday, 0, sizeof(g_yday));
        if (!t || !g_today.ok || g_today.ymd != t)
            memset(&g_today, 0, sizeof(g_today));
    }
    if (job == JOB_PV) {
        g_havePvV = g_havePvW = g_havePvA = false;
        g_pvV = g_pvWatt = g_pvA = NAN;
    }
    return true;
}

static void histTick() {
    if (g_mode != MODE_GATT || g_job != JOB_HIST) return;
    if (g_yday.ok) { histFinish(); return; }
    if (millis() - g_gattStartMs > 26000) { histFinish(); return; }
    if ((int32_t)(millis() - g_histNextMs) < 0) return;
    if (!g_sawNotify && millis() - g_gattStartMs < 5000) {
        wrHex(g_chCtrl, "f941");
        g_histNextMs = millis() + 500;
        return;
    }
    static const uint16_t yfirst[] = {
        REG_YDAY, REG_YDAY_X, REG_EDD1, REG_EDD0
    };
    static const uint16_t todayb[] = {
        REG_TODAY, REG_TODAY_X, REG_EDD3, REG_EDD2
    };
    if (g_histPhase == 0) {
        getRegs81Concat(yfirst, 4);
        wrHex(g_chCtrl, "f941");
        logln("TX hist81 yday first 1051");
        g_histPhase = 1; g_histNextMs = millis() + 800; return;
    }
    if (g_histPhase == 1) {
        getRegs(yfirst, 4, 0x82);
        logln("TX hist82 yday 1051");
        g_histPhase = 2; g_histNextMs = millis() + 1200; return;
    }
    if (g_histPhase == 2) {
        if (g_yday.ok) { histFinish(); return; }
        getRegs81Concat(todayb, 4);
        g_histPhase = 3; g_histNextMs = millis() + 1500; return;
    }
    g_histPhase = 0;
    histFinish();
}

static void pvTick() {
    if (g_mode != MODE_GATT || g_job != JOB_PV) return;
    if ((int32_t)(millis() - g_histNextMs) < 0) return;
    if (g_pvPhase == 0) { getReg(REG_PV_V); logln("TX pv EDBB"); g_pvPhase = 1; g_histNextMs = millis() + 300; return; }
    if (g_pvPhase == 1) { getReg(REG_PV_W); logln("TX pv EDBC"); g_pvPhase = 2; g_histNextMs = millis() + 300; return; }
    if (g_pvPhase == 2) { getReg(REG_PV_A); logln("TX pv EDBD"); g_pvPhase = 3; g_histNextMs = millis() + 800; return; }
    g_pvPhase = 0;
    g_wantPv = false;
    pubPv();
    logln("STEP pv_done disconnect");
    gattDrop();
}

static void maybeGatt() {
    if (g_mode != MODE_ADV) return;
    if (!g_wantHist && !g_wantPv) return;
    static uint32_t last;
    if (millis() - last < 12000) return;
    last = millis();
    if (!fresh(DEV_MPPT)) { logln("WAIT mppt adv"); return; }
    int job = g_wantHist ? JOB_HIST : JOB_PV;
    gattBurst(job);
}

void setup() {
    Serial.begin(115200);
    delay(200);
    g_bootMs = millis();
    pinMode(PIN_LED, OUTPUT);
    led(true);
    prefs.begin("vtgatt", false);
    recLoad("today", g_today);
    recLoad("yday", g_yday);
    Serial.println("\n=== HUZZAH32 Victron ADV 10s + GATT day0+day1 NTP ===");
    Serial.printf("PIN compile %06lu today=%d/%d yday=%d/%d\n",
                  (unsigned long)g_pin, (int)g_today.ok, g_today.ymd,
                  (int)g_yday.ok, g_yday.ymd);
    Serial.println("cmd: {\"cmd\":\"hist\"} | {\"cmd\":\"pv\"} | {\"cmd\":\"unpair\"}");
    wifiConnect();
    uint32_t t0 = millis();
    while (WiFi.status() != WL_CONNECTED && millis() - t0 < 15000) delay(200);
    Serial.printf("wifi st=%d ip=%s\n", (int)WiFi.status(),
                  WiFi.status() == WL_CONNECTED ? WiFi.localIP().toString().c_str() : "-");
    if (WiFi.status() == WL_CONNECTED) { ntpStart(); mqttConnect(); }
    NimBLEDevice::init("vt-br");
    bleSec();
    bleScanStart();
    logln("STEP scan on");
}

void loop() {
    mqttKeep();
    drainRx();
    if (g_histPubPending) {
        g_histPubPending = false;
        if (g_today.ok) pubDay(g_today, "today", 0);
        if (g_yday.ok) pubDay(g_yday, "yesterday", 1);
    }
    if (g_mode == MODE_GATT && g_job == JOB_HIST && g_yday.ok)
        histFinish();
    publishLive();
    publishStatus();
    if (g_wantUnpair) {
        g_wantUnpair = false;
        gattDrop();
        NimBLEDevice::deleteAllBonds();
        memset(&g_yday, 0, sizeof(g_yday));
        memset(&g_today, 0, sizeof(g_today));
        prefs.remove("yday");
        prefs.remove("today");
        logln("STEP unpair + hist nvs reset");
        bleScanStart();
    }
    if (g_mode == MODE_GATT && millis() - g_gattStartMs > 28000) {
        logln("FAIL gatt timeout");
        if (g_job == JOB_HIST) histFinish();
        else gattDrop();
    }
    maybeGatt();
    histTick();
    pvTick();
}

/*
 * Sunseeker V3 Robot Lawn Mower BLE gateway — Adafruit HUZZAH32 ESP32 Feather
 * WiFi + MQTT + BLE noline_encrypted
 * LoRa / GPS / TFT 없음.
 *
 *   sub  mower/cmd
 *   pub  mower/status  mower/notify  mower/lwt
 *   pub  mower/ble/tx  mower/ble/rx
 *   충전(9)/만충(10)이면 idle pos 1Hz 생략. status 변경은 report_property로 즉시 pub.
 *
 * Arduino IDE
 *   보드: Adafruit ESP32 Feather   (esp32 by Espressif)
 *   Upload Speed: 921600
 *   USB: CP2104  (/dev/cu.SLAB_USBtoUART 또는 /dev/cu.usbserial-*)
 *
 * 라이브러리: NimBLE-Arduino 2.x, PubSubClient, ArduinoJson 7.x
 *
 * BLE
 *   부팅 후 스캔 → 자동 연결. 끊기면 스캔 후 재연결.
 *   disconnect 는 stayDown — 스캔만 유지, 재연결 안 함 (ADV RSSI).
 *   scan / connect 가 stayDown 을 푼다.
 *   handshake/RC 는 명령 올 때만. 연결만으로 예초를 멈추지 않음.
 */

#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <NimBLEDevice.h>
#include <mbedtls/aes.h>
#include <mbedtls/base64.h>
#include <ctype.h>
#include <math.h>

/* WiFi / MQTT 접속 정보는 secrets.h 에 둔다 (git 제외).
 * secrets.h.example 을 secrets.h 로 복사한 뒤 값을 채울 것. */
#if __has_include("secrets.h")
#include "secrets.h"
#else
#error "secrets.h 없음: secrets.h.example 을 secrets.h 로 복사 후 값 입력"
#endif
static const char *MQTT_CLIENT  = "feather-mower-gw";

static const char *TOPIC_CMD    = "mower/cmd";
static const char *TOPIC_STATUS = "mower/status";
static const char *TOPIC_NOTIFY = "mower/notify";
static const char *TOPIC_LWT    = "mower/lwt";
static const char *TOPIC_BLE_TX = "mower/ble/tx";
static const char *TOPIC_BLE_RX = "mower/ble/rx";

#ifndef LED_BUILTIN
#define LED_BUILTIN 13
#endif
static const int PIN_LED  = LED_BUILTIN;
static const int PIN_VBAT = 35;   /* A13, 1/2 divider */

static const uint32_t STREAM_PERIOD_MS = 250;  /* min gap between ctl */
static const uint32_t HOLD_WATCHDOG_MS = 800;  /* 셔틀 펄스만. 패드 홀드는 제외 */
static const uint32_t STATUS_PERIOD_MS = 2000;
static const uint32_t STATUS_DOCK_MS   = 10000; /* 충전중 mqtt heartbeat */
static const uint32_t POSE_PERIOD_MS   = 1000; /* idle only */
static const uint32_t POSE_AFTER_CMD_MS = 150; /* ctl 후 pos 요청 */
static const uint32_t POSE_WAIT_MAX_MS  = 450; /* pos 없으면 다음 ctl */
static const uint32_t ELEC_PERIOD_MS   = 15000;
static const uint32_t ELEC_DOCK_MS     = 120000; /* 충전중 elec */
static const uint32_t MOWST_PERIOD_MS  = 2000;
static const uint32_t MOWST_DOCK_MS    = 20000; /* 충전중 status poll */
static const uint32_t ACK_TIMEOUT_MS   = 2500;
static const uint32_t ADV_FRESH_MS     = 15000;
static const uint16_t BLE_MTU          = 512;
static const uint16_t BLE_ITVL_MIN     = 24;
static const uint16_t BLE_ITVL_MAX     = 40;
static const uint16_t BLE_LATENCY      = 0;
static const uint16_t BLE_SUP_TO       = 800;  /* 8s — WiFi 공존 타임아웃 여유 */

enum SpeedLevel { SPD_SLOW, SPD_MEDIUM, SPD_FAST };
struct SpeedSet { int v; int w; int spin; uint32_t spin_ms; };
static const SpeedSet SPEEDS[3] = {
    {120, 280,  500, 1800},
    {220, 500,  800, 1500},
    {350, 900, 1000, 1200},
};
static SpeedLevel g_speed = SPD_MEDIUM;

/* noline_encrypted AES-256 키: 앱에서 리버스 엔지니어링한 공개 고정 키.
 * github.com/Sdahl1234/Sunseeker-lawn-mower 에 공개된 키와 동일 */
static const uint8_t NOLINE_AES_KEY[32] = {
    0x4E, 0x54, 0xF3, 0xE8, 0xBC, 0xA7, 0x4F, 0xB6,
    0xFD, 0x30, 0xAA, 0xDA, 0x29, 0x31, 0xA3, 0x85,
    0xFB, 0x20, 0xA6, 0xC6, 0xB5, 0xD0, 0x99, 0x2E,
    0xD2, 0xEF, 0x0C, 0x86, 0xBB, 0x83, 0xB5, 0x6B
};

static const char *NUS_SERVICE = "6e400001-b5a3-f393-e0a9-e50e24dcca91";
static const char *NUS_WRITE   = "6e400002-b5a3-f393-e0a9-e50e24dcca93";
static const char *NUS_NOTIFY  = "6e400003-b5a3-f393-e0a9-e50e24dcca95";
static const char *FFF0_WRITE  = "0000abf3-0000-1000-8000-00805f9b34fb";
static const char *FFF0_NOTIFY = "0000abf4-0000-1000-8000-00805f9b34fb";

WiFiClient   wifiClient;
PubSubClient mqtt(wifiClient);

NimBLEClient               *g_ble = nullptr;
NimBLERemoteCharacteristic *g_write = nullptr;
NimBLERemoteCharacteristic *g_notify = nullptr;
NimBLEAddress               g_foundAddr;
String                      g_foundName;
bool                        g_scanHit = false;
bool                        g_foundNotified = false;
bool                        g_scanning = false;
bool                        g_bleReady = false;
bool                        g_rcMode = false;
bool                        g_stayDown = false;   /* disconnect 후 재연결 금지, 스캔은 유지 */
volatile bool               g_bleDrop = false;
volatile int                g_dropReason = 0;
uint32_t                    g_lastWifiTry = 0;
uint8_t                     g_failStreak = 0;
uint32_t                    g_lastReconnect = 0;
uint8_t                     g_reconnectTry = 0;
uint32_t                    g_bootMs = 0;
uint32_t                    g_bleUpMs = 0;

int      g_holdV = 0, g_holdW = 0, g_holdH = 0;
bool     g_holding = false;
uint32_t g_holdUntil = 0;
uint32_t g_lastDriveMs = 0;
bool     g_pulseHold = false;  /* true = 셔틀 ms 펄스, false = 패드 홀드 */
uint32_t g_lastStream = 0, g_lastStatus = 0, g_lastPose = 0, g_lastElec = 0, g_lastMowSt = 0;
uint32_t g_lastNotifyMs = 0, g_lastCtlLogMs = 0;
uint32_t g_lastCmdMs = 0, g_poseDueMs = 0;
bool     g_poseAsked = false;
bool     g_poseAfterCmd = true;  /* last ctl 이후 pos 수신 */
bool     g_bladeOn = false;

int16_t  g_advRssi = 0;
uint32_t g_advMs = 0;

bool     g_poseOk = false;
float    g_poseX = 0, g_poseY = 0, g_poseA = 0;
uint32_t g_poseMs = 0;
int      g_mowStatus = -1;
int      g_mowFault = 0;
int      g_mowElec = -1;
volatile bool g_mowDirty = false;
volatile bool g_stChange = false;
bool     g_gotoOn = false;
float    g_tgtX = 0, g_tgtY = 0, g_arrive = 0.50f;

String g_lastCmd;

#define BLELOG_N   32
#define BLELOG_LEN 300
static volatile uint8_t g_logHead = 0;
static volatile uint8_t g_logTail = 0;
static char g_logDir[BLELOG_N];
static char g_logTxt[BLELOG_N][BLELOG_LEN];

static void handleCmdPayload(const char *payload);
static void publishStatus(const char *extra = nullptr);
static void publishNotify(const char *event, const char *detail);
static bool bleWriteJson(const char *json);
static bool bleRemoteCtl(int v, int w, int h);
static bool bleHandshake();
static void mqttKeep();
static void bleScanStart();
static void bleDestroyClient();

static void led(bool on) { digitalWrite(PIN_LED, on ? HIGH : LOW); }

static float batteryV() {
    int raw = analogRead(PIN_VBAT);
    return raw * (3.3f / 4095.0f) * 2.0f;
}

static bool nameLooksLikeMower(const String &name) {
    return name.startsWith("Wirefree_Mower_") ||
           name.startsWith("Wireless_Mower_") ||
           name.startsWith("Mower_");
}

static int pkcs7Pad(uint8_t *buf, int len, int block = 16) {
    int pad = block - (len % block);
    if (pad == 0) pad = block;
    for (int i = 0; i < pad; i++) buf[len + i] = (uint8_t)pad;
    return len + pad;
}
static int pkcs7Unpad(uint8_t *buf, int len) {
    if (len <= 0 || (len % 16) != 0) return -1;
    int pad = buf[len - 1];
    if (pad < 1 || pad > 16) return -1;
    for (int i = 0; i < pad; i++)
        if (buf[len - 1 - i] != (uint8_t)pad) return -1;
    return len - pad;
}

static bool nolineEncrypt(const char *plain, uint8_t *outB64, size_t outCap, size_t *outLen) {
    size_t plen = strlen(plain);
    if (plen > 480) return false;
    uint8_t padded[512];
    memcpy(padded, plain, plen);
    int clen = pkcs7Pad(padded, (int)plen, 16);
    mbedtls_aes_context aes;
    mbedtls_aes_init(&aes);
    if (mbedtls_aes_setkey_enc(&aes, NOLINE_AES_KEY, 256) != 0) {
        mbedtls_aes_free(&aes);
        return false;
    }
    uint8_t cipher[512];
    for (int off = 0; off < clen; off += 16)
        mbedtls_aes_crypt_ecb(&aes, MBEDTLS_AES_ENCRYPT, padded + off, cipher + off);
    mbedtls_aes_free(&aes);
    size_t olen = 0;
    if (mbedtls_base64_encode(outB64, outCap, &olen, cipher, (size_t)clen) != 0) return false;
    *outLen = olen;
    return true;
}

static bool nolineDecrypt(const uint8_t *in, size_t inLen, char *out, size_t outCap) {
    uint8_t cipher[512];
    size_t clen = 0;
    if (mbedtls_base64_decode(cipher, sizeof(cipher), &clen, in, inLen) != 0) return false;
    if (clen == 0 || (clen % 16) != 0 || clen > 512) return false;
    mbedtls_aes_context aes;
    mbedtls_aes_init(&aes);
    if (mbedtls_aes_setkey_dec(&aes, NOLINE_AES_KEY, 256) != 0) {
        mbedtls_aes_free(&aes);
        return false;
    }
    uint8_t plain[512];
    for (size_t off = 0; off < clen; off += 16)
        mbedtls_aes_crypt_ecb(&aes, MBEDTLS_AES_DECRYPT, cipher + off, plain + off);
    mbedtls_aes_free(&aes);
    int ulen = pkcs7Unpad(plain, (int)clen);
    if (ulen < 0 || (size_t)ulen + 1 > outCap) return false;
    memcpy(out, plain, (size_t)ulen);
    out[ulen] = 0;
    return true;
}

static bool logLooksCtlOnly(const char *text) {
    if (!text || !strstr(text, "remote_ctl")) return false;
    if (strstr(text, "robot_pos")) return false;
    if (strstr(text, "report_property")) return false;
    if (strstr(text, "report_event")) return false;
    return true;
}

static void bleLogPush(char dir, const char *text) {
    if (logLooksCtlOnly(text)) {
        uint32_t now = millis();
        if (now - g_lastCtlLogMs < 1000) return;
        g_lastCtlLogMs = now;
    }
    uint8_t next = (uint8_t)((g_logHead + 1) % BLELOG_N);
    if (next == g_logTail) g_logTail = (uint8_t)((g_logTail + 1) % BLELOG_N);
    g_logDir[g_logHead] = dir;
    strncpy(g_logTxt[g_logHead], text ? text : "", BLELOG_LEN - 1);
    g_logTxt[g_logHead][BLELOG_LEN - 1] = 0;
    g_logHead = next;
}

static void bleLogFlush(int maxN = 16) {
    if (!mqtt.connected()) return;
    int n = 0;
    while (g_logTail != g_logHead && n < maxN) {
        uint8_t i = g_logTail;
        const char *topic = (g_logDir[i] == 'T') ? TOPIC_BLE_TX : TOPIC_BLE_RX;
        mqtt.publish(topic, g_logTxt[i], false);
        g_logTail = (uint8_t)((g_logTail + 1) % BLELOG_N);
        n++;
    }
}

static void mqttPub(const char *topic, const char *body, bool retain = false) {
    if (!mqtt.connected()) return;
    mqtt.publish(topic, body, retain);
}

static void onMqtt(char *topic, byte *message, unsigned int length) {
    if (strcmp(topic, TOPIC_CMD) != 0) return;
    char buf[400];
    if (length >= sizeof(buf) - 1) length = sizeof(buf) - 1;
    memcpy(buf, message, length);
    buf[length] = 0;
    handleCmdPayload(buf);
}

static void mqttConnect() {
    mqtt.setServer(MQTT_HOST, MQTT_PORT);
    mqtt.setCallback(onMqtt);
    mqtt.setBufferSize(1024);
    mqtt.setKeepAlive(30);
    mqtt.setSocketTimeout(4);
    if (!mqtt.connect(MQTT_CLIENT, MQTT_USER, MQTT_PASS, TOPIC_LWT, 0, true, "offline")) {
        Serial.printf("[mqtt] fail rc=%d\n", mqtt.state());
        return;
    }
    mqtt.publish(TOPIC_LWT, "online", true);
    mqtt.subscribe(TOPIC_CMD, 0);
    Serial.printf("[mqtt] ok %s\n", WiFi.localIP().toString().c_str());
}

static void wifiConnect(uint32_t waitMs = 20000) {
    WiFi.persistent(false);
    WiFi.mode(WIFI_STA);
    WiFi.setSleep(false);
    WiFi.setHostname(MQTT_CLIENT);
    WiFi.begin(WIFI_SSID, WIFI_PASS);
    Serial.printf("[wifi] %s\n", WIFI_SSID);
    uint32_t t0 = millis();
    while (WiFi.status() != WL_CONNECTED && millis() - t0 < waitMs) {
        delay(250);
        led((millis() / 250) & 1);
        yield();
    }
    led(WiFi.status() == WL_CONNECTED);
    if (WiFi.status() == WL_CONNECTED)
        Serial.printf("[wifi] %s\n", WiFi.localIP().toString().c_str());
    else
        Serial.println("[wifi] FAIL");
}

static void mqttKeep() {
    if (WiFi.status() != WL_CONNECTED) {
        if (millis() - g_lastWifiTry < 5000) return;
        g_lastWifiTry = millis();
        wifiConnect(8000);
        return;
    }
    if (!mqtt.connected()) {
        if (millis() - g_lastWifiTry < 3000) return;
        g_lastWifiTry = millis();
        mqttConnect();
        return;
    }
    mqtt.loop();
    bleLogFlush(g_holding ? 2 : 16);
}

static void publishStatus(const char *extra) {
    if (!mqtt.connected()) return;
    String cmd = g_lastCmd;
    cmd.replace("\"", "'");
    cmd.replace("\n", " ");
    if (cmd.length() > 32) cmd = cmd.substring(0, 32);
    const char *mode = g_bleReady ? "ble_local" : (g_scanning ? "scan" : "idle");
    bool advFresh = g_advMs && (millis() - g_advMs) < ADV_FRESH_MS;
    char buf[560];
    snprintf(buf, sizeof(buf),
             "{\"ble\":%s,\"rc\":%s,\"mower\":\"%s\",\"holding\":%s,"
             "\"v\":%d,\"w\":%d,\"last_cmd\":\"%s\","
             "\"wifi_rssi\":%d,\"ble_rssi\":%d,\"ble_rssi_ok\":%s,"
             "\"adv_rssi\":%d,\"adv_fresh\":%s,\"heard_ms\":%lu,"
             "\"ip\":\"%s\",\"board\":\"feather-gw\",\"mode\":\"%s\","
             "\"found\":%s,\"stay_down\":%s,\"vbat\":%.2f,\"hz\":4,"
             "\"s\":%d,\"elec\":%d%s%s%s}",
             g_bleReady ? "true" : "false",
             g_rcMode ? "true" : "false",
             g_foundName.c_str(),
             g_holding ? "true" : "false",
             g_holdV, g_holdW, cmd.c_str(),
             WiFi.RSSI(),
             (g_bleReady && g_ble) ? g_ble->getRssi() : 0,
             (g_bleReady && g_ble) ? "true" : "false",
             g_advMs ? (int)g_advRssi : 0,
             advFresh ? "true" : "false",
             (unsigned long)(g_advMs ? (millis() - g_advMs) : 0),
             WiFi.isConnected() ? WiFi.localIP().toString().c_str() : "",
             mode,
             g_scanHit ? "true" : "false",
             g_stayDown ? "true" : "false",
             batteryV(),
             g_mowStatus,
             g_mowElec,
             extra && extra[0] ? ",\"msg\":\"" : "",
             extra && extra[0] ? extra : "",
             extra && extra[0] ? "\"" : "");
    mqtt.publish(TOPIC_STATUS, buf, false);
}

static void publishNotify(const char *event, const char *detail) {
    char buf[220];
    snprintf(buf, sizeof(buf),
             "{\"event\":\"%s\",\"mower\":\"%s\",\"addr\":\"%s\",\"ble\":%s,\"rc\":%s%s%s%s}",
             event ? event : "",
             g_foundName.c_str(),
             g_foundAddr.toString().c_str(),
             g_bleReady ? "true" : "false",
             g_rcMode ? "true" : "false",
             detail && detail[0] ? ",\"detail\":\"" : "",
             detail && detail[0] ? detail : "",
             detail && detail[0] ? "\"" : "");
    mqttPub(TOPIC_NOTIFY, buf);
    Serial.printf("[notify] %s\n", buf);
}

class ScanCB : public NimBLEScanCallbacks {
    void onResult(const NimBLEAdvertisedDevice *dev) override {
        String name = String(dev->getName().c_str());
        bool nameOk = nameLooksLikeMower(name);
        bool addrOk = g_scanHit && (dev->getAddress() == g_foundAddr);
        if (!nameOk && !addrOk) return;
        if (nameOk) {
            g_foundName = name;
            g_foundAddr = dev->getAddress();
            g_scanHit = true;
        }
        g_advRssi = (int16_t)dev->getRSSI();
        g_advMs = millis();
    }
};
class ClientCB : public NimBLEClientCallbacks {
    void onDisconnect(NimBLEClient *, int reason) override {
        Serial.printf("[ble] drop reason=%d\n", reason);
        g_bleReady = false;
        g_rcMode = false;
        g_holding = false;
        g_write = nullptr;
        g_notify = nullptr;
        g_bleDrop = true;
        g_dropReason = reason;
    }
};
static ScanCB   g_scanCb;
static ClientCB g_clientCb;

static void takeElec(JsonVariant dat) {
    if (dat.isNull() || dat["elec"].isNull()) return;
    if (dat["elec"].is<JsonObject>()) {
        JsonVariant e = dat["elec"];
        if (!e["level"].isNull()) g_mowElec = (int)(e["level"] | g_mowElec);
        else if (!e["percent"].isNull()) g_mowElec = (int)(e["percent"] | g_mowElec);
        else if (!e["value"].isNull()) g_mowElec = (int)(e["value"] | g_mowElec);
        else if (!e["soc"].isNull()) g_mowElec = (int)(e["soc"] | g_mowElec);
    } else {
        g_mowElec = (int)(dat["elec"] | g_mowElec);
    }
}

static void onNotify(NimBLERemoteCharacteristic *, uint8_t *raw, size_t length, bool) {
    char text[512];
    if (!nolineDecrypt(raw, length, text, sizeof(text))) return;
    g_lastNotifyMs = millis();
    bleLogPush('R', text);
    JsonDocument d;
    if (deserializeJson(d, text)) return;
    JsonVariant dat = d["data"];
    if (!dat.isNull()) {
        int prevS = g_mowStatus;
        int prevF = g_mowFault;
        int prevE = g_mowElec;
        if (!dat["status"].isNull()) g_mowStatus = (int)(dat["status"] | -1);
        if (!dat["fault"].isNull()) g_mowFault = (int)(dat["fault"] | 0);
        takeElec(dat);
        if (g_mowStatus != prevS || g_mowFault != prevF || g_mowElec != prevE)
            g_mowDirty = true;
    }
    if (dat.isNull() || dat["robot_pos"].isNull()) return;
    JsonVariant rp = dat["robot_pos"];
    if (rp.isNull() || !rp["point"].is<JsonArray>()) return;
    JsonArray pt = rp["point"];
    if (pt.size() < 2) return;
    g_poseX = (float)pt[0];
    g_poseY = (float)pt[1];
    g_poseA = (float)(rp["angle"] | 0.0);
    g_poseMs = millis();
    g_poseOk = true;
    /* 명령 직후(80ms+) 온 pos만 이 명령의 결과로 본다 */
    if (!g_lastCmdMs || (int32_t)(g_poseMs - g_lastCmdMs) >= 80)
        g_poseAfterCmd = true;
}

static void bleScanStop() {
    NimBLEScan *s = NimBLEDevice::getScan();
    if (s) s->stop();
    g_scanning = false;
}

static void bleScanStart() {
    if (g_bleReady || g_scanning) return;
    Serial.println("[ble] scan start");
    NimBLEScan *scan = NimBLEDevice::getScan();
    scan->setScanCallbacks(&g_scanCb, false);
    scan->setActiveScan(true);
    scan->setInterval(160);
    scan->setWindow(120);
    scan->setDuplicateFilter(false);
    scan->start(0, false);
    g_scanning = true;
}

static NimBLERemoteCharacteristic *findChar(NimBLEClient *c, const char *uuid) {
    NimBLEUUID u(uuid);
    auto svcs = c->getServices(false);
    if (svcs.empty()) svcs = c->getServices(true);
    for (auto *svc : svcs) {
        if (!svc) continue;
        auto *ch = svc->getCharacteristic(u);
        if (ch) return ch;
    }
    return nullptr;
}

static void bleDestroyClient() {
    g_write = nullptr;
    g_notify = nullptr;
    g_bleReady = false;
    g_rcMode = false;
    if (!g_ble) return;
    if (g_ble->isConnected()) g_ble->disconnect();
    NimBLEDevice::deleteClient(g_ble);
    g_ble = nullptr;
}

static bool bleConnect() {
    bleScanStop();
    delay(80);
    if (!g_scanHit) {
        bleScanStart();
        return false;
    }
    Serial.printf("[ble] connect %s @ %s\n",
                  g_foundName.c_str(), g_foundAddr.toString().c_str());
    bleDestroyClient();
    g_ble = NimBLEDevice::createClient();
    g_ble->setClientCallbacks(&g_clientCb, false);
    g_ble->setConnectTimeout(20000);
    g_ble->setConnectRetries(3);
    g_ble->setConnectionParams(BLE_ITVL_MIN, BLE_ITVL_MAX, BLE_LATENCY, BLE_SUP_TO, 80, 40);
    if (!g_ble->connect(g_foundAddr, true, false, true)) {
        Serial.printf("[ble] connect fail err=%d\n", g_ble->getLastError());
        bleDestroyClient();
        g_failStreak++;
        if (g_failStreak >= 5) {
            NimBLEDevice::deinit(true);
            delay(200);
            NimBLEDevice::init("");
            NimBLEDevice::setPower(ESP_PWR_LVL_P9);
            NimBLEDevice::setMTU(BLE_MTU);
            g_failStreak = 0;
        }
        return false;
    }
    g_ble->updateConnParams(BLE_ITVL_MIN, BLE_ITVL_MAX, BLE_LATENCY, BLE_SUP_TO);
    g_ble->discoverAttributes();
    g_write  = findChar(g_ble, NUS_WRITE);
    g_notify = findChar(g_ble, NUS_NOTIFY);
    if (!g_write || !g_notify) {
        g_write  = findChar(g_ble, FFF0_WRITE);
        g_notify = findChar(g_ble, FFF0_NOTIFY);
    }
    if (!g_write || !g_notify) {
        Serial.println("[ble] GATT fail");
        bleDestroyClient();
        return false;
    }
    if (!g_notify->subscribe(true, onNotify, true))
        Serial.println("[ble] subscribe fail");
    g_bleReady = true;
    g_rcMode = false;
    g_failStreak = 0;
    g_stayDown = false;
    g_lastNotifyMs = millis();
    g_bleUpMs = millis();
    g_lastPose = millis();   /* 연결 직후 pose 폭주 방지 */
    g_lastElec = millis();
    Serial.println("[ble] ready");
    return true;
}

static bool bleWriteJson(const char *json) {
    if (!g_bleReady || !g_write) return false;
    uint8_t b64[700];
    size_t n = 0;
    if (!nolineEncrypt(json, b64, sizeof(b64), &n)) return false;
    if (!g_write->writeValue(b64, n, false)) {
        g_bleReady = false;
        g_bleDrop = true;
        g_dropReason = -1;
        return false;
    }
    bleLogPush('T', json);
    return true;
}

static bool bleRemoteCtl(int v, int w, int h) {
    char buf[240];
    snprintf(buf, sizeof(buf),
             "{\"id\":\"remoteControl\",\"method\":\"action\","
             "\"params\":{\"cmd\":\"remote_ctl\",\"v\":%d,\"w\":%d,"
             "\"b\":{\"h\":%d,\"s\":%d}}}",
             v, w, h, g_bladeOn ? 1 : 0);
    bool ok = bleWriteJson(buf);
    uint32_t now = millis();
    g_lastStream = now;
    g_lastCmdMs = now;
    g_poseDueMs = now + POSE_AFTER_CMD_MS;
    g_poseAsked = false;
    if (v != 0 || w != 0) g_poseAfterCmd = false;
    else g_poseAfterCmd = true;
    return ok;
}

static bool bleHandshake() {
    if (!g_bleReady) return false;
    const char *steps[] = {
        "{\"id\":\"GetFCState\",\"method\":\"get_property\",\"params\":{\"key\":\"getfc_state\"}}",
        "{\"id\":\"getDevStatus\",\"method\":\"get_property\",\"params\":{\"key\":\"status\"}}",
        "{\"id\":\"getDevFault\",\"method\":\"get_property\",\"params\":{\"key\":\"fault\"}}",
        "{\"id\":\"getDevElec\",\"method\":\"get_property\",\"params\":{\"key\":\"elec\"}}",
        "{\"id\":\"getBuildMapStatus\",\"method\":\"get_property\",\"params\":{\"key\":\"build_map_status\"}}",
    };
    for (auto *s : steps) {
        bleWriteJson(s);
        delay(120);
        mqttKeep();
    }
    bleWriteJson("{\"id\":\"remoteControlMap\",\"method\":\"action\","
                 "\"params\":{\"cmd\":\"remote_ctl\",\"v\":-1,\"w\":0}}");
    delay(400);
    mqttKeep();
    g_rcMode = true;
    return true;
}

static void bleDisconnect() {
    g_holding = false;
    g_gotoOn = false;
    g_rcMode = false;
    if (g_ble && g_ble->isConnected() && g_write) {
        bleWriteJson("{\"id\":\"bleDisconnect\",\"method\":\"action\","
                     "\"params\":{\"cmd\":\"ble_disconnect\"}}");
        delay(80);
    }
    bleDestroyClient();
}

static void startHold(int v, int w, uint32_t ms, const char *label) {
    if (!g_bleReady) {
        publishNotify("ble_not_connected", "send connect first");
        return;
    }
    if (!g_rcMode) bleHandshake();
    g_gotoOn = false;
    g_holdV = v;
    g_holdW = w;
    g_holdH = (v == 0 && w == 0) ? 0 : 100;
    g_holding = true;
    if (ms == 0) {
        g_holdUntil = 0;
        g_pulseHold = false;
    } else {
        if (ms > 2000) ms = 2000;
        g_holdUntil = millis() + ms;
        g_pulseHold = true;
    }
    g_lastDriveMs = millis();
    g_lastStream = 0;
    g_lastCmd = label;
    g_poseAfterCmd = true;
    bleRemoteCtl(v, w, g_holdH);
}

static void stopDrive() {
    g_holding = false;
    g_gotoOn = false;
    g_bladeOn = false;
    g_holdUntil = 0;
    g_lastDriveMs = 0;
    g_pulseHold = false;
    g_holdV = 0;
    g_holdW = 0;
    g_holdH = 0;
    g_lastCmd = "stop";
    if (g_bleReady) bleRemoteCtl(0, 0, 0);
}

static float wrapPi(float a) {
    while (a >  3.14159265f) a -= 6.2831853f;
    while (a < -3.14159265f) a += 6.2831853f;
    return a;
}

static bool poseReadyForNextCtl() {
    if (g_holdV == 0 && g_holdW == 0) return true;
    if (g_poseAfterCmd) return true;
    if (g_lastCmdMs && (millis() - g_lastCmdMs) >= POSE_WAIT_MAX_MS) return true;
    return false;
}

static bool dockChargeIdle() {
    /* 9 충전 10 만충. RC/goto/홀드 중이면 폴링 유지 */
    if (g_holding || g_gotoOn || g_rcMode) return false;
    return g_mowStatus == 9 || g_mowStatus == 10;
}

static void requestPose() {
    if (!g_bleReady) return;
    g_lastPose = millis();
    g_poseAsked = true;
    bleWriteJson("{\"id\":\"mapNone\",\"method\":\"get_property\",\"params\":{\"key\":\"robot_pos\"}}");
}

static void gotoTick() {
    if (!g_gotoOn || !g_bleReady) return;
    if (!g_rcMode) bleHandshake();
    uint32_t now = millis();
    if (!g_poseOk || (now - g_poseMs) > 2500) {
        if (now - g_lastPose >= 300) requestPose();
        return;
    }
    /* 직전 명령 결과 pos 오기 전에는 각도 다시 계산하지 않음 */
    if (!poseReadyForNextCtl()) return;
    float dx = g_tgtX - g_poseX, dy = g_tgtY - g_poseY;
    float dist = sqrtf(dx * dx + dy * dy);
    if (dist <= g_arrive) {
        stopDrive();
        publishStatus("arrived");
        publishNotify("arrived", "");
        return;
    }
    float bear = atan2f(-dx, dy);
    float err = wrapPi(bear - g_poseA);
    const SpeedSet &sp = SPEEDS[g_speed];
    const float ALIGN = 0.28f;
    int v = 0, w = 0;
    if (fabsf(err) > ALIGN) {
        int spin = SPEEDS[SPD_SLOW].w;
        if (spin < 160) spin = 160;
        if (spin > 280) spin = 280;
        w = (err > 0) ? spin : -spin;
        v = 0;
    } else {
        v = sp.v;
        w = (int)(err / ALIGN * 120.0f);
        if (w > 120) w = 120;
        if (w < -120) w = -120;
    }
    g_holdV = v;
    g_holdW = w;
    g_holdH = 100;
    g_holding = true;
    g_holdUntil = 0;
    g_lastCmd = "goto";
    if (now - g_lastStream >= STREAM_PERIOD_MS) {
        bleRemoteCtl(v, w, 100);
    }
}

static void handleCmdObject(JsonVariantConst obj) {
    const char *cmd = obj["cmd"] | obj["command"] | "";
    String c = String(cmd);
    c.toLowerCase();

    int v = obj["v"] | 0;
    int w = obj["w"] | 0;
    bool hasV = !obj["v"].isNull();
    bool hasW = !obj["w"].isNull();
    uint32_t ms = 0;
    bool hasDur = false;
    if (!obj["ms"].isNull()) {
        ms = (uint32_t)(obj["ms"] | 0);
        hasDur = true;
    } else if (!obj["seconds"].isNull()) {
        ms = (uint32_t)(((float)obj["seconds"]) * 1000.0f);
        hasDur = true;
    }
    SpeedLevel lvl = g_speed;
    {
        String sl = "";
        if (!obj["speed"].isNull()) sl = String((const char *)(obj["speed"] | ""));
        else if (!obj["level"].isNull()) sl = String((const char *)(obj["level"] | ""));
        sl.toLowerCase();
        if (sl == "slow") lvl = SPD_SLOW;
        else if (sl == "fast") lvl = SPD_FAST;
        else if (sl == "medium" || sl == "mid") lvl = SPD_MEDIUM;
    }
    const SpeedSet &sp = SPEEDS[lvl];
    auto clampW = [](int x, int lim) {
        if (x > lim) return lim;
        if (x < -lim) return -lim;
        return x;
    };

    if (c == "bridge" || c == "link" || c == "bridge_ble" || c == "bble") {
        publishStatus("no_lora");
        return;
    }
    if (c == "scan") {
        g_stayDown = false;
        bleDisconnect();
        g_scanHit = false;
        g_foundNotified = false;
        bleScanStart();
        publishStatus("scan");
        publishNotify("scan", "");
        return;
    }
    if (c == "connect") {
        g_stayDown = false;
        if (g_bleReady) {
            publishStatus("connected");
            return;
        }
        bool ok = bleConnect();
        publishStatus(ok ? "connected" : "connect_fail");
        publishNotify(ok ? "connected" : "connect_fail", g_foundName.c_str());
        if (!ok) bleScanStart();
        return;
    }
    if (c == "disconnect") {
        g_stayDown = true;
        stopDrive();
        bleDisconnect();
        bleScanStart();
        publishStatus("disconnected");
        publishNotify("disconnected", "");
        return;
    }
    if (c == "handshake") {
        bleHandshake();
        publishStatus("handshake");
        return;
    }
    if (c == "status") { publishStatus("ok"); return; }
    if (c == "blade") {
        bool on = false;
        if (!obj["on"].isNull()) on = (bool)(obj["on"] | false);
        else {
            String s = String(obj["level"] | "");
            s.toLowerCase();
            on = (s == "on" || s == "1" || s == "true");
        }
        g_bladeOn = on;
        if (!g_rcMode) bleHandshake();
        bleRemoteCtl(g_holding ? g_holdV : 0, g_holding ? g_holdW : 0, g_holding ? 100 : 0);
        g_lastCmd = on ? "blade_on" : "blade_off";
        publishStatus(g_lastCmd.c_str());
        return;
    }
    if (c == "speed") {
        String s = String(obj["level"] | "");
        s.toLowerCase();
        if (s == "slow") g_speed = SPD_SLOW;
        else if (s == "fast") g_speed = SPD_FAST;
        else g_speed = SPD_MEDIUM;
        publishStatus("speed");
        return;
    }
    if (c == "pause") {
        stopDrive();
        bleWriteJson("{\"id\":\"pauseWork\",\"method\":\"action\",\"params\":{\"cmd\":\"pause\"}}");
        g_lastCmd = "pause";
        publishStatus("pause");
        return;
    }
    if (c == "start") {
        stopDrive();
        bleWriteJson("{\"id\":\"startWork\",\"method\":\"action\",\"params\":{\"cmd\":\"start\"}}");
        g_lastCmd = "start";
        publishStatus("start");
        return;
    }
    if (c == "stop" || c == "0") {
        /* 복귀(7)/작업(2): remoteControl v=0 은 ACK 나와도 계속 움직임.
           remoteControlMap v=-1 이면 7→11→8. 충전(9/10)은 언독 안 함. */
        if (g_bleReady && !g_rcMode && g_mowStatus != 9 && g_mowStatus != 10) {
            bleWriteJson("{\"id\":\"remoteControlMap\",\"method\":\"action\","
                         "\"params\":{\"cmd\":\"remote_ctl\",\"v\":-1,\"w\":0}}");
            delay(150);
            mqttKeep();
            g_rcMode = true;
        }
        stopDrive();
        publishStatus("stop");
        return;
    }
    if (c == "goto") {
        if (obj["x"].isNull() || obj["y"].isNull()) { publishStatus("goto_bad"); return; }
        g_tgtX = (float)(obj["x"] | 0.0);
        g_tgtY = (float)(obj["y"] | 0.0);
        if (!obj["arrive"].isNull()) g_arrive = (float)(obj["arrive"] | 0.50);
        if (g_arrive < 0.10f) g_arrive = 0.10f;
        g_gotoOn = true;
        g_holding = true;
        g_holdUntil = 0;
        g_lastCmd = "goto";
        if (!g_rcMode) bleHandshake();
        publishStatus("goto");
        return;
    }
    if (c == "home" || c == "dock" || c == "charger") {
        stopDrive();
        bleWriteJson("{\"id\":\"startFindCharger\",\"method\":\"action\","
                     "\"params\":{\"cmd\":\"start_find_charger\"}}");
        g_lastCmd = "home";
        publishStatus("home");
        return;
    }
    if (c == "fwd" || c == "forward" || c == "1") {
        startHold(hasV ? v : sp.v, hasW ? w : 0, hasDur ? ms : 0, "FWD");
        return;
    }
    if (c == "rev" || c == "back" || c == "reverse" || c == "2") {
        startHold(hasV ? v : -sp.v, hasW ? w : 0, hasDur ? ms : 0, "REV");
        return;
    }
    /* 좌우는 명령에 붙은 speed/v/w 우선. 기본은 slow.w (280).
       medium 전역 속도여도 follow 의 w=±120 을 500 으로 덮지 않음. */
    if (c == "left" || c == "3") {
        int ww = hasW ? w : SPEEDS[SPD_SLOW].w;
        startHold(hasV ? v : 0, clampW(ww, 320), hasDur ? ms : 0, "LEFT");
        return;
    }
    if (c == "right" || c == "4") {
        int ww = hasW ? w : -SPEEDS[SPD_SLOW].w;
        if (!hasW) ww = -SPEEDS[SPD_SLOW].w;
        else if (ww > 0) ww = -ww;
        startHold(hasV ? v : 0, clampW(ww, 320), hasDur ? ms : 0, "RIGHT");
        return;
    }
    if (c == "spin" || c == "turn" || c == "5") {
        startHold(0, hasW ? clampW(w, 500) : sp.spin, hasDur ? ms : sp.spin_ms, "SPIN");
        return;
    }
    if (c == "drive") { startHold(v, w, hasDur ? ms : 0, "DRIVE"); return; }
    if (c == "hold") { startHold(v, w, 0, "HOLD"); return; }
    Serial.printf("[cmd] unknown %s\n", c.c_str());
}

static bool cmdNeedsBle(const char *payload) {
    String s = String(payload);
    s.trim();
    s.toLowerCase();
    if (s.startsWith("{")) {
        JsonDocument doc;
        if (!deserializeJson(doc, payload) && doc.is<JsonObject>()) {
            s = String(doc["cmd"] | doc["command"] | "");
            s.toLowerCase();
        }
    }
    if (s == "scan" || s == "disconnect" || s == "status" || s == "speed" ||
        s == "connect" || s == "bridge" || s == "link" || s == "bridge_ble" || s == "bble")
        return false;
    return true;
}

static void handleCmdPayload(const char *payload) {
    Serial.printf("[mqtt] cmd %s\n", payload);
    if (cmdNeedsBle(payload) && !g_bleReady) {
        publishStatus("ble_not_connected");
        publishNotify("ble_not_connected", "send connect first");
        return;
    }
    JsonDocument doc;
    if (!deserializeJson(doc, payload) && doc.is<JsonObject>()) {
        handleCmdObject(doc.as<JsonObjectConst>());
        return;
    }
    String s = String(payload);
    s.trim();
    s.toLowerCase();
    JsonDocument wrap;
    wrap["cmd"] = s;
    handleCmdObject(wrap.as<JsonObjectConst>());
}

void setup() {
    Serial.begin(115200);
    delay(200);
    g_bootMs = millis();
    pinMode(PIN_LED, OUTPUT);
    led(true);

    Serial.println("\n=== mower GW  Adafruit ESP32 Feather  WiFi+BLE (no LoRa) ===");

    wifiConnect();
    if (WiFi.status() == WL_CONNECTED) mqttConnect();

    NimBLEDevice::init("");
    NimBLEDevice::setPower(ESP_PWR_LVL_P9);
    NimBLEDevice::setMTU(BLE_MTU);
    bleScanStart();
}

void loop() {
    mqttKeep();
    uint32_t now = millis();

    if (g_stChange) {
        g_stChange = false;
        publishStatus("st_change");
    }

    if (g_bleDrop) {
        char why[20];
        snprintf(why, sizeof(why), "ble_drop_%d", g_dropReason);
        g_bleDrop = false;
        bleDestroyClient();
        publishStatus(why);
        publishNotify("ble_drop", why);
        g_lastReconnect = now;
        bleScanStart();
    }

    if (!g_bleReady) {
        if (!g_scanning) bleScanStart();
        if (g_scanHit && !g_foundNotified) {
            g_foundNotified = true;
            publishStatus("found");
            publishNotify("found", g_foundName.c_str());
        }
        if (!g_stayDown) {
            uint32_t waitMs = (g_reconnectTry >= 3) ? 8000 : (g_reconnectTry >= 1 ? 4000 : 2000);
            if (g_scanHit && (now - g_lastReconnect) >= waitMs) {
                g_lastReconnect = now;
                g_reconnectTry++;
                if (bleConnect()) {
                    g_reconnectTry = 0;
                    publishStatus("connected");
                    publishNotify("connected", g_foundName.c_str());
                }
            }
        }
    } else {
        g_reconnectTry = 0;
    }

    if (g_bleReady && g_holding && g_lastNotifyMs &&
        (now - g_lastNotifyMs) > ACK_TIMEOUT_MS) {
        g_bleDrop = true;
        g_dropReason = -2;
    }

    if (g_bleReady && g_poseDueMs && !g_poseAsked &&
        (int32_t)(now - g_poseDueMs) >= 0) {
        if (!g_holding || (now - g_lastStream) > 80) requestPose();
        else g_poseDueMs = now + 40;
    }

    if (g_gotoOn && g_bleReady) {
        gotoTick();
    } else if (g_holding && g_bleReady) {
        if ((g_holdUntil != 0 && (int32_t)(now - g_holdUntil) >= 0) ||
            (g_pulseHold && g_lastDriveMs && (now - g_lastDriveMs) > HOLD_WATCHDOG_MS)) {
            stopDrive();
            publishStatus("hold_timeout");
        } else if (poseReadyForNextCtl() && now - g_lastStream >= STREAM_PERIOD_MS) {
            bleRemoteCtl(g_holdV, g_holdW, g_holdH);
        }
    }

    if (g_bleReady && g_bleUpMs && (now - g_bleUpMs) >= 1500 &&
        !g_holding && !g_gotoOn && !dockChargeIdle()) {
        if (now - g_lastPose >= POSE_PERIOD_MS) requestPose();
    }

    uint32_t elecWait = dockChargeIdle() ? ELEC_DOCK_MS : ELEC_PERIOD_MS;
    if (g_bleReady && now - g_lastElec >= elecWait) {
        if (!g_holding || (now - g_lastStream) > 80) {
            g_lastElec = now;
            bleWriteJson("{\"id\":\"getDevElec\",\"method\":\"get_property\",\"params\":{\"key\":\"elec\"}}");
        }
    }

    uint32_t stWait = dockChargeIdle() ? MOWST_DOCK_MS : MOWST_PERIOD_MS;
    if (g_bleReady && g_bleUpMs && (now - g_bleUpMs) >= 1500 &&
        now - g_lastMowSt >= stWait) {
        if (!g_holding || (now - g_lastStream) > 80) {
            g_lastMowSt = now;
            bleWriteJson("{\"id\":\"getDevStatus\",\"method\":\"get_property\",\"params\":{\"key\":\"status\"}}");
        }
    }

    if (g_mowDirty) {
        g_mowDirty = false;
        g_lastStatus = now;
        publishStatus("mow");
    }

    uint32_t mqttSt = dockChargeIdle() ? STATUS_DOCK_MS : STATUS_PERIOD_MS;
    if (now - g_lastStatus >= mqttSt) {
        g_lastStatus = now;
        publishStatus();
        led(g_bleReady || (g_advMs && (now - g_advMs) < ADV_FRESH_MS));
    }
}

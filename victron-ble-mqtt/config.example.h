#pragma once
/* Copy this file to config.h and fill in local values.
 * config.h must NOT be committed.
 */

static const char *WIFI_SSID    = "YOUR_WIFI_SSID";
static const char *WIFI_PASS    = "YOUR_WIFI_PASSWORD";
static const char *MQTT_HOST    = "YOUR_MQTT_HOST";
static const uint16_t MQTT_PORT = 1883;
static const char *MQTT_USER    = "YOUR_MQTT_USER";
static const char *MQTT_PASS    = "YOUR_MQTT_PASSWORD";
static const char *MQTT_CLIENT  = "victron-gatt-bridge";

#define VICTRON_PIN 000000u

/* Instant Readout: MAC is 12 lowercase hex chars, no colons.
 * AES-128 key: copy from VictronConnect
 *   Settings → ⋮ → Product Info → Instant Readout Details → Show
 * One 16-byte key per device. Do not commit real keys.
 */
struct VictronDev {
    const char *id, *label, *macHex;
    uint8_t key[16];
};

#ifndef VICTRON_DEV_DEFINED
#define VICTRON_DEV_DEFINED
enum { DEV_MPPT = 0, DEV_SENSE = 1, DEV_N = 2 };
static VictronDev g_dev[DEV_N] = {
    { "mppt", "YOUR_MPPT_SN", "aabbccddeeff",
      { 0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00 } },
    { "sense", "YOUR_SENSE_SN", "112233445566",
      { 0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00 } }
};
#endif

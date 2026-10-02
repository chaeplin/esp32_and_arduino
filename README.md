# esp32_and_arduino

ESP32 / Arduino sketches and local bridges. More projects will be added as folders under this repo.

## Projects

<table>
<tr>
<td>

### [victron-ble-mqtt](https://github.com/chaeplin/esp32_and_arduino/tree/master/victron-ble-mqtt)

Local ESP32 (Adafruit HUZZAH32) bridge for Victron Instant Readout.

Decrypts BLE advertisements, publishes MQTT JSON, and opens short GATT sessions for **daily history (up to 30 days)** and PV registers.

Built with [SuperGrok](https://grok.com) / Grokbot from captured ADV / GATT packets and Feather serial logs. Not an official Victron SDK.

[README](./victron-ble-mqtt/README.md) · MIT

</td>
</tr>
</table>

<table>
<tr>
<td>

### [sunseeker-ble](https://github.com/chaeplin/esp32_and_arduino/tree/master/sunseeker-ble)

ESP32 Feather BLE gateway for a Sunseeker V3 Plus robot mower.

The board talks to the mower over BLE (AES-256-ECB, Base64 JSON) and bridges to MQTT. Python serves a control pad (`:8765`) and a live position map (`:8767`, optional InfluxDB track).

[README](./sunseeker-ble/README.md) · MIT

</td>
</tr>
</table>

<!--
Add the next project by copying this block:

<table>
<tr>
<td>

### [name](https://github.com/chaeplin/esp32_and_arduino/tree/master/name)

One-paragraph description.

[README](./name/README.md)

</td>
</tr>
</table>
-->

## Other folders

- [`_docs`](./_docs) — ESP32 datasheets / reference PDFs
- [`_pins`](./_pins) — ESP32 pinmap images

# esp32_and_arduino

ESP32 / Arduino sketches and local bridges. More projects will be added as folders under this repo.

Korean: [README.ko.md](README.ko.md)

## Projects

<table>
<tr>
<td>

### [victron-ble-mqtt](https://github.com/chaeplin/esp32_and_arduino/tree/master/victron-ble-mqtt)

Local ESP32 (Adafruit HUZZAH32) bridge for Victron Instant Readout.

Decrypts BLE advertisements, publishes MQTT JSON, and opens short GATT sessions for **daily history (up to 30 days)** and PV registers.

Built with [SuperGrok](https://grok.com) / Grokbot from captured ADV / GATT packets and Feather serial logs. Not an official Victron SDK.

[README](./victron-ble-mqtt/README.md) · [한국어](./victron-ble-mqtt/README.ko.md) · MIT

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

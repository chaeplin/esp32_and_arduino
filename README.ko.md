# esp32_and_arduino

ESP32 / Arduino 스케치와 로컬 브리지 모음입니다. 프로젝트는 이 저장소 아래 폴더로 계속 추가합니다.

영문 기본: [README.md](README.md)

## 프로젝트

<table>
<tr>
<td>

### [victron-ble-mqtt](https://github.com/chaeplin/esp32_and_arduino/tree/master/victron-ble-mqtt)

Victron Instant Readout용 로컬 ESP32(Adafruit HUZZAH32) 브리지.

BLE 광고를 복호화해 MQTT JSON으로 올리고, GATT는 **일일 이력(최대 30일)** 과 PV 레지스터가 필요할 때만 짧게 붙습니다.

ADV / GATT 패킷 캡처와 Feather 시리얼 로그를 [슈퍼그록(SuperGrok)](https://grok.com) / Grokbot과 맞춰 가며 만들었습니다. 공식 Victron SDK가 아닙니다.

[README](./victron-ble-mqtt/README.md) · [한국어](./victron-ble-mqtt/README.ko.md) · MIT

</td>
</tr>
</table>

<table>
<tr>
<td>

### [sunseeker-ble](https://github.com/chaeplin/esp32_and_arduino/tree/master/sunseeker-ble)

Sunseeker V3 Plus BLE 게이트웨이 · 위치 맵 · 컨트롤 패드.

ESP32 Feather가 잔디깎이에 BLE(AES-256-ECB + Base64 JSON)로 붙어 MQTT로 중계합니다. Python이 조종 패드(`:8765`)와 위치 맵(`:8767`)을 띄웁니다. InfluxDB 궤적은 선택입니다.

[README](./sunseeker-ble/README.md) · MIT

</td>
</tr>
</table>

<!--
다음 프로젝트는 이 블록을 복사하세요.

<table>
<tr>
<td>

### [name](https://github.com/chaeplin/esp32_and_arduino/tree/master/name)

한 줄 설명.

[README](./name/README.md)

</td>
</tr>
</table>
-->

## 다른 폴더

- [`_docs`](./_docs) — ESP32 데이터시트 / 레퍼런스 PDF
- [`_pins`](./_pins) — ESP32 핀맵 이미지

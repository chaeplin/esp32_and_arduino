# victron-ble-mqtt

ESP32(Adafruit HUZZAH32)로 Victron Instant Readout(BLE ADV)을 복호화해 MQTT로 올리고, 필요할 때만 GATT로 일일 이력·PV 레지스터를 읽는 로컬 브리지입니다.

평소에는 광고 패킷만 듣고 10초 평균을 발행합니다. `hist` / `pv` 명령이 오면 MPPT에 짧게 연결해 값을 읽고 바로 끊습니다. `hist` 는 `{"cmd":"hist","days":N}` 으로 오늘+최대 30일까지 읽습니다.

> 예제 값은 모두 익명화되어 있습니다. Wi-Fi, MQTT, PIN, MAC, Instant Readout 키는 로컬에서만 채우세요.

영문 README가 기본입니다. 이 파일은 한국어 설명입니다.

## 제작

VictronConnect의 BLE Instant Readout 광고와 GATT hist/pv 패킷을 캡처하고, HUZZAH32 시리얼 로그로 맞춰 가며 [슈퍼그록(SuperGrok)](https://grok.com)과 합작으로 만들었습니다. **30일 이력**(`hist` `days` 최대 30, 세션 분할, NVS 캐시)은 Grokbot과 이어서 붙였습니다. Instant Readout 광고 형식은 Victron이 공개한 Extra Manufacturer Data를 참고했습니다. 공식 SDK나 샘플 코드가 아닙니다.

## 화면

로컬 대시보드와 VictronConnect 비교입니다. 앱 쪽 시리얼은 가렸습니다.

라이브 값은 찍은 시각이 달라 숫자가 어긋날 수 있습니다. **맞는지 볼 것은 어제·그제처럼 이미 닫힌 날** 입니다. GATT `hist` 는 이제 **오늘 + 최대 30일** (`0x1050` … `0x106E`) 을 읽을 수 있습니다. 아래 비교 화면은 그 이전 것이라 **어제·그제**만 맞춰 보면 됩니다.

### 로컬 대시보드

![dashboard](dashboard.png)

`victron_mqtt_view.py` — http://127.0.0.1:8772

### VictronConnect 상태

![app-status](app-status.png)

라이브(태양광 V, 배터리 V, 온도)는 시간차 참고용입니다.

### VictronConnect 기록

![app-history](app-history.png)

| 날 | 수율 | 최대 P | 최대 Vpv | 배터리 max/min | 소비 |
|---|---|---|---|---|---|
| 어제 | 120 Wh | 23 W | 36.30 V | 13.43 / 12.64 V | 100 Wh |
| 그제 | 130 Wh | 23 W | 37.20 V | 13.48 / 12.62 V | 110 Wh |

어제 값은 GATT `hist` day1(`0x1051`)과 같습니다. 그제 이전 닫힌 날도 같은 hist 페이지에서 오고, 첫 채움 이후 NVS에 남습니다.

## 구성

```
SmartSolar MPPT 75/15  ──BLE ADV (AES-128-CTR)──┐
Smart Battery Sense    ──BLE ADV (AES-128-CTR)──┤
                                                ▼
                                       HUZZAH32 Feather
                                       ADV 10s + GATT burst
                                                │ MQTT
                                                ▼
                             broker ──┬── victron_poll.py      첫 hist days=7, 이후 어제 + 10분 PV
                                      ├── victron_mqtt_view.py 로컬 대시보드 :8772
                                      ├── yard_time_pub.py     KST epoch → yard/time
                                      └── (선택) InfluxDB
```

## 하드웨어 / 라이브러리

- Adafruit HUZZAH32 (ESP32 Feather)
- Arduino IDE 보드: `Adafruit ESP32 Feather`
- NimBLE-Arduino 2.x
- PubSubClient
- 대상 기기 예: SmartSolar MPPT 75/15, Smart Battery Sense

## 파일

```
victron_gatt_bridge_feather.ino   ESP32 펌웨어 (ADV + GATT)
config.example.h                  비밀값 템플릿 (복사해서 로컬에만 사용)
LICENSE                           MIT
README.md                         English (default)
README.ko.md                      한국어
victron_poll.py                   첫 hist days=7, 이후 어제 days=1 + 10분 PV
victron_mqtt_view.py              로컬 대시보드 http://127.0.0.1:8772
yard_time_pub.py                  Feather 시각 보조 (yard/time)
victron-poll.service              systemd user 유닛
victron_days.json                 일일 이력 예제
.env.example                      호스트 MQTT/Influx 템플릿
dashboard.png                     로컬 대시보드 화면
app-status.png                    VictronConnect 상태 (비교)
app-history.png                   VictronConnect 기록 (비교)
.gitignore
```

## 올리지 말 것

- Wi-Fi SSID / 비밀번호
- MQTT 호스트·계정·비밀번호
- Victron BLE PIN
- 기기 시리얼 / MAC
- Instant Readout AES-128 키 (16바이트)
- InfluxDB 토큰 (`influx.token`, `influx.env`)
- 홈 디렉터리 실경로

`.gitignore` 에 `config.h`, `.env`, `influx.token`, `*.log`, `victron_store/` 가 들어 있습니다.

PIN 예제는 `000000`, AES 키 예제는 `0x00` 16바이트입니다.

Instant Readout AES 키는 이 저장소가 만들지 않습니다. VictronConnect 앱에서 기기별로 복사해 `g_dev[].key` 에 넣습니다.

## Instant Readout AES 키

1. VictronConnect에서 해당 기기에 연결(PIN)
2. 설정(톱니) → 메뉴 → **Product Info**
3. **Instant Readout via Bluetooth** 켜기
4. **Instant Readout Details** 옆 **Show**
5. **Advertisement key**(hex 32자 = 16바이트)와 MAC을 복사
6. 스케치 `g_dev[].macHex` / `g_dev[].key` 에 붙여 넣기 (로컬만)

MPPT와 Sense는 키를 따로 복사합니다. GitHub에는 `0x00` × 16만 둡니다.

## MQTT 토픽

| 토픽 | 방향 | 내용 |
|---|---|---|
| `victron/mppt` | pub | ADV 10초 평균 (vbat, ibat, power, yield_wh, load_a, state) |
| `victron/sense` | pub | ADV 10초 평균 (vbat, temp) |
| `victron/status` | pub | 보드 상태 (mode, rssi, clock, nvs, wifi, uptime) |
| `victron/mppt/hist` | pub | 하루당 JSON 1개 (today / yesterday / day N) |
| `victron/mppt/pv` | pub | GATT PV (vpv, ppv, ipv). 없는 값은 null |
| `victron/gatt/cmd` | sub | `{"cmd":"hist"}` / `{"cmd":"hist","days":N}` / `{"cmd":"pv"}` / `{"cmd":"unpair"}` |
| `victron/lwt` | pub | online / offline (retain) |
| `yard/time` | sub | `{"epoch":..., "kst":"..."}` NTP 보조 |

## 동작

1. **ADV** — manufacturer data CID `0x02E1`, record `0x10` 을 AES-128-CTR로 풀어 10초 평균 발행.
2. **hist** — `{"cmd":"hist"}` 또는 `{"cmd":"hist","days":N}` (N ≤ 30). day0=오늘 `0x1050`, day1=어제 `0x1051`, … dayN=`0x1050+N` (최대 `0x106E`). NTP 또는 `yard/time`으로 KST 날짜가 있어야 함. 닫힌 날은 NVS 링 캐시. 한 세션에서 약 16일을 읽고 재연결해 이어서 받음. 첫 채움 이후 poll은 어제(`days`: 1)와 오늘만 요청.
3. **pv** — `0xEDBB` / `0xEDBC` / `0xEDBD` 읽고 `victron/mppt/pv` 발행 후 해제.
4. **unpair** — 본드와 NVS 이력 삭제.

## 펌웨어

1. VictronConnect에서 Instant Readout 키·MAC을 복사해 스케치 상단(또는 로컬 `config.h`)에 넣는다.
2. Arduino IDE에서 `Adafruit ESP32 Feather` 선택 후 업로드.
3. Feather는 `pool.ntp.org` / `time.google.com` 과 `yard/time` 을 함께 씁니다.

## 호스트

```bash
python3 -m pip install paho-mqtt
cp .env.example .env
python3 yard_time_pub.py
python3 victron_poll.py
python3 victron_mqtt_view.py
# 대시보드 http://127.0.0.1:8772
```

`victron_poll.py`: 00:15 이후 어제가 없으면 — 처음엔 `{"cmd":"hist","days":7}`, 닫힌 날이 쌓이면 `{"cmd":"hist","days":1}` 만. 10분마다 재시도. MPPT state가 `off`가 아니면 10분마다 `pv`. 오늘 소비는 ADV `load_a × vbat` 적분, 오늘 수율이 생기면 어제 소비/수율 비. 수동 `{"cmd":"hist","days":30}` 이면 기기 창 전체를 채움.

systemd user 유닛은 `victron-poll.service` 참고. `WorkingDirectory` / `ExecStart` 경로만 맞추면 됩니다.

InfluxDB는 선택입니다. 토큰은 `INFLUX_TOKEN` 또는 gitignore된 `./influx.token`.

## 라이선스

[MIT](LICENSE).

공식 Victron SDK가 아닙니다. Victron / VictronConnect 상표는 Victron Energy 소유입니다.

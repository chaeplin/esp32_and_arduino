#!/usr/bin/env python3
"""Daily hist (yesterday) + 10-min PV when MPPT is not off.

    python3 victron_poll.py
    systemd: victron_poll.service
    tail -f victron_poll.log
"""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import paho.mqtt.client as mqtt

CMD = "victron/gatt/cmd"
T_MPPT = "victron/mppt"
T_HIST = "victron/mppt/hist"
T_PV = "victron/mppt/pv"
T_STATUS = "victron/status"

HERE = Path(__file__).resolve().parent


def _load_dotenv() -> None:
    for path in (HERE / ".env", Path.cwd() / ".env"):
        if not path.is_file():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k, v = k.strip(), v.strip().strip("'").strip('"')
            if k and k not in os.environ:
                os.environ[k] = v


_load_dotenv()
MQTT_HOST = os.environ.get("MQTT_HOST", "YOUR_MQTT_HOST")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USER", "YOUR_MQTT_USER")
MQTT_PASS = os.environ.get("MQTT_PASSWORD", os.environ.get("MQTT_PASS", "YOUR_MQTT_PASSWORD"))
STORE = HERE / "victron_store"
DAYS_FILE = STORE / "days.json"
DAYS_VIEW = HERE / "victron_days.json"
PV_FILE = STORE / "pv.jsonl"
LOG_FILE = HERE / "victron_poll.log"
KST = ZoneInfo("Asia/Seoul")

lock = threading.Lock()
mppt_state = "off"
last_hist_try = 0.0
last_pv_s = 0.0
last_load_s = 0.0


def now_kst() -> datetime:
    return datetime.now(KST)


def log(msg: str) -> None:
    line = f"{now_kst():%Y-%m-%d %H:%M:%S} {msg}"
    print(line, flush=True)
    try:
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def ymd_of(d) -> int:
    return d.year * 10000 + d.month * 100 + d.day


def today_ymd() -> int:
    return ymd_of(now_kst().date())


def yesterday_ymd() -> int:
    return ymd_of(now_kst().date() - timedelta(days=1))


def load_days() -> dict:
    out: dict = {}
    for path in (DAYS_FILE, DAYS_VIEW):
        if not path.is_file():
            continue
        try:
            obj = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(obj, dict):
                out.update(obj)
        except Exception:
            pass
    return out


def save_days(days: dict) -> None:
    STORE.mkdir(parents=True, exist_ok=True)
    text = json.dumps(days, indent=2, ensure_ascii=False)
    for path in (DAYS_FILE, DAYS_VIEW):
        tmp = path.with_suffix(".tmp")
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(path)


def save_day(obj: dict) -> None:
    ymd = obj.get("ymd")
    if not ymd:
        return
    key = str(int(ymd))
    rec = {
        "ymd": int(ymd),
        "kind": obj.get("kind") or "",
        "yield_kwh": obj.get("yield_kwh"),
        "consumed_kwh": obj.get("consumed_kwh"),
        "pmax_w": obj.get("pmax_w"),
        "vpv_max": obj.get("vpv_max"),
        "vbat_max": obj.get("vbat_max"),
        "vbat_min": obj.get("vbat_min"),
        "src": obj.get("src") or "gatt",
        "ts": now_kst().isoformat(timespec="seconds"),
    }
    with lock:
        days = load_days()
        old = days.get(key) or {}
        for k, v in list(rec.items()):
            if v is None and old.get(k) is not None:
                rec[k] = old[k]
        days[key] = rec
        save_days(days)
    log(f"SAVE day {key} kind={rec.get('kind')} y={rec.get('yield_kwh')} cons={rec.get('consumed_kwh')} pmax={rec.get('pmax_w')}")


def upsert_today_adv(obj: dict) -> None:
    global last_load_s
    ywh = obj.get("yield_wh")
    yk = None
    if ywh is not None:
        try:
            yk = float(ywh) / 1000.0
        except Exception:
            yk = None
    rec = {
        "ymd": today_ymd(),
        "kind": "today",
        "yield_kwh": yk,
        "consumed_kwh": obj.get("consumed_kwh"),
        "pmax_w": obj.get("power") or obj.get("pmax_w"),
        "vpv_max": obj.get("vpv"),
        "vbat_max": obj.get("vbat"),
        "vbat_min": obj.get("vbat"),
        "src": "adv",
        "consumed_src": None,
        "ts": now_kst().isoformat(timespec="seconds"),
    }
    la = obj.get("load_a")
    vb = obj.get("vbat")
    now = time.time()
    add_wh = 0.0
    if la is not None and vb is not None:
        try:
            lw = max(0.0, float(la) * float(vb))
            if last_load_s > 0:
                dt = min(30.0, max(0.0, now - last_load_s))
                add_wh = lw * dt / 3600.0
            last_load_s = now
        except Exception:
            add_wh = 0.0
    key = str(rec["ymd"])
    with lock:
        days = load_days()
        old = days.get(key) or {}
        if old.get("kind") == "yesterday" and int(old.get("ymd") or 0) != today_ymd():
            return
        if old.get("pmax_w") is not None and rec.get("pmax_w") is not None:
            rec["pmax_w"] = max(float(old["pmax_w"]), float(rec["pmax_w"]))
        elif old.get("pmax_w") is not None:
            rec["pmax_w"] = old["pmax_w"]
        if old.get("vpv_max") is not None and rec.get("vpv_max") is not None:
            rec["vpv_max"] = max(float(old["vpv_max"]), float(rec["vpv_max"]))
        elif old.get("vpv_max") is not None:
            rec["vpv_max"] = old["vpv_max"]
        if old.get("vbat_max") is not None and rec.get("vbat_max") is not None:
            rec["vbat_max"] = max(float(old["vbat_max"]), float(rec["vbat_max"]))
        if old.get("vbat_min") is not None and rec.get("vbat_min") is not None:
            rec["vbat_min"] = min(float(old["vbat_min"]), float(rec["vbat_min"]))
        if old.get("src") == "gatt" and old.get("consumed_kwh") is not None:
            rec["consumed_kwh"] = old["consumed_kwh"]
            rec["consumed_src"] = "gatt"
        else:
            base = float(old.get("load_wh") or 0)
            if old.get("consumed_src") == "load":
                base = float(old.get("consumed_kwh") or 0) * 1000.0
            load_wh = base + add_wh
            rec["load_wh"] = round(load_wh, 2)
            if rec.get("yield_kwh") and rec["yield_kwh"] > 0:
                yy = None
                yc = None
                ykey = str(yesterday_ymd())
                yd = days.get(ykey) or {}
                try:
                    yy = float(yd.get("yield_kwh") or 0)
                    yc = float(yd.get("consumed_kwh") or 0)
                except Exception:
                    pass
                if yy and yy > 0 and yc == yc:
                    rec["consumed_kwh"] = rec["yield_kwh"] * (yc / yy)
                    rec["consumed_src"] = "ratio"
                else:
                    rec["consumed_kwh"] = load_wh / 1000.0
                    rec["consumed_src"] = "load"
            else:
                rec["consumed_kwh"] = load_wh / 1000.0
                rec["consumed_src"] = "load"
        days[key] = rec
        save_days(days)


def save_pv(obj: dict) -> None:
    STORE.mkdir(parents=True, exist_ok=True)
    row = {
        "ts": now_kst().isoformat(timespec="seconds"),
        "vpv": obj.get("vpv"),
        "ppv": obj.get("ppv"),
        "ipv": obj.get("ipv"),
    }
    with PV_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
    log(f"SAVE pv vpv={row['vpv']} ppv={row['ppv']}")


def pub_cmd(c: mqtt.Client, cmd: str) -> None:
    payload = json.dumps({"cmd": cmd})
    c.publish(CMD, payload, qos=0)
    log(f"TX {CMD} {payload}")


def on_connect(c, _u, _f, rc, _p=None) -> None:
    log(f"mqtt rc={rc}")
    if rc != 0:
        return
    c.subscribe(T_MPPT)
    c.subscribe(T_HIST)
    c.subscribe(T_PV)
    c.subscribe(T_STATUS)


def on_message(_c, _u, msg: mqtt.MQTTMessage) -> None:
    global mppt_state
    raw = msg.payload.decode("utf-8", errors="replace")
    try:
        obj = json.loads(raw)
    except Exception:
        return
    if not isinstance(obj, dict):
        return
    if msg.topic == T_MPPT:
        st = str(obj.get("state") or "").lower()
        if st:
            with lock:
                mppt_state = st
        upsert_today_adv(obj)
    elif msg.topic == T_HIST:
        if obj.get("ymd") or str(obj.get("kind") or "") in ("yesterday", "today"):
            save_day(obj)
    elif msg.topic == T_PV:
        save_pv(obj)


def after_hist_gate(t: datetime) -> bool:
    return t.hour > 0 or t.minute >= 15


def loop(c: mqtt.Client) -> None:
    global last_hist_try, last_pv_s
    STORE.mkdir(parents=True, exist_ok=True)
    while True:
        t = now_kst()
        ymd_y = yesterday_ymd()
        days = load_days()
        have_y = str(ymd_y) in days
        if not have_y and after_hist_gate(t) and time.time() - last_hist_try >= 600:
            log(f"MISS yday {ymd_y} — hist retry")
            pub_cmd(c, "hist")
            last_hist_try = time.time()
        with lock:
            st = mppt_state
        if st and st != "off" and time.time() - last_pv_s >= 600:
            pub_cmd(c, "pv")
            last_pv_s = time.time()
        time.sleep(15)


def main() -> None:
    ver = getattr(mqtt, "CallbackAPIVersion", None)
    c = mqtt.Client(ver.VERSION2, client_id="victron-poll") if ver else mqtt.Client(client_id="victron-poll")
    c.username_pw_set(MQTT_USER, MQTT_PASS)
    c.on_connect = on_connect
    c.on_message = on_message
    c.connect(MQTT_HOST, MQTT_PORT, 30)
    threading.Thread(target=c.loop_forever, daemon=True).start()
    time.sleep(1)
    loop(c)


if __name__ == "__main__":
    main()

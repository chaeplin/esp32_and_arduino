#!/usr/bin/env python3
"""Victron view — live MQTT, history InfluxDB, zoom charts.

    INFLUX_TOKEN=... python3 victron_mqtt_view.py
    # or put token in ./influx.token
    browser: http://127.0.0.1:8772
"""
from __future__ import annotations

import csv
import io
import json
import os
import threading
import time
import urllib.error
import urllib.request
import webbrowser
from collections import deque
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import paho.mqtt.client as mqtt

MQTT_HOST = os.environ.get("MQTT_HOST", "YOUR_MQTT_HOST")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USER", "YOUR_MQTT_USER")
MQTT_PASS = os.environ.get("MQTT_PASSWORD", os.environ.get("MQTT_PASS", "YOUR_MQTT_PASSWORD"))
HTTP_HOST = "0.0.0.0"
HTTP_PORT = 8772

TOPIC_MPPT = "victron/mppt"
TOPIC_SENSE = "victron/sense"
TOPIC_STATUS = "victron/status"
TOPIC_HIST = "victron/mppt/hist"
TOPIC_PV = "victron/mppt/pv"
TOPIC_LWT = "victron/lwt"

HERE = Path(__file__).resolve().parent
KST = ZoneInfo("Asia/Seoul")
DAYS_FILE = HERE / "victron_days.json"
PV_FILE = HERE / "victron_pv.json"


def _load_influx_env() -> None:
    cands = [
        HERE / "influx.env",
        HERE / "influx.token",
        Path.cwd() / "influx.env",
        Path.cwd() / "influx.token",
        Path.home() / "influx.env",
        Path.home() / "influx.token",
    ]
    seen = set()
    for cand in cands:
        try:
            key = str(cand.resolve())
        except OSError:
            key = str(cand)
        if key in seen or not cand.is_file():
            continue
        seen.add(key)
        try:
            txt = cand.read_text(encoding="utf-8")
        except OSError:
            continue
        if cand.name in ("influx.token", "token", "influx_token.txt"):
            tok = ""
            for line in txt.splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    k, v = line.split("=", 1)
                    if k.strip() in ("INFLUX_TOKEN", "TOKEN", "token"):
                        tok = v.strip().strip("'").strip('"')
                        break
                else:
                    tok = line
                    break
            if tok and "INFLUX_TOKEN" not in os.environ:
                os.environ["INFLUX_TOKEN"] = tok
            continue
        for line in txt.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k, v = k.strip(), v.strip().strip("'").strip('"')
            if k and k not in os.environ:
                os.environ[k] = v


_load_influx_env()
INFLUX_URL = os.environ.get("INFLUX_URL", "http://127.0.0.1:8086").rstrip("/")
INFLUX_ORG = os.environ.get("INFLUX_ORG", "your-org")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET", "victron")
LOG_UI_MAX = 12

RANGES: dict[str, float] = {
    "12h": 12 * 3600,
    "24h": 24 * 3600,
    "48h": 48 * 3600,
    "1w": 7 * 24 * 3600,
    "1m": 31 * 24 * 3600,
    "all": 90 * 24 * 3600,
}
WIN: dict[str, str] = {
    "12h": "20s",
    "24h": "1m",
    "48h": "2m",
    "1w": "10m",
    "1m": "30m",
    "all": "1h",
}

state_lock = threading.Lock()
state: dict[str, Any] = {
    "mqtt": "connecting",
    "influx": "?",
    "lwt": "",
    "mppt": {},
    "sense": {},
    "status": {},
    "hist": {},
    "pv": {},
    "days": {},
    "updated": 0.0,
    "log": [],
    "board_pts": [],
}

BOARD_KEEP = 24 * 3600
BOARD_MIN_DT = 15.0

def _load_days_store() -> dict:
    out = {}
    for cand in (HERE / "victron_store" / "days.json", HERE / "victron_days.json"):
        if not cand.is_file():
            continue
        try:
            obj = json.loads(cand.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(obj, dict):
            out.update(obj)
    return out

BOARD_FILE = HERE / "victron_board_24h.jsonl"
_board_q: deque[list] = deque()
_board_last = 0.0


def _load_board() -> None:
    if not BOARD_FILE.is_file():
        return
    cut = time.time() - BOARD_KEEP
    try:
        for line in BOARD_FILE.read_text(encoding="utf-8").splitlines():
            try:
                p = json.loads(line)
            except Exception:
                continue
            if not isinstance(p, list) or len(p) < 5:
                continue
            if float(p[0]) >= cut:
                _board_q.append(p)
    except OSError:
        pass
    with state_lock:
        state["board_pts"] = list(_board_q)


def _save_board() -> None:
    try:
        tmp = BOARD_FILE.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            for p in _board_q:
                f.write(json.dumps(p) + "\n")
        tmp.replace(BOARD_FILE)
    except OSError:
        pass


_load_board()
state["days"] = _load_days_store()
if DAYS_FILE.is_file():
    try:
        d = json.loads(DAYS_FILE.read_text(encoding="utf-8"))
        if isinstance(d, dict):
            state["days"] = d
    except Exception:
        pass
if PV_FILE.is_file():
    try:
        state["pv"] = json.loads(PV_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass


def _push_board(obj: dict[str, Any]) -> None:
    global _board_last
    now = time.time()
    if now - _board_last < BOARD_MIN_DT:
        return
    _board_last = now
    try:
        wifi = float(obj.get("wifi_rssi")) if obj.get("wifi_rssi") is not None else None
        mp = float(obj.get("mppt_rssi")) if obj.get("mppt_rssi") is not None else None
        se = float(obj.get("sense_rssi")) if obj.get("sense_rssi") is not None else None
        vb = float(obj.get("vbat")) if obj.get("vbat") is not None else None
    except (TypeError, ValueError):
        return
    if wifi is not None and wifi >= 0:
        wifi = None
    if mp is not None and mp >= 0:
        mp = None
    if se is not None and se >= 0:
        se = None
    _board_q.append([now, wifi, mp, se, vb])
    cut = now - BOARD_KEEP
    while _board_q and _board_q[0][0] < cut:
        _board_q.popleft()
    if len(_board_q) % 4 == 0:
        _save_board()


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _log(msg: str) -> None:
    with state_lock:
        state["log"].append({"ts": _ts(), "msg": msg[:240]})
        if len(state["log"]) > LOG_UI_MAX:
            del state["log"][: len(state["log"]) - LOG_UI_MAX]
        state["updated"] = time.time()


def _parse_token_text(txt: str) -> str:
    for line in txt.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, v = line.split("=", 1)
            if k.strip() in ("INFLUX_TOKEN", "TOKEN", "token"):
                return v.strip().strip("'").strip('"')
            continue
        return line.strip("'").strip('"')
    return ""


def _influx_token() -> str:
    t = os.environ.get("INFLUX_TOKEN", "").strip().strip("'").strip('"')
    if t.startswith("INFLUX_TOKEN="):
        t = t.split("=", 1)[1].strip().strip("'").strip('"')
    if t:
        return t
    for p in (
        HERE / "influx.token",
        HERE / "influx.env",
        Path.cwd() / "influx.token",
        Path.cwd() / "influx.env",
        Path.home() / "influx.token",
        Path.home() / "influx.env",
    ):
        try:
            if p.is_file():
                tok = _parse_token_text(p.read_text(encoding="utf-8"))
                if tok:
                    return tok
        except OSError:
            pass
    return ""


def _flux_csv(q: str) -> list[dict[str, str]]:
    token = _influx_token()
    if not token:
        raise RuntimeError("INFLUX_TOKEN 없음  (env 또는 ./influx.token)")
    url = f"{INFLUX_URL}/api/v2/query?org={INFLUX_ORG}"
    req = urllib.request.Request(
        url,
        data=q.encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Token {token}",
            "Content-Type": "application/vnd.flux",
            "Accept": "application/csv",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=12) as r:
            raw = r.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:300]
        raise RuntimeError(f"influx {e.code} {body}") from e
    rows: list[dict[str, str]] = []
    for block in raw.split("\n\n"):
        lines = [ln for ln in block.splitlines() if ln and not ln.startswith("#")]
        if not lines:
            continue
        reader = csv.DictReader(io.StringIO("\n".join(lines)))
        for rec in reader:
            if rec.get("_value") in (None, ""):
                continue
            rows.append(rec)
    return rows


def _iso_to_unix(s: str) -> float:
    if not s:
        return 0.0
    s = s.replace("Z", "+00:00")
    if "." in s:
        head, rest = s.split(".", 1)
        frac, tz = rest, ""
        for sep in ("+", "-"):
            if sep in rest[1:] or rest.startswith(sep):
                i = rest.find(sep, 1) if not rest.startswith(sep) else 0
                if i > 0:
                    frac, tz = rest[:i], rest[i:]
                    break
        frac = (frac + "000000")[:6]
        s = f"{head}.{frac}{tz or '+00:00'}"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _norm_unix(t: float) -> float:
    while t > 1e12:
        t = t / 1000.0
    return t


def _series(rows: list[dict[str, str]], field: str) -> list[list[float]]:
    out: list[list[float]] = []
    for rec in rows:
        if (rec.get("_field") or "").strip() != field:
            continue
        try:
            t = _norm_unix(_iso_to_unix(rec.get("_time") or ""))
            v = float(rec["_value"])
        except (TypeError, ValueError):
            continue
        if t < 1e9:
            continue
        out.append([t, v])
    out.sort(key=lambda p: p[0])
    return out



def _query_daily(n: int = 8) -> list[dict[str, Any]]:
    nd = max(3, min(14, int(n)))
    start = "-" + str(nd) + "d"
    q = (
        'from(bucket: "%s")\n'
        "  |> range(start: %s)\n"
        '  |> filter(fn: (r) =>\n'
        '       (r._measurement == "victron_mppt" and (r._field == "vbat" or r._field == "power"\n'
        '          or r._field == "yield_wh" or r._field == "load_a"))\n'
        '       or (r._measurement == "victron_sense" and r._field == "vbat"))\n'
        "  |> aggregateWindow(every: 10m, fn: mean, createEmpty: false)\n"
        '  |> keep(columns: ["_time","_value","_field","_measurement"])\n'
    ) % (INFLUX_BUCKET, start)
    rows = _flux_csv(q)
    buckets: dict[str, dict[str, Any]] = {}
    prev: dict[str, tuple[float, float]] = {}
    for rec in rows:
        try:
            ts = _iso_to_unix(rec.get("_time") or "")
            v = float(rec["_value"])
        except (TypeError, ValueError):
            continue
        day = datetime.fromtimestamp(ts, tz=KST).strftime("%Y-%m-%d")
        b = buckets.setdefault(day, {
            "day": day, "wh": None, "pmax": None,
            "vmax": None, "vmin": None, "cwh": 0.0,
        })
        meas = rec.get("_measurement")
        field = rec.get("_field")
        if meas == "victron_mppt" and field == "yield_wh":
            if b["wh"] is None or v > b["wh"]:
                b["wh"] = v
        elif meas == "victron_mppt" and field == "power":
            if b["pmax"] is None or v > b["pmax"]:
                b["pmax"] = v
        elif field == "vbat":
            if b["vmax"] is None or v > b["vmax"]:
                b["vmax"] = v
            if b["vmin"] is None or v < b["vmin"]:
                b["vmin"] = v
        if meas == "victron_mppt" and field == "load_a":
            key = "load"
            if key in prev:
                t0, a0 = prev[key]
                dt_h = max(0.0, min(0.5, (ts - t0) / 3600.0))
                vbat = b["vmax"] if b["vmax"] is not None else 12.8
                b["cwh"] += abs(a0) * vbat * dt_h
            prev[key] = (ts, v)
    out = []
    for k in sorted(buckets):
        b = buckets[k]
        out.append({
            "day": b["day"],
            "wh": None if b["wh"] is None else round(float(b["wh"]), 0),
            "pmax": None if b["pmax"] is None else round(float(b["pmax"]), 0),
            "vmax": None if b["vmax"] is None else round(float(b["vmax"]), 2),
            "vmin": None if b["vmin"] is None else round(float(b["vmin"]), 2),
            "cwh": round(float(b["cwh"]), 0),
        })
    return out[-nd:]



def _daily_table(n_days: int = 14) -> list[dict[str, Any]]:
    """KST day buckets from raw-ish 10m means: yield max, Pmax, Vbat max/min, load Wh."""
    q = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -{n_days}d)
  |> filter(fn: (r) => r._measurement == "victron_mppt" and
       (r._field == "power" or r._field == "vbat" or r._field == "yield_wh" or r._field == "load_a"))
  |> aggregateWindow(every: 10m, fn: mean, createEmpty: false)
  |> keep(columns: ["_time","_value","_field"])
'''
    try:
        rows = _flux_csv(q)
    except Exception:
        return []
    days: dict[str, dict[str, Any]] = {}
    prev_t: dict[str, float] = {}
    for rec in rows:
        field = rec.get("_field")
        try:
            ts = _iso_to_unix(rec.get("_time") or "")
            v = float(rec["_value"])
        except (TypeError, ValueError):
            continue
        day = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
        d = days.setdefault(day, {
            "day": day, "wh": None, "pmax": None,
            "vmax": None, "vmin": None, "load_wh": 0.0,
        })
        if field == "yield_wh":
            d["wh"] = v if d["wh"] is None else max(d["wh"], v)
        elif field == "power":
            d["pmax"] = v if d["pmax"] is None else max(d["pmax"], v)
        elif field == "vbat":
            d["vmax"] = v if d["vmax"] is None else max(d["vmax"], v)
            d["vmin"] = v if d["vmin"] is None else min(d["vmin"], v)
        elif field == "load_a":
            last = prev_t.get(day)
            dt_h = ((ts - last) / 3600.0) if last else (10.0 / 60.0)
            if dt_h < 0:
                dt_h = 10.0 / 60.0
            if dt_h > 0.5:
                dt_h = 10.0 / 60.0
            vb = d["vmax"] if d["vmax"] is not None else 13.0
            d["load_wh"] += max(0.0, v) * vb * dt_h
            prev_t[day] = ts
    out = []
    for k in sorted(days):
        d = days[k]
        d["wh"] = None if d["wh"] is None else round(d["wh"], 1)
        d["pmax"] = None if d["pmax"] is None else round(d["pmax"], 1)
        d["vmax"] = None if d["vmax"] is None else round(d["vmax"], 2)
        d["vmin"] = None if d["vmin"] is None else round(d["vmin"], 2)
        d["load_wh"] = round(d["load_wh"], 1)
        out.append(d)
    return out[-n_days:]


def _query_hist(key: str) -> dict[str, Any]:
    now = time.time()
    span = RANGES.get(key, RANGES["24h"])
    every = WIN.get(key, "1m")
    start = f"-{int(span)}s"
    q = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: {start})
  |> filter(fn: (r) =>
       (r._measurement == "victron_mppt" and (r._field == "vbat" or r._field == "power"
          or r._field == "ibat" or r._field == "yield_wh" or r._field == "load_a" or r._field == "rssi"))
       or (r._measurement == "victron_sense" and (r._field == "vbat" or r._field == "temp" or r._field == "rssi")))
  |> aggregateWindow(every: {every}, fn: mean, createEmpty: false)
  |> keep(columns: ["_time","_value","_field","_measurement"])
'''
    rows = _flux_csv(q)
    def _meas(r: dict[str, str]) -> str:
        return (r.get("_measurement") or "").strip()
    mppt = [r for r in rows if _meas(r) == "victron_mppt"] or rows
    sense = [r for r in rows if _meas(r) == "victron_sense"] or rows
    t0 = now - span
    out = {
        "range": key,
        "t0": t0,
        "t1": now,
        "every": every,
        "mppt_v": _series(mppt, "vbat"),
        "mppt_p": _series(mppt, "power") or _series(rows, "power"),
        "mppt_i": _series(mppt, "ibat") or _series(rows, "ibat"),
        "sns_v": _series(sense, "vbat"),
        "sns_t": _series(sense, "temp") or _series(rows, "temp"),
        "days": _query_daily(7),
        "n": len(rows),
        "wifi_rssi": [],
        "mppt_rssi": _series(mppt, "rssi") or _series(rows, "rssi"),
        "sense_rssi": _series(sense, "rssi"),
        "board_v": [],
    }
    try:
        bq = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: {start})
  |> filter(fn: (r) => r._measurement == "victron_status" and
       (r._field == "vbat" or r._field == "wifi_rssi" or r._field == "mppt_rssi" or r._field == "sense_rssi"))
  |> aggregateWindow(every: {every}, fn: mean, createEmpty: false)
  |> keep(columns: ["_time","_value","_field"])
'''
        brows = _flux_csv(bq)
        out["wifi_rssi"] = _series(brows, "wifi_rssi")
        if _series(brows, "mppt_rssi"):
            out["mppt_rssi"] = _series(brows, "mppt_rssi")
        if _series(brows, "sense_rssi"):
            out["sense_rssi"] = _series(brows, "sense_rssi")
        out["board_v"] = _series(brows, "vbat")
    except Exception:
        pass
    with state_lock:
        pts = list(state.get("board_pts") or [])
    cut = now - span
    def _fill(key: str, idx: int) -> None:
        if out.get(key):
            return
        out[key] = [[p[0], p[idx]] for p in pts if p[0] >= cut and p[idx] is not None]
    _fill("wifi_rssi", 1)
    _fill("mppt_rssi", 2)
    _fill("sense_rssi", 3)
    _fill("board_v", 4)
    return out


def _daily_bars(n: int = 14) -> list[dict[str, Any]]:
    q = (
        'from(bucket: "' + INFLUX_BUCKET + '")\n'
        '  |> range(start: -' + str(n) + 'd)\n'
        '  |> filter(fn: (r) => r._measurement == "victron_mppt" and '
        '(r._field == "yield_wh" or r._field == "power" or r._field == "vbat" or r._field == "load_a"))\n'
        '  |> aggregateWindow(every: 5m, fn: last, createEmpty: false)\n'
        '  |> keep(columns: ["_time","_value","_field"])\n'
    )
    try:
        rows = _flux_csv(q)
    except Exception:
        return []
    days: dict[str, dict[str, Any]] = {}
    last_v = None
    prev_t = None
    for rec in rows:
        try:
            t = _iso_to_unix(rec.get("_time") or "")
            v = float(rec["_value"])
        except (TypeError, ValueError):
            continue
        if not t:
            continue
        key = datetime.fromtimestamp(t, KST).strftime("%Y-%m-%d")
        d = days.setdefault(key, {
            "day": key, "yield_wh": 0.0, "pmax": 0.0,
            "vmax": None, "vmin": None, "load_wh": 0.0,
        })
        f = rec.get("_field")
        if f == "yield_wh" and v > d["yield_wh"]:
            d["yield_wh"] = v
        elif f == "power" and v > d["pmax"]:
            d["pmax"] = v
        elif f == "vbat":
            d["vmax"] = v if d["vmax"] is None else max(d["vmax"], v)
            d["vmin"] = v if d["vmin"] is None else min(d["vmin"], v)
            last_v = v
        elif f == "load_a" and last_v is not None:
            dt_h = 5.0 / 60.0
            if prev_t:
                dt_h = max(0.0, min(0.25, (t - prev_t) / 3600.0))
            d["load_wh"] += max(0.0, v) * last_v * dt_h
        prev_t = t
    out = []
    for k in sorted(days)[-n:]:
        d = days[k]
        d["yield_wh"] = round(d["yield_wh"], 0)
        d["wh"] = d["yield_wh"]
        d["pmax"] = round(d["pmax"], 0)
        d["load_wh"] = round(d["load_wh"], 0)
        if d["vmax"] is not None:
            d["vmax"] = round(d["vmax"], 2)
        if d["vmin"] is not None:
            d["vmin"] = round(d["vmin"], 2)
        out.append(d)
    return out


def on_connect(client: mqtt.Client, _u, _f, rc, _p=None) -> None:
    with state_lock:
        state["mqtt"] = "ok" if rc == 0 else f"rc={rc}"
    if rc == 0:
        client.subscribe(TOPIC_MPPT)
        client.subscribe(TOPIC_SENSE)
        client.subscribe(TOPIC_STATUS)
        client.subscribe(TOPIC_HIST)
        client.subscribe(TOPIC_PV)
        client.subscribe(TOPIC_LWT)
        _log("mqtt connected")


def on_disconnect(_c, _u, rc, _p=None) -> None:
    with state_lock:
        state["mqtt"] = "offline"
    _log(f"mqtt drop rc={rc}")


def on_message(_c, _u, msg: mqtt.MQTTMessage) -> None:
    raw = msg.payload.decode("utf-8", errors="replace")
    now = time.time()
    with state_lock:
        if msg.topic == TOPIC_LWT:
            state["lwt"] = raw
            state["updated"] = now
            return
        try:
            obj = json.loads(raw)
        except Exception:
            obj = {"raw": raw}
        if msg.topic == TOPIC_MPPT:
            obj["_ts"] = _ts()
            state["mppt"] = obj
        elif msg.topic == TOPIC_SENSE:
            obj["_ts"] = _ts()
            state["sense"] = obj
        elif msg.topic == TOPIC_STATUS:
            state["status"] = obj
            _push_board(obj)
            state["board_pts"] = list(_board_q)
        elif msg.topic == TOPIC_HIST:
            obj["_ts"] = _ts()
            state["hist"] = obj
            ymd = obj.get("ymd")
            if ymd:
                days = dict(state.get("days") or {})
                days[str(ymd)] = obj
                state["days"] = days
                try:
                    (HERE / "victron_days.json").write_text(
                        json.dumps(days, ensure_ascii=False, indent=2), encoding="utf-8")
                except OSError:
                    pass
        elif msg.topic == TOPIC_PV:
            obj["_ts"] = _ts()
            state["pv"] = obj
        state["updated"] = now


def make_client() -> mqtt.Client:
    ver = getattr(mqtt, "CallbackAPIVersion", None)
    if ver is not None:
        return mqtt.Client(ver.VERSION2, client_id="victron-view")
    return mqtt.Client(client_id="victron-view")


def mqtt_thread() -> None:
    c = make_client()
    c.username_pw_set(MQTT_USER, MQTT_PASS)
    c.on_connect = on_connect
    c.on_disconnect = on_disconnect
    c.on_message = on_message
    while True:
        try:
            c.connect(MQTT_HOST, MQTT_PORT, 30)
            c.loop_forever()
        except Exception as e:
            with state_lock:
                state["mqtt"] = "retry"
            _log(f"mqtt err {e}")
            time.sleep(2)


HTML = r"""<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Victron</title>
<style>
:root { color-scheme: dark; }
*{ box-sizing:border-box; }
html,body { height:100%; }
body {
  margin:0; background:#15202b; color:#e8eef4;
  font:16px/1.3 -apple-system,system-ui,sans-serif;
}
header { padding:8px 16px 4px; display:flex; justify-content:space-between; align-items:baseline; gap:12px; }
h1 { font-size:20px; margin:0; font-weight:700; }
.sub { color:#8aa0b3; font-size:13px; }
.wrap { padding:8px 16px 20px; }
.grid { display:flex; flex-direction:column; gap:12px; }
.schem { background:#1c2b3a; border-radius:14px; padding:14px 12px 10px; text-align:center; }
.nodes { display:flex; align-items:center; justify-content:space-between; gap:8px; }
.node { flex:1; }
.ico { width:52px; height:40px; margin:0 auto 4px; }
.lab { font-size:13px; color:#8aa0b3; }
.val { font-size:20px; font-weight:700; }
.wire { height:2px; background:#3d556c; flex:0.3; }
.pwr { font-size:56px; font-weight:800; letter-spacing:-2px; line-height:1; }
.unit { font-size:20px; font-weight:650; color:#c5d4e0; }
.meta { display:flex; justify-content:space-around; margin-top:10px; color:#c5d4e0; font-size:13px; }
.meta b { display:block; font-size:20px; color:#fff; margin-top:2px; }
.states { display:flex; flex-wrap:wrap; gap:6px; justify-content:center; margin-top:10px; }
.states span {
  font-size:12px; padding:4px 8px; border-radius:999px;
  color:#6d8296; background:#14202c; border:1px solid #2a3b4d;
  font-family:ui-monospace,Menlo,monospace;
}
.states span.on { color:#fff; background:#0e4a44; border-color:#00b3a4; font-weight:700; }
.side { display:grid; grid-template-columns:1fr 1fr; gap:12px; }
.card { background:#1c2b3a; border-radius:14px; padding:10px 12px; }
.row { display:flex; justify-content:space-between; align-items:center;
       padding:6px 0; border-bottom:1px solid #2a3b4d; font-size:15px; }
.row:last-child { border-bottom:0; }
.k { color:#8aa0b3; } .v { font-variant-numeric:tabular-nums; font-weight:700; }
.ok { color:#6f6; } .bad { color:#f66; }
table.mm { width:100%; border-collapse:collapse; font-size:14px; }
table.mm th, table.mm td { padding:6px 4px; text-align:right; font-variant-numeric:tabular-nums; }
table.mm th { color:#8aa0b3; font-weight:600; }
table.mm td:first-child, table.mm th:first-child { text-align:left; color:#8aa0b3; font-weight:500; }
table.mm td { font-weight:700; }
.matwrap { overflow-x:auto; margin-top:12px; background:#1c2b3a; border-radius:12px; padding:8px; }
table.mat { width:100%; border-collapse:collapse; font-size:12px; min-width:720px; }
table.mat th, table.mat td { padding:5px 4px; text-align:right; white-space:nowrap; }
table.mat th:first-child, table.mat td:first-child { text-align:left; color:#8aa0b3; position:sticky; left:0; background:#1c2b3a; }
table.mat th { color:#c5d4e0; font-weight:700; }
table.mat .on { background:#243646; }
table.mat .est { display:block; font-size:10px; font-weight:600; color:#8aa0b3; }
.hint { color:#6d8296; font-size:12px; padding:8px 4px 0; }
body.only-days header, body.only-days .grid { display:none !important; }
body.only-days .matwrap { margin:0; border-radius:0; min-height:100dvh; }
@media (max-width: 820px) {
  .side { grid-template-columns: 1fr; }
  .side { grid-template-columns: 1fr; }
}
</style>
</head>
<body>
<header>
  <h1>SmartSolar MPPT 75/15</h1>
  <div class="sub">MQTT <span id="mqtt">-</span> · <span id="age">-</span></div>
</header>
<div class="wrap">
  <div class="grid">
    <div class="schem" id="hero">
      <div class="nodes">
        <div class="node">
          <svg class="ico" viewBox="0 0 56 44"><rect x="6" y="6" width="44" height="28" rx="3" fill="none" stroke="#8fd14f" stroke-width="2"/><path d="M6 20h44M20 6v28M36 6v28" stroke="#8fd14f" fill="none"/></svg>
          <div class="lab">태양광</div>
          <div class="val" id="pv_v">—</div>
        </div>
        <div class="wire"></div>
        <div class="node">
          <div class="pwr"><span id="pwr">—</span> <span class="unit">W</span></div>
        </div>
        <div class="wire"></div>
        <div class="node">
          <svg class="ico" viewBox="0 0 56 44"><rect x="10" y="10" width="32" height="22" rx="3" fill="none" stroke="#6ec1ff" stroke-width="2"/></svg>
          <div class="lab">배터리</div>
          <div class="val" id="bat_v">—</div>
        </div>
      </div>
      <div class="meta">
        <div>배터리 전류<b id="bat_i">—</b></div>
        <div>온도<b id="tmp">—</b></div>
        <div>부하<b id="load_a">—</b></div>
        <div>부하 전력<b id="load_w">—</b></div>
      </div>
      <div class="states" id="states"></div>
    </div>
    <div class="side" id="side">
      <div class="card">
        <div class="row"><span class="k">오늘 수확 ADV</span><span class="v" id="y_td">—</span></div>
        <div class="row"><span class="k">어제 수확</span><span class="v" id="y_yd">—</span></div>
        <div class="row"><span class="k">어제 소비</span><span class="v" id="c_yd">—</span></div>
        <div class="row"><span class="k">어제 최대 P</span><span class="v" id="p_yd">—</span></div>
        <div class="row"><span class="k">어제 최대 Vpv</span><span class="v" id="v_yd">—</span></div>
        <div class="row"><span class="k">GATT PV</span><span class="v" id="gatt_pv">—</span></div>
        <div class="row"><span class="k">부하 ADV</span><span class="v" id="load_row">—</span></div>
      </div>
      <div class="card">
        <div class="row" style="border:0;padding-bottom:4px"><span class="k">보드</span><span></span></div>
        <table class="mm">
          <tr><th></th><th>현재</th><th>최소</th><th>최대</th></tr>
          <tr><td>WiFi</td><td id="w_now">—</td><td id="w_min">—</td><td id="w_max">—</td></tr>
          <tr><td>MPPT BLE</td><td id="m_now">—</td><td id="m_min">—</td><td id="m_max">—</td></tr>
          <tr><td>Sense BLE</td><td id="s_now">—</td><td id="s_min">—</td><td id="s_max">—</td></tr>
          <tr><td>보드 V</td><td id="bv_now">—</td><td id="bv_min">—</td><td id="bv_max">—</td></tr>
        </table>
      </div>
    </div>
  </div>
  <div class="matwrap" id="daymat"><table class="mat"><tr><th></th><th class="on">오늘</th><th class="on">어제</th><th>09/29</th><th>09/28</th><th>09/27</th><th>09/26</th><th>09/25</th><th>09/24</th><th>09/23</th><th>09/22</th><th>09/21</th><th>09/20</th><th>09/19</th><th>09/18</th><th>09/17</th></tr><tr><td>수율 Wh</td><td class="on">0</td><td class="on">120</td><td>130</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td></tr><tr><td>최대 P W</td><td class="on">0</td><td class="on">23</td><td>23</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td></tr><tr><td>최대 Vpv</td><td class="on">1.68</td><td class="on">36.30</td><td>37.20</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td></tr><tr><td>배터리 최대</td><td class="on">12.84</td><td class="on">13.43</td><td>13.48</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td></tr><tr><td>배터리 최소</td><td class="on">12.77</td><td class="on">12.64</td><td>12.62</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td></tr><tr><td>소비 Wh</td><td class="on">10</td><td class="on">100</td><td>110</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td></tr></table></div>
  <div class="hint">15일 · 오늘 소비: 수율 0이면 부하전력 적분 예상, 수율 생기면 어제 비율 추정으로 바뀜.</div>
</div>
<script>
if (new URLSearchParams(location.search).get('only')==='days') document.body.classList.add('only-days');
const STATES = ['off','low_power','fault','bulk','absorption','float'];
const STATE_ALIAS = {
  abs:'absorption', absorption:'absorption',
  flt:'float', float:'float',
  eq:'equalize', equalise:'equalize', equalize:'equalize',
  start:'starting', starting:'starting',
  sto:'storage', storage:'storage',
  lp:'low_power', lowpower:'low_power', low_power:'low_power'
};
let DAYS = [];
let HIST = {wifi:[], mppt:[], sense:[], board:[]};
let LIVE = {ymd:'', yield_kwh:null, consumed_kwh:null, consumed_src:null, pmax_w:null, vpv_max:null, vbat_max:null, vbat_min:null, load_wh:0, load_t:0};

function renderStates(cur){
  const el = document.getElementById('states');
  if (!el) return;
  const raw = (cur||'').toLowerCase();
  const now = STATE_ALIAS[raw] || raw;
  el.innerHTML = STATES.map(s =>
    '<span class="'+(s===now?'on':'')+'">'+s+'</span>'
  ).join('');
}
function n(v){ return (v==null || v==='') ? '—' : v; }
function f(v,d,u){
  if (v==null || v==='') return '—';
  const x = Number(v);
  if (!Number.isFinite(x)) return '—';
  return x.toFixed(d) + (u||'');
}
function push(arr, v){
  const x = Number(v);
  if (!Number.isFinite(x)) return;
  arr.push(x);
  if (arr.length > 2400) arr.splice(0, arr.length-2400);
}
function rssiOk(v){
  const x = Number(v);
  return Number.isFinite(x) && x < 0;
}
function mm(arr){
  if (!arr || !arr.length) return null;
  const vs = arr.filter(x => Number.isFinite(x) && x !== 0);
  if (!vs.length) return null;
  let a=vs[0], b=vs[0];
  for (const x of vs){ if (x<a) a=x; if (x>b) b=x; }
  return [a,b];
}
function set3(id, cur, arr, dp){
  document.getElementById(id+'_now').textContent = f(cur, dp, '');
  const r = mm(arr);
  document.getElementById(id+'_min').textContent = r ? r[0].toFixed(dp) : '—';
  document.getElementById(id+'_max').textContent = r ? r[1].toFixed(dp) : '—';
}
function ymdList(){
  const out=[];
  const fmt = new Intl.DateTimeFormat('en-CA', {timeZone:'Asia/Seoul', year:'numeric', month:'2-digit', day:'2-digit'});
  for (let i=0;i<=14;i++){
    const d = new Date(Date.now() - i*86400000);
    out.push(fmt.format(d).replace(/-/g,''));
  }
  return out;
}
function lab(ymd){
  if (!ymd) return '';
  const keys = ymdList();
  if (String(ymd)===String(keys[0])) return '오늘';
  if (String(ymd)===String(keys[1])) return '어제';
  return String(ymd).slice(4,6)+'/'+String(ymd).slice(6,8);
}
function recOf(ymd){
  const key = String(ymd);
  const stored = DAYS.find(d => String(d.ymd)===key || String(d.day)===key) || {};
  const today = ymdList()[0];
  if (key !== String(today)) return stored;
  const out = Object.assign({}, stored, {ymd:today});
  if (LIVE.yield_wh!=null) out.yield_kwh = Number(LIVE.yield_wh)/1000;
  else if (LIVE.yield_kwh!=null) out.yield_kwh = LIVE.yield_kwh;
  const ty = (out.yield_kwh!=null) ? Number(out.yield_kwh) : ((LIVE.yield_wh!=null)?Number(LIVE.yield_wh)/1000:null);
  const yest = ymdList()[1];
  const yd = DAYS.find(d => String(d.ymd)===String(yest) || String(d.day)===String(yest)) || {};
  const yy = Number(yd.yield_kwh), yc = Number(yd.consumed_kwh);
  if (ty!=null && ty>0 && yy>0 && Number.isFinite(yc)) {
    out.consumed_kwh = ty * (yc/yy);
    out.consumed_src = 'ratio';
  } else if (LIVE.load_wh!=null) {
    out.consumed_kwh = Number(LIVE.load_wh)/1000;
    out.consumed_src = 'load';
  } else if (LIVE.consumed_kwh!=null) {
    out.consumed_kwh = LIVE.consumed_kwh;
    out.consumed_src = LIVE.consumed_src || stored.consumed_src || 'load';
  } else if (stored.consumed_kwh!=null) {
    out.consumed_src = stored.consumed_src || 'load';
  }
  const pmax = LIVE.pmax_w!=null ? LIVE.pmax_w : LIVE.pmax;
  const vpvm = LIVE.vpv_max!=null ? LIVE.vpv_max : LIVE.vpvmax;
  const vbM = LIVE.vbat_max!=null ? LIVE.vbat_max : LIVE.vbmax;
  const vbm = LIVE.vbat_min!=null ? LIVE.vbat_min : LIVE.vbmin;
  if (pmax!=null) out.pmax_w = pmax;
  if (vpvm!=null) out.vpv_max = vpvm;
  if (vbM!=null) out.vbat_max = vbM;
  if (vbm!=null) out.vbat_min = vbm;
  return out;
}
function renderMat(){
  const el = document.getElementById('daymat');
  if (!el) return;
  const keys = ymdList();
  const today = keys[0];
  const yest = keys[1];
  const rows = [
    ['수율 Wh', d => (d.yield_kwh!=null?d.yield_kwh*1000:null), 0],
    ['최대 P W', d => d.pmax_w, 0],
    ['최대 Vpv', d => d.vpv_max, 2],
    ['배터리 최대', d => d.vbat_max, 2],
    ['배터리 최소', d => d.vbat_min, 2],
    ['소비 Wh', d => (d.consumed_kwh!=null?d.consumed_kwh*1000:null), 0, true],
  ];
  let th = '<tr><th></th>';
  keys.forEach(k => {
    const on = (k===today||k===yest) ? ' class="on"' : '';
    th += '<th'+on+'>'+lab(k)+'</th>';
  });
  th += '</tr>';
  let body = '';
  rows.forEach(([name, get, dp]) => {
    body += '<tr><td>'+name+'</td>';
    keys.forEach(k => {
      const rec = recOf(k);
      const v = get(rec);
      const on = (k===today||k===yest) ? ' class="on"' : '';
      let txt = f(v, dp, '');
      if (name==='소비 Wh' && k===today && v!=null){
        txt += rec.consumed_src==='ratio' ? '<div class="est">추정·수율비</div>' :
               rec.consumed_src==='gatt' ? '' : '<div class="est">추정·부하</div>';
      }
      body += '<td'+on+'>'+txt+'</td>';
    });
    body += '</tr>';
  });
  el.innerHTML = '<table class="mat">'+th+body+'</table>';
}
try { renderMat(); } catch (e) { console.log(e); }

async function getJson(url, ms){
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), ms||4000);
  try {
    const r = await fetch(url, {signal: ctl.signal});
    return await r.json();
  } catch(e) { return null; }
  finally { clearTimeout(t); }
}
function setMqtt(text, ok){
  const el=document.getElementById('mqtt');
  el.textContent=text; el.className=ok?'ok':'bad';
}
async function tick(){
  try{
    const s = await getJson('/state', 2500);
    if (!s) return;
    const lag = s.updated ? ((Date.now()/1000)-s.updated) : 99;
    if (s.mqtt!=='ok') setMqtt(s.mqtt||'offline', false);
    else setMqtt('ok', true);
    document.getElementById('age').textContent = lag<99 ? lag.toFixed(0)+'s' : '—';
    const m=s.mppt||{}, b=s.sense||{}, stt=s.status||{}, pv=s.pv||{}, h=s.hist||{};
    const vpv = pv.vpv != null ? pv.vpv : m.vpv;
    const ppv = pv.ppv != null ? pv.ppv : m.power;
    document.getElementById('pv_v').textContent = f(vpv, 2, ' V');
    document.getElementById('pwr').textContent = (ppv==null||ppv==='') ? '—' : Number(ppv).toFixed(0);
    document.getElementById('bat_v').textContent = f(m.vbat ?? b.vbat, 2, ' V');
    document.getElementById('bat_i').textContent = f(m.ibat, 2, ' A');
    document.getElementById('tmp').textContent = f(b.temp ?? m.temp, 1, ' °C');
    const la = m.load_a;
    document.getElementById('load_a').textContent = f(la, 1, ' A');
    {
      const now = Date.now()/1000;
      const vb = Number(m.vbat);
      const a = Number(la);
      if (LIVE.load_t>0 && Number.isFinite(a) && a>=0 && Number.isFinite(vb) && vb>0){
        const dt = Math.min(30, Math.max(0, now-LIVE.load_t));
        LIVE.load_wh = (LIVE.load_wh||0) + (a*vb)*dt/3600;
        try { localStorage.setItem('loadint', JSON.stringify({ymd:LIVE.ymd, wh:LIVE.load_wh})); } catch(e) {}
      }
      if (Number.isFinite(a) && Number.isFinite(vb)) LIVE.load_t = now;
    }
    const lw = (la!=null && m.vbat!=null) ? Number(la)*Number(m.vbat) : null;
    document.getElementById('load_w').textContent = f(lw, 0, ' W');
    if (document.getElementById('load_row')) document.getElementById('load_row').textContent = (la==null)?'—':(f(la,1,' A')+' / '+f(lw,0,' W'));
    renderStates(m.state);
    const daysKeys = ymdList();
    const today = daysKeys[0];
    if (LIVE.ymd !== today) {
      let saved = 0;
      try {
        const o = JSON.parse(localStorage.getItem('loadint')||'{}');
        if (String(o.ymd)===String(today)) saved = Number(o.wh)||0;
      } catch(e) {}
      LIVE = {ymd:today, yield_wh:null, yield_kwh:null, consumed_kwh:null, consumed_src:null,
              pmax:null, pmax_w:null, vpvmax:null, vpv_max:null,
              vbmax:null, vbmin:null, vbat_max:null, vbat_min:null,
              load_wh:saved, load_t:0};
    }
    if (m.yield_wh != null) {
      LIVE.yield_wh = Number(m.yield_wh);
      LIVE.yield_kwh = Number(m.yield_wh)/1000;
    }
    const yday = recOf(daysKeys[1]);
    const yY = Number(yday.yield_kwh), yC = Number(yday.consumed_kwh);
    if (LIVE.consumed_kwh==null && LIVE.yield_kwh!=null && yY>0 && Number.isFinite(yC)) {
      LIVE.consumed_kwh = LIVE.yield_kwh * (yC / yY);
      LIVE.consumed_est = true;
    }
    if (h.kind==='today'){
      if (h.consumed_kwh!=null) LIVE.consumed_kwh = h.consumed_kwh;
      if (h.pmax_w!=null) LIVE.pmax_w = Math.max(LIVE.pmax_w||0, h.pmax_w);
      if (h.vpv_max!=null) LIVE.vpv_max = Math.max(LIVE.vpv_max||0, h.vpv_max);
    }
    if (m.power!=null) LIVE.pmax_w = Math.max(LIVE.pmax_w||0, Number(m.power));
    if (pv.ppv!=null) LIVE.pmax_w = Math.max(LIVE.pmax_w||0, Number(pv.ppv));
    if (pv.vpv!=null) LIVE.vpv_max = Math.max(LIVE.vpv_max||0, Number(pv.vpv));
    const batv = m.vbat!=null ? Number(m.vbat) : (b.vbat!=null ? Number(b.vbat) : null);
    if (batv!=null && Number.isFinite(batv)){
      LIVE.vbat_max = LIVE.vbat_max==null ? batv : Math.max(LIVE.vbat_max, batv);
      LIVE.vbat_min = LIVE.vbat_min==null ? batv : Math.min(LIVE.vbat_min, batv);
    }
    LIVE.pmax = LIVE.pmax_w; LIVE.vpvmax = LIVE.vpv_max;
    LIVE.vbmax = LIVE.vbat_max; LIVE.vbmin = LIVE.vbat_min;
    document.getElementById('y_td').textContent = f(m.yield_wh, 0, ' Wh');
    const yrec = recOf(daysKeys[1]);
    const fromH = (h.kind==='yesterday') ? h : yrec;
    document.getElementById('y_yd').textContent = f(fromH.yield_kwh!=null?fromH.yield_kwh*1000:null, 0, ' Wh');
    document.getElementById('c_yd').textContent = f(fromH.consumed_kwh!=null?fromH.consumed_kwh*1000:null, 0, ' Wh');
    document.getElementById('p_yd').textContent = f(fromH.pmax_w, 0, ' W');
    document.getElementById('v_yd').textContent = f(fromH.vpv_max, 2, ' V');
    document.getElementById('gatt_pv').textContent =
      (pv.vpv!=null || pv.ppv!=null) ? (f(pv.vpv,2,' V')+' / '+f(pv.ppv,2,' W')) : '—';
    const wr = stt.wifi_rssi, mrss = stt.mppt_rssi != null ? stt.mppt_rssi : m.rssi;
    const srss = stt.sense_rssi != null ? stt.sense_rssi : b.rssi;
    if (rssiOk(wr)) push(HIST.wifi, wr);
    if (rssiOk(mrss)) push(HIST.mppt, mrss);
    if (rssiOk(srss)) push(HIST.sense, srss);
    if (stt.vbat!=null) push(HIST.board, stt.vbat);
    set3('w', rssiOk(wr)?wr:null, HIST.wifi, 0);
    set3('m', rssiOk(mrss)?mrss:null, HIST.mppt, 0);
    set3('s', rssiOk(srss)?srss:null, HIST.sense, 0);
    set3('bv', stt.vbat, HIST.board, 2);
    if (s.days) {
      DAYS = Object.keys(s.days).map(k => Object.assign({ymd:k}, s.days[k]));
    }
    renderMat();
  } catch(e) {}
}

async function loadDays(){
  const s = await getJson('/days', 4000);
  if (s && s.days){
    DAYS = Object.keys(s.days).map(k => Object.assign({ymd:k}, s.days[k]));
    renderMat();
  }
}
try { renderMat(); } catch (e) {}
renderMat();
loadDays();
tick();
setInterval(tick, 2000);
(function(){
  const q = new URLSearchParams(location.search);
  const only = q.get('only') || q.get('view') || '';
  if (only==='days' || only==='table' || only==='map'){
    const h=document.getElementById('hero');
    const s=document.getElementById('side');
    const hd=document.querySelector('header');
    if (h) h.style.display='none';
    if (s) s.style.display='none';
    if (hd) hd.style.display='none';
    document.body.style.background='#15202b';
  } else if (only==='top'){
    const m=document.getElementById('daymat');
    const hint=document.querySelector('.hint');
    if (m) m.style.display='none';
    if (hint) hint.style.display='none';
  }
})();

</script>
</body>
</html>
"""


class H(BaseHTTPRequestHandler):
    def log_message(self, *_a) -> None:
        return

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path == "/state":
            with state_lock:
                snap = dict(state)
            days = _load_days_store()
            days.update(snap.get("days") or {})
            snap["days"] = days
            body = json.dumps(snap).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
            return
        if path == "/days":
            with state_lock:
                days = dict(state.get("days") or {})
            extra = _load_days_store()
            extra.update(days)
            body = json.dumps({"days": extra}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
            return
        if path == "/days":
            days = {}
            if DAYS_FILE.is_file():
                try:
                    days = json.loads(DAYS_FILE.read_text(encoding="utf-8"))
                except Exception:
                    days = {}
            with state_lock:
                if state.get("days"):
                    days = dict(state["days"])
            body = json.dumps({"days": days}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
            return
        if path == "/hist":
            key = (parse_qs(urlparse(self.path).query).get("range") or ["24h"])[0]
            if key not in RANGES:
                key = "24h"
            try:
                payload = _query_hist(key)
                with state_lock:
                    state["influx"] = "ok"
                    gh = dict(state.get("hist") or {})
                payload["gatt"] = gh
                days = list(payload.get("days") or [])
                by = {d.get("day"): d for d in days if d.get("day")}
                def _put(day, y, c, pmax, vmax, vmin, pvmax):
                    if not day:
                        return
                    d = by.get(day) or {"day": day}
                    if y is not None:
                        d["wh"] = y
                        d["yield_wh"] = y
                    if c is not None:
                        d["cwh"] = c
                        d["load_wh"] = c
                        d["cons_wh"] = c
                    if pmax:
                        d["pmax"] = pmax
                    if vmax is not None:
                        d["vmax"] = vmax
                    if vmin is not None:
                        d["vmin"] = vmin
                    if pvmax is not None:
                        d["pvmax"] = pvmax
                    d["src"] = "gatt"
                    by[day] = d
                _put(gh.get("day"), gh.get("yield_td_wh"), gh.get("cons_td_wh"),
                     gh.get("max_td_w"), gh.get("vmax_td"), gh.get("vmin_td"),
                     gh.get("vpvmax_td"))
                _put(gh.get("yesterday"), gh.get("yield_yd_wh"), gh.get("cons_yd_wh"),
                     gh.get("max_yd_w"), gh.get("vmax_yd"), gh.get("vmin_yd"),
                     gh.get("vpvmax_yd"))
                payload["days"] = [by[k] for k in sorted(by)]
                payload["total_kwh"] = gh.get("total_kwh")
                payload["user_kwh"] = gh.get("user_kwh")
                payload["pv_v"] = gh.get("pv_v")
                payload["pv_i"] = gh.get("pv_i")
            except Exception as e:
                with state_lock:
                    state["influx"] = "err"
                payload = {
                    "range": key, "t0": time.time()-RANGES[key], "t1": time.time(),
                    "mppt_v": [], "mppt_p": [], "sns_v": [], "sns_t": [],
                    "days": [], "n": 0, "err": str(e)[:180],
                }
            body = json.dumps(payload).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
            return
        body = HTML.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    tok = _influx_token()
    print("influx", INFLUX_URL, "org", INFLUX_ORG, "bucket", INFLUX_BUCKET,
          "token", "ok" if tok else "MISSING")
    threading.Thread(target=mqtt_thread, daemon=True).start()
    httpd = ThreadingHTTPServer((HTTP_HOST, HTTP_PORT), H)
    print(f"victron      http://127.0.0.1:{HTTP_PORT}")
    try:
        webbrowser.open(f"http://127.0.0.1:{HTTP_PORT}")
    except Exception:
        pass
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nstop")
    finally:
        try:
            httpd.shutdown()
        except Exception:
            pass
        httpd.server_close()


if __name__ == "__main__":
    main()

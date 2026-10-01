#!/usr/bin/env python3
"""Victron view — live MQTT, history InfluxDB, zoom charts.

    cp .env.example .env   # MQTT_HOST/MQTT_USER/MQTT_PASS, INFLUX_* (or export them)
    python3 victron_mqtt_view.py
    # Influx token: INFLUX_TOKEN in .env / env, or ./influx.token
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
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import paho.mqtt.client as mqtt

# MQTT_* / INFLUX_* / HTTP_* come from the environment or a .env file (see .env.example),
# read by _load_influx_env() below

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
        HERE / ".env",
        Path.cwd() / ".env",
        HERE / "influx.env",
        HERE / "influx.token",
        Path.cwd() / "influx.env",
        Path.cwd() / "influx.token",
        Path.home() / "local-web-mqtt" / "influx.env",
        Path.home() / "local-web-mqtt" / "influx.token",
        Path.home() / "influx.env",
        Path.home() / "influx.token",
        Path.home() / "influxdb-cfg" / "token",
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
MQTT_HOST = os.environ.get("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USER", "")
MQTT_PASS = os.environ.get("MQTT_PASS", "")
HTTP_HOST = os.environ.get("HTTP_HOST", "0.0.0.0")
HTTP_PORT = int(os.environ.get("HTTP_PORT", "8772"))
INFLUX_URL = os.environ.get("INFLUX_URL", "http://127.0.0.1:8086").rstrip("/")
INFLUX_ORG = os.environ.get("INFLUX_ORG", "yard")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET", "mower")
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


# ---- statuspage2: seq-aware history day store ----
# The Victron rolls its history day when PV goes dark in the evening (not at midnight), so a
# wall-clock-labelled firmware publishes the NEW day0 (higher seq) under the old date. Records are
# therefore placed by the device day sequence number "seq" (0..364, wraps), not blindly by "ymd".
SEQ_MOD = 365


def _ymd_int(v: Any) -> int | None:
    try:
        i = int(v)
    except (TypeError, ValueError):
        return None
    if not 19000101 <= i <= 29991231:
        return None
    try:
        datetime.strptime(str(i), "%Y%m%d")
    except ValueError:
        return None
    return i


def _ymd_add(ymd: int, n: int) -> int:
    d = datetime.strptime(str(ymd), "%Y%m%d") + timedelta(days=n)
    return int(d.strftime("%Y%m%d"))


def _ymd_gap(a: int, b: int) -> int:
    """calendar days a - b"""
    return (datetime.strptime(str(a), "%Y%m%d") - datetime.strptime(str(b), "%Y%m%d")).days


def _seq_of(rec: Any) -> int | None:
    if not isinstance(rec, dict):
        return None
    s = rec.get("seq")
    if s is None or isinstance(s, bool):
        return None
    try:
        return int(s)
    except (TypeError, ValueError):
        return None


def _seq_diff(a: int, b: int) -> int:
    """a - b on the 365-day ring, in -182..182"""
    d = (a - b) % SEQ_MOD
    return d - SEQ_MOD if d > SEQ_MOD // 2 else d


def _is_final(rec: Any) -> bool:
    """day1..N records are closed days (authoritative); day0 / kind today is still open"""
    if not isinstance(rec, dict):
        return False
    try:
        if int(rec.get("day") or 0) >= 1:
            return True
    except (TypeError, ValueError):
        pass
    return rec.get("kind") not in (None, "today")


def _trusted(rec: Any) -> bool:
    return _is_final(rec) or (isinstance(rec, dict) and rec.get("label") == "seq")


def _wall_ymd() -> int:
    return int(datetime.now(KST).strftime("%Y%m%d"))


def _put_moved(days: dict, rec: dict, new_ymd: int, fw_ymd: int) -> None:
    o = dict(rec)
    o.setdefault("ymd_fw", fw_ymd)        # the date the firmware put on it
    o["ymd"] = new_ymd
    days[str(new_ymd)] = o


def _drop_dup_seq(days: dict, key: str, rec: dict) -> None:
    """a trusted record owns its seq: remove untrusted copies of that seq within +-7 days"""
    s = _seq_of(rec)
    y = _ymd_int(key)
    if s is None or y is None or not _trusted(rec):
        return
    for k in list(days.keys()):
        if k == key:
            continue
        ky = _ymd_int(k)
        if ky is None or abs(_ymd_gap(ky, y)) > 7:
            continue
        o = days[k]
        if _seq_of(o) == s and not _trusted(o):
            del days[k]


def _hist_store(days: dict, obj: dict) -> tuple[str | None, str]:
    """place one victron/mppt/hist record into days (mutated). returns (key or None, action)"""
    ymd = _ymd_int(obj.get("ymd"))
    if ymd is None:
        return None, "bad-ymd"
    seq = _seq_of(obj)
    key = str(ymd)
    cur = days.get(key)
    cs = _seq_of(cur)
    if seq is None:
        # old firmware / EDDx fallback without seq: never replaces a record that has a seq
        if cur is not None and cs is not None:
            return None, "skip-noseq"
        days[key] = obj
        return key, "store"
    if cur is None or cs is None:
        days[key] = obj
        _drop_dup_seq(days, key, obj)
        return key, "store"
    d = _seq_diff(seq, cs)
    if d == 0:
        days[key] = obj
        _drop_dup_seq(days, key, obj)
        return key, "update"
    if d > 0:
        # same date, newer device day: evening rollover labelled with the old date -> ymd + d
        tymd = _ymd_add(ymd, d)
        tkey = str(tymd)
        t = days.get(tkey)
        ts = _seq_of(t)
        if t is not None and ts is not None and ts != seq:
            return None, "skip-target-has-other-seq"
        # target empty / without seq / same seq (newer values of the same device day) -> store
        _put_moved(days, obj, tymd, ymd)
        return tkey, "shift"
    # d < 0: an older device day for a date that holds a newer one
    if _is_final(obj):
        # a closed day record (day>=1) is authoritative: the stored newer one was mislabelled,
        # move it forward by the seq gap unless the target already holds that day or a newer one
        tymd = _ymd_add(ymd, -d)
        tkey = str(tymd)
        t = days.get(tkey)
        ts = _seq_of(t)
        if t is None or ts is None or _seq_diff(cs, ts) > 0:
            _put_moved(days, cur, tymd, _ymd_int(cur.get("ymd_fw")) or ymd)
        days[key] = obj
        return key, "repair"
    return None, "skip-older"


def _repair_days(days: dict, wall: int | None = None) -> list[str]:
    """repair a stored day map in place (e.g. victron_days.json written by the old viewer).
    an open (kind today) record whose seq is further ahead of the previous stored day than the
    calendar gap was labelled with a too-early date -> move it forward by the excess.
    duplicates of one seq: the trusted (final or label=seq) copy wins."""
    wall = wall or _wall_ymd()
    notes: list[str] = []
    changed = True
    rounds = 0
    while changed and rounds < 5:
        changed = False
        rounds += 1
        keys = sorted(k for k in days if _ymd_int(k) is not None and _seq_of(days[k]) is not None)
        for i in range(1, len(keys)):
            a, b = keys[i - 1], keys[i]
            ra, rb = days.get(a), days.get(b)
            if ra is None or rb is None:
                continue
            sa, sb = _seq_of(ra), _seq_of(rb)
            ya, yb = int(a), int(b)
            gap = _ymd_gap(yb, ya)
            if gap > 7:
                continue
            ds = _seq_diff(sb, sa)
            if ds == 0:
                # same device day twice: keep the trusted one, else the later date
                loser = a if (_trusted(rb) or not _trusted(ra)) else b
                notes.append(f"dup seq {sa}: drop {loser}")
                del days[loser]
                changed = True
                break
            if ds > gap and not _is_final(rb):
                tymd = _ymd_add(ya, ds)
                if _ymd_gap(tymd, wall) > 1:
                    continue
                tkey = str(tymd)
                t = days.get(tkey)
                ts = _seq_of(t)
                if t is None or ts is None:
                    _put_moved(days, rb, tymd, yb)
                    notes.append(f"move seq {sb} {b} -> {tkey}")
                elif ts == sb:
                    notes.append(f"seq {sb} already at {tkey}: drop {b}")
                else:
                    continue
                del days[b]
                changed = True
                break
    return notes


def _dev_today(days: dict, wall: int | None = None) -> int | None:
    """device's current day = record with the highest seq near the wall date (wall-1..wall+1)"""
    wall = wall or _wall_ymd()
    best = None
    for k, r in days.items():
        y = _ymd_int(k)
        s = _seq_of(r)
        if y is None or s is None or abs(_ymd_gap(y, wall)) > 1:
            continue
        if best is None or _seq_diff(s, best[1]) > 0 or (s == best[1] and y > best[0]):
            best = (y, s)
    return best[0] if best else None


def _save_days(days: dict) -> None:
    try:
        tmp = DAYS_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(days, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(DAYS_FILE)
    except OSError:
        pass


def _days_view(state_days: dict) -> dict:
    """/state and /days: on-disk stores + live state (state wins), repaired"""
    days = _load_days_store()
    days.update(state_days or {})
    _repair_days(days)
    return days


_load_board()
state["days"] = _load_days_store()
if DAYS_FILE.is_file():
    try:
        d = json.loads(DAYS_FILE.read_text(encoding="utf-8"))
        if isinstance(d, dict):
            state["days"] = d
    except Exception:
        pass
_rep_notes = _repair_days(state["days"])
if _rep_notes:
    print("days repair:", "; ".join(_rep_notes))
    _save_days(state["days"])
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
    if vb is not None and vb <= 0.5:          # statuspage3: 0.00 = no ADC reading
        vb = None
    _board_q.append([now, wifi, mp, se, vb])
    cut = now - BOARD_KEEP
    while _board_q and _board_q[0][0] < cut:
        _board_q.popleft()
    if len(_board_q) % 4 == 0:
        _save_board()


# ---- victron/status history: numeric fields exactly as received (no Influx, no interpolation) ----
STATUS_HIST_FILE = HERE / "victron_status_hist.json"
STATUS_KEEP = 24 * 3600
STATUS_MAX = 1500
STATUS_RANGES: dict[str, float] = {"1h": 3600, "6h": 6 * 3600, "24h": 24 * 3600}
_status_q: deque[dict] = deque()
_status_lock = threading.Lock()
_status_saved = 0.0


def _sig_ok(field: str, v: Any) -> bool:
    """statuspage3: RSSI 0 / >= 0 / <= -127 = no reading; board vbat <= 0.5 V = no reading"""
    if v is None or isinstance(v, bool):
        return False
    try:
        x = float(v)
    except (TypeError, ValueError):
        return False
    if x != x:
        return False
    if field.endswith("rssi"):
        return -127.0 < x < 0.0
    if field in ("vbat", "board_v"):
        return x > 0.5
    return True


MM_FIELDS = (("wifi", "wifi_rssi"), ("mppt", "mppt_rssi"), ("sense", "sense_rssi"), ("board", "vbat"))
MM_NOW_MAX_AGE = 150.0     # "현재" only from a status received within this many seconds


def _board_mm(span: float) -> dict[str, Any]:
    """main-page panel: current/min/max per field over the last `span` s of received
    victron/status messages (same source as /status), 0/null excluded"""
    now = time.time()
    with _status_lock:
        pts = [p for p in _status_q if p["rx"] >= now - span]
    last_rx = pts[-1]["rx"] if pts else None
    out: dict[str, Any] = {"span": span, "n": len(pts), "last_rx": last_rx}
    for key, field in MM_FIELDS:
        vals = [float(p["d"][field]) for p in pts if _sig_ok(field, p["d"].get(field))]
        cur = None
        if pts and now - pts[-1]["rx"] <= MM_NOW_MAX_AGE and _sig_ok(field, pts[-1]["d"].get(field)):
            cur = float(pts[-1]["d"][field])
        out[key] = {"now": cur, "min": min(vals) if vals else None,
                    "max": max(vals) if vals else None, "n": len(vals)}
    return out


def _status_nums(obj: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    for k, v in obj.items():
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            continue
        if v != v or v in (float("inf"), float("-inf")):
            continue
        out[str(k)] = v
    return out


def _load_status_hist() -> None:
    if not STATUS_HIST_FILE.is_file():
        return
    cut = time.time() - STATUS_KEEP
    try:
        obj = json.loads(STATUS_HIST_FILE.read_text(encoding="utf-8"))
    except Exception:
        return
    pts = obj.get("pts") if isinstance(obj, dict) else None
    if not isinstance(pts, list):
        return
    with _status_lock:
        for p in pts:
            if isinstance(p, dict) and isinstance(p.get("rx"), (int, float)) and p["rx"] >= cut \
                    and isinstance(p.get("d"), dict):
                _status_q.append({"rx": float(p["rx"]), "d": _status_nums(p["d"])})
        while len(_status_q) > STATUS_MAX:
            _status_q.popleft()


def _save_status_hist() -> None:
    global _status_saved
    with _status_lock:
        pts = list(_status_q)
    try:
        tmp = STATUS_HIST_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps({"v": 1, "pts": pts}, separators=(",", ":")), encoding="utf-8")
        tmp.replace(STATUS_HIST_FILE)
        _status_saved = time.time()
    except OSError:
        pass


def _push_status(obj: dict[str, Any], now: float) -> None:
    d = _status_nums(obj)
    if not d:
        return
    with _status_lock:
        _status_q.append({"rx": now, "d": d})
        cut = now - STATUS_KEEP
        while _status_q and (_status_q[0]["rx"] < cut or len(_status_q) > STATUS_MAX):
            _status_q.popleft()
        n = len(_status_q)
    if n % 10 == 0 or now - _status_saved > 300:
        _save_status_hist()


def _status_hist(key: str) -> dict[str, Any]:
    span = STATUS_RANGES.get(key, STATUS_RANGES["24h"])
    now = time.time()
    t0 = now - span
    with _status_lock:
        pts = [p for p in _status_q if p["rx"] >= t0]
    fields: list[str] = []
    seen: set[str] = set()
    for p in pts:
        for k in p["d"]:
            if k not in seen:
                seen.add(k)
                fields.append(k)
    def _cell(k: str, v: Any) -> Any:
        if (k.endswith("rssi") or k == "vbat") and not _sig_ok(k, v):
            return None                     # statuspage3: 0 = no reading → no point
        return v
    cols = {k: [_cell(k, p["d"].get(k)) for p in pts] for k in fields}
    return {"range": key, "t0": t0, "t1": now, "n": len(pts),
            "rx": [round(p["rx"], 3) for p in pts], "f": cols}


_load_status_hist()


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
        Path.home() / "local-web-mqtt" / "influx.token",
        Path.home() / "local-web-mqtt" / "influx.env",
        Path.home() / "influxdb-cfg" / "token",
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
    for k in ("wifi_rssi", "mppt_rssi", "sense_rssi", "board_v"):
        if out.get(k):                        # statuspage3: 0 rssi / 0 V = no reading
            out[k] = [p for p in out[k] if _sig_ok(k, p[1])]
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


_pending_logs: list[str] = []


def on_message(_c, _u, msg: mqtt.MQTTMessage) -> None:
    _on_message_impl(_c, _u, msg)
    while _pending_logs:
        _log(_pending_logs.pop(0))


def _on_message_impl(_c, _u, msg: mqtt.MQTTMessage) -> None:
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
            if isinstance(obj, dict):
                _push_status(obj, now)
            state["board_pts"] = list(_board_q)
        elif msg.topic == TOPIC_HIST:
            obj["_ts"] = _ts()
            if not isinstance(obj, dict) or not obj.get("ymd"):
                state["hist"] = obj
            else:
                days = dict(state.get("days") or {})
                key, act = _hist_store(days, obj)
                # state["hist"] carries the date the record was actually stored under
                state["hist"] = days[key] if key else obj
                if key:
                    state["days"] = days
                    _save_days(days)
                if act not in ("store", "update"):
                    # _log takes state_lock (not re-entrant) -> logged by on_message after release
                    _pending_logs.append(f"hist {obj.get('kind')} ymd={obj.get('ymd')} seq={obj.get('seq')} -> {act} {key or ''}")
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
    if MQTT_USER:
        c.username_pw_set(MQTT_USER, MQTT_PASS or None)
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
table.mat th, table.mat td { padding:5px 4px; text-align:right; white-space:nowrap; vertical-align:bottom; }
table.mat th:first-child, table.mat td:first-child { text-align:left; color:#8aa0b3; position:sticky; left:0; background:#1c2b3a; }
table.mat th { color:#c5d4e0; font-weight:700; }
table.mat .on { background:#243646; }
.stcell { position:relative; display:flex; justify-content:center; overflow:visible; }
.stcell:hover .sttip { display:block; }
.stcell.flip .sttip { left:auto; right:calc(50% + 16px); }
.sttip {
  display:none; position:absolute; left:calc(50% + 16px); bottom:12px;
  transform:none; z-index:8;
  background:#1a2733; border:1px solid #5b8fb8; border-radius:8px;
  padding:6px 8px; font-size:11px; line-height:1.45; white-space:nowrap;
  color:#e8eef4; box-shadow:0 6px 16px rgba(0,0,0,.35); pointer-events:none;
}
.sttip b { font-variant-numeric:tabular-nums; }
.sttrack { height:120px; display:flex; align-items:flex-end; justify-content:center; position:relative; }
.stbar { width:28px; min-height:3px; display:flex; flex-direction:column-reverse; background:#243646; border-radius:3px 3px 0 0; overflow:hidden; }
.stbar.empty { background:#1a2733; }
.stbar .stb { background:#d9dde2; width:100%; }
.stbar .sta { background:#9eb4c7; width:100%; }
.stbar .stf { background:#5b8fb8; width:100%; }
.stpct { display:block; font-size:10px; font-weight:600; color:#8aa0b3; }
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
  <div class="sub">MQTT <span id="mqtt">-</span> · <span id="age">-</span> · <a href="/status" style="color:#6ec1ff;text-decoration:none">상태 그래프</a></div>
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
        <div class="row" style="border:0;padding-bottom:4px"><span class="k">보드</span><span class="k" id="mmwin" style="cursor:pointer" title="최소/최대 기간 = 수신한 victron/status (클릭: 1h ↔ 24h). RSSI 0 / 0 V 는 제외">최근 1h</span></div>
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
  <div class="matwrap" id="daymat"></div>
  <div class="hint">최근 15일 · 막대에 올리면 벌크/흡수/플로트 시간과 비율. 오류 0,0,0,0 은 0.</div>
  <div class="matwrap" id="daymat2"></div>
  <div class="hint">그 이전 15일 (day16–30). 오류 0,0,0,0 은 0, 난 코드만 표시.</div>
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
let DEV = null;   /* device's current history day (highest seq), from the server */
let HIST = {wifi:[], mppt:[], sense:[], board:[]};
let LIVE = {ymd:'', yield_kwh:null, consumed_kwh:null, pmax_w:null, vpv_max:null, vbat_max:null, vbat_min:null};

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
/* statuspage3: panel values come from the server (received victron/status history, same as /status) */
let MMWIN = localStorage.getItem('mm_win') || '1h';
function setMM(id, o, dp){
  o = o || {};
  document.getElementById(id+'_now').textContent = f(o.now, dp, '');
  document.getElementById(id+'_min').textContent = f(o.min, dp, '');
  document.getElementById(id+'_max').textContent = f(o.max, dp, '');
}
function showMM(s){
  const el = document.getElementById('mmwin');
  if (el) el.textContent = '최근 ' + MMWIN;
  const bm = (s && s.board_mm) ? s.board_mm[MMWIN] : null;
  if (!bm) return;
  setMM('w', bm.wifi, 0); setMM('m', bm.mppt, 0); setMM('s', bm.sense, 0); setMM('bv', bm.board, 2);
}
let LAST_STATE = null;
(function(){
  const el = document.getElementById('mmwin');
  if (el) el.addEventListener('click', () => {
    MMWIN = (MMWIN === '1h') ? '24h' : '1h';
    localStorage.setItem('mm_win', MMWIN);
    showMM(LAST_STATE);
  });
})();
function set3(id, cur, arr, dp){
  document.getElementById(id+'_now').textContent = f(cur, dp, '');
  const r = mm(arr);
  document.getElementById(id+'_min').textContent = r ? r[0].toFixed(dp) : '—';
  document.getElementById(id+'_max').textContent = r ? r[1].toFixed(dp) : '—';
}
function ymdList(n, off){
  n = (n==null) ? 15 : n;
  off = off||0;
  const out=[];
  const fmt = new Intl.DateTimeFormat('en-CA', {timeZone:'Asia/Seoul', year:'numeric', month:'2-digit', day:'2-digit'});
  for (let i=off;i<off+n;i++){
    const d = new Date(Date.now() - i*86400000);
    out.push(fmt.format(d).replace(/-/g,''));
  }
  return out;
}
function ymdPrev(ymd){
  const s = String(ymd);
  const d = new Date(Date.UTC(+s.slice(0,4), +s.slice(4,6)-1, +s.slice(6,8)) - 86400000);
  return d.toISOString().slice(0,10).replace(/-/g,'');
}
/* "today" column = the record with the highest seq (device day). after the Victron's evening
   rollover that is tomorrow's date; the wall-clock date then keeps its final values. */
function devToday(){
  const w = ymdList()[0];
  const d = DEV ? String(DEV) : '';
  return (d && (d===w || ymdPrev(d)===w || ymdPrev(w)===d)) ? d : w;
}
function lab(ymd){
  if (!ymd) return '';
  const keys = ymdList();
  if (String(ymd)===devToday() && String(ymd) > String(keys[0])) return '새 날';
  if (String(ymd)===String(keys[0])) return '오늘';
  if (String(ymd)===String(keys[1])) return '어제';
  return String(ymd).slice(4,6)+'/'+String(ymd).slice(6,8);
}
function recOf(ymd){
  const key = String(ymd);
  const stored = DAYS.find(d => String(d.ymd)===key || String(d.day)===key) || {};
  const today = devToday();
  if (key !== String(today)) return stored;
  const out = Object.assign({}, stored, {ymd:today});
  if (LIVE.yield_wh!=null) out.yield_kwh = Number(LIVE.yield_wh)/1000;
  else if (LIVE.yield_kwh!=null) out.yield_kwh = LIVE.yield_kwh;
  /* consumption: history (victron/mppt/hist day0) only — no estimate */
  if (LIVE.consumed_kwh!=null) out.consumed_kwh = LIVE.consumed_kwh;
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
function errTxt(v){
  let arr = [];
  if (v==null || v==='') return '0';
  if (Array.isArray(v)) arr = v.map(Number);
  else if (typeof v === 'string') arr = v.split(/[,\s]+/).map(Number);
  else arr = [Number(v)];
  const nz = arr.filter(x => Number.isFinite(x) && x !== 0);
  return nz.length ? nz.join(',') : '0';
}
function fmtMin(m){
  if (m==null || m==='') return '—';
  m = Number(m);
  if (!Number.isFinite(m) || m < 0) return '—';
  if (m < 60) return Math.round(m) + 'm';
  const h = Math.floor(m/60), r = Math.round(m % 60);
  return r ? (h + '시간 ' + r + '분') : (h + '시간');
}
function stageParts(d){
  const b = Number(d.bulk_min), a = Number(d.abs_min), fl = Number(d.float_min);
  const bb = Number.isFinite(b) ? b : 0;
  const aa = Number.isFinite(a) ? a : 0;
  const ff = Number.isFinite(fl) ? fl : 0;
  const t = bb + aa + ff;
  return {b:bb, a:aa, f:ff, t:t};
}
function stageBar(d, flip){
  const s = stageParts(d);
  const y = (d.yield_kwh!=null) ? Number(d.yield_kwh)*1000 : 0;
  const h = Math.max((s.t || y) ? 8 : 3, Math.min(100, (y/250)*100));
  if (!s.t) return '<div class="stcell"><div class="sttrack"><div class="stbar empty" style="height:'+h+'%"></div></div></div>';
  const tip = '<div>벌크충전 <b>'+fmtMin(s.b)+'</b> '+Math.round(100*s.b/s.t)+'%</div>'
            + '<div>흡수충전 <b>'+fmtMin(s.a)+'</b> '+Math.round(100*s.a/s.t)+'%</div>'
            + '<div>플로트 <b>'+fmtMin(s.f)+'</b> '+Math.round(100*s.f/s.t)+'%</div>';
  return '<div class="stcell'+(flip?' flip':'')+'">'
    + '<div class="sttrack"><div class="stbar" style="height:'+h+'%">'
    + '<span class="stb" style="height:'+(100*s.b/s.t).toFixed(1)+'%"></span>'
    + '<span class="sta" style="height:'+(100*s.a/s.t).toFixed(1)+'%"></span>'
    + '<span class="stf" style="height:'+(100*s.f/s.t).toFixed(1)+'%"></span>'
    + '</div></div>'
    + '<div class="sttip">'+tip+'</div>'
    + '</div>';
}
function matTable(keys){
  const today = ymdList()[0];
  const yest = ymdList()[1];
  const dev = devToday();
  const rows = [
    ['충전단계', (d, i, n) => stageBar(d, i >= n - 3), 'html'],
    ['수율 Wh', d => (d.yield_kwh!=null?d.yield_kwh*1000:null), 0],
    ['최대 P W', d => d.pmax_w, 0],
    ['최대 Vpv', d => d.vpv_max, 2],
    ['배터리 최대', d => d.vbat_max, 2],
    ['배터리 최소', d => d.vbat_min, 2],
    ['최대 Ibat', d => d.ibat_max, 1],
    ['소비 Wh', d => (d.consumed_kwh!=null?d.consumed_kwh*1000:null), 0],
    ['오류', d => d.err, 'err'],
  ];
  let th = '<tr><th></th>';
  keys.forEach(k => {
    const on = (k===today||k===yest||k===dev) ? ' class="on"' : '';
    th += '<th'+on+'>'+lab(k)+'</th>';
  });
  th += '</tr>';
  let body = '';
  rows.forEach(([name, get, dp]) => {
    body += '<tr><td>'+name+'</td>';
    keys.forEach((k, i) => {
      const rec = recOf(k);
      const v = (dp==='html') ? get(rec, i, keys.length) : get(rec);
      const on = (k===today||k===yest||k===dev) ? ' class="on"' : '';
      let txt;
      if (dp==='html') txt = v;
      else if (dp==='err') txt = errTxt(v);
      else txt = f(v, dp, '');
      body += '<td'+on+'>'+txt+'</td>';
    });
    body += '</tr>';
  });
  return '<table class="mat">'+th+body+'</table>';
}
function bindTips(root){
  if (!root) return;
  root.querySelectorAll('.stcell').forEach(el => {
    el.addEventListener('mouseenter', () => {
      const tip = el.querySelector('.sttip');
      if (!tip) return;
      el.classList.remove('flip');
      tip.style.display = 'block';
      const wrap = el.closest('.matwrap');
      const wr = wrap ? wrap.getBoundingClientRect() : {right: window.innerWidth};
      const tr = tip.getBoundingClientRect();
      if (tr.right > wr.right - 6) el.classList.add('flip');
      tip.style.display = '';
    });
  });
}
function renderMat(){
  const el = document.getElementById('daymat');
  const el2 = document.getElementById('daymat2');
  const k1 = ymdList(15, 0);
  const dv = devToday();
  if (dv > k1[0]) k1.unshift(dv);           /* device already rolled to tomorrow's date */
  if (el) el.innerHTML = matTable(k1);
  if (el2) el2.innerHTML = matTable(ymdList(16, 15));
  bindTips(el); bindTips(el2);
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
    const lw = (la!=null && m.vbat!=null) ? Number(la)*Number(m.vbat) : null;
    document.getElementById('load_w').textContent = f(lw, 0, ' W');
    if (document.getElementById('load_row')) document.getElementById('load_row').textContent = (la==null)?'—':(f(la,1,' A')+' / '+f(lw,0,' W'));
    renderStates(m.state);
    if (s.dev_ymd) DEV = String(s.dev_ymd);
    const daysKeys = ymdList();
    const today = devToday();
    if (LIVE.ymd !== today) {
      LIVE = {ymd:today, yield_wh:null, yield_kwh:null, consumed_kwh:null,
              pmax:null, pmax_w:null, vpvmax:null, vpv_max:null,
              vbmax:null, vbmin:null, vbat_max:null, vbat_min:null};
    }
    if (m.yield_wh != null) {
      LIVE.yield_wh = Number(m.yield_wh);
      LIVE.yield_kwh = Number(m.yield_wh)/1000;
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
    const yk = ymdPrev(today);
    const yrec = recOf(yk);
    const fromH = (h.kind==='yesterday' && String(h.ymd)===yk) ? h : yrec;
    document.getElementById('y_yd').textContent = f(fromH.yield_kwh!=null?fromH.yield_kwh*1000:null, 0, ' Wh');
    document.getElementById('c_yd').textContent = f(fromH.consumed_kwh!=null?fromH.consumed_kwh*1000:null, 0, ' Wh');
    document.getElementById('p_yd').textContent = f(fromH.pmax_w, 0, ' W');
    document.getElementById('v_yd').textContent = f(fromH.vpv_max, 2, ' V');
    document.getElementById('gatt_pv').textContent =
      (pv.vpv!=null || pv.ppv!=null) ? (f(pv.vpv,2,' V')+' / '+f(pv.ppv,2,' W')) : '—';
    LAST_STATE = s;
    showMM(s);
    if (s.days) {
      DAYS = Object.keys(s.days).map(k => Object.assign({ymd:k}, s.days[k]));
    }
    renderMat();
  } catch(e) {}
}

async function loadDays(){
  const s = await getJson('/days', 4000);
  if (s && s.days){
    if (s.dev_ymd) DEV = String(s.dev_ymd);
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


STATUS_HTML = r"""<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Victron status</title>
<style>
:root { color-scheme: dark; }
*{ box-sizing:border-box; }
body { margin:0; background:#15202b; color:#e8eef4; font:16px/1.3 -apple-system,system-ui,sans-serif; }
header { padding:8px 16px 4px; display:flex; justify-content:space-between; align-items:baseline; gap:12px; flex-wrap:wrap; }
h1 { font-size:20px; margin:0; font-weight:700; }
a { color:#6ec1ff; text-decoration:none; }
.sub { color:#8aa0b3; font-size:13px; }
.wrap { padding:8px 16px 20px; }
.bar { display:flex; gap:6px; align-items:center; flex-wrap:wrap; margin-bottom:10px; }
.bar button {
  font-size:12px; padding:4px 10px; border-radius:999px; cursor:pointer;
  color:#8aa0b3; background:#14202c; border:1px solid #2a3b4d; font-family:ui-monospace,Menlo,monospace;
}
.bar button.on { color:#fff; background:#0e4a44; border-color:#00b3a4; font-weight:700; }
.charts { display:grid; grid-template-columns:repeat(auto-fill,minmax(340px,1fr)); gap:12px; }
.card { background:#1c2b3a; border-radius:14px; padding:8px 10px 6px; }
.ct { display:flex; justify-content:space-between; font-size:13px; color:#8aa0b3; }
.ct b { color:#fff; font-variant-numeric:tabular-nums; }
canvas { width:100%; height:120px; display:block; }
table.rst { width:100%; border-collapse:collapse; font-size:13px; }
table.rst th, table.rst td { padding:5px 4px; text-align:left; border-bottom:1px solid #2a3b4d; font-variant-numeric:tabular-nums; }
table.rst th { color:#8aa0b3; font-weight:600; }
.hint { color:#6d8296; font-size:12px; padding:8px 4px 0; }
</style>
</head>
<body>
<header>
  <h1>Status · victron/status</h1>
  <div class="sub"><a href="/">← 메인</a> · <span id="info">-</span></div>
</header>
<div class="wrap">
  <div class="bar" id="ranges">
    <button data-r="1h">1h</button><button data-r="6h">6h</button><button data-r="24h">24h</button>
    <span class="sub" style="margin-left:8px">자동 새로고침 15s · 수신한 값만 표시 (점 = 수신, 선 = 연속 수신 사이만)</span>
  </div>
  <div class="card" style="margin-bottom:12px">
    <div class="ct"><span>rst (리셋 원인) 값</span><span id="rstinfo"></span></div>
    <table class="rst" id="rst"></table>
  </div>
  <div class="charts" id="charts"></div>
  <div class="hint">x축 = 대시보드 수신 시각(KST). ts_offset = 장치 ts − 수신 시각(초). 간격이 중앙값의 3배(최소 3분)를 넘으면 선을 끊음.</div>
</div>
<script>
const ORDER = ['wifi_rssi','mppt_rssi','sense_rssi','rssi','vbat','ts','ts_offset','boot','rst','awake_ms','prev_awake_ms',
  'wdt','wifi_ms','dec_ok','dec_fail','hits','heap','heap_min','adv_mppt_n','adv_sense_n','ntp_adj_ms','ntp_age',
  'jobs','job_pend'];
const STEP = new Set(['boot','rst','wdt','jobs','job_pend','day0_done','yday_done','today','yday']);
const RST = {0:'UNKNOWN',1:'POWERON',2:'EXT',3:'SW',4:'PANIC',5:'INT_WDT',6:'TASK_WDT',7:'WDT',8:'DEEPSLEEP',
  9:'BROWNOUT',10:'SDIO',11:'USB',12:'JTAG',13:'EFUSE',14:'PWR_GLITCH',15:'CPU_LOCKUP'};
let range = localStorage.getItem('st_range') || '24h';
let data = null;

function fmtT(t){ return new Date(t*1000).toLocaleTimeString('ko-KR',{timeZone:'Asia/Seoul',hour12:false,hour:'2-digit',minute:'2-digit'}); }
function fmtDT(t){ return new Date(t*1000).toLocaleString('ko-KR',{timeZone:'Asia/Seoul',hour12:false}); }
function fmtV(v){ if(v==null) return '—'; const a=Math.abs(v);
  if(a>=1e6) return v.toFixed(0); if(a>=100) return v.toFixed(0); if(a>=10) return v.toFixed(1);
  return (Math.round(v*100)/100).toString(); }

function series(name){
  if(name==='ts_offset'){
    const ts=data.f.ts; if(!ts) return null;
    return ts.map((v,i)=> (v==null||v<=0)? null : v - data.rx[i]);
  }
  return data.f[name] || null;
}

function draw(cv, name, ys){
  const dpr = window.devicePixelRatio||1, W = cv.clientWidth, H = cv.clientHeight;
  cv.width = W*dpr; cv.height = H*dpr;
  const g = cv.getContext('2d'); g.setTransform(dpr,0,0,dpr,0,0);
  g.clearRect(0,0,W,H);
  const L=46, R=6, T=6, B=18, pw=W-L-R, ph=H-T-B;
  const t0=data.t0, t1=data.t1, rx=data.rx;
  const pts=[]; for(let i=0;i<rx.length;i++){ if(ys[i]!=null) pts.push([rx[i], ys[i]]); }
  g.strokeStyle='#2a3b4d'; g.lineWidth=1; g.strokeRect(L+.5,T+.5,pw,ph);
  g.fillStyle='#6d8296'; g.font='10px ui-monospace,Menlo,monospace';
  // time ticks
  const span=t1-t0, step = span<=3600? 600 : span<=6*3600? 3600 : 4*3600;
  g.textAlign='center';
  for(let t=Math.ceil(t0/step)*step; t<=t1; t+=step){
    const x=L+(t-t0)/span*pw; g.strokeStyle='#22323f'; g.beginPath(); g.moveTo(x+.5,T); g.lineTo(x+.5,T+ph); g.stroke();
    g.fillText(fmtT(t), x, H-5);
  }
  if(!pts.length){ g.textAlign='center'; g.fillText('수신 데이터 없음', L+pw/2, T+ph/2); return; }
  let lo=Infinity, hi=-Infinity; for(const p of pts){ lo=Math.min(lo,p[1]); hi=Math.max(hi,p[1]); }
  if(lo===hi){ lo-=1; hi+=1; } const pad=(hi-lo)*0.08; lo-=pad; hi+=pad;
  g.textAlign='right'; g.fillText(fmtV(hi-pad), L-4, T+9); g.fillText(fmtV(lo+pad), L-4, T+ph);
  const X=t=>L+(t-t0)/span*pw, Y=v=>T+(hi-v)/(hi-lo)*ph;
  // gap threshold from received intervals
  const dts=[]; for(let i=1;i<pts.length;i++) dts.push(pts[i][0]-pts[i-1][0]);
  dts.sort((a,b)=>a-b); const med = dts.length? dts[dts.length>>1] : 60; const gap=Math.max(180, med*3);
  const step_ = STEP.has(name);
  g.strokeStyle = name==='ts_offset' ? '#f0b44c' : step_ ? '#c792ea' : '#00b3a4'; g.lineWidth=1.5;
  g.beginPath();
  for(let i=0;i<pts.length;i++){
    const [t,v]=pts[i];
    if(i===0 || t-pts[i-1][0]>gap){ g.moveTo(X(t),Y(v)); continue; }
    if(step_) g.lineTo(X(t),Y(pts[i-1][1]));
    g.lineTo(X(t),Y(v));
  }
  g.stroke();
  g.fillStyle = g.strokeStyle;
  const r = pts.length>400 ? 1.2 : 2;
  for(const [t,v] of pts){ g.beginPath(); g.arc(X(t),Y(v),r,0,7); g.fill(); }
  cv._pts=pts; cv._X=X; cv._Y=Y;
}

function rstTable(){
  const el=document.getElementById('rst'), rs=data.f.rst||[], m={};
  rs.forEach((v,i)=>{ if(v==null) return; const o=m[v]||(m[v]={n:0,first:data.rx[i],last:data.rx[i]}); o.n++; o.last=data.rx[i]; });
  const keys=Object.keys(m).sort((a,b)=>a-b);
  el.innerHTML = '<tr><th>rst</th><th>원인</th><th>횟수</th><th>처음</th><th>마지막</th></tr>' +
    (keys.length? keys.map(k=>`<tr><td>${k}</td><td>${RST[k]||'?'}</td><td>${m[k].n}</td><td>${fmtDT(m[k].first)}</td><td>${fmtDT(m[k].last)}</td></tr>`).join('')
      : '<tr><td colspan="5">rst 값 없음</td></tr>');
}

function render(){
  const box=document.getElementById('charts');
  const names = Object.keys(data.f);
  if(data.f.ts) names.push('ts_offset');
  names.sort((a,b)=>{ const ia=ORDER.indexOf(a), ib=ORDER.indexOf(b);
    return (ia<0?999:ia)-(ib<0?999:ib) || a.localeCompare(b); });
  const want = names.join(',');
  if(box.dataset.names!==want){
    box.dataset.names=want;
    box.innerHTML = names.map(n=>`<div class="card"><div class="ct"><span>${n==='ts_offset'?'ts_offset (ts − 수신, s)':n}</span><span><b id="v_${n}">—</b> <span id="h_${n}"></span></span></div><canvas id="c_${n}"></canvas></div>`).join('');
    names.forEach(n=>{ const cv=document.getElementById('c_'+n);
      cv.addEventListener('mousemove',e=>hover(cv,n,e)); cv.addEventListener('mouseleave',()=>{document.getElementById('h_'+n).textContent='';}); });
  }
  for(const n of names){
    const ys=series(n)||[]; draw(document.getElementById('c_'+n), n, ys);
    let last=null; for(let i=ys.length-1;i>=0;i--) if(ys[i]!=null){ last=ys[i]; break; }
    document.getElementById('v_'+n).textContent = (n==='ts' && last)? fmtDT(last) : fmtV(last);
  }
  rstTable();
  const lastRx = data.rx.length? data.rx[data.rx.length-1] : null;
  document.getElementById('info').textContent = `${data.n} pts · ${range} · 마지막 수신 ${lastRx? fmtDT(lastRx):'—'}`;
}

function hover(cv,n,e){
  const pts=cv._pts; if(!pts||!pts.length) return;
  const x=e.offsetX; let best=null, bd=1e9;
  for(const p of pts){ const d=Math.abs(cv._X(p[0])-x); if(d<bd){ bd=d; best=p; } }
  if(best && bd<20) document.getElementById('h_'+n).textContent = `@${fmtT(best[0])} ${n==='ts'?fmtDT(best[1]):fmtV(best[1])}`;
}

async function load(){
  try{
    const r = await fetch('/api/status_hist?range='+range, {cache:'no-store'});
    data = await r.json(); render();
  }catch(e){ document.getElementById('info').textContent='err '+e; }
}
document.querySelectorAll('#ranges button').forEach(b=>{
  b.classList.toggle('on', b.dataset.r===range);
  b.onclick=()=>{ range=b.dataset.r; localStorage.setItem('st_range',range);
    document.querySelectorAll('#ranges button').forEach(x=>x.classList.toggle('on',x===b)); load(); };
});
window.addEventListener('resize',()=>{ if(data) render(); });
load(); setInterval(load, 15000);
</script>
</body>
</html>
"""


class H(BaseHTTPRequestHandler):
    def log_message(self, *_a) -> None:
        return

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path == "/api/status_hist":
            key = (parse_qs(urlparse(self.path).query).get("range") or ["24h"])[0]
            if key not in STATUS_RANGES:
                key = "24h"
            body = json.dumps(_status_hist(key)).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
            return
        if path in ("/status", "/status/"):
            body = STATUS_HTML.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(body)
            return
        if path == "/state":
            with state_lock:
                snap = dict(state)
            days = _days_view(snap.get("days") or {})
            snap["days"] = days
            snap["dev_ymd"] = _dev_today(days)
            snap["board_mm"] = {"1h": _board_mm(3600), "24h": _board_mm(24 * 3600)}
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
            extra = _days_view(days)
            body = json.dumps({"days": extra, "dev_ymd": _dev_today(extra)}).encode()
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
    print(f"status graph http://127.0.0.1:{HTTP_PORT}/status")
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

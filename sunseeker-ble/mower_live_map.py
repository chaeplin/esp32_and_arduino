#!/usr/bin/env python3
"""Mower live map — no control. BLE pos only.

    python3 -m pip install paho-mqtt
    python3 mower_live_map.py
    # http://127.0.0.1:8767/

Influx bucket mower + Feather GW BLE pos. jsonl 없음.
INFLUX_TOKEN 필요 (influx.env 또는 환경변수). MQTT 는 mower.env 또는 환경변수.
"""

from __future__ import annotations

import csv
import io
import json
import urllib.error
import urllib.request
from datetime import datetime, timezone
import os
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urlparse

import paho.mqtt.client as mqtt

HERE = Path(__file__).resolve().parent


def _load_env_file(path: Path) -> None:
    """KEY=VALUE 파일을 읽어 아직 없는 환경변수만 채운다."""
    try:
        txt = path.read_text(encoding="utf-8")
    except OSError:
        return
    for line in txt.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k, v = k.strip(), v.strip().strip("'").strip('"')
        if k and k not in os.environ:
            os.environ[k] = v


# 접속 정보: 환경변수 또는 이 파일 옆의 mower.env (mower.env.example 참고)
_load_env_file(HERE / "mower.env")
MQTT_HOST = os.environ.get("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USER", "")
MQTT_PASS = os.environ.get("MQTT_PASS", "")
HTTP_HOST = "0.0.0.0"
HTTP_PORT = 8767

TOPIC_STATUS = "mower/status"
TOPIC_REPORT = "mower/report"
TOPIC_BLE_RX = "mower/ble/rx"
TOPIC_YOLO = "mower/yolo"
# 선택: go2rtc 등 JPEG 스냅샷 URL. 비우면 /snap 비활성
GO2_SNAP = os.environ.get("GO2_SNAP_URL", "").strip()


def _load_influx_env() -> None:
    cands = [
        HERE / "influx.env",
        HERE / ".influx.env",
        HERE / "influx.token",
        HERE / "influx_token.txt",
        Path.cwd() / "influx.env",
        Path.cwd() / "influx.token",
        Path.home() / "influx.env",
        Path.home() / "influx.token",
        Path.home() / "influxdb-cfg" / "token",
        Path.home() / "Downloads" / "influx.env",
        Path.home() / "Downloads" / "influx.token",
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
        name = cand.name
        if name in ("influx.token", "token", "influx_token.txt"):
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
INFLUX_TOKEN = os.environ.get("INFLUX_TOKEN", "").strip().strip("'").strip('"')
if INFLUX_TOKEN.startswith("INFLUX_TOKEN="):
    INFLUX_TOKEN = INFLUX_TOKEN.split("=", 1)[1].strip().strip("'").strip('"')
INFLUX_ORG = os.environ.get("INFLUX_ORG", "yard")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET", "mower")

_recent_cache: dict[str, Any] = {"at": 0.0, "hours": None, "pts": []}

state_lock = threading.Lock()
state: dict[str, Any] = {
    "mqtt": "connecting",
    "status": {},
    "status_ts": 0.0,
    "report": {},
    "report_ts": 0.0,
    "live_pts": [],
    "influx": "idle",
    "yolo": {},
}

_snap = {"jpg": b"", "t": 0.0}


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_influx_time(s: str) -> float:
    """RFC3339 → unix. py3.9 fromisoformat rejects nanoseconds."""
    s = (s or "").strip()
    if not s:
        raise ValueError("empty time")
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    if "." in s:
        head, rest = s.split(".", 1)
        sign_at = max(rest.rfind("+"), rest.rfind("-"))
        if sign_at > 0:
            frac, tz = rest[:sign_at], rest[sign_at:]
        else:
            frac, tz = rest, "+00:00"
        frac = "".join(ch for ch in frac if ch.isdigit())[:6].ljust(6, "0")
        s = f"{head}.{frac}{tz}"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _flux_csv(flux: str) -> list[dict[str, str]]:
    url = f"{INFLUX_URL}/api/v2/query?org={quote(INFLUX_ORG)}"
    req = urllib.request.Request(
        url,
        data=flux.encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Token {INFLUX_TOKEN}",
            "Content-Type": "application/vnd.flux",
            "Accept": "application/csv",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=14) as r:
            text = r.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8", errors="replace")[:400]
        except Exception:
            body = ""
        print("influx http", exc.code, body.replace("\n", " "))
        raise
    rows: list[dict[str, str]] = []
    for block in text.split("\n\n"):
        lines = [ln for ln in block.splitlines() if ln and not ln.startswith("#")]
        if not lines:
            continue
        for rec in csv.DictReader(io.StringIO("\n".join(lines))):
            rows.append(rec)
    return rows


def _recent_from_influx(hours: float) -> tuple[list[dict[str, Any]] | None, str]:
    if not INFLUX_URL or not INFLUX_TOKEN:
        return None, "no-token"
    mins = max(15, int(round(float(hours) * 60)))
    flux = (
        'from(bucket: "%s")\n'
        "  |> range(start: -%dm)\n"
        '  |> filter(fn: (r) => r._measurement == "mower"'
        ' and (r._field == "x" or r._field == "y"'
        ' or r._field == "a" or r._field == "status"))\n'
        '  |> keep(columns: ["_time", "_field", "_value"])\n'
    ) % (INFLUX_BUCKET, mins)
    try:
        raw = _flux_csv(flux)
    except Exception as exc:
        print("influx query fail", type(exc).__name__, exc)
        return None, "fail"
    rows: dict[int, dict[str, Any]] = {}
    parsed = 0
    for rec in raw:
        traw = rec.get("_time") or ""
        if not traw:
            continue
        try:
            ts = _parse_influx_time(traw)
        except (ValueError, TypeError):
            continue
        parsed += 1
        bucket = int(ts)
        row = rows.setdefault(bucket, {"t": ts, "s": -1})
        if rec.get("x") not in (None, "", "null") or rec.get("y") not in (None, "", "null"):
            for key, dst in (("x", "x"), ("y", "y"), ("a", "a"), ("status", "s")):
                val = rec.get(key)
                if val in (None, "", "null"):
                    continue
                try:
                    row[dst] = int(float(val)) if dst == "s" else float(val)
                except (TypeError, ValueError):
                    pass
            continue
        field = rec.get("_field")
        val = rec.get("_value")
        if not field or val in (None, "", "null"):
            continue
        try:
            v = float(val)
        except (TypeError, ValueError):
            continue
        if field == "x":
            row["x"] = v
        elif field == "y":
            row["y"] = v
        elif field == "a":
            row["a"] = v
        elif field == "status":
            row["s"] = int(v)
    out: list[dict[str, Any]] = []
    last = None
    for ts in sorted(rows):
        pnt = rows[ts]
        if "x" not in pnt or "y" not in pnt:
            continue
        xy = (float(pnt["x"]), float(pnt["y"]))
        if last is None or abs(xy[0] - last[0]) + abs(xy[1] - last[1]) > 0.03:
            out.append({"x": xy[0], "y": xy[1], "s": pnt.get("s", -1), "a": pnt.get("a"), "t": pnt.get("t", float(ts))})
            last = xy
    if not out:
        return [], "empty" if parsed else "no-xy"
    return out, "influx"


def _merge_live(pts: list[dict[str, Any]], hours: float) -> list[dict[str, Any]]:
    now = time.time()
    cut = now - max(0.25, float(hours)) * 3600.0
    last = (pts[-1]["x"], pts[-1]["y"]) if pts else None
    with state_lock:
        extra = list(state.get("live_pts") or [])
    for p in extra:
        try:
            t = float(p.get("t") or now)
            xy = (float(p["x"]), float(p["y"]))
        except (TypeError, ValueError, KeyError):
            continue
        if t < cut:
            continue
        if last is None or abs(xy[0] - last[0]) + abs(xy[1] - last[1]) > 0.03:
            pts.append({"x": xy[0], "y": xy[1], "s": p.get("s", -1), "t": t})
            last = xy
    return pts


def _recent_points(hours: float) -> dict[str, Any]:
    now = time.time()
    if (
        _recent_cache.get("pts") is not None
        and _recent_cache["hours"] == hours
        and now - _recent_cache["at"] < 6
        and _recent_cache.get("src")
    ):
        pts = list(_recent_cache["pts"])
        src = str(_recent_cache.get("src") or "cache")
    else:
        raw, src = _recent_from_influx(hours)
        pts = [] if raw is None else raw
        _recent_cache["at"] = now
        _recent_cache["hours"] = hours
        _recent_cache["pts"] = list(pts)
        _recent_cache["src"] = src
        print("recent", src, "n=", len(pts), "h=", hours)
    n_hist = len(pts)
    pts = _merge_live(list(pts), hours)
    return {"pts": pts, "hours": hours, "n": len(pts), "n_hist": n_hist, "src": src}


HTML = r"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>mower live map</title>
<script>
if (/[?&](map=1|map=&|only=map|embed=1)(?:&|$)/.test(location.search) || /[?&]map(?:&|$)/.test(location.search))
  document.documentElement.classList.add('maponly');
</script>
<style>
  :root { --soil:#141611; --sage:#b7c4b2; --clip:#e8eadf; --now:#e24b3a; }
  * { box-sizing:border-box; }
  html,body { margin:0; height:100%; background:var(--soil); color:var(--clip);
    font:14px/1.4 ui-sans-serif,system-ui,sans-serif; }
  .wrap { display:grid; grid-template-columns: 270px 1fr; height:100%; }
  html.maponly .wrap, .wrap.maponly { grid-template-columns: 1fr; }
  html.maponly aside, .wrap.maponly aside { display:none; }
  html.maponly #floatbar { display:none; }
  .wrap.maponly #floatbar { display:none; }
  aside { border-right:1px solid #2a3128; padding:12px; overflow:auto; }
  h1 { font-size:15px; color:var(--sage); margin:0 0 8px; }
  .chips { display:flex; gap:6px; margin:0 0 8px; }
  .chip { flex:1; text-align:center; font-size:11px; font-weight:650;
    padding:7px 3px; border-radius:8px; background:#1c201b; border:1px solid #2a3128; color:#7e8778; }
  .chip.on { color:#9ccc8a; border-color:#3d5a3a; }
  .chip.off { color:#d9897a; border-color:#4a2e2a; }
  .chip.wait { color:#d6c37a; border-color:#4a4530; }
  #info { font:12px/1.45 ui-monospace,Menlo,monospace; color:#c9d1c4; margin:0 0 10px; white-space:pre-wrap; }
  .logs label { display:flex; gap:8px; align-items:flex-start; padding:4px 0;
    font:12px/1.3 ui-monospace,Menlo,monospace; color:#c9d1c4; }
  canvas { width:100%; height:100%; display:block; background:#10140f; touch-action:none; }
  .hint { font-size:11px; color:#7e8778; margin:0 0 10px; }
  .hbox { margin:10px 0 12px; background:#1c201b; border:1px solid #2a3128;
    border-radius:10px; padding:8px 8px 6px; }
  #mowerBox { width:100%; height:168px; display:block; }
  .hbox .deg { text-align:center; color:#e24b3a; font-weight:700; font-size:20px; letter-spacing:0.02em; }
  .hbox .cap { text-align:center; color:#7e8778; font-size:11px; margin-top:2px; }
  label.row { display:flex; gap:8px; align-items:center; margin:5px 0; font-size:13px; }
  button { background:#3d4a3a; color:var(--clip); border:0; border-radius:8px;
    padding:8px 10px; font-weight:650; cursor:pointer; width:100%; margin-top:8px; }
  .hrs { display:flex; flex-wrap:wrap; gap:4px; margin:8px 0; }
  .hrs button { width:auto; flex:1 1 28%; margin:0; min-height:32px; background:#1c201b;
    border:1px solid #2a3128; color:#c9d1c4; font-size:12px; }
  .hrs button.on { background:#e24b3a; color:#e8eadf; border-color:#e24b3a; }
  #floatbar { position:absolute; right:10px; top:10px; display:flex; flex-wrap:wrap; gap:4px; z-index:3; max-width:62%; justify-content:flex-end; }
  #floatbar button { width:auto; margin:0; min-height:30px; padding:0 8px; font-size:12px;
    background:#1c201b; border:1px solid #2a3128; color:#e8eadf; }
  #floatbar button.on { background:#e24b3a; color:#e8eadf; }
  #zbar { position:absolute; left:10px; top:10px; display:flex; flex-direction:column; gap:4px; z-index:3; }
  #zbar button { width:36px; margin:0; min-height:32px; padding:0; font-size:16px;
    background:#1c201b; border:1px solid #2a3128; color:#e8eadf; }
  .stage { position:relative; min-height:0; }
  @media (max-width:700px){
    .wrap { grid-template-columns:1fr; grid-template-rows:auto 1fr; }
    aside { max-height:38vh; border-right:0; border-bottom:1px solid #2a3128; }
    .wrap.maponly { grid-template-rows:1fr; }
  }
</style>
</head>
<body>
<div class="wrap" id="wrap">
  <aside>
    <h1>실시간 맵</h1>
    <div class="chips">
      <span id="chip-gw" class="chip">GW --</span>
      <span id="chip-adv" class="chip">ADV --</span>
      <span id="chip-ble" class="chip">BLE --</span>
    </div>
    <div id="info">연결 중</div>
    <div class="hint">위=자북 · 독축=자북+14° · 적색=전면 · 주황=복귀 s=7</div>
    <div class="hrs">
      <button type="button" data-h="0.5" class="on" onclick="setHours(0.5)">30분</button>
      <button type="button" data-h="1" onclick="setHours(1)">1시간</button>
      <button type="button" data-h="2" onclick="setHours(2)">2시간</button>
      <button type="button" data-h="3" onclick="setHours(3)">3시간</button>
      <button type="button" data-h="6" onclick="setHours(6)">6시간</button>
      <button type="button" data-h="12" onclick="setHours(12)">12시간</button>
    </div>
    <label class="row"><input id="showLive" type="checkbox" checked onchange="draw()"/> 실시간</label>
    <label class="row"><input id="onlyRet" type="checkbox" onchange="draw()"/> 복귀만</label>
    <button onclick="toggleMapOnly()">맵만 보기</button>
    <div class="hbox">
      <canvas id="mowerBox"></canvas>
      <div class="deg" id="boxDeg">--</div>
      <div class="cap">전면 적색 · 위=자북 · 독 0°=14°</div>
    </div>
  </aside>
  <div class="stage">
    <div id="zbar">
      <button type="button" onclick="mapZoom(1.3)">+</button>
      <button type="button" onclick="mapZoom(1/1.3)">−</button>
      <button type="button" onclick="mapFit()">맞춤</button>
    </div>
    <div id="floatbar">
      <button type="button" data-h="0.5" class="on" onclick="setHours(0.5)">30m</button>
      <button type="button" data-h="1" onclick="setHours(1)">1h</button>
      <button type="button" data-h="2" onclick="setHours(2)">2h</button>
      <button type="button" data-h="3" onclick="setHours(3)">3h</button>
      <button type="button" data-h="6" onclick="setHours(6)">6h</button>
      <button type="button" data-h="12" onclick="setHours(12)">12h</button>
      <button type="button" id="fmap" onclick="toggleMapOnly()">맵만</button>
    </div>
    <canvas id="cv"></canvas>
  </div>
</div>
<script>
const COLS = ['#6b7c5a','#8aa0c0','#c08060','#c4b89a','#5a6a50','#9a8a60','#7a9a6a','#a07060'];
let tracks = [];
let live = {pts:[], report:{}, age:999, status:{}};
let hours = 0.5;
let mapOnly = new URLSearchParams(location.search).has('map') || location.search.indexOf('only=map')>=0;
(function(){ const w=document.getElementById('wrap'); if(w) w.classList.toggle('maponly', mapOnly); const b=document.getElementById('fmap'); if(b){ b.classList.toggle('on', mapOnly); b.textContent = mapOnly ? '목록' : '맵만'; }})();
let hist = [];
let histMeta = {src:'', n:0, n_hist:0};
function setChip(id, text, kind){
  const el = document.getElementById(id);
  el.textContent = text;
  el.className = 'chip' + (kind ? ' '+kind : '');
}
function setHours(h){
  hours = h;
  document.querySelectorAll('[data-h]').forEach(el => {
    el.classList.toggle('on', Number(el.dataset.h) === h);
  });
  loadRecent();
}
function liveTrail(){
  const out = [];
  const seen = new Set();
  function add(p){
    if (!p || p.x==null || p.y==null) return;
    if (!inWin(p)) return;
    const k = p.x.toFixed(2)+','+p.y.toFixed(2)+','+((p.t||0).toFixed?Number(p.t||0).toFixed(0):0);
    if (seen.has(k)) return;
    seen.add(k);
    out.push(p);
  }
  (hist||[]).forEach(add);
  (live.pts||[]).forEach(add);
  const rp = live.report||{};
  if (rp.x!=null) add({x:+rp.x, y:+rp.y, s:rp.status, a:rp.a, t:Date.now()/1000});
  out.sort((a,b) => (+a.t||0) - (+b.t||0));
  return out;
}
async function loadRecent(){
  try {
    const r = await fetch('/recent?hours='+hours);
    const j = await r.json();
    hist = j.pts || [];
    histMeta = {src: j.src || '', n: Number(j.n||0), n_hist: Number(j.n_hist||0)};
  } catch(e) {
    hist = [];
    histMeta = {src:'fail', n:0, n_hist:0};
  }
  draw();
}
function toggleMapOnly(){
  mapOnly = !mapOnly;
  document.getElementById('wrap').classList.toggle('maponly', mapOnly);
  const b = document.getElementById('fmap');
  if (b) { b.classList.toggle('on', mapOnly); b.textContent = mapOnly ? '목록' : '맵만'; }
  draw();
}
(function(){
  const q = new URLSearchParams(location.search);
  if (q.get('map')==='1' || q.get('only')==='map' || q.get('embed')==='1'){
    mapOnly = true;
    const w = document.getElementById('wrap');
    if (w) w.classList.add('maponly');
    const b = document.getElementById('fmap');
    if (b) { b.classList.add('on'); b.textContent = '목록'; }
  }
})();
function since(){
  return (Date.now()/1000) - hours*3600;
}
function mergeTrail(){
  const seen = new Set();
  const out = [];
  const add = p => {
    if (!p || p.x==null || p.y==null) return;
    const k = Number(p.x).toFixed(3)+','+Number(p.y).toFixed(3);
    if (seen.has(k)) return;
    seen.add(k);
    out.push(p);
  };
  (hist||[]).forEach(add);
  (live.pts||[]).forEach(add);
  const rp = live.report||{};
  if (rp.x!=null) add({x:+rp.x, y:+rp.y, s:rp.status, a:rp.a, t:Date.now()/1000});
  return out;
}
function inWin(p){
  if (!p) return false;
  if (p.t == null || p.t === '') return true;
  return +p.t >= since();
}
function degTxt(a){
  let d = (+a||0) * 180 / Math.PI;
  while (d > 180) d -= 360;
  while (d < -180) d += 360;
  return (d>=0?'+':'') + Math.round(d) + '°';
}
let mapView = {cx:0, cy:0, span:12, user:false};
// 맵 위 = 자북. 독 a=0 축 = 자북에서 시계 14° (폰 나침반).
// a=0 은 pos +Y. 원본 pos 는 그대로, 표시만 회전.
const DOCK_MAG_DEG = 14;
const DOCK_MAG = DOCK_MAG_DEG * Math.PI / 180;
function worldXY(x, y){
  x = +x; y = +y;
  const c = Math.cos(DOCK_MAG), s = Math.sin(DOCK_MAG);
  return [x * c + y * s, -x * s + y * c];
}
function worldA(a){
  return (+a || 0) - DOCK_MAG;
}
function mapApplyFit(){
  const d = fit();
  mapView.cx = (d.x0+d.x1)/2;
  mapView.cy = (d.y0+d.y1)/2;
  mapView.span = Math.max(d.x1-d.x0, d.y1-d.y0, 2);
  mapView.user = false;
}
function mapBox(W, H){
  if (!mapView.user) mapApplyFit();
  const aspect = W / Math.max(1, H);
  let wx = mapView.span, wy = mapView.span;
  if (aspect >= 1) wx = mapView.span * aspect;
  else wy = mapView.span / aspect;
  return {x0: mapView.cx-wx/2, x1: mapView.cx+wx/2, y0: mapView.cy-wy/2, y1: mapView.cy+wy/2};
}
function mapZoomAt(cssX, cssY, k){
  const cv = document.getElementById('cv');
  const W = cv.clientWidth, H = cv.clientHeight;
  const b = mapBox(W, H);
  const wx = b.x0 + (cssX / Math.max(1,W)) * (b.x1-b.x0);
  const wy = b.y1 - (cssY / Math.max(1,H)) * (b.y1-b.y0);
  mapView.span = Math.min(200, Math.max(1.2, mapView.span / k));
  mapView.user = true;
  const b2 = mapBox(W, H);
  mapView.cx += wx - (b2.x0 + (cssX / Math.max(1,W)) * (b2.x1-b2.x0));
  mapView.cy += wy - (b2.y1 - (cssY / Math.max(1,H)) * (b2.y1-b2.y0));
}
function mapPan(dx, dy){
  const cv = document.getElementById('cv');
  const W = cv.clientWidth, H = cv.clientHeight;
  const b = mapBox(W, H);
  mapView.cx -= dx / Math.max(1,W) * (b.x1-b.x0);
  mapView.cy += dy / Math.max(1,H) * (b.y1-b.y0);
  mapView.user = true;
}
function mapZoom(k){
  const cv = document.getElementById('cv');
  mapZoomAt(cv.clientWidth/2, cv.clientHeight/2, k);
  draw();
}
function mapFit(){ mapApplyFit(); draw(); }
function fit(){
  const onlyRet = document.getElementById('onlyRet').checked;
  const showLive = document.getElementById('showLive').checked;
  let xs=[], ys=[];
  tracks.forEach(t => {
    (t.pts||[]).forEach(p => {
      if (!onlyRet || p.s===7) { xs.push(p.x); ys.push(p.y); }
    });
  });
  if (showLive) {
    liveTrail().forEach(p => {
      if (!onlyRet || p.s===7) { xs.push(p.x); ys.push(p.y); }
    });
    const rp = live.report||{};
    if (rp.x!=null) { xs.push(+rp.x); ys.push(+rp.y); }
  }
  xs.push(0); ys.push(0);
  const xx=[], yy=[];
  for (let i=0;i<xs.length;i++){
    const w = worldXY(xs[i], ys[i]);
    xx.push(w[0]); yy.push(w[1]);
  }
  if (!xx.length) return {x0:-8,y0:-4,x1:4,y1:4};
  const pad = 1.4;
  return {x0:Math.min(...xx)-pad, y0:Math.min(...yy)-pad, x1:Math.max(...xx)+pad, y1:Math.max(...yy)+pad};
}

function drawMowerBox(aRaw){
  const cv = document.getElementById('mowerBox');
  if (!cv) return;
  const dpr = window.devicePixelRatio || 1;
  const cssW = cv.clientWidth || 220, cssH = cv.clientHeight || 168;
  cv.width = Math.max(1, cssW * dpr);
  cv.height = Math.max(1, cssH * dpr);
  const ctx = cv.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, cssW, cssH);
  const cx = cssW / 2, cy = cssH / 2 - 4;
  const R = Math.min(cssW, cssH) * 0.40;
  ctx.strokeStyle = '#2a3128';
  ctx.lineWidth = 1;
  ctx.beginPath(); ctx.arc(cx, cy, R, 0, Math.PI*2); ctx.stroke();
  ctx.fillStyle = '#7e8778';
  ctx.font = '11px ui-sans-serif';
  ctx.textAlign = 'center';
  ctx.fillText('N 자북', cx, cy - R - 6);
  const degEl = document.getElementById('boxDeg');
  if (aRaw == null || Number.isNaN(+aRaw)){
    if (degEl) degEl.textContent = '--';
    ctx.fillStyle = '#3d4a3a';
    ctx.fillText('pos 없음', cx, cy+4);
    return;
  }
  if (degEl) degEl.textContent = degTxt(+aRaw);
  const a = worldA(+aRaw || 0);
  ctx.save();
  ctx.translate(cx, cy);
  ctx.rotate(-a);
  const L = 36, Ww = 22;
  ctx.fillStyle = '#1c1f1a';
  ctx.strokeStyle = '#b7c4b2';
  ctx.lineWidth = 2;
  ctx.beginPath();
  ctx.moveTo(-Ww+8, -L);
  ctx.lineTo(Ww-8, -L);
  ctx.quadraticCurveTo(Ww, -L, Ww, -L+8);
  ctx.lineTo(Ww, L-8);
  ctx.quadraticCurveTo(Ww, L, Ww-8, L);
  ctx.lineTo(-Ww+8, L);
  ctx.quadraticCurveTo(-Ww, L, -Ww, L-8);
  ctx.lineTo(-Ww, -L+8);
  ctx.quadraticCurveTo(-Ww, -L, -Ww+8, -L);
  ctx.closePath();
  ctx.fill(); ctx.stroke();
  ctx.fillStyle = '#e24b3a';
  ctx.beginPath();
  ctx.moveTo(-Ww+3, -L+1);
  ctx.lineTo(Ww-3, -L+1);
  ctx.lineTo(Ww-5, -L+12);
  ctx.lineTo(-Ww+5, -L+12);
  ctx.closePath();
  ctx.fill();
  ctx.beginPath();
  ctx.moveTo(0, -L-8);
  ctx.lineTo(-8, -L+2);
  ctx.lineTo(8, -L+2);
  ctx.closePath();
  ctx.fill();
  ctx.fillStyle = '#c9cfc4';
  ctx.fillRect(-10, L-4, 6, 5);
  ctx.fillRect(4, L-4, 6, 5);
  ctx.fillStyle = '#141611';
  ctx.strokeStyle = '#7e8778';
  ctx.lineWidth = 1;
  [[-Ww-1, L-14],[Ww+1, L-14],[-Ww-1, -8],[Ww+1, -8]].forEach(p=>{
    ctx.beginPath(); ctx.arc(p[0], p[1], 4.5, 0, Math.PI*2); ctx.fill(); ctx.stroke();
  });
  ctx.fillStyle = '#e24b3a';
  ctx.beginPath(); ctx.arc(0, 0, 2.5, 0, Math.PI*2); ctx.fill();
  ctx.restore();
}

function draw(){
  const onlyRet = document.getElementById('onlyRet').checked;
  const showLive = document.getElementById('showLive').checked;
  const cv = document.getElementById('cv');
  const dpr = window.devicePixelRatio || 1;
  cv.width = cv.clientWidth * dpr; cv.height = cv.clientHeight * dpr;
  const ctx = cv.getContext('2d');
  ctx.fillStyle = '#10140f'; ctx.fillRect(0,0,cv.width,cv.height);
  const cssW = cv.clientWidth, cssH = cv.clientHeight;
  if (!cssW || !cssH) return;
  const b = mapBox(cssW, cssH);
  const W = cv.width, H = cv.height;
  const s = Math.min(W / Math.max(0.1, b.x1-b.x0), H / Math.max(0.1, b.y1-b.y0));
  const ox = (W - (b.x1-b.x0)*s)/2;
  const oy = (H - (b.y1-b.y0)*s)/2;
  const X = x => ox + (x-b.x0)*s;
  const Y = y => H - (oy + (y-b.y0)*s);
  ctx.strokeStyle = '#242a22'; ctx.lineWidth = 1;
  for (let x=Math.ceil(b.x0); x<=b.x1; x++){
    ctx.beginPath(); ctx.moveTo(X(x), Y(b.y0)); ctx.lineTo(X(x), Y(b.y1)); ctx.stroke();
  }
  for (let y=Math.ceil(b.y0); y<=b.y1; y++){
    ctx.beginPath(); ctx.moveTo(X(b.x0), Y(y)); ctx.lineTo(X(b.x1), Y(y)); ctx.stroke();
  }
  tracks.forEach((t,i) => {
    const c = COLS[i % COLS.length];
    const all = t.pts || [];
    const pts = onlyRet ? all.filter(p => p.s===7) : all;
    if (pts.length){
      ctx.strokeStyle = c; ctx.globalAlpha = 0.5; ctx.lineWidth = 1.5; ctx.beginPath();
      pts.forEach((p,k) => { const w=worldXY(p.x,p.y); k? ctx.lineTo(X(w[0]),Y(w[1])) : ctx.moveTo(X(w[0]),Y(w[1])); });
      ctx.stroke(); ctx.globalAlpha = 1;
    }
    ctx.fillStyle = c; ctx.font = '12px ui-sans-serif';
    ctx.fillText(t.name, 16, 22+i*16);
  });
  if (showLive) {
    const srcPts = liveTrail();
    const livePts = onlyRet ? srcPts.filter(p => p.s===7) : srcPts;
    if (livePts.length){
      ctx.strokeStyle = '#b7c4b2'; ctx.lineWidth = 2.5; ctx.beginPath();
      livePts.forEach((p,k) => { const w=worldXY(p.x,p.y); k? ctx.lineTo(X(w[0]),Y(w[1])) : ctx.moveTo(X(w[0]),Y(w[1])); });
      ctx.stroke();
    }
    const ret = livePts.filter(p => p.s===7);
    if (ret.length){
      ctx.strokeStyle = '#d9897a'; ctx.lineWidth = 2.5; ctx.beginPath();
      ret.forEach((p,k) => { const w=worldXY(p.x,p.y); k? ctx.lineTo(X(w[0]),Y(w[1])) : ctx.moveTo(X(w[0]),Y(w[1])); });
      ctx.stroke();
    }
    const rp = live.report || {};
    // dock 0,0 — body along a=0 (자북+14°)
    {
      const d = worldXY(0,0);
      const dx = X(d[0]), dy = Y(d[1]);
      const aw = worldA(0);
      ctx.save();
      ctx.translate(dx, dy);
      ctx.rotate(-aw);
      ctx.strokeStyle = '#e8eadf'; ctx.lineWidth = 1.5;
      ctx.strokeRect(-7, -10, 14, 20);
      ctx.fillStyle = '#e24b3a';
      ctx.fillRect(-5, -10, 10, 4);
      ctx.restore();
      ctx.fillStyle = '#e8eadf';
      ctx.font = '11px ui-sans-serif';
      ctx.fillText('dock 14°', dx+12, dy-8);
      // north tick top-left of map
      ctx.fillStyle = '#b7c4b2';
      ctx.fillText('N 자북', 58, 22);
    }
    if (rp.x!=null){
      const w = worldXY(+rp.x, +rp.y);
      const px = X(w[0]), py = Y(w[1]), a = worldA(+rp.a || 0);
      ctx.fillStyle = '#e24b3a';
      ctx.beginPath(); ctx.arc(px, py, 7, 0, 7); ctx.fill();
      ctx.strokeStyle = '#e24b3a'; ctx.lineWidth = 2;
      ctx.beginPath(); ctx.moveTo(px, py);
      ctx.lineTo(px - Math.sin(a)*22, py - Math.cos(a)*22); ctx.stroke();
      ctx.fillStyle = '#e24b3a';
      ctx.font = '12px ui-sans-serif';
      ctx.fillText(degTxt(+rp.a||0), px + 12, py - 10);
      drawMowerBox(+rp.a || 0);
    } else {
      drawMowerBox(null);
    }
  } else {
    drawMowerBox(null);
  }
}
async function tick(){
  try {
    const s = await (await fetch('/state')).json();
    live.pts = s.live_pts || [];
    live.report = s.report || {};
    live.status = s.status || {};
    live.age = s.report_ts ? (Date.now()/1000 - s.report_ts) : 999;
    const st = live.status, rp = live.report;
    const gwLabel = (st.board || st.ip) ? ((st.board||'GW')+' '+(st.wifi_rssi||'')) : 'GW --';
    const gwAge = s.status_ts ? (Date.now()/1000 - s.status_ts) : 999;
    const gwCls = (st.board || st.ip) ? (gwAge < 20 ? 'on' : 'wait') : 'off';
    setChip('chip-gw', gwLabel, gwCls);
    if (st.adv_fresh && st.adv_rssi) setChip('chip-adv', 'ADV '+st.adv_rssi, 'on');
    else if (st.adv_rssi) setChip('chip-adv', 'ADV '+st.adv_rssi, 'wait');
    else setChip('chip-adv', 'ADV --', 'off');
    if (st.ble) setChip('chip-ble', 'BLE '+(st.ble_rssi||''), 'on');
    else if (st.stay_down) setChip('chip-ble', 'BLE 해제', 'wait');
    else setChip('chip-ble', 'BLE --', 'off');
    const el = document.getElementById('info');
    el.textContent = [
      (rp.mower || st.mower || ''),
      rp.x!=null ? ('pos '+Number(rp.x).toFixed(2)+','+Number(rp.y).toFixed(2)+'  '+degTxt(rp.a)) : 'pos --',
      rp.status!=null ? ('s='+rp.status) : ((st.s!=null) ? ('s='+st.s) : ''),
      (hours<1? (hours*60)+'분' : hours+'h'),
      'influx '+(histMeta.src||'--')+' hist='+((histMeta.n_hist!=null)?histMeta.n_hist:0)+' live='+((live.pts||[]).length),
      live.age>5 ? 'stale' : ''
    ].filter(Boolean).join('\n');
    draw();
    if (!tick._n) tick._n = 0;
    tick._n += 1;
    if (tick._n % 20 === 0) loadRecent();
  } catch(e) {}
  setTimeout(tick, 400);
}
let _recentAt = 0;
setInterval(() => { loadRecent(); }, 8000);
window.addEventListener('resize', draw);
(function bindLiveMap(){
  const cv = document.getElementById('cv');
  if (!cv) return;
  const pts = new Map();
  let pinch = 0;
  cv.addEventListener('wheel', ev => {
    ev.preventDefault(); ev.stopPropagation();
    const r = cv.getBoundingClientRect();
    mapZoomAt(ev.clientX - r.left, ev.clientY - r.top, ev.deltaY < 0 ? 1.15 : 1/1.15);
    draw();
  }, {passive:false});
  cv.addEventListener('pointerdown', ev => {
    ev.preventDefault();
    cv.setPointerCapture(ev.pointerId);
    pts.set(ev.pointerId, {x: ev.clientX, y: ev.clientY});
    if (pts.size === 2) {
      const a = [...pts.values()];
      pinch = Math.hypot(a[0].x-a[1].x, a[0].y-a[1].y) || 1;
    }
  });
  cv.addEventListener('pointermove', ev => {
    if (!pts.has(ev.pointerId)) return;
    if (pts.size === 2) {
      pts.set(ev.pointerId, {x: ev.clientX, y: ev.clientY});
      const a = [...pts.values()];
      const d = Math.hypot(a[0].x-a[1].x, a[0].y-a[1].y) || 1;
      const r = cv.getBoundingClientRect();
      mapZoomAt((a[0].x+a[1].x)/2 - r.left, (a[0].y+a[1].y)/2 - r.top, d / pinch);
      pinch = d;
      draw();
      return;
    }
    const p = pts.get(ev.pointerId);
    mapPan(ev.clientX - p.x, ev.clientY - p.y);
    pts.set(ev.pointerId, {x: ev.clientX, y: ev.clientY});
    draw();
  });
  const up = ev => { pts.delete(ev.pointerId); };
  cv.addEventListener('pointerup', up);
  cv.addEventListener('pointercancel', up);
})();
loadRecent();
tick();
</script>
</body>
</html>
"""

def make_client() -> mqtt.Client:
    cid = "mower-live-map-%d" % os.getpid()
    kwargs: dict[str, Any] = {"client_id": cid}
    ver = getattr(mqtt, "CallbackAPIVersion", None)
    if ver is not None:
        kwargs["callback_api_version"] = ver.VERSION2
        client = mqtt.Client(**kwargs)
    else:
        client = mqtt.Client(client_id=cid)
    if MQTT_USER:
        client.username_pw_set(MQTT_USER, MQTT_PASS or None)
    return client


client = make_client()


def on_connect(client, _u, _f, reason_code, _p=None) -> None:
    client.subscribe(TOPIC_STATUS)
    client.subscribe(TOPIC_REPORT)
    client.subscribe(TOPIC_BLE_RX)
    client.subscribe(TOPIC_YOLO)
    rc = getattr(reason_code, "value", reason_code)
    with state_lock:
        state["mqtt"] = "ok" if rc in (0, "Success") else f"rc={rc}"


def on_disconnect(client, _u, *args) -> None:
    with state_lock:
        state["mqtt"] = "offline"


def on_message(_c, _u, msg) -> None:
    text = msg.payload.decode("utf-8", "replace")
    try:
        obj = json.loads(text)
    except json.JSONDecodeError:
        return
    if not isinstance(obj, dict):
        return
    if msg.topic == TOPIC_YOLO:
        with state_lock:
            state["yolo"] = obj
        return
    if msg.topic == TOPIC_STATUS:
        if obj.get("method") or obj.get("data"):
            return
        with state_lock:
            # 같은 토픽에 하트비트(board/ip)와 짧은 이벤트(ip 없음)가 섞이면
            # 통째로 갈아끼울 때 GW -- / feather-gw 가 번갈아 나온다. 병합.
            prev = dict(state.get("status") or {})
            for k, v in obj.items():
                if k in ("ip", "board", "wifi_rssi") and not v:
                    continue
                prev[k] = v
            state["status"] = prev
            state["status_ts"] = time.time()
            rp = dict(state.get("report") or {})
            try:
                sv = int(obj.get("s")) if obj.get("s") is not None else None
            except (TypeError, ValueError):
                sv = None
            if sv is not None and sv >= 0:
                rp["status"] = sv
                state["report"] = rp
        return
    if msg.topic == TOPIC_REPORT:
        with state_lock:
            if "x" in obj:
                state["report"] = obj
                state["report_ts"] = time.time()
                try:
                    x, y = float(obj["x"]), float(obj["y"])
                    stv = int(obj["status"]) if obj.get("status") is not None else -1
                except (TypeError, ValueError):
                    pass
                else:
                    pts = state["live_pts"]
                    last = pts[-1] if pts else None
                    if last is None or abs(x - last["x"]) + abs(y - last["y"]) > 0.03 or last.get("s") != stv:
                        pts.append({"x": x, "y": y, "s": stv, "a": float(obj.get("a") or 0), "t": time.time()})
                        if len(pts) > 8000:
                            del pts[: len(pts) - 8000]
        return
    if msg.topic == TOPIC_BLE_RX:
        data = obj.get("data") if isinstance(obj.get("data"), dict) else {}
        rp = data.get("robot_pos") if isinstance(data.get("robot_pos"), dict) else None
        if isinstance(rp, dict) and isinstance(rp.get("point"), list) and len(rp["point"]) >= 2:
            try:
                x, y = float(rp["point"][0]), float(rp["point"][1])
                a = float(rp.get("angle") or 0)
            except (TypeError, ValueError):
                return
            with state_lock:
                prev = state.get("report") or {}
                stv = int(prev.get("status") or -1)
                state["report"] = {
                    "x": x, "y": y, "a": a, "status": stv, "src": "ble_local",
                    "mower": prev.get("mower"),
                }
                state["report_ts"] = time.time()
                pts = state["live_pts"]
                last = pts[-1] if pts else None
                if last is None or abs(x - last["x"]) + abs(y - last["y"]) > 0.03 or last.get("s") != stv:
                    pts.append({"x": x, "y": y, "s": stv, "a": a, "t": time.time()})
                    if len(pts) > 8000:
                        del pts[: len(pts) - 8000]
        if data.get("status") is not None:
            try:
                cur = int(data["status"])
            except (TypeError, ValueError):
                return
            with state_lock:
                rp2 = dict(state.get("report") or {})
                rp2["status"] = cur
                state["report"] = rp2


client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: Any) -> None:
        if self.path.split("?", 1)[0] in ("/state", "/recent", "/snap"):
            return
        print("[%s] " % self.log_date_time_string() + fmt % args)

    def _send(self, code: int, body: bytes, ctype: str) -> None:
        try:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path in ("/", "/index.html", "/map"):
            q = parse_qs(urlparse(self.path).query)
            want = (
                path == "/map"
                or "map" in q
                or (q.get("only") or [""])[0] in ("map", "1")
            )
            html = HTML
            if want:
                html = html.replace('<body>', '<body class="maponly">', 1)
                html = html.replace('<div class="wrap" id="wrap">',
                                    '<div class="wrap maponly" id="wrap">', 1)
            self._send(200, html.encode("utf-8"), "text/html; charset=utf-8")
            return
        if path == "/snap":
            jpg = _snap.get("jpg") or b""
            if not jpg:
                self._send(503, b"", "image/jpeg")
                return
            self._send(200, jpg, "image/jpeg")
            return
        if path == "/state":
            with state_lock:
                payload = dict(state)
            self._send(200, json.dumps(payload, ensure_ascii=False).encode("utf-8"), "application/json")
            return
        if path == "/recent":
            raw = (parse_qs(urlparse(self.path).query).get("hours") or ["0.5"])[0]
            try:
                hrs = float(raw)
            except ValueError:
                hrs = 0.5
            if hrs not in (0.5, 1.0, 2.0, 3.0, 6.0, 12.0):
                hrs = 0.5
            body = json.dumps(_recent_points(hrs), ensure_ascii=False).encode("utf-8")
            self._send(200, body, "application/json")
            return
        self._send(404, b"not found", "text/plain")


def snap_loop() -> None:
    while True:
        try:
            req = urllib.request.Request(GO2_SNAP, headers={"User-Agent": "mower-live-map"})
            with urllib.request.urlopen(req, timeout=3) as r:
                data = r.read()
            if data[:2] == b"\xff\xd8":
                _snap["jpg"] = data
                _snap["t"] = time.time()
        except Exception:
            time.sleep(0.5)
        time.sleep(0.35)


def mqtt_thread() -> None:
    while True:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, 30)
            client.loop_forever()
        except Exception as exc:
            print("mqtt", exc)
            time.sleep(2)


def main() -> None:
    threading.Thread(target=mqtt_thread, daemon=True).start()
    if GO2_SNAP:
        threading.Thread(target=snap_loop, daemon=True).start()
    httpd = ThreadingHTTPServer((HTTP_HOST, HTTP_PORT), Handler)
    url = f"http://127.0.0.1:{HTTP_PORT}/"
    print(f"live map  {url}")
    if INFLUX_URL and INFLUX_TOKEN:
        print(f"recent    influx {INFLUX_URL} {INFLUX_BUCKET}")
    else:
        print("recent    INFLUX_TOKEN missing — put influx.env or influx.token next to this py")
    try:
        webbrowser.open(url)
    except Exception:
        pass
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print('stop')


if __name__ == "__main__":
    main()

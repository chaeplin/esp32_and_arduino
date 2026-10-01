#!/usr/bin/env python3
"""Sunseeker mower MQTT drive pad — browser UI (no Tk).

Apple CLT python3.9 + system Tk 8.5 crashes on macOS 15 (Tcl_Panic / TkpInit).
This pad serves a local page instead.

    python3 -m pip install paho-mqtt
    python3 mower_mqtt_pad.py
    # browser: http://127.0.0.1:8765

속도는 중간 고정. 스냅 이미지 없음.
s=18 비상정지. 상태 표시 2줄.
맵: MQTT 실시간 + Influx 궤적. jsonl 없음.
"""

from __future__ import annotations

import csv
import io
import json
import math
import os
import threading
import time
import urllib.error
import urllib.request
import webbrowser
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

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
HTTP_PORT = 8765

TOPIC_CMD = "mower/cmd"
TOPIC_STATUS = "mower/status"
TOPIC_NOTIFY = "mower/notify"
TOPIC_LWT = "mower/lwt"
TOPIC_BLE_TX = "mower/ble/tx"
TOPIC_BLE_RX = "mower/ble/rx"
TOPIC_REPORT = "mower/report"
TOPIC_GPS = "gps/rover"
MACRO_DIR = HERE / "macros"
PAD_LIVE_AGE = 3600.0

INFLUX_URL = os.environ.get("INFLUX_URL", "http://127.0.0.1:8086").rstrip("/")
INFLUX_ORG = os.environ.get("INFLUX_ORG", "yard")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET", "mower")
_hist_cache: dict[str, Any] = {"at": 0.0, "hours": None, "pts": [], "err": ""}


def _influx_token() -> str:
    t = os.environ.get("INFLUX_TOKEN", "").strip()
    if t:
        return t
    for p in (HERE / "influx.token", Path.home() / "influxdb-cfg" / "token"):
        try:
            if p.is_file():
                return p.read_text(encoding="utf-8").strip().splitlines()[0].strip()
        except OSError:
            pass
    return ""


def _iso_to_unix(s: str) -> float:
    if not s:
        return 0.0
    s = s.replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _influx_rows(q: str) -> list[dict[str, str]]:
    token = _influx_token()
    if not token:
        raise RuntimeError("INFLUX_TOKEN")
    url = f"{INFLUX_URL}/api/v2/query?org={INFLUX_ORG}"
    req = urllib.request.Request(
        url, data=q.encode("utf-8"), method="POST",
        headers={
            "Authorization": f"Token {token}",
            "Content-Type": "application/vnd.flux",
            "Accept": "application/csv",
        },
    )
    with urllib.request.urlopen(req, timeout=12) as r:
        raw = r.read().decode("utf-8", errors="replace")
    rows: list[dict[str, str]] = []
    for block in raw.split("\n\n"):
        lines = [ln for ln in block.splitlines() if ln and not ln.startswith("#")]
        if not lines:
            continue
        for rec in csv.DictReader(io.StringIO("\n".join(lines))):
            if rec.get("_value") not in (None, ""):
                rows.append(rec)
    return rows


def _recent_from_influx(hours: float) -> tuple[list[dict[str, Any]], str]:
    now = time.time()
    if _hist_cache["pts"] and _hist_cache["hours"] == hours and now - _hist_cache["at"] < 8:
        return list(_hist_cache["pts"]), _hist_cache.get("err") or "cache"
    rng = max(0.25, float(hours))
    q = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -{rng}h)
  |> filter(fn: (r) => r._measurement == "mower" and (r._field == "x" or r._field == "y" or r._field == "a" or r._field == "status"))
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
'''
    try:
        rows = _influx_rows(q)
    except Exception as e:
        _hist_cache.update({"at": now, "hours": hours, "pts": [], "err": str(e)[:120]})
        return [], str(e)[:120]
    pts: list[dict[str, Any]] = []
    last = None
    tmp: list[dict[str, Any]] = []
    for rec in rows:
        try:
            t = _iso_to_unix(rec.get("_time") or "")
            x = float(rec["x"]) if rec.get("x") not in (None, "") else None
            y = float(rec["y"]) if rec.get("y") not in (None, "") else None
        except (TypeError, ValueError):
            continue
        if x is None or y is None:
            continue
        s = -1
        a = 0.0
        try:
            if rec.get("status") not in (None, ""):
                s = int(float(rec["status"]))
        except (TypeError, ValueError):
            pass
        try:
            if rec.get("a") not in (None, ""):
                a = float(rec["a"])
        except (TypeError, ValueError):
            pass
        tmp.append({"t": t, "x": x, "y": y, "s": s, "a": a})
    tmp.sort(key=lambda p: p["t"])
    for p in tmp:
        xy = (p["x"], p["y"])
        if last is None or abs(xy[0] - last[0]) + abs(xy[1] - last[1]) > 0.03:
            pts.append(p)
            last = xy
    _hist_cache.update({"at": now, "hours": hours, "pts": list(pts), "err": ""})
    return pts, "ok"


def _prune_live(pts: list[dict[str, Any]], now: float | None = None) -> None:
    cut = (now if now is not None else time.time()) - PAD_LIVE_AGE
    i = 0
    n = len(pts)
    while i < n and float(pts[i].get("t") or 0) < cut:
        i += 1
    if i:
        del pts[:i]


state_lock = threading.Lock()
state: dict[str, Any] = {
    "mqtt": "connecting %s:%s" % (MQTT_HOST, MQTT_PORT),
    "lwt": "",
    "status": {},
    "notify": "",
    "last_cmd": "",
    "updated": 0.0,
    "session": "",
    "recording": False,
    "saved": 0,
    "report": {},
    "mode": "",
    "report_ts": 0.0,
    "status_ts": 0.0,
    "last_s": None,
    "live_pts": [],
    "live_gps": [],
    "gps_lock": False,
    "gps_warm": 0,
    "seq_on": False,
    "seq_n": 0,
    "seq_file": "",
}

_session_path: Path | None = None
_session_fh = None
_was_ble = False
_was_report = False
_last_mow_st = None
_gps_warm: list[tuple[float, float]] = []  # lat,lon until lock


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def _append_log(dir_: str, text: str, persist: bool) -> None:
    with state_lock:
        state["updated"] = time.time()
        state["recording"] = False


def _day_dir(now: datetime | None = None) -> Path:
    return HERE


def _open_session(mower: str) -> None:
    with state_lock:
        state["session"] = ""
        state["recording"] = False
        state["saved"] = 0


def _ensure_session(tag: str) -> None:
    return


def _close_session(reason: str) -> None:
    global _session_fh
    if state.get("recording"):
        _append_log("meta", "session end " + reason, True)
    if _session_fh:
        try:
            _session_fh.close()
        except Exception:
            pass
        _session_fh = None
    with state_lock:
        state["recording"] = False



HTML = r"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no, viewport-fit=cover"/>
<meta name="apple-mobile-web-app-capable" content="yes"/>
<meta name="mobile-web-app-capable" content="yes"/>
<title>Mower MQTT pad</title>
<style>
  :root { --soil:#141611; --sage:#b7c4b2; --clip:#e8eadf; --ink:#1c1f1a; --moss:#3d4a3a; --stop:#8b3a2a; }
  * { box-sizing: border-box; -webkit-tap-highlight-color: transparent; }
  html, body, main, button, .status, .hint, h1, label {
    -webkit-user-select: none; user-select: none;
    -webkit-touch-callout: none; -webkit-user-drag: none;
  }
  html { touch-action: manipulation; }
  html, body { height:100%; margin:0; background:var(--soil); color:var(--clip);
    font: 16px/1.35 ui-sans-serif, system-ui, sans-serif; overflow:auto; overscroll-behavior: none; }
  main {
    height:100vh; height:100dvh; max-width: 520px; margin:0 auto;
    padding: 8px 14px 8px; display:flex; flex-direction:column; gap:6px;
  }
  .top { flex:0 0 auto; }
  h1 { font-size: 16px; font-weight: 650; letter-spacing:.02em; margin:0 0 6px; color:var(--sage); }
  button.rec.on { background:#8b3a2a; color:#e8eadf; }
  .status {
    background:#1c201b; border:1px solid #2a3128; border-radius:10px;
    padding:8px 10px; font-size:12px; min-height:4.6em; line-height:1.45;
    overflow:hidden; white-space:pre-wrap; color:#c9d1c4;
  }
  .links { display:flex; gap:6px; }
  .chip {
    flex:1; text-align:center; font-size:11px; font-weight:650;
    padding:7px 4px; border-radius:8px; background:#1c201b;
    border:1px solid #2a3128; color:#7e8778;
  }
  .chip.on { color:#9ccc8a; border-color:#3d5a3a; background:#182016; }
  .chip.off { color:#d9897a; border-color:#4a2e2a; background:#201614; }
  .chip.wait { color:#d6c37a; border-color:#4a4530; background:#1c1a12; }
  .modes { display:flex; flex-wrap:wrap; gap:4px; }
  .modes .chip { flex:1 1 auto; min-width:3.2em; padding:6px 4px; font-size:10px; }
  .modes .chip.sel { color:#141611; border-color:#b7c4b2; background:#b7c4b2; }
  .modes .chip.estop { color:#e8eadf; border-color:#8b3a2a; background:#5a241c; }
  .modes .chip.estop.sel { color:#fff; border-color:#d9897a; background:#8b3a2a; }
  .modes .chip.emg { color:#d9897a; border-color:#4a2e2a; }
  .modes .chip.sel.emg { color:#e8eadf; border-color:#8b3a2a; background:#8b3a2a; }
  .row { display:flex; gap:8px; align-items:center; flex-wrap:nowrap; }
  .row.wrap { flex-wrap:wrap; }
  label { font-size:12px; color:var(--sage); white-space:nowrap; }
  select, input[type=number] {
    background:#1c201b; color:var(--clip); border:1px solid #3d4a3a; border-radius:8px;
    padding:8px 10px; font-size:16px;  /* iOS focus zoom 방지 */
    -webkit-user-select: text; user-select: text;
  }
  .pad {
    flex:0 0 auto;
    display:grid; grid-template-columns: 1fr 1fr 1fr;
    grid-template-rows: 56px 56px 56px;
    gap:8px;
  }
  .actions { flex:0 0 auto; display:grid; grid-template-columns: repeat(4, 1fr); gap:6px; }
  .actions2 { flex:0 0 auto; display:grid; grid-template-columns: repeat(4, 1fr); gap:6px; }
  button {
    appearance:none; border:0; border-radius:12px; padding:0; cursor:pointer;
    font-size:16px; font-weight:650; background:var(--moss); color:var(--clip);
    min-height:56px; touch-action: none;
  }
  button:active { filter: brightness(1.15); }
  button.stop { background: var(--stop); }
  button.ghost { background:#242a22; color:var(--sage); font-size:12px; min-height:40px; padding:0 4px; }
  .hint { flex:0 0 auto; font-size:11px; color:#7e8778; margin:0; }
  .ok { color:#9ccc8a; } .bad { color:#d9897a; }
  #mmap-wrap { position:relative; flex:0 0 auto; height:22vh; min-height:130px; max-height:190px;
    background:#0c0e0b; border-radius:10px; overflow:hidden; border:1px solid #2a3128; }
  #mmap { width:100%; height:100%; display:block; touch-action:none; }
  .zbar { position:absolute; right:6px; top:6px; display:flex; flex-direction:column; gap:4px; width:auto; }
  .zbar .zbtn { min-height:32px; width:36px; margin:0; padding:0; font-size:16px; }
</style>
</head>
<body>
<main>
  <div class="top">
    <div class="links">
      <span id="chip-gw" class="chip">GW --</span>
      <span id="chip-adv" class="chip">ADV --</span>
      <span id="chip-ble" class="chip">BLE --</span>
      <span id="chip-batt" class="chip">BAT --</span>
    </div>
    <div id="mqtt" class="status">connecting…</div>
    <div id="modes" class="modes"></div>
  </div>
  <div id="mmap-wrap">
    <canvas id="mmap"></canvas>
    <div class="zbar">
      <button type="button" class="ghost zbtn" onclick="mapZoom(1.3)">+</button>
      <button type="button" class="ghost zbtn" onclick="mapZoom(1/1.3)">−</button>
      <button type="button" class="ghost zbtn" onclick="mapFit()">맞춤</button>
      <button type="button" class="ghost zbtn" data-h="0.5" onclick="setMapH(0.5)">30m</button>
      <button type="button" class="ghost zbtn" data-h="1" onclick="setMapH(1)">1h</button>
      <button type="button" class="ghost zbtn" data-h="2" onclick="setMapH(2)">2h</button>
    </div>
  </div>
  <div class="row">
    <label>속도 <b style="color:#9ccc8a">중간 고정</b></label>
    <button id="b-blade" class="ghost" style="min-height:36px;flex:1" onclick="toggleBlade()">칼날 OFF</button>
  </div>
  <div class="pad">
    <span></span>
    <button id="b-fwd">전</button>
    <span></span>
    <button id="b-left">좌</button>
    <button class="stop" onclick="send({cmd:'stop'}); bladeOn=false; paintBlade();">정지</button>
    <button id="b-right">우</button>
    <span></span>
    <button id="b-rev">후</button>
    <span></span>
  </div>
  <div class="actions">
    <button class="ghost" onclick="send({cmd:'spin', ms: spinMs()})">제자리회전</button>
    <button class="ghost" onclick="send({cmd:'pause'})">작업중지</button>
    <button class="ghost" onclick="send({cmd:'start'})">자동예초</button>
    <button class="ghost" onclick="send({cmd:'home'})">충전대</button>
  </div>
  <div class="actions2">
    <button class="ghost" onclick="send({cmd:'scan'})">스캔</button>
    <button class="ghost" onclick="send({cmd:'connect'})">연결</button>
    <button class="ghost" onclick="send({cmd:'handshake'})">RC진입</button>
    <button class="ghost" onclick="if(confirm('BLE 해제?')) send({cmd:'disconnect'})">해제</button>
  </div>
</main>
<script>
function speed(){ return 'medium'; }
function spinMs(){ return 1500; }
function applySpeed(){ send({cmd:'speed', level:'medium'}); }
let holding = null;
let bladeOn = false;
function paintBlade(){
  const el = document.getElementById('b-blade');
  el.textContent = bladeOn ? '칼날 ON' : '칼날 OFF';
  el.style.background = bladeOn ? '#8b3a2a' : '#242a22';
  el.style.color = bladeOn ? '#e8eadf' : '';
}
function toggleBlade(){
  bladeOn = !bladeOn;
  paintBlade();
  send({cmd:'blade', on: bladeOn});
}
function hold(cmd){
  if (holding === cmd) return;
  holding = cmd;
  send({cmd});
}
function release(){
  if (!holding) return;
  holding = null;
  send({cmd:'stop'});
}
function bindHold(id, cmd){
  const el = document.getElementById(id);
  const down = ev => { ev.preventDefault(); ev.stopPropagation(); if (el.setPointerCapture && ev.pointerId!=null) el.setPointerCapture(ev.pointerId); hold(cmd); };
  const up = ev => { ev.preventDefault(); release(); };
  el.addEventListener('pointerdown', down);
  el.addEventListener('pointerup', up);
  el.addEventListener('pointercancel', up);
  el.addEventListener('lostpointercapture', release);
  el.addEventListener('touchstart', down, {passive:false});
  el.addEventListener('touchend', up, {passive:false});
  el.addEventListener('touchcancel', up, {passive:false});
  el.addEventListener('contextmenu', ev => ev.preventDefault());
}
document.addEventListener('gesturestart', ev => ev.preventDefault());
document.addEventListener('dblclick', ev => ev.preventDefault());
document.addEventListener('contextmenu', ev => {
  if (ev.target && ev.target.closest && ev.target.closest('button')) ev.preventDefault();
});
bindHold('b-fwd','fwd');
bindHold('b-rev','rev');
bindHold('b-left','left');
bindHold('b-right','right');
const KEY_CMD = {
  ArrowUp:'fwd', ArrowDown:'rev', ArrowLeft:'left', ArrowRight:'right',
  w:'fwd', s:'rev', a:'left', d:'right', W:'fwd', S:'rev', A:'left', D:'right'
};
function keyTargetOk(ev){
  const t = ev.target;
  if (!t) return true;
  const tag = (t.tagName||'').toLowerCase();
  return tag !== 'input' && tag !== 'textarea' && tag !== 'select';
}
document.addEventListener('keydown', ev => {
  if (!keyTargetOk(ev)) return;
  if (ev.key === ' ' || ev.code === 'Space') {
    ev.preventDefault();
    release();
    return;
  }
  const cmd = KEY_CMD[ev.key];
  if (!cmd) return;
  ev.preventDefault();
  if (ev.repeat) return;
  hold(cmd);
});
document.addEventListener('keyup', ev => {
  if (!keyTargetOk(ev)) return;
  const cmd = KEY_CMD[ev.key];
  if (!cmd) return;
  ev.preventDefault();
  if (holding === cmd) release();
});
window.addEventListener('blur', release);
async function send(payload){
  try {
    await fetch('/cmd', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
  } catch(e) { document.getElementById('mqtt').textContent = 'pad error '+e; }
}
function setChip(id, text, kind){
  const el = document.getElementById(id);
  el.textContent = text;
  el.className = 'chip' + (kind ? ' '+kind : '');
}
const MOW_ST = [
  [1,'대기'],[2,'작업'],[3,'일시정지'],[7,'복귀'],
  [8,'복귀정지'],[9,'충전'],[10,'만충'],[11,'RC'],[14,'이어예초'],
  [18,'비상정지']
];
let lastS = null;
let lastRp = {};
let lastElec = null;
function paintModes(cur){
  const box = document.getElementById('modes');
  const known = new Set(MOW_ST.map(x=>x[0]));
  let items = MOW_ST.slice();
  if (cur!=null && cur>=0 && !known.has(cur)) items = items.concat([[cur,'s='+cur]]);
  box.innerHTML = items.map(([n,lab]) =>
    '<span class="chip'+(n===cur?' sel':'')+(n===18?' emg':'')+'" data-s="'+n+'">'+lab+'</span>'
  ).join('');
}
let mapPts = [];
let mapHours = 1;
function setMapH(h){
  mapHours = h;
  document.querySelectorAll('[data-h]').forEach(el => el.classList.toggle('on', Number(el.dataset.h)===h));
  loadMapHist();
}
async function loadMapHist(){
  try {
    const r = await fetch('/recent?hours='+mapHours);
    const j = await r.json();
    mapPts = j.pts || [];
    drawMap();
  } catch(e) {}
}
let mapRp = {};
let mapView = {cx:0, cy:0, span:12, user:false};
function worldXY(x, y){ return [-(+y), +x]; }
function worldA(a){ return (+a || 0) + Math.PI/2; }
function mapBounds(){
  const xs=[], ys=[];
  (mapPts||[]).forEach(p => { const w=worldXY(p.x,p.y); xs.push(w[0]); ys.push(w[1]); });
  if (mapRp && mapRp.x!=null) { const w=worldXY(mapRp.x,mapRp.y); xs.push(w[0]); ys.push(w[1]); }
  if (!xs.length) return {x0:-6,y0:-4,x1:4,y1:4};
  const pad = 1.2;
  return {x0:Math.min(...xs)-pad, y0:Math.min(...ys)-pad, x1:Math.max(...xs)+pad, y1:Math.max(...ys)+pad};
}
function mapApplyFit(){
  const d = mapBounds();
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
  const cv = document.getElementById('mmap');
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
  const cv = document.getElementById('mmap');
  const W = cv.clientWidth, H = cv.clientHeight;
  const b = mapBox(W, H);
  mapView.cx -= dx / Math.max(1,W) * (b.x1-b.x0);
  mapView.cy += dy / Math.max(1,H) * (b.y1-b.y0);
  mapView.user = true;
}
function mapZoom(k){
  const cv = document.getElementById('mmap');
  mapZoomAt(cv.clientWidth/2, cv.clientHeight/2, k);
  drawMap();
}
function mapFit(){ mapApplyFit(); drawMap(); }
function drawMap(){
  const cv = document.getElementById('mmap');
  if (!cv) return;
  const dpr = window.devicePixelRatio || 1;
  const cssW = cv.clientWidth, cssH = cv.clientHeight;
  if (!cssW || !cssH) return;
  cv.width = cssW * dpr; cv.height = cssH * dpr;
  const ctx = cv.getContext('2d');
  ctx.fillStyle = '#10140f'; ctx.fillRect(0,0,cv.width,cv.height);
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
  const pts = mapPts || [];
  if (pts.length){
    ctx.strokeStyle = '#b7c4b2'; ctx.lineWidth = 2.5; ctx.beginPath();
    pts.forEach((p,k) => { const w=worldXY(p.x,p.y); k? ctx.lineTo(X(w[0]),Y(w[1])) : ctx.moveTo(X(w[0]),Y(w[1])); });
    ctx.stroke();
    const ret = pts.filter(p => p.s===7);
    if (ret.length){
      ctx.strokeStyle = '#d9897a'; ctx.lineWidth = 2.5; ctx.beginPath();
      ret.forEach((p,k) => { const w=worldXY(p.x,p.y); k? ctx.lineTo(X(w[0]),Y(w[1])) : ctx.moveTo(X(w[0]),Y(w[1])); });
      ctx.stroke();
    }
  }
  const d0 = worldXY(0,0);
  ctx.fillStyle = '#3a4038';
  ctx.beginPath(); ctx.arc(X(d0[0]), Y(d0[1]), 5, 0, 7); ctx.fill();
  if (mapRp && mapRp.x!=null){
    const w = worldXY(+mapRp.x, +mapRp.y);
    const px = X(w[0]), py = Y(w[1]), a = worldA(+mapRp.a || 0);
    ctx.fillStyle = (mapRp.status===7 || mapRp.status===18) ? '#d9897a' : '#e24b3a';
    ctx.beginPath(); ctx.arc(px, py, 6, 0, 7); ctx.fill();
    ctx.strokeStyle = '#e8eadf'; ctx.lineWidth = 2;
    ctx.beginPath(); ctx.moveTo(px, py);
    ctx.lineTo(px + Math.sin(a)*18, py - Math.cos(a)*18); ctx.stroke();
  }
}
function render(s){
  const el = document.getElementById('mqtt');
  const st = s.status || {};
  let rp = s.report || {};
  const mqttOk = (s.mqtt||'').indexOf('ok')>=0;
  const stAge = s.status_ts ? (Date.now()/1000 - s.status_ts) : 999;
  const rpAge = s.report_ts ? (Date.now()/1000 - s.report_ts) : 999;
  const gwLive = mqttOk && (stAge < 20 || rpAge < 20);
  const gwBle = !!st.ble || (rpAge < 12 && Number(rp.ble_rssi) < 0);
  const advFresh = !!st.adv_fresh && stAge < 20;
  const heard = Number(st.heard_ms);
  const gwLabel = st.board || st.ip || (rpAge<8 ? 'GW rpt' : '');
  setChip('chip-gw', gwLive ? (gwLabel+' '+(st.wifi_rssi||'')) : (mqttOk ? 'GW mqtt' : 'GW --'), gwLive ? 'on' : (mqttOk ? 'wait' : 'off'));
  if (advFresh && st.adv_rssi) setChip('chip-adv', 'ADV '+st.adv_rssi+'  '+Math.round(heard/1000)+'s', 'on');
  else if (st.adv_rssi) setChip('chip-adv', 'ADV '+st.adv_rssi+' stale', 'wait');
  else setChip('chip-adv', 'ADV --', 'off');
  if (gwBle) setChip('chip-ble', 'BLE '+(st.ble_rssi||rp.ble_rssi||''), 'on');
  else if (st.stay_down) setChip('chip-ble', 'BLE 해제', 'wait');
  else setChip('chip-ble', 'BLE --', 'off');
  let elec = (st.elec!=null && st.elec>=0) ? st.elec : rp.elec;
  if (elec!=null && Number(elec)>=0) setChip('chip-batt', 'BAT '+Number(elec)+'%', Number(elec)<=20 ? 'off' : 'on');
  else setChip('chip-batt', 'BAT --', 'off');
  const cand = [st.s, rp.status, rp.s];
  let curS = cand.map(Number).find(v => Number.isFinite(v) && v >= 0);
  if (curS!=null) lastS = curS; else curS = lastS;
  if (rp.x!=null) lastRp = rp; else rp = lastRp;
  if (elec!=null && Number(elec)>=0) lastElec = elec; else elec = lastElec;
  const stName = (MOW_ST.find(x => x[0]===curS)||[])[1] || (curS!=null ? ('s='+curS) : '상태없음');
  const offline = !gwBle;
  const line1 = [
    st.mower || rp.mower || lastRp.mower || '',
    offline ? '오프라인' : (st.rc ? 'RC' : 'BLE'),
    st.holding ? 'HOLD' : '',
    st.mode || rp.src || '',
    s.mqtt || '',
    (curS===18) ? '비상정지' : ''
  ].filter(Boolean).join('  ·  ') || '대기';
  const line2 = [
    stName,
    (curS!=null) ? ('s='+curS) : '',
    (rp.x!=null) ? ('pos '+Number(rp.x).toFixed(2)+','+Number(rp.y).toFixed(2)) : 'pos --',
    (elec!=null && Number(elec)>=0) ? ('batt '+Number(elec)+'%') : '',
    offline ? (stAge<999 ? ('gw '+Math.round(stAge)+'s') : '') : '',
    s.notify ? String(s.notify).slice(0,48) : ''
  ].filter(Boolean).join('  ·  ');
  el.textContent = line1 + '\n' + line2;
  el.title = line1 + '\n' + line2;
  el.className = 'status ' + ((gwLive && gwBle) ? 'ok' : '') + (curS===18 ? ' bad' : '');
  paintModes(curS);
  if (s.live_pts && s.live_pts.length) {
    const seen = new Set(mapPts.map(p => Number(p.x).toFixed(2)+','+Number(p.y).toFixed(2)));
    (s.live_pts||[]).forEach(p => {
      const k = Number(p.x).toFixed(2)+','+Number(p.y).toFixed(2);
      if (!seen.has(k)) { mapPts.push(p); seen.add(k); }
    });
  }
  mapRp = rp;
  drawMap();
}
async function poll(){
  try {
    const r = await fetch('/state');
    render(await r.json());
  } catch(e) {}
  setTimeout(poll, 350);
}
poll();
applySpeed();
loadMapHist();
setInterval(loadMapHist, 15000);
window.addEventListener('resize', drawMap);
(function bindMiniMap(){
  const cv = document.getElementById('mmap');
  if (!cv) return;
  const pts = new Map();
  let pinch = 0;
  cv.addEventListener('wheel', ev => {
    ev.preventDefault(); ev.stopPropagation();
    const r = cv.getBoundingClientRect();
    mapZoomAt(ev.clientX - r.left, ev.clientY - r.top, ev.deltaY < 0 ? 1.15 : 1/1.15);
    drawMap();
  }, {passive:false});
  cv.addEventListener('pointerdown', ev => {
    ev.preventDefault(); ev.stopPropagation();
    cv.setPointerCapture(ev.pointerId);
    pts.set(ev.pointerId, {x: ev.clientX, y: ev.clientY});
    if (pts.size === 2) {
      const a = [...pts.values()];
      pinch = Math.hypot(a[0].x-a[1].x, a[0].y-a[1].y) || 1;
    }
  });
  cv.addEventListener('pointermove', ev => {
    if (!pts.has(ev.pointerId)) return;
    ev.preventDefault(); ev.stopPropagation();
    if (pts.size === 2) {
      pts.set(ev.pointerId, {x: ev.clientX, y: ev.clientY});
      const a = [...pts.values()];
      const d = Math.hypot(a[0].x-a[1].x, a[0].y-a[1].y) || 1;
      const r = cv.getBoundingClientRect();
      mapZoomAt((a[0].x+a[1].x)/2 - r.left, (a[0].y+a[1].y)/2 - r.top, d / pinch);
      pinch = d;
      drawMap();
      return;
    }
    const p = pts.get(ev.pointerId);
    mapPan(ev.clientX - p.x, ev.clientY - p.y);
    pts.set(ev.pointerId, {x: ev.clientX, y: ev.clientY});
    drawMap();
  });
  const up = ev => { pts.delete(ev.pointerId); };
  cv.addEventListener('pointerup', up);
  cv.addEventListener('pointercancel', up);
  cv.addEventListener('contextmenu', ev => ev.preventDefault());
})();
window.addEventListener('keydown', ev => {
  if (ev.repeat) return;
  if (ev.target && (ev.target.tagName==='INPUT' || ev.target.tagName==='SELECT')) return;
  const k = ev.key;
  if (k==='1') hold('fwd');
  else if (k==='2') hold('rev');
  else if (k==='3') hold('left');
  else if (k==='4') hold('right');
  else if (k==='5') send({cmd:'spin', ms: spinMs()});
  else if (k==='b' || k==='B') toggleBlade();
  else if (k===' ') { ev.preventDefault(); release(); send({cmd:'stop'}); }
  else if (k==='h' || k==='H') send({cmd:'home'});
});
window.addEventListener('keyup', ev => {
  const k = ev.key;
  if (k==='1'||k==='2'||k==='3'||k==='4') release();
});
window.addEventListener('blur', release);
</script>
</body>
</html>
"""


def make_client() -> mqtt.Client:
    kwargs: dict[str, Any] = {"client_id": "mower-mqtt-pad-%d" % os.getpid()}
    ver = getattr(mqtt, "CallbackAPIVersion", None)
    if ver is not None:
        kwargs["callback_api_version"] = ver.VERSION2
        client = mqtt.Client(**kwargs)
    else:
        client = mqtt.Client(client_id="mower-mqtt-pad-%d" % os.getpid())
    if MQTT_USER:
        client.username_pw_set(MQTT_USER, MQTT_PASS or None)
    return client


client = make_client()


def on_connect(client, _u, _f, reason_code, _p=None) -> None:
    client.subscribe(TOPIC_STATUS)
    client.subscribe(TOPIC_NOTIFY)
    client.subscribe(TOPIC_LWT)
    client.subscribe(TOPIC_BLE_TX)
    client.subscribe(TOPIC_BLE_RX)
    client.subscribe(TOPIC_REPORT)
    client.subscribe(TOPIC_GPS)
    rc = getattr(reason_code, "value", reason_code)
    ok = rc in (0, "Success") or str(rc) in ("0", "Success")
    with state_lock:
        state["mqtt"] = "ok" if ok else f"rc={rc}"
        state["updated"] = time.time()


def on_disconnect(client, _u, *args) -> None:
    with state_lock:
        state["mqtt"] = "offline"
        state["updated"] = time.time()


def on_message(_c, _u, msg) -> None:
    global _was_ble, _was_report, _last_mow_st
    text = msg.payload.decode("utf-8", "replace")
    if msg.topic in (TOPIC_REPORT, TOPIC_GPS):
        try:
            obj = json.loads(text)
        except json.JSONDecodeError:
            obj = {"raw": text[:200]}
        tag = str(obj.get("mower") or obj.get("src") or "rover")
        _ensure_session(tag)
        _was_report = True
        with state_lock:
            if msg.topic == TOPIC_REPORT and isinstance(obj, dict) and "x" in obj:
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
                        now = time.time()
                        pts.append({"x": x, "y": y, "s": stv, "a": float(obj.get("a") or 0), "t": now})
                        _prune_live(pts, now)
                _ingest_gps(obj)
            state["updated"] = time.time()
        _append_log("rpt" if msg.topic == TOPIC_REPORT else "gps", text, True)
        if msg.topic == TOPIC_REPORT and isinstance(obj, dict) and "status" in obj:
            cur = (obj.get("status"), obj.get("fault"))
            if cur != _last_mow_st:
                _last_mow_st = cur
                _append_log("st", json.dumps({"status": obj.get("status"), "fault": obj.get("fault"),
                                              "x": obj.get("x"), "y": obj.get("y")}, separators=(",", ":")), True)
        return
    if msg.topic == TOPIC_BLE_TX:
        _append_log("tx", text, True)
        return
    if msg.topic == TOPIC_BLE_RX:
        _append_log("rx", text, True)
        try:
            obj = json.loads(text)
        except json.JSONDecodeError:
            return
        data = obj.get("data") if isinstance(obj, dict) else None
        if isinstance(data, dict) and isinstance(data.get("robot_pos"), dict):
            rp = data["robot_pos"]
            pt = rp.get("point") if isinstance(rp, dict) else None
            if isinstance(pt, list) and len(pt) >= 2:
                try:
                    x, y = float(pt[0]), float(pt[1])
                    a = float(rp.get("angle") or 0)
                except (TypeError, ValueError):
                    return
                with state_lock:
                    prev = state.get("report") or {}
                    stv = int(prev.get("status") or -1)
                    state["report"] = {"x": x, "y": y, "a": a, "status": stv, "src": "ble_local"}
                    state["report_ts"] = time.time()
                    pts = state["live_pts"]
                    last = pts[-1] if pts else None
                    if last is None or abs(x - last["x"]) + abs(y - last["y"]) > 0.03 or last.get("s") != stv:
                        now = time.time()
                        pts.append({"x": x, "y": y, "s": stv, "a": a, "t": now})
                        _prune_live(pts, now)
        if isinstance(data, dict) and data.get("elec") is not None:
            ev = data["elec"]
            try:
                if isinstance(ev, dict):
                    ev = ev.get("level", ev.get("percent", ev.get("value", ev.get("soc"))))
                ev = int(ev)
            except (TypeError, ValueError):
                ev = None
            if ev is not None:
                with state_lock:
                    rp = dict(state.get("report") or {})
                    rp["elec"] = ev
                    state["report"] = rp
                _append_log("st", json.dumps({"elec": ev}, separators=(",", ":")), True)
        if isinstance(data, dict) and data.get("status") is not None:
            try:
                cur = int(data["status"])
            except (TypeError, ValueError):
                cur = None
            if cur is not None:
                with state_lock:
                    rp = dict(state.get("report") or {})
                    rp["status"] = cur
                    if "fault" in data:
                        rp["fault"] = data.get("fault")
                    state["report"] = rp
                if (cur, data.get("fault")) != _last_mow_st:
                    _last_mow_st = (cur, data.get("fault"))
                    _append_log("st", json.dumps({"status": cur, "fault": data.get("fault")}, separators=(",", ":")), True)
        return
    if msg.topic == TOPIC_NOTIFY:
        with state_lock:
            state["notify"] = text[:400]
            state["updated"] = time.time()
        _append_log("ev", text, True)
        return
    if msg.topic == TOPIC_LWT:
        with state_lock:
            state["lwt"] = text
            state["updated"] = time.time()
        return
    if msg.topic == TOPIC_STATUS:
        try:
            st = json.loads(text)
        except json.JSONDecodeError:
            st = {"raw": text[:300]}
        ble = bool(st.get("ble"))
        mower = str(st.get("mower") or "")
        with state_lock:
            state["status"] = st
            state["status_ts"] = time.time()
            state["mode"] = str(st.get("mode") or "")
            state["updated"] = time.time()
            rp = dict(state.get("report") or {})
            if st.get("elec") is not None:
                rp["elec"] = st.get("elec")
            try:
                sv = int(st.get("s")) if st.get("s") is not None else None
            except (TypeError, ValueError):
                sv = None
            if sv is not None and sv >= 0:
                rp["status"] = sv
                state["last_s"] = sv
            state["report"] = rp
            if st.get("pose") and st.get("x") is not None:
                rp = dict(state.get("report") or {})
                rp.update({
                    "x": st.get("x"), "y": st.get("y"), "a": st.get("a"),
                    "status": st.get("s", rp.get("status")),
                    "mower": st.get("mower") or rp.get("mower"),
                })
                state["report"] = rp
                state["report_ts"] = time.time()
        if (ble and not _was_ble) or (st.get("found") and not state.get("recording")):
            _ensure_session(mower)
        _was_ble = ble
        msg_extra = st.get("msg")
        if msg_extra:
            _append_log("ev", str(msg_extra), True)


client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message


# gps/rover → 로컬 pos 변환 원점 (도크 위치). 환경변수로 지정
GPS_LAT0 = float(os.environ.get("MOWER_GPS_LAT0", "0") or 0)
GPS_LON0 = float(os.environ.get("MOWER_GPS_LON0", "0") or 0)


def _gps_to_pos(lat: float, lon: float) -> tuple[float, float]:
    lat0, lon0 = GPS_LAT0, GPS_LON0
    e = (lon - lon0) * 111320.0 * math.cos(math.radians(lat0))
    n = (lat - lat0) * 111320.0
    th = math.radians(5.0)
    x = e * math.cos(th) - n * math.sin(th)
    y = e * math.sin(th) + n * math.cos(th) - 0.5
    return x, y


def _gps_ok(obj: dict) -> bool:
    try:
        lat = float(obj.get("lat") or 0)
        lon = float(obj.get("lon") or 0)
        sat = int(obj.get("sat") or 0)
        fix = int(obj.get("fix") or 0)
    except (TypeError, ValueError):
        return False
    if fix < 1 or sat < 8:
        return False
    if not (33.0 < lat < 39.5 and 124.0 < lon < 132.0):
        return False
    return True


def _gps_hop_m(a: tuple[float, float], b: tuple[float, float]) -> float:
    lat0 = (a[0] + b[0]) * 0.5
    de = (b[1] - a[1]) * 111320.0 * math.cos(math.radians(lat0))
    dn = (b[0] - a[0]) * 111320.0
    return math.hypot(de, dn)


def _ingest_gps(obj: dict) -> None:
    global _gps_warm
    if not isinstance(obj, dict) or not _gps_ok(obj):
        return
    lat, lon = float(obj["lat"]), float(obj["lon"])
    if not state["gps_lock"]:
        if _gps_warm and _gps_hop_m(_gps_warm[-1], (lat, lon)) > 12.0:
            _gps_warm = [(lat, lon)]
            state["gps_warm"] = 1
            return
        _gps_warm.append((lat, lon))
        state["gps_warm"] = len(_gps_warm)
        if len(_gps_warm) < 6:
            return
        state["gps_lock"] = True
        _gps_warm = []
    gx, gy = _gps_to_pos(lat, lon)
    gpts = state["live_gps"]
    last = gpts[-1] if gpts else None
    if last is None or abs(gx - last["x"]) + abs(gy - last["y"]) > 0.08:
        gpts.append({"x": gx, "y": gy})
        if len(gpts) > 2500:
            del gpts[: len(gpts) - 2500]


def publish_cmd(payload: dict) -> None:
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    client.publish(TOPIC_CMD, body)
    with state_lock:
        state["last_cmd"] = body
        state["updated"] = time.time()
    _append_log("ev", "cmd " + body, True)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: Any) -> None:
        if self.path.split("?", 1)[0] in ("/state", "/logpts"):
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
        if path in ("/", "/index.html"):
            self._send(200, HTML.encode("utf-8"), "text/html; charset=utf-8")
            return
        if path == "/state":
            with state_lock:
                _prune_live(state["live_pts"])
                payload = dict(state)
                payload["live_pts"] = list(state["live_pts"])
                payload["recording"] = False
            self._send(200, json.dumps(payload, ensure_ascii=False).encode("utf-8"), "application/json")
            return
        if path == "/recent":
            q = parse_qs(urlparse(self.path).query)
            try:
                hours = float((q.get("hours") or ["1"])[0])
            except ValueError:
                hours = 1.0
            pts, src = _recent_from_influx(hours)
            with state_lock:
                extra = list(state.get("live_pts") or [])
            last = (pts[-1]["x"], pts[-1]["y"]) if pts else None
            cut = time.time() - hours * 3600
            for p in extra:
                try:
                    t = float(p.get("t") or 0)
                    xy = (float(p["x"]), float(p["y"]))
                except (TypeError, ValueError, KeyError):
                    continue
                if t < cut:
                    continue
                if last is None or abs(xy[0] - last[0]) + abs(xy[1] - last[1]) > 0.03:
                    pts.append({"x": xy[0], "y": xy[1], "s": p.get("s", -1), "a": p.get("a", 0), "t": t})
                    last = xy
            body = json.dumps({"pts": pts, "hours": hours, "n": len(pts), "src": src}, ensure_ascii=False).encode()
            self._send(200, body, "application/json")
            return
        self._send(404, b"not found", "text/plain")

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n) if n else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8") or "{}")
            if not isinstance(payload, dict):
                raise ValueError("json object")
        except (ValueError, json.JSONDecodeError) as exc:
            self._send(400, str(exc).encode(), "text/plain")
            return
        if path != "/cmd":
            self._send(404, b"not found", "text/plain")
            return
        if not payload.get("cmd"):
            self._send(400, b"cmd required", "text/plain")
            return
        publish_cmd(payload)
        self._send(200, b'{"ok":true}', "application/json")


def mqtt_thread() -> None:
    while True:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, 30)
            client.loop_forever()
        except Exception as exc:
            with state_lock:
                state["mqtt"] = "err %s" % exc
            time.sleep(2)


def main() -> int:
    MACRO_DIR.mkdir(parents=True, exist_ok=True)
    threading.Thread(target=mqtt_thread, daemon=True).start()
    httpd = ThreadingHTTPServer((HTTP_HOST, HTTP_PORT), Handler)
    url = "http://127.0.0.1:%d/" % HTTP_PORT
    print("MQTT %s:%s" % (MQTT_HOST, MQTT_PORT))
    print("pad  %s  (아이폰은 이 맥 LAN IP:%d)" % (url, HTTP_PORT))
    print("influx", INFLUX_URL, "token", "ok" if _influx_token() else "MISSING")
    print("Apple python3 + Tk is broken on this macOS — using the browser instead.")
    threading.Timer(0.4, lambda: webbrowser.open(url)).start()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nbye")
        _close_session("pad_exit")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

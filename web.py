#!/usr/bin/env python3
"""
Web UI for Market Surveillance — real-time browser dashboard.

Run:  python web.py
Open: http://localhost:7777
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import traceback
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

import httpx
import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import uvicorn

# Import the engine + feeds from surveillance.py
from surveillance import (
    Engine, Tick, BookLevel, BookSnapshot, PolyToken, PolyMarket,
    TIMEFRAME_SEC, GAMMA_SERIES, GAMMA_API, KILL_ZONE_SEC,
    feed_binance_trades, feed_binance_depth, feed_alpaca, feed_polymarket,
    feed_poly_holders, _handle_clob_msg,
)

# Parse args early so lifespan has access
_ARGS = None
engine: Engine | None = None
WS_CLIENTS: set[WebSocket] = set()
_FEED_TASKS: list[asyncio.Task] = []


def _log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


async def _guarded_feed(name: str, coro):
    """Wrap a feed coroutine with error logging."""
    try:
        _log(f"  Feed '{name}' starting...")
        await coro
    except asyncio.CancelledError:
        _log(f"  Feed '{name}' cancelled")
    except Exception as e:
        _log(f"  Feed '{name}' CRASHED: {e}")
        traceback.print_exc()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start data feeds when the server is ready."""
    global engine, _FEED_TASKS
    args = _ARGS

    coins = [c.strip().upper() for c in args.coins.split(",")]
    engine = Engine(coins=coins, timeframe=args.timeframe, log_dir=args.log_dir)

    _log(f"Engine created: coins={coins} tf={args.timeframe}")

    # Start feeds as background tasks
    _FEED_TASKS = [
        asyncio.create_task(_guarded_feed("binance_trades", feed_binance_trades(engine, coins))),
        asyncio.create_task(_guarded_feed("binance_depth", feed_binance_depth(engine, coins))),
        asyncio.create_task(_guarded_feed("polymarket", feed_polymarket(engine, coins, args.timeframe))),
        asyncio.create_task(_guarded_feed("poly_holders", feed_poly_holders(engine, coins, poll_interval=20))),
        asyncio.create_task(_guarded_feed("broadcast", broadcast_loop())),
    ]

    alpaca_key = os.environ.get("ALPACA_API_KEY", "")
    alpaca_secret = os.environ.get("ALPACA_API_SECRET", "")
    if not args.no_alpaca and alpaca_key and alpaca_secret:
        _FEED_TASKS.append(asyncio.create_task(
            _guarded_feed("alpaca", feed_alpaca(engine, coins, alpaca_key, alpaca_secret))
        ))
        _log("Alpaca feed enabled")
    else:
        _log("Alpaca feed disabled")

    _log(f"All feeds launched ({len(_FEED_TASKS)} tasks)")
    _log(f"Dashboard ready at http://localhost:{args.port}")

    yield  # server is running

    # Shutdown
    _log("Shutting down feeds...")
    for t in _FEED_TASKS:
        t.cancel()
    await asyncio.gather(*_FEED_TASKS, return_exceptions=True)
    engine.close()
    _log("Done.")


app = FastAPI(lifespan=lifespan)

# ── HTML Dashboard ──────────────────────────────────────────────

DASHBOARD_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>SURVEIL</title>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;700;800&display=swap" rel="stylesheet">
<style>
:root {
  --bg: #06080c;
  --surface: #0c1018;
  --surface2: #111826;
  --border: #1a2236;
  --border-hi: #2a3a56;
  --text: #c8d6e5;
  --text-dim: #5a6a80;
  --text-bright: #eaf0f6;
  --accent: #3b82f6;
  --green: #10b981;
  --green-dim: rgba(16,185,129,0.15);
  --red: #ef4444;
  --red-dim: rgba(239,68,68,0.15);
  --amber: #f59e0b;
  --amber-dim: rgba(245,158,11,0.15);
  --cyan: #06b6d4;
  --purple: #8b5cf6;
  --kz-red: #dc2626;
  --mono: 'JetBrains Mono', 'Consolas', monospace;
  --sans: 'Instrument Sans', system-ui, sans-serif;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
html, body { height: 100%; overflow: hidden; }
body {
  background: var(--bg);
  color: var(--text);
  font-family: var(--mono);
  font-size: 12px;
  display: flex;
  flex-direction: column;
}

/* ── SCANLINE OVERLAY ── */
body::after {
  content: '';
  position: fixed;
  inset: 0;
  pointer-events: none;
  background: repeating-linear-gradient(
    0deg,
    transparent,
    transparent 2px,
    rgba(0,0,0,0.03) 2px,
    rgba(0,0,0,0.03) 4px
  );
  z-index: 9999;
}

/* ── TOP BAR ── */
.topbar {
  display: flex;
  align-items: center;
  gap: 16px;
  padding: 8px 16px;
  background: var(--surface);
  border-bottom: 1px solid var(--border);
  flex-shrink: 0;
  min-height: 52px;
}
.logo {
  font-family: var(--sans);
  font-weight: 800;
  font-size: 15px;
  letter-spacing: 3px;
  color: var(--accent);
  text-transform: uppercase;
  white-space: nowrap;
}
.logo span { color: var(--text-dim); font-weight: 400; font-size: 11px; letter-spacing: 1px; margin-left: 6px; }

.cycle-wrap {
  flex: 1;
  display: flex;
  align-items: center;
  gap: 12px;
  min-width: 0;
}
.cycle-bar-outer {
  flex: 1;
  height: 28px;
  background: var(--surface2);
  border-radius: 4px;
  position: relative;
  overflow: hidden;
  border: 1px solid var(--border);
  max-width: 600px;
}
.cycle-fill {
  position: absolute;
  left: 0; top: 0; bottom: 0;
  background: linear-gradient(90deg, rgba(59,130,246,0.25), rgba(59,130,246,0.4));
  transition: width 0.4s ease;
  border-right: 2px solid var(--accent);
}
.cycle-kz {
  position: absolute;
  right: 0; top: 0; bottom: 0;
  background: repeating-linear-gradient(
    -45deg,
    rgba(220,38,38,0.08),
    rgba(220,38,38,0.08) 4px,
    transparent 4px,
    transparent 8px
  );
  border-left: 1px dashed rgba(220,38,38,0.4);
}
.cycle-kz.active {
  background: repeating-linear-gradient(
    -45deg,
    rgba(220,38,38,0.2),
    rgba(220,38,38,0.2) 4px,
    transparent 4px,
    transparent 8px
  );
  border-left-color: var(--kz-red);
}
.cycle-label {
  position: absolute;
  left: 8px; top: 50%; transform: translateY(-50%);
  font-size: 10px;
  color: var(--text-dim);
  font-weight: 500;
}
.timer-box {
  font-size: 26px;
  font-weight: 700;
  color: var(--green);
  min-width: 80px;
  text-align: right;
  white-space: nowrap;
  font-variant-numeric: tabular-nums;
}
.timer-box.kz { color: var(--kz-red); animation: kz-pulse 0.6s ease infinite alternate; }
@keyframes kz-pulse { to { opacity: 0.55; } }

.phase-pill {
  padding: 4px 12px;
  border-radius: 3px;
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 1px;
  text-transform: uppercase;
  white-space: nowrap;
}
.phase-IDLE { background: var(--surface2); color: var(--text-dim); }
.phase-DRAIN_DETECTED { background: var(--amber-dim); color: var(--amber); border: 1px solid rgba(245,158,11,0.3); }
.phase-MM_FLEEING { background: rgba(249,115,22,0.15); color: #f97316; border: 1px solid rgba(249,115,22,0.3); animation: kz-pulse 0.8s ease infinite alternate; }
.phase-STRIKE_IMMINENT { background: var(--red-dim); color: var(--red); border: 1px solid rgba(239,68,68,0.4); animation: kz-pulse 0.3s ease infinite alternate; font-size: 12px; }

.stat-chip {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 2px 8px;
  font-size: 11px;
  background: var(--surface2);
  border-radius: 3px;
  white-space: nowrap;
}
.stat-chip .lbl { color: var(--text-dim); }
.stat-chip .val { font-weight: 600; }

/* ── MAIN GRID ── */
.main {
  flex: 1;
  display: grid;
  grid-template-columns: 1fr 1fr;
  grid-template-rows: 1fr 1fr auto auto;
  gap: 1px;
  background: var(--border);
  overflow: hidden;
}
.cell {
  background: var(--surface);
  padding: 10px 12px;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  position: relative;
}
.cell-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 6px;
  flex-shrink: 0;
}
.cell-title {
  font-family: var(--sans);
  font-weight: 600;
  font-size: 11px;
  letter-spacing: 1.5px;
  text-transform: uppercase;
  color: var(--text-dim);
}
.cell-badge {
  font-size: 10px;
  padding: 2px 6px;
  border-radius: 2px;
  font-weight: 600;
}
.span-2 { grid-column: span 2; }

/* ── PRICE DISPLAY ── */
.price-big {
  font-size: 28px;
  font-weight: 700;
  letter-spacing: -1px;
  line-height: 1;
  font-variant-numeric: tabular-nums;
}
.change-tag {
  display: inline-block;
  font-size: 13px;
  font-weight: 600;
  padding: 2px 8px;
  border-radius: 3px;
  margin-left: 10px;
  vertical-align: middle;
}
.change-up { background: var(--green-dim); color: var(--green); }
.change-down { background: var(--red-dim); color: var(--red); }
.change-flat { background: var(--surface2); color: var(--text-dim); }

.stats-row {
  display: flex;
  gap: 10px;
  flex-wrap: wrap;
  margin-top: 6px;
}

/* ── POLY PANEL ── */
.poly-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 8px;
  margin-top: 4px;
}
.poly-card {
  background: var(--surface2);
  border-radius: 4px;
  padding: 8px 10px;
  border: 1px solid var(--border);
}
.poly-card .side-label {
  font-size: 10px;
  font-weight: 600;
  letter-spacing: 1px;
  text-transform: uppercase;
}
.poly-card .side-price {
  font-size: 22px;
  font-weight: 700;
  font-variant-numeric: tabular-nums;
}
.mm-pill {
  display: inline-block;
  padding: 3px 10px;
  border-radius: 3px;
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 1px;
}
.mm-PRESENT { background: var(--green-dim); color: var(--green); }
.mm-THINNING { background: var(--amber-dim); color: var(--amber); }
.mm-FLED { background: var(--red-dim); color: var(--red); animation: kz-pulse 0.5s ease infinite alternate; }
.mm-UNKNOWN { background: var(--surface2); color: var(--text-dim); }

/* ── CHARTS ── */
.chart-wrap {
  flex: 1;
  position: relative;
  min-height: 0;
}
.chart-wrap canvas {
  position: absolute;
  inset: 0;
  width: 100%;
  height: 100%;
}

/* ── DEPTH VIS ── */
.depth-vis {
  display: flex;
  align-items: center;
  gap: 2px;
  height: 20px;
  margin: 4px 0;
}
.depth-vis .bid-bar {
  height: 100%;
  background: linear-gradient(90deg, transparent, var(--green));
  border-radius: 2px 0 0 2px;
  transition: width 0.3s;
}
.depth-vis .ask-bar {
  height: 100%;
  background: linear-gradient(90deg, var(--red), transparent);
  border-radius: 0 2px 2px 0;
  transition: width 0.3s;
}
.depth-vis .mid-line {
  width: 2px;
  height: 100%;
  background: var(--text-dim);
  border-radius: 1px;
  flex-shrink: 0;
}

/* ── ALERTS ── */
.alert-scroll {
  flex: 1;
  overflow-y: auto;
  min-height: 0;
  scrollbar-width: thin;
  scrollbar-color: var(--border) transparent;
}
.alert-row {
  display: flex;
  gap: 6px;
  padding: 3px 0;
  border-bottom: 1px solid var(--border);
  font-size: 11px;
  align-items: baseline;
}
.alert-row:last-child { border-bottom: none; }
.alert-ts { color: var(--text-dim); white-space: nowrap; flex-shrink: 0; }
.alert-sev {
  font-weight: 700;
  width: 52px;
  flex-shrink: 0;
  text-align: center;
  padding: 1px 4px;
  border-radius: 2px;
  font-size: 9px;
  letter-spacing: 0.5px;
}
.sev-CRITICAL { background: var(--red-dim); color: var(--red); }
.sev-HIGH { background: rgba(249,115,22,0.15); color: #f97316; }
.sev-WARN { background: var(--amber-dim); color: var(--amber); }
.sev-INFO { background: var(--surface2); color: var(--text-dim); }
.alert-msg { color: var(--text); min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }

/* ── HISTORY TABLE ── */
.hist-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 11px;
}
.hist-table th {
  text-align: left;
  padding: 4px 6px;
  color: var(--text-dim);
  font-weight: 500;
  border-bottom: 1px solid var(--border);
  font-size: 10px;
  letter-spacing: 0.5px;
  text-transform: uppercase;
  position: sticky;
  top: 0;
  background: var(--surface);
}
.hist-table td {
  padding: 3px 6px;
  border-bottom: 1px solid var(--border);
  font-variant-numeric: tabular-nums;
}
.hist-table .rev-yes { color: var(--red); font-weight: 700; }
.hist-table .rev-no { color: var(--text-dim); }
.c-up { color: var(--green); }
.c-down { color: var(--red); }
.c-flat { color: var(--text-dim); }

/* ── PREDICTION BOX ── */
.predict-box {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 6px 10px;
  background: var(--surface2);
  border-radius: 4px;
  border: 1px solid var(--border);
  margin-top: 6px;
}
.predict-dir {
  font-size: 18px;
  font-weight: 800;
  font-family: var(--sans);
}
.predict-conf {
  font-size: 11px;
  color: var(--text-dim);
}
.predict-bar-outer {
  flex: 1;
  height: 6px;
  background: var(--bg);
  border-radius: 3px;
  overflow: hidden;
}
.predict-bar-fill {
  height: 100%;
  border-radius: 3px;
  transition: width 0.3s;
}

/* ── PATTERN STATS ── */
.pattern-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 8px;
}
.pattern-card {
  background: var(--surface2);
  border-radius: 4px;
  padding: 8px 10px;
  border: 1px solid var(--border);
  text-align: center;
}
.pattern-card .p-val {
  font-size: 20px;
  font-weight: 700;
  line-height: 1.2;
  font-variant-numeric: tabular-nums;
}
.pattern-card .p-lbl {
  font-size: 10px;
  color: var(--text-dim);
  letter-spacing: 0.5px;
  text-transform: uppercase;
  margin-top: 2px;
}

/* ── CONNECTION STATUS ── */
.conn-dot {
  width: 7px; height: 7px;
  border-radius: 50%;
  background: var(--green);
  display: inline-block;
  margin-right: 4px;
}
.conn-dot.off { background: var(--red); }
</style>
</head>
<body>

<!-- ═══ TOP BAR ═══ -->
<div class="topbar">
  <div class="logo">SURVEIL<span>mkt surveillance</span></div>
  <div class="cycle-wrap">
    <div class="stat-chip"><span class="lbl" id="tf-label">5m</span></div>
    <div class="stat-chip"><span class="lbl">UTC</span> <span class="val" id="window-utc">--:--:--</span></div>
    <div class="cycle-bar-outer">
      <div class="cycle-fill" id="cycle-fill" style="width:0%"></div>
      <div class="cycle-kz" id="cycle-kz" style="width:10%"></div>
      <div class="cycle-label" id="cycle-label">0%</div>
    </div>
    <div class="timer-box" id="timer">--s</div>
  </div>
  <div id="phase-pill" class="phase-pill phase-IDLE">IDLE</div>
  <div class="stat-chip"><span class="conn-dot" id="conn-dot"></span><span class="val" id="tick-rate">--</span><span class="lbl">/s</span></div>
</div>

<!-- ═══ MAIN GRID ═══ -->
<div class="main">

  <!-- ROW 1: Price + Poly -->
  <div class="cell" id="cell-price">
    <div class="cell-header">
      <div class="cell-title">BTC / Binance</div>
      <div class="stats-row">
        <span class="stat-chip"><span class="lbl">Open</span> <span class="val" id="btc-open">--</span></span>
        <span class="stat-chip"><span class="lbl">Ticks</span> <span class="val" id="tick-count">--</span></span>
      </div>
    </div>
    <div style="display:flex;align-items:baseline;gap:6px;">
      <span class="price-big" id="btc-price">$--</span>
      <span class="change-tag change-flat" id="btc-change">--</span>
    </div>
    <div class="chart-wrap"><canvas id="chart-price"></canvas></div>
  </div>

  <div class="cell" id="cell-poly">
    <div class="cell-header">
      <div class="cell-title">Polymarket</div>
      <span class="mm-pill mm-UNKNOWN" id="mm-pill">UNKNOWN</span>
    </div>
    <div class="poly-grid">
      <div class="poly-card">
        <div class="side-label" style="color:var(--green)">YES / UP</div>
        <div class="side-price" id="poly-yes" style="color:var(--green)">$--</div>
      </div>
      <div class="poly-card">
        <div class="side-label" style="color:var(--red)">NO / DOWN</div>
        <div class="side-price" id="poly-no" style="color:var(--red)">$--</div>
      </div>
    </div>
    <div class="chart-wrap"><canvas id="chart-poly"></canvas></div>
    <div class="stats-row" style="margin-top:4px">
      <span class="stat-chip"><span class="lbl">Depth</span> <span class="val" id="poly-depth">--%</span></span>
      <span class="stat-chip"><span class="lbl">Spread</span> <span class="val" id="poly-spread">--</span></span>
      <span class="stat-chip"><span class="lbl">Strike</span> <span class="val" id="strike-dir" style="color:var(--text-dim)">?</span></span>
    </div>
  </div>

  <!-- ROW 2: CVD+Volume | Alerts + Depth -->
  <div class="cell">
    <div class="cell-header">
      <div class="cell-title">CVD & Volume</div>
      <div class="stats-row">
        <span class="stat-chip"><span class="lbl">CVD</span> <span class="val" id="cvd-val">--</span></span>
        <span class="stat-chip"><span class="lbl">OBI</span> <span class="val" id="obi-val">--</span></span>
        <span class="stat-chip"><span class="lbl">Buy 10s</span> <span class="val" id="vol-buy">--</span></span>
        <span class="stat-chip"><span class="lbl">Sell</span> <span class="val" id="vol-sell">--</span></span>
      </div>
    </div>
    <div class="chart-wrap" style="flex:1"><canvas id="chart-cvd"></canvas></div>
    <div class="chart-wrap" style="flex:1;margin-top:2px"><canvas id="chart-vol"></canvas></div>
  </div>

  <div class="cell">
    <div class="cell-header">
      <div class="cell-title">Alerts</div>
      <div class="stat-chip"><span class="lbl">Drains</span> <span class="val" id="drain-count">0</span></div>
    </div>
    <div class="alert-scroll" id="alerts"></div>
    <div class="predict-box">
      <span class="predict-dir" id="predict-dir" style="color:var(--text-dim)">?</span>
      <div class="predict-bar-outer"><div class="predict-bar-fill" id="predict-bar" style="width:0;background:var(--text-dim)"></div></div>
      <span class="predict-conf" id="predict-conf">0%</span>
    </div>
  </div>

  <!-- ROW 3: History (full width) -->
  <div class="cell span-2">
    <div class="cell-header">
      <div class="cell-title">Kill Zone History — BTC vs Polymarket Outcome</div>
      <div class="pattern-grid" id="patterns" style="gap:6px">
        <div class="pattern-card" style="padding:4px 8px"><div class="p-val" style="font-size:14px;color:var(--text-dim)">--</div><div class="p-lbl">Windows</div></div>
        <div class="pattern-card" style="padding:4px 8px"><div class="p-val" style="font-size:14px;color:var(--red)">--</div><div class="p-lbl">Reversals</div></div>
        <div class="pattern-card" style="padding:4px 8px"><div class="p-val" style="font-size:14px;color:var(--amber)">--</div><div class="p-lbl">Avg KZ Move</div></div>
        <div class="pattern-card" style="padding:4px 8px"><div class="p-val" style="font-size:14px;color:var(--cyan)">--</div><div class="p-lbl">CVD Acc</div></div>
      </div>
    </div>
    <div class="alert-scroll" style="max-height:180px">
      <table class="hist-table">
        <thead><tr><th>UTC</th><th>BTC</th><th>Chg%</th><th>KZ</th><th>KZ Chg%</th><th>KZ $Vol</th><th>CVD</th><th>POLY</th><th>YES</th><th>NO</th><th>Rev?</th></tr></thead>
        <tbody id="hist-body"></tbody>
      </table>
    </div>
  </div>

  <!-- ROW 4: Holders + Depth -->
  <div class="cell">
    <div class="cell-header">
      <div class="cell-title">Top Holders — YES / UP</div>
      <div class="stat-chip"><span class="lbl">Flow</span> <span class="val c-up" id="flow-yes-buy">0</span> <span class="lbl">/</span> <span class="val c-down" id="flow-yes-sell">0</span></div>
    </div>
    <div class="alert-scroll" id="holders-yes" style="font-size:11px"></div>
  </div>

  <div class="cell">
    <div class="cell-header">
      <div class="cell-title">Top Holders — NO / DOWN</div>
      <div class="stat-chip"><span class="lbl">Flow</span> <span class="val c-up" id="flow-no-buy">0</span> <span class="lbl">/</span> <span class="val c-down" id="flow-no-sell">0</span></div>
    </div>
    <div class="alert-scroll" id="holders-no" style="font-size:11px"></div>
  </div>

</div>

<script>
// ═══════════════════════════════════════
// CHART RENDERER (lightweight canvas)
// ═══════════════════════════════════════
const C = {
  bg: '#06080c',
  surface: '#0c1018',
  grid: 'rgba(26,34,54,0.6)',
  green: '#10b981',
  greenDim: 'rgba(16,185,129,0.12)',
  red: '#ef4444',
  redDim: 'rgba(239,68,68,0.12)',
  accent: '#3b82f6',
  accentDim: 'rgba(59,130,246,0.1)',
  amber: '#f59e0b',
  cyan: '#06b6d4',
  text: 'rgba(200,214,229,0.5)',
  kzBg: 'rgba(220,38,38,0.06)',
};
const dpr = window.devicePixelRatio || 1;

function setupCanvas(canvas) {
  const rect = canvas.parentElement.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  canvas.style.width = rect.width + 'px';
  canvas.style.height = rect.height + 'px';
  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);
  return { ctx, w: rect.width, h: rect.height };
}

function drawLine(ctx, pts, w, h, color, fill, yMin, yMax) {
  if (pts.length < 2) return;
  const xMax = pts[pts.length - 1][0] || 1;
  const range = yMax - yMin || 1;
  ctx.beginPath();
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.5;
  ctx.lineJoin = 'round';
  for (let i = 0; i < pts.length; i++) {
    const x = (pts[i][0] / xMax) * w;
    const y = h - ((pts[i][1] - yMin) / range) * (h - 4) - 2;
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  }
  ctx.stroke();
  if (fill) {
    ctx.lineTo(w, h);
    ctx.lineTo(0, h);
    ctx.closePath();
    ctx.fillStyle = fill;
    ctx.fill();
  }
}

function drawKillZone(ctx, w, h, tfSec, pctElapsed) {
  const kzPct = 30 / tfSec;
  const kzStartX = w * (1 - kzPct);
  // KZ background region
  ctx.fillStyle = C.kzBg;
  ctx.fillRect(kzStartX, 0, w - kzStartX, h);
  // dashed line at KZ start
  ctx.beginPath();
  ctx.setLineDash([3, 3]);
  ctx.strokeStyle = 'rgba(220,38,38,0.3)';
  ctx.lineWidth = 1;
  ctx.moveTo(kzStartX, 0);
  ctx.lineTo(kzStartX, h);
  ctx.stroke();
  ctx.setLineDash([]);
  // Current time cursor
  const curX = (pctElapsed / 100) * w;
  ctx.beginPath();
  ctx.strokeStyle = 'rgba(255,255,255,0.15)';
  ctx.lineWidth = 1;
  ctx.moveTo(curX, 0);
  ctx.lineTo(curX, h);
  ctx.stroke();
}

function drawGrid(ctx, w, h, yMin, yMax, numLines) {
  const range = yMax - yMin || 1;
  ctx.textAlign = 'right';
  ctx.font = '9px JetBrains Mono';
  ctx.fillStyle = C.text;
  for (let i = 0; i <= numLines; i++) {
    const y = (i / numLines) * h;
    const val = yMax - (i / numLines) * range;
    ctx.beginPath();
    ctx.strokeStyle = C.grid;
    ctx.lineWidth = 0.5;
    ctx.moveTo(0, y);
    ctx.lineTo(w, y);
    ctx.stroke();
    ctx.fillText(val.toFixed(val > 100 ? 0 : 3), w - 4, y - 3);
  }
}

function drawBars(ctx, bars, w, h) {
  if (!bars.length) return;
  const maxV = Math.max(...bars.map(b => b.buy + b.sell), 0.0001);
  const bw = Math.max(2, (w / bars.length) - 1);
  for (let i = 0; i < bars.length; i++) {
    const x = (i / bars.length) * w;
    const buyH = (bars[i].buy / maxV) * h;
    const sellH = (bars[i].sell / maxV) * h;
    ctx.fillStyle = C.green;
    ctx.fillRect(x, h - buyH - sellH, bw, buyH);
    ctx.fillStyle = C.red;
    ctx.fillRect(x, h - sellH, bw, sellH);
  }
}

function drawCVD(ctx, pts, w, h, tfSec, pctElapsed) {
  if (pts.length < 2) return;
  const xMax = pts[pts.length-1][0] || 1;
  const vals = pts.map(p => p[1]);
  const yMin = Math.min(0, ...vals);
  const yMax = Math.max(0, ...vals);
  const range = Math.max(Math.abs(yMin), Math.abs(yMax), 0.01) * 1.1;
  // Zero line
  const zeroY = h * 0.5;
  ctx.beginPath();
  ctx.strokeStyle = 'rgba(200,214,229,0.1)';
  ctx.lineWidth = 0.5;
  ctx.moveTo(0, zeroY);
  ctx.lineTo(w, zeroY);
  ctx.stroke();
  drawKillZone(ctx, w, h, tfSec, pctElapsed);
  // CVD line
  ctx.beginPath();
  ctx.lineWidth = 1.5;
  ctx.lineJoin = 'round';
  for (let i = 0; i < pts.length; i++) {
    const x = (pts[i][0] / xMax) * w;
    const y = zeroY - (pts[i][1] / range) * (h * 0.45);
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  }
  const lastVal = pts[pts.length-1][1];
  ctx.strokeStyle = lastVal >= 0 ? C.green : C.red;
  ctx.stroke();
  // fill
  const lastX = w;
  ctx.lineTo(lastX, zeroY);
  ctx.lineTo(0, zeroY);
  ctx.closePath();
  ctx.fillStyle = lastVal >= 0 ? C.greenDim : C.redDim;
  ctx.fill();
}

// ═══════════════════════════════════════
// WEBSOCKET + STATE
// ═══════════════════════════════════════
let connected = false;
const ws = new WebSocket("ws://" + location.host + "/ws");

ws.onopen = () => {
  connected = true;
  document.getElementById('conn-dot').classList.remove('off');
};
ws.onclose = () => {
  connected = false;
  document.getElementById('conn-dot').classList.add('off');
  document.getElementById('timer').textContent = 'DC';
  document.getElementById('timer').style.color = '#ef4444';
};

ws.onmessage = (e) => {
  const d = JSON.parse(e.data);
  const coin = d.coins[0];
  if (!coin) return;
  const c = d.coin_data[coin] || {};

  // ── Top bar ──
  document.getElementById('tf-label').textContent = d.timeframe;
  document.getElementById('window-utc').textContent = d.window_utc;
  const timer = document.getElementById('timer');
  timer.textContent = d.remaining.toFixed(1) + 's';
  timer.className = 'timer-box' + (d.kill_zone ? ' kz' : '');
  if (!d.kill_zone) timer.style.color = '';

  const fill = document.getElementById('cycle-fill');
  fill.style.width = d.pct_elapsed + '%';
  if (d.kill_zone) fill.style.background = 'linear-gradient(90deg, rgba(220,38,38,0.25), rgba(220,38,38,0.5))';
  else fill.style.background = '';
  if (d.kill_zone) fill.style.borderRightColor = 'var(--kz-red)';
  else fill.style.borderRightColor = '';

  const kzEl = document.getElementById('cycle-kz');
  kzEl.style.width = (30 / d.tf_sec * 100) + '%';
  kzEl.className = 'cycle-kz' + (d.kill_zone ? ' active' : '');
  document.getElementById('cycle-label').textContent = d.pct_elapsed.toFixed(0) + '%';

  const phase = document.getElementById('phase-pill');
  phase.textContent = d.phase;
  phase.className = 'phase-pill phase-' + d.phase;

  // ── Price panel ──
  const dir = c.change >= 0 ? 'up' : 'down';
  const priceEl = document.getElementById('btc-price');
  priceEl.textContent = '$' + (c.price||0).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2});
  priceEl.style.color = dir==='up' ? 'var(--green)' : 'var(--red)';

  const changeEl = document.getElementById('btc-change');
  changeEl.textContent = (c.change>=0?'+':'') + (c.change||0).toFixed(4) + '%';
  changeEl.className = 'change-tag change-' + dir;

  document.getElementById('btc-open').textContent = '$'+(c.open_price||0).toLocaleString(undefined,{minimumFractionDigits:2});
  document.getElementById('tick-count').textContent = (c.ticks||0).toLocaleString();
  document.getElementById('tick-rate').textContent = (c.tick_rate||0).toFixed(0);

  // ── Price chart ──
  const priceCanvas = document.getElementById('chart-price');
  if (c.price_hist && c.price_hist.length > 2) {
    const {ctx,w,h} = setupCanvas(priceCanvas);
    ctx.clearRect(0,0,w,h);
    const vals = c.price_hist.map(p=>p[1]);
    const yMin = Math.min(...vals) - 5;
    const yMax = Math.max(...vals) + 5;
    drawGrid(ctx,w,h,yMin,yMax,4);
    drawKillZone(ctx,w,h,d.tf_sec,d.pct_elapsed);
    drawLine(ctx,c.price_hist,w,h,dir==='up'?C.green:C.red,dir==='up'?C.greenDim:C.redDim,yMin,yMax);
  }

  // ── Poly panel ──
  document.getElementById('poly-yes').textContent = '$'+(c.poly_yes||0.5).toFixed(2);
  document.getElementById('poly-no').textContent = '$'+(c.poly_no||0.5).toFixed(2);
  const mmPill = document.getElementById('mm-pill');
  mmPill.textContent = c.mm_status||'UNKNOWN';
  mmPill.className = 'mm-pill mm-'+(c.mm_status||'UNKNOWN');
  const depthPct = (c.poly_depth_pct||100);
  const bidD = (c.poly_bid_depth||0);
  const askD = (c.poly_ask_depth||0);
  document.getElementById('poly-depth').textContent = depthPct.toFixed(0)+'% ($'+bidD.toFixed(0)+' / $'+askD.toFixed(0)+')';
  document.getElementById('poly-spread').textContent = (c.poly_spread||0).toFixed(3);
  const strikeEl = document.getElementById('strike-dir');
  strikeEl.textContent = c.strike_dir||'?';
  strikeEl.style.color = c.strike_dir==='UP'?'var(--green)':c.strike_dir==='DOWN'?'var(--red)':'var(--text-dim)';

  // Poly chart
  const polyCanvas = document.getElementById('chart-poly');
  const yh = c.poly_yes_hist || [];
  const nh = c.poly_no_hist || [];
  if (yh.length > 1 || nh.length > 1) {
    const {ctx,w,h} = setupCanvas(polyCanvas);
    ctx.clearRect(0,0,w,h);
    const allV = [...yh.map(p=>p[1]),...nh.map(p=>p[1])];
    const yMin = Math.min(...allV, 0.3);
    const yMax = Math.max(...allV, 0.7);
    drawGrid(ctx,w,h,yMin,yMax,3);
    drawKillZone(ctx,w,h,d.tf_sec,d.pct_elapsed);
    if (yh.length > 1) drawLine(ctx,yh,w,h,C.green,C.greenDim,yMin,yMax);
    if (nh.length > 1) drawLine(ctx,nh,w,h,C.red,C.redDim,yMin,yMax);
  }

  // ── CVD & Volume ──
  const cvdEl = document.getElementById('cvd-val');
  cvdEl.textContent = (c.cvd||0).toFixed(3);
  cvdEl.style.color = (c.cvd||0) >= 0 ? 'var(--green)' : 'var(--red)';
  document.getElementById('obi-val').textContent = (c.obi||0.5).toFixed(3);
  document.getElementById('vol-buy').textContent = (c.buy_vol_10s||0).toFixed(3);
  document.getElementById('vol-sell').textContent = (c.sell_vol_10s||0).toFixed(3);

  // CVD chart
  const cvdCanvas = document.getElementById('chart-cvd');
  if (c.cvd_hist && c.cvd_hist.length > 2) {
    const {ctx,w,h} = setupCanvas(cvdCanvas);
    ctx.clearRect(0,0,w,h);
    drawCVD(ctx,c.cvd_hist,w,h,d.tf_sec,d.pct_elapsed);
  }

  // Volume bars chart
  const volCanvas = document.getElementById('chart-vol');
  if (c.vol_bars && c.vol_bars.length) {
    const {ctx,w,h} = setupCanvas(volCanvas);
    ctx.clearRect(0,0,w,h);
    drawBars(ctx,c.vol_bars,w,h);
  }

  // ── Depth chart ──
  const depthCanvas = document.getElementById('chart-depth');
  const dh = c.poly_depth_hist || [];
  if (dh.length > 2) {
    const {ctx,w,h} = setupCanvas(depthCanvas);
    ctx.clearRect(0,0,w,h);
    const xMax = dh[dh.length-1][0] || 1;
    const bidPts = dh.map(d => [d[0], d[1]]);
    const askPts = dh.map(d => [d[0], d[2]]);
    const allD = [...bidPts.map(p=>p[1]),...askPts.map(p=>p[1])];
    const yMin = 0;
    const yMax = Math.max(...allD, 1) * 1.1;
    drawKillZone(ctx,w,h,d.tf_sec,d.pct_elapsed);
    drawLine(ctx,bidPts,w,h,C.green,C.greenDim,yMin,yMax);
    drawLine(ctx,askPts,w,h,C.red,C.redDim,yMin,yMax);
  }

  // ── Alerts ──
  const alertDiv = document.getElementById('alerts');
  alertDiv.innerHTML = (d.alerts||[]).map(a =>
    '<div class="alert-row">' +
    '<span class="alert-ts">' + a.time + '</span>' +
    '<span class="alert-sev sev-' + a.severity + '">' + a.severity + '</span>' +
    '<span class="alert-msg">' + a.message + '</span>' +
    '</div>'
  ).join('');
  document.getElementById('drain-count').textContent = (c.drain_events||[]).length;

  // ── Prediction ──
  const predDir = document.getElementById('predict-dir');
  predDir.textContent = c.predict_dir || '?';
  predDir.style.color = c.predict_dir==='UP'?'var(--green)':c.predict_dir==='DOWN'?'var(--red)':'var(--text-dim)';
  const conf = (c.predict_conf || 0);
  document.getElementById('predict-conf').textContent = (conf*100).toFixed(0)+'%';
  const predBar = document.getElementById('predict-bar');
  predBar.style.width = (conf*100)+'%';
  predBar.style.background = c.predict_dir==='UP'?'var(--green)':c.predict_dir==='DOWN'?'var(--red)':'var(--text-dim)';

  // ── History ──
  const tbody = document.getElementById('hist-body');
  tbody.innerHTML = (d.history||[]).map(w => {
    const dc = w.dir==='UP'?'c-up':w.dir==='DOWN'?'c-down':'c-flat';
    const kc = w.kz_dir==='UP'?'c-up':w.kz_dir==='DOWN'?'c-down':'c-flat';
    const pc = (w.poly||'').includes('UP')?'c-up':(w.poly||'').includes('DOWN')?'c-down':'c-flat';
    const match = w.poly && w.dir && ((w.poly.includes('UP')&&w.dir==='UP')||(w.poly.includes('DOWN')&&w.dir==='DOWN'));
    return '<tr>'+
      '<td>'+w.time+'</td>'+
      '<td class="'+dc+'">'+w.dir+'</td>'+
      '<td class="'+dc+'">'+(w.chg>=0?'+':'')+w.chg.toFixed(4)+'%</td>'+
      '<td class="'+kc+'">'+w.kz_dir+'</td>'+
      '<td class="'+kc+'">'+(w.kz_chg>=0?'+':'')+w.kz_chg.toFixed(4)+'%</td>'+
      '<td>$'+w.kz_dvol.toLocaleString(undefined,{maximumFractionDigits:0})+'</td>'+
      '<td>'+w.cvd.toFixed(2)+'</td>'+
      '<td class="'+pc+'" style="font-weight:700">'+(w.poly||'?')+'</td>'+
      '<td style="color:var(--green)">'+(w.py||'--')+'</td>'+
      '<td style="color:var(--red)">'+(w.pn||'--')+'</td>'+
      '<td class="'+(w.rev?'rev-yes':'rev-no')+'">'+(w.rev?'REV':'—')+'</td>'+
    '</tr>';
  }).join('');

  // ── Patterns ──
  const p = d.patterns || {};
  const pg = document.getElementById('patterns');
  pg.innerHTML =
    '<div class="pattern-card"><div class="p-val" style="color:var(--text-bright)">'+(p.count||0)+'</div><div class="p-lbl">Windows</div></div>'+
    '<div class="pattern-card"><div class="p-val" style="color:var(--red)">'+(p.reversals||0)+' <small style="font-size:11px;color:var(--text-dim)">('+(p.rev_pct||0).toFixed(0)+'%)</small></div><div class="p-lbl">Reversals</div></div>'+
    '<div class="pattern-card"><div class="p-val" style="color:var(--amber)">'+(p.avg_kz_chg||0).toFixed(4)+'%</div><div class="p-lbl">Avg KZ Move</div></div>'+
    '<div class="pattern-card"><div class="p-val" style="font-size:14px;color:var(--cyan)">'+(p.cvd_accuracy||0).toFixed(0)+'%</div><div class="p-lbl">CVD Acc</div></div>';

  // ── Holders ──
  function renderHolders(containerId, holders) {
    const el = document.getElementById(containerId);
    if (!holders || !holders.length) {
      el.innerHTML = '<div style="color:var(--text-dim);padding:8px">Loading holders...</div>';
      return;
    }
    const maxAmt = Math.max(...holders.map(h => h.amount), 1);
    el.innerHTML = holders.map((h, i) => {
      const pct = (h.amount / maxAmt * 100);
      const isYes = containerId.includes('yes');
      const barColor = isYes ? 'var(--green)' : 'var(--red)';
      const barDimColor = isYes ? 'var(--green-dim)' : 'var(--red-dim)';
      return '<div style="display:flex;align-items:center;gap:6px;padding:2px 0;border-bottom:1px solid var(--border)">'+
        '<span style="width:14px;color:var(--text-dim);text-align:right">'+(i+1)+'</span>'+
        '<span style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="'+h.wallet+'">'+h.name+'</span>'+
        '<div style="width:80px;height:10px;background:var(--surface2);border-radius:2px;overflow:hidden"><div style="height:100%;width:'+pct+'%;background:'+barColor+';border-radius:2px"></div></div>'+
        '<span style="min-width:55px;text-align:right;font-weight:600;font-variant-numeric:tabular-nums">$'+h.amount.toLocaleString(undefined,{maximumFractionDigits:0})+'</span>'+
      '</div>';
    }).join('');
  }
  renderHolders('holders-yes', c.holders_yes || []);
  renderHolders('holders-no', c.holders_no || []);

  // ── Poly Flow ──
  const pf = c.poly_flow || {};
  document.getElementById('flow-yes-buy').textContent = (pf.yes_buy||0).toFixed(1);
  document.getElementById('flow-yes-sell').textContent = (pf.yes_sell||0).toFixed(1);
  document.getElementById('flow-no-buy').textContent = (pf.no_buy||0).toFixed(1);
  document.getElementById('flow-no-sell').textContent = (pf.no_sell||0).toFixed(1);

  // ── Phase glow on body ──
  if (d.phase === 'STRIKE_IMMINENT') {
    document.body.style.boxShadow = 'inset 0 0 80px rgba(239,68,68,0.15)';
  } else if (d.phase === 'MM_FLEEING') {
    document.body.style.boxShadow = 'inset 0 0 40px rgba(249,115,22,0.08)';
  } else if (d.phase === 'DRAIN_DETECTED') {
    document.body.style.boxShadow = 'inset 0 0 30px rgba(245,158,11,0.06)';
  } else {
    document.body.style.boxShadow = 'none';
  }
};

// Handle resize
let resizeTimer;
window.addEventListener('resize', () => {
  clearTimeout(resizeTimer);
  resizeTimer = setTimeout(() => {
    // Charts will re-draw on next WS message
  }, 200);
});
</script>
</body>
</html>"""


@app.get("/")
async def index():
    html = (Path(__file__).parent / "dashboard.html").read_text(encoding="utf-8")
    return HTMLResponse(html)


@app.get("/health")
async def health():
    if not engine:
        return {"status": "starting", "engine": False}
    coin0 = engine.coins[0] if engine.coins else "?"
    return {
        "status": "ok",
        "coins": engine.coins,
        "ticks": {c: engine.total_ticks.get(c, 0) for c in engine.coins},
        "price": {c: engine.latest_price.get(c) for c in engine.coins},
        "poly_markets": {c: bool(engine.poly_markets.get(c)) for c in engine.coins},
        "ws_clients": len(WS_CLIENTS),
        "uptime": round(time.time() - engine.start_time, 1),
        "feeds_alive": len([t for t in _FEED_TASKS if not t.done()]),
        "feeds_dead": len([t for t in _FEED_TASKS if t.done()]),
    }


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    WS_CLIENTS.add(websocket)
    try:
        while True:
            # We don't expect messages from client, just keep alive
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=0.5)
            except asyncio.TimeoutError:
                pass
            except WebSocketDisconnect:
                break
    finally:
        WS_CLIENTS.discard(websocket)


async def broadcast_loop():
    """Push state to all connected WebSocket clients every 500ms."""
    global WS_CLIENTS
    while True:
        await asyncio.sleep(0.5)
        if not engine or not WS_CLIENTS:
            continue

        now_ms = int(time.time() * 1000)
        now_s = now_ms // 1000
        coin0 = engine.coins[0] if engine.coins else "BTC"
        ws_start = engine.window_start()
        ws_start_ms = ws_start * 1000
        ws_dt = datetime.fromtimestamp(ws_start, tz=timezone.utc)

        # Chart window: 3 complete past windows + current partial, aligned to cycle boundaries
        base_sec = (now_s // engine.tf_sec - 2) * engine.tf_sec
        base_ms = base_sec * 1000
        chart_span_ms = now_ms - base_ms

        # Anchor UI to window start to keep all components on the same cycle clock
        window_start_ms = ws_start_ms
        window_elapsed_ms = max(0, now_ms - window_start_ms)
        window_remaining_ms = max(0, engine.tf_sec * 1000 - window_elapsed_ms)
        window_pct_elapsed = min(100.0, window_elapsed_ms / (engine.tf_sec * 1000) * 100)

        coin_data = {}
        for coin in engine.coins:
            bp = engine.latest_price.get(coin, 0)
            op = engine.window_open_price.get(coin, bp) or bp
            chg = (bp - op) / op * 100 if op else 0

            bv10 = sum(engine.buy_vol_sec[coin].get(now_s - i, 0) for i in range(10))
            sv10 = sum(engine.sell_vol_sec[coin].get(now_s - i, 0) for i in range(10))

            # Book
            book = engine.binance_book.get(coin)
            obi = book.imbalance if book else 0.5

            # Binance order book levels (for depth display)
            binance_bids, binance_asks = [], []
            if book:
                binance_bids = [[round(b.price, 2), round(b.qty, 4)] for b in book.bids[:12]]
                binance_asks = [[round(a.price, 2), round(a.qty, 4)] for a in book.asks[:12]]

            # Poly
            mkt = engine.poly_markets.get(coin)
            poly_yes = mkt.up_token.price if mkt and mkt.up_token else 0.5
            poly_no = mkt.down_token.price if mkt and mkt.down_token else 0.5
            poly_end_time = mkt.end_date.strftime("%H:%M:%SZ") if mkt else "--"

            # Poly order book levels
            poly_yes_bids, poly_yes_asks = [], []
            poly_no_bids, poly_no_asks = [], []
            if mkt:
                if mkt.up_token:
                    yb = engine.poly_books.get(mkt.up_token.token_id)
                    if yb:
                        poly_yes_bids = [[round(b.price, 4), round(b.qty, 2)] for b in yb.bids[:8]]
                        poly_yes_asks = [[round(a.price, 4), round(a.qty, 2)] for a in yb.asks[:8]]
                if mkt.down_token:
                    nb = engine.poly_books.get(mkt.down_token.token_id)
                    if nb:
                        poly_no_bids = [[round(b.price, 4), round(b.qty, 2)] for b in nb.bids[:8]]
                        poly_no_asks = [[round(a.price, 4), round(a.qty, 2)] for a in nb.asks[:8]]

            # Poly depth
            depth_series = list(engine.poly_depth_series.get(coin, []))
            poly_bid = 0
            poly_ask = 0
            poly_spread = 0
            poly_depth_pct = 100
            if depth_series:
                _, bid_d, ask_d, spr, _, _ = depth_series[-1]
                poly_bid = bid_d
                poly_ask = ask_d
                poly_spread = spr
                baseline = engine.poly_depth_baseline.get(coin, 0)
                if baseline > 0:
                    poly_depth_pct = (bid_d + ask_d) / baseline * 100

            # Prediction
            pred = engine.prediction.get(coin, ("?", 0))

            # Drain events
            drains = []
            for ts, pct_drop, side in list(engine.drain_events.get(coin, []))[:5]:
                drains.append({"pct": pct_drop, "side": side, "age": (time.time() * 1000 - ts) / 1000})

            # Price history — cycle-aligned base, 3 windows back
            price_hist = []
            for t in engine.ticks[coin]:
                if t.source == "binance" and t.ts_ms >= base_ms:
                    price_hist.append([t.ts_ms - base_ms, round(t.price, 2)])
            if len(price_hist) > 600:
                step = len(price_hist) // 600
                price_hist = price_hist[::step] + [price_hist[-1]]

            # CVD history — current window only, but offset from base_ms for chart alignment
            cvd_hist = []
            for ts_ms_v, val in engine.cvd_series[coin]:
                if ts_ms_v >= ws_start_ms:
                    cvd_hist.append([ts_ms_v - base_ms, round(val, 4)])
            if len(cvd_hist) > 300:
                step = len(cvd_hist) // 300
                cvd_hist = cvd_hist[::step] + [cvd_hist[-1]]

            # Poly YES/NO price history — 3 windows back, same base
            poly_yes_hist = [[ts - base_ms, round(p, 4)]
                             for ts, p in engine.poly_yes_price[coin] if ts >= base_ms]
            poly_no_hist = [[ts - base_ms, round(p, 4)]
                            for ts, p in engine.poly_no_price[coin] if ts >= base_ms]

            # OBI history for chart — 3 windows back
            obi_hist = [[ts - base_ms, round(v, 4)]
                        for ts, v in engine.obi_series[coin] if ts >= base_ms]
            if len(obi_hist) > 300:
                step = len(obi_hist) // 300
                obi_hist = obi_hist[::step] + [obi_hist[-1]]

            # Poly depth history
            poly_depth_hist = []
            for ts, bid_d, ask_d, spr, bb, ba in engine.poly_depth_series.get(coin, []):
                if ts >= base_ms:
                    poly_depth_hist.append([ts - base_ms, round(bid_d, 1), round(ask_d, 1)])
            if len(poly_depth_hist) > 300:
                step = len(poly_depth_hist) // 300
                poly_depth_hist = poly_depth_hist[::step] + [poly_depth_hist[-1]]

            # Volume history in 15s buckets (same timeframe as price chart)
            bucket_sec = 15
            n_buckets = int((now_ms - base_ms) / 1000 / bucket_sec) + 2
            bin_vol_hist = []
            poly_vol_hist = []
            for i in range(n_buckets):
                s0 = base_sec + i * bucket_sec
                s1 = s0 + bucket_sec
                if s0 * 1000 > now_ms:
                    break
                buy_v = sum(engine.buy_vol_sec[coin].get(s, 0) for s in range(s0, s1))
                sell_v = sum(engine.sell_vol_sec[coin].get(s, 0) for s in range(s0, s1))
                yes_v = sum(engine.poly_yes_vol_sec[coin].get(s, 0) for s in range(s0, s1))
                no_v = sum(engine.poly_no_vol_sec[coin].get(s, 0) for s in range(s0, s1))
                off = (s0 - base_sec) * 1000
                bin_vol_hist.append([off, round(buy_v, 4), round(sell_v, 4)])
                poly_vol_hist.append([off, round(yes_v, 4), round(no_v, 4)])

            uptime = max(1, time.time() - engine.start_time)
            coin_data[coin] = {
                "price": round(bp, 2),
                "open_price": round(op, 2),
                "change": round(chg, 4),
                "ticks": engine.total_ticks[coin],
                "tick_rate": round(engine.total_ticks[coin] / uptime, 1),
                "cvd": round(engine.cvd[coin], 3),
                "obi": round(obi, 3),
                "buy_vol_10s": round(bv10, 4),
                "sell_vol_10s": round(sv10, 4),
                "poly_yes": poly_yes,
                "poly_no": poly_no,
                "poly_end_time": poly_end_time,
                "poly_bid_depth": round(poly_bid, 1),
                "poly_ask_depth": round(poly_ask, 1),
                "poly_spread": round(poly_spread, 4),
                "poly_depth_pct": round(poly_depth_pct, 1),
                "poly_spread_baseline": round(engine.poly_spread_baseline.get(coin, 0), 4),
                "poly_depth_baseline": round(engine.poly_depth_baseline.get(coin, 0), 1),
                "mm_status": engine.mm_status.get(coin, "UNKNOWN"),
                "predict_dir": pred[0],
                "predict_conf": round(pred[1], 2),
                "strike_dir": engine.manip_predicted_dir.get(coin, "?"),
                "manip_phase": engine.manip_phase.get(coin, "IDLE"),
                "drain_events": drains,
                # Order book levels
                "binance_bids": binance_bids,
                "binance_asks": binance_asks,
                "poly_yes_bids": poly_yes_bids,
                "poly_yes_asks": poly_yes_asks,
                "poly_no_bids": poly_no_bids,
                "poly_no_asks": poly_no_asks,
                # Holders
                "holders_yes": engine.holders.get(coin, {}).get("yes", [])[:10],
                "holders_no": engine.holders.get(coin, {}).get("no", [])[:10],
                "holders_totals": engine.holders_totals.get(coin, {"yes_count": 0, "no_count": 0, "yes_amt": 0, "no_amt": 0}),
                "holders_age": round(time.time() - engine.holders_updated.get(coin, 0), 0),
                # Poly trade flow
                "poly_flow": engine.poly_flow.get(coin, {}),
                # Chart data (all using base_ms offset for aligned X axis)
                "price_hist": price_hist,
                "cvd_hist": cvd_hist,
                "poly_yes_hist": poly_yes_hist,
                "poly_no_hist": poly_no_hist,
                "obi_hist": obi_hist,
                "poly_depth_hist": poly_depth_hist,
                "bin_vol_hist": bin_vol_hist,
                "poly_vol_hist": poly_vol_hist,
            }

        # Alerts
        alerts = []
        for a in list(engine.alerts)[:20]:
            dt = datetime.fromtimestamp(a.ts_ms / 1000, tz=timezone.utc)
            alerts.append({
                "time": dt.strftime("%H:%M:%S.") + f"{a.ts_ms % 1000:03d}",
                "severity": a.severity,
                "message": a.message,
            })

        # History
        hist_items = []
        for w in engine.history.get(coin0, [])[-25:]:
            dt = datetime.fromtimestamp(w.window_start, tz=timezone.utc)
            overall_chg = (w.close_price - w.open_price) / w.open_price * 100 if w.open_price else 0
            # Poly actual outcome: token that went to ~$1.00 is the winner
            # >=0.95 means it settled (market chose it); <0.95 means it was manipulated price at close
            if w.poly_yes_end >= 0.95:
                poly_outcome = "UP"
                poly_settled = True
            elif w.poly_no_end >= 0.95:
                poly_outcome = "DOWN"
                poly_settled = True
            elif w.poly_yes_end > 0.65:
                poly_outcome = "UP~"
                poly_settled = False
            elif w.poly_no_end > 0.65:
                poly_outcome = "DOWN~"
                poly_settled = False
            elif w.poly_yes_end > w.poly_no_end:
                poly_outcome = "UP?"
                poly_settled = False
            elif w.poly_no_end > w.poly_yes_end:
                poly_outcome = "DOWN?"
                poly_settled = False
            else:
                poly_outcome = "?"
                poly_settled = False
            hist_items.append({
                "time": dt.strftime("%H:%M"),
                "ts": w.window_start,
                "dir": w.direction,
                "chg": round(overall_chg, 4),
                "open": round(w.open_price, 2),
                "close": round(w.close_price, 2),
                "kz_dir": w.kz_direction,
                "kz_chg": round(w.kz_price_change_pct, 5),
                "kz_dvol": round(w.kz_dollar_volume, 0),
                "kz_cvd": round(w.kz_cvd, 3),
                "cvd": round(w.cvd_at_close, 3),
                "rev": w.kz_reversed,
                "poly": poly_outcome,
                "poly_settled": poly_settled,
                "py": round(w.poly_yes_end, 2),
                "pn": round(w.poly_no_end, 2),
                "py_start": round(w.poly_yes_start, 2),
                "pn_start": round(w.poly_no_start, 2),
                "obi": round(w.avg_obi, 3),
                "kz_obi": round(w.kz_avg_obi, 3),
            })

        # Patterns
        h = engine.history.get(coin0, [])
        patterns = {}
        if len(h) >= 2:
            patterns = {
                "count": len(h),
                "reversals": sum(1 for w in h if w.kz_reversed),
                "rev_pct": sum(1 for w in h if w.kz_reversed) / len(h) * 100,
                "avg_kz_chg": sum(abs(w.kz_price_change_pct) for w in h) / len(h),
                "cvd_accuracy": sum(1 for w in h if (w.cvd_at_close > 0 and w.direction == "UP") or (w.cvd_at_close < 0 and w.direction == "DOWN")) / len(h) * 100,
                "obi_accuracy": round(sum(1 for w in h if (w.avg_obi > 0.52 and w.direction == "UP") or (w.avg_obi < 0.48 and w.direction == "DOWN")) / len(h) * 100, 0),
            }

        payload = {
            "timeframe": engine.tf,
            "tf_sec": engine.tf_sec,
            "kz_sec": KILL_ZONE_SEC,
            "window_utc": ws_dt.strftime("%H:%M:%S"),
            "remaining": round(window_remaining_ms / 1000, 1),
            "pct_elapsed": round(window_pct_elapsed, 1),
            "kill_zone": window_remaining_ms / 1000 <= KILL_ZONE_SEC,
            "phase": engine.manip_phase.get(coin0, "IDLE"),
            "coins": engine.coins,
            "coin_data": coin_data,
            "alerts": alerts,
            "history": hist_items,
            "patterns": patterns,
            # Chart timing (for aligned X axis across all sub-charts)
            "now_ms": now_ms,
            "base_ms": base_ms,
            "chart_span_ms": chart_span_ms,
        }

        data = json.dumps(payload)
        dead = set()
        for client in WS_CLIENTS:
            try:
                await client.send_text(data)
            except Exception:
                dead.add(client)
        WS_CLIENTS -= dead


def main():
    import argparse
    global _ARGS

    p = argparse.ArgumentParser()
    p.add_argument("--coins", default="BTC")
    p.add_argument("--timeframe", default="5m", choices=["5m", "15m", "1h"])
    p.add_argument("--port", type=int, default=7777)
    p.add_argument("--log-dir", default="data/surveillance")
    p.add_argument("--no-alpaca", action="store_true")
    _ARGS = p.parse_args()

    _log(f"Market Surveillance starting...")
    _log(f"Coins: {_ARGS.coins} | TF: {_ARGS.timeframe} | Port: {_ARGS.port}")

    uvicorn.run(app, host="0.0.0.0", port=_ARGS.port, log_level="warning")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Market Surveillance — Real-time tick-level multi-exchange monitoring
for detecting manipulation around Polymarket 5-min BTC settlements.

Feeds:
  1. Binance WebSocket — individual trades (sub-100ms) + order book depth (100ms)
  2. Alpaca WebSocket  — individual crypto trades (cross-exchange comparison)
  3. Polymarket CLOB   — YES/NO token orderbook + trades (the market itself)
  4. Polymarket Gamma   — market discovery (which tokens to watch)

Analytics:
  - Cumulative Volume Delta (CVD): buy pressure - sell pressure over time
  - Order Book Imbalance (OBI): bid depth vs ask depth at best levels
  - Volume acceleration per second (heatmap)
  - Large trade detection & direction tracking
  - Cross-exchange price divergence (Binance vs Alpaca)
  - Polymarket token flow (who's buying YES vs NO)
  - Kill Zone analysis (last 30s of each 5-min window)
  - Window-over-window pattern detection
  - Manipulation prediction signal

Usage:
  python surveillance.py
  python surveillance.py --coins BTC,ETH --timeframe 5m
  python surveillance.py --no-ui        # headless logging only
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

import httpx
import websockets
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

# ═══════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════

BINANCE_WS = "wss://stream.binance.com:9443/ws"
ALPACA_WS = "wss://stream.data.alpaca.markets/v1beta3/crypto/us"
POLYMARKET_CLOB_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
POLYMARKET_DATA_API = "https://data-api.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"

TIMEFRAME_SEC = {"5m": 300, "15m": 900, "1h": 3600}

# Series IDs for Gamma market discovery
GAMMA_SERIES = {
    "5m":  {"BTC": "10684", "ETH": "10683", "SOL": "10686", "XRP": "10685"},
    "15m": {"BTC": "10192", "ETH": "10191", "SOL": "10423", "XRP": "10422"},
}

KILL_ZONE_SEC = 30          # last 30 seconds of each window
LARGE_TRADE_BTC = 0.5       # BTC threshold for "whale" trades
VOLUME_SPIKE_MULT = 3.0     # volume spike = 3x rolling average
PRICE_JUMP_PCT = 0.015      # 0.015% in 1 second = suspicious
TICK_BUFFER = 20000          # ticks in memory per coin
BOOK_DEPTH_LEVELS = 10       # order book levels to track

# ── Book drain / MM detection thresholds ──
POLY_DEPTH_DROP_PCT = 30     # 30% depth drop in 5s = book being drained
POLY_SPREAD_WIDE_MULT = 2.0 # spread 2x normal = MMs pulling out
BTC_QUIET_THRESHOLD = 0.005  # BTC moving < 0.005% in 10s = "quiet"
BOOK_DRAIN_WINDOW_SEC = 5    # look back 5s for drain detection
MM_FLEE_SPREAD_MULT = 3.0   # spread 3x baseline = MMs definitely gone


# ═══════════════════════════════════════════════════════════════════════
# DATA TYPES
# ═══════════════════════════════════════════════════════════════════════

@dataclass
class Tick:
    source: str       # "binance" | "alpaca"
    symbol: str       # "BTCUSDT"
    price: float
    qty: float
    side: str         # "buy" | "sell"
    ts_ms: int        # millisecond timestamp
    trade_id: str = ""
    dollar_value: float = 0.0

    def __post_init__(self):
        self.dollar_value = self.price * self.qty


@dataclass
class BookLevel:
    price: float
    qty: float


@dataclass
class BookSnapshot:
    source: str       # "binance" | "polymarket"
    symbol: str
    bids: list[BookLevel]
    asks: list[BookLevel]
    ts_ms: int

    @property
    def best_bid(self) -> float:
        return self.bids[0].price if self.bids else 0

    @property
    def best_ask(self) -> float:
        return self.asks[0].price if self.asks else 0

    @property
    def spread(self) -> float:
        return self.best_ask - self.best_bid if self.best_ask and self.best_bid else 0

    @property
    def mid(self) -> float:
        return (self.best_bid + self.best_ask) / 2 if self.best_bid and self.best_ask else 0

    @property
    def bid_depth(self) -> float:
        return sum(l.qty for l in self.bids[:BOOK_DEPTH_LEVELS])

    @property
    def ask_depth(self) -> float:
        return sum(l.qty for l in self.asks[:BOOK_DEPTH_LEVELS])

    @property
    def imbalance(self) -> float:
        """Bid-ask imbalance: >0.5 = more bids, <0.5 = more asks."""
        total = self.bid_depth + self.ask_depth
        return self.bid_depth / total if total > 0 else 0.5


@dataclass
class PolyToken:
    token_id: str
    outcome: str      # "Up" | "Down"
    price: float = 0.5


@dataclass
class PolyMarket:
    condition_id: str
    coin: str
    timeframe: str
    end_date: datetime
    tokens: list[PolyToken] = field(default_factory=list)

    @property
    def up_token(self) -> PolyToken | None:
        return next((t for t in self.tokens if t.outcome == "Up"), None)

    @property
    def down_token(self) -> PolyToken | None:
        return next((t for t in self.tokens if t.outcome == "Down"), None)


@dataclass
class WindowStats:
    window_start: int
    open_price: float
    close_price: float
    high: float
    low: float
    total_volume: float
    total_dollar_volume: float
    trade_count: int
    buy_volume: float
    sell_volume: float
    cvd_at_close: float          # cumulative volume delta at close
    # Kill zone
    kz_price_change_pct: float
    kz_volume: float
    kz_dollar_volume: float
    kz_trade_count: int
    kz_max_single_trade: float
    kz_buy_volume: float
    kz_sell_volume: float
    kz_cvd: float                # CVD during kill zone
    # Direction
    direction: str               # "UP" | "DOWN" | "FLAT"
    kz_direction: str
    kz_reversed: bool
    # Polymarket
    poly_yes_start: float
    poly_yes_end: float
    poly_no_start: float
    poly_no_end: float
    # Book
    avg_obi: float               # average order book imbalance during window
    kz_avg_obi: float            # average OBI during kill zone


@dataclass
class Alert:
    ts_ms: int
    alert_type: str
    message: str
    severity: str     # "INFO" | "WARN" | "HIGH" | "CRITICAL"


# ═══════════════════════════════════════════════════════════════════════
# SURVEILLANCE ENGINE
# ═══════════════════════════════════════════════════════════════════════

class Engine:
    def __init__(self, coins: list[str], timeframe: str, log_dir: str):
        self.coins = coins
        self.tf = timeframe
        self.tf_sec = TIMEFRAME_SEC[timeframe]
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # ── Per-coin state ──
        self.ticks: dict[str, deque[Tick]] = {c: deque(maxlen=TICK_BUFFER) for c in coins}
        self.total_ticks: dict[str, int] = {c: 0 for c in coins}
        self.latest_price: dict[str, float] = {}
        self.latest_ts: dict[str, int] = {}

        # Volume per second (epoch_sec -> vol)
        self.buy_vol_sec: dict[str, dict[int, float]] = {c: defaultdict(float) for c in coins}
        self.sell_vol_sec: dict[str, dict[int, float]] = {c: defaultdict(float) for c in coins}
        self.trades_sec: dict[str, dict[int, int]] = {c: defaultdict(int) for c in coins}
        self.dollar_vol_sec: dict[str, dict[int, float]] = {c: defaultdict(float) for c in coins}

        # Cumulative Volume Delta — reset each window
        self.cvd: dict[str, float] = {c: 0.0 for c in coins}
        self.cvd_series: dict[str, deque[tuple[int, float]]] = {c: deque(maxlen=600) for c in coins}

        # Order book snapshots
        self.binance_book: dict[str, BookSnapshot | None] = {c: None for c in coins}
        self.obi_series: dict[str, deque[tuple[int, float]]] = {c: deque(maxlen=600) for c in coins}

        # Large trades in current window
        self.large_trades: dict[str, list[Tick]] = {c: [] for c in coins}

        # Window tracking
        self.window_open_price: dict[str, float | None] = {c: None for c in coins}
        self.window_ticks: dict[str, list[Tick]] = {c: [] for c in coins}
        self.window_obi_samples: dict[str, list[float]] = {c: [] for c in coins}
        self._last_window_start: dict[str, int] = {c: 0 for c in coins}

        # Completed windows
        self.history: dict[str, list[WindowStats]] = {c: [] for c in coins}

        # Polymarket state
        self.poly_markets: dict[str, PolyMarket] = {}  # coin -> current active market
        self.poly_books: dict[str, BookSnapshot | None] = {}  # token_id -> book
        self.poly_yes_price: dict[str, deque[tuple[int, float]]] = {c: deque(maxlen=600) for c in coins}
        self.poly_no_price: dict[str, deque[tuple[int, float]]] = {c: deque(maxlen=600) for c in coins}
        self.poly_trades: dict[str, deque[Tick]] = {c: deque(maxlen=200) for c in coins}

        # Alpaca latest (for cross-exchange)
        self.alpaca_price: dict[str, float] = {}
        self.alpaca_ts: dict[str, int] = {}

        # Alerts
        self.alerts: deque[Alert] = deque(maxlen=300)

        # Prediction
        self.prediction: dict[str, tuple[str, float]] = {}  # coin -> (direction, confidence)

        # ── Polymarket Book Drain / MM Tracking ──
        # Time series of total depth on each Poly token book
        # (ts_ms, total_bid_depth, total_ask_depth, spread, best_bid, best_ask)
        self.poly_depth_series: dict[str, deque[tuple[int, float, float, float, float, float]]] = {
            c: deque(maxlen=1200) for c in coins  # ~2 min at 100ms updates
        }
        # Baseline spread (rolling median over last 60s)
        self.poly_spread_baseline: dict[str, float] = {c: 0.0 for c in coins}
        # Baseline depth (rolling median)
        self.poly_depth_baseline: dict[str, float] = {c: 0.0 for c in coins}
        # Current MM status
        self.mm_status: dict[str, str] = {c: "UNKNOWN" for c in coins}  # "PRESENT" | "THINNING" | "FLED"
        # Book drain events: (ts_ms, depth_drop_pct, side_drained)
        self.drain_events: dict[str, deque[tuple[int, float, str]]] = {
            c: deque(maxlen=100) for c in coins
        }
        # BTC volatility state
        self.btc_quiet: dict[str, bool] = {c: True for c in coins}
        # Manipulation sequence state machine per coin
        # Phases: IDLE -> DRAIN_DETECTED -> MM_FLEEING -> STRIKE_IMMINENT -> STRIKE
        self.manip_phase: dict[str, str] = {c: "IDLE" for c in coins}
        self.manip_phase_ts: dict[str, int] = {c: 0 for c in coins}
        # Predicted strike direction based on which side of Poly book is being drained
        self.manip_predicted_dir: dict[str, str] = {c: "?" for c in coins}

        # ── Holders / Positions Tracking ──
        # Top holders per coin: {coin: {"yes": [...], "no": [...]}}
        self.holders: dict[str, dict] = {c: {"yes": [], "no": []} for c in coins}
        self.holders_totals: dict[str, dict] = {c: {"yes_count": 0, "no_count": 0, "yes_amt": 0.0, "no_amt": 0.0} for c in coins}
        self.holders_updated: dict[str, float] = {c: 0.0 for c in coins}

        # ── Poly Trade Flow (aggressive buy vs sell on YES/NO tokens) ──
        # Running tally reset each window: {coin: {"yes_buy": 0, "yes_sell": 0, "no_buy": 0, "no_sell": 0}}
        self.poly_flow: dict[str, dict[str, float]] = {
            c: {"yes_buy": 0, "yes_sell": 0, "no_buy": 0, "no_sell": 0} for c in coins
        }
        # Recent poly trade prices for aggressor detection
        self.poly_last_price: dict[str, float] = {}  # token_id -> last price
        # Poly volume per second (for time-bucketed volume chart)
        self.poly_yes_vol_sec: dict[str, dict[int, float]] = {c: defaultdict(float) for c in coins}
        self.poly_no_vol_sec: dict[str, dict[int, float]] = {c: defaultdict(float) for c in coins}

        self.start_time = time.time()
        self._log_handles: dict[str, object] = {}

    # ── Window timing ──

    def window_start(self) -> int:
        now = int(time.time())
        return now - (now % self.tf_sec)

    def sec_elapsed(self) -> float:
        return time.time() - self.window_start()

    def sec_remaining(self) -> float:
        return self.tf_sec - self.sec_elapsed()

    def pct_elapsed(self) -> float:
        return self.sec_elapsed() / self.tf_sec * 100

    def in_kill_zone(self) -> bool:
        return self.sec_remaining() <= KILL_ZONE_SEC

    # ── Process tick ──

    def process_tick(self, tick: Tick):
        coin = tick.symbol.replace("USDT", "").replace("USD", "").replace("/", "")
        if coin not in self.coins:
            return

        ws = self.window_start()

        # Window rollover detection
        if self._last_window_start[coin] and self._last_window_start[coin] < ws:
            self._finalize_window(coin, self._last_window_start[coin])
            self.cvd[coin] = 0.0
            self.large_trades[coin] = []
            self.window_obi_samples[coin] = []
            self.window_open_price[coin] = None
            self.window_ticks[coin] = []
            self.poly_flow[coin] = {"yes_buy": 0, "yes_sell": 0, "no_buy": 0, "no_sell": 0}
            self.manip_phase[coin] = "IDLE"
            self.manip_phase_ts[coin] = 0
            self.manip_predicted_dir[coin] = "?"
        self._last_window_start[coin] = ws

        # Store tick
        self.ticks[coin].append(tick)
        self.total_ticks[coin] += 1
        self.window_ticks[coin].append(tick)

        # Source-specific price tracking
        if tick.source == "binance":
            self.latest_price[coin] = tick.price
            self.latest_ts[coin] = tick.ts_ms
        elif tick.source == "alpaca":
            self.alpaca_price[coin] = tick.price
            self.alpaca_ts[coin] = tick.ts_ms

        # Window open
        if self.window_open_price[coin] is None:
            self.window_open_price[coin] = tick.price

        # Per-second buckets
        sec = tick.ts_ms // 1000
        self.trades_sec[coin][sec] += 1
        self.dollar_vol_sec[coin][sec] += tick.dollar_value
        if tick.side == "buy":
            self.buy_vol_sec[coin][sec] += tick.qty
        else:
            self.sell_vol_sec[coin][sec] += tick.qty

        # CVD
        delta = tick.qty if tick.side == "buy" else -tick.qty
        self.cvd[coin] += delta
        self.cvd_series[coin].append((tick.ts_ms, self.cvd[coin]))

        # Large trade
        if tick.qty >= LARGE_TRADE_BTC:
            self.large_trades[coin].append(tick)

        # Anomaly detection
        self._detect(coin, tick)

        # Log
        self._log_tick(coin, tick)

    # ── Process order book ──

    def process_book(self, book: BookSnapshot):
        coin_raw = book.symbol.replace("USDT", "").replace("USD", "")

        if book.source == "binance":
            coin = coin_raw
            if coin not in self.coins:
                return
            self.binance_book[coin] = book
            self.obi_series[coin].append((book.ts_ms, book.imbalance))
            self.window_obi_samples[coin].append(book.imbalance)
        elif book.source == "polymarket":
            self.poly_books[book.symbol] = book
            # Resolve which coin & outcome this token belongs to
            for coin in self.coins:
                mkt = self.poly_markets.get(coin)
                if not mkt:
                    continue
                for tok in mkt.tokens:
                    if tok.token_id == book.symbol:
                        self.process_poly_book_update(coin, tok.outcome, book)
                        return

    # ── Process Polymarket trade ──

    def process_poly_trade(self, coin: str, outcome: str, price: float, size: float, ts_ms: int):
        if coin not in self.coins:
            return
        # Track volume per second
        sec = ts_ms // 1000
        if outcome == "Up":
            self.poly_yes_price[coin].append((ts_ms, price))
            self.poly_yes_vol_sec[coin][sec] += size
        else:
            self.poly_no_price[coin].append((ts_ms, price))
            self.poly_no_vol_sec[coin][sec] += size
        self.poly_trades[coin].append(Tick(
            source="polymarket", symbol=f"{coin}_{outcome}",
            price=price, qty=size, side=outcome.lower(), ts_ms=ts_ms,
        ))

        # ── Track aggressive buy/sell flow on Poly tokens ──
        # Detect aggressor: if price went UP from last trade, buyer was aggressive
        # If price went DOWN, seller was aggressive
        mkt = self.poly_markets.get(coin)
        if mkt:
            tok = next((t for t in mkt.tokens if t.outcome == outcome), None)
            if tok:
                last_p = self.poly_last_price.get(tok.token_id)
                self.poly_last_price[tok.token_id] = price
                if last_p is not None:
                    key_side = outcome.lower()  # "up" or "down"
                    if price > last_p:
                        # Price ticked up = aggressive buyer
                        self.poly_flow[coin][f"{'yes' if outcome == 'Up' else 'no'}_buy"] += size
                    elif price < last_p:
                        # Price ticked down = aggressive seller
                        self.poly_flow[coin][f"{'yes' if outcome == 'Up' else 'no'}_sell"] += size

    # ── Anomaly detection ──

    def _detect(self, coin: str, tick: Tick):
        sec = tick.ts_ms // 1000
        remaining = self.sec_remaining()
        kz = self.in_kill_zone()
        kz_tag = f" [{remaining:.0f}s left]" if remaining < 60 else ""

        # Large trade
        if tick.qty >= LARGE_TRADE_BTC:
            sev = "CRITICAL" if kz else "HIGH"
            self.alerts.appendleft(Alert(
                tick.ts_ms, "WHALE",
                f"{coin} {tick.side.upper()} {tick.qty:.3f} BTC (${tick.dollar_value:,.0f}) "
                f"@ ${tick.price:,.2f}{kz_tag}",
                sev,
            ))

        # Volume spike
        recent = [self.buy_vol_sec[coin].get(sec - i, 0) + self.sell_vol_sec[coin].get(sec - i, 0)
                  for i in range(1, 31)]
        avg = statistics.mean(recent) if recent and any(v > 0 for v in recent) else 0
        cur = self.buy_vol_sec[coin].get(sec, 0) + self.sell_vol_sec[coin].get(sec, 0)
        if avg > 0 and cur > avg * VOLUME_SPIKE_MULT:
            sev = "CRITICAL" if kz else "HIGH"
            self.alerts.appendleft(Alert(
                tick.ts_ms, "VOL_SPIKE",
                f"{coin} {cur:.2f} BTC/s ({cur/avg:.1f}x avg){kz_tag}",
                sev,
            ))

        # Price jump in 1 second
        recent_ticks = [t for t in self.ticks[coin]
                        if 800 < tick.ts_ms - t.ts_ms < 1500 and t.source == "binance"]
        if recent_ticks:
            old_p = recent_ticks[-1].price
            jump = abs(tick.price - old_p) / old_p * 100
            if jump >= PRICE_JUMP_PCT:
                d = "UP" if tick.price > old_p else "DOWN"
                sev = "CRITICAL" if kz else "HIGH"
                self.alerts.appendleft(Alert(
                    tick.ts_ms, "PRICE_JUMP",
                    f"{coin} {d} {jump:.4f}% in 1s (${old_p:,.2f} -> ${tick.price:,.2f}){kz_tag}",
                    sev,
                ))

        # CVD divergence from price (price goes up but CVD goes down = suspicious)
        if len(self.cvd_series[coin]) > 60:
            cvd_30s_ago = None
            for ts, val in self.cvd_series[coin]:
                if tick.ts_ms - ts > 29000 and tick.ts_ms - ts < 31000:
                    cvd_30s_ago = val
                    break
            if cvd_30s_ago is not None:
                cvd_delta = self.cvd[coin] - cvd_30s_ago
                price_30s = [t.price for t in self.ticks[coin]
                             if 29000 < tick.ts_ms - t.ts_ms < 31000 and t.source == "binance"]
                if price_30s:
                    price_delta = tick.price - price_30s[-1]
                    # Price up but CVD down = sells pushing price up? Manipulation.
                    if price_delta > 0 and cvd_delta < -0.5:
                        self.alerts.appendleft(Alert(
                            tick.ts_ms, "CVD_DIVERGE",
                            f"{coin} Price UP but CVD DOWN (cvd={cvd_delta:+.2f}) — possible manipulation{kz_tag}",
                            "HIGH" if kz else "WARN",
                        ))
                    elif price_delta < 0 and cvd_delta > 0.5:
                        self.alerts.appendleft(Alert(
                            tick.ts_ms, "CVD_DIVERGE",
                            f"{coin} Price DOWN but CVD UP (cvd={cvd_delta:+.2f}) — possible manipulation{kz_tag}",
                            "HIGH" if kz else "WARN",
                        ))

        # Update prediction at 50% and 80% elapsed
        pct = self.pct_elapsed()
        if 49 < pct < 51 or 79 < pct < 81 or (kz and int(remaining) % 5 == 0):
            self._update_prediction(coin)

    # ── Prediction ──

    def _update_prediction(self, coin: str):
        signals = []
        weights = []

        # 1. CVD direction (strongest signal)
        if self.cvd[coin] > 0.3:
            signals.append(1.0)  # UP
        elif self.cvd[coin] < -0.3:
            signals.append(-1.0)  # DOWN
        else:
            signals.append(0.0)
        weights.append(3.0)

        # 2. Book imbalance
        book = self.binance_book.get(coin)
        if book:
            obi = book.imbalance
            signals.append((obi - 0.5) * 2)  # -1 to +1
            weights.append(2.0)

        # 3. Large trade bias
        if self.large_trades[coin]:
            buy_vol = sum(t.qty for t in self.large_trades[coin] if t.side == "buy")
            sell_vol = sum(t.qty for t in self.large_trades[coin] if t.side == "sell")
            total = buy_vol + sell_vol
            if total > 0:
                signals.append((buy_vol - sell_vol) / total)
                weights.append(2.5)

        # 4. Price momentum (last 30s)
        series = [(t.ts_ms, t.price) for t in self.ticks[coin]
                  if time.time() * 1000 - t.ts_ms < 30000 and t.source == "binance"]
        if len(series) > 10:
            p_start = series[0][1]
            p_end = series[-1][1]
            mom = (p_end - p_start) / p_start * 100
            signals.append(max(-1, min(1, mom / 0.05)))  # normalize around 0.05%
            weights.append(1.5)

        # 5. Cross-exchange divergence (Binance vs Alpaca)
        bp = self.latest_price.get(coin)
        ap = self.alpaca_price.get(coin)
        if bp and ap and bp > 0:
            div = (ap - bp) / bp * 100  # if Alpaca > Binance, Alpaca leads UP
            if abs(div) > 0.005:
                signals.append(max(-1, min(1, div / 0.02)))
                weights.append(1.0)

        # 6. Polymarket book drain direction (STRONGEST leading signal!)
        if self.manip_predicted_dir[coin] in ("UP", "DOWN"):
            drain_signal = 1.0 if self.manip_predicted_dir[coin] == "UP" else -1.0
            # Weight heavily if we're in an active manipulation sequence
            phase_weight = 5.0 if self.manip_phase[coin] != "IDLE" else 1.0
            signals.append(drain_signal)
            weights.append(phase_weight)

        # 7. Poly YES/NO token price movement (leading indicator)
        py = list(self.poly_yes_price[coin])
        pn = list(self.poly_no_price[coin])
        if len(py) > 3 and len(pn) > 3:
            yes_trend = py[-1][1] - py[0][1]
            no_trend = pn[-1][1] - pn[0][1]
            if abs(yes_trend) > 0.01 or abs(no_trend) > 0.01:
                poly_signal = max(-1, min(1, (yes_trend - no_trend) * 5))
                signals.append(poly_signal)
                weights.append(3.0)  # Poly leads BTC!

        # 8. Poly aggressive trade flow — who's buying YES vs NO tokens?
        pf = self.poly_flow[coin]
        yes_net = pf["yes_buy"] - pf["yes_sell"]  # positive = aggressively buying YES
        no_net = pf["no_buy"] - pf["no_sell"]    # positive = aggressively buying NO
        flow_total = abs(yes_net) + abs(no_net)
        if flow_total > 0.5:  # minimum activity threshold
            # More YES buying = UP, more NO buying = DOWN
            flow_signal = max(-1, min(1, (yes_net - no_net) / flow_total))
            flow_weight = 4.0 if self.manip_phase[coin] != "IDLE" else 2.0
            signals.append(flow_signal)
            weights.append(flow_weight)

        # Weighted average
        if not signals or not weights:
            return

        weighted_sum = sum(s * w for s, w in zip(signals, weights))
        total_weight = sum(weights)
        score = weighted_sum / total_weight  # -1 to +1

        direction = "UP" if score > 0.1 else ("DOWN" if score < -0.1 else "FLAT")
        confidence = min(1.0, abs(score))

        self.prediction[coin] = (direction, confidence)

    # ── Polymarket Book Drain & MM Detection ──

    def process_poly_book_update(self, coin: str, outcome: str, book: BookSnapshot):
        """Track Polymarket orderbook depth changes — the KEY leading indicator."""
        if coin not in self.coins:
            return

        total_bid = book.bid_depth
        total_ask = book.ask_depth
        spread = book.spread
        ts = book.ts_ms

        self.poly_depth_series[coin].append((ts, total_bid, total_ask, spread, book.best_bid, book.best_ask))

        # Update baselines (rolling median over last 60s of samples)
        recent = [(b, a, s) for t, b, a, s, _, _ in self.poly_depth_series[coin]
                  if ts - t < 60000]
        if len(recent) > 10:
            self.poly_depth_baseline[coin] = statistics.median(b + a for b, a, _ in recent)
            self.poly_spread_baseline[coin] = statistics.median(s for _, _, s in recent)

        # ── Detect book drain ──
        # Compare depth now vs 5 seconds ago
        old_depth = [(b, a) for t, b, a, _, _, _ in self.poly_depth_series[coin]
                     if 4000 < ts - t < 6000]
        if old_depth:
            old_total = old_depth[-1][0] + old_depth[-1][1]
            new_total = total_bid + total_ask
            if old_total > 0:
                drop_pct = (old_total - new_total) / old_total * 100
                if drop_pct > POLY_DEPTH_DROP_PCT:
                    # Which side is being drained?
                    bid_drop = (old_depth[-1][0] - total_bid) / old_depth[-1][0] * 100 if old_depth[-1][0] > 0 else 0
                    ask_drop = (old_depth[-1][1] - total_ask) / old_depth[-1][1] * 100 if old_depth[-1][1] > 0 else 0

                    if bid_drop > ask_drop:
                        side = f"{outcome}_BIDS"  # bids being eaten = someone selling into bids
                        predicted = "DOWN" if outcome == "Up" else "UP"
                    else:
                        side = f"{outcome}_ASKS"  # asks being eaten = someone buying through asks
                        predicted = "UP" if outcome == "Up" else "DOWN"

                    self.drain_events[coin].appendleft((ts, drop_pct, side))
                    self.manip_predicted_dir[coin] = predicted

                    kz_tag = f" [{self.sec_remaining():.0f}s left]" if self.sec_remaining() < 60 else ""
                    self.alerts.appendleft(Alert(
                        ts, "BOOK_DRAIN",
                        f"{coin} POLY {outcome} book DRAINING: -{drop_pct:.0f}% in 5s "
                        f"({side}) -> predict {predicted}{kz_tag}",
                        "CRITICAL" if self.in_kill_zone() else "HIGH",
                    ))

        # ── Detect MM status ──
        baseline_spread = self.poly_spread_baseline[coin]
        baseline_depth = self.poly_depth_baseline[coin]

        if baseline_spread > 0 and baseline_depth > 0:
            spread_ratio = spread / baseline_spread if baseline_spread > 0 else 1
            depth_ratio = (total_bid + total_ask) / baseline_depth if baseline_depth > 0 else 1

            old_status = self.mm_status[coin]

            if spread_ratio > MM_FLEE_SPREAD_MULT or depth_ratio < 0.3:
                self.mm_status[coin] = "FLED"
            elif spread_ratio > POLY_SPREAD_WIDE_MULT or depth_ratio < 0.6:
                self.mm_status[coin] = "THINNING"
            else:
                self.mm_status[coin] = "PRESENT"

            # Alert on status change
            if self.mm_status[coin] != old_status and self.mm_status[coin] in ("FLED", "THINNING"):
                kz_tag = f" [{self.sec_remaining():.0f}s left]" if self.sec_remaining() < 60 else ""
                self.alerts.appendleft(Alert(
                    ts, "MM_STATUS",
                    f"{coin} MMs {self.mm_status[coin]}! spread={spread_ratio:.1f}x "
                    f"depth={depth_ratio:.0%} of baseline{kz_tag}",
                    "CRITICAL" if self.mm_status[coin] == "FLED" else "HIGH",
                ))

        # ── Check BTC quiet state ──
        bp = self.latest_price.get(coin)
        if bp:
            old_prices = [t.price for t in self.ticks[coin]
                         if ts - t.ts_ms < 10000 and t.source == "binance"]
            if len(old_prices) > 5:
                p_range = (max(old_prices) - min(old_prices)) / old_prices[0] * 100
                self.btc_quiet[coin] = p_range < BTC_QUIET_THRESHOLD

        # ── Manipulation sequence state machine ──
        self._update_manip_sequence(coin, ts)

    def _update_manip_sequence(self, coin: str, ts_ms: int):
        """
        Track the manipulation sequence:
        IDLE -> DRAIN_DETECTED -> MM_FLEEING -> STRIKE_IMMINENT

        The pattern from Joacim:
        1. BTC is quiet (not moving)
        2. Someone aggressively drains the Poly orderbook
        3. Small BTC volatility spike triggers MMs to pull orders
        4. MMs flee, spread widens
        5. They push price through the empty book
        """
        phase = self.manip_phase[coin]
        age_ms = ts_ms - self.manip_phase_ts[coin]

        # Reset if too old (>60s in any phase = stale)
        if age_ms > 60000 and phase != "IDLE":
            self.manip_phase[coin] = "IDLE"
            self.manip_predicted_dir[coin] = "?"
            self.manip_phase_ts[coin] = ts_ms
            phase = "IDLE"

        if phase == "IDLE":
            # Look for drain events while BTC is quiet
            recent_drains = [d for d in self.drain_events[coin] if ts_ms - d[0] < 10000]
            if recent_drains and self.btc_quiet.get(coin, True):
                self.manip_phase[coin] = "DRAIN_DETECTED"
                self.manip_phase_ts[coin] = ts_ms
                self.alerts.appendleft(Alert(
                    ts_ms, "SEQUENCE",
                    f"{coin} Phase 1: BOOK DRAIN while BTC quiet -> predict {self.manip_predicted_dir[coin]}",
                    "HIGH",
                ))

        elif phase == "DRAIN_DETECTED":
            # Look for MM status change
            if self.mm_status[coin] in ("THINNING", "FLED"):
                self.manip_phase[coin] = "MM_FLEEING"
                self.manip_phase_ts[coin] = ts_ms
                self.alerts.appendleft(Alert(
                    ts_ms, "SEQUENCE",
                    f"{coin} Phase 2: MMs {self.mm_status[coin]} -> STRIKE COMING {self.manip_predicted_dir[coin]}",
                    "CRITICAL",
                ))
            # Also check if BTC starts moving (vol spike = trigger)
            if not self.btc_quiet.get(coin, True):
                self.manip_phase[coin] = "MM_FLEEING"
                self.manip_phase_ts[coin] = ts_ms

        elif phase == "MM_FLEEING":
            # Only escalate inside the kill zone to avoid random blinking
            if self.in_kill_zone():
                self.manip_phase[coin] = "STRIKE_IMMINENT"
                self.manip_phase_ts[coin] = ts_ms
                self.alerts.appendleft(Alert(
                    ts_ms, "SEQUENCE",
                    f"{coin} Phase 3: STRIKE IMMINENT -> {self.manip_predicted_dir[coin]} "
                    f"(book empty, MMs gone, {self.sec_remaining():.0f}s left)",
                    "CRITICAL",
                ))
            else:
                self.manip_phase_ts[coin] = ts_ms

        elif phase == "STRIKE_IMMINENT":
            # Reset after 15s
            if age_ms > 15000:
                self.manip_phase[coin] = "IDLE"
                self.manip_predicted_dir[coin] = "?"
                self.manip_phase_ts[coin] = ts_ms

    # ── Window finalization ──

    def _finalize_window(self, coin: str, ws: int):
        # Guard against duplicate finalization
        if self.history[coin] and self.history[coin][-1].window_start == ws:
            return
        ticks = self.window_ticks[coin]
        if not ticks:
            return

        prices = [t.price for t in ticks if t.source == "binance"]
        if not prices:
            prices = [t.price for t in ticks]
        if not prices:
            return

        open_p, close_p = prices[0], prices[-1]
        buy_v = sum(t.qty for t in ticks if t.side == "buy")
        sell_v = sum(t.qty for t in ticks if t.side == "sell")
        total_dv = sum(t.dollar_value for t in ticks)

        # Kill zone
        kz_start = ws + self.tf_sec - KILL_ZONE_SEC
        kz_ticks = [t for t in ticks if t.ts_ms // 1000 >= kz_start]
        kz_prices = [t.price for t in kz_ticks if t.source == "binance"] or [close_p]
        kz_entry = kz_prices[0]
        kz_buy = sum(t.qty for t in kz_ticks if t.side == "buy")
        kz_sell = sum(t.qty for t in kz_ticks if t.side == "sell")
        kz_dv = sum(t.dollar_value for t in kz_ticks)
        kz_chg = (close_p - kz_entry) / kz_entry * 100 if kz_entry else 0

        pre_dir = "UP" if kz_entry > open_p else ("DOWN" if kz_entry < open_p else "FLAT")
        final_dir = "UP" if close_p > open_p else ("DOWN" if close_p < open_p else "FLAT")
        kz_dir = "UP" if close_p > kz_entry else ("DOWN" if close_p < kz_entry else "FLAT")
        reversed_ = pre_dir != "FLAT" and final_dir != "FLAT" and pre_dir != final_dir

        # Polymarket prices
        py_s = self.poly_yes_price[coin][0][1] if self.poly_yes_price[coin] else 0.5
        py_e = self.poly_yes_price[coin][-1][1] if self.poly_yes_price[coin] else 0.5
        pn_s = self.poly_no_price[coin][0][1] if self.poly_no_price[coin] else 0.5
        pn_e = self.poly_no_price[coin][-1][1] if self.poly_no_price[coin] else 0.5

        obi_all = self.window_obi_samples[coin]
        avg_obi = statistics.mean(obi_all) if obi_all else 0.5
        # OBI during kill zone (last samples)
        kz_obi_count = max(1, int(len(obi_all) * KILL_ZONE_SEC / self.tf_sec))
        kz_obi = statistics.mean(obi_all[-kz_obi_count:]) if obi_all else 0.5

        stats = WindowStats(
            window_start=ws,
            open_price=open_p, close_price=close_p,
            high=max(prices), low=min(prices),
            total_volume=buy_v + sell_v,
            total_dollar_volume=total_dv,
            trade_count=len(ticks),
            buy_volume=buy_v, sell_volume=sell_v,
            cvd_at_close=self.cvd[coin],
            kz_price_change_pct=kz_chg,
            kz_volume=kz_buy + kz_sell,
            kz_dollar_volume=kz_dv,
            kz_trade_count=len(kz_ticks),
            kz_max_single_trade=max((t.qty for t in kz_ticks), default=0),
            kz_buy_volume=kz_buy, kz_sell_volume=kz_sell,
            kz_cvd=kz_buy - kz_sell,
            direction=final_dir, kz_direction=kz_dir, kz_reversed=reversed_,
            poly_yes_start=py_s, poly_yes_end=py_e,
            poly_no_start=pn_s, poly_no_end=pn_e,
            avg_obi=avg_obi, kz_avg_obi=kz_obi,
        )
        self.history[coin].append(stats)
        self._log_window(coin, stats)

        if reversed_:
            self.alerts.appendleft(Alert(
                int(time.time() * 1000), "REVERSAL",
                f"{coin} KILL ZONE REVERSAL: was {pre_dir} -> ended {final_dir} "
                f"(KZ: {kz_chg:+.4f}%, vol=${kz_dv:,.0f}, CVD={kz_buy-kz_sell:+.3f})",
                "CRITICAL",
            ))

        # Clear per-window polymarket price series
        self.poly_yes_price[coin].clear()
        self.poly_no_price[coin].clear()

        # Cleanup old second-buckets
        cutoff = int(time.time()) - 600
        for d in (self.buy_vol_sec[coin], self.sell_vol_sec[coin],
                  self.trades_sec[coin], self.dollar_vol_sec[coin],
                  self.poly_yes_vol_sec[coin], self.poly_no_vol_sec[coin]):
            for k in [k for k in d if k < cutoff]:
                del d[k]

    # ── Helpers ──

    def vol_per_sec(self, coin: str, sec: int) -> float:
        return self.buy_vol_sec[coin].get(sec, 0) + self.sell_vol_sec[coin].get(sec, 0)

    def get_price_spark(self, coin: str, n_sec: int = 60) -> list[float]:
        cutoff = int(time.time() * 1000) - n_sec * 1000
        return [t.price for t in self.ticks[coin]
                if t.ts_ms >= cutoff and t.source == "binance"]

    def get_cvd_spark(self, coin: str, n_sec: int = 60) -> list[float]:
        cutoff = int(time.time() * 1000) - n_sec * 1000
        return [v for ts, v in self.cvd_series[coin] if ts >= cutoff]

    def get_obi_spark(self, coin: str, n_sec: int = 60) -> list[float]:
        cutoff = int(time.time() * 1000) - n_sec * 1000
        return [v for ts, v in self.obi_series[coin] if ts >= cutoff]

    # ── Logging ──

    def _log_tick(self, coin: str, tick: Tick):
        key = f"ticks_{coin}"
        if key not in self._log_handles:
            p = self.log_dir / f"ticks_{coin}_{datetime.now().strftime('%Y%m%d')}.jsonl"
            self._log_handles[key] = open(p, "a", buffering=1)
        r = {
            "ts": tick.ts_ms, "src": tick.source, "p": tick.price,
            "q": tick.qty, "s": tick.side, "rem": round(self.sec_remaining(), 2),
            "kz": self.in_kill_zone(),
        }
        self._log_handles[key].write(json.dumps(r) + "\n")

    def _log_window(self, coin: str, ws: WindowStats):
        key = f"win_{coin}"
        if key not in self._log_handles:
            p = self.log_dir / f"windows_{coin}_{datetime.now().strftime('%Y%m%d')}.jsonl"
            self._log_handles[key] = open(p, "a", buffering=1)
        r = {
            "ws": ws.window_start,
            "open": ws.open_price, "close": ws.close_price,
            "high": ws.high, "low": ws.low,
            "vol": round(ws.total_volume, 4), "dvol": round(ws.total_dollar_volume, 2),
            "trades": ws.trade_count,
            "buy_vol": round(ws.buy_volume, 4), "sell_vol": round(ws.sell_volume, 4),
            "cvd": round(ws.cvd_at_close, 4),
            "dir": ws.direction,
            "kz_chg": round(ws.kz_price_change_pct, 5),
            "kz_vol": round(ws.kz_volume, 4), "kz_dvol": round(ws.kz_dollar_volume, 2),
            "kz_trades": ws.kz_trade_count,
            "kz_max": round(ws.kz_max_single_trade, 4),
            "kz_buy": round(ws.kz_buy_volume, 4), "kz_sell": round(ws.kz_sell_volume, 4),
            "kz_cvd": round(ws.kz_cvd, 4),
            "kz_dir": ws.kz_direction, "kz_rev": ws.kz_reversed,
            "py_s": ws.poly_yes_start, "py_e": ws.poly_yes_end,
            "pn_s": ws.poly_no_start, "pn_e": ws.poly_no_end,
            "obi": round(ws.avg_obi, 4), "kz_obi": round(ws.kz_avg_obi, 4),
        }
        self._log_handles[key].write(json.dumps(r) + "\n")

    def close(self):
        for h in self._log_handles.values():
            try:
                h.close()
            except Exception:
                pass


# ═══════════════════════════════════════════════════════════════════════
# WEBSOCKET FEEDS
# ═══════════════════════════════════════════════════════════════════════

async def feed_binance_trades(engine: Engine, coins: list[str]):
    """Binance individual trade stream — every single trade, sub-100ms."""
    streams = "/".join(f"{c.lower()}usdt@trade" for c in coins)
    url = f"{BINANCE_WS}/{streams}"
    backoff = 1.0
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, close_timeout=5) as ws:
                backoff = 1.0
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        d = msg.get("data", msg)
                        if d.get("e") != "trade":
                            continue
                        engine.process_tick(Tick(
                            source="binance", symbol=d["s"],
                            price=float(d["p"]), qty=float(d["q"]),
                            side="sell" if d.get("m") else "buy",
                            ts_ms=int(d["T"]),
                            trade_id=str(d.get("t", "")),
                        ))
                    except (KeyError, ValueError, TypeError):
                        continue
        except asyncio.CancelledError:
            return
        except Exception:
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


async def feed_binance_depth(engine: Engine, coins: list[str]):
    """Binance order book depth stream — updates every 100ms."""
    streams = "/".join(f"{c.lower()}usdt@depth20@100ms" for c in coins)
    url = f"{BINANCE_WS}/{streams}"
    backoff = 1.0
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, close_timeout=5) as ws:
                backoff = 1.0
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        d = msg.get("data", msg)
                        # depth20 gives top 20 bid/ask levels
                        bids = [BookLevel(float(p), float(q)) for p, q in d.get("bids", [])]
                        asks = [BookLevel(float(p), float(q)) for p, q in d.get("asks", [])]
                        # Extract symbol from stream name or guess from first coin
                        stream = msg.get("stream", "")
                        sym = stream.split("@")[0].upper() if stream else f"{coins[0]}USDT"
                        coin = sym.replace("USDT", "")
                        engine.process_book(BookSnapshot(
                            source="binance", symbol=sym,
                            bids=bids, asks=asks,
                            ts_ms=int(time.time() * 1000),
                        ))
                    except (KeyError, ValueError, TypeError):
                        continue
        except asyncio.CancelledError:
            return
        except Exception:
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


async def feed_alpaca(engine: Engine, coins: list[str], key: str, secret: str):
    """Alpaca crypto trade stream — cross-exchange comparison."""
    symbols = [f"{c}/USD" for c in coins]
    backoff = 1.0
    while True:
        try:
            async with websockets.connect(ALPACA_WS, ping_interval=20) as ws:
                backoff = 1.0
                await ws.recv()  # welcome
                await ws.send(json.dumps({"action": "auth", "key": key, "secret": secret}))
                resp = json.loads(await ws.recv())
                if isinstance(resp, list):
                    for r in resp:
                        if r.get("T") == "error":
                            code = r.get("code", 0)
                            msg = r.get("msg", "unknown")
                            engine.alerts.appendleft(Alert(
                                int(time.time() * 1000), "ALPACA",
                                f"Auth error ({code}): {msg}", "HIGH"))
                            if code == 406:
                                engine.alerts.appendleft(Alert(
                                    int(time.time() * 1000), "ALPACA",
                                    "Connection limit — close other Alpaca sessions", "WARN"))
                            return
                await ws.send(json.dumps({"action": "subscribe", "trades": symbols}))
                await ws.recv()  # sub confirmation
                async for raw in ws:
                    try:
                        msgs = json.loads(raw)
                        if not isinstance(msgs, list):
                            msgs = [msgs]
                        for m in msgs:
                            if m.get("T") != "t":
                                continue
                            ts_str = m.get("t", "")
                            try:
                                dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                                ts_ms = int(dt.timestamp() * 1000)
                            except Exception:
                                ts_ms = int(time.time() * 1000)
                            engine.process_tick(Tick(
                                source="alpaca",
                                symbol=m["S"].replace("/", ""),
                                price=float(m["p"]), qty=float(m["s"]),
                                side=m.get("tks", "unknown"),
                                ts_ms=ts_ms,
                                trade_id=str(m.get("i", "")),
                            ))
                    except (KeyError, ValueError, TypeError):
                        continue
        except asyncio.CancelledError:
            return
        except Exception:
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


async def feed_polymarket(engine: Engine, coins: list[str], timeframe: str):
    """Polymarket CLOB WebSocket — live orderbook + trades on prediction tokens."""

    async def discover_markets():
        """Find current active markets via Gamma API."""
        async with httpx.AsyncClient(timeout=10) as client:
            for coin in coins:
                series_id = GAMMA_SERIES.get(timeframe, {}).get(coin)
                if not series_id:
                    continue
                try:
                    now = datetime.now(timezone.utc)
                    # Lookahead: find markets ending in the next 10-30 min
                    lookahead = {"5m": 600, "15m": 1800, "1h": 7200}.get(timeframe, 1800)
                    end_min = now.strftime("%Y-%m-%dT%H:%M:%SZ")
                    end_max = datetime.fromtimestamp(
                        now.timestamp() + lookahead, tz=timezone.utc
                    ).strftime("%Y-%m-%dT%H:%M:%SZ")
                    resp = await client.get(f"{GAMMA_API}/events", params={
                        "series_id": series_id,
                        "closed": "false",
                        "end_date_min": end_min,
                        "end_date_max": end_max,
                        "limit": 3,
                        "order": "endDate",
                        "ascending": "true",
                    })
                    if resp.status_code != 200:
                        continue
                    events = resp.json()
                    for ev in events:
                        for mkt in ev.get("markets", []):
                            end_str = mkt.get("endDate", "")
                            try:
                                end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                            except Exception:
                                continue
                            if end_dt < now:
                                continue
                            tokens = []
                            cid = mkt.get("conditionId", "")

                            # Parse token IDs — can be JSON array or comma-separated
                            raw_tids = mkt.get("clobTokenIds", "")
                            try:
                                token_ids = json.loads(raw_tids) if raw_tids.startswith("[") else [t.strip().strip('"') for t in raw_tids.split(",")]
                            except Exception:
                                token_ids = [t.strip().strip('"') for t in raw_tids.split(",")]
                            token_ids = [t for t in token_ids if t]

                            # Parse outcomes
                            outcomes = mkt.get("outcomes", "")
                            try:
                                out_list = json.loads(outcomes) if isinstance(outcomes, str) else outcomes
                            except Exception:
                                out_list = ["Up", "Down"]

                            if len(out_list) == len(token_ids):
                                tokens = [PolyToken(token_id=tid, outcome=o)
                                          for tid, o in zip(token_ids, out_list)]
                            else:
                                for i, tid in enumerate(token_ids):
                                    tokens.append(PolyToken(token_id=tid, outcome="Up" if i == 0 else "Down"))

                            if tokens and cid:
                                engine.poly_markets[coin] = PolyMarket(
                                    condition_id=cid, coin=coin,
                                    timeframe=timeframe, end_date=end_dt,
                                    tokens=tokens,
                                )
                            break  # take first active market
                except Exception:
                    continue

    # Discover markets periodically
    while True:
        await discover_markets()
        # Collect all token IDs we need to subscribe to
        all_token_ids = []
        for coin in coins:
            mkt = engine.poly_markets.get(coin)
            if mkt:
                for tok in mkt.tokens:
                    all_token_ids.append(tok.token_id)

        if not all_token_ids:
            engine.alerts.appendleft(Alert(
                int(time.time() * 1000), "POLY_DISC",
                f"No active Polymarket markets found for {coins} {timeframe}", "WARN"))
            await asyncio.sleep(30)
            continue

        # Connect to CLOB WebSocket
        backoff = 1.0
        try:
            # CLOB WS requires assets in the initial connection message
            init_msg = {"assets_ids": all_token_ids, "type": "market"}
            url = POLYMARKET_CLOB_WS
            async with websockets.connect(url, ping_interval=10, close_timeout=5) as ws:
                backoff = 1.0
                await ws.send(json.dumps(init_msg))

                # Build token -> (coin, outcome) lookup
                token_map: dict[str, tuple[str, str]] = {}
                for coin_name in coins:
                    mkt = engine.poly_markets.get(coin_name)
                    if mkt:
                        for tok in mkt.tokens:
                            token_map[tok.token_id] = (coin_name, tok.outcome)

                reconnect_at = time.time() + 120  # reconnect every 2 min to refresh markets
                async for raw in ws:
                    if time.time() > reconnect_at:
                        break  # reconnect to discover new markets

                    try:
                        parsed = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    # CLOB WS sometimes sends lists of messages
                    msgs = parsed if isinstance(parsed, list) else [parsed]
                    for msg in msgs:
                        if not isinstance(msg, dict):
                            continue
                        _handle_clob_msg(msg, token_map, engine)

        except asyncio.CancelledError:
            return
        except Exception:
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


def _handle_clob_msg(msg: dict, token_map: dict, engine: Engine):
    """Process a single CLOB WebSocket message."""
    ev_type = msg.get("event_type", "")

    # Trade events
    if ev_type == "trade":
        for item in msg.get("data", [msg]):
            if not isinstance(item, dict):
                continue
            asset_id = item.get("asset_id", "")
            mapping = token_map.get(asset_id)
            if not mapping:
                continue
            c, outcome = mapping
            try:
                price = float(item.get("price", 0))
                size = float(item.get("size", 0))
                ts = int(item.get("timestamp", time.time()))
                if ts < 1e12:
                    ts *= 1000
                engine.process_poly_trade(c, outcome, price, size, ts)
                mkt = engine.poly_markets.get(c)
                if mkt:
                    for tok in mkt.tokens:
                        if tok.token_id == asset_id:
                            tok.price = price
            except (ValueError, TypeError):
                continue

    # Price change events (batched) — the MAIN data feed from Poly
    elif ev_type == "price_change":
        changes = msg.get("price_changes", msg.get("data", []))
        if isinstance(changes, dict):
            changes = [changes]
        for ch in changes:
            if not isinstance(ch, dict):
                continue
            asset_id = ch.get("asset_id", "")
            mapping = token_map.get(asset_id)
            if not mapping:
                continue
            c, outcome = mapping
            try:
                price = float(ch.get("price", 0))
                mkt = engine.poly_markets.get(c)
                if mkt:
                    for tok in mkt.tokens:
                        if tok.token_id == asset_id:
                            tok.price = price
                ts_ms = int(time.time() * 1000)
                if outcome == "Up":
                    engine.poly_yes_price[c].append((ts_ms, price))
                else:
                    engine.poly_no_price[c].append((ts_ms, price))
            except (ValueError, TypeError):
                continue

    # Last trade price — another way to get trade data
    elif ev_type == "last_trade_price":
        asset_id = msg.get("asset_id", "")
        mapping = token_map.get(asset_id)
        if mapping:
            c, outcome = mapping
            try:
                price = float(msg.get("price", 0))
                mkt = engine.poly_markets.get(c)
                if mkt:
                    for tok in mkt.tokens:
                        if tok.token_id == asset_id:
                            tok.price = price
                ts_ms = int(time.time() * 1000)
                if outcome == "Up":
                    engine.poly_yes_price[c].append((ts_ms, price))
                else:
                    engine.poly_no_price[c].append((ts_ms, price))
            except (ValueError, TypeError):
                pass

    # Book snapshots — CRITICAL for drain detection
    elif ev_type in ("book", "book_snapshot"):
        asset_id = msg.get("asset_id", "")
        mapping = token_map.get(asset_id)
        if not mapping:
            return
        c, outcome = mapping
        try:
            bids = [BookLevel(float(b["price"]), float(b["size"]))
                    for b in msg.get("bids", [])]
            asks = [BookLevel(float(a["price"]), float(a["size"]))
                    for a in msg.get("asks", [])]
            engine.process_book(BookSnapshot(
                source="polymarket",
                symbol=asset_id,
                bids=bids, asks=asks,
                ts_ms=int(time.time() * 1000),
            ))
        except (ValueError, TypeError, KeyError):
            pass


async def feed_poly_holders(engine: Engine, coins: list[str], poll_interval: int = 20):
    """Poll Polymarket Data API for top holders on active markets."""
    while True:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                for coin in coins:
                    mkt = engine.poly_markets.get(coin)
                    if not mkt:
                        continue
                    try:
                        resp = await client.get(f"{POLYMARKET_DATA_API}/holders", params={
                            "conditionId": mkt.condition_id,
                            "limit": 200,
                        })
                        if resp.status_code != 200:
                            _log(f"Holders API {coin} HTTP {resp.status_code}")
                            continue
                        data = resp.json()
                        if isinstance(data, dict):
                            if "data" in data:
                                data = data["data"]
                            elif "results" in data:
                                data = data["results"]
                            elif "holders" in data:
                                data = data["holders"]
                        if not isinstance(data, list):
                            _log(f"Holders API {coin} unexpected payload: {str(type(data))} keys={list(data.keys()) if isinstance(data, dict) else 'n/a'}")
                            continue
                        yes_holders = []
                        no_holders = []
                        yes_count = 0
                        no_count = 0
                        yes_amt = 0.0
                        no_amt = 0.0
                        up_tid = mkt.up_token.token_id if mkt.up_token else ""
                        down_tid = mkt.down_token.token_id if mkt.down_token else ""
                        for h in data:
                            name = h.get("pseudonym") or h.get("proxyWallet", "")[:10]
                            amount = float(h.get("amount", 0))
                            asset = h.get("asset", "")
                            if amount <= 0:
                                continue
                            entry = {"name": name, "amount": round(amount, 2), "wallet": h.get("proxyWallet", "")}
                            if asset == up_tid:
                                yes_holders.append(entry)
                                yes_count += 1
                                yes_amt += amount
                            elif asset == down_tid:
                                no_holders.append(entry)
                                no_count += 1
                                no_amt += amount
                        if yes_count + no_count == 0:
                            _log(f"Holders API {coin} returned 0 holders (cond={mkt.condition_id})")
                        yes_holders.sort(key=lambda x: x["amount"], reverse=True)
                        no_holders.sort(key=lambda x: x["amount"], reverse=True)
                        engine.holders[coin] = {"yes": yes_holders[:10], "no": no_holders[:10]}
                        engine.holders_totals[coin] = {
                            "yes_count": yes_count,
                            "no_count": no_count,
                            "yes_amt": round(yes_amt, 2),
                            "no_amt": round(no_amt, 2),
                        }
                        engine.holders_updated[coin] = time.time()
                        if yes_count or no_count:
                            _log(f"Holders API {coin} ok: yes={yes_count} no={no_count}")
                    except Exception as e:
                        _log(f"Holders API {coin} error: {e}")
                        continue

                    # Fallback: log conditionId if no holders yet
                    if not engine.holders.get(coin, {}).get("yes") and not engine.holders.get(coin, {}).get("no"):
                        _log(f"Holders API {coin} waiting (cond={mkt.condition_id})")

        except asyncio.CancelledError:
            return
        except Exception:
            pass
        await asyncio.sleep(poll_interval)


# ═══════════════════════════════════════════════════════════════════════
# TERMINAL DASHBOARD
# ═══════════════════════════════════════════════════════════════════════

def sparkline(vals: list[float], w: int = 50) -> str:
    if len(vals) < 2:
        return "[dim]waiting...[/]"
    blocks = " ▁▂▃▄▅▆▇█"
    mn, mx = min(vals), max(vals)
    rng = mx - mn or 1
    step = max(1, len(vals) // w)
    s = [vals[i] for i in range(0, len(vals), step)][:w]
    return "".join(blocks[min(8, int((v - mn) / rng * 8))] for v in s)


def signed_spark(vals: list[float], w: int = 40) -> str:
    """Sparkline centered on zero — green above, red below."""
    if len(vals) < 2:
        return "[dim]waiting...[/]"
    blocks_up = " ▁▂▃▄▅▆▇█"
    mx = max(abs(v) for v in vals) or 1
    step = max(1, len(vals) // w)
    s = [vals[i] for i in range(0, len(vals), step)][:w]
    chars = []
    for v in s:
        idx = min(8, int(abs(v) / mx * 8))
        c = "green" if v >= 0 else "red"
        chars.append(f"[{c}]{blocks_up[idx]}[/]")
    return "".join(chars)


def render(engine: Engine, console: Console) -> Layout:
    layout = Layout()
    rem = engine.sec_remaining()
    pct = engine.pct_elapsed()
    kz = engine.in_kill_zone()
    coin0 = engine.coins[0]

    # ── HEADER: Window timer ──
    ws_dt = datetime.fromtimestamp(engine.window_start(), tz=timezone.utc)
    pw = 50
    filled = int(pct / 100 * pw)
    kz_start = int((engine.tf_sec - KILL_ZONE_SEC) / engine.tf_sec * pw)
    bar = ""
    for i in range(pw):
        if i < filled:
            bar += f"[{'bold red' if i >= kz_start else 'green'}]█[/]"
        elif i >= kz_start:
            bar += "[red]░[/]"
        else:
            bar += "[dim]░[/]"

    kz_tag = "  [bold red blink]>>> KILL ZONE <<<[/]" if kz else ""
    header = Panel(Text.from_markup(
        f"  {engine.tf} Window: [bold]{ws_dt.strftime('%H:%M:%S')}[/] UTC  "
        f"|  {pct:.1f}%  |  [bold]{rem:.1f}s[/] remaining{kz_tag}\n  {bar}"
    ), title="[bold cyan]MARKET SURVEILLANCE[/]",
       border_style="bold red" if kz else "cyan")

    # ── PRICES + ORDER FLOW (per coin) ──
    price_panels = []
    for coin in engine.coins:
        bp = engine.latest_price.get(coin)
        ap = engine.alpaca_price.get(coin)
        if bp is None:
            price_panels.append(Panel("[dim]Waiting for data...[/]", title=f"[bold]{coin}[/]"))
            continue

        op = engine.window_open_price.get(coin, bp)
        chg = (bp - op) / op * 100 if op else 0
        chg_c = "green" if chg >= 0 else "red"
        d_str = "[bold green]UP[/]" if chg > 0.01 else ("[bold red]DOWN[/]" if chg < -0.01 else "[dim]FLAT[/]")

        # Cross-exchange
        xe_str = ""
        if ap and bp:
            div = (ap - bp) / bp * 10000  # in basis points
            xe_str = f"  |  Alpaca: ${ap:,.2f} ([{'green' if div>0 else 'red'}]{div:+.1f}bp[/])"

        # CVD
        cvd_val = engine.cvd[coin]
        cvd_c = "green" if cvd_val > 0 else "red"

        # OBI
        book = engine.binance_book.get(coin)
        obi_str = ""
        if book:
            obi = book.imbalance
            obi_c = "green" if obi > 0.55 else ("red" if obi < 0.45 else "yellow")
            bid_d = book.bid_depth
            ask_d = book.ask_depth
            obi_str = (f"  Book: [{obi_c}]OBI={obi:.2f}[/] "
                       f"(bids={bid_d:.2f} asks={ask_d:.2f} spread={book.spread:.2f})")

        # Sparklines
        p_spark = sparkline(engine.get_price_spark(coin, 60), 55)
        cvd_spark = signed_spark(engine.get_cvd_spark(coin, 60), 55)
        obi_spark = sparkline(engine.get_obi_spark(coin, 30), 30)

        # Tick rate
        ticks = engine.total_ticks[coin]
        uptime = max(1, time.time() - engine.start_time)

        # Volume last 10s
        now_s = int(time.time())
        bv10 = sum(engine.buy_vol_sec[coin].get(now_s - i, 0) for i in range(10))
        sv10 = sum(engine.sell_vol_sec[coin].get(now_s - i, 0) for i in range(10))
        vr = bv10 / sv10 if sv10 > 0 else 99
        vr_c = "green" if vr > 1.2 else ("red" if vr < 0.8 else "yellow")

        # Prediction
        pred = engine.prediction.get(coin, ("?", 0))
        pred_c = "green" if pred[0] == "UP" else ("red" if pred[0] == "DOWN" else "dim")

        # Large trades
        lt = engine.large_trades.get(coin, [])
        lt_str = f"  Whales: {len(lt)}" if lt else ""
        if lt:
            lt_buy = sum(t.qty for t in lt if t.side == "buy")
            lt_sell = sum(t.qty for t in lt if t.side == "sell")
            lt_str += f" (B={lt_buy:.2f} S={lt_sell:.2f})"

        # Polymarket
        poly_str = ""
        mkt = engine.poly_markets.get(coin)
        if mkt:
            ut = mkt.up_token
            dt_ = mkt.down_token
            if ut and dt_:
                poly_str = (f"\n  Poly: YES=[bold green]${ut.price:.2f}[/] "
                           f"NO=[bold red]${dt_.price:.2f}[/]  "
                           f"end={mkt.end_date.strftime('%H:%M:%S')}Z")

        # MM Status & Manipulation Sequence
        mm = engine.mm_status.get(coin, "UNKNOWN")
        mm_c = {"PRESENT": "green", "THINNING": "yellow", "FLED": "bold red", "UNKNOWN": "dim"}.get(mm, "dim")
        phase = engine.manip_phase.get(coin, "IDLE")
        phase_c = {"IDLE": "dim", "DRAIN_DETECTED": "yellow", "MM_FLEEING": "red",
                   "STRIKE_IMMINENT": "bold red blink"}.get(phase, "dim")
        pred_dir = engine.manip_predicted_dir.get(coin, "?")
        pred_dir_c = "green" if pred_dir == "UP" else ("red" if pred_dir == "DOWN" else "dim")

        # Poly depth info
        depth_str = ""
        depth_series = list(engine.poly_depth_series.get(coin, []))
        if depth_series:
            latest = depth_series[-1]
            _, bid_d, ask_d, spread, bb, ba = latest
            baseline = engine.poly_depth_baseline.get(coin, 0)
            depth_now = bid_d + ask_d
            depth_pct = depth_now / baseline * 100 if baseline > 0 else 100
            d_color = "green" if depth_pct > 80 else ("yellow" if depth_pct > 50 else "bold red")
            depth_str = (f"\n  Poly Book: depth=[{d_color}]{depth_pct:.0f}%[/] of baseline "
                        f"(bids={bid_d:.0f} asks={ask_d:.0f} spread={spread:.3f})")

        # Recent drain events
        drains = list(engine.drain_events.get(coin, []))[:3]
        drain_str = ""
        if drains:
            drain_parts = []
            for ts, pct_drop, side in drains:
                age = (time.time() * 1000 - ts) / 1000
                drain_parts.append(f"-{pct_drop:.0f}% {side} {age:.0f}s ago")
            drain_str = f"\n  [bold red]Drains: {' | '.join(drain_parts)}[/]"

        text = (
            f"  Binance: [bold]${bp:,.2f}[/]  [{chg_c}]{chg:+.4f}%[/]  {d_str}{xe_str}\n"
            f"  Price:   {p_spark}\n"
            f"  CVD:     [{cvd_c}]{cvd_val:+.3f}[/]  {cvd_spark}\n"
            f"  Vol 10s: buy={bv10:.3f} sell={sv10:.3f} [{vr_c}]ratio={vr:.2f}[/]{lt_str}\n"
            f" {obi_str}  OBI: {obi_spark}\n"
            f"  MM: [{mm_c}]{mm}[/]  |  Phase: [{phase_c}]{phase}[/]  "
            f"|  Strike: [{pred_dir_c}]{pred_dir}[/]  "
            f"|  Predict: [{pred_c}]{pred[0]}[/] {pred[1]:.0%}"
            f"  ({ticks:,} ticks {ticks/uptime:.0f}/s)"
            f"{poly_str}{depth_str}{drain_str}"
        )
        # Flashing border when strike imminent
        if phase == "STRIKE_IMMINENT":
            border = "bold red"
        elif phase in ("DRAIN_DETECTED", "MM_FLEEING"):
            border = "bold yellow"
        elif kz:
            border = "red"
        else:
            border = "green" if chg > 0 else "red"
        price_panels.append(Panel(Text.from_markup(text), title=f"[bold yellow]{coin}[/]",
                                  border_style=border))

    # ── VOLUME HEATMAP ──
    now_s = int(time.time())
    ws_start = engine.window_start()
    hm_chars = []
    blocks = " ░▒▓█"
    vols_60 = [engine.vol_per_sec(coin0, now_s - i) for i in range(59, -1, -1)]
    mx_v = max(vols_60) if vols_60 and max(vols_60) > 0 else 1
    for i, v in enumerate(vols_60):
        intensity = min(4, int(v / mx_v * 4))
        sec = now_s - 59 + i
        in_kz_sec = (sec - ws_start) >= (engine.tf_sec - KILL_ZONE_SEC)
        c = "red" if in_kz_sec else "green"
        hm_chars.append(f"[{c}]{blocks[intensity]}[/]")
    hm = "".join(hm_chars)

    # Buy/Sell bar per second (last 30s)
    bs_bars = []
    for i in range(29, -1, -1):
        sec = now_s - i
        bv = engine.buy_vol_sec[coin0].get(sec, 0)
        sv = engine.sell_vol_sec[coin0].get(sec, 0)
        total = bv + sv
        if total > 0:
            ratio = bv / total
            if ratio > 0.6:
                bs_bars.append("[green]▲[/]")
            elif ratio < 0.4:
                bs_bars.append("[red]▼[/]")
            else:
                bs_bars.append("[yellow]─[/]")
        else:
            bs_bars.append("[dim]·[/]")
    bs_str = "".join(bs_bars)

    heatmap = Panel(Text.from_markup(
        f"  Volume (60s):   {hm}\n"
        f"  Buy/Sell (30s): {bs_str}  [green]▲=buy[/] [red]▼=sell[/] [yellow]─=neutral[/]"
    ), title="[bold]VOLUME & FLOW[/]", border_style="blue")

    # ── ALERTS ──
    alert_lines = []
    for a in list(engine.alerts)[:12]:
        dt = datetime.fromtimestamp(a.ts_ms / 1000, tz=timezone.utc)
        ts = dt.strftime("%H:%M:%S.") + f"{a.ts_ms % 1000:03d}"
        sc = {"INFO": "dim", "WARN": "yellow", "HIGH": "red", "CRITICAL": "bold red"}.get(a.severity, "white")
        alert_lines.append(f" [{sc}][{a.severity:8s}][/] {ts} [{sc}]{a.message}[/]")
    alert_text = "\n".join(alert_lines) or " [dim]No alerts yet...[/]"
    alerts = Panel(Text.from_markup(alert_text), title="[bold red]ALERTS[/]", border_style="red")

    # ── KILL ZONE HISTORY ──
    ktable = Table(title=f"Kill Zone History ({coin0})", border_style="red", expand=True, padding=(0, 1))
    ktable.add_column("UTC", style="dim", width=5)
    ktable.add_column("Dir", justify="center", width=4)
    ktable.add_column("Chg%", justify="right", width=8)
    ktable.add_column("KZ Dir", justify="center", width=5)
    ktable.add_column("KZ Chg%", justify="right", width=9)
    ktable.add_column("KZ $Vol", justify="right", width=9)
    ktable.add_column("CVD", justify="right", width=7)
    ktable.add_column("KZ CVD", justify="right", width=7)
    ktable.add_column("OBI", justify="right", width=5)
    ktable.add_column("REV?", justify="center", width=5)

    for w in engine.history.get(coin0, [])[-15:]:
        dt = datetime.fromtimestamp(w.window_start, tz=timezone.utc)
        dc = "green" if w.direction == "UP" else ("red" if w.direction == "DOWN" else "dim")
        kdc = "green" if w.kz_direction == "UP" else ("red" if w.kz_direction == "DOWN" else "dim")
        overall_chg = (w.close_price - w.open_price) / w.open_price * 100 if w.open_price else 0
        rev = "[bold red blink]YES[/]" if w.kz_reversed else "[dim]no[/]"
        ktable.add_row(
            dt.strftime("%H:%M"),
            f"[{dc}]{w.direction}[/]",
            f"[{dc}]{overall_chg:+.4f}[/]",
            f"[{kdc}]{w.kz_direction}[/]",
            f"[{kdc}]{w.kz_price_change_pct:+.4f}[/]",
            f"${w.kz_dollar_volume:,.0f}",
            f"{w.cvd_at_close:+.2f}",
            f"{w.kz_cvd:+.2f}",
            f"{w.kz_avg_obi:.2f}",
            rev,
        )

    # ── PATTERN SUMMARY ──
    hist = engine.history.get(coin0, [])
    if len(hist) >= 3:
        revs = sum(1 for w in hist if w.kz_reversed)
        n = len(hist)
        avg_kz_dv = statistics.mean(w.kz_dollar_volume for w in hist)
        avg_kz_chg = statistics.mean(abs(w.kz_price_change_pct) for w in hist)
        max_kz_chg = max(abs(w.kz_price_change_pct) for w in hist)
        # Correlation: does CVD predict direction?
        correct_cvd = sum(1 for w in hist
                         if (w.cvd_at_close > 0 and w.direction == "UP") or
                            (w.cvd_at_close < 0 and w.direction == "DOWN"))
        # Does OBI predict?
        correct_obi = sum(1 for w in hist
                         if (w.avg_obi > 0.52 and w.direction == "UP") or
                            (w.avg_obi < 0.48 and w.direction == "DOWN"))

        pat = (
            f"  Windows: {n}  |  [bold red]Reversals: {revs} ({revs/n*100:.0f}%)[/]\n"
            f"  Avg KZ move: {avg_kz_chg:.4f}%  |  Max: {max_kz_chg:.4f}%  |  Avg KZ $vol: ${avg_kz_dv:,.0f}\n"
            f"  CVD predicts direction: {correct_cvd}/{n} ({correct_cvd/n*100:.0f}%)\n"
            f"  OBI predicts direction: {correct_obi}/{n} ({correct_obi/n*100:.0f}%)"
        )
    else:
        pat = f"  [dim]Need 3+ completed windows (have {len(hist)})...[/]"
    patterns = Panel(Text.from_markup(pat), title="[bold magenta]PATTERNS[/]", border_style="magenta")

    # ── ASSEMBLE ──
    n_coins = len(engine.coins)
    layout.split_column(
        Layout(header, name="header", size=4),
        Layout(name="prices", size=min(9 * n_coins, 27)),
        Layout(heatmap, name="heatmap", size=4),
        Layout(name="bottom"),
    )
    if len(price_panels) == 1:
        layout["prices"].update(price_panels[0])
    else:
        layout["prices"].split_column(*[Layout(p, size=9) for p in price_panels])

    layout["bottom"].split_row(
        Layout(alerts, name="alerts", ratio=1),
        Layout(name="right", ratio=1),
    )
    layout["right"].split_column(
        Layout(ktable, name="history", ratio=2),
        Layout(patterns, name="patterns", ratio=1),
    )
    return layout


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

async def main():
    p = argparse.ArgumentParser(description="Market Surveillance Tool")
    p.add_argument("--coins", default="BTC", help="Comma-separated coins (default: BTC)")
    p.add_argument("--timeframe", default="5m", choices=["5m", "15m", "1h"])
    p.add_argument("--log-dir", default="data/surveillance")
    p.add_argument("--no-ui", action="store_true", help="Headless logging mode")
    p.add_argument("--no-alpaca", action="store_true", help="Disable Alpaca feed")
    p.add_argument("--no-poly", action="store_true", help="Disable Polymarket feed")
    args = p.parse_args()

    coins = [c.strip().upper() for c in args.coins.split(",")]
    alpaca_key = os.environ.get("ALPACA_API_KEY", "")
    alpaca_secret = os.environ.get("ALPACA_API_SECRET", "")

    engine = Engine(coins=coins, timeframe=args.timeframe, log_dir=args.log_dir)
    console = Console()

    tasks: list[asyncio.Task] = []

    # Binance (always — free, no key needed)
    tasks.append(asyncio.create_task(feed_binance_trades(engine, coins)))
    tasks.append(asyncio.create_task(feed_binance_depth(engine, coins)))
    console.print("[bold green]Binance[/] trades + depth streams starting...")

    # Alpaca
    if not args.no_alpaca and alpaca_key and alpaca_secret:
        tasks.append(asyncio.create_task(feed_alpaca(engine, coins, alpaca_key, alpaca_secret)))
        console.print("[bold green]Alpaca[/]  crypto trades starting...")
    else:
        console.print("[dim]Alpaca disabled (no keys or --no-alpaca)[/]")

    # Polymarket
    if not args.no_poly:
        tasks.append(asyncio.create_task(feed_polymarket(engine, coins, args.timeframe)))
        console.print("[bold green]Polymarket[/] CLOB + Gamma starting...")
    else:
        console.print("[dim]Polymarket disabled (--no-poly)[/]")

    console.print(f"\n[bold cyan]Surveillance active:[/] {coins} {args.timeframe} "
                  f"kill_zone={KILL_ZONE_SEC}s  logs={args.log_dir}")
    console.print("[dim]Ctrl+C to stop[/]\n")

    await asyncio.sleep(2)

    if args.no_ui:
        try:
            while True:
                await asyncio.sleep(5)
                for c in coins:
                    bp = engine.latest_price.get(c, 0)
                    ap = engine.alpaca_price.get(c, 0)
                    cvd = engine.cvd.get(c, 0)
                    t = engine.total_ticks.get(c, 0)
                    kz = " KZ!" if engine.in_kill_zone() else ""
                    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
                          f"{c} Bin=${bp:,.2f} Alp=${ap:,.2f} CVD={cvd:+.3f} "
                          f"{t:,}ticks {engine.sec_remaining():.0f}s{kz}")
        except asyncio.CancelledError:
            pass
    else:
        try:
            with Live(console=console, refresh_per_second=2, screen=True) as live:
                while True:
                    live.update(render(engine, console))
                    await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            pass

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    engine.close()
    console.print(f"\n[bold]Stopped. Logs in {args.log_dir}/[/]")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")

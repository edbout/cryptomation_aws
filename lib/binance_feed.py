"""Binance Spot WebSocket feed — additional trigger source.

Parallels CoinbaseFeed in main.py but ALSO fires trading triggers (Coinbase is
passive). A trigger fires when, on close of a 1m kline:
  1. |binance_pct| (5m bar pct) > Config.ALIGNMENT_MIN_PCT
  2. The just-closed 1m candle volume is above the rolling average
     (above-average-volume gate — required for Binance triggers, per spec).
  3. The 5s per-symbol trigger throttle has elapsed.

The actual 2-of-3 alignment decision (Bybit/Binance/Coinbase) is centralised in
BybitManager.get_signal. BinanceFeed only does its own local volume+pct gate
before kicking validation; the final go/no-go is made there.

Stream: wss://stream.binance.com:9443/stream
Channels: <symbol>@kline_1m for each symbol in Config.BINANCE_SYMBOLS.
1m kline payload carries close price (k.c), volume (k.v) and a "is_closed" flag
(k.x), so we get one per-minute volume number on each bar close — exactly what
the above-average-volume gate needs, without per-tick aggregation.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from typing import Dict, Optional

import websockets

from config import Config
from lib.bybit_trackers import OrderBookTracker
from lib.helpers import get_current_5m_bar_ts

logger = logging.getLogger(__name__)


class _BinanceVolumeTracker:
    """Thin per-symbol 1m-volume tracker for Binance.

    Stores closed 1m bucket volumes in a bounded deque. Because Binance's
    kline_1m stream already delivers one volume number per closed minute,
    we don't need the per-tick aggregation that VolumeTracker (Bybit) has.
    """

    def __init__(self, max_history: int = 30):
        self.history: deque = deque(maxlen=max_history)  # list of {minute_start, volume}

    def append_closed(self, minute_start: int, volume: float) -> None:
        # Guard against duplicate appends of the same minute (Binance can emit
        # a closed kline twice if the WS message is replayed during reconnect).
        if self.history and self.history[-1]["minute_start"] == minute_start:
            self.history[-1]["volume"] = volume
            return
        self.history.append({"minute_start": minute_start, "volume": volume})

    def has_high_volume_prev_minute(
        self, symbol: str, multiplier: float = 1.25, lookback: int = 10
    ) -> bool:
        """True iff the most recently closed 1m candle volume exceeds
        mean(prev `lookback` candles) * `multiplier`.
        """
        h = list(self.history)
        if len(h) < lookback + 1:
            return False
        latest = h[-1]["volume"]
        baseline_pool = [c["volume"] for c in h[-(lookback + 1):-1]]
        if not baseline_pool:
            return False
        baseline = sum(baseline_pool) / len(baseline_pool)
        if baseline <= 0:
            return False
        high = latest > baseline * multiplier
        logger.debug(
            "📊 binance_vol | %s last=%.2f baseline=%.2f×%.2f=%.2f high=%s",
            symbol, latest, baseline, multiplier, baseline * multiplier, high,
        )
        return high

    def avg_volume(self, lookback: int = 10) -> Optional[float]:
        h = list(self.history)
        if len(h) < lookback:
            return None
        return sum(c["volume"] for c in h[-lookback:]) / lookback


class BinanceFeed:
    """Binance Spot WebSocket feed + trigger emitter.

    State mirrors CoinbaseFeed:
      - last_prices[sym]          → latest trade close from kline (or trade) stream
      - binance_5m_bases[sym]     → first price of the current 5m bar (UTC-aligned)
      - binance_5m_ts[sym]        → wall-clock ts of that 5m bar open
      - bars[sym]                 → 5m epoch bar_start

    Plus:
      - volume_trackers[sym]      → _BinanceVolumeTracker for the volume gate
      - _last_trigger_ts[sym]     → 5s throttle
    """

    # Map Binance Spot symbol → Bybit inverse symbol used as the canonical
    # `sym` argument everywhere downstream (execute_trading_validation, get_signal).
    _BINANCE_TO_BYBIT = {
        "BTCUSDT": "BTCUSD",
        "ETHUSDT": "ETHUSD",
        "XRPUSDT": "XRPUSD",
        "SOLUSDT": "SOLUSD",
    }

    WS_URL = "wss://stream.binance.com:9443/stream"

    def __init__(self):
        self.last_prices: Dict[str, float] = {s: 0.0 for s in Config.BINANCE_SYMBOLS}
        self.binance_5m_bases: Dict[str, float] = {s: 0.0 for s in Config.BINANCE_SYMBOLS}
        self.binance_5m_ts: Dict[str, float] = {s: 0.0 for s in Config.BINANCE_SYMBOLS}
        self.bars: Dict[str, Optional[int]] = {s: None for s in Config.BINANCE_SYMBOLS}
        self.global_last_snapshot: int = 0

        self.volume_trackers: Dict[str, _BinanceVolumeTracker] = {
            s: _BinanceVolumeTracker() for s in Config.BINANCE_SYMBOLS
        }
        self._last_trigger_ts: Dict[str, float] = {s: 0.0 for s in Config.BINANCE_SYMBOLS}

        # Binance perpetuals OBI trackers — keyed by Binance perp symbol
        # (BTCUSDT, etc). Populated by the fstream.binance.com partial-depth
        # listener and consumed by BybitManager.get_signal as a shadow
        # comparison against Bybit's perp OBI. Read via get_perp_obi() /
        # get_perp_obi_trend() so callers don't need to know about symbol
        # mapping or feature flags.
        self.obi_trackers: Dict[str, OrderBookTracker] = {
            s: OrderBookTracker(window=6) for s in Config.BINANCE_SYMBOLS
        }
        # Per-symbol wall-clock timestamp of the most recent partial-depth
        # update. Read by get_perp_obi() to enforce
        # Config.BINANCE_PERP_OBI_MAX_AGE — prevents trading on stale data
        # after a WS disconnect (the OrderBookTracker's deque retains values
        # indefinitely otherwise, so without this we'd silently keep returning
        # the last smoothed value).
        self._perp_ob_last_update: Dict[str, float] = {s: 0.0 for s in Config.BINANCE_SYMBOLS}

        # Late-bound — set via `attach_validator` so we avoid a circular import
        # against main.py at module load time.
        self._validator = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        self.running = False
        self.task: Optional[asyncio.Task] = None
        # Separate task for the perp OBI WS so a failure on either stream
        # cannot disrupt the other.
        self.perp_ob_task: Optional[asyncio.Task] = None

    # ── Wiring ───────────────────────────────────────────────────────────────

    def attach_validator(self, validator_coro_factory, loop: asyncio.AbstractEventLoop):
        """Inject the async trigger entry point.

        validator_coro_factory: callable(symbol_bybit_format) -> coroutine
            Typically `execute_trading_validation` from main.py.
        loop: the asyncio event loop on which the coroutine should run.
        """
        self._validator = validator_coro_factory
        self._loop = loop

    def start(self):
        if self.running or self.task:
            return
        if not Config.BINANCE_ENABLED:
            logger.info("✗ BinanceFeed | BINANCE_ENABLED=false, not starting")
            return
        self.running = True
        self.task = asyncio.create_task(self.listen_all())
        logger.info("✓ BinanceFeed | starting | symbols=%s", Config.BINANCE_SYMBOLS)

        # Shadow stream: Binance perpetuals partial-depth book → OBI trackers.
        # Logging-only consumer; failure here must not affect kline_1m above.
        if Config.BINANCE_PERP_OBI_ENABLED and not self.perp_ob_task:
            self.perp_ob_task = asyncio.create_task(self.listen_perp_orderbook())
            logger.info(
                "✓ BinanceFeed | starting perp OBI shadow stream | depth=%d",
                Config.BINANCE_PERP_OBI_DEPTH,
            )

    def stop(self):
        if self.task:
            self.task.cancel()
            self.task = None
        if self.perp_ob_task:
            self.perp_ob_task.cancel()
            self.perp_ob_task = None
        self.running = False

    # ── Bar / pct tracking ───────────────────────────────────────────────────

    def _update_5m_base(self, sym: str, price: float, now_ts: float) -> None:
        """Update last_price and (if a new 5m bar opened) the 5m base."""
        self.last_prices[sym] = price
        bar_start = get_current_5m_bar_ts(now_ts)
        prior_bar = self.bars.get(sym)

        if prior_bar == bar_start:
            return

        self.bars[sym] = bar_start

        if prior_bar is None:
            self.binance_5m_bases[sym] = price
            self.binance_5m_ts[sym] = now_ts
            logger.debug("📥 binance | first bar %s (bar %s)", sym, bar_start)
            return

        # Global snapshot once per bar
        if bar_start != self.global_last_snapshot:
            self.global_last_snapshot = bar_start
            for s in self.binance_5m_bases:
                base = self.binance_5m_bases[s]
                cur = self.last_prices[s]
                if base == 0.0:
                    logger.debug("⏳ binance | %s: %.4f | FIRST", s, cur)
                else:
                    change_pct = 100.0 * (cur - base) / base
                    direction = "🟢   UP" if change_pct > 0 else "🔴 DOWN" if change_pct < 0 else "⚪ FLAT"
                    logger.info(
                        "🔄 update_from_binance   | %s | %s | %+.3f%% | %10.4f | %s",
                        direction, s.rjust(9), change_pct, cur, bar_start,
                    )

        self.binance_5m_bases[sym] = price
        self.binance_5m_ts[sym] = now_ts

    def _current_pct(self, sym: str) -> float:
        cur = self.last_prices.get(sym, 0.0)
        base = self.binance_5m_bases.get(sym, 0.0)
        if cur > 0 and base > 0:
            return 100.0 * (cur - base) / base
        return 0.0

    # ── Trigger ──────────────────────────────────────────────────────────────

    def _maybe_trigger(self, sym: str, now_ts: float) -> None:
        """Decide whether to dispatch a trading validation for `sym`.

        Local gates (before deferring to BybitManager.get_signal):
          - |5m pct| > Config.ALIGNMENT_MIN_PCT
          - has_high_volume_prev_minute() — above-average volume gate
          - 5s per-symbol throttle
        """
        if not self._validator or not self._loop:
            return

        pct = self._current_pct(sym)
        if abs(pct) <= Config.ALIGNMENT_MIN_PCT:
            return

        high_vol = self.volume_trackers[sym].has_high_volume_prev_minute(
            sym, multiplier=Config.VOL_MULTIPLIER,
            lookback=Config.VOL_LOOKBACK,
        ) if Config.REQUIRE_VOL else True

        if not high_vol:
            logger.debug(
                "🚫 binance_trigger | %s pct=%+.3f%% but volume below avg ×%.2f",
                sym, pct, Config.VOL_MULTIPLIER,
            )
            return

        if now_ts - self._last_trigger_ts[sym] < Config.TRIGGER_THROTTLE_SEC:
            return
        self._last_trigger_ts[sym] = now_ts

        # Hand off to the Bybit-style symbol used elsewhere (e.g. BTCUSDT → BTCUSD)
        bybit_sym = self._BINANCE_TO_BYBIT.get(sym, sym)
        logger.info(
            "📊 BinanceFeed | %s | Triggering | pct=%+.3f%% | high_vol=ok | → get_signal(%s)",
            sym, pct, bybit_sym,
        )

        if self._loop.is_running():
            asyncio.run_coroutine_threadsafe(self._validator(bybit_sym), self._loop)

    # ── WebSocket message handling ───────────────────────────────────────────

    def _handle_kline(self, payload: dict) -> None:
        """Process one kline payload. payload is the inner `data` from the
        combined-stream envelope (`{"stream": "...", "data": {...}}`)."""
        try:
            sym = payload.get("s") or ""
            k = payload.get("k") or {}
            if sym not in Config.BINANCE_SYMBOLS or not k:
                return

            close_price = float(k.get("c") or 0.0)
            if close_price <= 0:
                return

            now_ts = time.time()
            self._update_5m_base(sym, close_price, now_ts)

            # Append closed 1m volume into our tracker; only on `x: true`.
            if bool(k.get("x")):
                vol = float(k.get("v") or 0.0)
                # k.t is the kline start time in ms
                minute_start = int(int(k.get("t") or 0) // 1000 // 60) * 60
                self.volume_trackers[sym].append_closed(minute_start, vol)
                # A closed 1m bucket is the natural moment to test the trigger:
                # we have an authoritative volume number AND a fresh close.
                self._maybe_trigger(sym, now_ts)

        except Exception as e:
            logger.debug("✗ BinanceFeed | _handle_kline error: %s", e)

    # ── WebSocket lifecycle ──────────────────────────────────────────────────

    def _build_streams_url(self) -> str:
        streams = "/".join(f"{s.lower()}@kline_1m" for s in Config.BINANCE_SYMBOLS)
        return f"{self.WS_URL}?streams={streams}"

    async def listen_all(self):
        url = self._build_streams_url()
        _retry_count = 0
        _disconnect_ts: Optional[float] = None
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=30) as ws:
                    if _disconnect_ts is not None:
                        down_secs = int(time.time() - _disconnect_ts)
                        logger.info("✓ BinanceFeed | reconnected after %ds down", down_secs)
                    _retry_count = 0
                    _disconnect_ts = None
                    logger.info(
                        "✓ BinanceFeed | connected | streams=%s",
                        [s.lower() + "@kline_1m" for s in Config.BINANCE_SYMBOLS],
                    )

                    async for msg in ws:
                        if not isinstance(msg, str):
                            continue
                        try:
                            envelope = json.loads(msg)
                        except json.JSONDecodeError:
                            continue
                        data = envelope.get("data") if isinstance(envelope, dict) else None
                        if not isinstance(data, dict):
                            continue
                        if data.get("e") != "kline":
                            continue
                        self._handle_kline(data)

            except (websockets.ConnectionClosed, ConnectionResetError) as e:
                if _disconnect_ts is None:
                    _disconnect_ts = time.time()
                _retry_count += 1
                wait = min(3 * (2 ** (_retry_count - 1)), 60)
                logger.warning(
                    "⚠️ BinanceFeed | disconnected (retry #%d in %ds): %s",
                    _retry_count, wait, e,
                )
                await asyncio.sleep(wait)
            except asyncio.CancelledError:
                logger.info("✓ BinanceFeed | listen_all cancelled")
                raise
            except Exception as e:
                if _disconnect_ts is None:
                    _disconnect_ts = time.time()
                _retry_count += 1
                wait = min(5 * (2 ** (_retry_count - 1)), 60)
                logger.exception(
                    "✗ BinanceFeed | top-level error (retry #%d in %ds): %r",
                    _retry_count, wait, e,
                )
                await asyncio.sleep(wait)

    # ── Helpers used by snapshots / outcome tracking ────────────────────────

    def log_volume_status(self, sym: str, lookback: int = 10):
        tracker = self.volume_trackers.get(sym)
        if not tracker or len(tracker.history) < 3:
            logger.info(
                "⏳ binance_vol | %s: building history (%d/%d)",
                sym, len(tracker.history) if tracker else 0, lookback,
            )
            return
        avg = tracker.avg_volume(lookback)
        if not avg:
            return
        latest = tracker.history[-1]["volume"]
        prev = tracker.history[-2]["volume"] if len(tracker.history) >= 2 else 0
        latest_x = latest / avg if avg else 0
        prev_x = prev / avg if avg else 0
        logger.debug(
            "📊 binance_vol_status | %s | Last:%.2f (%.2fx) | Prev:%.2f (%.2fx) | Avg:%.2f",
            sym.rjust(8), latest, latest_x, prev, prev_x, avg,
        )

    # ── Perpetuals OBI shadow stream (logging-only) ─────────────────────────
    #
    # Connects to fstream.binance.com partial-depth (depth5/10/20 @100ms) for
    # each BINANCE_SYMBOLS pair. Partial-depth streams deliver the *full
    # top-N snapshot* on every push, so we can sum bid/ask qty directly and
    # feed OrderBookTracker without maintaining a local diff-based book.
    #
    # Bybit perp inverse uses depth=50 with a snapshot+delta book; Binance
    # depth=20 partial is shallower in level count but typically deeper in
    # USD value per level, so this is still a "meaningful" book signal.
    # Threshold scaling for the shadow comparison happens in BybitManager,
    # not here — this listener just maintains raw normalized OBI per symbol.

    def _build_perp_streams_url(self) -> str:
        depth = Config.BINANCE_PERP_OBI_DEPTH
        streams = "/".join(
            f"{s.lower()}@depth{depth}@100ms" for s in Config.BINANCE_SYMBOLS
        )
        return f"{Config.BINANCE_PERP_WS_URL}?streams={streams}"

    def _handle_perp_depth(self, payload: dict) -> None:
        """Parse one partial-depth message; sum top-N bid/ask qty into the tracker.

        Partial-depth payload shape:
          {"e":"depthUpdate","s":"BTCUSDT","b":[[price,qty],...],"a":[[...]],...}
        Levels are strings — coerce to float defensively.
        """
        try:
            sym = payload.get("s") or ""
            if sym not in self.obi_trackers:
                return
            bids = payload.get("b") or []
            asks = payload.get("a") or []
            bid_qty = sum(float(level[1]) for level in bids if len(level) >= 2)
            ask_qty = sum(float(level[1]) for level in asks if len(level) >= 2)
            if bid_qty <= 0 and ask_qty <= 0:
                return
            self.obi_trackers[sym].update(bid_qty, ask_qty)
            self._perp_ob_last_update[sym] = time.time()
            logger.debug(
                "📊 _handle_perp_depth | %s bid=%.2f ask=%.2f obi=%+.4f",
                sym, bid_qty, ask_qty, self.obi_trackers[sym].get(),
            )
        except Exception as e:
            logger.debug("✗ BinanceFeed | _handle_perp_depth error: %s", e)

    async def listen_perp_orderbook(self):
        """Async WS task: maintain self.obi_trackers from Binance perps depth stream.

        Has its own reconnect/backoff loop, completely independent from
        listen_all (kline_1m). A failure here logs and retries; it must
        never interfere with the trigger-emitting kline stream.
        """
        url = self._build_perp_streams_url()
        _retry_count = 0
        _disconnect_ts: Optional[float] = None
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=30) as ws:
                    if _disconnect_ts is not None:
                        down_secs = int(time.time() - _disconnect_ts)
                        logger.info(
                            "✓ BinanceFeed | perp OBI reconnected after %ds down",
                            down_secs,
                        )
                    _retry_count = 0
                    _disconnect_ts = None
                    logger.info(
                        "✓ BinanceFeed | perp OBI connected | streams=%s",
                        [
                            f"{s.lower()}@depth{Config.BINANCE_PERP_OBI_DEPTH}@100ms"
                            for s in Config.BINANCE_SYMBOLS
                        ],
                    )

                    async for msg in ws:
                        if not isinstance(msg, str):
                            continue
                        try:
                            envelope = json.loads(msg)
                        except json.JSONDecodeError:
                            continue
                        data = envelope.get("data") if isinstance(envelope, dict) else None
                        if not isinstance(data, dict):
                            continue
                        self._handle_perp_depth(data)

            except (websockets.ConnectionClosed, ConnectionResetError) as e:
                if _disconnect_ts is None:
                    _disconnect_ts = time.time()
                _retry_count += 1
                wait = min(3 * (2 ** (_retry_count - 1)), 60)
                logger.warning(
                    "⚠️ BinanceFeed | perp OBI disconnected (retry #%d in %ds): %s",
                    _retry_count, wait, e,
                )
                await asyncio.sleep(wait)
            except asyncio.CancelledError:
                logger.info("✓ BinanceFeed | perp OBI listener cancelled")
                raise
            except Exception as e:
                if _disconnect_ts is None:
                    _disconnect_ts = time.time()
                _retry_count += 1
                wait = min(5 * (2 ** (_retry_count - 1)), 60)
                logger.exception(
                    "✗ BinanceFeed | perp OBI top-level error (retry #%d in %ds): %r",
                    _retry_count, wait, e,
                )
                await asyncio.sleep(wait)

    # ── Public accessors for BybitManager shadow comparison ─────────────────

    @classmethod
    def _bybit_to_binance(cls, bybit_sym: str) -> Optional[str]:
        """Inverse of _BINANCE_TO_BYBIT. Returns None if no mapping exists."""
        for binance_sym, by_sym in cls._BINANCE_TO_BYBIT.items():
            if by_sym == bybit_sym:
                return binance_sym
        return None

    def _resolve_fresh_tracker(self, bybit_sym: str) -> Optional[OrderBookTracker]:
        """Map `bybit_sym` → tracker, returning None if disabled, missing,
        or last update is older than Config.BINANCE_PERP_OBI_MAX_AGE.

        Centralises the freshness gate so get_perp_obi() and
        get_perp_obi_trend() never disagree about availability.
        """
        if not Config.BINANCE_PERP_OBI_ENABLED:
            return None
        binance_sym = self._bybit_to_binance(bybit_sym)
        if binance_sym is None:
            return None
        tracker = self.obi_trackers.get(binance_sym)
        if tracker is None:
            return None
        last = self._perp_ob_last_update.get(binance_sym, 0.0)
        if last <= 0.0:
            return None  # never received a sample
        if (time.time() - last) > Config.BINANCE_PERP_OBI_MAX_AGE:
            return None  # stale — WS likely disconnected
        return tracker

    def get_perp_obi(self, bybit_sym: str) -> Optional[float]:
        """Latest smoothed Binance perp OBI for `bybit_sym` (e.g. 'BTCUSD').

        Returns None when the perp stream is disabled, never produced a
        sample, or the latest sample is older than BINANCE_PERP_OBI_MAX_AGE.
        Callers should treat None as 'Binance OBI not available' and apply
        the dual-source veto policy accordingly.
        """
        tracker = self._resolve_fresh_tracker(bybit_sym)
        if tracker is None or len(tracker.history) == 0:
            return None
        return tracker.get()

    def get_perp_obi_trend(self, bybit_sym: str) -> Optional[float]:
        """Linear-regression slope of recent Binance perp OBI samples.

        Returns None until the tracker has accumulated >= 3 samples (matching
        OrderBookTracker.trend()'s own guard), or when the data is stale.
        """
        tracker = self._resolve_fresh_tracker(bybit_sym)
        if tracker is None or len(tracker.history) < 3:
            return None
        return tracker.trend()

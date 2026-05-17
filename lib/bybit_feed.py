"""Bybit market-data feed.

Parallels lib/binance_feed.py: owns the Bybit WebSocket connections, maintains
per-symbol tick / orderbook / candle / liquidation state, and emits trading
triggers when a 5m bar pct clears the dynamic threshold.

Two WebSocket connections:
  - inverse channel: ticker + orderbook (depth=50) per symbol
  - linear channel:  liquidation stream per asset (USDT-margined)

pybit runs its own daemon thread with auto-reconnect; callbacks cross into the
asyncio event loop via run_coroutine_threadsafe(self._validator(sym), loop).

Signal-generation logic (consensus voting across Bybit/Binance/Coinbase) lives
separately in lib/bybit_manager.py and reads state off this feed.

Dependency injection
--------------------
Several collaborators are constructed in main.py (rdb, finder, CLOB client,
price tracker, execute_trading_validation). Pull them in after construction:

  feed = BybitFeed(chainlink_feed=..., coinbase_feed=..., binance_feed=...)
  feed.attach_components(finder=..., client=..., price_tracker=...)
  feed.attach_validator(execute_trading_validation, loop)
  feed.start_websocket(loop)
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from collections import deque
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from pybit.unified_trading import WebSocket

from config import Config, RedisCache
from lib.bybit_trackers import OrderBookTracker, TickData, VolumeTracker
from lib.helpers import (
    get_current_5m_bar_ts,
    get_seconds_since_5m_start,
    get_utc_now,
    normalize_asset,
)
from lib.polymarket_mid_cache import POLY_MID_CACHE

logger = logging.getLogger(__name__)
UTC = ZoneInfo("UTC")


class BybitCandle5m:
    """Per-symbol 5-minute candle tracker + bar-close outcome resolver.

    `feed` is a back-reference to the BybitFeed that owns this candle, used by
    `_update_order_outcomes` to read the live coinbase/binance/chainlink feeds
    without referencing main.py module globals.
    """

    def __init__(self, feed: "BybitFeed"):
        self._feed = feed
        self.redis = RedisCache()
        self.candle: Optional[Dict] = None
        self.bar_start: Optional[int] = None
        self.volume_trackers: Dict[str, VolumeTracker] = {}

    def update_from_bybit(self, symbol: str, price: float, volume: float, ts: float) -> float:
        """Update 5m candle from price tick. Returns % change."""
        bar_start = get_current_5m_bar_ts(ts)

        if self.candle is None or self.candle["start_bar_ts"] != bar_start:
            if self.candle:
                old_change = self._pct_change()
                direction = "\U0001f7e2   UP" if old_change > 0 else "\U0001f534 DOWN" if old_change < 0 else "⚪ FLAT"
                outcome = 'up' if old_change > 0 else 'down'
                records = self._update_outcomes(symbol, outcome)
                self._update_raw_outcomes(symbol, outcome)
                self._update_order_outcomes(symbol, old_change, self.candle["start_bar_ts"])
                logger.info(
                    f"\U0001f504 update_from_bybit     | {direction} | {symbol:>9} | {old_change:+.3f}% | {price:10.4f} | {bar_start} | records updated: {records}"
                )

            self.candle = {
                "start_bar_ts": bar_start,
                "open": price,
                "close": price,
            }
            self._bar_start = bar_start
            return 0.0

        self.candle["close"] = price
        return self._pct_change()

    def _update_outcomes(self, symbol: str, outcome: str):
        """Update ALL history records (shorter cutoff for sparse signals)."""
        now = datetime.now(UTC)
        now_ts = now.timestamp()
        window_start = now_ts - 360

        history_key = f"prices:signals:{normalize_asset(symbol)}"
        total = self.redis.zcard(history_key)
        cur_members = self.redis.zrangebyscore(history_key, window_start, now_ts)
        logger.debug("\U0001f504 update_outcomes | %s FLIP → outcome=%s | total=%d cur=%d", symbol, outcome, total, len(cur_members))

        stale = self.redis.zrangebyscore(history_key, '-inf', window_start - 1)
        stale_na = [m for m in stale if m.endswith(':na')]
        if stale_na:
            self.redis.zrem(history_key, *stale_na)
            logger.info("\U0001f9f9 update_outcomes | %s: Removed %d stale :na records", symbol, len(stale_na))

        if not cur_members:
            logger.debug("⏳ update_outcomes | %s: No current records yet (need record_signal() signals)", symbol)
            return 0

        pipe = self.redis.pipeline()
        updated = 0

        for member in cur_members:
            if not member.endswith(':na'):
                logger.debug("⏭️ update_outcomes | %s: %s already updated (not 'na')", symbol, member)
                continue

            parts = member.rsplit(':', 1)
            base = parts[0]
            new_member = f"{base}:{outcome}"
            old_ts = self.redis.zscore(history_key, member) or 0

            pipe.zrem(history_key, member)
            pipe.zadd(history_key, {new_member: now.timestamp()})
            updated += 1

            logger.debug("✨ update_outcomes | %s: %s → %s (ts:%.0f→%.0f)",
                            symbol, member, new_member, old_ts, now.timestamp())
        if updated:
            pipe.execute()
            logger.debug("\U0001f504 update_outcomes | %s: Updated %d/%d records → outcome=%s",
                    symbol, updated, len(cur_members), outcome)

        return len(cur_members)

    def _update_raw_outcomes(self, symbol: str, outcome: str) -> int:
        """Resolve :na outcomes in prices:signals_raw:{asset} at bar close."""
        now = datetime.now(UTC)
        now_ts = now.timestamp()
        window_start = now_ts - 360

        key = f"prices:signals_raw:{normalize_asset(symbol)}"
        cur_members = self.redis.zrangebyscore(key, window_start, now_ts)

        stale_na = [m for m in self.redis.zrangebyscore(key, '-inf', window_start - 1) if m.endswith(':na')]
        if stale_na:
            self.redis.zrem(key, *stale_na)
            logger.info("\U0001f9f9 update_raw_outcomes | %s: removed %d stale :na", symbol, len(stale_na))

        if not cur_members:
            return 0

        pipe = self.redis.pipeline()
        updated = 0
        for member in cur_members:
            if not member.endswith(':na'):
                continue
            base = member[:-3]
            new_member = f"{base}:{outcome}"
            pipe.zrem(key, member)
            pipe.zadd(key, {new_member: now_ts})
            updated += 1

        if updated:
            pipe.execute()
            logger.debug("\U0001f504 update_raw_outcomes | %s: resolved %d → %s", symbol, updated, outcome)

        return updated

    def _update_order_outcomes(self, symbol: str, bar_pct: float, bar_start_ts: int):
        """Write Bybit/Binance/Coinbase/Chainlink consensus direction to orders placed during the closed bar."""
        asset = normalize_asset(symbol)
        pending_key = f"orders:pending_outcome:{asset}"
        order_ids = self.redis.zrangebyscore(pending_key, bar_start_ts, bar_start_ts)
        if not order_ids:
            return

        bybit_dir = 'UP' if bar_pct > 0 else 'DOWN'
        directions = [bybit_dir]
        coinbase_dir = ''
        chainlink_dir = ''
        binance_dir = ''
        cb_pct = None
        cl_pct = None
        bn_pct = None

        try:
            feed = self._feed
            if feed is not None:
                # Coinbase: BTCUSD → BTC-PERP
                cb_sym = symbol.replace('USD', '') + '-PERP'
                cb_cur  = feed.coinbase_feed.last_prices.get(cb_sym, 0.0)
                cb_base = feed.coinbase_feed.coinbase_5m_bases.get(cb_sym, 0.0)
                if cb_cur > 0 and cb_base > 0:
                    cb_pct = (cb_cur - cb_base) / cb_base * 100
                    coinbase_dir = 'UP' if cb_pct > 0 else 'DOWN'
                    directions.append(coinbase_dir)

                # Binance Spot: BTCUSD → BTCUSDT
                if Config.BINANCE_ENABLED and feed.binance_feed is not None:
                    bn_sym = symbol + 'T'
                    bn_cur  = feed.binance_feed.last_prices.get(bn_sym, 0.0)
                    bn_base = feed.binance_feed.binance_5m_bases.get(bn_sym, 0.0)
                    if bn_cur > 0 and bn_base > 0:
                        bn_pct = (bn_cur - bn_base) / bn_base * 100
                        binance_dir = 'UP' if bn_pct > 0 else 'DOWN'
                        directions.append(binance_dir)

                # Chainlink: BTCUSD → btc/usd
                cl_sym = symbol.replace('USD', '').lower() + '/usd'
                cl_cur  = feed.chainlink_feed.last_prices.get(cl_sym, 0.0)
                cl_base = feed.chainlink_feed.chainlink_5m_bases.get(cl_sym, 0.0)
                if cl_cur > 0 and cl_base > 0:
                    cl_pct = (cl_cur - cl_base) / cl_base * 100
                    chainlink_dir = 'UP' if cl_pct > 0 else 'DOWN'
                    directions.append(chainlink_dir)
        except Exception as e:
            logger.debug("_update_order_outcomes | feed access failed: %s", e)

        consensus = max(set(directions), key=directions.count)
        agree = f"{directions.count(consensus)}/{len(directions)}"

        now_ts = int(time.time())
        for order_id in order_ids:
            self.redis.hset(f"order:{order_id}", mapping={
                'bar_direction':  bybit_dir,
                'bar_pct':        round(bar_pct, 3),
                'bar_binance':    binance_dir,
                'bar_bin_pct':    '' if bn_pct is None else round(bn_pct, 3),
                'bar_coinbase':   coinbase_dir,
                'bar_cb_pct':     '' if cb_pct is None else round(cb_pct, 3),
                'bar_chainlink':  chainlink_dir,
                'bar_cl_pct':     '' if cl_pct is None else round(cl_pct, 3),
                'bar_consensus':  consensus,
                'bar_agree':      agree,
                'bar_updated_at': now_ts,
            })
            logger.info(
                "\U0001f4ca update_order_outcomes | %s | %s → %s (bybit) | BN:%s CB:%s CL:%s | consensus:%s %s",
                asset, order_id[:8], bybit_dir, binance_dir or '?', coinbase_dir or '?', chainlink_dir or '?', consensus, agree
            )

        self.redis.zrem(pending_key, *order_ids)

    def on_stream_tick(self, symbol: str, volume_delta: float, timestamp: float):
        if symbol not in self.volume_trackers:
            self.volume_trackers[symbol] = VolumeTracker()
        self.volume_trackers[symbol].update_stream_volume(symbol, volume_delta, timestamp)

    def is_high_volume_minute(self, symbol: str, multiplier: float = 1) -> bool:
        tracker = self.volume_trackers.get(symbol)
        return tracker.is_high_volume_minute(symbol, multiplier) if tracker else False

    def has_high_volume_prev_minute(self, symbol: str, multiplier: float = 1) -> bool:
        tracker = self.volume_trackers.get(symbol)
        if not tracker or not tracker.volume_history:
            return False
        return tracker.has_high_volume_prev_minute(symbol, multiplier)

    def get_avg_volume(self, symbol: str, lookback: int = 10) -> Optional[float]:
        """Get the average volume of the last N completed 1min candles."""
        tracker = self.volume_trackers.get(symbol)
        if not tracker:
            return None

        history = [c["volume"] for c in tracker.volume_history]
        if len(history) < lookback:
            return None

        return sum(history[-lookback:]) / lookback

    def _pct_change(self) -> float:
        if self.candle is None:
            return 0.0
        o = Decimal(str(self.candle["open"]))
        c = Decimal(str(self.candle["close"]))
        if o == 0:
            return 0.0
        return float((c - o) / o * 100)

    def log_volume_status(self, symbol: str, lookback: int = 10):
        """Log full volume status with color indicators."""
        tracker = self.volume_trackers.get(symbol)
        if not tracker or len(tracker.volume_history) < 3:
            logger.info(f"⏳ log_volume_status | {symbol}: Building volume history ({len(tracker.volume_history) if tracker else 0}/10)")
            return

        avg_vol = self.get_avg_volume(symbol, lookback)
        if not avg_vol:
            return

        latest_vol = tracker.volume_history[-1]["volume"]
        prev_vol = tracker.volume_history[-2]["volume"] if len(tracker.volume_history) >= 2 else 0
        latest_x = latest_vol / avg_vol if avg_vol else 0
        prev_x = prev_vol / avg_vol if avg_vol else 0

        def volume_color(x: float) -> str:
            if x > 1.01:
                return "\U0001f7e2"
            if x < 0.99:
                return "\U0001f534"
            return "\U0001f7e1"

        latest_color = volume_color(latest_x)
        prev_color = volume_color(prev_x)

        latest_vol_int = int(round(latest_vol))
        prev_vol_int = int(round(prev_vol))
        avg_vol_int = int(round(avg_vol))

        logger.debug(
            f"\U0001f4ca log_volume_status | {symbol:>8} | Last:{latest_vol_int:>13,} {latest_color} ({latest_x:>5.1f}x) | "
            f"Prev:{prev_vol_int:>13,} {prev_color} ({prev_x:>5.1f}x) | Avg:{avg_vol_int:>13,}"
        )


class BybitFeed:
    """Bybit WebSocket feed: ticker + orderbook + liquidations + 5m candle tracking.

    State exposed for the signal layer (BybitManager.get_signal reads these):
      - data[sym]                 → most recent TickData
      - obi_trackers[sym]         → OrderBookTracker
      - bybit_candles[sym]        → BybitCandle5m
      - _btc_momentum_direction   → "UP" / "DOWN" / None (BTC-lead-lag flag)
      - _btc_momentum_ts          → wall-clock ts of last BTC momentum update
      - _funding_rate[sym]        → latest funding rate from ticker stream
      - _liq_events[sym]          → deque of (ts, side, usd_value) liquidations

    Triggers `execute_trading_validation(sym)` on the asyncio loop when the
    5m bar pct clears the dynamic threshold (calibrated by epoch-bias and
    BTC lead-lag windows).
    """

    # Stable mapping built once at class definition time
    _ASSET_TO_BYBIT: Dict[str, str] = {normalize_asset(s): s for s in Config.BYBIT_SYMBOLS}

    def __init__(self, *, chainlink_feed: Any, coinbase_feed: Any, binance_feed: Any):
        self.data: Dict[str, TickData] = {}
        self.chainlink_feed = chainlink_feed
        self.coinbase_feed = coinbase_feed
        self.binance_feed = binance_feed
        self.bybit_candles: Dict[str, BybitCandle5m] = {
            sym: BybitCandle5m(feed=self) for sym in Config.BYBIT_SYMBOLS
        }
        self.thread: Optional[threading.Thread] = None
        self.running = False
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.obi_trackers: Dict[str, OrderBookTracker] = {
            sym: OrderBookTracker(window=6) for sym in Config.BYBIT_SYMBOLS
        }
        self._token_cache: Dict[str, Tuple[str, str]] = {}
        self._token_cache_ts: float = 0.0
        self._TOKEN_CACHE_TTL: float = 10 * 60

        # WebSocket state
        self._ws: Optional[WebSocket] = None
        self._liq_ws: Optional[WebSocket] = None
        self._ob_state: Dict[str, Dict] = {sym: {"b": {}, "a": {}} for sym in Config.BYBIT_SYMBOLS}
        self._last_ticker_ts: Dict[str, float] = {sym: 0.0 for sym in Config.BYBIT_SYMBOLS}
        self._last_trigger_ts: Dict[str, float] = {sym: 0.0 for sym in Config.BYBIT_SYMBOLS}
        self._funding_rate: Dict[str, float] = {sym: 0.0 for sym in Config.BYBIT_SYMBOLS}
        self._last_volume24h: Dict[str, float] = {}
        self._liq_events: Dict[str, deque] = {sym: deque() for sym in Config.BYBIT_SYMBOLS}
        self._btc_momentum_direction: Optional[str] = None
        self._btc_momentum_ts: float = 0.0

        # Injected late via attach_components / attach_validator (avoids circular import)
        self._finder: Optional[Any] = None
        self._client: Optional[Any] = None
        self._price_tracker: Optional[Any] = None
        self._validator: Optional[Any] = None

    # ── Wiring ───────────────────────────────────────────────────────────────────
    def attach_components(self, *, finder: Any, client: Any, price_tracker: Any) -> None:
        """Inject the Polymarket finder, CLOB client, and price tracker.

        Must be called before start_websocket. main.py owns these globals; the
        order of construction means they aren't available when BybitFeed() is
        created; this method binds them in.
        """
        self._finder = finder
        self._client = client
        self._price_tracker = price_tracker

    def attach_validator(self, validator_coro_factory: Any, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        """Inject the async trade-trigger entry point (typically execute_trading_validation).

        `loop` is optional here; start_websocket() also sets self.loop. Accepting it
        here matches BinanceFeed.attach_validator for symmetry.
        """
        self._validator = validator_coro_factory
        if loop is not None:
            self.loop = loop

    def _refresh_token_cache(self) -> None:
        """Populate/refresh asset→token_id cache from find_polymarket_targets.
        Called from the cache refresh thread. Silently skips on failure.
        """
        if self._finder is None:
            logger.debug("_refresh_token_cache | finder not attached yet, skipping")
            return
        try:
            markets, _ = self._finder.find_polymarket_targets(Config.ASSETS)
            new_cache: Dict[str, Tuple[str, str]] = {}
            for asset, market in markets.items():
                raw_tokens = market.get("clobTokenIds", "[]")
                token_list = json.loads(raw_tokens) if isinstance(raw_tokens, str) else raw_tokens
                if len(token_list) >= 2:
                    new_cache[asset] = (token_list[0], token_list[1])
            self._token_cache = new_cache
            self._token_cache_ts = time.time()
            logger.info(
                f"✓ _refresh_token_cache | {len(new_cache)} assets cached: {list(new_cache.keys())}"
            )
            all_ids = [tid for yes, no in new_cache.values() for tid in (yes, no)]
            POLY_MID_CACHE.subscribe(all_ids)
        except Exception as e:
            logger.warning(f"⚠️ _refresh_token_cache | Failed: {e}")

    def _refresh_active_tokens(self) -> None:
        """Tell PolymarketMidCache which YES/NO tokens are 'active' based on
        the current Bybit direction per asset. The cache polls active tokens
        at full cadence and inactive tokens at the slower watch cadence
        (Config.POLY_POLL_INTERVAL_WATCH).

        Mapping per asset:
          candle_5m_pct > 0   → YES is active (we'd buy YES on UP)
          candle_5m_pct < 0   → NO  is active
          candle_5m_pct == 0  → both active (conservative — flat / no signal)
          no tick data        → both active (cold start)

        Called from _on_ticker after the new tick has been written to
        self.data[sym]. Safe to call from the pybit WS daemon thread:
        set_active_tokens() does a single bare assignment which is GIL-atomic.
        """
        active: List[str] = []
        for sym in Config.BYBIT_SYMBOLS:
            asset_key = normalize_asset(sym)
            tokens = self._token_cache.get(asset_key)
            if tokens is None:
                continue
            yes_id, no_id = tokens
            tick = self.data.get(sym)
            if tick is None or tick.candle_5m_pct == 0:
                # No data or genuinely flat — keep both sides warm so a
                # direction flip doesn't blackout the 60s volatility window
                # for whichever side is suddenly active.
                active.append(yes_id)
                active.append(no_id)
                continue
            active.append(yes_id if tick.candle_5m_pct > 0 else no_id)
        POLY_MID_CACHE.set_active_tokens(active)

    # ── WebSocket callbacks ──────────────────────────────────────────────────────
    def _on_orderbook(self, msg: Dict) -> None:
        """Handle Bybit orderbook.5 WebSocket messages (snapshot + delta).

        Maintains a per-symbol local book in self._ob_state so OBI can be
        computed from accumulated depth rather than a single point-in-time poll.
        Snapshots replace state entirely; deltas update only changed levels
        (size="0" means remove, size>0 means add/update).
        OBI is recomputed and stored in obi_trackers after every message.
        """
        try:
            data = msg.get("data", {})
            sym = data.get("s", "")
            if sym not in Config.BYBIT_SYMBOLS:
                return

            msg_type = msg.get("type", "delta")
            raw_bids = data.get("b", [])
            raw_asks = data.get("a", [])

            ob = self._ob_state[sym]

            if msg_type == "snapshot":
                ob["b"] = {p: float(s) for p, s in raw_bids if float(s) > 0}
                ob["a"] = {p: float(s) for p, s in raw_asks if float(s) > 0}
            else:
                for p, s in raw_bids:
                    size = float(s)
                    if size == 0:
                        ob["b"].pop(p, None)
                    else:
                        ob["b"][p] = size
                for p, s in raw_asks:
                    size = float(s)
                    if size == 0:
                        ob["a"].pop(p, None)
                    else:
                        ob["a"][p] = size

            bid_qty = sum(ob["b"].values())
            ask_qty = sum(ob["a"].values())
            self.obi_trackers[sym].update(bid_qty, ask_qty)
            logger.debug(f"\U0001f4ca _on_orderbook | {sym} [{msg_type}] OBI update bid={bid_qty:.2f} ask={ask_qty:.2f}")

        except Exception as e:
            logger.debug(f"\U0001f4ca _on_orderbook | handler error: {e}")

    def _on_liquidation(self, msg: Dict) -> None:
        """Handle Bybit liquidation WebSocket messages.

        For inverse perpetuals 1 contract = 1 USD, so size is USD value directly.
        side="Buy"  → short was liquidated (short squeeze) → bullish pressure
        side="Sell" → long was liquidated (long wipeout)   → bearish pressure
        Events are stored in a deque and pruned in get_kelly_boost().
        """
        try:
            events = msg.get("data", [])
            if isinstance(events, dict):
                events = [events]
            ts = time.time()
            for event in events:
                linear_sym = event.get("symbol", "")
                sym = linear_sym[:-1] if linear_sym.endswith("USDT") else linear_sym
                if sym not in Config.BYBIT_SYMBOLS:
                    continue
                side = event.get("side", "")
                usd_val = float(event.get("size", 0))
                if usd_val <= 0:
                    continue
                self._liq_events[sym].append((ts, side, usd_val))
                logger.info(f"⚡ liquidation | {sym} {side} ${usd_val:,.0f} @ {event.get('price', '?')}")
        except Exception as e:
            logger.debug(f"⚡ _on_liquidation | handler error: {e}")

    def _on_ticker(self, msg: Dict) -> None:
        """Handle Bybit ticker WebSocket messages.

        Per message: updates the 5m candle, volume bucket, Polymarket fair-value
        (throttled by record_fairvalue's snap-to-mark logic), and TickData. Fires
        trading validation when pct_change clears the dynamic threshold.

        1s per-symbol throttle prevents flooding internal state at the 20-50 msg/s
        rate Bybit pushes for BTC/ETH. 5s trigger throttle caps validation calls.
        """
        try:
            data = msg.get("data", {})
            if not data:
                return

            sym = data.get("symbol", "")
            if sym not in Config.BYBIT_SYMBOLS:
                return

            price_str = data.get("lastPrice")
            if not price_str:
                return
            price = float(price_str)
            if price <= 0:
                return

            now_ts = time.time()

            if now_ts - self._last_ticker_ts[sym] < 1.0:
                return
            self._last_ticker_ts[sym] = now_ts

            volume24h = float(data.get("volume24h", 0) or 0)
            if sym not in self._last_volume24h:
                self._last_volume24h[sym] = volume24h
                volume = 0.0
            else:
                prev_vol24h = self._last_volume24h[sym]
                volume = max(volume24h - prev_vol24h, 0.0)
                self._last_volume24h[sym] = volume24h

            funding_str = data.get("fundingRate")
            if funding_str:
                try:
                    self._funding_rate[sym] = float(funding_str)
                except (ValueError, TypeError):
                    pass

            candle_tracker = self.bybit_candles.get(sym)
            if not candle_tracker:
                return

            candle_tracker.on_stream_tick(sym, volume, now_ts)
            pct_change = candle_tracker.update_from_bybit(sym, price, volume, now_ts)
            high_vol = candle_tracker.has_high_volume_prev_minute(sym, multiplier=1.25) if Config.REQUIRE_VOL else True

            asset_key = normalize_asset(sym)
            now = get_utc_now()
            candle_seconds = get_seconds_since_5m_start(now)

            SAMPLE_MARKS = {0, 30, 60, 90, 120, 150, 180, 210, 240, 270}
            near_mark = any(abs(candle_seconds - m) <= 7 for m in SAMPLE_MARKS)
            if near_mark and asset_key in self._token_cache and self._client is not None and self._price_tracker is not None:
                yes_token_id, _ = self._token_cache[asset_key]
                try:
                    price_resp = self._client.get_midpoint(yes_token_id)
                    if isinstance(price_resp, dict) and "mid" in price_resp:
                        poly_mid = float(price_resp["mid"])
                        if poly_mid > 0:
                            self._price_tracker.record_fairvalue(
                                asset_key, candle_seconds, poly_mid, pct_change, now_ts
                            )
                except Exception as poly_err:
                    err_str = str(poly_err)
                    if '404' in err_str or 'No orderbook' in err_str:
                        self._token_cache.pop(asset_key, None)
                        self._token_cache_ts = 0
                        logger.debug(f"\U0001f4ca _on_ticker | {sym} token expired, cache evicted")
                    else:
                        logger.warning(f"⚠️ _on_ticker | {sym} poly midpoint failed: {poly_err}")

            obi = self.obi_trackers[sym].get()
            self.data[sym] = TickData(
                last_price=price, candle_5m_pct=pct_change,
                high_volume=high_vol, order_book_imbalance=obi,
                candle_seconds=candle_seconds,
            )
            # Inform the Polymarket mid cache which token sides are currently
            # "active" so it can poll the inactive side at a slower cadence.
            # Cheap (O(num_assets) dict lookups) and idempotent — safe on every tick.
            self._refresh_active_tokens()

            btc_lag = (
                sym != "BTCUSD"
                and self._btc_momentum_direction is not None
                and (now_ts - self._btc_momentum_ts) < Config.BTC_LAG_TTL
                and ((pct_change > 0) == (self._btc_momentum_direction == "UP"))
            )
            in_epoch_bias = candle_seconds <= Config.EPOCH_BIAS_SECS
            threshold = Config.REDUCED_THRESHOLD_PCT if (in_epoch_bias or btc_lag) else 0.05

            if abs(pct_change) <= threshold:
                return

            if now_ts - self._last_trigger_ts[sym] < 5.0:
                return

            direction = "UP" if pct_change > 0 else "DOWN"
            _obi_thresh = self._effective_obi_thresh(sym, btc_lag, direction)
            obi_trend = self.obi_trackers[sym].trend()
            obi_contradicts = self._obi_contradicts(obi, obi_trend, _obi_thresh, pct_change)
            low_vol = not high_vol

            if obi_contradicts or (Config.REQUIRE_VOL and low_vol):
                logger.info(
                    f"⚠️ _on_ticker | {sym:>8} | Suppressed | {pct_change:+.2f}% | "
                    f"low_vol={low_vol} | obi={obi:+.3f} trend={obi_trend:+.4f} "
                    f"thresh={_obi_thresh:.2f} OBI_contra={obi_contradicts}"
                )
                return

            reason = "epoch_bias" if in_epoch_bias else ("btc_lag" if btc_lag else "normal")
            logger.info(
                f"\U0001f4ca _on_ticker | {sym:>8} | Triggering [{reason}] | {price:.2f} | "
                f"5m {pct_change:+.2f}% | OBI {obi:+.3f} | vol {high_vol}"
            )
            self._last_trigger_ts[sym] = now_ts

            if asset_key in self._token_cache:
                _yes_id, _ = self._token_cache[asset_key]
                _cached_mid = POLY_MID_CACHE.get(_yes_id)
                if _cached_mid is not None and (
                    _cached_mid >= Config.NEAR_RESOLVED_THRESHOLD
                    or _cached_mid <= 1.0 - Config.NEAR_RESOLVED_THRESHOLD
                ):
                    logger.debug(
                        "⏭️ _on_ticker | %s near-resolved (yes_mid=%.4f) — skip trigger",
                        sym, _cached_mid,
                    )
                    return

            if sym == "BTCUSD":
                self._btc_momentum_direction = "UP" if pct_change > 0 else "DOWN"
                self._btc_momentum_ts = now_ts
                logger.debug(f"⚡ btc_lag | BTC momentum {self._btc_momentum_direction} set, TTL {Config.BTC_LAG_TTL:.0f}s")

            if self.loop and self.loop.is_running() and self._validator is not None:
                asyncio.run_coroutine_threadsafe(
                    self._trigger_trading_validation(sym), self.loop
                )
            else:
                logger.warning(f"✗ _on_ticker | {sym} | No running event loop or validator for trigger")

        except Exception as e:
            logger.error(f"✗ _on_ticker | handler error: {e}")

    # ── OBI helpers (also called by BybitManager.get_signal via reference) ───────
    def _multi_asset_aligned(self, sym: str, direction: str) -> bool:
        """Returns True if at least one of the other major assets (BTC, ETH) has a
        5m candle confirming the same direction as sym.
        Used to relax OBI suppression during confirmed cross-asset moves.
        direction: 'UP' or 'DOWN'
        """
        is_up = (direction == "UP")
        for check_sym in ("BTCUSD", "ETHUSD"):
            if check_sym == sym:
                continue
            tick = self.data.get(check_sym)
            if tick and ((tick.candle_5m_pct > 0) == is_up):
                return True
        return False

    def _effective_obi_thresh(self, sym: str, btc_lag: bool, direction: str) -> float:
        """Compute the effective OBI veto threshold for sym, applying BTC-lag relaxation
        when btc_lag is active and cross-asset alignment is confirmed.
        """
        base = Config.OBI_THRESHOLDS.get(normalize_asset(sym), 0.15)
        if btc_lag and self._multi_asset_aligned(sym, direction):
            return base * Config.OBI_BTC_LAG_RELAX
        return base

    def _obi_contradicts(self, obi: float, obi_trend: float, thresh: float,
                         pct_change: float) -> bool:
        """Core OBI contradiction check shared between _on_ticker and get_signal.

        A signal is vetoed only when:
          1. OBI level strongly contradicts the price direction (calibrated thresh), AND
          2. OBI trend is NOT recovering toward balance (trend-aware veto).

        obi_trend > OBI_RECOVERY_RATE  → bids strengthening in a bull move  → pass
        obi_trend < -OBI_RECOVERY_RATE → asks strengthening in a bear move  → pass
        """
        level_contra = (
            (pct_change > 0 and obi < -thresh) or
            (pct_change < 0 and obi > thresh)
        )
        if not level_contra:
            return False
        recovering = (
            (pct_change > 0 and obi_trend >  Config.OBI_RECOVERY_RATE) or
            (pct_change < 0 and obi_trend < -Config.OBI_RECOVERY_RATE)
        )
        return not recovering

    def get_kelly_boost(self, asset: str, direction: str) -> float:
        """Compute a Kelly bankroll multiplier from funding rate and recent liquidations.

        asset: Polymarket/Config format e.g. "BTCUSDT"
        direction: "BUY" or "SELL"

        Boost logic:
          Funding threshold 0.03% per 8h (=0.0003) is the cutoff for meaningful crowding.
          - Squeeze scenario (crowd caught offside): +40% — violent, directional moves
          - Trend alignment (crowd winning):         +15% — confirms momentum
          - Liquidation ≥ $50k in signal direction in last 30s: +30%
          Capped at 2.0x to prevent oversizing.
        """
        sym = self._ASSET_TO_BYBIT.get(asset)
        if not sym:
            return 1.0

        boost = 1.0
        FUNDING_THRESHOLD = 0.0003

        funding = self._funding_rate.get(sym, 0.0)
        if abs(funding) >= FUNDING_THRESHOLD:
            squeeze = (direction == "BUY" and funding < 0) or (direction == "SELL" and funding > 0)
            if squeeze:
                boost += 0.40
                logger.debug(f"⚡ kelly_boost | {sym} squeeze funding={funding:+.5f} direction={direction} +0.40")
            else:
                boost += 0.15
                logger.debug(f"⚡ kelly_boost | {sym} align   funding={funding:+.5f} direction={direction} +0.15")

        now_ts = time.time()
        cutoff = now_ts - 30.0
        events = self._liq_events.get(sym, deque())
        while events and events[0][0] < cutoff:
            events.popleft()
        liq_side = "Buy" if direction == "BUY" else "Sell"
        liq_usd = sum(usd for ts, side, usd in events if side == liq_side)
        if liq_usd >= 50_000:
            boost += 0.30
            logger.debug(f"⚡ kelly_boost | {sym} liq_usd=${liq_usd:,.0f} direction={direction} +0.30")

        final = min(boost, 2.0)
        if final > 1.0:
            logger.info(f"⚡ kelly_boost | {sym} {direction} boost={final:.2f}x (funding={funding:+.5f} liq=${liq_usd:,.0f})")
        return final

    # ── WebSocket lifecycle ──────────────────────────────────────────────────────
    def start_websocket(self, loop: asyncio.AbstractEventLoop) -> None:
        """Start Bybit WebSocket feeds.

        Inverse channel: ticker + orderbook (depth=50) per symbol.
        Linear channel: liquidation stream per symbol (inverse channel doesn't support it).
        pybit runs its own daemon thread with auto-reconnect; callbacks cross into the
        asyncio event loop via run_coroutine_threadsafe.
        A lightweight _cache_refresh_loop thread keeps the Polymarket token cache fresh.
        """
        if not Config.BYBIT_ENABLED:
            logger.info("✗ ByBitFeed | BYBIT_ENABLED=false, not starting")
            return

        self.loop = loop
        self.running = True

        self._refresh_token_cache()

        self._ws = WebSocket(testnet=False, channel_type="inverse")

        for sym in Config.BYBIT_SYMBOLS:
            self._ws.ticker_stream(symbol=sym, callback=self._on_ticker)
            self._ws.orderbook_stream(depth=50, symbol=sym, callback=self._on_orderbook)

        self._liq_ws = WebSocket(testnet=False, channel_type="linear")
        for asset in Config.ASSETS:
            self._liq_ws.all_liquidation_stream(symbol=asset, callback=self._on_liquidation)

        self.thread = threading.Thread(target=self._cache_refresh_loop, daemon=True, name="bybit-cache")
        self.thread.start()

        logger.info(
            "✓ start_websocket | Bybit WebSocket started | %d symbols | streams: ticker + orderbook (inverse) + liquidation (linear)",
            len(Config.BYBIT_SYMBOLS),
        )

    def _cache_refresh_loop(self) -> None:
        """Keep the token cache fresh. WebSocket callbacks handle all market data."""
        while self.running:
            time.sleep(60)
            if not self.running:
                break
            now_ts = time.time()
            if now_ts - self._token_cache_ts > self._TOKEN_CACHE_TTL:
                self._refresh_token_cache()

    async def _trigger_trading_validation(self, symbol: str):
        """Async trigger — calls injected validator (execute_trading_validation)."""
        if self._validator is not None:
            await self._validator(symbol)

    def stop(self):
        """Clean shutdown."""
        self.running = False
        for ws, label in [(self._ws, "inverse"), (self._liq_ws, "linear-liq")]:
            if ws:
                try:
                    ws.exit()
                except Exception as e:
                    logger.debug(f"\U0001f4ca stop | {label} WS exit error (safe to ignore): {e}")
        self._ws = None
        self._liq_ws = None
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)

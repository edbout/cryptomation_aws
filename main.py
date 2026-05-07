#!/usr/bin/env python3
# Polymarket 5-Minute Momentum Trading Bot 

import asyncio
import websockets
import json
import logging
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Dict, Optional, List, Tuple, Any
from logging.handlers import TimedRotatingFileHandler
from decimal import Decimal
from collections import deque
from dataclasses import dataclass
from pybit.unified_trading import WebSocket
import threading
import time
import time as timemodule

UTC = ZoneInfo("UTC")

# Setup logging FIRST
def setup_logging() -> None:
    """Production logging setup with hourly log rotation."""
    logger = logging.getLogger()
    logger.handlers.clear()  # prevent double-logging
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    # Console handler (stdout)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Local dev file logging with hourly rotation
    from dotenv import load_dotenv
    load_dotenv()
    os.makedirs("./log", exist_ok=True)

        file_handler = TimedRotatingFileHandler(
            filename="./log/bot.log",
            when="midnight",      # Rotate at midnight (daily)
            interval=1,
            backupCount=7,        # Keep 7 days
            encoding="utf-8",
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

setup_logging()

logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("py_clob_client_v2.http_helpers.helpers").setLevel(logging.CRITICAL)

# Core imports (now safe to log)
from config import Config, RedisCache
from components import Components
from redeem import run_redeem_non_interactive
from lib.helpers import  get_utc_now, get_seconds_since_5m_start, get_current_5m_bar_ts, normalize_asset

@dataclass
class TickData:
    last_price: float
    candle_5m_pct: float
    high_volume: bool
    order_book_imbalance: float = 0.0
    candle_seconds: float = 0.0  # seconds elapsed since 5m epoch start

class OrderBookTracker:
    """Tracks rolling order book imbalance from Bybit top-of-book data.
    OBI = (bid_qty - ask_qty) / (bid_qty + ask_qty)
    Smoothed over last N poll samples (each 5s) to reduce noise.
    """
    def __init__(self, window: int = 6):  # 6 × 5s = 30s rolling window
        self.history: deque = deque(maxlen=window)

    def update(self, bid_qty: float, ask_qty: float) -> float:
        total = bid_qty + ask_qty
        if total == 0:
            return 0.0
        raw_obi = (bid_qty - ask_qty) / total
        self.history.append(raw_obi)
        return sum(self.history) / len(self.history)

    def get(self) -> float:
        if not self.history:
            return 0.0
        return sum(self.history) / len(self.history)

class VolumeTracker:
    def __init__(self, max_history: int = 20):
        self.volume_history = deque(maxlen=max_history)
        self.current_minute_start = None
        self._current_minute_volume = 0.0

    def update_stream_volume(self, symbol: str, volume_delta: float, timestamp: float):
        """Process stream tick. Aggregates into 1min buckets."""
        minute_start = int(timestamp // 60) * 60  # Floor to minute boundary
        if self.current_minute_start != minute_start:
            # Close previous minute, start new
            if self.current_minute_start is not None and self._current_minute_volume > 0:
                self.volume_history.append({
                    "symbol": symbol,
                    "minute_start": self.current_minute_start,
                    "volume": self._current_minute_volume
                })
            self.current_minute_start = minute_start
            self._current_minute_volume = 0.0

        self._current_minute_volume += volume_delta

    def _avg_volume_last_n(self, volumes: list, n: int = 10) -> Optional[float]:
        if len(volumes) < n:
            return None
        return sum(volumes[-n:]) / n

    def is_high_volume_minute(self, symbol: str, multiplier: float = 1.25, lookback: int = 10) -> bool:
        """Check if latest completed 1m candle is high volume vs previous lookback candles."""
        history = list(self.volume_history)
        if len(history) < lookback + 1:
            return False

        latest_volume = history[-1]["volume"]
        baseline = self._avg_volume_last_n([c["volume"] for c in history[:-1]], lookback)
        if baseline is None or baseline == 0:
            return False

        high_volume = latest_volume > (baseline * multiplier)
        logger.debug(
            "📊 is_high_volume_minute | %s 1M VOL: %.0f > %.0f*%.2f=%.0f? %s",
            symbol, latest_volume, baseline, multiplier, baseline * multiplier, high_volume
        )
        return high_volume

    def has_high_volume_prev_minute(self, symbol: str, multiplier: float = 1.25, lookback: int = 10) -> bool:
        """Check if previous completed 1m candle is high volume vs prior lookback candles."""
        history = list(self.volume_history)
        if len(history) < lookback + 2:
            return False

        prev_volume = history[-2]["volume"]
        baseline = self._avg_volume_last_n([c["volume"] for c in history[:-2]], lookback)
        if baseline is None or baseline == 0:
            return False

        high_volume = prev_volume > (baseline * multiplier)
        logger.debug(
            "📊 has_high_volume_prev_minute | %s PREV1M VOL: %.0f > %.0f*%.2f=%.0f? %s",
            symbol, prev_volume, baseline, multiplier, baseline * multiplier, high_volume
        )
        return high_volume

class BybitCandle5m:
    def __init__(self):
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
                direction = "🟢   UP" if old_change > 0 else "🔴 DOWN" if old_change < 0 else "⚪ FLAT"                
                outcome = 'up' if old_change > 0 else 'down'                
                records = self._update_outcomes(symbol, outcome)                
                logger.info(
                    f"🔄 update_from_bybit     | {direction} | {symbol:>9} | {old_change:+.3f}% | {price:10.4f} | {bar_start} | records updated: {records}"
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
        window_start = now_ts - 360  # 360s covers the full 5m epoch plus processing delay

        history_key = f"prices:signals:{normalize_asset(symbol)}"
        total = self.redis.zcard(history_key)
        cur_members = self.redis.zrangebyscore(history_key, window_start, now_ts)
        logger.debug("🔄 update_outcomes | %s FLIP → outcome=%s | total=%d cur=%d", symbol, outcome, total, len(cur_members))
        
        if not cur_members:
            logger.debug("⏳ update_outcomes | %s: No current records yet (need record_signal() signals)", symbol)
            return 0
        
        pipe = self.redis.pipeline()
        updated = 0
        
        for member in cur_members:
            if not member.endswith(':na'):
                logger.debug("⏭️ update_outcomes | %s: %s already updated (not 'na')", symbol, member)
                continue
            
            parts = member.rsplit(':', 1)  # Simpler: split off last ':na'
            base = parts[0]  # "260:0.964000:-0.0628:down"
            new_member = f"{base}:{outcome}"
            old_ts = self.redis.zscore(history_key, member) or 0
            
            pipe.zrem(history_key, member)
            pipe.zadd(history_key, {new_member: now.timestamp()})
            updated += 1
            
            logger.debug("✨ update_outcomes | %s: %s → %s (ts:%.0f→%.0f)", 
                            symbol, member, new_member, old_ts, now.timestamp())
        if updated:
            pipe.execute()
            logger.debug("🔄 update_outcomes | %s: Updated %d/%d records → outcome=%s", 
                    symbol, updated, len(cur_members), outcome)
            
        return len(cur_members)                

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

    def bar_start_utc(self) -> Optional[datetime]:
        if self._bar_start is None:
            return None
        return datetime.fromtimestamp(self._bar_start, tz=timezone.utc)
    
    def log_volume_status(self, symbol: str, lookback: int = 10):
        """Log full volume status with color indicators."""
        tracker = self.volume_trackers.get(symbol)
        if not tracker or len(tracker.volume_history) < 3:  # ↓ 11 → 3
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
                return "🟢"
            if x < 0.99:
                return "🔴"
            return "🟡"

        latest_color = volume_color(latest_x)
        prev_color = volume_color(prev_x)

        latest_vol_int = int(round(latest_vol))
        prev_vol_int = int(round(prev_vol)) 
        avg_vol_int = int(round(avg_vol))

        logger.debug(
            f"📊 log_volume_status | {symbol:>8} | Last:{latest_vol_int:>13,} {latest_color} ({latest_x:>5.1f}x) | "
            f"Prev:{prev_vol_int:>13,} {prev_color} ({prev_x:>5.1f}x) | Avg:{avg_vol_int:>13,}"
        )

class BybitManager:
    """Encapsulates all Bybit logic: WebSocket feeds, candle/volume tracking, signal triggers."""

    # Stable mapping built once at class definition time
    _ASSET_TO_BYBIT: Dict[str, str] = {normalize_asset(s): s for s in Config.BYBIT_SYMBOLS}

    def __init__(self):
        self.data: Dict[str, TickData] = {}  # Per-symbol ticks
        self.chainlink_feed = chainlink_feed
        self.coinbase_feed = coinbase_feed
        self.bybit_candles: Dict[str, BybitCandle5m] = {
            sym: BybitCandle5m() for sym in Config.BYBIT_SYMBOLS
        }
        self.thread: Optional[threading.Thread] = None
        self.running = False
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.obi_trackers: Dict[str, OrderBookTracker] = {
            sym: OrderBookTracker(window=6) for sym in Config.BYBIT_SYMBOLS
        }
        # Lightweight asset→(yes_token_id, no_token_id) cache, refreshed every 10 min.
        self._token_cache: Dict[str, Tuple[str, str]] = {}
        self._token_cache_ts: float = 0.0
        self._TOKEN_CACHE_TTL: float = 10 * 60

        # WebSocket state
        self._ws: Optional[WebSocket] = None
        self._liq_ws: Optional[WebSocket] = None  # separate linear WS for liquidations
        # Local order book for delta maintenance: sym → {"b": {price_str: qty}, "a": {...}}
        self._ob_state: Dict[str, Dict] = {sym: {"b": {}, "a": {}} for sym in Config.BYBIT_SYMBOLS}
        # Per-symbol throttle timestamps
        self._last_ticker_ts: Dict[str, float] = {sym: 0.0 for sym in Config.BYBIT_SYMBOLS}
        self._last_trigger_ts: Dict[str, float] = {sym: 0.0 for sym in Config.BYBIT_SYMBOLS}
        # Funding rate per symbol (updated from ticker stream)
        self._funding_rate: Dict[str, float] = {sym: 0.0 for sym in Config.BYBIT_SYMBOLS}
        # Last seen volume24h per symbol — used to compute tick-by-tick volume deltas
        self._last_volume24h: Dict[str, float] = {}
        # Liquidation events: sym → deque of (timestamp, side, usd_value)
        self._liq_events: Dict[str, deque] = {sym: deque() for sym in Config.BYBIT_SYMBOLS}
        # BTC momentum flag for cross-asset lead-lag
        self._btc_momentum_direction: Optional[str] = None  # "UP" or "DOWN"
        self._btc_momentum_ts: float = 0.0

    def _refresh_token_cache(self) -> None:
        """Populate/refresh asset→token_id cache from find_polymarket_targets.
        Called from the cache refresh thread. Silently skips on failure.
        """
        try:
            markets, _ = finder.find_polymarket_targets(Config.ASSETS)
            new_cache: Dict[str, Tuple[str, str]] = {}
            for asset, market in markets.items():
                raw_tokens = market.get("clobTokenIds", "[]")
                token_list = json.loads(raw_tokens) if isinstance(raw_tokens, str) else raw_tokens
                if len(token_list) >= 2:
                    new_cache[asset] = (token_list[0], token_list[1])  # (yes, no)
            self._token_cache = new_cache
            self._token_cache_ts = timemodule.time()
            logger.debug(
                f"✓ _refresh_token_cache | {len(new_cache)} assets cached: {list(new_cache.keys())}"
            )
        except Exception as e:
            logger.warning(f"⚠️ _refresh_token_cache | Failed: {e}")

    # ── WebSocket callbacks ───────────────────────────────────────────────────

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
            else:  # delta
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
            logger.debug(f"📊 _on_orderbook | {sym} [{msg_type}] OBI update bid={bid_qty:.2f} ask={ask_qty:.2f}")

        except Exception as e:
            logger.debug(f"📊 _on_orderbook | handler error: {e}")

    def _on_liquidation(self, msg: Dict) -> None:
        """Handle Bybit liquidation WebSocket messages.

        For inverse perpetuals 1 contract = 1 USD, so size is USD value directly.
        side="Buy"  → short was liquidated (short squeeze) → bullish pressure
        side="Sell" → long was liquidated (long wipeout)   → bearish pressure
        Events are stored in a deque and pruned in get_kelly_boost().
        """
        try:
            # allLiquidation delivers data as a list of events
            events = msg.get("data", [])
            if isinstance(events, dict):
                events = [events]
            ts = timemodule.time()
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

            now_ts = timemodule.time()

            # 1-second throttle: skip candle/volume update if too recent
            if now_ts - self._last_ticker_ts[sym] < 1.0:
                return
            self._last_ticker_ts[sym] = now_ts

            volume24h = float(data.get("volume24h", 0) or 0)
            if sym not in self._last_volume24h:
                # First tick: initialise without contributing volume to avoid a huge spurious spike
                self._last_volume24h[sym] = volume24h
                volume = 0.0
            else:
                prev_vol24h = self._last_volume24h[sym]
                volume = max(volume24h - prev_vol24h, 0.0)  # negative = 24h window rolled, treat as 0
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

            # Polymarket fair-value recording — record_fairvalue's snap-to-mark
            # logic (±7s of 30s marks) acts as its own throttle; safe to call every tick.
            asset_key = normalize_asset(sym)
            now = get_utc_now()
            candle_seconds = get_seconds_since_5m_start(now)

            if asset_key in self._token_cache:
                yes_token_id, _ = self._token_cache[asset_key]
                try:
                    price_resp = client.get_midpoint(yes_token_id)
                    if isinstance(price_resp, dict) and "mid" in price_resp:
                        poly_mid = float(price_resp["mid"])
                        if poly_mid > 0:
                            price_tracker.record_fairvalue(
                                asset_key, candle_seconds, poly_mid, pct_change, now_ts
                            )
                except Exception as poly_err:
                    logger.debug(f"📊 _on_ticker | {sym} poly midpoint failed: {poly_err}")

            # Read OBI from tracker (updated by _on_orderbook independently)
            obi = self.obi_trackers[sym].get()
            self.data[sym] = TickData(
                last_price=price, candle_5m_pct=pct_change,
                high_volume=high_vol, order_book_imbalance=obi,
                candle_seconds=candle_seconds,
            )

            # Dynamic trigger threshold:
            #   epoch bias  — market makers lag in seconds 0-30 of each 5m epoch
            #   BTC lead-lag — BTC fired recently; other assets likely not yet repriced
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

            # 5-second trigger throttle — preserves current trading cadence
            if now_ts - self._last_trigger_ts[sym] < 5.0:
                return

            obi_contradicts = (
                (pct_change > 0 and obi < -0.15) or
                (pct_change < 0 and obi > 0.15)
            )
            low_vol = not high_vol

            if obi_contradicts or (Config.REQUIRE_VOL and low_vol):
                logger.debug(
                    f"⚠️ _on_ticker | {sym:>8} | Skipping | {pct_change:+.2f}% | "
                    f"low_vol={low_vol} | {obi:+.3f} OBI_contra={obi_contradicts}"
                )
                return

            reason = "epoch_bias" if in_epoch_bias else ("btc_lag" if btc_lag else "normal")
            logger.debug(
                f"📊 _on_ticker | {sym:>8} | Triggering [{reason}] | {price:.2f} | "
                f"5m {pct_change:+.2f}% | OBI {obi:+.3f} | vol {high_vol}"
            )
            self._last_trigger_ts[sym] = now_ts

            # Set BTC momentum flag so lagging assets get a 60s window
            if sym == "BTCUSD":
                self._btc_momentum_direction = "UP" if pct_change > 0 else "DOWN"
                self._btc_momentum_ts = now_ts
                logger.debug(f"⚡ btc_lag | BTC momentum {self._btc_momentum_direction} set, TTL {Config.BTC_LAG_TTL:.0f}s")

            if self.loop and self.loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    self._trigger_trading_validation(sym), self.loop
                )
            else:
                logger.warning(f"✗ _on_ticker | {sym} | No running event loop for trigger")

        except Exception as e:
            logger.error(f"✗ _on_ticker | handler error: {e}")

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
        FUNDING_THRESHOLD = 0.0003  # 0.03% per 8h

        funding = self._funding_rate.get(sym, 0.0)
        if abs(funding) >= FUNDING_THRESHOLD:
            squeeze = (direction == "BUY" and funding < 0) or (direction == "SELL" and funding > 0)
            if squeeze:
                boost += 0.40
                logger.debug(f"⚡ kelly_boost | {sym} squeeze funding={funding:+.5f} direction={direction} +0.40")
            else:
                boost += 0.15
                logger.debug(f"⚡ kelly_boost | {sym} align   funding={funding:+.5f} direction={direction} +0.15")

        now_ts = timemodule.time()
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

    # ── WebSocket lifecycle ───────────────────────────────────────────────────

    def start_websocket(self, loop: asyncio.AbstractEventLoop) -> None:
        """Start Bybit WebSocket feeds.

        Inverse channel: ticker + orderbook (depth=50) per symbol.
        Linear channel: liquidation stream per symbol (inverse channel doesn't support it).
        pybit runs its own daemon thread with auto-reconnect; callbacks cross into the
        asyncio event loop via run_coroutine_threadsafe.
        A lightweight _cache_refresh_loop thread keeps the Polymarket token cache fresh.
        """
        self.loop = loop
        self.running = True

        # Eagerly populate token cache so fair-value recording works immediately
        self._refresh_token_cache()

        self._ws = WebSocket(testnet=False, channel_type="inverse")

        for sym in Config.BYBIT_SYMBOLS:
            self._ws.ticker_stream(symbol=sym, callback=self._on_ticker)
            self._ws.orderbook_stream(depth=50, symbol=sym, callback=self._on_orderbook)

        # Liquidation stream requires the linear channel (USDT-margined symbols).
        # _on_liquidation maps BTCUSDT → BTCUSD before storing in _liq_events.
        self._liq_ws = WebSocket(testnet=False, channel_type="linear")
        for asset in Config.ASSETS:  # e.g. "BTCUSDT"
            self._liq_ws.all_liquidation_stream(symbol=asset, callback=self._on_liquidation)

        # Lightweight thread just for periodic token cache refresh
        self.thread = threading.Thread(target=self._cache_refresh_loop, daemon=True, name="bybit-cache")
        self.thread.start()

        logger.info(
            "✓ start_websocket | Bybit WebSocket started | %d symbols | streams: ticker + orderbook (inverse) + liquidation (linear)",
            len(Config.BYBIT_SYMBOLS),
        )

    def _cache_refresh_loop(self) -> None:
        """Keep the token cache fresh. WebSocket callbacks handle all market data."""
        while self.running:
            timemodule.sleep(60)
            if not self.running:
                break
            now_ts = timemodule.time()
            if now_ts - self._token_cache_ts > self._TOKEN_CACHE_TTL:
                self._refresh_token_cache()

    async def _trigger_trading_validation(self, symbol: str):
        """Async trigger - calls execute trading validation."""
        await execute_trading_validation(symbol) 

    def stop(self):
        """Clean shutdown."""
        self.running = False
        for ws, label in [(self._ws, "inverse"), (self._liq_ws, "linear-liq")]:
            if ws:
                try:
                    ws.exit()
                except Exception as e:
                    logger.debug(f"📊 stop | {label} WS exit error (safe to ignore): {e}")
        self._ws = None
        self._liq_ws = None
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)

    def get_signal(self, sym: str) -> Optional[Tuple[str, str, float, float]]:
        tick = self.data.get(sym)
        logger.debug(f"📊 get_signal | {sym} tick data: {tick}")
        if not tick or tick.candle_5m_pct is None:
            return None

        bybit_5m_pct = tick.candle_5m_pct
        high_vol = tick.high_volume if Config.REQUIRE_VOL else True
        open_price = tick.last_price

        symbol_map = {
            "BTCUSD":   ("btc/usd",   "BTC-PERP"),
            "ETHUSD":   ("eth/usd",   "ETH-PERP"),
            "XRPUSD":   ("xrp/usd",   "XRP-PERP"),
            "SOLUSD":   ("sol/usd",   "SOL-PERP"),
            "DOGEUSD":  ("doge/usd",  "DOGE-PERP")
        }
        chainlink_sym, coinbase_sym = symbol_map.get(sym, (None, None))

        if sym in symbol_map:
           
            chainlink_current = self.chainlink_feed.last_prices.get(chainlink_sym, 0.0)
            chainlink_base_5m = self.chainlink_feed.chainlink_5m_bases.get(chainlink_sym, 0.0)

            if chainlink_current > 0 and chainlink_base_5m > 0:
                chainlink_pct = 100.0 * (chainlink_current - chainlink_base_5m) / chainlink_base_5m
            else:
                chainlink_pct = 0.0

            # Chainlink oracle only fires on 0.5%+ deviation — stale when move is small
            now_ts_sig = timemodule.time()
            chainlink_age = now_ts_sig - self.chainlink_feed.chainlink_last_update_ts.get(chainlink_sym, 0.0)
            chainlink_fresh = chainlink_age < 30.0

            coinbase_current = self.coinbase_feed.last_prices.get(coinbase_sym, 0.0)
            coinbase_base_5m = self.coinbase_feed.coinbase_5m_bases.get(coinbase_sym, 0.0)

            if coinbase_current > 0 and coinbase_base_5m > 0:
                coinbase_pct = 100.0 * (coinbase_current - coinbase_base_5m) / coinbase_base_5m
            else:
                coinbase_pct = 0.0

            # When Chainlink is fresh: enforce magnitude, direction, and divergence
            # When stale: exclude entirely — stale oracle data from a different market
            # regime actively corrupts the direction vote; Bybit+Coinbase is sufficient
            if chainlink_fresh:
                chainlink_strong = abs(chainlink_pct) > 0.03
                use_chainlink_direction = True
            else:
                logger.debug(f"⚠️ get_signal | {sym} Chainlink stale ({chainlink_age:.0f}s) — excluded from alignment")
                chainlink_strong = True          # stale — don't gate on it
                use_chainlink_direction = False  # stale — don't include in direction vote

            strong_enough = (
                abs(bybit_5m_pct) > 0.03
                and abs(coinbase_pct) > 0.03
                and chainlink_strong
            )

            if use_chainlink_direction:
                same_direction = (bybit_5m_pct > 0) == (chainlink_pct > 0) == (coinbase_pct > 0)
            else:
                same_direction = (bybit_5m_pct > 0) == (coinbase_pct > 0)

            max_div = max(0.12, abs(bybit_5m_pct) * 0.6)  # relative ±60%, min 0.12%
            chainlink_div_ok = (not chainlink_fresh) or (abs(bybit_5m_pct - chainlink_pct) <= max_div)
            not_too_far = (
                chainlink_div_ok
                and abs(bybit_5m_pct - coinbase_pct) <= max_div
            )

            aligned = strong_enough and same_direction and not_too_far
            if not aligned:
                logger.debug(
                        f"🚫 get_signal | {sym:>8} | Bybit: {bybit_5m_pct:+.2f}% | "
                        f"Coinbase: {coinbase_pct:+.2f}% | Chainlink: {chainlink_pct:+.2f}% | No alignment"
                    )
                rdb.hincrby(f"stats:trade:{normalize_asset(sym)}", "alignment_fail", 1)
                return None
            else:
                logger.debug(
                        f"✓ get_signal | {sym:>8} | Bybit: {bybit_5m_pct:+.2f}% | "
                        f"Coinbase: {coinbase_pct:+.2f}% | Chainlink: {chainlink_pct:+.2f}% | Aligned"
                    )
                rdb.hincrby(f"stats:trade:{normalize_asset(sym)}", "alignment_pass", 1)

                # Relax side threshold during epoch bias window or BTC lead-lag
                btc_lag = (
                    sym != "BTCUSD"
                    and BYBIT_MANAGER is not None
                    and BYBIT_MANAGER._btc_momentum_direction is not None
                    and (timemodule.time() - BYBIT_MANAGER._btc_momentum_ts) < Config.BTC_LAG_TTL
                    and ((bybit_5m_pct > 0) == (BYBIT_MANAGER._btc_momentum_direction == "UP"))
                )
                in_epoch_bias = tick.candle_seconds <= Config.EPOCH_BIAS_SECS
                side_threshold = Config.REDUCED_THRESHOLD_PCT if (in_epoch_bias or btc_lag) else 0.05
                side = 'SELL' if bybit_5m_pct < -side_threshold else 'BUY' if bybit_5m_pct > side_threshold else None
                obi = tick.order_book_imbalance

                # Block only when the book strongly contradicts the signal direction
                obi_contradicts = (
                    (side == 'BUY'  and obi < -0.15) or
                    (side == 'SELL' and obi > 0.15)
                )

                reason = "epoch_bias" if in_epoch_bias else ("btc_lag" if btc_lag else "normal")
                logger.debug(
                    f"📊 get_signal | {sym:>8} | chg={bybit_5m_pct:+.2f}% | OBI={obi:+.3f} | "
                    f"contradicts={obi_contradicts} | volume={high_vol} | [{reason}]"
                )

                if high_vol and side and not obi_contradicts:
                    return (normalize_asset(sym), side, bybit_5m_pct, open_price)
                return None

BYBIT_MANAGER: Optional[BybitManager] = None

shutting_down = False

# === TRADING COMPONENTS ===
components = Components.create()
client = components['client']
checker = components['checker']
geo_checker = components['geo_checker']
order_mgr = components['order_mgr']
finder= components['finder'] 
rpc_manager = components['rpc_manager']
price_tracker = components['price_tracker'] 

def log_config() -> None:
    """Log trading parameters."""
    logger.info("🔧 Config settings:")
    logger.info(f"  DRY_RUN             = {Config.DRY_RUN}")
    logger.info(f"  PRICE_MIN           = {Config.PRICE_MIN}")
    logger.info(f"  PRICE_MAX           = {Config.PRICE_MAX}")
    logger.info(f"  ASSETS              = {Config.ASSETS}")

rdb = RedisCache()

def test_redis() -> bool:
    """Test Redis connection."""
    try:
        rdb.ping()
        logger.info("✓ test_redis | Redis OK")
        return True
    except Exception as e:
        logger.error(f"✗ test_redis | Redis failed: {e}")
        return False

class AppState:
    def __init__(self):
        self.can_trade = False

APP_STATE = AppState()

class CoinbaseFeed:
    def __init__(self):
        """Minimal state for Coinbase tracking."""
        self.last_prices = {k: 0.0 for k in Config.COINBASE_SYMBOLS}
        self.coinbase_5m_bases = {sym: 0.0 for sym in Config.COINBASE_SYMBOLS}
        self.coinbase_5m_ts = {sym: 0.0 for sym in Config.COINBASE_SYMBOLS}
        self.bars = {k: 0 for k in Config.COINBASE_SYMBOLS}
        self.global_last_snapshot = 0 

        self.running = False
        self.task: Optional[asyncio.Task] = None
        
    def start(self):
        if self.running or self.task:
            return
        self.running = True
        self.task = asyncio.create_task(self.listen_all())

    def stop(self):
        if self.task:
            self.task.cancel()
            self.task = None
        self.running = False

    def update_from_coinbase(self, product_id: str, price: float):
        """Update Coinbase price tracking. Returns True if new 5m bar."""
        reverse = {v: k for k, v in Config.COINBASE_SYMBOLS.items()}
        
        if product_id not in reverse:
            logger.warning(f"📥 update_from_coinbase | Unknown: {product_id}")
            return False
        
        internal_sym = reverse[product_id]
        now = time.time()
        bar_start = get_current_5m_bar_ts(now)
        
        # Always track latest price
        self.last_prices[internal_sym] = price
        
        # Check for new 5m bar
        prior_bar = self.bars.get(internal_sym)
        if prior_bar == bar_start:
            return False
        
        self.bars[internal_sym] = bar_start
        
        # First update ever
        if prior_bar is None:
            self.coinbase_5m_bases[internal_sym] = price
            self.coinbase_5m_ts[internal_sym] = now
            logger.debug(f"📥 update_from_coinbase | First {internal_sym} (bar {bar_start})")
            return False
        
        # NEW 5M BAR - log snapshot
        logger.debug(f"🕐 update_from_coinbase | New bar {internal_sym} at {bar_start}")
        
         # Global snapshot logging (once per bar across all symbols)
        if bar_start != self.global_last_snapshot:
            self.global_last_snapshot = bar_start
            logger.debug("🔄 update_from_chainlink | Logging ALL assets snapshot")

            # Log all assets snapshot
            for s in self.coinbase_5m_bases:
                base = self.coinbase_5m_bases[s]
                current_price = self.last_prices[s]

                if base == 0.0:
                    logger.debug(f"⏳ update_from_coinbase | {s}: {current_price:.4f} | FIRST")
                else:
                    change_pct = 100.0 * (current_price - base) / base
                    direction = "🟢   UP" if change_pct > 0 else "🔴 DOWN" if change_pct < 0 else "⚪ FLAT"
                    logger.info(
                        f"🔄 update_from_coinbase  | {direction} | {s:>9} | {change_pct:+.3f}% | {current_price:10.4f} | {bar_start}"
                    )

        # Set true 5m bar open (first price of new bar)
        self.coinbase_5m_bases[internal_sym] = price
        self.coinbase_5m_ts[internal_sym] = now
        
        return True
    
    async def listen_all(self):
        url = "wss://advanced-trade-ws.coinbase.com"
        products = list(Config.COINBASE_SYMBOLS.values())

        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=30) as ws:
                    logger.info(f"✓ CoinbaseFeed | Connected and subscribed to {url} for {products}")
                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "channel": "ticker",
                        "product_ids": products,
                    }))
                    async for msg in ws:
                        if not isinstance(msg, str):
                            continue
                        try:
                            data = json.loads(msg)
                        except json.JSONDecodeError:
                            continue
                        if data.get("channel") != "ticker":
                            continue
                        for event in data.get("events", []):
                            if not isinstance(event, dict):
                                continue
                            for tick in event.get("tickers", []):
                                if not isinstance(tick, dict):
                                    continue
                                product_id = tick.get("product_id")
                                price_str = tick.get("price")
                                if not product_id or price_str is None:
                                    continue
                                try:
                                    price = float(price_str)
                                except (TypeError, ValueError):
                                    continue
                                self.update_from_coinbase(product_id, price)

            except (websockets.ConnectionClosed, ConnectionResetError) as e:
                logger.warning(f"⚠️ CoinbaseFeed | WebSocket disconnected, reconnecting in 3s: {e}")
                await asyncio.sleep(3)
            except Exception as e:
                logger.exception(f"✗ CoinbaseFeed | top-level error: {e!r}")
                await asyncio.sleep(5)

coinbase_feed = CoinbaseFeed()

class ChainlinkFeed:
    def __init__(self):
        self.last_prices = Config.CHAINLINK_SYMBOLS.copy()
        self.chainlink_5m_bases = {sym: 0.0 for sym in Config.CHAINLINK_SYMBOLS}
        self.chainlink_5m_ts = {sym: 0.0 for sym in Config.CHAINLINK_SYMBOLS}
        self.chainlink_bars = {}
        self.chainlink_last_update_ts: Dict[str, float] = {}
        self.global_last_snapshot = 0

        self.running = False
        self.task: Optional[asyncio.Task] = None

    def start(self):
        if self.running or self.task:
            return
        self.running = True
        self.task = asyncio.create_task(self.listen_all())

    def stop(self):
        if self.task:
            self.task.cancel()
            self.task = None
        self.running = False

    def update_from_chainlink(self, symbol: str, price: float):
        """Update Chainlink price tracking. Returns True if new 5m bar started."""
        now = time.time()
        bar_start = get_current_5m_bar_ts(now)

        symbol = symbol.lower()
        self.last_prices[symbol] = price
        self.chainlink_last_update_ts[symbol] = now  # track oracle freshness

        # Check if new 5m bar
        prior_bar = self.chainlink_bars.get(symbol)
        if prior_bar == bar_start:
            return False  # Same bar, no action needed

        self.chainlink_bars[symbol] = bar_start

        # First update ever - initialize 5m base
        if prior_bar is None:
            self.chainlink_5m_bases[symbol] = price
            self.chainlink_5m_ts[symbol] = now
            logger.debug(f"📥 update_from_chainlink | First {symbol} (bar {bar_start})")
            return False

        logger.debug(f"🕐 update_from_chainlink | New 5min bar {symbol} at {bar_start}")

        # Global snapshot logging (once per bar across all symbols)
        if bar_start != self.global_last_snapshot:
            self.global_last_snapshot = bar_start
            logger.debug("🔄 update_from_chainlink | Logging ALL assets snapshot")

            for s in self.chainlink_5m_bases:
                base = self.chainlink_5m_bases[s]
                current_price = self.last_prices[s]

                if base == 0.0:
                    logger.debug(f"⏳ update_from_chainlink | {s.upper()}: {current_price:.4f} | FIRST")
                else:
                    change_pct = 100.0 * (current_price - base) / base
                    direction = "🟢   UP" if change_pct > 0 else "🔴 DOWN" if change_pct < 0 else "⚪ FLAT"
                    logger.info(
                        f"🔄 update_from_chainlink | {direction} | {s.upper():>9} | {change_pct:+.3f}% | {current_price:10.4f} | {bar_start}"
                    )
                
        # Set 5m base for this symbol (true bar open price)
        self.chainlink_5m_bases[symbol] = price
        self.chainlink_5m_ts[symbol] = now
        
        return True

    async def listen_all(self):
        while True:
            try:
                async with websockets.connect(Config.WS_URL, ping_interval=20, ping_timeout=30) as ws:
                    
                    await ws.send(json.dumps({
                        "action": "subscribe",
                        "subscriptions": [
                            {
                                "topic": Config.CHAINLINK_FEED,
                                "type": "*",
                                "filters": ""  # or symbol-specific filters
                            }
                        ]
                    }))
                    logger.info(f"✓ ChainlinkFeed | Connected and subscribed to {Config.CHAINLINK_FEED} for {list(Config.CHAINLINK_SYMBOLS.keys())}")

                    async for msg in ws:
                        # --- Only parse JSON text frames; skip ping/pong ---
                        if not isinstance(msg, str):
                            continue  # e.g. bytes, ping/pong

                        try:
                            data = json.loads(msg)
                        except json.JSONDecodeError as e:
                            logger.debug(f"💬 ChainlinkFeed | Non-JSON/ws-control: {repr(msg)}")
                            continue

                        if data.get("topic") != Config.CHAINLINK_FEED:
                            continue

                        payload = data.get("payload")
                        if not payload:
                            continue

                        symbol = payload.get("symbol")
                        if not symbol or symbol not in Config.CHAINLINK_SYMBOLS:
                            continue

                        try:
                            price = float(payload["value"])
                        except (ValueError, TypeError):
                            continue

                        logger.debug(
                            f"✓ ChainlinkFeed | {symbol.upper()}: {price:.4f} "
                            f"@ {payload.get('timestamp', 'N/A')}"
                        )

                        self.update_from_chainlink(symbol, price)

            except (websockets.ConnectionClosed, ConnectionResetError) as e:
                logger.warning(f"⚠️ ChainlinkFeed | WebSocket disconnected, reconnecting in 3s: {e}")
                await asyncio.sleep(3)
            except Exception as e:
                logger.exception(f"✗ ChainlinkFeed | top-level error: {e!r}")
                await asyncio.sleep(5)

chainlink_feed = ChainlinkFeed()

async def execute_trading_validation(symbol: str = None) -> Optional[Dict]:
    """Trading validation - single symbol for poll() efficiency."""
    if shutting_down:
        return None
    
    if Config.DRY_RUN:
        logger.debug("🧪 execute_trading_validation | DRY RUN - allow trading validation")
    elif not APP_STATE.can_trade:
        logger.debug("✗ execute_trading_validation | Skipping trading: can_trade = False")
        return None
    
    global BYBIT_MANAGER
    if BYBIT_MANAGER is None:
        logger.error(" execute_trading_validation | BYBIT_MANAGER is None")
        return None
    
    logger.debug("🎯 execute_trading_validation | Trading validation%s", f" for {symbol}" if symbol else "")

    target_assets = [normalize_asset(symbol)] if symbol else Config.ASSETS
    markets, next_markets = finder.find_polymarket_targets(target_assets)

    # Simple - getsignal() handles everything
    if symbol:
        raw_signals = [await getsignal(symbol)]
    else:
        signal_coros = [getsignal(s) for s in Config.BYBIT_SYMBOLS]
        raw_signals = await asyncio.gather(*signal_coros, return_exceptions=True)
    
    signals: List[Tuple[str, str, float, float]] = [s for s in raw_signals if isinstance(s, tuple)]

    if not signals:
        logger.debug("No signals for %s", symbol or "all")
        if not symbol:  # Full scan handles approvals
            await handle_next_markets_approvals()
        return None
    
    if len(markets) < len(signals):
        logger.warning("execute_trading_validation | Signal count (%d) exceeds market count (%d) for %s", len(signals), len(markets), symbol or "all")
        
    trade_results = await _execute_parallel_trades(markets, signals)
    successful_trades = [r for r in trade_results if r is not None]
    return {"trades": successful_trades} if successful_trades else None

async def _execute_parallel_trades(
    markets: Dict[str, Dict], 
    signals: List[Tuple[str, str, float, float]]
) -> List[Optional[Dict]]:
    """Execute trades concurrently with semaphore for capacity control."""
    semaphore = asyncio.Semaphore(3)  # Limit to 3 concurrent trades
    
    async def _trade_with_semaphore(
        asset: str, direction: str, confidence: float, open_price: float,
        market_slug: str, token_id: str, token: str, kelly_boost: float
    ) -> Optional[Dict]:
        async with semaphore:
            result = await order_mgr.safe_place_order(
                market_slug, token_id, token, asset, open_price, confidence, kelly_boost
            )
            if result:
                logger.debug("✓ execute_parallel_trades | Trade executed %s (open: %.2f)", asset, open_price)
            else:
                logger.debug("✗ execute_parallel_trades | Trade failed %s", asset)
            return result
   
    tasks = []
    for asset, direction, confidence, sig_open in signals:
        if asset not in markets:
            continue

        market = markets[asset]
        market_slug = market["slug"]
        raw_tokens = market["clobTokenIds"]
        token_list = json.loads(raw_tokens) if isinstance(raw_tokens, str) else raw_tokens

        if direction == "BUY":
            token_id = token_list[0]  # YES token
            token = "YES"
            logger.debug("🚀 execute_parallel_trades | BUYING YES for %s (open: %.2f)", asset, sig_open)
        else:  # SELL
            token_id = token_list[1]  # NO token
            token = "NO"
            logger.debug("🚀 BUYING NO for %s (open: %.2f)", asset, sig_open)

        kelly_boost = BYBIT_MANAGER.get_kelly_boost(asset, direction) if BYBIT_MANAGER else 1.0

        tasks.append(_trade_with_semaphore(
            asset, direction, confidence, sig_open, market_slug, token_id, token, kelly_boost
        ))
    
    return await asyncio.gather(*tasks, return_exceptions=True)

async def handle_next_markets_approvals() -> None:
    """Approve next markets in parallel - standalone reusable method."""
    
    method_start = time.perf_counter()  # ⏱️ Full method timing
    
    trigger_minute = int(time.strftime("%M")) % 5    
    markets, next_markets = finder.find_polymarket_targets(Config.ASSETS)
    
    if trigger_minute != 1:  # Only on minute 1
        method_end = time.perf_counter()
        logger.debug(f"⏱️ handle_next_markets_approvals skipped (min {trigger_minute}) → {(method_end - method_start)*1000:.1f}ms")
        return
    
    if not next_markets:
        method_end = time.perf_counter()
        logger.debug(f"⏱️ handle_next_markets_approvals no markets → {(method_end - method_start)*1000:.1f}ms")
        return
    
    approval_tasks = []
    approval_count = 0
    for asset, next_market in next_markets.items():
        if Config.DRY_RUN:
            logger.info("🧪 handle_next_markets_approvals | DRY APPROVAL %s next market", asset)
            continue

        raw_tokens = next_market["clobTokenIds"]
        token_list = json.loads(raw_tokens) if isinstance(raw_tokens, str) else raw_tokens

        logger.debug("⏱️ handle_next_markets_approvals | Approving %s for next market (%s tokens)", asset, len(token_list))
        # Parallel approvals for YES/NO tokens
        approval_tasks.extend([
            order_mgr.fast_approve("CONDITIONAL", token_list[0]),  # YES
            order_mgr.fast_approve("CONDITIONAL", token_list[1])   # NO
        ])
        approval_count += 2

    if approval_tasks:
        approve_start = time.perf_counter()
        results = await asyncio.gather(*approval_tasks, return_exceptions=True)
        approve_end = time.perf_counter()
        
        # Log approval batch timing
        logger.info(f"⏱️ handle_next_markets_approvals | Approvals batch ({len(approval_tasks)} tasks) → {(approve_end - approve_start)*1000:.1f}ms")
        
        fails = sum(1 for result in results if isinstance(result, Exception))
        logger.info(f"⏱️ handle_next_markets_approvals | Approvals: {len(results)-fails}/{len(results)} success ({100*(len(results)-fails)/len(results):.0f}%)")
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error("✗ handle_next_markets_approvals | Approval %d failed: %s", i+1, result)
    else:
        logger.info("⏱️ handle_next_markets_approvals | no tasks → 0ms")
    
    method_end = time.perf_counter()
    logger.info(f"⏱️ handle_next_markets_approvals | Finished {(method_end - method_start)*1000:.1f}ms ({approval_count} approvals)")

async def redeem() -> Dict:
    """Async wrapper for optimized redeem.py."""
    try:
        logger.debug("🔄 Redeem | Running auto-redemption...")
        
        # Run sync redeem in thread pool 
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None,
            lambda: run_redeem_non_interactive(mode="high_gas")
        )
        
        # Handle new return format
        if result["status"] == "success":
            logger.info(f"✅ Redeem | success")
            return {"success": True}
        elif result["status"] == "no_positions":
            logger.debug("✓ Redeem | No redeemable positions found")
            return {"success": True}
        else:
            logger.error(f"✗ Redeem | error")
            return {"success": False}
            
    except Exception as e:
        logger.error(f"✗ Redeem | failed: {e}")
        return {"success": False, "message": str(e)}

async def balance_check():
    """Async balance check."""
    try:
        balance = checker.pusd_balance
        can_trade = checker.check_trading_capacity(Config.POSITION_SIZE)
        rdb.set("bot:live_bankroll", str(round(balance, 2)), ex=300)
        logger.info("💰 balance_check | pUSD: $%.2f | Can trade: %s", balance, can_trade)
        return can_trade
    except Exception as e:
        logger.error("💰 balance_check | failed: %s", e)
        return False

async def check_trading_ready() -> bool:
    if Config.DRY_RUN:
        return True
    try:
        return await asyncio.wait_for(balance_check(), timeout=5.0)
    except asyncio.TimeoutError:
        return False

async def timer_loop():
    """Precise 5-minute scheduler using modulo arithmetic."""
    logger.info("✓ Timer loop | Precise 5min scheduling")
    
    last_trading = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0}  # Dict tracks per-minute timestamps
    APP_STATE.can_trade = await check_trading_ready()
    while not shutting_down:
        now = datetime.now()
        minute = now.minute
        second = now.second
        minute_mod = minute % 5        
        
        try:    
            # REDEEM: Every 5min on the 10th second
            if minute_mod == 0 and second == 10:
                logger.info(f"🎯 Timer loop | Redeem at {now.strftime('%H:%M:%S')}")
                await asyncio.wait_for(redeem(), timeout=45.0)
                APP_STATE.can_trade = await check_trading_ready()
                logger.debug(f"✓ Timer loop | Balance check: can_trade={APP_STATE.can_trade}")                
                
                for sym in Config.BYBIT_SYMBOLS:
                    if BYBIT_MANAGER and sym in BYBIT_MANAGER.bybit_candles:
                        BYBIT_MANAGER.bybit_candles[sym].log_volume_status(sym)
                
                if minute == 0:
                    await asyncio.to_thread(price_tracker.run, limit=5)
                await asyncio.sleep(1.1)
                continue

            # TRADING: every minute at second 0 if ready
            elif second == 0 and APP_STATE.can_trade and minute_mod in (0, 1, 2, 3, 4):
                ts = now.timestamp()
                if ts - last_trading[minute_mod] > 295:
                    logger.info(f"🎯 Timer loop | Trading M{minute_mod} at {now.strftime('%H:%M:%S')} | can_trade={APP_STATE.can_trade} | dry_run={Config.DRY_RUN}")
                    last_trading[minute_mod] = ts
                    await handle_next_markets_approvals()
                await asyncio.sleep(1.1)
                continue
            
            # Default: sleep 1 second (CPU efficient)
            await asyncio.sleep(1)
            
        except asyncio.CancelledError:
            logger.info("✗ Timer loop | Cancelled")
            raise
        except asyncio.TimeoutError:
            logger.warning("✗ Timer loop | Timeout - continuing")
        except Exception as e:
            logger.error(f"✗ Timer loop | Error: {e}")
            await asyncio.sleep(5)

async def getsignal(sym: str) -> Optional[Tuple[str, str, float, float]]:
    global BYBIT_MANAGER
    if not BYBIT_MANAGER:
        return None
    return BYBIT_MANAGER.get_signal(sym)

async def main():
    global shutting_down, BYBIT_MANAGER, coinbase_feed, chainlink_feed
    
    # Early exits with cleanup
    if not geo_checker.test_geo():
        logger.warning("🌍 main | Geoblocked - monitoring only")
    
    if not test_redis():
        logger.error("✗ main | Redis required")
        return 
    
    # Initialize ALL feeds FIRST (before starting)
    coinbase_feed = CoinbaseFeed()
    chainlink_feed = ChainlinkFeed()
    
    log_config()
    checker.log_status()     
    
    logger.info("🚀 main | Starting")

    # Clear orders + start background threads
    await order_mgr.clear_open_orders()
    await asyncio.to_thread(price_tracker.run, limit=5)
    
    # START feeds AFTER initialization
    coinbase_feed.start()
    chainlink_feed.start()

    BYBIT_MANAGER = BybitManager()
    loop = asyncio.get_running_loop()
    BYBIT_MANAGER.start_websocket(loop)
         
    sched_task = asyncio.create_task(timer_loop())
    
    try:
        await sched_task
    except asyncio.CancelledError:
        logger.info("🛑 main | Cancelled - cleaning up")
    finally:
        logger.info("🔄 main | Shutting down...")
        
        # Sequential cleanup (Bybit first, then feeds)
        if BYBIT_MANAGER:
            BYBIT_MANAGER.stop()
            logger.info("✓ main | Bybit feed stopped")
            BYBIT_MANAGER = None  # Clear global

        # Stop feeds (they have their own task.cancel())
        if coinbase_feed:
            coinbase_feed.stop()
            logger.info("✓ main | Coinbase feed stopped")
        if chainlink_feed:
            chainlink_feed.stop()
            logger.info("✓ main | Chainlink feed stopped")

        # Final resource cleanup
        if 'rdb' in globals():
            rdb.close()
            logger.info("✓ main | Redis closed")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 main | Stopped by user (Ctrl+C)")
    except asyncio.TimeoutError:
        logger.warning("⏰ main | Task limit exceeded")
    except Exception as e:         
        logger.error(f"💥 main | Crashed: {e}", exc_info=True)
    finally:
        logger.info("✓ main | Shutdown complete")
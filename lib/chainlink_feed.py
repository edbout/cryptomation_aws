"""Chainlink oracle WebSocket feed — informational price source.

Chainlink is **informational only** in the alignment vote (Bybit / Binance /
Coinbase are the three voting feeds). It does not fire triggers and does not
contribute to consensus; it's surfaced for context and freshness monitoring.

Stream: Config.WS_URL
Topic:  Config.CHAINLINK_FEED
Symbols: Config.CHAINLINK_SYMBOLS (e.g. {"btc/usd": ..., "eth/usd": ...})

On extended disconnect (>60s) emits a Telegram alert via lib.telegram_alert.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Dict, Optional

import websockets

from config import Config
from lib.helpers import get_current_5m_bar_ts
from lib.telegram_alert import send_alert

logger = logging.getLogger(__name__)


class ChainlinkFeed:
    def __init__(self):
        # NOTE: copy() so we don't mutate Config.CHAINLINK_SYMBOLS via last_prices.
        self.last_prices: Dict[str, float] = Config.CHAINLINK_SYMBOLS.copy()
        self.chainlink_5m_bases: Dict[str, float] = {sym: 0.0 for sym in Config.CHAINLINK_SYMBOLS}
        self.chainlink_5m_ts: Dict[str, float] = {sym: 0.0 for sym in Config.CHAINLINK_SYMBOLS}
        self.chainlink_bars: Dict[str, int] = {}
        self.chainlink_last_update_ts: Dict[str, float] = {}
        self.global_last_snapshot: int = 0

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

    def update_from_chainlink(self, symbol: str, price: float) -> bool:
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
        _retry_count = 0
        _disconnect_ts: Optional[float] = None
        while True:
            try:
                async with websockets.connect(Config.WS_URL, ping_interval=20, ping_timeout=30) as ws:
                    # Successful (re)connect — reset backoff state
                    if _disconnect_ts is not None:
                        down_secs = int(time.time() - _disconnect_ts)
                        logger.info(f"✓ ChainlinkFeed | Reconnected after {down_secs}s down")
                    _retry_count = 0
                    _disconnect_ts = None

                    await ws.send(json.dumps({
                        "action": "subscribe",
                        "subscriptions": [
                            {
                                "topic": Config.CHAINLINK_FEED,
                                "type": "*",
                                "filters": ""
                            }
                        ]
                    }))
                    logger.info(f"✓ ChainlinkFeed | Connected and subscribed to {Config.CHAINLINK_FEED} for {list(Config.CHAINLINK_SYMBOLS.keys())}")

                    async for msg in ws:
                        if not isinstance(msg, str):
                            continue

                        try:
                            data = json.loads(msg)
                        except json.JSONDecodeError:
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
                if _disconnect_ts is None:
                    _disconnect_ts = time.time()
                _retry_count += 1
                wait = min(3 * (2 ** (_retry_count - 1)), 60)
                down_secs = int(time.time() - _disconnect_ts)
                logger.warning(f"⚠️ ChainlinkFeed | Disconnected ({down_secs}s), retry #{_retry_count} in {wait}s: {e}")
                if down_secs > 60:
                    await send_alert(f"⚠️ <b>Chainlink feed down</b> for {down_secs}s\nRetry #{_retry_count} — signals may use only Bybit+Coinbase")
                await asyncio.sleep(wait)
            except asyncio.CancelledError:
                logger.info("✓ ChainlinkFeed | listen_all cancelled")
                raise
            except Exception as e:
                if _disconnect_ts is None:
                    _disconnect_ts = time.time()
                _retry_count += 1
                wait = min(5 * (2 ** (_retry_count - 1)), 60)
                logger.exception(f"✗ ChainlinkFeed | top-level error (retry #{_retry_count} in {wait}s): {e!r}")
                await asyncio.sleep(wait)

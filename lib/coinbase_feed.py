"""Coinbase Advanced Trade WebSocket feed — passive price source.

Parallels BinanceFeed in lib/binance_feed.py but is **passive**: it never fires
trading triggers. It only maintains last-price and 5m-bar-base state so the
2-of-3 alignment check in BybitManager.get_signal can read a Coinbase pct.

Stream: wss://advanced-trade-ws.coinbase.com
Channel: ticker for each product in Config.COINBASE_SYMBOLS (values).
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

logger = logging.getLogger(__name__)


class CoinbaseFeed:
    def __init__(self):
        """Minimal state for Coinbase tracking."""
        self.last_prices: Dict[str, float] = {k: 0.0 for k in Config.COINBASE_SYMBOLS}
        self.coinbase_5m_bases: Dict[str, float] = {sym: 0.0 for sym in Config.COINBASE_SYMBOLS}
        self.coinbase_5m_ts: Dict[str, float] = {sym: 0.0 for sym in Config.COINBASE_SYMBOLS}
        self.bars: Dict[str, int] = {k: 0 for k in Config.COINBASE_SYMBOLS}
        self.global_last_snapshot: int = 0

        # Cache product_id → internal symbol once (was rebuilt on every tick).
        self._product_to_internal: Dict[str, str] = {
            v: k for k, v in Config.COINBASE_SYMBOLS.items()
        }

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

    def update_from_coinbase(self, product_id: str, price: float) -> bool:
        """Update Coinbase price tracking. Returns True if new 5m bar."""
        internal_sym = self._product_to_internal.get(product_id)
        if internal_sym is None:
            logger.warning(f"📥 update_from_coinbase | Unknown: {product_id}")
            return False

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
        if prior_bar is None or prior_bar == 0:
            self.coinbase_5m_bases[internal_sym] = price
            self.coinbase_5m_ts[internal_sym] = now
            logger.debug(f"📥 update_from_coinbase | First {internal_sym} (bar {bar_start})")
            return False

        # NEW 5M BAR - log snapshot
        logger.debug(f"🕐 update_from_coinbase | New bar {internal_sym} at {bar_start}")

        # Global snapshot logging (once per bar across all symbols)
        if bar_start != self.global_last_snapshot:
            self.global_last_snapshot = bar_start
            logger.debug("🔄 update_from_coinbase | Logging ALL assets snapshot")

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
            except asyncio.CancelledError:
                logger.info("✓ CoinbaseFeed | listen_all cancelled")
                raise
            except Exception as e:
                logger.exception(f"✗ CoinbaseFeed | top-level error: {e!r}")
                await asyncio.sleep(5)

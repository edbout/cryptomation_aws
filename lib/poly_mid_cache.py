#!/usr/bin/env python3
"""Real-time Polymarket mid-price cache backed by a CLOB WebSocket subscription.

The Polymarket CLOB WebSocket lives at wss://clob.polymarket.com (same host as
the HTTP API at https://clob.polymarket.com).  It is separate from the
wss://ws-live-data.polymarket.com server which serves oracle/Chainlink feeds.

Subscription envelope (CLOB WebSocket protocol):
    {"assets_ids": ["TOKEN_ID_1", "TOKEN_ID_2", ...], "type": "market"}

Server pushes back book snapshots / deltas:
    {"event_type": "book",  "asset_id": "...", "buys": [...], "sells": [...]}
    {"event_type": "price_change", "asset_id": "...", "price": "0.74"}
    {"event_type": "last_trade_price", "asset_id": "...", "price": "0.74"}

Usage
-----
    from lib.poly_mid_cache import POLY_MID_CACHE

    asyncio.create_task(POLY_MID_CACHE.run())          # start once
    POLY_MID_CACHE.subscribe(["token_id_1", ...])      # call whenever tokens rotate
    mid = POLY_MID_CACHE.get("token_id_1")             # None → fall back to HTTP
"""

import asyncio
import json
import logging
import time
from typing import Dict, List, Optional, Set

import websockets

logger = logging.getLogger(__name__)

# WebSocket URL — CLOB server (NOT ws-live-data which is oracle-only)
WS_URL = "wss://clob.polymarket.com"

# Treat a cached price as stale after this many seconds without a WS update.
STALE_SECS: float = 30.0

# Log raw messages until we've seen this many, to help diagnose subscription issues.
_RAW_LOG_BURST = 5


class PolymarketMidCache:
    """Live mid-price cache for Polymarket YES tokens via CLOB WebSocket.

    Thread-safety: `get()` is safe from any thread; everything else runs
    inside the asyncio event loop.
    """

    def __init__(self) -> None:
        self._prices: Dict[str, float] = {}    # token_id → latest mid
        self._ts: Dict[str, float] = {}         # token_id → epoch of last update
        self._subscribed: Set[str] = set()      # tokens the WS currently covers
        self._pending: Set[str] = set()         # queued until next (re)connect
        self._running: bool = False
        self._hit: int = 0
        self._miss: int = 0
        self._raw_seen: int = 0                 # messages received since connect

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def get(self, token_id: str) -> Optional[float]:
        """Return a fresh cached mid, or None (caller should fall back to HTTP)."""
        ts = self._ts.get(token_id)
        if ts is None or (time.time() - ts) > STALE_SECS:
            self._miss += 1
            return None
        self._hit += 1
        return self._prices[token_id]

    def subscribe(self, token_ids: List[str]) -> None:
        """Queue token_ids for subscription.  Safe to call before run() starts."""
        new = set(token_ids) - self._subscribed - self._pending
        if new:
            self._pending.update(new)
            logger.debug("PolymarketMidCache | queued %d new token(s)", len(new))

    def stats(self) -> str:
        total = self._hit + self._miss
        hit_rate = 100 * self._hit / total if total else 0
        return (
            f"hits={self._hit} misses={self._miss} "
            f"hit_rate={hit_rate:.0f}% cached_tokens={len(self._prices)}"
        )

    # ------------------------------------------------------------------ #
    # Async run loop                                                       #
    # ------------------------------------------------------------------ #

    async def run(self) -> None:
        """Connect to the CLOB WebSocket and pump messages — reconnects forever."""
        self._running = True
        _retry_count = 0
        _disconnect_ts: Optional[float] = None

        while self._running:
            try:
                async with websockets.connect(
                    WS_URL,
                    ping_interval=20,
                    ping_timeout=30,
                    additional_headers={"Origin": "https://polymarket.com"},
                ) as ws:
                    if _disconnect_ts is not None:
                        down_secs = int(time.time() - _disconnect_ts)
                        logger.info("✓ PolymarketMidCache | reconnected after %ds down", down_secs)
                    _retry_count = 0
                    _disconnect_ts = None
                    self._raw_seen = 0

                    all_tokens = self._subscribed | self._pending
                    if all_tokens:
                        await self._send_subscribe(ws, all_tokens)
                        self._subscribed = set(all_tokens)
                        self._pending.clear()

                    logger.info(
                        "✓ PolymarketMidCache | connected to %s (%d tokens)",
                        WS_URL, len(self._subscribed),
                    )

                    async for raw in ws:
                        if self._pending:
                            await self._send_subscribe(ws, self._pending)
                            self._subscribed |= self._pending
                            self._pending.clear()

                        if not isinstance(raw, str):
                            continue

                        # Diagnostic burst: log the first N raw messages at INFO
                        # so we can see exactly what the server sends back.
                        if self._raw_seen < _RAW_LOG_BURST:
                            self._raw_seen += 1
                            logger.info(
                                "📡 PolymarketMidCache | raw[%d]: %.300s",
                                self._raw_seen, raw,
                            )

                        try:
                            self._handle_message(raw)
                        except Exception as exc:
                            logger.debug(
                                "PolymarketMidCache | parse error: %s | raw=%.120s", exc, raw
                            )

            except asyncio.CancelledError:
                logger.info("PolymarketMidCache | cancelled")
                break
            except (websockets.ConnectionClosed, ConnectionResetError) as exc:
                if _disconnect_ts is None:
                    _disconnect_ts = time.time()
                _retry_count += 1
                wait = min(3 * (2 ** (_retry_count - 1)), 60)
                down_secs = int(time.time() - _disconnect_ts)
                logger.warning(
                    "⚠️ PolymarketMidCache | disconnected (%ds), retry #%d in %ds: %s",
                    down_secs, _retry_count, wait, exc,
                )
                await asyncio.sleep(wait)
            except Exception as exc:
                if _disconnect_ts is None:
                    _disconnect_ts = time.time()
                _retry_count += 1
                wait = min(5 * (2 ** (_retry_count - 1)), 60)
                logger.exception(
                    "✗ PolymarketMidCache | error (retry #%d in %ds): %r",
                    _retry_count, wait, exc,
                )
                await asyncio.sleep(wait)

    # ------------------------------------------------------------------ #
    # Internal helpers                                                     #
    # ------------------------------------------------------------------ #

    async def _send_subscribe(self, ws, token_ids: Set[str]) -> None:
        """Send a CLOB market subscription for the given token IDs."""
        if not token_ids:
            return
        msg = json.dumps({
            "assets_ids": list(token_ids),
            "type": "market",
        })
        await ws.send(msg)
        logger.info(
            "✓ PolymarketMidCache | subscribed to %d token(s): %s…",
            len(token_ids),
            ", ".join(list(token_ids)[:2]),
        )

    def _handle_message(self, raw: str) -> None:
        """Parse a server message and update the price cache."""
        data = json.loads(raw)

        # CLOB pushes a list for book snapshots, a dict for single events.
        if isinstance(data, list):
            for item in data:
                self._dispatch(item)
        elif isinstance(data, dict):
            self._dispatch(data)

    def _dispatch(self, data: dict) -> None:
        event = data.get("event_type") or data.get("type", "")
        if event == "book":
            self._update_from_book(data)
        elif event in ("price_change", "last_trade_price", "midpoint"):
            self._update_from_price(data)
        # heartbeat / unknown events → ignored

    def _update_from_book(self, data: dict) -> None:
        token_id = data.get("asset_id") or data.get("token_id")
        if not token_id:
            return

        buys  = data.get("buys")  or data.get("bids")  or []
        sells = data.get("sells") or data.get("asks")  or []

        try:
            best_bid = max(float(b["price"]) for b in buys)  if buys  else None
            best_ask = min(float(s["price"]) for s in sells) if sells else None
        except (KeyError, TypeError, ValueError):
            return

        if best_bid is not None and best_ask is not None:
            mid = (best_bid + best_ask) / 2.0
        elif best_bid is not None:
            mid = best_bid
        elif best_ask is not None:
            mid = best_ask
        else:
            return

        self._set(token_id, mid)

    def _update_from_price(self, data: dict) -> None:
        token_id  = data.get("asset_id") or data.get("token_id")
        price_str = data.get("price") or data.get("mid")
        if token_id and price_str:
            try:
                self._set(token_id, float(price_str))
            except (ValueError, TypeError):
                pass

    def _set(self, token_id: str, mid: float) -> None:
        if 0.0 < mid < 1.0:
            was_new = token_id not in self._prices
            self._prices[token_id] = mid
            self._ts[token_id] = time.time()
            if was_new:
                logger.info("✓ PolymarketMidCache | first price for token …%s: %.4f", token_id[-8:], mid)


# Module-level singleton — import this everywhere.
POLY_MID_CACHE = PolymarketMidCache()

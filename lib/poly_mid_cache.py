#!/usr/bin/env python3
"""Polymarket mid-price cache — background HTTP polling.

Goal: eliminate the per-order HTTP get_midpoint latency in safe_place_order.

Approach: a background asyncio task polls client.get_midpoint() for every
subscribed token once per second.  When safe_place_order fires, the latest
price is already in memory — zero additional latency.

Both YES and NO tokens are subscribed (4 assets × 2 sides = 8 tokens) because
safe_place_order trades either side depending on signal direction.

WebSocket approach was attempted first and exhausted:
  - wss://clob.polymarket.com      → HTTP 200 (REST root, no WS upgrade)
  - wss://clob.polymarket.com/ws   → HTTP 404
  - wss://ws-live-data.polymarket.com (assets_ids subscription) → connects
    but sends zero messages for 35+ min (server only serves oracle/Chainlink
    topics; CLOB order book data is not available via WebSocket)

HTTP polling gives 95% of the WebSocket benefit with guaranteed reliability:
  - 8 tokens × 1 poll/s = 8 HTTP calls/s (vs. ~100 blocking calls/min before)
  - Price staleness ≤ POLL_INTERVAL seconds instead of 100–400 ms per order

Usage
-----
    from lib.poly_mid_cache import POLY_MID_CACHE

    POLY_MID_CACHE.set_client(clob_client)           # call once before run()
    asyncio.create_task(POLY_MID_CACHE.run())        # start background loop
    POLY_MID_CACHE.subscribe(["token_id_1", ...])    # call whenever tokens rotate

    mid = POLY_MID_CACHE.get("token_id_1")           # None → fall back to HTTP
"""

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)

# How often to poll each token (seconds).
# 1 s gives ≤1 s staleness; increase if rate-limiting becomes a concern.
POLL_INTERVAL: float = 1.0

# Treat a cached price as stale after this long without a fresh poll.
# Set to 3× POLL_INTERVAL so a single failed poll doesn't flush the cache.
STALE_SECS: float = 3.0


class PolymarketMidCache:
    """Background-polled mid-price cache for Polymarket YES and NO tokens.

    Polls all subscribed tokens concurrently every POLL_INTERVAL seconds using
    asyncio.to_thread so the blocking HTTP call doesn't stall the event loop.
    Tokens are added via subscribe() and picked up on the next poll cycle.

    Thread-safety: get() is safe from any thread; everything else runs
    inside the asyncio event loop.
    """

    def __init__(self) -> None:
        self._prices: Dict[str, float] = {}   # token_id → latest mid
        self._ts: Dict[str, float] = {}        # token_id → epoch of last update
        self._subscribed: Set[str] = set()     # tokens being polled
        self._pending: Set[str] = set()        # queued until next poll cycle
        self._client: Optional[Any] = None     # ClobClient, injected via set_client()
        self._running: bool = False
        self._hit: int = 0
        self._miss: int = 0
        self._polls: int = 0                   # total successful polls
        self._errors: int = 0                  # consecutive poll errors

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def set_client(self, client: Any) -> None:
        """Inject the ClobClient.  Must be called before run()."""
        self._client = client
        logger.info("✓ PolymarketMidCache | client set")

    def get(self, token_id: str) -> Optional[float]:
        """Return a fresh cached mid, or None (caller falls back to HTTP)."""
        ts = self._ts.get(token_id)
        if ts is None or (time.time() - ts) > STALE_SECS:
            self._miss += 1
            return None
        self._hit += 1
        return self._prices[token_id]

    def subscribe(self, token_ids: List[str]) -> None:
        """Add token_ids to the polling set.  Safe to call at any time."""
        new = set(token_ids) - self._subscribed - self._pending
        if new:
            self._pending.update(new)
            logger.debug("PolymarketMidCache | queued %d new token(s) for polling", len(new))

    def stats(self) -> str:
        total = self._hit + self._miss
        hit_rate = 100 * self._hit / total if total else 0
        return (
            f"hits={self._hit} misses={self._miss} "
            f"hit_rate={hit_rate:.0f}% cached_tokens={len(self._prices)} polls={self._polls}"
        )

    # ------------------------------------------------------------------ #
    # Background polling loop                                              #
    # ------------------------------------------------------------------ #

    async def run(self) -> None:
        """Poll get_midpoint for every subscribed token once per second."""
        self._running = True

        if self._client is None:
            logger.error("✗ PolymarketMidCache | no client set — polling disabled")
            return

        logger.info("✓ PolymarketMidCache | polling loop started (interval=%.1fs)", POLL_INTERVAL)

        while self._running:
            # Flush any newly queued tokens
            if self._pending:
                self._subscribed |= self._pending
                self._pending.clear()
                logger.info(
                    "✓ PolymarketMidCache | now polling %d token(s)", len(self._subscribed)
                )

            # Poll all tokens concurrently
            if self._subscribed:
                tasks = [
                    self._fetch_one(token_id)
                    for token_id in list(self._subscribed)
                ]
                await asyncio.gather(*tasks, return_exceptions=True)

            await asyncio.sleep(POLL_INTERVAL)

    async def _fetch_one(self, token_id: str) -> None:
        """Fetch and cache the mid-price for a single token."""
        try:
            resp = await asyncio.to_thread(self._client.get_midpoint, token_id)
            if isinstance(resp, dict) and "mid" in resp:
                mid = float(resp["mid"])
                was_new = token_id not in self._prices
                self._set(token_id, mid)
                self._polls += 1
                self._errors = 0
                if was_new:
                    logger.info(
                        "✓ PolymarketMidCache | first price for token …%s: %.4f",
                        token_id[-8:], mid,
                    )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # 404 = token expired (market resolved or epoch rolled over).
            # Unsubscribe immediately so we stop polling a dead token.
            if getattr(exc, "status_code", None) == 404 or "No orderbook" in str(exc):
                self._subscribed.discard(token_id)
                self._prices.pop(token_id, None)
                self._ts.pop(token_id, None)
                logger.debug(
                    "PolymarketMidCache | token …%s expired (404) — unsubscribed",
                    token_id[-8:],
                )
                return
            self._errors += 1
            # Only log other errors occasionally to avoid log spam
            if self._errors == 1 or self._errors % 60 == 0:
                logger.warning(
                    "⚠️ PolymarketMidCache | poll error for …%s (×%d): %s",
                    token_id[-8:], self._errors, exc,
                )

    def _set(self, token_id: str, mid: float) -> None:
        if 0.0 < mid < 1.0:
            self._prices[token_id] = mid
            self._ts[token_id] = time.time()


# Module-level singleton — import this everywhere.
POLY_MID_CACHE = PolymarketMidCache()

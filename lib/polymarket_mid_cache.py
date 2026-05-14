#!/usr/bin/env python3
"""Polymarket mid-price cache — background HTTP polling."""

import asyncio
import logging
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)

POLL_INTERVAL: float = 1.0
STALE_SECS: float = 3.0
MAX_BACKOFF_SECS: float = 30.0


class PolymarketMidCache:
    """Background-polled mid-price cache for Polymarket YES and NO tokens."""

    def __init__(self) -> None:
        self._prices: Dict[str, float] = {}
        self._ts: Dict[str, float] = {}
        self._subscribed: Set[str] = set()
        self._pending: Set[str] = set()
        self._client: Optional[Any] = None
        self._running: bool = False
        self._hit: int = 0
        self._miss: int = 0
        self._polls: int = 0
        self._errors_by_token: Dict[str, int] = defaultdict(int)
        self._next_poll_at: Dict[str, float] = {}

    def set_client(self, client: Any) -> None:
        """Inject the ClobClient. Must be called before run()."""
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
        """Add token_ids to the polling set. Safe to call at any time."""
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

    async def run(self) -> None:
        """Poll get_midpoint for every subscribed token once per second."""
        self._running = True

        if self._client is None:
            logger.error("✗ PolymarketMidCache | no client set — polling disabled")
            return

        logger.info("✓ PolymarketMidCache | polling loop started (interval=%.1fs)", POLL_INTERVAL)

        while self._running:
            now = time.time()

            if self._pending:
                self._subscribed |= self._pending
                self._pending.clear()
                logger.info("✓ PolymarketMidCache | now polling %d token(s)", len(self._subscribed))

            tokens_to_poll = [
                token_id for token_id in list(self._subscribed)
                if self._next_poll_at.get(token_id, 0.0) <= now
            ]

            if tokens_to_poll:
                await asyncio.gather(
                    *(self._fetch_one(token_id) for token_id in tokens_to_poll),
                    return_exceptions=True,
                )

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
                self._errors_by_token[token_id] = 0
                self._next_poll_at.pop(token_id, None)

                if was_new:
                    logger.info(
                        "✓ PolymarketMidCache | first price for token …%s: %.4f",
                        token_id[-8:],
                        mid,
                    )
                return

            raise ValueError(f"unexpected midpoint response: {resp!r}")

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            msg = str(exc)
            status_code = getattr(exc, "status_code", None)

            if status_code == 404 or "No orderbook" in msg:
                self._subscribed.discard(token_id)
                self._pending.discard(token_id)
                self._prices.pop(token_id, None)
                self._ts.pop(token_id, None)
                self._errors_by_token.pop(token_id, None)
                self._next_poll_at.pop(token_id, None)
                logger.debug(
                    "PolymarketMidCache | token …%s expired (404) — unsubscribed",
                    token_id[-8:],
                )
                return

            self._errors_by_token[token_id] += 1
            errors = self._errors_by_token[token_id]

            backoff = min(MAX_BACKOFF_SECS, 2 ** min(errors - 1, 5))
            self._next_poll_at[token_id] = time.time() + backoff

            if errors == 1 or errors % 10 == 0:
                logger.warning(
                    "⚠️ PolymarketMidCache | poll error for …%s (×%d, backoff=%ss): %s",
                    token_id[-8:],
                    errors,
                    backoff,
                    exc,
                    exc_info=True,
                )

    def _set(self, token_id: str, mid: float) -> None:
        if 0.0 < mid < 1.0:
            self._prices[token_id] = mid
            self._ts[token_id] = time.time()


POLY_MID_CACHE = PolymarketMidCache()
#!/usr/bin/env python3
"""Polymarket mid-price cache — background HTTP polling."""

import asyncio
import logging
import time
from collections import defaultdict, deque
from typing import Any, Dict, Iterable, List, Optional, Set

from config import Config

logger = logging.getLogger(__name__)

# Legacy single-cadence default. When Config.POLY_ASYMMETRIC_POLLING is false
# (or no active set has been provided), every token is polled at this rate —
# preserving the original behavior exactly.
POLL_INTERVAL: float = 1.0
STALE_SECS: float = 3.0
MAX_BACKOFF_SECS: float = 30.0


def _is_http2_reset(exc: Exception) -> bool:
    """Return True for HTTP/2 GOAWAY/RST_STREAM errors that warrant an immediate retry.

    The Polymarket CLOB API occasionally terminates HTTP/2 connections mid-stream
    (error_code=1, RST_STREAM/GOAWAY). These are server-side connection resets that
    resolve on the very next attempt — not rate-limits or application errors.
    We detect them by class name rather than importing httpx/httpcore directly.
    """
    return "RemoteProtocolError" in type(exc).__name__ or "ConnectionTerminated" in str(exc)


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
        # Rolling window of absolute 1-second % changes per token (last 60 readings).
        # Used by order_manager to compute per-token volatility for SL scaling.
        self._price_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=60))
        # Serialises concurrent asyncio.to_thread CLOB calls: httpx.Client is not
        # thread-safe, so running multiple get_midpoint calls simultaneously causes
        # "deque mutated during iteration" errors inside httpx's connection pool.
        # Initialised lazily in run() so it is always bound to the running event loop.
        self._fetch_sem: Optional[asyncio.Semaphore] = None
        # Tokens currently considered "active" for asymmetric polling. Empty set
        # is treated as "all active" — a safe default for cold start before
        # the caller (BybitFeed) has determined direction. Populated by
        # set_active_tokens(); read in _cadence_for().
        self._active_tokens: Set[str] = set()

    def set_client(self, client: Any) -> None:
        """Inject the ClobClient. Must be called before run()."""
        self._client = client
        logger.info("✓ PolymarketMidCache | client set")

    def get(self, token_id: str) -> Optional[float]:
        """Return a fresh cached mid, or None (caller falls back to HTTP).

        Staleness threshold scales with the token's polling cadence: active
        tokens stay strict at STALE_SECS, watch tokens get cadence × 1.5 so
        their normal poll gaps don't look "stale" to callers. Without this,
        watch tokens would be marked stale ~40% of the time even when the
        cache is healthy — driving needless HTTP fallback for the
        near-resolved gate during downtrends.
        """
        ts = self._ts.get(token_id)
        if ts is None:
            self._miss += 1
            return None
        threshold = max(STALE_SECS, self._cadence_for(token_id) * 1.5)
        if (time.time() - ts) > threshold:
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

    def set_active_tokens(self, token_ids: Iterable[str]) -> None:
        """Mark which subscribed token IDs should poll at the active cadence.

        Tokens NOT in this set are polled at the slower 'watch' cadence
        (Config.POLY_POLL_INTERVAL_WATCH). Passing an empty iterable resets
        to 'all active' — a safe degradation that callers can use when they
        don't yet know which side is currently relevant (e.g. Bybit feed
        cold start, market data gaps).

        Replaces the previous set atomically. Safe to call from any thread
        because a bare assignment is GIL-atomic for set references.
        """
        self._active_tokens = set(token_ids)

    def _cadence_for(self, token_id: str) -> float:
        """Return the seconds-to-next-poll cadence for `token_id`.

        Falls back to the legacy single cadence when asymmetric polling is
        disabled OR when no active set has been provided yet (cold start).
        Both are safe degradations — they reduce to the prior behavior.
        """
        if not Config.POLY_ASYMMETRIC_POLLING:
            return POLL_INTERVAL
        if not self._active_tokens:
            return Config.POLY_POLL_INTERVAL_ACTIVE
        if token_id in self._active_tokens:
            return Config.POLY_POLL_INTERVAL_ACTIVE
        return Config.POLY_POLL_INTERVAL_WATCH

    def stats(self) -> str:
        total = self._hit + self._miss
        hit_rate = 100 * self._hit / total if total else 0
        # Active / watch counts (only meaningful when asymmetric polling is on).
        active_n = len(self._subscribed & self._active_tokens) if self._active_tokens else len(self._subscribed)
        watch_n  = len(self._subscribed) - active_n
        return (
            f"hits={self._hit} misses={self._miss} "
            f"hit_rate={hit_rate:.0f}% cached_tokens={len(self._prices)} "
            f"polls={self._polls} active={active_n} watch={watch_n}"
        )

    async def run(self) -> None:
        """Poll get_midpoint for every subscribed token at its per-token cadence.

        When asymmetric polling is enabled, the "active" side (matching Bybit's
        current direction) is polled at POLY_POLL_INTERVAL_ACTIVE; the "watch"
        side at POLY_POLL_INTERVAL_WATCH. The master sleep tracks the active
        cadence so watch tokens are re-evaluated at every active tick — they
        just defer themselves via _next_poll_at.
        """
        self._running = True
        # Create the semaphore here so it is always bound to the running event loop.
        self._fetch_sem = asyncio.Semaphore(1)

        if self._client is None:
            logger.error("✗ PolymarketMidCache | no client set — polling disabled")
            return

        if Config.POLY_ASYMMETRIC_POLLING:
            logger.info(
                "✓ PolymarketMidCache | polling loop started (asymmetric: active=%.1fs watch=%.1fs)",
                Config.POLY_POLL_INTERVAL_ACTIVE, Config.POLY_POLL_INTERVAL_WATCH,
            )
        else:
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

            # Master sleep is always the active cadence — watch tokens defer
            # themselves via _next_poll_at and won't be picked up here until
            # their per-token delay elapses.
            sleep_for = Config.POLY_POLL_INTERVAL_ACTIVE if Config.POLY_ASYMMETRIC_POLLING else POLL_INTERVAL
            await asyncio.sleep(sleep_for)

    async def _fetch_one(self, token_id: str) -> None:
        """Fetch and cache the mid-price for a single token.

        Uses a semaphore (self._fetch_sem) so only one get_midpoint call runs in a
        thread at a time — httpx.Client is not thread-safe and concurrent calls cause
        "deque mutated during iteration" inside its connection pool (R1 fix).

        HTTP/2 connection resets (RemoteProtocolError / ConnectionTerminated) are
        retried once immediately without backoff, since they always resolve on the next
        attempt and should not count as application errors (R4 fix).
        """
        last_exc: Optional[Exception] = None

        for attempt in range(2):  # attempt 0 = first try; attempt 1 = retry after HTTP/2 reset
            try:
                assert self._fetch_sem is not None, "semaphore not initialised — call run() first"
                async with self._fetch_sem:
                    resp = await asyncio.to_thread(self._client.get_midpoint, token_id)

                if isinstance(resp, dict) and "mid" in resp:
                    mid = float(resp["mid"])
                    was_new = token_id not in self._prices
                    self._set(token_id, mid)
                    self._polls += 1
                    self._errors_by_token[token_id] = 0
                    # Schedule the next poll at this token's cadence. For
                    # active tokens this is essentially the current behavior
                    # (poll again next loop iteration); for watch tokens it
                    # defers by the watch interval, which is what gives us
                    # the API savings.
                    self._next_poll_at[token_id] = time.time() + self._cadence_for(token_id)

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
                # HTTP/2 GOAWAY / RST_STREAM — retry once immediately, no backoff
                if attempt == 0 and _is_http2_reset(exc):
                    logger.debug(
                        "PolymarketMidCache | HTTP/2 connection reset for …%s — retrying",
                        token_id[-8:],
                    )
                    last_exc = exc
                    continue

                last_exc = exc
                break  # fall through to error handling

        # ── Error handling (reached only when both attempts fail or non-reset error) ──
        exc = last_exc  # type: ignore[assignment]
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
            # Transient HTTP/2 stream resets against the Polymarket CLOB are
            # routine (status_code=None, "Request exception!"). The backoff loop
            # auto-recovers, so don't dump the full ~50-frame httpx traceback
            # for isolated blips — only log if it *persists*.
            if errors < 3:
                logger.debug(
                    "PolymarketMidCache | poll error for …%s (×%d, backoff=%ss): %s",
                    token_id[-8:], errors, backoff, exc,
                )
            elif errors == 3 or errors % 10 == 0:
                logger.warning(
                    "⚠️ PolymarketMidCache | poll error for …%s (×%d, backoff=%ss): %s",
                    token_id[-8:], errors, backoff, exc,
                )

    def get_volatility(self, token_id: str, min_samples: int = 10) -> Optional[float]:
        """Return the rolling std-dev of 1-second mid-price % changes for a token.

        Uses the last 60 polled readings (≈60 seconds of data at 1 Hz).
        Returns None when fewer than min_samples points are available — callers
        should fall back to the static SL base in that case.
        """
        history = self._price_history.get(token_id)
        if not history or len(history) < min_samples:
            return None
        samples = list(history)
        n = len(samples)
        mean = sum(samples) / n
        variance = sum((x - mean) ** 2 for x in samples) / n
        return variance ** 0.5

    def _set(self, token_id: str, mid: float) -> None:
        if 0.0 < mid < 1.0:
            prev = self._prices.get(token_id)
            self._prices[token_id] = mid
            self._ts[token_id] = time.time()
            # Record absolute % change from the previous reading for volatility tracking.
            if prev is not None and prev > 0:
                pct_change = abs((mid - prev) / prev) * 100
                self._price_history[token_id].append(pct_change)


POLY_MID_CACHE = PolymarketMidCache()

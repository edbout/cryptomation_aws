"""Lightweight in-memory store for vetoed signals.

Written by BybitManager.get_signal() whenever a suppression fires on a
signal that had a valid direction (side is not None).

Drained by two paths (whichever runs first pops the entry — pop is
GIL-atomic so no double-emission can occur):

1. OrderManager.polymarket_order_outcome() — fires only when a real
   trade was placed in the same (asset, epoch) bucket and resolves.
   Coverage: ~1% of suppressions (epochs friendly enough that something
   else got through anyway). Pre-existing path, kept for backward compat.

2. resolve_loop() — independent background coroutine that scans _STORE
   on a timer (every SCAN_INTERVAL seconds), and for each entry whose
   bar has closed + RESOLVE_DELAY seconds buffer, looks up the
   Polymarket token mid via POLY_MID_CACHE / get_midpoint, classifies the
   outcome, and emits the 🔍 suppressed_outcome line directly. Coverage:
   100% of suppressions (independent of any real-trade placement).

Both paths emit the same log format so downstream analysis is uniform:
  🔍 suppressed_outcome | {asset} | vetoed_dir={UP/DOWN} | epoch={ts} |
     resolved={YES/NO} | would_be={WIN/LOSS}

Design notes
------------
- Module-level singleton dict — single asyncio thread, no locks.
- Keyed by (asset_usdt, epoch_ts). Only the first suppression per pair
  is stored; subsequent suppressions in the same epoch window are no-ops.
- Auto-pruned on every read/write: entries older than _TTL_SECS are
  dropped. TTL > 10 min so the resolver always gets a chance.
- The resolver uses POLY_MID_CACHE (zero-cost, already polled) when a
  mid is available; falls back to a single client.get_midpoint() call
  only when the cache has nothing. Token discovery uses PolymarketFinder
  whose Redis cache makes repeat lookups for active markets essentially
  free.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Dict, Optional, Tuple

from config import Config
from lib.polymarket_mid_cache import POLY_MID_CACHE

logger = logging.getLogger(__name__)

# (asset_usdt e.g. "ETHUSDT", epoch_ts e.g. 1779066300) → entry dict
_STORE: Dict[Tuple[str, int], dict] = {}

# Keep entries long enough to survive through epoch resolution + retries.
# 15 min >> 5-min epoch + resolve delay + a couple of resolver scans.
_TTL_SECS: int = 900


def record(asset_usdt: str, epoch_ts: int, vetoed_dir: str) -> None:
    """Record the first suppressed signal for (asset, epoch).

    Called from get_signal() immediately before returning None when a
    suppression fires on a signal with a valid direction (side not None).

    Args:
        asset_usdt: canonical Polymarket asset string, e.g. "ETHUSDT"
        epoch_ts:   UTC Unix timestamp of the current 5-min bar start
        vetoed_dir: "UP" or "DOWN" — direction the signal intended to trade
    """
    _prune()
    key = (asset_usdt, epoch_ts)
    if key not in _STORE:
        _STORE[key] = {"vetoed_dir": vetoed_dir, "suppressed_at": time.time()}


def pop(asset_usdt: str, epoch_ts: int) -> Optional[dict]:
    """Retrieve and remove the suppression record for (asset, epoch), if any.

    Called from polymarket_order_outcome() after the market resolves.
    Returns None when no suppressed signal was recorded for this pair
    (either no suppression happened, or the resolver loop already popped it).

    Args:
        asset_usdt: e.g. "ETHUSDT"
        epoch_ts:   epoch start timestamp parsed from the market slug
    """
    _prune()
    return _STORE.pop((asset_usdt, epoch_ts), None)


def _prune() -> None:
    """Drop entries older than _TTL_SECS to bound memory usage."""
    cutoff = time.time() - _TTL_SECS
    stale = [k for k, v in _STORE.items() if v["suppressed_at"] < cutoff]
    for k in stale:
        del _STORE[k]


# ── Independent resolver loop ───────────────────────────────────────────────
#
# Started from main.py via asyncio.create_task(resolve_loop(client, finder)).
# Runs forever; one entry per (asset, epoch) is resolved exactly once because
# pop() is GIL-atomic — either this loop wins the race with order_manager's
# pop or vice versa.


# Asset (USDT form) → Polymarket asset stem used by PolymarketFinder.
# Mirrors normalize_polymarket_asset's expected input.
_ASSET_TO_PM_STEM = {
    "BTCUSDT": "btc",
    "ETHUSDT": "eth",
    "XRPUSDT": "xrp",
    "SOLUSDT": "sol",
}


async def resolve_loop(client: Any, finder: Any) -> None:
    """Background coroutine that resolves overdue suppression entries.

    Args:
        client: Polymarket CLOB client (used for fallback get_midpoint
                only — primary lookup goes through POLY_MID_CACHE).
        finder: PolymarketFinder instance (Redis-cached slug lookup).
    """
    if not getattr(Config, "SUPPRESSED_OUTCOME_INDEPENDENT_ENABLED", True):
        logger.info("✓ suppression resolve_loop disabled (SUPPRESSED_OUTCOME_INDEPENDENT_ENABLED=false)")
        return

    delay = int(getattr(Config, "SUPPRESSED_OUTCOME_RESOLVE_DELAY_SEC", 25))
    interval = int(getattr(Config, "SUPPRESSED_OUTCOME_SCAN_INTERVAL_SEC", 30))
    logger.info(
        f"✓ suppression resolve_loop starting | resolve_delay={delay}s | scan_interval={interval}s"
    )

    # Per-(asset, epoch) retry counter so we don't spin forever on tokens
    # whose mid never lands outside [0.25, 0.75] (rare resolution lag).
    retry_counts: Dict[Tuple[str, int], int] = {}
    MAX_RETRIES = 3

    while True:
        try:
            await _resolve_pass(client, finder, delay, retry_counts, MAX_RETRIES)
        except Exception as e:
            logger.error(f"💥 suppression resolve_loop pass failed: {e}", exc_info=True)
        await asyncio.sleep(interval)


async def _resolve_pass(
    client: Any,
    finder: Any,
    delay: int,
    retry_counts: Dict[Tuple[str, int], int],
    max_retries: int,
) -> None:
    """One scan of _STORE: resolve every entry whose bar closed ≥ `delay` ago."""
    now = time.time()
    # Snapshot keys to allow safe mutation during iteration.
    candidates = []
    for (asset, epoch), entry in list(_STORE.items()):
        bar_end = epoch + 300
        if now < bar_end + delay:
            continue  # not yet ready
        candidates.append((asset, epoch, entry))

    if not candidates:
        return

    for asset, epoch, entry in candidates:
        try:
            await _resolve_one(client, finder, asset, epoch, entry, retry_counts, max_retries)
        except Exception as e:
            logger.warning(
                f"⚠️ suppressed_outcome resolve failed | {asset} epoch={epoch}: {e}"
            )


async def _resolve_one(
    client: Any,
    finder: Any,
    asset: str,
    epoch: int,
    entry: dict,
    retry_counts: Dict[Tuple[str, int], int],
    max_retries: int,
) -> None:
    """Resolve a single (asset, epoch) suppression and emit the log line.

    Atomically pops only on success or terminal failure so that in-flight
    races with order_manager.pop() don't double-emit. The order_manager
    path emits the same format from its own pop(), so whoever wins the race
    produces exactly one log line per suppression.
    """
    key = (asset, epoch)

    # Step 1 — find the YES token for this asset+epoch via PolymarketFinder.
    # Slug format: "{stem}-updown-5m-{epoch}". The finder's _fetch_market_by_slug
    # uses a 60s Redis cache, so repeat lookups for the same epoch are free.
    stem = _ASSET_TO_PM_STEM.get(asset)
    if stem is None:
        logger.debug(f"⏭️ suppressed_outcome | {asset} epoch={epoch}: no PM stem mapping; dropping")
        _STORE.pop(key, None)
        return

    slug = f"{stem}-updown-5m-{epoch}"
    market = await asyncio.to_thread(finder._fetch_market_by_slug, slug)
    if not market or market.get("error") == "not_found":
        # Market never existed (e.g. asset paused at the time) — terminal,
        # no point retrying.
        logger.debug(f"⏭️ suppressed_outcome | {asset} epoch={epoch}: market {slug} not found")
        _STORE.pop(key, None)
        return

    raw_tokens = market.get("clobTokenIds")
    if not raw_tokens:
        logger.debug(f"⏭️ suppressed_outcome | {asset} epoch={epoch}: no clobTokenIds in market")
        _STORE.pop(key, None)
        return
    try:
        token_list = json.loads(raw_tokens) if isinstance(raw_tokens, str) else raw_tokens
        token_yes = token_list[0]
    except (json.JSONDecodeError, IndexError, TypeError) as e:
        logger.debug(f"⏭️ suppressed_outcome | {asset} epoch={epoch}: token parse failed: {e}")
        _STORE.pop(key, None)
        return

    # Step 2 — fetch the YES-token resolution price. We always classify
    # against the YES side and translate to bought-side outcome below.
    yes_mid: Optional[float] = POLY_MID_CACHE.get(token_yes)
    if yes_mid is None:
        try:
            resp = await asyncio.to_thread(client.get_midpoint, token_yes)
            yes_mid = float((resp or {}).get("mid") or 0.0)
        except Exception as e:
            logger.debug(f"⏭️ suppressed_outcome | {asset} epoch={epoch}: midpoint fetch failed: {e}")
            yes_mid = None

    # Step 3 — classify. Leave ambiguous (mid in [0.25, 0.75]) for retry,
    # bounded by max_retries so we don't loop forever on a stuck market.
    if yes_mid is None or 0.25 <= yes_mid <= 0.75:
        attempts = retry_counts.get(key, 0) + 1
        retry_counts[key] = attempts
        if attempts < max_retries:
            return  # leave entry in _STORE, try again on next scan
        # Give up — record an indeterminate outcome and drop.
        logger.info(
            f"\U0001f50d suppressed_outcome | {asset} | vetoed_dir={entry['vetoed_dir']} | "
            f"epoch={epoch} | resolved=NA | would_be=NA | (max_retries reached, yes_mid={yes_mid})"
        )
        _STORE.pop(key, None)
        retry_counts.pop(key, None)
        return

    # yes_mid is clear. Determine which side won.
    resolved = "YES" if yes_mid > 0.75 else "NO"  # yes_mid < 0.25 ⇒ NO resolved
    vetoed_dir = entry["vetoed_dir"]
    # UP signal would buy YES; DOWN would buy NO. would_be=WIN iff the
    # bought side matches the resolved side.
    bought_side = "YES" if vetoed_dir == "UP" else "NO"
    would_be = "WIN" if bought_side == resolved else "LOSS"

    # Step 4 — atomically pop and emit. If order_manager.pop() already
    # claimed this entry, popped is None and we skip silently (it already
    # emitted from its side).
    popped = _STORE.pop(key, None)
    retry_counts.pop(key, None)
    if popped is None:
        return

    logger.info(
        f"\U0001f50d suppressed_outcome | {asset} | vetoed_dir={vetoed_dir} | "
        f"epoch={epoch} | resolved={resolved} | would_be={would_be}"
    )

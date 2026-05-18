"""Lightweight in-memory store for vetoed signals.

Written by BybitManager.get_signal() whenever a suppression fires on a
signal that had a valid direction (side is not None).

Read by OrderManager.polymarket_order_outcome() to emit a
  🔍 suppressed_outcome | …
log line so we can measure OBI veto effectiveness over time.

Design notes
------------
- Module-level singleton dict — no class needed, single asyncio thread.
- Keyed by (asset_usdt, epoch_ts) so each (asset, 5-min window) pair
  maps to exactly one entry.  Only the *first* suppression per pair is
  stored; subsequent suppressions in the same epoch window are no-ops.
- Auto-pruned on every read/write: entries older than _TTL_SECS are
  dropped.  TTL = 10 min >> 5-min epoch, so the outcome call always
  arrives before the entry expires.
- No Redis, no I/O, no locks.  Zero trading impact.
"""
from __future__ import annotations

import time
from typing import Dict, Optional, Tuple

# (asset_usdt e.g. "ETHUSDT", epoch_ts e.g. 1779066300) → entry dict
_STORE: Dict[Tuple[str, int], dict] = {}

# Keep entries long enough to survive through epoch resolution.
# 10 min >> 5-min epoch, so the outcome callback always arrives in time.
_TTL_SECS: int = 600


def record(asset_usdt: str, epoch_ts: int, vetoed_dir: str) -> None:
    """Record the first suppressed signal for (asset, epoch).

    Called from get_signal() immediately before returning None when a
    suppression fires on a signal with a valid direction (side not None).

    Args:
        asset_usdt: canonical Polymarket asset string, e.g. "ETHUSDT"
        epoch_ts:   UTC Unix timestamp of the current 5-min bar start,
                    as returned by get_current_5m_bar_ts(time.time())
        vetoed_dir: "UP" or "DOWN" — direction the signal intended to trade
    """
    _prune()
    key = (asset_usdt, epoch_ts)
    if key not in _STORE:
        _STORE[key] = {"vetoed_dir": vetoed_dir, "suppressed_at": time.time()}


def pop(asset_usdt: str, epoch_ts: int) -> Optional[dict]:
    """Retrieve and remove the suppression record for (asset, epoch), if any.

    Called from polymarket_order_outcome() after the market resolves.
    Returns None when no suppressed signal was recorded for this pair.

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

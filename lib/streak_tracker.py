"""Per-asset consecutive-loss streak tracker for the streak circuit-breaker.

Written by OrderManager.polymarket_order_outcome() after every resolved trade.
Read by BybitManager.get_signal() to decide whether to suppress (live) or
shadow-log (shadow mode, the current default).

State per asset
---------------
  consecutive_losses : int   — resets to 0 on a win; increments on a loss
  last_loss_at       : float — Unix timestamp of the most recent loss

Pause logic
-----------
A signal is paused when:
  consecutive_losses >= Config.STREAK_PAUSE_MIN_LOSSES (default 2)
  AND
  time.time() - last_loss_at < Config.STREAK_PAUSE_COOLDOWN (default 900s / 15 min)

The cooldown runs from the most recent loss, so it auto-extends as long as
losses keep coming.  A win at any point resets consecutive_losses to 0.

Design notes
------------
- Module-level singleton dict — asyncio single-thread, no locks needed.
- Zero I/O, zero Redis, zero trading impact in shadow mode.
- Config knobs are read at call time so they can be changed without restart
  (useful for tuning the threshold during the shadow-mode evaluation period).
"""
from __future__ import annotations

import time
from typing import Dict, Optional, Tuple

# Per-asset state
_losses: Dict[str, int] = {}            # consecutive loss count
_last_loss_at: Dict[str, float] = {}    # Unix ts of most recent loss


def record_outcome(asset_usdt: str, won: bool) -> None:
    """Update streak state after a resolved trade.

    Args:
        asset_usdt: canonical Polymarket asset string, e.g. "ETHUSDT"
        won:        True if the trade was profitable (result > 0)
    """
    if won:
        _losses[asset_usdt] = 0
        _last_loss_at.pop(asset_usdt, None)
    else:
        _losses[asset_usdt] = _losses.get(asset_usdt, 0) + 1
        _last_loss_at[asset_usdt] = time.time()


def check(asset_usdt: str, min_losses: int, cooldown_secs: float) -> Tuple[bool, int, float]:
    """Return streak state for asset at the moment of a get_signal() call.

    Args:
        asset_usdt:    e.g. "ETHUSDT"
        min_losses:    Config.STREAK_PAUSE_MIN_LOSSES
        cooldown_secs: Config.STREAK_PAUSE_COOLDOWN

    Returns:
        (paused, n_losses, secs_remaining)
          paused        — True when the cooldown window is still active
          n_losses      — current consecutive loss count for this asset
          secs_remaining — seconds left in the cooldown (0.0 when not paused)
    """
    n = _losses.get(asset_usdt, 0)
    last = _last_loss_at.get(asset_usdt)

    if n >= min_losses and last is not None:
        elapsed = time.time() - last
        remaining = cooldown_secs - elapsed
        if remaining > 0:
            return True, n, remaining

    return False, n, 0.0

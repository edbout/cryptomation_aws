"""
Raw-signal trader: places small fire-and-forget GTD limit orders on every
raw signal that passes alignment / OBI / fairvalue filters, BEFORE the
edge check is applied.

Purpose
-------
Validate at live execution whether the raw-signal directional accuracy
seen on the paper-trade tracker (84-91% on the /stats dashboard) holds
up when traded for real, with no SL/TP/Trail management. These trades
are intentionally NOT touched by `order_manager.manage_positions` — they
sit as limit orders until they fill or expire at bar end, then resolve.

Co-existence
------------
This runs IN PARALLEL with the normal Kelly-sized trade path. Both can
fire on the same signal independently; they are tracked separately and
do not share state. The user-facing dashboard reads `stats:trade:{asset}`
for the main strategy and `stats:raw_signal_trade:{asset}` for this one.

Safety
------
• `Config.RAW_SIGNAL_TRADER_ENABLED` — master kill switch (default OFF).
• `Config.RAW_SIGNAL_THROTTLE_SEC` — atomic per-token-id throttle in Redis.
• `Config.RAW_SIGNAL_MAX_DAILY_USD` — hard daily spend cap (UTC-bucketed).
• `Config.DRY_RUN` — when true, logs the would-be order and does not call
  the CLOB. Stats and outcomes are still recorded so the path can be
  exercised end-to-end without spending money.

Redis schema
------------
  raw_signal_trade:throttle:{token_id}    SETNX TTL (per-token cooldown)
  raw_signal_trade:daily_spend:{YYYYMMDD} INCRBYFLOAT (daily cap counter)
  raw_signal_trade:order:{order_id}       HSET (one record per placement)
  stats:raw_signal_trade:{asset}          HINCRBY (placed/resolved/win/loss/na/unfilled)

Outcome semantics
-----------------
At bar end + buffer we query the token midpoint. Polymarket binary
markets resolve to ~1.0 (YES wins) or ~0.0 (NO wins). We classify:
  > 0.75 → win   (token we bought resolved YES)
  < 0.25 → loss  (token we bought resolved NO)
  else   → na    (resolution lag or market not yet resolved)

A separate `unfilled` counter tracks limit orders that expired without
filling — those are not losses, they are non-events. Fill detection is
best-effort via `client.get_open_orders`; on error the order is recorded
as `unknown_fill` and excluded from the win-rate calculation.
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Any, Optional

import py_clob_client_v2.order_builder.constants as OrderConstants
from py_clob_client_v2.clob_types import OrderArgsV2 as OrderArgs, OrderType, OpenOrderParams

from config import Config, RedisCache

logger = logging.getLogger(__name__)


# ── Redis key helpers ──────────────────────────────────────────────────────

def _throttle_key(token_id: str) -> str:
    return f"raw_signal_trade:throttle:{token_id}"


def _daily_cap_key() -> str:
    """UTC-bucketed daily spend counter — resets at 00:00 UTC."""
    bucket = time.strftime("%Y%m%d", time.gmtime())
    return f"raw_signal_trade:daily_spend:{bucket}"


def _order_key(order_id: str) -> str:
    return f"raw_signal_trade:order:{order_id}"


def _stats_key(asset: str) -> str:
    return f"stats:raw_signal_trade:{asset}"


# ── Public entry point ────────────────────────────────────────────────────

async def place_raw_signal_order(
    client: Any,
    asset: str,
    token_id: str,
    mid_price: float,
    trigger_minute: int,
    candle_seconds: int,
) -> None:
    """Fire-and-forget: try to place a $1 GTD limit order on this raw signal.

    All exceptions are swallowed — this MUST NOT impact the main trading
    pipeline. Call site (`order_manager.validate_adjust_price`) wraps it
    in a task; we add an extra defensive try/except here.
    """
    if not Config.RAW_SIGNAL_TRADER_ENABLED:
        return
    if mid_price <= 0 or mid_price >= 1.0:
        return

    try:
        await _place_inner(client, asset, token_id, mid_price, trigger_minute, candle_seconds)
    except Exception as e:
        logger.error(f"💥 raw_signal_trade {asset} | unexpected error: {e}", exc_info=True)


async def _place_inner(
    client: Any,
    asset: str,
    token_id: str,
    mid_price: float,
    trigger_minute: int,
    candle_seconds: int,
) -> None:
    redis = RedisCache().client
    if not redis:
        logger.warning(f"⚠️ raw_signal_trade {asset} | redis unavailable, skip")
        return

    # ── Per-token throttle ────────────────────────────────────────────────
    # SETNX + TTL is atomic: only the first caller within the window claims.
    throttle_key = _throttle_key(token_id)
    try:
        claimed = redis.set(throttle_key, "1", nx=True, ex=Config.RAW_SIGNAL_THROTTLE_SEC)
    except Exception as e:
        logger.warning(f"⚠️ raw_signal_trade {asset} | throttle check failed: {e}")
        return
    if not claimed:
        logger.debug(
            f"🚫 raw_signal_trade {asset} | throttled token={token_id[:12]} "
            f"(≤1 per {Config.RAW_SIGNAL_THROTTLE_SEC}s)"
        )
        return

    # ── Daily $ cap (circuit-breaker) ────────────────────────────────────
    cap_key = _daily_cap_key()
    try:
        spend_so_far = float(redis.get(cap_key) or 0.0)
    except Exception:
        spend_so_far = 0.0
    if spend_so_far + Config.RAW_SIGNAL_SIZE_USD > Config.RAW_SIGNAL_MAX_DAILY_USD:
        logger.warning(
            f"🚫 raw_signal_trade {asset} | daily cap hit "
            f"(${spend_so_far:.2f} + ${Config.RAW_SIGNAL_SIZE_USD:.2f} > "
            f"${Config.RAW_SIGNAL_MAX_DAILY_USD:.2f}) — skip"
        )
        return

    # ── Compute limit price & share count ────────────────────────────────
    # Limit at LIMIT_MULT × mid (default 0.95 = 5% discount). Clamp to the
    # Polymarket-valid range [0.01, 0.99]; round to 4dp.
    raw_limit = mid_price * Config.RAW_SIGNAL_LIMIT_MULT
    limit_price = round(max(0.01, min(0.99, raw_limit)), 4)
    # Shares so the total $ matches RAW_SIGNAL_SIZE_USD. Round down so we
    # never overspend; ensure ≥1 share (Polymarket minimum).
    shares = max(1.0, round(Config.RAW_SIGNAL_SIZE_USD / limit_price, 2))
    expiration_ts = int(time.time()) + Config.RAW_SIGNAL_EXPIRATION_SEC

    order_id: Optional[str] = None
    status = "dryrun"

    if Config.DRY_RUN:
        order_id = f"dryrun_raw_{uuid.uuid4().hex[:12]}"
        logger.info(
            f"🧪 raw_signal_trade {asset} DRY | token={token_id[:12]} | "
            f"mid={mid_price:.4f} → limit={limit_price:.4f} × {shares:.2f}sh "
            f"(${Config.RAW_SIGNAL_SIZE_USD:.2f}) | tm={trigger_minute} cs={candle_seconds} | "
            f"exp=+{Config.RAW_SIGNAL_EXPIRATION_SEC}s"
        )
    else:
        try:
            args = OrderArgs(
                token_id=token_id,
                price=limit_price,
                size=shares,
                side=OrderConstants.BUY,
                expiration=expiration_ts,
            )
            signed = await asyncio.to_thread(client.create_order, args)
            resp = await asyncio.to_thread(client.post_order, signed, OrderType.GTD)
        except Exception as e:
            logger.warning(f"✗ raw_signal_trade {asset} | place_order raised: {e}")
            _bump_stat(redis, asset, "place_error")
            return

        status = (resp or {}).get("status", "?")
        order_id = (resp or {}).get("orderID") or (resp or {}).get("orderId")
        if not order_id or status not in ("matched", "live", "delayed"):
            logger.warning(
                f"✗ raw_signal_trade {asset} | place failed status={status} resp={resp}"
            )
            _bump_stat(redis, asset, "place_failed")
            return
        logger.info(
            f"📥 raw_signal_trade {asset} | token={token_id[:12]} | "
            f"mid={mid_price:.4f} → limit={limit_price:.4f} × {shares:.2f}sh "
            f"(${Config.RAW_SIGNAL_SIZE_USD:.2f}) | tm={trigger_minute} cs={candle_seconds} | "
            f"exp=+{Config.RAW_SIGNAL_EXPIRATION_SEC}s | status={status} id={str(order_id)[:8]}"
        )

    # ── Reserve against daily cap ────────────────────────────────────────
    try:
        redis.incrbyfloat(cap_key, Config.RAW_SIGNAL_SIZE_USD)
        redis.expire(cap_key, 86400 * 2)
    except Exception as e:
        logger.warning(f"⚠️ raw_signal_trade {asset} | daily cap incr failed: {e}")

    # ── Persist order record + bump placed counter ───────────────────────
    record = {
        "asset": asset,
        "token_id": token_id,
        "order_id": str(order_id),
        "mid_price": mid_price,
        "limit_price": limit_price,
        "shares": shares,
        "size_usd": Config.RAW_SIGNAL_SIZE_USD,
        "trigger_minute": trigger_minute,
        "candle_seconds": candle_seconds,
        "placed_ts": int(time.time()),
        "expiration_ts": expiration_ts,
        "status": status,
        "dry_run": int(Config.DRY_RUN),
    }
    try:
        redis.hset(_order_key(str(order_id)), mapping={k: str(v) for k, v in record.items()})
        redis.expire(_order_key(str(order_id)), 86400 * 7)
    except Exception as e:
        logger.warning(f"⚠️ raw_signal_trade {asset} | record write failed: {e}")

    _bump_stat(redis, asset, "placed")

    # ── Schedule outcome resolution at bar end ───────────────────────────
    # Delay = expiration + small buffer so the limit has fully expired and
    # the Polymarket mid has had time to settle on the resolved side.
    delay = Config.RAW_SIGNAL_EXPIRATION_SEC + 20
    asyncio.create_task(
        _delayed_resolve(client, asset, str(order_id), token_id, limit_price, shares, delay)
    )


# ── Outcome resolution ────────────────────────────────────────────────────

async def _delayed_resolve(
    client: Any,
    asset: str,
    order_id: str,
    token_id: str,
    limit_price: float,
    shares: float,
    delay: float,
) -> None:
    try:
        await asyncio.sleep(delay)
        await _resolve_outcome(client, asset, order_id, token_id, limit_price, shares)
    except Exception as e:
        logger.error(f"💥 raw_signal_trade_outcome {asset} | scheduler error: {e}", exc_info=True)


async def _resolve_outcome(
    client: Any,
    asset: str,
    order_id: str,
    token_id: str,
    limit_price: float,
    shares: float,
) -> None:
    """Determine whether the limit order filled, and if so, the bar-end outcome.

    Fill detection: query open orders for the token; if order_id is no longer
    open AND wasn't seen as expired, treat it as filled. Best-effort — any
    error falls back to "unknown_fill" which is excluded from win-rate.

    Outcome classification (only meaningful when filled):
      mid > 0.75 → win   (bought token resolved YES)
      mid < 0.25 → loss  (bought token resolved NO)
      else       → na    (resolution lag / market in flight)
    """
    redis = RedisCache().client
    if not redis:
        return

    # ── Step 1: Detect whether the order filled ──────────────────────────
    filled: Optional[bool] = None
    if Config.DRY_RUN:
        # In DRY mode we don't actually have an order on the exchange. Treat
        # as "always filled" so the outcome path is exercised end-to-end.
        filled = True
    else:
        try:
            params = OpenOrderParams(asset_id=token_id)
            open_orders = await asyncio.to_thread(client.get_open_orders, params)
            still_open_ids = {str(o.get("id") or o.get("orderID")) for o in (open_orders or [])}
            filled = str(order_id) not in still_open_ids
        except Exception as e:
            logger.warning(f"⚠️ raw_signal_trade_outcome {asset} | fill check failed: {e}")
            filled = None  # unknown

    if filled is False:
        # Limit expired without filling — record and exit.
        _bump_stat(redis, asset, "unfilled")
        _hset_outcome(redis, order_id, "unfilled", None, None)
        logger.info(
            f"📥 raw_signal_trade_outcome {asset} | order={str(order_id)[:8]} | "
            f"unfilled (limit={limit_price:.4f})"
        )
        return
    if filled is None:
        _bump_stat(redis, asset, "unknown_fill")
        _hset_outcome(redis, order_id, "unknown_fill", None, None)
        return

    # ── Step 2: Fetch the resolution price ───────────────────────────────
    # Mirrors order_manager.polymarket_order_outcome: midpoint first, fall
    # back to last trade if midpoint is still mid-range.
    resolved_price: Optional[float] = None
    try:
        mid_resp = await asyncio.to_thread(client.get_midpoint, token_id)
        resolved_price = float((mid_resp or {}).get("mid") or 0.0)
        if 0.0 < resolved_price <= 0.75:
            try:
                last_resp = await asyncio.to_thread(client.get_last_trade_price, token_id)
                if isinstance(last_resp, dict):
                    resolved_price = float(last_resp.get("price") or resolved_price)
                elif last_resp:
                    resolved_price = float(last_resp[0])
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"⚠️ raw_signal_trade_outcome {asset} | price fetch failed: {e}")
        _bump_stat(redis, asset, "price_error")
        _hset_outcome(redis, order_id, "price_error", None, None)
        return

    # ── Step 3: Classify and record ──────────────────────────────────────
    if resolved_price is None or resolved_price <= 0 or resolved_price >= 1:
        outcome = "na"
        pnl_usd = 0.0
    elif resolved_price > 0.75:
        outcome = "win"
        pnl_usd = round((1.0 - limit_price) * shares, 4)  # YES resolves to ~1.0
    elif resolved_price < 0.25:
        outcome = "loss"
        pnl_usd = round((0.0 - limit_price) * shares, 4)  # token went to ~0.0
    else:
        outcome = "na"
        pnl_usd = 0.0

    _bump_stat(redis, asset, "resolved")
    _bump_stat(redis, asset, outcome)
    try:
        # Track cumulative $ PnL on the strategy.
        redis.hincrbyfloat(_stats_key(asset), "pnl_usd", pnl_usd)
    except Exception:
        pass
    _hset_outcome(redis, order_id, outcome, resolved_price, pnl_usd)

    logger.info(
        f"📥 raw_signal_trade_outcome {asset} | order={str(order_id)[:8]} | "
        f"entry={limit_price:.4f} → resolved={resolved_price:.4f} | "
        f"{outcome} | pnl=${pnl_usd:+.4f}"
    )


# ── Small helpers ─────────────────────────────────────────────────────────

def _bump_stat(redis: Any, asset: str, field: str) -> None:
    try:
        redis.hincrby(_stats_key(asset), field, 1)
    except Exception:
        pass


def _hset_outcome(
    redis: Any,
    order_id: str,
    outcome: str,
    resolved_price: Optional[float],
    pnl_usd: Optional[float],
) -> None:
    try:
        mapping = {
            "outcome": outcome,
            "outcome_ts": str(int(time.time())),
        }
        if resolved_price is not None:
            mapping["resolved_price"] = str(round(resolved_price, 4))
        if pnl_usd is not None:
            mapping["pnl_usd"] = str(round(pnl_usd, 4))
        redis.hset(_order_key(order_id), mapping=mapping)
    except Exception:
        pass

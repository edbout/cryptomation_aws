#!/usr/bin/env python3
"""Order management for Polymarket trading bot."""
import asyncio
import json
import logging
import time
import requests
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple, List
from zoneinfo import ZoneInfo
import py_clob_client_v2.order_builder.constants as OrderConstants
from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.clob_types import MarketOrderArgsV2 as MarketOrderArgs, OrderArgsV2 as OrderArgs, OrderType, BalanceAllowanceParams, AssetType, OrderPayload, OpenOrderParams
from py_clob_client_v2.exceptions import PolyApiException

from config import Config, RedisCache
from price_tracker import PriceTracker
from lib.polymarket_mid_cache import POLY_MID_CACHE
from lib.polymarket_positions import PolymarketPositionManager
from lib.helpers import safe_float, get_utc_now, get_seconds_since_5m_start, get_current_5m_bar_ts
from lib.telegram_alert import send_alert as _tg_alert_async

UTC = ZoneInfo("UTC")

logger = logging.getLogger(__name__)

class OrderManager:
    """Centralized order management with position tracking and closing."""
    def __init__(self, client: ClobClient):
        self.client = client
        # V2 (EOA trading, signature_type=0): positions held by the EOA, not the proxy wallet.
        eoa_address = client.get_address() or Config.PROXY_WALLET
        self.position_manager = PolymarketPositionManager(eoa_address)
        self.redis = RedisCache()
        self.tracker = PriceTracker()
        self._pending_tasks = []
        self._weak_signal_last_bar: Dict[str, int] = {}  # asset → bar_start of last logged weak signal
        self._no_stats_last_bar: Dict[str, int] = {}   # asset → bar_start of last logged should_trade=False

    def get_direction(self, symbol: str) -> tuple[str, float, float]:
        """Returns ("BUY"/"SELL"/"ERROR", open_price, close_price) for CURRENT 5min candle."""
        try:
            bybit_symbol = symbol.replace('USDT', 'USD')
            url = f"https://api.bybit.com/v5/market/kline?category=inverse&symbol={bybit_symbol}&interval=5&limit=1"
            resp = requests.get(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
            
            if resp.status_code != 200:
                logger.warning(f"get_direction | Bybit API {resp.status_code} for {bybit_symbol}")
                return "ERROR", 0.0, 0.0
            
            data = resp.json()
            if data.get('retCode') != 0 or not data['result']['list']:
                logger.warning(f"get_direction | Bybit empty data: {data.get('retMsg', 'Unknown')}")
                return "ERROR", 0.0, 0.0
            
            # CURRENT candle (index 0, may be incomplete) - Bybit: NEWEST first
            current_candle = data['result']['list'][0]
            open_price = float(current_candle[1])
            close_price = float(current_candle[4])
            
            direction = "BUY" if close_price >= open_price else "SELL"
            
            pct_change = ((close_price - open_price) / open_price) * 100
            logger.debug(f"get_direction | Bybit 5m {bybit_symbol}: {direction} (O:{open_price:.4f}→C:{close_price:.4f} | {pct_change:+.2f}%)")
            
            return direction, open_price, close_price
            
        except Exception as e:
            logger.error(f"get_direction | Exception for {symbol}: {e}")
            return "ERROR", 0.0, 0.0
        
    async def get_active_positions(self, minvalue: float = 0.01) -> List[Dict[str, Any]]:
        """Get active positions via shared manager (thread-safe)."""
        return await asyncio.to_thread(self.position_manager.get_active_positions, minvalue)

    async def update_price(self, asset: str, price: float, percentage: float, trigger_minute: int) -> Optional[Dict]:
        """
        Async price update → returns tracker result hash directly.
        """
        loop = asyncio.get_running_loop()
        
        # 🔥 Returns tracker result (hash dict)
        result = await loop.run_in_executor(
            None,
            self._update_price_sync, asset, price, percentage, trigger_minute
        )
        
        logger.debug(f"✓ update_price | result={result}")
        return result

    def _update_price_sync(self, asset: str, price: float, percentage: float, trigger_minute: int) -> Optional[Dict]:
        """Returns tracker.record_signal() result DIRECTLY."""
        try:
            result = self.tracker.record_signal(asset, price, percentage, trigger_minute)
            logger.debug(f"✓ update_price | Tracked {asset}: {result}")
            return result
        except Exception as e:
            logger.error(f"✗ update_price | Price update failed {asset}: {e}")
            return None
    
    async def get_fairvalue_avg(self, asset: str, candle_seconds: int, confidence: float, bar_open: bool = False) -> float:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None, self._get_fairvalue_avg_sync, asset, candle_seconds, confidence, bar_open
        )
        logger.debug(f"✓ get_fairvalue_avg | {asset}: {result}")
        return result

    def _get_fairvalue_avg_sync(self, asset: str, candle_seconds: int, confidence: float, bar_open: bool = False) -> Optional[float]:
        """Sync helper — returns None when no data (caller decides fallback behaviour)."""
        try:
            historical_avg = self.tracker.get_fairvalue_avg(asset, candle_seconds, confidence, bar_open=bar_open)
            if historical_avg is None:
                logger.debug(f"⏳ get_fairvalue_avg | No historical data {asset}")
                return None
            logger.debug(f"✓ get_fairvalue_avg | {asset}: avg={historical_avg:.4f}")
            return historical_avg
            
        except Exception as e:
            logger.error(f"✗ get_fairvalue_avg | Historical avg failed {asset}: {e}")
            return 0.5  # Safe default
        
    async def get_order_from_redis(self, orderid: str) -> Optional[Dict[str, Any]]:
        try:
            # Direct key lookup first (O(1)) — pattern is "order:{order_id}"
            order_key = f"order:{orderid}"
            orderdata = self.redis.hget(order_key, "data")
            if orderdata:
                return json.loads(orderdata)
            # Fallback SCAN for any legacy key format
            cursor = 0
            while True:
                cursor, keys = self.redis.scan(cursor, match="order:*", count=100)
                for key in keys:
                    orderdata = self.redis.hget(key, "data")
                    if orderdata:
                        order = json.loads(orderdata)
                        if order.get("orderid") == orderid or order.get("order_id") == orderid:
                            return order
                if cursor == 0:
                    break
            return None
        except Exception:
            return None
        
    async def get_order_from_redis_by_token(self, token_id: str) -> Optional[Dict[str, Any]]:
        """O(1) token→order lookup via reverse index. Falls back to SCAN if index missing (legacy records)."""
        try:
            token_index_key = f"order_token_idx:{token_id}"
            order_id = self.redis.get(token_index_key)

            if order_id:
                order_id_str = order_id.decode() if isinstance(order_id, bytes) else order_id
                order_key = f"order:{order_id_str}"
                orderdata = self.redis.hget(order_key, "data")
                if orderdata:
                    return json.loads(orderdata)

            # Fallback: legacy SCAN for orders written before this fix
            logger.debug(f"get_order_from_redis_by_token | Index miss for {token_id[:16]}, falling back to SCAN")
            cursor = 0
            while True:
                cursor, keys = self.redis.scan(cursor, match="order:*", count=100)
                for key in keys:
                    orderdata = self.redis.hget(key, "data")
                    if orderdata:
                        order = json.loads(orderdata)
                        if order.get("token_id") == token_id:
                            # Backfill the index so future lookups are fast
                            self.redis.set(token_index_key, order.get("order_id", ""), ex=30 * 24 * 3600)
                            return order
                if cursor == 0:
                    break
            return None
        except Exception:
            return None
        
    async def clear_open_orders(self, market: str = None) -> dict:
        """Cancel all open orders to free USDC reservations."""
        try:
            if not market:
                # cancel_all() is a single call — no need to fetch orders first
                await asyncio.to_thread(self.client.cancel_all)
                logger.info(f"✓ clear_open_orders | cancel_all sent")
                return {'canceled': -1, 'failed': {}, 'total_open': -1}

            params = OpenOrderParams(market=market)
            open_orders = await asyncio.to_thread(self.client.get_open_orders, params)

            logger.info(f"🔍 clear_open_orders | Found {len(open_orders)} open orders for {market}")

            if not open_orders:
                return {'canceled': 0, 'failed': {}, 'total_open': 0}

            order_ids = [o['id'] for o in open_orders]

            # Batch cancel first
            try:
                result = await asyncio.to_thread(self.client.cancel_orders, order_ids)
                canceled_count = len(result.get('canceled', []))
                failed = result.get('not_canceled', {})
            except Exception:
                # Fallback to individual cancels if batch fails
                canceled_count = 0
                failed = {}
                for uid in order_ids:
                    try:
                        await asyncio.to_thread(self.client.cancel_order, OrderPayload(orderID=uid))
                        canceled_count += 1
                    except Exception as e:
                        failed[uid] = str(e)

            logger.info(f"✓ clear_open_orders | Cleared {canceled_count}/{len(order_ids)} orders")
            return {'canceled': canceled_count, 'failed': failed, 'total_open': len(open_orders)}

        except Exception as e:
            logger.error(f"✗ clear_open_orders | failed: {e}")
            return {'canceled': 0, 'failed': {'all': str(e)}, 'total_open': 0}
    
    async def close_all_positions(self, min_value: float = 0.01) -> dict:
        """Close ALL active positions with exact sizes."""
        positions = await self.get_active_positions(min_value)
        results = {"closed": 0, "failed": 0, "total_value": 0.0}
        
        logger.info(f"📊 close_all_positions | Closing {len(positions)} positions (total value: ${sum(p['value'] for p in positions):.2f})")
        
        if Config.DRY_RUN:
            logger.info("🧪 close_all_positions | DRY RUN - Would close all positions")
            return {"closed": len(positions), "failed": 0, "dry_run": True}
        
        for pos in positions:
            try:
                cooldown_key = f"close_cooldown:{pos['token_id']}"
                if self.redis.exists(cooldown_key):
                    logger.debug(f"⏳ close_all_positions | Skip cooldown active: {pos['token_id'][-8:]}")
                    continue
                
                # Set cooldown FIRST
                self.redis.setex(cooldown_key, 300, "1")
                
                market_order_args = MarketOrderArgs(
                    token_id=pos['token_id'],
                    amount=pos['size'],
                    side=OrderConstants.SELL
                )
                
                signed_order = self.client.create_market_order(market_order_args)                
                response = await asyncio.to_thread(self.client.post_order(signed_order, OrderType.FAK))
                if response and response.get("success"):
                    logger.info(f"✓ close_all_positions | Closed {pos['token_id'][-8:]}: {pos['size']:.4f} shares (${pos['value']:.2f})")
                    results["closed"] += 1
                    results["total_value"] += pos['value']
                else:
                    logger.error(f"✗ close_all_positions | Failed {pos['token_id'][-8:]}: {response}")
                    results["failed"] += 1
                    
            except Exception as e:
                logger.error(f"✗ close_all_positions | Close failed {pos['token_id'][-8:]}: {e}")
                results["failed"] += 1
        
        logger.info(f"🎉 Close summary: {results['closed']}/{len(positions)} closed (${results['total_value']:.2f})")
        return results
    
    async def _dust_cleanup(self, token_id: str, pos_size: float) -> bool:
        """Attempt to close a sub-5-share position.

        Polymarket CLOB minimum is $1.00 notional (price × size >= 1.0 USDC).
        A flat share-count floor is wrong — 3 shares at 0.50 ($1.50) is closeable,
        3 shares at 0.10 ($0.30) is not.

        Strategy:
          1. Fetch mid-price to calculate notional value
          2. If notional < $1.00 → true dust, abandon (log + Redis record, no order)
          3. If notional >= $1.00 → attempt FOK first, fall back to GTC at mid-1%
        """
        POLY_MIN_NOTIONAL = 1.0  # USDC — Polymarket CLOB hard minimum

        if pos_size >= 5.0:
            logger.warning(f"⚠️ _dust_cleanup {token_id[-8:]} | Size {pos_size} >= 5, not dust")
            return False

        try:
            # Fetch mid-price to determine notional value
            book = self.client.get_order_book(token_id)
            asks = book.get("asks") if isinstance(book, dict) else getattr(book, "asks", None)
            bids = book.get("bids") if isinstance(book, dict) else getattr(book, "bids", None)
            if not book or not asks or not bids:
                logger.error(f"📉 _dust_cleanup {token_id[-8:]} | Invalid or empty book")
                return False

            try:
                ask_px = float(asks[0]["price"] if isinstance(asks[0], dict) else asks[0].price)
                bid_px = float(bids[0]["price"] if isinstance(bids[0], dict) else bids[0].price)
            except (IndexError, AttributeError, TypeError, ValueError) as e:
                logger.error(f"📉 _dust_cleanup {token_id[-8:]} | Bad book structure: {e}")
                return False

            mid_price   = (ask_px + bid_px) / 2
            notional    = round(pos_size * mid_price, 4)
            sell_price  = round(mid_price * 0.99, 4)
            close_size  = round(pos_size, 0)

            logger.info(
                f"🧹 _dust_cleanup {token_id[-8:]} | "
                f"{pos_size:.4f} shares @ mid={mid_price:.4f} = ${notional:.4f} notional"
            )

            # True dust — notional below $1 minimum, exchange will reject any order.
            # Record in Redis for auditing, don't place an unresolvable order.
            if notional < POLY_MIN_NOTIONAL:
                logger.info(
                    f"🧹 _dust_cleanup {token_id[-8:]} | Notional ${notional:.4f} < ${POLY_MIN_NOTIONAL} "
                    f"minimum — abandoning dust (not worth closing)"
                )
                # Mark as abandoned so manage_positions stops trying
                self.redis.setex(f"dust_abandoned:{token_id}", 86400 * 7, str(pos_size))
                return False

            # Notional >= $1 — place GTC limit order to close the dust position
            logger.info(
                f"🧹 _dust_cleanup {token_id[-8:]} | ${notional:.4f} notional >= ${POLY_MIN_NOTIONAL} "
                f"— placing GTC limit SELL at {sell_price}"
            )
            limit_args = OrderArgs(
                token_id=token_id,
                price=sell_price,
                size=close_size,
                side=OrderConstants.SELL,
            )
            signed_order = await asyncio.to_thread(self.client.create_order, limit_args)
            response = await asyncio.to_thread(
                self.client.post_order, signed_order, OrderType.GTC
            )

            if response and response.get("success"):
                order_id = response.get("orderID", "unknown")
                logger.info(f"✓ _dust_cleanup {token_id[-8:]} | GTC placed: {order_id}")
                return True

            logger.error(f"✗ _dust_cleanup {token_id[-8:]} | GTC failed: {response}")
            return False

        except PolyApiException as e:
            # Most common cause: insufficient balance for the GTC order (on-chain allowance
            # is partially committed to open positions). Log clearly without a full traceback.
            logger.error(f"💥 _dust_cleanup {token_id[-8:]} | PolyAPI error — {e}")
            return False
        except Exception:
            logger.exception(f"💥 _dust_cleanup {token_id[-8:]} | Unexpected error")
            return False

    async def _wait_for_active_asset_lock_and_get_position(
        self,
        asset: str,
        token_id: str,
        lock_ttl_seconds: int = 300,
    ) -> Optional[Dict[str, Any]]:
        """
        Acquire active asset lock and return the active position for token_id.

        Waits up to (lock_ttl_seconds - current TTL) if the active lock is still hot.
        """
        active_asset_key = f"active_{token_id}"
        ttl = self.redis.ttl(active_asset_key)

        wait_threshold = lock_ttl_seconds - 10  # 285 when lock_ttl_seconds=300

        if ttl > wait_threshold:
            wait_sec = lock_ttl_seconds - ttl
            logger.info(
                f"⏳ wait_for_active_asset_lock_and_get_position {asset} | Wait {wait_sec:.0f}s for active lock (TTL: {ttl}s)"
            )
            await asyncio.sleep(wait_sec)
        elif ttl >= 0:
            logger.debug(
                f"ℹ️ wait_for_active_asset_lock_and_get_position {asset} | Lock expired (TTL: {ttl}s)"
            )
        else:
            logger.debug(
                f"ℹ️ wait_for_active_asset_lock_and_get_position {asset} | No lock (TTL: {ttl})"
            )

        # Fetch active positions with a short timeout
        positions = await asyncio.wait_for(self.get_active_positions(), timeout=10.0)
        return next((p for p in positions if p.get("token_id") == token_id), None)
   
    async def close_position_by_token(self, asset: str, token_id: str, size: float, cooldown_key: str, reason: str = "manual") -> bool:
        """Safely close position by token_id. Returns True if fully or partially closed.

        Close sequence:
          Attempt 1 — FOK at current mid        (immediate full fill or nothing)
          Attempt 2 — FAK at mid - 1% slippage  (takes partial fills, aggressive)
          Attempt 3 — FAK at mid - 2% slippage  (last resort, widest slippage)

        Partial fills are tracked per-attempt: remaining_size is reduced after each
        partial fill and subsequent attempts only close what's left.
        A partial close returns True — any reduction in position is a risk win.
        """

        lock_key = f"close_lock:{token_id}"
        if not self.redis.client.set(lock_key, "1", nx=True, ex=30):
            logger.debug(f"close_position_by_token {asset} | skip, lock held | {token_id[-8:]}")
            return False
        
        def _extract_error_msg(error_obj) -> str:
            # PolyApiException stores its payload as .error_msg (dict) not .error_message.
            # Check both to avoid silently falling through to the "Unknown API error" catch-all.
            for attr in ("error_msg", "error_message"):
                payload = getattr(error_obj, attr, None)
                if payload:
                    if isinstance(payload, dict):
                        error = payload.get("error", "")
                        if isinstance(error, str) and error:
                            return _classify_error(error)
                    elif isinstance(payload, str) and payload:
                        return _classify_error(payload)
            # Log full detail so the actual rejection reason is visible
            status = getattr(error_obj, "status_code", "?")
            raw = repr(error_obj)[:120]
            logger.debug(f"close_position_by_token | unclassified API response status={status}: {raw}")
            return f"Unknown API error (status={status})"

        def _classify_error(error_str: str) -> str:
            error_lower = error_str.lower()
            if "duplicated" in error_lower:
                return "Duplicate order rejected"
            elif "not enough balance / allowance" in error_lower:
                return "Insufficient balance or allowance"
            elif "order couldn't be fully filled" in error_lower or "couldn't be fully filled" in error_lower:
                return "FOK unfilled"  # retryable — try FAK next
            elif "not enough balance" in error_lower:
                return "Insufficient balance"
            elif "sum of matched orders" in error_lower:
                return "Liquidity constraint"
            else:
                return f"API error: {error_str[:50]}"

        def _is_non_retryable(error_msg: str) -> bool:
            # NOTE: partial fill intentionally excluded — on close any fill is progress
            # NOTE: "insufficient balance or allowance" is NOT listed here; it is handled
            #       separately below by calling fast_approve() before the next attempt.
            non_retryable = [
                "duplicate order rejected",
            ]
            return any(msg in error_msg.lower() for msg in non_retryable)

        def _is_allowance_error(error_msg: str) -> bool:
            return any(kw in error_msg.lower() for kw in (
                "insufficient balance or allowance",
                "insufficient balance",
                "not enough balance",
            ))

        if Config.DRY_RUN:
            exit_price = 0.0
            pnl_pct_dry = 0.0
            pnl_usd_dry = 0.0
            try:
                mid_resp = await asyncio.to_thread(self.client.get_midpoint, token_id)
                exit_price = float(mid_resp.get("mid", 0)) if isinstance(mid_resp, dict) else 0.0
            except Exception:
                pass
            order_dry = await self.get_order_from_redis_by_token(token_id)
            if order_dry and exit_price > 0:
                entry_dry  = float(order_dry.get("price", exit_price))
                size_dry   = abs(float(order_dry.get("size", size)))
                if entry_dry > 0:
                    pnl_pct_dry = (exit_price - entry_dry) / entry_dry * 100
                    pnl_usd_dry = pnl_pct_dry / 100 * size_dry * entry_dry
                order_id_dry = order_dry.get("order_id", "")
                if order_id_dry:
                    try:
                        self.redis.hset(f"dryrun:trade:{order_id_dry}", mapping={
                            "status":     "closed",
                            "exit_price": str(round(exit_price, 4)),
                            "exit_time":  get_utc_now().isoformat(),
                            "exit_reason": reason,
                            "pnl_pct":    str(round(pnl_pct_dry, 2)),
                            "pnl_usd":    str(round(pnl_usd_dry, 4)),
                        })
                    except Exception:
                        pass
            logger.info(
                f"🧪 DRY close {asset} | reason={reason} | exit={exit_price:.3f} | "
                f"pnl={pnl_pct_dry:+.1f}% (${pnl_usd_dry:+.2f}) | {token_id[-8:]}"
            )
            self.redis.setex(cooldown_key, 300, "1")
            return True

        try:
            pos = await self._wait_for_active_asset_lock_and_get_position(asset, token_id)
            if not pos:
                logger.info(f"ℹ️ close_position_by_token {asset} | No position found | {token_id[-8:]}")
                self.redis.setex(cooldown_key, 300, "1")
                return False

            position_size  = float(pos.get("size", size))
            position_title = pos.get("title", "na")

            if position_size <= 0:
                logger.info(f"ℹ️ close_position_by_token {asset} | Zero/negative size | {position_title}")
                return False

            if position_size < 5:
                logger.info(f"ℹ️ close_position_by_token {asset} | Too small ({position_size:.4f}) | {token_id[-8:]}")
                await self._dust_cleanup(token_id, position_size)
                self.redis.setex(cooldown_key, 300, "1")
                return False

            # (type_label, order_type, slippage_below_mid)
            CLOSE_ATTEMPTS = [
                ("FOK", OrderType.FOK, 0.000),
                ("FAK", OrderType.FAK, 0.010),
                ("FAK", OrderType.FAK, 0.020),
            ]

            remaining_size = abs(position_size)
            total_filled   = 0.0
            total_value    = 0.0
            any_filled     = False

            for attempt, (type_str, order_type, slippage) in enumerate(CLOSE_ATTEMPTS):
                if remaining_size < 5:
                    logger.info(f"✅ close_position_by_token {asset} | Remaining {remaining_size:.2f} < 5, done")
                    break

                start_time = time.time()

                # Fetch fresh mid-price each attempt — market may have moved
                close_price = None
                try:
                    pr = await asyncio.to_thread(self.client.get_midpoint, token_id)
                    if isinstance(pr, dict) and "mid" in pr:
                        mid = float(pr["mid"])
                        if mid > 0:
                            close_price = round(max(mid - slippage, 0.01), 3)
                except Exception as price_err:
                    logger.debug(f"⚠️ close_position_by_token {asset} | mid-price fetch failed: {price_err}")

                logger.info(
                    f"🔧 close_position_by_token {asset} | attempt {attempt+1}/3 {type_str} "
                    f"size={remaining_size:.2f} price={close_price} slippage={slippage:.1%} | {position_title}"
                )

                market_order_args = MarketOrderArgs(
                    token_id=token_id,
                    amount=remaining_size,
                    price=close_price,
                    side=OrderConstants.SELL,
                )

                try:
                    signed_order = await asyncio.to_thread(
                        self.client.create_market_order, market_order_args
                    )
                    response = await asyncio.to_thread(
                        self.client.post_order, signed_order, order_type
                    )
                    exec_time = time.time() - start_time

                    if response and response.get("success"):
                        filled_size  = float(response.get("makingAmount", 0) or 0)
                        filled_value = float(response.get("takingAmount", 0) or 0)

                        if filled_size > 0:
                            total_filled   += filled_size
                            total_value    += filled_value
                            remaining_size  = max(remaining_size - filled_size, 0)
                            any_filled      = True
                            avg_px = filled_value / filled_size if filled_size else 0
                            logger.info(
                                f"✅ close_position_by_token {asset} | {type_str} filled "
                                f"{filled_size:.4f} @ ${avg_px:.4f} "
                                f"| remaining={remaining_size:.2f} ({exec_time:.1f}s)"
                            )
                            if remaining_size < 5:
                                self.redis.setex(cooldown_key, 300, "1")
                                self._track_close_pnl(asset, token_id, total_filled, total_value)
                                logger.info(
                                    f"✅ close_position_by_token {asset} | Fully closed | "
                                    f"total={total_filled:.4f} value=${total_value:.2f}"
                                )
                                return True
                            continue  # partial — loop with reduced remaining_size
                        else:
                            logger.info(
                                f"⏳ close_position_by_token {asset} | {type_str} zero fill "
                                f"({exec_time:.1f}s) — next attempt"
                            )
                    else:
                        error_msg = _extract_error_msg(response)
                        if _is_allowance_error(error_msg):
                            logger.warning(
                                f"🔑 close_position_by_token {asset} | Allowance error on attempt {attempt+1}/3 "
                                f"— refreshing approvals and retrying | {error_msg}"
                            )
                            await self.fast_approve("COLLATERAL")
                            await self.fast_approve("CONDITIONAL", token_id)
                        elif _is_non_retryable(error_msg):
                            logger.error(f"✗ close_position_by_token {asset} | Non-retryable: {error_msg}")
                            break
                        else:
                            logger.warning(
                                f"🌐 close_position_by_token {asset} | {type_str} fail "
                                f"{attempt+1}/3 ({exec_time:.1f}s): {error_msg}"
                            )

                except PolyApiException as api_e:
                    exec_time = time.time() - start_time
                    error_msg = _extract_error_msg(api_e)
                    if _is_allowance_error(error_msg):
                        logger.warning(
                            f"🔑 close_position_by_token {asset} | Allowance error on attempt {attempt+1}/3 "
                            f"— refreshing approvals and retrying | {error_msg}"
                        )
                        await self.fast_approve("COLLATERAL")
                        await self.fast_approve("CONDITIONAL", token_id)
                    elif _is_non_retryable(error_msg):
                        logger.error(f"✗ close_position_by_token {asset} | API non-retryable: {error_msg}")
                        break
                    else:
                        logger.warning(
                            f"🌐 close_position_by_token {asset} | {type_str} API fail "
                            f"{attempt+1}/3 ({exec_time:.1f}s): {error_msg}"
                        )

                # Flat 0.5s between attempts — fast enough for position management
                if attempt < len(CLOSE_ATTEMPTS) - 1:
                    await asyncio.sleep(0.5)

            # All attempts done
            if any_filled:
                self.redis.setex(cooldown_key, 300, "1")
                self._track_close_pnl(asset, token_id, total_filled, total_value)
                logger.info(
                    f"⚠️ close_position_by_token {asset} | Partially closed | "
                    f"filled={total_filled:.4f} value=${total_value:.2f} remaining={remaining_size:.2f}"
                )
                return True

            logger.error(f"✗ close_position_by_token {asset} | All 3 attempts failed | {position_title}")
            return False

        except asyncio.TimeoutError:
            logger.error(f"⏰ close_position_by_token {asset} | Position fetch timeout {token_id[-8:]}")
            return False
        except Exception:
            logger.exception(f"💥 close_position_by_token {asset} | Unexpected error {token_id[-8:]}")
            return False        
        finally:
            self.redis.delete(lock_key)

    async def show_positions_dashboard(self):
        """Display current positions dashboard."""
        positions = await self.get_active_positions()
        
        print("\n" + "="*100)
        print("📊 ACTIVE POSITIONS DASHBOARD")
        print("="*100)
        
        if not positions:
            print("✅ No active positions")
            return
        
        total_value = sum(p["value"] for p in positions)
        print(f"Count: {len(positions)} | Total Value: ${total_value:>8.2f}")
        print("-"*100)
        print("TOKEN_ID       | SIZE      | PRICE   | VALUE   | SIDE")
        print("-"*100)
        
        for p in sorted(positions, key=lambda x: x["value"], reverse=True):
            print(f"{p['token_id'][-8:]:<12} | {p['size']:>8.4f} | {p['cur_price']:>6.3f} | ${p['value']:>6.2f} | {p['side']}")
        
        print("-"*100)

    def _track_close_pnl(self, asset: str, token_id: str, filled_shares: float, filled_value: float) -> None:
        """Record realized PnL to daily circuit-breaker and rolling live-PnL list."""
        try:
            order_id = self.redis.get(f"order_token_idx:{token_id}")
            if not order_id:
                return
            order_data_str = self.redis.hget(f"order:{order_id}", "data")
            if not order_data_str:
                return
            entry_price = float(json.loads(order_data_str).get("price", 0))
            if entry_price <= 0 or filled_shares <= 0:
                return
            pnl_usd = filled_value - entry_price * filled_shares
            if pnl_usd < 0:
                _now = get_utc_now()
                _bucket_hour = (_now.hour // 8) * 8
                _bucket = f"{_now.strftime('%Y-%m-%d')}-{_bucket_hour:02d}"
                loss_key = f"loss_8h:{asset}:{_bucket}"
                self.redis.incrbyfloat(loss_key, abs(pnl_usd))
                self.redis.expire(loss_key, 8 * 3600 * 2)
                logger.debug(f"📊 _track_close_pnl | {asset} loss=${abs(pnl_usd):.2f} added to {loss_key}")
            # Rolling live PnL (last 20 trades) for in-loop awareness
            live_key = f"stats:live_pnl:{asset}"
            self.redis.lpush(live_key, round(pnl_usd, 4))
            self.redis.ltrim(live_key, 0, 19)
            self.redis.expire(live_key, 86400)
        except Exception:
            pass

    def schedule_once(self, coro, delay_seconds: float) -> asyncio.Task:
        async def delayed():
            sleep_start = time.perf_counter()
            await asyncio.sleep(delay_seconds)
            sleep_ms = (time.perf_counter() - sleep_start) * 1000

            coro_name = coro.__name__ if hasattr(coro, '__name__') else 'anon'
            exec_start = time.perf_counter()
            try:
                result_coro = coro() if callable(coro) else coro
                await result_coro
                exec_ms = (time.perf_counter() - exec_start) * 1000
                logger.debug(f"⏱️ schedule_once {coro_name} sleep:{sleep_ms:.0f}ms exec:{exec_ms:.0f}ms ✓")
            except Exception as e:
                exec_ms = (time.perf_counter() - exec_start) * 1000
                logger.warning(f"⏱️ schedule_once {coro_name} sleep:{sleep_ms:.0f}ms exec:{exec_ms:.0f}ms ✗ {str(e)[:50]}")
            finally:
                if task in self._pending_tasks:
                    self._pending_tasks.remove(task)

        task = asyncio.create_task(delayed())
        self._pending_tasks.append(task)
        return task

    async def fast_approve(self, asset_type: str, token_id: str = None):
        dedup_key = (asset_type, token_id)
        if not hasattr(self, "_approval_in_flight"):
            self._approval_in_flight = set()
        
        if dedup_key in self._approval_in_flight:
            return

        self._approval_in_flight.add(dedup_key)
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                if asset_type == "COLLATERAL":
                    params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
                else:
                    params = BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
                
                resp = self.client.update_balance_allowance(params)
                logger.debug(f"✓ fast_approve | {asset_type} {token_id}")
                await asyncio.sleep(1) # Short sync
                break # Exit loop on success
                
            except Exception as e:
                # Check if network error (PolyApiException status_code is None)
                is_network = isinstance(e, PolyApiException) and e.status_code is None
                
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # 1s, 2s, 4s...
                    logger.debug(f"✓ fast_approve | Attempt {attempt+1} failed ({asset_type}). Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"✗ fast_approve | Failed after {max_retries} attempts | {e}")
            finally:
                if attempt == max_retries - 1:
                    self._approval_in_flight.discard(dedup_key)
                    
    def _check_clob_liquidity(
        self,
        token_id: str,
        asset: str,
        usd_amount: float,
        mid_price: float,
    ) -> tuple[bool, str, float]:
        """Check Polymarket CLOB spread and depth before placing a trade.

        We always BUY (YES or NO token), so we walk the ask side of the book.
        Returns (ok, reason, estimated_fill_price).
        Fails open on errors so a transient API hiccup never silently kills trading.
        """
        try:
            book = self.client.get_order_book(token_id)

            def _field(obj, key):
                return obj[key] if isinstance(obj, dict) else getattr(obj, key, None)

            asks = _field(book, "asks") if book else None
            bids = _field(book, "bids") if book else None

            if not book:
                return False, "no book response", mid_price
            if not asks:
                return False, "no asks (book empty or asks-side withdrawn)", mid_price
            if not bids:
                return False, "no bids (book empty or bids-side withdrawn)", mid_price

            def _price(level):
                return float(level["price"] if isinstance(level, dict) else level.price)

            def _size(level):
                v = level.get("size", 0) if isinstance(level, dict) else getattr(level, "size", 0)
                return float(v)

            # asks ascending  → [0] = best (lowest) ask  = cheapest to buy
            # bids descending → [0] = best (highest) bid = most willing to buy
            asks_sorted = sorted(asks, key=_price)
            bids_sorted = sorted(bids, key=_price, reverse=True)

            best_ask = _price(asks_sorted[0])
            best_bid = _price(bids_sorted[0])

            if best_ask <= 0 or best_bid <= 0:
                return False, "invalid book prices", mid_price

            spread = round(best_ask - best_bid, 4)
            if spread > Config.CLOB_MAX_SPREAD:
                return False, f"spread {spread:.3f} > max {Config.CLOB_MAX_SPREAD:.3f}", mid_price

            # Walk ask levels cheapest-first to estimate fill for usd_amount (we always BUY)
            remaining_usd = usd_amount
            total_shares = 0.0
            total_cost = 0.0

            for level in asks_sorted:
                level_price = _price(level)
                level_size = _size(level)
                if level_size <= 0 or level_price <= 0:
                    continue

                available_usd = level_price * level_size
                if remaining_usd <= available_usd:
                    shares_filled = remaining_usd / level_price
                    total_shares += shares_filled
                    total_cost += remaining_usd
                    remaining_usd = 0.0
                    break
                else:
                    total_shares += level_size
                    total_cost += available_usd
                    remaining_usd -= available_usd

            # Allow up to $0.10 rounding gap before declaring book too thin
            if remaining_usd > 0.10:
                return (
                    False,
                    f"insufficient depth (${remaining_usd:.2f} of ${usd_amount:.2f} unfillable)",
                    mid_price,
                )

            est_fill = total_cost / total_shares if total_shares > 0 else mid_price
            slippage_pct = 100.0 * (est_fill - mid_price) / mid_price if mid_price > 0 else 0.0

            if slippage_pct > Config.CLOB_MAX_SLIPPAGE_PCT:
                return (
                    False,
                    f"est. slippage {slippage_pct:.1f}% > max {Config.CLOB_MAX_SLIPPAGE_PCT:.1f}%",
                    est_fill,
                )

            return True, f"spread={spread:.3f} slippage={slippage_pct:.1f}% est_fill={est_fill:.4f}", est_fill

        except Exception as e:
            # Fail open — a check error must not block live trading
            logger.warning(f"_check_clob_liquidity | {asset} check error (passing through): {e}")
            return True, "check_error_passthrough", mid_price

    async def _fak_depth_check(
        self,
        token_id: str,
        asset: str,
        target_price: float,
        size: float,
    ) -> tuple[bool, bool]:
        """Live ask-side depth check immediately before a FAK escalation.

        Fetches the current CLOB book and sums USD available at or below
        target_price.  Returns (has_liquidity, should_abort).

        should_abort is True when fillable depth is below MIN_FAK_FILL_FRACTION
        of the order size — at that point the FAK would return "no orders found"
        anyway, so we save the round-trip and log a diagnostic book snapshot
        instead.  Fails open on any API error so a transient hiccup never blocks
        trading.
        """
        # Minimum fraction of the order that must be fillable to justify the FAK.
        # Below 25% the fill is too small to be worth the API cost.
        MIN_FAK_FILL_FRACTION = 0.25

        try:
            book = await asyncio.to_thread(self.client.get_order_book, token_id)

            def _field(obj, key):
                return obj[key] if isinstance(obj, dict) else getattr(obj, key, None)

            def _price(level):
                return float(level["price"] if isinstance(level, dict) else level.price)

            def _size(level):
                v = level.get("size", 0) if isinstance(level, dict) else getattr(level, "size", 0)
                return float(v)

            asks = _field(book, "asks") if book else None
            bids = _field(book, "bids") if book else None

            best_bid = (
                _price(sorted(bids, key=_price, reverse=True)[0]) if bids else 0.0
            )

            if not asks:
                logger.warning(
                    f"📭 fak_depth_check {asset} | EMPTY ask side | "
                    f"target={target_price:.4f} best_bid={best_bid:.4f} size=${size:.2f} — aborting FAK"
                )
                return False, True

            asks_sorted = sorted(asks, key=_price)
            best_ask = _price(asks_sorted[0])

            # USD value of resting asks at or below our target price
            fillable_usd = sum(
                _price(lvl) * _size(lvl)
                for lvl in asks_sorted
                if _price(lvl) <= target_price and _size(lvl) > 0
            )

            if fillable_usd < size * MIN_FAK_FILL_FRACTION:
                top_asks = [
                    (f"{_price(l):.4f}", f"${_price(l) * _size(l):.2f}")
                    for l in asks_sorted[:5]
                ]
                logger.warning(
                    f"📭 fak_depth_check {asset} | thin asks at target={target_price:.4f} | "
                    f"fillable=${fillable_usd:.2f} of ${size:.2f} needed "
                    f"(min {MIN_FAK_FILL_FRACTION:.0%}) | "
                    f"best_ask={best_ask:.4f} best_bid={best_bid:.4f} | "
                    f"top 5 asks (price, usd): {top_asks} — aborting FAK"
                )
                return False, True

            logger.debug(
                f"✓ fak_depth_check {asset} | ok at target={target_price:.4f} | "
                f"fillable=${fillable_usd:.2f} best_ask={best_ask:.4f}"
            )
            return True, False

        except Exception as e:
            # Fail open — a depth-check error must not silently kill trading
            logger.debug(f"fak_depth_check {asset} | check error (proceeding): {e}")
            return True, False

    def _calc_kelly_size(self, win_rate_pct: float, order_price: float, kelly_boost: float = 1.0, bankroll: float = None) -> float:
        """Fractional Kelly position sizing for binary Polymarket outcomes.

        Kelly formula for a binary contract at market price p:
          b     = (1 - p) / p          net profit per $ risked if win
          f*    = (b * win_p - lose_p) / b   full Kelly fraction of bankroll
          size  = bankroll * KELLY_FRACTION * f*   fractional Kelly USD amount

        Clamped to [KELLY_MIN_BET, KELLY_MAX_BET].
        Falls back to POSITION_SIZE on degenerate inputs.

        Example at price=0.50, win_rate=62%:
          b=1.0, f*=(1.0*0.62-0.38)/1.0=0.24, quarter-Kelly on $50=0.25*0.24*50=$3.00
        Example at price=0.50, win_rate=70%:
          b=1.0, f*=0.40, quarter-Kelly on $50=$5.00
        """
        if order_price <= 0.0 or order_price >= 1.0:
            return Config.POSITION_SIZE

        p = win_rate_pct / 100.0
        q = 1.0 - p
        b = (1.0 - order_price) / order_price  # net odds ratio

        if b <= 0:
            return Config.KELLY_MIN_BET

        f_star = (b * p - q) / b  # full Kelly fraction

        if f_star <= 0:
            return 0.0  # negative EV — caller must skip the trade

        effective_bankroll = bankroll if bankroll and bankroll > 0 else Config.KELLY_BANKROLL
        kelly_usd = effective_bankroll * kelly_boost * Config.KELLY_FRACTION * f_star
        size = round(max(Config.KELLY_MIN_BET, min(kelly_usd, Config.KELLY_MAX_BET)), 2)

        logger.debug(
            f"_calc_kelly_size | p={p:.2f} q={q:.2f} b={b:.3f} f*={f_star:.4f} "
            f"boost={kelly_boost:.2f} bankroll=${effective_bankroll:.0f} → ${kelly_usd:.2f} → clamped ${size:.2f}"
        )
        return size

    async def safe_place_order(
            self,
            market_slug: str,
            token_id: str,
            token: str,
            asset: Optional[str] = None,
            open_price: float = 0.0,
            confidence: float = 0.0,
            kelly_boost: float = 1.0,
            consensus: Optional[dict] = None,
        ) -> Optional[Dict[str, Any]]:
        """Place limit order with price validation. Retries only on PolyApiException FOK errors."""
        # 5-minute order cap — prevent over-exposure during correlated macro moves.
        # Lightweight Redis counter that auto-resets at the next 5-minute boundary.
        order_count_key = "bot:order_count:5m"
        try:
            current_count = self.redis.get(order_count_key)
            current_count = int(current_count) if current_count else 0
            if current_count >= Config.MAX_CONCURRENT_POSITIONS:
                logger.info(
                    f"✓ safe_place_order | {asset} | 5m order cap reached "
                    f"({current_count}/{Config.MAX_CONCURRENT_POSITIONS}) — skipping"
                )
                return None
        except Exception:
            pass  # don't block trading if redis fetch fails

        max_retries = 3
        retry_count = 0
        while retry_count < max_retries:
            try:
                # Get current market mid-price.
                # Try the in-memory WebSocket cache first (zero latency); fall back
                # to the HTTP get_midpoint call if the cache has no fresh data yet.
                price = POLY_MID_CACHE.get(token_id)
                if price is None:
                    price_response = self.client.get_midpoint(token_id)
                    logger.debug(f"✓ price response (HTTP fallback): {price_response}")
                    if not isinstance(price_response, dict) or 'mid' not in price_response:
                        logger.error(f"✗ safe_place_order | bad price response: {price_response}")
                        return None
                    price = float(price_response['mid'])
                else:
                    logger.debug("✓ safe_place_order | %s mid=%.4f (from WS cache)", asset, price)
                if price <= 0 or price >= 1:
                    logger.warning(f"✗ safe_place_order | invalid price {price} for {asset}")
                    return None

                # Early exit: market already near resolution — no edge to capture and
                # validate_adjust_price would reject it anyway (order_price > PRICE_MAX).
                # Avoids wasting order-construction cycles on resolved or near-resolved epochs.
                if price >= Config.NEAR_RESOLVED_THRESHOLD:
                    logger.info(
                        "✗ safe_place_order | %s near-resolved early-exit (mid=%.4f ≥ %.2f) — skip",
                        asset, price, Config.NEAR_RESOLVED_THRESHOLD,
                    )
                    return None

                logger.debug(f"✓ safe_place_order | retrieved price for {asset} #{token_id}: {price}")

                # Calculate base order price (slippage protected)
                trigger_minute = int(time.strftime("%M")) % 5
                order_price = max(round(price * 0.999, 2), Config.PRICE_MIN)
                if order_price >= 1.0:
                    # Polymarket mid near 1.0 means market has resolved — skip cleanly
                    logger.warning(f"✗ safe_place_order | {asset} near-resolved (mid={price:.4f} → order_price={order_price}) — skip")
                    return None

                # Forward open_price to validation
                _vap = await self._validate_adjust_price(
                    trigger_minute=trigger_minute,
                    order_price=order_price,
                    asset=asset,
                    token=token,
                    token_id=token_id,
                    confidence=confidence,
                    consensus=consensus,
                )

                if _vap is None:
                    return None
                order_price = _vap['price']
                _fair_value = _vap['fair_value']  # historical avg — better Kelly win probability than direction win_rate
                
                # CLOB pre-trade check: verify spread and depth on Polymarket's own book
                clob_ok, clob_reason, est_fill = self._check_clob_liquidity(
                    token_id, asset, Config.POSITION_SIZE, price
                )
                if not clob_ok:
                    logger.warning(f"✗ safe_place_order | CLOB rejected {asset} {token}: {clob_reason}")
                    self.redis.hincrby(f"stats:trade:{asset}", "clob_fail", 1)
                    return None
                logger.debug(f"✓ safe_place_order | CLOB ok {asset} {token}: {clob_reason}")

                # Kelly-optimal position sizing.
                # Use fair_value (historical avg price) as win probability — it is the
                # best estimate of settlement probability. Direction win_rate measures
                # Bybit alignment accuracy, not whether the token resolves at $1.
                _stats = self.tracker.minute_stats(asset, trigger_minute)
                _win_rate = _stats.win_rate if _stats else Config.MIN_WIN_RATE_THRESHOLD
                _kelly_win_rate = max(_win_rate, _fair_value * 100)
                live_bankroll_str = self.redis.get("bot:live_bankroll")
                live_bankroll = float(live_bankroll_str) if live_bankroll_str else Config.KELLY_BANKROLL
                size = self._calc_kelly_size(_kelly_win_rate, order_price, kelly_boost, bankroll=live_bankroll)
                if size <= 0:
                    logger.warning(
                        f"✗ safe_place_order | Kelly f*<=0 for {asset} @ {order_price:.3f} "
                        f"(win={_win_rate:.1f}% fv={_fair_value:.3f}) — negative EV, skipping"
                    )
                    return None
                
                # Validate client integrity
                if not hasattr(self.client, 'post_order') or not callable(getattr(self.client, 'post_order')):
                    logger.error(f"💥 safe_place_order critical issue | client corrupted...")
                    return None
                
                logger.info(
                    f"💰 safe_place_order | {asset} | {token} | Kelly size ${size:.2f} "
                    f"(dir_win={_win_rate:.1f}% fv={_fair_value:.3f} kelly_win={_kelly_win_rate:.1f}% "
                    f"price={order_price:.4f} bankroll=${live_bankroll:.0f})"
                )
                
                # Execute order based on config
                if Config.DRY_RUN:
                    # Guard: same dedup key used by _execute_order in live mode.
                    # Without this, the 5-second Bybit trigger fires again before the
                    # first dry order is stored and stacks duplicate positions.
                    active_asset_key = f"active_{token_id}"
                    if self.redis.exists(active_asset_key):
                        logger.debug(f"⏳ safe_place_order {asset} | DRY RUN — active order exists, skipping duplicate")
                        return None
                    self.redis.setex(active_asset_key, 300, "1")

                    dry_order_id = f"dry_{int(time.time())}_{token_id[-8:]}"
                    logger.info("🧪 DRY %s %s @ %.4f size=%.2f id=%s", token, asset, order_price, size, dry_order_id)
                    # makingAmount = USDC spent (what we give), takingAmount = shares received.
                    # Keeping consistent with the live API convention so that
                    #   order_price = makingAmount / takingAmount = price_per_share
                    # is calculated correctly downstream in the success handler.
                    response = {
                        "success": True,
                        "result": {
                            "orderID": dry_order_id,
                            "status": "matched",
                            "makingAmount": str(round(size * order_price, 4)),  # USDC cost
                            "takingAmount": str(round(size, 4)),                # shares received
                        },
                    }

                else:                  
                    response = await self._execute_order(token_id, token, asset, size, order_price)
                    logger.debug(f"✓ safe_place_order | Order response: {response}")

                # Handle success
                if response and response.get("success"):
                    # Bump 5m order counter; first hit in the window sets the TTL
                    # so the key auto-expires at the next 5-minute boundary.
                    try:
                        new_count = self.redis.incr(order_count_key)
                        if new_count == 1:
                            ttl = 300 - get_seconds_since_5m_start()
                            self.redis.expire(order_count_key, ttl if ttl > 0 else 300)
                    except Exception:
                        pass  # counter is best-effort, never block the trade

                    result = response.get("result", {})
                    status = result.get("status", "")
                    
                    making_raw = result.get("makingAmount", "")
                    taking_raw = result.get("takingAmount", "")
                    
                    if status == "live" and not making_raw and not taking_raw:
                        # FAK partial or pending — use input values as estimate
                        making = size
                        taking = round(size / order_price, 3)
                        logger.info(f"✅ safe_place_order {asset} FAK partial | {size:.2f}@{order_price:.3f} | pending | {market_slug}")
                    else:
                        # Filled or FOK
                        making = round(float(making_raw or 0), 3)
                        taking = round(float(taking_raw or 0), 3)
                        if taking > 0:
                            order_price = round(making / taking, 3)
                    
                    # Store and schedule
                    await self._handle_order_success(
                        response, market_slug, token_id, asset, token, size,
                        order_price, open_price, trigger_minute
                    )
                    self.redis.hincrby(f"stats:trade:{asset}", "order_placed", 1)
                        
                    asyncio.create_task(_tg_alert_async(
                        f"✅ {{market_slug}} | {token} | size ${size:.2f} | win_rate {_win_rate:.1f}% | price {order_price:.4f} | "
                        f"conf {confidence:.4f} | agree {consensus.get('agree', '')} | bankroll ${live_bankroll:.0f}"
                    ))
                    return response

                else:
                    logger.debug(f"✗ safe_place_order | Order response: {response}")
                    return None

            except PolyApiException as e:
                retry_count += 1
                if retry_count >= max_retries:
                    logger.error(f"✗ safe_place_order | failed after {max_retries} retries: {e}")
                    return None
                logger.warning(f"✗ safe_place_order | PolyApiException (attempt {retry_count}/{max_retries}): {e}")
                await asyncio.sleep(2 ** (retry_count - 1))  # 1s, 2s, 4s
                
            except Exception as e:
                logger.exception(f"✗ safe_place_order | failed (non-retryable): %s: %s", type(e).__name__, e)
                return None  # Non-PolyApiException errors fail immediately

    async def _execute_order(
        self,
        token_id: str,
        token: str,
        asset: Optional[str],
        size: float,
        order_price: float
    ) -> Optional[Dict[str, Any]]:
        start_time = time.time()
        token_short = token[-8:]

        # 3-tier escalating execution: (label, order_type, slippage_vs_mid)
        OPEN_ATTEMPTS = [
            ("FOK", OrderType.FOK, -0.005),  # Tier 1: strict fill-or-kill at mid - 0.5%
            ("FAK", OrderType.FAK, +0.005),  # Tier 2: accepts partials at mid + 0.5%
            ("FAK", OrderType.FAK, +0.015),  # Tier 3: desperate, wide slippage at mid + 1.5%
        ]
        max_retries = len(OPEN_ATTEMPTS)

        def is_retryable_error(error_msg: str) -> bool:
            msg = error_msg.lower()
            return any(
                p in msg
                for p in [
                    "order couldn't be fully filled",
                    "order couldn't be",
                    "sum of matched orders",
                    "the market is not yet ready",
                    "too early",
                    "no orders found to match",  # escalate to next tier instead of stopping
                ]
            )

        def short_error_msg(err: Any) -> str:
            try:
                if isinstance(err, dict):
                    raw = err.get("error") or err.get("error_message")
                    return str(raw)[:80]
                # PolyApiException stores the response as .error_msg (not .error_message)
                if hasattr(err, "error_msg"):
                    em = err.error_msg
                    if isinstance(em, str):
                        return em[:80]
                    if isinstance(em, dict):
                        return str(em.get("error", em))[:80]
            except Exception:
                pass
            return str(err)[:80]

        active_asset_key = f"active_{token_id}"
        if self.redis.exists(active_asset_key):
            logger.debug(f"⏳ execute_order {asset} | active order exists | skipped")
            return None

        self.redis.setex(active_asset_key, 300, "1")

        base_price = order_price  # updated each attempt with fresh mid
        last_error = ""           # tracks the last non-empty error across all tiers

        for attempt in range(max_retries):
            label, order_type, slippage = OPEN_ATTEMPTS[attempt]
            result = None
            error_msg = ""

            # Fetch fresh mid-price each attempt to track market movement between retries.
            # Use the WebSocket cache first; only hit HTTP if cache has gone stale.
            try:
                fresh_mid = POLY_MID_CACHE.get(token_id)
                if fresh_mid is None:
                    price_resp = await asyncio.to_thread(self.client.get_midpoint, token_id)
                    fresh_mid = float(price_resp.get("mid") or price_resp.get("midpoint") or 0)
                if fresh_mid and fresh_mid > 0:
                    base_price = fresh_mid
            except Exception:
                pass  # keep using last known base_price

            attempt_price = round(base_price * (1 + slippage), 2)
            attempt_price = max(Config.PRICE_MIN, min(attempt_price, Config.PRICE_MAX))

            logger.info(
                f"🔧 execute_order attempt {attempt+1}/{max_retries} | {label} | {asset} | "
                f"price={attempt_price:.4f} (mid={base_price:.4f} slippage={slippage:+.1%}) | ${size:.2f}"
            )

            did_succeed = False
            did_retry = False
            did_fail = False

            try:
                market_order_args = MarketOrderArgs(
                    token_id=token_id,
                    price=attempt_price,
                    amount=size,
                    side=OrderConstants.BUY,
                )
                signed_order = await asyncio.to_thread(
                    self.client.create_market_order, market_order_args
                )
                if order_type == OrderType.FOK:
                    try:
                        _o = signed_order.order
                        logger.info(
                            "🔍 execute_order FOK %s | maker=%s… signer=%s… sig_type=%s",
                            asset,
                            str(_o["maker"])[:10], str(_o["signer"])[:10],
                            _o["signatureType"],
                        )
                    except Exception:
                        pass
                result = await asyncio.to_thread(
                    self.client.post_order, signed_order, order_type
                )

                logger.debug(f"⏳ execute_order {label} {asset} | result={result}")

                if result and result.get("success"):
                    exec_time = time.time() - start_time
                    logger.info(
                        f"✅ exec_order {label} {asset} | {token_short} | "
                        f"${size:.2f}@{order_price:.3f} → {exec_time:.1f}s"
                    )
                    asyncio.create_task(_tg_alert_async(
                        f"✅ BUY {token_short} {label} {asset} | "
                        f"${size:.2f}@{order_price:.3f} → {exec_time:.1f}s"
                    ))                    
                    did_succeed = True
                else:
                    error_msg = short_error_msg(result)
                    if not is_retryable_error(error_msg):
                        logger.info(
                            f"⚠️ execute_order {label} {asset} | non‑retryable result fail: {error_msg} | result={result}"
                        )
                        did_fail = True
                    else:
                        logger.info(
                            f"⏳ execute_order {label} {asset} | retryable result fail: {error_msg}"
                        )
                        did_retry = True

            except PolyApiException as e:
                exec_time = time.time() - start_time
                error_msg = short_error_msg(e)

                if "order_version_mismatch" in error_msg.lower():
                    logger.error(
                        f"✗ exec_order {label} {asset} | order_version_mismatch "
                        f"{attempt+1}/{max_retries} ({exec_time:.1f}s) — SDK/contract mismatch, aborting"
                    )
                    did_fail = True
                elif not is_retryable_error(error_msg):
                    logger.info(
                        f"⚠️ exec_order {label} {asset} | non‑retryable exception "
                        f"{attempt+1}/{max_retries} ({exec_time:.1f}s): {error_msg}"
                    )
                    did_fail = True
                else:
                    logger.debug(
                        f"⏳ exec_order {label} {asset} | retryable exception "
                        f"{attempt+1}/{max_retries} ({exec_time:.1f}s): {error_msg}"
                    )
                    did_retry = True

            # ---- Final outcome per attempt ----
            if did_succeed:
                logger.info(
                    f"✅ exec_order {label} {asset} | attempt {attempt+1} succeeded"
                )
                return {"success": True, "result": result, "attempts": attempt + 1}

            if did_fail:
                logger.info(
                    f"✗ exec_order {label} {asset} | attempt {attempt+1} failed (non‑retryable)"
                )
                return {"success": False, "error": error_msg, "attempts": attempt + 1}

            if error_msg:
                last_error = error_msg  # preserve across iterations for final log

            if did_retry and attempt < max_retries - 1:
                # Before escalating to a FAK tier, verify there is resting ask-side
                # liquidity at the next attempt price.  If the book is empty or too
                # thin the FAK will return "no orders found" immediately — we abort
                # early and emit a diagnostic book snapshot instead of wasting the
                # API round-trip.
                _next_label, _next_type, _next_slip = OPEN_ATTEMPTS[attempt + 1]
                if _next_type == OrderType.FAK:
                    _next_price = max(Config.PRICE_MIN, min(
                        round(base_price * (1 + _next_slip), 2), Config.PRICE_MAX
                    ))
                    _liq_ok, _abort = await self._fak_depth_check(
                        token_id, asset, _next_price, size
                    )
                    if _abort:
                        last_error = (
                            f"no ask-side liquidity at {_next_price:.4f} "
                            f"(pre-FAK depth check, tier {attempt+2})"
                        )
                        logger.error(
                            f"✗ exec_order {asset} | aborting before tier "
                            f"{attempt+2}/{max_retries} FAK — {last_error}"
                        )
                        break  # skip remaining tiers; falls to "All tiers exhausted"
                logger.info(
                    f"⏳ exec_order {asset} | escalating to tier {attempt+2}/{max_retries} after {label} fail"
                )

        # All tiers exhausted
        logger.error(
            f"✗ exec_order {asset} | failed {token_short} after {max_retries} attempts"
            + (f" — last error: {last_error}" if last_error else "")
        )
        return {"success": False, "error": last_error or "Max retries exceeded", "attempts": max_retries}

    async def _handle_order_success(
        self,
        response: Dict[str, Any],
        market_slug: str,
        token_id: str,
        asset: str,
        token: str,
        size: float,
        order_price: float,
        open_price: float,
        trigger_minute: int,
    ) -> None:
        
        """Store order and schedule position checks + TP orders after success."""
        await self.store_order_permanent(
            response,
            market_slug,
            token_id,
            asset,
            token,
            size,
            order_price,
            open_price,
            trigger_minute,
        )

        now = get_utc_now()
        candle_seconds = get_seconds_since_5m_start(now)

        time_left = 285 - candle_seconds  # seconds until next 5m boundary
        if time_left <= 0:
            logger.info(
                f"✗ handle_order_success {asset} | No time left for 5s checks (time_left={time_left})"
            )
            return

        # TP at 10,15,20,..., but never beyond time_left
        max_t = min(60, time_left)
        tp_delays = [5 * i for i in range(2, int(max_t // 5) + 1)]  # 10,15,...,max_t

        logger.debug(f"✓ handle_order_success | TP delays: {tp_delays}")
        logger.info(
            "✓ handle_order_success %s | Scheduling %d TP attempts every 5s "
            "(skipping first 10s, up to %ds, time left: %ds)",
            asset,
            len(tp_delays),
            max_t,
            time_left
        )

        for delay in tp_delays:
            self.schedule_once(
                lambda tid=token_id, a=asset, ms=market_slug, s=size, p=order_price, tm=trigger_minute:
                    self.place_tp_orders(tid, a, ms, s, p, tm),
                delay,
            )
   
    async def place_tp_orders(
        self,
        token_id: str,
        asset: Optional[str],
        market_slug: str,
        size: float,
        order_price: float,
        trigger_minute: int,
    ) -> None:
        asset_label = asset or "-"
        active_tp_key = f"active_tp_{token_id}"

        if self.redis.exists(active_tp_key):
            logger.debug(f"ℹ️ place_tp_orders {asset_label:>8} | TP guard exists for {market_slug}, skipping scheduling")
            return

        try:
            if Config.DRY_RUN:
                # No real position in dry-run — use stored order size so manage_positions still runs
                order_record = await self.get_order_from_redis_by_token(token_id)
                if not order_record:
                    logger.debug(f"🧪 place_tp_orders DRY_RUN | No stored order for {token_id[-8:]}")
                    return
                position_size = abs(float(order_record.get("size", size)))
            else:
                # Polymarket Data API has a 30-60s lag after fill — use the Redis order record
                # (written at fill time) as the primary source for position size.
                # manage_positions runs at 30s+ intervals by which time the API is consistent.
                order_record = await self.get_order_from_redis_by_token(token_id)
                if order_record:
                    position_size = abs(float(order_record.get("size", size)))
                    logger.debug(
                        f"✓ place_tp_orders {asset_label:>8} | Redis order size={position_size:.2f} (API lag bypass)"
                    )
                else:
                    # Fallback: API lookup (should not be needed in normal flow)
                    pos = await self._wait_for_active_asset_lock_and_get_position(asset, token_id)
                    if not pos:
                        logger.info(f"ℹ️ place_tp_orders {asset_label:>8} | No position found for {market_slug}")
                        return
                    position_size = float(pos.get("size", size))

            if position_size < 5.0:
                logger.info(
                    f"ℹ️ place_tp_orders {asset_label:>8} | Position too small for TP orders: {position_size:.1f}; "
                    f"will rely on trailing in manage_positions only"
                )

            # --- Guard key and schedule ---
            self.redis.setex(active_tp_key, 300, "1")

            now = get_utc_now()
            candle_seconds = get_seconds_since_5m_start(now)
            time_left = (285 - candle_seconds)
            if time_left <= 0:
                logger.info(
                    f"✗ manage_positions {asset_label:>8} | No time left for 5s checks (time_left={time_left})"
                )
            else:
                num_intervals = int(time_left // 5)
                manage_delays = [5 * i for i in range(0, num_intervals + 1)]
                for delay in manage_delays:
                    self.schedule_once(
                        lambda ms=market_slug, tid=token_id, tm=trigger_minute:
                            self.manage_positions(ms, tid, tm),
                        delay,
                    )

            # --- Only place TP if position is large enough ---
            if position_size < 5.0:
                logger.debug(
                    f"ℹ️ place_tp_orders {asset_label:>8} | Position < 5.0; skipping TP posting, "
                    f"TP key set for trailing in manage_positions"
                )
                return

            # Dry run: manage_positions is already scheduled above; no real CLOB order needed.
            # Without this guard the code falls through to client.create_order / post_order
            # which fails with a balance error and leaves the position unprotected.
            if Config.DRY_RUN:
                logger.debug(
                    f"🧪 place_tp_orders {asset_label:>8} | DRY RUN — TP limit skipped, "
                    f"manage_positions handles SL/TP via scheduled tasks"
                )
                return

            tp_price_threshold = min(order_price * 1.18, 0.96)
            tp1_size = max(5.0, round(position_size * 0.5, 0))
            if tp_price_threshold >= Config.PRICE_MAX:
                tp1_size = position_size  # If threshold is unrealistic, just place a full TP instead of half
                
            try:
                price_resp = self.client.get_midpoint(token_id)
                current_mid = float(price_resp.get("mid", 0))
            except Exception as e:
                logger.warning(f"⚠️ place_tp_orders {asset_label:>8} | midpoint fetch failed: {e}")
                return

            cooldown_key_tp = f"close_cooldown:{token_id}"

            if current_mid >= tp_price_threshold:
                logger.info(
                    f"🟢 place_tp_orders {asset_label:>8} | TP1 HIT | "
                    f"mid={current_mid:.3f} >= {tp_price_threshold:.3f} | "
                    f"closing {tp1_size:.0f} shares"
                )
                await self.close_position_by_token(asset, token_id, tp1_size, cooldown_key_tp, reason="tp")
                return

            # Otherwise, place a TP limit
            try:
                args = OrderArgs(
                    token_id=token_id,
                    size=tp1_size,
                    side=OrderConstants.SELL,
                    price=tp_price_threshold,
                    expiration=int(time.time()) + 300,
                )
                signed = self.client.create_order(args)
                resp = self.client.post_order(signed, OrderType.GTD)

                status = resp.get("status", "?")
                error = resp.get("errorMsg", "")
                logger.info(
                    f"⏳ place_tp_orders {asset_label:>8} | TP1 waiting | "
                    f"mid={current_mid:.3f} < {tp_price_threshold:.3f} | "
                    f"size {tp1_size:.0f} shares | [{status}:{error}]"
                )

            except PolyApiException as e:
                # Resolve error message from whichever attribute PolyApiException uses
                _payload = getattr(e, "error_msg", None) or getattr(e, "error_message", None)
                if isinstance(_payload, dict):
                    error_msg = _payload.get("error", str(e))
                elif isinstance(_payload, str):
                    error_msg = _payload
                else:
                    error_msg = str(e)
                if "not enough balance" in error_msg.lower():
                    logger.warning(
                        f"⚠️ place_tp_orders {asset_label:>8} | Allowance error — refreshing and retrying TP | {error_msg[:100]}"
                    )
                    await self.fast_approve("COLLATERAL")
                    await self.fast_approve("CONDITIONAL", token_id)
                    # Re-attempt: place a new TP order after approval refresh
                    try:
                        signed2 = self.client.create_order(args)
                        resp2 = self.client.post_order(signed2, OrderType.GTD)
                        status2 = resp2.get("status", "?")
                        error2 = resp2.get("errorMsg", "")
                        logger.info(
                            f"⏳ place_tp_orders {asset_label:>8} | TP retry after approval | "
                            f"mid={current_mid:.3f} | [{status2}:{error2}]"
                        )
                    except Exception as retry_e:
                        logger.error(f"✗ place_tp_orders {asset_label:>8} | TP retry failed: {retry_e}")
                else:
                    logger.error(f"✗ place_tp_orders {asset_label:>8} | {error_msg}")
            except Exception as e:
                logger.error(f"✗ place_tp_orders {asset_label:>8} | {e}")

        except Exception as e:
            logger.error(f"✗ place_tp_orders {asset_label:>8} | Critical error: {e}")
            return
        
    async def _validate_adjust_price(
        self,
        trigger_minute: int,
        order_price: float,
        asset: str,
        token: str,
        token_id: str,
        confidence: float,
        consensus: Optional[dict] = None,
    ) -> Optional[float]:
        """Robust price validation: historical fair value + liquidity + direction.
        Dynamic edge threshold replaces hardcoded Config.EDGE_THRESHOLD:
          - Base: backcalculated optimal_edge from history (falls back to Config.EDGE_THRESHOLD)
          - Time-decay multiplier: edge grows as candle matures (less time = need more cushion)
          - Momentum adjustment: strong signal → lower edge needed; weak → higher
          - Volatility scaling: high price_std asset → wider natural spread, need more edge
        """
        
        stats = self.tracker.minute_stats(asset, trigger_minute)

        if order_price > Config.PRICE_MAX:
            logger.info("✗ validate_adjust_price %-8s | tm=%d | order_price=%.4f > max=%.4f | skipped",
                        asset, trigger_minute, order_price, Config.PRICE_MAX)
            # Increment session skip counter for PRICE_MAX calibration monitoring.
            # Visible in balance_check log line; reset on each bot restart.
            try:
                self.redis.incr("bot:skip:price_max")
            except Exception:
                pass
            return None

        if order_price < Config.PRICE_MIN:
            logger.info("✗ validate_adjust_price %-8s | tm=%d | order_price=%.4f < min=%.4f | skipped",
                        asset, trigger_minute, order_price, Config.PRICE_MIN)
            return None

        # Compute candle timing early so raw signals can be written before the stats gate.
        # Dead/closing zones are excluded — they're timing noise, not meaningful signals.
        _now_raw = get_utc_now()
        _candle_seconds_raw = get_seconds_since_5m_start(_now_raw)
        if 5 <= _candle_seconds_raw <= 285:
            try:
                _c = consensus or {}
                await asyncio.to_thread(
                    self.tracker.record_signal_raw,
                    asset, token, order_price, confidence, trigger_minute, _candle_seconds_raw,
                    _c.get('bybit_dir', ''), _c.get('cb_dir', ''), _c.get('cl_dir', ''), _c.get('agree', ''),
                    _c.get('binance_dir', ''),
                )
            except Exception:
                pass

        if not stats or not stats.should_trade:
            bar_start = get_current_5m_bar_ts(time.time())
            if self._no_stats_last_bar.get(asset) != bar_start:
                self._no_stats_last_bar[asset] = bar_start
                win_rate = getattr(stats, 'win_rate', 0.0) if stats else 0.0
                avg_price = getattr(stats, 'avg_price', 0.0) if stats else 0.0
                count = getattr(stats, 'count', 0) if stats else 0
                if not stats:
                    # No stats key at all — asset has no resolved signal history in Redis.
                    # This means seed_stats_from_raw found no raw resolved signals for this asset.
                    # Trading will unlock automatically once ≥10 outcomes are recorded.
                    logger.warning(
                        f"⏳ validate_adjust_price {asset:>8} | tm={trigger_minute} | "
                        f"should_trade=False (no Redis stats — asset needs ≥10 resolved outcomes to trade) — skip"
                    )
                elif count < 5:
                    # Stats exist but this specific trigger_minute has too few samples (need ≥5).
                    logger.info(
                        f"⏳ validate_adjust_price {asset:>8} | tm={trigger_minute} | "
                        f"should_trade=False (count={count} < 5 for M{trigger_minute}) | "
                        f"win_rate={win_rate:.1f}% avg_price={avg_price:.3f} — skip"
                    )
                else:
                    reason = f"win_rate={win_rate:.1f}% count={count}"
                    logger.info(
                        f"⏳ validate_adjust_price {asset:>8} | tm={trigger_minute} | "
                        f"should_trade=False ({reason}) | avg_price={avg_price:.3f} — skip"
                    )
            return None
        
        # 8-hour loss circuit breaker — pause asset if it lost >15% of bankroll in the current 8h window
        _now = get_utc_now()
        _bucket_hour = (_now.hour // 8) * 8
        _bucket = f"{_now.strftime('%Y-%m-%d')}-{_bucket_hour:02d}"
        loss_key = f"loss_8h:{asset}:{_bucket}"
        window_loss = float(self.redis.get(loss_key) or 0)
        live_bankroll_str = self.redis.get("bot:live_bankroll")
        live_bankroll = float(live_bankroll_str) if live_bankroll_str else Config.KELLY_BANKROLL
        max_window_loss = live_bankroll * 0.15
        if window_loss >= max_window_loss:
            # Log/alert only once per 8h window per asset
            alert_key = f"loss_8h_alerted:{asset}:{_bucket}"
            if self.redis.client.set(alert_key, "1", nx=True, ex=8 * 3600):
                logger.info(
                    f"🛑 validate_adjust_price {asset} | 8h loss ${window_loss:.2f} >= limit ${max_window_loss:.2f} — pausing"
                )
                asyncio.create_task(_tg_alert_async(
                    f"⚠️ <b>8h loss limit hit</b> for {asset}\nLoss: ${window_loss:.2f} / limit ${max_window_loss:.2f} — trading paused for this 8h window"
                ))
            return None

        active_asset_key = f"active_{token_id}"
        if self.redis.exists(active_asset_key):
            logger.debug(f"⏳ validate_adjust_price {asset:>8} | active order {active_asset_key} already exists | skipped")
            return None

        avg_price = getattr(stats, 'avg_price', 0.0)
        win_rate = getattr(stats, 'win_rate', 0.0)
        count = getattr(stats, 'count', 0)
        price_std = getattr(stats, 'price_std', 0.0)
        optimal_edge_base = getattr(stats, 'optimal_edge', Config.EDGE_THRESHOLD)

        logger.debug(f"✓ validate_adjust_price {asset:>8} | tm={trigger_minute} stats | "
                    f"win_rate={win_rate:.1f}% | avg_price={avg_price:.4f} | "
                    f"price_std={price_std:.6f} | optimal_edge_base={optimal_edge_base:.1f}% ({count})")

        # ── CANDLE TIMING ROUTING ─────────────────────────────────────────────
        # Zones (seconds into 5-minute candle):
        #   0- 4  : dead zone   — WebSocket candle open, pct_change not yet meaningful
        #   5-20  : bar-open    — fast-path with stricter signal gates, wider hist buckets
        #  21-270 : standard    — normal path, full historical match criteria
        # 271-285 : late-entry  — aggressive late entry, max 30 seconds before close
        # 286-299 : closing     — skip entry (10s exec lag + position visibility lag)
        now = get_utc_now()
        candle_seconds = get_seconds_since_5m_start(now)

        # Dead zone and closing zone — hard exits
        if candle_seconds < 5:
            logger.info(
                f"⏳ validate_adjust_price {asset:>8} | tm={trigger_minute} | "
                f"s={candle_seconds} | dead zone (feed lag)"
            )
            return None
        
        if candle_seconds > 285:
            logger.info(
                f"⏳ validate_adjust_price {asset:>8} | tm={trigger_minute} | "
                f"s={candle_seconds} | closing zone (exec lag)"
            )
            return None

        # Determine path
        is_bar_open = candle_seconds <= 20

        if is_bar_open:
            # Require stronger cross-source momentum before trusting early-candle data.
            # These are upstream of the historical avg check so we fail fast.
            BAR_OPEN_MIN_PCT = Config.BAR_OPEN_MIN_PCT
            BAR_OPEN_EDGE_SURCHARGE = Config.BAR_OPEN_EDGE_SURCHARGE

            if abs(confidence) < BAR_OPEN_MIN_PCT:
                # Suppress repeat logs within the same 5-minute bar — the Bybit trigger
                # fires every 5s so this would otherwise spam 3-4 identical lines per bar.
                bar_start = get_current_5m_bar_ts(time.time())
                if self._weak_signal_last_bar.get(asset) != bar_start:
                    self._weak_signal_last_bar[asset] = bar_start
                    logger.info(
                        f"⏳ validate_adjust_price {asset:>8} | tm={trigger_minute} | "
                        f"s={candle_seconds} | bar-open weak signal: "
                        f"|{confidence:.3f}%| < {BAR_OPEN_MIN_PCT}% — skip"
                    )
                return None

            # Also require OBI confirmation in bar-open window (no neutral book allowed)
            try:
                from main import BYBIT_MANAGER
                if BYBIT_MANAGER is not None:
                    asset_to_bybit = {
                        "btc": "BTCUSD", "eth": "ETHUSD", "xrp": "XRPUSD", "sol": "SOLUSD"
                    }
                    bybit_sym = asset_to_bybit.get(asset.lower(), "")
                    if bybit_sym and bybit_sym in BYBIT_MANAGER.data:
                        obi = BYBIT_MANAGER.data[bybit_sym].order_book_imbalance
                        is_yes = token == "YES"
                        # Must have positive OBI confirmation, not just neutral
                        obi_ok = (is_yes and obi > 0.10) or (not is_yes and obi < -0.10)
                        if not obi_ok:
                            logger.info(
                                f"⏳ validate_adjust_price {asset:>8} | tm={trigger_minute} | "
                                f"s={candle_seconds} | bar-open OBI not confirmed: "
                                f"{obi:+.3f} for {token} — skip"
                            )
                            return None
                        logger.debug(
                            f"✓ validate_adjust_price {asset:>8} | bar-open OBI confirmed: {obi:+.3f}"
                        )
            except Exception as obi_err:
                # If we can't read OBI, don't trade bar-open (too risky without confirmation)
                logger.info(
                    f"⏳ validate_adjust_price {asset:>8} | bar-open OBI unavailable ({obi_err}) — skip"
                )
                return None
        else:
            BAR_OPEN_EDGE_SURCHARGE = 0.0

        # Historical fair value — bar-open uses relaxed bucket matching
        # prices:fairvalue always stores the YES token mid-price with signed pct.
        # For NO tokens, fair value = 1 - YES fair value (binary complement).
        # Use direction-adjusted confidence so YES lookups match up-move records
        # and NO lookups match down-move records — avoids direction averaging toward 0.5.
        confidence_for_fv = abs(confidence) if token == "YES" else -abs(confidence)
        historical_avg = await self.get_fairvalue_avg(
            asset, candle_seconds, confidence_for_fv, bar_open=is_bar_open
        )

        no_fairvalue_data = historical_avg is None
        if no_fairvalue_data:
            logger.debug(
                f"✗ validate_adjust_price {asset:>8} | no fair value data @ {candle_seconds}s "
                f"[{token}] — skip"
            )
            return None
        elif token == "NO":
            historical_avg = round(1.0 - historical_avg, 6)
            logger.debug(
                f"🔄 validate_adjust_price {asset:>8} | NO complement: "
                f"YES_avg={1.0 - historical_avg:.4f} → NO_avg={historical_avg:.4f}"
            )

        # ── DYNAMIC EDGE CALCULATION ──────────────────────────────────────────
        # 1. Base: use backcalculated optimal edge (from history backtest),
        #    clamped to [3%, 12%] — lower cap prevents sparse data from demanding huge discounts.
        edge_base = max(3.0, min(12.0, optimal_edge_base))

        # 2. Time-decay: later in candle = less time to recover, require more cushion.
        #    Multiplier: 1.0x at second 0 → 1.5x at second 285.
        time_multiplier = 1.0 + (candle_seconds / 285) * 0.5

        # 3. Momentum adjustment: strong price move = higher conviction = less edge needed.
        #    abs(confidence) is bybit 5m pct change (e.g. 0.15 = 0.15%).
        #    Scale: 0% move → 1.0x (no adj), 0.5%+ move → 0.6x (40% discount).
        momentum_factor = max(0.6, 1.0 - abs(confidence) * 0.8)

        # 4. Volatility scaling: high price_std means wider natural spread.
        #    price_std is in price units (0-1 range). Scale to % for comparison.
        #    Typical range: 0.02-0.15 → adds 0-3% to required edge.
        vol_adj = price_std * 20.0  # e.g. std=0.05 → +1.0%
        vol_adj = max(0.0, min(5.0, vol_adj))  # cap at +5%

        required_edge = (edge_base * time_multiplier * momentum_factor) + vol_adj + BAR_OPEN_EDGE_SURCHARGE
        required_edge = round(max(2.0, min(15.0, required_edge)), 2)  # absolute bounds

        # Fairness score: negative = we're cheaper than history (good), positive = expensive (bad)
        edge_pct = (order_price - historical_avg) / historical_avg * 100
        path_tag = "open" if is_bar_open else "default"
        status_icon = (
            "🟢" if edge_pct < -required_edge else
            "🟡" if edge_pct < 0 else
            "🔴"
        )

        logger.info(
            "%s | %-8s | tm=%d-%3ds | win_rate=%.1f%% | "
            "chg:%+.3f%% | Hist:%6.4f | Now:%6.4f | "
            "Edge:%+5.1f%% (Need:-%.1f%%)",
            status_icon, asset, trigger_minute, candle_seconds, win_rate,
            confidence, historical_avg, order_price,
            edge_pct, required_edge
        )

        # Edge check applies to ALL windows including 270-285s late entries.
        # time_multiplier is ~1.49x at 280s so late trades naturally need stronger edge.
        if edge_pct > -required_edge:
            return None

        logger.info(
            f"✅ validate_adjust_price {asset:>8} | Approved [{path_tag}] | tm={trigger_minute} | "
            f"{token} | price={order_price} | Edge:{edge_pct:+.1f}% vs need:-{required_edge:.1f}%"
        )
        # Record signal only after all gates have passed — keeps prices:signals clean for accurate win-rate stats
        try:
            await asyncio.to_thread(self.tracker.record_signal, asset, order_price, confidence, trigger_minute)
        except Exception:
            pass
        return {'price': order_price, 'fair_value': historical_avg}

    async def store_order_permanent(
        self,
        response: Dict[str, Any],
        market_slug: str,
        token_id: str,
        asset: str,
        side: str,
        size: float,
        price: float,
        open_price: float,
        trigger_minute: int,
    ) -> None:
        """Store order permanently in Redis HASHES - NO DUPLICATES, perfect analytics."""
        try:
            token_id = str(token_id).strip()
            if len(token_id) < 40:
                logger.error(f"store_order_permanent | Invalid token_id too short: {len(token_id)} chars")
                return

            result = response.get("result", {})
            order_id = result.get("orderID")

            if not order_id:
                logger.error("No orderID in response")
                return

            order_data = {
                "order_id": order_id,
                "market_slug": market_slug,
                "token_id": token_id,
                "asset": asset,
                "side": side,
                "size": float(size),
                "price": float(price),
                "exchange_open_price": open_price,
                "trigger_minute": trigger_minute,
                "status": response.get("status", "live"),
                "created_at": datetime.now(UTC).isoformat(),
            }

            order_json = json.dumps(order_data, separators=(",", ":"))

            # 🔥 HASH-BASED DEDUPLICATION + PERMANENT STORAGE
            order_key = f"order:{order_id}"
            # NEW: reverse index for O(1) token→order lookup (replaces full SCAN)
            token_index_key = f"order_token_idx:{token_id}"

            with self.redis.pipeline() as pipe:
                # 1. Check if exists
                pipe.exists(order_key)
                # 2. Get current version
                pipe.hget(order_key, "data")
                # 3. Always save latest (overwrites = status updates)
                pipe.hset(order_key, mapping={"minute": trigger_minute, "data": order_json})
                # 4. 30‑day TTL
                pipe.expire(order_key, 30 * 24 * 3600)
                # 5. NEW: write reverse index token_id → order_id
                pipe.set(token_index_key, order_id, ex=30 * 24 * 3600)
                exists, current, _, _, _ = pipe.execute()

            # Log change detection
            if exists and current != order_json:
                logger.debug(f"🔄 store_order_permanent | Updated {order_id[:8]} {asset} {side} → {order_data['status']}")
            elif not exists:
                logger.debug(f"✅ store_order_permanent | New order {order_id[:8]} {asset} {side} {size:.1f}@{price:.3f}")

        except Exception as e:
            logger.error(f"store_order_permanent | Store order exception: {type(e).__name__}: {e}", exc_info=True)
            return

        # Dry-run performance tracking — written unconditionally so the dashboard can query results later.
        # Keys: dryrun:trade:{order_id}  (hash, 7d TTL)
        #       dryrun:daily:{YYYY-MM-DD} (sorted set of order_ids, 14d TTL)
        if Config.DRY_RUN:
            tp_price_track  = round(float(price) + (1.0 - float(price)) * 0.50, 4)
            _sl_base_track  = min(trigger_minute * 3 + 10, 22)
            _vol_track      = POLY_MID_CACHE.get_volatility(token_id)
            sl_pct_track    = (
                min(round(_sl_base_track * max(1.0, _vol_track / 0.30), 1), 35.0)
                if _vol_track is not None else float(_sl_base_track)
            )
            dry_key = f"dryrun:trade:{order_id}"
            today   = datetime.now(UTC).strftime("%Y-%m-%d")
            try:
                self.redis.hset(dry_key, mapping={
                    "order_id":        order_id,
                    "status":          "open",
                    "asset":           asset,
                    "side":            side,
                    "entry_price":     str(price),
                    "kelly_size":      str(size),
                    "trigger_minute":  str(trigger_minute),
                    "market_slug":     market_slug,
                    "token_id":        token_id,
                    "tp_price_target": str(tp_price_track),
                    "sl_pct":          str(sl_pct_track),
                    "entry_time":      datetime.now(UTC).isoformat(),
                })
                self.redis.expire(dry_key, 86400 * 7)
                self.redis.zadd(f"dryrun:daily:{today}", {order_id: time.time()})
                self.redis.expire(f"dryrun:daily:{today}", 86400 * 14)
                logger.debug(f"🧪 dryrun:trade:{order_id[:12]} stored | tp={tp_price_track:.4f} sl={sl_pct_track}%")
            except Exception as _e:
                logger.warning(f"🧪 dryrun tracking write failed: {_e}")

        # Register order in pending-outcome index so _update_order_outcomes (main.py) can
        # find it at the next bar close and write Bybit/Coinbase/Chainlink consensus.
        bar_start = get_current_5m_bar_ts(time.time())
        pending_key = f"orders:pending_outcome:{asset}"
        self.redis.zadd(pending_key, {order_id: bar_start})
        self.redis.expire(pending_key, 30 * 24 * 3600)

        # Schedule Polymarket outcome (final truth — token resolution price)
        delay = (5 - (datetime.now().minute % 5)) * 60
        self.schedule_once(
            lambda oid=order_id, tid=token_id, ms=market_slug, a=asset, si=side, p=price:
                self.polymarket_order_outcome(oid, tid, ms, a, si, p),
            delay,
        )
        logger.debug(f"store_order_permanent | Scheduled polymarket_order_outcome for {order_id[:8]} in {delay}s")

    async def polymarket_order_outcome(
        self,
        order_id: str,
        token_id: str,
        market_slug: str,
        asset: str,
        side: str,
        price: float,
    ) -> bool:
        order_key = f"order:{order_id}"

        try:
            price_resp = self.client.get_midpoint(token_id)
            outcome_price = safe_float(price_resp.get("mid", price))

            if outcome_price < 0 or outcome_price > 1:
                logger.warning(
                    f"polymarket_order_outcome {asset} | Invalid outcome_price={outcome_price} for {token_id[:16]}"
                )
                return False

            elif outcome_price <= 0.75:
                last_resp = self.client.get_last_trade_price(token_id)

                # Handle dict {'price': str} or tuple/list with price first
                if isinstance(last_resp, dict):
                    outcome_price = safe_float(last_resp.get("price", outcome_price))
                else:  # tuple/list
                    outcome_price = safe_float(last_resp[0] if last_resp else outcome_price)

            # Decide outcome
            if side == "YES":
                if outcome_price > 0.75:
                    outcome = "YES"
                else:
                    outcome = "NO"  
            elif side == "NO": 
                if outcome_price > 0.75:
                    outcome = "NO"
                else:              
                    outcome = "YES"       
            else:
                outcome = "NA"

            price_diff = outcome_price - price
            result = round(price_diff, 4)
            result_percent = round((price_diff / price) * 100, 2)

            # Store in Redis
            mapping = {
                "polymarket_outcome_price": outcome_price,
                "polymarket_updated_at": int(time.time()),
                "polymarket_status": "outcome",
                "polymarket_direction": outcome,
                "polymarket_pnl": result,
                "polymarket_pct": result_percent,
                "market_slug": market_slug,
                "asset": asset,
            }
            self.redis.hset(order_key, mapping=mapping)
            self.redis.expire(order_key, 3600 * 24 * 30)

            logger.info(
                f"✓ polymarket_order_outcome {asset} | {market_slug} | {side}→{outcome} | "
                f"price={price:.3f}→{outcome_price:.3f} | "
                f"pnl={result:.4f} ({result_percent:+.1f}%)"
            )
            return True

        except Exception as e:
            logger.error(f"✗ {order_id[:8]} outcome failed: {e}")
            self.redis.hset(order_key, "polymarket_status", "failed")
            return False

    async def safe_price(self, token_id: str, entry_price: float = 0.5) -> tuple[float, float]:
        """Returns (current_price, pnl_pct) safely."""
        try:
            price_resp = await asyncio.to_thread(self.client.get_midpoint, token_id)
            current = safe_float(price_resp.get("mid") or price_resp.get("midpoint"), entry_price)
            if current == 0 or entry_price == 0:
                return entry_price, 0.0
            
            pnl = ((current - entry_price) / entry_price) * 100
            return current, pnl
            
        except Exception as e:
            logger.warning(f"safe_price exception ({token_id}): {e}")
            return entry_price, 0.0

    async def manage_positions(
        self,
        market_slug: Optional[str] = None,
        token_id: Optional[str] = None,
        trigger_minute: Optional[int] = None,
    ) -> None:
        """
        Manage open positions with TP/SL + trailing stop loss.
        Tracks max_pnl_pct in Redis per token for trailing logic.
        Cleans state even on API failures.
        """
        max_key = None
        cooldown_key = None
        
        try:
            if token_id:
                active_asset_key_tp = f"active_tp_{token_id}"
                if not self.redis.exists(active_asset_key_tp):
                    logger.info(f"✗ Manage positions | {market_slug} | waiting for take profit orders...")
                    return

                cooldown_key = f"close_cooldown:{token_id}"
                if self.redis.exists(cooldown_key):
                    logger.debug(f"⏳ Manage positions | {market_slug} | Skip {token_id[:16]}... already closing")
                    return

                # Single order mode
                order = await self.get_order_from_redis_by_token(token_id)
                if not order:
                    logger.error(f"✗ Manage positions | {market_slug} | order with token_id {token_id[:8]} not in Redis")
                    return

                asset = order["asset"]
                size = abs(float(order["size"]))
                entry_price = float(order["price"])

                # Current price & PnL
                current_price, pnl_pct = await self.safe_price(token_id, entry_price)
                pnl_usd = pnl_pct / 100 * size * entry_price  # USD P&L

                if current_price == 0:
                    logger.error(f"✗ Manage positions | {market_slug} | no price for {asset} {token_id[:8]}")
                    return

                # TP: capture 50% of remaining distance to 1.0 — avoids impossible %-of-entry targets for high-price entries
                tp_price_target = min(entry_price + (1.0 - entry_price) * 0.50, 0.97)
                # SL: base grows with trigger minute (10–22%).  Scaled upward when the
                # market is choppy so normal noise doesn't fire the stop prematurely.
                # Baseline: 0.30% per-second mid change in calm conditions.
                # Example: vol=0.60% → scale=2.0 → a M2 base-16% stop widens to 32%.
                # Hard cap at 35% to bound worst-case loss regardless of vol reading.
                _sl_base = min(trigger_minute * 3 + 10, 22)
                _vol = POLY_MID_CACHE.get_volatility(token_id)
                if _vol is not None:
                    _vol_scale = max(1.0, _vol / 0.30)
                    sl_pct = min(round(_sl_base * _vol_scale, 1), 35.0)
                    logger.debug(
                        f"📐 SL vol-adjust {asset} | vol={_vol:.3f}% scale={_vol_scale:.2f}x "
                        f"base={_sl_base}% → sl={sl_pct}%"
                    )
                else:
                    sl_pct = float(_sl_base)

                # Trailing stop logic
                max_key = f"max_pnl:{token_id}"
                stored_max = self.redis.get(max_key)
                max_pnl_pct = float(stored_max) if stored_max is not None else float('-inf')

                # Update max if new high
                if pnl_pct > max_pnl_pct:
                    max_pnl_pct = pnl_pct
                    self.redis.set(max_key, max_pnl_pct, ex=300)  
                    logger.debug(f"📈 Manage positions | {market_slug} | new max PnL {token_id[:8]}: {max_pnl_pct:.1f}%")

                # Trailing stop inactive below 15% peak — binary tokens have normal 5% noise swings.
                trailing_stop_pct = max_pnl_pct * 0.75 if max_pnl_pct > 15 else float('-inf')

                logger.info(
                    f"➖ Manage positions {asset} | tm={trigger_minute} | "
                    f"size=${size:.2f} | {entry_price:.3f}→{current_price:.3f} | "
                    f"{'🟢' if pnl_pct > 0 else '🔴'} {pnl_pct:+.1f}% : ${pnl_usd:+.2f} | "
                    f"TP:{tp_price_target:.3f} | SL:{sl_pct}% | Max:{max_pnl_pct:+.1f}%  | Trail:{trailing_stop_pct:+.1f}%"
                )

                # ── POST-ENTRY COUNTER-SIGNAL CHECK ─────────────────────────
                # If all three data sources strongly flip direction after entry, exit early.
                # Avoids riding a position into confirmed reversal.
                try:
                    from main import BYBIT_MANAGER  # lazy import to avoid circular dep
                    if BYBIT_MANAGER is not None:
                        # Map asset name back to bybit symbol
                        asset_to_bybit = {
                            "btc": "BTCUSD", "eth": "ETHUSD", "xrp": "XRPUSD", "sol": "SOLUSD"
                        }
                        clean_asset = asset.lower().replace("usdt", "")
                        bybit_sym = asset_to_bybit.get(clean_asset)
                        if bybit_sym and bybit_sym in BYBIT_MANAGER.data:
                            tick = BYBIT_MANAGER.data[bybit_sym]
                            obi = tick.order_book_imbalance
                            pct = tick.candle_5m_pct
                            # Counter-signal: price moving against our position AND book confirms it
                            is_yes_position = "YES" in str(order.get("side", ""))
                            strong_reversal = (
                                (is_yes_position and pct < -0.10 and obi < -0.15) or
                                (not is_yes_position and pct > 0.10 and obi > 0.15)
                            )
                            if strong_reversal:
                                logger.info(
                                    f"🔁 Manage positions {asset} | COUNTER-SIGNAL | "
                                    f"pct={pct:+.2f}% OBI={obi:+.3f} | closing early"
                                )
                                await self._close_with_cleanup(asset, token_id, size, cooldown_key, reason="counter_signal")
                                return
                except Exception as cs_err:
                    logger.debug(f"⚠️ Manage positions {asset} | counter-signal check failed: {cs_err}")

                # Close triggers (in priority order)

                # Highest priority: force-close within 20s of bar expiry.
                # Binary markets resolve instantly at bar end — holding to resolution
                # means the losing token collapses to ~0.01 with no exit possible.
                seconds_to_expiry = 300 - get_seconds_since_5m_start(get_utc_now())
                if seconds_to_expiry <= 35:
                    emoji = "🟢" if pnl_pct > 0 else "🔴"
                    logger.info(
                        f"{emoji} Manage positions {asset} EXPIRY CLOSE | {seconds_to_expiry:.0f}s to bar end | "
                        f"pnl={pnl_pct:.1f}% | Closing {market_slug} before resolution"
                    )
                    self.redis.hincrby(f"stats:trade:{asset}", "expiry_close", 1)
                    if pnl_pct > 0:
                        self.redis.hincrby(f"stats:trade:{asset}", "correct_direction", 1)
                    await self._close_with_cleanup(asset, token_id, size, cooldown_key, reason="expiry")

                    asyncio.create_task(_tg_alert_async(
                        f"{emoji} Manage positions {asset} | closure before resolution | "
                        f"{seconds_to_expiry:.0f}s to bar end | pnl {pnl_pct:.1f}%"
                    ))
                    return

                elif current_price >= tp_price_target:
                    logger.info(f"🟢 Manage positions {asset} TP HIT | pnl {pnl_pct:.1f}% | price {current_price:.3f} >= {tp_price_target:.3f}")
                    self.redis.hincrby(f"stats:trade:{asset}", "tp", 1)
                    self.redis.hincrby(f"stats:trade:{asset}", "correct_direction", 1)
                    await self._close_with_cleanup(asset, token_id, size, cooldown_key, reason="tp")
                    asyncio.create_task(_tg_alert_async(f"🟢 Manage positions {asset} TP HIT | pnl {pnl_pct:.1f}% | price {current_price:.3f} >= {tp_price_target:.3f}"))
                    return

                elif pnl_pct <= -sl_pct:
                    logger.info(f"🔴 Manage positions {asset} SL HIT | {pnl_pct:.1f}% <= -{sl_pct:.1f}%")
                    self.redis.hincrby(f"stats:trade:{asset}", "sl", 1)
                    await self._close_with_cleanup(asset, token_id, size, cooldown_key, reason="sl")
                    asyncio.create_task(_tg_alert_async(f"🔴 Manage positions {asset} SL HIT | pnl {pnl_pct:.1f}% <= -{sl_pct:.1f}%"))
                    return

                elif max_pnl_pct > 15 and pnl_pct <= trailing_stop_pct:
                    logger.info(f"🟠 Manage positions {asset} TRAIL HIT pnl={pnl_pct:.1f}% | peak {max_pnl_pct:.1f}% | pnl {pnl_pct:.1f}% <= stop {trailing_stop_pct:.1f}%")
                    self.redis.hincrby(f"stats:trade:{asset}", "trail_stop", 1)
                    self.redis.hincrby(f"stats:trade:{asset}", "correct_direction", 1)
                    await self._close_with_cleanup(asset, token_id, size, cooldown_key, reason="trail")
                    asyncio.create_task(_tg_alert_async(f"🟠 Manage positions {asset} TRAIL HIT | peak {max_pnl_pct:.1f}% | pnl {pnl_pct:.1f}% <= stop {trailing_stop_pct:.1f}%"))
                    return

        except Exception as e:
            logger.error(f"💥 Manage_positions | {market_slug} | {e}", exc_info=True)

    async def _close_with_cleanup(self, asset: str, token_id: str, size: float, cooldown_key: str, reason: str = "manual") -> None:
        close_success = False
        try:
            close_success = await self.close_position_by_token(asset, token_id, size, cooldown_key, reason)
            if close_success:
                logger.info(f"✓ close_with_cleanup {asset} | Close success | Success: {close_success}")
                if cooldown_key:
                    self.redis.setex(cooldown_key, 300, "1")
            else:
                logger.info(f"✗ close_with_cleanup {asset} | Close failed | Success: {close_success}")

        except Exception as e:
            logger.error(f"✗ close_with_cleanup {asset} | Close failed | {e}")

        finally:
            pass

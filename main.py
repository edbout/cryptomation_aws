#!/usr/bin/env python3
# Polymarket 5-Minute Momentum Trading Bot 

import asyncio
import math
import signal
import json
import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, Optional, List, Tuple, Any
from logging.handlers import TimedRotatingFileHandler
from dataclasses import dataclass
import time

UTC = ZoneInfo("UTC")

# Setup logging FIRST
def setup_logging() -> None:
    """Production logging setup with hourly log rotation."""
    logger = logging.getLogger()
    logger.handlers.clear()  # prevent double-logging
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    # Console handler (stdout)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Local dev file logging with hourly rotation
    if os.getenv("HEROKU") != "true":
        from dotenv import load_dotenv
        load_dotenv()
        os.makedirs("./log", exist_ok=True)

        file_handler = TimedRotatingFileHandler(
            filename="./log/bot.log",
            when="midnight",      # Rotate at midnight (daily)
            interval=1,
            backupCount=7,        # Keep 7 days
            encoding="utf-8",
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

setup_logging()

logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("py_clob_client_v2.http_helpers.helpers").setLevel(logging.CRITICAL)

# Core imports
from config import Config, RedisCache
from components import Components
from redeem import run_redeem_non_interactive
from lib.helpers import  get_utc_now, normalize_asset
from lib.telegram_alert import send_alert
from lib.polymarket_mid_cache import POLY_MID_CACHE
from lib.binance_feed import BinanceFeed
from lib.coinbase_feed import CoinbaseFeed
from lib.chainlink_feed import ChainlinkFeed
from lib.bybit_manager import BybitManager
from lib.bybit_feed import BybitFeed

BYBIT_MANAGER: Optional[BybitManager] = None
BYBIT_FEED: Optional[BybitFeed] = None

shutting_down = False

# === TRADING COMPONENTS ===
components = Components.create()
client = components['client']
checker = components['checker']
geo_checker = components['geo_checker']
order_mgr = components['order_mgr']
finder= components['finder'] 
rpc_manager = components['rpc_manager']
price_tracker = components['price_tracker'] 

def log_config() -> None:
    """Log trading parameters."""
    logger.info("🔧 Config settings:")
    logger.info(f"  DRY_RUN             = {Config.DRY_RUN}")
    logger.info(f"  PRICE_MIN           = {Config.PRICE_MIN}")
    logger.info(f"  PRICE_MAX           = {Config.PRICE_MAX}")
    logger.info(f"  ASSETS              = {Config.ASSETS}")

rdb = RedisCache()

def test_redis() -> bool:
    """Test Redis connection."""
    try:
        rdb.ping()
        logger.info("✓ test_redis | Redis OK")
        return True
    except Exception as e:
        logger.error(f"✗ test_redis | Redis failed: {e}")
        return False

class AppState:
    def __init__(self):
        self.can_trade = False

APP_STATE = AppState()

# Feed singletons live here (the classes themselves are in lib/*_feed.py).
# main() re-instantiates these on every (re)start, so keep these as the
# canonical place to read "the current feed".
coinbase_feed = CoinbaseFeed()
chainlink_feed = ChainlinkFeed()
binance_feed = BinanceFeed()

async def execute_trading_validation(symbol: str = None) -> Optional[Dict]:
    """Trading validation - single symbol for poll() efficiency."""
    if shutting_down:
        return None
    
    if Config.DRY_RUN:
        logger.debug("🧪 execute_trading_validation | DRY RUN - allow trading validation")
    elif not APP_STATE.can_trade:
        logger.debug("✗ execute_trading_validation | Skipping trading: can_trade = False")
        return None
    
    global BYBIT_MANAGER
    if BYBIT_MANAGER is None:
        logger.error(" execute_trading_validation | BYBIT_MANAGER is None")
        return None
    
    logger.debug("🎯 execute_trading_validation | Trading validation%s", f" for {symbol}" if symbol else "")

    target_assets = [normalize_asset(symbol)] if symbol else Config.ASSETS
    markets, next_markets = finder.find_polymarket_targets(target_assets)

    # Simple - getsignal() handles everything
    if symbol:
        raw_signals = [await getsignal(symbol)]
    else:
        signal_coros = [getsignal(s) for s in Config.BYBIT_SYMBOLS]
        raw_signals = await asyncio.gather(*signal_coros, return_exceptions=True)
    
    signals: List[Tuple[str, str, float, float, dict]] = [s for s in raw_signals if isinstance(s, tuple)]

    if not signals:
        logger.debug("No signals for %s", symbol or "all")
        if not symbol:  # Full scan handles approvals
            await handle_next_markets_approvals()
        return None
    
    if markets and len(markets) < len(signals):
        logger.warning("✗ execute_trading_validation | Signal count (%d) exceeds market count (%d) for %s", len(signals), len(markets), symbol or "all")
        
    trade_results = await _execute_parallel_trades(markets, signals)
    successful_trades = [r for r in trade_results if r is not None]
    return {"trades": successful_trades} if successful_trades else None

async def _execute_parallel_trades(
    markets: Dict[str, Dict],
    signals: List[Tuple[str, str, float, float, dict]]
) -> List[Optional[Dict]]:
    """Execute trades concurrently with semaphore for capacity control."""
    semaphore = asyncio.Semaphore(3)  # Limit to 3 concurrent trades

    async def _trade_with_semaphore(
        asset: str, direction: str, confidence: float, open_price: float,
        market_slug: str, token_id: str, token: str, kelly_boost: float,
        consensus: dict
    ) -> Optional[Dict]:
        async with semaphore:
            result = await order_mgr.safe_place_order(
                market_slug, token_id, token, asset, open_price, confidence, kelly_boost,
                consensus=consensus
            )
            if result:
                logger.debug("✓ execute_parallel_trades | Trade executed %s (open: %.2f)", asset, open_price)
            else:
                logger.debug("✗ execute_parallel_trades | Trade failed %s", asset)
            return result

    tasks = []
    for asset, direction, confidence, sig_open, consensus in signals:
        if asset not in markets:
            continue

        market = markets[asset]
        market_slug = market["slug"]
        raw_tokens = market["clobTokenIds"]
        token_list = json.loads(raw_tokens) if isinstance(raw_tokens, str) else raw_tokens

        if direction == "BUY":
            token_id = token_list[0]  # YES token
            token = "YES"
            logger.info("🚀 execute_parallel_trades | BUYING YES for %s (open: %.2f)", asset, sig_open)
        else:  # SELL
            token_id = token_list[1]  # NO token
            token = "NO"
            logger.info("🚀 execute_parallel_trades | BUYING NO for %s (open: %.2f)", asset, sig_open)

        kelly_boost = BYBIT_FEED.get_kelly_boost(asset, direction) if BYBIT_FEED else 1.0

        tasks.append(_trade_with_semaphore(
            asset, direction, confidence, sig_open, market_slug, token_id, token, kelly_boost,
            consensus
        ))
    
    return await asyncio.gather(*tasks, return_exceptions=True)

async def handle_next_markets_approvals() -> None:
    """Approve next markets in parallel - standalone reusable method."""
    
    method_start = time.perf_counter()  # ⏱️ Full method timing
    
    trigger_minute = int(time.strftime("%M")) % 5    
    markets, next_markets = finder.find_polymarket_targets(Config.ASSETS)
    
    if trigger_minute != 1:  # Only on minute 1
        method_end = time.perf_counter()
        logger.debug(f"⏱️ handle_next_markets_approvals skipped (min {trigger_minute}) → {(method_end - method_start)*1000:.1f}ms")
        return
    
    if not next_markets:
        method_end = time.perf_counter()
        logger.debug(f"⏱️ handle_next_markets_approvals no markets → {(method_end - method_start)*1000:.1f}ms")
        return
    
    approval_tasks = []
    approval_count = 0
    for asset, next_market in next_markets.items():
        if Config.DRY_RUN:
            logger.info("🧪 handle_next_markets_approvals | DRY APPROVAL %s next market", asset)
            continue

        raw_tokens = next_market["clobTokenIds"]
        token_list = json.loads(raw_tokens) if isinstance(raw_tokens, str) else raw_tokens

        logger.debug("⏱️ handle_next_markets_approvals | Approving %s for next market (%s tokens)", asset, len(token_list))
        # Parallel approvals for YES/NO tokens
        approval_tasks.extend([
            order_mgr.fast_approve("CONDITIONAL", token_list[0]),  # YES
            order_mgr.fast_approve("CONDITIONAL", token_list[1])   # NO
        ])
        approval_count += 2

    if approval_tasks:
        approve_start = time.perf_counter()
        results = await asyncio.gather(*approval_tasks, return_exceptions=True)
        approve_end = time.perf_counter()
        
        # Log approval batch timing
        logger.info(f"⏱️ handle_next_markets_approvals | Approvals batch ({len(approval_tasks)} tasks) → {(approve_end - approve_start)*1000:.1f}ms")
        
        fails = sum(1 for result in results if isinstance(result, Exception))
        logger.info(f"⏱️ handle_next_markets_approvals | Approvals: {len(results)-fails}/{len(results)} success ({100*(len(results)-fails)/len(results):.0f}%)")
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error("✗ handle_next_markets_approvals | Approval %d failed: %s", i+1, result)
    else:
        logger.info("⏱️ handle_next_markets_approvals | no tasks → 0ms")
    
    method_end = time.perf_counter()
    logger.info(f"⏱️ handle_next_markets_approvals | Finished {(method_end - method_start)*1000:.1f}ms ({approval_count} approvals)")

async def redeem() -> Dict:
    """Async wrapper for optimized redeem.py."""
    try:
        logger.debug("🔄 Redeem | Running auto-redemption...")
        
        # Run sync redeem in thread pool 
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None,
            lambda: run_redeem_non_interactive(mode="high_gas")
        )
        
        # Handle new return format
        if result["status"] == "success":
            logger.info(f"✅ Redeem | success")
            return {"success": True}
        elif result["status"] == "no_positions":
            logger.debug("✓ Redeem | No redeemable positions found")
            return {"success": True}
        else:
            logger.error(f"✗ Redeem | error")
            return {"success": False}
            
    except Exception as e:
        logger.error(f"✗ Redeem | failed: {e}")
        return {"success": False, "message": str(e)}

async def balance_check():
    """Async balance check. Alerts via Telegram when balance drops below $5."""
    try:
        balance = checker.pusd_balance
        can_trade = checker.check_trading_capacity(Config.POSITION_SIZE)
        rdb.set("bot:live_bankroll", str(round(balance, 2)), ex=300)
        # Surface PRICE_MAX skip rate for calibration auditing.
        # A high count means many signals are firing when Polymarket odds are already > PRICE_MAX.
        # Consider raising PRICE_MAX or NEAR_RESOLVED_THRESHOLD if this grows large relative to trades.
        price_max_skips = int(rdb.get("bot:skip:price_max") or 0)
        logger.info(
            "💰 balance_check | pUSD: $%.2f | Can trade: %s | price_max_skips: %d (PRICE_MAX=%.2f) | mid_cache: %s",
            balance, can_trade, price_max_skips, Config.PRICE_MAX, POLY_MID_CACHE.stats(),
        )
        if balance < 5.0:
            await send_alert(f"⚠️ <b>Low balance</b>: ${balance:.2f} pUSD\nTrading suspended until topped up")
        return can_trade
    except Exception as e:
        logger.error("💰 balance_check | failed: %s", e)
        return False

async def check_trading_ready() -> bool:
    if Config.DRY_RUN:
        return True
    try:
        # Global 8h drawdown stop — halt all trading if total losses exceed threshold in current 8h window
        _now = get_utc_now()
        _bucket_hour = (_now.hour // 8) * 8
        _bucket = f"{_now.strftime('%Y-%m-%d')}-{_bucket_hour:02d}"
        total_window_loss = sum(
            float(rdb.get(f"loss_8h:{asset}:{_bucket}") or 0)
            for asset in Config.ASSETS
        )
        max_global_loss = Config.KELLY_BANKROLL * Config.MAX_GLOBAL_8H_LOSS_PCT
        if total_window_loss >= max_global_loss:
            # Log/alert only once per 8h window
            alert_key = f"global_loss_8h_alerted:{_bucket}"
            if rdb.set(alert_key, "1", nx=True, ex=8 * 3600):
                logger.warning(
                    f"🛑 check_trading_ready | Global 8h loss ${total_window_loss:.2f} >= ${max_global_loss:.2f} — suspending all trading"
                )
                await send_alert(
                    f"🛑 <b>Global drawdown stop</b>\nTotal 8h loss: ${total_window_loss:.2f} / limit ${max_global_loss:.2f}\nAll trading suspended for this 8h window"
                )
            return False

        return await asyncio.wait_for(balance_check(), timeout=5.0)
    except asyncio.TimeoutError:
        return False

async def _retry_failed_redemptions() -> None:
    """Scan redeem:retry:* keys and re-attempt any queued failed redemptions."""
    try:
        keys = rdb.keys("redeem:retry:*")
        if not keys:
            return
        from redeem import _redeemer, PolymarketRedeemer, RedeemPosition
        redeemer = _redeemer or PolymarketRedeemer(mode="high_gas")
        for key in keys:
            try:
                raw = rdb.get(key)
                if not raw:
                    continue
                data = json.loads(raw)
                pos = RedeemPosition(
                    condition_id=data["condition_id"],
                    indexes=data["index_sets"],
                    title="retry",
                    value=data.get("value", 0.0),
                    size=0,
                )
                logger.info(f"🔄 retry_redeem | Attempting {pos.condition_id}")
                ok = await asyncio.to_thread(redeemer.redeem_high_gas, pos)
                if ok:
                    rdb.delete(key)
                    logger.info(f"✓ retry_redeem | Success for {pos.condition_id}")
            except Exception as ex:
                logger.warning(f"✗ retry_redeem | {key}: {ex}")
    except Exception as e:
        logger.warning(f"✗ retry_redeem | scan failed: {e}")

async def timer_loop():
    """Precise 5-minute scheduler with monotonic second-boundary wakeups."""
    logger.info("✓ Timer loop | Precise 5min scheduling")

    last_trading = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0}
    _last_retry_redeem_ts = 0.0
    APP_STATE.can_trade = await check_trading_ready()
    while not shutting_down:
        # Sleep precisely to the next wall-clock second boundary to eliminate drift
        _now_ts = time.time()
        await asyncio.sleep(math.ceil(_now_ts) - _now_ts)

        now = datetime.now()
        minute = now.minute
        second = now.second
        minute_mod = minute % 5

        try:
            # REDEEM: Every 5min on the 10th second
            if minute_mod == 0 and second == 10:
                logger.info(f"🎯 Timer loop | Redeem at {now.strftime('%H:%M:%S')}")
                await asyncio.wait_for(redeem(), timeout=45.0)
                APP_STATE.can_trade = await check_trading_ready()
                logger.debug(f"✓ Timer loop | Balance check: can_trade={APP_STATE.can_trade}")

                for sym in Config.BYBIT_SYMBOLS:
                    if BYBIT_FEED and sym in BYBIT_FEED.bybit_candles:
                        BYBIT_FEED.bybit_candles[sym].log_volume_status(sym)

                await asyncio.to_thread(price_tracker.run, limit=5)
                continue

            # RETRY FAILED REDEMPTIONS: every ~10 minutes
            if _now_ts - _last_retry_redeem_ts > 600:
                _last_retry_redeem_ts = _now_ts
                asyncio.create_task(_retry_failed_redemptions())

            # TRADING: every minute at second 0 if ready
            elif second == 0 and APP_STATE.can_trade and minute_mod in (0, 1, 2, 3, 4):
                ts = now.timestamp()
                if ts - last_trading[minute_mod] > 295:
                    logger.info(f"🎯 Timer loop | Trading M{minute_mod} at {now.strftime('%H:%M:%S')} | can_trade={APP_STATE.can_trade} | dry_run={Config.DRY_RUN}")
                    last_trading[minute_mod] = ts
                    await handle_next_markets_approvals()
                continue

        except asyncio.CancelledError:
            logger.info("✗ Timer loop | Cancelled")
            raise
        except asyncio.TimeoutError:
            logger.warning("✗ Timer loop | Timeout - continuing")
        except Exception as e:
            logger.error(f"✗ Timer loop | Error: {e}")
            await asyncio.sleep(5)

async def getsignal(sym: str) -> Optional[Tuple[str, str, float, float, dict]]:
    global BYBIT_MANAGER
    if not BYBIT_MANAGER:
        return None
    return BYBIT_MANAGER.get_signal(sym)

async def main():
    global shutting_down, BYBIT_MANAGER, BYBIT_FEED, coinbase_feed, chainlink_feed, binance_feed

    # Early exits with cleanup
    if not geo_checker.test_geo():
        logger.warning("🌍 main | Geoblocked - monitoring only")

    if not test_redis():
        logger.error("✗ main | Redis required")
        return

    # Initialize ALL feeds FIRST (before starting)
    coinbase_feed = CoinbaseFeed()
    chainlink_feed = ChainlinkFeed()
    binance_feed = BinanceFeed()

    log_config()
    checker.log_status()

    logger.info("🚀 main | Starting")
    await send_alert(f"<b>🚀 Restarted Bot</b>")

    # Clear orders + start background threads
    await order_mgr.clear_open_orders()
    await asyncio.to_thread(price_tracker.run, limit=5)
    await order_mgr.fast_approve("COLLATERAL")

    # START feeds AFTER initialization
    coinbase_feed.start()
    chainlink_feed.start()
    POLY_MID_CACHE.set_client(client)
    asyncio.create_task(POLY_MID_CACHE.run())
    logger.info("✓ main | PolymarketMidCache started")

    loop = asyncio.get_running_loop()
    BYBIT_FEED = BybitFeed(
        chainlink_feed=chainlink_feed,
        coinbase_feed=coinbase_feed,
        binance_feed=binance_feed,
    )
    BYBIT_FEED.attach_components(
        finder=finder, client=client, price_tracker=price_tracker,
    )
    # Late-bound to avoid a circular import in lib/bybit_feed.
    BYBIT_FEED.attach_validator(execute_trading_validation, loop)
    BYBIT_FEED.start_websocket(loop)

    BYBIT_MANAGER = BybitManager(bybit_feed=BYBIT_FEED)
    BYBIT_MANAGER.attach_rdb(rdb)

    # Wire BinanceFeed's trigger entry point to the same event loop the Bybit
    # callbacks use. Late binding avoids a circular import in lib/binance_feed.
    binance_feed.attach_validator(execute_trading_validation, loop)
    binance_feed.start()
         
    sched_task = asyncio.create_task(timer_loop())

    def _on_sigterm():
        global shutting_down
        shutting_down = True
        logger.info("🛑 main | SIGTERM received — initiating clean shutdown")
        sched_task.cancel()

    try:
        loop.add_signal_handler(signal.SIGTERM, _on_sigterm)
        logger.info("✓ main | SIGTERM handler registered")
    except (NotImplementedError, AttributeError):
        logger.debug("⚠️ main | SIGTERM handler not supported on this platform (Windows)")

    try:
        await sched_task
    except asyncio.CancelledError:
        logger.info("🛑 main | Cancelled - cleaning up")
    finally:
        logger.info("🔄 main | Shutting down...")
        
        # Sequential cleanup (Bybit first, then feeds)
        if BYBIT_FEED:
            BYBIT_FEED.stop()
            logger.info("✓ main | Bybit feed stopped")
            BYBIT_FEED = None  # Clear global
            BYBIT_MANAGER = None

        # Stop feeds (they have their own task.cancel())
        if coinbase_feed:
            coinbase_feed.stop()
            logger.info("✓ main | Coinbase feed stopped")
        if chainlink_feed:
            chainlink_feed.stop()
            logger.info("✓ main | Chainlink feed stopped")
        if binance_feed:
            binance_feed.stop()
            logger.info("✓ main | Binance feed stopped")

        # Final resource cleanup
        if 'rdb' in globals():
            rdb.close()
            logger.info("✓ main | Redis closed")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 main | Stopped by user (Ctrl+C)")
    except asyncio.TimeoutError:
        logger.warning("⏰ main | Task limit exceeded")
    except Exception as e:         
        logger.error(f"💥 main | Crashed: {e}", exc_info=True)
    finally:
        logger.info("✓ main | Shutdown complete")

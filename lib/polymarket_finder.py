import requests
import logging
import json
import time
from datetime import datetime, timezone
from typing import List, Dict, Tuple
from lib.helpers import normalize_polymarket_asset
logger = logging.getLogger(__name__)

from config import RedisCache

class PolymarketFinder:
    """Helper to find current 5m up/down markets for BTC/ETH/XRP/SOL/DOGE on Polymarket."""
    def __init__(self):
        self.rdb = RedisCache()

    def get_current_and_next_5m_market(self, asset: str) -> Tuple[dict, dict]:
        """
        Find current AND next 5m up/down markets for BTC/ETH/SOL/XRP.
        Returns (current_market, next_market), where either can be None.
        """
        # Normalize asset name (e.g. BTCUSDT → btc)
        asset = normalize_polymarket_asset(asset)
        if not asset:
            logger.error("✗ get_current_and_next_5m_market | Empty or invalid asset name after normalization")
            return None, None

        # 5m window = 300 seconds; align to epoch boundary
        now_utc = datetime.now(timezone.utc)
        window_seconds = 300
        current_epoch = int(now_utc.timestamp()) // window_seconds * window_seconds
        next_epoch = current_epoch + window_seconds

        # Build slugs: e.g. btc-updown-5m-1640995200
        current_slug = f"{asset}-updown-5m-{current_epoch}"
        next_slug = f"{asset}-updown-5m-{next_epoch}"

        logger.debug(
            f"🔍 get_current_and_next_5m_market | Checking {asset.upper()} 5m markets: current={current_slug}, next={next_slug}"
        )

        # Fetch both markets
        current_market = self._fetch_market_by_slug(current_slug)
        next_market = self._fetch_market_by_slug(next_slug)

        return current_market, next_market

    def _fetch_market_by_slug(self, slug: str) -> dict:
        CACHE_KEY = f"gamma_market:{slug}"
        CACHE_TTL = 60  # ↑ 60s

        # Try cache first
        cached = self.rdb.get(CACHE_KEY)
        if cached is not None:
            logger.debug(f"✅ fetch_market_by_slug | Cache HIT for {slug}")
            return json.loads(cached)

        # Cache miss: API call with RETRY
        url = f"https://gamma-api.polymarket.com/markets?slug={slug}"
        for attempt in range(3):  # Retry 3x
            try:
                response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
                if response.status_code == 200 and response.json():
                    market = response.json()[0]
                    self.rdb.setex(CACHE_KEY, CACHE_TTL, json.dumps(market))
                    return market
                elif response.status_code == 404:
                    # Cache 404s for 30s to avoid spam
                    self.rdb.setex(CACHE_KEY, 30, json.dumps({"error": "not_found"}))
                    return None
            except:
                pass
            time.sleep(0.5 * attempt)  # Progressive backoff
        return None

    def find_polymarket_targets(self, assets: List[str]) -> Tuple[Dict[str, dict], Dict[str, dict]]:
        """
        Filter open Polymarket 5-minute markets suitable for immediate execution.
        Returns (current_markets, next_markets). next_markets is populated whenever
        the next 5m slug already exists on Polymarket, triggering allowance/approval.
        """
        valid_markets = {}
        next_markets = {}
        now_ts = int(datetime.now(timezone.utc).timestamp())

        for asset in assets:            
           
            current_market, next_market = self.get_current_and_next_5m_market(asset)
            # Check next market exists → trigger allowance/approval
            if next_market:
                seconds_into_current = now_ts % 300
                logger.debug(f"🎯 find_polymarket_targets | {asset}: Next 5m market detected ({seconds_into_current}s into current). Triggering allowance/approval...")
                next_markets[asset] = next_market

            # No next market → fall back to current market logic
            if not current_market:
                continue

            try:
                # DOUBLE-CHECK MARKET STATUS
                if current_market.get('closed') or not current_market.get('active', True):
                    logger.debug(f"⏭️ find_polymarket_targets | {asset}: market already filtered as closed")
                    continue

                # Parse end time (ISO string or timestamp)
                end_date = current_market.get("endDate") or current_market.get("endDateIso")
                if not end_date:
                    logger.debug(f"⏭️ {asset}: no endDate")
                    continue

                if "T" in end_date:
                    end_ts = int(
                        datetime.fromisoformat(
                            end_date.replace("Z", "+00:00")
                        ).timestamp()
                    )
                else:
                    end_ts = int(end_date)

                remaining = end_ts - now_ts

                # Only markets expiring in 1–299 seconds (current market logic)
                if 1 <= remaining <= 299:
                    raw_tokens = current_market["clobTokenIds"]
                    slug = current_market.get("slug", "N/A")
                    if isinstance(raw_tokens, str):
                        token_list = json.loads(raw_tokens)
                    else:
                        token_list = raw_tokens

                    token_yes = token_list[0]
                    token_no = token_list[1]

                    logger.debug(f"✓ find_polymarket_targets | {asset}: {remaining}s remaining, #{slug} | YES: {token_yes[:5]}... NO: {token_no[:5]}...")
                    valid_markets[asset] = current_market

            except Exception as e:
                logger.error(f"✗ find_polymarket_targets | Error parsing {asset} market time: {e}")

        return valid_markets, next_markets

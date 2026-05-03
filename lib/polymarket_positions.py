import requests
import logging
from typing import List, Dict, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)

POLYMARKET_POSITIONS_URL = "https://data-api.polymarket.com/positions"

"""        
Position: {'token_id': '106239685938958303075341305753465916371583247452960787490507420042579833419242', 
           'proxy_wallet': '0x4ff44f5e2c039122daab3047f03d390aacda8915', 
           'condition_id': '0x04cbdefa0795373edbabfe8a2838d107c67697d95038f288f61cb37a49baa9f9', 
           'size': 6.8952, 
           'avg_price': 0.7099, 
           'cur_price': 0.735, 
           'initial_value': 4.8955, 
           'current_value': 5.0679, 
           'cash_pnl': 0.1723, 
           'percent_pnl': 3.5194, 
           'total_bought': 7.0422, 
           'realized_pnl': 0.0, 
           'percent_realized_pnl': 1.3581, 
           'unrealized_pnl': 0.1730695200000001, 
           'value': 5.0679, 
           'redeemable': False, 
           'mergeable': False, 
           'title': 'XRP Up or Down - April 8, 11:45PM-11:50PM ET', 
           'slug': 'xrp-updown-5m-1775706300', 
           'icon': 'https://polymarket-upload.s3.us-east-2.amazonaws.com/XRP-logo.png', 
           'event_slug': 'xrp-updown-5m-1775706300', 
           'outcome': 'Down', 
           'outcome_index': 1, 
           'opposite_outcome': 'Up', 
           'opposite_asset': '73447328620402916170573607653687004083184028098778475928812213305812800374697', 
           'end_date': '2026-04-09', 
           'negative_risk': False, 
           'side': '', 
           'raw': {'proxyWallet': '0x4ff44f5e2c039122daab3047f03d390aacda8915', 'asset': '106239685938958303075341305753465916371583247452960787490507420042579833419242', 
           'conditionId': '0x04cbdefa0795373edbabfe8a2838d107c67697d95038f288f61cb37a49baa9f9', 'size': 6.8952, 'avgPrice': 0.7099, 'initialValue': 4.8955, 
           'currentValue': 5.0679, 'cashPnl': 0.1723, 'percentPnl': 3.5194, 'totalBought': 7.0422, 'realizedPnl': 0, 'percentRealizedPnl': 1.3581, 'curPrice': 0.735, 
           'redeemable': False, 'mergeable': False, 'title': 'XRP Up or Down - April 8, 11:45PM-11:50PM ET', 'slug': 'xrp-updown-5m-1775706300', 
           'icon': 'https://polymarket-upload.s3.us-east-2.amazonaws.com/XRP-logo.png', 'eventId': '356013', 'eventSlug': 'xrp-updown-5m-1775706300', 
           'outcome': 'Down', 'outcomeIndex': 1, 'oppositeOutcome': 'Up', 'oppositeAsset': '73447328620402916170573607653687004083184028098778475928812213305812800374697', 
           'endDate': '2026-04-09', 'negativeRisk': False}}
"""

@dataclass
class Position:
    token_id: str
    size: float
    cur_price: float
    avg_price: float
    value: float
    title: str = ""

class PolymarketPositionManager:
    def __init__(self, proxy_wallet: str):
        self.proxy_wallet = proxy_wallet
        self.positions_url = POLYMARKET_POSITIONS_URL

    def get_active_positions(self, min_value: float = 0.01) -> List[Dict[str, Any]]:
        """Get active positions with full Polymarket fields."""
        try:
            params = {
                "user": self.proxy_wallet,
                "sizeThreshold": 0.000001,
                "limit": 1000,
            }
            resp = requests.get(self.positions_url, params=params, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            positions = data if isinstance(data, list) else data.get("positions", [])

            active = []
            for pos in positions:
                token_id = pos.get("asset", "")
                if not token_id:
                    continue

                size = float(pos.get("size", 0) or 0)
                cur_price = float(pos.get("curPrice", pos.get("currPrice", 0)) or 0)
                avg_price = float(pos.get("avgPrice", pos.get("avg_entry_price", 0)) or 0)
                initial_value = float(pos.get("initialValue", 0) or 0)
                current_value = float(pos.get("currentValue", size * cur_price) or 0)
                cash_pnl = float(pos.get("cashPnl", current_value - initial_value) or 0)
                percent_pnl = float(pos.get("percentPnl", 0) or 0)
                total_bought = float(pos.get("totalBought", 0) or 0)
                realized_pnl = float(pos.get("realizedPnl", 0) or 0)
                percent_realized_pnl = float(pos.get("percentRealizedPnl", 0) or 0)
                value = current_value

                if value < min_value or size <= 0:
                    continue

                active.append({
                    "token_id": token_id,
                    "proxy_wallet": pos.get("proxyWallet", self.proxy_wallet),
                    "condition_id": pos.get("conditionId", ""),
                    "size": size,
                    "avg_price": avg_price,
                    "cur_price": cur_price,
                    "initial_value": initial_value,
                    "current_value": current_value,
                    "cash_pnl": cash_pnl,
                    "percent_pnl": percent_pnl,
                    "total_bought": total_bought,
                    "realized_pnl": realized_pnl,
                    "percent_realized_pnl": percent_realized_pnl,
                    "unrealized_pnl": (cur_price - avg_price) * size,
                    "value": value,
                    "redeemable": bool(pos.get("redeemable", False)),
                    "mergeable": bool(pos.get("mergeable", False)),
                    "title": (pos.get("title") or "unknown")[:200],
                    "slug": pos.get("slug", ""),
                    "icon": pos.get("icon", ""),
                    "event_slug": pos.get("eventSlug", ""),
                    "outcome": pos.get("outcome", ""),
                    "outcome_index": pos.get("outcomeIndex", None),
                    "opposite_outcome": pos.get("oppositeOutcome", ""),
                    "opposite_asset": pos.get("oppositeAsset", ""),
                    "end_date": pos.get("endDate", ""),
                    "negative_risk": bool(pos.get("negativeRisk", False)),
                    "side": pos.get("side", ""),
                    "raw": pos,
                })

            return active

        except Exception as e:
            logger.error(f"✗ get_active_positions | Position fetch error: {e}")
            return []
        
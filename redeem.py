#!/usr/bin/env python3
# redeem_production.py - COMPLETE with --verbose-cache flag

import os
import time
import json
import sys
import argparse
from datetime import datetime, UTC, timezone, timedelta
from typing import List, Dict
from dataclasses import dataclass
import requests
from web3 import Web3
from eth_account import Account
import logging

# Constants
UTC = timezone.utc

logger = logging.getLogger(__name__)

# Load .env safely
if os.getenv("HEROKU") != "true":
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except:
        pass

from config import RedisCache
from lib.rpc_utils import RPCManager
from lib.polymarket_positions import PolymarketPositionManager

@dataclass
class RedeemPosition:
    condition_id: str
    indexes: List[int]
    title: str
    value: float
    size: float

class PolymarketRedeemer:
    CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
    # CTF splits use bridged USDC as collateral — verified on-chain via getPositionId.
    # Trading uses pUSD, but the CTF layer always holds USDC; redemption pays out USDC.
    USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # bridged USDC (Polygon)
    PARENT_COLLECTION_ID = bytes(32)
    
    CTF_ABI = [
        {
            "inputs": [
                {"name": "account", "type": "address"},
                {"name": "ids", "type": "uint256[]"}
            ],
            "name": "balanceOfBatch",
            "outputs": [{"name": "", "type": "uint256[]"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [{"name": "conditionId", "type": "bytes32"}],
            "name": "payoutsOf",
            "outputs": [{"name": "", "type": "uint256[]"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [
                {"name": "collateralToken", "type": "address"},
                {"name": "parentCollectionId", "type": "bytes32"},
                {"name": "conditionId", "type": "bytes32"},
                {"name": "indexSets", "type": "uint256[]"}
            ],
            "name": "redeemPositions",
            "outputs": [],
            "stateMutability": "nonpayable",
            "type": "function"
        }
    ]
    
    def __init__(self, mode: str = "high_gas", verbose_cache: bool = False):
        self.mode = mode.lower()
        self.verbose_cache = verbose_cache  # ← NEW: Verbose flag
        self._validate_env()
        self.redis = RedisCache()
        
        self._builder_creds = {
            "key": os.getenv("API_KEY"),
            "secret": os.getenv("API_SECRET"),
            "passphrase": os.getenv("API_PASSPHRASE")
        }
        
        self.private_key = os.getenv("PRIVATE_KEY")
        self.owner_account = Account.from_key(self.private_key)
        self.eoa_address = self.owner_account.address
        self.proxy_wallet = Web3.to_checksum_address(
            os.getenv("PROXY_WALLET", "0x6367BB01F6d3A257b7a71A6F9E826b59b0Be5846")
        )

        self.rpc_manager = RPCManager()
        self.w3 = self.rpc_manager.get_w3(timeout=15)
        if not self.w3.is_connected():
            raise RuntimeError("Failed to connect to RPC")
        self.ctf = self.w3.eth.contract(address=self.CTF_ADDRESS, abi=self.CTF_ABI)
        # V2 (EOA trading): positions held by EOA; V1 legacy positions held by proxy wallet
        self._pm_eoa   = PolymarketPositionManager(self.eoa_address)
        self._pm_proxy = PolymarketPositionManager(self.proxy_wallet)
        self.position_manager = self._pm_eoa  # default for show_active_positions_dashboard
        logger.info(f"init | {self.mode.upper()} mode | EOA: {self.eoa_address} | Proxy: {self.proxy_wallet} | Verbose: {self.verbose_cache}")
    
    def _validate_env(self):
        required = ["PRIVATE_KEY"]
        missing = [var for var in required if not os.getenv(var)]
        if missing:
            raise ValueError(f"Missing env vars: {missing}")
    
    def check_proxy_wallet(self) -> bool:
        try:
            code = self.w3.eth.get_code(self.proxy_wallet)
            deployed = len(code) > 0
            logger.debug(f"check_proxy_wallet | Proxy wallet deployed: {'YES' if deployed else 'NO'}")
            return deployed
        except Exception as e:
            logger.error(f"check_proxy_wallet | Proxy check failed: {e}")
            return False
    
    def show_active_positions_dashboard(self, min_value: float = 0.01, positions: list = None):
        """Get active positions via shared manager."""
        if positions is None:
            positions = self.position_manager.get_active_positions(min_value)
        
        if not positions:
            logger.debug(f"✅ show_active_positions_dashboard | No active positions (all redeemed/cleared)")
            return
        
        print("="*100)
        print("📊 ACTIVE POSITIONS DASHBOARD")
        print("="*100)

        total_value = sum(p["value"] for p in positions)
        total_realized = sum(p["cash_pnl"] for p in positions)

        print(f"Count: {len(positions)} | Total Value: ${total_value:>8.2f} | Realized PnL: ${total_realized:>+8.2f}")
        print("-"*100)
        print("MARKET                                             |   SIZE   |  PRICE |  VALUE  |   PnL   | OUTCOME")
        print("-"*100)

        for p in sorted(positions, key=lambda x: x["value"], reverse=True):
            title = p["title"][:50].ljust(50)
            size_str = f"{p['size']:>8.4f}"
            price_str = f"{p['cur_price']:>6.3f}"
            value_str = f"${p['value']:>6.2f}"
            pnl_str = f"${p['cash_pnl']:>+6.2f}"
            outcome = str(p["outcome"])

            print(f"{title} | {size_str} | {price_str} | {value_str} | {pnl_str} | {outcome}")

        print("-"*100)
                    
    def get_redeemable_positions(self, min_value: float = 0.01, positions: list = None) -> List[RedeemPosition]:
        try:
            if positions is None:
                positions = self._pm_eoa.get_active_positions(min_value)
                if not positions:
                    positions = self._pm_proxy.get_active_positions(min_value)
            logger.info(
                f"🔄 get_redeemable_positions | {len(positions)} positions (eoa={self.eoa_address[:10]}…)"
            )
                        
            # 🔥 FULL REDIS CACHING WITH VERBOSE OPTION
            winners_saved = positions_saved = skipped = 0
            
            for pos in positions:
               
                token_id = pos.get('token_id', 'asset')
                if not token_id:                                      
                    logger.warning(f"✓ get_redeemable_positions | Skipping position with missing asset ID")
                    skipped += 1
                    continue
                
                pnl_pct = float(pos.get('percent_realized_pnl', 'percentRealizedPnl'))
                title = pos.get('title', 'unknown')
                
                try:                    
                    if pnl_pct > 0:  # WINNER
                        key = f"WINNER:{token_id}"
                        realized_pnl = pos.get('realized_pnl', 'realizedPnl')
                        logger.debug(f"✓ get_redeemable_positions | [WINNER +{pnl_pct:.1%}] {title[:50]} | ${realized_pnl:.2f}")
                        winners_saved += 1
                    else:
                        key = f"ALL:{token_id}"
                        if self.verbose_cache:  # ← VERBOSE CONTROL
                            logger.debug(f"✓ get_redeemable_positions | [POS {pnl_pct:.1%}] {title[:50]}")
                        positions_saved += 1
                    end_date_str = pos.get('end_date', 'endDate')
                    if not end_date_str:
                        continue

                    try:
                        end_date = datetime.strptime(end_date_str, "%Y-%m-%d").replace(tzinfo=UTC)
                    except ValueError:
                        continue

                    if end_date > datetime.now(UTC) + timedelta(days=90):
                        continue

                    self.redis.hset(key, mapping={
                        "data": json.dumps(pos, separators=(",", ":")),
                        "captured_at": datetime.now(UTC).isoformat()
                    })
                    self.redis.expire(key, 3600 * 24 * 90)
                    
                except Exception as e:
                    logger.error(f"✗ get_redeemable_positions | Save failed {token_id[-8:]}: {e}")
                    skipped += 1
                    continue
            
            logger.debug(f"✓ get_redeemable_positions | [CACHE] Winners: {winners_saved} | Positions: {positions_saved} | Skipped: {skipped}")
            
            # Aggregate redeemable positions
            markets = {}
            total_value = 0
            
            for pos in positions:                            
                cur_price = float(pos.get('cur_price', 'curPrice'))
                size = float(pos.get('size', 0))
                value = size * cur_price
                
                is_redeemable = pos.get("redeemable") or cur_price >= 0.99
                if is_redeemable and value > min_value:
                    cid = pos.get('condition_id','conditionId')
                    total_value += value
                    
                    if cid not in markets:                        
                        markets[cid] = {
                            'title': pos['title'][:60],
                            'value': 0,
                            'size': 0,
                            'outcomeIndex': int(pos.get('outcome_index', 'outcomeIndex'))
                        }
                    markets[cid]['value'] += value
                    markets[cid]['size'] += size
                
                if pos.get("redeemable") or cur_price >= 0.99:
                    logger.debug(f"get_redeemable_positions | [WINNER DETECTED] {pos['title'][:50]} -> ${value:.2f}")
            
            positions_list = [
                RedeemPosition(
                    condition_id=cid,
                    indexes=[1, 2],
                    title=m['title'],
                    value=m['value'],
                    size=m['size']
                )
                for cid, m in markets.items()
            ]
            
            logger.debug(f"✓get_redeemable_positions | [READY] Found {len(positions_list)} redeemable | Total: ${total_value:.2f}")
            return positions_list
            
        except Exception as e:
            logger.error(f"✗ get_redeemable_positions | [API ERROR] {e}")
            return []
   
    def redeem_high_gas(self, position: RedeemPosition) -> bool:
        if not self.w3.is_connected():
            return False

        max_retries = 3
        for attempt in range(max_retries):
            try:
                nonce = self.w3.eth.get_transaction_count(self.owner_account.address)
                base_fee = self.w3.eth.get_block('latest')['baseFeePerGas']
                base_fee_gwei = int(Web3.from_wei(base_fee, 'gwei')) + 150
                priority_fee_gwei = 50 + (attempt * 30)

                tx = self.ctf.functions.redeemPositions(
                    self.USDC_ADDRESS,
                    self.PARENT_COLLECTION_ID,
                    Web3.to_bytes(hexstr=position.condition_id),
                    position.indexes
                ).build_transaction({
                    'from': self.owner_account.address,
                    'value': 0,
                    'gas': 500_000,
                    'maxFeePerGas': Web3.to_wei(base_fee_gwei + priority_fee_gwei, 'gwei'),
                    'maxPriorityFeePerGas': Web3.to_wei(priority_fee_gwei, 'gwei'),
                    'nonce': nonce,
                    'chainId': 137,
                })

                signed_tx = self.owner_account.sign_transaction(tx)
                tx_hash = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)

                logger.info(f"redeem_high_gas | [PENDING] https://polygonscan.com/tx/{tx_hash.hex()}")
                receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
                if receipt.status == 1:
                    logger.info(f"redeem_high_gas | [CONFIRMED] ${position.value:.2f} block={receipt.blockNumber}")
                    return True
                else:
                    logger.error(f"redeem_high_gas | [REVERTED] tx={tx_hash.hex()}")
                    return False

            except Exception as e:
                error_str = str(e).lower()
                if any(x in error_str for x in ["already known", "nonce too low"]):
                    logger.info(f"redeem_high_gas | [TX] Already pending")
                    return True
                elif "underpriced" in error_str:
                    logger.info(f"redeem_high_gas | [GAS] Retry {attempt+1}/{max_retries}")
                    time.sleep(2)
                else:
                    logger.error(f"redeem_high_gas | [TX FAIL] {e}")
                    time.sleep(2)

        return False
    
    def redeem_gasless(self, position: RedeemPosition) -> bool:
        if not all(self._builder_creds.values()):
            return self.redeem_high_gas(position)
        
        try:
            redeem_fn = self.ctf.functions.redeemPositions(
                self.USDC_ADDRESS,
                self.PARENT_COLLECTION_ID,
                Web3.to_bytes(hexstr=position.condition_id),
                position.indexes
            )
            calldata = redeem_fn.build_transaction({'from': self.eoa_address})['data']
            
            url = "https://relayer-v2.polymarket.com/submit"
            headers = {
                "POLYMARKET_BUILDER_API_KEY": self._builder_creds["key"],
                "POLYMARKET_BUILDER_SECRET": self._builder_creds["secret"],
                "POLYMARKET_BUILDER_PASSPHRASE": self._builder_creds["passphrase"],
                "Content-Type": "application/json"
            }
            payload = {
                "transactions": [{
                    "to": self.CTF_ADDRESS,
                    "data": calldata,
                    "value": "0x0",
                    "wallet": self.eoa_address
                }]
            }
            
            resp = requests.post(url, headers=headers, json=payload, timeout=45)
            if resp.status_code in [200, 202]:
                # Relayer accepted — fall through to high-gas to get on-chain confirmation.
                # Gasless gives no tx hash we can wait on, so high-gas acts as the
                # confirmed path (it will hit "already known" if relayer already broadcast it).
                logger.info(f"redeem_gasless | [RELAYER ACCEPTED] falling through to confirmed path")
                return self.redeem_high_gas(position)
            else:
                logger.error(f"redeem_gasless | [GASLESS] FAIL {resp.status_code}")
                return self.redeem_high_gas(position)
        except Exception as e:
            logger.error(f"redeem_gasless | [GASLESS ERROR] {e}")
            return self.redeem_high_gas(position)
    
    def run_redeem_pipeline(self, auto_confirm: bool = False, positions: list = None) -> dict:
        logger.debug("=" * 80)
        logger.debug("POLYMARKET PRODUCTION REDEEMER")
        logger.debug(f"Mode: {self.mode.upper()} | Auto: {auto_confirm}")
        logger.debug("=" * 80)
        
        positions = self.get_redeemable_positions(positions=positions)
        if not positions:
            logger.debug("No profitable positions found")
            return {"status": "no_positions"}
        
        total_value = sum(p.value for p in positions)
        gas_cost = len(positions) * 0.05 if self.mode == "high_gas" else 0
        # net_profit = total_value - gas_cost
        
        # self._print_summary(positions, total_value, gas_cost, net_profit)
        
        if not auto_confirm:
            response = input(f"\nExecute {len(positions)}? [Y/n]: ").lower().strip()
            if response not in ['', 'y', 'yes']:
                return {"status": "cancelled"}
        
        successful = 0
        for i, pos in enumerate(positions, 1):
            redis_key = f"redeemed:{pos.condition_id}"
            if self.redis.exists(redis_key):
                logger.debug(f"run_redeem_pipeline | [{i}/{len(positions)}] Skipping already-redeemed {pos.title[:40]} (within 24h)")
                continue
            logger.debug(f"\n run_redeem_pipeline | [{i}/{len(positions)}] {pos.title}")
            success = self.redeem_gasless(pos) if self.mode == "gasless" else self.redeem_high_gas(pos)
            if success:
                self.redis.setex(redis_key, 86400, "1")  # skip for 24h — API cache lag
                successful += 1
        
        logger.debug(f"run_redeem_pipeline | [DONE] {successful}/{len(positions)} successful")
        return {"status": "success"}
    
    def _print_summary(self, positions, total_value, gas_cost, net_profit):
        print("\nPROFIT SUMMARY")
        print("-" * 80)
        for i, pos in enumerate(positions, 1):
            print(f"{i:2d}. {pos.title:<50} ${pos.value:>6.2f}")
        print("-" * 80)
        print(f"TOTAL:     ${total_value:>6.2f}")
        print(f"GAS COST:  ${gas_cost:>6.2f}")
        print(f"NET:       ${net_profit:>6.2f}")

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler()]
    )
    parser = argparse.ArgumentParser(description="Polymarket Redeemer")
    parser.add_argument("--mode", choices=["high_gas", "gasless"], default="high_gas")
    parser.add_argument("--auto", action="store_true", help="Auto-confirm")
    parser.add_argument("--verbose-cache", action="store_true", help="Verbose caching logs") 
    args = parser.parse_args()
    
    try:
        redeemer = PolymarketRedeemer(
            mode=args.mode,
            verbose_cache=args.verbose_cache
        )
        redeemer.show_active_positions_dashboard()
        redeemer.run_redeem_pipeline(auto_confirm=args.auto)
    except KeyboardInterrupt:
        logger.info("main | Cancelled")
    except Exception as e:
        logger.error(f"main | Fatal: {e}", exc_info=True)
        sys.exit(1)

_redeemer: "PolymarketRedeemer | None" = None

def run_redeem_non_interactive(mode: str = "high_gas", verbose_cache: bool = False) -> Dict:
    """
    Non-interactive runner for bot.py integration.
    Returns: {"status": str, "count": int, "value": float, "message": str}
    """
    global _redeemer
    try:
        if _redeemer is None:
            _redeemer = PolymarketRedeemer(mode=mode, verbose_cache=verbose_cache)

        positions = _redeemer._pm_eoa.get_active_positions()
        if not positions:
            positions = _redeemer._pm_proxy.get_active_positions()
        _redeemer.show_active_positions_dashboard(positions=positions)
        result = _redeemer.run_redeem_pipeline(auto_confirm=True, positions=positions)
        return result

    except Exception as e:
        _redeemer = None  # force re-init on next call if something went wrong
        return {"status": "error", "count": 0, "value": 0.0, "message": str(e)}

if __name__ == "__main__":
    main()
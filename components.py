#!/usr/bin/env python3
"""
Bot Components - Centralized creation and configuration of all bot components.
"""

import logging

from lib.clob_factory import ClobClientFactory
from lib.balance_checker import BalanceChecker
from lib.geoblock_checker import GeoBlockChecker
from lib.rpc_utils import RPCManager
from lib.order_manager import OrderManager
from lib.polymarket_finder import PolymarketFinder 
from price_tracker import PriceTracker 

logger = logging.getLogger(__name__)

CTF_ABI = [{"constant": True, "inputs": [{"name": "conditionId", "type": "bytes32"}], 
           "name": "payoutsOf", "outputs": [{"name": "", "type": "uint256[2]"}], "type": "function"}]

class Components:
    @staticmethod
    def create() -> dict:  
        rpc_manager = RPCManager()
        w3, rpc_url = rpc_manager.get_w3_with_url()
        
        logger.info(f"🔗 create | Bot RPC: {rpc_url}")
        
        # Clob client (independent of RPC)
        client = ClobClientFactory.from_env().create_client()
        order_mgr = OrderManager(client)  
        
        # CTF contract
        ctf_address = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
        ctf = w3.eth.contract(address=ctf_address, abi=CTF_ABI)
        
        checker = BalanceChecker()
        tracker = PriceTracker()
        finder = PolymarketFinder()
        geo = GeoBlockChecker()

        return {
            'client': client,
            'order_mgr': order_mgr,
            'checker': checker,
            'w3': w3,
            'rpc_url': rpc_url,
            'rpc_manager': rpc_manager,
            'geo_checker': geo,
            'price_tracker': tracker,
            'finder': finder, 
            'ctf': ctf,
        }
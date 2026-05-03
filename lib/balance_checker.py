#!/usr/bin/env python3
import os
import logging
from typing import Tuple
from web3 import Web3
from dotenv import load_dotenv
from dataclasses import dataclass
from lib.rpc_utils import RPCManager
from decimal import Decimal

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class BalanceInfo:
    """Immutable dataclass for balance information."""
    pusd: float
    allowance: float
    pub_key: str
    raw_pusd: int
    raw_allowance: int
    clob_proxy: str
    rpc_status: str
    proxy_pusd: float

class BalanceChecker:
    PUSD_DECIMALS = 6
    CACHE_BALANCE_USD = 5
    CACHE_ALLOWANCE_RAW = int(1e36)
    CACHE_PROXY_USD = 0.0

    def __init__(self, private_key_env: str = "PRIVATE_KEY") -> None:
        load_dotenv()

        self.rpc_manager = RPCManager()
        self.w3 = self.rpc_manager.get_w3()
        self.rpc_url = self.rpc_manager.last_success

        self.priv_key = os.environ[private_key_env]
        self.account = Web3().eth.account.from_key(self.priv_key)
        self.pub_key = self.account.address

        self.pusd_address = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"  # pUSD (Polymarket V2 collateral)
        self.clob_proxy = os.getenv("PROXY_WALLET", "0x6367BB01F6d3A257b7a71A6F9E826b59b0Be5846")
        # V2: pUSD allowances are granted directly to exchange contracts, not a proxy wallet
        self._allowance_spender = os.getenv("ALLOWANCE_SPENDER", "0xE111180000d2663C0091e4f400237545B87B996B")
        self.pusd_abi = [
            {
                "constant": True,
                "inputs": [{"name": "_owner", "type": "address"}],
                "name": "balanceOf",
                "outputs": [{"name": "balance", "type": "uint256"}],
                "type": "function"
            },
            {
                "constant": True,
                "inputs": [{"name": "_owner", "type": "address"}, {"name": "_spender", "type": "address"}],
                "name": "allowance",
                "outputs": [{"name": "", "type": "uint256"}],
                "type": "function"
            }
        ]

    def _get_fresh_w3(self) -> Web3:
        return self.rpc_manager.get_w3()

    def _fetch_balances(self, contract) -> Tuple[int, int, int]:
        try:
            meta_balance = contract.functions.balanceOf(self.pub_key).call()
            proxy_balance = contract.functions.balanceOf(self.clob_proxy).call()
            allowance_raw = contract.functions.allowance(self.pub_key, self._allowance_spender).call()
            return meta_balance, proxy_balance, allowance_raw
        except (ValueError, Exception) as e:
            logger.warning(f"✗ fetch_balances | Failed to fetch balances: {type(e).__name__}: {e}")
            raise

    def _to_float(self, raw: int) -> float:
        return float(raw) / (10 ** self.PUSD_DECIMALS)

    @property
    def info(self) -> BalanceInfo:
        try:
            w3 = self._get_fresh_w3()
            contract = w3.eth.contract(
                address=Web3.to_checksum_address(self.pusd_address),
                abi=self.pusd_abi
            )

            meta_raw, proxy_raw, allowance_raw = self._fetch_balances(contract)

            return BalanceInfo(
                pusd=self._to_float(meta_raw),
                allowance=self._to_float(allowance_raw),
                pub_key=self.pub_key,
                raw_pusd=meta_raw,
                raw_allowance=allowance_raw,
                clob_proxy=self.clob_proxy,
                rpc_status="online",
                proxy_pusd=self._to_float(proxy_raw)
            )
        except Exception as e:
            logger.debug(f"✗ info | RPC failed, using cache: {e}")
            return BalanceInfo(
                pusd=self.CACHE_BALANCE_USD,
                allowance=self._to_float(self.CACHE_ALLOWANCE_RAW),
                pub_key=self.pub_key,
                raw_pusd=int(self.CACHE_BALANCE_USD * 10**self.PUSD_DECIMALS),
                raw_allowance=self.CACHE_ALLOWANCE_RAW,
                clob_proxy=self.clob_proxy,
                rpc_status="cached",
                proxy_pusd=self.CACHE_PROXY_USD
            )

    def format_allowance(self, allowance: float) -> str:
        return "∞" if allowance > 1e30 else f"${allowance:,.2f}"

    @property
    def pusd_balance(self) -> float:
        return self.info.pusd

    @property
    def pusd_allowance(self) -> float:
        return self.info.allowance

    def log_status(self) -> None:
        info = self.info
        logger.info(f"🔍 WALLET STATUS | {info.rpc_status}")
        logger.info(f"💵 MetaMask: ${info.pusd:>10,.2f}")
        logger.info(f"🎯 Proxy:    ${info.proxy_pusd:>10,.2f}")
        logger.info(f"👛 MetaMask:  {info.pub_key}")
        logger.info(f"🔐 Proxy:     {info.clob_proxy}")
        logger.info(f"🔗 RPC: {self.rpc_url}")
        logger.info(f"🔗 MetaMask: https://polygonscan.com/address/{info.pub_key}")
        logger.info(f"🔗 Polymarket: https://polymarket.com/profile/{info.clob_proxy}")

    def check_trading_capacity(self, usd_amount: float) -> bool:
        info = self.info
        buffer_amount = usd_amount * 1.02
        balance_ok = info.pusd >= buffer_amount
        allowance_ok = info.allowance >= buffer_amount

        status = "✓ READY" if balance_ok and allowance_ok else "✗ LOW FUNDS"
        logger.debug(
            f"💰 check_trading_capacity | Trading ${usd_amount}: {status} | "
            f"MetaMask: ${info.pusd:.2f} | Allowance: {self.format_allowance(info.allowance)}"
        )
        return balance_ok and allowance_ok

    def __str__(self) -> str:
        info = self.info
        return f"BalanceChecker({self.pub_key}) MetaMask=${info.pusd:.2f}"

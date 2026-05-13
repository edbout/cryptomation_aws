#!/usr/bin/env python3
"""
Wrap USDC.e → pUSD via Polymarket's CollateralOnramp.

Why this exists:
  CTF redemptions pay out in bridged USDC (USDC.e), but trading uses pUSD.
  After every redeem, USDC.e accumulates and pUSD dries up. This module
  sweeps any USDC.e balance back into pUSD 1:1 so the bot can keep trading.

Contract:  0x93070a847efEf7F70739046A929D47a521F5B8ee (CollateralOnramp)
Function:  wrap(address _asset, address _to, uint256 _amount)
Docs:      https://docs.polymarket.com/concepts/pusd
"""
import os
import logging
from typing import Optional
from web3 import Web3

logger = logging.getLogger(__name__)

# ─── addresses ──────────────────────────────────────────────────────────
USDC_E_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # bridged USDC on Polygon
PUSD_ADDRESS   = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"  # pUSD (Polymarket V2 collateral)
ONRAMP_ADDRESS = "0x93070a847efEf7F70739046A929D47a521F5B8ee"  # Permissionless Collateral Onramp

DECIMALS       = 6           # both USDC.e and pUSD use 6 decimals
INFINITE       = 2**256 - 1
GAS_PRICE_GWEI = 500          # matches set_allowances_once.py — Polygon needs juice

RPCS = [
    "https://1rpc.io/matic",
    "https://polygon-rpc.com",
    "https://rpc-mainnet.matic.network",
]

ERC20_ABI = [
    {"inputs": [{"name": "owner", "type": "address"}, {"name": "spender", "type": "address"}],
     "name": "allowance", "outputs": [{"name": "", "type": "uint256"}],
     "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "account", "type": "address"}],
     "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}],
     "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "_spender", "type": "address"}, {"name": "_value", "type": "uint256"}],
     "name": "approve", "outputs": [{"name": "", "type": "bool"}],
     "stateMutability": "nonpayable", "type": "function"},
]
ONRAMP_ABI = [
    {"inputs": [
        {"name": "_asset",  "type": "address"},
        {"name": "_to",     "type": "address"},
        {"name": "_amount", "type": "uint256"}],
     "name": "wrap", "outputs": [],
     "stateMutability": "nonpayable", "type": "function"},
]


def _connect() -> Optional[Web3]:
    for url in RPCS:
        try:
            w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 30}))
            if w3.is_connected():
                logger.debug(f"usdce_wrapper | connected via {url}")
                return w3
        except Exception:
            continue
    return None


def wrap_usdce_to_pusd(min_amount_usdc: float = 1.0, timeout: int = 180) -> Optional[str]:
    """
    Sweep the wallet's entire USDC.e balance into pUSD (1:1, no fees).

    Args:
        min_amount_usdc: skip if balance below this (avoids dust gas waste)
        timeout: seconds to wait for receipts

    Returns:
        wrap tx hash on success, None if skipped or failed.
    """
    priv_key = os.environ.get("PRIVATE_KEY")
    if not priv_key:
        logger.error("wrap_usdce_to_pusd | PRIVATE_KEY env missing")
        return None

    w3 = _connect()
    if not w3:
        logger.error("wrap_usdce_to_pusd | no RPC available")
        return None

    account = w3.eth.account.from_key(priv_key)
    wallet  = account.address

    usdce       = w3.eth.contract(address=Web3.to_checksum_address(USDC_E_ADDRESS), abi=ERC20_ABI)
    onramp      = w3.eth.contract(address=Web3.to_checksum_address(ONRAMP_ADDRESS), abi=ONRAMP_ABI)
    onramp_addr = Web3.to_checksum_address(ONRAMP_ADDRESS)
    usdce_addr  = Web3.to_checksum_address(USDC_E_ADDRESS)

    # 1) balance gate — don't burn gas on dust
    raw_balance = usdce.functions.balanceOf(wallet).call()
    balance     = raw_balance / 10**DECIMALS
    if balance < min_amount_usdc:
        logger.info(f"wrap_usdce_to_pusd | balance ${balance:.4f} < ${min_amount_usdc:.2f} — skipping")
        return None

    logger.info(f"wrap_usdce_to_pusd | wrapping ${balance:.4f} USDC.e → pUSD")

    gas_price = w3.to_wei(str(GAS_PRICE_GWEI), "gwei")
    nonce     = w3.eth.get_transaction_count(wallet, "pending")

    # 2) one-time approval of the Onramp to spend USDC.e
    current_allowance = usdce.functions.allowance(wallet, onramp_addr).call()
    if current_allowance < raw_balance:
        logger.info("wrap_usdce_to_pusd | approving CollateralOnramp on USDC.e (one-time)")
        approve_tx = usdce.functions.approve(onramp_addr, INFINITE).build_transaction({
            "from":     wallet,
            "nonce":    nonce,
            "gas":      80_000,
            "gasPrice": gas_price,
            "chainId":  137,
        })
        signed  = w3.eth.account.sign_transaction(approve_tx, priv_key)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        logger.info(f"wrap_usdce_to_pusd | approve tx https://polygonscan.com/tx/{tx_hash.hex()}")
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=timeout)
        if receipt.status != 1:
            logger.error(f"wrap_usdce_to_pusd | approve reverted: {tx_hash.hex()}")
            return None
        nonce += 1

    # 3) wrap — sweeps full balance to pUSD in the same wallet
    wrap_tx = onramp.functions.wrap(usdce_addr, wallet, raw_balance).build_transaction({
        "from":     wallet,
        "nonce":    nonce,
        "gas":      200_000,
        "gasPrice": gas_price,
        "chainId":  137,
    })
    signed  = w3.eth.account.sign_transaction(wrap_tx, priv_key)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    logger.info(f"wrap_usdce_to_pusd | wrap tx https://polygonscan.com/tx/{tx_hash.hex()}")
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=timeout)
    if receipt.status != 1:
        logger.error(f"wrap_usdce_to_pusd | wrap reverted: {tx_hash.hex()}")
        return None

    logger.info(f"wrap_usdce_to_pusd | ✅ wrapped ${balance:.4f} USDC.e → pUSD in block {receipt.blockNumber}")
    return tx_hash.hex()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    wrap_usdce_to_pusd()

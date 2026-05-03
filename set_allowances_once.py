#!/usr/bin/env python3
"""
ONE-TIME UNIVERSAL APPROVAL — Polymarket V2 (live April 28 2026)
Covers ALL market types: standard binary + neg risk (BTC/ETH/SOL/XRP 5-min etc.)
"""
import os
import sys
import time
from web3 import Web3
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# V2 CONTRACT ADDRESSES
# ============================================================
PUSD_ADDRESS     = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"  # pUSD (new collateral)
CTF_ADDRESS      = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"  # CTF ERC-1155 (unchanged)
CTF_EXCHANGE_V2  = "0xE111180000d2663C0091e4f400237545B87B996B"  # CTF Exchange V2
NEG_RISK_CTF_V2  = "0xe2222d279d744050d28e00520010520000310F59"  # Neg Risk CTF Exchange V2
NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"  # Neg Risk Adapter (unchanged)
# ============================================================

RPCS = [
    "https://1rpc.io/matic",
    "https://polygon-rpc.com",
    "https://rpc-mainnet.matic.network",
]
INFINITE       = 2**256 - 1
GAS_PRICE_GWEI = 500   # high enough to get picked up fast on Polygon
TIMEOUT        = 300   # 5 minutes

ERC20_ABI = [
    {"inputs": [{"name": "owner", "type": "address"}, {"name": "spender", "type": "address"}],
     "name": "allowance", "outputs": [{"name": "", "type": "uint256"}],
     "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "_spender", "type": "address"}, {"name": "_value", "type": "uint256"}],
     "name": "approve", "outputs": [{"name": "", "type": "bool"}],
     "stateMutability": "nonpayable", "type": "function"},
]
ERC1155_ABI = [
    {"inputs": [{"name": "operator", "type": "address"}, {"name": "approved", "type": "bool"}],
     "name": "setApprovalForAll", "outputs": [],
     "stateMutability": "nonpayable", "type": "function"},
    {"inputs": [{"name": "account", "type": "address"}, {"name": "operator", "type": "address"}],
     "name": "isApprovedForAll", "outputs": [{"name": "", "type": "bool"}],
     "stateMutability": "view", "type": "function"},
]


def connect():
    for url in RPCS:
        try:
            w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 30}))
            if w3.is_connected():
                print(f"ℹ️  Connected to {url}")
                return w3
        except Exception:
            pass
        print(f"⚠️  {url} failed, trying next...")
    return None


def main():
    w3 = connect()
    if not w3:
        print("❌ All RPCs failed — check your internet connection")
        return

    priv_key = os.environ["PRIVATE_KEY"]
    account  = w3.eth.account.from_key(priv_key)
    wallet   = account.address

    print(f"✅ Chain ID : {w3.eth.chain_id}")
    print(f"✅ Wallet   : {wallet}")
    print(f"✅ Gas price: {GAS_PRICE_GWEI} gwei")

    # Pass a nonce override on the command line to resume after a failure:
    #   python set_allowances_once.py 614
    if len(sys.argv) > 1:
        nonce = int(sys.argv[1])
        print(f"⚠️  Using override nonce: {nonce}")
    else:
        nonce = w3.eth.get_transaction_count(wallet, "pending")
        print(f"ℹ️  Starting nonce (pending): {nonce}")

    pusd = w3.eth.contract(address=Web3.to_checksum_address(PUSD_ADDRESS), abi=ERC20_ABI)
    ctf  = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDRESS),  abi=ERC1155_ABI)

    pusd_spenders = [
        (CTF_EXCHANGE_V2,  "CTF Exchange V2"),
        (NEG_RISK_CTF_V2,  "Neg Risk CTF V2"),
        (NEG_RISK_ADAPTER, "Neg Risk Adapter"),
    ]
    ctf_operators = [
        (CTF_EXCHANGE_V2,  "CTF Exchange V2"),
        (NEG_RISK_CTF_V2,  "Neg Risk CTF V2"),
        (NEG_RISK_ADAPTER, "Neg Risk Adapter"),
    ]

    failed = []

    def send(fn, desc):
        nonlocal nonce
        tx = fn.build_transaction({
            "from":     wallet,
            "nonce":    nonce,
            "gas":      150000,
            "gasPrice": w3.to_wei(str(GAS_PRICE_GWEI), "gwei"),
            "chainId":  137,
        })
        signed = w3.eth.account.sign_transaction(tx, priv_key)
        try:
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        except Exception as e:
            print(f"  ❌ RPC rejected: {e}")
            failed.append((desc, nonce, str(e)))
            nonce += 1
            return
        print(f"  📤 {desc}: {tx_hash.hex()}")
        print(f"     🔗 https://polygonscan.com/tx/{tx_hash.hex()}")
        print(f"     ⏳ Waiting up to {TIMEOUT}s for confirmation...")
        try:
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=TIMEOUT)
            status  = "✅" if receipt.status == 1 else "❌ REVERTED"
            print(f"     {status} Confirmed in block {receipt.blockNumber}")
        except Exception:
            print(f"     ⚠️  Timed out — check the Polygonscan link above.")
            print(f"     The tx may still confirm. Wait a minute, check the link,")
            print(f"     then re-run with: python set_allowances_once.py {nonce + 1}")
            failed.append((desc, nonce, "timeout"))
        nonce += 1
        time.sleep(1)  # small pause between txs

    print("\n─── pUSD (ERC-20) approvals ─────────────────────────────")
    for spender_addr, label in pusd_spenders:
        spender = Web3.to_checksum_address(spender_addr)
        try:
            current = pusd.functions.allowance(wallet, spender).call()
            if current >= INFINITE:
                print(f"  ✅ {label}: already approved")
                continue
        except Exception:
            pass  # if allowance check fails, try to approve anyway
        print(f"  ⏳ {label}: approving...")
        send(pusd.functions.approve(spender, INFINITE), label)

    print("\n─── CTF ERC-1155 setApprovalForAll ──────────────────────")
    for operator_addr, label in ctf_operators:
        operator = Web3.to_checksum_address(operator_addr)
        try:
            approved = ctf.functions.isApprovedForAll(wallet, operator).call()
            if approved:
                print(f"  ✅ {label}: already approved")
                continue
        except Exception:
            pass
        print(f"  ⏳ {label}: approving...")
        send(ctf.functions.setApprovalForAll(operator, True), label)

    print(f"\n─── Summary ──────────────────────────────────────────────")
    if not failed:
        print(f"🎉 All V2 approvals done!")
    else:
        print(f"⚠️  {len(failed)} approval(s) need attention:")
        for desc, n, reason in failed:
            print(f"   • {desc} (nonce {n}): {reason}")
        print(f"   Check Polygonscan for each tx above, then re-run if needed.")

    print(f"\n🔗 https://polygonscan.com/address/{wallet}#tokentxns")
    print()
    print("📌 Next step: wrap your USDC.e → pUSD before trading.")
    print("   Log into polymarket.com — you'll see a one-time conversion prompt.")

if __name__ == "__main__":
    main()
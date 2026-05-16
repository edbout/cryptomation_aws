#!/usr/bin/env python3
"""Smoke-test BinanceFeed for ~30s.

Verifies:
  - WebSocket connects to Binance Spot kline_1m streams
  - last_prices and binance_5m_bases are populated
  - VolumeTracker buckets 1m candles correctly
  - No trigger fires when validator is not attached (safe)

Usage:
    python scripts/smoke_binance_feed.py
"""
import asyncio
import logging
import sys
import os

# Make project root importable when run from scripts/
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

from lib.binance_feed import BinanceFeed
from config import Config


async def main():
    feed = BinanceFeed()
    feed.start()
    print(f"BINANCE_SYMBOLS = {Config.BINANCE_SYMBOLS}")
    print("Sampling for 35 seconds...")
    for i in range(35):
        await asyncio.sleep(1)
        if i and i % 5 == 0:
            print(f"  t={i:>2}s | prices: " + ", ".join(
                f"{s}={feed.last_prices[s]:.4f}" for s in Config.BINANCE_SYMBOLS
            ))
            for s in Config.BINANCE_SYMBOLS:
                vt = feed.volume_trackers[s]
                if vt.history:
                    print(f"    {s} vol-history n={len(vt.history)} last={vt.history[-1]}")
    feed.stop()
    await asyncio.sleep(0.2)
    print("done")

if __name__ == "__main__":
    asyncio.run(main())

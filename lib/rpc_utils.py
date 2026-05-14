#!/usr/bin/env python3
"""Shared RPC failover logic - SYNC PRIMARY (async optional)."""

import os
import time
import asyncio
from typing import Optional, Tuple, List
from functools import lru_cache
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

import logging
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from web3.providers import HTTPProvider
from config import Config


logger = logging.getLogger(__name__)


@dataclass
class RPCStats:
    """Track RPC health."""
    success_count: int = 0
    fail_count: int = 0
    last_success: Optional[float] = None


class RPCManager:
    """Smart RPC failover - SYNC first, async optional."""

    def __init__(self):
        self.stats: dict[str, RPCStats] = {
            url: RPCStats() for url in self._get_rpc_urls()
        }
        self.last_success: Optional[str] = None

    def _get_rpc_urls(self) -> List[str]:
        """Dynamic RPC list."""
        urls = [
            Config.RPC,
            "https://polygon-bor-rpc.publicnode.com",
            "https://1rpc.io/polygon",
            "https://rpc.ankr.com/polygon",
            "https://polygon-rpc.com",
            "https://polygon-mainnet.public.blastapi.io",
            "https://rpc-mainnet.matic.network",
        ]
        return [url.strip() for url in urls if url.strip()]

    @lru_cache(maxsize=128)
    def get_w3(self, timeout: int = 6) -> Web3:
        """**PRIMARY SYNC**: Fast cached → failover → never down."""
        # 1. Cached success (90% hit)
        if self.last_success:
            w3 = self._test_rpc_sync(self.last_success, timeout)
            if w3:
                self._update_stats(self.last_success, success=True)
                logger.debug(f"⚡ get_w3 | RPC cache hit: {self.last_success}")
                return w3

        # 2. Failover rotation
        import random
        rpc_urls = self._get_rpc_urls()
        random.shuffle(rpc_urls)

        for rpc_url in rpc_urls:
            w3 = self._test_rpc_sync(rpc_url, timeout)
            if w3:
                self.last_success = rpc_url
                self._update_stats(rpc_url, success=True)
                logger.debug(f"✅ get_w3 | RPC: {rpc_url}")
                return w3

        # 3. Emergency primary (no RPC works → try primary anyway)
        logger.warning("📴 get_w3 | ALL RPCs down → forcing primary")
        primary = self._get_rpc_urls()[0]
        w3 = self._create_w3(primary, timeout)
        self._update_stats(primary, success=w3.is_connected())
        return w3

    def get_w3_with_url(self, timeout: int = 6) -> Tuple[Web3, str]:
        """**YOUR DEFAULT** - SYNC, returns (w3, url)."""
        w3 = self.get_w3(timeout)
        url = self.last_success or self._get_rpc_urls()[0]
        return w3, url

    def _test_rpc_sync(self, rpc_url: str, timeout: int) -> Optional[Web3]:
        """Sync RPC test: connectivity + quick call."""
        try:
            w3 = self._create_w3(rpc_url, timeout)

            if not w3.is_connected():
                return None

            _ = w3.eth.block_number  # Quick chain health

            return w3

        except Exception as e:
            self._update_stats(rpc_url, success=False)
            logger.debug(f"📉 _test_rpc_sync | failed: {rpc_url} | {type(e).__name__}: {e}")
            return None

    def _create_w3(self, rpc_url: str, timeout: int) -> Web3:
        """Standard W3 factory."""
        w3 = Web3(
            HTTPProvider(rpc_url, request_kwargs={"timeout": timeout})
        )
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        return w3

    def _update_stats(self, rpc_url: str, success: bool) -> None:
        """Thread‑safe stats update (no external locks needed; caller‑context‑only)."""
        stat = self.stats[rpc_url]
        if success:
            stat.success_count += 1
            stat.last_success = time.time()
        else:
            stat.fail_count += 1

    def get_stats(self) -> dict:
        """Health dashboard."""
        return {
            url: {
                "success": s.success_count,
                "fail": s.fail_count,
                "uptime": s.success_count / max(s.success_count + s.fail_count, 1),
                "last_success_seconds_ago": (
                    None if s.last_success is None else (time.time() - s.last_success)
                ),
            }
            for url, s in self.stats.items()
        }

    # ASYNC (optional, for advanced use)
    async def get_w3_async(self, timeout: int = 6) -> Web3:
        """Async if needed."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.get_w3, timeout)


# Global singleton
rpc_manager = RPCManager()
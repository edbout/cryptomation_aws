#!/usr/bin/env python3
"""Real-time Polymarket mid-price cache backed by a WebSocket subscription.

Instead of calling `client.get_midpoint(token_id)` via HTTP on every order
attempt (100–400 ms round-trip), this module maintains a live in-memory dict
  token_id → float (mid-price)
that is updated by the same wss://ws-live-data.polymarket.com server already
used for the Chainlink price feed.

Usage
-----
    from lib.poly_mid_cache import POLY_MID_CACHE

    # Start the feed (call once from the asyncio event loop)
    asyncio.create_task(POLY_MID_CACHE.run())

    # Tell it which tokens to subscribe to (safe to call at any time)
    POLY_MID_CACHE.subscribe(["token_id_1", "token_id_2"])

    # Read cached mid (returns None if stale / not yet received → fall back to HTTP)
    mid = POLY_MID_CACHE.get("token_id_1")

Design notes
------------
* Mirrors the ChainlinkFeed pattern from main.py: same WS URL, same subscribe
  envelope, same reconnect logic.
* Subscribes to "type": "book" events.  Book snapshots give us best-bid and
  best-ask so we can compute mid = (best_bid + best_ask) / 2.  We also accept
  "price_change" and "last_trade_price" events as cheaper alternatives if the
  server emits them.
* A cached price is considered stale after STALE_SECS seconds and will cause
  the caller to fall back to the HTTP get_midpoint call transparently.
* Token subscriptions can be added at any time; if the socket is already open
  a re-subscribe message is sent immediately, otherwise they are queued and
  sent on the next (re)connect.
"""

import asyncio
import json
import logging
import time
from typing import Dict, Optional, Set

import websockets

from config import Config

logger = logging.getLogger(__name__)

# Treat a cached price as stale this many seconds after the last WebSocket update.
# 30 s is conservative — the WS should push book updates every few seconds on
# active markets.  Callers fall back to HTTP on a stale / missing entry.
STALE_SECS: float = 30.0


class PolymarketMidCache:
    """Live mid-price cache for Polymarket YES tokens.

    Thread-safety: read (`get`) is safe from any thread; `subscribe` and the
    internal `_set` are only called from the asyncio event loop.
    """

    def __init__(self) -> None:
        self._prices: Dict[str, float] = {}   # token_id → latest mid
        self._ts: Dict[str, float] = {}        # token_id → epoch of last update
        self._subscribed: Set[str] = set()     # token_ids the WS is subscribed to
        self._pending: Set[str] = set()        # queued until next (re)connect
        self._running: bool = False
        self._hit: int = 0                     # cache hits since startup
        self._miss: int = 0                    # cache misses (stale / absent)

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def get(self, token_id: str) -> Optional[float]:
        """Return a fresh cached mid, or None if stale / not yet received.

        None means the caller should fall back to HTTP get_midpoint.
        """
        ts = self._ts.get(token_id)
        if ts is None or (time.time() - ts) > STALE_SECS:
            self._miss += 1
            return None
        self._hit += 1
        return self._prices[token_id]

    def subscribe(self, token_ids: list) -> None:
        """Queue token_ids for subscription.  Safe to call before run() starts
        and while the WebSocket is live (sends a fresh subscribe message)."""
        new = set(token_ids) - self._subscribed - self._pending
        if new:
            self._pending.update(new)
            logger.debug("PolymarketMidCache | queued %d new token(s) for subscription", len(new))

    def stats(self) -> str:
        total = self._hit + self._miss
        hit_rate = 100 * self._hit / total if total else 0
        return (
            f"hits={self._hit} misses={self._miss} "
            f"hit_rate={hit_rate:.0f}% cached_tokens={len(self._prices)}"
        )

    # ------------------------------------------------------------------ #
    # Async run loop (mirrors ChainlinkFeed.listen_all)                   #
    # ------------------------------------------------------------------ #

    async def run(self) -> None:
        """Connect, subscribe and pump messages — reconnects forever."""
        self._running = True
        _retry_count = 0
        _disconnect_ts: Optional[float] = None

        while self._running:
            try:
                async with websockets.connect(
                    Config.WS_URL,
                    ping_interval=20,
                    ping_timeout=30,
                ) as ws:
                    if _disconnect_ts is not None:
                        down_secs = int(time.time() - _disconnect_ts)
                        logger.info("✓ PolymarketMidCache | reconnected after %ds down", down_secs)
                    _retry_count = 0
                    _disconnect_ts = None

                    # Subscribe to everything we know about so far
                    all_tokens = self._subscribed | self._pending
                    if all_tokens:
                        await self._send_subscribe(ws, all_tokens)
                        self._subscribed = all_tokens
                        self._pending.clear()

                    logger.info(
                        "✓ PolymarketMidCache | connected to %s (%d tokens subscribed)",
                        Config.WS_URL, len(self._subscribed),
                    )

                    async for raw in ws:
                        # Flush any tokens that arrived while we were iterating
                        if self._pending:
                            await self._send_subscribe(ws, self._pending)
                            self._subscribed |= self._pending
                            self._pending.clear()

                        if not isinstance(raw, str):
                            continue
                        try:
                            self._handle_message(raw)
                        except Exception as exc:
                            logger.debug(
                                "PolymarketMidCache | parse error: %s | raw=%.120s", exc, raw
                            )

            except asyncio.CancelledError:
                logger.info("PolymarketMidCache | cancelled")
                break
            except (websockets.ConnectionClosed, ConnectionResetError) as exc:
                if _disconnect_ts is None:
                    _disconnect_ts = time.time()
                _retry_count += 1
                wait = min(3 * (2 ** (_retry_count - 1)), 60)
                down_secs = int(time.time() - _disconnect_ts)
                logger.warning(
                    "⚠️ PolymarketMidCache | disconnected (%ds), retry #%d in %ds: %s",
                    down_secs, _retry_count, wait, exc,
                )
                await asyncio.sleep(wait)
            except Exception as exc:
                if _disconnect_ts is None:
                    _disconnect_ts = time.time()
                _retry_count += 1
                wait = min(5 * (2 ** (_retry_count - 1)), 60)
                logger.exception(
                    "✗ PolymarketMidCache | error (retry #%d in %ds): %r", _retry_count, wait, exc
                )
                await asyncio.sleep(wait)

    # ------------------------------------------------------------------ #
    # Internal helpers                                                     #
    # ------------------------------------------------------------------ #

    async def _send_subscribe(self, ws, token_ids: Set[str]) -> None:
        """Send a subscribe message for the given token IDs."""
        if not token_ids:
            return
        # One subscription entry per token — same envelope as ChainlinkFeed.
        msg = json.dumps({
            "action": "subscribe",
            "subscriptions": [
                {"topic": "market", "type": "book", "filters": tid}
                for tid in token_ids
            ],
        })
        await ws.send(msg)
        logger.info(
            "✓ PolymarketMidCache | subscribed to %d token(s): %s…",
            len(token_ids),
            ", ".join(list(token_ids)[:2]),
        )

    def _handle_message(self, raw: str) -> None:
        """Parse a WebSocket message and update the price cache.

        Polymarket's ws-live-data server can push several event shapes.
        We handle the two most likely ones and ignore the rest silently.
        """
        data = json.loads(raw)

        # ── Shape 1: flat event  {"event_type": "book", "asset_id": ..., ...} ──
        event = data.get("event_type") or data.get("type", "")

        # ── Shape 2: wrapped   {"topic": "market", "payload": {...}} ──
        if not event and "payload" in data:
            payload = data["payload"]
            event = payload.get("event_type") or payload.get("type", "")
            data = payload  # unwrap so the rest of the parsing is uniform

        if event == "book":
            self._update_from_book(data)
        elif event in ("price_change", "last_trade_price", "midpoint"):
            self._update_from_price(data)
        # heartbeat / unknown topics → silently ignored

    def _update_from_book(self, data: dict) -> None:
        """Compute mid from best bid/ask in a book snapshot or delta."""
        token_id = data.get("asset_id") or data.get("token_id")
        if not token_id:
            return

        # "buys" / "bids" are bids (highest first); "sells" / "asks" are asks (lowest first)
        buys  = data.get("buys")  or data.get("bids")  or []
        sells = data.get("sells") or data.get("asks")  or []

        try:
            best_bid = max(float(b["price"]) for b in buys)  if buys  else None
            best_ask = min(float(s["price"]) for s in sells) if sells else None
        except (KeyError, TypeError, ValueError):
            return

        if best_bid is not None and best_ask is not None:
            mid = (best_bid + best_ask) / 2.0
        elif best_bid is not None:
            mid = best_bid
        elif best_ask is not None:
            mid = best_ask
        else:
            return

        self._set(token_id, mid)

    def _update_from_price(self, data: dict) -> None:
        """Update from a direct price event (price_change / last_trade_price)."""
        token_id  = data.get("asset_id") or data.get("token_id")
        price_str = data.get("price") or data.get("mid")
        if token_id and price_str:
            try:
                self._set(token_id, float(price_str))
            except (ValueError, TypeError):
                pass

    def _set(self, token_id: str, mid: float) -> None:
        """Store a mid-price if it is sane (strictly between 0 and 1)."""
        if 0.0 < mid < 1.0:
            self._prices[token_id] = mid
            self._ts[token_id] = time.time()


# Module-level singleton — import this everywhere instead of instantiating locally.
POLY_MID_CACHE = PolymarketMidCache()

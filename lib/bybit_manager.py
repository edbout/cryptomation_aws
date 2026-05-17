"""Bybit signal manager — cross-feed consensus voting.

This is the *decision* layer sitting on top of the four market-data feeds:
  - lib/bybit_feed.py     (BybitFeed)     — primary, also fires triggers
  - lib/binance_feed.py   (BinanceFeed)   — also fires triggers
  - lib/coinbase_feed.py  (CoinbaseFeed)  — passive, voting only
  - lib/chainlink_feed.py (ChainlinkFeed) — informational only

`get_signal(sym)` performs N-of-M alignment voting across the three voting
feeds (Bybit / Binance / Coinbase). A signal is "aligned" when at least
Config.ALIGNMENT_MIN_SOURCES feeds with |pct| > Config.ALIGNMENT_MIN_PCT
agree on direction. The aligned signal is then gated by OBI level/trend
checks delegated to BybitFeed (same logic the ticker handler uses).

Wiring
------
  mgr = BybitManager(bybit_feed=bybit_feed)
  mgr.attach_rdb(rdb)   # for alignment_pass / alignment_fail stats
"""

from __future__ import annotations

import logging
import time
from typing import Any, Optional, Tuple

from config import Config
from lib.helpers import normalize_asset

logger = logging.getLogger(__name__)


class BybitManager:
    """Slim signal-generation layer over BybitFeed.

    State is owned by feeds — this class just reads, votes, gates, and emits.
    """

    def __init__(self, *, bybit_feed: Any):
        self.bybit_feed = bybit_feed
        # Convenience aliases — read-only views onto the other feeds.
        self.chainlink_feed = bybit_feed.chainlink_feed
        self.coinbase_feed = bybit_feed.coinbase_feed
        self.binance_feed = bybit_feed.binance_feed

        # Injected late to avoid a circular import in main.py.
        self._rdb: Optional[Any] = None

    def attach_rdb(self, rdb: Any) -> None:
        """Inject the Redis cache used for per-asset alignment stats."""
        self._rdb = rdb

    def get_signal(self, sym: str) -> Optional[Tuple[str, str, float, float, dict]]:
        """Compute the consensus signal for `sym`.

        Returns (asset_key, side, bybit_5m_pct, open_price, consensus_dict) or None.

        Pipeline:
          1. Read TickData for sym (from BybitFeed).
          2. Compute each feed's 5m pct.
          3. N-of-M alignment vote (Bybit / Binance / Coinbase).
          4. Apply side threshold (relaxed during epoch_bias / btc_lag windows).
          5. OBI level + trend veto (delegated to BybitFeed).
          6. Return signal if all gates pass.
        """
        feed = self.bybit_feed
        tick = feed.data.get(sym)
        logger.debug(f"\U0001f4ca get_signal | {sym} tick data: {tick}")
        if not tick or tick.candle_5m_pct is None:
            return None

        bybit_5m_pct = tick.candle_5m_pct
        high_vol = tick.high_volume if Config.REQUIRE_VOL else True
        open_price = tick.last_price

        symbol_map = {
            "BTCUSD":   ("btc/usd",   "BTC-PERP"),
            "ETHUSD":   ("eth/usd",   "ETH-PERP"),
            "XRPUSD":   ("xrp/usd",   "XRP-PERP"),
            "SOLUSD":   ("sol/usd",   "SOL-PERP")
        }
        chainlink_sym, coinbase_sym = symbol_map.get(sym, (None, None))

        if sym not in symbol_map:
            return None

        chainlink_current = self.chainlink_feed.last_prices.get(chainlink_sym, 0.0)
        chainlink_base_5m = self.chainlink_feed.chainlink_5m_bases.get(chainlink_sym, 0.0)

        if chainlink_current > 0 and chainlink_base_5m > 0:
            chainlink_pct = 100.0 * (chainlink_current - chainlink_base_5m) / chainlink_base_5m
        else:
            chainlink_pct = 0.0

        now_ts_sig = time.time()
        chainlink_age = now_ts_sig - self.chainlink_feed.chainlink_last_update_ts.get(chainlink_sym, 0.0)

        coinbase_current = self.coinbase_feed.last_prices.get(coinbase_sym, 0.0)
        coinbase_base_5m = self.coinbase_feed.coinbase_5m_bases.get(coinbase_sym, 0.0)

        if coinbase_current > 0 and coinbase_base_5m > 0:
            coinbase_pct = 100.0 * (coinbase_current - coinbase_base_5m) / coinbase_base_5m
        else:
            coinbase_pct = 0.0

        binance_sym = normalize_asset(sym)
        binance_pct = 0.0
        if Config.BINANCE_ENABLED and self.binance_feed is not None:
            bn_current = self.binance_feed.last_prices.get(binance_sym, 0.0)
            bn_base_5m = self.binance_feed.binance_5m_bases.get(binance_sym, 0.0)
            if bn_current > 0 and bn_base_5m > 0:
                binance_pct = 100.0 * (bn_current - bn_base_5m) / bn_base_5m

        # ----------------------------------------------------------------
        # Alignment: N-of-M direction voting across
        #   {Bybit Futures, Binance Spot, Coinbase Futures}.
        # A source "votes" only when |pct| > ALIGNMENT_MIN_PCT.
        # Aligned when >= ALIGNMENT_MIN_SOURCES of the active votes agree on
        # direction. Chainlink stays informational only.
        # ----------------------------------------------------------------
        min_pct = Config.ALIGNMENT_MIN_PCT
        votes = []
        if Config.BYBIT_ENABLED and abs(bybit_5m_pct) > min_pct:
            votes.append(("bybit", bybit_5m_pct))
        if Config.BINANCE_ENABLED and abs(binance_pct) > min_pct:
            votes.append(("binance", binance_pct))
        if Config.COINBASE_ENABLED and abs(coinbase_pct) > min_pct:
            votes.append(("coinbase", coinbase_pct))

        ups = sum(1 for _, p in votes if p > 0)
        downs = sum(1 for _, p in votes if p < 0)
        direction_votes = max(ups, downs)
        agree_dir = "UP" if ups >= downs else "DOWN"
        aligned = direction_votes >= Config.ALIGNMENT_MIN_SOURCES

        n_sources = 3 if Config.BYBIT_ENABLED and Config.BINANCE_ENABLED and Config.COINBASE_ENABLED else 2

        if not aligned:
            logger.info(
                    f"\U0001f6ab get_signal | {sym:>8} | Bybit: {bybit_5m_pct:+.2f}% | "
                    f"Binance: {binance_pct:+.2f}% | Coinbase: {coinbase_pct:+.2f}% | "
                    f"Chainlink: {chainlink_pct:+.2f}% (age={chainlink_age:.0f}s, info) | "
                    f"votes={direction_votes}/{n_sources} need {Config.ALIGNMENT_MIN_SOURCES}"
                )
            if self._rdb is not None:
                self._rdb.hincrby(f"stats:trade:{normalize_asset(sym)}", "alignment_fail", 1)
            return None

        logger.info(
                f"\U0001f4ca get_signal | {sym:>8} | Bybit: {bybit_5m_pct:+.2f}% | "
                f"Binance: {binance_pct:+.2f}% | Coinbase: {coinbase_pct:+.2f}% | "
                f"Chainlink: {chainlink_pct:+.2f}% (age={chainlink_age:.0f}s, info) | "
                f"Aligned {direction_votes}/{n_sources} → {agree_dir}"
            )
        if self._rdb is not None:
            self._rdb.hincrby(f"stats:trade:{normalize_asset(sym)}", "alignment_pass", 1)

        btc_lag = (
            sym != "BTCUSD"
            and feed._btc_momentum_direction is not None
            and (time.time() - feed._btc_momentum_ts) < Config.BTC_LAG_TTL
            and ((bybit_5m_pct > 0) == (feed._btc_momentum_direction == "UP"))
        )
        in_epoch_bias = tick.candle_seconds <= Config.EPOCH_BIAS_SECS
        side_threshold = Config.REDUCED_THRESHOLD_PCT if (in_epoch_bias or btc_lag) else Config.BAR_OPEN_MIN_PCT
        side = 'SELL' if bybit_5m_pct < -side_threshold else 'BUY' if bybit_5m_pct > side_threshold else None
        obi = tick.order_book_imbalance

        gs_direction = "UP" if bybit_5m_pct > 0 else "DOWN"
        _obi_thresh = feed._effective_obi_thresh(sym, btc_lag, gs_direction)
        obi_trend = feed.obi_trackers[sym].trend() if sym in feed.obi_trackers else 0.0
        obi_contradicts = feed._obi_contradicts(obi, obi_trend, _obi_thresh, bybit_5m_pct)

        reason = "epoch_bias" if in_epoch_bias else ("btc_lag" if btc_lag else "normal")

        signal_ok = bool(side) and high_vol and not obi_contradicts

        log_prefix = "\U0001f4ca get_signal" if signal_ok else "\U0001f6ab get_signal"
        logger.info(
            f"{log_prefix} | {sym:>8} | chg={bybit_5m_pct:+.2f}% | OBI={obi:+.3f} | "
            f"trend={obi_trend:+.4f} thresh={_obi_thresh:.2f} "
            f"contradicts={obi_contradicts} | volume={high_vol} | [{reason}]"
        )

        # ── OBI shadow comparison: Bybit perps vs Binance perps ─────────────
        # Logging-only. Does NOT affect signal_ok or the trade decision.
        # Lets us assess (over time) whether Binance perp OBI would produce
        # different / better veto decisions than Bybit perp OBI. Threshold is
        # scaled because Binance's deeper book compresses normalized OBI
        # toward 0, so reusing OBI_THRESHOLDS as-is would under-veto Binance.
        binance_obi = None
        binance_obi_trend = None
        if Config.BINANCE_PERP_OBI_ENABLED and self.binance_feed is not None:
            binance_obi = self.binance_feed.get_perp_obi(sym)
            binance_obi_trend = self.binance_feed.get_perp_obi_trend(sym)

        if binance_obi is not None:
            binance_thresh = _obi_thresh * Config.BINANCE_OBI_SCALE
            # Trend can still be None (fewer than 3 samples); 0.0 is a safe
            # neutral that disables the "recovering" relaxation in
            # _obi_contradicts, matching how the live Bybit path treats a
            # cold tracker.
            binance_trend_used = binance_obi_trend if binance_obi_trend is not None else 0.0
            binance_would_contradict = feed._obi_contradicts(
                binance_obi, binance_trend_used, binance_thresh, bybit_5m_pct
            )
            sign_agree = (
                (obi > 0 and binance_obi > 0)
                or (obi < 0 and binance_obi < 0)
                or (obi == 0 and binance_obi == 0)
            )
            verdict_agree = (obi_contradicts == binance_would_contradict)
            logger.info(
                f"\U0001f50d OBI_shadow | {sym:>8} | "
                f"bybit  obi={obi:+.3f} trend={obi_trend:+.4f} thresh={_obi_thresh:.2f} contra={obi_contradicts} | "
                f"binance obi={binance_obi:+.3f} trend={binance_trend_used:+.4f} thresh={binance_thresh:.2f} contra={binance_would_contradict} | "
                f"sign_agree={sign_agree} verdict_agree={verdict_agree}"
            )
        elif Config.BINANCE_PERP_OBI_ENABLED:
            logger.debug(
                f"\U0001f50d OBI_shadow | {sym:>8} | binance perp OBI not yet available (warming up)"
            )

        if not signal_ok:
            return None

        bybit_dir = "UP" if bybit_5m_pct > 0 else "DOWN"
        cb_dir = "UP" if coinbase_pct > 0 else ("DOWN" if coinbase_pct < 0 else "")
        cl_dir = "UP" if chainlink_pct > 0 else ("DOWN" if chainlink_pct < 0 else "")
        binance_dir = "UP" if binance_pct > 0 else ("DOWN" if binance_pct < 0 else "")

        consensus = {
            "bybit_dir": bybit_dir,
            "binance_dir": binance_dir,
            "cb_dir": cb_dir,
            "cl_dir": cl_dir,
            "agree": f"{direction_votes}/{n_sources}",
        }

        return normalize_asset(sym), side, bybit_5m_pct, open_price, consensus

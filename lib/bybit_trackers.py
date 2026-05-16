"""Bybit market-data tracker utilities.

Pure helpers shared by BybitManager:

  - TickData          — per-symbol tick snapshot (last price, 5m pct, OBI, etc.)
  - OrderBookTracker  — rolling order-book imbalance with trend()
  - VolumeTracker     — stream-tick 1-minute volume aggregator with hi-vol gate

These have no dependencies on the rest of main.py (no rdb, no finder, no other
globals). Kept in their own module so BybitManager can be tested in isolation
and so the orchestration layer in main.py stays focused.
"""

from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class TickData:
    last_price: float
    candle_5m_pct: float
    high_volume: bool
    order_book_imbalance: float = 0.0
    candle_seconds: float = 0.0  # seconds elapsed since 5m epoch start


class OrderBookTracker:
    """Tracks rolling order book imbalance from Bybit top-of-book data.
    OBI = (bid_qty - ask_qty) / (bid_qty + ask_qty)
    Smoothed over last N poll samples (each ~5s) to reduce noise.
    Also exposes trend(): positive = OBI improving toward balance,
    negative = OBI worsening. Used by trend-aware suppression (Change 2).
    """
    def __init__(self, window: int = 6):  # 6 × 5s = 30s rolling window
        self.history: deque = deque(maxlen=window)

    def update(self, bid_qty: float, ask_qty: float) -> float:
        total = bid_qty + ask_qty
        if total == 0:
            return 0.0
        raw_obi = (bid_qty - ask_qty) / total
        self.history.append(raw_obi)
        return self.get()

    def get(self) -> float:
        if not self.history:
            return 0.0
        return sum(self.history) / len(self.history)

    def trend(self) -> float:
        """Linear regression slope of OBI samples over the rolling window.
        Positive  → OBI improving (moving toward 0 or positive).
        Negative  → OBI worsening (moving further negative).
        Returns 0.0 if fewer than 3 samples are available.
        """
        h = list(self.history)
        n = len(h)
        if n < 3:
            return 0.0
        mean_x = (n - 1) / 2.0
        mean_y = sum(h) / n
        num = sum((i - mean_x) * (h[i] - mean_y) for i in range(n))
        den = sum((i - mean_x) ** 2 for i in range(n))
        return num / den if den != 0 else 0.0


class VolumeTracker:
    def __init__(self, max_history: int = 20):
        self.volume_history = deque(maxlen=max_history)
        self.current_minute_start = None
        self._current_minute_volume = 0.0

    def update_stream_volume(self, symbol: str, volume_delta: float, timestamp: float):
        """Process stream tick. Aggregates into 1min buckets."""
        minute_start = int(timestamp // 60) * 60  # Floor to minute boundary
        if self.current_minute_start != minute_start:
            # Close previous minute, start new
            if self.current_minute_start is not None and self._current_minute_volume > 0:
                self.volume_history.append({
                    "symbol": symbol,
                    "minute_start": self.current_minute_start,
                    "volume": self._current_minute_volume
                })
            self.current_minute_start = minute_start
            self._current_minute_volume = 0.0

        self._current_minute_volume += volume_delta

    def _avg_volume_last_n(self, volumes: list, n: int = 10) -> Optional[float]:
        if len(volumes) < n:
            return None
        return sum(volumes[-n:]) / n

    def is_high_volume_minute(self, symbol: str, multiplier: float = 1.25, lookback: int = 10) -> bool:
        """Check if latest completed 1m candle is high volume vs previous lookback candles."""
        history = list(self.volume_history)
        if len(history) < lookback + 1:
            return False

        latest_volume = history[-1]["volume"]
        baseline = self._avg_volume_last_n([c["volume"] for c in history[:-1]], lookback)
        if baseline is None or baseline == 0:
            return False

        high_volume = latest_volume > (baseline * multiplier)
        logger.debug(
            "📊 is_high_volume_minute | %s 1M VOL: %.0f > %.0f*%.2f=%.0f? %s",
            symbol, latest_volume, baseline, multiplier, baseline * multiplier, high_volume
        )
        return high_volume

    def has_high_volume_prev_minute(self, symbol: str, multiplier: float = 1.25, lookback: int = 10) -> bool:
        """Check if previous completed 1m candle is high volume vs prior lookback candles."""
        history = list(self.volume_history)
        if len(history) < lookback + 2:
            return False

        prev_volume = history[-2]["volume"]
        baseline = self._avg_volume_last_n([c["volume"] for c in history[:-2]], lookback)
        if baseline is None or baseline == 0:
            return False

        high_volume = prev_volume > (baseline * multiplier)
        logger.debug(
            "📊 has_high_volume_prev_minute | %s PREV1M VOL: %.0f > %.0f*%.2f=%.0f? %s",
            symbol, prev_volume, baseline, multiplier, baseline * multiplier, high_volume
        )
        return high_volume

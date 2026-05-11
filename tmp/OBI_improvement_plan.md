# OBI Suppression Improvement Plan — XRP Persistent Negative Bias

**Date:** 2026-05-11  
**Observed issue:** 400 XRP signals suppressed in a single session (08:41–08:43 window was the most visible cluster). All 400 had OBI < −0.12 while price was trending bullish (+0.09–0.10%). Only 79 XRP signals passed through (OBI avg −0.117, just under the veto line).

---

## Root Cause

The Bybit `orderbook.5` data for XRP/USD is structurally **ask-heavy during upward price moves**. When buyers push the price up, they consume ask liquidity — but market makers immediately replenish asks at higher prices faster than they bid, leaving the book numerically offer-heavy even during a genuine bull move. This is normal XRP microstructure; it does not indicate a bearish signal.

The three compounding factors that make this worse:

1. **Threshold too tight (0.12).** XRP's veto threshold is the lowest in the config — more aggressive than BTC (0.20) and ETH (0.15). Small structural imbalances trigger a veto.

2. **30s simple rolling average is slow.** The 6-sample window (6 × 5s = 30s) creates a "sticky" OBI that lags real book changes. A momentary improvement in bids is diluted by the prior negative samples and doesn't clear the threshold in time to allow a signal through.

3. **No OBI trend awareness.** The veto fires on the level of OBI, not its direction. An OBI of −0.19 trending rapidly toward 0 is treated identically to an OBI of −0.19 trending toward −0.40. Only the latter is genuinely worrying.

---

## Improvement Plan

Three changes are proposed, in order of ascending complexity. Each is independent and can be deployed separately.

---

### Change 1 — Raise the XRP OBI threshold (Quick Win)

**File:** `config.py`  
**Risk:** Low | **Effort:** 5 minutes

The current XRP threshold of 0.12 is the tightest across all assets. Raising it to 0.18 brings it in line with ETH and reduces false vetoes from structural ask-side bias.

From the session log, 60 of the 400 suppressed events had OBI between −0.12 and −0.20. Raising the threshold to 0.20 would have passed roughly **15%** of suppressed signals immediately.

**Before:**
```python
OBI_THRESHOLDS: dict = {
    "BTCUSDT": float(os.getenv("OBI_THRESHOLD_BTC", "0.20")),
    "ETHUSDT": float(os.getenv("OBI_THRESHOLD_ETH", "0.15")),
    "XRPUSDT": float(os.getenv("OBI_THRESHOLD_XRP", "0.12")),   # ← too tight
    "SOLUSDT": float(os.getenv("OBI_THRESHOLD_SOL", "0.12")),
}
```

**After:**
```python
OBI_THRESHOLDS: dict = {
    "BTCUSDT": float(os.getenv("OBI_THRESHOLD_BTC", "0.20")),
    "ETHUSDT": float(os.getenv("OBI_THRESHOLD_ETH", "0.15")),
    "XRPUSDT": float(os.getenv("OBI_THRESHOLD_XRP", "0.18")),   # raised: XRP books structurally ask-heavy during rallies
    "SOLUSDT": float(os.getenv("OBI_THRESHOLD_SOL", "0.15")),   # raised modestly too
}
```

> **Note:** Keep `OBI_THRESHOLD_XRP` as an env var override so this can be tuned live without a redeploy.

---

### Change 2 — Add OBI Trend Awareness to OrderBookTracker (Better Signal Quality)

**File:** `main.py` — `OrderBookTracker` class and suppression checks  
**Risk:** Low–Medium | **Effort:** ~1 hour

Instead of vetoing on OBI level alone, also compute the OBI **trend** (slope over the window). A signal should only be suppressed if OBI is significantly negative **and getting worse**. If OBI is negative but improving toward 0, it likely means bids are recovering in response to the price move — this is a pass, not a veto.

**Updated `OrderBookTracker` class:**
```python
class OrderBookTracker:
    """Tracks rolling order book imbalance from Bybit top-of-book data.
    OBI = (bid_qty - ask_qty) / (bid_qty + ask_qty)
    Smoothed over last N poll samples (each ~5s) to reduce noise.
    Also exposes `trend()`: positive = OBI improving, negative = OBI worsening.
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
        """Returns the OBI slope over the window (positive = improving, negative = worsening).
        Computed as simple linear regression slope over sample indices.
        Returns 0.0 if fewer than 3 samples available.
        """
        h = list(self.history)
        n = len(h)
        if n < 3:
            return 0.0
        mean_x = (n - 1) / 2
        mean_y = sum(h) / n
        num = sum((i - mean_x) * (h[i] - mean_y) for i in range(n))
        den = sum((i - mean_x) ** 2 for i in range(n))
        return num / den if den > 0 else 0.0
```

**Updated suppression check in `_on_ticker` and `get_signal`:**

Replace the current static OBI veto block:

```python
# CURRENT
_obi_thresh = Config.OBI_THRESHOLDS.get(normalize_asset(sym), 0.15)
obi_contradicts = (
    (pct_change > 0 and obi < -_obi_thresh) or
    (pct_change < 0 and obi > _obi_thresh)
)
```

With a trend-aware version:

```python
# IMPROVED
_obi_thresh = Config.OBI_THRESHOLDS.get(normalize_asset(sym), 0.15)
obi_trend = self.obi_trackers[sym].trend()   # positive = OBI improving

# OBI contradicts if it is strongly against the signal direction...
_obi_level_contra = (
    (pct_change > 0 and obi < -_obi_thresh) or
    (pct_change < 0 and obi > _obi_thresh)
)
# ...but only veto if it is also NOT recovering (trend not helping)
# Threshold: ignore veto if OBI is improving at > 0.005/sample (meaningful recovery)
OBI_RECOVERY_RATE = 0.005
obi_recovering = (
    (pct_change > 0 and obi_trend > OBI_RECOVERY_RATE) or  # bullish & bids improving
    (pct_change < 0 and obi_trend < -OBI_RECOVERY_RATE)    # bearish & asks improving
)
obi_contradicts = _obi_level_contra and not obi_recovering
```

This means: a negative OBI reading doesn't kill the signal if the book is actively recovering toward balance. In the 08:41–08:43 window, several XRP OBI values were oscillating between −0.12 and −0.20, suggesting active recovery — those would now pass through.

The same change applies in both `_on_ticker` (line ~621) and `get_signal` (line ~871). For `get_signal`, use `BYBIT_MANAGER.obi_trackers[sym].trend()`.

---

### Change 3 — Relax OBI Veto During Confirmed Multi-Asset BTC Lag (Smarter Context)

**File:** `main.py` — `_on_ticker` and `get_signal` suppression logic  
**Risk:** Medium | **Effort:** 1–2 hours

The most active suppression period (08:41–08:43) was dominated by `btc_lag` triggers: BTC and ETH were both signalling UP, and XRP's price confirmed the direction (+0.09–0.10%). In this context, suppressing XRP due to its own book's microstructure is overly conservative — the asset's direction is confirmed externally.

When all of the following are true, apply a **reduced OBI threshold** (e.g., 50% relaxation):
- `btc_lag` is active for the signal
- BTC and ETH are both signalling in the same direction as XRP
- At least 2 of 3 price sources (Bybit, Coinbase, Chainlink) agree on direction

**Implementation — add a helper method to `BybitManager`:**

```python
def _multi_asset_aligned(self, sym: str, direction: str) -> bool:
    """Returns True if BTC and at least one other asset confirm `direction`.
    direction: 'UP' or 'DOWN'
    Used to relax OBI suppression during confirmed cross-asset momentum.
    """
    is_up = (direction == 'UP')
    aligned_count = 0
    for check_sym in ("BTCUSD", "ETHUSD"):
        if check_sym == sym:
            continue
        tick = self.data.get(check_sym)
        if tick and ((tick.candle_5m_pct > 0) == is_up):
            aligned_count += 1
    return aligned_count >= 1
```

**Updated suppression block in `_on_ticker`:**

```python
_obi_thresh = Config.OBI_THRESHOLDS.get(normalize_asset(sym), 0.15)

# Relax threshold by 40% when multi-asset BTC lag confirms direction
if btc_lag and self._multi_asset_aligned(sym, "UP" if pct_change > 0 else "DOWN"):
    _obi_thresh *= 1.4   # e.g. XRP: 0.18 → 0.25 during confirmed lag moves

obi_contradicts = (
    (pct_change > 0 and obi < -_obi_thresh) or
    (pct_change < 0 and obi > _obi_thresh)
)
```

This is conservative: it only relaxes when the broader market context independently confirms the direction. XRP's own book microstructure is only trusted when it's the only signal; when BTC and ETH agree, the XRP OBI is treated as noise.

---

## Recommended Deployment Order

| # | Change | When | Expected Impact |
|---|--------|------|-----------------|
| 1 | Raise XRP threshold to 0.18 | Immediately (1 env var change) | ~15% more XRP signals pass through |
| 2 | OBI trend awareness | Next deploy | Removes false vetoes when book is recovering |
| 3 | BTC-lag OBI relaxation | After 1 week of monitoring Change 2 | Captures confirmed cross-asset moves |

---

## Monitoring After Deployment

After deploying Change 1, add these metrics to the log analysis task:

- **XRP suppression rate**: suppressed / (suppressed + triggered). Current: 400/479 = 83%. Target after changes: < 50% during active market hours.
- **OBI trend at suppression time**: log `obi_trend` value alongside OBI in the Suppressed log line to validate Change 2's impact.
- **False negative rate**: track sessions where XRP had clear alignment (BTC + ETH both signalling same direction) but XRP was suppressed. Current session: 08:41–08:43 is a clear example.

---

## What NOT to Do

- **Don't disable OBI checking for XRP entirely.** The 38% of suppressed events with OBI < −0.20 are likely genuine vetoes (strongly bearish books during a price spike that may not sustain). The goal is to be more selective, not to remove the filter.
- **Don't shrink the rolling window below 4 samples.** A 2–3 sample window would introduce noise that causes the threshold to flip every few seconds, creating erratic suppression behavior.


#!/usr/bin/env python3
"""
calibrate_params.py
-------------------
Derives statistically optimal values for the three get_fairvalue_avg parameters:

    time_window   (±Xs)     — how wide a seconds-bucket to group records
    pct_tolerance (±Y%)     — how similar the pct_change must be to count as a comp
    min_matches   (N)       — minimum records needed to trust the average

TWO DATASETS are calibrated separately because they serve different purposes:

  prices:signals:{asset}   — validated signal moments with resolved outcomes.
                             Used by analyze_asset() → cache_trade_stats() for
                             win rate and optimal edge. Sparse (~10k records, biased
                             toward moments when signals fired).

  prices:fairvalue:{asset} — unconditional price-at-time every 30s. Used by
                             get_fairvalue_avg() to compute fair value reference.
                             Dense (~50k records, unbiased population sample).

Parameters should be calibrated against prices:fairvalue because that is what
get_fairvalue_avg() reads. The signal dataset is also shown for comparison.

Methodology
-----------
For each parameter we hold the other two fixed and sweep candidates. At each value:

  1. Coverage   — % of moments that returned a valid avg (>= min_matches)
  2. Avg error  — mean(|predicted_avg - actual_price|). Lower = better predictor.
  3. EV proxy   — win_rate of trades that cleared the edge threshold.

Optimal = maximises EV proxy, coverage >= 30%, avg_error <= 0.05.

Run from your project root (with Redis available):
    python3 calibrate_params.py

Outputs:
  - Console table per dataset, per asset, per minute-bucket
  - calibration_results.json  (paste _HIST_PARAMS values into price_tracker.py)
"""

import json
import sys
import os
from collections import defaultdict
from statistics import mean, stdev, median
from typing import List, Dict, Any, Optional, Tuple

# ---------------------------------------------------------------------------
# Redis connection — mirrors your existing RedisCache pattern
# ---------------------------------------------------------------------------
try:
    from config import RedisCache
    rdb = RedisCache()
except ImportError:
    import redis
    # Fallback: adjust host/port/db to match your environment
    _r = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"), port=6379, db=0)
    class RedisCache:
        def keys(self, pattern): return _r.keys(pattern)
        def zrevrange(self, k, s, e, withscores=False): return _r.zrevrange(k, s, e, withscores=withscores)
    rdb = RedisCache()

# ---------------------------------------------------------------------------
# Parameter sweep ranges
# ---------------------------------------------------------------------------
# fairvalue records are written only at fixed 30s marks (0,30,60,...,270).
# A trigger can fire at any second, so tw must be >= 15 to always reach the
# nearest mark from any position (worst case: midpoint between marks = 15s away).
SAMPLE_MARK_INTERVAL     = 30
MIN_TIME_WINDOW          = SAMPLE_MARK_INTERVAL // 2           # 15s floor
TIME_WINDOW_CANDIDATES   = [15, 20, 30, 45, 60]               # seconds; < 15 removed
PCT_TOL_CANDIDATES       = [0.005, 0.01, 0.025, 0.05, 0.075, 0.10, 0.15]
MIN_MATCHES_CANDIDATES   = [3, 5, 7, 10, 15, 20]

# Defaults held fixed while sweeping the other parameter
DEFAULT_TIME_WINDOW  = 15
DEFAULT_PCT_TOL      = 0.025
DEFAULT_MIN_MATCHES  = 10

# Evaluation thresholds
MIN_COVERAGE_FLOOR   = 0.30   # reject candidates that cover < 30% of moments
MAX_AVG_ERROR_CEIL   = 0.05   # reject candidates with avg |error| > 0.05 price units

# ---------------------------------------------------------------------------
# Data loading — two separate datasets
# ---------------------------------------------------------------------------

def load_signal_history(asset: str, max_count: int = 10000) -> List[Dict]:
    """Load from prices:signals:{asset} — validated signals with resolved outcomes.
    Used for win rate / EV calibration. Only returns completed records (outcome != 'na').
    """
    key = f"prices:signals:{asset}"
    try:
        members = rdb.zrevrange(key, 0, max_count - 1, withscores=True)
        history = []
        for mem_bytes, ts in members:
            mem_str = mem_bytes.decode() if isinstance(mem_bytes, bytes) else mem_bytes
            parts = mem_str.split(":")
            if len(parts) == 5 and parts[4] != "na":
                history.append({
                    "seconds":   int(parts[0]),
                    "price":     float(parts[1]),
                    "pct":       float(parts[2]),
                    "direction": parts[3],
                    "outcome":   parts[4],
                    "ts":        float(ts),
                    "minute":    int(parts[0]) // 60,
                })
        return history
    except Exception as e:
        print(f"  ERROR loading signals {asset}: {e}")
        return []


def load_fairvalue_history(asset: str, max_count: int = 50000) -> List[Dict]:
    """Load from prices:fairvalue:{asset} — unconditional 30s price samples.
    This is what get_fairvalue_avg() reads — calibrate params against this dataset.
    3-field format: seconds:price:pct. Also handles legacy 5-field format.
    """
    key = f"prices:fairvalue:{asset}"
    try:
        members = rdb.zrevrange(key, 0, max_count - 1, withscores=True)
        history = []
        for mem_bytes, ts in members:
            mem_str = mem_bytes.decode() if isinstance(mem_bytes, bytes) else mem_bytes
            parts = mem_str.split(":")
            if len(parts) == 3:
                history.append({
                    "seconds": int(parts[0]),
                    "price":   float(parts[1]),
                    "pct":     float(parts[2]),
                    "ts":      float(ts),
                    "minute":  int(parts[0]) // 60,
                    # No direction/outcome — fairvalue records never resolve
                })
            elif len(parts) == 5:
                # Legacy format written before Apr 2026 rename (neutral:na)
                history.append({
                    "seconds": int(parts[0]),
                    "price":   float(parts[1]),
                    "pct":     float(parts[2]),
                    "ts":      float(ts),
                    "minute":  int(parts[0]) // 60,
                })
        return history
    except Exception as e:
        print(f"  ERROR loading fairvalue {asset}: {e}")
        return []


def get_assets() -> List[str]:
    """Discover assets from prices:signals:* keys."""
    keys = rdb.keys("prices:signals:*")
    assets = set()
    for k in keys:
        k_str = k.decode() if isinstance(k, bytes) else k
        assets.add(k_str.split(":")[-1])
    return sorted(assets)


# ---------------------------------------------------------------------------
# Core evaluation helper
# ---------------------------------------------------------------------------

def evaluate_lookup(
    query_record: Dict,
    history: List[Dict],
    time_window: int,
    pct_tol: float,
    min_matches: int,
) -> Optional[Tuple[float, float, bool]]:
    """Simulate a get_fairvalue_avg call for one query record.

    Returns (predicted_avg, abs_error, is_win) if enough matches, else None.

    For prices:signals records (have direction/outcome):
      is_win = predicted_avg < actual_price AND outcome == direction (momentum held).
      This is a meaningful EV proxy — use it to pick params for signal calibration.

    For prices:fairvalue records (no direction/outcome):
      is_win = predicted_avg < actual_price (price-prediction accuracy proxy).
      EV is less meaningful here — prioritise avg_error and coverage instead.
    """
    s            = query_record["seconds"]
    pct          = query_record["pct"]
    actual_price = query_record["price"]
    actual_outcome = query_record.get("outcome")
    actual_dir     = query_record.get("direction")

    # Exclude the query record itself to avoid lookahead bias
    pool = [
        h for h in history
        if h is not query_record
        and (s - time_window) <= h["seconds"] <= (s + time_window)
        and abs(abs(h["pct"]) - abs(pct)) <= pct_tol
    ]

    if len(pool) < min_matches:
        return None

    predicted_avg = mean(h["price"] for h in pool)
    abs_error     = abs(predicted_avg - actual_price)

    if actual_outcome and actual_dir:
        # Signal record — full EV proxy (direction + outcome must agree)
        is_win = predicted_avg < actual_price and actual_dir == actual_outcome
    else:
        # Fairvalue record — price-accuracy proxy only
        is_win = predicted_avg < actual_price

    return predicted_avg, abs_error, is_win


# ---------------------------------------------------------------------------
# Sweep engine
# ---------------------------------------------------------------------------

def sweep_parameter(
    param_name: str,
    candidates: List,
    history: List[Dict],
    minute: Optional[int] = None,
) -> List[Dict]:
    """Sweep one parameter, hold others fixed. Returns sorted result rows."""

    subset = history if minute is None else [h for h in history if h["minute"] == minute]
    if len(subset) < 30:
        return []

    results = []
    for cand in candidates:
        if param_name == "time_window":
            tw, pt, mm = cand, DEFAULT_PCT_TOL, DEFAULT_MIN_MATCHES
        elif param_name == "pct_tol":
            tw, pt, mm = DEFAULT_TIME_WINDOW, cand, DEFAULT_MIN_MATCHES
        else:  # min_matches
            tw, pt, mm = DEFAULT_TIME_WINDOW, DEFAULT_PCT_TOL, cand

        valid, errors, wins = [], [], []
        for record in subset:
            res = evaluate_lookup(record, subset, tw, pt, mm)
            if res is not None:
                _, err, win = res
                valid.append(record)
                errors.append(err)
                wins.append(win)

        coverage  = len(valid) / len(subset)
        avg_error = mean(errors) if errors else 999.0
        ev        = (sum(wins) / len(wins) * 100) if wins else 0.0

        # Reject if below floor/ceiling
        viable = coverage >= MIN_COVERAGE_FLOOR and avg_error <= MAX_AVG_ERROR_CEIL

        results.append({
            "param":     param_name,
            "value":     cand,
            "coverage":  round(coverage, 3),
            "avg_error": round(avg_error, 5),
            "ev_pct":    round(ev, 2),
            "n_valid":   len(valid),
            "n_total":   len(subset),
            "viable":    viable,
        })

    return sorted(results, key=lambda r: (-r["ev_pct"], r["avg_error"]))


# ---------------------------------------------------------------------------
# Per-asset, per-minute analysis
# ---------------------------------------------------------------------------

def analyse_asset(asset: str, history: List[Dict]) -> Dict:
    """Run all three sweeps for each minute-bucket and return recommendations."""

    print(f"\n{'='*70}")
    print(f"  {asset.upper()}  ({len(history)} completed records)")
    print(f"{'='*70}")

    recommendations = {}

    # Global (all minutes combined) and per-minute
    buckets = {"all": None}
    for m in range(5):
        cnt = sum(1 for h in history if h["minute"] == m)
        if cnt >= 30:
            buckets[f"m{m}"] = m

    for label, minute in buckets.items():
        subset_label = f"minute {minute}" if minute is not None else "all minutes"
        subset = history if minute is None else [h for h in history if h["minute"] == minute]
        n = len(subset)

        print(f"\n  [{label}] {subset_label}  ({n} records)")

        rec = {"n": n, "minute": minute}

        for param_name, candidates in [
            ("time_window",  TIME_WINDOW_CANDIDATES),
            ("pct_tol",      PCT_TOL_CANDIDATES),
            ("min_matches",  MIN_MATCHES_CANDIDATES),
        ]:
            rows = sweep_parameter(param_name, candidates, history, minute)
            if not rows:
                print(f"    {param_name:<14}: insufficient data")
                continue

            # Best viable candidate; fall back to best overall if none viable
            viable = [r for r in rows if r["viable"]]
            best = viable[0] if viable else rows[0]

            # Enforce minimum time_window: production triggers fire at arbitrary
            # seconds, not just at sample marks — tw < MIN_TIME_WINDOW will find
            # zero records when the trigger fires mid-gap between marks.
            if param_name == "time_window" and best["value"] < MIN_TIME_WINDOW:
                print(
                    f"    ⚠ time_window {best['value']}s overridden → {MIN_TIME_WINDOW}s "
                    f"(sample marks every {SAMPLE_MARK_INTERVAL}s; tw must be >= {MIN_TIME_WINDOW}s)"
                )
                best = next((r for r in rows if r["value"] == MIN_TIME_WINDOW), best)
                best = dict(best)
                best["value"] = max(best["value"], MIN_TIME_WINDOW)

            print(
                f"    {param_name:<14}: "
                f"{'★ ' if viable else '  '}"
                f"{str(best['value']):<8}  "
                f"cov={best['coverage']:.0%}  "
                f"err={best['avg_error']:.4f}  "
                f"ev={best['ev_pct']:.1f}%  "
                f"({'viable' if viable else 'NO VIABLE — best effort'})"
            )

            # Show top 3 for context
            for row in rows[:3]:
                marker = "★" if row is best else " "
                print(
                    f"      {marker} {str(row['value']):<8}  "
                    f"cov={row['coverage']:.0%}  "
                    f"err={row['avg_error']:.4f}  "
                    f"ev={row['ev_pct']:.1f}%  "
                    f"n={row['n_valid']}/{row['n_total']}"
                )

            rec[param_name] = best["value"]
            rec[f"{param_name}_ev"]  = best["ev_pct"]
            rec[f"{param_name}_cov"] = best["coverage"]

        recommendations[label] = rec

    return recommendations


# ---------------------------------------------------------------------------
# Distribution diagnostics (shows you the shape of your data)
# ---------------------------------------------------------------------------

def print_distribution(asset: str, history: List[Dict]):
    if not history:
        return
    
    print(f"  Distribution diagnostics for {asset.upper()}")
    
    secs = [h['seconds'] for h in history]
    print(f"    seconds:  min={min(secs)}  max={max(secs)}  median={median(secs):.0f}  std={stdev(secs):.1f}")
    
    pcts = [abs(h['pct']) for h in history]
    print(f"    |pct|:    min={min(pcts):.4f}  max={max(pcts):.4f}  median={median(pcts):.4f}  std={stdev(pcts):.4f}")
    
    # Only show win rates for signals dataset (has direction/outcome)
    if any("direction" in h and "outcome" in h for h in history):
        for m in range(5):
            sub = [h for h in history if h['minute'] == m]
            if len(sub) >= 5:
                wins = sum(1 for h in sub if h.get("direction") == h.get("outcome"))
                print(f"    m{m}:  {len(sub):4} records  win={wins/len(sub)*100:.1f}%")
    
    # Sparse bucket analysis (both datasets)
    bucket_counts = defaultdict(int)
    for h in history:
        bucket = h['seconds'] // 15 * 15
        bucket_counts[bucket] += 1
    
    sparse = sum(1 for b, c in sorted(bucket_counts.items()) if c < 10)
    if sparse:
        print(f"    sparse: {sparse} 15s-buckets (< 10 records)")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Polymarket Bot — Parameter Calibration")
    print("Calibrates params for prices:signals AND prices:fairvalue separately.\n")
    print("Reading from Redis...\n")

    assets = get_assets()
    if not assets:
        print("ERROR: No price data found in Redis (prices:signals:* keys missing).")
        print("       Run the bot first to collect signal data.")
        sys.exit(1)

    print(f"Found {len(assets)} asset(s): {', '.join(assets)}")

    all_signal_results   = {}  # from prices:signals  — for win rate / EV reference
    all_fairvalue_results = {}  # from prices:fairvalue — primary: what get_fairvalue_avg reads

    for asset in assets:
        # ── Signals dataset ────────────────────────────────────────────────
        signal_history = load_signal_history(asset)
        if len(signal_history) < 30:
            print(f"\n  {asset} signals: only {len(signal_history)} completed records — skipping")
        else:
            print(f"\n{'─'*70}")
            print(f"  SIGNALS DATASET: {asset}  ({len(signal_history)} completed records)")
            print(f"  (use these for win rate / EV analysis only)")
            print_distribution(asset, signal_history)
            recs = analyse_asset(asset, signal_history)
            all_signal_results[asset] = recs

        # ── Fairvalue dataset ──────────────────────────────────────────────
        fv_history = load_fairvalue_history(asset)
        if len(fv_history) < 30:
            print(f"\n  {asset} fairvalue: only {len(fv_history)} records — skipping")
            print(f"  (bot needs ~2-3 candles of data before fairvalue is populated)")
        else:
            print(f"\n{'─'*70}")
            print(f"  FAIRVALUE DATASET: {asset}  ({len(fv_history)} records)")
            print(f"  ★ USE THESE PARAMS — this is what get_fairvalue_avg() reads")
            print_distribution(asset, fv_history)
            recs = analyse_asset(asset, fv_history)
            all_fairvalue_results[asset] = recs

    # ── Summary tables ──────────────────────────────────────────────────────
    for label, results, note in [
        ("FAIRVALUE (★ use these in _HIST_PARAMS)", all_fairvalue_results,
         "Paste into price_tracker.py _HIST_PARAMS"),
        ("SIGNALS (reference only — win rate / EV)", all_signal_results,
         "For comparison — not used in _HIST_PARAMS"),
    ]:
        if not results:
            continue
        print(f"\n\n{'='*70}")
        print(f"  RECOMMENDED VALUES — {label}")
        print(f"  {note}")
        print(f"{'='*70}")
        print(f"  {'Asset':<12} {'Bucket':<8} {'time_window':>12} {'pct_tol':>10} {'min_matches':>12} {'n_records':>10}")
        print(f"  {'-'*66}")

        for asset, recs in results.items():
            for bucket_label, rec in recs.items():
                tw = rec.get("time_window",  DEFAULT_TIME_WINDOW)
                pt = rec.get("pct_tol",      DEFAULT_PCT_TOL)
                mm = rec.get("min_matches",  DEFAULT_MIN_MATCHES)
                n  = rec.get("n",            0)
                print(f"  {asset:<12} {bucket_label:<8} {tw:>12} {pt:>10} {mm:>12} {n:>10}")

    # ── JSON output — fairvalue params (primary) ────────────────────────────
    config_out = {}
    for asset, recs in all_fairvalue_results.items():
        config_out[asset] = {}
        for bucket_label, rec in recs.items():
            config_out[asset][bucket_label] = {
                "time_window":   rec.get("time_window",  DEFAULT_TIME_WINDOW),
                "pct_tolerance": rec.get("pct_tol",      DEFAULT_PCT_TOL),
                "min_matches":   rec.get("min_matches",  DEFAULT_MIN_MATCHES),
                "n_records":     rec.get("n",            0),
                "dataset":       "fairvalue",
            }
    # Also include signal params for reference
    for asset, recs in all_signal_results.items():
        if asset not in config_out:
            config_out[asset] = {}
        for bucket_label, rec in recs.items():
            config_out[asset][f"signals_{bucket_label}"] = {
                "time_window":   rec.get("time_window",  DEFAULT_TIME_WINDOW),
                "pct_tolerance": rec.get("pct_tol",      DEFAULT_PCT_TOL),
                "min_matches":   rec.get("min_matches",  DEFAULT_MIN_MATCHES),
                "n_records":     rec.get("n",            0),
                "dataset":       "signals",
            }

    out_path = "calibration_results.json"
    with open(out_path, "w") as f:
        json.dump(config_out, f, indent=2)
    print(f"\n  Results saved → {out_path}")

    # ── Cross-asset consensus from fairvalue dataset ─────────────────────────
    all_tw, all_pt, all_mm = [], [], []
    for recs in all_fairvalue_results.values():
        for bucket_label, rec in recs.items():
            if bucket_label == "all":
                all_tw.append(rec.get("time_window", DEFAULT_TIME_WINDOW))
                all_pt.append(rec.get("pct_tol",     DEFAULT_PCT_TOL))
                all_mm.append(rec.get("min_matches",  DEFAULT_MIN_MATCHES))

    if all_tw:
        consensus_tw = sorted(all_tw)[len(all_tw) // 2]
        consensus_pt = sorted(all_pt)[len(all_pt) // 2]
        consensus_mm = sorted(all_mm)[len(all_mm) // 2]

        print(f"\n  CROSS-ASSET FAIRVALUE CONSENSUS (single fallback set):")
        print(f"    time_window   = {consensus_tw}s   (current default: {DEFAULT_TIME_WINDOW}s)")
        print(f"    pct_tolerance = {consensus_pt}   (current default: {DEFAULT_PCT_TOL})")
        print(f"    min_matches   = {consensus_mm}    (current default: {DEFAULT_MIN_MATCHES})")
        print()
        print("  Paste into price_tracker.py _HIST_PARAMS_DEFAULT:")
        print(f'    _HIST_PARAMS_DEFAULT = {{"time_window": {consensus_tw}, "pct_tol": {consensus_pt}, "min_matches": {consensus_mm}}}')
        print()
        print("  bar_open multipliers (applied automatically in _get_hist_params):")
        print(f"    time_window  → min({consensus_tw}*2, 60) = {min(consensus_tw*2, 60)}s")
        print(f"    pct_tol      → min({consensus_pt}*2, 0.20) = {min(consensus_pt*2, 0.20)}")
        print(f"    min_matches  → max({consensus_mm}//2, 3) = {max(consensus_mm//2, 3)}")

    print("\nDone.\n")


if __name__ == "__main__":
    main()

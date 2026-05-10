#!/usr/bin/env python3
"""
backtest_kelly_boost.py
-----------------------
Validates whether the Kelly boost multipliers (funding rate +15/40%, liquidation +30%)
actually improve expected value using resolved historical signals from Redis.

DATA SOURCE:  prices:signals:{asset}
              Members: {seconds}:{price}:{pct}:{direction}:{outcome}
              Outcome must be :up or :down (not :na) to be included.

METHODOLOGY:
  For each resolved signal, we know:
    - entry_price  (market price when signal fired)
    - direction    (BUY or SELL)
    - outcome      (up or down)
    - win          (direction == outcome)

  We simulate two Kelly stacks:
    A) No boost      — bet = bankroll * 0.25 * f*
    B) With boost    — bet = bankroll * 0.25 * f* * boost_factor

  boost_factor is approximated from stored signal data because historical
  funding rates / liquidation events aren't persisted per-signal. Instead,
  we compare outcomes in two groups:
    - Signals that fired on a HIGH-momentum bar (abs pct > 0.08%) → assume boost likely
    - Signals that fired on a LOW-momentum bar  (abs pct <= 0.08%) → assume no boost

  EV per trade = win_rate * (1 - price) - (1 - win_rate) * price

Run from project root (with Redis available):
    python3 backtest_kelly_boost.py [--asset BTCUSDT] [--min-signals 30]
"""

import json
import sys
import os
import argparse
from statistics import mean, stdev
from collections import defaultdict
from typing import List, Dict, Optional, Tuple

try:
    from config import RedisCache, Config
    rdb = RedisCache()
except ImportError:
    import redis
    rdb = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"), port=6379, db=0, decode_responses=True)

BANKROLL        = float(os.getenv("KELLY_BANKROLL", "50.0"))
KELLY_FRACTION  = float(os.getenv("KELLY_FRACTION", "0.25"))
KELLY_MIN_BET   = float(os.getenv("KELLY_MIN_BET",  "1.0"))
KELLY_MAX_BET   = float(os.getenv("KELLY_MAX_BET",  "10.0"))
BOOST_FACTOR    = 1.40   # max realistic boost (funding squeeze + liquidation)
HIGH_MOM_PCT    = 0.08   # abs pct threshold to classify "high momentum" (boost scenario)


def load_signals(asset: str) -> List[Dict]:
    """Load resolved signal records from Redis."""
    key = f"prices:signals:{asset.lower()}"
    raw = rdb.zrange(key, 0, -1)
    records = []
    for entry in raw:
        parts = entry.split(":")
        if len(parts) < 5:
            continue
        outcome = parts[4]
        if outcome not in ("up", "down"):
            continue  # skip unresolved
        try:
            records.append({
                "seconds":   int(parts[0]),
                "price":     float(parts[1]),
                "pct":       float(parts[2]),
                "direction": parts[3],
                "outcome":   outcome,
            })
        except (ValueError, IndexError):
            continue
    return records


def kelly_bet(price: float, win_rate: float, boost: float = 1.0) -> float:
    if price <= 0 or price >= 1:
        return 0.0
    p = win_rate
    q = 1.0 - p
    b = (1.0 - price) / price
    if b <= 0:
        return 0.0
    f_star = (b * p - q) / b
    if f_star <= 0:
        return 0.0
    bet = BANKROLL * KELLY_FRACTION * f_star * boost
    return max(KELLY_MIN_BET, min(bet, KELLY_MAX_BET))


def simulate(records: List[Dict], boost: float = 1.0) -> Dict:
    """Simulate Kelly betting over the signal history. Returns summary stats."""
    bankroll = BANKROLL
    trades, wins, total_bet, total_pnl = 0, 0, 0.0, 0.0
    pnl_series = []

    # Compute overall win rate first (for Kelly sizing)
    all_wins = sum(1 for r in records if r["direction"] == r["outcome"])
    win_rate = all_wins / len(records) if records else 0.5

    for r in records:
        price = r["price"]
        direction = r["direction"]
        outcome = r["outcome"]
        win = (direction == outcome)

        bet = kelly_bet(price, win_rate, boost)
        if bet <= 0:
            continue

        if win:
            profit = bet * (1.0 - price) / price
            pnl = profit
            wins += 1
        else:
            pnl = -bet

        bankroll += pnl
        total_bet += bet
        total_pnl += pnl
        pnl_series.append(pnl)
        trades += 1

    actual_win_rate = wins / trades if trades else 0
    avg_bet = total_bet / trades if trades else 0
    ev_per_trade = total_pnl / trades if trades else 0
    pnl_std = stdev(pnl_series) if len(pnl_series) > 1 else 0.0

    return {
        "trades":           trades,
        "wins":             wins,
        "win_rate":         actual_win_rate,
        "total_pnl":        total_pnl,
        "ev_per_trade":     ev_per_trade,
        "avg_bet":          avg_bet,
        "pnl_std":          pnl_std,
        "final_bankroll":   bankroll,
    }


def analyse_asset(asset: str, records: List[Dict]) -> None:
    total = len(records)
    if total < 10:
        print(f"  {asset}: only {total} resolved signals — need ≥10. Skipping.")
        return

    win_rate = sum(1 for r in records if r["direction"] == r["outcome"]) / total

    # Split into high-momentum (boost scenario) vs low-momentum (no-boost scenario)
    high_mom = [r for r in records if abs(r["pct"]) > HIGH_MOM_PCT]
    low_mom  = [r for r in records if abs(r["pct"]) <= HIGH_MOM_PCT]

    print(f"\n{'─'*60}")
    print(f"  Asset: {asset.upper()}   ({total} resolved signals)")
    print(f"  Overall win rate: {win_rate:.1%}")
    print(f"  High-momentum (>{HIGH_MOM_PCT:.2%}): {len(high_mom)} signals  ← boost scenario")
    print(f"  Low-momentum  (≤{HIGH_MOM_PCT:.2%}): {len(low_mom)} signals  ← no-boost scenario")

    print(f"\n  ── Full dataset simulation ──")
    res_no_boost = simulate(records, boost=1.0)
    res_boost    = simulate(records, boost=BOOST_FACTOR)
    _print_comparison(res_no_boost, res_boost, "all signals")

    if len(high_mom) >= 10:
        print(f"\n  ── High-momentum signals only (boost=1.0 vs {BOOST_FACTOR}x) ──")
        res_hm_no  = simulate(high_mom, boost=1.0)
        res_hm_yes = simulate(high_mom, boost=BOOST_FACTOR)
        _print_comparison(res_hm_no, res_hm_yes, "high-momentum")

    if len(low_mom) >= 10:
        print(f"\n  ── Low-momentum signals only (no boost expected) ──")
        res_lm = simulate(low_mom, boost=1.0)
        _print_sim(res_lm, "low-momentum (no boost)")

    # Recommendation
    delta_ev = res_boost["ev_per_trade"] - res_no_boost["ev_per_trade"]
    delta_pnl = res_boost["total_pnl"] - res_no_boost["total_pnl"]
    print(f"\n  VERDICT for {asset.upper()}:")
    if delta_ev > 0.005:
        print(f"  ✅ Boost IMPROVES EV by ${delta_ev:.4f}/trade (${delta_pnl:.2f} total) → keep boost")
    elif delta_ev < -0.005:
        print(f"  ❌ Boost HURTS EV by ${abs(delta_ev):.4f}/trade (${abs(delta_pnl):.2f} total) → reconsider boost")
    else:
        print(f"  ➖ Boost has negligible effect (ΔEV=${delta_ev:.4f}/trade) → inconclusive")


def _print_comparison(no_boost: Dict, with_boost: Dict, label: str) -> None:
    print(f"  {'':20} {'No Boost':>12}  {'With Boost':>12}  {'Delta':>10}")
    print(f"  {'':20} {'─'*12}  {'─'*12}  {'─'*10}")
    for field, fmt in [
        ("trades",       "{:>12d}"),
        ("win_rate",     "{:>12.1%}"),
        ("avg_bet",      "{:>12.2f}"),
        ("ev_per_trade", "{:>12.4f}"),
        ("total_pnl",    "{:>12.2f}"),
        ("pnl_std",      "{:>12.4f}"),
    ]:
        v1 = no_boost[field]
        v2 = with_boost[field]
        delta = v2 - v1 if isinstance(v1, float) else ""
        d_str = f"{delta:>+10.4f}" if isinstance(delta, float) else f"{'':>10}"
        print(f"  {field:<20} {fmt.format(v1)}  {fmt.format(v2)}  {d_str}")


def _print_sim(res: Dict, label: str) -> None:
    print(f"  {label}")
    for field, fmt in [
        ("trades",       "{:>12d}"),
        ("win_rate",     "{:>12.1%}"),
        ("ev_per_trade", "{:>12.4f}"),
        ("total_pnl",    "{:>12.2f}"),
    ]:
        print(f"    {field:<18} {fmt.format(res[field])}")


def main():
    parser = argparse.ArgumentParser(description="Kelly boost EV backtester")
    parser.add_argument("--asset", help="Single asset to analyse (e.g. BTCUSDT). Default: all.")
    parser.add_argument("--min-signals", type=int, default=30, help="Minimum resolved signals to include asset")
    args = parser.parse_args()

    if args.asset:
        assets = [args.asset.lower()]
    else:
        keys = rdb.keys("prices:signals:*")
        assets = [k.split("prices:signals:")[1] for k in keys]

    if not assets:
        print("No prices:signals:* keys found in Redis. Run the bot first to collect history.")
        sys.exit(1)

    print(f"\nKelly Boost EV Backtest")
    print(f"  Bankroll: ${BANKROLL}  |  Kelly fraction: {KELLY_FRACTION}  |  Min bet: ${KELLY_MIN_BET}  |  Max bet: ${KELLY_MAX_BET}")
    print(f"  Boost factor tested: {BOOST_FACTOR}x  |  High-momentum threshold: {HIGH_MOM_PCT:.2%}")
    print(f"  Assets: {assets}")

    for asset in sorted(assets):
        records = load_signals(asset)
        if len(records) < args.min_signals:
            print(f"\n  {asset}: {len(records)} resolved signals (< {args.min_signals} min) — skipping")
            continue
        analyse_asset(asset, records)

    print(f"\n{'='*60}")
    print("Done. Use results to adjust boost multipliers in main.py get_kelly_boost().")
    print("  File: main.py  Lines ~654-698 (BybitManager.get_kelly_boost)")
    print()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
backtest_kelly_boost.py
-----------------------
Validates whether the Kelly boost multipliers (funding rate +15/40%, liquidation +30%)
actually improve expected value using resolved historical signals from Redis.

DATA SOURCES:
  prices:signals:{ASSET}         — gated signals (post edge/OBI/volume/alignment).
                                    Members: {seconds}:{price}:{pct}:{direction}:{outcome}
  prices:signals_raw:{ASSET}     — pre-edge aligned signals.
                                    Members: {trigger_minute}:{seconds}:{price}:{pct}:
                                             {token}:{bybit_dir}:{cb_dir}:{cl_dir}:
                                             {agree}:{outcome}

  Outcome must be :up or :down (not :na) to be included.
  Asset keys are case-sensitive in Redis and stored UPPERCASE (e.g. BTCUSDT).

METHODOLOGY:
  For each resolved signal, we know:
    - entry_price  (market price when signal fired, 0-1 Polymarket contract price)
    - direction    (up/down for `signals`, UP/DOWN for `signals_raw`)
    - outcome      (up or down)
    - win          (direction == outcome, case-insensitive)

  We simulate two Kelly stacks:
    A) No boost      — bet = bankroll * 0.25 * f*           (applied to all trades)
    B) Segmented     — bet = bankroll * 0.25 * f* * boost   ONLY on high-momentum bars
                       (low-momentum trades stay at boost=1.0)

  boost_factor is approximated from stored signal data because historical
  funding rates / liquidation events aren't persisted per-signal. Instead,
  we assume:
    - Signals that fired on a HIGH-momentum bar (abs pct > HIGH_MOM_PCT) → boost was likely
    - Signals that fired on a LOW-momentum bar  (abs pct <= HIGH_MOM_PCT) → no boost

  EV per trade = win_rate * (1 - price) - (1 - win_rate) * price

Run from project root (with Redis available):
    python3 backtest_kelly_boost.py [--asset BTCUSDT] [--source signals|raw|both] [--min-signals 30]
"""

import sys
import os
import argparse
from statistics import stdev
from typing import List, Dict

try:
    from config import RedisCache
    rdb = RedisCache()
except ImportError:
    import redis
    rdb = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"), port=6379, db=0, decode_responses=True)

BANKROLL        = float(os.getenv("KELLY_BANKROLL", "50.0"))
KELLY_FRACTION  = float(os.getenv("KELLY_FRACTION", "0.25"))
KELLY_MIN_BET   = float(os.getenv("KELLY_MIN_BET",  "1.0"))
KELLY_MAX_BET   = float(os.getenv("KELLY_MAX_BET",  "10.0"))
BOOST_FACTOR    = 1.40   # max realistic boost (funding squeeze + liquidation)
HIGH_MOM_PCT    = 0.08   # abs pct threshold in PERCENT (0.08 = 0.08%); classifies "high momentum"


def load_signals(asset: str) -> List[Dict]:
    """Load resolved gated-signal records from prices:signals:{asset}.

    Member format: {seconds}:{price}:{pct}:{direction}:{outcome}
    direction/outcome are lowercase ('up'/'down').
    """
    key = f"prices:signals:{asset}"
    raw = rdb.zrange(key, 0, -1)
    records = []
    for entry in raw:
        parts = entry.split(":")
        if len(parts) < 5:
            continue
        outcome = parts[4].lower()
        if outcome not in ("up", "down"):
            continue  # skip unresolved
        try:
            records.append({
                "seconds":   int(parts[0]),
                "price":     float(parts[1]),
                "pct":       float(parts[2]),
                "direction": parts[3].lower(),
                "outcome":   outcome,
                "source":    "signals",
            })
        except (ValueError, IndexError):
            continue
    return records


def load_raw_signals(asset: str) -> List[Dict]:
    """Load resolved raw-signal records from prices:signals_raw:{asset}.

    Member format: {trigger_minute}:{seconds}:{price}:{pct}:{token}:
                   {bybit_dir}:{cb_dir}:{cl_dir}:{agree}:{outcome}
    bybit_dir is 'UP'/'DOWN' (uppercase), outcome is 'up'/'down' (lowercase).
    """
    key = f"prices:signals_raw:{asset}"
    raw = rdb.zrange(key, 0, -1)
    records = []
    for entry in raw:
        parts = entry.split(":")
        if len(parts) < 10:
            continue
        outcome = parts[9].lower()
        if outcome not in ("up", "down"):
            continue
        try:
            records.append({
                "seconds":   int(parts[1]),
                "price":     float(parts[2]),
                "pct":       float(parts[3]),
                "direction": parts[5].lower(),  # bybit_dir
                "outcome":   outcome,
                "source":    "raw",
            })
        except (ValueError, IndexError):
            continue
    return records


def load_all(asset: str, source: str) -> List[Dict]:
    if source == "signals":
        return load_signals(asset)
    if source == "raw":
        return load_raw_signals(asset)
    if source == "both":
        return load_signals(asset) + load_raw_signals(asset)
    raise ValueError(f"unknown source: {source}")


def kelly_bet(price: float, win_rate: float, boost: float = 1.0):
    """Return (bet, clamp) where clamp in {'none', 'min', 'max'}.
    bet = 0.0 if Kelly says don't bet.
    """
    if price <= 0 or price >= 1:
        return 0.0, "none"
    p = win_rate
    q = 1.0 - p
    b = (1.0 - price) / price
    if b <= 0:
        return 0.0, "none"
    f_star = (b * p - q) / b
    if f_star <= 0:
        return 0.0, "none"
    raw_bet = BANKROLL * KELLY_FRACTION * f_star * boost
    if raw_bet < KELLY_MIN_BET:
        return KELLY_MIN_BET, "min"
    if raw_bet > KELLY_MAX_BET:
        return KELLY_MAX_BET, "max"
    return raw_bet, "none"


def simulate(records: List[Dict], boost_fn) -> Dict:
    """Simulate Kelly betting over the signal history.

    boost_fn: callable(record) -> float, returns the boost multiplier per trade.
    """
    bankroll = BANKROLL
    trades, wins, total_bet, total_pnl = 0, 0, 0.0, 0.0
    clamp_min, clamp_max = 0, 0
    pnl_series = []

    # Compute overall win rate first (used for Kelly sizing across all trades)
    all_wins = sum(1 for r in records if r["direction"] == r["outcome"])
    win_rate = all_wins / len(records) if records else 0.5

    for r in records:
        price = r["price"]
        win = (r["direction"] == r["outcome"])

        boost = boost_fn(r)
        bet, clamp = kelly_bet(price, win_rate, boost)
        if bet <= 0:
            continue
        if clamp == "min":
            clamp_min += 1
        elif clamp == "max":
            clamp_max += 1

        if win:
            pnl = bet * (1.0 - price) / price
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
        "clamp_min":        clamp_min,
        "clamp_max":        clamp_max,
    }


def _const_boost(boost: float):
    return lambda r: boost


def _segmented_boost(boost: float, threshold: float):
    return lambda r: boost if abs(r["pct"]) > threshold else 1.0


def analyse_asset(asset: str, records: List[Dict]) -> None:
    total = len(records)
    if total < 10:
        print(f"  {asset}: only {total} resolved signals - need >=10. Skipping.")
        return

    win_rate = sum(1 for r in records if r["direction"] == r["outcome"]) / total

    high_mom = [r for r in records if abs(r["pct"]) > HIGH_MOM_PCT]
    low_mom  = [r for r in records if abs(r["pct"]) <= HIGH_MOM_PCT]
    hm_wr = (sum(1 for r in high_mom if r["direction"] == r["outcome"]) / len(high_mom)) if high_mom else 0.0
    lm_wr = (sum(1 for r in low_mom  if r["direction"] == r["outcome"]) / len(low_mom))  if low_mom  else 0.0

    print(f"\n{'-'*60}")
    print(f"  Asset: {asset.upper()}   ({total} resolved signals)")
    print(f"  Overall win rate: {win_rate:.1%}")
    print(f"  High-momentum (|pct|>{HIGH_MOM_PCT}%): {len(high_mom)} signals  win_rate={hm_wr:.1%}  <- boost scenario")
    print(f"  Low-momentum  (|pct|<={HIGH_MOM_PCT}%): {len(low_mom)} signals  win_rate={lm_wr:.1%}  <- no-boost scenario")

    print(f"\n  -- Segmented test (boost {BOOST_FACTOR}x ONLY on high-momentum bars) --")
    res_no_boost  = simulate(records, _const_boost(1.0))
    res_segmented = simulate(records, _segmented_boost(BOOST_FACTOR, HIGH_MOM_PCT))
    _print_comparison(res_no_boost, res_segmented, "no-boost", "segmented")

    print(f"\n  -- Naive test (boost {BOOST_FACTOR}x on EVERY trade - for reference) --")
    res_all_boost = simulate(records, _const_boost(BOOST_FACTOR))
    _print_comparison(res_no_boost, res_all_boost, "no-boost", "all-boost")

    if len(high_mom) >= 10:
        print(f"\n  -- High-momentum subset only (1.0x vs {BOOST_FACTOR}x) --")
        res_hm_no  = simulate(high_mom, _const_boost(1.0))
        res_hm_yes = simulate(high_mom, _const_boost(BOOST_FACTOR))
        _print_comparison(res_hm_no, res_hm_yes, "no-boost", "boost")

    if len(low_mom) >= 10:
        print(f"\n  -- Low-momentum subset only (no boost expected) --")
        res_lm = simulate(low_mom, _const_boost(1.0))
        _print_sim(res_lm, "low-momentum (no boost)")

    # Recommendation - the segmented test mirrors the live boost logic.
    delta_ev
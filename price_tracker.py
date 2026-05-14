#import time
import json
from typing import List, Dict, Any, Optional, Tuple
#from datetime import datetime, UTC
from statistics import mean, stdev
from collections import defaultdict
import logging
#from functools import lru_cache
from dataclasses import dataclass
from lib.helpers import normalize_asset, get_utc_now, get_seconds_since_5m_start
from config import RedisCache, Config
rdb = RedisCache()

# Production logging (no basicConfig - bot.py handles this)
logger = logging.getLogger(__name__)

@dataclass
class MinuteStats:
    """Rich minute outcome for trade decisions."""
    win_rate: float
    avg_price: float 
    avg_seconds: float
    count: int
    should_trade: bool
    price_std: float = 0.0      
    optimal_edge: float = 3.0

class PriceTracker:
    def __init__(self):
        self.rdb = RedisCache()
        self._stats_cache = {}  # In-memory cache for hot stats

    def minute_stats(self, asset: str, trigger_minute: int) -> Optional[MinuteStats]:
        """Get minute stats for trade validation - cached."""
        normalized_asset = normalize_asset(asset)
        stats_key = f"stats:summary:{normalized_asset}"
        
        # In-memory cache first (hot path)
        cache_key = f"{normalized_asset}:{trigger_minute}"
        if cache_key in self._stats_cache:
            return self._stats_cache[cache_key]
        
        stats_json = self.rdb.get(stats_key)
        if not stats_json:
            logger.debug(f"⏳ minute_stats | No cached stats for {normalized_asset}")
            return None
        
        try:
            outcomes = json.loads(stats_json)
            minute_data = outcomes.get(str(trigger_minute), {})
            
            if not minute_data:
                logger.debug(f"⏳ minute_stats | No data for {normalized_asset} M{trigger_minute}")
                return None

            # Safe float/int conversion with defaults
            win_rate = float(minute_data.get('win_rate', 0.0))
            avg_price = float(minute_data.get('avg_price', 0.0))
            avg_seconds = float(minute_data.get('avg_seconds', 0.0))
            count = int(minute_data.get('count', 0))
            price_std = float(minute_data.get('price_std', 0.0))
            optimal_edge = float(minute_data.get('optimal_edge', 3.0))

            should_trade = (
                count >= 5
                and win_rate >= Config.MIN_WIN_RATE_THRESHOLD
            )
            
            stats = MinuteStats(
                win_rate=round(win_rate, 3),
                avg_price=round(avg_price, 3),
                avg_seconds=round(avg_seconds, 0),
                count=count,
                should_trade=should_trade,
                price_std=round(price_std, 6),
                optimal_edge=round(optimal_edge, 2),
            )
            
            # Cache for instance lifetime (cleared on data update)
            self._stats_cache[cache_key] = stats
            return stats
            
        except (json.JSONDecodeError, ValueError, TypeError) as e:
            logger.warning(f"✗ minute_stats | Stats parse error {normalized_asset}: {e}")
            return None
         
    def record_signal(self, asset: str, price: float, percentage: float, trigger_minute: int) -> Dict[str, Any]:
        """Record a validated signal moment to prices:signals:{asset}.

        Called only when a signal passes OBI + volume + multi-source alignment.
        This is the filtered signal population used for win rate, optimal edge
        calibration, and outcome tracking via _update_outcomes().

        Separate from prices:fairvalue which is written unconditionally every 30s.
        """
        if not 0 <= trigger_minute <= 4:
            logger.error(f"✗ {asset} | record_signal | invalid trigger_minute: {trigger_minute}")
            return {}

        if price >= 1.0:
            logger.debug(f"✗ {asset} | record_signal | resolved market price {price:.4f} — skip")
            return {}
        
        if price < 0.01:
            logger.warning(f"✗ {asset} | record_signal | unexpectedly low price: {price}")
            return {}

        now = get_utc_now()
        candle_seconds = get_seconds_since_5m_start(now)
        timestamp = now.timestamp() 

        direction = 'up' if percentage >= 0 else 'down'
        data = {
            'asset': asset,
            'minute': trigger_minute,
            'seconds': candle_seconds,
            'price': round(price, 6),
            'pct': round(percentage, 4), 
            'direction': direction,
            'ts': timestamp
        }
        
        history_key = f"prices:signals:{asset}"

        pipe = self.rdb.pipeline()
        member = f"{candle_seconds}:{price:.6f}:{percentage:.4f}:{direction}:na"
        pipe.zadd(history_key, {member: timestamp})
        pipe.zremrangebyrank(history_key, 0, -10001)  
        pipe.expire(history_key, 86400 * 7)
        pipe.execute()

        # Clear in-memory cache for this asset
        for key in list(self._stats_cache.keys()):
            if normalize_asset(asset) in key:
                del self._stats_cache[key]
                
        return self.minute_stats(asset, trigger_minute)

    def record_signal_raw(
        self,
        asset: str,
        token: str,
        pm_price: float,
        bybit_pct: float,
        trigger_minute: int,
        candle_seconds: int,
        bybit_dir: str = '',
        cb_dir: str = '',
        cl_dir: str = '',
        agree: str = '',
    ) -> None:
        """Record pre-edge aligned signal to prices:signals_raw:{asset}.

        Written for every signal that passes OBI + fairvalue + alignment,
        before the edge threshold. Used for paper-trade backtesting and
        signal quality analysis independent of edge gating.
        """
        if pm_price <= 0 or pm_price >= 1.0:
            return

        now = get_utc_now()
        timestamp = now.timestamp()
        key = f"prices:signals_raw:{asset}"
        member = (
            f"{trigger_minute}:{candle_seconds}:{pm_price:.6f}:{bybit_pct:.4f}:{token}:"
            f"{bybit_dir}:{cb_dir}:{cl_dir}:{agree}:na"
        )

        pipe = self.rdb.pipeline()
        pipe.zadd(key, {member: timestamp})
        pipe.zremrangebyrank(key, 0, -10001)
        pipe.expire(key, 86400 * 7)
        pipe.execute()

    def get_assets(self, limit: int = 10) -> List[str]:
        assets = set()
        for k in self.rdb.keys("prices:signals:*"):
            try:
                key_str = k.decode() if isinstance(k, bytes) else k
                assets.add(key_str.split(':')[-1])
            except:
                continue
        return sorted(list(assets))[:limit]

    def get_signal_history(self, asset: str, max_count: int = 10000) -> List[Dict]:
        """Read completed signal records from prices:signals:{asset}.
        Returns ONLY resolved outcomes (outcome != 'na') — used for win rate,
        optimal edge backtest, and stats caching. Never used for fair value
        lookup (that uses get_fairvalue_history() from prices:fairvalue).
        """
        key = f"prices:signals:{asset}"
        try:
            members = self.rdb.zrevrange(key, 0, max_count-1, withscores=True)
            history = []
            for mem_bytes, ts in members:
                mem_str = mem_bytes.decode() if isinstance(mem_bytes, bytes) else mem_bytes
                parts = mem_str.split(':')
                if len(parts) == 5:
                    record = {
                        'seconds': int(parts[0]), 
                        'price': float(parts[1]), 
                        'pct': float(parts[2]), 
                        'direction': parts[3],
                        'outcome': parts[4],
                        'ts': float(ts),
                        'minute': int(parts[0]) // 60
                    }
                    # Filter: only completed outcomes
                    if record['outcome'] != 'na':
                        history.append(record)
            logger.debug(f"📈 get_history | {asset}: {len(history)} completed / {len(members)} total")
            return history
        except Exception as e:
            logger.error(f"❌ get_history | {asset}: {e}")
            return []
        
    # ---------------------------------------------------------------------------
    # Calibrated lookup table — generated by calibrate_params.py
    # Structure: asset_key -> minute (0-4) or "all" -> {time_window, pct_tol, min_matches}
    # "all" is the per-asset global fallback when a per-minute entry is missing.
    # Re-run calibrate_params.py periodically as your history grows.
    # ---------------------------------------------------------------------------
    _HIST_PARAMS: Dict[str, Dict] = {
        "btcusdt": {
            "all": {"time_window": 30, "pct_tol": 0.005, "min_matches": 20},
            0:     {"time_window": 30, "pct_tol": 0.005, "min_matches":  3},
            1:     {"time_window": 30, "pct_tol": 0.150, "min_matches": 20},
            2:     {"time_window": 15, "pct_tol": 0.150, "min_matches": 10},
            3:     {"time_window": 30, "pct_tol": 0.005, "min_matches": 20},
            4:     {"time_window": 30, "pct_tol": 0.050, "min_matches": 20},
        },
        "ethusdt": {
            "all": {"time_window": 15, "pct_tol": 0.005, "min_matches": 20},
            0:     {"time_window": 15, "pct_tol": 0.005, "min_matches": 20},
            1:     {"time_window": 30, "pct_tol": 0.005, "min_matches": 20},
            2:     {"time_window": 15, "pct_tol": 0.005, "min_matches": 20},
            3:     {"time_window": 30, "pct_tol": 0.005, "min_matches": 20},
            4:     {"time_window": 15, "pct_tol": 0.005, "min_matches": 20},
        },
        "solusdt": {
            "all": {"time_window": 15, "pct_tol": 0.010, "min_matches": 20},
            0:     {"time_window": 30, "pct_tol": 0.005, "min_matches":  5},
            1:     {"time_window": 30, "pct_tol": 0.150, "min_matches": 20},
            2:     {"time_window": 30, "pct_tol": 0.005, "min_matches": 20},
            3:     {"time_window": 15, "pct_tol": 0.010, "min_matches": 20},
            4:     {"time_window": 30, "pct_tol": 0.025, "min_matches":  3},
        },
        "xrpusdt": {
            "all": {"time_window": 15, "pct_tol": 0.005, "min_matches": 20},
            0:     {"time_window": 15, "pct_tol": 0.025, "min_matches": 10},
            1:     {"time_window": 15, "pct_tol": 0.005, "min_matches": 20},
            2:     {"time_window": 15, "pct_tol": 0.005, "min_matches": 20},
            3:     {"time_window": 30, "pct_tol": 0.005, "min_matches": 20},
            4:     {"time_window": 15, "pct_tol": 0.005, "min_matches": 20},
        },
    }
    _HIST_PARAMS_DEFAULT = Config.HIST_PARAMS_DEFAULT # {"time_window": 15, "pct_tol": 0.025, "min_matches": 10}

    def _get_hist_params(self, asset: str, minute: int, bar_open: bool) -> Dict:
        """Resolve calibrated params for this asset+minute, apply bar_open multipliers.

        Priority: Redis (auto-calibrated) > per-asset per-minute > per-asset global > hardcoded default.
        bar_open doubles time_window and pct_tol, halves min_matches (floor 3),
        applied ON TOP of calibrated values so relative tuning is preserved.
        """
        key = asset.lower().replace("-", "").replace("/", "")
        if not key.endswith("usdt"):
            key = key + "usdt"

        # Check Redis for auto-calibrated params (written by calibrate_params.py)
        try:
            redis_key = f"calibration:params:{key}:{minute}"
            redis_raw = rdb.get(redis_key)
            if redis_raw:
                p = dict(json.loads(redis_raw))
                if bar_open:
                    p = {
                        "time_window": min(p["time_window"] * 2, 60),
                        "pct_tol":     min(p["pct_tol"] * 2,    0.20),
                        "min_matches": max(p["min_matches"] // 2, 3),
                    }
                return p
        except Exception:
            pass

        asset_map = self._HIST_PARAMS.get(key)
        if asset_map is None:
            p = dict(self._HIST_PARAMS_DEFAULT)
        else:
            p = dict(asset_map.get(minute, asset_map.get("all", self._HIST_PARAMS_DEFAULT)))

        if bar_open:
            p = {
                "time_window": min(p["time_window"] * 2, 60),
                "pct_tol":     min(p["pct_tol"] * 2,    0.20),
                "min_matches": max(p["min_matches"] // 2, 3),
            }
        return p

    def get_fairvalue_avg(
        self,
        asset: str,
        seconds_into_candle: int,
        pct_change: float = 0.0,
        bar_open: bool = False,
    ):
        """Return fair value average price for a given candle position and momentum.

        Reads from prices:fairvalue — the unbiased continuous price series written
        unconditionally every 30s by record_fairvalue(). This is intentionally
        separate from prices:signals (validated signal moments with outcomes) to
        avoid survivorship bias in the fair value reference.

        Uses per-asset per-minute calibrated parameters from _HIST_PARAMS.
        bar_open=True applies looser multipliers on top of calibrated base values.
        """
        normalized_asset = normalize_asset(asset)
        # Use unbiased continuous series for fair-value reference (not signals history)
        history = self.get_fairvalue_history(normalized_asset)
        minute = seconds_into_candle // 60

        p = self._get_hist_params(normalized_asset, minute, bar_open)
        time_window = p["time_window"]
        pct_tol     = p["pct_tol"]
        min_matches = p["min_matches"]

        min_history = max(min_matches * 4, 30 if bar_open else 50)
        label = " [bar-open]" if bar_open else ""

        if len(history) < min_history:
            logger.info(
                f"⏳ get_fairvalue_avg | {normalized_asset} m{minute}: "
                f"{len(history)}/{min_history} history{label}"
            )
            return None

        matches = [
            h for h in history
            if (seconds_into_candle - time_window) <= h["seconds"] <= (seconds_into_candle + time_window)
            and abs(h["pct"] - pct_change) <= pct_tol
        ]

        if len(matches) < min_matches:
            logger.info(
                f"⏳ get_fairvalue_avg | {normalized_asset} m{minute}: "
                f"{seconds_into_candle:3d}s chg={pct_change:+.4f}% "
                f"| {len(matches)}/{min_matches} matches (tw={time_window}s pt={pct_tol}){label}"
            )
            return None

        avg_price = mean(h["price"] for h in matches)
        logger.debug(
            f"✓ get_fairvalue_avg | {normalized_asset} m{minute}: "
            f"{len(matches)}# tw={time_window}s pt={pct_tol} -> {avg_price:.4f}{label}"
        )
        return avg_price

    def record_fairvalue(
        self,
        asset: str,
        candle_seconds: int,
        poly_price: float,
        pct_change: float,
        timestamp: float,
    ) -> None:
        """Write unconditional Polymarket price sample to continuous history.

        Called on every Bybit poll tick at fixed 30-second marks within the candle.
        No momentum/OBI/volume filtering — this is the unbiased reference series
        used by get_fairvalue_avg to compute fair value.

        Member format: "{snapped_seconds}:{poly_price:.6f}:{pct_change:.4f}"
        pct_change is the signed live Bybit 5m candle pct at recording time (positive=up,
        negative=down) — used by get_fairvalue_avg to match records with the same
        directional momentum context, so up-move and down-move YES prices are not mixed.
        No direction/outcome fields: continuous records never resolve.

        Sample marks: {0,30,60,...,270} — 10 samples per 5-minute candle.
        Key:   prices:fairvalue:{asset}
        Cap:   50,000 records (~17 days at 10 samples/candle, 288 candles/day)
        Floor: records newer than 72h are never evicted by score-based cleanup
        """
        if poly_price >= 1.0 or poly_price <= 0.0:
            return  # market resolved or bad data — don't pollute fairvalue history

        SAMPLE_MARKS = {0, 30, 60, 90, 120, 150, 180, 210, 240, 270}
        closest = min(SAMPLE_MARKS, key=lambda m: abs(candle_seconds - m))
        if abs(candle_seconds - closest) > 7:
            return

        normalized = normalize_asset(asset)
        key = f"prices:fairvalue:{normalized}"
        member = f"{closest}:{poly_price:.6f}:{pct_change:.4f}"
        floor_ts = timestamp - (72 * 3600)

        try:
            with self.rdb.pipeline() as pipe:
                pipe.zadd(key, {member: timestamp})
                pipe.zremrangebyscore(key, "-inf", floor_ts)   # 72h floor first
                pipe.zremrangebyrank(key, 0, -50001)           # then 50k cap
                pipe.expire(key, 86400 * 14)                   # 14-day key TTL
                pipe.execute()
            logger.debug(
                f"📈 record_fairvalue | {normalized} s={closest} "
                f"price={poly_price:.4f} pct={pct_change:+.4f}%"
            )
        except Exception as e:
            logger.warning(f"⚠️ record_fairvalue | {normalized}: {e}")

    def get_fairvalue_history(self, asset: str, max_count: int = 50000) -> List[Dict]:
        """Read the unbiased fair value price series from prices:fairvalue:{asset}.
        Written unconditionally every 30s by record_fairvalue() — no signal filtering.
        Used exclusively by get_fairvalue_avg() to compute fair value reference price.
        Member format: "{seconds}:{price}:{pct}" — 3 fields, no direction/outcome.
        Also accepts legacy 5-field format (":neutral:na") written before Apr 2026.
        """
        key = f"prices:fairvalue:{normalize_asset(asset)}"
        try:
            members = self.rdb.zrevrange(key, 0, max_count - 1, withscores=True)
            history = []
            for mem_bytes, ts in members:
                mem_str = mem_bytes.decode() if isinstance(mem_bytes, bytes) else mem_bytes
                parts = mem_str.split(":")
                if len(parts) == 3:
                    # Current format: seconds:price:pct
                    history.append({
                        "seconds": int(parts[0]),
                        "price":   float(parts[1]),
                        "pct":     float(parts[2]),
                        "ts":      float(ts),
                        "minute":  int(parts[0]) // 60,
                    })
                elif len(parts) == 5:
                    # Legacy format: seconds:price:0.0000:neutral:na — migrate on read
                    history.append({
                        "seconds": int(parts[0]),
                        "price":   float(parts[1]),
                        "pct":     float(parts[2]),
                        "ts":      float(ts),
                        "minute":  int(parts[0]) // 60,
                    })
            logger.debug(
                f"📈 get_fairvalue_history | {normalize_asset(asset)}: {len(history)} records"
            )
            return history
        except Exception as e:
            logger.error(f"❌ get_fairvalue_history | {asset}: {e}")
            return []
    
    def analyze_asset(self, asset: str, history: Optional[List[Dict]] = None) -> Dict[str, Any]:
        """Compute win rates, avg price, volatility and per-minute stats from signal history. Expects raw Redis key suffix (e.g. BTCUSDT), no normalization applied."""
        if history is None:
            history = self.get_signal_history(asset)
        if not history:
            return {'asset': asset, 'entries': 0, 'valid': False}

        prices = [h['price'] for h in history]
        pcts   = [h['pct']   for h in history]

        successes = sum(
            1 for h in history
            if h.get('direction') == h.get('outcome') and h.get('direction')
        )
        win_rate  = (successes / len(history)) * 100
        avg_price = mean(prices)
        std_price = stdev(prices) if len(prices) > 1 else 0.0
        avg_pct   = mean(pcts)

        minute_stats = defaultdict(lambda: {'prices': [], 'pcts': [], 'wins': 0, 'count': 0})
        for h in history:
            minute = h['minute']
            minute_stats[minute]['prices'].append(h['price'])
            minute_stats[minute]['pcts'].append(h['pct'])
            minute_stats[minute]['count'] += 1
            if h.get('direction') == h.get('outcome') and h.get('direction'):
                minute_stats[minute]['wins'] += 1

        stats = {}
        for minute, data in minute_stats.items():
            if data['count'] > 0:
                stats[int(minute)] = {
                    'price_avg': mean(data['prices']),
                    'price_std': stdev(data['prices']) if data['count'] > 1 else 0.0,
                    'count':     data['count'],
                    'pct_avg':   mean(data['pcts']),
                    'win_rate':  (data['wins'] / data['count']) * 100,
                }

        latest = history[-1] if history else {}
        return {
            'asset':     asset,
            'entries':   len(history),
            'avg_price': avg_price,
            'price_std': std_price,
            'win_rate':  win_rate,
            'avg_pct':   avg_pct,
            'stats':     stats,
            'latest':    latest,
            'history':   history,
            'valid':     len(history) >= 10,
        }

    def _calc_optimal_edge(self, history: List[Dict], minute: int) -> float:
        """Backtest edge thresholds 2-20% against historical data to find the EV-maximising threshold.
        Returns the edge % that gave highest win rate with sufficient sample (>=10 trades).
        Falls back to 3.0 if insufficient data.
        """
        minute_history = [h for h in history if h.get('minute') == minute]
        if len(minute_history) < 10:
            return 3.0

        best_edge = 3.0
        best_ev = 0.0

        minute_avg = mean(h['price'] for h in minute_history)

        for edge_pct in [e * 0.5 for e in range(4, 41)]:  # 2.0% to 20.0% in 0.5% steps
            # Qualify trades where price was edge_pct% below the minute's average fair value
            qualifying = [
                h for h in minute_history
                if minute_avg > 0 and (minute_avg - h.get('price', minute_avg)) / minute_avg * 100 >= edge_pct
            ]
            if len(qualifying) < 10:
                continue
            wins = sum(1 for h in qualifying if h.get('direction') == h.get('outcome'))
            win_rate = wins / len(qualifying)
            # Simple EV: win_rate * (1 - avg_entry) - (1 - win_rate) * avg_entry
            avg_entry = mean(h['price'] for h in qualifying)
            ev = win_rate * (1 - avg_entry) - (1 - win_rate) * avg_entry
            if ev > best_ev:
                best_ev = ev
                best_edge = edge_pct

        return best_edge

    def cache_trade_stats(self, summary: Dict[str, Any]) -> None:
        """Cache optimized stats - called post-analysis. Now includes price_std and optimal_edge."""
        if not summary.get('valid', False):
            return
            
        asset = summary['asset']
        minute_seconds = defaultdict(list)
        minute_prices = defaultdict(list)
        
        for h in summary['history']:
            minute_seconds[h['minute']].append(h['seconds'])
            minute_prices[h['minute']].append(h['price'])

        outcomes = {}
        prev_avg_price = None
        for minute in sorted(summary['stats'].keys()):
            row = summary['stats'][minute]
            seconds_list = minute_seconds[minute]
            prices_list = minute_prices[minute]
            avg_seconds = mean(seconds_list) if seconds_list else 0.0
            avg_price = mean(prices_list) if prices_list else 0.0
            price_std = stdev(prices_list) if len(prices_list) > 1 else 0.0

            pct_chg = 0.0
            if prev_avg_price and prev_avg_price != 0:
                pct_chg = ((avg_price - prev_avg_price) / prev_avg_price) * 100
            prev_avg_price = avg_price

            # Backcalculate optimal edge for this minute
            optimal_edge = self._calc_optimal_edge(summary['history'], minute)

            outcomes[int(minute)] = {
                'win_rate': round(float(row['win_rate']), 2),
                'avg_price': float(avg_price),
                'avg_seconds': float(avg_seconds),
                'count': int(row['count']),
                'pct_chg': round(pct_chg, 2),
                'price_std': round(price_std, 6),    # NEW: per-minute volatility
                'optimal_edge': round(optimal_edge, 2),  # NEW: backtested edge threshold
            }

        stats_key = f"stats:summary:{asset}"
        self.rdb.set(stats_key, json.dumps(outcomes), ex=7200)  # 2h TTL — overlaps hourly refresh

        ranked = sorted(outcomes.items(), key=lambda x: x[1]['win_rate'], reverse=True)
        best_minute, best_data = ranked[0]
        optimal = (
            f"minute:{best_minute},seconds:{best_data['avg_seconds']:.0f},"
            f"win_rate:{best_data['win_rate']:.1f},pct_chg:{best_data['pct_chg']:.2f},"
            f"optimal_edge:{best_data['optimal_edge']:.1f}"
        )
        self.rdb.set(f"stats:optimal:{asset}", optimal, ex=86400)

    def seed_stats_from_raw(self, asset: str) -> bool:
        """Bootstrap stats:summary from prices:signals_raw when prices:signals is empty.

        Breaks the chicken-and-egg deadlock: prices:signals is only written after a
        trade passes all gates, but should_trade requires stats built from that same
        history. Raw signals carry the same directional data with resolved bar-close
        outcomes and are sufficient to seed the win-rate cache.

        Only activates when prices:signals has < 10 records so real trade data always
        takes precedence once it accumulates.
        """
        normalized = normalize_asset(asset) if not asset.endswith('USDT') else asset
        sig_key = f"prices:signals:{normalized}"
        if self.rdb.zcard(sig_key) >= 50:
            return False

        raw_key = f"prices:signals_raw:{normalized}"
        members = self.rdb.zrangebyscore(raw_key, '-inf', '+inf', withscores=True)
        if not members:
            logger.debug(f"⏳ seed_stats_from_raw | {normalized}: no raw signals found")
            return False

        history = []
        for mem_bytes, ts in members:
            mem_str = mem_bytes.decode() if isinstance(mem_bytes, bytes) else mem_bytes
            parts = mem_str.split(':')
            if len(parts) < 10:
                continue
            outcome = parts[9]
            if outcome == 'na':
                continue
            try:
                history.append({
                    'seconds':   int(parts[1]),
                    'price':     float(parts[2]),
                    'pct':       float(parts[3]),
                    'direction': 'up' if parts[5].upper() == 'UP' else 'down',
                    'outcome':   outcome,
                    'ts':        float(ts),
                    'minute':    int(parts[0]),
                })
            except (ValueError, IndexError):
                continue

        if len(history) < 10:
            logger.info(f"⏳ seed_stats_from_raw | {normalized}: only {len(history)} resolved raw signals — need 10")
            return False

        summary = self.analyze_asset(normalized, history=history)
        if summary.get('valid'):
            self.cache_trade_stats(summary)
            logger.info(
                f"✅ seed_stats_from_raw | {normalized}: seeded stats from {len(history)} raw signals "
                f"(win_rate={summary['win_rate']:.1f}%)"
            )
            return True
        return False

    def run(self, limit: int = 5, details: bool = False) -> List[Dict[str, Any]]:
        """Analyze all tracked assets, cache trade stats for those with >=10 signals, return summaries.
        Falls back to seeding from raw signals for assets that have not yet traded."""
        logger.debug("🔥 Price tracker | Update Cache | details={details}")

        # Always include known configured assets; supplement with any dynamic Redis keys
        assets = set(Config.ASSETS)
        for k in self.rdb.keys("prices:signals:*") + self.rdb.keys("prices:signals_raw:*"):
            key_str = k.decode() if isinstance(k, bytes) else k
            assets.add(key_str.split(':')[-1])
        assets = sorted(assets)[:limit]

        logger.debug(f"📊 run | Found {len(assets)} assets: {assets}")
        summaries = []

        for asset in assets:
            try:
                summary = self.analyze_asset(asset)
                logger.debug(f"✓ {asset}: {summary['entries']:,} entries")
                if details:
                    self.print_asset_detail(summary)

                if summary['entries'] >= 50:
                    self.cache_trade_stats(summary)
                    summaries.append(summary)
                else:
                    # Fewer than 50 real trades — seed from raw signals (more data, better per-minute stats)
                    self.seed_stats_from_raw(asset)
            except Exception as e:
                logger.error(f"✗ {asset}: {e}")
        
        if summaries and details:
            print("="*60)
            self.print_overview(summaries)
        
        return summaries
    
    def print_overview(self, summaries: List[Dict[str, Any]]) -> None:
        """Print assets ranked by avg price with entry count and win rate."""
        print("🏆 TOP ASSETS BY AVG PRICE")
        print("Asset    | Avg Price | Entries | Win%  | Latest")
        print("-" * 60)
        valid = [s for s in summaries if s.get('entries', 0) > 0]
        for s in sorted(valid, key=lambda x: x['avg_price'], reverse=True):
            latest_p = s['latest'].get('price', 0) if s['latest'] else 0
            print(f"{s['asset']:>8} | ${s['avg_price']:>8.2f} |  {s['entries']:>6,} | "
                  f"{s['win_rate']:>4.1f}% | ${latest_p:>7.2f}")

    def print_asset_detail(self, summary: Dict[str, Any]) -> None:
        """Print per-minute win rate breakdown with price/pct distribution."""
        if summary['entries'] == 0:
            print(f"⚠️ {summary['asset']}: No data")
            return

        print("=" * 60)
        print(f"📊 {summary['asset'].upper()} (direction/outcome wins)")

        seconds_list = [h['seconds'] for h in summary.get('history', []) if 'seconds' in h]
        if seconds_list:
            print(f"⏱️  Seconds: {min(seconds_list):3d}-{max(seconds_list):3d} (avg {mean(seconds_list):.0f})")

        print(f"💰 Average: ${summary['avg_price']:,.4f} ± ${summary['price_std']:,.4f}")
        print(f"🎯 Win Rate: {summary['win_rate']:.1f}% | {summary['entries']:,} #")

        latest_price = summary['latest'].get('price', 0) if summary.get('latest') else 0
        latest_pct = summary['latest'].get('pct', 0) if summary.get('latest') else 0
        print(f"📍 Latest: ${latest_price:>10,.4f} ({latest_pct:+6.2f}%)")

        print("\nMinute |   Bucket%  | Count |  Win%  |   Avg$  | Seconds")
        print("-" * 60)

        buckets = [
            ('-0.25%', float('-inf'), -0.20),
            ('-0.20%', -0.20, -0.15),
            ('-0.15%', -0.15, -0.10),
            ('-0.10%', -0.10, -0.05),
            ('-0.05%', -0.05, 0.00),
            ('0.05%', 0.00, 0.05),
            ('0.10%', 0.05, 0.10),
            ('0.15%', 0.10, 0.15),
            ('0.20%', 0.15, 0.20),
            ('0.25%', 0.20, float('inf')),
        ]

        groups = defaultdict(lambda: defaultdict(lambda: {'count': 0, 'wins': 0, 'prices': [], 'seconds': []}))

        for h in summary.get('history', []):
            pct = h.get('pct', 0)
            minute = int(h.get('minute', 0))
            price = h.get('price', 0)

            secs = h.get('seconds', 0)

            for bucket_name, low, high in buckets:
                if low <= pct < high:
                    is_win = h.get('direction') == h.get('outcome')
                    groups[minute][bucket_name]['count'] += 1
                    groups[minute][bucket_name]['wins'] += 1 if is_win else 0
                    groups[minute][bucket_name]['prices'].append(price)
                    groups[minute][bucket_name]['seconds'].append(secs)
                    break

        group_count = 0

        for minute in sorted(groups.keys()):
            for bucket_name, _, _ in buckets:
                if bucket_name not in groups[minute]:
                    continue

                data = groups[minute][bucket_name]
                if data['count'] == 0:
                    continue

                group_count += 1
                avg_price = mean(data['prices'])
                avg_secs = mean(data['seconds'])
                win_rate = (data['wins'] / data['count']) * 100

                print(
                    f"{minute:>6} | {bucket_name:>10} | {data['count']:>5} | "
                    f"{win_rate:>5.1f}% | ${avg_price:>6.2f} | {avg_secs:>6.0f}"
                )

        print("-" * 60)
        #print(f"📈 {group_count} minute/bucket groups")
        
# In main(), add:
def main():
    tracker = PriceTracker()
    tracker.run(limit=5, details=True)

if __name__ == "__main__":
    main()
# Polymarket Trading Bot

Event-driven momentum bot that trades Polymarket's 5-minute up/down binary markets for BTC, ETH, XRP and SOL. Live crypto market data from multiple exchanges is cross-validated for direction, gated by orderbook-imbalance checks, and turned into Kelly-sized orders on the corresponding YES or NO token. The bot is 100% reactive — the only timer is a per-minute redemption / pre-approval job.

## Functional design

**Inputs — four concurrent WebSocket feeds, each owning its state:**
- **Bybit** (inverse perps, `BTCUSD` etc.) — primary trigger source. Tick-driven; fires when its 5m bar pct clears a dynamic threshold.
- **Binance** (spot 1m klines + perpetuals partial-depth book) — secondary direction voter; primary source for the dual-source OBI confirmation.
- **Coinbase Futures** (`BTC-PERP-INTX` etc.) — passive direction voter.
- **Chainlink** — on-chain price oracle, informational only.

**Decision pipeline.** Every trigger funnels through `BybitManager.get_signal()` — the single decision point that gates every trade:
1. **Alignment vote** — at least 2 of {Bybit, Binance, Coinbase} must agree on direction with |pct| above threshold.
2. **Bybit perp OBI veto** — order-book imbalance level + trend must not contradict the price direction.
3. **Dual-source OBI confirmation** — Binance perp OBI must agree on sign and verdict; suppresses if Binance is stale or unavailable (env-tunable via `OBI_REQUIRE_BINANCE_AGREE`).
4. **Side selection** — YES token on UP, NO on DOWN. Skipped if Polymarket mid is near-resolved (≥ `NEAR_RESOLVED_THRESHOLD`).
5. **Kelly sizing** — position size from recent win rate × price edge, bounded by `KELLY_MIN_BET` / `KELLY_MAX_BET`.
6. **CLOB FAK order** — tiered price escalation with bid-ask spread and slippage caps.

**Position management.** Once filled, `OrderManager` polls every 5 seconds applying TP / SL / trailing-stop logic. SL scaling is driven by per-token volatility computed from the Polymarket mid-cache's rolling 60-second window.

**Resolution.** After the 5m market closes, a redeem job at `M:00:10` collects winnings.

## Repository layout

- **Trading bot** (Python) — `main.py`, `lib/`, `config.py`
- **Operator dashboard** (Ruby / Rack) — `dashboard.rb`, `config.ru`, `views/*.erb`. Reads trade history and per-asset stats from Redis.
- **Data contract** — Redis Cloud is the shared interface between the two: Python writes the keys, Ruby reads them. Treat them as separate codebases sharing one schema.

## Infrastructure

- **Redis Cloud** — state, signals, order tracking, stats, caching
- **Polygon RPC** — balance / allowance reads with 7-provider failover
- **AWS** — Amazon Linux deployment target (also runs locally)


# Release notes

15-05-26
Suppress repeated entries into near-resolved markets.
The bot continues to fire `execute_parallel_trades` even when the midpoint is at 0.99+, generating clusters of `near-resolved — skip` warnings and wasting evaluation cycles. A simple pre-check in `_on_ticker` or `get_signal` that gates execution when `mid > 0.95` (or a configurable threshold) would eliminate this noise and reduce unnecessary API calls during epoch transitions.

16-05-26
FAK order failures (21/50 = 42% fatal failure rate)**
The "no orders found to match with FAK order" error means the CLOB had no resting liquidity even at mid+1.5% on tier 3. 
Possible mitigations:
- Add a pre-trade liquidity check (minimum ask depth at the target price) before escalating past tier 2.
- Log the orderbook state at the time of each FAK failure to diagnose whether this is thin-market or timing-related.

17-05-26
- Add the perp OBI stream to BinanceFeed, now requires Binance & ByBit OBI alignment
- Asymmetric mid-price polling implementation
- Per-source accuracy summary on /results dashboard (Bybit / Binance / Coinbase / Chainlink — Overall, When voting, Coverage)

18-05-26
- Add post-suppression outcome tracking: `lib/suppression_store.py` records the first vetoed signal per (asset, epoch); `polymarket_order_outcome()` emits `🔍 suppressed_outcome | … | vetoed_dir=UP/DOWN | resolved=YES/NO | would_be=WIN/LOSS` so OBI veto effectiveness can be quantified from logs.

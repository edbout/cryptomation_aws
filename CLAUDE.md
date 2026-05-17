# CLAUDE.md

## Project Overview
This repository contains a Polymarket trading bot focused on momentum-based decision making using multiple data sources. The system ingests market data, sentiment/price feeds, orderbook signals, and execution state to generate trade decisions and manage risk.

## Repo Layout
`lib/` contains feed modules (`bybit_feed.py`, `binance_feed.py`, `coinbase_feed.py`, `chainlink_feed.py`), the orchestration layer `bybit_manager.py`, and pure helpers (`bybit_trackers.py`, `helpers.py`). `config.py` is at repo root and holds all env-driven knobs. `main.py` wires everything together at startup. Feeds own state; `BybitManager.get_signal()` is the single decision point that gates all trades.

The repo is polyglot: the trading bot itself is Python (`main.py`, `lib/`, `config.py`), but the reporting / dashboard layer is Ruby — `dashboard.rb` at the repo root (mounted via `config.ru` as a Rack app) reads trade history and per-asset stats from Redis and renders the operator view. Treat them as two separate codebases that share Redis as the data contract: Python writes the keys, Ruby reads them. Don't assume changes in one will be picked up by the other without an explicit schema update on both sides.

## Core Principles
- Prefer small, safe changes over broad refactors.
- Preserve existing behavior unless explicitly asked to change it.
- Keep trading logic deterministic and easy to audit.
- Treat execution, risk, and state management as production-critical.
- Never introduce hidden assumptions about market data or token formats.

## Trading Logic
- The bot uses momentum signals from multiple sources.
- Signal generation should be based on clearly defined rules.
- When sources disagree, prefer explicit conflict handling over silent averaging.
- Any new indicator or filter must explain how it affects entry, exit, or risk.
- Avoid overfitting to one datasource unless the user requests it.
- OBI (order book imbalance) = `(bid_qty - ask_qty) / (bid_qty + ask_qty)`, normalized to [-1, +1], smoothed over a rolling deque (window=6). Acts as a contradiction veto: a signal is suppressed when OBI level strongly opposes the 5m price direction AND OBI trend isn't recovering toward balance. New OBI sources must scale thresholds for book depth (deeper books compress OBI toward 0).

## Data Sources
- The bot may consume Polymarket market data, orderbooks, external price feeds, and related analytics.
- Always validate source freshness, timestamp alignment, and missing-data behavior.
- If a datasource is unavailable, fail gracefully and log the reason.
- Do not assume all feeds update at the same cadence.

## Code Style
- Keep functions focused and short.
- Prefer readable conditionals over clever one-liners.
- Use explicit variable names for trading state.
- Preserve existing logging style unless improving clarity.
- Add comments only where logic is non-obvious or risk-sensitive.

## Error Handling
- Never swallow exceptions silently.
- Log enough context to diagnose failures quickly.
- For retries, use bounded backoff and clear retry limits.
- Distinguish between transient network failures and logic/data errors.
- If a failure could affect live trading, surface it prominently.
- WebSocket listeners follow a consistent pattern: each gets its own asyncio task with an independent retry/backoff loop (exponential, capped at 60s), so a failure on one stream cannot disrupt others. New WS additions should match this pattern.

## Safety and Risk
- Treat order placement, cancellation, and position sizing as sensitive operations.
- Be cautious with any change that affects leverage, sizing, or execution timing.
- If a fix may change trade frequency or risk exposure, call that out explicitly.
- Prefer conservative defaults when behavior is unclear.

## Testing Expectations
- When modifying strategy logic, include a clear explanation of how it should be validated.
- For execution code, test edge cases such as empty data, stale data, partial fills, and API failures.
- If behavior depends on timing, mention the assumptions.
- Keep tests aligned with real exchange/API behavior, not idealized mocks only.

## Logging Expectations
- Logs should help answer:
  - What signal was seen?
  - Why was a trade taken or skipped?
  - What data sources agreed or disagreed?
  - What was sent to the exchange?
- Include symbols, side, timestamp, and key metric values where useful.
- Avoid noisy logs in tight loops unless they are rate-limited.
- Use the established emoji prefixes: 📊 for info / state updates, 🚫 for suppressed trades (always with reason), ⚠️ for transient warnings (reconnects, dropped messages), ✓ for successful startup / connection, ✗ for hard errors, 🔍 for shadow / comparison observability, 📥 for ingest events. The verb-prefix pair lets you grep logs by intent.

## Refactoring Rules
- Do not rename public methods or change signatures unless necessary.
- Preserve data contracts between modules.
- If a refactor touches strategy and execution together, separate the changes if possible.
- Keep backward compatibility with existing config and environment variables.
- `BybitManager.get_signal()` is the single decision point. New trade-gating logic (filters, vetoes, confirmations) goes here, not in the per-feed trigger handlers. Per-feed handlers should only do their own local pre-filtering and defer the final go/no-go to `get_signal`.

## Environment and Configuration
- Assume secrets come from environment variables or local config files.
- Never hardcode API keys, tokens, or private endpoints.
- If a config option is missing, prefer a clear validation error over silent fallback.
- Document any new required environment variables.

## Working with Claude
- Before making changes, identify the relevant file and the exact behavior to preserve.
- If a request is ambiguous, ask a focused clarification before changing strategy logic.
- When asked to improve code, prefer minimal diffs that solve the problem directly.
- When asked for a patch, provide only the relevant code unless the user requests broader context.
- When adding a new signal source, filter, or veto, ship it as logging-only first ("shadow mode"). After enough data has been logged to validate behavior, promote it to a live decision in a separate diff. Never combine "add the source" and "change the trade rule" in one change.

## Repo-Specific Notes
- This bot uses multiple momentum inputs; changes must consider source agreement and conflict handling.
- Execution correctness matters more than theoretical signal complexity.
- If a fix touches a live trading path, explain the operational impact briefly.
- Keep the bot easy to debug during market hours.
- Symbol formats vary across feeds. The canonical internal symbol is the Bybit inverse form (`BTCUSD`); other feeds are mapped to/from it:

  | Asset | Bybit (canonical) | Binance | Coinbase | Chainlink |
  |---|---|---|---|---|
  | BTC | `BTCUSD` | `BTCUSDT` | `BTC-PERP-INTX` | `btc/usd` |
  | ETH | `ETHUSD` | `ETHUSDT` | `ETH-PERP-INTX` | `eth/usd` |
  | XRP | `XRPUSD` | `XRPUSDT` | `XRP-PERP-INTX` | `xrp/usd` |
  | SOL | `SOLUSD` | `SOLUSDT` | `SOL-PERP-INTX` | `sol/usd` |

  When crossing feed boundaries, always map explicitly — never assume two feeds share a symbol string.
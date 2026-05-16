# CLAUDE.md

## Project Overview
This repository contains a Polymarket trading bot focused on momentum-based decision making using multiple data sources. The system ingests market data, sentiment/price feeds, orderbook signals, and execution state to generate trade decisions and manage risk.

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

## Refactoring Rules
- Do not rename public methods or change signatures unless necessary.
- Preserve data contracts between modules.
- If a refactor touches strategy and execution together, separate the changes if possible.
- Keep backward compatibility with existing config and environment variables.

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

## Repo-Specific Notes
- This bot uses multiple momentum inputs; changes must consider source agreement and conflict handling.
- Execution correctness matters more than theoretical signal complexity.
- If a fix touches a live trading path, explain the operational impact briefly.
- Keep the bot easy to debug during market hours.
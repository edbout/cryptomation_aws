---
name: log-file-analyses
description: Cryptomation log file analysis focused on P&L attribution, missed-money diagnosis, and actionable changes ranked by estimated dollar impact.
---

## Cryptomation Log File Analysis — Money-Making Lens

The job of this analysis is **not** to summarize what the bot did. It is to answer four questions:

1. **Did we make or lose money, and where did it come from?**
2. **What money did we leave on the table?** (suppressed signals that would have been winners, missed fills, late entries, premature exits)
3. **What cost us money we shouldn't have lost?** (bad fills, stale data acted on, veto failures, retries that delayed exits, leverage/sizing mistakes)
4. **What single change, shipped tomorrow, has the biggest expected $ impact?**

Everything else (errors, latency, reconnects) only matters if it ties back to one of those four. Mention infrastructure issues only when they cost or risked money.

### Step 1 — Mount the project folder

Use the `request_cowork_directory` tool to request access to the Cryptomation project folder at:
`C:\Users\EdwinBout\Documents\Prive\Cryptomation aws`

### Step 2 — Read the log and (if useful) cross-reference Redis

Read `log/bot.log` from the mounted folder. If the dashboard's Redis trade history is reachable via a Bash call or another connector, pull realized P&L per trade and per asset to ground the analysis in real numbers rather than log-inferred ones. If Redis is not reachable, infer P&L from log lines that include entry, exit, side, and size — and clearly mark figures as "log-inferred" in the report.

### Step 3 — Validate the latest release notes against log evidence

Before producing the money analysis, confirm that the most recent code changes actually took effect in the running bot. A money analysis comparing against the wrong baseline is misleading; if a release isn't deployed, the operator needs to know that first.

**Find the latest release block.** Open `README.md` and locate the `# Release notes` section near the end. Each block is headed by a date in `DD-MM-YY` format on its own line (e.g. `20-05-26`). Take the **most recent block** — the bottom-most dated block in the file. If the log's time range doesn't include the day after that block was written, also include the block before it, since changes may not have had time to manifest.

**Extract testable signatures from each bullet.** For each bullet in the chosen block, pull out anything that should be visible in the log when the code path fires:

- **Log-line prefixes / emojis** — the most reliable signal. The repo uses 📊 / 🚫 / ⚠️ / ✓ / ✗ / 🔍 / 📥 / 📐 / 🧪 / 🟢 / 🔴 / 🟠 with a verb (e.g. `📥 raw_signal_trade`, `🔍 raw_outcome_compare`, `📐 SL vol-adjust`, `🧪 raw_signal_trade ... DRY`). Bullets that introduce new code paths almost always describe their log prefix.
- **Function or module names** — e.g. `_update_raw_outcomes_pm`, `manage_positions`, `raw_signal_trader`. These often appear in the log alongside the prefix.
- **Env vars** — e.g. `RAW_OUTCOME_SOURCE`, `SL_BASE_MULTIPLIER_SOL`, `RAW_SIGNAL_TRADER_ENABLED`. Note these and check whether their effect is observable (e.g. `RAW_OUTCOME_SOURCE=polymarket` should produce `🔍 raw_outcome_compare` lines).
- **Redis keys** — e.g. `stats:raw_signal_trade:{asset}`, `raw_signal_trade:order:{order_id}`. If Redis is reachable, confirm the key exists or its counters are incrementing.
- **Behavior changes without a new prefix** — some bullets describe a numeric tweak (e.g. a multiplier change) with no new log line. For these, derive a proxy: e.g. for `SL_BASE_MULTIPLIER_SOL`, check the `📐 SL vol-adjust SOL` log line for the new `mult=` field with a non-1.0 value.

**Grep the log and classify each bullet:**

- **✓ confirmed** — at least one log line matches the expected signature. Capture an example line + timestamp + occurrence count.
- **⚠️ not seen** — no matching line. State the most likely reason: feature flag off (cite the env var and its default), no triggering signal in the window (e.g. raw-signal trader is per-signal — if there were no raw signals, no orders fire), feature is shadow-mode and emits only when a specific edge case occurs, or the log file rotated and the change happened before the current log started.
- **✗ unexpected / regression** — the OLD behavior is still present when the new should override (e.g. `RAW_OUTCOME_SOURCE=polymarket` is set but no `🔍 raw_outcome_compare` lines exist, AND old-style outcome writes are still visible). This is the most important class — surface prominently.

If the log window is short (< 1 bar for some signals, < 1 day for daily-bucketed features) or sparse, note that the "not seen" classification may be a sample-size issue, not a deployment problem.

### Step 4 — Produce the report

Structure the markdown report in this order. **Section 1 is the headline.** Everything else supports it.

#### 1. Bottom line — money summary

- Time range covered (UTC).
- Net realized P&L over the window, in USD and as % of deployed capital if inferable.
- Per-asset breakdown (BTC / ETH / XRP / SOL): trades, win rate, avg win, avg loss, net P&L, expectancy per trade.
- Per-source attribution: which feed (Bybit / Binance / Coinbase / Chainlink momentum) had the highest hit rate and the highest $ contribution. Call out any feed that is net-negative — that's a candidate to demote or shadow.
- Largest single winner and largest single loser, with timestamp, side, asset, and the log lines that led to entry. The point is: can we reproduce the win, and can we prevent the loss?

#### 2. Release validation

For each bullet in the most recent dated block of `README.md`, report status, evidence, and (if not confirmed) the most likely reason. Keep the bullet's original wording (or a tight paraphrase) so the operator can match against the source.

Table form:

| Status | Release-note bullet (short form) | Expected signature | Evidence / reason |
|---|---|---|---|
| ✓ | "Switch raw-signal outcome to Polymarket mid" | `🔍 raw_outcome_compare` | 84 occurrences, e.g. `2026-05-20 06:30:01 🔍 raw_outcome_compare \| SOL \| resolved=12 agree=10 disagree=2 ...` |
| ⚠️ | "Add raw-signal $-floor trader" | `📥 raw_signal_trade` | 0 occurrences. Likely cause: `RAW_SIGNAL_TRADER_ENABLED` default is false; check the env. |
| ✗ | "Per-asset SL multiplier" | `mult=1.50x` in `📐 SL vol-adjust SOL` | 12 `📐 SL vol-adjust SOL` lines but all show `mult=1.00x` — `SL_BASE_MULTIPLIER_SOL` env var not set. |

After the table, add a one-line **Deployment health** verdict:

- **Healthy** — all bullets ✓ (or ⚠️ with a benign explanation like "no triggering signal in window").
- **Partial** — some ✓, some ⚠️ that look like missing env config.
- **Broken** — any ✗, or ⚠️ on a feature that should have fired given the activity in the log window.

If status is **Broken**, this becomes the headline item in Section 5 (Ranked recommendations) — fixing a non-deployed change is almost always higher $-impact than tuning a deployed one.

#### 3. Money left on the table

- **Suppressed signals** (🚫 lines): tabulate by veto reason (OBI contradiction, conflict, freshness, etc.). For each veto reason, estimate what the trade *would have* made by looking at price action in the minutes after the suppression. Report: count, would-be win rate, would-be net P&L. If a veto is net-negative for P&L, flag it as a candidate to loosen.
- **OBI veto effectiveness specifically**: was the veto saving us from losses or costing us winners? Compute the would-be P&L of OBI-vetoed signals and compare against actually-taken signals of the same source/asset.
- **Late entries / late exits**: any log evidence of latency between signal and order (retry loops, reconnects, queueing) on trades that ended up profitable but would have been more profitable earlier — or losing trades that would have been smaller losses with faster exit.
- **Missed fills**: orders placed but not filled, or partially filled, where the move continued in our direction.

#### 4. Avoidable losses

- Trades taken on **stale data** — any case where a feed's last-update timestamp was older than its expected cadence at the moment of decision. Sum the $ lost on these.
- Trades where sources **disagreed** but the bot took the trade anyway (or vice versa) — was the dissenting source right? Quantify.
- **Execution cost**: slippage between intended and filled price, fees, and any evidence of adverse selection (fills that immediately moved against us).
- **Retry / reconnect cost**: WS reconnect storms (⚠️) that overlapped with open positions — did any cause delayed exits?
- **Risk events**: largest drawdown intra-window, time to recover, and whether sizing rules held.

#### 5. Ranked recommendations (ordered by estimated $ impact)

Each recommendation must include:

- **What to change** — file/function reference where possible (`BybitManager.get_signal()`, a specific feed module, a config knob).
- **Why** — the log evidence (line counts, example timestamps, $ amounts).
- **Estimated $ impact** — over the analyzed window, with the assumption stated. E.g. "Loosening OBI veto threshold from 0.6 to 0.5 would have unlocked 7 trades, est. +$420 over the window, assumes same execution quality."
- **Risk of the change** — what could go wrong, and a safer path (e.g. ship as shadow-mode first per CLAUDE.md conventions).
- **Confidence** — High / Medium / Low based on sample size.

Sort strictly by expected $ impact, descending. Do not pad with low-impact items; if there are only two real recommendations, give two.

#### 6. Infrastructure notes (brief)

Only include errors/warnings/latency that did **not** already get tied to money in sections 3 or 4. Keep this short — a few bullets, not a section.

### Step 5 — Save the report

Save the markdown report to the mounted folder at:
`C:\Users\EdwinBout\Documents\Prive\Cryptomation aws\log\bot_log_analysis_YYYY-MM-DD.md`

(use today's UTC date). Create the `log/` subfolder if it does not exist. After writing, return a `computer://` link to the file plus a 4-line summary: deployment health (Healthy / Partial / Broken — from Section 2), net P&L, the single highest-$-impact recommendation, and the biggest avoidable loss. Put deployment health first if it is **Broken** — that supersedes the money headline.

### Tone and discipline

- Numbers, not vibes. Every claim ("the OBI veto is too tight") needs a $ number attached or it doesn't go in.
- If the log doesn't contain enough info to answer a question, say so and propose the specific log line that should be added (per the CLAUDE.md logging conventions — 📊 / 🚫 / 🔍 prefixes).
- Don't recommend rewrites. Recommend the smallest diff that captures the $ on the table, consistent with the repo's "small, safe changes" principle and shadow-mode-first rule.
- Respect the data contract between Python and the Ruby dashboard: never recommend a Redis key change without flagging that both sides need updating.

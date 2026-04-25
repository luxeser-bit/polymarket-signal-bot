# Polymarket Signal Bot

This is a wallet-following research bot for Polymarket data. It collects public
leaderboard, trade, and activity data, scores wallets, produces copy-trading
signals, and simulates entries in a local paper portfolio.

Primary scale target: build toward 86,000,000 analyzed trades and 14,000 tracked
wallets. The current code is still a local research system, but ingestion is
structured around resumable wallet checkpoints.

The project learning strategy is documented in
[`docs/learning_model.md`](docs/learning_model.md). Future work should follow
that three-level model: rule auto-calibration, adaptive wallet scoring, and an
ML signal classifier once enough paper decisions exist.

It does not place real orders and does not store private keys. Add live trading
only after legal review, exchange access review, and a long paper-trading run.

## Quick start

```powershell
python -m polymarket_signal_bot demo
python -m polymarket_signal_bot report
python -m polymarket_signal_bot dashboard
```

The demo command creates `data/polysignal.db`, loads synthetic trades, scores
wallets, creates signals, and opens paper positions.

Open the dashboard at `http://127.0.0.1:8765`. It uses the same SQLite database
and shows the paper portfolio in a terminal-style control room.

## Live public-data run

Network access is required for these commands.

```powershell
python -m polymarket_signal_bot init-db
python -m polymarket_signal_bot discover --limit 50 --time-period WEEK
python -m polymarket_signal_bot sync-market-trades --market-limit 25 --trades-per-market 200
python -m polymarket_signal_bot sync --days 7 --per-wallet-limit 250
python -m polymarket_signal_bot discover-from-trades --limit 5000 --min-notional 100 --min-trades 2
python -m polymarket_signal_bot wallets-export --out data/watchlist.txt
python -m polymarket_signal_bot bulk-sync --wallet-limit 100 --max-pages-per-wallet 2
python -m polymarket_signal_bot sync-books --asset-limit 40 --lookback-minutes 1440
python -m polymarket_signal_bot history-backfill
python -m polymarket_signal_bot analytics-export --duckdb data/polysignal.duckdb
python -m polymarket_signal_bot analytics-report --duckdb data/polysignal.duckdb
python -m polymarket_signal_bot scan --bankroll 200 --min-wallet-score 0.55 --min-trade-usdc 50
python -m polymarket_signal_bot backtest --history-days 30 --bankroll 200 --compare-cohort-policy
python -m polymarket_signal_bot policy-optimizer --history-days 30 --bankroll 200
python -m polymarket_signal_bot cohort-report --history-days 30 --min-trades 2 --min-notional 100
python -m polymarket_signal_bot wallet-learning --since-days 90 --limit 20
python -m polymarket_signal_bot report
python -m polymarket_signal_bot paper-journal --since-days 30 --limit 20
python -m polymarket_signal_bot features-build
python -m polymarket_signal_bot reviews --status PENDING
```

Then start the local interface:

```powershell
python -m polymarket_signal_bot dashboard
```

## Monitor mode

The monitor keeps the local database and dashboard fresh:

```powershell
python -m polymarket_signal_bot monitor --interval-seconds 60 --leaderboard-limit 50 --wallet-limit 50
```

For a short smoke run:

```powershell
python -m polymarket_signal_bot monitor --iterations 1 --interval-seconds 5 --no-discover --no-sync
```

Telegram alerts are optional. If these variables are absent, alerts are recorded
as skipped and nothing is sent:

```powershell
$env:TELEGRAM_BOT_TOKEN="..."
$env:TELEGRAM_CHAT_ID="..."
python -m polymarket_signal_bot monitor
```

Use `--telegram-dry-run` to record what would be sent without sending messages.

## Bulk ingestion

Use bulk sync to grow toward the 86M/14K target in controlled chunks. It keeps a
checkpoint per wallet.

```powershell
python -m polymarket_signal_bot bulk-sync --wallet-limit 500 --page-size 500 --max-pages-per-wallet 3
python -m polymarket_signal_bot bulk-sync --wallet-limit 14000 --page-size 500 --max-pages-per-wallet 10 --analytics-export
```

Start small and increase limits after API behavior and local disk growth look
stable.

## Watchlist expansion

The bot can promote wallets found inside saved trade flow into the watchlist. This
keeps the 14K wallet target separate from per-wallet sync checkpoints.

```powershell
python -m polymarket_signal_bot sync-market-trades --market-limit 50 --trades-per-market 500 --min-trade-cash 25
python -m polymarket_signal_bot discover-from-trades --limit 5000 --min-notional 100 --min-trades 2
python -m polymarket_signal_bot wallets-export --out data/watchlist.txt
python -m polymarket_signal_bot wallets-import --path data/watchlist.txt --source file
```

`discover-from-trades` only adds new wallet candidates. Existing leaderboard
wallets keep their source, PnL, and volume metadata.

`sync-market-trades` is the broader discovery layer: it finds active high-volume
markets, pulls public trades by condition ID, stores those trades, and promotes
new wallets that clear the notional/trade-count filters.

## DuckDB analytics

DuckDB is optional and used for large local analytics once the SQLite store grows.
Install the optional package first:

```powershell
python -m pip install duckdb
```

Then create a snapshot and report:

```powershell
python -m polymarket_signal_bot analytics-export --duckdb data/polysignal.duckdb --chunk-size 50000
python -m polymarket_signal_bot analytics-report --duckdb data/polysignal.duckdb
```

The export creates aggregate views for wallet flow, market flow, category flow,
daily flow, wallet cohort stability, latest liquidity, wallet outcomes, and
decision features.

`bulk-sync` can refresh the snapshot automatically:

```powershell
python -m polymarket_signal_bot bulk-sync --wallet-limit 500 --max-pages-per-wallet 3 --analytics-export
```

`monitor` can refresh it every N loops:

```powershell
python -m polymarket_signal_bot monitor --analytics-export-every 10
```

Market-flow discovery can be enabled in monitor mode:

```powershell
python -m polymarket_signal_bot monitor --market-flow-every 5 --market-flow-market-limit 25
```

Or run the full public-data loop once:

```powershell
python -m polymarket_signal_bot run-once --leaderboard-limit 50 --days 7 --bankroll 200
```

## Main ideas

- Wallet discovery comes from the public Data API leaderboard.
- Trade history comes from the public Data API activity endpoint.
- Wallet quality is based on leaderboard PnL/volume, recent trade size,
  activity consistency, and market diversity.
- Signals are generated only for recent buy trades by scored wallets.
- Paper entries use conservative slippage, position caps, stop loss, take profit,
  and duplicate-position checks.
- Paper risk limits cap total exposure, market exposure, wallet exposure, new
  positions per scan, worst stop loss, and 24-hour realized loss.
- Paper exit engine closes positions on stop loss, take profit, max hold,
  stale prices, and gradual risk trimming when the portfolio is locked.
- Cohort policy changes signal size and review priority. `STABLE` wallets get
  more weight, `CANDIDATE` wallets are sized cautiously, and `WATCH`/`NOISE`
  signals require manual approval before paper opening.
- The policy optimizer compares baseline, strict cohort, balanced cohort,
  stable-only, and liquidity-watch modes on the same replay and saves the best
  paper-mode recommendation. `scan`, dashboard scan, and monitor use that saved
  recommendation unless cohort policy is explicitly disabled.
- The paper decision journal records created signals, opened paper positions,
  blocked entries, and closed positions with reason, policy, cohort, risk state,
  size, price, PnL, and hold time for future learning.
- Every order-book sync now appends a historical liquidity snapshot, so spread,
  depth, and liquidity can be analyzed over time instead of only as latest state.
- Outcome-aware wallet learning ranks wallet/category pairs by paper decisions,
  realized PnL, hit rate, blocked rate, and risk-exit rate.
- Signal generation uses outcome-aware wallet learning as a live adjustment:
  profitable wallet/category pairs get larger, higher-confidence paper signals,
  while weak or risky pairs are reduced or routed to manual review.

## Roadmap status

This is the working checklist for the project.

Done:

- MVP without real trades: public leaderboard/activity collection, local SQLite,
  wallet scoring, signal generation, paper positions, local dashboard.
- Signal engine v1: scored wallets, recent trade filters, minimum trade size,
  suggested entry price, position size, confidence, stop loss, take profit,
  duplicate-position protection.
- Paper trading v1: simulated entries, conservative slippage, open/closed paper
  positions, realized/unrealized PnL in the dashboard.
- Monitor v1: recurring scan loop, dashboard heartbeat, Telegram alert router
  with duplicate protection and dry-run mode.
- Market/order-book v1: public CLOB `/books` sync, best bid/ask, spread, depth,
  liquidity score, and liquidity-aware signal confidence.
- Backtest v1: historical trade replay, copy-delay simulation, slippage,
  stop/take/expiry exits, PnL, hit rate, max drawdown, max exposure, and
  liquidity-bucket breakdown.
- Wallet ranking v2: repeatability and drawdown components from historical
  buy/sell round trips.
- Category analytics v1: local market categorization and backtest breakdown by
  category and wallet.
- Noise filters v2: max wallet trade frequency, minimum cluster wallet count,
  minimum cluster notional, spread, depth, liquidity, and late-entry filters.
- Manual approval v1: pending/approved/rejected signal queue and a paper-only
  `open-approved` command.
- Bulk ingestion v1: checkpointed per-wallet sync for scaling toward 14K wallets
  and 86M analyzed trades.
- DuckDB analytics v1: optional SQLite-to-DuckDB snapshot, asset categories, and
  aggregate views for large local analysis.
- Watchlist expansion v1: promote new wallets from saved trade flow, import/export
  wallet lists, and show watchlist progress against the 14K target in the dashboard.
- Market-flow ingestion v1: active Gamma market discovery, Data API market-trade
  sync, minimum cash filter, and automatic wallet promotion from public flow.
- Cohort stability v1: wallet cohorts by status/source with stability scoring
  from active days, notional balance, market diversity, repeatability, drawdown,
  and trading discipline.
- Cohort policy v1: cohort-aware confidence/position sizing, manual-review
  priority, and auto-open gating for paper positions.
- Policy optimizer v1: historical comparison of several cohort-policy regimes
  with a saved recommended paper mode in runtime state, used by paper scans.
- Paper risk guard v1: total exposure, market/wallet exposure, open-position,
  per-scan, daily-loss, and worst-stop caps before opening paper positions.
- Paper exit engine v1: close reasons, max-hold exits, stale-price exits,
  and risk-trim exits that gradually unload an over-limit paper portfolio.
- Paper decision journal v1: `paper_events` logs signal/open/block/close
  decisions and is exported into DuckDB for the three-level learning loop.
- Order-book history v1: `order_books_history` stores historical spread, depth,
  and liquidity snapshots from each book sync and exports them into DuckDB.
- Wallet learning v1: `wallet-learning` ranks wallet/category outcomes from the
  paper decision journal for adaptive wallet scoring.
- Learning-adjusted signals v1: `scan`, dashboard scan, and monitor feed
  wallet/category paper outcomes back into signal confidence, position size, and
  auto-open gating.
- Decision feature table v1: `features-build` converts paper decisions into
  ML-ready rows with signal, cohort, liquidity, learning-adjustment, and outcome
  labels, then exports them into DuckDB.

Partial:

- Market/order-book data: live CLOB depth exists for recent assets; websocket
  streaming and longer depth history are still missing.
- Backtest: replay exists with baseline-vs-cohort-policy comparison and
  auto-ranking of cohort-policy regimes; deeper market-flow history is still
  needed before trusting deltas.
- Wallet ranking: PnL, volume, frequency, activity, diversity, trade size,
  repeatability, drawdown, liquidity-aware signals, cohort stability, and
  cohort-aware sizing/review priority are used now.

Next:

- Accumulate deeper multi-day market-flow history before trusting cohort-policy
  backtest deltas.
- Add an approval inbox action in the dashboard instead of CLI-only approval.
- Keep live execution blocked until manual approval and legal/access checks are
  explicitly handled.

## Safety defaults

- No CLOB order placement code is included.
- No wallet private key is required.
- The dashboard is local and paper-only.
- Position sizing defaults to a small fraction of bankroll.
- Existing open paper positions block duplicate entries for the same asset.
- The code is designed to be useful even if you later switch to a regulated venue
  or a manually approved execution workflow.

## Useful commands

```powershell
python -m polymarket_signal_bot --help
python -m polymarket_signal_bot discover --help
python -m polymarket_signal_bot sync-market-trades --help
python -m polymarket_signal_bot discover-from-trades --help
python -m polymarket_signal_bot wallets-export --help
python -m polymarket_signal_bot wallets-import --help
python -m polymarket_signal_bot sync --help
python -m polymarket_signal_bot bulk-sync --help
python -m polymarket_signal_bot sync-books --help
python -m polymarket_signal_bot analytics-export --help
python -m polymarket_signal_bot analytics-report --help
python -m polymarket_signal_bot scan --help
python -m polymarket_signal_bot backtest --help
python -m polymarket_signal_bot policy-optimizer --help
python -m polymarket_signal_bot cohort-report --help
python -m polymarket_signal_bot reviews --help
python -m polymarket_signal_bot review --help
python -m polymarket_signal_bot open-approved --help
python -m polymarket_signal_bot report --help
```

## Legal and risk note

Prediction markets are risky and may be unavailable or restricted in some
jurisdictions. Do not use VPNs or similar tools to bypass geographic or account
restrictions. This project is for research and paper trading.

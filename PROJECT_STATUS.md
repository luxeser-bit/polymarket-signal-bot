# PROJECT STATUS

Snapshot: 2026-04-28 19:20:30 +09:00  
Workspace: `C:\Users\User\Documents\New project`  
Git HEAD: `2097f13`  
Python package: `polymarket-signal-bot` `0.1.0`  
React dashboard package: `polymarket-signal-dashboard` `0.1.0`

## Current Goal

Build an autonomous Polymarket signal system that indexes full Polygon / Polymarket history, targets `86,000,000` raw trade/event rows, tracks `14,000` wallets, scores wallets, assigns cohorts, trains an exit model, and runs paper trading before any live trading.

Current scale snapshot:

- Raw indexed events: `664,511 / 86,000,000` (`~0.77%`)
- `OrderFilled` rows usable for wallet scoring: `2,380`
- Distinct wallets from `OrderFilled`: `325 / 14,000` (`~2.32%`)
- Scored wallets: `325`
- Wallet cohorts: `STABLE=3`, `CANDIDATE=6`, `WATCH=75`, `NOISE=241`
- Last indexed block in `data/indexer.db`: `52,656,600`
- Last full training run: `2026-04-28 17:57:27 +09:00`
- Exit examples generated: `394` total, `10` saved to `data/exit_examples.json`

## Key Files

| Path | Lines | Main function |
| --- | ---: | --- |
| `polymarket_signal_bot/indexer.py` | 1,436 | Main Polygon blockchain indexer. Reads Polymarket contract logs, normalizes events into `raw_transactions`, resumes from `indexer_state`, supports `--sync`, `--test`, chunk sizing and RPC rate limits. |
| `src/indexer.py` | 6 | Compatibility wrapper that launches `polymarket_signal_bot.indexer`. |
| `server/api.py` | 996 | FastAPI backend. Starts/stops system subprocesses, exposes system status, metrics, wallets, positions, paper controls, and `/ws/live`. |
| `polymarket_signal_bot/live_paper_runner.py` | 1,299 | Async real-time paper runner. Opens/closes paper positions, persists `paper_state.db`, supports dry-run, polling/WebSocket listener architecture, and logs portfolio metrics. |
| `polymarket_signal_bot/auto_trainer.py` | 435 | Automatic training loop. Runs wallet scoring, updates cohorts, trains/saves exit model, writes exit stats/examples, logs each stage. |
| `src/auto_trainer.py` | 6 | Compatibility wrapper for `polymarket_signal_bot.auto_trainer`. |
| `polymarket_signal_bot/scoring.py` | 586 | SQL-based wallet scoring from `raw_transactions`, currently filtered to `event_type = 'OrderFilled'`; saves `scored_wallets`. |
| `polymarket_signal_bot/cohorts.py` | 419 | Assigns wallets to `STABLE`, `CANDIDATE`, `WATCH`, `NOISE` and writes `wallet_cohorts`. |
| `polymarket_signal_bot/monitor.py` | 493 | Signal monitor. Polls/scans markets, can trigger retraining, passes signals to live/paper flows. |
| `polymarket_signal_bot/signals.py` | 382 | Signal generation and filtering logic from wallet/cohort activity. |
| `polymarket_signal_bot/paper.py` | 449 | Core paper trading engine: virtual balance, positions, fees, slippage, PnL. |
| `dashboard.py` | 1,513 | Disabled legacy Streamlit entry point. Kept for compatibility helpers/tests; running it shows a React migration notice instead of the old dashboard. |
| `crypto-dashboard/src/App.jsx` | 133 | React root. Loads API/WebSocket state and composes the dashboard. |
| `crypto-dashboard/src/components/DashboardLayout.jsx` | 102 | Main terminal-style layout, runway bar, top metrics, grid sections. Runway day is tied to system uptime in 24-hour cycles. |
| `crypto-dashboard/src/components/SystemControl.jsx` | 69 | Start/Stop All control for backend components. |
| `crypto-dashboard/src/components/IndexerMetrics.jsx` | 46 | Raw event count, last block, speed, progress toward 86M. |
| `crypto-dashboard/src/components/WalletCohorts.jsx` | 152 | SCORED/STABLE/CANDIDATE/WATCH/NOISE display, Train Model button, 14k wallet target progress, exit examples summary. |
| `crypto-dashboard/src/components/PositionsTable.jsx` | 84 | Open paper positions table and PnL display. |
| `crypto-dashboard/src/components/EquityChart.jsx` | 71 | Equity/balance chart area. |
| `crypto-dashboard/src/components/PaperTradingControl.jsx` | 67 | Start/Stop Paper Trading and dry-run toggle. |
| `crypto-dashboard/src/hooks/useWebSocket.js` | 65 | Live WebSocket connection with reconnect logic. |
| `crypto-dashboard/src/hooks/useApi.js` | 73 | REST API helpers for dashboard actions and data. |
| `crypto-dashboard/src/styles/index.css` | 142 | Terminal-like dark/turquoise dashboard styling. |
| `crypto-dashboard/vite.config.js` | 28 | Vite dev config with API/WebSocket proxy to FastAPI. |
| `START_POLYSIGNAL.bat` | new | One-click Windows launcher for FastAPI, React, and backend workers. |
| `STOP_POLYSIGNAL.bat` | new | One-click Windows stop command for local dashboard/API/workers. |
| `scripts/start_polysignal.ps1` | new | Main startup automation used by the launcher and optional Windows autostart task. |
| `scripts/install_autostart.ps1` | new | Optional Windows Task Scheduler installer for startup on user logon. |

Compatibility note:

- There is no root `live_paper_runner.py` tracked in git.
- There are no tracked `src/scoring.py`, `src/monitor.py`, `src/cohorts.py`, `src/signals.py`, or `src/paper.py` files. The real modules are under `polymarket_signal_bot/`.
- `src/indexer.py` and `src/auto_trainer.py` exist only as thin compatibility wrappers.

## Current Runtime Status

Measured through local process list and FastAPI endpoints.

### Running UI / API processes

- FastAPI backend: running on `http://127.0.0.1:8000`, process seen as `python.exe server/api.py`.
- React/Vite dashboard: running on `http://127.0.0.1:3000`, process seen as `node ./node_modules/vite/bin/vite.js`.
- Streamlit legacy dashboard: disabled and stopped. `dashboard.py` now points users to React instead of rendering the old control room.
- Windows one-click startup is available through `START_POLYSIGNAL.bat`.

### Managed system components from `/system/status`

At snapshot time FastAPI reports:

- Indexer: `running`
- Monitor: `running`
- Live Paper: `running`

Important context:

- The indexer log was cleaned after the earlier failed launch without `.env`.
- The server now opens component logs in overwrite mode on each start, so old startup errors do not remain in the active log tail.
- Current clean indexer log starts with fresh contract verification and new `OrderFilled sample` lines.

## Data Stores

| Path | Purpose | Current state |
| --- | --- | --- |
| `data/indexer.db` | Main indexed Polygon/Polymarket history and training tables. | `664,511` raw rows, `2,380+` `OrderFilled`, `325` scored wallets from latest training. |
| `data/paper_state.db` | Paper trading state. | Balance `203.11`, PnL `3.1144`, `10` open positions, `17` total positions. |
| `data/polysignal.db` | Legacy/local market, signal, and monitor data. | Used by monitor/live paper runner. |
| `data/polysignal.duckdb` | Analytics export for faster local analysis. | Present. |
| `data/exit_model.pkl` | Saved exit model. | Present, updated `2026-04-28 17:57:27`. |
| `data/exit_stats.json` | Exit model aggregate stats. | Present. |
| `data/exit_examples.json` | Saved exit examples for dashboard/debug. | Present with `10` saved examples. |

Exit stats:

- Median hold time: `422` seconds
- P75 hold time: `2,216` seconds
- P90 hold time: `11,347` seconds
- Average entry notional: `361.4581`
- Post-fixation reversal rate: `0.108`

## Active Dashboard Sections

### React dashboard

The React dashboard is the active control room.

Active sections:

- `System Control`: Start/Stop All for indexer, monitor, and live paper.
- `Indexer Metrics`: raw events, last block, speed, progress to `86M`.
- `Wallet Cohorts`: scored wallets, 14k wallet target progress, cohort counts, top wallets.
- `Train Model`: calls the training endpoint and refreshes scored/cohort/exit data.
- `Exit Examples`: reads `data/exit_examples.json` through the API; currently non-empty with `10` saved examples.
- `Paper Trading`: dry-run toggle, start/stop paper trading, balance/PnL/open positions.
- `Open Positions`: reads current paper positions from `paper_state.db` through FastAPI.
- `Equity Curve`: balance/equity chart area.
- `Live WebSocket`: receives live metrics from `/ws/live` when FastAPI is available.

### Current visible cohort data

- SCORED: `325`
- STABLE: `3`
- CANDIDATE: `6`
- WATCH: `75`
- NOISE: `241`
- EXIT EXAMPLES: `10` displayed/saved, `394` generated during latest training.

Top examples from `/api/wallets`:

- `0x45cc...6400`: `STABLE`, PnL `5974.88`, Sharpe `0.3678`, trades `47`
- `0x539f...cdcf`: `STABLE`, PnL `1397.16`, Sharpe `0.0`, trades `210`
- `0x209c...d319`: `STABLE`, PnL `7207.63`, Sharpe `0.7378`, trades `24`
- `0x0213...7d1b`: `CANDIDATE`, PnL `-1366.21`, Sharpe `-0.0912`, trades `63`
- `0x8a4c...532b`: `CANDIDATE`, PnL `1953.00`, Sharpe `0.5777`, trades `75`

## Training Pipeline Status

Current implemented flow:

1. `raw_transactions` is filled by `polymarket_signal_bot/indexer.py`.
2. `polymarket_signal_bot/scoring.py` scores wallets using SQL aggregations and only `event_type = 'OrderFilled'`.
3. Scores are saved into `scored_wallets` with compatibility fields including `wallet`, `address`, `computed_at`, and `calculated_at`.
4. `polymarket_signal_bot/auto_trainer.py` calls `cohorts.update_cohorts()` and fills `wallet_cohorts`.
5. Exit examples are generated from STABLE/CANDIDATE wallet history.
6. Exit model is saved to `data/exit_model.pkl`.
7. Exit stats are saved to `data/exit_stats.json`.
8. Dashboard/API read `scored_wallets`, `wallet_cohorts`, and `exit_examples.json`.

Latest training log summary:

- Raw rows: `662,200`
- Scored wallets: `325`
- Cohorts: `CANDIDATE=6`, `NOISE=241`, `STABLE=3`, `WATCH=75`
- Stable/Candidate wallets used for exit model: `9`
- Exit examples: `394`
- Saved examples: `10`
- Result: `ok=true`
- Elapsed: `0.135s`

## Known Problems / Open Items

1. Data volume is still far below the target.
   - Current: `663,905 / 86,000,000` raw rows.
   - Wallet target: `325 / 14,000`.
   - Current model/cohort quality is useful for wiring validation, not yet enough for production-grade whale following.

2. Paper trading state is live but still paper-only.
   - Last paper state: balance `203.11`, PnL `3.1144`, `10` open positions.
   - Runner is started in dry-run/paper mode; no real orders are placed.

3. Streamlit is disabled.
   - `dashboard.py` remains in the repo for compatibility helpers/tests only.
   - The active UI is React + FastAPI at `http://127.0.0.1:3000` and `http://127.0.0.1:8000`.

4. `live_paper_runner.py` path mismatch.
   - User-facing tasks sometimes refer to root `live_paper_runner.py`.
   - Actual tracked implementation is `polymarket_signal_bot/live_paper_runner.py`.
   - FastAPI starts it with `python -m polymarket_signal_bot.live_paper_runner`.

5. Full historical indexing is the main bottleneck.
   - Need long-running stable indexer/backfill cycles to grow from hundreds of thousands of rows toward 86M.
   - After larger backfills, rerun Train Model to expand `scored_wallets`, `wallet_cohorts`, and exit examples.

6. Windows autostart is prepared but not enabled by default.
   - Manual one-click launch: `START_POLYSIGNAL.bat`.
   - Optional login autostart: `scripts/install_autostart.ps1`.

## Next Practical Step

Start the system from React with Start All, confirm indexer remains running for more than a few minutes, then watch:

- `raw_events` growth toward `86M`
- `OrderFilled` growth
- wallet progress toward `14,000`
- Train Model output after each meaningful data increase

After indexing grows materially, rerun training and compare:

- number of scored wallets
- STABLE/CANDIDATE counts
- exit examples count
- paper trading PnL behavior

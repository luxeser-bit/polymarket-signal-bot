from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from .api import ApiError, PolymarketClient
from .alerts import TelegramConfig
from .analytics import (
    DEFAULT_DUCKDB_PATH,
    DuckDbMissingError,
    ExportConfig,
    analytics_report,
    export_to_duckdb,
    format_bytes,
    missing_duckdb_message,
    refresh_analytics_snapshot,
    sqlite_counts,
)
from .backtest import BacktestConfig, run_backtest
from .bulk_sync import BulkSync, BulkSyncConfig
from .cohorts import CohortConfig, format_cohort_summary, wallet_cohort_report
from .dashboard import serve_dashboard
from .demo import demo_trades, demo_wallets
from .features import build_decision_features, format_feature_summary
from .learning import format_wallet_outcome_summary, wallet_outcome_lookup, wallet_outcome_report
from .live_paper_runner import LivePaperConfig, LivePaperRunner
from .market_flow import MarketFlowConfig, format_market_flow_summary, sync_market_flow
from .monitor import Monitor, MonitorConfig
from .models import OrderBookSnapshot, Trade, Wallet
from .paper import ExitConfig, PaperBroker, RiskConfig
from .policy_optimizer import OptimizerConfig, policy_settings_from_recommendation, run_policy_optimizer
from .scoring import score_wallets
from .signals import SignalConfig, generate_signals
from .storage import DEFAULT_DB_PATH, Store
from .streaming import MissingWebsocketError, StreamConfig, StreamProcessor, missing_websocket_message
from .taxonomy import market_category, market_category_label


def main(argv: list[str] | None = None) -> int:
    _configure_output_encoding()
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except ApiError as exc:
        print(f"API error: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130


def _configure_output_encoding() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="polysignal",
        description="Public-data Polymarket wallet signal bot with paper trading only.",
    )
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help="SQLite database path.")
    sub = parser.add_subparsers(dest="command", required=True)

    init_db = sub.add_parser("init-db", help="Create or migrate the local database.")
    init_db.set_defaults(func=cmd_init_db)

    discover = sub.add_parser("discover", help="Import wallets from the public leaderboard.")
    discover.add_argument("--limit", type=int, default=50, help="Total wallets to fetch.")
    discover.add_argument("--category", default="OVERALL")
    discover.add_argument("--time-period", default="WEEK", choices=["DAY", "WEEK", "MONTH", "ALL"])
    discover.add_argument("--order-by", default="PNL", choices=["PNL", "VOL"])
    discover.set_defaults(func=cmd_discover)

    discover_flow = sub.add_parser("discover-from-trades", help="Promote wallets seen in saved trades into the watchlist.")
    discover_flow.add_argument("--limit", type=int, default=1000)
    discover_flow.add_argument("--min-notional", type=float, default=0.0)
    discover_flow.add_argument("--min-trades", type=int, default=1)
    discover_flow.add_argument("--since-days", type=int, default=30)
    discover_flow.set_defaults(func=cmd_discover_from_trades)

    market_flow = sub.add_parser("sync-market-trades", help="Collect trades from active markets and promote new wallets.")
    market_flow.add_argument("--market-limit", type=int, default=25)
    market_flow.add_argument("--market-offset", type=int, default=0)
    market_flow.add_argument("--market-order", default="volume24hr")
    market_flow.add_argument("--ascending", action="store_true")
    market_flow.add_argument("--min-market-volume", type=float, default=1000.0)
    market_flow.add_argument("--min-market-liquidity", type=float, default=250.0)
    market_flow.add_argument("--trades-per-market", type=int, default=200)
    market_flow.add_argument("--min-trade-cash", type=float, default=25.0)
    market_flow.add_argument("--include-makers", action="store_true")
    market_flow.add_argument("--no-promote-wallets", action="store_true")
    market_flow.add_argument("--wallet-limit", type=int, default=5000)
    market_flow.add_argument("--min-wallet-notional", type=float, default=100.0)
    market_flow.add_argument("--min-wallet-trades", type=int, default=2)
    market_flow.add_argument("--wallet-since-days", type=int, default=30)
    market_flow.add_argument("--stop-on-error", action="store_true")
    market_flow.add_argument("--analytics-export", action="store_true")
    market_flow.add_argument("--duckdb", default=str(DEFAULT_DUCKDB_PATH))
    market_flow.add_argument("--analytics-chunk-size", type=int, default=50000)
    market_flow.add_argument("--analytics-rebuild", action="store_true")
    market_flow.set_defaults(func=cmd_sync_market_trades)

    wallets_export = sub.add_parser("wallets-export", help="Export watched wallets to a text file.")
    wallets_export.add_argument("--out", required=True)
    wallets_export.add_argument("--limit", type=int, default=None)
    wallets_export.set_defaults(func=cmd_wallets_export)

    wallets_import = sub.add_parser("wallets-import", help="Import watched wallets from a text file.")
    wallets_import.add_argument("--path", required=True)
    wallets_import.add_argument("--source", default="file")
    wallets_import.set_defaults(func=cmd_wallets_import)

    sync = sub.add_parser("sync", help="Fetch recent trade activity for watched wallets.")
    sync.add_argument("--wallet", action="append", default=[], help="Wallet address. Repeat as needed.")
    sync.add_argument("--wallet-file", help="Text file with one wallet address per line.")
    sync.add_argument("--wallet-limit", type=int, default=None, help="Limit wallets loaded from the DB.")
    sync.add_argument("--days", type=int, default=7, help="History window to fetch.")
    sync.add_argument("--per-wallet-limit", type=int, default=250, help="Max activity rows per wallet.")
    sync.set_defaults(func=cmd_sync)

    bulk_sync = sub.add_parser("bulk-sync", help="Incrementally sync many watched wallets with checkpoints.")
    bulk_sync.add_argument("--wallet-limit", type=int, default=100)
    bulk_sync.add_argument("--page-size", type=int, default=500)
    bulk_sync.add_argument("--max-pages-per-wallet", type=int, default=2)
    bulk_sync.add_argument("--days", type=int, default=30)
    bulk_sync.add_argument("--overlap-seconds", type=int, default=3600)
    bulk_sync.add_argument("--sleep-seconds", type=float, default=0.15)
    bulk_sync.add_argument("--stop-on-error", action="store_true")
    bulk_sync.add_argument("--analytics-export", action="store_true", help="Refresh DuckDB snapshot after sync.")
    bulk_sync.add_argument("--duckdb", default=str(DEFAULT_DUCKDB_PATH))
    bulk_sync.add_argument("--analytics-chunk-size", type=int, default=50000)
    bulk_sync.add_argument("--analytics-rebuild", action="store_true")
    bulk_sync.set_defaults(func=cmd_bulk_sync)

    sync_books = sub.add_parser("sync-books", help="Fetch public CLOB order books for recent assets.")
    sync_books.add_argument("--asset", action="append", default=[], help="Token ID. Repeat as needed.")
    sync_books.add_argument("--asset-limit", type=int, default=40)
    sync_books.add_argument("--lookback-minutes", type=int, default=24 * 60)
    sync_books.set_defaults(func=cmd_sync_books)

    history_backfill = sub.add_parser("history-backfill", help="Backfill historical learning tables from local snapshots.")
    history_backfill.set_defaults(func=cmd_history_backfill)

    scan = sub.add_parser("scan", help="Score wallets, create signals, and update paper positions.")
    add_signal_args(scan)
    scan.add_argument("--history-days", type=int, default=14, help="Trade history used for wallet scoring.")
    scan.add_argument("--no-cohort-policy", action="store_true", help="Disable cohort-based signal sizing and auto-open gating.")
    scan.set_defaults(func=cmd_scan)

    backtest = sub.add_parser("backtest", help="Replay saved trades through the copy-trading strategy.")
    backtest.add_argument("--bankroll", type=float, default=200.0)
    backtest.add_argument("--min-wallet-score", type=float, default=0.55)
    backtest.add_argument("--min-trade-usdc", type=float, default=50.0)
    backtest.add_argument("--copy-delay-seconds", type=int, default=45)
    backtest.add_argument("--history-days", type=int, default=30)
    backtest.add_argument("--limit", type=int, default=100000)
    backtest.add_argument("--warmup-trades", type=int, default=8)
    backtest.add_argument("--use-cohort-policy", action="store_true")
    backtest.add_argument(
        "--cohort-policy-mode",
        default="strict",
        choices=["strict", "balanced", "stable_only", "liquidity_watch"],
    )
    backtest.add_argument("--compare-cohort-policy", action="store_true")
    backtest.set_defaults(func=cmd_backtest)

    optimizer = sub.add_parser("policy-optimizer", help="Backtest several cohort policies and save the best paper mode.")
    optimizer.add_argument("--bankroll", type=float, default=200.0)
    optimizer.add_argument("--min-wallet-score", type=float, default=0.55)
    optimizer.add_argument("--min-trade-usdc", type=float, default=50.0)
    optimizer.add_argument("--copy-delay-seconds", type=int, default=45)
    optimizer.add_argument("--history-days", type=int, default=30)
    optimizer.add_argument("--limit", type=int, default=100000)
    optimizer.add_argument("--warmup-trades", type=int, default=8)
    optimizer.add_argument("--min-closed-trades", type=int, default=25)
    optimizer.add_argument("--no-save", action="store_true", help="Do not save the recommended mode into runtime state.")
    optimizer.set_defaults(func=cmd_policy_optimizer)

    cohort_report = sub.add_parser("cohort-report", help="Rank wallets by cohort stability and repeatability.")
    cohort_report.add_argument("--history-days", type=int, default=30)
    cohort_report.add_argument("--min-notional", type=float, default=0.0)
    cohort_report.add_argument("--min-trades", type=int, default=1)
    cohort_report.add_argument("--limit", type=int, default=20)
    cohort_report.set_defaults(func=cmd_cohort_report)

    wallet_learning = sub.add_parser("wallet-learning", help="Rank wallet/category outcomes from paper decisions.")
    wallet_learning.add_argument("--since-days", type=int, default=90)
    wallet_learning.add_argument("--min-events", type=int, default=1)
    wallet_learning.add_argument("--limit", type=int, default=20)
    wallet_learning.set_defaults(func=cmd_wallet_learning)

    features_build = sub.add_parser("features-build", help="Build ML-ready feature rows from paper decisions.")
    features_build.add_argument("--since-days", type=int, default=None)
    features_build.add_argument("--limit", type=int, default=None)
    features_build.set_defaults(func=cmd_features_build)

    analytics_export = sub.add_parser("analytics-export", help="Export SQLite data into a DuckDB analytics snapshot.")
    analytics_export.add_argument("--duckdb", default=str(DEFAULT_DUCKDB_PATH))
    analytics_export.add_argument("--chunk-size", type=int, default=50000)
    analytics_export.add_argument("--rebuild", action="store_true")
    analytics_export.set_defaults(func=cmd_analytics_export)

    analytics_cmd = sub.add_parser("analytics-report", help="Show aggregates from a DuckDB analytics snapshot.")
    analytics_cmd.add_argument("--duckdb", default=str(DEFAULT_DUCKDB_PATH))
    analytics_cmd.add_argument("--limit", type=int, default=10)
    analytics_cmd.set_defaults(func=cmd_analytics_report)

    run_once = sub.add_parser("run-once", help="Discover, sync, scan, and report in one run.")
    run_once.add_argument("--leaderboard-limit", type=int, default=50)
    run_once.add_argument("--days", type=int, default=7)
    run_once.add_argument("--per-wallet-limit", type=int, default=250)
    run_once.add_argument("--time-period", default="WEEK", choices=["DAY", "WEEK", "MONTH", "ALL"])
    run_once.add_argument("--no-cohort-policy", action="store_true")
    add_signal_args(run_once)
    run_once.set_defaults(func=cmd_run_once)

    demo = sub.add_parser("demo", help="Load synthetic data and run the signal pipeline.")
    add_signal_args(demo)
    demo.add_argument("--no-cohort-policy", action="store_true")
    demo.set_defaults(func=cmd_demo)

    report = sub.add_parser("report", help="Show scores, recent signals, and paper portfolio.")
    report.add_argument("--limit", type=int, default=10)
    report.set_defaults(func=cmd_report)

    journal = sub.add_parser("paper-journal", help="Show the paper decision journal for learning.")
    journal.add_argument("--limit", type=int, default=20)
    journal.add_argument("--since-days", type=int, default=30)
    journal.set_defaults(func=cmd_paper_journal)

    reviews = sub.add_parser("reviews", help="List pending, approved, or rejected signal reviews.")
    reviews.add_argument("--status", default="PENDING", choices=["PENDING", "APPROVED", "REJECTED"])
    reviews.add_argument("--limit", type=int, default=20)
    reviews.set_defaults(func=cmd_reviews)

    review = sub.add_parser("review", help="Approve or reject a signal by signal id.")
    review.add_argument("signal_id")
    review.add_argument("--status", required=True, choices=["APPROVED", "REJECTED", "PENDING"])
    review.add_argument("--note", default="")
    review.set_defaults(func=cmd_review)

    open_approved = sub.add_parser("open-approved", help="Open paper positions for approved signals.")
    open_approved.add_argument("--limit", type=int, default=25)
    open_approved.set_defaults(func=cmd_open_approved)

    live_paper = sub.add_parser("live-paper", help="Run async live paper signal and position management.")
    live_paper.add_argument("--poll-interval", type=int, default=60)
    live_paper.add_argument("--price-interval", type=int, default=15)
    live_paper.add_argument("--use-stream-queue", action="store_true", help="Consume stream_events instead of polling APIs for signal ticks.")
    live_paper.add_argument("--stream-queue-interval", type=float, default=1.0)
    live_paper.add_argument("--stream-batch-limit", type=int, default=500)
    live_paper.add_argument("--stream-min-events", type=int, default=1)
    live_paper.add_argument("--manual-confirm", action="store_true")
    live_paper.add_argument("--dry-run", action="store_true")
    live_paper.add_argument("--no-close-on-stop", action="store_true")
    live_paper.add_argument("--state-path", default="data/live_paper_state.json")
    live_paper.add_argument("--log-path", default="data/live_paper_runner.log")
    live_paper.add_argument("--leaderboard-limit", type=int, default=0)
    live_paper.add_argument("--wallet-limit", type=int, default=100)
    live_paper.add_argument("--days", type=int, default=7)
    live_paper.add_argument("--per-wallet-limit", type=int, default=150)
    live_paper.add_argument("--bankroll", type=float, default=200.0)
    live_paper.add_argument("--min-wallet-score", type=float, default=0.55)
    live_paper.add_argument("--min-trade-usdc", type=float, default=50.0)
    live_paper.add_argument("--lookback-minutes", type=int, default=120)
    live_paper.add_argument("--max-signals", type=int, default=20)
    live_paper.add_argument("--book-asset-limit", type=int, default=40)
    live_paper.add_argument("--max-spread", type=float, default=0.08)
    live_paper.add_argument("--min-liquidity-score", type=float, default=0.10)
    live_paper.add_argument("--min-depth-usdc", type=float, default=25.0)
    live_paper.add_argument("--stop-loss-pct", type=float, default=0.28)
    live_paper.add_argument("--take-profit-pct", type=float, default=0.40)
    live_paper.add_argument("--no-cohort-policy", action="store_true")
    live_paper.add_argument("--no-learning-policy", action="store_true")
    live_paper.set_defaults(func=cmd_live_paper)

    dashboard = sub.add_parser("dashboard", help="Run the local browser dashboard.")
    dashboard.add_argument("--host", default="127.0.0.1")
    dashboard.add_argument("--port", type=int, default=8765)
    dashboard.set_defaults(func=cmd_dashboard)

    stream = sub.add_parser("stream", help="Listen to public Polymarket CLOB WebSocket events.")
    stream.add_argument("--asset", action="append", default=[], help="CLOB token id. Repeat as needed.")
    stream.add_argument("--asset-limit", type=int, default=40)
    stream.add_argument("--lookback-minutes", type=int, default=24 * 60)
    stream.add_argument("--max-events", type=int, default=0, help="0 means stream forever.")
    stream.add_argument("--batch-size", type=int, default=50)
    stream.add_argument("--flush-interval-seconds", type=float, default=1.0)
    stream.add_argument("--reconcile-min-notional", type=float, default=50.0)
    stream.add_argument("--reconcile-limit", type=int, default=30)
    stream.add_argument("--reconcile-window-seconds", type=int, default=45)
    stream.add_argument("--no-reconcile", action="store_true")
    stream.add_argument("--scan-every-events", type=int, default=0, help="Run paper scan after N stream events. 0 disables.")
    stream.add_argument("--wallet-limit", type=int, default=100)
    stream.add_argument("--bankroll", type=float, default=200.0)
    stream.add_argument("--min-wallet-score", type=float, default=0.55)
    stream.add_argument("--min-trade-usdc", type=float, default=50.0)
    stream.add_argument("--max-signals", type=int, default=20)
    stream.set_defaults(func=cmd_stream)

    monitor = sub.add_parser("monitor", help="Continuously update data, signals, paper positions, and alerts.")
    monitor.add_argument("--interval-seconds", type=int, default=60)
    monitor.add_argument("--iterations", type=int, default=0, help="0 means run forever.")
    monitor.add_argument("--leaderboard-limit", type=int, default=50)
    monitor.add_argument("--discover-every", type=int, default=10, help="Discover wallets every N loops.")
    monitor.add_argument("--wallet-limit", type=int, default=50)
    monitor.add_argument("--days", type=int, default=7)
    monitor.add_argument("--per-wallet-limit", type=int, default=150)
    monitor.add_argument("--bankroll", type=float, default=200.0)
    monitor.add_argument("--min-wallet-score", type=float, default=0.55)
    monitor.add_argument("--min-trade-usdc", type=float, default=50.0)
    monitor.add_argument("--lookback-minutes", type=int, default=120)
    monitor.add_argument("--max-signals", type=int, default=20)
    monitor.add_argument("--max-spread", type=float, default=0.08)
    monitor.add_argument("--min-liquidity-score", type=float, default=0.10)
    monitor.add_argument("--min-depth-usdc", type=float, default=25.0)
    monitor.add_argument("--max-book-price-deviation", type=float, default=0.12)
    monitor.add_argument("--max-wallet-trades-per-day", type=float, default=60.0)
    monitor.add_argument("--min-cluster-wallets", type=int, default=1)
    monitor.add_argument("--min-cluster-notional", type=float, default=0.0)
    monitor.add_argument("--no-learning-policy", action="store_true")
    monitor.add_argument("--max-total-exposure-pct", type=float, default=0.65)
    monitor.add_argument("--max-open-positions", type=int, default=20)
    monitor.add_argument("--max-new-positions-per-run", type=int, default=6)
    monitor.add_argument("--max-market-exposure-usdc", type=float, default=40.0)
    monitor.add_argument("--max-wallet-exposure-usdc", type=float, default=60.0)
    monitor.add_argument("--max-daily-loss-usdc", type=float, default=20.0)
    monitor.add_argument("--max-worst-stop-loss-usdc", type=float, default=35.0)
    monitor.add_argument("--paper-max-hold-hours", type=int, default=36)
    monitor.add_argument("--paper-stale-price-hours", type=int, default=48)
    monitor.add_argument("--max-risk-trim-per-run", type=int, default=4)
    monitor.add_argument("--no-risk-trim", action="store_true")
    monitor.add_argument("--no-discover", action="store_true", help="Use wallets already in the database.")
    monitor.add_argument("--no-sync", action="store_true", help="Skip API activity sync and only rescan local data.")
    monitor.add_argument("--no-books", action="store_true", help="Skip CLOB order-book sync.")
    monitor.add_argument("--book-asset-limit", type=int, default=40)
    monitor.add_argument("--telegram-token", default=os.environ.get("TELEGRAM_BOT_TOKEN", ""))
    monitor.add_argument("--telegram-chat-id", default=os.environ.get("TELEGRAM_CHAT_ID", ""))
    monitor.add_argument("--telegram-dry-run", action="store_true")
    monitor.add_argument("--market-flow-every", type=int, default=0, help="Sync active market trades every N loops. 0 disables.")
    monitor.add_argument("--market-flow-market-limit", type=int, default=15)
    monitor.add_argument("--market-flow-trades-per-market", type=int, default=100)
    monitor.add_argument("--market-flow-min-trade-cash", type=float, default=25.0)
    monitor.add_argument("--market-flow-min-wallet-notional", type=float, default=100.0)
    monitor.add_argument("--market-flow-min-wallet-trades", type=int, default=2)
    monitor.add_argument("--analytics-export-every", type=int, default=0, help="Refresh DuckDB every N loops. 0 disables.")
    monitor.add_argument("--duckdb", default=str(DEFAULT_DUCKDB_PATH))
    monitor.add_argument("--analytics-chunk-size", type=int, default=50000)
    monitor.add_argument("--analytics-rebuild", action="store_true")
    monitor.set_defaults(func=cmd_monitor)

    return parser


def add_signal_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--bankroll", type=float, default=200.0)
    parser.add_argument("--risk-per-signal", type=float, default=0.04)
    parser.add_argument("--max-position-usdc", type=float, default=25.0)
    parser.add_argument("--min-wallet-score", type=float, default=0.55)
    parser.add_argument("--min-trade-usdc", type=float, default=50.0)
    parser.add_argument("--lookback-minutes", type=int, default=90)
    parser.add_argument("--max-signals", type=int, default=20)
    parser.add_argument("--max-spread", type=float, default=0.08)
    parser.add_argument("--min-liquidity-score", type=float, default=0.0)
    parser.add_argument("--min-depth-usdc", type=float, default=0.0)
    parser.add_argument("--max-book-price-deviation", type=float, default=0.12)
    parser.add_argument("--max-wallet-trades-per-day", type=float, default=60.0)
    parser.add_argument("--min-cluster-wallets", type=int, default=1)
    parser.add_argument("--min-cluster-notional", type=float, default=0.0)
    parser.add_argument("--no-learning-policy", action="store_true")
    parser.add_argument("--max-total-exposure-pct", type=float, default=0.65)
    parser.add_argument("--max-open-positions", type=int, default=20)
    parser.add_argument("--max-new-positions-per-run", type=int, default=6)
    parser.add_argument("--max-market-exposure-usdc", type=float, default=40.0)
    parser.add_argument("--max-wallet-exposure-usdc", type=float, default=60.0)
    parser.add_argument("--max-daily-loss-usdc", type=float, default=20.0)
    parser.add_argument("--max-worst-stop-loss-usdc", type=float, default=35.0)
    parser.add_argument("--paper-max-hold-hours", type=int, default=36)
    parser.add_argument("--paper-stale-price-hours", type=int, default=48)
    parser.add_argument("--max-risk-trim-per-run", type=int, default=4)
    parser.add_argument("--no-risk-trim", action="store_true")


def cmd_init_db(args: argparse.Namespace) -> int:
    with open_store(args.db) as store:
        store.init_schema()
    print(f"Database ready: {args.db}")
    return 0


def cmd_discover(args: argparse.Namespace) -> int:
    client = PolymarketClient()
    wallets: list[Wallet] = []
    remaining = max(0, args.limit)
    offset = 0
    while remaining > 0:
        page_limit = min(50, remaining)
        rows = client.leaderboard(
            limit=page_limit,
            offset=offset,
            category=args.category,
            time_period=args.time_period,
            order_by=args.order_by,
        )
        if not rows:
            break
        wallets.extend(Wallet.from_leaderboard(row) for row in rows)
        fetched = len(rows)
        remaining -= fetched
        offset += fetched
        if fetched < page_limit:
            break
    with open_store(args.db) as store:
        store.init_schema()
        count = store.upsert_wallets(wallets)
    print(f"Imported wallets: {count}")
    return 0


def cmd_discover_from_trades(args: argparse.Namespace) -> int:
    with open_store(args.db) as store:
        store.init_schema()
        since_ts = int(time.time()) - max(1, args.since_days) * 86400
        wallets = store.discover_wallets_from_trades(
            limit=args.limit,
            min_notional=args.min_notional,
            min_trades=args.min_trades,
            since_ts=since_ts,
        )
        count = store.insert_wallets_ignore_existing(wallets)
    print(f"Wallets discovered from trades: {count}")
    return 0


def cmd_sync_market_trades(args: argparse.Namespace) -> int:
    with open_store(args.db) as store:
        store.init_schema()
        summary = sync_market_flow(
            store,
            config=MarketFlowConfig(
                market_limit=args.market_limit,
                market_offset=args.market_offset,
                market_order=args.market_order,
                market_ascending=args.ascending,
                min_market_volume=args.min_market_volume,
                min_market_liquidity=args.min_market_liquidity,
                trades_per_market=args.trades_per_market,
                min_trade_cash=args.min_trade_cash,
                taker_only=not args.include_makers,
                promote_wallets=not args.no_promote_wallets,
                wallet_limit=args.wallet_limit,
                min_wallet_notional=args.min_wallet_notional,
                min_wallet_trades=args.min_wallet_trades,
                wallet_since_days=args.wallet_since_days,
                stop_on_error=args.stop_on_error,
            ),
        )
        analytics_result = None
        if args.analytics_export:
            analytics_result = refresh_analytics_snapshot(
                store,
                duckdb_path=args.duckdb,
                chunk_size=args.analytics_chunk_size,
                rebuild=args.analytics_rebuild,
            )
    print(format_market_flow_summary(summary))
    if summary["error_samples"]:
        print("Market flow errors:")
        for item in summary["error_samples"]:
            print(f"  {item}")
    if analytics_result:
        if analytics_result["ok"]:
            print(
                f"Analytics refreshed: {analytics_result['duckdb_path']} "
                f"{format_bytes(analytics_result['size_bytes'])}"
            )
        else:
            print(f"Analytics refresh failed: {analytics_result['error']}", file=sys.stderr)
    return 0


def cmd_wallets_export(args: argparse.Namespace) -> int:
    with open_store(args.db) as store:
        store.init_schema()
        wallets = store.fetch_wallets(limit=args.limit)
    path = Path(args.out)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(wallet.address for wallet in wallets) + ("\n" if wallets else ""), encoding="utf-8")
    print(f"Wallets exported: {len(wallets)} -> {path}")
    return 0


def cmd_wallets_import(args: argparse.Namespace) -> int:
    wallets = [Wallet(address=address.lower(), source=args.source) for address in read_wallet_file(Path(args.path))]
    with open_store(args.db) as store:
        store.init_schema()
        count = store.insert_wallets_ignore_existing(wallets)
    print(f"Wallets imported: {count}")
    return 0


def cmd_sync(args: argparse.Namespace) -> int:
    client = PolymarketClient()
    with open_store(args.db) as store:
        store.init_schema()
        wallets = load_requested_wallets(args, store)
        if not wallets:
            print("No wallets to sync. Run discover first or pass --wallet.", file=sys.stderr)
            return 1
        start = int(time.time()) - max(1, args.days) * 86400
        total_inserted = 0
        for index, wallet in enumerate(wallets, start=1):
            rows = client.user_activity(
                wallet.address,
                limit=min(500, args.per_wallet_limit),
                start=start,
                types=["TRADE"],
            )
            trades = [Trade.from_api(row, source="activity") for row in rows]
            inserted = store.insert_trades(trades)
            total_inserted += inserted
            print(f"[{index}/{len(wallets)}] {wallet.address}: {inserted} new trades")
    print(f"Sync complete. New trades: {total_inserted}")
    return 0


def cmd_bulk_sync(args: argparse.Namespace) -> int:
    with open_store(args.db) as store:
        store.init_schema()
        summary = BulkSync(
            store=store,
            config=BulkSyncConfig(
                wallet_limit=args.wallet_limit,
                page_size=args.page_size,
                max_pages_per_wallet=args.max_pages_per_wallet,
                days=args.days,
                overlap_seconds=args.overlap_seconds,
                sleep_seconds=args.sleep_seconds,
                stop_on_error=args.stop_on_error,
            ),
        ).run_once()
        analytics_result = None
        if args.analytics_export:
            analytics_result = refresh_analytics_snapshot(
                store,
                duckdb_path=args.duckdb,
                chunk_size=args.analytics_chunk_size,
                rebuild=args.analytics_rebuild,
            )
    print(
        "Bulk sync complete: "
        f"wallets={summary['wallets']} seen={summary['seen']} "
        f"inserted={summary['inserted']} pages={summary['pages']} errors={summary['errors']}"
    )
    if analytics_result:
        if analytics_result["ok"]:
            print(
                f"Analytics refreshed: {analytics_result['duckdb_path']} "
                f"{format_bytes(analytics_result['size_bytes'])}"
            )
        else:
            print(f"Analytics refresh failed: {analytics_result['error']}", file=sys.stderr)
    return 0


def cmd_sync_books(args: argparse.Namespace) -> int:
    client = PolymarketClient()
    with open_store(args.db) as store:
        store.init_schema()
        assets = args.asset or store.recent_assets(
            limit=args.asset_limit,
            since_ts=int(time.time()) - args.lookback_minutes * 60,
        )
        if not assets:
            print("No assets to sync. Run sync first or pass --asset.", file=sys.stderr)
            return 1
        total = 0
        for chunk in chunks(assets, 20):
            rows = client.order_books(chunk)
            books = [OrderBookSnapshot.from_api(row) for row in rows]
            raw_by_asset = {str(row.get("asset_id") or ""): row for row in rows}
            total += store.upsert_order_books(books, raw_by_asset=raw_by_asset)
        history = store.order_book_history_summary()
        print(f"Order books synced: {total}")
        print(f"Order-book history: snapshots={int(history['snapshots'])} assets={int(history['assets'])}")
    return 0


def cmd_history_backfill(args: argparse.Namespace) -> int:
    with open_store(args.db) as store:
        store.init_schema()
        inserted = store.backfill_order_book_history_from_latest()
        history = store.order_book_history_summary()
    print(f"Order-book history backfilled: {inserted}")
    print(
        f"History snapshots={int(history['snapshots'])} assets={int(history['assets'])} "
        f"avg_spread={history['avg_spread']:.4f} avg_liquidity={history['avg_liquidity']:.3f}"
    )
    return 0


def cmd_scan(args: argparse.Namespace) -> int:
    with open_store(args.db) as store:
        store.init_schema()
        signals, opened, closed = run_scan(args, store)
        print(f"Signals created: {len(signals)}")
        print(f"Paper positions opened: {len(opened)}")
        print(f"Paper positions closed: {len(closed)}")
        _print_runtime_line(store, "exit_last_summary", "Exit")
        _print_runtime_line(store, "risk_last_summary", "Risk")
    return 0


def cmd_backtest(args: argparse.Namespace) -> int:
    with open_store(args.db) as store:
        store.init_schema()
        since_ts = int(time.time()) - max(1, args.history_days) * 86400
        trades = store.fetch_trades_chronological(since_ts=since_ts, limit=args.limit)
        order_books = store.fetch_order_books(list({trade.asset for trade in trades}))
    if args.compare_cohort_policy:
        baseline = run_backtest(
            trades,
            _backtest_config_from_args(args, use_cohort_policy=False),
            order_books=order_books,
        )
        cohort = run_backtest(
            trades,
            _backtest_config_from_args(args, use_cohort_policy=True),
            order_books=order_books,
        )
        print("\nBacktest comparison")
        print(
            "  baseline: "
            f"pnl={money(baseline['pnl'])} trades={baseline['closed_trades']} "
            f"dd={baseline['max_drawdown'] * 100:.1f}% skipped={baseline['skipped']}"
        )
        print(
            "  cohort:   "
            f"pnl={money(cohort['pnl'])} trades={cohort['closed_trades']} "
            f"dd={cohort['max_drawdown'] * 100:.1f}% skipped={cohort['skipped']}"
        )
        print(
            "  delta:    "
            f"pnl={money(cohort['pnl'] - baseline['pnl'])} "
            f"trades={cohort['closed_trades'] - baseline['closed_trades']} "
            f"dd={(cohort['max_drawdown'] - baseline['max_drawdown']) * 100:.1f}pp"
        )
        _print_backtest_result(cohort, title="Backtest cohort-aware")
        return 0

    result = run_backtest(
        trades,
        _backtest_config_from_args(args, use_cohort_policy=args.use_cohort_policy),
        order_books=order_books,
    )
    _print_backtest_result(result, title="Backtest")
    return 0


def cmd_policy_optimizer(args: argparse.Namespace) -> int:
    with open_store(args.db) as store:
        store.init_schema()
        since_ts = int(time.time()) - max(1, args.history_days) * 86400
        trades = store.fetch_trades_chronological(since_ts=since_ts, limit=args.limit)
        order_books = store.fetch_order_books(list({trade.asset for trade in trades}))
        result = run_policy_optimizer(
            trades,
            _backtest_config_from_args(args, use_cohort_policy=False),
            order_books=order_books,
            optimizer_config=OptimizerConfig(min_closed_trades=args.min_closed_trades),
        )
        if not args.no_save:
            _save_policy_optimizer_result(store, result)

    _print_policy_optimizer_result(result)
    if not args.no_save:
        print("\nSaved recommended paper mode into runtime_state.")
    return 0


def _backtest_config_from_args(
    args: argparse.Namespace,
    *,
    use_cohort_policy: bool,
    cohort_policy_mode: str | None = None,
) -> BacktestConfig:
    return BacktestConfig(
        bankroll=args.bankroll,
        min_wallet_score=args.min_wallet_score,
        min_trade_usdc=args.min_trade_usdc,
        copy_delay_seconds=args.copy_delay_seconds,
        warmup_trades=args.warmup_trades,
        use_cohort_policy=use_cohort_policy,
        cohort_policy_mode=cohort_policy_mode or getattr(args, "cohort_policy_mode", "strict"),
    )


def _save_policy_optimizer_result(store: Store, result: dict[str, object]) -> None:
    recommended = result.get("recommended") or {}
    if not isinstance(recommended, dict):
        recommended = {}
    policy = str(recommended.get("policy") or "baseline")
    summary = (
        f"recommended={policy} "
        f"score={float(recommended.get('optimizer_score') or 0.0):.2f} "
        f"pnl={money(float(recommended.get('pnl') or 0.0))} "
        f"dd={float(recommended.get('max_drawdown') or 0.0) * 100:.1f}% "
        f"trades={int(recommended.get('closed_trades') or 0)}"
    )
    store.set_runtime_state("policy_optimizer_recommended", policy)
    store.set_runtime_state("policy_optimizer_last_summary", summary)
    store.set_runtime_state(
        "policy_optimizer_last_result",
        json.dumps(
            {
                "recommended": recommended,
                "rows": result.get("rows") or [],
                "config": result.get("config") or {},
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
    )


def _print_policy_optimizer_result(result: dict[str, object]) -> None:
    rows = result.get("rows") or []
    recommended = result.get("recommended") or {}
    print("\nPolicy optimizer")
    if isinstance(recommended, dict) and recommended:
        print(
            "  recommended: "
            f"{recommended['policy']} "
            f"score={float(recommended['optimizer_score']):.2f} "
            f"pnl={money(float(recommended['pnl']))} "
            f"dd={float(recommended['max_drawdown']) * 100:.1f}% "
            f"trades={recommended['closed_trades']}"
        )
    else:
        print("  No recommendation: no trades in the selected period.")
    print("\nPolicies")
    print("  policy             pnl       dd     trades  hit    skipped  liq    score")
    for row in rows:
        print(
            f"  {row['policy']:<17} "
            f"{money(float(row['pnl'])):>9} "
            f"{float(row['max_drawdown']) * 100:>5.1f}% "
            f"{int(row['closed_trades']):>6} "
            f"{float(row['hit_rate']) * 100:>5.1f}% "
            f"{int(row['skipped']):>8} "
            f"{float(row['liquidity_coverage']) * 100:>5.1f}% "
            f"{float(row['optimizer_score']):>8.2f}"
        )


def _print_backtest_result(result: dict[str, object], *, title: str) -> None:
    print(f"\n{title}")
    print(f"  period: {format_ts(result['first_trade_at'])} -> {format_ts(result['last_trade_at'])}")
    print(f"  bankroll: {money(result['start_bankroll'])} -> {money(result['end_bankroll'])}")
    print(f"  pnl: {money(result['pnl'])}")
    print(
        f"  trades: {result['closed_trades']} wins={result['wins']} losses={result['losses']} "
        f"hit_rate={result['hit_rate'] * 100:.1f}%"
    )
    print(
        f"  max_drawdown={result['max_drawdown'] * 100:.1f}% "
        f"max_exposure={money(result['max_exposure'])} "
        f"liquidity_coverage={result['liquidity_coverage'] * 100:.1f}% "
        f"skipped={result['skipped']}"
    )
    if result["by_category"]:
        print("\nBy category")
        for item in result["by_category"]:
            print(
                f"  {item['category']:<9} pnl={money(item['pnl'])} "
                f"trades={item['trades']} hit={item['hit_rate'] * 100:.1f}%"
            )
    if result["by_liquidity"]:
        print("\nBy liquidity")
        for item in result["by_liquidity"]:
            print(
                f"  {item['liquidity_bucket']:<9} pnl={money(item['pnl'])} "
                f"trades={item['trades']} hit={item['hit_rate'] * 100:.1f}%"
            )
    if result["by_cohort"]:
        print("\nBy cohort")
        for item in result["by_cohort"]:
            print(
                f"  {item['cohort_status']:<9} pnl={money(item['pnl'])} "
                f"trades={item['trades']} hit={item['hit_rate'] * 100:.1f}%"
            )
    if result["by_wallet"]:
        print("\nTop wallets in replay")
        for item in result["by_wallet"][:5]:
            print(
                f"  {short_wallet(item['wallet'])} pnl={money(item['pnl'])} "
                f"trades={item['trades']} hit={item['hit_rate'] * 100:.1f}%"
            )
    if result["sample"]:
        print("\nRecent simulated exits")
        for item in result["sample"]:
            print(
                f"  {item['reason']} {item['outcome'] or item['asset']} "
                f"{item['entry']:.3f}->{item['exit']:.3f} pnl={money(item['pnl'])}"
            )


def cmd_cohort_report(args: argparse.Namespace) -> int:
    with open_store(args.db) as store:
        store.init_schema()
        report = wallet_cohort_report(
            store,
            CohortConfig(
                history_days=args.history_days,
                min_notional=args.min_notional,
                min_trades=args.min_trades,
                limit=args.limit,
            ),
        )
    print("\nWallet cohort stability")
    print(f"  {format_cohort_summary(report)}")
    if report["statusCohorts"]:
        print("\nBy status")
        for row in report["statusCohorts"]:
            print(
                f"  {row['status']:<9} wallets={row['wallets']} stable={row['stable']} "
                f"avg={row['avgStability']:.3f} notional={money(row['notional'])}"
            )
    if report["sourceCohorts"]:
        print("\nBy source")
        for row in report["sourceCohorts"]:
            print(
                f"  {row['source']:<12} wallets={row['wallets']} stable={row['stable']} "
                f"avg={row['avgStability']:.3f} notional={money(row['notional'])}"
            )
    print("\nTop wallets")
    if not report["wallets"]:
        print("  No wallets matched the cohort filters.")
    for row in report["wallets"]:
        print(
            f"  {row['status']:<9} {row['stabilityScore']:.3f} {short_wallet(row['wallet'])} "
            f"src={row['source']} days={row['activeDays']} markets={row['marketCount']} "
            f"trades={row['trades']} notional={money(row['notional'])} "
            f"repeat={row['repeatability']:.2f} dd={row['drawdown']:.2f}"
        )
    return 0


def cmd_wallet_learning(args: argparse.Namespace) -> int:
    with open_store(args.db) as store:
        store.init_schema()
        report = wallet_outcome_report(
            store,
            since_days=args.since_days,
            min_events=args.min_events,
            limit=args.limit,
        )
    print("\nWallet learning report")
    print(f"  {format_wallet_outcome_summary(report)}")

    print("\nTop wallet/category outcomes")
    if not report["wallets"]:
        print("  No paper decision outcomes yet.")
    for row in report["wallets"]:
        print(
            f"  {row['learningScore']:.3f} {row['categoryLabel']:<8} {short_wallet(row['wallet'])} "
            f"events={row['events']} sig={row['signals']} open={row['opened']} "
            f"block={row['blocked']} closed={row['closed']} pnl={money(row['pnl'])} "
            f"hit={row['hitRate']:.0%} risk_exit={row['riskExitRate']:.0%}"
        )
        detail = []
        if row["topCohort"]:
            detail.append(f"cohort={row['topCohort']}")
        if row["topBlockReason"]:
            detail.append(f"top_block={row['topBlockReason']}")
        if detail:
            print(f"    {' '.join(detail)}")

    print("\nBy category")
    for row in report["categories"]:
        print(
            f"  {row['categoryLabel']:<8} events={row['events']} open={row['opened']} "
            f"block={row['blocked']} closed={row['closed']} pnl={money(row['pnl'])} "
            f"score={row['learningScore']:.3f}"
        )
    return 0


def cmd_features_build(args: argparse.Namespace) -> int:
    with open_store(args.db) as store:
        store.init_schema()
        result = build_decision_features(store, since_days=args.since_days, limit=args.limit)
    summary = result["summary"]
    print(f"Decision features built: {result['inserted']}")
    print(f"Summary: {format_feature_summary(summary)}")
    print("Labels")
    for row in summary["labels"][:10]:
        print(
            f"  {str(row['label']):<8} {str(row['category']):<9} "
            f"rows={row['rows']} pnl={money(row['pnl'])}"
        )
    return 0


def cmd_analytics_export(args: argparse.Namespace) -> int:
    try:
        with Store(args.db) as store:
            store.init_schema()
            result = refresh_analytics_snapshot(
                store,
                duckdb_path=args.duckdb,
                chunk_size=args.chunk_size,
                rebuild=args.rebuild,
            )
    except DuckDbMissingError:
        counts = sqlite_counts(args.db)
        print(missing_duckdb_message(), file=sys.stderr)
        if counts:
            print("SQLite tables available:")
            for table, count in counts.items():
                print(f"  {table}: {count}")
        return 2
    if not result["ok"]:
        print(f"Analytics refresh failed: {result['error']}", file=sys.stderr)
        return 2
    print(f"DuckDB snapshot: {result['duckdb_path']}")
    print(f"Elapsed: {result['elapsed_seconds']}s Size: {format_bytes(result['size_bytes'])}")
    for table, count in result["counts"].items():
        print(f"  {table}: {count}")
    return 0


def cmd_analytics_report(args: argparse.Namespace) -> int:
    try:
        report = analytics_report(args.duckdb, limit=args.limit)
    except DuckDbMissingError:
        print(missing_duckdb_message(), file=sys.stderr)
        return 2
    print("\nAnalytics snapshot")
    if report["meta"]:
        meta = report["meta"][0]
        print(f"  source={meta.get('sqlite_path')} exported_at={meta.get('exported_at')}")
    print("\nCounts")
    for table, count in report["counts"].items():
        print(f"  {table}: {count}")
    print("\nCategories")
    for row in report["categories"]:
        print(f"  {row['category']:<9} trades={row['trades']} notional={money(row['notional'])}")
    print("\nTop wallets")
    for row in report["wallets"]:
        print(
            f"  {short_wallet(row['proxy_wallet'])} trades={row['trades']} "
            f"notional={money(row['notional'])} days={row['active_days']}"
        )
    print("\nTop markets")
    for row in report["markets"]:
        print(
            f"  {row['category']:<9} trades={row['trades']} "
            f"notional={money(row['notional'])} {str(row['title'])[:72]}"
        )
    print("\nStable cohorts")
    for row in report["cohorts"]:
        print(
            f"  {row['status']:<9} {row['stability_score']:.3f} "
            f"{short_wallet(row['proxy_wallet'])} src={row['source']} "
            f"days={row['active_days']} markets={row['market_count']} "
            f"notional={money(row['notional'])}"
        )
    print("\nPaper decisions")
    for row in report["paper_events"]:
        print(
            f"  {row['event_type']:<14} {str(row['reason']):<22} "
            f"events={row['events']} pnl={money(row['pnl'])} avg_conf={row['avg_confidence']:.3f}"
        )
    print("\nWallet outcomes")
    for row in report["wallet_outcomes"]:
        print(
            f"  {row['learning_score']:.3f} {str(row['category']).upper():<8} "
            f"{short_wallet(row['wallet'])} events={row['events']} closed={row['closed']} "
            f"pnl={money(row['pnl'])} hit={row['hit_rate']:.0%} blocked={row['blocked_rate']:.0%}"
        )
    print("\nDecision features")
    for row in report["decision_features"]:
        print(
            f"  {str(row['label']):<8} {str(row['category']):<9} rows={row['rows']} "
            f"pnl={money(row['pnl'])} conf={row['avg_confidence']:.3f} "
            f"learn={row['avg_learning_score']:.3f} liq={row['avg_liquidity_score']:.3f}"
        )
    print("\nStream events")
    for row in report["stream_events"]:
        print(
            f"  {str(row['event_type']):<18} events={row['events']} "
            f"assets={row['assets']} markets={row['markets']} notional={money(row['notional'])}"
        )
    print("\nOrder-book history")
    for row in report["book_history"]:
        print(
            f"  {row['observed_day']} snapshots={row['snapshots']} assets={row['assets']} "
            f"spread={row['avg_spread']:.4f} liquidity={row['avg_liquidity']:.3f} "
            f"depth={money(row['avg_depth'])}"
        )
    return 0


def cmd_run_once(args: argparse.Namespace) -> int:
    discover_args = argparse.Namespace(
        db=args.db,
        limit=args.leaderboard_limit,
        category="OVERALL",
        time_period=args.time_period,
        order_by="PNL",
    )
    code = cmd_discover(discover_args)
    if code:
        return code
    sync_args = argparse.Namespace(
        db=args.db,
        wallet=[],
        wallet_file=None,
        wallet_limit=args.leaderboard_limit,
        days=args.days,
        per_wallet_limit=args.per_wallet_limit,
    )
    code = cmd_sync(sync_args)
    if code:
        return code
    scan_args = argparse.Namespace(**vars(args))
    scan_args.history_days = args.days
    code = cmd_scan(scan_args)
    if code:
        return code
    return cmd_report(argparse.Namespace(db=args.db, limit=10))


def cmd_demo(args: argparse.Namespace) -> int:
    with open_store(args.db) as store:
        store.init_schema()
        wallets = demo_wallets()
        trades = demo_trades()
        store.upsert_wallets(wallets)
        inserted = store.insert_trades(trades)
        signals, opened, closed = run_scan(args, store)
        print(f"Demo wallets loaded: {len(wallets)}")
        print(f"Demo trades inserted: {inserted}")
        print(f"Signals created: {len(signals)}")
        print(f"Paper positions opened: {len(opened)}")
        print(f"Paper positions closed: {len(closed)}")
        _print_runtime_line(store, "exit_last_summary", "Exit")
        _print_runtime_line(store, "risk_last_summary", "Risk")
    return cmd_report(argparse.Namespace(db=args.db, limit=10))


def cmd_report(args: argparse.Namespace) -> int:
    with open_store(args.db) as store:
        store.init_schema()
        scores = list(store.fetch_scores().values())[: args.limit]
        signals = store.fetch_recent_signals(args.limit)
        positions = store.fetch_open_positions()
        closed_positions = store.fetch_recent_closed_positions(limit=args.limit)
        books = store.fetch_order_books([position.asset for position in positions])
        summary = store.paper_summary()
        journal = store.paper_event_summary(limit=5)
        risk = PaperBroker(store).risk_snapshot()

    print("\nTop wallets")
    if not scores:
        print("  No wallet scores yet.")
    for score in scores:
        print(
            f"  {score.score:.2f} {short_wallet(score.wallet)} "
            f"trades={score.trade_count} pnl={money(score.leaderboard_pnl)} "
            f"vol={money(score.leaderboard_volume)}"
        )

    print("\nRecent signals")
    if not signals:
        print("  No signals yet.")
    for signal in signals:
        print(
            f"  {signal.confidence:.2f} {market_category_label(market_category(signal))} "
            f"{signal.action} {signal.outcome or signal.asset} "
            f"at <= {signal.suggested_price:.3f}, size {money(signal.size_usdc)} "
            f"from {short_wallet(signal.wallet)}"
        )
        if signal.title:
            print(f"    {signal.title[:100]}")

    print("\nPaper portfolio")
    print(
        f"  total={int(summary['total_positions'])} open={int(summary['open_positions'])} "
        f"open_cost={money(summary['open_cost'])} realized_pnl={money(summary['realized_pnl'])}"
    )
    print(
        f"  risk={risk['status']} exposure={money(risk['open_cost'])}/{money(risk['max_total_exposure'])} "
        f"worst_stop={money(risk['worst_stop_loss'])}/{money(risk['worst_stop_limit'])} "
        f"pnl24h={money(risk['realized_pnl_24h'])}"
    )
    print(f"  decisions={journal['total_events']} journal_pnl={money(float(journal['pnl']))}")
    for position in positions[: args.limit]:
        book = books.get(position.asset)
        latest_value = book.mid if book and book.mid > 0 else None
        latest = f"{latest_value:.3f}" if latest_value is not None else "n/a"
        print(
            f"  OPEN {position.outcome or position.asset} entry={position.entry_price:.3f} "
            f"cost={money(position.size_usdc)} latest={latest}"
        )
    if closed_positions:
        print("\nRecent paper exits")
        for position in closed_positions[: args.limit]:
            print(
                f"  {position.close_reason or 'closed'} {position.outcome or position.asset} "
                f"entry={position.entry_price:.3f} exit={(position.exit_price or 0):.3f} "
                f"pnl={money(position.realized_pnl)}"
            )
    return 0


def cmd_paper_journal(args: argparse.Namespace) -> int:
    since_ts = int(time.time()) - max(1, args.since_days) * 86400
    with open_store(args.db) as store:
        store.init_schema()
        summary = store.paper_event_summary(since_ts=since_ts, limit=args.limit)

    print("\nPaper decision journal")
    print(
        f"  window={args.since_days}d decisions={summary['total_events']} "
        f"pnl={money(float(summary['pnl']))} avg_conf={float(summary['avg_confidence']):.3f}"
    )

    print("\nBy decision/reason")
    if not summary["by_reason"]:
        print("  No paper events yet.")
    for row in summary["by_reason"]:
        print(
            f"  {str(row['event_type']):<14} {str(row['reason']):<22} "
            f"events={int(row['events'])} pnl={money(float(row['pnl']))} "
            f"avg_conf={float(row['avg_confidence']):.3f}"
        )

    print("\nRecent decisions")
    recent = summary["recent"]
    if not recent:
        print("  No recent decisions.")
    for event in recent:
        subject = event.outcome or event.asset
        print(
            f"  {format_ts(event.event_at)} {event.event_type:<14} {event.reason:<22} "
            f"{short_wallet(event.wallet)} {subject[:40]} "
            f"price={event.price:.3f} size={money(event.size_usdc)} pnl={money(event.pnl)}"
        )
    return 0


def cmd_reviews(args: argparse.Namespace) -> int:
    with open_store(args.db) as store:
        store.init_schema()
        rows = store.fetch_review_queue(args.status, limit=args.limit)
    print(f"\n{args.status.title()} signal reviews")
    if not rows:
        print("  No signals.")
    for row in rows:
        print(
            f"  p{int(row.get('priority') or 50):02d} {str(row.get('cohort_status') or '-'):<9} "
            f"{row['signal_id'][:8]} {row['confidence']:.2f} "
            f"{row['action']} {row['outcome'] or row['asset']} "
            f"<= {row['suggested_price']:.3f} size={money(row['size_usdc'])}"
        )
        print(f"    {str(row['title'])[:100]}")
    return 0


def cmd_review(args: argparse.Namespace) -> int:
    with open_store(args.db) as store:
        store.init_schema()
        ok = store.set_signal_review_status(args.signal_id, args.status, args.note)
    if not ok:
        print(f"Signal not found in review queue: {args.signal_id}", file=sys.stderr)
        return 1
    print(f"Signal {args.signal_id} -> {args.status}")
    return 0


def cmd_open_approved(args: argparse.Namespace) -> int:
    with open_store(args.db) as store:
        store.init_schema()
        opened = PaperBroker(store).open_approved(limit=args.limit)
        risk_summary = store.runtime_state().get("risk_last_summary", {}).get("value", "")
    print(f"Paper positions opened from approved signals: {len(opened)}")
    if risk_summary:
        print(f"Risk: {risk_summary}")
    return 0


def cmd_live_paper(args: argparse.Namespace) -> int:
    import asyncio

    config = LivePaperConfig(
        db_path=args.db,
        poll_interval_seconds=args.poll_interval,
        price_interval_seconds=args.price_interval,
        use_stream_queue=args.use_stream_queue,
        stream_queue_interval_seconds=args.stream_queue_interval,
        stream_batch_limit=args.stream_batch_limit,
        stream_min_events=args.stream_min_events,
        manual_confirm=args.manual_confirm,
        dry_run=args.dry_run,
        close_on_stop=not args.no_close_on_stop,
        state_path=args.state_path,
        log_path=args.log_path,
        leaderboard_limit=args.leaderboard_limit,
        wallet_limit=args.wallet_limit,
        days=args.days,
        per_wallet_limit=args.per_wallet_limit,
        bankroll=args.bankroll,
        min_wallet_score=args.min_wallet_score,
        min_trade_usdc=args.min_trade_usdc,
        lookback_minutes=args.lookback_minutes,
        max_signals=args.max_signals,
        book_asset_limit=args.book_asset_limit,
        max_spread=args.max_spread,
        min_liquidity_score=args.min_liquidity_score,
        min_depth_usdc=args.min_depth_usdc,
        stop_loss_pct=args.stop_loss_pct,
        take_profit_pct=args.take_profit_pct,
        use_cohort_policy=not args.no_cohort_policy,
        use_learning_policy=not args.no_learning_policy,
    )
    try:
        asyncio.run(LivePaperRunner(config).run())
    except KeyboardInterrupt:
        return 130
    return 0


def cmd_dashboard(args: argparse.Namespace) -> int:
    serve_dashboard(args.db, host=args.host, port=args.port)
    return 0


def cmd_stream(args: argparse.Namespace) -> int:
    config = StreamConfig(
        assets=tuple(args.asset or ()),
        asset_limit=args.asset_limit,
        lookback_minutes=args.lookback_minutes,
        max_events=args.max_events,
        batch_size=args.batch_size,
        flush_interval_seconds=args.flush_interval_seconds,
        reconcile_trades=not args.no_reconcile,
        reconcile_min_notional=args.reconcile_min_notional,
        reconcile_limit=args.reconcile_limit,
        reconcile_window_seconds=args.reconcile_window_seconds,
        scan_every_events=args.scan_every_events,
        wallet_limit=args.wallet_limit,
        bankroll=args.bankroll,
        min_wallet_score=args.min_wallet_score,
        min_trade_usdc=args.min_trade_usdc,
        max_signals=args.max_signals,
    )
    try:
        with open_store(args.db) as store:
            store.init_schema()
            assets = list(config.assets) or store.recent_assets(
                limit=config.asset_limit,
                since_ts=int(time.time()) - config.lookback_minutes * 60,
            )
            print(
                f"CLOB stream starting: assets={len(assets)} "
                f"reconcile={int(config.reconcile_trades)} scan_every={config.scan_every_events}"
            )
            StreamProcessor(store, config=config).run()
    except MissingWebsocketError:
        print(missing_websocket_message(), file=sys.stderr)
        return 2
    return 0


def cmd_monitor(args: argparse.Namespace) -> int:
    config = MonitorConfig(
        interval_seconds=args.interval_seconds,
        iterations=args.iterations,
        leaderboard_limit=args.leaderboard_limit,
        discover_every=args.discover_every,
        wallet_limit=args.wallet_limit,
        days=args.days,
        per_wallet_limit=args.per_wallet_limit,
        bankroll=args.bankroll,
        min_wallet_score=args.min_wallet_score,
        min_trade_usdc=args.min_trade_usdc,
        lookback_minutes=args.lookback_minutes,
        max_signals=args.max_signals,
        live_discover=not args.no_discover,
        live_sync=not args.no_sync,
        sync_books=not args.no_books,
        book_asset_limit=args.book_asset_limit,
        max_spread=args.max_spread,
        min_liquidity_score=args.min_liquidity_score,
        min_depth_usdc=args.min_depth_usdc,
        max_book_price_deviation=args.max_book_price_deviation,
        max_wallet_trades_per_day=args.max_wallet_trades_per_day,
        min_cluster_wallets=args.min_cluster_wallets,
        min_cluster_notional=args.min_cluster_notional,
        use_learning_policy=not args.no_learning_policy,
        max_total_exposure_pct=args.max_total_exposure_pct,
        max_open_positions=args.max_open_positions,
        max_new_positions_per_run=args.max_new_positions_per_run,
        max_market_exposure_usdc=args.max_market_exposure_usdc,
        max_wallet_exposure_usdc=args.max_wallet_exposure_usdc,
        max_daily_loss_usdc=args.max_daily_loss_usdc,
        max_worst_stop_loss_usdc=args.max_worst_stop_loss_usdc,
        paper_max_hold_hours=args.paper_max_hold_hours,
        paper_stale_price_hours=args.paper_stale_price_hours,
        max_risk_trim_per_run=args.max_risk_trim_per_run,
        risk_trim_enabled=not args.no_risk_trim,
        market_flow_every=args.market_flow_every,
        market_flow_market_limit=args.market_flow_market_limit,
        market_flow_trades_per_market=args.market_flow_trades_per_market,
        market_flow_min_trade_cash=args.market_flow_min_trade_cash,
        market_flow_min_wallet_notional=args.market_flow_min_wallet_notional,
        market_flow_min_wallet_trades=args.market_flow_min_wallet_trades,
        analytics_export_every=args.analytics_export_every,
        analytics_duckdb_path=args.duckdb,
        analytics_chunk_size=args.analytics_chunk_size,
        analytics_rebuild=args.analytics_rebuild,
    )
    telegram = TelegramConfig(
        token=args.telegram_token,
        chat_id=args.telegram_chat_id,
        dry_run=args.telegram_dry_run,
    )
    with Store(args.db) as store:
        Monitor(store=store, config=config, telegram=telegram).run()
    return 0


def run_scan(args: argparse.Namespace, store: Store):
    wallets = store.fetch_wallets()
    since_score = int(time.time()) - max(1, getattr(args, "history_days", 14)) * 86400
    trades_by_wallet = {
        wallet.address: store.fetch_trades_for_wallet(wallet.address, since_score)
        for wallet in wallets
    }
    scores = score_wallets(wallets, trades_by_wallet)
    store.upsert_scores(scores)

    score_map = store.fetch_scores(min_score=args.min_wallet_score)
    since_signal = int(time.time()) - args.lookback_minutes * 60
    recent_trades = store.fetch_recent_trades(
        since_ts=since_signal,
        min_notional=args.min_trade_usdc,
        limit=2500,
    )
    order_books = store.fetch_order_books([trade.asset for trade in recent_trades])
    policy_enabled, policy_mode = _active_signal_policy(store, no_cohort=getattr(args, "no_cohort_policy", False))
    config = SignalConfig(
        bankroll=args.bankroll,
        risk_per_signal=args.risk_per_signal,
        max_position_usdc=args.max_position_usdc,
        min_wallet_score=args.min_wallet_score,
        min_trade_usdc=args.min_trade_usdc,
        lookback_minutes=args.lookback_minutes,
        max_signals=args.max_signals,
        max_spread=args.max_spread,
        min_liquidity_score=args.min_liquidity_score,
        min_depth_usdc=args.min_depth_usdc,
        max_book_price_deviation=args.max_book_price_deviation,
        max_wallet_trades_per_day=args.max_wallet_trades_per_day,
        min_cluster_wallets=args.min_cluster_wallets,
        min_cluster_notional=args.min_cluster_notional,
        use_cohort_policy=policy_enabled,
        cohort_policy_mode=policy_mode,
        use_learning_policy=not bool(getattr(args, "no_learning_policy", False)),
    )
    wallet_cohorts = {}
    if policy_enabled:
        cohort_report = wallet_cohort_report(
            store,
            CohortConfig(
                history_days=max(1, getattr(args, "history_days", 14)),
                min_notional=1,
                min_trades=1,
                limit=max(100, len(wallets)),
            ),
        )
        wallet_cohorts = {str(item["wallet"]): item for item in cohort_report["wallets"]}
    learning_enabled = not bool(getattr(args, "no_learning_policy", False))
    wallet_outcomes = {}
    if learning_enabled:
        outcome_report = wallet_outcome_report(
            store,
            since_days=max(30, getattr(args, "history_days", 14)),
            min_events=1,
            limit=50000,
        )
        wallet_outcomes = wallet_outcome_lookup(outcome_report)
    store.set_runtime_state(
        "signal_policy_active",
        f"enabled={int(policy_enabled)} mode={policy_mode} learning={int(learning_enabled)}",
    )
    signals = generate_signals(
        recent_trades,
        score_map,
        config,
        order_books=order_books,
        wallet_cohorts=wallet_cohorts,
        wallet_outcomes=wallet_outcomes,
    )
    store.insert_signals(signals)
    broker = PaperBroker(store, risk_config=_risk_config_from_args(args), exit_config=_exit_config_from_args(args))
    broker.record_signal_created(signals)
    closed = broker.mark_and_close()
    opened = broker.open_from_signals(signals)
    return signals, opened, closed


def _risk_config_from_args(args: argparse.Namespace) -> RiskConfig:
    return RiskConfig(
        bankroll=float(getattr(args, "bankroll", 200.0)),
        max_total_exposure_pct=float(getattr(args, "max_total_exposure_pct", 0.65)),
        max_position_usdc=float(getattr(args, "max_position_usdc", 25.0)),
        max_market_exposure_usdc=float(getattr(args, "max_market_exposure_usdc", 40.0)),
        max_wallet_exposure_usdc=float(getattr(args, "max_wallet_exposure_usdc", 60.0)),
        max_open_positions=int(getattr(args, "max_open_positions", 20)),
        max_new_positions_per_run=int(getattr(args, "max_new_positions_per_run", 6)),
        max_daily_realized_loss_usdc=float(getattr(args, "max_daily_loss_usdc", 20.0)),
        max_worst_stop_loss_usdc=float(getattr(args, "max_worst_stop_loss_usdc", 35.0)),
    )


def _exit_config_from_args(args: argparse.Namespace) -> ExitConfig:
    return ExitConfig(
        max_hold_hours=int(getattr(args, "paper_max_hold_hours", 36)),
        stale_price_hours=int(getattr(args, "paper_stale_price_hours", 48)),
        risk_trim_enabled=not bool(getattr(args, "no_risk_trim", False)),
        max_risk_trim_positions_per_run=int(getattr(args, "max_risk_trim_per_run", 4)),
    )


def _active_signal_policy(store: Store, *, no_cohort: bool = False) -> tuple[bool, str]:
    if no_cohort:
        return False, "strict"
    runtime = store.runtime_state()
    recommended = runtime.get("policy_optimizer_recommended")
    if not recommended:
        return True, "strict"
    return policy_settings_from_recommendation(str(recommended.get("value") or ""), default_enabled=True)


def load_requested_wallets(args: argparse.Namespace, store: Store) -> list[Wallet]:
    wallets: list[Wallet] = []
    for address in args.wallet:
        wallets.append(Wallet(address=address.lower(), source="manual"))
    if args.wallet_file:
        for address in read_wallet_file(Path(args.wallet_file)):
            wallets.append(Wallet(address=address.lower(), source="file"))
    if not wallets:
        wallets = store.fetch_wallets(limit=args.wallet_limit)
    if wallets:
        store.upsert_wallets(wallets)
    seen: set[str] = set()
    unique: list[Wallet] = []
    for wallet in wallets:
        if wallet.address and wallet.address not in seen:
            unique.append(wallet)
            seen.add(wallet.address)
    return unique


def read_wallet_file(path: Path) -> list[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    return [
        line.strip()
        for line in lines
        if line.strip() and not line.lstrip().startswith("#")
    ]


def chunks(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


class open_store:
    def __init__(self, path: str | Path) -> None:
        self.store = Store(path)

    def __enter__(self) -> Store:
        return self.store

    def __exit__(self, exc_type, exc, tb) -> None:
        self.store.close()


def short_wallet(wallet: str) -> str:
    if len(wallet) <= 12:
        return wallet
    return f"{wallet[:6]}...{wallet[-4:]}"


def money(value: float) -> str:
    return f"${value:,.2f}"


def format_ts(value: int) -> str:
    return time.strftime("%Y-%m-%d %H:%M", time.localtime(value))


def _print_runtime_line(store: Store, key: str, label: str) -> None:
    value = store.runtime_state().get(key, {}).get("value", "")
    if value:
        print(f"{label}: {value}")


if __name__ == "__main__":
    raise SystemExit(main())

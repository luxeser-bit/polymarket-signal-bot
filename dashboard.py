from __future__ import annotations

import datetime as dt
import os
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

try:
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover - handled at runtime.
    pd = None  # type: ignore[assignment]

try:
    import streamlit as st
except ModuleNotFoundError:  # pragma: no cover - handled at runtime.
    st = None  # type: ignore[assignment]

try:
    import plotly.express as px
except ModuleNotFoundError:  # pragma: no cover - optional dependency.
    px = None  # type: ignore[assignment]

from polymarket_signal_bot.bulk_sync import TARGET_TRADES, TARGET_WALLETS
from polymarket_signal_bot.demo import demo_trades, demo_wallets
from polymarket_signal_bot.monitor import Monitor, MonitorConfig
from polymarket_signal_bot.storage import DEFAULT_DB_PATH, Store


ROOT = Path(__file__).resolve().parent
DEFAULT_MAIN_DB = ROOT / DEFAULT_DB_PATH
DEFAULT_STATE_DB = ROOT / "data" / "paper_state.db"
DEFAULT_LOG_PATH = ROOT / "data" / "streamlit_live_paper.log"


def main() -> None:
    _require_dashboard_dependencies()
    st.set_page_config(page_title="PolySignal Paper Desk", layout="wide", page_icon="◈")
    _inject_style()
    _init_session_state()

    st.title("POLYSIGNAL // PAPER DESK")
    st.caption("Бирюзовый Streamlit-дэшборд для данных, сканирования и live paper simulation.")

    with st.sidebar:
        st.header("Control")
        main_db = _resolve_path(
            st.text_input("Main SQLite DB", value=str(DEFAULT_MAIN_DB.relative_to(ROOT)))
        )
        state_db = _resolve_path(
            st.text_input("Paper state DB", value=str(DEFAULT_STATE_DB.relative_to(ROOT)))
        )
        dry_run = st.toggle("Dry run", value=True)
        use_stream_queue = st.toggle("Use stream queue", value=False)
        use_websocket = st.toggle("Use WebSocket", value=False)
        poll_interval = st.number_input("Poll interval, sec", min_value=5, max_value=600, value=60, step=5)
        price_interval = st.number_input("Price interval, sec", min_value=2, max_value=300, value=15, step=1)

    tabs = st.tabs(["Demo / Overview", "Scan", "Paper Trade"])
    with tabs[0]:
        _render_overview(main_db, state_db)
    with tabs[1]:
        _render_scan(main_db)
    with tabs[2]:
        _render_paper_trade(
            main_db=main_db,
            state_db=state_db,
            dry_run=dry_run,
            use_stream_queue=use_stream_queue,
            use_websocket=use_websocket,
            poll_interval=int(poll_interval),
            price_interval=int(price_interval),
        )


def _render_overview(main_db: Path, state_db: Path) -> None:
    st.subheader("Demo / Overview")
    if st.button("Demo", use_container_width=True):
        try:
            result = _load_demo(main_db)
            st.success(
                f"Demo loaded: wallets={result['wallets']} trades={result['trades']} "
                f"signals={result['signals']} opened={result['opened']} closed={result['closed']}"
            )
        except Exception as exc:  # noqa: BLE001 - UI should show the failure.
            st.error(f"Demo failed: {exc}")

    stats = _main_db_stats(main_db)
    cohorts = _cohort_counts(main_db)
    latest_balance = _latest_balance(state_db)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Whales / wallets", f"{stats['wallets']:,}", f"{stats['wallets'] / TARGET_WALLETS:.2%} of 14K")
    col2.metric("Trades", f"{stats['trades']:,}", f"{stats['trades'] / TARGET_TRADES:.4%} of 86M")
    col3.metric("Cohort-ready", f"{cohorts['tracked']:,}", f"STABLE {cohorts['stable']:,}")
    col4.metric("Last balance", _money(latest_balance["balance"]), _money(latest_balance["pnl"]))

    st.progress(min(1.0, stats["trades"] / TARGET_TRADES), text="SCALE TARGET // 86M TRADES")
    st.progress(min(1.0, stats["wallets"] / TARGET_WALLETS), text="WATCHLIST // 14K WALLETS")

    left, right = st.columns([1.1, 1])
    with left:
        st.markdown("#### Cohorts")
        cohort_df = pd.DataFrame(
            [
                {"cohort": "STABLE", "wallets": cohorts["stable"]},
                {"cohort": "CANDIDATE", "wallets": cohorts["candidate"]},
                {"cohort": "WATCH", "wallets": cohorts["watch"]},
                {"cohort": "NOISE", "wallets": cohorts["noise"]},
            ]
        )
        if px:
            fig = px.bar(cohort_df, x="cohort", y="wallets", color="cohort", template="plotly_dark")
            fig.update_layout(height=320, paper_bgcolor="#061416", plot_bgcolor="#061416", showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.dataframe(cohort_df, use_container_width=True, hide_index=True)
    with right:
        st.markdown("#### Balance History")
        history = _balance_history(state_db, limit=200)
        if history.empty:
            st.info("paper_state.db пока не создан. Запусти paper trading, и тут появится баланс.")
        elif px:
            history["time"] = history["timestamp"].map(_format_ts)
            fig = px.line(history, x="time", y="balance", template="plotly_dark")
            fig.update_traces(line_color="#16f4f4", line_width=3)
            fig.update_layout(height=320, paper_bgcolor="#061416", plot_bgcolor="#061416")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.dataframe(history.tail(20), use_container_width=True, hide_index=True)


def _render_scan(main_db: Path) -> None:
    st.subheader("Manual Scan")
    wallet_limit = st.slider("Wallet limit", min_value=10, max_value=1000, value=100, step=10)
    min_wallet_score = st.slider("Min wallet score", min_value=0.0, max_value=1.0, value=0.55, step=0.01)
    min_trade_usdc = st.slider("Min trade USDC", min_value=1.0, max_value=500.0, value=50.0, step=5.0)
    max_signals = st.slider("Max signals", min_value=1, max_value=100, value=20, step=1)

    if st.button("Scan", use_container_width=True):
        try:
            with st.spinner("Scanning local wallet/trade data..."):
                result = _run_manual_scan(
                    main_db,
                    wallet_limit=wallet_limit,
                    min_wallet_score=min_wallet_score,
                    min_trade_usdc=min_trade_usdc,
                    max_signals=max_signals,
                )
            st.success(
                f"Scan complete: signals={result['signals']} opened={result['opened']} closed={result['closed']}"
            )
        except Exception as exc:  # noqa: BLE001 - UI should show the failure.
            st.error(f"Scan failed: {exc}")

    recent = _recent_signals(main_db)
    if recent.empty:
        st.info("No signals yet.")
    else:
        st.dataframe(recent, use_container_width=True, hide_index=True)


def _render_paper_trade(
    *,
    main_db: Path,
    state_db: Path,
    dry_run: bool,
    use_stream_queue: bool,
    use_websocket: bool,
    poll_interval: int,
    price_interval: int,
) -> None:
    st.subheader("Paper Trade")
    running = _paper_process_running()
    control_label = "⏹ Stop Paper Trading" if running else "▶ Start Paper Trading"

    if st.button(control_label, type="primary", use_container_width=True):
        if running:
            _stop_paper_process()
            st.rerun()
        else:
            try:
                _start_paper_process(
                    main_db=main_db,
                    state_db=state_db,
                    dry_run=dry_run,
                    use_stream_queue=use_stream_queue,
                    use_websocket=use_websocket,
                    poll_interval=poll_interval,
                    price_interval=price_interval,
                )
                st.rerun()
            except FileNotFoundError as exc:
                st.error(str(exc))
            except Exception as exc:  # noqa: BLE001 - UI should show the failure.
                st.error(f"Paper trading failed to start: {exc}")

    if not _paper_process_running():
        _render_live_metrics(state_db, running=False)
        return

    placeholder = st.empty()
    while _paper_process_running():
        with placeholder.container():
            _render_live_metrics(state_db, running=True)
        time.sleep(2)
        st.rerun()


def _render_live_metrics(state_db: Path, *, running: bool) -> None:
    latest = _latest_balance(state_db)
    daily_pnl = _daily_pnl(state_db)
    uptime = _uptime_label()
    positions = _open_state_positions(state_db)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Status", "RUNNING" if running else "STOPPED")
    col2.metric("Balance", _money(latest["balance"]))
    col3.metric("Daily P&L", _money(daily_pnl))
    col4.metric("Uptime", uptime)

    st.markdown("#### Open Positions")
    if positions.empty:
        st.info("No open paper positions.")
    else:
        st.dataframe(positions, use_container_width=True, hide_index=True)


def _start_paper_process(
    *,
    main_db: Path,
    state_db: Path,
    dry_run: bool,
    use_stream_queue: bool,
    use_websocket: bool,
    poll_interval: int,
    price_interval: int,
) -> None:
    runner_path = ROOT / "polymarket_signal_bot" / "live_paper_runner.py"
    if not runner_path.exists():
        raise FileNotFoundError(f"live_paper_runner.py not found: {runner_path}")

    command = [
        sys.executable,
        "-m",
        "polymarket_signal_bot.live_paper_runner",
        "--db",
        str(main_db),
        "--state-db",
        str(state_db),
        "--log-path",
        str(DEFAULT_LOG_PATH),
        "--poll-interval",
        str(poll_interval),
        "--price-interval",
        str(price_interval),
    ]
    if dry_run:
        command.append("--dry-run")
    if use_stream_queue:
        command.append("--use-stream-queue")
    if use_websocket:
        command.append("--use-websocket")

    env = os.environ.copy()
    env.setdefault("PYTHONUTF8", "1")
    process = subprocess.Popen(
        command,
        cwd=ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    st.session_state.paper_process = process
    st.session_state.paper_pid = process.pid
    st.session_state.paper_started_at = time.time()
    st.session_state.paper_command = " ".join(command)


def _stop_paper_process() -> None:
    process = st.session_state.get("paper_process")
    if process and process.poll() is None:
        try:
            process.send_signal(signal.SIGTERM)
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)
    st.session_state.paper_process = None
    st.session_state.paper_pid = None
    st.session_state.paper_started_at = None


def _paper_process_running() -> bool:
    process = st.session_state.get("paper_process")
    if not process:
        return False
    code = process.poll()
    if code is None:
        return True
    st.session_state.paper_process = None
    st.session_state.paper_pid = None
    st.session_state.paper_started_at = None
    if code != 0:
        st.error(f"Paper trading process stopped with exit code {code}. Check {DEFAULT_LOG_PATH}.")
    return False


def _load_demo(db_path: Path) -> dict[str, int]:
    with Store(db_path) as store:
        store.init_schema()
        wallets = demo_wallets()
        trades = demo_trades()
        wallets_inserted = store.upsert_wallets(wallets)
        trades_inserted = store.insert_trades(trades)
        signals, opened, closed = Monitor(
            store=store,
            config=MonitorConfig(live_discover=False, live_sync=False, sync_books=False, wallet_limit=50),
        ).scan()
    return {
        "wallets": wallets_inserted,
        "trades": trades_inserted,
        "signals": signals,
        "opened": opened,
        "closed": closed,
    }


def _run_manual_scan(
    db_path: Path,
    *,
    wallet_limit: int,
    min_wallet_score: float,
    min_trade_usdc: float,
    max_signals: int,
) -> dict[str, int]:
    with Store(db_path) as store:
        store.init_schema()
        monitor = Monitor(
            store=store,
            config=MonitorConfig(
                live_discover=False,
                live_sync=False,
                sync_books=False,
                wallet_limit=wallet_limit,
                min_wallet_score=min_wallet_score,
                min_trade_usdc=min_trade_usdc,
                max_signals=max_signals,
            ),
        )
        scan_signals = getattr(monitor, "scan_signals", None)
        if callable(scan_signals):
            result = scan_signals()
            if isinstance(result, dict):
                return {
                    "signals": int(result.get("signals", 0)),
                    "opened": int(result.get("opened", 0)),
                    "closed": int(result.get("closed", 0)),
                }
        signals, opened, closed = monitor.scan()
    return {"signals": signals, "opened": opened, "closed": closed}


def _main_db_stats(db_path: Path) -> dict[str, int]:
    return {
        "wallets": _count_rows(db_path, "wallets"),
        "trades": _count_rows(db_path, "trades"),
        "signals": _count_rows(db_path, "signals"),
    }


def _cohort_counts(db_path: Path) -> dict[str, int]:
    df = _read_sql_df(
        db_path,
        """
        SELECT
            COALESCE(SUM(CASE WHEN score >= 0.70 THEN 1 ELSE 0 END), 0) AS stable,
            COALESCE(SUM(CASE WHEN score >= 0.55 AND score < 0.70 THEN 1 ELSE 0 END), 0) AS candidate,
            COALESCE(SUM(CASE WHEN score >= 0.35 AND score < 0.55 THEN 1 ELSE 0 END), 0) AS watch,
            COALESCE(SUM(CASE WHEN score < 0.35 THEN 1 ELSE 0 END), 0) AS noise,
            COUNT(*) AS tracked
        FROM wallet_scores
        """,
    )
    if df.empty:
        return {"stable": 0, "candidate": 0, "watch": 0, "noise": 0, "tracked": 0}
    row = df.iloc[0]
    return {key: int(row[key] or 0) for key in ["stable", "candidate", "watch", "noise", "tracked"]}


def _latest_balance(state_db: Path) -> dict[str, float]:
    df = _read_sql_df(
        state_db,
        "SELECT timestamp, balance, pnl FROM balance_history ORDER BY timestamp DESC LIMIT 1",
    )
    if df.empty:
        return {"balance": 0.0, "pnl": 0.0}
    row = df.iloc[0]
    return {"balance": float(row["balance"] or 0.0), "pnl": float(row["pnl"] or 0.0)}


def _daily_pnl(state_db: Path) -> float:
    start = int(dt.datetime.combine(dt.date.today(), dt.time.min).timestamp())
    df = _read_sql_df(
        state_db,
        """
        SELECT timestamp, pnl
        FROM balance_history
        WHERE timestamp >= ?
        ORDER BY timestamp ASC
        """,
        (start,),
    )
    if df.empty:
        return _latest_balance(state_db)["pnl"]
    return float(df.iloc[-1]["pnl"] or 0.0) - float(df.iloc[0]["pnl"] or 0.0)


def _balance_history(state_db: Path, *, limit: int = 200) -> Any:
    return _read_sql_df(
        state_db,
        """
        SELECT timestamp, balance, pnl
        FROM balance_history
        ORDER BY timestamp DESC
        LIMIT ?
        """,
        (limit,),
    ).sort_values("timestamp")


def _open_state_positions(state_db: Path) -> Any:
    df = _read_sql_df(
        state_db,
        """
        SELECT id, market_id, side, size, entry_price, tp_pct, sl_pct, status, opened_at, pnl
        FROM positions
        WHERE LOWER(status) = 'open'
        ORDER BY opened_at DESC
        """,
    )
    if df.empty:
        return df
    df["opened_at"] = df["opened_at"].map(_format_ts)
    for col in ["size", "entry_price", "tp_pct", "sl_pct", "pnl"]:
        df[col] = df[col].astype(float).round(4)
    return df


def _recent_signals(db_path: Path) -> Any:
    df = _read_sql_df(
        db_path,
        """
        SELECT generated_at, action, wallet, asset, outcome, title, suggested_price, size_usdc, confidence, reason
        FROM signals
        ORDER BY generated_at DESC
        LIMIT 25
        """,
    )
    if df.empty:
        return df
    df["generated_at"] = df["generated_at"].map(_format_ts)
    df["wallet"] = df["wallet"].map(_short_wallet)
    return df


def _count_rows(db_path: Path, table: str) -> int:
    if table not in {"wallets", "trades", "signals"}:
        return 0
    df = _read_sql_df(db_path, f"SELECT COUNT(*) AS count FROM {table}")
    if df.empty:
        return 0
    return int(df.iloc[0]["count"] or 0)


def _read_sql_df(db_path: Path, query: str, params: tuple[Any, ...] = ()) -> Any:
    if pd is None:
        raise RuntimeError("pandas is required. Install: python -m pip install -r requirements-streamlit.txt")
    if not db_path.exists():
        return pd.DataFrame()
    uri = f"{db_path.resolve().as_uri()}?mode=ro"
    try:
        with sqlite3.connect(uri, uri=True) as conn:
            conn.row_factory = sqlite3.Row
            return pd.read_sql_query(query, conn, params=params)
    except Exception:  # noqa: BLE001 - missing tables should not break the dashboard.
        return pd.DataFrame()


def _resolve_path(value: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = ROOT / path
    return path


def _format_ts(value: Any) -> str:
    try:
        timestamp = int(float(value or 0))
    except (TypeError, ValueError):
        timestamp = 0
    if timestamp <= 0:
        return "-"
    return dt.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")


def _short_wallet(value: str) -> str:
    value = str(value or "")
    if len(value) <= 12:
        return value
    return f"{value[:6]}...{value[-4:]}"


def _money(value: float) -> str:
    sign = "-" if value < 0 else ""
    return f"{sign}${abs(float(value)):,.2f}"


def _uptime_label() -> str:
    started = st.session_state.get("paper_started_at")
    if not started:
        return "00:00:00"
    seconds = max(0, int(time.time() - float(started)))
    return str(dt.timedelta(seconds=seconds))


def _init_session_state() -> None:
    st.session_state.setdefault("paper_process", None)
    st.session_state.setdefault("paper_pid", None)
    st.session_state.setdefault("paper_started_at", None)
    st.session_state.setdefault("paper_command", "")


def _require_dashboard_dependencies() -> None:
    missing = []
    if st is None:
        missing.append("streamlit")
    if pd is None:
        missing.append("pandas")
    if missing:
        raise RuntimeError(
            "Missing Streamlit dashboard dependencies: "
            + ", ".join(missing)
            + ". Install: python -m pip install -r requirements-streamlit.txt"
        )


def _inject_style() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background: radial-gradient(circle at top left, rgba(22,244,244,.13), transparent 34%),
                        linear-gradient(180deg, #031012 0%, #061719 100%);
            color: #d9feff;
        }
        h1, h2, h3, h4, [data-testid="stMetricLabel"] {
            color: #bffeff !important;
            letter-spacing: 0;
        }
        [data-testid="stMetric"] {
            background: rgba(6, 36, 39, .88);
            border: 1px solid rgba(22, 244, 244, .35);
            border-left: 4px solid #16f4f4;
            border-radius: 8px;
            padding: 14px 16px;
            box-shadow: 0 0 22px rgba(22, 244, 244, .08);
        }
        .stButton > button {
            background: linear-gradient(90deg, #0ccac9, #22f7e9);
            color: #001416;
            border: 0;
            border-radius: 7px;
            font-weight: 800;
        }
        .stButton > button:hover {
            color: #001416;
            border: 0;
            box-shadow: 0 0 18px rgba(34, 247, 233, .35);
        }
        [data-testid="stDataFrame"] {
            border: 1px solid rgba(22, 244, 244, .18);
            border-radius: 8px;
        }
        section[data-testid="stSidebar"] {
            background: #041315;
            border-right: 1px solid rgba(22, 244, 244, .2);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()

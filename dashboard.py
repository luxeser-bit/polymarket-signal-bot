from __future__ import annotations

import datetime as dt
import html
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
except ModuleNotFoundError:  # pragma: no cover - shown in UI startup error.
    pd = None  # type: ignore[assignment]

try:
    import streamlit as st
except ModuleNotFoundError:  # pragma: no cover - shown in UI startup error.
    st = None  # type: ignore[assignment]

from polymarket_signal_bot.bulk_sync import TARGET_TRADES, TARGET_WALLETS
from polymarket_signal_bot.dashboard import build_dashboard_state
from polymarket_signal_bot.demo import demo_trades, demo_wallets
from polymarket_signal_bot.monitor import Monitor, MonitorConfig
from polymarket_signal_bot.storage import DEFAULT_DB_PATH, Store


ROOT = Path(__file__).resolve().parent
DEFAULT_MAIN_DB = ROOT / DEFAULT_DB_PATH
DEFAULT_STATE_DB = ROOT / "data" / "paper_state.db"
DEFAULT_LOG_PATH = ROOT / "data" / "streamlit_live_paper.log"
DEFAULT_BANKROLL = 200.0


def main() -> None:
    _require_dashboard_dependencies()
    st.set_page_config(page_title="PolySignal 200 Seed", layout="wide", page_icon="*")
    _inject_style()
    _init_session_state()

    main_db = DEFAULT_MAIN_DB
    state_db = DEFAULT_STATE_DB
    state = _terminal_state(main_db, state_db)

    _render_terminal_header(state)
    _render_terminal_actions(main_db, state_db)
    state = _terminal_state(main_db, state_db)
    _render_terminal_shell(state)

    if _paper_process_running():
        time.sleep(2)
        st.rerun()


def _render_terminal_header(state: dict[str, Any]) -> None:
    stats = state["stats"]
    meta = state["meta"]
    trades_pct = min(96.0, max(8.0, _float(stats.get("trades")) / TARGET_TRADES * 100))
    runway = f"DAY {max(1, int(_float(stats.get('trades')) // 25) + 1)}"
    monitor = str(meta.get("monitorStatus") or "offline").upper()
    live_status = "RUNNING" if state["paper"]["running"] else str(meta.get("status") or "STANDBY").upper()
    st.markdown(
        f"""
        <div class="terminal-shell top-only">
          <header class="topbar">
            <div class="brand"><span class="led"></span><span>POLYMARKET 200 SEED</span></div>
            <div class="topline">
              <span>ENGINE poly_signal</span>
              <span>MONITOR <b>{_e(monitor)}</b></span>
              <span>WALLETS <b>{_intfmt(stats.get("wallets"))}</b></span>
              <span>TRADES <b>{_intfmt(stats.get("trades"))}</b></span>
              <span class="live">LIVE</span>
              <span>PAPER ONLY</span>
            </div>
          </header>
          <section class="cyclebar">
            <div><span>RUNWAY</span> <b>{_e(runway)}</b></div>
            <div class="progress-track"><span style="width:{trades_pct:.2f}%"></span></div>
            <div><b>{_e(live_status)}</b></div>
            <div class="mode-chip">STREAMLIT CONTROL ROOM</div>
          </section>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_terminal_actions(main_db: Path, state_db: Path) -> None:
    cols = st.columns([1, 1, 1, 5])
    with cols[0]:
        if st.button("DEMO", width="stretch"):
            try:
                result = _load_demo(main_db)
                st.toast(
                    f"Demo loaded: trades={result['trades']} signals={result['signals']} opened={result['opened']}"
                )
                st.rerun()
            except Exception as exc:  # noqa: BLE001 - show action errors.
                st.error(f"Demo failed: {exc}")
    with cols[1]:
        if st.button("SCAN", width="stretch"):
            try:
                result = _run_manual_scan(main_db)
                st.toast(
                    f"Scan complete: signals={result['signals']} opened={result['opened']} closed={result['closed']}"
                )
                st.rerun()
            except Exception as exc:  # noqa: BLE001 - show action errors.
                st.error(f"Scan failed: {exc}")
    with cols[2]:
        label = "STOP PAPER" if _paper_process_running() else "START PAPER"
        if st.button(label, width="stretch", type="primary"):
            if _paper_process_running():
                _stop_paper_process()
            else:
                try:
                    _start_paper_process(main_db=main_db, state_db=state_db)
                except Exception as exc:  # noqa: BLE001 - show action errors.
                    st.error(f"Paper trading failed to start: {exc}")
                    return
            st.rerun()
    with cols[3]:
        st.markdown(
            "<div class='action-note'>Demo / Overview, Scan and Paper Trade are sections on this same terminal screen.</div>",
            unsafe_allow_html=True,
        )


def _render_terminal_shell(state: dict[str, Any]) -> None:
    stats = state["stats"]
    paper = state["paper"]
    positions = paper["statePositions"] or state.get("positions", [])
    metrics = [
        ("BANKROLL", _money(stats.get("bankroll"))),
        ("WALLETS", _intfmt(stats.get("wallets"))),
        ("WIN RATE", _pct(stats.get("winRate"))),
        ("OPEN POS", _intfmt(len(positions) or stats.get("openPositions"))),
        ("OPEN COST", _money(stats.get("openCost"))),
        ("UNREAL", _money(stats.get("unrealizedPnl"))),
        ("SIGNALS", _intfmt(stats.get("signals"))),
        ("DAY PNL", _money(paper["dailyPnl"])),
        ("UPTIME", paper["uptime"]),
        ("STREAM", _intfmt(stats.get("streamEvents"))),
    ]
    html_payload = f"""
    <div class="terminal-shell">
      <section class="metrics">{''.join(_metric(label, value) for label, value in metrics)}</section>
      <main class="grid">
        {_panel("scanner", "MARKET SCANNER // TOP FLOW", _scanner_rows(state.get("scanner", [])))}
        {_panel("consensus", "AGENT CONSENSUS // COPY CANDIDATES", _consensus_rows(state.get("consensus", [])))}
        {_panel("positions", "ACTIVE POSITIONS", _position_rows(positions))}
        {_curve_panel(stats, state.get("bankrollCurve", []))}
        {_panel("depth", "ORDER BOOK DEPTH // CLOB", _depth_rows(state.get("orderBook", [])))}
        {_panel("risk", "RISK MONITOR", _risk_rows(state.get("risk", [])))}
        {_panel("votes", "TRADE LOG", _trade_rows(state.get("tradeLog", [])))}
        {_panel("exits", "EXIT TRIGGERS", _exit_rows(state.get("exitTriggers", [])))}
        {_panel("whales", "WHALE TRACKER // SCORED WALLETS", _whale_rows(state.get("whales", [])))}
        {_panel("alerts", "ALERT ROUTER // TELEGRAM", _alert_rows(state.get("alerts", [])))}
        {_panel("reviews", "MANUAL APPROVAL QUEUE", _review_rows(state.get("reviews", [])))}
        {_panel("scale", "SCALE TARGET // 86M TRADES / 14K WALLETS", _scale_rows(state))}
      </main>
      <footer class="statusbar">
        <span>{_e(state["meta"].get("monitorSummary") or state["meta"].get("livePaperSummary") or "READY")}</span>
        <span>{dt.datetime.now().strftime("%H:%M:%S")}</span>
      </footer>
    </div>
    """
    st.markdown(html_payload, unsafe_allow_html=True)


def _terminal_state(main_db: Path, state_db: Path) -> dict[str, Any]:
    with Store(main_db) as store:
        store.init_schema()
        state = build_dashboard_state(store, bankroll=DEFAULT_BANKROLL)
    latest = _latest_balance(state_db)
    if latest["balance"] > 0:
        state["stats"]["bankroll"] = round(latest["balance"], 2)
    state["paper"] = {
        "running": _paper_process_running(),
        "dailyPnl": _daily_pnl(state_db),
        "uptime": _uptime_label(),
        "statePositions": _state_positions_for_panel(state_db),
    }
    return state


def _metric(label: str, value: str) -> str:
    return f"<div class='metric'><strong>{_e(value)}</strong><span>{_e(label)}</span></div>"


def _panel(panel_class: str, title: str, body: str) -> str:
    return (
        f"<section class='panel {panel_class}'>"
        f"<div class='panel-title'>{_e(title)}</div>"
        f"<div class='feed'>{body or '<div class=\"empty\">NO DATA</div>'}</div>"
        "</section>"
    )


def _scanner_rows(rows: list[dict[str, Any]]) -> str:
    return "".join(
        (
            "<div class='row'>"
            f"<span class='tag {_tag_class(row.get('status'))}'>{_e(row.get('status'))}</span>"
            f"<span class='market'>{_e(row.get('category'))} {_e(row.get('market'))}</span>"
            f"<span class='value'>{_money(row.get('notional'))} @ {_price(row.get('price'))}</span>"
            "</div>"
        )
        for row in rows[:12]
    )


def _consensus_rows(rows: list[dict[str, Any]]) -> str:
    return "".join(
        (
            "<div class='row'>"
            f"<span class='tag'>{_e(row.get('action'))}</span>"
            f"<span class='market'>{_e(row.get('category'))} {_e(row.get('outcome'))} {_e(row.get('market'))}</span>"
            f"<span class='value'>{_pct(row.get('confidence'), decimals=0)}</span>"
            "</div>"
        )
        for row in rows[:12]
    )


def _position_rows(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""
    html_rows = []
    for row in rows[:9]:
        progress = max(1, min(100, int(_float(row.get("progress")) * 100)))
        pnl = row.get("unrealizedPnl", row.get("pnl", 0))
        pct = row.get("unrealizedPct", 0)
        html_rows.append(
            "<div class='row'>"
            f"<span class='market'>{_e(row.get('category', 'PAPER'))} {_e(row.get('outcome'))} / {_e(row.get('market'))}</span>"
            f"<span class='value'>{_money(pnl)}</span>"
            f"<span class='muted'>{_pct(pct)}</span>"
            f"<div class='posbar'><span style='width:{progress}%'></span></div>"
            "</div>"
        )
    return "".join(html_rows)


def _curve_panel(stats: dict[str, Any], points: list[float]) -> str:
    return (
        "<section class='panel curve'>"
        "<div class='panel-title'>BANKROLL CURVE // PAPER</div>"
        f"<div class='bankroll-big'>{_money(stats.get('bankroll'))}</div>"
        f"{_curve_svg(points)}"
        "</section>"
    )


def _curve_svg(points: list[float]) -> str:
    values = [float(value) for value in points if value is not None] or [DEFAULT_BANKROLL]
    width, height, pad = 680, 260, 26
    min_v, max_v = min(values), max(values)
    span = max(1.0, max_v - min_v)
    coords = []
    for index, value in enumerate(values):
        x = (index / max(1, len(values) - 1)) * width
        y = height - pad - ((value - min_v) / span) * (height - pad * 2)
        coords.append((round(x, 2), round(y, 2)))
    line = " ".join(f"{x},{y}" for x, y in coords)
    area = f"0,{height} {line} {width},{height}"
    grid = "".join(f"<line x1='0' y1='{y}' x2='{width}' y2='{y}' />" for y in range(40, height, 42))
    return (
        "<svg class='curve-svg' viewBox='0 0 680 260' preserveAspectRatio='none'>"
        f"<g class='grid-lines'>{grid}</g>"
        f"<polygon class='curve-area' points='{area}' />"
        f"<polyline class='curve-line' points='{line}' />"
        "</svg>"
    )


def _depth_rows(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""
    out = []
    for row in rows[:8]:
        bid = max(1, min(100, int(_float(row.get("bidDepth")) * 100)))
        ask = max(1, min(100, int(_float(row.get("askDepth")) * 100)))
        out.append(
            "<div class='depth-row'>"
            f"<span class='value'>{_price(row.get('price'))}</span>"
            f"<span class='bookbar bid' style='width:{bid}%'></span>"
            f"<span class='bookbar ask' style='width:{ask}%'></span>"
            "</div>"
        )
    return "".join(out)


def _risk_rows(rows: list[dict[str, Any]]) -> str:
    return "".join(
        (
            "<div class='row'>"
            f"<span class='tag {_tag_class(row.get('state'))}'>{_e(row.get('state'))}</span>"
            f"<span class='market'>{_e(row.get('label'))}</span>"
            f"<span class='value'>{_e(row.get('value'))}</span>"
            "</div>"
        )
        for row in rows[:8]
    )


def _trade_rows(rows: list[dict[str, Any]]) -> str:
    return "".join(
        (
            "<div class='row'>"
            f"<span class='tag {_tag_class(row.get('side'))}'>{_e(row.get('side'))}</span>"
            f"<span class='market'>{_e(row.get('market'))}</span>"
            f"<span class='value'>{_money(row.get('notional'))}</span>"
            "</div>"
        )
        for row in rows[:10]
    )


def _exit_rows(rows: list[dict[str, Any]]) -> str:
    return "".join(
        (
            "<div class='row'>"
            f"<span class='tag {_tag_class(row.get('state'))}'>{_e(row.get('state'))}</span>"
            f"<span class='market'>{_e(row.get('rule'))} {_e(row.get('market'))}</span>"
            f"<span class='value'>{_price(row.get('price'))}</span>"
            "</div>"
        )
        for row in rows[:8]
    )


def _whale_rows(rows: list[dict[str, Any]]) -> str:
    return "".join(
        (
            "<div class='row'>"
            f"<span class='tag'>{_e(row.get('wallet'))}</span>"
            f"<span class='market'>stable {_num(row.get('stability'))} score {_num(row.get('score'))} rep {_num(row.get('repeat'))}</span>"
            f"<span class='value'>{_e(row.get('state'))}</span>"
            "</div>"
        )
        for row in rows[:9]
    )


def _alert_rows(rows: list[dict[str, Any]]) -> str:
    return "".join(
        (
            "<div class='row'>"
            f"<span class='tag {_tag_class(row.get('status'))}'>{_e(row.get('status'))}</span>"
            f"<span class='market'>{_e(row.get('destination'))} {_e(row.get('message'))}</span>"
            f"<span class='value'>{'ERR' if row.get('error') else 'OK'}</span>"
            "</div>"
        )
        for row in rows[:8]
    )


def _review_rows(rows: list[dict[str, Any]]) -> str:
    return "".join(
        (
            "<div class='row'>"
            f"<span class='tag warn'>P{int(_float(row.get('priority'), 50)):02d}</span>"
            f"<span class='market'>{_e(row.get('action'))} {_e(row.get('outcome'))} {_e(row.get('market'))}</span>"
            f"<span class='value'>{_e(row.get('cohort') or row.get('status'))} {_pct(row.get('confidence'), decimals=0)}</span>"
            "</div>"
        )
        for row in rows[:8]
    )


def _scale_rows(state: dict[str, Any]) -> str:
    stats = state["stats"]
    meta = state["meta"]
    rows = [
        ("TRADES", f"{_intfmt(stats.get('trades'))} local / {_intfmt(TARGET_TRADES)}", _float(stats.get("trades")) / TARGET_TRADES),
        ("WATCHLIST", f"{_intfmt(stats.get('wallets'))} / {_intfmt(TARGET_WALLETS)}", _float(stats.get("wallets")) / TARGET_WALLETS),
        ("LIVE PAPER", meta.get("livePaperMetrics") or meta.get("livePaperSummary") or meta.get("livePaperStatus"), 1.0 if state["paper"]["running"] else 0.0),
        ("POLICY", meta.get("signalPolicyActive") or meta.get("policySummary") or meta.get("policyRecommended"), 0.8),
        ("STREAM", meta.get("streamLastSummary") or meta.get("streamSummary") or "not listening", min(1.0, _float(stats.get("streamEvents")) / 1000)),
        ("FEATURES", meta.get("featuresSummary") or "not built yet", min(1.0, _float(stats.get("decisionFeatures")) / 1000)),
    ]
    out = []
    for status, label, pct_value in rows:
        pct_value = max(0.0, min(1.0, pct_value))
        out.append(
            "<div class='row'>"
            f"<span class='tag'>{_e(status)}</span>"
            f"<span class='market'>{_e(label)}</span>"
            f"<span class='value'>{pct_value * 100:.4f}%</span>"
            f"<div class='posbar'><span style='width:{max(1, int(pct_value * 100))}%'></span></div>"
            "</div>"
        )
    return "".join(out)


def _load_demo(db_path: Path) -> dict[str, int]:
    with Store(db_path) as store:
        store.init_schema()
        wallets_inserted = store.upsert_wallets(demo_wallets())
        trades_inserted = store.insert_trades(demo_trades())
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


def _run_manual_scan(db_path: Path) -> dict[str, int]:
    with Store(db_path) as store:
        store.init_schema()
        monitor = Monitor(
            store=store,
            config=MonitorConfig(
                live_discover=False,
                live_sync=False,
                sync_books=False,
                wallet_limit=100,
                min_wallet_score=0.55,
                min_trade_usdc=50,
                max_signals=20,
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


def _start_paper_process(*, main_db: Path, state_db: Path) -> None:
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
        "60",
        "--price-interval",
        "15",
        "--dry-run",
    ]
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


def _latest_balance(state_db: Path) -> dict[str, float]:
    df = _read_sql_df(
        state_db,
        "SELECT timestamp, balance, pnl FROM balance_history ORDER BY timestamp DESC LIMIT 1",
    )
    if df.empty or "balance" not in df.columns:
        return {"balance": 0.0, "pnl": 0.0}
    row = df.iloc[0]
    return {"balance": float(row["balance"] or 0.0), "pnl": float(row["pnl"] or 0.0)}


def _daily_pnl(state_db: Path) -> float:
    start = int(dt.datetime.combine(dt.date.today(), dt.time.min).timestamp())
    df = _read_sql_df(
        state_db,
        "SELECT timestamp, pnl FROM balance_history WHERE timestamp >= ? ORDER BY timestamp ASC",
        (start,),
    )
    if df.empty or "pnl" not in df.columns:
        return _latest_balance(state_db)["pnl"]
    return float(df.iloc[-1]["pnl"] or 0.0) - float(df.iloc[0]["pnl"] or 0.0)


def _balance_history(state_db: Path, *, limit: int = 200) -> Any:
    df = _read_sql_df(
        state_db,
        "SELECT timestamp, balance, pnl FROM balance_history ORDER BY timestamp DESC LIMIT ?",
        (limit,),
    )
    if df.empty or "timestamp" not in df.columns:
        return pd.DataFrame()
    return df.sort_values("timestamp")


def _state_positions_for_panel(state_db: Path) -> list[dict[str, Any]]:
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
        return []
    rows = []
    for _, row in df.iterrows():
        rows.append(
            {
                "category": "PAPER",
                "outcome": row.get("side", "BUY"),
                "market": row.get("market_id", row.get("id", "")),
                "unrealizedPnl": float(row.get("pnl") or 0.0),
                "unrealizedPct": 0.0,
                "progress": 0.5,
            }
        )
    return rows


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
    except Exception:  # noqa: BLE001 - missing DB/tables should not break the dashboard.
        return pd.DataFrame()


def _uptime_label() -> str:
    started = st.session_state.get("paper_started_at")
    if not started:
        return "00:00:00"
    return str(dt.timedelta(seconds=max(0, int(time.time() - float(started)))))


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
            "Missing dashboard dependencies: "
            + ", ".join(missing)
            + ". Install: python -m pip install -r requirements-streamlit.txt"
        )


def _inject_style() -> None:
    st.markdown(
        """
        <style>
        :root {
          --bg:#02070a; --panel:#041216; --line:#155d67;
          --line-soft:rgba(39,158,174,.42); --text:#8af7ff;
          --muted:#4f9ea8; --dim:#245a61; --hot:#35e6f2;
          --amber:#dcc865; --red:#ff6670;
        }
        .stApp {
          background: radial-gradient(circle at 50% 10%, rgba(31,127,138,.18), transparent 40%),
                      linear-gradient(180deg,#02080b 0%,#010304 100%);
          color: var(--text);
          font-family: "Cascadia Mono", Consolas, "Courier New", monospace;
        }
        header[data-testid="stHeader"], #MainMenu, footer {display:none;}
        .block-container {max-width:none; padding:14px 14px 8px;}
        .terminal-shell {position:relative; color:var(--text); font-family:"Cascadia Mono",Consolas,"Courier New",monospace; font-size:13px;}
        .terminal-shell::before {
          content:""; position:fixed; inset:0; pointer-events:none;
          background:repeating-linear-gradient(to bottom,rgba(255,255,255,.02),rgba(255,255,255,.02) 1px,transparent 1px,transparent 5px);
          mix-blend-mode:screen; opacity:.38;
        }
        .topbar,.cyclebar,.statusbar {border:1px solid var(--line-soft); background:rgba(3,13,17,.88);}
        .topbar {display:grid; grid-template-columns:minmax(260px,1fr) 2fr; align-items:center; gap:20px; padding:10px 14px;}
        .brand {display:flex; align-items:center; gap:10px; color:var(--hot); font-weight:800; font-size:18px; text-shadow:0 0 12px rgba(53,230,242,.5);}
        .led {width:7px; height:7px; background:var(--hot); box-shadow:0 0 10px var(--hot); display:inline-block;}
        .topline {display:flex; justify-content:flex-end; gap:18px; color:var(--muted); white-space:nowrap; overflow:hidden;}
        .topline b,.live {color:var(--hot);}
        .cyclebar {display:grid; grid-template-columns:220px 1fr 130px auto; gap:14px; align-items:center; margin-top:10px; padding:8px 10px; color:var(--muted);}
        .cyclebar b,.mode-chip {color:var(--hot);}
        .progress-track {height:9px; border:1px solid var(--line); background:#031014; overflow:hidden;}
        .progress-track span {display:block; height:100%; background:linear-gradient(90deg,#106774,#42f5ff); box-shadow:0 0 14px rgba(66,245,255,.48);}
        div[data-testid="stButton"] button {
          min-width:68px; border:1px solid var(--line); background:#061920; color:var(--hot);
          padding:8px 10px; cursor:pointer; text-transform:uppercase; border-radius:0; font-weight:800;
        }
        div[data-testid="stButton"] button:hover {background:#0a2b34; color:var(--hot); border:1px solid var(--line); box-shadow:0 0 14px rgba(53,230,242,.2);}
        .action-note {height:38px; display:flex; align-items:center; color:var(--muted); border:1px solid var(--line-soft); background:rgba(3,13,17,.88); padding:0 10px;}
        .metrics {display:grid; grid-template-columns:repeat(10,minmax(90px,1fr)); margin-top:10px; border:1px solid var(--line-soft);}
        .metric {min-height:72px; padding:13px 12px; border-right:1px solid var(--line-soft); background:rgba(4,17,22,.88);}
        .metric:last-child {border-right:0;}
        .metric strong {display:block; color:var(--hot); font-size:clamp(17px,1.7vw,28px); line-height:1; text-shadow:0 0 14px rgba(53,230,242,.45);}
        .metric span {display:block; margin-top:8px; color:var(--muted); font-size:11px;}
        .grid {display:grid; grid-template-columns:1.1fr 1.1fr 1fr; grid-template-rows:310px 330px 245px 245px 245px 220px; gap:10px; margin-top:10px;}
        .panel {min-width:0; min-height:0; border:1px solid var(--line-soft); background:linear-gradient(180deg,rgba(5,24,30,.92),rgba(2,10,13,.94)); overflow:hidden;}
        .panel-title {height:28px; padding:7px 10px 6px; border-bottom:1px solid var(--line-soft); color:var(--hot); font-size:12px; font-weight:800;}
        .feed {height:calc(100% - 28px); overflow:hidden; padding:8px 10px;}
        .row {display:grid; grid-template-columns:58px minmax(0,1fr) auto; gap:9px; align-items:center; min-height:24px; color:#9cebf0; border-bottom:1px solid rgba(37,123,132,.22);}
        .row:last-child {border-bottom:0;}
        .tag {color:var(--hot); font-weight:800;} .tag.warn {color:var(--amber);} .tag.bad {color:var(--red);}
        .muted {color:var(--muted);} .value {color:var(--hot); font-weight:800;}
        .market {overflow:hidden; text-overflow:ellipsis; white-space:nowrap;}
        .positions .row {grid-template-columns:minmax(0,1fr) 76px 72px; min-height:36px;}
        .posbar {grid-column:1/-1; height:8px; border:1px solid var(--line); background:#031014;}
        .posbar span {display:block; height:100%; background:linear-gradient(90deg,#0d5e6a,#42f5ff);}
        .curve {grid-column:1/3; position:relative;}
        .bankroll-big {position:absolute; left:18px; top:54px; z-index:1; color:var(--hot); font-size:clamp(30px,4.4vw,56px); font-weight:900; text-shadow:0 0 18px rgba(53,230,242,.5);}
        .curve-svg {width:100%; height:calc(100% - 28px); display:block; background:rgba(2,9,5,.6);}
        .curve-svg .grid-lines line {stroke:rgba(39,158,174,.28); stroke-width:1;}
        .curve-area {fill:rgba(53,230,242,.20);} .curve-line {fill:none; stroke:#35e6f2; stroke-width:3; filter:drop-shadow(0 0 8px #35e6f2);}
        .depth {grid-column:3; grid-row:2;} .risk {grid-column:1; grid-row:3;} .votes {grid-column:2; grid-row:3;} .exits {grid-column:3; grid-row:3;}
        .whales {grid-column:1; grid-row:4;} .alerts {grid-column:2/4; grid-row:4;} .reviews {grid-column:1/4; grid-row:5;} .scale {grid-column:1/4; grid-row:6;}
        .depth-row {display:grid; grid-template-columns:52px 1fr 1fr; gap:8px; align-items:center; height:28px; padding:0 10px;}
        .bookbar {height:18px; background:#09242b;} .bookbar.bid {justify-self:end; background:#0d4350;} .bookbar.ask {background:#147887;}
        .statusbar {display:flex; justify-content:space-between; gap:14px; margin-top:10px; padding:8px 10px; color:var(--muted);}
        .empty {padding:22px 4px; color:var(--dim);}
        @media (max-width:1080px) {.topbar,.cyclebar{grid-template-columns:1fr}.topline{justify-content:flex-start;flex-wrap:wrap}.metrics{grid-template-columns:repeat(2,minmax(130px,1fr))}.grid{grid-template-columns:1fr;grid-template-rows:none}.panel,.curve,.depth,.risk,.votes,.exits,.whales,.reviews,.scale,.alerts{grid-column:auto;grid-row:auto;min-height:280px}}
        </style>
        """,
        unsafe_allow_html=True,
    )


def _tag_class(value: Any) -> str:
    text = str(value or "").upper()
    if text in {"KILL", "HIGH", "SELL", "EXIT", "LOCKED", "ERROR", "FAILED"}:
        return "bad"
    if text in {"WATCH", "CHECK", "ARMED", "TAKE", "TRIM", "TIME", "STALE", "PENDING"}:
        return "warn"
    return ""


def _e(value: Any) -> str:
    return html.escape(str(value if value is not None else ""))


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _money(value: Any) -> str:
    number = _float(value)
    sign = "-" if number < 0 else ""
    return f"{sign}${abs(number):,.0f}"


def _pct(value: Any, *, decimals: int = 1) -> str:
    return f"{_float(value) * 100:.{decimals}f}%"


def _price(value: Any) -> str:
    return f"{_float(value):.3f}"


def _num(value: Any) -> str:
    return f"{_float(value):.3f}"


def _intfmt(value: Any) -> str:
    return f"{int(_float(value)):,.0f}".replace(",", " ")


if __name__ == "__main__":
    main()

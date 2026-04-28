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

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - dashboard can still run without .env support.
    load_dotenv = None  # type: ignore[assignment]

from polymarket_signal_bot.bulk_sync import TARGET_TRADES, TARGET_WALLETS
from polymarket_signal_bot.dashboard import build_dashboard_state
from polymarket_signal_bot.demo import demo_trades, demo_wallets
from polymarket_signal_bot.monitor import Monitor, MonitorConfig
from polymarket_signal_bot.storage import DEFAULT_DB_PATH, Store


ROOT = Path(__file__).resolve().parent
DEFAULT_MAIN_DB = ROOT / DEFAULT_DB_PATH
DEFAULT_STATE_DB = ROOT / "data" / "paper_state.db"
DEFAULT_INDEXER_DB = ROOT / "data" / "indexer.db"
DEFAULT_LOG_PATH = ROOT / "data" / "streamlit_live_paper.log"
DEFAULT_TRAINER_LOG_PATH = ROOT / "data" / "auto_trainer_dashboard.log"
DEFAULT_SYSTEM_LOG_DIR = ROOT / "data" / "system_logs"
DEFAULT_BANKROLL = 200.0
INDEXER_TARGET_RECORDS = 86_000_000
INDEXER_REFRESH_SECONDS = 1.0
INDEXER_STALE_SECONDS = 120.0
TAB_KEYS = ("overview", "scan", "paper", "indexer")
SYSTEM_COMPONENT_KEYS = ("indexer", "monitor", "live_paper")


def main() -> None:
    _require_dashboard_dependencies()
    st.set_page_config(page_title="PolySignal 200 Seed", layout="wide", page_icon="*")
    _inject_style()
    _init_session_state()

    main_db = DEFAULT_MAIN_DB
    state_db = DEFAULT_STATE_DB
    indexer_db = _indexer_db_path()
    state = _terminal_state(main_db, state_db)

    _render_terminal_header(state)
    active_tab = _render_dashboard_tabs(indexer_db)
    _render_system_control(main_db, state_db, indexer_db)

    if active_tab == "overview":
        _render_terminal_actions(main_db, state_db)
        state = _terminal_state(main_db, state_db)
        _render_terminal_shell(state)
        _render_equity_chart(state_db)
    elif active_tab == "scan":
        _render_scan_tab(main_db, state)
    elif active_tab == "paper":
        _render_paper_tab(main_db, state_db, state)
    elif active_tab == "indexer":
        _render_indexer_tab(indexer_db)

    if active_tab != "indexer" and _paper_process_running():
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


def _render_dashboard_tabs(indexer_db: Path) -> str:
    active = _active_tab_from_query()
    live = _indexer_update_active(indexer_db)
    labels = _dashboard_tab_labels(indexer_live=live)
    items = []
    for key in TAB_KEYS:
        active_class = " active" if key == active else ""
        items.append(
            f"<a class='dashboard-tab{active_class}' href='?tab={key}'>{labels[key]}</a>"
        )
    st.markdown(
        "<nav class='dashboard-tabs'>" + "".join(items) + "</nav>",
        unsafe_allow_html=True,
    )
    return active


def _dashboard_tab_labels(*, indexer_live: bool) -> dict[str, str]:
    live = " <span class='live-dot'>● Live</span>" if indexer_live else ""
    return {
        "overview": "Overview",
        "scan": "Scan",
        "paper": "Paper Trading",
        "indexer": f"Indexer{live}",
    }


def _active_tab_from_query() -> str:
    try:
        raw_value = st.query_params.get("tab", "overview")
    except Exception:  # noqa: BLE001 - compatibility with older Streamlit.
        raw_params = st.experimental_get_query_params()
        raw_value = raw_params.get("tab", ["overview"])
    if isinstance(raw_value, list):
        raw_value = raw_value[0] if raw_value else "overview"
    value = str(raw_value or "overview").lower()
    return value if value in TAB_KEYS else "overview"


def _render_system_control(main_db: Path, state_db: Path, indexer_db: Path) -> None:
    specs = _system_component_specs(main_db, state_db, indexer_db)
    status = _system_component_statuses(specs)
    all_running = _system_all_running(status)
    st.markdown("<div class='system-control-title'>SYSTEM CONTROL // START ALL COMPONENTS</div>", unsafe_allow_html=True)
    cols = st.columns([1.1, 1.1, 4.8])
    autostart_default = bool(st.session_state.get("system_autostart", _dashboard_autostart_requested()))
    with cols[0]:
        button_label = _system_button_label(status)
        if st.button(button_label, width="stretch", type="primary", key="system_toggle"):
            if all_running:
                _stop_all_system(specs)
            else:
                _start_all_system(specs)
            st.rerun()
    with cols[1]:
        st.session_state.system_autostart = st.checkbox(
            "Autostart",
            value=autostart_default,
            key="system_autostart_checkbox",
        )
    with cols[2]:
        st.markdown(_system_status_html(status), unsafe_allow_html=True)

    if st.session_state.system_autostart and not st.session_state.get("system_autostart_done"):
        st.session_state.system_autostart_done = True
        if not all_running:
            _start_all_system(specs)
            st.rerun()

    with st.expander("SYSTEM LOGS", expanded=False):
        for key in SYSTEM_COMPONENT_KEYS:
            spec = specs[key]
            st.caption(str(spec["label"]))
            st.code(_tail_file(Path(spec["log_path"]), max_lines=80) or "No log yet", language="text")


def _system_all_running(status: dict[str, dict[str, Any]]) -> bool:
    return bool(status) and all(bool(item.get("running")) for item in status.values())


def _system_button_label(status: dict[str, dict[str, Any]]) -> str:
    return "STOP ALL" if _system_all_running(status) else "START ALL"


def _render_terminal_actions(main_db: Path, state_db: Path) -> None:
    cols = st.columns([1, 1, 1.15, 1.15, 4])
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
        st.session_state.paper_dry_run = st.checkbox(
            "Dry-run mode",
            value=bool(st.session_state.get("paper_dry_run", True)),
            key="paper_dry_run_checkbox",
        )
    with cols[3]:
        label = "STOP PAPER" if _paper_process_running() else "START PAPER"
        if st.button(label, width="stretch", type="primary"):
            if _paper_process_running():
                _stop_paper_process()
            else:
                try:
                    _start_paper_process(
                        main_db=main_db,
                        state_db=state_db,
                        dry_run=bool(st.session_state.get("paper_dry_run", True)),
                    )
                except Exception as exc:  # noqa: BLE001 - show action errors.
                    st.error(f"Paper trading failed to start: {exc}")
                    return
            st.rerun()
    with cols[4]:
        st.markdown(
            "<div class='action-note'>Demo / Overview, Scan and Paper Trade are sections on this same terminal screen.</div>",
            unsafe_allow_html=True,
        )


def _render_scan_tab(main_db: Path, state: dict[str, Any]) -> None:
    st.markdown("<div class='equity-title'>SCAN // MANUAL MARKET FLOW</div>", unsafe_allow_html=True)
    cols = st.columns([1, 1, 5])
    with cols[0]:
        if st.button("RUN SCAN", width="stretch", key="scan_tab_run"):
            try:
                result = _run_manual_scan(main_db)
                st.toast(
                    f"Scan complete: signals={result['signals']} opened={result['opened']} closed={result['closed']}"
                )
                st.rerun()
            except Exception as exc:  # noqa: BLE001 - show action errors.
                st.error(f"Scan failed: {exc}")
    with cols[1]:
        if st.button("LOAD DEMO", width="stretch", key="scan_tab_demo"):
            try:
                result = _load_demo(main_db)
                st.toast(
                    f"Demo loaded: trades={result['trades']} signals={result['signals']} opened={result['opened']}"
                )
                st.rerun()
            except Exception as exc:  # noqa: BLE001 - show action errors.
                st.error(f"Demo failed: {exc}")
    with cols[2]:
        st.markdown(
            "<div class='action-note'>Manual scan uses the same monitor and signal engine as the control room.</div>",
            unsafe_allow_html=True,
        )
    st.markdown(
        "<div class='terminal-shell scan-grid'>"
        f"{_panel('scanner', 'MARKET SCANNER // TOP FLOW', _scanner_rows(state.get('scanner', [])))}"
        f"{_panel('consensus', 'AGENT CONSENSUS // COPY CANDIDATES', _consensus_rows(state.get('consensus', [])))}"
        f"{_panel('reviews', 'MANUAL APPROVAL QUEUE', _review_rows(state.get('reviews', [])))}"
        "</div>",
        unsafe_allow_html=True,
    )


def _render_paper_tab(main_db: Path, state_db: Path, state: dict[str, Any]) -> None:
    st.markdown("<div class='equity-title'>PAPER TRADING // LIVE SIMULATION</div>", unsafe_allow_html=True)
    paper = state["paper"]
    positions = paper["statePositions"] or state.get("positions", [])
    cols = st.columns([1.2, 1.2, 4.6])
    with cols[0]:
        st.session_state.paper_dry_run = st.checkbox(
            "Dry-run mode",
            value=bool(st.session_state.get("paper_dry_run", True)),
            key="paper_tab_dry_run_checkbox",
        )
    with cols[1]:
        label = "STOP PAPER" if _paper_process_running() else "START PAPER"
        if st.button(label, width="stretch", type="primary", key="paper_tab_toggle"):
            if _paper_process_running():
                _stop_paper_process()
            else:
                try:
                    _start_paper_process(
                        main_db=main_db,
                        state_db=state_db,
                        dry_run=bool(st.session_state.get("paper_dry_run", True)),
                    )
                except Exception as exc:  # noqa: BLE001 - show action errors.
                    st.error(f"Paper trading failed to start: {exc}")
                    return
            st.rerun()
    with cols[2]:
        status = "RUNNING" if paper["running"] else "STOPPED"
        st.markdown(
            f"<div class='action-note'>STATUS {status} // DAY PNL {_money(paper['dailyPnl'])} // UPTIME {_e(paper['uptime'])}</div>",
            unsafe_allow_html=True,
        )
    st.markdown(
        "<div class='terminal-shell paper-grid'>"
        f"{_panel('positions', 'ACTIVE POSITIONS', _position_rows(positions))}"
        f"{_panel('risk', 'RISK MONITOR', _risk_rows(state.get('risk', [])))}"
        f"{_panel('exits', 'EXIT TRIGGERS', _exit_rows(state.get('exitTriggers', [])))}"
        "</div>",
        unsafe_allow_html=True,
    )


def _render_indexer_tab(indexer_db: Path) -> None:
    placeholder = st.empty()
    cols = st.columns([1, 1, 5])
    with cols[0]:
        manual_refresh = st.button("REFRESH INDEXER", width="stretch", key="indexer_refresh")
    with cols[1]:
        retrain_running = _retrain_process_running()
        if st.button("RETRAIN NOW", width="stretch", key="indexer_retrain", disabled=retrain_running):
            try:
                _start_retrain_process(indexer_db)
            except Exception as exc:  # noqa: BLE001 - keep dashboard visible.
                st.error(f"Retrain failed to start: {exc}")
            st.rerun()
    with cols[2]:
        train_status = "TRAINING" if _retrain_process_running() else "IDLE"
        st.markdown(
            f"<div class='action-note'>AUTO TRAINER {train_status} // SOURCE {_e(indexer_db)}</div>",
            unsafe_allow_html=True,
        )
    snapshot = _indexer_snapshot(indexer_db)
    previous = st.session_state.get("indexer_previous_snapshot")
    speed = _indexer_speed(snapshot, previous)
    st.session_state.indexer_previous_snapshot = {
        "at": time.time(),
        "last_block": snapshot["last_block"],
    }

    with placeholder.container():
        _render_indexer_snapshot(indexer_db, snapshot, speed)

    if manual_refresh:
        st.rerun()
    if snapshot["update_active"] or snapshot["retrain_running"]:
        time.sleep(INDEXER_REFRESH_SECONDS)
        st.rerun()


def _render_indexer_snapshot(indexer_db: Path, snapshot: dict[str, Any], speed: float) -> None:
    live = " <span class='live-dot'>● Live</span>" if snapshot["update_active"] or snapshot["retrain_running"] else ""
    st.markdown(
        f"<div class='equity-title'>INDEXER // POLYGON RAW TRANSACTIONS{live}</div>",
        unsafe_allow_html=True,
    )
    if not snapshot["db_exists"]:
        st.info(f"Indexer DB not found: {indexer_db}")
        if snapshot["running"]:
            st.info("Indexer process is running, waiting for the database file.")
        return
    if not snapshot["schema_ready"]:
        st.info(f"Indexer tables are not ready yet: {indexer_db}")
        return

    records = int(snapshot["records"])
    progress = min(1.0, records / INDEXER_TARGET_RECORDS)
    block_label = _intfmt(snapshot["last_block"]) if snapshot["last_block"] else "0"
    speed_label = f"{speed:.2f} blk/s"
    updated = _timestamp_label(snapshot.get("updated_at"))
    status = "RUNNING" if snapshot["running"] else "STOPPED"
    metrics = [
        ("RAW EVENTS", _intfmt(records)),
        ("LAST BLOCK", block_label),
        ("SPEED", speed_label),
        ("TARGET", _intfmt(INDEXER_TARGET_RECORDS)),
        ("STATUS", status),
        ("UPDATED", updated),
    ]
    cohort_counts = snapshot.get("cohort_counts") if isinstance(snapshot.get("cohort_counts"), dict) else {}
    if snapshot.get("retrain_running"):
        train_status = "RUNNING"
    elif snapshot.get("last_training_ok") is True:
        train_status = "OK"
    elif snapshot.get("last_training_ok") is False:
        train_status = "ERROR"
    else:
        train_status = "WAIT"
    training_metrics = [
        ("TRAIN", train_status),
        ("LAST TRAIN", _datetime_label(snapshot.get("last_training_at"))),
        ("SCORED", _intfmt(snapshot.get("training_scored_wallets"))),
        ("STABLE", _intfmt(cohort_counts.get("STABLE", 0))),
        ("CANDIDATE", _intfmt(cohort_counts.get("CANDIDATE", 0))),
        ("EXIT EXAMPLES", _intfmt(snapshot.get("exit_examples"))),
    ]
    st.markdown(
        "<div class='terminal-shell indexer-shell'>"
        f"<section class='metrics indexer-metrics'>{''.join(_metric(label, value) for label, value in metrics)}</section>"
        f"<section class='metrics indexer-metrics training-metrics'>{''.join(_metric(label, value) for label, value in training_metrics)}</section>"
        "<section class='panel indexer-panel'>"
        "<div class='panel-title'>SCALE TARGET // 86M RAW EVENTS</div>"
        f"<div class='indexer-progress-label'>{_intfmt(records)} / {_intfmt(INDEXER_TARGET_RECORDS)} ({progress * 100:.4f}%)</div>"
        f"<div class='progress-track indexer-progress'><span style='width:{max(0.5, progress * 100):.4f}%'></span></div>"
        f"<div class='indexer-path'>DB {_e(indexer_db)}</div>"
        "</section>"
        "</div>",
        unsafe_allow_html=True,
    )
    st.progress(progress)
    if not snapshot["running"]:
        st.info("Indexer is not running or has not updated its checkpoint recently.")


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


def _render_equity_chart(state_db: Path) -> None:
    st.markdown("<div class='equity-title'>OVERVIEW // EQUITY CURVE</div>", unsafe_allow_html=True)
    history = _balance_history(state_db, limit=500)
    if history.empty:
        st.info("Нет данных для графика")
        return
    chart = history[["timestamp", "balance"]].copy()
    chart["time"] = pd.to_datetime(chart["timestamp"], unit="s")
    chart = chart.set_index("time")[["balance"]]
    st.line_chart(chart, height=220)


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


def _system_component_specs(main_db: Path, state_db: Path, indexer_db: Path) -> dict[str, dict[str, Any]]:
    _load_dotenv_file()
    DEFAULT_SYSTEM_LOG_DIR.mkdir(parents=True, exist_ok=True)
    env = _system_env(main_db, state_db, indexer_db)
    live_command = [
        sys.executable,
        "-m",
        "polymarket_signal_bot.live_paper_runner",
        "--db",
        str(main_db.resolve()),
        "--state-db",
        str(state_db.resolve()),
        "--log-path",
        str(DEFAULT_LOG_PATH.resolve()),
        "--poll-interval",
        "60",
        "--price-interval",
        "15",
    ]
    if _env_bool("DRY_RUN", True):
        live_command.append("--dry-run")
    return {
        "indexer": {
            "label": "Indexer",
            "command": [
                sys.executable,
                "-m",
                "src.indexer",
                "--sync",
                "--db",
                str(indexer_db.resolve()),
            ],
            "patterns": ("*src.indexer*", "*polymarket_signal_bot.indexer*"),
            "log_path": DEFAULT_SYSTEM_LOG_DIR / "indexer.log",
            "env": env,
        },
        "monitor": {
            "label": "Monitor",
            "command": [
                sys.executable,
                "-m",
                "polymarket_signal_bot",
                "--db",
                str(main_db.resolve()),
                "monitor",
                "--interval-seconds",
                "60",
                "--training-db",
                str(indexer_db.resolve()),
                "--retrain-min-new-records",
                "10000",
            ],
            "patterns": ("*polymarket_signal_bot*monitor*", "*src.monitor*"),
            "log_path": DEFAULT_SYSTEM_LOG_DIR / "monitor.log",
            "env": env,
        },
        "live_paper": {
            "label": "Live Paper",
            "command": live_command,
            "patterns": ("*polymarket_signal_bot.live_paper_runner*", "*polymarket_signal_bot*live-paper*"),
            "log_path": DEFAULT_SYSTEM_LOG_DIR / "live_paper.log",
            "env": env,
        },
    }


def _system_env(main_db: Path, state_db: Path, indexer_db: Path) -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("PYTHONUTF8", "1")
    env["POLYSIGNAL_DB"] = str(main_db.resolve())
    env["POLYSIGNAL_PAPER_STATE_DB"] = str(state_db.resolve())
    env["INDEXER_DB_PATH"] = str(indexer_db.resolve())
    env.setdefault("RPC_RPS", "5")
    return env


def _load_dotenv_file() -> None:
    if st is not None and st.session_state.get("dotenv_loaded"):
        return
    if load_dotenv is not None:
        load_dotenv(ROOT / ".env", override=False)
    if st is not None:
        st.session_state.dotenv_loaded = True


def _start_all_system(specs: dict[str, dict[str, Any]]) -> None:
    processes = dict(st.session_state.get("system_processes") or {})
    pids = dict(st.session_state.get("system_pids") or {})
    for key in SYSTEM_COMPONENT_KEYS:
        if _system_component_running(key, specs[key]):
            discovered_pid = _discover_component_pid(specs[key])
            if discovered_pid:
                pids[key] = discovered_pid
            continue
        process = _start_system_component(key, specs[key])
        processes[key] = process
        pids[key] = process.pid
        if key == "live_paper":
            st.session_state.paper_process = process
            st.session_state.paper_pid = process.pid
            st.session_state.paper_started_at = time.time()
            st.session_state.paper_command = " ".join(str(part) for part in specs[key]["command"])
    st.session_state.system_processes = processes
    st.session_state.system_pids = pids
    st.session_state.system_status = "Running"


def _start_system_component(key: str, spec: dict[str, Any]) -> subprocess.Popen:
    log_path = Path(spec["log_path"])
    log_path.parent.mkdir(parents=True, exist_ok=True)
    header = (
        f"\n\n[{dt.datetime.now().isoformat(timespec='seconds')}] "
        f"START {key}: {' '.join(str(part) for part in spec['command'])}\n"
    )
    log_path.write_text(header, encoding="utf-8", errors="replace")
    log_file = log_path.open("ab")
    try:
        return subprocess.Popen(
            [str(part) for part in spec["command"]],
            cwd=ROOT,
            env=spec["env"],
            stdout=log_file,
            stderr=log_file,
        )
    finally:
        log_file.close()


def _stop_all_system(specs: dict[str, dict[str, Any]]) -> None:
    processes = dict(st.session_state.get("system_processes") or {})
    pids = dict(st.session_state.get("system_pids") or {})
    for key in reversed(SYSTEM_COMPONENT_KEYS):
        process = processes.get(key)
        pid = int(pids.get(key) or 0) or _discover_component_pid(specs[key])
        if process and process.poll() is None:
            _terminate_process(process, timeout_seconds=10)
        elif pid:
            _terminate_pid(pid, timeout_seconds=10)
        processes.pop(key, None)
        pids.pop(key, None)
    st.session_state.system_processes = processes
    st.session_state.system_pids = pids
    st.session_state.system_status = "Stopped"
    st.session_state.paper_process = None
    st.session_state.paper_pid = None
    st.session_state.paper_started_at = None


def _system_component_statuses(specs: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    statuses = {}
    pids = dict(st.session_state.get("system_pids") or {})
    for key in SYSTEM_COMPONENT_KEYS:
        pid = int(pids.get(key) or 0) or _discover_component_pid(specs[key])
        running = _system_component_running(key, specs[key], pid=pid)
        if running and pid:
            pids[key] = pid
        elif not running:
            pids.pop(key, None)
        statuses[key] = {"label": specs[key]["label"], "running": running, "pid": pid if running else 0}
    st.session_state.system_pids = pids
    return statuses


def _system_component_running(key: str, spec: dict[str, Any], *, pid: int | None = None) -> bool:
    process = (st.session_state.get("system_processes") or {}).get(key) if st is not None else None
    if process and process.poll() is None:
        return True
    if pid and _pid_running(pid):
        return True
    discovered_pid = _discover_component_pid(spec)
    return bool(discovered_pid and _pid_running(discovered_pid))


def _system_status_html(status: dict[str, dict[str, Any]]) -> str:
    rows = []
    for key in SYSTEM_COMPONENT_KEYS:
        item = status[key]
        dot_class = "on" if item["running"] else "off"
        pid = f"PID {item['pid']}" if item["pid"] else "STOPPED"
        rows.append(
            "<div class='system-row'>"
            f"<span class='system-dot {dot_class}'></span>"
            f"<span>{_e(item['label'])}</span>"
            f"<b>{_e(pid)}</b>"
            "</div>"
        )
    return "<div class='system-status'>" + "".join(rows) + "</div>"


def _discover_component_pid(spec: dict[str, Any]) -> int:
    patterns = tuple(str(pattern) for pattern in spec.get("patterns", ()))
    if not patterns:
        return 0
    try:
        if os.name == "nt":
            clauses = " -or ".join(f"$_.CommandLine -like '{pattern}'" for pattern in patterns)
            command = (
                "Get-CimInstance Win32_Process | "
                f"Where-Object {{ $_.Name -like 'python*' -and ({clauses}) }} | "
                "Select-Object -First 1 -ExpandProperty ProcessId"
            )
            result = subprocess.run(
                ["powershell.exe", "-NoProfile", "-Command", command],
                capture_output=True,
                text=True,
                timeout=2,
                check=False,
            )
            return int(result.stdout.strip() or 0)
        pattern = "|".join(pattern.strip("*") for pattern in patterns)
        result = subprocess.run(["pgrep", "-f", pattern], capture_output=True, text=True, timeout=2, check=False)
        first = result.stdout.strip().splitlines()[0] if result.stdout.strip() else "0"
        return int(first)
    except Exception:  # noqa: BLE001 - process discovery is best effort.
        return 0


def _pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        if os.name == "nt":
            result = subprocess.run(
                [
                    "powershell.exe",
                    "-NoProfile",
                    "-Command",
                    f"Get-CimInstance Win32_Process -Filter \"ProcessId = {int(pid)}\"",
                ],
                capture_output=True,
                text=True,
                timeout=2,
                check=False,
            )
            return bool(result.stdout.strip())
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _terminate_process(process: subprocess.Popen, *, timeout_seconds: int) -> None:
    try:
        process.send_signal(signal.SIGTERM)
        process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=3)
    except Exception:
        return


def _terminate_pid(pid: int, *, timeout_seconds: int) -> None:
    if not _pid_running(pid):
        return
    try:
        if os.name == "nt":
            subprocess.run(["taskkill", "/PID", str(pid), "/T"], capture_output=True, timeout=timeout_seconds, check=False)
            deadline = time.time() + timeout_seconds
            while time.time() < deadline and _pid_running(pid):
                time.sleep(0.2)
            if _pid_running(pid):
                subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True, timeout=5, check=False)
        else:
            os.kill(pid, signal.SIGTERM)
            deadline = time.time() + timeout_seconds
            while time.time() < deadline and _pid_running(pid):
                time.sleep(0.2)
            if _pid_running(pid):
                os.kill(pid, signal.SIGKILL)
    except Exception:
        return


def _tail_file(path: Path, *, max_lines: int) -> str:
    if not path.exists():
        return ""
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return ""
    return "\n".join(lines[-max_lines:])


def _dashboard_autostart_requested() -> bool:
    if "--autostart" in sys.argv:
        return True
    try:
        value = st.query_params.get("autostart", "")
    except Exception:
        value = ""
    if isinstance(value, list):
        value = value[0] if value else ""
    return _truthy(value) or _env_bool("POLYSIGNAL_AUTOSTART", False)


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return _truthy(value)


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


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


def _start_paper_process(*, main_db: Path, state_db: Path, dry_run: bool) -> None:
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
    ]
    if dry_run:
        command.append("--dry-run")
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


def _start_retrain_process(indexer_db: Path) -> None:
    command = [
        sys.executable,
        "-m",
        "src.auto_trainer",
        "--db",
        str(indexer_db),
    ]
    env = os.environ.copy()
    env.setdefault("PYTHONUTF8", "1")
    DEFAULT_TRAINER_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with DEFAULT_TRAINER_LOG_PATH.open("ab") as log_file:
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            env=env,
            stdout=log_file,
            stderr=log_file,
        )
    st.session_state.retrain_process = process
    st.session_state.retrain_pid = process.pid
    st.session_state.retrain_started_at = time.time()
    st.session_state.retrain_command = " ".join(command)


def _retrain_process_running(*, show_error: bool = True) -> bool:
    if st is None:
        return False
    try:
        process = st.session_state.get("retrain_process")
    except Exception:  # noqa: BLE001 - tests may call helpers outside Streamlit runtime.
        return False
    if not process:
        return False
    code = process.poll()
    if code is None:
        return True
    st.session_state.retrain_process = None
    st.session_state.retrain_pid = None
    st.session_state.retrain_started_at = None
    if code != 0 and show_error:
        st.error(f"Retrain process stopped with exit code {code}. Check {DEFAULT_TRAINER_LOG_PATH}.")
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


def _indexer_db_path() -> Path:
    raw_path = os.getenv("INDEXER_DB_PATH")
    if not raw_path:
        return DEFAULT_INDEXER_DB
    path = Path(raw_path)
    return path if path.is_absolute() else ROOT / path


def _indexer_update_active(_db_path: Path) -> bool:
    return _indexer_process_running()


def _indexer_snapshot(db_path: Path) -> dict[str, Any]:
    running = _indexer_process_running()
    snapshot = {
        "db_exists": db_path.exists(),
        "schema_ready": False,
        "records": 0,
        "last_block": 0,
        "updated_at": 0,
        "running": running,
        "update_active": running,
        "recent_checkpoint": False,
        "retrain_running": _retrain_process_running(show_error=False),
        "last_training_at": 0,
        "last_training_ok": None,
        "training_raw_transactions": 0,
        "training_scored_wallets": 0,
        "exit_examples": 0,
        "cohort_counts": {},
        "has_data": False,
    }
    if not db_path.exists():
        return snapshot

    conn: sqlite3.Connection | None = None
    try:
        uri = f"{db_path.resolve().as_uri()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000;")
        tables = {
            str(row["name"])
            for row in conn.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'table' AND name IN ('raw_transactions', 'indexer_state')
                """
            ).fetchall()
        }
        snapshot["schema_ready"] = {"raw_transactions", "indexer_state"}.issubset(tables)
        if not snapshot["schema_ready"]:
            return snapshot

        record_row = conn.execute("SELECT COUNT(*) AS records FROM raw_transactions").fetchone()
        state_row = conn.execute(
            """
            SELECT last_block, updated_at
            FROM indexer_state
            ORDER BY updated_at DESC
            LIMIT 1
            """
        ).fetchone()
        snapshot["records"] = int(record_row["records"] if record_row else 0)
        if state_row:
            snapshot["last_block"] = int(state_row["last_block"] or 0)
            snapshot["updated_at"] = int(state_row["updated_at"] or 0)
        training = _training_snapshot(conn)
        snapshot.update(training)
        snapshot["has_data"] = bool(snapshot["records"] or snapshot["last_block"])
        if snapshot["updated_at"]:
            snapshot["recent_checkpoint"] = time.time() - int(snapshot["updated_at"]) <= INDEXER_STALE_SECONDS
        return snapshot
    except Exception:  # noqa: BLE001 - dashboard should stay up if the DB is mid-write.
        snapshot["schema_ready"] = False
        return snapshot
    finally:
        if conn is not None:
            conn.close()


def _training_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    tables = {
        str(row["name"])
        for row in conn.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'table' AND name IN ('training_runs', 'wallet_cohorts')
            """
        ).fetchall()
    }
    snapshot: dict[str, Any] = {
        "last_training_at": 0,
        "last_training_ok": None,
        "training_raw_transactions": 0,
        "training_scored_wallets": 0,
        "exit_examples": 0,
        "cohort_counts": {},
    }
    if "training_runs" in tables:
        row = conn.execute(
            """
            SELECT started_at, ok, raw_transactions, scored_wallets, exit_examples
            FROM training_runs
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
        if row:
            snapshot.update(
                {
                    "last_training_at": int(row["started_at"] or 0),
                    "last_training_ok": bool(row["ok"]),
                    "training_raw_transactions": int(row["raw_transactions"] or 0),
                    "training_scored_wallets": int(row["scored_wallets"] or 0),
                    "exit_examples": int(row["exit_examples"] or 0),
                }
            )
    if "wallet_cohorts" in tables:
        rows = conn.execute("SELECT status, COUNT(*) AS wallets FROM wallet_cohorts GROUP BY status").fetchall()
        snapshot["cohort_counts"] = {str(row["status"]): int(row["wallets"] or 0) for row in rows}
    return snapshot


def _indexer_speed(snapshot: dict[str, Any], previous: Any) -> float:
    if not previous:
        return 0.0
    now = time.time()
    last_block = int(snapshot.get("last_block") or 0)
    previous_block = int(previous.get("last_block") or 0)
    previous_at = float(previous.get("at") or now)
    elapsed = max(0.001, now - previous_at)
    return max(0.0, (last_block - previous_block) / elapsed)


def _indexer_process_running() -> bool:
    try:
        if os.name == "nt":
            command = (
                "Get-CimInstance Win32_Process | "
                "Where-Object { $_.Name -like 'python*' -and "
                "($_.CommandLine -like '*src.indexer*' -or "
                "$_.CommandLine -like '*polymarket_signal_bot.indexer*') } | "
                "Select-Object -First 1 -ExpandProperty ProcessId"
            )
            result = subprocess.run(
                ["powershell.exe", "-NoProfile", "-Command", command],
                capture_output=True,
                text=True,
                timeout=2,
                check=False,
            )
            return bool(result.stdout.strip())
        result = subprocess.run(
            ["pgrep", "-f", "src.indexer|polymarket_signal_bot.indexer"],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
        return result.returncode == 0
    except Exception:  # noqa: BLE001 - process checks are best effort.
        return False


def _read_sql_df(db_path: Path, query: str, params: tuple[Any, ...] = ()) -> Any:
    if pd is None:
        raise RuntimeError("pandas is required. Install: python -m pip install -r requirements-streamlit.txt")
    if not db_path.exists():
        return pd.DataFrame()
    conn: sqlite3.Connection | None = None
    try:
        if _is_state_db(db_path):
            conn = sqlite3.connect(db_path, timeout=30)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout=30000;")
        else:
            uri = f"{db_path.resolve().as_uri()}?mode=ro"
            conn = sqlite3.connect(uri, uri=True, timeout=30)
        conn.row_factory = sqlite3.Row
        return pd.read_sql_query(query, conn, params=params)
    except Exception:  # noqa: BLE001 - missing DB/tables should not break the dashboard.
        return pd.DataFrame()
    finally:
        if conn is not None:
            conn.close()


def _is_state_db(db_path: Path) -> bool:
    try:
        return db_path.resolve() == DEFAULT_STATE_DB.resolve()
    except OSError:
        return db_path.name == DEFAULT_STATE_DB.name


def _uptime_label() -> str:
    started = st.session_state.get("paper_started_at")
    if not started:
        return "00:00:00"
    return str(dt.timedelta(seconds=max(0, int(time.time() - float(started)))))


def _timestamp_label(value: Any) -> str:
    timestamp = int(_float(value))
    if timestamp <= 0:
        return "NEVER"
    return dt.datetime.fromtimestamp(timestamp).strftime("%H:%M:%S")


def _datetime_label(value: Any) -> str:
    timestamp = int(_float(value))
    if timestamp <= 0:
        return "NEVER"
    return dt.datetime.fromtimestamp(timestamp).strftime("%m-%d %H:%M")


def _init_session_state() -> None:
    st.session_state.setdefault("paper_process", None)
    st.session_state.setdefault("paper_pid", None)
    st.session_state.setdefault("paper_started_at", None)
    st.session_state.setdefault("paper_command", "")
    st.session_state.setdefault("paper_dry_run", True)
    st.session_state.setdefault("indexer_previous_snapshot", None)
    st.session_state.setdefault("retrain_process", None)
    st.session_state.setdefault("retrain_pid", None)
    st.session_state.setdefault("retrain_started_at", None)
    st.session_state.setdefault("retrain_command", "")
    st.session_state.setdefault("system_processes", {})
    st.session_state.setdefault("system_pids", {})
    st.session_state.setdefault("system_status", "Stopped")
    st.session_state.setdefault("system_autostart", False)
    st.session_state.setdefault("system_autostart_done", False)
    st.session_state.setdefault("dotenv_loaded", False)


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
        section[data-testid="stSidebar"] {background:rgba(2,8,11,.96); border-right:1px solid var(--line-soft);}
        section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"],
        section[data-testid="stSidebar"] label,
        section[data-testid="stSidebar"] span {font-family:"Cascadia Mono",Consolas,"Courier New",monospace;}
        .side-title,.system-control-title {height:30px; display:flex; align-items:center; color:var(--hot); font-weight:900; border:1px solid var(--line-soft); background:rgba(3,13,17,.88); padding:0 10px; margin-bottom:8px;}
        .system-control-title {margin-top:8px;}
        .system-status {display:grid; grid-template-columns:repeat(3,minmax(150px,1fr)); border:1px solid var(--line-soft); background:rgba(3,13,17,.88); min-height:38px;}
        .system-row {display:grid; grid-template-columns:16px minmax(0,1fr) auto; gap:8px; align-items:center; min-height:38px; padding:5px 8px; border-right:1px solid rgba(37,123,132,.22); color:var(--text);}
        .system-row:last-child {border-right:0;}
        .system-row b {color:var(--muted); font-size:11px;}
        .system-dot {width:8px; height:8px; border-radius:50%; display:inline-block; background:#59656a;}
        .system-dot.on {background:#37ff7d; box-shadow:0 0 8px rgba(55,255,125,.72);}
        .system-dot.off {background:#59656a;}
        .dashboard-tabs {display:flex; gap:8px; border-bottom:1px solid var(--line-soft); margin-top:10px; padding-bottom:8px;}
        .dashboard-tab {
          height:38px; display:flex; align-items:center; border:1px solid var(--line-soft); background:rgba(3,13,17,.88);
          color:var(--muted); border-radius:0; padding:0 14px; font-family:"Cascadia Mono",Consolas,"Courier New",monospace;
          font-weight:800; text-decoration:none;
        }
        .dashboard-tab:hover {color:var(--hot); border-color:var(--line);}
        .dashboard-tab.active {color:var(--hot); border-color:var(--line); box-shadow:0 0 12px rgba(53,230,242,.16);}
        .live-dot {color:#37ff7d; font-weight:900; text-shadow:0 0 8px rgba(55,255,125,.72);}
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
        .stCheckbox {height:38px; display:flex; align-items:center; border:1px solid var(--line-soft); background:rgba(3,13,17,.88); padding:0 10px;}
        .stCheckbox label, .stCheckbox span {color:var(--hot) !important; font-family:"Cascadia Mono",Consolas,"Courier New",monospace; font-weight:800;}
        .equity-title {margin-top:10px; height:28px; padding:7px 10px 6px; border:1px solid var(--line-soft); background:rgba(3,13,17,.88); color:var(--hot); font-size:12px; font-weight:800;}
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
        .scan-grid,.paper-grid {display:grid; grid-template-columns:1fr 1fr 1fr; gap:10px; margin-top:10px;}
        .scan-grid .panel,.paper-grid .panel {min-height:310px;}
        .indexer-shell {margin-top:10px;}
        .indexer-metrics {grid-template-columns:repeat(6,minmax(120px,1fr));}
        .training-metrics {margin-top:10px;}
        .indexer-panel {margin-top:10px; min-height:126px;}
        .indexer-progress-label {padding:16px 12px 10px; color:var(--hot); font-size:16px; font-weight:900;}
        .indexer-progress {margin:0 12px 12px; height:12px;}
        .indexer-path {padding:0 12px 14px; color:var(--muted); overflow:hidden; text-overflow:ellipsis; white-space:nowrap;}
        .empty {padding:22px 4px; color:var(--dim);}
        @media (max-width:1080px) {.topbar,.cyclebar{grid-template-columns:1fr}.topline{justify-content:flex-start;flex-wrap:wrap}.dashboard-tabs{flex-wrap:wrap}.system-status{grid-template-columns:1fr}.system-row{border-right:0;border-bottom:1px solid rgba(37,123,132,.22)}.system-row:last-child{border-bottom:0}.metrics{grid-template-columns:repeat(2,minmax(130px,1fr))}.grid,.scan-grid,.paper-grid{grid-template-columns:1fr;grid-template-rows:none}.panel,.curve,.depth,.risk,.votes,.exits,.whales,.reviews,.scale,.alerts{grid-column:auto;grid-row:auto;min-height:280px}}
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

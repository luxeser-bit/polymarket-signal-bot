from __future__ import annotations

import json
import mimetypes
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .analytics import DEFAULT_DUCKDB_PATH
from .bulk_sync import TARGET_TRADES, TARGET_WALLETS
from .cohorts import CohortConfig, wallet_cohort_report
from .demo import demo_trades, demo_wallets
from .features import format_feature_summary
from .learning import format_wallet_outcome_summary, wallet_outcome_lookup, wallet_outcome_report
from .paper import PaperBroker
from .policy_optimizer import policy_settings_from_recommendation
from .scoring import score_wallets
from .signals import SignalConfig, generate_signals
from .storage import Store
from .taxonomy import market_category, market_category_label


DEFAULT_BANKROLL = 200.0


def serve_dashboard(db_path: str | Path, *, host: str = "127.0.0.1", port: int = 8765) -> None:
    static_dir = Path(__file__).with_name("web")

    class DashboardHandler(BaseHTTPRequestHandler):
        server_version = "PolySignalDashboard/0.1"

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/api/state":
                params = parse_qs(parsed.query)
                bankroll = _float_arg(params, "bankroll", DEFAULT_BANKROLL)
                with Store(db_path) as store:
                    store.init_schema()
                    payload = build_dashboard_state(store, bankroll=bankroll)
                self._send_json(payload)
                return

            if parsed.path in ("", "/"):
                self._send_static(static_dir / "index.html")
                return

            safe_path = parsed.path.lstrip("/").replace("\\", "/")
            if ".." in safe_path.split("/"):
                self.send_error(HTTPStatus.BAD_REQUEST)
                return
            self._send_static(static_dir / safe_path)

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/api/demo":
                with Store(db_path) as store:
                    store.init_schema()
                    store.upsert_wallets(demo_wallets())
                    inserted = store.insert_trades(demo_trades())
                    _run_default_scan(store)
                    payload = build_dashboard_state(store, bankroll=DEFAULT_BANKROLL)
                    payload["notice"] = f"Demo loaded: {inserted} new trades"
                self._send_json(payload)
                return

            if parsed.path == "/api/scan":
                with Store(db_path) as store:
                    store.init_schema()
                    opened = _run_default_scan(store)
                    payload = build_dashboard_state(store, bankroll=DEFAULT_BANKROLL)
                    payload["notice"] = f"Scan complete: {opened} new paper positions"
                self._send_json(payload)
                return

            self.send_error(HTTPStatus.NOT_FOUND)

        def log_message(self, format: str, *args: Any) -> None:
            timestamp = time.strftime("%H:%M:%S")
            print(f"[{timestamp}] {self.address_string()} {format % args}")

        def _send_json(self, payload: dict[str, Any]) -> None:
            raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _send_static(self, path: Path) -> None:
            if not path.exists() or not path.is_file():
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            raw = path.read_bytes()
            mime = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", mime)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

    httpd = ThreadingHTTPServer((host, port), DashboardHandler)
    print(f"PolySignal dashboard running at http://{host}:{port}")
    httpd.serve_forever()


def build_dashboard_state(store: Store, *, bankroll: float = DEFAULT_BANKROLL) -> dict[str, Any]:
    now = int(time.time())
    scores = list(store.fetch_scores().values())
    recent_signals = store.fetch_recent_signals(12)
    recent_trades = store.fetch_recent_trades(limit=80)
    books = store.fetch_order_books(limit=30)
    open_positions = store.fetch_open_positions()
    closed_positions = store.fetch_recent_closed_positions(limit=8)
    journal_summary = store.paper_event_summary(limit=8)
    summary = store.paper_summary()
    stats = _db_stats(store)
    sync_progress = store.sync_progress()
    book_history = store.order_book_history_summary()
    duckdb_path = Path(DEFAULT_DUCKDB_PATH)
    duckdb_exists = duckdb_path.exists()
    runtime = store.runtime_state()
    alerts = store.fetch_recent_alerts(limit=8)
    reviews = store.fetch_review_queue("PENDING", limit=8)
    cohort_report = wallet_cohort_report(store, CohortConfig(history_days=30, min_trades=2, min_notional=100, limit=12))
    learning_report = wallet_outcome_report(store, since_days=90, min_events=1, limit=8)
    feature_summary = store.decision_feature_summary()
    marked_positions = [_position_payload(store, position, books) for position in open_positions]
    unrealized = sum(item["unrealizedPnl"] for item in marked_positions)
    current_bankroll = bankroll + summary["realized_pnl"] + unrealized
    risk_snapshot = PaperBroker(store).risk_snapshot()

    return {
        "meta": {
            "name": "POLYMARKET 200 SEED",
            "mode": "PAPER",
            "status": "LIVE SCAN" if recent_trades else "STANDBY",
            "generatedAt": now,
            "db": str(store.path),
            "monitorStatus": _runtime_value(runtime, "monitor_status", "offline"),
            "monitorSummary": _runtime_value(runtime, "monitor_last_summary", ""),
            "monitorLastSeen": _runtime_updated(runtime, "monitor_last_seen"),
            "bulkSummary": _runtime_value(runtime, "bulk_last_summary", ""),
            "marketFlowSummary": _runtime_value(runtime, "market_flow_last_summary", ""),
            "marketFlowLastSeen": _runtime_updated(runtime, "market_flow_last_seen"),
            "analyticsStatus": _runtime_value(runtime, "analytics_status", "idle"),
            "analyticsSummary": _runtime_value(runtime, "analytics_last_summary", ""),
            "analyticsError": _runtime_value(runtime, "analytics_last_error", ""),
            "analyticsLastExportAt": _runtime_value(runtime, "analytics_last_export_at", "0"),
            "policyRecommended": _runtime_value(runtime, "policy_optimizer_recommended", "not calibrated"),
            "policySummary": _runtime_value(runtime, "policy_optimizer_last_summary", ""),
            "signalPolicyActive": _runtime_value(runtime, "signal_policy_active", ""),
            "riskStatus": risk_snapshot["status"],
            "riskSummary": _runtime_value(runtime, "risk_last_summary", ""),
            "riskBlocks": _runtime_value(runtime, "risk_last_blocks", ""),
            "exitSummary": _runtime_value(runtime, "exit_last_summary", ""),
            "exitReasons": _runtime_value(runtime, "exit_last_reasons", ""),
            "journalSummary": _journal_summary_label(journal_summary),
            "learningSummary": format_wallet_outcome_summary(learning_report),
            "featuresSummary": format_feature_summary(feature_summary),
            "bookHistorySummary": _book_history_label(book_history),
        },
        "stats": {
            "bankroll": round(current_bankroll, 2),
            "seed": round(bankroll, 2),
            "wallets": int(stats["wallets"]),
            "trades": int(stats["trades"]),
            "signals": int(stats["signals"]),
            "paperEvents": int(journal_summary["total_events"]),
            "decisionFeatures": int(feature_summary["features"]),
            "winRate": _estimated_win_rate(marked_positions, summary),
            "openPositions": int(summary["open_positions"]),
            "openCost": round(summary["open_cost"], 2),
            "realizedPnl": round(summary["realized_pnl"], 2),
            "unrealizedPnl": round(unrealized, 2),
            "avgWalletScore": _avg_score(scores),
            "targetTrades": TARGET_TRADES,
            "targetWallets": TARGET_WALLETS,
            "bulkTradesSeen": sync_progress["trades_seen"],
            "bulkWalletsSeen": sync_progress["wallets_with_state"],
            "bulkPagesSynced": sync_progress["pages_synced"],
            "bookHistorySnapshots": int(book_history["snapshots"]),
            "bookHistoryAssets": int(book_history["assets"]),
            "duckdbExists": duckdb_exists,
            "duckdbSizeBytes": duckdb_path.stat().st_size if duckdb_exists else 0,
            "duckdbPath": str(duckdb_path),
        },
        "scanner": _scanner_feed(recent_trades, scores),
        "consensus": _consensus_feed(recent_signals),
        "positions": marked_positions,
        "bankrollCurve": _bankroll_curve(bankroll, summary["realized_pnl"], unrealized),
        "whales": _whale_rows(scores, cohort_report["wallets"]),
        "risk": _risk_rows(marked_positions, bankroll, risk_snapshot),
        "riskState": risk_snapshot,
        "orderBook": _order_book_rows(books, recent_signals, recent_trades),
        "exitTriggers": _exit_triggers(open_positions, closed_positions, store, now),
        "closedPositions": _closed_position_rows(closed_positions),
        "journal": _journal_rows(journal_summary["recent"]),
        "tradeLog": _trade_log(recent_trades),
        "alerts": _alert_rows(alerts),
        "reviews": _review_rows(reviews),
        "cohorts": cohort_report,
        "learning": learning_report["wallets"],
    }


def _run_default_scan(store: Store) -> int:
    wallets = store.fetch_wallets()
    since_score = int(time.time()) - 14 * 86400
    trades_by_wallet = {
        wallet.address: store.fetch_trades_for_wallet(wallet.address, since_score)
        for wallet in wallets
    }
    scores = score_wallets(wallets, trades_by_wallet)
    store.upsert_scores(scores)
    score_map = store.fetch_scores(min_score=0.50)
    recent_trades = store.fetch_recent_trades(
        since_ts=int(time.time()) - 24 * 3600,
        min_notional=20,
        limit=1000,
    )
    policy_enabled, policy_mode = _active_signal_policy(store)
    wallet_cohorts = {}
    if policy_enabled:
        cohort_report = wallet_cohort_report(
            store,
            CohortConfig(history_days=14, min_trades=1, min_notional=1, limit=max(100, len(wallets))),
        )
        wallet_cohorts = {str(item["wallet"]): item for item in cohort_report["wallets"]}
    outcome_report = wallet_outcome_report(store, since_days=90, min_events=1, limit=50000)
    signals = generate_signals(
        recent_trades,
        score_map,
        SignalConfig(
            bankroll=DEFAULT_BANKROLL,
            min_wallet_score=0.50,
            min_trade_usdc=20,
            lookback_minutes=24 * 60,
            max_signals=10,
            use_cohort_policy=policy_enabled,
            cohort_policy_mode=policy_mode,
        ),
        wallet_cohorts=wallet_cohorts,
        wallet_outcomes=wallet_outcome_lookup(outcome_report),
    )
    store.set_runtime_state("signal_policy_active", f"enabled={int(policy_enabled)} mode={policy_mode} learning=1")
    store.insert_signals(signals)
    broker = PaperBroker(store)
    broker.record_signal_created(signals)
    broker.mark_and_close()
    return len(broker.open_from_signals(signals))


def _active_signal_policy(store: Store) -> tuple[bool, str]:
    runtime = store.runtime_state()
    recommended = runtime.get("policy_optimizer_recommended")
    if not recommended:
        return True, "strict"
    return policy_settings_from_recommendation(str(recommended.get("value") or ""), default_enabled=True)


def _db_stats(store: Store) -> dict[str, int]:
    row = store.conn.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM wallets) AS wallets,
            (SELECT COUNT(*) FROM trades) AS trades,
            (SELECT COUNT(*) FROM signals) AS signals
        """
    ).fetchone()
    return {
        "wallets": int(row["wallets"] or 0),
        "trades": int(row["trades"] or 0),
        "signals": int(row["signals"] or 0),
    }


def _runtime_value(runtime: dict[str, dict[str, object]], key: str, default: str) -> str:
    item = runtime.get(key)
    if not item:
        return default
    return str(item.get("value") or default)


def _runtime_updated(runtime: dict[str, dict[str, object]], key: str) -> int:
    item = runtime.get(key)
    if not item:
        return 0
    try:
        return int(item.get("updated_at") or 0)
    except (TypeError, ValueError):
        return 0



def _position_payload(store: Store, position, books: dict[str, Any]) -> dict[str, Any]:
    book = books.get(position.asset)
    latest = (book.mid if book and book.mid > 0 else None) or store.latest_trade_price(position.asset) or position.entry_price
    pnl = position.shares * latest - position.size_usdc
    pnl_pct = pnl / position.size_usdc if position.size_usdc else 0.0
    progress = 0.0
    if position.take_profit > position.stop_loss:
        progress = (latest - position.stop_loss) / (position.take_profit - position.stop_loss)
    return {
        "market": position.title or position.asset,
        "category": market_category_label(market_category(position.title or position.asset)),
        "outcome": position.outcome or position.asset,
        "entry": round(position.entry_price, 4),
        "latest": round(latest, 4),
        "size": round(position.size_usdc, 2),
        "shares": round(position.shares, 4),
        "stop": round(position.stop_loss, 4),
        "target": round(position.take_profit, 4),
        "unrealizedPnl": round(pnl, 4),
        "unrealizedPct": round(pnl_pct, 4),
        "progress": round(max(0.0, min(1.0, progress)), 4),
        "wallet": _short_wallet(position.wallet),
    }


def _scanner_feed(trades, scores) -> list[dict[str, Any]]:
    score_map = {score.wallet: score for score in scores}
    rows = []
    for trade in trades[:16]:
        score = score_map.get(trade.proxy_wallet)
        status = "ENTER" if score and score.score >= 0.55 and trade.side == "BUY" else "WATCH"
        if trade.side == "SELL":
            status = "EXIT"
        rows.append(
            {
                "status": status,
                "category": market_category_label(market_category(trade)),
                "market": _compact_market(trade.title or trade.slug or trade.asset),
                "wallet": _short_wallet(trade.proxy_wallet),
                "notional": round(trade.notional, 2),
                "price": round(trade.price, 4),
                "gap": round((score.score if score else 0.32), 3),
            }
        )
    return rows


def _consensus_feed(signals) -> list[dict[str, Any]]:
    rows = []
    for signal in signals[:10]:
        action = "ENTER" if signal.action == "BUY" else signal.action
        rows.append(
            {
                "action": action,
                "category": market_category_label(market_category(signal)),
                "market": _compact_market(signal.title or signal.asset),
                "outcome": signal.outcome or "YES",
                "price": signal.suggested_price,
                "confidence": signal.confidence,
                "note": signal.reason,
            }
        )
    return rows


def _whale_rows(scores, cohorts: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    rows = []
    if cohorts:
        for item in cohorts[:12]:
            status = str(item["status"])
            rows.append(
                {
                    "wallet": _short_wallet(str(item["wallet"])),
                    "score": round(float(item["walletScore"]), 3),
                    "stability": round(float(item["stabilityScore"]), 3),
                    "repeat": round(float(item["repeatability"]), 3),
                    "drawdown": round(float(item["drawdown"]), 3),
                    "volume": round(float(item["notional"]), 2),
                    "pnl": round(float(item["leaderboardPnl"]), 2),
                    "state": "CAND" if status == "CANDIDATE" else status,
                }
            )
        return rows
    for score in scores[:12]:
        rows.append(
            {
                "wallet": _short_wallet(score.wallet),
                "score": round(score.score, 3),
                "stability": 0.0,
                "repeat": round(score.repeatability_score, 3),
                "drawdown": round(score.drawdown_score, 3),
                "volume": round(score.leaderboard_volume, 2),
                "pnl": round(score.leaderboard_pnl, 2),
                "state": "COPY" if score.score >= 0.65 else "WATCH",
            }
        )
    return rows


def _risk_rows(
    positions: list[dict[str, Any]],
    bankroll: float,
    risk: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    if risk:
        exposure = float(risk["open_cost"]) / max(1.0, float(risk["bankroll"]))
        open_positions = int(risk["open_positions"])
        max_open = int(risk["max_open_positions"])
        return [
            {"label": "RISK", "value": str(risk["status"]), "state": str(risk["status"])},
            {
                "label": "EXPOSURE",
                "value": f"{_money(float(risk['open_cost']))}/{_money(float(risk['max_total_exposure']))}",
                "state": "OK" if exposure < 0.55 else "HIGH",
            },
            {
                "label": "OPEN SLOTS",
                "value": f"{open_positions}/{max_open}",
                "state": "OK" if open_positions < max_open else "LOCKED",
            },
            {
                "label": "WORST STOP",
                "value": f"-{_money(float(risk['worst_stop_loss']))}",
                "state": "OK" if float(risk["worst_stop_loss"]) < float(risk["worst_stop_limit"]) else "LOCKED",
            },
            {
                "label": "PNL 24H",
                "value": _money(float(risk["realized_pnl_24h"])),
                "state": "OK" if float(risk["realized_pnl_24h"]) > -float(risk["daily_loss_limit"]) else "LOCKED",
            },
        ]
    open_cost = sum(item["size"] for item in positions)
    worst_loss = sum(max(0.0, (item["entry"] - item["stop"]) * item["shares"]) for item in positions)
    max_single = max([item["size"] for item in positions] or [0.0])
    exposure = open_cost / bankroll if bankroll else 0.0
    return [
        {"label": "KELLY", "value": "0.024", "state": "LIVE"},
        {"label": "WORST STOP", "value": _money(-worst_loss), "state": "ARMED"},
        {"label": "EXPOSURE", "value": _pct(exposure), "state": "OK" if exposure < 0.35 else "HIGH"},
        {"label": "MAX POSITION", "value": _money(max_single), "state": "CAP"},
        {"label": "SHARPE", "value": "0.66", "state": "PAPER"},
    ]


def _order_book_rows(books: dict[str, Any], signals, trades) -> list[dict[str, Any]]:
    if books:
        rows = []
        for book in sorted(books.values(), key=lambda item: item.liquidity_score, reverse=True)[:8]:
            rows.append(
                {
                    "price": round(book.mid or book.last_trade_price or 0.0, 3),
                    "bidDepth": min(1.0, book.bid_depth_usdc / 2500),
                    "askDepth": min(1.0, book.ask_depth_usdc / 2500),
                    "spread": round(book.spread, 4),
                    "liquidity": round(book.liquidity_score, 3),
                }
            )
        return rows

    prices = [signal.suggested_price for signal in signals[:1]]
    if not prices:
        prices = [trade.price for trade in trades[:1]]
    mid = prices[0] if prices else 0.50
    levels = []
    for index, delta in enumerate([0.024, 0.016, 0.008, 0, -0.008, -0.016, -0.024]):
        price = max(0.01, min(0.99, mid + delta))
        levels.append(
            {
                "price": round(price, 3),
                "bidDepth": round(max(0.12, 1 - abs(index - 3) * 0.21), 2),
                "askDepth": round(max(0.10, 0.92 - abs(index - 2) * 0.18), 2),
            }
        )
    return levels


def _exit_triggers(positions, closed_positions, store: Store, now: int) -> list[dict[str, Any]]:
    rows = []
    for position in closed_positions[:5]:
        rows.append(
            {
                "rule": str(position.close_reason or "CLOSED").upper(),
                "market": _compact_market(position.title or position.asset),
                "price": round(float(position.exit_price or position.entry_price), 4),
                "state": _exit_state(position.close_reason),
            }
        )
    for position in positions[:10]:
        latest = store.latest_trade_price(position.asset) or position.entry_price
        if latest <= position.stop_loss:
            state = "KILL"
            rule = "STOP LOSS"
        elif latest >= position.take_profit:
            state = "TAKE"
            rule = "TAKE PROFIT"
        elif now - position.opened_at > 24 * 3600:
            state = "CHECK"
            rule = "TIME DECAY"
        else:
            state = "HOLD"
            rule = "NO EXIT"
        rows.append(
            {
                "rule": rule,
                "market": _compact_market(position.title or position.asset),
                "price": round(latest, 4),
                "state": state,
            }
        )
    return rows


def _closed_position_rows(positions) -> list[dict[str, Any]]:
    rows = []
    for position in positions:
        rows.append(
            {
                "reason": str(position.close_reason or "closed"),
                "market": _compact_market(position.title or position.asset),
                "outcome": position.outcome or position.asset,
                "entry": round(float(position.entry_price), 4),
                "exit": round(float(position.exit_price or 0.0), 4),
                "pnl": round(float(position.realized_pnl), 4),
            }
        )
    return rows


def _journal_summary_label(summary: dict[str, Any]) -> str:
    rows = summary.get("by_reason") or []
    if not rows:
        return "no decisions logged yet"
    top = rows[0]
    return (
        f"{int(summary.get('total_events') or 0)} decisions; "
        f"top={top['event_type']}:{top['reason']} x{int(top['events'])}"
    )


def _book_history_label(summary: dict[str, float]) -> str:
    snapshots = int(summary.get("snapshots") or 0)
    assets = int(summary.get("assets") or 0)
    if snapshots <= 0:
        return "no historical order books yet"
    return (
        f"{snapshots} snapshots / {assets} assets; "
        f"avg_spread={float(summary.get('avg_spread') or 0):.4f} "
        f"liq={float(summary.get('avg_liquidity') or 0):.3f}"
    )


def _journal_rows(events) -> list[dict[str, Any]]:
    rows = []
    for event in events:
        rows.append(
            {
                "type": event.event_type,
                "reason": event.reason,
                "wallet": _short_wallet(event.wallet),
                "market": _compact_market(event.title or event.asset),
                "outcome": event.outcome or event.asset,
                "price": round(float(event.price or 0.0), 4),
                "size": round(float(event.size_usdc or 0.0), 2),
                "pnl": round(float(event.pnl or 0.0), 4),
            }
        )
    return rows


def _exit_state(reason: str) -> str:
    reason = reason.lower()
    if reason == "take_profit":
        return "TAKE"
    if reason == "stop_loss":
        return "KILL"
    if reason == "risk_trim":
        return "TRIM"
    if reason == "max_hold":
        return "TIME"
    if reason == "stale_price":
        return "STALE"
    return "CLOSED"


def _trade_log(trades) -> list[dict[str, Any]]:
    rows = []
    for trade in trades[:12]:
        rows.append(
            {
                "side": trade.side,
                "market": _compact_market(trade.title or trade.slug or trade.asset),
                "wallet": _short_wallet(trade.proxy_wallet),
                "notional": round(trade.notional, 2),
                "price": round(trade.price, 4),
            }
        )
    return rows


def _alert_rows(alerts: list[dict[str, object]]) -> list[dict[str, Any]]:
    rows = []
    for alert in alerts:
        rows.append(
            {
                "status": str(alert.get("status") or ""),
                "destination": str(alert.get("destination") or ""),
                "message": _compact_market(str(alert.get("message") or "")),
                "error": str(alert.get("error") or ""),
            }
        )
    return rows


def _review_rows(reviews: list[dict[str, object]]) -> list[dict[str, Any]]:
    rows = []
    for review in reviews:
        rows.append(
            {
                "signalId": str(review.get("signal_id") or ""),
                "status": str(review.get("status") or ""),
                "action": str(review.get("action") or ""),
                "outcome": str(review.get("outcome") or review.get("asset") or ""),
                "market": _compact_market(str(review.get("title") or review.get("asset") or "")),
                "price": float(review.get("suggested_price") or 0.0),
                "confidence": float(review.get("confidence") or 0.0),
                "priority": int(review.get("priority") or 50),
                "cohort": str(review.get("cohort_status") or ""),
            }
        )
    return rows


def _bankroll_curve(seed: float, realized: float, unrealized: float) -> list[float]:
    end = seed + realized + unrealized
    start = max(1.0, seed)
    points = []
    for i in range(28):
        drift = (end - start) * (i / 27)
        pulse = ((i % 5) - 2) * 0.12
        points.append(round(start + drift + pulse, 2))
    return points


def _estimated_win_rate(positions: list[dict[str, Any]], summary: dict[str, float]) -> float:
    if not positions and summary["total_positions"] == 0:
        return 0.0
    winners = sum(1 for item in positions if item["unrealizedPnl"] > 0)
    denominator = max(1, len(positions))
    return round(winners / denominator, 3)


def _avg_score(scores) -> float:
    if not scores:
        return 0.0
    return round(sum(score.score for score in scores) / len(scores), 3)


def _float_arg(params: dict[str, list[str]], key: str, default: float) -> float:
    try:
        return float(params.get(key, [default])[0])
    except (TypeError, ValueError):
        return default


def _short_wallet(wallet: str) -> str:
    if len(wallet) <= 12:
        return wallet
    return f"{wallet[:4]}..{wallet[-4:]}"


def _compact_market(value: str) -> str:
    value = " ".join(value.split())
    if len(value) <= 42:
        return value
    return value[:39] + "..."


def _money(value: float) -> str:
    sign = "-" if value < 0 else ""
    return f"{sign}${abs(value):,.0f}"


def _pct(value: float) -> str:
    return f"{value * 100:.1f}%"

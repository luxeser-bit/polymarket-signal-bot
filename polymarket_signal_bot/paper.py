from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from typing import Any

from .models import PaperPosition, Signal
from .storage import Store


@dataclass(frozen=True)
class RiskConfig:
    bankroll: float = 200.0
    max_total_exposure_pct: float = 0.65
    max_position_usdc: float = 25.0
    max_market_exposure_usdc: float = 40.0
    max_wallet_exposure_usdc: float = 60.0
    max_open_positions: int = 20
    max_new_positions_per_run: int = 6
    max_daily_realized_loss_usdc: float = 20.0
    max_worst_stop_loss_usdc: float = 35.0

    @property
    def max_total_exposure_usdc(self) -> float:
        return max(0.0, self.bankroll * self.max_total_exposure_pct)


class PaperBroker:
    def __init__(self, store: Store, risk_config: RiskConfig | None = None) -> None:
        self.store = store
        self.risk_config = risk_config or RiskConfig()

    def open_from_signals(
        self,
        signals: list[Signal],
        *,
        respect_auto_policy: bool = True,
        risk_config: RiskConfig | None = None,
    ) -> list[PaperPosition]:
        risk_config = risk_config or self.risk_config
        opened: list[PaperPosition] = []
        blocked: dict[str, int] = {}
        now = int(time.time())
        existing_positions = self.store.fetch_open_positions()
        for signal in signals:
            if signal.action != "BUY":
                continue
            if respect_auto_policy and not _auto_open_allowed(signal):
                _count(blocked, "cohort_auto_policy")
                continue
            if signal.suggested_price <= 0:
                _count(blocked, "invalid_price")
                continue
            current_positions = existing_positions + opened
            if _has_open_position_for_asset(current_positions, signal.asset):
                _count(blocked, "duplicate_asset")
                continue
            risk_reason = self._risk_block_reason(signal, current_positions, len(opened), risk_config, now)
            if risk_reason:
                _count(blocked, risk_reason)
                continue
            shares = signal.size_usdc / signal.suggested_price
            position = PaperPosition(
                position_id=_position_id(signal.signal_id, signal.asset),
                signal_id=signal.signal_id,
                opened_at=now,
                wallet=signal.wallet,
                condition_id=signal.condition_id,
                asset=signal.asset,
                outcome=signal.outcome,
                title=signal.title,
                entry_price=signal.suggested_price,
                size_usdc=signal.size_usdc,
                shares=round(shares, 6),
                stop_loss=signal.stop_loss,
                take_profit=signal.take_profit,
            )
            opened.append(position)
        self.store.insert_paper_positions(opened)
        self._record_risk_state(risk_config, opened_count=len(opened), blocked=blocked, now=now)
        return opened

    def open_approved(self, *, limit: int = 25) -> list[PaperPosition]:
        return self.open_from_signals(
            self.store.fetch_approved_unopened_signals(limit=limit),
            respect_auto_policy=False,
        )

    def risk_snapshot(self, risk_config: RiskConfig | None = None, *, now: int | None = None) -> dict[str, Any]:
        risk_config = risk_config or self.risk_config
        now = now or int(time.time())
        positions = self.store.fetch_open_positions()
        open_cost = _open_cost(positions)
        worst_stop_loss = _worst_stop_loss(positions)
        realized_pnl_24h = self.store.realized_pnl_since(now - 24 * 3600)
        max_market = max((_market_exposure(positions, position.condition_id) for position in positions), default=0.0)
        max_wallet = max((_wallet_exposure(positions, position.wallet) for position in positions), default=0.0)
        status = _risk_status(
            positions=positions,
            open_cost=open_cost,
            worst_stop_loss=worst_stop_loss,
            realized_pnl_24h=realized_pnl_24h,
            max_market_exposure=max_market,
            max_wallet_exposure=max_wallet,
            config=risk_config,
        )
        return {
            "status": status,
            "bankroll": round(risk_config.bankroll, 2),
            "open_positions": len(positions),
            "max_open_positions": risk_config.max_open_positions,
            "open_cost": round(open_cost, 2),
            "max_total_exposure": round(risk_config.max_total_exposure_usdc, 2),
            "exposure_pct": round(open_cost / max(1.0, risk_config.bankroll), 4),
            "max_market_exposure": round(max_market, 2),
            "market_limit": round(risk_config.max_market_exposure_usdc, 2),
            "max_wallet_exposure": round(max_wallet, 2),
            "wallet_limit": round(risk_config.max_wallet_exposure_usdc, 2),
            "worst_stop_loss": round(worst_stop_loss, 2),
            "worst_stop_limit": round(risk_config.max_worst_stop_loss_usdc, 2),
            "realized_pnl_24h": round(realized_pnl_24h, 2),
            "daily_loss_limit": round(risk_config.max_daily_realized_loss_usdc, 2),
        }

    def mark_and_close(self) -> list[tuple[PaperPosition, float, str]]:
        closed: list[tuple[PaperPosition, float, str]] = []
        now = int(time.time())
        for position in self.store.fetch_open_positions():
            price = self.store.latest_trade_price(position.asset)
            if price is None:
                continue
            reason = ""
            if price <= position.stop_loss:
                reason = "stop_loss"
            elif price >= position.take_profit:
                reason = "take_profit"
            if not reason:
                continue
            pnl = position.shares * price - position.size_usdc
            self.store.close_position(position.position_id, now, price, round(pnl, 4))
            closed.append((position, price, reason))
        return closed

    def _risk_block_reason(
        self,
        signal: Signal,
        current_positions: list[PaperPosition],
        opened_this_run: int,
        config: RiskConfig,
        now: int,
    ) -> str:
        if opened_this_run >= config.max_new_positions_per_run:
            return "run_position_cap"
        if len(current_positions) >= config.max_open_positions:
            return "open_position_cap"
        if self.store.realized_pnl_since(now - 24 * 3600) <= -abs(config.max_daily_realized_loss_usdc):
            return "daily_loss_lock"
        if signal.size_usdc <= 0:
            return "invalid_size"
        if signal.size_usdc > config.max_position_usdc:
            return "position_size_cap"
        if _open_cost(current_positions) + signal.size_usdc > config.max_total_exposure_usdc:
            return "total_exposure_cap"
        if _market_exposure(current_positions, signal.condition_id) + signal.size_usdc > config.max_market_exposure_usdc:
            return "market_exposure_cap"
        if _wallet_exposure(current_positions, signal.wallet) + signal.size_usdc > config.max_wallet_exposure_usdc:
            return "wallet_exposure_cap"
        if _worst_stop_loss(current_positions) + _signal_stop_risk(signal) > config.max_worst_stop_loss_usdc:
            return "worst_stop_cap"
        return ""

    def _record_risk_state(
        self,
        risk_config: RiskConfig,
        *,
        opened_count: int,
        blocked: dict[str, int],
        now: int,
    ) -> None:
        snapshot = self.risk_snapshot(risk_config, now=now)
        block_summary = ",".join(f"{reason}:{count}" for reason, count in sorted(blocked.items())) or "none"
        summary = (
            f"status={snapshot['status']} "
            f"exposure=${snapshot['open_cost']:.2f}/${snapshot['max_total_exposure']:.2f} "
            f"open={snapshot['open_positions']}/{snapshot['max_open_positions']} "
            f"worst_stop=${snapshot['worst_stop_loss']:.2f}/${snapshot['worst_stop_limit']:.2f} "
            f"pnl24h=${snapshot['realized_pnl_24h']:.2f} "
            f"opened={opened_count} blocked={sum(blocked.values())}"
        )
        self.store.set_runtime_state("risk_status", str(snapshot["status"]))
        self.store.set_runtime_state("risk_last_summary", summary)
        self.store.set_runtime_state("risk_last_blocks", block_summary)


def _position_id(signal_id: str, asset: str) -> str:
    return hashlib.sha256(f"{signal_id}|{asset}".encode("utf-8")).hexdigest()[:32]


def _auto_open_allowed(signal: Signal) -> bool:
    if "cohort=" not in signal.reason:
        return True
    return "auto_open=1" in signal.reason


def _has_open_position_for_asset(positions: list[PaperPosition], asset: str) -> bool:
    return any(position.asset == asset and position.status == "OPEN" for position in positions)


def _open_cost(positions: list[PaperPosition]) -> float:
    return sum(position.size_usdc for position in positions if position.status == "OPEN")


def _market_exposure(positions: list[PaperPosition], condition_id: str) -> float:
    return sum(
        position.size_usdc
        for position in positions
        if position.status == "OPEN" and position.condition_id == condition_id
    )


def _wallet_exposure(positions: list[PaperPosition], wallet: str) -> float:
    return sum(
        position.size_usdc
        for position in positions
        if position.status == "OPEN" and position.wallet == wallet
    )


def _position_stop_risk(position: PaperPosition) -> float:
    return max(0.0, (position.entry_price - position.stop_loss) * position.shares)


def _signal_stop_risk(signal: Signal) -> float:
    if signal.suggested_price <= 0:
        return 0.0
    shares = signal.size_usdc / signal.suggested_price
    return max(0.0, (signal.suggested_price - signal.stop_loss) * shares)


def _worst_stop_loss(positions: list[PaperPosition]) -> float:
    return sum(_position_stop_risk(position) for position in positions if position.status == "OPEN")


def _risk_status(
    *,
    positions: list[PaperPosition],
    open_cost: float,
    worst_stop_loss: float,
    realized_pnl_24h: float,
    max_market_exposure: float,
    max_wallet_exposure: float,
    config: RiskConfig,
) -> str:
    if realized_pnl_24h <= -abs(config.max_daily_realized_loss_usdc):
        return "LOCKED"
    if len(positions) >= config.max_open_positions:
        return "LOCKED"
    if open_cost >= config.max_total_exposure_usdc:
        return "LOCKED"
    if worst_stop_loss >= config.max_worst_stop_loss_usdc:
        return "LOCKED"
    if max_market_exposure >= config.max_market_exposure_usdc:
        return "LOCKED"
    if max_wallet_exposure >= config.max_wallet_exposure_usdc:
        return "LOCKED"
    if open_cost >= config.max_total_exposure_usdc * 0.85:
        return "WARN"
    if worst_stop_loss >= config.max_worst_stop_loss_usdc * 0.85:
        return "WARN"
    return "OK"


def _count(values: dict[str, int], key: str) -> None:
    values[key] = values.get(key, 0) + 1

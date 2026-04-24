from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass

from .models import Signal
from .storage import Store


@dataclass(frozen=True)
class TelegramConfig:
    token: str = ""
    chat_id: str = ""
    timeout_seconds: int = 15
    dry_run: bool = False

    @property
    def enabled(self) -> bool:
        return bool(self.token and self.chat_id)


class TelegramNotifier:
    destination = "telegram"

    def __init__(self, config: TelegramConfig) -> None:
        self.config = config

    def notify_new_signals(self, store: Store, *, limit: int = 10) -> int:
        signals = store.fetch_unsent_signals_for_destination(self.destination, limit=limit)
        sent = 0
        for signal in signals:
            message = format_signal_message(signal)
            inserted = store.record_alert_event(
                signal_id=signal.signal_id,
                destination=self.destination,
                status="PENDING" if self.config.enabled else "SKIPPED",
                message=message,
            )
            if not inserted:
                continue
            if not self.config.enabled:
                continue
            if self.config.dry_run:
                store.update_alert_event_status(
                    signal_id=signal.signal_id,
                    destination=self.destination,
                    status="DRY_RUN",
                    sent_at=int(time.time()),
                )
                sent += 1
                continue
            try:
                self._send(message)
                store.update_alert_event_status(
                    signal_id=signal.signal_id,
                    destination=self.destination,
                    status="SENT",
                    sent_at=int(time.time()),
                )
                sent += 1
            except Exception as exc:  # noqa: BLE001 - preserve notifier loop.
                store.update_alert_event_status(
                    signal_id=signal.signal_id,
                    destination=self.destination,
                    status="ERROR",
                    error=str(exc)[:300],
                )
        return sent

    def _send(self, message: str) -> None:
        endpoint = f"https://api.telegram.org/bot{self.config.token}/sendMessage"
        payload = urllib.parse.urlencode(
            {
                "chat_id": self.config.chat_id,
                "text": message,
                "disable_web_page_preview": "true",
            }
        ).encode("utf-8")
        request = urllib.request.Request(endpoint, data=payload, method="POST")
        with urllib.request.urlopen(request, timeout=self.config.timeout_seconds) as response:
            body = response.read().decode("utf-8")
        data = json.loads(body)
        if not data.get("ok"):
            raise RuntimeError(body[:300])


def format_signal_message(signal: Signal) -> str:
    title = signal.title or signal.asset
    if len(title) > 120:
        title = title[:117] + "..."
    return "\n".join(
        [
            "POLYSIGNAL PAPER ALERT",
            f"{signal.action} {signal.outcome or signal.asset}",
            title,
            f"entry <= {signal.suggested_price:.3f} | size ${signal.size_usdc:.2f}",
            f"confidence {signal.confidence * 100:.0f}% | wallet {short_wallet(signal.wallet)}",
            f"stop {signal.stop_loss:.3f} | target {signal.take_profit:.3f}",
            signal.reason,
        ]
    )


def short_wallet(wallet: str) -> str:
    if len(wallet) <= 12:
        return wallet
    return f"{wallet[:6]}...{wallet[-4:]}"

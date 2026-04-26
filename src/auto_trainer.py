from __future__ import annotations

"""Compatibility entry point for ``python -m src.auto_trainer``."""

from polymarket_signal_bot.auto_trainer import *  # noqa: F401,F403
from polymarket_signal_bot.auto_trainer import main


if __name__ == "__main__":
    raise SystemExit(main())

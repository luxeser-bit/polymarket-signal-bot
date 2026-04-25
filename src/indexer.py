from __future__ import annotations

"""Compatibility entry point for ``python -m src.indexer``."""

from polymarket_signal_bot.indexer import *  # noqa: F401,F403
from polymarket_signal_bot.indexer import main


if __name__ == "__main__":
    raise SystemExit(main())

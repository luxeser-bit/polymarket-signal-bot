from __future__ import annotations

"""Compatibility entry point for ``python -m src.dune_fetcher``."""

from polymarket_signal_bot.dune_fetcher import *  # noqa: F401,F403
from polymarket_signal_bot.dune_fetcher import main


if __name__ == "__main__":
    raise SystemExit(main())

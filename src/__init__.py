from __future__ import annotations

"""Compatibility aliases for older ``src.*`` imports.

The actual package is ``polymarket_signal_bot``. This module keeps imports such
as ``from src.live_paper_runner import LivePaperRunner`` working for tools or
agents that still refer to the project as ``src``.
"""

import importlib
import sys


_MODULES = [
    "api",
    "alerts",
    "analytics",
    "backtest",
    "bulk_sync",
    "cli",
    "cohorts",
    "consensus_engine",
    "dashboard",
    "demo",
    "features",
    "learning",
    "live_paper_runner",
    "market_flow",
    "models",
    "monitor",
    "paper",
    "policy_optimizer",
    "scoring",
    "signals",
    "storage",
    "streaming",
    "taxonomy",
]


for _name in _MODULES:
    _module = importlib.import_module(f"polymarket_signal_bot.{_name}")
    sys.modules.setdefault(f"{__name__}.{_name}", _module)


__all__ = list(_MODULES)

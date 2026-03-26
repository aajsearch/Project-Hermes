#!/usr/bin/env python3
"""
Run a single V2 hourly pipeline cycle as a canary (no infinite threads).

This is intended for safe verification:
- force hourly enabled in-memory
- force enabled_assets=[asset] in-memory
- optionally force shadow-mode (dry-run) for hourly regardless of config

Usage:
  python tools/v2_hourly_canary_once.py --asset btc --shadow
  python tools/v2_hourly_canary_once.py --asset btc --shadow --enable-strategies
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure repo root is importable when running as a script.
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.client.kalshi_client import KalshiClient

from bot.pipeline.aggregator import OrderAggregator
from bot.pipeline.data_layer import DataLayer
from bot.pipeline.executor import PipelineExecutor
from bot.pipeline.registry import OrderRegistry, init_v2_db
from bot.pipeline.run_unified import run_pipeline_cycle
from bot.pipeline.tick_logger import TickLogger
from bot.pipeline.strategies.hourly_last_90s_limit_99 import HourlyLast90sLimit99Strategy
from bot.pipeline.strategies.hourly_signals_farthest import HourlySignalsFarthestStrategy
from bot.v2_config_loader import load_v2_config


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--asset", required=True, choices=["btc", "eth", "sol", "xrp", "doge"])
    ap.add_argument("--shadow", action="store_true", help="Force dry-run execution for hourly (no orders placed).")
    ap.add_argument(
        "--enable-strategies",
        action="store_true",
        help="Temporarily set both hourly strategies enabled=true in-memory (config files remain unchanged).",
    )
    args = ap.parse_args()

    config = load_v2_config()
    init_v2_db()

    # Enable hourly interval in-memory and restrict to single asset.
    config.setdefault("intervals", {}).setdefault("hourly", {})["enabled"] = True
    config["intervals"]["hourly"]["assets"] = [args.asset]

    # Feature flag: allow this one asset.
    config.setdefault("feature_flags", {}).setdefault("v2_hourly", {})["enabled_assets"] = [args.asset]
    config["feature_flags"]["v2_hourly"]["shadow_mode"] = bool(args.shadow)

    if args.enable_strategies:
        hourly = config.get("hourly") or {}
        strategies = hourly.get("strategies") if isinstance(hourly, dict) else None
        if isinstance(strategies, dict):
            for k in ("hourly_signals_farthest", "hourly_last_90s_limit_99"):
                if isinstance(strategies.get(k), dict):
                    strategies[k]["enabled"] = True

    kalshi_client = KalshiClient()
    registry = OrderRegistry()
    data_layer = DataLayer(kalshi_client=kalshi_client)
    aggregator = OrderAggregator()
    executor = PipelineExecutor(registry, dry_run=True, kalshi_client=kalshi_client)  # always safe for canary
    tick_logger = TickLogger()

    strategies = [
        HourlyLast90sLimit99Strategy(config),
        HourlySignalsFarthestStrategy(config),
    ]

    run_pipeline_cycle(
        "hourly",
        config,
        data_layer,
        strategies,
        aggregator,
        executor,
        registry,
        kalshi_client=kalshi_client,
        tick_logger=tick_logger,
    )
    registry.close()
    print("OK: ran one hourly canary cycle (dry-run).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


"""
Bot V2 entry point: unified pipeline with configurable intervals (fifteen_min, hourly).
Loads v2 config, initializes v2_state.db, runs pipeline cycles per interval in separate threads.
DO NOT modify V1 code; this is greenfield.
"""
from __future__ import annotations

import logging
import os
import sys
import threading
import time
from pathlib import Path
from typing import List

from dotenv import load_dotenv

from src.client.kalshi_client import KalshiClient

from bot.pipeline.aggregator import OrderAggregator
from bot.pipeline.data_layer import DataLayer
from bot.pipeline.executor import PipelineExecutor
from bot.pipeline.registry import OrderRegistry, init_v2_db
from bot.pipeline.run_unified import run_pipeline_cycle
from bot.pipeline.tick_logger import TickLogger
from bot.pipeline.strategies.atm_breakout import AtmBreakoutStrategy
from bot.pipeline.strategies.hourly_last_90s_limit_99 import HourlyLast90sLimit99Strategy
from bot.pipeline.strategies.hourly_signals_farthest import HourlySignalsFarthestStrategy
from bot.pipeline.strategies.knife_catcher import KnifeCatcherStrategy
from bot.pipeline.strategies.last_90s import Last90sStrategy
from bot.v2_config_loader import load_v2_config


def _configure_logging() -> None:
    """
    Log to both stdout and a persistent file (append across restarts).
    File path: data/console.log
    """
    log_path = Path(__file__).resolve().parents[1] / "data" / "console.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # Avoid duplicate handlers if main() is invoked multiple times in-process.
    if getattr(root, "_v2_handlers_configured", False):
        return

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)

    fh = logging.FileHandler(str(log_path), mode="a", encoding="utf-8")
    fh.setFormatter(fmt)

    root.handlers = [sh, fh]
    setattr(root, "_v2_handlers_configured", True)


logger = logging.getLogger(__name__)


def _resolve_dry_run(config: dict) -> bool:
    """Resolve dry_run: env V2_DRY_RUN overrides config. Default True (no real orders)."""
    raw = os.environ.get("V2_DRY_RUN", "").strip().lower()
    if raw in ("false", "0", "no"):
        return False
    if raw in ("true", "1", "yes"):
        return True
    return bool(config.get("dry_run", True))


def main() -> None:
    _configure_logging()
    load_dotenv()
    logger.info("Environment variables loaded.")

    config = load_v2_config()
    init_v2_db()

    dry_run = _resolve_dry_run(config)
    kalshi_base = os.environ.get("KALSHI_BASE_URL", "")
    base_display = (kalshi_base or "NOT SET")[:50] + ("..." if len(kalshi_base or "") > 50 else "")

    if dry_run:
        logger.warning(
            "[V2] DRY RUN MODE — NO ORDERS WILL BE PLACED ON KALSHI. Set dry_run: false in config or V2_DRY_RUN=false to trade live."
        )
    else:
        logger.info("[V2] LIVE TRADING — Orders will be sent to Kalshi. Base URL: %s", base_display)

    kalshi_client = KalshiClient()
    registry = OrderRegistry()
    data_layer = DataLayer(kalshi_client=kalshi_client)

    # Start spot oracle WebSocket (single source: Coinbase) so pipeline can use spot=WS when data is fresh
    try:
        from bot.oracle_ws_manager import start_ws_oracles
        start_ws_oracles()
    except Exception as e:
        logger.debug("Oracle WS not started: %s", e)
    aggregator = OrderAggregator()
    executor = PipelineExecutor(registry, dry_run=dry_run, kalshi_client=kalshi_client)
    ff = config.get("feature_flags") or {}
    v2h = ff.get("v2_hourly") if isinstance(ff, dict) else {}
    shadow_mode = bool(v2h.get("shadow_mode", False)) if isinstance(v2h, dict) else False
    hourly_executor = PipelineExecutor(
        registry,
        dry_run=(True if shadow_mode else dry_run),
        kalshi_client=kalshi_client,
    )
    tick_logger = TickLogger()

    strat_last90s = Last90sStrategy(config)
    strat_atm = AtmBreakoutStrategy(config)
    strat_knife_catcher = KnifeCatcherStrategy(config)
    strat_hourly_last90s = HourlyLast90sLimit99Strategy(config)
    strat_hourly_regular = HourlySignalsFarthestStrategy(config)

    fifteen_min_strats = [strat_last90s, strat_knife_catcher, strat_atm]
    hourly_strats: list = [strat_hourly_last90s, strat_hourly_regular]

    def run_interval_loop(
        interval: str,
        strategies: list,
        kalshi_client: KalshiClient,
        executor: PipelineExecutor,
    ) -> None:
        interval_cfg = (config.get(interval) or {}).get("pipeline") or {}
        sleep_secs = float(interval_cfg.get("run_interval_seconds", 1))
        while True:
            try:
                run_pipeline_cycle(
                    interval,
                    config,
                    data_layer,
                    strategies,
                    aggregator,
                    executor,
                    registry,
                    kalshi_client=kalshi_client,
                    tick_logger=tick_logger,
                )
            except Exception as e:
                logger.exception("[%s] Pipeline cycle error: %s", interval, e)
            time.sleep(sleep_secs)

    threads: List[threading.Thread] = []
    intervals_cfg = config.get("intervals") or {}
    if intervals_cfg.get("fifteen_min", {}).get("enabled", False):
        t = threading.Thread(
            target=run_interval_loop,
            args=("fifteen_min", fifteen_min_strats, kalshi_client, executor),
            name="v2_fifteen_min",
            daemon=True,
        )
        t.start()
        threads.append(t)
        logger.info("Started fifteen_min pipeline thread")
    if intervals_cfg.get("hourly", {}).get("enabled", False):
        t = threading.Thread(
            target=run_interval_loop,
            args=("hourly", hourly_strats, kalshi_client, hourly_executor),
            name="v2_hourly",
            daemon=True,
        )
        t.start()
        threads.append(t)
        logger.info("Started hourly pipeline thread")

    if not threads:
        logger.warning("No intervals enabled in config; exiting")
        return

    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt, shutting down")
    finally:
        registry.close()


if __name__ == "__main__":
    main()

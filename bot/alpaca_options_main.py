"""
Alpaca multi-leg options runtime (PCS / CCS / IC) via multi-leg ``mleg`` limit orders.

Kalshi V2 is untouched; this is a separate Python process:
  python3 -m bot.alpaca_options_main
"""

from __future__ import annotations

import atexit
import logging
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

from bot.alpaca_put_spread.alpaca_clients import make_alpaca_clients
from bot.alpaca_put_spread.config import load_alpaca_options_config
from bot.alpaca_put_spread.db import init_alpaca_db
from bot.alpaca_put_spread.singleton_lock import acquire_singleton_lock, release_singleton_lock
from bot.alpaca_put_spread.strategy import AlpacaPutSpreadRunner


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

    if getattr(root, "_alpaca_handlers_configured", False):
        return

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    fh = logging.FileHandler(str(log_path), mode="a", encoding="utf-8")
    fh.setFormatter(fmt)

    root.handlers = [sh, fh]
    setattr(root, "_alpaca_handlers_configured", True)


logger = logging.getLogger(__name__)


def main() -> None:
    _configure_logging()
    load_dotenv()

    if not acquire_singleton_lock():
        sys.exit(1)
    atexit.register(release_singleton_lock)

    init_alpaca_db()
    cfg = load_alpaca_options_config()

    logger.info(
        "[ALPACA_OPTIONS] starting paper=%s execute=%s underlyings=%s target_credit=%.4f",
        cfg.paper,
        cfg.execute,
        cfg.get_underlyings_for_today(),
        cfg.target_credit,
    )

    trading_client, stock_data_client, option_data_client = make_alpaca_clients(paper=cfg.paper)

    runner = AlpacaPutSpreadRunner(
        trading_client=trading_client,
        stock_data_client=stock_data_client,
        option_data_client=option_data_client,
        cfg=cfg,
    )

    # A small sleep so Alpaca account state stabilizes after process start.
    time.sleep(1)
    # run_forever() calls sync_pending_orders_with_broker() at the top of each loop before pricing/entries.
    runner.run_forever()


if __name__ == "__main__":
    main()

"""
Single-instance guard for the Alpaca ETF scalper bot.
Uses an exclusive file lock on data/alpaca_etf_scalper.lock.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

LOCK_PATH = Path(__file__).resolve().parents[2] / "data" / "alpaca_etf_scalper.lock"
_lock_fd: Optional[int] = None


def acquire_singleton_lock() -> bool:
    global _lock_fd
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        _lock_fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            import fcntl

            fcntl.flock(_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except ImportError:
            logger.warning("fcntl not available; skipping singleton lock.")
            return True
        except OSError as e:
            if "Resource temporarily unavailable" in str(e) or "35" in str(e):
                # Ensure the user sees a message even if logging isn't configured yet.
                msg = "Another Alpaca ETF scalper bot is already running (lock held). Exiting."
                try:
                    print(msg, file=sys.stderr, flush=True)
                except Exception:
                    pass
                logger.error(msg)
                os.close(_lock_fd)
                _lock_fd = None
                return False
            raise
    except Exception as e:
        logger.exception("Failed to acquire singleton lock: %s", e)
        return False


def release_singleton_lock() -> None:
    global _lock_fd
    if _lock_fd is None:
        return
    try:
        import fcntl

        fcntl.flock(_lock_fd, fcntl.LOCK_UN)
    except Exception:
        pass
    try:
        os.close(_lock_fd)
    except Exception:
        pass
    _lock_fd = None


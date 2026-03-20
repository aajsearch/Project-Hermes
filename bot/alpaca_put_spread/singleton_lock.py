"""
Single-instance guard: ensures only one Alpaca put spread bot runs at a time.
Uses an exclusive file lock on data/alpaca_put_spread.lock.
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

LOCK_PATH = Path(__file__).resolve().parents[1].parent / "data" / "alpaca_put_spread.lock"
_lock_fd: Optional[int] = None


def acquire_singleton_lock() -> bool:
    """
    Acquire exclusive lock. Returns True if acquired, False if another instance holds it.
    Call release_singleton_lock() on exit. Uses fcntl on Unix; on Windows, skips lock (warning).
    """
    global _lock_fd
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        _lock_fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            import fcntl
            fcntl.flock(_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except ImportError:
            logger.warning("fcntl not available (Windows?); skipping singleton lock.")
            return True
        except OSError as e:
            if "Resource temporarily unavailable" in str(e) or "35" in str(e):
                logger.error("Another Alpaca put spread bot is already running (lock held). Exiting.")
                os.close(_lock_fd)
                _lock_fd = None
                return False
            raise
    except Exception as e:
        logger.exception("Failed to acquire singleton lock: %s", e)
        return False


def release_singleton_lock() -> None:
    """Release the lock. Idempotent."""
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

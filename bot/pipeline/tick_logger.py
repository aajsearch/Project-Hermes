"""
Tick Logger: in-memory tick-by-tick tracking per (window, asset), flushed as one row per asset per window.
Stores sec, yes_bid, no_bid, strike, k_spot, cb_spot as columnar arrays; at end of window
serializes to JSON and writes a single row to v2_tick_log (window_id, asset, tick_history_json).
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

TICK_LOG_TABLE = "v2_tick_log"


def _default_db_path() -> Path:
    return Path(__file__).resolve().parents[1].parent / "data" / "v2_state.db"


class TickTracker:
    """
    In-memory columnar storage for one (window_id, asset).
    Holds arrays: sec, yes_bid, no_bid, strike, k_spot, cb_spot.
    """

    __slots__ = ("sec", "yes_bid", "no_bid", "strike", "k_spot", "cb_spot")

    def __init__(self) -> None:
        self.sec: List[float] = []
        self.yes_bid: List[int] = []
        self.no_bid: List[int] = []
        self.strike: List[Optional[float]] = []
        self.k_spot: List[Optional[float]] = []
        self.cb_spot: List[Optional[float]] = []

    def append(
        self,
        sec: float,
        yes_bid: int,
        no_bid: int,
        strike: Optional[float],
        k_spot: Optional[float],
        cb_spot: Optional[float],
    ) -> None:
        self.sec.append(sec)
        self.yes_bid.append(yes_bid)
        self.no_bid.append(no_bid)
        self.strike.append(strike)
        self.k_spot.append(k_spot)
        self.cb_spot.append(cb_spot)

    def to_dict(self) -> Dict[str, List[Any]]:
        """Serialize to a dict suitable for JSON (one key per column, lists)."""
        return {
            "sec": list(self.sec),
            "yes_bid": list(self.yes_bid),
            "no_bid": list(self.no_bid),
            "strike": list(self.strike),
            "k_spot": list(self.k_spot),
            "cb_spot": list(self.cb_spot),
        }

    def __len__(self) -> int:
        return len(self.sec)


class TickLogger:
    """
    In-memory tracker per (window_id, asset). Update every tick; flush to DB when window ends.
    Single row per asset per window: window_id, asset, tick_history_json, created_at.
    """

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self._db_path = Path(db_path) if db_path is not None else _default_db_path()
        # window_id -> asset -> TickTracker
        self._trackers: Dict[str, Dict[str, TickTracker]] = {}
        self._ensure_table()

    def _ensure_table(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        try:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {TICK_LOG_TABLE} (
                    window_id TEXT NOT NULL,
                    asset TEXT NOT NULL,
                    tick_history_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    PRIMARY KEY (window_id, asset)
                )
                """
            )
            conn.commit()
        finally:
            conn.close()

    def _get_or_create_tracker(self, window_id: str, asset: str) -> TickTracker:
        if window_id not in self._trackers:
            self._trackers[window_id] = {}
        if asset not in self._trackers[window_id]:
            self._trackers[window_id][asset] = TickTracker()
        return self._trackers[window_id][asset]

    def record_tick(
        self,
        window_id: str,
        asset: str,
        sec: float,
        yes_bid: int,
        no_bid: int,
        strike: Optional[float] = None,
        k_spot: Optional[float] = None,
        cb_spot: Optional[float] = None,
    ) -> None:
        """
        Append one tick for (window_id, asset). Call every second from the pipeline.
        """
        tracker = self._get_or_create_tracker(window_id, asset)
        tracker.append(
            sec=float(sec),
            yes_bid=int(yes_bid),
            no_bid=int(no_bid),
            strike=float(strike) if strike is not None else None,
            k_spot=float(k_spot) if k_spot is not None else None,
            cb_spot=float(cb_spot) if cb_spot is not None else None,
        )

    def flush_window(self, window_id: str) -> int:
        """
        Serialize all trackers for this window to JSON and write one row per asset to the DB.
        Removes the trackers from memory. Returns number of rows written.
        """
        if window_id not in self._trackers or not self._trackers[window_id]:
            return 0
        assets_data = self._trackers.pop(window_id)
        created_at = time.time()
        rows_written = 0
        conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        try:
            for asset, tracker in assets_data.items():
                if len(tracker) == 0:
                    continue
                tick_history_json = json.dumps(tracker.to_dict(), allow_nan=False)
                conn.execute(
                    f"""
                    INSERT OR REPLACE INTO {TICK_LOG_TABLE}
                    (window_id, asset, tick_history_json, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (window_id, asset, tick_history_json, created_at),
                )
                rows_written += 1
            conn.commit()
        finally:
            conn.close()
        if rows_written:
            logger.info(
                "[TICK_LOG] Flushed window_id=%s assets=%s rows=%d",
                window_id,
                list(assets_data.keys()),
                rows_written,
            )
        return rows_written

"""
Central order registry and strategy reports for Bot V2 (v2_state.db).
Tracks every V2 order for ownership, exits, and caps; records trade outcomes.
"""
from __future__ import annotations

import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import List, Optional

from bot.pipeline.intents import OrderRecord

logger = logging.getLogger(__name__)

REGISTRY_TABLE = "v2_order_registry"
REPORTS_TABLE = "v2_strategy_reports"
SQLITE_TIMEOUT_SECONDS = 10.0
SQLITE_BUSY_TIMEOUT_MS = 5000


def _default_db_path() -> Path:
    return Path(__file__).resolve().parents[1].parent / "data" / "v2_state.db"


def init_v2_db(db_path: Optional[Path] = None) -> None:
    """
    Create v2_state.db and the two tables if they do not exist.
    Safe to call multiple times (idempotent).
    """
    path = db_path or _default_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), check_same_thread=False, timeout=SQLITE_TIMEOUT_SECONDS)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    try:
        conn.executescript(
            f"""
            CREATE TABLE IF NOT EXISTS {REGISTRY_TABLE} (
                order_id TEXT PRIMARY KEY,
                strategy_id TEXT NOT NULL,
                interval TEXT NOT NULL,
                market_id TEXT NOT NULL,
                asset TEXT NOT NULL,
                ticker TEXT NOT NULL,
                side TEXT NOT NULL,
                status TEXT NOT NULL,
                filled_count INTEGER DEFAULT 0,
                count INTEGER NOT NULL,
                limit_price_cents INTEGER,
                entry_fill_price_cents INTEGER,
                placed_at REAL NOT NULL,
                client_order_id TEXT,
                placement_bid_cents INTEGER,
                entry_distance REAL,
                entry_distance_at_fill REAL
            );
            CREATE INDEX IF NOT EXISTS idx_registry_strategy_interval_market_asset
                ON {REGISTRY_TABLE}(strategy_id, interval, market_id, asset);
            CREATE INDEX IF NOT EXISTS idx_registry_status ON {REGISTRY_TABLE}(status);

            CREATE TABLE IF NOT EXISTS {REPORTS_TABLE} (
                order_id TEXT PRIMARY KEY,
                strategy_id TEXT NOT NULL,
                interval TEXT NOT NULL,
                window_id TEXT NOT NULL,
                asset TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_price_cents INTEGER,
                exit_price_cents INTEGER,
                outcome TEXT,
                is_stop_loss INTEGER DEFAULT 0,
                pnl_cents INTEGER,
                resolved_at REAL
            );
            CREATE INDEX IF NOT EXISTS idx_reports_strategy_interval ON {REPORTS_TABLE}(strategy_id, interval);

            CREATE TABLE IF NOT EXISTS v2_tick_log (
                window_id TEXT NOT NULL,
                asset TEXT NOT NULL,
                tick_history_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                PRIMARY KEY (window_id, asset)
            );
            """
        )
        conn.commit()
        # Migration: add placement_bid_cents if missing (existing DBs)
        try:
            conn.execute(f"ALTER TABLE {REGISTRY_TABLE} ADD COLUMN placement_bid_cents INTEGER")
            conn.commit()
            logger.info("V2 DB: added placement_bid_cents to %s", REGISTRY_TABLE)
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e).lower():
                logger.warning("V2 DB migration placement_bid_cents: %s", e)
        try:
            conn.execute(f"ALTER TABLE {REGISTRY_TABLE} ADD COLUMN entry_distance REAL")
            conn.commit()
            logger.info("V2 DB: added entry_distance to %s", REGISTRY_TABLE)
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e).lower():
                logger.warning("V2 DB migration entry_distance: %s", e)
        try:
            conn.execute(f"ALTER TABLE {REGISTRY_TABLE} ADD COLUMN entry_distance_at_fill REAL")
            conn.commit()
            logger.info("V2 DB: added entry_distance_at_fill to %s", REGISTRY_TABLE)
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e).lower():
                logger.warning("V2 DB migration entry_distance_at_fill: %s", e)
        try:
            conn.execute(f"ALTER TABLE {REGISTRY_TABLE} ADD COLUMN entry_fill_price_cents INTEGER")
            conn.commit()
            logger.info("V2 DB: added entry_fill_price_cents to %s", REGISTRY_TABLE)
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e).lower():
                logger.warning("V2 DB migration entry_fill_price_cents: %s", e)
        logger.info("V2 DB initialized at %s", path)
    finally:
        conn.close()


class OrderRegistry:
    """
    CRUD for v2_order_registry and v2_strategy_reports.
    Uses short-lived per-operation connections (V1-style) for thread safety and resilience.
    """

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self._path = Path(db_path) if db_path else _default_db_path()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()

    def _new_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._path), check_same_thread=False, timeout=SQLITE_TIMEOUT_SECONDS)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
        conn.row_factory = sqlite3.Row
        return conn

    def _recover_malformed_db(self) -> None:
        """
        Rotate malformed DB files and recreate schema so loops can continue.
        """
        stamp = time.strftime("%Y%m%d_%H%M%S")
        db = self._path
        db_corrupt = db.with_name(f"{db.stem}.corrupt.{stamp}{db.suffix}")
        wal = Path(str(db) + "-wal")
        shm = Path(str(db) + "-shm")
        wal_corrupt = Path(str(db_corrupt) + "-wal")
        shm_corrupt = Path(str(db_corrupt) + "-shm")

        if db.exists():
            db.rename(db_corrupt)
        if wal.exists():
            wal.rename(wal_corrupt)
        if shm.exists():
            shm.rename(shm_corrupt)

        logger.error(
            "Detected malformed sqlite DB. Rotated to %s and recreating fresh %s",
            db_corrupt,
            db,
        )
        init_v2_db(db_path=db)
    def _execute(
        self,
        query: str,
        params: tuple = (),
        *,
        commit: bool = False,
        fetch: str = "none",
    ):
        """
        Execute query with thread serialization and one-shot recovery on malformed DB.
        fetch: "none" | "one" | "all"
        """
        with self._lock:
            for attempt in (1, 2):
                try:
                    conn = self._new_conn()
                    try:
                        cur = conn.execute(query, params)
                        if commit:
                            conn.commit()
                        if fetch == "one":
                            return cur.fetchone()
                        if fetch == "all":
                            return cur.fetchall()
                        return None
                    finally:
                        conn.close()
                except sqlite3.DatabaseError as e:
                    if "malformed" not in str(e).lower() or attempt == 2:
                        raise
                    self._recover_malformed_db()

    def register_order(
        self,
        order_id: str,
        strategy_id: str,
        interval: str,
        market_id: str,
        asset: str,
        ticker: str,
        side: str,
        count: int,
        placed_at: float,
        limit_price_cents: Optional[int] = None,
        client_order_id: Optional[str] = None,
        placement_bid_cents: Optional[int] = None,
        entry_distance: Optional[float] = None,
    ) -> None:
        """Insert a new order into v2_order_registry (status=resting, filled_count=0)."""
        self._execute(
            f"""
            INSERT INTO {REGISTRY_TABLE}
            (order_id, strategy_id, interval, market_id, asset, ticker, side, status, filled_count, count, limit_price_cents, entry_fill_price_cents, placed_at, client_order_id, placement_bid_cents, entry_distance)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'resting', 0, ?, ?, NULL, ?, ?, ?, ?)
            """,
            (
                order_id,
                strategy_id,
                interval,
                market_id,
                asset,
                ticker,
                side,
                count,
                limit_price_cents,
                placed_at,
                client_order_id,
                placement_bid_cents,
                entry_distance,
            ),
            commit=True,
        )
        logger.debug("Registered order %s for %s/%s", order_id, strategy_id, interval)

    def update_order_status(
        self,
        order_id: str,
        status: str,
        filled_count: Optional[int] = None,
        entry_distance_at_fill: Optional[float] = None,
        entry_fill_price_cents: Optional[int] = None,
    ) -> None:
        """Update status and optionally filled_count and/or entry_distance_at_fill for an order."""
        set_parts = ["status = ?"]
        params: List[object] = [status]
        if filled_count is not None:
            set_parts.append("filled_count = ?")
            params.append(filled_count)
        if entry_distance_at_fill is not None:
            set_parts.append("entry_distance_at_fill = ?")
            params.append(entry_distance_at_fill)
        if entry_fill_price_cents is not None:
            set_parts.append("entry_fill_price_cents = ?")
            params.append(entry_fill_price_cents)
        params.append(order_id)
        self._execute(
            f"UPDATE {REGISTRY_TABLE} SET {', '.join(set_parts)} WHERE order_id = ?",
            tuple(params),
            commit=True,
        )
        logger.debug("Updated order %s -> status=%s filled_count=%s", order_id, status, filled_count)

    def get_orders_by_strategy(
        self,
        strategy_id: str,
        interval: str,
        market_id: Optional[str] = None,
        asset: Optional[str] = None,
        active_only: bool = True,
    ) -> List[OrderRecord]:
        """Return OrderRecords for the given strategy/interval, optionally filtered by market_id/asset."""
        query = f"""
            SELECT order_id, strategy_id, interval, market_id, asset, ticker, side, status,
                   filled_count, count, limit_price_cents, entry_fill_price_cents, placed_at, placement_bid_cents, entry_distance, entry_distance_at_fill
            FROM {REGISTRY_TABLE}
            WHERE strategy_id = ? AND interval = ?
            """
        params: List[object] = [strategy_id, interval]
        if market_id is not None:
            query += " AND market_id = ?"
            params.append(market_id)
        if asset is not None:
            query += " AND asset = ?"
            params.append(asset)
        if active_only:
            query += " AND status = 'resting'"
        query += " ORDER BY placed_at DESC"
        rows = self._execute(query, tuple(params), fetch="all")
        return [self._row_to_order_record(dict(r)) for r in rows]

    def get_order_by_id(self, order_id: str) -> Optional[OrderRecord]:
        """Return the OrderRecord for the given order_id, or None if not in registry."""
        row = self._execute(
            f"""
            SELECT order_id, strategy_id, interval, market_id, asset, ticker, side, status,
                   filled_count, count, limit_price_cents, entry_fill_price_cents, placed_at, placement_bid_cents, entry_distance, entry_distance_at_fill
            FROM {REGISTRY_TABLE}
            WHERE order_id = ?
            """,
            (order_id,),
            fetch="one",
        )
        if row is None:
            return None
        return self._row_to_order_record(dict(row))

    def get_all_active_orders_for_cap_check(
        self,
        interval: str,
        market_id: Optional[str] = None,
    ) -> List[OrderRecord]:
        """
        Return all orders with status='resting' for the given interval (and optionally market_id).
        Used by aggregator to enforce caps (max_orders_per_ticker, max_total_orders_per_interval).
        """
        query = f"""
            SELECT order_id, strategy_id, interval, market_id, asset, ticker, side, status,
                   filled_count, count, limit_price_cents, entry_fill_price_cents, placed_at, placement_bid_cents, entry_distance, entry_distance_at_fill
            FROM {REGISTRY_TABLE}
            WHERE interval = ? AND status = 'resting'
            """
        params: List[object] = [interval]
        if market_id is not None:
            query += " AND market_id = ?"
            params.append(market_id)
        rows = self._execute(query, tuple(params), fetch="all")
        return [self._row_to_order_record(dict(r)) for r in rows]

    def record_trade_outcome(
        self,
        order_id: str,
        strategy_id: str,
        interval: str,
        window_id: str,
        asset: str,
        side: str,
        entry_price_cents: Optional[int],
        exit_price_cents: Optional[int],
        outcome: str,  # win | loss | resolved_yes | resolved_no
        is_stop_loss: bool,
        pnl_cents: Optional[int],
        resolved_at: float,
    ) -> None:
        """Insert a row into v2_strategy_reports for a closed trade."""
        self._execute(
            f"""
            INSERT OR REPLACE INTO {REPORTS_TABLE}
            (order_id, strategy_id, interval, window_id, asset, side, entry_price_cents, exit_price_cents, outcome, is_stop_loss, pnl_cents, resolved_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                order_id,
                strategy_id,
                interval,
                window_id,
                asset,
                side,
                entry_price_cents,
                exit_price_cents,
                outcome,
                1 if is_stop_loss else 0,
                pnl_cents,
                resolved_at,
            ),
            commit=True,
        )
        logger.debug("Recorded trade outcome for order %s: outcome=%s pnl_cents=%s", order_id, outcome, pnl_cents)

    def get_reports_by_strategy(
        self,
        strategy_id: str,
        interval: Optional[str] = None,
        asset: Optional[str] = None,
        limit: int = 500,
    ) -> List[dict]:
        """
        Return closed-trade rows from v2_strategy_reports for the given strategy.
        Each row has: order_id, strategy_id, interval, window_id, asset, side,
        entry_price_cents, exit_price_cents, outcome, is_stop_loss, pnl_cents, resolved_at.
        """
        query = f"SELECT * FROM {REPORTS_TABLE} WHERE strategy_id = ?"
        params: List[object] = [strategy_id]
        if interval is not None:
            query += " AND interval = ?"
            params.append(interval)
        if asset is not None:
            query += " AND LOWER(asset) = LOWER(?)"
            params.append(asset)
        query += " ORDER BY resolved_at DESC LIMIT ?"
        params.append(limit)
        rows = self._execute(query, tuple(params), fetch="all")
        return [dict(r) for r in rows]

    @staticmethod
    def _row_to_order_record(row: dict) -> OrderRecord:
        return OrderRecord(
            order_id=row["order_id"],
            strategy_id=row["strategy_id"],
            interval=row["interval"],
            market_id=row["market_id"],
            asset=row["asset"],
            ticker=row["ticker"],
            side=row["side"],
            status=row["status"],
            filled_count=row["filled_count"] or 0,
            count=row["count"] or 0,
            limit_price_cents=row.get("limit_price_cents"),
            entry_fill_price_cents=row.get("entry_fill_price_cents"),
            placed_at=row["placed_at"],
            placement_bid_cents=row.get("placement_bid_cents"),
            entry_distance=float(row["entry_distance"]) if row.get("entry_distance") is not None else None,
            entry_distance_at_fill=float(row["entry_distance_at_fill"]) if row.get("entry_distance_at_fill") is not None else None,
        )

    def close(self) -> None:
        """No-op: registry uses short-lived per-operation connections."""
        logger.debug("OrderRegistry closed for %s", self._path)

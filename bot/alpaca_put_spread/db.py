"""
SQLite persistence for Alpaca options bot: orders, spreads, events.
Uses data/alpaca_put_spread.db (separate from v2_state.db).
"""
from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

ORDERS_TABLE = "alpaca_orders"
SPREADS_TABLE = "alpaca_spreads"
EVENTS_TABLE = "alpaca_events"


def _default_db_path() -> Path:
    return Path(__file__).resolve().parents[1].parent / "data" / "alpaca_put_spread.db"


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return {str(row[1]) for row in cur.fetchall()}


def _migrate_schema(conn: sqlite3.Connection) -> None:
    """Upgrade legacy short_put/long_put or legs_json to legs + strategy_type."""
    for table in (ORDERS_TABLE, SPREADS_TABLE):
        cols = _table_columns(conn, table)
        if "legs" not in cols:
            if "legs_json" in cols:
                try:
                    conn.execute(f"ALTER TABLE {table} RENAME COLUMN legs_json TO legs")
                    cols = _table_columns(conn, table)
                except sqlite3.OperationalError:
                    conn.execute(f"ALTER TABLE {table} ADD COLUMN legs TEXT")
            else:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN legs TEXT")

        cols = _table_columns(conn, table)
        if "strategy_type" not in cols:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN strategy_type TEXT DEFAULT 'PCS'")

        cols = _table_columns(conn, table)
        if "short_put_symbol" in cols and "long_put_symbol" in cols:
            conn.execute(
                f"""
                UPDATE {table}
                SET legs = json_array(short_put_symbol, long_put_symbol)
                WHERE (legs IS NULL OR legs = '')
                  AND short_put_symbol IS NOT NULL AND long_put_symbol IS NOT NULL
                """
            )
            conn.execute(
                f"""
                UPDATE {table}
                SET strategy_type = COALESCE(NULLIF(TRIM(strategy_type), ''), 'PCS')
                WHERE strategy_type IS NULL OR TRIM(strategy_type) = ''
                """
            )
            for legacy in ("short_put_symbol", "long_put_symbol"):
                if legacy in _table_columns(conn, table):
                    try:
                        conn.execute(f"ALTER TABLE {table} DROP COLUMN {legacy}")
                    except sqlite3.OperationalError as e:
                        logger.warning(
                            "Could not DROP COLUMN %s from %s (SQLite may be < 3.35): %s",
                            legacy,
                            table,
                            e,
                        )

        conn.execute(
            f"""
            UPDATE {table}
            SET strategy_type = 'PCS'
            WHERE strategy_type IS NULL OR TRIM(strategy_type) = ''
            """
        )
    conn.commit()


def init_alpaca_db(db_path: Optional[Path] = None) -> None:
    """Create DB and tables if not exist. Idempotent. Runs schema migration."""
    path = db_path or _default_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        conn.executescript(
            f"""
            CREATE TABLE IF NOT EXISTS {ORDERS_TABLE} (
                order_id TEXT PRIMARY KEY,
                client_order_id TEXT,
                underlying TEXT NOT NULL,
                side TEXT NOT NULL,
                status TEXT NOT NULL,
                legs TEXT,
                strategy_type TEXT DEFAULT 'PCS',
                limit_price REAL,
                qty INTEGER,
                submitted_at REAL,
                updated_at REAL,
                raw_snapshot_json TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_orders_underlying ON {ORDERS_TABLE}(underlying);
            CREATE INDEX IF NOT EXISTS idx_orders_side ON {ORDERS_TABLE}(side);
            CREATE INDEX IF NOT EXISTS idx_orders_submitted_at ON {ORDERS_TABLE}(submitted_at);
            CREATE INDEX IF NOT EXISTS idx_orders_strategy_type ON {ORDERS_TABLE}(strategy_type);

            CREATE TABLE IF NOT EXISTS {SPREADS_TABLE} (
                spread_id INTEGER PRIMARY KEY AUTOINCREMENT,
                underlying TEXT NOT NULL,
                legs TEXT,
                strategy_type TEXT DEFAULT 'PCS',
                entry_order_id TEXT,
                close_order_id TEXT,
                entry_credit_mid REAL,
                close_debit_mid REAL,
                pnl_dollars REAL,
                opened_at REAL,
                closed_at REAL,
                close_reason TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_spreads_underlying ON {SPREADS_TABLE}(underlying);
            CREATE INDEX IF NOT EXISTS idx_spreads_opened_at ON {SPREADS_TABLE}(opened_at);
            CREATE INDEX IF NOT EXISTS idx_spreads_strategy_type ON {SPREADS_TABLE}(strategy_type);

            CREATE TABLE IF NOT EXISTS {EVENTS_TABLE} (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                underlying TEXT,
                message TEXT,
                extra_json TEXT,
                created_at REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_events_type ON {EVENTS_TABLE}(event_type);
            CREATE INDEX IF NOT EXISTS idx_events_created_at ON {EVENTS_TABLE}(created_at);
            """
        )
        conn.commit()
        _migrate_schema(conn)
        logger.info("Alpaca options DB initialized at %s", path)
    finally:
        conn.close()


def _legs_json(legs: Optional[List[str]], short_sym: Optional[str], long_sym: Optional[str]) -> Optional[str]:
    if legs:
        return json.dumps([str(x) for x in legs], separators=(",", ":"))
    if short_sym and long_sym:
        return json.dumps([short_sym, long_sym], separators=(",", ":"))
    return None


def insert_order(
    order_id: str,
    underlying: str,
    side: str,
    status: str,
    *,
    client_order_id: Optional[str] = None,
    strategy_type: str = "PCS",
    legs: Optional[List[str]] = None,
    short_put_symbol: Optional[str] = None,
    long_put_symbol: Optional[str] = None,
    limit_price: Optional[float] = None,
    qty: Optional[int] = None,
    raw_snapshot: Optional[Dict[str, Any]] = None,
    db_path: Optional[Path] = None,
) -> None:
    import time

    ts = time.time()
    path = db_path or _default_db_path()
    raw_json = json.dumps(raw_snapshot) if raw_snapshot else None
    legs_val = _legs_json(legs, short_put_symbol, long_put_symbol)
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {ORDERS_TABLE}
            (order_id, client_order_id, underlying, side, status,
             legs, strategy_type,
             limit_price, qty, submitted_at, updated_at, raw_snapshot_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                order_id,
                client_order_id,
                underlying,
                side,
                status,
                legs_val,
                strategy_type,
                limit_price,
                qty,
                ts,
                ts,
                raw_json,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def update_order_status(
    order_id: str,
    status: str,
    *,
    db_path: Optional[Path] = None,
) -> None:
    import time

    path = db_path or _default_db_path()
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            f"UPDATE {ORDERS_TABLE} SET status = ?, updated_at = ? WHERE order_id = ?",
            (status, time.time(), order_id),
        )
        conn.commit()
    finally:
        conn.close()


def insert_spread(
    underlying: str,
    entry_credit_mid: float,
    entry_order_id: Optional[str] = None,
    *,
    strategy_type: str = "PCS",
    legs: Optional[List[str]] = None,
    short_put_symbol: Optional[str] = None,
    long_put_symbol: Optional[str] = None,
    db_path: Optional[Path] = None,
) -> int:
    import time

    path = db_path or _default_db_path()
    legs_val = _legs_json(legs, short_put_symbol, long_put_symbol)
    conn = sqlite3.connect(str(path))
    try:
        cur = conn.execute(
            f"""
            INSERT INTO {SPREADS_TABLE}
            (underlying, legs, strategy_type,
             entry_order_id, entry_credit_mid, opened_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                underlying,
                legs_val,
                strategy_type,
                entry_order_id,
                entry_credit_mid,
                time.time(),
            ),
        )
        conn.commit()
        return cur.lastrowid or 0
    finally:
        conn.close()


def close_spread(
    spread_id: int,
    close_order_id: str,
    close_debit_mid: float,
    close_reason: str,
    entry_credit_mid: float,
    qty: int = 1,
    *,
    db_path: Optional[Path] = None,
) -> None:
    import time

    pnl = (float(entry_credit_mid) - float(close_debit_mid)) * 100.0 * int(qty)
    path = db_path or _default_db_path()
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            f"""
            UPDATE {SPREADS_TABLE}
            SET close_order_id = ?, close_debit_mid = ?, pnl_dollars = ?, closed_at = ?, close_reason = ?
            WHERE spread_id = ?
            """,
            (close_order_id, close_debit_mid, pnl, time.time(), close_reason, spread_id),
        )
        conn.commit()
    finally:
        conn.close()


def get_daily_pnl(db_path: Optional[Path] = None) -> float:
    """Sum of pnl_dollars for spreads closed today (UTC)."""
    from datetime import datetime, timezone

    path = db_path or _default_db_path()
    if not path.exists():
        return 0.0
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    conn = sqlite3.connect(str(path))
    try:
        row = conn.execute(
            f"SELECT COALESCE(SUM(pnl_dollars), 0) FROM {SPREADS_TABLE} WHERE closed_at >= ? AND pnl_dollars IS NOT NULL",
            (today_start,),
        ).fetchone()
        return float(row[0]) if row else 0.0
    finally:
        conn.close()


def get_pnl_by_underlying_today(underlying: str, db_path: Optional[Path] = None) -> float:
    """Sum of pnl_dollars for given underlying closed today (UTC)."""
    from datetime import datetime, timezone

    path = db_path or _default_db_path()
    if not path.exists():
        return 0.0
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    conn = sqlite3.connect(str(path))
    try:
        row = conn.execute(
            f"SELECT COALESCE(SUM(pnl_dollars), 0) FROM {SPREADS_TABLE} WHERE underlying = ? AND closed_at >= ? AND pnl_dollars IS NOT NULL",
            (underlying, today_start),
        ).fetchone()
        return float(row[0]) if row else 0.0
    finally:
        conn.close()


def log_event(
    event_type: str,
    message: str,
    *,
    underlying: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
    db_path: Optional[Path] = None,
) -> None:
    import time

    path = db_path or _default_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    init_alpaca_db(path)
    extra_json = json.dumps(extra) if extra else None
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            f"INSERT INTO {EVENTS_TABLE} (event_type, underlying, message, extra_json, created_at) VALUES (?, ?, ?, ?, ?)",
            (event_type, underlying, message, extra_json, time.time()),
        )
        conn.commit()
    finally:
        conn.close()

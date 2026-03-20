"""
SQLite persistence for Alpaca put spread bot: orders, spreads, events.
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


def init_alpaca_db(db_path: Optional[Path] = None) -> None:
    """Create DB and tables if not exist. Idempotent."""
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
                short_put_symbol TEXT,
                long_put_symbol TEXT,
                limit_price REAL,
                qty INTEGER,
                submitted_at REAL,
                updated_at REAL,
                raw_snapshot_json TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_orders_underlying ON {ORDERS_TABLE}(underlying);
            CREATE INDEX IF NOT EXISTS idx_orders_side ON {ORDERS_TABLE}(side);
            CREATE INDEX IF NOT EXISTS idx_orders_submitted_at ON {ORDERS_TABLE}(submitted_at);

            CREATE TABLE IF NOT EXISTS {SPREADS_TABLE} (
                spread_id INTEGER PRIMARY KEY AUTOINCREMENT,
                underlying TEXT NOT NULL,
                short_put_symbol TEXT NOT NULL,
                long_put_symbol TEXT NOT NULL,
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
        logger.info("Alpaca put spread DB initialized at %s", path)
    finally:
        conn.close()


def insert_order(
    order_id: str,
    underlying: str,
    side: str,
    status: str,
    *,
    client_order_id: Optional[str] = None,
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
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {ORDERS_TABLE}
            (order_id, client_order_id, underlying, side, status, short_put_symbol, long_put_symbol,
             limit_price, qty, submitted_at, updated_at, raw_snapshot_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (order_id, client_order_id, underlying, side, status, short_put_symbol, long_put_symbol,
             limit_price, qty, ts, ts, raw_json),
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
    short_put_symbol: str,
    long_put_symbol: str,
    entry_credit_mid: float,
    entry_order_id: Optional[str] = None,
    *,
    db_path: Optional[Path] = None,
) -> int:
    import time
    path = db_path or _default_db_path()
    conn = sqlite3.connect(str(path))
    try:
        cur = conn.execute(
            f"""
            INSERT INTO {SPREADS_TABLE}
            (underlying, short_put_symbol, long_put_symbol, entry_order_id, entry_credit_mid, opened_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (underlying, short_put_symbol, long_put_symbol, entry_order_id, entry_credit_mid, time.time()),
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
    # PnL = (entry_credit - close_debit) * 100 * qty (options multiplier $100/contract)
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
    import time
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

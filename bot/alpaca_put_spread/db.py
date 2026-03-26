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
_DB_INIT_LOGGED_PATHS: set[str] = set()

ORDERS_TABLE = "alpaca_orders"
SPREADS_TABLE = "alpaca_spreads"
EVENTS_TABLE = "alpaca_events"
TELEMETRY_TABLE = "spread_telemetry"


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

    cols_orders = _table_columns(conn, ORDERS_TABLE)
    if "filled_avg_price" not in cols_orders:
        conn.execute(f"ALTER TABLE {ORDERS_TABLE} ADD COLUMN filled_avg_price REAL")

    cols_spreads = _table_columns(conn, SPREADS_TABLE)
    if "raw_snapshot_json" not in cols_spreads:
        conn.execute(f"ALTER TABLE {SPREADS_TABLE} ADD COLUMN raw_snapshot_json TEXT")

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TELEMETRY_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            spread_id INTEGER NOT NULL,
            timestamp REAL NOT NULL,
            underlying_price REAL,
            current_mark_mid REAL,
            current_natural_ask REAL,
            distance_to_short_pct REAL,
            gate_open INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_telemetry_spread_ts ON {TELEMETRY_TABLE}(spread_id, timestamp)"
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
                raw_snapshot_json TEXT,
                filled_avg_price REAL
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
                close_reason TEXT,
                raw_snapshot_json TEXT
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

            CREATE TABLE IF NOT EXISTS {TELEMETRY_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                spread_id INTEGER NOT NULL,
                timestamp REAL NOT NULL,
                underlying_price REAL,
                current_mark_mid REAL,
                current_natural_ask REAL,
                distance_to_short_pct REAL,
                gate_open INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_telemetry_spread_ts ON {TELEMETRY_TABLE}(spread_id, timestamp);
            """
        )
        conn.commit()
        _migrate_schema(conn)
        path_key = str(path.resolve())
        if path_key not in _DB_INIT_LOGGED_PATHS:
            logger.info("Alpaca options DB initialized at %s", path)
            _DB_INIT_LOGGED_PATHS.add(path_key)
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
    filled_avg_price: Optional[float] = None,
    db_path: Optional[Path] = None,
) -> None:
    import time

    path = db_path or _default_db_path()
    conn = sqlite3.connect(str(path))
    try:
        if filled_avg_price is not None:
            conn.execute(
                f"""
                UPDATE {ORDERS_TABLE}
                SET status = ?, updated_at = ?, filled_avg_price = COALESCE(?, filled_avg_price)
                WHERE order_id = ?
                """,
                (status, time.time(), float(filled_avg_price), order_id),
            )
        else:
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
    raw_snapshot: Optional[Dict[str, Any]] = None,
    db_path: Optional[Path] = None,
) -> int:
    import time

    path = db_path or _default_db_path()
    init_alpaca_db(path)
    legs_val = _legs_json(legs, short_put_symbol, long_put_symbol)
    raw_json = json.dumps(raw_snapshot, separators=(",", ":")) if raw_snapshot else None
    conn = sqlite3.connect(str(path))
    try:
        cur = conn.execute(
            f"""
            INSERT INTO {SPREADS_TABLE}
            (underlying, legs, strategy_type,
             entry_order_id, entry_credit_mid, opened_at, raw_snapshot_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                underlying,
                legs_val,
                strategy_type,
                entry_order_id,
                entry_credit_mid,
                time.time(),
                raw_json,
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
    filled_avg_price: Optional[float] = None,
    db_path: Optional[Path] = None,
) -> None:
    import time

    # Prefer Alpaca-reported net fill when provided (realized debit to close the spread).
    debit_for_pnl = (
        abs(float(filled_avg_price)) if filled_avg_price is not None else abs(float(close_debit_mid))
    )
    pnl = (float(entry_credit_mid) - debit_for_pnl) * 100.0 * int(qty)
    path = db_path or _default_db_path()
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            f"""
            UPDATE {SPREADS_TABLE}
            SET close_order_id = ?, close_debit_mid = ?, pnl_dollars = ?, closed_at = ?, close_reason = ?
            WHERE spread_id = ?
            """,
            (close_order_id, debit_for_pnl, pnl, time.time(), close_reason, spread_id),
        )
        conn.commit()
    finally:
        conn.close()


def merge_order_raw_snapshot(
    order_id: str,
    extra: Dict[str, Any],
    *,
    db_path: Optional[Path] = None,
) -> None:
    """Merge keys into alpaca_orders.raw_snapshot_json (e.g. entry greeks at fill)."""
    import time

    path = db_path or _default_db_path()
    init_alpaca_db(path)
    conn = sqlite3.connect(str(path))
    try:
        row = conn.execute(
            f"SELECT raw_snapshot_json FROM {ORDERS_TABLE} WHERE order_id = ?",
            (order_id,),
        ).fetchone()
        base: Dict[str, Any] = {}
        if row and row[0]:
            try:
                parsed = json.loads(str(row[0]))
                if isinstance(parsed, dict):
                    base = parsed
            except Exception:
                base = {}
        base.update(extra)
        conn.execute(
            f"""
            UPDATE {ORDERS_TABLE}
            SET raw_snapshot_json = ?, updated_at = ?
            WHERE order_id = ?
            """,
            (json.dumps(base, separators=(",", ":")), time.time(), order_id),
        )
        conn.commit()
    finally:
        conn.close()


def insert_telemetry(
    spread_id: int,
    *,
    underlying_price: float,
    current_mark_mid: Optional[float],
    current_natural_ask: Optional[float],
    distance_to_short_pct: Optional[float],
    gate_open: bool,
    db_path: Optional[Path] = None,
) -> None:
    """Append one row to spread_telemetry for MAE/MFE-style analysis."""
    import time

    ts = time.time()
    path = db_path or _default_db_path()
    init_alpaca_db(path)
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            f"""
            INSERT INTO {TELEMETRY_TABLE}
            (spread_id, timestamp, underlying_price, current_mark_mid,
             current_natural_ask, distance_to_short_pct, gate_open)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(spread_id),
                ts,
                float(underlying_price),
                current_mark_mid,
                current_natural_ask,
                distance_to_short_pct,
                1 if gate_open else 0,
            ),
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


def _parse_legs_field(legs_field: Optional[str]) -> Optional[List[str]]:
    if not legs_field:
        return None
    try:
        v = json.loads(str(legs_field))
        if isinstance(v, list):
            return [str(x) for x in v]
    except Exception:
        return None
    return None


def find_open_spread_id_by_legs(
    *,
    underlying: str,
    strategy_type: str,
    legs: List[str],
    db_path: Optional[Path] = None,
) -> Optional[int]:
    """
    Best-effort lookup: find the most recent spread row (close_order_id is NULL)
    matching (underlying, strategy_type, legs).
    """
    path = db_path or _default_db_path()
    if not path.exists():
        return None
    legs_key = tuple(sorted(str(x) for x in legs))
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            f"""
            SELECT spread_id, legs
            FROM {SPREADS_TABLE}
            WHERE underlying = ?
              AND strategy_type = ?
              AND close_order_id IS NULL
            ORDER BY opened_at DESC
            LIMIT 50
            """,
            (str(underlying), str(strategy_type)),
        ).fetchall()
        for r in rows:
            cur_legs = _parse_legs_field(r["legs"])
            if cur_legs and tuple(sorted(cur_legs)) == legs_key:
                return int(r["spread_id"])
    finally:
        conn.close()
    return None


def list_filled_close_orders_without_spread_link(
    *,
    since_ts: Optional[float] = None,
    db_path: Optional[Path] = None,
) -> List[Dict[str, Any]]:
    """
    Return close orders that are filled in alpaca_orders but not yet linked to alpaca_spreads.close_order_id.
    This is used as a reconciliation fallback when JSON state is missing pending_close metadata.
    """
    path = db_path or _default_db_path()
    if not path.exists():
        return []
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        where_time = ""
        params: List[Any] = []
        if since_ts is not None:
            where_time = "AND submitted_at >= ?"
            params.append(float(since_ts))
        rows = conn.execute(
            f"""
            SELECT order_id, underlying, strategy_type, legs, limit_price, filled_avg_price, submitted_at
            FROM {ORDERS_TABLE}
            WHERE side = 'close'
              AND status = 'filled'
              {where_time}
              AND order_id NOT IN (
                SELECT close_order_id FROM {SPREADS_TABLE} WHERE close_order_id IS NOT NULL
              )
            ORDER BY submitted_at ASC
            """,
            tuple(params),
        ).fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append(dict(r))
        return out
    finally:
        conn.close()


def get_spread_entry_credit_mid(spread_id: int, *, db_path: Optional[Path] = None) -> Optional[float]:
    path = db_path or _default_db_path()
    if not path.exists():
        return None
    conn = sqlite3.connect(str(path))
    try:
        row = conn.execute(
            f"SELECT entry_credit_mid FROM {SPREADS_TABLE} WHERE spread_id = ?",
            (int(spread_id),),
        ).fetchone()
        if not row or row[0] is None:
            return None
        return float(row[0])
    finally:
        conn.close()

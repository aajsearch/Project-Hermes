from __future__ import annotations

import logging
import sqlite3
import time
from pathlib import Path
from threading import Lock
from typing import Optional, Tuple


class SqliteStore:
    def __init__(self, path: str, schema_path: str):
        self.path = Path(path)
        self.schema_path = Path(schema_path)
        self._lock = Lock()
        self._logger = logging.getLogger("mm_bot.storage")

    def _connect_new(self) -> sqlite3.Connection:
        """
        Create a fresh SQLite connection.
        We intentionally do NOT share connections across threads to avoid sqlite driver hangs.
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.path), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA busy_timeout=30000;")
        return conn

    def init_schema(self) -> None:
        with self._lock:
            sql = self.schema_path.read_text()
            with self._connect_new() as conn:
                conn.executescript(sql)
        self._logger.info("sqlite_schema_ready", extra={"extra": {"path": str(self.path)}})

    def close(self) -> None:
        # Connections are per-operation; nothing to close.
        self._logger.info("sqlite_closed")

    # --- Orders ---
    def get_order_id_by_client_order_id(self, client_order_id: str) -> Optional[str]:
        with self._lock:
            with self._connect_new() as conn:
                row = conn.execute(
                    "SELECT order_id FROM orders WHERE client_order_id = ?",
                    (client_order_id,),
                ).fetchone()
                if row is None:
                    return None
                return str(row["order_id"])

    def insert_order(
        self,
        *,
        order_id: str,
        client_order_id: str,
        product_id: str,
        side: str,
        price: float,
        size: float,
        post_only: bool,
        status: str,
        exchange_status: Optional[str] = None,
        ts_ms: Optional[int] = None,
    ) -> None:
        now = int(ts_ms if ts_ms is not None else time.time() * 1000)
        with self._lock:
            with self._connect_new() as conn:
                try:
                    conn.execute(
                        """
                        INSERT INTO orders(order_id, client_order_id, product_id, side, price, size, post_only, status, exchange_status, created_ts, updated_ts)
                        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(order_id) DO UPDATE SET
                          client_order_id=excluded.client_order_id,
                          price=excluded.price,
                          size=excluded.size,
                          post_only=excluded.post_only,
                          status=excluded.status,
                          exchange_status=excluded.exchange_status,
                          updated_ts=excluded.updated_ts
                        """,
                        (
                            order_id,
                            client_order_id,
                            product_id,
                            side.upper(),
                            float(price),
                            float(size),
                            1 if post_only else 0,
                            status,
                            exchange_status,
                            now,
                            now,
                        ),
                    )
                except sqlite3.IntegrityError as e:
                    # Common in our bot: we reuse client_order_id across requotes.
                    # If the existing row uses the same order_id, just update it.
                    if "orders.client_order_id" not in str(e):
                        raise

                    row = conn.execute(
                        "SELECT order_id FROM orders WHERE client_order_id = ?",
                        (client_order_id,),
                    ).fetchone()
                    existing_order_id = (row["order_id"] if row is not None else None) if row is not None else None

                    if existing_order_id == order_id:
                        conn.execute(
                            """
                            UPDATE orders
                               SET product_id=?,
                                   side=?,
                                   price=?,
                                   size=?,
                                   post_only=?,
                                   status=?,
                                   exchange_status=?,
                                   updated_ts=?
                             WHERE client_order_id=?
                            """,
                            (
                                product_id,
                                side.upper(),
                                float(price),
                                float(size),
                                1 if post_only else 0,
                                status,
                                exchange_status,
                                now,
                                client_order_id,
                            ),
                        )
                        return

                    # If order_id changed (e.g., stale DB from old runs), we can only
                    # replace the row if nothing in fills references the old order_id.
                    if existing_order_id:
                        fill_count = conn.execute(
                            "SELECT COUNT(*) AS n FROM fills WHERE order_id = ?",
                            (existing_order_id,),
                        ).fetchone()["n"]
                        if int(fill_count) == 0:
                            conn.execute("DELETE FROM orders WHERE client_order_id = ?", (client_order_id,))
                            conn.execute(
                                """
                                INSERT INTO orders(order_id, client_order_id, product_id, side, price, size, post_only, status, exchange_status, created_ts, updated_ts)
                                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    order_id,
                                    client_order_id,
                                    product_id,
                                    side.upper(),
                                    float(price),
                                    float(size),
                                    1 if post_only else 0,
                                    status,
                                    exchange_status,
                                    now,
                                    now,
                                ),
                            )
                            return

                    # Last resort: keep existing PK, update fields so DB reflects latest quote.
                    self._logger.warning(
                        "sqlite_client_order_id_conflict",
                        extra={
                            "extra": {
                                "client_order_id": client_order_id,
                                "existing_order_id": existing_order_id,
                                "new_order_id": order_id,
                            }
                        },
                    )
                    conn.execute(
                        """
                        UPDATE orders
                           SET product_id=?,
                               side=?,
                               price=?,
                               size=?,
                               post_only=?,
                               status=?,
                               exchange_status=?,
                               updated_ts=?
                         WHERE client_order_id=?
                        """,
                        (
                            product_id,
                            side.upper(),
                            float(price),
                            float(size),
                            1 if post_only else 0,
                            status,
                            exchange_status,
                            now,
                            client_order_id,
                        ),
                    )

    def update_order_status(self, order_id: str, status: str, *, ts_ms: Optional[int] = None) -> None:
        now = int(ts_ms if ts_ms is not None else time.time() * 1000)
        with self._lock:
            with self._connect_new() as conn:
                conn.execute(
                    "UPDATE orders SET status=?, updated_ts=? WHERE order_id=?",
                    (status, now, order_id),
                )

    def get_fill_count(self) -> int:
        with self._lock:
            with self._connect_new() as conn:
                row = conn.execute("SELECT COUNT(*) AS n FROM fills").fetchone()
                return int(row["n"] if row is not None else 0)

    def get_fills(self, product_id: Optional[str] = None):
        """
        Return fills ordered by time as list of dict-like rows.
        Used for PnL reconstruction in reporting/risk.
        """
        with self._lock:
            with self._connect_new() as conn:
                if product_id:
                    rows = conn.execute(
                        "SELECT ts, product_id, side, price, size, fee FROM fills WHERE product_id=? ORDER BY ts ASC",
                        (product_id,),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        "SELECT ts, product_id, side, price, size, fee FROM fills ORDER BY ts ASC"
                    ).fetchall()
                return rows

    # --- Fills ---
    def insert_fill(
        self,
        *,
        fill_id: str,
        order_id: str,
        product_id: str,
        side: str,
        price: float,
        size: float,
        fee: float = 0.0,
        liquidity: Optional[str] = None,
        ts_ms: Optional[int] = None,
    ) -> bool:
        now = int(ts_ms if ts_ms is not None else time.time() * 1000)
        with self._lock:
            with self._connect_new() as conn:
                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO fills(fill_id, order_id, product_id, side, price, size, fee, liquidity, ts)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        fill_id,
                        order_id,
                        product_id,
                        side.upper(),
                        float(price),
                        float(size),
                        float(fee),
                        liquidity,
                        now,
                    ),
                )
                return int(getattr(cur, "rowcount", 0) or 0) > 0

    # --- Positions ---
    def get_position(self, asset: str) -> Tuple[float, float]:
        with self._lock:
            with self._connect_new() as conn:
                row = conn.execute(
                    "SELECT qty, avg_price FROM positions WHERE asset=?", (asset,)
                ).fetchone()
                if row is None:
                    return 0.0, 0.0
                return float(row["qty"]), float(row["avg_price"])

    def update_position(self, asset: str, qty_change: float, execution_price: float, *, ts_ms: Optional[int] = None) -> Tuple[float, float]:
        """
        Update running inventory and average price.
        - For increasing absolute position in same direction: update weighted avg
        - For reducing/closing: keep avg_price as-is (placeholder; realized PnL handled elsewhere)
        """
        now = int(ts_ms if ts_ms is not None else time.time() * 1000)
        dq = float(qty_change)
        px = float(execution_price)
        with self._lock:
            with self._connect_new() as conn:
                row = conn.execute(
                    "SELECT qty, avg_price FROM positions WHERE asset=?", (asset,)
                ).fetchone()
                if row is None:
                    new_qty = dq
                    new_avg = px if abs(dq) > 0 else 0.0
                else:
                    qty = float(row["qty"])
                    avg = float(row["avg_price"])
                    new_qty = qty + dq
                    if qty == 0 or (qty > 0 and dq > 0) or (qty < 0 and dq < 0):
                        total_qty = qty + dq
                        if total_qty != 0:
                            new_avg = (avg * qty + px * dq) / total_qty
                        else:
                            new_avg = 0.0
                    else:
                        new_avg = avg if new_qty != 0 else 0.0

                conn.execute(
                    """
                    INSERT INTO positions(asset, qty, avg_price, updated_ts)
                    VALUES(?, ?, ?, ?)
                    ON CONFLICT(asset) DO UPDATE SET qty=excluded.qty, avg_price=excluded.avg_price, updated_ts=excluded.updated_ts
                    """,
                    (asset, new_qty, new_avg, now),
                )
                return new_qty, new_avg


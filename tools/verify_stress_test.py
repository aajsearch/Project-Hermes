#!/usr/bin/env python3
"""
Verify Alpaca options stress-test results against local SQLite state.

Checks:
1) Local spread summary by strategy (PCS / CCS / IC)
2) Exit reason distribution for closed spreads
3) Local DB orders vs Alpaca brokerage orders reconciliation
4) Slippage for locally/alpaca-filled orders
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from dotenv import load_dotenv

try:
    import pandas as _pd  # optional
except Exception:  # pragma: no cover
    _pd = None

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import QueryOrderStatus
from alpaca.trading.requests import GetOrdersRequest


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "alpaca_put_spread.db"


def _normalize_status(s: Any) -> str:
    raw = str(s or "").strip().lower()
    return raw.split(".")[-1]


def _print_table(title: str, columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> None:
    print(f"\n=== {title} ===")
    if not rows:
        print("(no rows)")
        return
    if _pd is not None:
        df = _pd.DataFrame(rows, columns=list(columns))
        print(df.to_string(index=False))
        return
    widths = [len(c) for c in columns]
    for row in rows:
        for i, v in enumerate(row):
            widths[i] = max(widths[i], len(str(v)))
    header = " | ".join(str(c).ljust(widths[i]) for i, c in enumerate(columns))
    sep = "-+-".join("-" * w for w in widths)
    print(header)
    print(sep)
    for row in rows:
        print(" | ".join(str(v).ljust(widths[i]) for i, v in enumerate(row)))


def _connect_with_retry(db_path: Path, retries: int = 8, sleep_s: float = 0.25) -> sqlite3.Connection:
    last_err: Exception | None = None
    for i in range(retries):
        try:
            conn = sqlite3.connect(str(db_path), timeout=30)
            conn.execute("PRAGMA busy_timeout = 30000")
            return conn
        except sqlite3.OperationalError as e:
            last_err = e
            msg = str(e).lower()
            if "database is locked" not in msg:
                raise
            time.sleep(sleep_s * (i + 1))
    raise RuntimeError(f"Failed to open DB after retries: {last_err}")


def _query_with_retry(
    conn: sqlite3.Connection,
    sql: str,
    params: Sequence[Any] = (),
    retries: int = 8,
    sleep_s: float = 0.2,
) -> List[Tuple[Any, ...]]:
    last_err: Exception | None = None
    for i in range(retries):
        try:
            cur = conn.execute(sql, params)
            return cur.fetchall()
        except sqlite3.OperationalError as e:
            last_err = e
            msg = str(e).lower()
            if "database is locked" not in msg:
                raise
            time.sleep(sleep_s * (i + 1))
    raise RuntimeError(f"Query failed after retries: {last_err}")


def _make_trading_client() -> TradingClient:
    api_key = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")
    if not api_key or not secret_key:
        raise RuntimeError("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY in environment")
    # Per requirement: stress verification against paper account.
    return TradingClient(api_key, secret_key, paper=True)


def _utc_day_start(d: str) -> datetime:
    """Parse YYYY-MM-DD as UTC midnight (start of that calendar day in UTC)."""
    dt = datetime.strptime(d.strip(), "%Y-%m-%d")
    return datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)


def _default_window_from_db(conn: sqlite3.Connection) -> Tuple[datetime, datetime]:
    """
    Alpaca GetOrdersRequest `after` / `until` are in UTC and filter by order submission time.
    Use min/max submitted_at from local DB so reconciliation includes past sessions (not only
    *today* UTC when this script runs — that was causing 0 API orders when trades were yesterday).
    """
    rows = _query_with_retry(
        conn,
        "SELECT MIN(submitted_at), MAX(submitted_at) FROM alpaca_orders WHERE submitted_at IS NOT NULL",
    )
    min_ts, max_ts = rows[0] if rows else (None, None)
    now = datetime.now(timezone.utc)
    if min_ts is None:
        after = now - timedelta(days=7)
        until = now + timedelta(seconds=1)
        return after, until
    after = datetime.fromtimestamp(float(min_ts), tz=timezone.utc) - timedelta(hours=1)
    until = datetime.fromtimestamp(float(max_ts), tz=timezone.utc) + timedelta(hours=1)
    return after, until


def module_spread_summary(conn: sqlite3.Connection) -> None:
    rows = _query_with_retry(
        conn,
        """
        SELECT
          COALESCE(strategy_type, 'UNKNOWN') AS strategy_type,
          COUNT(*) AS total_trades,
          SUM(CASE WHEN closed_at IS NOT NULL THEN 1 ELSE 0 END) AS closed_trades,
          SUM(CASE WHEN closed_at IS NULL THEN 1 ELSE 0 END) AS open_trades,
          ROUND(COALESCE(SUM(pnl_dollars), 0), 2) AS pnl_dollars
        FROM alpaca_spreads
        GROUP BY COALESCE(strategy_type, 'UNKNOWN')
        ORDER BY total_trades DESC
        """,
    )
    _print_table(
        "Module 1: Local DB Spread Summary",
        ("strategy_type", "total_trades", "closed_trades", "open_trades", "sum_pnl_dollars"),
        rows,
    )


def module_exit_reason_distribution(conn: sqlite3.Connection) -> None:
    rows = _query_with_retry(
        conn,
        """
        SELECT
          COALESCE(NULLIF(TRIM(close_reason), ''), '(null)') AS close_reason,
          COUNT(*) AS occurrences
        FROM alpaca_spreads
        WHERE closed_at IS NOT NULL
        GROUP BY COALESCE(NULLIF(TRIM(close_reason), ''), '(null)')
        ORDER BY occurrences DESC
        """,
    )
    _print_table(
        "Module 2: Exit Reason Distribution (closed spreads)",
        ("close_reason", "count"),
        rows,
    )


def _fetch_alpaca_orders_window(
    client: TradingClient,
    after: datetime,
    until: datetime,
) -> Dict[str, Any]:
    """
    Alpaca Trading API: GET /v2/orders — `after` / `until` filter by **order submission time** (UTC).
    Per alpaca-py ``GetOrdersRequest``: response includes orders submitted after `after` and until `until`.
    (Using only *today* UTC midnight was wrong when the script runs on a *later* calendar day than the trades.)
    """
    req = GetOrdersRequest(
        status=QueryOrderStatus.ALL,
        after=after,
        until=until,
        limit=500,
        nested=False,
        direction="desc",
    )
    batch = client.get_orders(filter=req) or []
    out: Dict[str, Any] = {}
    for o in batch:
        oid = str(getattr(o, "id", "") or "")
        if oid:
            out[oid] = o
    if len(batch) >= 500:
        print(
            "WARNING: Alpaca returned 500 orders (API max per request). "
            "Narrow --since/--until or run again; some orders may be missing from this page."
        )
    return out


def module_reconciliation_and_slippage(
    conn: sqlite3.Connection,
    client: TradingClient,
    *,
    after: datetime,
    until: datetime,
) -> None:
    local_rows = _query_with_retry(
        conn,
        """
        SELECT
          order_id,
          COALESCE(status, '') AS local_status,
          COALESCE(limit_price, 0.0) AS limit_price
        FROM alpaca_orders
        ORDER BY submitted_at DESC
        """,
    )
    print(
        f"\nAlpaca list-orders window (UTC): after={after.isoformat()} until={until.isoformat()}"
    )
    alpaca_orders = _fetch_alpaca_orders_window(client, after, until)

    mismatches: List[Tuple[str, str, str]] = []
    not_found_today: List[Tuple[str, str]] = []
    rejected_count = 0
    slippages: List[float] = []

    for order_id, local_status_raw, limit_price_raw in local_rows:
        local_status = _normalize_status(local_status_raw)
        alpaca_obj = alpaca_orders.get(str(order_id))
        if not alpaca_obj:
            not_found_today.append((str(order_id), local_status))
            continue

        alpaca_status = _normalize_status(getattr(alpaca_obj, "status", ""))
        if alpaca_status == "rejected":
            rejected_count += 1
        if alpaca_status != local_status:
            mismatches.append((str(order_id), local_status, alpaca_status))

        if local_status == "filled" and alpaca_status == "filled":
            try:
                avg_fill = float(getattr(alpaca_obj, "filled_avg_price", 0.0) or 0.0)
                local_limit_abs = abs(float(limit_price_raw or 0.0))
                if avg_fill > 0 and local_limit_abs > 0:
                    slippages.append(avg_fill - local_limit_abs)
            except Exception:
                pass

    _print_table(
        "Module 3: Reconciliation Discrepancies (local vs Alpaca API window)",
        ("order_id", "local_status", "alpaca_status"),
        mismatches,
    )
    print(f"\nReconciliation stats:")
    print(f"- Local DB orders checked: {len(local_rows)}")
    print(f"- Alpaca orders fetched (API window): {len(alpaca_orders)}")
    print(f"- Status mismatches: {len(mismatches)}")
    print(f"- Alpaca rejected (in fetched set): {rejected_count}")
    print(f"- Local orders not found in Alpaca fetch: {len(not_found_today)}")

    avg_slippage = (sum(slippages) / len(slippages)) if slippages else None
    _print_table(
        "Module 4: Slippage (filled local + filled Alpaca)",
        ("filled_order_pairs", "avg_slippage_dollars"),
        [(
            len(slippages),
            f"{avg_slippage:.4f}" if avg_slippage is not None else "n/a",
        )],
    )


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Verify local alpaca_put_spread.db vs Alpaca paper API (orders by submission time, UTC)."
    )
    ap.add_argument(
        "--since",
        metavar="YYYY-MM-DD",
        help="UTC day start: only include orders submitted on/after this date 00:00 UTC.",
    )
    ap.add_argument(
        "--until",
        metavar="YYYY-MM-DD",
        help="UTC day end: only include orders submitted on/before this date 23:59:59.999 UTC.",
    )
    args = ap.parse_args()

    load_dotenv()
    print(f"Using DB: {DB_PATH}")
    if not DB_PATH.exists():
        print("ERROR: DB file not found.")
        return 1

    try:
        conn = _connect_with_retry(DB_PATH)
    except Exception as e:
        print(f"ERROR: Could not open SQLite DB: {e}")
        return 1

    try:
        module_spread_summary(conn)
        module_exit_reason_distribution(conn)
    except Exception as e:
        print(f"ERROR: Failed local DB modules: {e}")
        conn.close()
        return 1

    try:
        client = _make_trading_client()
    except Exception as e:
        print(f"ERROR: Could not initialize Alpaca TradingClient: {e}")
        conn.close()
        return 1

    try:
        if args.since or args.until:
            if args.until:
                end_day = _utc_day_start(args.until)
                until = end_day + timedelta(days=1) - timedelta(microseconds=1)
            else:
                until = datetime.now(timezone.utc) + timedelta(seconds=1)
            if args.since:
                after = _utc_day_start(args.since)
            elif args.until:
                after = _utc_day_start(args.until) - timedelta(days=7)
            else:
                after = datetime.now(timezone.utc) - timedelta(days=7)
            if after > until:
                print("ERROR: --since / --until window is invalid (after > until).")
                conn.close()
                return 1
        else:
            after, until = _default_window_from_db(conn)
        module_reconciliation_and_slippage(conn, client, after=after, until=until)
    except Exception as e:
        print(f"ERROR: Failed Alpaca reconciliation module: {e}")
        conn.close()
        return 1

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

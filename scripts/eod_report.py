#!/usr/bin/env python3
"""
End-of-day master ledger: all spreads *opened* during today's trading session.

Usage:
  python3 scripts/eod_report.py
  python3 scripts/eod_report.py --date 2026-03-27 --tz America/Los_Angeles
  python3 scripts/eod_report.py --db /path/to/alpaca_put_spread.db

DB timestamps are Unix seconds (REAL). The trading day is the calendar date in --tz
(default America/Los_Angeles).
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

ORDERS = "alpaca_orders"
SPREADS = "alpaca_spreads"
TELEMETRY = "spread_telemetry"


@dataclass(frozen=True)
class DayBounds:
    start_ts: float
    end_ts: float
    label: str


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def default_db_path() -> Path:
    return _project_root() / "data" / "alpaca_put_spread.db"


def day_bounds_trading_tz(day: date, tz_name: str) -> DayBounds:
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        print("Python 3.9+ required (zoneinfo).", file=sys.stderr)
        raise SystemExit(1)

    tz = ZoneInfo(tz_name)
    start = datetime.combine(day, datetime.min.time(), tzinfo=tz)
    end = start + timedelta(days=1)
    return DayBounds(
        start_ts=start.timestamp(),
        end_ts=end.timestamp(),
        label=f"{day.isoformat()} [{tz_name}]",
    )


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (name,),
    ).fetchone()
    return row is not None


def _ledger_sql(has_orders: bool, has_telemetry: bool) -> str:
    """All spreads opened in [start, end); optional joins for qty and telemetry aggregates."""
    sub_min = "NULL"
    sub_max = "NULL"
    if has_telemetry:
        sub_min = f"""(
            SELECT MIN(t.current_natural_ask) FROM {TELEMETRY} t
            WHERE t.spread_id = s.spread_id AND t.current_natural_ask IS NOT NULL
        )"""
        sub_max = f"""(
            SELECT MAX(t.current_natural_ask) FROM {TELEMETRY} t
            WHERE t.spread_id = s.spread_id AND t.current_natural_ask IS NOT NULL
        )"""

    if has_orders:
        return f"""
            SELECT
                s.spread_id,
                s.underlying,
                s.strategy_type,
                s.opened_at,
                s.closed_at,
                s.close_reason,
                s.entry_credit_mid,
                s.close_debit_mid,
                s.pnl_dollars,
                o.qty AS entry_qty,
                {sub_min} AS min_natural_ask,
                {sub_max} AS max_natural_ask
            FROM {SPREADS} s
            LEFT JOIN {ORDERS} o ON o.order_id = s.entry_order_id
            WHERE s.opened_at >= ? AND s.opened_at < ?
            ORDER BY s.opened_at ASC
            """

    return f"""
        SELECT
            s.spread_id,
            s.underlying,
            s.strategy_type,
            s.opened_at,
            s.closed_at,
            s.close_reason,
            s.entry_credit_mid,
            s.close_debit_mid,
            s.pnl_dollars,
            NULL AS entry_qty,
            {sub_min} AS min_natural_ask,
            {sub_max} AS max_natural_ask
        FROM {SPREADS} s
        WHERE s.opened_at >= ? AND s.opened_at < ?
        ORDER BY s.opened_at ASC
        """


def _safe_qty(raw: Any) -> int:
    try:
        q = int(raw) if raw is not None else 1
    except (TypeError, ValueError):
        q = 1
    return max(1, q)


def _status_from_row(close_reason: Any) -> str:
    """Per spec: NULL/empty close_reason => OPEN, else CLOSED."""
    if close_reason is None:
        return "OPEN"
    if str(close_reason).strip() == "":
        return "OPEN"
    return "CLOSED"


def _reason_display(close_reason: Any) -> str:
    if close_reason is None or str(close_reason).strip() == "":
        return "pending_expiry"
    return str(close_reason).strip()


def _fmt_close_db(v: Optional[float], width: int) -> str:
    if v is None:
        return f"{'-':>{width}}"
    return f"{float(v):,.2f}".rjust(width)


def run_report(db_path: Path, day: date, tz_name: str) -> int:
    if not db_path.exists():
        print(f"Database not found: {db_path}", file=sys.stderr)
        return 1

    bounds = day_bounds_trading_tz(day, tz_name)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    try:
        if not _table_exists(conn, SPREADS):
            print(f"No {SPREADS} table in {db_path}", file=sys.stderr)
            return 1

        has_orders = _table_exists(conn, ORDERS)
        has_telemetry = _table_exists(conn, TELEMETRY)

        sql = _ledger_sql(has_orders, has_telemetry)
        rows = list(conn.execute(sql, (bounds.start_ts, bounds.end_ts)))

        # Volume: any spread closed during the session (may include prior-day opens)
        n_closed_today = conn.execute(
            f"""
            SELECT COUNT(*) FROM {SPREADS}
            WHERE closed_at IS NOT NULL
              AND closed_at >= ? AND closed_at < ?
            """,
            (bounds.start_ts, bounds.end_ts),
        ).fetchone()[0]

        n_opened_today = len(rows)
        n_open_status = sum(1 for r in rows if _status_from_row(r["close_reason"]) == "OPEN")
        n_closed_in_ledger = n_opened_today - n_open_status

        unrealized = 0.0
        realized_subset = 0.0
        for r in rows:
            qty = _safe_qty(r["entry_qty"])
            ec = r["entry_credit_mid"]
            ec_f = float(ec) if ec is not None else 0.0
            if _status_from_row(r["close_reason"]) == "OPEN":
                unrealized += ec_f * qty * 100.0
            else:
                pnl = r["pnl_dollars"]
                if pnl is not None:
                    realized_subset += float(pnl)

        # --- Report ---
        W = 130
        line = "=" * W
        print(line)
        print(f"  MASTER LEDGER — opens on {bounds.label}")
        print(f"  Database: {db_path}")
        print(line)
        print()
        print("  SESSION SUMMARY")
        print("  " + "-" * (W - 4))
        print(f"  Spreads opened this session:     {n_opened_today:>6}")
        print(f"    · Still OPEN (in ledger):      {n_open_status:>6}")
        print(f"    · Marked CLOSED (in ledger):     {n_closed_in_ledger:>6}")
        print(f"  Spreads closed this session:     {n_closed_today:>6}  (any open date; see DB)")
        print(f"  Unrealized PnL (OPEN @ 100% win): {_fmt_usd(unrealized):>16}  (= Σ entry_cr × qty × 100)")
        print(f"  Realized PnL (CLOSED in ledger):  {_fmt_usd(realized_subset):>16}  (= Σ pnl_dollars)")
        print()

        # Column widths (crisp monospace table)
        c_id, c_ast, c_typ, c_st, c_qty = 6, 7, 5, 7, 4
        c_ecr, c_cdb, c_rsn = 9, 9, 28
        c_mn, c_mx = 9, 9

        print("  MASTER LEDGER (all opens in session)")
        print("  " + "-" * (W - 4))
        hdr = (
            f"  {'ID':>{c_id}}  "
            f"{'Asset':<{c_ast}} "
            f"{'Type':<{c_typ}} "
            f"{'Status':<{c_st}} "
            f"{'Qty':>{c_qty}}  "
            f"{'Entry Cr':>{c_ecr}}  "
            f"{'Close Db':>{c_cdb}}  "
            f"{'Reason':<{c_rsn}}  "
            f"{'Min Ask':>{c_mn}}  "
            f"{'Max Ask':>{c_mx}}"
        )
        print(hdr)
        print("  " + "-" * (W - 4))

        if not rows:
            print(f"  {'(no spreads opened in this window)':<{W - 4}}")
        else:
            for r in rows:
                sid = int(r["spread_id"])
                ast = str(r["underlying"] or "")[:c_ast].ljust(c_ast)
                typ = str(r["strategy_type"] or "")[:c_typ].ljust(c_typ)
                st = _status_from_row(r["close_reason"]).ljust(c_st)
                qty = _safe_qty(r["entry_qty"])
                ec = r["entry_credit_mid"]
                ec_s = f"{float(ec):,.2f}".rjust(c_ecr) if ec is not None else f"{'n/a':>{c_ecr}}"

                cdb_raw = r["close_debit_mid"]
                cdb_s = _fmt_close_db(float(cdb_raw) if cdb_raw is not None else None, c_cdb)

                rsn = _reason_display(r["close_reason"])
                if len(rsn) > c_rsn:
                    rsn = rsn[: c_rsn - 1] + "…"
                rsn = rsn.ljust(c_rsn)

                mn = r["min_natural_ask"]
                mx = r["max_natural_ask"]
                mn_s = f"{float(mn):,.2f}".rjust(c_mn) if mn is not None else f"{'n/a':>{c_mn}}"
                mx_s = f"{float(mx):,.2f}".rjust(c_mx) if mx is not None else f"{'n/a':>{c_mx}}"

                print(
                    f"  {sid:>{c_id}}  {ast} {typ} {st} {qty:>{c_qty}}  "
                    f"{ec_s}  {cdb_s}  {rsn}  {mn_s}  {mx_s}"
                )

        print()
        print("  Notes:")
        print("    • Status OPEN iff close_reason is NULL/empty; else CLOSED.")
        print("    • Reason shows close_reason, or pending_expiry when still open / unset.")
        print("    • Min Ask / Max Ask: best vs worst natural close quote from spread_telemetry (n/a if none).")
        print("    • Unrealized PnL assumes OPEN spreads expire worthless (full entry credit × 100 × qty).")
        if not has_telemetry:
            print(f"    • No {TELEMETRY} table — Min/Max Ask columns are n/a.")
        if not has_orders:
            print(f"    • No {ORDERS} table — qty defaults to 1.")
        print()
        print(line)
        return 0
    finally:
        conn.close()


def _fmt_usd(x: float) -> str:
    sign = "-" if x < 0 else ""
    return f"{sign}${abs(x):,.2f}"


def main() -> None:
    ap = argparse.ArgumentParser(description="Master EOD ledger for alpaca_put_spread.db")
    ap.add_argument("--db", type=Path, default=default_db_path(), help="path to SQLite DB")
    ap.add_argument("--date", type=str, default=None, help="YYYY-MM-DD (default: today in --tz)")
    ap.add_argument(
        "--tz",
        type=str,
        default="America/Los_Angeles",
        help="IANA timezone for calendar day (default: America/Los_Angeles)",
    )
    args = ap.parse_args()

    if args.date:
        try:
            y, m, d = args.date.strip().split("-")
            dday = date(int(y), int(m), int(d))
        except ValueError:
            print("Invalid --date; use YYYY-MM-DD", file=sys.stderr)
            raise SystemExit(1)
    else:
        from zoneinfo import ZoneInfo

        dday = datetime.now(ZoneInfo(args.tz)).date()

    raise SystemExit(run_report(args.db, dday, args.tz))


if __name__ == "__main__":
    main()

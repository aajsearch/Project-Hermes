#!/usr/bin/env python3
"""
Dump v2_tick_log table to CSV with same column names as the table.
By default exports the last 10 windows (by most recent created_at).
Columns: window_id, asset, tick_history_json, created_at
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = REPO_ROOT / "data" / "v2_state.db"
TABLE = "v2_tick_log"
COLUMNS = ("window_id", "asset", "tick_history_json", "created_at")
DEFAULT_OUT = REPO_ROOT / "data" / "v2_tick_log.csv"

# Subquery: last N distinct windows by most recent created_at
LAST_N_WINDOWS_SQL = """
    SELECT window_id, asset, tick_history_json, created_at
    FROM {table}
    WHERE window_id IN (
        SELECT window_id
        FROM {table}
        GROUP BY window_id
        ORDER BY MAX(created_at) DESC
        LIMIT ?
    )
    ORDER BY created_at DESC, window_id, asset
""".format(
    table=TABLE
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Dump last N windows from v2_tick_log to CSV (same column names as table)."
    )
    parser.add_argument(
        "-n", "--windows",
        type=int,
        default=10,
        help="Number of most recent windows to export (default: 10)",
    )
    parser.add_argument(
        "-o", "--out",
        type=Path,
        default=DEFAULT_OUT,
        help=f"Output CSV path (default: {DEFAULT_OUT})",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DB_PATH,
        help=f"Path to v2_state.db (default: {DB_PATH})",
    )
    args = parser.parse_args()

    if not args.db.exists():
        print(f"DB not found: {args.db}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(str(args.db))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(LAST_N_WINDOWS_SQL, (args.windows,))
    rows = cur.fetchall()
    conn.close()

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(COLUMNS)
        for row in rows:
            w.writerow([row[c] for c in COLUMNS])

    print(f"Wrote {len(rows)} rows (last {args.windows} windows) to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

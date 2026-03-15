#!/usr/bin/env python3
"""
Dump all v2_tick_log data to one CSV per asset (same column names as table).
Columns: window_id, asset, tick_history_json, created_at
Output: data/v2_tick_log_<asset>.csv per asset (e.g. v2_tick_log_btc.csv).

Run from project root: python -m tools.dump_v2_tick_log_csv_per_asset [--out-dir path] [--db path]
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
DEFAULT_OUT_DIR = REPO_ROOT / "data"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Dump v2_tick_log to one CSV per asset (same column names as table)."
    )
    parser.add_argument(
        "-o", "--out-dir",
        type=Path,
        default=DEFAULT_OUT_DIR,
        help=f"Output directory for CSVs (default: {DEFAULT_OUT_DIR})",
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
    cur = conn.execute(
        f"SELECT {', '.join(COLUMNS)} FROM {TABLE} ORDER BY asset, created_at DESC, window_id"
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    # Group by asset (normalize key for filename: lowercase, safe)
    by_asset: dict[str, list[dict]] = {}
    for r in rows:
        asset = (r.get("asset") or "").strip().lower() or "unknown"
        by_asset.setdefault(asset, []).append(r)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    total = 0
    for asset, asset_rows in sorted(by_asset.items()):
        out_path = args.out_dir / f"v2_tick_log_{asset}.csv"
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
            w.writeheader()
            for r in asset_rows:
                w.writerow({k: r.get(k, "") for k in COLUMNS})
        total += len(asset_rows)
        print(f"  {out_path.name}: {len(asset_rows)} rows")

    print(f"Wrote {total} rows across {len(by_asset)} assets to {args.out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

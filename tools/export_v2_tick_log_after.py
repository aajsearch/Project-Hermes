#!/usr/bin/env python3
"""
Export v2_tick_log rows after a given window cutoff (e.g. 26MAR150400 = Mar 15, 2026 04:00).

- Source: v2_tick_log in data/v2_state.db
- Filter: window_id suffix (after '-') >= cutoff (e.g. ...26MAR150400 or later)
- Columns: window_id, asset, tick_history_json, created_at

Usage:
  python tools/export_v2_tick_log_after.py 26MAR150400
  python tools/export_v2_tick_log_after.py 26MAR150400 --out data/v2_tick_log_after_26MAR150400.csv
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TABLE = "v2_tick_log"
COLUMNS = ("window_id", "asset", "tick_history_json", "created_at")


def main() -> int:
    parser = argparse.ArgumentParser(description="Export v2_tick_log after a window cutoff (e.g. 26MAR150400).")
    parser.add_argument("cutoff", type=str, help="Window cutoff e.g. 26MAR150400 (Mar 15, 2026 04:00)")
    parser.add_argument("--out", "-o", type=Path, default=None, help="Output CSV path")
    parser.add_argument("--db", type=Path, default=None, help="Path to v2_state.db")
    args = parser.parse_args()

    cutoff = (args.cutoff or "").strip().upper()
    if not cutoff:
        print("Invalid cutoff; use e.g. 26MAR150400", file=sys.stderr)
        return 1

    db_path = args.db or (ROOT / "data" / "v2_state.db")
    if not db_path.exists():
        print(f"DB not found: {db_path}", file=sys.stderr)
        return 1

    out_path = args.out or (ROOT / "data" / f"v2_tick_log_after_{cutoff}.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    # window_id format: fifteen_min_26MAR150400 (underscore before date); use prefix for comparison
    window_prefix = f"fifteen_min_{cutoff}"
    cur = conn.execute(
        """
        SELECT window_id, asset, tick_history_json, created_at
        FROM v2_tick_log
        WHERE window_id >= ?
        ORDER BY created_at ASC, window_id, asset
        """,
        (window_prefix,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in COLUMNS})

    print(f"Wrote {len(rows)} rows to {out_path} (window_id after {cutoff})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

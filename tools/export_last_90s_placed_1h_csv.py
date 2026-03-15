#!/usr/bin/env python3
"""
Export last 1 hour of last_90s_limit_99 rows where we placed an order.
- Source: v2_telemetry_last_90s in data/v2_state.db
- Filter: placed=1, timestamp in last 1 hour
- CSV columns: same as table (id, window_id, asset, placed, seconds_to_close, bid, distance, reason, pre_data, timestamp)

Run from project root: python -m tools.export_last_90s_placed_1h_csv [--out path]
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TABLE = "v2_telemetry_last_90s"
COLUMNS = (
    "id",
    "window_id",
    "asset",
    "placed",
    "seconds_to_close",
    "bid",
    "distance",
    "reason",
    "pre_data",
    "timestamp",
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Export last 1h last_90s_limit_99 placements to CSV (table columns only).")
    parser.add_argument("--hours", type=float, default=1.0, help="Hours of history (default 1)")
    parser.add_argument("--out", type=Path, default=None, help="Output CSV (default: data/last_90s_placed_1h.csv)")
    parser.add_argument("--db", type=Path, default=None, help="Path to v2_state.db (default: data/v2_state.db)")
    args = parser.parse_args()

    db_path = args.db or (ROOT / "data" / "v2_state.db")
    if not db_path.exists():
        print(f"DB not found: {db_path}", file=sys.stderr)
        return 1

    out_path = args.out or (ROOT / "data" / "last_90s_placed_1h.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    cutoff = time.time() - (args.hours * 3600.0)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        f"""
        SELECT {", ".join(COLUMNS)}
        FROM {TABLE}
        WHERE placed = 1 AND timestamp >= ?
        ORDER BY timestamp ASC, id
        """,
        (cutoff,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in COLUMNS})

    print(f"Wrote {len(rows)} rows to {out_path} (last {args.hours}h, placed=1)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

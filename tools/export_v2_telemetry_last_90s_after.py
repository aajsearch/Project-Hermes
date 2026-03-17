#!/usr/bin/env python3
"""
Export v2_telemetry_last_90s rows after a given window cutoff (e.g. 26MAR150400 = Mar 15, 2026 04:00).

- Source: v2_telemetry_last_90s in data/v2_state.db
- Filter: window_id contains the date part and is >= cutoff (e.g. ...26MAR150400 or later)
- Output: CSV with table columns (id, window_id, asset, placed, seconds_to_close, bid, distance, reason, pre_data, timestamp)

Usage:
  python tools/export_v2_telemetry_last_90s_after.py 26MAR150400
  python tools/export_v2_telemetry_last_90s_after.py 26MAR150400 --out data/v2_telemetry_last_90s_after_26MAR150400.csv
"""
from __future__ import annotations

import argparse
import csv
import re
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TABLE = "v2_telemetry_last_90s"
COLUMNS = (
    "id", "window_id", "asset", "placed", "seconds_to_close", "bid", "distance",
    "reason", "pre_data", "timestamp",
)


def _parse_window_cutoff(s: str) -> str:
    """
    Parse 26MAR150400 -> ensure we have a comparable suffix.
    window_id format: fifteen_min_KXBTC15M-26MAR150400 (market_id has 26MAR150400).
    We want window_id >= any id that contains 26MAR150400.
    """
    s = (s or "").strip().upper()
    # Allow 26MAR150400 or 26MAR15 or similar
    if not s:
        return ""
    # If it's 10 chars (DDMMMHHMM), use as-is for LIKE/compare
    return s


def main() -> int:
    parser = argparse.ArgumentParser(description="Export v2_telemetry_last_90s after a window cutoff (e.g. 26MAR150400).")
    parser.add_argument("cutoff", type=str, help="Window cutoff e.g. 26MAR150400 (Mar 15, 2026 04:00)")
    parser.add_argument("--out", "-o", type=Path, default=None, help="Output CSV path")
    parser.add_argument("--db", type=Path, default=None, help="Path to v2_state.db")
    args = parser.parse_args()

    cutoff = _parse_window_cutoff(args.cutoff)
    if not cutoff:
        print("Invalid cutoff; use e.g. 26MAR150400", file=sys.stderr)
        return 1

    db_path = args.db or (ROOT / "data" / "v2_state.db")
    if not db_path.exists():
        print(f"DB not found: {db_path}", file=sys.stderr)
        return 1

    out_path = args.out or (ROOT / "data" / f"v2_telemetry_last_90s_after_{cutoff}.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    # window_id is like fifteen_min_KXBTC15M-26MAR150400; we want rows where the market part >= 26MAR150400
    # So: window_id LIKE '%26MAR15%' AND window_id >= 'fifteen_min_%-26MAR150400' doesn't work.
    # Use: window_id LIKE '%26MAR15%' and extract the time part, or use timestamp.
    # Simpler: window_id contains the date (26MAR15) and the suffix after the last - is >= cutoff.
    # In SQL: (window_id LIKE '%' || ? || '%' AND SUBSTR(window_id, INSTR(window_id, '-') + 1) >= ?)
    # cutoff is 26MAR150400; so we need window_id where the part after '-' >= '26MAR150400'
    cur = conn.execute(
        """
        SELECT id, window_id, asset, placed, seconds_to_close, bid, distance, reason, pre_data, timestamp
        FROM v2_telemetry_last_90s
        WHERE INSTR(window_id, '-') > 0
          AND SUBSTR(window_id, INSTR(window_id, '-') + 1) >= ?
        ORDER BY timestamp ASC, window_id, asset, id
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

    print(f"Wrote {len(rows)} rows to {out_path} (window_id after {cutoff})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

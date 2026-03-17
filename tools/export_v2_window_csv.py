#!/usr/bin/env python3
"""
Export v2_tick_log and v2_telemetry_last_90s for a single window to two separate CSV files.

- v2_tick_log: window_id = fifteen_min_<WINDOW> (e.g. fifteen_min_26MAR152015)
- v2_telemetry_last_90s: window_id suffix (after '-') = WINDOW (e.g. ...-26MAR152015)

Usage:
  python tools/export_v2_window_csv.py 26MAR152015
  python tools/export_v2_window_csv.py 26MAR152015 --tick-out data/tick_26MAR152015.csv --telemetry-out data/telemetry_26MAR152015.csv
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

TICK_TABLE = "v2_tick_log"
TICK_COLUMNS = ("window_id", "asset", "tick_history_json", "created_at")
TELEMETRY_TABLE = "v2_telemetry_last_90s"
TELEMETRY_COLUMNS = (
    "id", "window_id", "asset", "placed", "seconds_to_close", "bid", "distance",
    "reason", "pre_data", "timestamp",
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Export v2_tick_log and v2_telemetry_last_90s for one window to two CSVs.")
    parser.add_argument("window", type=str, help="Window id e.g. 26MAR152015")
    parser.add_argument("--tick-out", type=Path, default=None, help="Output CSV for v2_tick_log")
    parser.add_argument("--telemetry-out", type=Path, default=None, help="Output CSV for v2_telemetry_last_90s")
    parser.add_argument("--db", type=Path, default=None, help="Path to v2_state.db")
    args = parser.parse_args()

    window = (args.window or "").strip().upper()
    if not window:
        print("Invalid window; use e.g. 26MAR152015", file=sys.stderr)
        return 1

    db_path = args.db or (ROOT / "data" / "v2_state.db")
    if not db_path.exists():
        print(f"DB not found: {db_path}", file=sys.stderr)
        return 1

    tick_out = args.tick_out or (ROOT / "data" / f"v2_tick_log_{window}.csv")
    telemetry_out = args.telemetry_out or (ROOT / "data" / f"v2_telemetry_last_90s_{window}.csv")
    tick_out.parent.mkdir(parents=True, exist_ok=True)
    telemetry_out.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # v2_tick_log: window_id = 'fifteen_min_26MAR152015'
    tick_window_id = f"fifteen_min_{window}"
    cur = conn.execute(
        f"""
        SELECT {", ".join(TICK_COLUMNS)}
        FROM {TICK_TABLE}
        WHERE window_id = ?
        ORDER BY asset
        """,
        (tick_window_id,),
    )
    tick_rows = [dict(r) for r in cur.fetchall()]

    with open(tick_out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=TICK_COLUMNS, extrasaction="ignore")
        w.writeheader()
        for r in tick_rows:
            w.writerow({k: r.get(k, "") for k in TICK_COLUMNS})
    print(f"Wrote {len(tick_rows)} rows to {tick_out} (v2_tick_log window={window})")

    # v2_telemetry_last_90s: window_id suffix after '-' = '26MAR152015'
    cur = conn.execute(
        f"""
        SELECT {", ".join(TELEMETRY_COLUMNS)}
        FROM {TELEMETRY_TABLE}
        WHERE INSTR(window_id, '-') > 0
          AND SUBSTR(window_id, INSTR(window_id, '-') + 1) = ?
        ORDER BY timestamp ASC, asset, id
        """,
        (window,),
    )
    telemetry_rows = [dict(r) for r in cur.fetchall()]

    with open(telemetry_out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=TELEMETRY_COLUMNS, extrasaction="ignore")
        w.writeheader()
        for r in telemetry_rows:
            w.writerow({k: r.get(k, "") for k in TELEMETRY_COLUMNS})
    print(f"Wrote {len(telemetry_rows)} rows to {telemetry_out} (v2_telemetry_last_90s window={window})")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

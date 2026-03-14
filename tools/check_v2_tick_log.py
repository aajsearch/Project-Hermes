#!/usr/bin/env python3
"""
Query v2_tick_log and print summary + sample to verify tick data is stored correctly.
Usage: from repo root, run: python tools/check_v2_tick_log.py [--window WINDOW_ID] [--limit N]
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

# Repo root
REPO_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = REPO_ROOT / "data" / "v2_state.db"
TABLE = "v2_tick_log"


def main() -> None:
    ap = argparse.ArgumentParser(description="Inspect v2_tick_log table")
    ap.add_argument("--window", type=str, help="Show only this window_id (e.g. fifteen_min_26MAR141400)")
    ap.add_argument("--limit", type=int, default=20, help="Max windows to list (default 20)")
    ap.add_argument("--sample-ticks", type=int, default=3, help="Show first N ticks in sample (default 3)")
    args = ap.parse_args()

    if not DB_PATH.exists():
        print(f"DB not found: {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Table summary
    cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
    total_rows = cur.fetchone()[0]
    cur.execute(f"SELECT COUNT(DISTINCT window_id) FROM {TABLE}")
    total_windows = cur.fetchone()[0]
    print(f"Table: {TABLE}")
    print(f"  Total rows: {total_rows}")
    print(f"  Distinct windows: {total_windows}")
    print()

    # List windows (latest first)
    if args.window:
        cur.execute(
            f"SELECT window_id, asset, length(tick_history_json) as json_len, created_at FROM {TABLE} WHERE window_id = ? ORDER BY asset",
            (args.window,),
        )
        rows = cur.fetchall()
        if not rows:
            print(f"No rows for window_id={args.window!r}")
            conn.close()
            sys.exit(0)
        print(f"Window: {args.window} ({len(rows)} assets)")
    else:
        cur.execute(
            f"""
            SELECT window_id, COUNT(*) as assets, MIN(created_at) as created_at
            FROM {TABLE}
            GROUP BY window_id
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (args.limit,),
        )
        windows = cur.fetchall()
        print(f"Latest {len(windows)} windows:")
        for w in windows:
            print(f"  {w['window_id']}  assets={w['assets']}  created_at={w['created_at']:.0f}")
        print()
        # Pick latest window for sample
        if not windows:
            conn.close()
            return
        sample_window = windows[0]["window_id"]
        cur.execute(
            f"SELECT window_id, asset, tick_history_json, created_at FROM {TABLE} WHERE window_id = ? ORDER BY asset",
            (sample_window,),
        )
        rows = cur.fetchall()
        print(f"Sample window: {sample_window}")
        print()

    # Per-row: asset, tick count, and first few ticks
    for row in rows:
        asset = row["asset"]
        js = row["tick_history_json"]
        try:
            data = json.loads(js)
        except Exception as e:
            print(f"  [{asset}] JSON parse error: {e}")
            continue
        sec = data.get("sec") or []
        n = len(sec)
        print(f"  asset={asset}  ticks={n}  keys={list(data.keys())}")
        if n > 0 and args.sample_ticks > 0:
            k = min(args.sample_ticks, n)
            sample = {key: (val[:k] if isinstance(val, list) else val) for key, val in data.items()}
            print(f"    first {k} tick(s): {json.dumps(sample, allow_nan=False)}")
        print()

    conn.close()


if __name__ == "__main__":
    main()

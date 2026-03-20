#!/usr/bin/env python3
"""
Asset-wise joined V2 reports for the last N hours.

For each asset with strategy report rows in the time range, this script writes:
1) data/v2_asset_join_last90s_<asset>_last8h.csv
2) data/v2_asset_join_atm_<asset>_last8h.csv

Each CSV is one row per strategy report (v2_strategy_reports) and joins:
  - v2_order_registry   (by order_id)
  - v2_tick_log         (by window_id + asset)
  - latest telemetry row (<= resolved_at) from the corresponding telemetry table

Column naming rules (to align with "same column names as tables"):
  - strategy_reports columns are used as-is.
  - order_registry columns included are unique (skips duplicate keys like order_id/asset/interval/strategy_id).
  - tick_log columns included are unique (tick_history_json, created_at).
  - telemetry columns included are used as-is, except we omit duplicate join keys:
      last_90s telemetry: omit window_id/asset
      atm telemetry: omit window_id/asset/side/entry_price_cents (already present in strategy_reports)
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Optional

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = ROOT / "data" / "v2_state.db"
DEFAULT_OUT_DIR = ROOT / "data"


STRAT_COLS = [
    "order_id",
    "strategy_id",
    "interval",
    "window_id",
    "asset",
    "side",
    "entry_price_cents",
    "exit_price_cents",
    "outcome",
    "is_stop_loss",
    "pnl_cents",
    "resolved_at",
]

REG_COLS = [
    "market_id",
    "ticker",
    "status",
    "filled_count",
    "count",
    "limit_price_cents",
    "placed_at",
    "client_order_id",
    "placement_bid_cents",
    "entry_distance",
    "entry_distance_at_fill",
]

TICK_COLS = [
    "tick_history_json",
    "created_at",
]

TELEMETRY_LAST90S_COLS = [
    "id",
    "placed",
    "seconds_to_close",
    "bid",
    "distance",
    "reason",
    "pre_data",
    "timestamp",
]

TELEMETRY_ATM_COLS = [
    # from v2_telemetry_atm, excluding duplicates:
    # - window_id, asset, side, entry_price_cents already exist in strategy_reports
    "id",
    "strike",
    "spot_kraken",
    "spot_coinbase",
    "distance",
    "reason",
    "pre_data",
    "timestamp",
]


def _fetch_one_as_dict(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...]) -> Optional[Dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    cur = conn.execute(sql, params)
    row = cur.fetchone()
    if row is None:
        return None
    return dict(row)


def main() -> int:
    parser = argparse.ArgumentParser(description="Export per-asset joined V2 CSVs for the last N hours.")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help=f"Path to v2_state.db (default: {DEFAULT_DB_PATH})")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR, help=f"Output directory (default: {DEFAULT_OUT_DIR})")
    parser.add_argument("--hours", type=float, default=8.0, help="Lookback hours (default: 8)")
    args = parser.parse_args()

    if not args.db.exists():
        print(f"DB not found: {args.db}")
        return 1

    cutoff_ts = time.time() - float(args.hours) * 3600.0
    assets_sql = """
        SELECT DISTINCT asset
        FROM v2_strategy_reports
        WHERE resolved_at >= ?
        ORDER BY asset
    """

    conn = sqlite3.connect(str(args.db))
    conn.row_factory = sqlite3.Row
    cur = conn.execute(assets_sql, (cutoff_ts,))
    assets = [str(r["asset"]).strip().lower() for r in cur.fetchall() if r["asset"] is not None]

    if not assets:
        print(f"No v2_strategy_reports rows found in last {args.hours} hours.")
        conn.close()
        return 2

    args.out_dir.mkdir(parents=True, exist_ok=True)

    # Common query fragments
    strat_sql = f"""
        SELECT {", ".join(STRAT_COLS)}
        FROM v2_strategy_reports
        WHERE resolved_at >= ? AND LOWER(asset) = ?
        ORDER BY resolved_at ASC, order_id
    """

    reg_sql = f"""
        SELECT {", ".join(REG_COLS)}
        FROM v2_order_registry
        WHERE order_id = ?
        LIMIT 1
    """

    tick_sql = f"""
        SELECT {", ".join(TICK_COLS)}
        FROM v2_tick_log
        WHERE window_id = ? AND LOWER(asset) = ?
        LIMIT 1
    """

    last90_sql = f"""
        SELECT {", ".join(TELEMETRY_LAST90S_COLS)}
        FROM v2_telemetry_last_90s
        WHERE window_id = ?
          AND LOWER(asset) = ?
          AND timestamp <= ?
        ORDER BY timestamp DESC, id DESC
        LIMIT 1
    """

    atm_sql = f"""
        SELECT {", ".join(TELEMETRY_ATM_COLS)}
        FROM v2_telemetry_atm
        WHERE window_id = ?
          AND LOWER(asset) = ?
          AND timestamp <= ?
        ORDER BY timestamp DESC, id DESC
        LIMIT 1
    """

    for asset in assets:
        strat_cur = conn.execute(strat_sql, (cutoff_ts, asset))
        strat_rows = [dict(r) for r in strat_cur.fetchall()]
        if not strat_rows:
            continue

        out_last90 = args.out_dir / f"v2_asset_join_last90s_{asset}_last{int(args.hours)}h.csv"
        out_atm = args.out_dir / f"v2_asset_join_atm_{asset}_last{int(args.hours)}h.csv"

        last90_fieldnames = STRAT_COLS + REG_COLS + TICK_COLS + TELEMETRY_LAST90S_COLS
        atm_fieldnames = STRAT_COLS + REG_COLS + TICK_COLS + TELEMETRY_ATM_COLS

        with out_last90.open("w", newline="", encoding="utf-8") as f1, out_atm.open("w", newline="", encoding="utf-8") as f2:
            w1 = csv.DictWriter(f1, fieldnames=last90_fieldnames, extrasaction="ignore")
            w2 = csv.DictWriter(f2, fieldnames=atm_fieldnames, extrasaction="ignore")
            w1.writeheader()
            w2.writeheader()

            for sr in strat_rows:
                window_id = sr.get("window_id")
                order_id = sr.get("order_id")
                resolved_at = sr.get("resolved_at") or cutoff_ts

                reg = _fetch_one_as_dict(conn, reg_sql, (order_id,))
                tick = _fetch_one_as_dict(conn, tick_sql, (window_id, asset))
                last90 = _fetch_one_as_dict(conn, last90_sql, (window_id, asset, resolved_at))
                atm = _fetch_one_as_dict(conn, atm_sql, (window_id, asset, resolved_at))

                base: Dict[str, Any] = {}
                for k in STRAT_COLS:
                    base[k] = sr.get(k)
                for k in REG_COLS:
                    base[k] = reg.get(k) if reg else None
                for k in TICK_COLS:
                    base[k] = tick.get(k) if tick else None

                row_last90 = dict(base)
                if last90:
                    for k in TELEMETRY_LAST90S_COLS:
                        row_last90[k] = last90.get(k)
                else:
                    for k in TELEMETRY_LAST90S_COLS:
                        row_last90[k] = None

                row_atm = dict(base)
                if atm:
                    for k in TELEMETRY_ATM_COLS:
                        row_atm[k] = atm.get(k)
                else:
                    for k in TELEMETRY_ATM_COLS:
                        row_atm[k] = None

                w1.writerow(row_last90)
                w2.writerow(row_atm)

        print(f"Wrote: {out_last90.name} and {out_atm.name} ({len(strat_rows)} rows)")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


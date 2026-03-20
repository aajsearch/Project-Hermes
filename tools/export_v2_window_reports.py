#!/usr/bin/env python3
"""
Export joined V2 debug reports (tick log + strategy reports + order registry + telemetry)
for a given window_id slot range, one CSV per asset.

Usage (from repo root):
  python tools/export_v2_window_reports.py --start-slot 26MAR172045 --end-slot 26MAR180100

When do tables get data?
  - v2_tick_log: every tick during the window (one row per asset per window, flushed on transition).
  - v2_telemetry_last_90s: when last_90s evaluate_entry runs in the active window (each skip or intent_fired).
  - v2_telemetry_atm: when ATM breakout records an entry or skip in the window.
  - v2_strategy_reports: only when a trade is closed (stop-loss or take-profit executed). No exit => no row.
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "v2_state.db"
OUT_DIR = ROOT / "data"


def _slot_from_window_id(window_id: str) -> str:
    """
    Extract the logical time slot from a window_id (supports both formats in DB).
    Examples:
      fifteen_min_26MAR172045 -> 26MAR172045
      fifteen_min_KXBTC15M-26MAR180100 -> 26MAR180100 (legacy: full market_id after interval_)
      hourly_26MAR180100 -> 26MAR180100
    """
    if not window_id:
        return ""
    rest = window_id.split("_", 1)[1] if "_" in window_id else window_id
    # Legacy: rest may be full market_id like KXBTC15M-26MAR180100; slot is after last "-".
    if "-" in rest:
        return rest.split("-")[-1].strip()
    return rest


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? COLLATE NOCASE",
        (name,),
    )
    return cur.fetchone() is not None


@dataclass
class JoinedRow:
    # Keys from v2_strategy_reports
    order_id: str
    strategy_id: str
    interval: str
    window_id: str
    asset: str
    side: str
    entry_price_cents: Optional[int]
    exit_price_cents: Optional[int]
    outcome: Optional[str]
    is_stop_loss: Optional[int]
    pnl_cents: Optional[int]
    resolved_at: Optional[float]
    # From v2_order_registry
    limit_price_cents: Optional[int]
    placement_bid_cents: Optional[int]
    placed_at: Optional[float]
    entry_distance: Optional[float]
    # From v2_tick_log
    tick_history_json: Optional[str]
    # Aggregated telemetry JSON blobs (per (window_id, asset))
    telemetry_last90_json: Optional[str]
    telemetry_atm_json: Optional[str]


def load_tick_log(conn: sqlite3.Connection, start_slot: str, end_slot: str) -> Dict[Tuple[str, str], str]:
    mapping: Dict[Tuple[str, str], str] = {}
    cur = conn.execute(
        "SELECT window_id, asset, tick_history_json FROM v2_tick_log"
    )
    for window_id, asset, tick_history_json in cur.fetchall():
        slot = _slot_from_window_id(window_id)
        if not slot:
            continue
        if start_slot <= slot <= end_slot:
            key = (window_id, (asset or "").strip().lower())
            mapping[key] = tick_history_json
    return mapping


def load_telemetry(
    conn: sqlite3.Connection,
    table: str,
    start_slot: str,
    end_slot: str,
) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    by_key: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    if not _table_exists(conn, table):
        return by_key
    # v2_telemetry_last_90s and v2_telemetry_atm have different schemas; select all and filter in Python.
    cur = conn.execute(f"SELECT * FROM {table}")
    cols = [d[0] for d in cur.description]
    for row in cur.fetchall():
        row_dict = {cols[i]: row[i] for i in range(len(cols))}
        window_id = row_dict.get("window_id") or ""
        asset = (row_dict.get("asset") or "").strip().lower()
        slot = _slot_from_window_id(window_id)
        if not slot or not (start_slot <= slot <= end_slot):
            continue
        key = (window_id, asset)
        by_key.setdefault(key, []).append(row_dict)
    return by_key


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export joined V2 reports (tick_log + strategy_reports + registry + telemetry) per asset."
    )
    parser.add_argument("--start-slot", required=True, help="Start logical slot, e.g. 26MAR172045")
    parser.add_argument("--end-slot", required=True, help="End logical slot, e.g. 26MAR180100")
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"DB not found at {DB_PATH}")
        return 1

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    tick_log = load_tick_log(conn, args.start_slot, args.end_slot)
    telemetry_last90 = load_telemetry(conn, "v2_telemetry_last_90s", args.start_slot, args.end_slot)
    telemetry_atm = load_telemetry(conn, "v2_telemetry_atm", args.start_slot, args.end_slot)

    # Load strategy reports and join with registry
    cur = conn.execute(
        """
        SELECT
            r.order_id,
            r.strategy_id,
            r.interval,
            r.window_id,
            r.asset,
            r.side,
            r.entry_price_cents,
            r.exit_price_cents,
            r.outcome,
            r.is_stop_loss,
            r.pnl_cents,
            r.resolved_at,
            reg.limit_price_cents,
            reg.placement_bid_cents,
            reg.placed_at,
            reg.entry_distance
        FROM v2_strategy_reports r
        LEFT JOIN v2_order_registry reg ON reg.order_id = r.order_id
        """
    )

    rows_by_asset: Dict[str, List[JoinedRow]] = {}
    for row in cur.fetchall():
        window_id = row["window_id"]
        slot = _slot_from_window_id(window_id)
        if not slot or not (args.start_slot <= slot <= args.end_slot):
            continue
        asset = (row["asset"] or "").strip().lower()
        key = (window_id, asset)
        tick_json = tick_log.get(key)
        last90_rows = telemetry_last90.get(key) or []
        atm_rows = telemetry_atm.get(key) or []
        jr = JoinedRow(
            order_id=row["order_id"],
            strategy_id=row["strategy_id"],
            interval=row["interval"],
            window_id=window_id,
            asset=asset,
            side=row["side"],
            entry_price_cents=row["entry_price_cents"],
            exit_price_cents=row["exit_price_cents"],
            outcome=row["outcome"],
            is_stop_loss=row["is_stop_loss"],
            pnl_cents=row["pnl_cents"],
            resolved_at=row["resolved_at"],
            limit_price_cents=row["limit_price_cents"],
            placement_bid_cents=row["placement_bid_cents"],
            placed_at=row["placed_at"],
            entry_distance=row["entry_distance"],
            tick_history_json=tick_json,
            telemetry_last90_json=json.dumps(last90_rows) if last90_rows else None,
            telemetry_atm_json=json.dumps(atm_rows) if atm_rows else None,
        )
        rows_by_asset.setdefault(asset, []).append(jr)

    conn.close()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for asset, rows in rows_by_asset.items():
        if not rows:
            continue
        out_path = OUT_DIR / f"v2_window_report_{asset}_{args.start_slot}_{args.end_slot}.csv"
        # Collect all fieldnames from JoinedRow dataclass
        fieldnames = list(asdict(rows[0]).keys())
        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for jr in rows:
                writer.writerow(asdict(jr))
        print(f"Wrote {len(rows)} rows to {out_path}")

    if not rows_by_asset:
        print("No strategy reports found in the requested slot range.")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


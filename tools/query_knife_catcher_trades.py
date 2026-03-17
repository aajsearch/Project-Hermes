#!/usr/bin/env python3
"""
Query trade information for the knife_catcher strategy: entries, and (when recorded) TP/SL outcomes.

Data sources:
- v2_order_registry: every order (entry) — placement_bid_cents, limit_price_cents, placed_at, status.
- v2_strategy_reports: closed trades with outcome, is_stop_loss, entry/exit price, pnl (when record_trade_outcome is used).
- v2_sl_execution_audit: exit attempts (exit_action: take_profit, stop_loss, market_sell) for TP/SL attribution.

Usage:
  python tools/query_knife_catcher_trades.py [--db PATH] [--interval fifteen_min] [--asset btc] [--hours 168]
  python tools/query_knife_catcher_trades.py --all   # no time filter on registry
  python tools/query_knife_catcher_trades.py --output reports/knife_catcher_report.md  # write report file
  python tools/query_knife_catcher_trades.py --csv data/knife_catcher_trades.csv        # export CSV
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

STRATEGY_ID = "knife_catcher"


def _default_db_path() -> Path:
    return PROJECT_ROOT / "data" / "v2_state.db"


def main() -> None:
    ap = argparse.ArgumentParser(description="Query knife_catcher entries and TP/SL reports")
    ap.add_argument("--db", type=str, default=None, help="Path to v2_state.db (default: data/v2_state.db)")
    ap.add_argument("--interval", type=str, default=None, help="Filter by interval (e.g. fifteen_min)")
    ap.add_argument("--asset", type=str, default=None, help="Filter by asset (btc, eth, sol, xrp)")
    ap.add_argument("--hours", type=float, default=168, help="Only orders placed in last N hours (default: 168 = 1 week)")
    ap.add_argument("--all", action="store_true", help="No time filter on registry entries")
    ap.add_argument("--output", "-o", type=str, default=None, help="Write report to file (e.g. reports/knife_catcher_report.md)")
    ap.add_argument("--csv", type=str, default=None, help="Export entries + closed trades to CSV file")
    args = ap.parse_args()

    db_path = Path(args.db) if args.db else _default_db_path()
    if not db_path.is_absolute():
        db_path = PROJECT_ROOT / db_path
    if not db_path.exists():
        print(f"DB not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # --- 1. Entries (v2_order_registry) ---
    cutoff_ts = None if args.all else (datetime.now(timezone.utc) - timedelta(hours=args.hours)).timestamp()
    query = """
        SELECT order_id, strategy_id, interval, market_id, asset, ticker, side, status,
               filled_count, count, limit_price_cents, placed_at, placement_bid_cents, client_order_id
        FROM v2_order_registry
        WHERE strategy_id = ?
    """
    params: list = [STRATEGY_ID]
    if args.interval:
        query += " AND interval = ?"
        params.append(args.interval)
    if args.asset:
        query += " AND LOWER(asset) = LOWER(?)"
        params.append(args.asset)
    if cutoff_ts is not None:
        query += " AND placed_at >= ?"
        params.append(cutoff_ts)
    query += " ORDER BY placed_at DESC"
    cur = conn.execute(query, params)
    entries = [dict(r) for r in cur.fetchall()]

    # --- 2. Closed trades / TP-SL (v2_strategy_reports) ---
    report_params: list = [STRATEGY_ID]
    report_query = "SELECT * FROM v2_strategy_reports WHERE strategy_id = ?"
    if args.interval:
        report_query += " AND interval = ?"
        report_params.append(args.interval)
    if args.asset:
        report_query += " AND LOWER(asset) = LOWER(?)"
        report_params.append(args.asset)
    report_query += " ORDER BY resolved_at DESC LIMIT 200"
    cur = conn.execute(report_query, report_params)
    reports = [dict(r) for r in cur.fetchall()]

    # --- 3. Exit attempts (v2_sl_execution_audit) ---
    try:
        audit_params: list = [STRATEGY_ID]
        audit_query = """
            SELECT order_id, exit_action, ticker, side, sell_count, attempt, success, ts
            FROM v2_sl_execution_audit
            WHERE order_id IN (
                SELECT order_id FROM v2_order_registry WHERE strategy_id = ?
            )
        """
        if cutoff_ts is not None:
            audit_query += " AND ts >= ?"
            audit_params.append(cutoff_ts)
        audit_query += " ORDER BY ts DESC LIMIT 100"
        cur = conn.execute(audit_query, audit_params)
        audits = [dict(r) for r in cur.fetchall()]
    except sqlite3.OperationalError:
        audits = []

    conn.close()

    # Build report lines
    lines: List[str] = []
    lines.append("# Knife Catcher Strategy Report")
    lines.append("")
    lines.append(f"Generated: {datetime.now(timezone.utc).isoformat()}")
    lines.append(f"DB: {db_path}")
    lines.append("")

    lines.append("## Entries (v2_order_registry)")
    lines.append("")
    if not entries:
        lines.append("  (none)")
    else:
        for r in entries:
            placed = datetime.fromtimestamp(r["placed_at"], tz=timezone.utc).isoformat() if r.get("placed_at") else ""
            lines.append(
                f"  order_id={r.get('order_id')} asset={r.get('asset')} side={r.get('side')} "
                f"status={r.get('status')} placement_bid={r.get('placement_bid_cents')}c "
                f"limit={r.get('limit_price_cents')}c count={r.get('count')} placed_at={placed}"
            )
    lines.append("")

    lines.append("## Closed Trades (TP/SL, PnL)")
    lines.append("")
    if not reports:
        lines.append("  (none)")
    else:
        total_pnl = sum((r.get("pnl_cents") or 0) for r in reports)
        wins = sum(1 for r in reports if (r.get("outcome") or "").lower() == "win")
        losses = sum(1 for r in reports if (r.get("outcome") or "").lower() == "loss")
        lines.append(f"  Total trades: {len(reports)}  |  Wins: {wins}  |  Losses: {losses}  |  PnL (cents): {total_pnl}")
        lines.append("")
        for r in reports:
            resolved = datetime.fromtimestamp(r["resolved_at"], tz=timezone.utc).isoformat() if r.get("resolved_at") else ""
            sl = "stop_loss=1" if r.get("is_stop_loss") else "stop_loss=0"
            lines.append(
                f"  order_id={r.get('order_id')} asset={r.get('asset')} side={r.get('side')} "
                f"entry={r.get('entry_price_cents')}c exit={r.get('exit_price_cents')}c "
                f"outcome={r.get('outcome')} {sl} pnl_cents={r.get('pnl_cents')} resolved_at={resolved}"
            )
    lines.append("")

    lines.append("## Exit Attempts (v2_sl_execution_audit)")
    lines.append("")
    if not audits:
        lines.append("  (none or table missing)")
    else:
        for r in audits:
            ts = datetime.fromtimestamp(r["ts"], tz=timezone.utc).isoformat() if r.get("ts") else ""
            lines.append(
                f"  order_id={r.get('order_id')} exit_action={r.get('exit_action')} "
                f"success={r.get('success')} sell_count={r.get('sell_count')} ts={ts}"
            )
    lines.append("")

    report_text = "\n".join(lines)
    if args.output:
        out_path = Path(args.output)
        if not out_path.is_absolute():
            out_path = PROJECT_ROOT / out_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(report_text, encoding="utf-8")
        print(f"Report written to {out_path}", file=sys.stderr)
    else:
        print(report_text)

    if args.csv:
        csv_path = Path(args.csv)
        if not csv_path.is_absolute():
            csv_path = PROJECT_ROOT / csv_path
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["order_id", "asset", "side", "status", "placement_bid_cents", "limit_price_cents", "count", "placed_at", "entry_price_cents", "exit_price_cents", "outcome", "is_stop_loss", "pnl_cents", "resolved_at"])
            order_to_report = {r["order_id"]: r for r in reports}
            for r in entries:
                rep = order_to_report.get(r["order_id"], {})
                placed = datetime.fromtimestamp(r["placed_at"], tz=timezone.utc).isoformat() if r.get("placed_at") else ""
                resolved = datetime.fromtimestamp(rep["resolved_at"], tz=timezone.utc).isoformat() if rep.get("resolved_at") else ""
                w.writerow([
                    r.get("order_id"), r.get("asset"), r.get("side"), r.get("status"),
                    r.get("placement_bid_cents"), r.get("limit_price_cents"), r.get("count"), placed,
                    rep.get("entry_price_cents"), rep.get("exit_price_cents"), rep.get("outcome"),
                    rep.get("is_stop_loss"), rep.get("pnl_cents"), resolved,
                ])
        print(f"CSV written to {csv_path}", file=sys.stderr)


if __name__ == "__main__":
    main()

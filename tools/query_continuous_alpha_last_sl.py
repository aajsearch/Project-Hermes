#!/usr/bin/env python3
"""
Offline analysis: last N stop-losses for continuous_alpha_limit_99 (V2) from v2_state.db.

Does not run the bot or change any trading logic. Safe to run while the bot is stopped
or read-only against a copied DB file.

For each row you get:
  - sl_reason: sub-reason from v2_sl_execution_audit (e.g. sl_normal_persistence, sl_pct_late_window)
  - sec_placement_to_sl: seconds from order placed_at to outcome resolved_at (proxy for "time to SL")
  - bid_at_sl_exit_cents: exit fill from v2_strategy_reports (IoC market sell)
  - entry: bid at entry, limit, distances, side, asset, window_id, ticker, order_id, timestamps

Usage (from repo root):
  python tools/query_continuous_alpha_last_sl.py
  python tools/query_continuous_alpha_last_sl.py --db /path/to/copy_of_v2_state.db --n 5
  python tools/query_continuous_alpha_last_sl.py --json
  python tools/query_continuous_alpha_last_sl.py --strategy-id last_90s_limit_99

Raw SQL (sqlite3 CLI) equivalent — strategy_id = 'continuous_alpha_limit_99':

  SELECT rep.order_id, rep.asset, rep.side,
         rep.entry_price_cents, rep.exit_price_cents, rep.resolved_at, rep.window_id,
         r.placed_at, r.placement_bid_cents, r.entry_distance, r.entry_distance_at_fill,
         r.limit_price_cents, r.ticker, r.market_id,
         (SELECT exit_action FROM v2_sl_execution_audit
          WHERE order_id = rep.order_id AND success = 1 ORDER BY id DESC LIMIT 1) AS sl_reason,
         (SELECT ts FROM v2_sl_execution_audit
          WHERE order_id = rep.order_id AND success = 1 ORDER BY id DESC LIMIT 1) AS sl_fired_ts
  FROM v2_strategy_reports rep
  INNER JOIN v2_order_registry r ON r.order_id = rep.order_id
  WHERE rep.strategy_id = 'continuous_alpha_limit_99' AND rep.is_stop_loss = 1
  ORDER BY rep.resolved_at DESC
  LIMIT 5;
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path


def _utc(ts: float | None) -> str:
    if ts is None:
        return ""
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except (TypeError, ValueError, OSError):
        return str(ts)


def fetch_last_n_sl(
    db_path: Path,
    strategy_id: str,
    n: int,
) -> list[dict]:
    sql_with_audit = """
        SELECT
            rep.order_id,
            rep.asset,
            rep.side,
            rep.entry_price_cents,
            rep.exit_price_cents,
            rep.resolved_at,
            rep.window_id,
            r.placed_at,
            r.placement_bid_cents,
            r.entry_distance,
            r.entry_distance_at_fill,
            r.limit_price_cents,
            r.ticker,
            r.market_id,
            (SELECT exit_action FROM v2_sl_execution_audit
             WHERE order_id = rep.order_id AND success = 1
             ORDER BY id DESC LIMIT 1) AS sl_reason,
            (SELECT ts FROM v2_sl_execution_audit
             WHERE order_id = rep.order_id AND success = 1
             ORDER BY id DESC LIMIT 1) AS sl_fired_ts
        FROM v2_strategy_reports rep
        INNER JOIN v2_order_registry r ON r.order_id = rep.order_id
        WHERE rep.strategy_id = ? AND rep.is_stop_loss = 1
        ORDER BY rep.resolved_at DESC
        LIMIT ?
    """
    sql_no_audit = """
        SELECT
            rep.order_id,
            rep.asset,
            rep.side,
            rep.entry_price_cents,
            rep.exit_price_cents,
            rep.resolved_at,
            rep.window_id,
            r.placed_at,
            r.placement_bid_cents,
            r.entry_distance,
            r.entry_distance_at_fill,
            r.limit_price_cents,
            r.ticker,
            r.market_id,
            NULL AS sl_reason,
            NULL AS sl_fired_ts
        FROM v2_strategy_reports rep
        INNER JOIN v2_order_registry r ON r.order_id = rep.order_id
        WHERE rep.strategy_id = ? AND rep.is_stop_loss = 1
        ORDER BY rep.resolved_at DESC
        LIMIT ?
    """
    conn = sqlite3.connect(str(db_path))
    try:
        has_audit = (
            conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='v2_sl_execution_audit'"
            ).fetchone()
            is not None
        )
        sql = sql_with_audit if has_audit else sql_no_audit
        cur = conn.execute(sql, (strategy_id, n))
        rows = cur.fetchall()
    finally:
        conn.close()

    out: list[dict] = []
    for row in rows:
        (
            order_id,
            asset,
            side,
            entry_price_cents,
            exit_price_cents,
            resolved_at,
            window_id,
            placed_at,
            placement_bid_cents,
            entry_distance,
            entry_distance_at_fill,
            limit_price_cents,
            ticker,
            market_id,
            sl_reason,
            sl_fired_ts,
        ) = row
        pf = float(placed_at) if placed_at is not None else None
        rf = float(resolved_at) if resolved_at is not None else None
        sec_to_sl = (rf - pf) if (pf is not None and rf is not None) else None
        out.append(
            {
                "order_id": order_id,
                "asset": (asset or "").strip().lower(),
                "side": side,
                "sl_reason": sl_reason or "unknown",
                "sec_placement_to_sl": round(sec_to_sl, 3) if sec_to_sl is not None else None,
                "bid_at_sl_exit_cents": exit_price_cents,
                "entry_bid_cents": entry_price_cents,
                "entry_limit_cents": limit_price_cents,
                "placement_bid_cents_registry": placement_bid_cents,
                "entry_distance_at_place": entry_distance,
                "entry_distance_at_fill": entry_distance_at_fill,
                "window_id": window_id,
                "ticker": ticker,
                "market_id": market_id,
                "placed_at_ts": pf,
                "resolved_at_ts": rf,
                "sl_audit_ts": float(sl_fired_ts) if sl_fired_ts is not None else None,
                "placed_at_utc": _utc(pf),
                "resolved_at_utc": _utc(rf),
                "sl_audit_utc": _utc(float(sl_fired_ts) if sl_fired_ts is not None else None),
            }
        )
    return out


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    default_db = root / "data" / "v2_state.db"
    ap = argparse.ArgumentParser(description="Offline: last N SL rows for continuous_alpha_limit_99")
    ap.add_argument("--db", type=Path, default=default_db, help="Path to v2_state.db")
    ap.add_argument("--n", type=int, default=5, help="How many most recent SL rows")
    ap.add_argument(
        "--strategy-id",
        default="continuous_alpha_limit_99",
        help="strategy_id in v2_strategy_reports (default: continuous_alpha_limit_99)",
    )
    ap.add_argument("--json", action="store_true", help="Print JSON only")
    args = ap.parse_args()

    if not args.db.is_file():
        print(f"ERROR: DB not found: {args.db}", file=sys.stderr)
        print("  Copy v2_state.db from the server or point --db to an existing file.", file=sys.stderr)
        return 1

    rows = fetch_last_n_sl(args.db, args.strategy_id, max(1, args.n))

    if args.json:
        print(json.dumps(rows, indent=2))
        return 0

    print(f"=== Last {len(rows)} stop-loss(es) strategy_id={args.strategy_id} db={args.db} ===\n")
    if not rows:
        print("No rows (no is_stop_loss=1 reports for this strategy, or empty DB).")
        return 0

    for i, r in enumerate(rows, start=1):
        print(f"[{i}] order_id={r['order_id']}")
        print(f"    sl_reason={r['sl_reason']}")
        print(f"    time_placement_to_sl_sec={r['sec_placement_to_sl']}")
        print(f"    bid_at_sl_exit_cents={r['bid_at_sl_exit_cents']}  (market sell fill)")
        print(
            f"    entry: asset={r['asset']} side={r['side']} "
            f"entry_bid_cents={r['entry_bid_cents']} limit_cents={r['entry_limit_cents']} "
            f"dist_place={r['entry_distance_at_place']} dist_fill={r['entry_distance_at_fill']}"
        )
        print(f"    window_id={r['window_id']} ticker={r['ticker']}")
        print(
            f"    placed_at={r['placed_at_utc']}  resolved_sl={r['resolved_at_utc']}  sl_audit={r['sl_audit_utc']}"
        )
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())

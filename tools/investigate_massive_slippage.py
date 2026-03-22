#!/usr/bin/env python3
"""
Forensic: tie v2_state.db (registry, SL audit, reports, tick_log), text logs, and tick JSON
into one timeline for catastrophic slippage analysis (liquidity vacuum vs bot delay).

Default investigation targets (override with --case):
  - BTC  @ window fifteen_min_26MAR201715
  - XRP  @ window fifteen_min_26MAR200945

Tick data lives in SQLite table v2_tick_log (not standalone CSVs); optional CSV exports
exist under tools/export_v2_window_csv.py.

Usage (repo root):
  python tools/investigate_massive_slippage.py
  python tools/investigate_massive_slippage.py --db data/v2_state.db --log data/console.log
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "data" / "v2_state.db"
DEFAULT_LOG = ROOT / "data" / "console.log"

# Default forensic cases: (asset, window_id without interval prefix is WRONG — full window_id as in v2_tick_log)
DEFAULT_CASES: List[Tuple[str, str]] = [
    ("btc", "fifteen_min_26MAR201715"),
    ("xrp", "fifteen_min_26MAR200945"),
]


def _utc(ts: Optional[float]) -> str:
    if ts is None:
        return ""
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " UTC"
    except (TypeError, ValueError, OSError):
        return str(ts)


def _distance(spot: Any, strike: Any) -> Optional[float]:
    try:
        if spot is None or strike is None:
            return None
        return abs(float(spot) - float(strike))
    except (TypeError, ValueError):
        return None


def fetch_order_for_window_asset(
    conn: sqlite3.Connection, window_id: str, asset: str
) -> Optional[Dict[str, Any]]:
    """Single SL row for this window + asset (is_stop_loss=1)."""
    cur = conn.execute(
        """
        SELECT r.order_id, r.placed_at, r.limit_price_cents, r.placement_bid_cents, r.asset, r.market_id,
               r.ticker, r.strategy_id, r.status,
               rep.entry_price_cents, rep.exit_price_cents, rep.pnl_cents, rep.resolved_at,
               rep.is_stop_loss, rep.outcome
        FROM v2_order_registry r
        INNER JOIN v2_strategy_reports rep ON rep.order_id = r.order_id
        WHERE rep.window_id = ? AND LOWER(r.asset) = LOWER(?)
          AND rep.is_stop_loss = 1
        ORDER BY rep.resolved_at DESC
        LIMIT 1
        """,
        (window_id, asset),
    )
    row = cur.fetchone()
    if not row:
        return None
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


def fetch_sl_audit(conn: sqlite3.Connection, order_id: str) -> List[Dict[str, Any]]:
    try:
        cur = conn.execute(
            """
            SELECT id, ts, order_id, exit_action, ticker, side, sell_count, attempt, success,
                   http_status, error_message, substr(response_body, 1, 200) AS response_snip
            FROM v2_sl_execution_audit
            WHERE order_id = ?
            ORDER BY id ASC
            """,
            (order_id,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    except sqlite3.OperationalError:
        return []


def load_tick_row(conn: sqlite3.Connection, window_id: str, asset: str) -> Optional[Tuple[Dict[str, List[Any]], float]]:
    """Returns (parsed_json_dict, created_at) or None."""
    cur = conn.execute(
        """
        SELECT tick_history_json, created_at
        FROM v2_tick_log
        WHERE window_id = ? AND LOWER(asset) = LOWER(?)
        """,
        (window_id, asset),
    )
    row = cur.fetchone()
    if not row:
        return None
    raw, created_at = row[0], float(row[1]) if row[1] is not None else 0.0
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict):
        return None
    return (data, created_at)


def slice_ticks_before_sl(
    data: Dict[str, List[Any]],
    resolved_at: float,
    tick_row_created_at: float,
    seconds_before: int = 30,
) -> List[Dict[str, Any]]:
    """
    Approximate ticks in the ~`seconds_before` wall-clock seconds before SL.

    v2_tick_log stores columnar sec=yes_bid=no_bid=strike=spot without per-tick wall time.
    We estimate seconds_to_close at SL as (tick_row_created_at - resolved_at), assuming
    flush happens at/near window close and SL resolved shortly before. Then keep ticks
    where sec_to_close is in [sec_at_sl, sec_at_sl + seconds_before] (countdown: higher sec
    = earlier wall time before SL).
    """
    sec = data.get("sec") or []
    yes = data.get("yes_bid") or []
    no_b = data.get("no_bid") or []
    strike = data.get("strike") or []
    spot = data.get("spot") or []
    n = min(len(sec), len(yes), len(no_b), len(strike), len(spot))
    if n == 0:
        return []

    sec_at_sl = tick_row_created_at - float(resolved_at)
    # Sanity: must be non-negative and plausible (< 20 min for 15m window + skew)
    if sec_at_sl < 0 or sec_at_sl > 1200:
        sec_at_sl = max(0.0, min(float(s) for s in sec[:n] if isinstance(s, (int, float))) or 0.0)

    lo = float(sec_at_sl)
    hi = float(sec_at_sl) + float(seconds_before)

    rows: List[Dict[str, Any]] = []
    for i in range(n):
        s = sec[i]
        try:
            sf = float(s)
        except (TypeError, ValueError):
            continue
        if lo <= sf <= hi:
            rows.append(
                {
                    "idx": i,
                    "sec_to_close": sf,
                    "spot": spot[i] if i < len(spot) else None,
                    "strike": strike[i] if i < len(strike) else None,
                    "yes_bid": yes[i] if i < len(yes) else None,
                    "no_bid": no_b[i] if i < len(no_b) else None,
                    "distance": _distance(spot[i] if i < len(spot) else None, strike[i] if i < len(strike) else None),
                }
            )

    if not rows:
        # Fallback: last N samples (≈ last N seconds at 1 Hz pipeline)
        tail = min(seconds_before + 5, n)
        for i in range(n - tail, n):
            try:
                sf = float(sec[i])
            except (TypeError, ValueError):
                continue
            rows.append(
                {
                    "idx": i,
                    "sec_to_close": sf,
                    "spot": spot[i] if i < len(spot) else None,
                    "strike": strike[i] if i < len(strike) else None,
                    "yes_bid": yes[i] if i < len(yes) else None,
                    "no_bid": no_b[i] if i < len(no_b) else None,
                    "distance": _distance(spot[i] if i < len(spot) else None, strike[i] if i < len(strike) else None),
                }
            )
    return rows


def scan_log_for_order_ids(log_path: Path, order_ids: Sequence[str]) -> List[str]:
    if not log_path.is_file():
        return []
    hits: List[str] = []
    needles = tuple(oid for oid in order_ids if oid)
    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                if not any(n in line for n in needles):
                    continue
                if "INFO" not in line and "EXECUTION" not in line and "last_90s" not in line:
                    continue
                hits.append(line.rstrip("\n"))
    except OSError as e:
        return [f"<read error: {e}>"]
    return hits


def print_section(title: str) -> None:
    print()
    print("=" * 80)
    print(title)
    print("=" * 80)


def run_case(conn: sqlite3.Connection, asset: str, window_id: str, log_path: Path, tail_seconds: int) -> Optional[str]:
    print_section(f"CASE: asset={asset.upper()} window_id={window_id}")

    row = fetch_order_for_window_asset(conn, window_id, asset)
    if not row:
        print(f"No v2_order_registry + v2_strategy_reports (is_stop_loss=1) row for window={window_id} asset={asset}")
        return None

    oid = row["order_id"]
    print("\n--- 1) Database (registry + report) ---")
    print(f"  order_id:           {oid}")
    print(f"  strategy_id:        {row.get('strategy_id')}")
    print(f"  placed_at:          {_utc(row.get('placed_at'))}")
    print(f"  limit_price_cents:  {row.get('limit_price_cents')}")
    print(f"  placement_bid_cents:{row.get('placement_bid_cents')}")
    print(f"  entry_price_cents:  {row.get('entry_price_cents')} (report)")
    print(f"  exit_price_cents:   {row.get('exit_price_cents')} (market sell fill)")
    print(f"  pnl_cents:          {row.get('pnl_cents')}")
    print(f"  resolved_at (SL):   {_utc(row.get('resolved_at'))}")
    print(f"  market_id:          {row.get('market_id')}")
    print(f"  ticker:             {row.get('ticker')}")

    audits = fetch_sl_audit(conn, oid)
    print("\n--- v2_sl_execution_audit ---")
    if not audits:
        print("  (no rows — table missing or no audit for this order_id)")
        sl_reason = None
    else:
        for a in audits:
            print(
                f"  id={a.get('id')} ts={_utc(a.get('ts'))} exit_action={a.get('exit_action')} "
                f"success={a.get('success')} attempt={a.get('attempt')}"
            )
        sl_reason = next((a.get("exit_action") for a in reversed(audits) if a.get("success")), None)
    print(f"  inferred sl_reason (last successful audit exit_action): {sl_reason or 'unknown'}")

    resolved_at = row.get("resolved_at")
    if resolved_at is None:
        print("\n--- 3) Tick log: skip (no resolved_at) ---")
        return oid

    tick_bundle = load_tick_row(conn, window_id, asset)
    print("\n--- 3) v2_tick_log (tick_history_json) ---")
    if not tick_bundle:
        print(f"  No row for window_id={window_id} asset={asset}")
        return oid

    data, created_at = tick_bundle
    sec_list = data.get("sec") or []
    print(f"  tick row created_at (flush): {_utc(created_at)}")
    print(f"  total samples in JSON:     {len(sec_list)}")
    sec_at_sl_est = created_at - float(resolved_at)
    print(f"  est. sec_to_close @ SL:    {sec_at_sl_est:.3f}  (= flush_time - resolved_at; heuristic)")

    slice_rows = slice_ticks_before_sl(data, float(resolved_at), created_at, seconds_before=tail_seconds)
    print(f"\n  Table: ticks with sec_to_close in [est@SL, est@SL+{tail_seconds}s] (or tail fallback)")
    print(f"  {'idx':>5} {'sec_to_close':>12} {'spot':>14} {'distance':>14} {'yes_bid':>8} {'no_bid':>8}")
    print("  " + "-" * 70)
    for r in slice_rows:
        sp = r.get("spot")
        dist = r.get("distance")
        print(
            f"  {r['idx']:5d} {r['sec_to_close']:12.3f} {str(sp) if sp is not None else '':>14} "
            f"{(f'{dist:.6g}' if dist is not None else ''):>14} {str(r.get('yes_bid')):>8} {str(r.get('no_bid')):>8}"
        )

    return oid


def main() -> int:
    ap = argparse.ArgumentParser(description="Forensic SL + tick log for selected windows")
    ap.add_argument("--db", type=Path, default=DEFAULT_DB)
    ap.add_argument("--log", type=Path, default=DEFAULT_LOG, help="Primary app log (V2: data/console.log)")
    ap.add_argument(
        "--extra-logs",
        type=Path,
        nargs="*",
        default=[],
        help="Additional log files to scan",
    )
    ap.add_argument("--tail-seconds", type=int, default=30, help="Tick window width in sec_to_close space")
    ap.add_argument(
        "--case",
        action="append",
        default=[],
        metavar="ASSET:WINDOW_ID",
        help="e.g. btc:fifteen_min_26MAR201715 (repeatable); default uses built-in BTC+XRP cases",
    )
    args = ap.parse_args()

    if not args.db.is_file():
        print(f"ERROR: DB not found: {args.db}", file=sys.stderr)
        return 1

    cases: List[Tuple[str, str]] = []
    if args.case:
        for c in args.case:
            if ":" not in c:
                print(f"ERROR: --case must be ASSET:WINDOW_ID, got {c!r}", file=sys.stderr)
                return 1
            a, w = c.split(":", 1)
            cases.append((a.strip().lower(), w.strip()))
    else:
        cases = list(DEFAULT_CASES)

    conn = sqlite3.connect(str(args.db))
    try:
        order_ids: List[str] = []
        for asset, window_id in cases:
            oid = run_case(conn, asset, window_id, args.log, args.tail_seconds)
            if oid:
                order_ids.append(oid)
    finally:
        conn.close()

    log_paths = [args.log] + list(args.extra_logs)
    print_section("2) Log file lines (INFO / EXECUTION / last_90s) mentioning order_id(s)")
    for lp in log_paths:
        print(f"\n--- file: {lp} ---")
        hits = scan_log_for_order_ids(lp, order_ids)
        if not hits:
            print("  (no matches or file missing)")
        else:
            for h in hits[:500]:
                print(h)
            if len(hits) > 500:
                print(f"  ... truncated ({len(hits)} total lines)")

    print_section("Notes")
    print("""
  - sl_reason comes from v2_sl_execution_audit.exit_action when present; else unknown.
  - sec_to_close @ SL is estimated as (v2_tick_log.created_at - resolved_at). If the flush
    is delayed, bias the slice; use the printed tail table and total sample count to sanity-check.
  - distance = |spot - strike| from tick JSON (same oracle/strike as live pipeline).
  - For raw SQL / CSV export of tick rows: tools/export_v2_window_csv.py --window 26MAR201715
""")
    return 0


if __name__ == "__main__":
    sys.exit(main())

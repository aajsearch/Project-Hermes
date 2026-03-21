#!/usr/bin/env python3
"""
Backtest: would danger_persistence_seconds have stopped out historical *winning* trades?

**Cohort `reported_win` (default):** rows in v2_strategy_reports with outcome='win' and is_stop_loss=0.
Note: the executor only calls record_trade_outcome on SL/TP *market* exits, so many DBs have *zero*
reported wins (settlements / held-to-expiry do not write a "win" row). If you see 0 rows, use
`--cohort filled_no_stop_loss`.

**Cohort `filled_no_stop_loss`:** filled registry orders (count=1) with no is_stop_loss=1 report —
proxy for positions that were never stop-lossed (typical "survivors" in the same DB).

Wall time per tick: (tick_log.created_at - sec_to_close), see tools/investigate_massive_slippage.py.

Usage (repo root):
  python tools/backtest_persistence_wins.py
  python tools/backtest_persistence_wins.py --cohort filled_no_stop_loss --decay-percent-for-threshold 80
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "data" / "v2_state.db"
DEFAULT_STRATEGY = "continuous_alpha_limit_99"
SECONDS_PER_HOUR = 3600.0

# Ensure `python tools/backtest_...py` can import bot.*
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bot.pipeline.window_utils import logical_window_slot  # noqa: E402


def _distance(spot: Any, strike: Any) -> Optional[float]:
    try:
        if spot is None or strike is None:
            return None
        return abs(float(spot) - float(strike))
    except (TypeError, ValueError):
        return None


def fetch_winning_trades(
    conn: sqlite3.Connection,
    *,
    strategy_id: str,
    max_hold_seconds: float,
) -> List[Dict[str, Any]]:
    """
    Wins: joined registry + reports, filled, explicit no SL, single contract, placement bid present,
    hold duration <= max_hold_seconds.
    """
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        """
        SELECT
            r.order_id,
            r.placed_at,
            r.market_id,
            r.asset,
            r.side,
            r.count,
            r.filled_count,
            r.placement_bid_cents,
            r.entry_distance,
            r.entry_distance_at_fill,
            rep.window_id,
            rep.resolved_at,
            rep.outcome,
            rep.entry_price_cents,
            rep.pnl_cents
        FROM v2_order_registry r
        INNER JOIN v2_strategy_reports rep ON rep.order_id = r.order_id
        WHERE r.strategy_id = ?
          AND rep.is_stop_loss = 0
          AND rep.outcome = 'win'
          AND r.count = 1
          AND COALESCE(r.filled_count, 0) >= 1
          AND r.placement_bid_cents IS NOT NULL
          AND rep.resolved_at IS NOT NULL
          AND r.placed_at IS NOT NULL
          AND (rep.resolved_at - r.placed_at) <= ?
        ORDER BY rep.resolved_at ASC
        """,
        (strategy_id, max_hold_seconds),
    )
    return [dict(row) for row in cur.fetchall()]


def fetch_filled_no_stop_loss_trades(
    conn: sqlite3.Connection,
    *,
    strategy_id: str,
) -> List[Dict[str, Any]]:
    """
    Filled single-contract orders with placement bid, never logged as stop-loss in v2_strategy_reports.
    window_id / resolved_at are filled in run_analysis from v2_tick_log (flush time ≈ window end).
    """
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        """
        SELECT
            r.order_id,
            r.placed_at,
            r.market_id,
            r.asset,
            r.side,
            r.count,
            r.filled_count,
            r.placement_bid_cents,
            r.entry_distance,
            r.entry_distance_at_fill,
            r.interval
        FROM v2_order_registry r
        WHERE r.strategy_id = ?
          AND r.count = 1
          AND COALESCE(r.filled_count, 0) >= 1
          AND r.placement_bid_cents IS NOT NULL
          AND r.placed_at IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM v2_strategy_reports s
              WHERE s.order_id = r.order_id AND s.is_stop_loss = 1
          )
        ORDER BY r.placed_at ASC
        """,
        (strategy_id,),
    )
    rows: List[Dict[str, Any]] = []
    for row in cur.fetchall():
        d = dict(row)
        interval = (d.get("interval") or "").strip()
        market_id = d.get("market_id") or ""
        slot = logical_window_slot(market_id)
        d["window_id"] = f"{interval}_{slot}" if interval else slot
        rows.append(d)
    return rows


def db_diagnostic_counts(conn: sqlite3.Connection, strategy_id: str) -> Dict[str, int]:
    """Lightweight counts for user-facing hints when cohort is empty."""
    conn.row_factory = None
    out: Dict[str, int] = {}
    try:
        row = conn.execute(
            """
            SELECT COUNT(*) FROM v2_strategy_reports
            WHERE strategy_id = ? AND outcome = 'win' AND is_stop_loss = 0
            """,
            (strategy_id,),
        ).fetchone()
        out["reported_wins"] = int(row[0]) if row else 0
    except sqlite3.Error:
        out["reported_wins"] = -1
    try:
        row = conn.execute(
            """
            SELECT COUNT(*) FROM v2_strategy_reports
            WHERE strategy_id = ? AND is_stop_loss = 1
            """,
            (strategy_id,),
        ).fetchone()
        out["reported_stop_losses"] = int(row[0]) if row else 0
    except sqlite3.Error:
        out["reported_stop_losses"] = -1
    return out


def load_tick_history(
    conn: sqlite3.Connection, window_id: str, asset: str
) -> Optional[Tuple[Dict[str, List[Any]], float]]:
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
    raw, created_at = row[0], float(row[1] or 0.0)
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict):
        return None
    return (data, created_at)


def ticks_with_wall_time(
    data: Dict[str, List[Any]],
    tick_created_at: float,
) -> List[Dict[str, Any]]:
    """Each tick: wall_ts, sec_to_close, distance (or None if missing spot/strike)."""
    sec = data.get("sec") or []
    strike = data.get("strike") or []
    spot = data.get("spot") or []
    n = min(len(sec), len(strike), len(spot))
    out: List[Dict[str, Any]] = []
    for i in range(n):
        try:
            s = float(sec[i])
        except (TypeError, ValueError):
            continue
        wall_ts = tick_created_at - s
        d = _distance(spot[i] if i < len(spot) else None, strike[i] if i < len(strike) else None)
        out.append({"wall_ts": wall_ts, "sec_to_close": s, "distance": d})
    out.sort(key=lambda r: r["wall_ts"])
    return out


@dataclass
class GhostResult:
    order_id: str
    ghost: bool
    sec_to_close_at_fire: Optional[float] = None
    wall_ts_at_fire: Optional[float] = None
    reason_skip: Optional[str] = None


def simulate_persistence(
    ticks: List[Dict[str, Any]],
    *,
    placed_at: float,
    resolved_at: float,
    reference_distance: float,
    threshold_mult: float,
    persistence_seconds: float,
) -> Tuple[bool, Optional[float], Optional[float]]:
    """
    Danger when distance is not None and distance <= reference_distance * threshold_mult.
    Ghost SL if danger holds continuously for >= persistence_seconds (wall clock between ticks).
    Returns (ghost, sec_to_close_at_fire, wall_ts_at_fire).
    """
    if reference_distance is None or reference_distance <= 0:
        return False, None, None
    threshold = abs(reference_distance) * threshold_mult

    filtered = [t for t in ticks if placed_at <= t["wall_ts"] <= resolved_at]
    if not filtered:
        return False, None, None

    danger_start: Optional[float] = None
    for t in filtered:
        dist = t["distance"]
        wall = t["wall_ts"]
        in_danger = dist is not None and dist <= threshold
        if not in_danger:
            danger_start = None
            continue
        if danger_start is None:
            danger_start = wall
        if wall - danger_start >= persistence_seconds:
            return True, float(t["sec_to_close"]), float(wall)
    return False, None, None


def run_analysis(
    conn: sqlite3.Connection,
    *,
    strategy_id: str,
    cohort: str,
    max_hold_hours: float,
    persistence_seconds: float,
    threshold_mult: float,
) -> Tuple[int, List[GhostResult], List[GhostResult]]:
    """
    threshold_mult: production-style distance_decay — danger when distance <= ref * threshold_mult
      (production uses per-asset distance_decay_pct in v2_fifteen_min.yaml). For entry*(1-0.80) use mult=0.20.
    Returns (sql_win_count, analyzed_results, skipped_results).
    """
    max_hold_seconds = max_hold_hours * SECONDS_PER_HOUR
    if cohort == "reported_win":
        trades = fetch_winning_trades(conn, strategy_id=strategy_id, max_hold_seconds=max_hold_seconds)
    elif cohort == "filled_no_stop_loss":
        trades = fetch_filled_no_stop_loss_trades(conn, strategy_id=strategy_id)
    else:
        raise ValueError(f"Unknown cohort: {cohort}")
    sql_win_count = len(trades)

    analyzed: List[GhostResult] = []
    skipped: List[GhostResult] = []

    for row in trades:
        oid = row["order_id"]
        window_id = row["window_id"]
        asset = row["asset"]
        placed_at = float(row["placed_at"])

        ref = row.get("entry_distance_at_fill")
        if ref is None:
            ref = row.get("entry_distance")
        try:
            reference_distance = float(ref) if ref is not None else 0.0
        except (TypeError, ValueError):
            reference_distance = 0.0

        if reference_distance <= 0:
            skipped.append(GhostResult(order_id=oid, ghost=False, reason_skip="no_entry_distance"))
            continue

        loaded = load_tick_history(conn, window_id, asset or "")
        if not loaded:
            skipped.append(GhostResult(order_id=oid, ghost=False, reason_skip="no_tick_log"))
            continue

        data, tick_created_at = loaded
        tick_created_at = float(tick_created_at)
        if cohort == "filled_no_stop_loss":
            resolved_at = tick_created_at
            if resolved_at - placed_at > max_hold_seconds:
                skipped.append(GhostResult(order_id=oid, ghost=False, reason_skip="hold_too_long"))
                continue
        else:
            resolved_at = float(row["resolved_at"])

        ticks = ticks_with_wall_time(data, tick_created_at)
        if not ticks:
            skipped.append(GhostResult(order_id=oid, ghost=False, reason_skip="empty_tick_arrays"))
            continue

        ghost, sec_fire, wall_fire = simulate_persistence(
            ticks,
            placed_at=placed_at,
            resolved_at=resolved_at,
            reference_distance=reference_distance,
            threshold_mult=threshold_mult,
            persistence_seconds=persistence_seconds,
        )
        if ghost:
            analyzed.append(
                GhostResult(
                    order_id=oid,
                    ghost=True,
                    sec_to_close_at_fire=sec_fire,
                    wall_ts_at_fire=wall_fire,
                )
            )
        else:
            analyzed.append(GhostResult(order_id=oid, ghost=False))

    return sql_win_count, analyzed, skipped


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Backtest danger persistence on historical winning trades (continuous_alpha_limit_99)."
    )
    ap.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to v2_state.db")
    ap.add_argument("--strategy-id", type=str, default=DEFAULT_STRATEGY, help="Registry strategy_id filter")
    ap.add_argument(
        "--cohort",
        type=str,
        choices=("reported_win", "filled_no_stop_loss"),
        default="reported_win",
        help=(
            "reported_win: v2_strategy_reports outcome=win, is_stop_loss=0 (often empty — SL/TP-only logging). "
            "filled_no_stop_loss: filled count=1 orders with no is_stop_loss=1 report (survivor proxy)."
        ),
    )
    ap.add_argument(
        "--max-hold-hours",
        type=float,
        default=30.0,
        help="Include trades only if (resolved_at - placed_at) <= this many hours (default: 30)",
    )
    ap.add_argument(
        "--persistence-seconds",
        type=float,
        default=1.5,
        help="Consecutive seconds in danger zone to count as Ghost SL (default: 1.5)",
    )
    ap.add_argument(
        "--threshold-mult",
        type=float,
        default=None,
        help=(
            "Danger when spot distance <= entry_ref * this multiplier. "
            "Default: 0.75 (strategy fallback if unset; YAML may use per-asset dict). "
            "For entry_distance * (1 - 0.80) pass 0.20."
        ),
    )
    ap.add_argument(
        "--decay-percent-for-threshold",
        type=float,
        default=None,
        help=(
            "If set, threshold_mult = 1 - (this/100), e.g. 80 → mult 0.20 (entry * (1-0.80)). "
            "Overrides --threshold-mult when both would apply; use one or the other."
        ),
    )
    args = ap.parse_args()
    if args.decay_percent_for_threshold is not None:
        threshold_mult = 1.0 - (float(args.decay_percent_for_threshold) / 100.0)
    elif args.threshold_mult is not None:
        threshold_mult = float(args.threshold_mult)
    else:
        threshold_mult = 0.75  # matches last_90s _DEFAULT_DISTANCE_DECAY_PCT when YAML omits key

    db_path = args.db
    if not db_path.is_file():
        print(f"ERROR: DB not found: {db_path}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    try:
        sql_wins, analyzed, skipped = run_analysis(
            conn,
            strategy_id=args.strategy_id.strip(),
            cohort=args.cohort,
            max_hold_hours=args.max_hold_hours,
            persistence_seconds=args.persistence_seconds,
            threshold_mult=threshold_mult,
        )
        diag = db_diagnostic_counts(conn, args.strategy_id.strip())
    finally:
        conn.close()

    total = len(analyzed)
    ghosts = [r for r in analyzed if r.ghost]
    y = len(ghosts)
    z = (100.0 * (total - y) / total) if total else 0.0

    print()
    print("=== Backtest: persistence vs historical wins ===")
    print(f"DB:                  {db_path}")
    print(f"Strategy:            {args.strategy_id}")
    print(f"Cohort:              {args.cohort}")
    print(f"Max hold filter:     <= {args.max_hold_hours} h (resolved - placed)")
    print(f"Persistence window:  {args.persistence_seconds} s consecutive in danger zone")
    print(f"Distance threshold:  distance <= entry_ref * {round(threshold_mult, 6)}")
    print()
    if sql_wins == 0 and args.cohort == "reported_win" and diag.get("reported_wins", 0) == 0:
        print(
            "NOTE: v2_strategy_reports has no outcome='win' rows for this strategy (typical: only SL/TP exits "
            "are logged). Re-run with:\n"
            "  --cohort filled_no_stop_loss\n"
            f"      (DB snapshot: reported wins={diag.get('reported_wins', '?')}, "
            f"rows with is_stop_loss=1={diag.get('reported_stop_losses', '?')})\n"
        )
    print(f"Trades matching cohort SQL filter:                                      {sql_wins}")
    print(f"Analyzed (tick data + entry distance):                                  {total}")
    print(f"Skipped (missing distance, no tick log, hold too long, etc.):             {len(skipped)}")
    print()
    print(f"Ghost Stop-Losses (would have fired):                         {y}")
    print(f"Survival rate:                                                {z:.1f}%")
    print()

    if ghosts:
        print("--- Ghost Stop-Loss detail (manual review) ---")
        for g in ghosts:
            print(
                f"  order_id={g.order_id}  sec_to_close_at_fire={g.sec_to_close_at_fire}  "
                f"wall_ts_at_fire={g.wall_ts_at_fire}"
            )
        print()

    if skipped:
        reasons: Dict[str, int] = {}
        for s in skipped:
            reasons[s.reason_skip or "unknown"] = reasons.get(s.reason_skip or "unknown", 0) + 1
        print("--- Skipped trades (reason counts) ---")
        for k, v in sorted(reasons.items(), key=lambda x: -x[1]):
            print(f"  {k}: {v}")
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

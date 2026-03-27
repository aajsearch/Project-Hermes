#!/usr/bin/env python3
"""
Sweep expired 0DTE (or past-expiry) Alpaca option spreads that never received close_reason.

Uses underlying *daily* close on expiration date from Alpaca stock bars to classify:
  - expired_worthless  (short legs expired OTM)
  - expired_itm_assigned (intrinsic settlement; loss capped by spread width on each wing)

Does NOT assume all expiries are wins.

Usage:
  python3 scripts/sweep_expired_trades.py [--dry-run] [--db PATH]
  # Requires ALPACA_API_KEY / ALPACA_SECRET_KEY (same as the options bot)

Timezone rule:
  - Eligible if calendar expiry date < today (America/Los_Angeles), OR
  - expiry date == today and local time >= 13:00 America/Los_Angeles (1 PM PST/PDT).
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, time as dt_time
from pathlib import Path
from typing import Any, List, Optional, Sequence, Tuple

# -----------------------------------------------------------------------------
# Repo imports
# -----------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from alpaca.data.historical import StockHistoricalDataClient  # noqa: E402
from alpaca.data.requests import StockBarsRequest  # noqa: E402
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit  # noqa: E402
import pandas as pd  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

from bot.alpaca_put_spread.option_symbol import parse_occ_option_symbol  # noqa: E402

SPREADS = "alpaca_spreads"
ORDERS = "alpaca_orders"

TZ_LA = "America/Los_Angeles"
DEFAULT_FEED = "iex"


@dataclass
class SettlementResult:
    close_reason: str
    pnl_dollars: float
    close_debit_mid: float
    underlying_close: float
    note: str


def _project_db() -> Path:
    return ROOT / "data" / "alpaca_put_spread.db"


def _now_la() -> datetime:
    from zoneinfo import ZoneInfo

    return datetime.now(ZoneInfo(TZ_LA))


def _parse_legs_json(raw: Any) -> List[str]:
    if not raw:
        return []
    try:
        data = json.loads(str(raw))
        if isinstance(data, list):
            return [str(x) for x in data if x]
    except (json.JSONDecodeError, TypeError):
        pass
    return []


def _symbols_from_raw_snapshot(raw: Any) -> dict[str, str]:
    out: dict[str, str] = {}
    if not raw:
        return out
    try:
        d = json.loads(str(raw))
        if isinstance(d, dict):
            for k in (
                "short_put_symbol",
                "long_put_symbol",
                "short_call_symbol",
                "long_call_symbol",
            ):
                v = d.get(k)
                if v:
                    out[k] = str(v)
    except (json.JSONDecodeError, TypeError):
        pass
    return out


def _expiry_date_from_symbols(symbols: Sequence[str]) -> Optional[date]:
    for sym in symbols:
        p = parse_occ_option_symbol(sym)
        if p:
            return p.expiry_date().date()
    return None


def _eligible_for_sweep(expiry_d: date, now_la: datetime) -> bool:
    d_today = now_la.date()
    if expiry_d < d_today:
        return True
    if expiry_d == d_today:
        return now_la.timetz() >= dt_time(13, 0, tzinfo=now_la.tzinfo)
    return False


def _fetch_daily_close(
    client: StockHistoricalDataClient,
    symbol: str,
    as_of: date,
    *,
    feed: str,
) -> Optional[float]:
    """
    Last regular-session daily close on or before *as_of* (US equity calendar).
    """
    start = datetime.combine(as_of - timedelta(days=14), dt_time.min)
    end = datetime.combine(as_of + timedelta(days=5), dt_time.max)

    req = StockBarsRequest(
        symbol_or_symbols=[symbol],
        timeframe=TimeFrame(1, TimeFrameUnit.Day),
        start=start,
        end=end,
        limit=100,
        feed=feed,
    )
    try:
        resp = client.get_stock_bars(req)
        df = resp.df
    except Exception as e:
        print(f"  [warn] Alpaca bars failed for {symbol}: {e}", file=sys.stderr)
        return None

    if df is None or df.empty:
        return None

    if hasattr(df.index, "names") and len(getattr(df.index, "names", [])) == 2:
        df = df.reset_index()
        if "symbol" in df.columns:
            df = df[df["symbol"] == symbol].copy()

    if "timestamp" not in df.columns:
        return None

    df = df.sort_values("timestamp")
    ts = pd.to_datetime(df["timestamp"], utc=True)
    bar_dates = ts.dt.date
    sub = df[bar_dates <= as_of]
    if sub.empty:
        return None
    return float(sub.iloc[-1]["close"])


def _intrinsic_put_spread_debit(short_strike: float, long_strike: float, spot: float) -> float:
    """Bull put credit: short higher Ks, long lower Kl. Debit to settle = max(0,Ks-S)-max(0,Kl-S) per share."""
    return max(0.0, short_strike - spot) - max(0.0, long_strike - spot)


def _intrinsic_call_spread_debit(short_strike: float, long_strike: float, spot: float) -> float:
    """Bear call credit: short lower Ks, long higher Kl."""
    return max(0.0, spot - short_strike) - max(0.0, spot - long_strike)


def _strike_pair_from_symbols(short_sym: str, long_sym: str) -> Optional[Tuple[float, float]]:
    ps = parse_occ_option_symbol(short_sym)
    pl = parse_occ_option_symbol(long_sym)
    if not ps or not pl:
        return None
    return (float(ps.strike), float(pl.strike))


def settle_pcs(
    *,
    short_put_sym: str,
    long_put_sym: str,
    entry_credit: float,
    qty: int,
    spot: float,
) -> SettlementResult:
    pair = _strike_pair_from_symbols(short_put_sym, long_put_sym)
    if not pair:
        raise ValueError("cannot parse PCS leg symbols")
    ks, kl = pair
    intrinsic = _intrinsic_put_spread_debit(ks, kl, spot)
    pnl_per_contract = entry_credit - intrinsic
    pnl = pnl_per_contract * 100.0 * float(qty)
    close_debit = entry_credit - pnl / (100.0 * float(qty))
    worthless = intrinsic <= 1e-9
    return SettlementResult(
        close_reason="expired_worthless" if worthless else "expired_itm_assigned",
        pnl_dollars=pnl,
        close_debit_mid=max(0.0, close_debit),
        underlying_close=spot,
        note=f"PCS Ks={ks} Kl={kl} intrinsic={intrinsic:.4f}",
    )


def settle_ccs(
    *,
    short_call_sym: str,
    long_call_sym: str,
    entry_credit: float,
    qty: int,
    spot: float,
) -> SettlementResult:
    pair = _strike_pair_from_symbols(short_call_sym, long_call_sym)
    if not pair:
        raise ValueError("cannot parse CCS leg symbols")
    ks, kl = pair
    intrinsic = _intrinsic_call_spread_debit(ks, kl, spot)
    pnl_per_contract = entry_credit - intrinsic
    pnl = pnl_per_contract * 100.0 * float(qty)
    close_debit = entry_credit - pnl / (100.0 * float(qty))
    worthless = intrinsic <= 1e-9
    return SettlementResult(
        close_reason="expired_worthless" if worthless else "expired_itm_assigned",
        pnl_dollars=pnl,
        close_debit_mid=max(0.0, close_debit),
        underlying_close=spot,
        note=f"CCS Ks={ks} Kl={kl} intrinsic={intrinsic:.4f}",
    )


def settle_ic(
    *,
    long_put: str,
    short_put: str,
    short_call: str,
    long_call: str,
    entry_credit: float,
    qty: int,
    spot: float,
) -> SettlementResult:
    pp = _strike_pair_from_symbols(short_put, long_put)
    pc = _strike_pair_from_symbols(short_call, long_call)
    if not pp or not pc:
        raise ValueError("cannot parse IC leg symbols")
    ks_p, kl_p = pp
    ks_c, kl_c = pc
    intrinsic = _intrinsic_put_spread_debit(ks_p, kl_p, spot) + _intrinsic_call_spread_debit(
        ks_c, kl_c, spot
    )
    pnl_per_contract = entry_credit - intrinsic
    pnl = pnl_per_contract * 100.0 * float(qty)
    close_debit = entry_credit - pnl / (100.0 * float(qty))
    worthless = intrinsic <= 1e-9
    return SettlementResult(
        close_reason="expired_worthless" if worthless else "expired_itm_assigned",
        pnl_dollars=pnl,
        close_debit_mid=max(0.0, close_debit),
        underlying_close=spot,
        note=f"IC put_intr+call_intr={intrinsic:.4f}",
    )


def _resolve_leg_symbols(
    strategy_type: str,
    legs: List[str],
    snap: dict[str, str],
) -> dict[str, str]:
    st = (strategy_type or "PCS").strip().upper()
    out: dict[str, str] = {}
    if st == "PCS":
        out["short_put"] = snap.get("short_put_symbol") or (legs[0] if len(legs) > 0 else "")
        out["long_put"] = snap.get("long_put_symbol") or (legs[1] if len(legs) > 1 else "")
    elif st == "CCS":
        out["short_call"] = snap.get("short_call_symbol") or (legs[0] if len(legs) > 0 else "")
        out["long_call"] = snap.get("long_call_symbol") or (legs[1] if len(legs) > 1 else "")
    elif st == "IC":
        out["long_put"] = snap.get("long_put_symbol") or (legs[0] if len(legs) > 0 else "")
        out["short_put"] = snap.get("short_put_symbol") or (legs[1] if len(legs) > 1 else "")
        out["short_call"] = snap.get("short_call_symbol") or (legs[2] if len(legs) > 2 else "")
        out["long_call"] = snap.get("long_call_symbol") or (legs[3] if len(legs) > 3 else "")
    return out


def run_sweep(
    db_path: Path,
    *,
    dry_run: bool,
    feed: str,
) -> int:
    load_dotenv()
    key = os.getenv("ALPACA_API_KEY")
    sec = os.getenv("ALPACA_SECRET_KEY")
    if not key or not sec:
        print("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY", file=sys.stderr)
        return 1

    if not db_path.exists():
        print(f"Database not found: {db_path}", file=sys.stderr)
        return 1

    stock = StockHistoricalDataClient(key, sec)
    now_la = _now_la()

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        f"""
        SELECT s.spread_id, s.underlying, s.strategy_type, s.legs, s.raw_snapshot_json,
               s.entry_credit_mid, s.entry_order_id,
               o.qty AS entry_qty
        FROM {SPREADS} s
        LEFT JOIN {ORDERS} o ON o.order_id = s.entry_order_id
        WHERE (s.close_reason IS NULL OR TRIM(s.close_reason) = '')
          AND s.entry_credit_mid IS NOT NULL
          AND s.entry_credit_mid > 0
        ORDER BY s.spread_id ASC
        """
    ).fetchall()

    wins: List[str] = []
    losses: List[str] = []
    skipped: List[str] = []
    updates: List[Tuple[Any, ...]] = []

    for r in rows:
        sid = int(r["spread_id"])
        und = str(r["underlying"] or "").strip().upper()
        st = str(r["strategy_type"] or "PCS").strip().upper()
        entry_credit = float(r["entry_credit_mid"])
        try:
            qty = int(r["entry_qty"]) if r["entry_qty"] is not None else 1
        except (TypeError, ValueError):
            qty = 1
        qty = max(1, qty)

        legs = _parse_legs_json(r["legs"])
        snap = _symbols_from_raw_snapshot(r["raw_snapshot_json"])
        exp_d = _expiry_date_from_symbols(legs + list(snap.values()))
        if exp_d is None:
            skipped.append(f"spread_id={sid} {und} {st}: cannot parse expiry from legs/snapshot")
            continue

        if not _eligible_for_sweep(exp_d, now_la):
            skipped.append(
                f"spread_id={sid} {und} {st}: expiry={exp_d} not yet sweepable (need date pass or 1PM LA on 0DTE)"
            )
            continue

        close_px = _fetch_daily_close(stock, und, exp_d, feed=feed)
        if close_px is None:
            skipped.append(f"spread_id={sid} {und}: no daily close from Alpaca for {exp_d}")
            continue

        legmap = _resolve_leg_symbols(st, legs, snap)
        try:
            if st == "PCS":
                if not legmap.get("short_put") or not legmap.get("long_put"):
                    raise ValueError("missing put legs")
                res = settle_pcs(
                    short_put_sym=legmap["short_put"],
                    long_put_sym=legmap["long_put"],
                    entry_credit=entry_credit,
                    qty=qty,
                    spot=close_px,
                )
            elif st == "CCS":
                if not legmap.get("short_call") or not legmap.get("long_call"):
                    raise ValueError("missing call legs")
                res = settle_ccs(
                    short_call_sym=legmap["short_call"],
                    long_call_sym=legmap["long_call"],
                    entry_credit=entry_credit,
                    qty=qty,
                    spot=close_px,
                )
            elif st == "IC":
                if not all(legmap.get(k) for k in ("long_put", "short_put", "short_call", "long_call")):
                    raise ValueError("missing IC legs")
                res = settle_ic(
                    long_put=legmap["long_put"],
                    short_put=legmap["short_put"],
                    short_call=legmap["short_call"],
                    long_call=legmap["long_call"],
                    entry_credit=entry_credit,
                    qty=qty,
                    spot=close_px,
                )
            else:
                skipped.append(f"spread_id={sid}: unsupported strategy_type={st}")
                continue
        except Exception as e:
            skipped.append(f"spread_id={sid} {und} {st}: {e}")
            continue

        oid = f"sweep_expired:{sid}:{int(time.time())}"
        closed_ts = time.time()
        updates.append(
            (
                res.close_reason,
                res.pnl_dollars,
                res.close_debit_mid,
                closed_ts,
                oid,
                sid,
            )
        )

        line = (
            f"  spread_id={sid:>4} {und:<6} {st:<4} {res.close_reason:<22} "
            f"pnl=${res.pnl_dollars:>10,.2f}  close@{exp_d}={res.underlying_close:.2f}  ({res.note})"
        )
        if res.pnl_dollars >= 0:
            wins.append(line)
        else:
            losses.append(line)

    # --- Console report ---
    W = 100
    print("=" * W)
    print(f"  SWEEP EXPIRED TRADES  —  now={now_la.isoformat(timespec='seconds')}  dry_run={dry_run}")
    print(f"  DB={db_path}")
    print("=" * W)

    if not rows:
        print("  No open rows (close_reason NULL) in alpaca_spreads.")
        conn.close()
        return 0

    print(f"\n  Candidates (NULL close_reason): {len(rows)}")
    print(f"  To update after filters:        {len(updates)}")
    print(f"  Skipped:                        {len(skipped)}")

    if skipped and len(skipped) <= 30:
        for s in skipped:
            print(f"    - {s}")
    elif skipped:
        for s in skipped[:15]:
            print(f"    - {s}")
        print(f"    ... ({len(skipped) - 15} more)")

    print("\n  --- WINS / BREAKEVEN (pnl >= 0) ---")
    for ln in wins:
        print(ln)
    if not wins:
        print("  (none)")

    print("\n  --- LOSSES (pnl < 0) ---")
    for ln in losses:
        print(ln)
    if not losses:
        print("  (none)")

    if dry_run:
        print("\n  [dry-run] No database writes.")
        conn.close()
        return 0

    cur = conn.cursor()
    for close_reason, pnl, cdb, closed_at, close_oid, spread_id in updates:
        cur.execute(
            f"""
            UPDATE {SPREADS}
            SET close_reason = ?,
                pnl_dollars = ?,
                close_debit_mid = ?,
                closed_at = ?,
                close_order_id = ?
            WHERE spread_id = ?
            """,
            (close_reason, pnl, cdb, closed_at, close_oid, spread_id),
        )
    conn.commit()
    conn.close()
    print(f"\n  Updated {len(updates)} row(s) in {SPREADS}.")
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(description="Sweep expired spreads with NULL close_reason into PnL ledger")
    ap.add_argument("--db", type=Path, default=_project_db(), help="path to alpaca_put_spread.db")
    ap.add_argument("--dry-run", action="store_true", help="print plan only; do not UPDATE")
    ap.add_argument(
        "--feed",
        default=DEFAULT_FEED,
        help="Alpaca stock bar feed (default: iex; use sip if your subscription allows)",
    )
    args = ap.parse_args()
    raise SystemExit(run_sweep(args.db, dry_run=args.dry_run, feed=args.feed))


if __name__ == "__main__":
    main()

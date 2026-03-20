#!/usr/bin/env python3
"""
Reconcile Alpaca put spread state: compare JSON state + DB vs live Alpaca positions/orders.
Reports drift (e.g. open_spread in state but no positions; positions but no state).
Usage:
  python tools/reconcile_alpaca_state.py
  python tools/reconcile_alpaca_state.py --fix   # optionally clear stale state (dry-run by default)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

# Add project root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv

load_dotenv()

STATE_PATH = Path(__file__).resolve().parents[1] / "data" / "alpaca_put_spread_state.json"
DB_PATH = Path(__file__).resolve().parents[1] / "data" / "alpaca_put_spread.db"


def main() -> int:
    ap = argparse.ArgumentParser(description="Reconcile Alpaca put spread state vs live")
    ap.add_argument("--fix", action="store_true", help="Propose fixes (does not modify by default)")
    args = ap.parse_args()

    from bot.alpaca_put_spread.alpaca_clients import make_alpaca_clients
    from trading_assistant.broker.alpaca.positions import list_open_positions

    paper = os.environ.get("ALPACA_PAPER", "true").lower() in ("1", "true", "yes")
    trading_client, _, _ = make_alpaca_clients(paper=paper)

    # Load JSON state
    state = {}
    if STATE_PATH.exists():
        state = json.loads(STATE_PATH.read_text(encoding="utf-8"))

    open_spreads = (state.get("open_spread_by_underlying") or {})
    pending_entry = (state.get("pending_entry_order_by_underlying") or {})
    pending_close = (state.get("pending_close_order_by_underlying") or {})

    # Fetch live positions
    try:
        positions = list_open_positions(trading_client)
    except Exception as e:
        print(f"ERROR: Failed to fetch Alpaca positions: {e}")
        return 1

    # Build set of symbols we expect to have positions for
    expected_symbols = set()
    for und, spread in open_spreads.items():
        short = spread.get("short_put_symbol")
        long_sym = spread.get("long_put_symbol")
        if short:
            expected_symbols.add(short)
        if long_sym:
            expected_symbols.add(long_sym)

    # Symbols we actually have
    actual_symbols = {s for s, p in (positions or {}).items() if float(p.get("qty", 0)) != 0}

    drift = []

    # Check: open_spread says we have positions, but we don't
    for und, spread in open_spreads.items():
        short = spread.get("short_put_symbol")
        long_sym = spread.get("long_put_symbol")
        short_qty = float((positions or {}).get(short, {}).get("qty", 0))
        long_qty = float((positions or {}).get(long_sym, {}).get("qty", 0))
        if short_qty == 0 and long_qty == 0:
            drift.append(f"STALE: open_spread for {und} but no positions (short={short}, long={long_sym})")

    # Check: we have option positions not in our state (manual trade or orphan)
    for sym in actual_symbols:
        found = any(
            (spread.get("short_put_symbol") == sym or spread.get("long_put_symbol") == sym)
            for spread in open_spreads.values()
        )
        if not found:
            drift.append(f"ORPHAN: position in {sym} not in open_spread_by_underlying")

    # Pending orders: optional check against Alpaca (would need to fetch orders)
    if pending_entry:
        drift.append(f"INFO: {len(pending_entry)} pending entry orders: {list(pending_entry.keys())}")
    if pending_close:
        drift.append(f"INFO: {len(pending_close)} pending close orders: {list(pending_close.keys())}")

    print("=== Alpaca Put Spread Reconciliation ===")
    print(f"Open spreads (state): {list(open_spreads.keys()) or 'none'}")
    print(f"Expected option symbols: {sorted(expected_symbols) or 'none'}")
    print(f"Actual positions (non-zero qty): {sorted(actual_symbols) or 'none'}")
    print()
    if drift:
        print("Drift / notes:")
        for d in drift:
            print(f"  - {d}")
        if args.fix:
            print()
            print("--fix: To clear stale open_spread, manually edit data/alpaca_put_spread_state.json")
            print("  or run with a future --fix-implemented flag (not yet implemented).")
    else:
        print("No drift detected.")

    return 0


if __name__ == "__main__":
    sys.exit(main())

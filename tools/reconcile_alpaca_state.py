#!/usr/bin/env python3
"""
Reconcile Alpaca options bot state: compare JSON state + DB vs live Alpaca positions.
Supports nested open_positions[underlying][PCS|CCS|IC].
Usage:
  python tools/reconcile_alpaca_state.py
  python tools/reconcile_alpaca_state.py --fix   # guidance only (no auto-edit)
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Add project root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv

load_dotenv()


def _symbols_from_open_spread(spread: dict) -> set[str]:
    legs = spread.get("legs")
    if isinstance(legs, list) and legs:
        return {str(x) for x in legs}
    out = set()
    for k in (
        "short_put_symbol",
        "long_put_symbol",
        "short_call_symbol",
        "long_call_symbol",
    ):
        v = spread.get(k)
        if v:
            out.add(str(v))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Reconcile Alpaca options state vs live")
    ap.add_argument("--fix", action="store_true", help="Print fix hints (no writes)")
    args = ap.parse_args()

    from bot.alpaca_put_spread.alpaca_clients import make_alpaca_clients
    from bot.alpaca_put_spread.state import load_state
    from trading_assistant.broker.alpaca.positions import list_open_positions

    paper = os.environ.get("ALPACA_PAPER", "true").lower() in ("1", "true", "yes")
    trading_client, _, _ = make_alpaca_clients(paper=paper)

    state = load_state()
    open_positions = state.get("open_positions") or {}
    pending_entry = state.get("pending_entry_order") or {}
    pending_close = state.get("pending_close_order") or {}

    try:
        positions = list_open_positions(trading_client)
    except Exception as e:
        print(f"ERROR: Failed to fetch Alpaca positions: {e}")
        return 1

    expected_symbols: set[str] = set()
    for und, by_st in open_positions.items():
        if not isinstance(by_st, dict):
            continue
        for st, spread in by_st.items():
            if not isinstance(spread, dict):
                continue
            expected_symbols |= _symbols_from_open_spread(spread)

    actual_symbols = {s for s, p in (positions or {}).items() if float(p.get("qty", 0)) != 0}

    drift = []

    for und, by_st in open_positions.items():
        if not isinstance(by_st, dict):
            continue
        for st, spread in by_st.items():
            if not isinstance(spread, dict):
                continue
            syms = _symbols_from_open_spread(spread)
            if not syms:
                continue
            flat = all(float((positions or {}).get(s, {}).get("qty", 0)) == 0 for s in syms)
            if flat:
                drift.append(f"STALE: open_positions[{und}][{st}] but no positions for {syms}")

    for sym in actual_symbols:
        found = False
        for by_st in open_positions.values():
            if not isinstance(by_st, dict):
                continue
            for spread in by_st.values():
                if isinstance(spread, dict) and sym in _symbols_from_open_spread(spread):
                    found = True
                    break
            if found:
                break
        if not found:
            drift.append(f"ORPHAN: position in {sym} not tracked in open_positions")

    pe_flat = sum(len(v) for v in pending_entry.values() if isinstance(v, dict))
    pc_flat = sum(len(v) for v in pending_close.values() if isinstance(v, dict))
    if pe_flat:
        drift.append(f"INFO: pending_entry_order slots: {pe_flat}")
    if pc_flat:
        drift.append(f"INFO: pending_close_order slots: {pc_flat}")

    print("=== Alpaca options reconciliation ===")
    print(f"Open underlyings: {list(open_positions.keys()) or 'none'}")
    print(f"Expected option symbols: {sorted(expected_symbols) or 'none'}")
    print(f"Actual positions (non-zero qty): {sorted(actual_symbols) or 'none'}")
    print()
    if drift:
        print("Drift / notes:")
        for d in drift:
            print(f"  - {d}")
        if args.fix:
            print()
            print("--fix: Edit data/alpaca_put_spread_state.json or clear stale open_positions entries.")
    else:
        print("No drift detected.")

    return 0


if __name__ == "__main__":
    sys.exit(main())

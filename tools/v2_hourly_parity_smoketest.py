#!/usr/bin/env python3
"""
Ad-hoc parity smoketest for V2 hourly selection vs legacy hourly selection.

This is a developer tool to catch obvious regressions in:
- eligible ticker filtering near spot (spot_window)
- farthest selection (generate_signals_farthest)

It fetches live Kalshi markets (REST) for the current hour event(s) and compares:
1) Legacy path: bot.market.fetch_eligible_tickers + bot.strategy.generate_signals_farthest
2) V2 path: HourlySignalsFarthestStrategy's normalization + same generator

Usage:
  python tools/v2_hourly_parity_smoketest.py --asset btc
  python tools/v2_hourly_parity_smoketest.py --asset eth --spot 3500
"""

from __future__ import annotations

import argparse
import json
from typing import Any, Dict, List, Optional

from bot.market import fetch_eligible_tickers, fetch_markets_for_event, get_current_hour_market_ids
from bot.strategy import generate_signals_farthest
from bot.pipeline.context import WindowContext
from bot.pipeline.strategies.hourly_signals_farthest import _normalize_event_quotes as v2_normalize_quotes


def _legacy_quotes(event_id: str, spot: float, window: float) -> List[Dict[str, Any]]:
    return fetch_eligible_tickers(event_id, spot_price=spot, window=window)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--asset", required=True, choices=["btc", "eth", "sol", "xrp", "doge"])
    ap.add_argument("--spot", type=float, default=None, help="Override spot (else inferred from market strikes only)")
    ap.add_argument("--spot_window", type=float, default=1500.0)
    args = ap.parse_args()

    event_ids = get_current_hour_market_ids(args.asset)
    if not event_ids:
        print("No current hour event ids.")
        return 1

    # Merge all markets across hourly event ids (above/below + range).
    markets_all: List[Dict[str, Any]] = []
    for eid in event_ids:
        mkts, err = fetch_markets_for_event(eid)
        if err:
            continue
        markets_all.extend([m for m in mkts if isinstance(m, dict)])
    if not markets_all:
        print("No markets fetched.")
        return 1

    # Spot: if not provided, approximate from mid of strikes (only for selection smoke).
    strikes = []
    for m in markets_all:
        s = m.get("strike_price") or m.get("cap_strike") or m.get("strike") or m.get("floor_strike") or m.get("ceiling_strike")
        try:
            if s is not None:
                strikes.append(float(s))
        except Exception:
            pass
    spot = float(args.spot) if args.spot is not None else (sum(strikes) / len(strikes) if strikes else 0.0)
    if spot <= 0:
        print("Could not infer spot; pass --spot.")
        return 1

    # Legacy: use the first event id (above/below) for eligible tickers (matches typical hourly loop)
    legacy_rows = _legacy_quotes(event_ids[0], spot, args.spot_window)
    # Convert into the quote shape used by generator
    legacy_quotes = []
    for r in legacy_rows:
        legacy_quotes.append(
            {
                "ticker": r.get("ticker"),
                "strike": r.get("strike"),
                "yes_bid": r.get("yes_bid"),
                "yes_ask": r.get("yes_ask"),
                "no_bid": r.get("no_bid"),
                "no_ask": r.get("no_ask"),
                "floor_strike": r.get("floor_strike"),
                "ceiling_strike": r.get("ceiling_strike"),
                "subtitle": r.get("subtitle", ""),
            }
        )

    # V2: build a minimal ctx and normalize from markets_all
    ctx = WindowContext(
        interval="hourly",
        market_id=event_ids[0],
        ticker=event_ids[0],
        asset=args.asset,
        seconds_to_close=30.0,
        quote={"yes_bid": 0, "yes_ask": 0, "no_bid": 0, "no_ask": 0},
        spot=spot,
        event_markets=markets_all,
        positions=[],
        open_orders=[],
        config={"hourly": {"strategies": {"hourly_signals_farthest": {"selection": {}}}}},
    )
    v2_quotes = v2_normalize_quotes(ctx, spot, args.spot_window)

    # Compare: farthest single
    thresholds = {"yes_min": 92, "yes_max": 99, "no_min": 92, "no_max": 99}
    legacy_signals = generate_signals_farthest(
        quotes=[type("Q", (), q)() for q in legacy_quotes],  # quick shim; generator expects TickerQuote-like attrs
        spot_price=spot,
        ctx_late_window=True,
        thresholds=thresholds,
        pick_all_in_range=False,
    )
    v2_signals = generate_signals_farthest(
        quotes=v2_quotes,
        spot_price=spot,
        ctx_late_window=True,
        thresholds=thresholds,
        pick_all_in_range=False,
    )

    print("=== hourly parity smoketest ===")
    print("asset:", args.asset, "spot:", spot, "spot_window:", args.spot_window, "events:", event_ids)
    print("legacy_eligible_count:", len(legacy_quotes))
    print("v2_event_quotes_count:", len(v2_quotes))
    print("legacy_best:", [s.__dict__ for s in legacy_signals] if legacy_signals else [])
    print("v2_best:", [s.__dict__ for s in v2_signals] if v2_signals else [])
    print("\nNOTE: This is a smoke test; mismatches require inspecting market_id selection and spot source.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


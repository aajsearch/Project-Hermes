"""Watchlist scanner with full evaluation records (pass + reject reasons)."""

from __future__ import annotations

from typing import Any

from .quotes import day_change_pct, mid_from_quote, spread_metrics


def evaluate_symbol(
    symbol: str,
    q: dict[str, Any],
    cfg: dict[str, Any],
    *,
    held: set[str],
    overrides: set[str],
) -> dict[str, Any]:
    sel = cfg.get("selection", {})
    cap = float(cfg.get("position_sizing", {}).get("target_notional_usd", 100))
    deprior = set(sel.get("deprioritize_symbols", []))
    core = set(sel.get("core_symbols", []))
    max_spread_pct = float(sel.get("max_spread_pct", 0.0015))
    max_spread_usd = float(sel.get("max_spread_usd", 0.25))
    min_day = float(sel.get("min_abs_day_change_pct", 0.003))
    max_day = float(sel.get("max_abs_day_change_pct", 0.008))
    min_price = float(sel.get("min_price_usd", 0))

    rec: dict[str, Any] = {
        "symbol": symbol,
        "qualified": False,
        "reject_reason": None,
        "mid": None,
        "spread_pct": None,
        "spread_usd": None,
        "day": None,
        "core": symbol in core,
        "deprior": symbol in deprior,
        "held": symbol in held,
    }

    if symbol in held:
        rec["reject_reason"] = "held"
        return rec

    mid = mid_from_quote(q)
    if mid is None:
        rec["reject_reason"] = "no_mid"
        return rec
    rec["mid"] = mid

    if min_price and mid < min_price:
        rec["reject_reason"] = "min_price"
        return rec

    sm = spread_metrics(q)
    if not sm:
        rec["reject_reason"] = "no_spread"
        return rec
    spread_pct, spread_usd = sm
    rec["spread_pct"] = spread_pct
    rec["spread_usd"] = spread_usd
    if not (spread_pct <= max_spread_pct and spread_usd <= max_spread_usd):
        rec["reject_reason"] = "spread"
        return rec

    day = day_change_pct(q)
    if day is None:
        rec["reject_reason"] = "no_day_move"
        return rec
    rec["day"] = day
    if not (min_day <= day <= max_day):
        rec["reject_reason"] = "day_move"
        return rec

    fits = mid <= cap or symbol in overrides
    if not fits and not cfg.get("entry", {}).get("allow_fractional_live", False):
        rec["reject_reason"] = "notional_cap"
        return rec

    rec["qualified"] = True
    return rec


def scan_watchlist_full(
    cfg: dict[str, Any],
    quotes: dict[str, dict],
    held_symbols: set[str],
    cap_overrides: list[str] | None = None,
) -> list[dict[str, Any]]:
    watchlist = list(cfg.get("watchlist", {}).get("all", []))
    overrides = set(cap_overrides or [])
    records = []
    for sym in watchlist:
        q = quotes.get(sym)
        if not q:
            records.append(
                {
                    "symbol": sym,
                    "qualified": False,
                    "reject_reason": "no_quote",
                    "mid": None,
                    "spread_pct": None,
                    "spread_usd": None,
                    "day": None,
                    "core": sym in set(cfg.get("selection", {}).get("core_symbols", [])),
                    "deprior": sym
                    in set(cfg.get("selection", {}).get("deprioritize_symbols", [])),
                    "held": sym in held_symbols,
                }
            )
            continue
        records.append(
            evaluate_symbol(sym, q, cfg, held=held_symbols, overrides=overrides)
        )

    qualified = [r for r in records if r["qualified"]]
    qualified.sort(
        key=lambda r: (
            r["deprior"],
            r["spread_pct"] or 999,
            not r["core"],
            -(r["day"] or 0),
        )
    )
    # Annotate rank on qualified
    for i, r in enumerate(qualified):
        r["rank"] = i + 1
    rank_map = {r["symbol"]: r["rank"] for r in qualified}
    for r in records:
        if r["symbol"] in rank_map:
            r["rank"] = rank_map[r["symbol"]]
    return records


def qualified_only(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        [r for r in records if r.get("qualified")],
        key=lambda r: r.get("rank") or 999,
    )

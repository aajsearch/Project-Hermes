"""Quote helpers (public Robinhood equity API)."""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from typing import Any

QUOTES_URL = "https://api.robinhood.com/quotes/"


def fetch_quotes(symbols: list[str]) -> dict[str, dict[str, Any]]:
    if not symbols:
        return {}
    url = QUOTES_URL + "?" + urllib.parse.urlencode({"symbols": ",".join(symbols)})
    req = urllib.request.Request(url, headers={"User-Agent": "hades-command-center/1.0"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.load(resp)
    results = data.get("results", data)
    out: dict[str, dict[str, Any]] = {}
    for row in results:
        sym = row.get("symbol")
        if sym:
            out[sym] = row
    return out


def mid_from_quote(q: dict) -> float | None:
    bid = float(q.get("bid_price") or 0)
    ask = float(q.get("ask_price") or 0)
    last = float(q.get("last_trade_price") or 0)
    if bid > 0 and ask > 0:
        return (bid + ask) / 2
    return last if last > 0 else None


def day_change_pct(q: dict, absolute: bool = True) -> float | None:
    prev = float(q.get("adjusted_previous_close") or q.get("previous_close") or 0)
    last = float(q.get("last_trade_price") or 0)
    if prev <= 0:
        return None
    chg = (last - prev) / prev
    return abs(chg) if absolute else chg


def signed_day_change_pct(q: dict) -> float | None:
    return day_change_pct(q, absolute=False)


def spread_metrics(q: dict) -> tuple[float, float] | None:
    bid = float(q.get("bid_price") or 0)
    ask = float(q.get("ask_price") or 0)
    if bid <= 0 or ask <= 0:
        return None
    mid = (bid + ask) / 2
    spread_usd = ask - bid
    spread_pct = spread_usd / mid if mid else 999
    return spread_pct, spread_usd

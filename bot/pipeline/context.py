"""
WindowContext: normalized per-tick data for strategy evaluation (Bot V2).
Built by the data layer once per (asset, interval) per tick.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal

logger = logging.getLogger(__name__)

IntervalType = Literal["fifteen_min", "hourly"]


def _default_quote() -> Dict[str, int]:
    return {"yes_bid": 0, "yes_ask": 0, "no_bid": 0, "no_ask": 0}


@dataclass
class WindowContext:
    """
    All normalized data for the current tick (one asset, one interval).
    Strategies read from ctx for evaluate_entry(ctx) and evaluate_exit(ctx, my_orders).
    """

    interval: IntervalType
    market_id: str
    ticker: str
    asset: str
    seconds_to_close: float
    quote: Dict[str, int] = field(default_factory=_default_quote)  # yes_bid, yes_ask, no_bid, no_ask (cents)
    # Hourly events have many tickers (strikes/ranges). For hourly strategies that need parity with legacy
    # selection and per-ticker exit checks, we attach the full event market list (REST or WS seed).
    event_markets: List[Dict[str, Any]] = field(default_factory=list)
    # Single spot source (provider-agnostic: currently Coinbase; swap to Kraken by changing data layer + oracle).
    spot: float | None = None
    spot_source: str = "?"  # "WS" (oracle WebSocket) or "REST"
    spot_age_s: float | None = None  # seconds since last update (when spot_source=WS)
    strike: float | None = None
    strike_source: str | None = None  # "api_fields", "subtitle", "title", or "ticker"
    distance: float | None = None  # abs(spot - strike) from the single spot source
    positions: List[Dict[str, Any]] = field(default_factory=list)  # [{ticker, side, count, entry_price_cents}, ...]
    open_orders: List[Any] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for key in ("yes_bid", "yes_ask", "no_bid", "no_ask"):
            if key not in self.quote:
                self.quote[key] = 0
        for pos in self.positions:
            if not all(k in pos for k in ("ticker", "side", "count", "entry_price_cents")):
                logger.warning(
                    "WindowContext.positions entry missing required keys: %s",
                    list(pos.keys()),
                )

    @property
    def yes_bid(self) -> int:
        return self.quote.get("yes_bid", 0)

    @property
    def yes_ask(self) -> int:
        return self.quote.get("yes_ask", 0)

    @property
    def no_bid(self) -> int:
        return self.quote.get("no_bid", 0)

    @property
    def no_ask(self) -> int:
        return self.quote.get("no_ask", 0)

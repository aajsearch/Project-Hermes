import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Optional

from alpaca.data.requests import StockBarsRequest, StockLatestQuoteRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

DEFAULT_FEED = "iex"


class DataError(RuntimeError):
    """Stock quote missing or unusable (e.g. zero bid/ask); do not trade on fabricated prices."""


def _equity_root_symbol_or_raise(symbol: str) -> str:
    """
    Stock latest-quote API must receive root tickers only (e.g. AMZN, SPY), never OCC option symbols.
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        raise ValueError("underlying symbol must be non-empty")
    if len(sym) > 5:
        raise ValueError(f"invalid equity root symbol (len>5): {symbol!r}")
    if any(ch.isdigit() for ch in sym):
        raise ValueError(f"invalid equity root symbol (digits/OCC not allowed): {symbol!r}")
    return sym


def _quote_price_to_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if x != x:  # NaN
        return None
    return x


def get_latest_mid(stock_data, symbol: str, feed: str = DEFAULT_FEED) -> float:
    sym = _equity_root_symbol_or_raise(symbol)
    req = StockLatestQuoteRequest(symbol_or_symbols=[sym], feed=feed)
    try:
        quotes = stock_data.get_stock_latest_quote(req)
    except Exception as e:
        raise DataError(f"get_stock_latest_quote failed for {sym!r}: {e}") from e
    if not isinstance(quotes, dict) or sym not in quotes:
        raise DataError(f"empty or missing quote response for {sym!r}")
    q = quotes[sym]
    if q is None:
        raise DataError(f"null quote object for {sym!r}")

    bid = _quote_price_to_float(getattr(q, "bid_price", None))
    ask = _quote_price_to_float(getattr(q, "ask_price", None))

    if bid is not None and bid > 0 and ask is not None and ask > 0:
        return (bid + ask) / 2.0
    if bid is not None and bid > 0 and (ask is None or ask <= 0):
        return float(bid)
    if ask is not None and ask > 0 and (bid is None or bid <= 0):
        return float(ask)

    raise DataError(f"no usable bid/ask for {sym!r}: bid={bid!r} ask={ask!r}")


def _parse_timeframe(timeframe: str) -> TimeFrame:
    tf = (timeframe or "5Min").strip()
    if tf == "5Min":
        return TimeFrame(5, TimeFrameUnit.Minute)
    if tf == "15Min":
        return TimeFrame(15, TimeFrameUnit.Minute)
    if tf == "1Min":
        return TimeFrame(1, TimeFrameUnit.Minute)
    if tf == "1Hour":
        return TimeFrame(1, TimeFrameUnit.Hour)
    if tf in ("1Day", "Day", "1D"):
        return TimeFrame(1, TimeFrameUnit.Day)
    return TimeFrame(5, TimeFrameUnit.Minute)


def _window_utc(timeframe: str, lookback: int):
    """
    Provide a conservative calendar-day window so Alpaca returns bars
    even on weekends/off-hours.
    """
    end = datetime.now(timezone.utc)

    tf = (timeframe or "5Min").strip()
    if tf == "1Min":
        days = max(7, int(lookback / 390) + 7)
    elif tf == "5Min":
        days = max(12, int(lookback / 78) + 12)
    elif tf == "15Min":
        days = max(25, int(lookback / 26) + 25)
    elif tf == "1Hour":
        days = max(90, int(lookback / 7) + 90)
    else:  # daily
        days = max(365, lookback + 60)

    start = end - timedelta(days=days)
    return start, end


def get_bars_df(stock_data, symbol: str, lookback: int = 200, timeframe: str = "5Min", feed: str = DEFAULT_FEED) -> pd.DataFrame:
    tf = _parse_timeframe(timeframe)
    start, end = _window_utc(timeframe, lookback)

    req = StockBarsRequest(
        symbol_or_symbols=[symbol],
        timeframe=tf,
        start=start,
        end=end,
        limit=10000,
        feed=feed,
    )

    resp = stock_data.get_stock_bars(req)
    bars = resp.df

    if bars is None or bars.empty:
        return pd.DataFrame()

    if isinstance(bars.index, pd.MultiIndex):
        bars = bars.reset_index()
        bars = bars[bars["symbol"] == symbol].copy()
        bars = bars.sort_values("timestamp")

    if len(bars) > lookback:
        bars = bars.iloc[-lookback:].copy()

    return bars

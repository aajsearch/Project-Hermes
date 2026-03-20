import pandas as pd
from datetime import datetime, timedelta, timezone

from alpaca.data.requests import StockBarsRequest, StockLatestQuoteRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

DEFAULT_FEED = "iex"


def get_latest_mid(stock_data, symbol: str, feed: str = DEFAULT_FEED) -> float:
    req = StockLatestQuoteRequest(symbol_or_symbols=[symbol], feed=feed)
    q = stock_data.get_stock_latest_quote(req)[symbol]
    if q.bid_price and q.ask_price:
        return (q.bid_price + q.ask_price) / 2.0
    return float(q.ask_price or q.bid_price or 0.0)


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

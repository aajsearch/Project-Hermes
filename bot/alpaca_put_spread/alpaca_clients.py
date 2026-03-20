from __future__ import annotations

import os
from typing import Tuple

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.historical.option import OptionHistoricalDataClient
from alpaca.trading.client import TradingClient


def make_alpaca_clients(*, paper: bool) -> Tuple[TradingClient, StockHistoricalDataClient, OptionHistoricalDataClient]:
    api_key = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")

    if not api_key or not secret_key:
        raise ValueError("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY in environment")

    trading = TradingClient(api_key, secret_key, paper=paper)
    stock_data = StockHistoricalDataClient(api_key, secret_key)
    option_data = OptionHistoricalDataClient(api_key, secret_key)
    return trading, stock_data, option_data


import os
from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.trading.client import TradingClient

load_dotenv()

API_KEY = os.getenv("ALPACA_API_KEY")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

def make_trading_client(paper: bool = True) -> TradingClient:
    if not API_KEY or not SECRET_KEY:
        raise ValueError("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY in .env")
    return TradingClient(API_KEY, SECRET_KEY, paper=paper)

def make_stock_data_client() -> StockHistoricalDataClient:
    if not API_KEY or not SECRET_KEY:
        raise ValueError("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY in .env")
    return StockHistoricalDataClient(API_KEY, SECRET_KEY)

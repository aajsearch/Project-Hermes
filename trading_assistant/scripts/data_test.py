import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from datetime import datetime, timedelta, timezone
from broker.alpaca.client import make_stock_data_client
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

stock_data = make_stock_data_client()

symbol = "SPY"
end = datetime.now(timezone.utc)
start = end - timedelta(days=10)  # enough to include last market session

req = StockBarsRequest(
    symbol_or_symbols=[symbol],
    timeframe=TimeFrame(5, TimeFrameUnit.Minute),
    start=start,
    end=end,
    limit=1000,
    feed="iex",
)

resp = stock_data.get_stock_bars(req)
df = resp.df

print("Start:", start.isoformat())
print("End:", end.isoformat())
print("Bars df is None?", df is None)
print("Bars df empty?", (df is not None and df.empty))
print("Columns:", list(df.columns) if df is not None else None)
print("Tail:\n", df.tail(5) if df is not None else None)

import time
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

def wait_for_order(trading_client, order_id: str, timeout_s: int = 60):
    start = time.time()
    last = None
    while time.time() - start < timeout_s:
        o = trading_client.get_order_by_id(order_id)
        if o.status != last:
            last = o.status
        if o.status in ("filled", "canceled", "rejected"):
            return o
        time.sleep(1)
    return trading_client.get_order_by_id(order_id)

def _looks_like_option_contract(symbol: str) -> bool:
    # Your contracts look like: SPY260515C00702000 (len > 10 and ends with digits)
    return bool(symbol) and len(symbol) > 10 and symbol[-1].isdigit()

def submit_market_order(
    trading_client,
    symbol: str,
    side: str,
    qty: float,
    time_in_force: TimeInForce = TimeInForce.DAY
):
    # Alpaca options: qty must be whole; TIF should be DAY
    if _looks_like_option_contract(symbol):
        qty = int(qty)
        time_in_force = TimeInForce.DAY

    req = MarketOrderRequest(
        symbol=symbol,
        side=OrderSide.BUY if side.upper() == "BUY" else OrderSide.SELL,
        qty=qty,
        time_in_force=time_in_force,
    )
    o = trading_client.submit_order(req)
    return str(o.id)  # Convert UUID to string at source

def cancel_open_orders_for_symbol_best_effort(trading_client, symbol: str):
    # Cancel by symbol isn't always available in alpaca-py
    try:
        trading_client.cancel_orders()
    except Exception:
        pass

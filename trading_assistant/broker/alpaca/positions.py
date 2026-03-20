from typing import Optional

def get_open_position_qty(trading_client, symbol: str) -> float:
    try:
        pos = trading_client.get_open_position(symbol)
        return float(pos.qty)
    except Exception:
        return 0.0

def list_open_positions(trading_client) -> dict:
    out = {}
    try:
        positions = trading_client.get_all_positions()
        for p in positions:
            out[p.symbol] = {
                "qty": float(p.qty),
                "avg_entry_price": float(p.avg_entry_price),
                "market_value": float(p.market_value),
                "unrealized_pl": float(p.unrealized_pl),
                "unrealized_plpc": float(p.unrealized_plpc),
            }
    except Exception:
        pass
    return out

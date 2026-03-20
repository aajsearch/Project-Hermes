from __future__ import annotations

import time
import uuid
from typing import Any, Dict, Optional

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderClass, OrderSide, OrderStatus, PositionIntent, TimeInForce
from alpaca.trading.requests import LimitOrderRequest, OptionLegRequest


def _normalize_order_status(status: str) -> str:
    """
    Alpaca SDK sometimes returns enum-like strings such as:
      - 'filled'
      - 'orderstatus.filled'
    We normalize by taking the last dot-separated token.
    """
    s = str(status or "").lower()
    if not s:
        return s
    return s.split(".")[-1]


def _order_terminal_status(status: str) -> bool:
    return _normalize_order_status(status) in ("filled", "canceled", "rejected", "expired")


def wait_for_order(
    trading_client: TradingClient,
    order_id: str,
    timeout_seconds: int = 60,
) -> Any:
    start = time.time()
    last_status: Optional[str] = None
    while time.time() - start < timeout_seconds:
        o = trading_client.get_order_by_id(order_id)
        status = _normalize_order_status(getattr(o, "status", ""))
        if status and status != last_status:
            last_status = status
        if _order_terminal_status(status):
            return o
        time.sleep(1.0)
    return trading_client.get_order_by_id(order_id)


def cancel_order(trading_client: TradingClient, order_id: str) -> None:
    try:
        trading_client.cancel_order_by_id(order_id)
    except Exception:
        # best-effort
        pass


def submit_mleg_limit_order(
    trading_client: TradingClient,
    *,
    qty: int,
    limit_price: float,
    legs: list[Dict[str, Any]],
    client_order_id: Optional[str] = None,
    time_in_force: TimeInForce = TimeInForce.DAY,
) -> str:
    """
    Submit an options multi-leg limit order (OrderClass.MLEG).

    Alpaca SDK semantics for mleg limit_price:
      - positive => debit
      - negative => credit
    """
    if not legs or len(legs) < 2:
        raise ValueError("legs must contain at least 2 option legs")

    if client_order_id is None:
        client_order_id = f"mleg:{uuid.uuid4().hex[:12]}"

    leg_reqs: list[OptionLegRequest] = []
    for leg in legs:
        leg_reqs.append(
            OptionLegRequest(
                symbol=str(leg["symbol"]),
                ratio_qty=float(leg.get("ratio_qty", 1.0)),
                side=leg.get("side"),
                position_intent=leg.get("position_intent"),
            )
        )

    req = LimitOrderRequest(
        qty=int(qty),
        limit_price=float(limit_price),
        time_in_force=time_in_force,
        order_class=OrderClass.MLEG,
        legs=leg_reqs,
        client_order_id=client_order_id,
    )
    o = trading_client.submit_order(req)
    return str(o.id)


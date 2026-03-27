from __future__ import annotations

import time
import uuid
from typing import Any, Callable, Dict, Optional, TypeVar

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderClass, OrderSide, PositionIntent, TimeInForce
from alpaca.trading.requests import LimitOrderRequest, OptionLegRequest

T = TypeVar("T")


def is_transient_request_error(exc: BaseException) -> bool:
    """
    True for network blips and retryable HTTP statuses (5xx / 429). Alpaca 4xx business errors are False.
    """
    try:
        import requests
    except ImportError:
        requests = None  # type: ignore
    if requests is not None:
        if isinstance(
            exc,
            (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError,
            ),
        ):
            return True
        if isinstance(exc, requests.exceptions.HTTPError):
            resp = getattr(exc, "response", None)
            code = getattr(resp, "status_code", None) if resp is not None else None
            if code is not None and int(code) in (429, 500, 502, 503, 504):
                return True
    try:
        import urllib3

        if isinstance(
            exc,
            (
                urllib3.exceptions.ProtocolError,
                urllib3.exceptions.ReadTimeoutError,
            ),
        ):
            return True
    except Exception:
        pass
    if isinstance(exc, (BrokenPipeError, ConnectionResetError)):
        return True
    if isinstance(exc, OSError) and getattr(exc, "errno", None) in (54, 104, 110):
        return True
    return False


def retry_transient(
    fn: Callable[[], T],
    *,
    attempts: int = 3,
    backoff_seconds: float = 0.5,
) -> T:
    last: Optional[BaseException] = None
    for i in range(max(1, attempts)):
        try:
            return fn()
        except BaseException as e:
            last = e
            if not is_transient_request_error(e) or i >= attempts - 1:
                raise
            time.sleep(backoff_seconds * (i + 1))
    assert last is not None
    raise last


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
    *,
    retry_attempts: int = 3,
    retry_backoff_seconds: float = 0.5,
) -> Any:
    start = time.time()
    last_status: Optional[str] = None
    while time.time() - start < timeout_seconds:

        def _get() -> Any:
            return trading_client.get_order_by_id(order_id)

        o = retry_transient(_get, attempts=retry_attempts, backoff_seconds=retry_backoff_seconds)
        status = _normalize_order_status(getattr(o, "status", ""))
        if status and status != last_status:
            last_status = status
        if _order_terminal_status(status):
            return o
        time.sleep(1.0)

    def _get_final() -> Any:
        return trading_client.get_order_by_id(order_id)

    return retry_transient(_get_final, attempts=retry_attempts, backoff_seconds=retry_backoff_seconds)


def cancel_order(
    trading_client: TradingClient,
    order_id: str,
    *,
    retry_attempts: int = 3,
    retry_backoff_seconds: float = 0.5,
) -> None:
    try:

        def _cancel() -> None:
            trading_client.cancel_order_by_id(order_id)

        retry_transient(_cancel, attempts=retry_attempts, backoff_seconds=retry_backoff_seconds)
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
    retry_attempts: int = 3,
    retry_backoff_seconds: float = 0.5,
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

    def _submit() -> Any:
        return trading_client.submit_order(req)

    o = retry_transient(_submit, attempts=retry_attempts, backoff_seconds=retry_backoff_seconds)
    return str(o.id)


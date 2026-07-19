from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional, Sequence

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce, QueryOrderStatus
from alpaca.trading.requests import GetOrdersRequest, LimitOrderRequest, MarketOrderRequest

from mm_bot.execution.base import ExecutionGateway, OrderRequest, OrderResponse


@dataclass(frozen=True)
class AlpacaOrder:
    order_id: str
    client_order_id: str
    status: str


class AlpacaExecution(ExecutionGateway):
    def __init__(self, trading_client: TradingClient, *, execute: bool):
        self._trading = trading_client
        self._execute = bool(execute)
        self._logger = logging.getLogger("alpaca_etf_scalper.execution")
        # Dry-run needs a local view of "open orders" so the bot doesn't
        # repeatedly submit the same TP/entry orders in a tight loop.
        self._dry_open_orders: dict[str, list[OrderResponse]] = {}
        self._dry_next_id: int = 1

    async def start(self) -> None:
        return

    async def close(self) -> None:
        return

    async def place_post_only_limit(self, req: OrderRequest) -> OrderResponse:
        # For equities, we just place a LIMIT; post_only is ignored.
        side = OrderSide.BUY if req.side.upper() == "BUY" else OrderSide.SELL
        order = LimitOrderRequest(
            symbol=req.product_id,
            qty=req.size,
            side=side,
            # Some assets (e.g., hard-to-borrow) only allow DAY orders.
            time_in_force=TimeInForce.DAY,
            limit_price=req.price,
            client_order_id=req.client_order_id,
        )
        if not self._execute:
            self._logger.info(
                "dry_run_limit_order",
                extra={"extra": {"symbol": req.product_id, "side": req.side, "qty": req.size, "limit_price": req.price}},
            )
            oid = f"dry:{self._dry_next_id}"
            self._dry_next_id += 1
            resp = OrderResponse(order_id=oid, client_order_id=req.client_order_id, status="OPEN", error_message=None)
            sym = str(req.product_id).upper()
            self._dry_open_orders.setdefault(sym, []).append(resp)
            return resp

        def _call():
            return self._trading.submit_order(order)

        try:
            resp = await asyncio.to_thread(_call)
            oid = getattr(resp, "id", "") or getattr(resp, "order_id", "") or ""
            return OrderResponse(order_id=str(oid), client_order_id=req.client_order_id, status="OPEN", error_message=None)
        except Exception as e:
            return OrderResponse(order_id="", client_order_id=req.client_order_id, status="REJECTED", error_message=str(e))

    async def place_market_order(self, *, product_id: str, side: str, base_size: float) -> OrderResponse:
        side_enum = OrderSide.BUY if str(side).upper() == "BUY" else OrderSide.SELL
        coid = f"alpaca_market:{product_id}:{side}:{int(time.time()*1000)}"
        order = MarketOrderRequest(symbol=product_id, qty=base_size, side=side_enum, time_in_force=TimeInForce.DAY, client_order_id=coid)
        if not self._execute:
            self._logger.warning(
                "dry_run_market_order",
                extra={"extra": {"symbol": product_id, "side": side, "qty": float(base_size)}},
            )
            return OrderResponse(order_id="", client_order_id=coid, status="OPEN", error_message=None)

        def _call():
            return self._trading.submit_order(order)

        try:
            resp = await asyncio.to_thread(_call)
            oid = getattr(resp, "id", "") or getattr(resp, "order_id", "") or ""
            self._logger.warning("market_order_submitted", extra={"extra": {"symbol": product_id, "side": side, "qty": float(base_size), "order_id": str(oid)}})
            return OrderResponse(order_id=str(oid), client_order_id=coid, status="OPEN", error_message=None)
        except Exception as e:
            return OrderResponse(order_id="", client_order_id=coid, status="REJECTED", error_message=str(e))

    async def cancel_all(self, product_id: str) -> None:
        if not self._execute:
            sym = str(product_id).upper()
            self._dry_open_orders.pop(sym, None)
            return
        # Alpaca "open" status can miss orders in states like "new"/"accepted".
        # We list broadly and cancel anything not terminal.
        terminal = {"canceled", "filled", "rejected", "expired", "done_for_day"}

        def _list():
            req = GetOrdersRequest(status=QueryOrderStatus.ALL, symbols=[product_id], limit=500)
            return self._trading.get_orders(filter=req)

        try:
            orders = await asyncio.to_thread(_list)
        except Exception:
            return
        ids = []
        for o in orders or []:
            st = str(getattr(o, "status", "") or "").lower()
            if st in terminal:
                continue
            oid = getattr(o, "id", None) or getattr(o, "order_id", None)
            if oid:
                ids.append(str(oid))
        for oid in ids:
            try:
                await asyncio.to_thread(self._trading.cancel_order_by_id, oid)
            except Exception:
                pass

    async def list_open_orders(self, product_id: str) -> Sequence[OrderResponse]:
        if not self._execute:
            sym = str(product_id).upper()
            return list(self._dry_open_orders.get(sym, []))

        terminal = {"canceled", "filled", "rejected", "expired", "done_for_day"}

        def _list():
            req = GetOrdersRequest(status=QueryOrderStatus.ALL, symbols=[product_id], limit=500)
            return self._trading.get_orders(filter=req)

        try:
            orders = await asyncio.to_thread(_list)
        except Exception:
            return []
        out = []
        for o in orders or []:
            st = str(getattr(o, "status", "") or "").lower()
            if st in terminal:
                continue
            oid = getattr(o, "id", "") or getattr(o, "order_id", "") or ""
            coid = getattr(o, "client_order_id", "") or ""
            out.append(OrderResponse(order_id=str(oid), client_order_id=str(coid), status="OPEN", error_message=None))
        return out


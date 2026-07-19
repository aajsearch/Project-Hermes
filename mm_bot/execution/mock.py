from __future__ import annotations

import asyncio
import logging
import random
import time
import uuid
from dataclasses import dataclass
from typing import Dict, Optional, Sequence

from mm_bot.execution.base import ExecutionGateway, OrderRequest, OrderResponse
from mm_bot.core.events import EventType, FillEvent


@dataclass
class _MockOrder:
    req: OrderRequest
    order_id: str
    client_order_id: str
    status: str  # OPEN/FILLED/CANCELED


class MockExecution(ExecutionGateway):
    """
    Safe testing gateway.
    - Always 'accepts' orders
    - Does not simulate fills by default (leave to future enhancement)
    """

    def __init__(
        self,
        fills_q: asyncio.Queue,
        *,
        starting_usd: float = 10_000.0,
        initial_order_id_by_client: Optional[Dict[str, str]] = None,
    ):
        self._fills_q = fills_q
        self._usd = float(starting_usd)
        self._open: Dict[str, _MockOrder] = {}
        self._order_id_by_client: Dict[str, str] = dict(initial_order_id_by_client or {})
        self._fills_task: Optional[asyncio.Task] = None
        self._logger = logging.getLogger("mm_bot.execution.mock")

    async def start(self) -> None:
        if self._fills_task is None:
            self._fills_task = asyncio.create_task(self._simulate_fills_loop(), name="mock_fill_simulator")

    async def close(self) -> None:
        if self._fills_task is not None:
            self._fills_task.cancel()
            try:
                await self._fills_task
            except asyncio.CancelledError:
                pass
            except Exception:
                # Don't let simulator failure prevent shutdown.
                pass
            self._fills_task = None

    async def place_post_only_limit(self, req: OrderRequest) -> OrderResponse:
        # Simulate network latency
        await asyncio.sleep(0.05)
        # Idempotency: reusing the same client_order_id returns the same order_id.
        oid = self._order_id_by_client.get(req.client_order_id)
        if oid is None:
            oid = str(uuid.uuid4())
            self._order_id_by_client[req.client_order_id] = oid
            self._open[oid] = _MockOrder(
                req=req,
                order_id=oid,
                client_order_id=req.client_order_id,
                status="OPEN",
            )
        else:
            # "Replace" semantics in mock: update price/size and reopen if needed.
            mo = self._open.get(oid)
            if mo is None:
                self._open[oid] = _MockOrder(req=req, order_id=oid, client_order_id=req.client_order_id, status="OPEN")
            else:
                mo.req = req
                mo.status = "OPEN"
        return OrderResponse(order_id=oid, client_order_id=req.client_order_id, status="OPEN", error_message=None)

    async def place_market_order(self, *, product_id: str, side: str, base_size: float) -> OrderResponse:
        # Simulate immediate fill
        await asyncio.sleep(0.02)
        oid = str(uuid.uuid4())
        coid = f"MOCK_MARKET:{product_id}:{side}:{int(time.time() * 1000)}"
        self._logger.warning(
            "mock_market_order",
            extra={"extra": {"product_id": product_id, "side": side, "size": float(base_size), "order_id": oid}},
        )
        try:
            self._fills_q.put_nowait(
                FillEvent(
                    type=EventType.FILL,
                    ts_ms=int(time.time() * 1000),
                    order_id=oid,
                    product_id=product_id,
                    side=side.upper(),
                    price=0.0,
                    size=float(base_size),
                    fee=0.0,
                    meta={"source": "mock_market"},
                )
            )
        except asyncio.QueueFull:
            pass
        return OrderResponse(order_id=oid, client_order_id=coid, status="FILLED", error_message=None)

    async def cancel_all(self, product_id: str) -> None:
        await asyncio.sleep(0.02)
        for o in self._open.values():
            if str(o.req.product_id) == str(product_id):
                o.status = "CANCELED"

    async def list_open_orders(self, product_id: str) -> Sequence[OrderResponse]:
        await asyncio.sleep(0.01)
        out = []
        for o in self._open.values():
            if o.status == "OPEN" and str(o.req.product_id) == str(product_id):
                out.append(OrderResponse(order_id=o.order_id, client_order_id=o.client_order_id, status="OPEN", error_message=None))
        return out

    async def _simulate_fills_loop(self) -> None:
        """
        Every few seconds, randomly fill one open order (30% probability).
        This is used to test inventory/risk flows without a real exchange.
        """
        while True:
            await asyncio.sleep(3)
            open_orders = [o for o in self._open.values() if o.status == "OPEN"]
            if not open_orders:
                continue
            if random.random() > 0.30:
                continue
            chosen = random.choice(open_orders)
            chosen.status = "FILLED"
            self._logger.info(
                "mock_fill_simulated",
                extra={
                    "extra": {
                        "order_id": chosen.order_id,
                        "client_order_id": chosen.client_order_id,
                        "side": chosen.req.side,
                        "price": chosen.req.price,
                        "size": chosen.req.size,
                    }
                },
            )
            # Publish fill event into the pipeline
            try:
                self._fills_q.put_nowait(
                    FillEvent(
                        type=EventType.FILL,
                        ts_ms=int(time.time() * 1000),
                        order_id=chosen.order_id,
                        product_id=chosen.req.product_id,
                        side=chosen.req.side.upper(),
                        price=float(chosen.req.price),
                        size=float(chosen.req.size),
                        fee=0.0,
                        meta={},
                    )
                )
            except asyncio.QueueFull:
                self._logger.warning("fills_queue_full", extra={"extra": {"dropped": True}})


from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import Optional, Sequence


@dataclass(frozen=True)
class OrderRequest:
    product_id: str
    side: str  # BUY/SELL
    price: float
    size: float
    post_only: bool
    client_order_id: str


@dataclass(frozen=True)
class OrderResponse:
    order_id: str
    client_order_id: str
    status: str  # OPEN / REJECTED / UNKNOWN
    error_message: Optional[str] = None


class ExecutionGateway(abc.ABC):
    @abc.abstractmethod
    async def start(self) -> None: ...

    @abc.abstractmethod
    async def close(self) -> None: ...

    @abc.abstractmethod
    async def place_post_only_limit(self, req: OrderRequest) -> OrderResponse: ...

    @abc.abstractmethod
    async def place_market_order(self, *, product_id: str, side: str, base_size: float) -> OrderResponse: ...

    @abc.abstractmethod
    async def cancel_all(self, product_id: str) -> None: ...

    @abc.abstractmethod
    async def list_open_orders(self, product_id: str) -> Sequence[OrderResponse]: ...


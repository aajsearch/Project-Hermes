from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional


class EventType(str, Enum):
    ORDER_INTENT = "order_intent"
    ORDER_UPDATE = "order_update"
    RISK = "risk"
    PNL = "pnl"
    FILL = "fill"
    CONTROL = "control"


@dataclass(frozen=True)
class BaseEvent:
    type: EventType
    ts_ms: int
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MarketDataEvent:
    """
    Market data event emitted by the Market Data module.

    Required fields (per spec):
    - timestamp
    - best_bid
    - best_ask
    - mid_price
    - rolling_volatility

    Notes:
    - `product_id` is included for multi-product support.
    - Backwards-compatible aliases: `.mid` and `.rolling_vol`.
    """

    timestamp_ms: int
    best_bid: Optional[float]
    best_ask: Optional[float]
    mid_price: Optional[float]
    rolling_volatility: Optional[float]
    product_id: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)

    @property
    def mid(self) -> Optional[float]:
        return self.mid_price

    @property
    def rolling_vol(self) -> Optional[float]:
        return self.rolling_volatility


@dataclass(frozen=True)
class OrderIntentEvent(BaseEvent):
    product_id: str = ""
    bid_price: Optional[float] = None
    bid_size: Optional[float] = None
    ask_price: Optional[float] = None
    ask_size: Optional[float] = None
    trigger_market_stop_loss: bool = False
    reason: str = ""

    @property
    def inventory_base(self) -> float:
        try:
            return float((self.meta or {}).get("inventory_base", 0.0) or 0.0)
        except (TypeError, ValueError):
            return 0.0

    @property
    def mid_price(self) -> Optional[float]:
        v = (self.meta or {}).get("mid_price", None)
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None


class OrderStatus(str, Enum):
    OPEN = "OPEN"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class OrderUpdateEvent(BaseEvent):
    product_id: str = ""
    order_id: str = ""
    client_order_id: str = ""
    side: str = ""  # BUY/SELL
    status: OrderStatus = OrderStatus.UNKNOWN
    filled_size: float = 0.0
    avg_fill_price: Optional[float] = None
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RiskEvent(BaseEvent):
    ok: bool = True
    reason: str = ""
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FillEvent(BaseEvent):
    order_id: str = ""
    product_id: str = ""
    side: str = ""  # BUY/SELL
    price: float = 0.0
    size: float = 0.0
    fee: float = 0.0


@dataclass(frozen=True)
class ControlEvent(BaseEvent):
    action: str = ""  # START/STOP/PAUSE/RESUME
    reason: str = ""


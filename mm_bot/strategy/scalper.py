from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Optional, Tuple

from mm_bot.core.events import EventType, MarketDataEvent, OrderIntentEvent
from mm_bot.strategy.market_maker import PositionState


@dataclass
class ScalperState:
    last_emit_ts_ms: int = 0
    last_bid_ask: Optional[Tuple[float, float]] = None


class SimpleScalper:
    """
    Directional position scalper:
    - If flat: place limit BUY at a spread from mid
    - If long: place limit SELL at profit target from avg entry
    - If stop loss threshold breached: trigger market sell override (handled by execution loop)
    """

    def __init__(
        self,
        product_id: str,
        cfg: dict,
        in_q: asyncio.Queue,
        out_q: asyncio.Queue,
        position: PositionState,
    ):
        self.product_id = product_id
        self.cfg = cfg or {}
        self.in_q = in_q
        self.out_q = out_q
        self.position = position
        self.state = ScalperState()

    async def run(self) -> None:
        while True:
            evt = await self.in_q.get()
            if not isinstance(evt, MarketDataEvent):
                continue
            if evt.mid_price is None:
                continue

            self.position.last_mid_price = float(evt.mid_price)
            intent = self._make_intent(evt)
            if intent is None:
                continue

            if self._should_emit(intent):
                await self.out_q.put(intent)
                self.state.last_emit_ts_ms = intent.ts_ms
                if intent.bid_price is not None and intent.ask_price is not None:
                    self.state.last_bid_ask = (float(intent.bid_price), float(intent.ask_price))

    def _make_intent(self, md: MarketDataEvent) -> Optional[OrderIntentEvent]:
        mid = float(md.mid_price or 0.0)
        if mid <= 0:
            return None

        half_spread_bps = float(self.cfg["half_spread_bps"])
        base_order_size = float(self.cfg["base_order_size"])
        profit_target_pct = float(self.cfg.get("profit_target_pct", 0.0) or 0.0)
        stop_loss_pct = float(self.cfg.get("stop_loss_pct", 0.0) or 0.0)

        inv = float(self.position.inventory_base)
        avg_entry = float(self.position.avg_entry_price or 0.0)

        # Flat: place a bid only
        if inv <= 0:
            bid_px = round(mid * (1.0 - half_spread_bps / 10_000.0), 2)
            return OrderIntentEvent(
                type=EventType.ORDER_INTENT,
                ts_ms=int(time.time() * 1000),
                product_id=self.product_id,
                bid_price=float(bid_px),
                bid_size=float(base_order_size),
                ask_price=None,
                ask_size=0.0,
                trigger_market_stop_loss=False,
                reason="scalp_entry",
                meta={"mid_price": mid, "inventory_base": inv},
            )

        # In position: do not buy more; set a static profit-target sell.
        ask_px = None
        if avg_entry > 0 and profit_target_pct > 0:
            ask_px = round(avg_entry * (1.0 + profit_target_pct), 2)

        trigger_sl = False
        if avg_entry > 0 and stop_loss_pct > 0:
            if mid <= avg_entry * (1.0 - stop_loss_pct):
                trigger_sl = True

        return OrderIntentEvent(
            type=EventType.ORDER_INTENT,
            ts_ms=int(time.time() * 1000),
            product_id=self.product_id,
            bid_price=None,
            bid_size=0.0,
            ask_price=(float(ask_px) if ask_px is not None else None),
            ask_size=float(inv),
            trigger_market_stop_loss=bool(trigger_sl),
            reason="scalp_manage",
            meta={
                "mid_price": mid,
                "inventory_base": inv,
                "avg_entry_price": avg_entry,
                "profit_target_pct": profit_target_pct,
                "stop_loss_pct": stop_loss_pct,
            },
        )

    def _should_emit(self, intent: OrderIntentEvent) -> bool:
        now_ms = intent.ts_ms
        throttling = self.cfg.get("throttling", {}) or {}
        min_interval_ms = int(throttling.get("min_interval_ms", 1000))
        move_bps = float(throttling.get("price_move_bps", 1.0))

        if self.state.last_bid_ask is None:
            return True
        if now_ms - int(self.state.last_emit_ts_ms or 0) >= min_interval_ms:
            return True

        # Stop-loss intents should pass quickly.
        if getattr(intent, "trigger_market_stop_loss", False):
            return True

        # Only compare when both bid+ask are present.
        if intent.bid_price is None or intent.ask_price is None:
            return False
        prev_bid, prev_ask = self.state.last_bid_ask
        new_bid = float(intent.bid_price)
        new_ask = float(intent.ask_price)

        def bps(a: float, b: float) -> float:
            if b == 0:
                return 0.0
            return abs(a - b) / b * 10_000.0

        return bps(new_bid, prev_bid) >= move_bps or bps(new_ask, prev_ask) >= move_bps


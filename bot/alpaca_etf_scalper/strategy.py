from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Optional

from mm_bot.core.events import EventType, MarketDataEvent, OrderIntentEvent
from mm_bot.strategy.market_maker import PositionState


@dataclass
class ScalperState:
    last_emit_ts_ms: int = 0
    # Once price crosses stop, latch so we emit trigger_market_stop_loss only once until
    # price recovers above a clear band (avoids flooding the intent queue every tick).
    sl_latched: bool = False


class SimpleScalper:
    """
    Equity scalper:
    - Flat: limit buy at spread
    - Long: limit sell at profit target
    - Stop loss: set trigger_market_stop_loss for market sell override
    """

    def __init__(self, symbol: str, cfg: dict, in_q: asyncio.Queue, out_q: asyncio.Queue, position: PositionState):
        self.symbol = symbol
        self.cfg = cfg
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
            intent = self._intent(evt)
            if intent is None:
                continue
            if self._should_emit(intent):
                await self.out_q.put(intent)
                self.state.last_emit_ts_ms = intent.ts_ms

    def _intent(self, md: MarketDataEvent) -> Optional[OrderIntentEvent]:
        mid = float(md.mid_price or 0.0)
        if mid <= 0:
            return None
        half_spread_bps = float(self.cfg["half_spread_bps"])
        qty = float(self.cfg["qty"])
        profit_target_pct = float(self.cfg["profit_target_pct"])
        stop_loss_pct = float(self.cfg["stop_loss_pct"])

        inv = float(self.position.inventory_base)
        avg_entry = float(self.position.avg_entry_price or 0.0)

        if inv <= 0:
            self.state.sl_latched = False
            bid_px = round(mid * (1.0 - half_spread_bps / 10_000.0), 2)
            return OrderIntentEvent(
                type=EventType.ORDER_INTENT,
                ts_ms=int(time.time() * 1000),
                product_id=self.symbol,
                bid_price=float(bid_px),
                bid_size=float(qty),
                ask_price=None,
                ask_size=0.0,
                trigger_market_stop_loss=False,
                reason="entry_buy",
                meta={"mid_price": mid, "inventory_base": inv},
            )

        ask_px = round(avg_entry * (1.0 + profit_target_pct), 2) if avg_entry > 0 else None
        trigger_sl = False
        if avg_entry > 0:
            stop_line = avg_entry * (1.0 - stop_loss_pct)
            # Hysteresis: require recovery slightly above the stop line before arming again.
            clear_bps = float((self.cfg.get("stop_loss") or {}).get("clear_bps", 15.0) or 15.0)
            clear_line = stop_line * (1.0 + clear_bps / 10_000.0)
            if mid <= stop_line:
                if not self.state.sl_latched:
                    self.state.sl_latched = True
                    trigger_sl = True
            elif mid >= clear_line:
                self.state.sl_latched = False

        return OrderIntentEvent(
            type=EventType.ORDER_INTENT,
            ts_ms=int(time.time() * 1000),
            product_id=self.symbol,
            bid_price=None,
            bid_size=0.0,
            ask_price=(float(ask_px) if ask_px is not None else None),
            ask_size=float(inv),
            trigger_market_stop_loss=bool(trigger_sl),
            reason="manage_position",
            meta={"mid_price": mid, "inventory_base": inv, "avg_entry_price": avg_entry},
        )

    def _should_emit(self, intent: OrderIntentEvent) -> bool:
        min_interval_ms = 1000
        throttling = self.cfg.get("throttling")
        if isinstance(throttling, dict) and throttling.get("min_interval_ms") is not None:
            min_interval_ms = int(throttling["min_interval_ms"])
        # Stop-loss is edge-triggered in _intent (sl_latched); still respect throttle if combined.
        if intent.trigger_market_stop_loss:
            return (intent.ts_ms - int(self.state.last_emit_ts_ms or 0)) >= min_interval_ms
        return (intent.ts_ms - int(self.state.last_emit_ts_ms or 0)) >= min_interval_ms


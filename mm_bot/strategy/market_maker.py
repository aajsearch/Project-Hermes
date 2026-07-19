from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Optional, Tuple

from mm_bot.core.events import EventType, MarketDataEvent, OrderIntentEvent


@dataclass
class StrategyState:
    # Placeholder inventory (will later be updated by fills/position events)
    base_position: float = 0.0

    last_emit_ts_ms: int = 0
    last_bid_ask: Optional[Tuple[float, float]] = None


@dataclass
class PositionState:
    """Shared in-memory position state (updated by fill processor)."""
    inventory_base: float = 0.0
    last_mid_price: Optional[float] = None
    avg_entry_price: float = 0.0


class SimpleMarketMaker:
    """
    Exactly 1 bid + 1 ask around mid.
    - Baseline half-spread (bps)
    - Widen on volatility spikes
    - Skew on inventory
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
        self.cfg = cfg
        self.in_q = in_q
        self.out_q = out_q
        self.position = position
        self.state = StrategyState()

    async def run(self) -> None:
        """
        Event loop:
        - Await MarketDataEvent from md_q
        - Compute bid/ask intent
        - Throttle emission (avoid spamming on microticks)
        - Push OrderIntentEvent to intent_q
        """
        while True:
            evt = await self.in_q.get()
            if not isinstance(evt, MarketDataEvent):
                continue

            mid = evt.mid_price
            if mid is None:
                continue
            # Share latest mid for reporting/PnL estimation.
            self.position.last_mid_price = float(mid)

            intent = self._quote(evt)
            if intent is None:
                continue

            if self._should_emit(intent):
                await self.out_q.put(intent)
                self.state.last_emit_ts_ms = intent.ts_ms
                if intent.bid_price is not None and intent.ask_price is not None:
                    self.state.last_bid_ask = (float(intent.bid_price), float(intent.ask_price))

    def _quote(self, md: MarketDataEvent) -> Optional[OrderIntentEvent]:
        mid = md.mid_price
        vol = md.rolling_volatility

        half_spread_bps = float(self.cfg["half_spread_bps"])
        vw = self.cfg.get("vol_widening", {}) or {}
        if vw.get("enabled", True) and vol is not None:
            thr = float(vw.get("vol_threshold", 0))
            if vol > thr and thr > 0:
                # Linear widening: at vol=thr => mult=1; at vol=2*thr => mult=2; cap at max_multiplier
                mult = min(float(vw.get("max_multiplier", 1.0)), max(1.0, vol / thr))
                half_spread_bps *= mult

        # Inventory comes from shared state (updated by fills processing loop)
        pos = float(self.position.inventory_base)
        inv_cfg = self.cfg.get("inventory", {}) or {}
        target = float(inv_cfg.get("target_base_position", 0.0))
        skew_bps_per_unit = float(inv_cfg.get("skew_bps_per_unit", 0.0))
        size_tilt_per_unit = float(inv_cfg.get("size_tilt_per_unit", 0.0))
        delta = pos - target
        skew_bps = delta * skew_bps_per_unit

        bid_px = float(mid) * (1.0 - half_spread_bps / 10_000.0)
        ask_px = float(mid) * (1.0 + half_spread_bps / 10_000.0)

        # Inventory skew: shift both quotes up/down (long -> down, short -> up)
        skew_mult = 1.0 - (skew_bps / 10_000.0)
        bid_px *= skew_mult
        ask_px *= skew_mult

        # Tick size rounding (fiat/USDC for now): 2 decimals
        bid_px = round(bid_px, 2)
        ask_px = round(ask_px, 2)

        base_size = float(self.cfg["base_order_size"])
        # Size tilt: long -> reduce bid size, increase ask size; short -> opposite.
        tilt = max(-0.5, min(0.5, delta * size_tilt_per_unit))
        bid_sz = max(0.0001, float(base_size) * (1.0 - tilt))
        ask_sz = max(0.0001, float(base_size) * (1.0 + tilt))

        # Inventory-aware selling:
        # - Never place asks when we have no inventory
        # - If we have less than the desired ask size, clamp to available inventory
        #   (avoids "no ask" due to tiny float/tilt differences, and supports unwind behavior).
        if pos <= 0:
            ask_px = None
            ask_sz = 0.0
        else:
            ask_sz = min(float(ask_sz), float(pos))

        return OrderIntentEvent(
            type=EventType.ORDER_INTENT,
            ts_ms=int(time.time() * 1000),
            product_id=self.product_id,
            bid_price=float(bid_px),
            bid_size=float(bid_sz),
            ask_price=(float(ask_px) if ask_px is not None else None),
            ask_size=float(ask_sz),
            reason="quote_update",
            meta={
                "mid_price": mid,
                "rolling_volatility": vol,
                "half_spread_bps": half_spread_bps,
                "inventory_base": pos,
                "target_base_position": target,
                "skew_bps": skew_bps,
                "size_tilt": tilt,
            },
        )

    def _should_emit(self, intent: OrderIntentEvent) -> bool:
        """
        Throttling:
        - emit if (bid/ask moved by >= threshold bps) OR (min interval passed)
        """
        now_ms = intent.ts_ms
        throttling = self.cfg.get("throttling", {}) or {}
        # Default to 1s to keep mock dry runs readable.
        min_interval_ms = int(throttling.get("min_interval_ms", 1000))
        move_bps = float(throttling.get("price_move_bps", 1.0))

        # Always emit first quote
        if self.state.last_bid_ask is None:
            return True

        # Time-based emit
        if now_ms - int(self.state.last_emit_ts_ms or 0) >= min_interval_ms:
            return True

        if intent.bid_price is None or intent.ask_price is None:
            return False
        prev_bid, prev_ask = self.state.last_bid_ask
        new_bid = float(intent.bid_price)
        new_ask = float(intent.ask_price)

        # bps move computed relative to previous prices
        def bps(a: float, b: float) -> float:
            if b == 0:
                return 0.0
            return abs(a - b) / b * 10_000.0

        if bps(new_bid, prev_bid) >= move_bps or bps(new_ask, prev_ask) >= move_bps:
            return True
        return False


from __future__ import annotations

import asyncio
import time
from typing import Optional

from mm_bot.core.events import EventType, OrderIntentEvent, RiskEvent


class RiskEngine:
    """
    Risk engine for quoting intents.

    Enforces:
    - max_position_base: blocks increasing inventory beyond limit (blocks bid, allows ask)
    - max_notional_usd: blocks bids when current inventory notionally exceeds limit

    Stop loss and daily loss kill-switch are enforced when shared state is provided.
    """

    def __init__(
        self,
        cfg: dict,
        in_q: asyncio.Queue,
        out_q: asyncio.Queue,
        *,
        position,
        pnl_state,
        shutdown,
    ):
        self.cfg = cfg or {}
        self.in_q = in_q
        self.out_q = out_q
        self.position = position
        self.pnl_state = pnl_state
        self.shutdown = shutdown

    async def run(self) -> None:
        while True:
            evt = await self.in_q.get()
            if isinstance(evt, OrderIntentEvent):
                await self.out_q.put(self._evaluate(evt))

    def _evaluate(self, intent: OrderIntentEvent) -> RiskEvent:
        ts_ms = int(time.time() * 1000)
        kill_enabled = bool((self.cfg.get("kill_switch", {}) or {}).get("enabled", True))
        daily_loss_limit = float(self.cfg.get("daily_loss_limit_usd", 0) or 0)
        stop_loss_pct = float(self.cfg.get("stop_loss_pct", 0) or 0)
        unwind_only = bool(self.cfg.get("unwind_only", False))
        unwind_exit_inventory_base = self.cfg.get("unwind_exit_inventory_base", None)

        max_pos = float(self.cfg.get("max_position_base", 0) or 0)
        max_notional = float(self.cfg.get("max_notional_usd", 0) or 0)

        inv = float(intent.inventory_base)
        mid = intent.mid_price
        bid_sz = float(intent.bid_size or 0.0)
        ask_sz = float(intent.ask_size or 0.0)

        # If unwind-only is enabled, auto-exit when inventory is (near) flat.
        # This avoids getting stuck due to dust balances.
        if unwind_only:
            if unwind_exit_inventory_base is None:
                # Conservative defaults by product type.
                pid = str(intent.product_id or "").upper()
                eps = 1e-6 if pid.startswith("BTC-") else 1e-4
            else:
                try:
                    eps = float(unwind_exit_inventory_base)
                except (TypeError, ValueError):
                    eps = 0.0
            if eps > 0 and abs(float(getattr(self.position, "inventory_base", inv) or inv)) <= eps:
                unwind_only = False

        allow_bid = True
        allow_ask = True
        reasons = []

        # 1) Daily loss kill switch
        # In unwind_only mode, we *don't* shut down. We block bids and allow asks so the bot can unwind.
        realized = float(getattr(self.pnl_state, "realized_pnl", 0.0) or 0.0)
        if kill_enabled and daily_loss_limit > 0 and realized <= -daily_loss_limit:
            if unwind_only:
                return RiskEvent(
                    type=EventType.RISK,
                    ts_ms=ts_ms,
                    ok=False,
                    reason="daily_loss_limit_hit_unwind_only",
                    details={
                        "product_id": intent.product_id,
                        "allow_bid": False,
                        "allow_ask": True,
                        "realized_pnl": realized,
                        "daily_loss_limit_usd": daily_loss_limit,
                    },
                )
            # Default behavior: block all and trigger shutdown.
            try:
                self.shutdown.trigger()
            except Exception:
                pass
            return RiskEvent(
                type=EventType.RISK,
                ts_ms=ts_ms,
                ok=False,
                reason="daily_loss_limit_hit",
                details={
                    "product_id": intent.product_id,
                    "allow_bid": False,
                    "allow_ask": False,
                    "realized_pnl": realized,
                    "daily_loss_limit_usd": daily_loss_limit,
                },
            )

        # Unwind-only mode: never place bids. Allow asks to unwind.
        if unwind_only:
            return RiskEvent(
                type=EventType.RISK,
                ts_ms=ts_ms,
                ok=True,
                reason="unwind_only",
                details={
                    "product_id": intent.product_id,
                    "allow_bid": False,
                    "allow_ask": True,
                    "inventory_base": inv,
                    "mid_price": mid,
                },
            )

        # 2) Stop loss: if holding inventory and drawdown exceeds threshold, block BID (allow ASK to unwind)
        avg_entry = float(getattr(self.position, "avg_entry_price", 0.0) or 0.0)
        inv_live = float(getattr(self.position, "inventory_base", inv) or inv)
        if stop_loss_pct > 0 and inv_live > 0 and mid is not None and avg_entry > 0:
            dd = (float(mid) - avg_entry) / avg_entry
            if dd <= -stop_loss_pct:
                allow_bid = False
                reasons.append("stop_loss_pct")
                return RiskEvent(
                    type=EventType.RISK,
                    ts_ms=ts_ms,
                    ok=False,
                    reason="stop_loss_triggered",
                    details={
                        "product_id": intent.product_id,
                        "allow_bid": False,
                        "allow_ask": True,
                        "inventory_base": inv_live,
                        "mid_price": float(mid),
                        "avg_entry_price": avg_entry,
                        "drawdown_pct": dd,
                        "stop_loss_pct": stop_loss_pct,
                    },
                )

        # Position limit: block bids that would increase inventory beyond limit.
        if max_pos > 0 and bid_sz > 0 and (inv + bid_sz) > max_pos:
            allow_bid = False
            reasons.append("max_position_base")

        # Notional limit: if current position notional exceeds limit, block bids (allow asks to reduce exposure).
        if max_notional > 0 and mid is not None:
            notional = abs(inv * float(mid))
            if notional > max_notional:
                allow_bid = False
                reasons.append("max_notional_usd")

        ok = allow_bid or allow_ask
        if not ok:
            return RiskEvent(
                type=EventType.RISK,
                ts_ms=ts_ms,
                ok=False,
                reason="blocked_all",
                details={
                    "product_id": intent.product_id,
                    "allow_bid": False,
                    "allow_ask": False,
                    "reasons": reasons,
                    "inventory_base": inv,
                    "mid_price": mid,
                },
            )

        if not allow_bid:
            return RiskEvent(
                type=EventType.RISK,
                ts_ms=ts_ms,
                ok=True,
                reason="bid_blocked_allow_ask",
                details={
                    "product_id": intent.product_id,
                    "allow_bid": False,
                    "allow_ask": True,
                    "reasons": reasons,
                    "inventory_base": inv,
                    "mid_price": mid,
                },
            )

        return RiskEvent(
            type=EventType.RISK,
            ts_ms=ts_ms,
            ok=True,
            reason="approved",
            details={"product_id": intent.product_id, "allow_bid": True, "allow_ask": True, "inventory_base": inv, "mid_price": mid},
        )


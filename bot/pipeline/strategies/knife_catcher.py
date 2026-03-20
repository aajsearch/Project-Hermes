"""
Knife Catcher: HFT volatility sniper that sits idle until a flash crash, buys the bottom
of the wick, and holds up to max_hold_seconds for a V-shape recovery.
Uses an in-memory sliding window (deque) for bid history to avoid DB queries on every tick.
"""
from __future__ import annotations

import logging
import time
import uuid
from collections import deque
from typing import Any, Dict, List, Optional

from bot.pipeline.context import WindowContext
from bot.pipeline.intents import ExitAction, OrderIntent, OrderRecord
from bot.pipeline.strategies.base import BaseV2Strategy

logger = logging.getLogger(__name__)


def _get_asset_config(value: Any, asset: str, default: Any) -> Any:
    """Resolve scalar or per-asset dict config."""
    if value is None:
        return default
    if isinstance(value, dict):
        a = (asset or "").strip().lower()
        return value.get(a, value.get(a.upper(), default))
    return value


def _min_spot_distance(ctx: WindowContext) -> Optional[float]:
    """Spot-to-strike distance from the single oracle."""
    return ctx.distance


def _spot_for_signed_distance(ctx: WindowContext) -> Optional[float]:
    """Single spot value for signed-distance (from sole oracle)."""
    return ctx.spot


def _signed_distance(ctx: WindowContext, side: str) -> Optional[float]:
    """
    Signed distance for Death Floor: for YES, spot - strike; for NO, strike - spot.
    Negative means spot has crossed the strike (contract more likely dead).
    Returns None if spot or strike is missing.
    """
    k_spot = _spot_for_signed_distance(ctx)
    strike = ctx.strike
    if k_spot is None or strike is None:
        return None
    if side == "yes":
        return k_spot - strike
    return strike - k_spot


class KnifeCatcherStrategy(BaseV2Strategy):
    """
    Flash-crash sniper: maintain strict dual sliding windows (YES and NO independently).
    Enter when that side's bid has recently been high (historic_high_bid), is now in the
    panic zone (min_entry_cents..max_entry_cents), and distance has pierced the floor.
    Single bullet per window/asset. Exit on take-profit, stop-loss, or time stop via market-sell.
    """

    STRATEGY_ID = "knife_catcher"

    def __init__(self, config: dict) -> None:
        super().__init__(self.STRATEGY_ID, config)
        # key: f"{market_id}_{asset}", value: deque(maxlen=lookback_seconds), created lazily
        self.yes_history: Dict[str, deque] = {}
        self.no_history: Dict[str, deque] = {}
        # key: f"{market_id}_{asset}_yes" | f"{market_id}_{asset}_no" -> first tick when crash conditions met (for execution_delay_seconds)
        self.crash_detected_at: Dict[str, float] = {}

    def _get_yes_deque(self, ctx: WindowContext, asset: str, lookback_seconds: int) -> deque:
        key = f"{ctx.market_id}_{asset}"
        if key not in self.yes_history:
            self.yes_history[key] = deque(maxlen=max(1, lookback_seconds))
        return self.yes_history[key]

    def _get_no_deque(self, ctx: WindowContext, asset: str, lookback_seconds: int) -> deque:
        key = f"{ctx.market_id}_{asset}"
        if key not in self.no_history:
            self.no_history[key] = deque(maxlen=max(1, lookback_seconds))
        return self.no_history[key]

    def evaluate_entry(
        self, ctx: WindowContext, my_orders: Optional[List[OrderRecord]] = None
    ) -> Optional[OrderIntent]:
        cfg = self._get_strategy_config(ctx)
        if not cfg or not cfg.get("enabled", False):
            return None

        asset = (ctx.asset or "").strip().lower()
        # Single bullet: at most one order per (window, asset).
        if my_orders and len(my_orders) > 0:
            return None

        yes_bid = int(ctx.quote.get("yes_bid", 0) or 0)
        no_bid = int(ctx.quote.get("no_bid", 0) or 0)
        lookback = int(cfg.get("lookback_seconds", 60))
        yes_dq = self._get_yes_deque(ctx, asset, lookback)
        no_dq = self._get_no_deque(ctx, asset, lookback)
        yes_dq.append(yes_bid)
        no_dq.append(no_bid)

        historic_high = int(cfg.get("historic_high_bid", 80))
        min_entry = int(cfg.get("min_entry_cents", 20))
        max_entry = int(cfg.get("max_entry_cents", 45))
        min_dist_placement = _get_asset_config(cfg.get("min_distance_at_placement"), asset, None)
        if min_dist_placement is None:
            return None
        try:
            threshold = float(min_dist_placement)
        except (TypeError, ValueError):
            return None
        distance_min = _min_spot_distance(ctx)
        if distance_min is None or distance_min >= threshold:
            return None

        def _yes_conditions_met() -> bool:
            return (
                len(yes_dq) > 5
                and max(yes_dq) >= historic_high
                and min_entry <= yes_bid <= max_entry
            )

        def _no_conditions_met() -> bool:
            return (
                len(no_dq) > 5
                and max(no_dq) >= historic_high
                and min_entry <= no_bid <= max_entry
            )

        base_key = f"{ctx.market_id}_{asset}"
        key_yes = f"{base_key}_yes"
        key_no = f"{base_key}_no"
        current_time = time.time()
        execution_delay_seconds = float(cfg.get("execution_delay_seconds", 3))
        max_entry_spread_cents = int(cfg.get("max_entry_spread_cents", 5))
        max_cost_cents = int(cfg.get("max_cost_cents", 500))
        raw_count = _get_asset_config(cfg.get("order_count"), asset, 5)
        try:
            desired_count = int(raw_count) if raw_count is not None else 5
        except (TypeError, ValueError):
            desired_count = 5

        # Death Floor: max_negative_distance_at_placement[asset]; if signed_distance < threshold, do not fire.
        death_floor_raw = _get_asset_config(cfg.get("max_negative_distance_at_placement"), asset, None)
        try:
            death_floor_threshold = float(death_floor_raw) if death_floor_raw is not None else None
        except (TypeError, ValueError):
            death_floor_threshold = None

        if not _yes_conditions_met():
            self.crash_detected_at.pop(key_yes, None)
        else:
            if key_yes not in self.crash_detected_at:
                self.crash_detected_at[key_yes] = current_time
            if current_time - self.crash_detected_at[key_yes] >= execution_delay_seconds:
                signed_distance = _signed_distance(ctx, "yes")
                if death_floor_threshold is not None and signed_distance is not None:
                    if signed_distance < death_floor_threshold:
                        logger.info(
                            "[knife_catcher] Death Floor YES: asset=%s signed_distance=%.4f < %.4f (skip)",
                            asset, signed_distance, death_floor_threshold,
                        )
                        return None
                calculated_limit = min(99, yes_bid + max_entry_spread_cents)
                max_by_cost = max_cost_cents // calculated_limit if calculated_limit else 0
                count = max(1, min(desired_count, max_by_cost)) if max_by_cost else max(1, desired_count)
                client_order_id = f"knife_catcher:{uuid.uuid4().hex[:12]}"
                self.crash_detected_at.pop(key_yes, None)
                logger.info(
                    "[knife_catcher] Entry YES: asset=%s yes_bid=%s limit=%s max_yes_history=%s distance_min=%s (sustained %.1fs)",
                    asset, yes_bid, calculated_limit, max(yes_dq), distance_min, execution_delay_seconds,
                )
                return OrderIntent(
                    side="yes",
                    price_cents=calculated_limit,
                    count=count,
                    order_type="limit",
                    client_order_id=client_order_id,
                    placement_bid_cents=yes_bid,
                )

        if not _no_conditions_met():
            self.crash_detected_at.pop(key_no, None)
        else:
            if key_no not in self.crash_detected_at:
                self.crash_detected_at[key_no] = current_time
            if current_time - self.crash_detected_at[key_no] >= execution_delay_seconds:
                signed_distance = _signed_distance(ctx, "no")
                if death_floor_threshold is not None and signed_distance is not None:
                    if signed_distance < death_floor_threshold:
                        logger.info(
                            "[knife_catcher] Death Floor NO: asset=%s signed_distance=%.4f < %.4f (skip)",
                            asset, signed_distance, death_floor_threshold,
                        )
                        return None
                calculated_limit = min(99, no_bid + max_entry_spread_cents)
                max_by_cost = max_cost_cents // calculated_limit if calculated_limit else 0
                count = max(1, min(desired_count, max_by_cost)) if max_by_cost else max(1, desired_count)
                client_order_id = f"knife_catcher:{uuid.uuid4().hex[:12]}"
                self.crash_detected_at.pop(key_no, None)
                logger.info(
                    "[knife_catcher] Entry NO: asset=%s no_bid=%s limit=%s max_no_history=%s distance_min=%s (sustained %.1fs)",
                    asset, no_bid, calculated_limit, max(no_dq), distance_min, execution_delay_seconds,
                )
                return OrderIntent(
                    side="no",
                    price_cents=calculated_limit,
                    count=count,
                    order_type="limit",
                    client_order_id=client_order_id,
                    placement_bid_cents=no_bid,
                )
        return None

    def evaluate_exit(self, ctx: WindowContext, my_orders: List[OrderRecord]) -> List[ExitAction]:
        cfg = self._get_strategy_config(ctx)
        if not cfg:
            return []

        take_profit_cents = int(cfg.get("take_profit_cents", 80))
        stop_loss_cents = int(cfg.get("stop_loss_cents", 10))
        max_hold_seconds = int(cfg.get("max_hold_seconds", 60))
        current_time = time.time()

        actions: List[ExitAction] = []
        for order in my_orders:
            if order.status != "filled" and (order.filled_count or 0) <= 0:
                continue
            side = (order.side or "no").strip().lower()
            if side not in ("yes", "no"):
                continue
            current_bid = ctx.yes_bid if side == "yes" else ctx.no_bid

            if current_bid >= take_profit_cents:
                actions.append(ExitAction(order_id=order.order_id, action="market_sell"))
                logger.info(
                    "[knife_catcher] Take profit: order_id=%s current_bid=%s >= %s",
                    order.order_id, current_bid, take_profit_cents,
                )
                continue
            if current_bid <= stop_loss_cents:
                actions.append(ExitAction(order_id=order.order_id, action="market_sell"))
                logger.info(
                    "[knife_catcher] Stop loss: order_id=%s current_bid=%s <= %s",
                    order.order_id, current_bid, stop_loss_cents,
                )
                continue
            hold_s = current_time - order.placed_at
            if hold_s >= max_hold_seconds:
                actions.append(ExitAction(order_id=order.order_id, action="market_sell"))
                logger.info(
                    "[knife_catcher] Time stop: order_id=%s hold_s=%.1f >= %s",
                    order.order_id, hold_s, max_hold_seconds,
                )
        return actions

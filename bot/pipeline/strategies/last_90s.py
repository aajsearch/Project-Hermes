"""
V2 port of last_90s_limit_99: limit order in final window_seconds, time-weighted bid floor, stop-loss exit.
Stateless: evaluates entry/exit from WindowContext and my_orders only.
Records telemetry (skips and intent_fired) to v2_telemetry_last_90s in v2_state.db.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from bot.pipeline.context import WindowContext
from bot.pipeline.intents import ExitAction, OrderIntent, OrderRecord
from bot.pipeline.strategies.base import BaseV2Strategy

logger = logging.getLogger(__name__)

TELEMETRY_TABLE = "v2_telemetry_last_90s"


def _v2_db_path() -> Path:
    return Path(__file__).resolve().parents[2].parent / "data" / "v2_state.db"


def _ensure_telemetry_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TELEMETRY_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            window_id TEXT,
            asset TEXT,
            placed INTEGER,
            seconds_to_close REAL,
            bid INTEGER,
            distance REAL,
            reason TEXT,
            pre_data TEXT,
            timestamp REAL
        )
        """
    )
    conn.commit()


def _get_asset_config(value: Any, asset: str, default: Any) -> Any:
    """Resolve scalar or per-asset dict config."""
    if value is None:
        return default
    if isinstance(value, dict):
        a = (asset or "").strip().lower()
        return value.get(a, value.get(a.upper(), default))
    return value


def _min_spot_distance(ctx: WindowContext) -> Optional[float]:
    """
    Compute the minimum spot-to-strike distance using both oracles when available.
    Falls back to ctx.distance when only one source exists.
    """
    candidates = [d for d in (ctx.distance_kraken, ctx.distance_coinbase) if d is not None]
    if candidates:
        return min(candidates)
    return ctx.distance


def _resolve_strategy_id(config: dict) -> str:
    """Use continuous_alpha_limit_99 if that block exists in any interval, else last_90s_limit_99."""
    for interval in ("fifteen_min", "hourly"):
        strategies = (config.get(interval) or {}).get("strategies") or {}
        if strategies.get("continuous_alpha_limit_99") is not None:
            return "continuous_alpha_limit_99"
    return "last_90s_limit_99"


class Last90sStrategy(BaseV2Strategy):
    """
    Limit-at-99 strategy: places a limit order when seconds_to_close <= window_seconds and
    distance/bid pass; exits with stop_loss using absolute distance buffer (entry_distance - buffer).
    Supports both config keys: continuous_alpha_limit_99 (300s horizon) and last_90s_limit_99.
    """

    def __init__(self, config: dict) -> None:
        strategy_id = _resolve_strategy_id(config)
        super().__init__(strategy_id, config)
        path = _v2_db_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path), check_same_thread=False)
        try:
            _ensure_telemetry_table(conn)
        finally:
            conn.close()

    def _get_strategy_config(self, ctx: WindowContext) -> dict:
        """Try continuous_alpha_limit_99 then last_90s_limit_99 so either YAML key works."""
        if not ctx.config or not ctx.interval:
            return {}
        interval_block = ctx.config.get(ctx.interval)
        if not isinstance(interval_block, dict):
            return {}
        strategies = interval_block.get("strategies")
        if not isinstance(strategies, dict):
            return {}
        out = strategies.get("continuous_alpha_limit_99") or strategies.get("last_90s_limit_99")
        return out if isinstance(out, dict) else {}

    def _record_telemetry(self, ctx: WindowContext, placed: int, reason: str) -> None:
        """
        Write one row to v2_telemetry_last_90s. All values from ctx only; no hardcoded 50.0, 45.0, or 97.
        pre_data = JSON of ctx.quote and ctx.spot_kraken.
        """
        pre_data = json.dumps({
            "quote": dict(ctx.quote),
            "spot_kraken": ctx.spot_kraken,
        })
        window_id = f"{ctx.interval}_{ctx.market_id}"
        asset_str = (ctx.asset or "").strip().lower()
        # Dynamic from ctx only; use -1.0 sentinel when missing (never 50.0, 45.0, or 97)
        seconds_to_close_raw = ctx.seconds_to_close
        seconds_to_close = float(seconds_to_close_raw) if seconds_to_close_raw is not None else -1.0
        # Bid from ctx.quote by configured side (yes|no|auto) for telemetry.
        strat_cfg = self._get_strategy_config(ctx) or {}
        side_cfg = (str(strat_cfg.get("side", "no")).strip().lower() or "no")
        yes_bid_q = int(ctx.quote.get("yes_bid", 0) or 0)
        no_bid_q = int(ctx.quote.get("no_bid", 0) or 0)
        if side_cfg == "yes":
            side = "yes"
            bid = yes_bid_q
        elif side_cfg == "no":
            side = "no"
            bid = no_bid_q
        else:
            # side=auto: log whichever side currently has the higher bid.
            side = "yes" if yes_bid_q >= no_bid_q else "no"
            bid = yes_bid_q if side == "yes" else no_bid_q
        distance_raw = ctx.distance
        distance = float(distance_raw) if distance_raw is not None else -1.0
        path = _v2_db_path()
        try:
            conn = sqlite3.connect(str(path), check_same_thread=False)
            try:
                conn.execute(
                    f"""
                    INSERT INTO {TELEMETRY_TABLE}
                    (window_id, asset, placed, seconds_to_close, bid, distance, reason, pre_data, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        window_id,
                        asset_str,
                        placed,
                        seconds_to_close,
                        bid,
                        distance,
                        reason,
                        pre_data,
                        time.time(),
                    ),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as e:
            logger.debug("[last_90s] Telemetry write failed: %s", e)

    def _resolve_entry_side(self, cfg: Dict[str, Any], ctx: WindowContext) -> tuple[Optional[str], int]:
        """
        Resolve entry side and bid for this tick.
        - side=yes|no: fixed side; require that side's bid >= min_bid_cents.
        - side=auto: pick the side with the higher bid (yes if yes_bid >= no_bid, else no),
          then require that bid >= min_bid_cents.
        Returns (side or None, bid_cents_for_that_side).
        min_bid_cents: scalar or per-asset dict (e.g. { btc: 85, eth: 80, ... }).
        """
        side_cfg = (str(cfg.get("side", "no")).strip().lower() or "no")
        asset = (ctx.asset or "").strip().lower()
        min_bid_raw = _get_asset_config(cfg.get("min_bid_cents"), asset, 90)
        min_bid_cents = int(min_bid_raw) if min_bid_raw is not None else 90
        yes_bid = int(ctx.quote.get("yes_bid", 0) or 0)
        no_bid = int(ctx.quote.get("no_bid", 0) or 0)

        if side_cfg == "yes":
            side = "yes"
            bid = yes_bid
        elif side_cfg == "no":
            side = "no"
            bid = no_bid
        else:
            # side=auto: choose side with higher bid.
            side = "yes" if yes_bid >= no_bid else "no"
            bid = yes_bid if side == "yes" else no_bid

        # Log the choice so we can debug behavior end-to-end.
        logger.info(
            "[last_90s_v2_side] [%s] sec_to_close=%.0f side_cfg=%s yes_bid=%s no_bid=%s chosen=%s bid=%s min_bid_cents=%s",
            (ctx.asset or "").upper(),
            ctx.seconds_to_close or -1,
            side_cfg,
            yes_bid,
            no_bid,
            side,
            bid,
            min_bid_cents,
        )

        if bid < min_bid_cents:
            return (None, bid)
        return (side, bid)

    def evaluate_entry(
        self, ctx: WindowContext, my_orders: Optional[List[OrderRecord]] = None
    ) -> Optional[OrderIntent]:
        cfg = self._get_strategy_config(ctx)
        if not cfg or not cfg.get("enabled", False):
            return None

        asset = (ctx.asset or "").strip().lower()
        window_seconds = float(_get_asset_config(cfg.get("window_seconds"), asset, 90))
        if ctx.seconds_to_close is None or ctx.seconds_to_close > window_seconds:
            return None

        # From here on we are in the active hunting phase; record telemetry on any skip or intent.
        min_dist = _get_asset_config(cfg.get("min_distance_at_placement"), asset, 0.0)
        if min_dist is not None and ctx.distance is not None:
            try:
                min_d = float(min_dist)
                if min_d > 0 and ctx.distance < min_d:
                    self._record_telemetry(ctx, 0, "low_distance")
                    return None
            except (TypeError, ValueError):
                pass

        # Side (yes/no/auto) and min_bid gate: place only if chosen side's bid >= min_bid_cents.
        side, bid_cents = self._resolve_entry_side(cfg, ctx)
        if side is None:
            self._record_telemetry(ctx, 0, "low_bid")
            return None

        limit_price_cents = int(cfg.get("limit_price_cents", 99))
        max_cost_cents = int(cfg.get("max_cost_cents", 10000))
        # Per-asset order_count, capped only by what max_cost_cents allows (min 1).
        raw_order_count = _get_asset_config(cfg.get("order_count"), asset, None)
        try:
            desired_count = int(raw_order_count) if raw_order_count is not None else None
        except (TypeError, ValueError):
            desired_count = None
        # Max contracts allowed by per-window/asset cost cap.
        max_by_cost = max_cost_cents // limit_price_cents if limit_price_cents else 0
        if max_by_cost <= 0:
            # Safety: if config is nonsensical, do not place (would breach cost or 0-size).
            self._record_telemetry(ctx, 0, "max_cost_too_low")
            return None
        if desired_count is None:
            # No explicit order_count: use max allowed by cost (but at least 1).
            count = max(1, max_by_cost)
        else:
            # Use the configured order_count but respect the per-window/asset cost cap.
            count = max(1, min(desired_count, max_by_cost))

        self._record_telemetry(ctx, 1, "intent_fired")
        client_order_id = f"last90s:{uuid.uuid4().hex[:12]}"
        # placement_bid_cents = bid at placement; used as entry cost for stop-loss (avoids phantom loss vs limit 99¢).
        return OrderIntent(
            side=side,
            price_cents=limit_price_cents,
            count=count,
            order_type="limit",
            client_order_id=client_order_id,
            placement_bid_cents=bid_cents,
        )

    def evaluate_exit(self, ctx: WindowContext, my_orders: List[OrderRecord]) -> List[ExitAction]:
        cfg = self._get_strategy_config(ctx)
        if not cfg:
            return []

        asset = (ctx.asset or "").strip().lower()
        stop_loss_pct = _get_asset_config(cfg.get("stop_loss_pct"), asset, 30)
        try:
            sl_pct = float(stop_loss_pct) / 100.0
        except (TypeError, ValueError):
            sl_pct = 0.30
        catastrophic_loss_pct = _get_asset_config(cfg.get("catastrophic_loss_pct"), asset, 25)
        try:
            catastrophic_pct = float(catastrophic_loss_pct) / 100.0
        except (TypeError, ValueError):
            catastrophic_pct = 0.25
        # Reference distance at placement (we required distance >= min_distance_at_placement when placing).
        min_dist_placement = _get_asset_config(cfg.get("min_distance_at_placement"), asset, 0.0)
        try:
            initial_placement_distance = float(min_dist_placement) if min_dist_placement is not None else 0.0
        except (TypeError, ValueError):
            initial_placement_distance = 0.0
        # Absolute buffer: danger_threshold = initial_placement_distance - stop_loss_absolute_buffer.
        # Stop-loss fires ONLY if current_min_distance < danger_threshold AND current_loss_pct > stop_loss_pct.
        buffer_raw = _get_asset_config(cfg.get("stop_loss_absolute_buffer"), asset, None)
        try:
            buffer_f = float(buffer_raw) if buffer_raw is not None else None
        except (TypeError, ValueError):
            buffer_f = None
        if buffer_f is not None and initial_placement_distance is not None:
            danger_threshold = initial_placement_distance - buffer_f
        else:
            # Fallback if stop_loss_absolute_buffer not set (e.g. old config): no distance gate.
            danger_threshold = None
        distance_min = _min_spot_distance(ctx)
        yes_bid = int(ctx.quote.get("yes_bid", 0) or 0)
        no_bid = int(ctx.quote.get("no_bid", 0) or 0)

        actions: List[ExitAction] = []
        for order in my_orders:
            side = (order.side or "no").strip().lower()
            if side not in ("yes", "no"):
                continue
            filled = order.status == "filled" or (order.filled_count or 0) > 0
            if not filled:
                continue
            # Use placement bid as entry cost so loss_pct reflects true drawdown (fix phantom loss from using limit 99¢).
            entry_cents = (
                order.placement_bid_cents
                if getattr(order, "placement_bid_cents", None) is not None
                else (order.limit_price_cents or 99)
            )
            if entry_cents <= 0:
                continue
            current_bid = yes_bid if side == "yes" else no_bid
            loss_pct = (entry_cents - current_bid) / float(entry_cents) if current_bid is not None else 0.0

            # Catastrophic bid collapse override: if loss >= catastrophic_loss_pct, fire immediately (ignore distance).
            if loss_pct >= catastrophic_pct:
                actions.append(ExitAction(order_id=order.order_id, action="stop_loss"))
                logger.info(
                    "[last_90s] Stop-loss (catastrophic override): order_id=%s side=%s entry=%sc bid=%s loss_pct=%.2f >= %.0f%%",
                    order.order_id, side, entry_cents, current_bid, loss_pct, catastrophic_pct * 100,
                )
                continue
            # Normal dual-condition: loss >= stop_loss_pct AND distance < danger_threshold.
            if loss_pct < sl_pct:
                continue
            if danger_threshold is not None and distance_min is not None:
                if distance_min >= danger_threshold:
                    continue
            actions.append(ExitAction(order_id=order.order_id, action="stop_loss"))
            logger.info(
                "[last_90s] Stop-loss: order_id=%s side=%s entry=%sc bid=%s loss_pct=%.2f distance_min=%s danger_threshold=%s",
                order.order_id, side, entry_cents, current_bid, loss_pct, distance_min, danger_threshold,
            )
        return actions

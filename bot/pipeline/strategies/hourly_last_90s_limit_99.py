"""
V2 hourly: port of legacy hourly_last_90s_limit_99.

This strategy operates in the final window_seconds of the hour and places limit buys at 99c
when the relevant side has bid >= min_bid_cents, subject to:
- per-market side caps (max_yes_per_market / max_no_per_market)
- per-ticker min distance to strike/boundary at placement

Exit: stop-loss when loss_pct >= stop_loss_pct AND distance has decayed to stop_loss_distance_factor * entry_distance.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from bot.market import TickerQuote, extract_strike_from_market
from bot.pipeline.context import WindowContext
from bot.pipeline.intents import ExitAction, OrderIntent, OrderRecord
from bot.pipeline.strategies.base import BaseV2Strategy
from bot.pipeline.window_utils import logical_window_slot
from bot.strategy import generate_signals_farthest

logger = logging.getLogger(__name__)

TELEMETRY_TABLE = "v2_telemetry_hourly_last90s"


def _v2_db_path() -> Path:
    return Path(__file__).resolve().parents[2].parent / "data" / "v2_state.db"


def _ensure_telemetry(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TELEMETRY_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            window_id TEXT,
            asset TEXT,
            action TEXT,
            ticker TEXT,
            side TEXT,
            reason TEXT,
            details_json TEXT,
            timestamp REAL
        )
        """
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_hourly_last90s_window ON {TELEMETRY_TABLE}(window_id, asset)")
    conn.commit()


def _log(
    *,
    window_id: str,
    asset: str,
    action: str,
    ticker: str = "",
    side: str = "",
    reason: str = "",
    details: Optional[Dict[str, Any]] = None,
) -> None:
    path = _v2_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), check_same_thread=False)
    try:
        _ensure_telemetry(conn)
        conn.execute(
            f"""
            INSERT INTO {TELEMETRY_TABLE}
            (window_id, asset, action, ticker, side, reason, details_json, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(window_id),
                str(asset),
                str(action),
                str(ticker),
                str(side),
                str(reason),
                json.dumps(details or {}, separators=(",", ":")),
                float(time.time()),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _asset_lower(asset: str) -> str:
    return (asset or "").strip().lower()


def _get_cfg(ctx: WindowContext, strategy_id: str) -> dict:
    interval_block = (ctx.config or {}).get(ctx.interval) or {}
    strategies = interval_block.get("strategies") or {}
    out = strategies.get(strategy_id) or {}
    return out if isinstance(out, dict) else {}


def _parse_int(v: Any, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _parse_float(v: Any, default: float) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _get_asset_cfg(value: Any, asset: str, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, dict):
        a = _asset_lower(asset)
        return value.get(a, value.get(a.upper(), default))
    return value


def _distance_to_boundary_or_strike(m: Dict[str, Any], spot: float, strike: float) -> float:
    """
    For range markets with floor/ceiling, use min distance to boundary.
    Else use abs(spot - strike).
    """
    if m.get("floor_strike") is not None and m.get("ceiling_strike") is not None:
        try:
            lo = float(m.get("floor_strike"))
            hi = float(m.get("ceiling_strike"))
            if lo <= spot <= hi:
                return min(abs(spot - lo), abs(hi - spot))
            # outside range: distance to nearest boundary
            return min(abs(spot - lo), abs(spot - hi))
        except Exception:
            pass
    return abs(float(spot) - float(strike))


def _normalize_quotes(ctx: WindowContext, spot: float) -> List[TickerQuote]:
    out: List[TickerQuote] = []
    for m in ctx.event_markets or []:
        if not isinstance(m, dict):
            continue
        t = m.get("ticker")
        if not t:
            continue
        strike = extract_strike_from_market(m, str(t))
        if strike <= 0:
            continue
        range_low = None
        range_high = None
        if m.get("floor_strike") is not None and m.get("ceiling_strike") is not None:
            try:
                range_low = float(m.get("floor_strike"))
                range_high = float(m.get("ceiling_strike"))
            except Exception:
                range_low = None
                range_high = None
        out.append(
            TickerQuote(
                ticker=str(t),
                strike=float(strike),
                yes_ask=m.get("yes_ask"),
                no_ask=m.get("no_ask"),
                yes_bid=m.get("yes_bid"),
                no_bid=m.get("no_bid"),
                subtitle=str(m.get("subtitle") or ""),
                range_low=range_low,
                range_high=range_high,
            )
        )
    return out


class HourlyLast90sLimit99Strategy(BaseV2Strategy):
    def __init__(self, config: dict) -> None:
        super().__init__("hourly_last_90s_limit_99", config)

    def evaluate_entry(self, ctx: WindowContext, my_orders: Optional[List[OrderRecord]] = None) -> Optional[OrderIntent]:
        cfg = _get_cfg(ctx, self.strategy_id)
        if not cfg.get("enabled", False):
            return None
        if ctx.spot is None:
            return None
        spot = float(ctx.spot)

        window_seconds = _parse_float(cfg.get("window_seconds"), 75.0)
        if float(ctx.seconds_to_close) > float(window_seconds):
            return None

        # Optional per-strategy assets list (defaults to ctx.interval assets in v2_common).
        allowed_assets = cfg.get("assets")
        if isinstance(allowed_assets, list) and allowed_assets:
            if _asset_lower(ctx.asset) not in {_asset_lower(x) for x in allowed_assets}:
                return None

        limit_price = _parse_int(cfg.get("limit_price_cents"), 99)
        min_bid = _parse_int(cfg.get("min_bid_cents"), 94)
        side_mode = str(cfg.get("side") or "both").strip().lower()

        max_yes = _parse_int(cfg.get("max_yes_per_market"), 0)
        max_no = _parse_int(cfg.get("max_no_per_market"), 0)

        # Enforce per-market caps using my_orders for this strategy in this window (includes filled + resting).
        yes_count = sum(1 for o in (my_orders or []) if str(o.side).lower() == "yes")
        no_count = sum(1 for o in (my_orders or []) if str(o.side).lower() == "no")

        window_id = f"{ctx.interval}_{logical_window_slot(ctx.market_id)}"

        quotes = _normalize_quotes(ctx, spot)
        if not quotes:
            _log(window_id=window_id, asset=ctx.asset, action="skip", reason="no_event_markets")
            return None

        # Use legacy signal generator in pick_all_in_range mode to get farthest-first candidates.
        # We provide min_bid_cents so it can qualify a side based on bid>=min_bid even when ask is not in band.
        thresholds = {
            "yes_min": 0,
            "yes_max": 99,
            "no_min": 0,
            "no_max": 99,
        }
        signals = generate_signals_farthest(
            quotes=quotes,
            spot_price=spot,
            ctx_late_window=True,
            thresholds=thresholds,
            pick_all_in_range=True,
            min_bid_cents=min_bid,
        )
        if not signals:
            _log(window_id=window_id, asset=ctx.asset, action="skip", reason="no_signals")
            return None

        by_ticker: Dict[str, Dict[str, Any]] = {}
        for m in ctx.event_markets or []:
            if isinstance(m, dict) and m.get("ticker"):
                by_ticker[str(m.get("ticker"))] = m

        # Iterate farthest-first, skip tickers already traded, enforce caps and distance filter.
        traded_tickers = {str(o.ticker) for o in (my_orders or []) if getattr(o, "ticker", None)}
        for s in signals:
            t = str(s.ticker)
            if t in traded_tickers:
                continue
            side = str(s.side).lower()
            if side not in ("yes", "no"):
                continue
            if side_mode in ("yes", "no") and side != side_mode:
                continue
            if side == "yes" and max_yes > 0 and yes_count >= max_yes:
                continue
            if side == "no" and max_no > 0 and no_count >= max_no:
                continue

            mkt = by_ticker.get(t)
            if not mkt:
                continue
            strike = extract_strike_from_market(mkt, t)
            if strike <= 0:
                continue
            distance = _distance_to_boundary_or_strike(mkt, spot, float(strike))

            min_dist_map = cfg.get("min_distance_at_placement") or {}
            min_dist = _get_asset_cfg(min_dist_map, ctx.asset, None)
            if min_dist is not None and distance < _parse_float(min_dist, 0.0):
                _log(
                    window_id=window_id,
                    asset=ctx.asset,
                    action="skip",
                    ticker=t,
                    side=side,
                    reason="min_distance_at_placement",
                    details={"distance": distance, "min_distance": float(min_dist)},
                )
                continue

            bid = int((mkt.get("yes_bid") or 0) if side == "yes" else (mkt.get("no_bid") or 0))
            if bid < min_bid:
                _log(
                    window_id=window_id,
                    asset=ctx.asset,
                    action="skip",
                    ticker=t,
                    side=side,
                    reason="bid_below_min",
                    details={"bid": bid, "min_bid": min_bid},
                )
                continue

            client_order_id = f"v2:{self.strategy_id}:{ctx.asset}:{window_id}:{uuid.uuid4().hex[:10]}"
            _log(
                window_id=window_id,
                asset=ctx.asset,
                action="intent",
                ticker=t,
                side=side,
                reason=str(s.reason),
                details={"limit_price_cents": limit_price, "bid": bid, "distance": distance},
            )
            return OrderIntent(
                side=side,
                price_cents=int(limit_price),
                count=1,
                order_type="limit",
                client_order_id=client_order_id,
                placement_bid_cents=int(bid) if bid > 0 else None,
                entry_distance=float(distance) if distance is not None else None,
            )

        return None

    def evaluate_exit(self, ctx: WindowContext, my_orders: List[OrderRecord]) -> List[ExitAction]:
        cfg = _get_cfg(ctx, self.strategy_id)
        if not cfg.get("enabled", False):
            return []
        if not my_orders:
            return []
        if ctx.spot is None:
            return []

        stop_loss_pct = _parse_float(cfg.get("stop_loss_pct"), 15.0)
        dist_factor = _parse_float(cfg.get("stop_loss_distance_factor"), 0.75)

        by_ticker: Dict[str, Dict[str, Any]] = {}
        for m in ctx.event_markets or []:
            if isinstance(m, dict) and m.get("ticker"):
                by_ticker[str(m.get("ticker"))] = m

        window_id = f"{ctx.interval}_{logical_window_slot(ctx.market_id)}"
        exits: List[ExitAction] = []
        spot = float(ctx.spot)

        for o in my_orders:
            if (o.status or "") not in ("filled", "executed"):
                continue
            t = str(o.ticker or "")
            if not t:
                continue
            side = str(o.side or "").lower()
            if side not in ("yes", "no"):
                continue
            mkt = by_ticker.get(t)
            if not mkt:
                continue

            strike = extract_strike_from_market(mkt, t)
            if strike <= 0:
                continue
            cur_dist = _distance_to_boundary_or_strike(mkt, spot, float(strike))

            entry_bid = o.placement_bid_cents or o.limit_price_cents
            if not entry_bid or entry_bid <= 0:
                continue
            cur_bid = int((mkt.get("yes_bid") or 0) if side == "yes" else (mkt.get("no_bid") or 0))
            if cur_bid <= 0:
                continue

            loss_pct = max(0.0, (float(entry_bid - cur_bid) / float(entry_bid)) * 100.0)
            if loss_pct < float(stop_loss_pct):
                continue

            base_dist = o.entry_distance or o.entry_distance_at_fill
            if base_dist is not None and cur_dist > (float(base_dist) * float(dist_factor)):
                # distance gate not armed yet
                continue

            exits.append(ExitAction(order_id=str(o.order_id), action="stop_loss", reason="stop_loss"))
            _log(
                window_id=window_id,
                asset=ctx.asset,
                action="exit",
                ticker=t,
                side=side,
                reason="stop_loss",
                details={
                    "loss_pct": loss_pct,
                    "entry_bid": entry_bid,
                    "cur_bid": cur_bid,
                    "cur_distance": cur_dist,
                    "entry_distance": base_dist,
                    "dist_factor": dist_factor,
                },
            )
        return exits


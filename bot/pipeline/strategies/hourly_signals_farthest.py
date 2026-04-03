"""
V2 hourly: port of legacy regular hourly loop (eligible tickers near spot + farthest / all-in-range).

Key parity goals (see docs/KALSHI_HOURLY_V1_V2_PARITY.md):
- Use the same selection algorithm as legacy: bot/strategy.generate_signals_farthest.
- Read full hourly event market list from ctx.event_markets (populated by run_unified for hourly).
- Emit at most one OrderIntent per tick; when pick_all_in_range=true, we iterate farthest-first across ticks
  by skipping tickers already traded by this strategy in the current window (my_orders).
- Stop-loss: per filled order, if current bid vs entry bid is down >= stop_loss_pct (default 20%), emit
  ExitAction(stop_loss). Execution (same as continuous_alpha_limit_99): executor runs Kalshi
  place_market_order → limit sell at 1¢ on that side, IOC + reduce_only (see bot/pipeline/executor.py).
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from bot.market import TickerQuote, extract_strike_from_market
from bot.pipeline.context import WindowContext
from bot.pipeline.intents import ExitAction, OrderIntent, OrderRecord
from bot.pipeline.strategies.base import BaseV2Strategy
from bot.pipeline.window_utils import logical_window_slot
from bot.strategy import generate_signals_farthest

logger = logging.getLogger(__name__)

TELEMETRY_TABLE = "v2_telemetry_hourly_signals"
SL_STATE_TABLE = "v2_hourly_sl_state"


def _v2_db_path() -> Path:
    return Path(__file__).resolve().parents[2].parent / "data" / "v2_state.db"


def _ensure_tables(conn: sqlite3.Connection) -> None:
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
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SL_STATE_TABLE} (
            order_id TEXT PRIMARY KEY,
            consecutive_polls INTEGER NOT NULL,
            last_updated_ts REAL NOT NULL
        )
        """
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_hourly_signals_window ON {TELEMETRY_TABLE}(window_id, asset)")
    conn.commit()


def _log_telemetry(
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
        _ensure_tables(conn)
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
        i = int(v)
        return i
    except Exception:
        return default


def _parse_float(v: Any, default: float) -> float:
    try:
        f = float(v)
        return f
    except Exception:
        return default


def _spot_window(cfg: dict, asset: str) -> float:
    sel = cfg.get("selection") or {}
    default = _parse_float(sel.get("spot_window_default"), 1500.0)
    by_asset = sel.get("spot_window_by_asset") or {}
    if isinstance(by_asset, dict):
        v = by_asset.get(_asset_lower(asset), by_asset.get(str(asset).upper()))
        if v is not None:
            return _parse_float(v, default)
    return default


def _thresholds(cfg: dict) -> Dict[str, int]:
    sel = cfg.get("selection") or {}
    ty = sel.get("thresholds_yes") or {}
    tn = sel.get("thresholds_no") or {}
    return {
        "yes_min": _parse_int((ty or {}).get("min"), 92),
        "yes_max": _parse_int((ty or {}).get("max"), 99),
        "no_min": _parse_int((tn or {}).get("min"), 92),
        "no_max": _parse_int((tn or {}).get("max"), 99),
    }


def _get_asset_cfg(value: Any, asset: str, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, dict):
        a = _asset_lower(asset)
        return value.get(a, value.get(a.upper(), default))
    return value


def _distance_buffer_min_required(cfg: dict, asset: str, spot: float) -> Optional[float]:
    guards = cfg.get("guards") or {}
    dist_cfg = guards.get("distance_buffer") or {}
    if not isinstance(dist_cfg, dict) or not dist_cfg.get("enabled", False):
        return None
    assets_cfg = dist_cfg.get("assets") or {}
    if not isinstance(assets_cfg, dict):
        return None
    a_cfg = assets_cfg.get(_asset_lower(asset), assets_cfg.get(str(asset).upper()))
    if not isinstance(a_cfg, dict):
        return None
    try:
        pct = float(a_cfg.get("pct")) if a_cfg.get("pct") is not None else 0.0
    except Exception:
        pct = 0.0
    try:
        floor_usd = float(a_cfg.get("floor_usd")) if a_cfg.get("floor_usd") is not None else 0.0
    except Exception:
        floor_usd = 0.0
    if pct <= 0 and floor_usd <= 0:
        return None
    return max(abs(float(spot)) * max(0.0, pct), max(0.0, floor_usd))


def _normalize_event_quotes(ctx: WindowContext, spot: float, window: float) -> List[TickerQuote]:
    def _to_cents(v: Any) -> Optional[int]:
        if v is None:
            return None
        try:
            f = float(v)
        except Exception:
            return None
        # Kalshi may return either cents (0..100) or dollars (0.00..1.00)
        if 0.0 <= f <= 1.0:
            c = int(round(f * 100.0))
        else:
            c = int(round(f))
        if c < 0:
            return None
        return c

    def _pick_price(m: Dict[str, Any], keys: List[str]) -> Optional[int]:
        for k in keys:
            c = _to_cents(m.get(k))
            if c is not None:
                return c
        return None

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
        if abs(float(strike) - float(spot)) > float(window):
            continue
        # Range (B) markets include bounds; legacy uses them to decide YES vs NO for in-range.
        range_low = None
        range_high = None
        if m.get("floor_strike") is not None and m.get("ceiling_strike") is not None:
            try:
                range_low = float(m.get("floor_strike"))
                range_high = float(m.get("ceiling_strike"))
            except Exception:
                range_low = None
                range_high = None
        # Use strict ask fields only (avoid ambiguous last-trade style price keys).
        yes_ask = _pick_price(m, ["yes_ask", "yes_ask_price", "yes_ask_dollars"])
        no_ask = _pick_price(m, ["no_ask", "no_ask_price", "no_ask_dollars"])
        yes_bid = _pick_price(m, ["yes_bid", "yes_bid_price", "yes_bid_dollars"])
        no_bid = _pick_price(m, ["no_bid", "no_bid_price", "no_bid_dollars"])
        out.append(
            TickerQuote(
                ticker=str(t),
                strike=float(strike),
                yes_ask=yes_ask,
                no_ask=no_ask,
                yes_bid=yes_bid,
                no_bid=no_bid,
                subtitle=str(m.get("subtitle") or ""),
                range_low=range_low,
                range_high=range_high,
            )
        )
    return out


def _already_traded_tickers(my_orders: Optional[List[OrderRecord]]) -> set:
    out: set = set()
    for o in my_orders or []:
        try:
            if o.ticker:
                out.add(str(o.ticker))
        except Exception:
            continue
    return out


def _get_current_bid_for_side(market: Dict[str, Any], side: str) -> Optional[int]:
    try:
        if side == "yes":
            v = market.get("yes_bid")
        else:
            v = market.get("no_bid")
        if v is None:
            return None
        i = int(v)
        # 0¢ bid is valid for SL (max drawdown vs entry); only None means missing quote.
        return i
    except Exception:
        return None


def _sl_state_get(order_id: str) -> Tuple[int, float]:
    """Return (consecutive_polls, last_updated_ts). Missing -> (0, 0.0)."""
    path = _v2_db_path()
    if not path.exists():
        return (0, 0.0)
    conn = sqlite3.connect(str(path), check_same_thread=False)
    try:
        _ensure_tables(conn)
        row = conn.execute(
            f"SELECT consecutive_polls, last_updated_ts FROM {SL_STATE_TABLE} WHERE order_id = ?",
            (str(order_id),),
        ).fetchone()
        if not row:
            return (0, 0.0)
        return (int(row[0] or 0), float(row[1] or 0.0))
    finally:
        conn.close()


def _sl_state_set(order_id: str, consecutive_polls: int) -> None:
    path = _v2_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), check_same_thread=False)
    try:
        _ensure_tables(conn)
        conn.execute(
            f"""
            INSERT INTO {SL_STATE_TABLE} (order_id, consecutive_polls, last_updated_ts)
            VALUES (?, ?, ?)
            ON CONFLICT(order_id) DO UPDATE SET
              consecutive_polls = excluded.consecutive_polls,
              last_updated_ts = excluded.last_updated_ts
            """,
            (str(order_id), int(consecutive_polls), float(time.time())),
        )
        conn.commit()
    finally:
        conn.close()


class HourlySignalsFarthestStrategy(BaseV2Strategy):
    def __init__(self, config: dict) -> None:
        super().__init__("hourly_signals_farthest", config)

    def evaluate_entry(self, ctx: WindowContext, my_orders: Optional[List[OrderRecord]] = None) -> Optional[OrderIntent]:
        cfg = _get_cfg(ctx, self.strategy_id)
        if not cfg.get("enabled", False):
            return None
        sec_to_close = float(ctx.seconds_to_close or 0.0)
        logger.info(
            "[hourly_signals_v2_tick] [%s] sec_to_close=%.0f enabled=true",
            (ctx.asset or "").upper(),
            sec_to_close,
        )
        max_orders_per_window = int(_get_asset_cfg(cfg.get("max_orders_per_window"), ctx.asset, 1) or 1)
        if max_orders_per_window < 1:
            max_orders_per_window = 1
        # Cap entry rate per (window, asset) for this strategy.
        if my_orders and len(my_orders) >= max_orders_per_window:
            logger.info(
                "[hourly_signals_v2_tick] [%s] skip=max_orders_per_window open_orders=%s cap=%s",
                (ctx.asset or "").upper(),
                len(my_orders or []),
                max_orders_per_window,
            )
            return None
        ew = cfg.get("entry_window") or {}
        late_window_minutes = _parse_float(ew.get("late_window_minutes"), 20.0)
        if (sec_to_close / 60.0) > late_window_minutes:
            logger.info(
                "[hourly_signals_v2_tick] [%s] skip=outside_late_window sec_to_close=%.0f late_window_minutes=%.1f",
                (ctx.asset or "").upper(),
                sec_to_close,
                late_window_minutes,
            )
            return None
        if ctx.spot is None:
            logger.info(
                "[hourly_signals_v2_tick] [%s] skip=spot_none",
                (ctx.asset or "").upper(),
            )
            return None

        spot = float(ctx.spot)
        window = _spot_window(cfg, ctx.asset)
        quotes = _normalize_event_quotes(ctx, spot, window)
        if not quotes:
            _log_telemetry(
                window_id=f"{ctx.interval}_{logical_window_slot(ctx.market_id)}",
                asset=ctx.asset,
                action="skip",
                reason="no_quotes_in_spot_window",
                details={"spot": spot, "spot_window": window, "event_markets": len(ctx.event_markets or [])},
            )
            return None

        sel = cfg.get("selection") or {}
        pick_all = bool(sel.get("pick_all_in_range", False))
        thresholds = _thresholds(cfg)
        min_dist_required = _distance_buffer_min_required(cfg, ctx.asset, spot)
        # Primary above/below hourly series must obey directional side:
        # strike < spot => YES only, strike > spot => NO only.
        # Detect primary from market id prefix (e.g., KXBTCD/KXETHD/KXSOLD/KXXRPD).
        mid = str(ctx.market_id or "").upper()
        directional_filter = mid.startswith(("KXBTCD-", "KXETHD-", "KXSOLD-", "KXXRPD-"))

        # For hourly range/above-below markets, legacy uses strike vs spot; we keep filter_side_by_spot_strike=False
        # because hourly includes range markets. generate_signals_farthest handles range bounds when present.
        signals = generate_signals_farthest(
            quotes=quotes,
            spot_price=spot,
            ctx_late_window=True,
            thresholds=thresholds,
            pick_all_in_range=pick_all,
            filter_side_by_spot_strike=directional_filter,
        )
        if not signals:
            yes_lo = int(thresholds.get("yes_min", 92))
            yes_hi = int(thresholds.get("yes_max", 99))
            no_lo = int(thresholds.get("no_min", 92))
            no_hi = int(thresholds.get("no_max", 99))
            ask_yes_in_band = 0
            ask_no_in_band = 0
            bid_yes_in_band = 0
            bid_no_in_band = 0
            best_yes_ask = None
            best_no_ask = None
            best_yes_bid = None
            best_no_bid = None
            for qq in quotes:
                ya = int(getattr(qq, "yes_ask", 0) or 0)
                na = int(getattr(qq, "no_ask", 0) or 0)
                yb = int(getattr(qq, "yes_bid", 0) or 0)
                nb = int(getattr(qq, "no_bid", 0) or 0)
                best_yes_ask = ya if best_yes_ask is None else max(best_yes_ask, ya)
                best_no_ask = na if best_no_ask is None else max(best_no_ask, na)
                best_yes_bid = yb if best_yes_bid is None else max(best_yes_bid, yb)
                best_no_bid = nb if best_no_bid is None else max(best_no_bid, nb)
                if yes_lo <= ya <= yes_hi:
                    ask_yes_in_band += 1
                if no_lo <= na <= no_hi:
                    ask_no_in_band += 1
                if yes_lo <= yb <= yes_hi:
                    bid_yes_in_band += 1
                if no_lo <= nb <= no_hi:
                    bid_no_in_band += 1
            logger.info(
                "[hourly_signals_v2_tick] [%s] skip=no_signals quotes=%s sec_to_close=%.0f "
                "yes_band=[%s,%s] no_band=[%s,%s] "
                "ask_in_band(yes=%s,no=%s) bid_in_band(yes=%s,no=%s) "
                "best(yes_ask=%s,no_ask=%s,yes_bid=%s,no_bid=%s)",
                (ctx.asset or "").upper(),
                len(quotes),
                sec_to_close,
                yes_lo,
                yes_hi,
                no_lo,
                no_hi,
                ask_yes_in_band,
                ask_no_in_band,
                bid_yes_in_band,
                bid_no_in_band,
                best_yes_ask,
                best_no_ask,
                best_yes_bid,
                best_no_bid,
            )
            return None

        already = _already_traded_tickers(my_orders)
        # When pick_all_in_range: emit farthest-first across ticks by skipping tickers already traded this window.
        for s in signals:
            if pick_all and str(s.ticker) in already:
                continue
            side = str(s.side).lower()
            if side not in ("yes", "no"):
                continue
            # Selection uses legacy thresholds (ask/bid qualification); limit price matches fifteen_min (configurable).
            price = _parse_int(cfg.get("limit_price_cents"), 99)
            if price < 1 or price > 99:
                price = 99

            order_count = int(_get_asset_cfg(cfg.get("order_count"), ctx.asset, 1) or 1)
            if order_count < 1:
                order_count = 1
            max_cost_raw = _get_asset_cfg(cfg.get("max_cost_cents_by_asset"), ctx.asset, None)
            if max_cost_raw is None:
                max_cost_raw = _get_asset_cfg(cfg.get("max_cost_cents"), ctx.asset, 50000)
            max_cost = int(max_cost_raw or 50000)
            if max_cost > 0 and (order_count * int(price)) > max_cost:
                _log_telemetry(
                    window_id=f"{ctx.interval}_{logical_window_slot(ctx.market_id)}",
                    asset=ctx.asset,
                    action="skip",
                    ticker=str(s.ticker),
                    side=side,
                    reason="max_cost_cents",
                    details={"order_count": order_count, "price_cents": int(price), "max_cost_cents": max_cost},
                )
                return None

            # placement bid for stop-loss baseline (match V2 executor expectations)
            q = next((qq for qq in quotes if qq.ticker == s.ticker), None)
            placement_bid = None
            entry_dist = None
            if q is not None:
                placement_bid = int(q.yes_bid or 0) if side == "yes" else int(q.no_bid or 0)
                if placement_bid <= 0:
                    placement_bid = None
                try:
                    entry_dist = abs(float(q.strike) - float(spot))
                except Exception:
                    entry_dist = None
                # Final hard directional guard (defensive): never allow opposite side on primary above/below.
                if directional_filter:
                    strike_v = float(q.strike or 0.0)
                    if strike_v > 0:
                        expected_side = "yes" if float(spot) > strike_v else "no" if float(spot) < strike_v else side
                        if side != expected_side:
                            logger.info(
                                "[hourly_signals_v2_tick] [%s] skip=direction_mismatch ticker=%s strike=%.2f spot=%.2f chosen=%s expected=%s",
                                (ctx.asset or "").upper(),
                                str(s.ticker),
                                strike_v,
                                float(spot),
                                side,
                                expected_side,
                            )
                            _log_telemetry(
                                window_id=f"{ctx.interval}_{logical_window_slot(ctx.market_id)}",
                                asset=ctx.asset,
                                action="skip",
                                ticker=str(s.ticker),
                                side=side,
                                reason="direction_mismatch",
                                details={
                                    "strike": strike_v,
                                    "spot": float(spot),
                                    "expected_side": expected_side,
                                },
                            )
                            continue

                # Distance buffer guard (directional): YES needs spot-strike >= min_dist,
                # NO needs strike-spot >= min_dist.
                if min_dist_required is not None:
                    distance_dir = (float(spot) - strike_v) if side == "yes" else (strike_v - float(spot))
                    if distance_dir < float(min_dist_required):
                        logger.info(
                            "[hourly_signals_v2_tick] [%s] skip=distance_buffer ticker=%s side=%s strike=%.2f spot=%.2f distance=%.2f min_required=%.2f",
                            (ctx.asset or "").upper(),
                            str(s.ticker),
                            side,
                            strike_v,
                            float(spot),
                            distance_dir,
                            float(min_dist_required),
                        )
                        _log_telemetry(
                            window_id=f"{ctx.interval}_{logical_window_slot(ctx.market_id)}",
                            asset=ctx.asset,
                            action="skip",
                            ticker=str(s.ticker),
                            side=side,
                            reason="distance_buffer",
                            details={
                                "strike": strike_v,
                                "spot": float(spot),
                                "distance": distance_dir,
                                "min_required": float(min_dist_required),
                            },
                        )
                        continue

            # Require chosen side bid to be in configured band before placement.
            # This makes side execution explicitly bid-aware (not ask-only).
            bid_lo = int(thresholds.get("yes_min")) if side == "yes" else int(thresholds.get("no_min"))
            bid_hi = int(thresholds.get("yes_max")) if side == "yes" else int(thresholds.get("no_max"))
            chosen_bid_for_gate = int(placement_bid or 0)
            if chosen_bid_for_gate <= 0 or chosen_bid_for_gate < bid_lo or chosen_bid_for_gate > bid_hi:
                logger.info(
                    "[hourly_signals_v2_tick] [%s] skip=chosen_bid_out_of_band ticker=%s side=%s bid=%s band=[%s,%s]",
                    (ctx.asset or "").upper(),
                    str(s.ticker),
                    side,
                    chosen_bid_for_gate,
                    bid_lo,
                    bid_hi,
                )
                _log_telemetry(
                    window_id=f"{ctx.interval}_{logical_window_slot(ctx.market_id)}",
                    asset=ctx.asset,
                    action="skip",
                    ticker=str(s.ticker),
                    side=side,
                    reason="chosen_bid_out_of_band",
                    details={
                        "bid": chosen_bid_for_gate,
                        "band_lo": bid_lo,
                        "band_hi": bid_hi,
                    },
                )
                continue

            # Always log signal-side resolution for observability, even if we may skip later on cost/order caps.
            yes_bid = int(getattr(q, "yes_bid", 0) or 0) if q is not None else 0
            no_bid = int(getattr(q, "no_bid", 0) or 0) if q is not None else 0
            chosen_bid = int(placement_bid or 0)
            logger.info(
                "[hourly_signals_v2_side] [%s] ticker=%s sec_to_close=%.0f side_cfg=auto strike=%.2f spot=%.2f "
                "yes_ask=%s no_ask=%s yes_bid=%s no_bid=%s chosen=%s bid=%s "
                "yes_band=[%s,%s] no_band=[%s,%s]",
                (ctx.asset or "").upper(),
                str(s.ticker),
                float(ctx.seconds_to_close or -1),
                float(getattr(q, "strike", 0.0) or 0.0) if q is not None else 0.0,
                float(spot),
                int(getattr(q, "yes_ask", 0) or 0) if q is not None else 0,
                int(getattr(q, "no_ask", 0) or 0) if q is not None else 0,
                yes_bid,
                no_bid,
                side,
                chosen_bid,
                thresholds.get("yes_min"),
                thresholds.get("yes_max"),
                thresholds.get("no_min"),
                thresholds.get("no_max"),
            )

            # Log the choice so it matches the fifteen_min style.
            logger.info(
                "[hourly_signals_v2_choice] [%s] ticker=%s sec_to_close=%.0f mode=%s yes_bid=%s no_bid=%s chosen=%s bid=%s "
                "yes_band=[%s,%s] no_band=[%s,%s]",
                (ctx.asset or "").upper(),
                str(s.ticker),
                float(ctx.seconds_to_close or -1),
                ("all_in_range" if pick_all else "farthest"),
                yes_bid,
                no_bid,
                side,
                chosen_bid,
                thresholds.get("yes_min"),
                thresholds.get("yes_max"),
                thresholds.get("no_min"),
                thresholds.get("no_max"),
            )

            window_id = f"{ctx.interval}_{logical_window_slot(ctx.market_id)}"
            client_order_id = f"v2:{self.strategy_id}:{ctx.asset}:{window_id}:{uuid.uuid4().hex[:10]}"

            _log_telemetry(
                window_id=window_id,
                asset=ctx.asset,
                action="intent",
                ticker=str(s.ticker),
                side=side,
                reason=str(s.reason),
                details={
                    "price_cents": price,
                    "order_count": order_count,
                    "placement_bid_cents": placement_bid,
                    "entry_distance": entry_dist,
                    "pick_all_in_range": pick_all,
                },
            )

            return OrderIntent(
                side=side,
                price_cents=int(price),
                count=int(order_count),
                order_type="limit",
                client_order_id=client_order_id,
                ticker=str(s.ticker),
                placement_bid_cents=placement_bid,
                entry_distance=entry_dist,
            )
        return None

    def evaluate_exit(self, ctx: WindowContext, my_orders: List[OrderRecord]) -> List[ExitAction]:
        cfg = _get_cfg(ctx, self.strategy_id)
        if not cfg.get("enabled", False):
            return []
        if not my_orders:
            return []

        exit_cfg = cfg.get("exit") or {}
        # Accept fraction (0.20) or percent points (20) like fifteen_min YAML — avoids SL never arming if
        # someone sets stop_loss_pct: 15 meaning 15%.
        stop_loss_frac = _parse_float(exit_cfg.get("stop_loss_pct"), 0.20)
        if stop_loss_frac > 1.0:
            stop_loss_frac = stop_loss_frac / 100.0
        persistence = _parse_int(exit_cfg.get("stop_loss_persistence_polls"), 1)
        if persistence < 1:
            persistence = 1

        # Build a quick lookup market dict by ticker for current bids.
        by_ticker: Dict[str, Dict[str, Any]] = {}
        for m in ctx.event_markets or []:
            if isinstance(m, dict) and m.get("ticker"):
                by_ticker[str(m.get("ticker"))] = m

        exits: List[ExitAction] = []
        window_id = f"{ctx.interval}_{logical_window_slot(ctx.market_id)}"

        for o in my_orders:
            # Match continuous_alpha_limit_99 (last_90s): treat partial fills as eligible for SL monitoring.
            st = (o.status or "").lower()
            fc = int(getattr(o, "filled_count", 0) or 0)
            if st not in ("filled", "executed", "complete") and fc < 1:
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
            cur_bid = _get_current_bid_for_side(mkt, side)
            if cur_bid is None:
                continue
            entry_bid = (
                o.entry_fill_price_cents
                or o.placement_bid_cents
                or o.limit_price_cents
            )
            if not entry_bid or entry_bid <= 0:
                continue

            loss_frac = max(0.0, float(entry_bid - cur_bid) / float(entry_bid))
            if loss_frac < stop_loss_frac:
                if persistence > 1:
                    _sl_state_set(o.order_id, 0)
                continue

            if persistence <= 1:
                exits.append(ExitAction(order_id=str(o.order_id), action="stop_loss", reason="stop_loss"))
                logger.info(
                    "[hourly_signals_v2_sl] [%s] order_id=%s ticker=%s side=%s reason=stop_loss bid_down>=%.0f%% entry_bid=%s cur_bid=%s loss_frac=%.4f",
                    (ctx.asset or "").upper(),
                    str(o.order_id),
                    t,
                    side,
                    stop_loss_frac * 100.0,
                    entry_bid,
                    cur_bid,
                    loss_frac,
                )
                _log_telemetry(
                    window_id=window_id,
                    asset=ctx.asset,
                    action="exit",
                    ticker=t,
                    side=side,
                    reason="stop_loss",
                    details={"entry_bid": entry_bid, "cur_bid": cur_bid, "loss_frac": loss_frac},
                )
                continue

            prev, _ = _sl_state_get(o.order_id)
            nxt = int(prev) + 1
            _sl_state_set(o.order_id, nxt)
            if nxt >= persistence:
                exits.append(
                    ExitAction(
                        order_id=str(o.order_id),
                        action="stop_loss",
                        reason=f"stop_loss_persist_{nxt}",
                    )
                )
                logger.info(
                    "[hourly_signals_v2_sl] [%s] order_id=%s ticker=%s side=%s reason=stop_loss_persist polls=%s/%s entry_bid=%s cur_bid=%s loss_frac=%.4f",
                    (ctx.asset or "").upper(),
                    str(o.order_id),
                    t,
                    side,
                    nxt,
                    persistence,
                    entry_bid,
                    cur_bid,
                    loss_frac,
                )
                _log_telemetry(
                    window_id=window_id,
                    asset=ctx.asset,
                    action="exit",
                    ticker=t,
                    side=side,
                    reason="stop_loss_persist",
                    details={"polls": nxt, "required": persistence, "entry_bid": entry_bid, "cur_bid": cur_bid, "loss_frac": loss_frac},
                )
        return exits


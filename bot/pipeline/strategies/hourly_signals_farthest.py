"""
V2 hourly: port of legacy regular hourly loop (eligible tickers near spot + farthest / all-in-range).

Key parity goals (see docs/KALSHI_HOURLY_V1_V2_PARITY.md):
- Use the same selection algorithm as legacy: bot/strategy.generate_signals_farthest.
- Read full hourly event market list from ctx.event_markets (populated by run_unified for hourly).
- Emit at most one OrderIntent per tick; when pick_all_in_range=true, we iterate farthest-first across ticks
  by skipping tickers already traded by this strategy in the current window (my_orders).
- Basic stop-loss exit using current bid vs placement_bid_cents with optional persistence via sqlite.
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


def _normalize_event_quotes(ctx: WindowContext, spot: float, window: float) -> List[TickerQuote]:
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
        return i if i > 0 else None
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
        ew = cfg.get("entry_window") or {}
        late_window_minutes = _parse_float(ew.get("late_window_minutes"), 20.0)
        if (ctx.seconds_to_close / 60.0) > late_window_minutes:
            return None
        if ctx.spot is None:
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

        # For hourly range/above-below markets, legacy uses strike vs spot; we keep filter_side_by_spot_strike=False
        # because hourly includes range markets. generate_signals_farthest handles range bounds when present.
        signals = generate_signals_farthest(
            quotes=quotes,
            spot_price=spot,
            ctx_late_window=True,
            thresholds=thresholds,
            pick_all_in_range=pick_all,
        )
        if not signals:
            return None

        already = _already_traded_tickers(my_orders)
        # When pick_all_in_range: emit farthest-first across ticks by skipping tickers already traded this window.
        for s in signals:
            if pick_all and str(s.ticker) in already:
                continue
            side = str(s.side).lower()
            if side not in ("yes", "no"):
                continue
            price = _parse_int(s.price, 0)
            if price <= 0:
                continue

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

            # Log the choice so it matches the fifteen_min style.
            yes_bid = int(getattr(q, "yes_bid", 0) or 0) if q is not None else 0
            no_bid = int(getattr(q, "no_bid", 0) or 0) if q is not None else 0
            chosen_bid = int(placement_bid or 0)
            logger.info(
                "[hourly_signals_v2_choice] [%s] sec_to_close=%.0f mode=%s yes_bid=%s no_bid=%s chosen=%s bid=%s "
                "yes_band=[%s,%s] no_band=[%s,%s]",
                (ctx.asset or "").upper(),
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
                    "placement_bid_cents": placement_bid,
                    "entry_distance": entry_dist,
                    "pick_all_in_range": pick_all,
                },
            )

            return OrderIntent(
                side=side,
                price_cents=int(price),
                count=1,
                order_type="limit",
                client_order_id=client_order_id,
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
        stop_loss_frac = _parse_float(exit_cfg.get("stop_loss_pct"), 0.30)
        panic_frac = _parse_float(exit_cfg.get("panic_stop_loss_pct"), stop_loss_frac)
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
            cur_bid = _get_current_bid_for_side(mkt, side)
            if cur_bid is None:
                continue
            entry_bid = o.placement_bid_cents or o.limit_price_cents
            if not entry_bid or entry_bid <= 0:
                continue

            loss_frac = max(0.0, float(entry_bid - cur_bid) / float(entry_bid))
            triggered = loss_frac >= stop_loss_frac
            panic = loss_frac >= panic_frac

            if not triggered:
                # Reset persistence counter if previously armed.
                if persistence > 1:
                    _sl_state_set(o.order_id, 0)
                continue

            if panic:
                exits.append(ExitAction(order_id=str(o.order_id), action="stop_loss", reason="panic_stop_loss"))
                logger.info(
                    "[hourly_signals_v2_sl] [%s] order_id=%s ticker=%s side=%s reason=panic_stop_loss entry_bid=%s cur_bid=%s loss_frac=%.4f",
                    (ctx.asset or "").upper(),
                    str(o.order_id),
                    t,
                    side,
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
                    reason="panic_stop_loss",
                    details={"entry_bid": entry_bid, "cur_bid": cur_bid, "loss_frac": loss_frac},
                )
                continue

            if persistence <= 1:
                exits.append(ExitAction(order_id=str(o.order_id), action="stop_loss", reason="stop_loss"))
                logger.info(
                    "[hourly_signals_v2_sl] [%s] order_id=%s ticker=%s side=%s reason=stop_loss entry_bid=%s cur_bid=%s loss_frac=%.4f",
                    (ctx.asset or "").upper(),
                    str(o.order_id),
                    t,
                    side,
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


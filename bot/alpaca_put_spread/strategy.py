from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

import pytz
from alpaca.data.requests import OptionLatestQuoteRequest
from alpaca.trading.enums import OrderSide, PositionIntent
from alpaca.trading.client import TradingClient

from bot.alpaca_put_spread.alerts import (
    alert_api_error,
    alert_close_filled,
    alert_close_submitted,
    alert_entry_filled,
    alert_entry_submitted,
    alert_loss_cap_hit,
    alert_sl_triggered,
    alert_tp_triggered,
)
from bot.alpaca_put_spread.call_credit_spread_selector import select_bear_call_credit_spread
from bot.alpaca_put_spread.config import AlpacaPutSpreadConfig
from bot.alpaca_put_spread.db import (
    close_spread as db_close_spread,
    get_daily_pnl,
    get_pnl_by_underlying_today,
    init_alpaca_db,
    insert_order as db_insert_order,
    insert_spread as db_insert_spread,
    log_event,
    update_order_status as db_update_order_status,
)
from bot.alpaca_put_spread.domain import Leg
from bot.alpaca_put_spread.execution import cancel_order, submit_mleg_limit_order, wait_for_order
from bot.alpaca_put_spread.iron_condor_selector import select_iron_condor
from bot.alpaca_put_spread.option_symbol import option_expiry_utc, parse_occ_option_symbol
from bot.alpaca_put_spread.pricing_logic import (
    CallSpreadCandidate,
    IronCondorCandidate,
    PutSpreadCandidate,
    current_net_credit_mid_from_legs,
    tp_sl_triggered,
)
from bot.alpaca_put_spread.put_spread_selector import _get_chain_silent, select_bull_put_credit_spread
from bot.alpaca_put_spread.state import (
    ensure_state_shape,
    get_nested,
    load_state,
    pop_nested,
    save_state,
    set_nested,
)
from bot.alpaca_put_spread.strategy_types import StrategyType

from trading_assistant.broker.alpaca.market_data import get_latest_mid
from trading_assistant.broker.alpaca.positions import list_open_positions


logger = logging.getLogger(__name__)

# Minimum dollar slippage buffer on close (avoids zero debit limit when mark is tiny).
_MIN_CLOSE_SLIPPAGE_DOLLARS = 0.01


def _close_debit_limit_with_min_slippage(current_net: float, slippage_pct: float) -> float:
    """
    Positive debit limit to close a credit spread: current_net + slippage buffer.
    Percentage slippage is current_net * slippage_pct; if that amount is below
    _MIN_CLOSE_SLIPPAGE_DOLLARS, use the minimum instead (handles mark near zero).
    """
    c = float(current_net)
    slip_amount = c * float(slippage_pct)
    if slip_amount < _MIN_CLOSE_SLIPPAGE_DOLLARS:
        slip_amount = _MIN_CLOSE_SLIPPAGE_DOLLARS
    return c + slip_amount


_STRATEGY_PCS = StrategyType.PUT_CREDIT_SPREAD.value
_STRATEGY_CCS = StrategyType.CALL_CREDIT_SPREAD.value
_STRATEGY_IC = StrategyType.IRON_CONDOR.value

PENDING_ENTRY = "pending_entry_order"
PENDING_CLOSE = "pending_close_order"
COOLDOWN = "cooldown_until_ts"
ENTRY_DISABLED = "entry_disabled"
ENTRY_RETRY = "entry_retry_count"


def _open_spread_strategy_type(open_spread: Dict[str, Any]) -> str:
    return str(open_spread.get("strategy_type") or _STRATEGY_PCS).strip().upper()


def _spread_legs_from_state(open_spread: Dict[str, Any]) -> List[str]:
    legs = open_spread.get("legs")
    if isinstance(legs, list) and legs:
        return [str(x) for x in legs]
    st = _open_spread_strategy_type(open_spread)
    if st == _STRATEGY_CCS:
        sc = open_spread.get("short_call_symbol")
        lc = open_spread.get("long_call_symbol")
        if sc and lc:
            return [str(sc), str(lc)]
    if st == _STRATEGY_IC:
        lp = open_spread.get("long_put_symbol")
        sp = open_spread.get("short_put_symbol")
        sc = open_spread.get("short_call_symbol")
        lc = open_spread.get("long_call_symbol")
        if lp and sp and sc and lc:
            return [str(lp), str(sp), str(sc), str(lc)]
    short_sym = open_spread.get("short_put_symbol")
    long_sym = open_spread.get("long_put_symbol")
    if short_sym and long_sym:
        return [str(short_sym), str(long_sym)]
    return []


def _pricing_legs_from_open(open_spread: Dict[str, Any]) -> List[Leg]:
    st = _open_spread_strategy_type(open_spread)
    if st == _STRATEGY_PCS:
        return [
            Leg(symbol=open_spread["short_put_symbol"], side="sell", intent="open", ratio=1),
            Leg(symbol=open_spread["long_put_symbol"], side="buy", intent="open", ratio=1),
        ]
    if st == _STRATEGY_CCS:
        return [
            Leg(symbol=open_spread["short_call_symbol"], side="sell", intent="open", ratio=1),
            Leg(symbol=open_spread["long_call_symbol"], side="buy", intent="open", ratio=1),
        ]
    if st == _STRATEGY_IC:
        return [
            Leg(symbol=open_spread["long_put_symbol"], side="buy", intent="open", ratio=1),
            Leg(symbol=open_spread["short_put_symbol"], side="sell", intent="open", ratio=1),
            Leg(symbol=open_spread["short_call_symbol"], side="sell", intent="open", ratio=1),
            Leg(symbol=open_spread["long_call_symbol"], side="buy", intent="open", ratio=1),
        ]
    return []


def _entry_legs_pcs(candidate: PutSpreadCandidate) -> List[Dict[str, Any]]:
    return [
        {
            "symbol": candidate.short_put_symbol,
            "ratio_qty": 1.0,
            "side": OrderSide.SELL,
            "position_intent": PositionIntent.SELL_TO_OPEN,
        },
        {
            "symbol": candidate.long_put_symbol,
            "ratio_qty": 1.0,
            "side": OrderSide.BUY,
            "position_intent": PositionIntent.BUY_TO_OPEN,
        },
    ]


def _entry_legs_ccs(candidate: CallSpreadCandidate) -> List[Dict[str, Any]]:
    return [
        {
            "symbol": candidate.short_call_symbol,
            "ratio_qty": 1.0,
            "side": OrderSide.SELL,
            "position_intent": PositionIntent.SELL_TO_OPEN,
        },
        {
            "symbol": candidate.long_call_symbol,
            "ratio_qty": 1.0,
            "side": OrderSide.BUY,
            "position_intent": PositionIntent.BUY_TO_OPEN,
        },
    ]


def _entry_legs_ic(candidate: IronCondorCandidate) -> List[Dict[str, Any]]:
    return [
        {
            "symbol": candidate.long_put_symbol,
            "ratio_qty": 1.0,
            "side": OrderSide.BUY,
            "position_intent": PositionIntent.BUY_TO_OPEN,
        },
        {
            "symbol": candidate.short_put_symbol,
            "ratio_qty": 1.0,
            "side": OrderSide.SELL,
            "position_intent": PositionIntent.SELL_TO_OPEN,
        },
        {
            "symbol": candidate.short_call_symbol,
            "ratio_qty": 1.0,
            "side": OrderSide.SELL,
            "position_intent": PositionIntent.SELL_TO_OPEN,
        },
        {
            "symbol": candidate.long_call_symbol,
            "ratio_qty": 1.0,
            "side": OrderSide.BUY,
            "position_intent": PositionIntent.BUY_TO_OPEN,
        },
    ]


def _exit_legs_from_open(open_spread: Dict[str, Any]) -> List[Dict[str, Any]]:
    st = _open_spread_strategy_type(open_spread)
    if st == _STRATEGY_PCS:
        short_sym = open_spread["short_put_symbol"]
        long_sym = open_spread["long_put_symbol"]
        return [
            {"symbol": short_sym, "ratio_qty": 1.0, "side": OrderSide.BUY, "position_intent": PositionIntent.BUY_TO_CLOSE},
            {"symbol": long_sym, "ratio_qty": 1.0, "side": OrderSide.SELL, "position_intent": PositionIntent.SELL_TO_CLOSE},
        ]
    if st == _STRATEGY_CCS:
        short_sym = open_spread["short_call_symbol"]
        long_sym = open_spread["long_call_symbol"]
        return [
            {"symbol": short_sym, "ratio_qty": 1.0, "side": OrderSide.BUY, "position_intent": PositionIntent.BUY_TO_CLOSE},
            {"symbol": long_sym, "ratio_qty": 1.0, "side": OrderSide.SELL, "position_intent": PositionIntent.SELL_TO_CLOSE},
        ]
    if st == _STRATEGY_IC:
        return [
            {
                "symbol": open_spread["long_put_symbol"],
                "ratio_qty": 1.0,
                "side": OrderSide.SELL,
                "position_intent": PositionIntent.SELL_TO_CLOSE,
            },
            {
                "symbol": open_spread["short_put_symbol"],
                "ratio_qty": 1.0,
                "side": OrderSide.BUY,
                "position_intent": PositionIntent.BUY_TO_CLOSE,
            },
            {
                "symbol": open_spread["short_call_symbol"],
                "ratio_qty": 1.0,
                "side": OrderSide.BUY,
                "position_intent": PositionIntent.BUY_TO_CLOSE,
            },
            {
                "symbol": open_spread["long_call_symbol"],
                "ratio_qty": 1.0,
                "side": OrderSide.SELL,
                "position_intent": PositionIntent.SELL_TO_CLOSE,
            },
        ]
    raise ValueError(f"Unknown strategy for exit legs: {st}")


def _exit_cfg(cfg: AlpacaPutSpreadConfig, strategy_type: str) -> SimpleNamespace:
    if strategy_type == _STRATEGY_PCS:
        x = cfg.put_credit_spread
        min_otm = float(x.min_short_otm_percent)
    elif strategy_type == _STRATEGY_CCS:
        x = cfg.call_credit_spread
        min_otm = float(x.min_short_otm_percent)
    elif strategy_type == _STRATEGY_IC:
        x = cfg.iron_condor
        min_otm = float(x.min_short_otm_percent) if x.min_short_otm_percent is not None else 0.02
    else:
        raise ValueError(strategy_type)
    return SimpleNamespace(
        tp_pct=x.tp_pct,
        sl_pct=x.sl_pct,
        distance_buffer_otm_fraction=x.distance_buffer_otm_fraction_of_min_short_otm,
        distance_buffer_points=x.distance_buffer_points,
        min_short_otm_percent=min_otm,
        exit_before_minutes=x.exit_before_minutes,
        exit_cooldown_minutes=x.exit_cooldown_minutes,
        close_limit_slippage_pct=x.close_limit_slippage_pct,
    )


def _option_mid(option_data_client: Any, symbol: str) -> Optional[float]:
    qreq = OptionLatestQuoteRequest(symbol_or_symbols=[symbol])
    quotes = option_data_client.get_option_latest_quote(qreq)
    q = quotes.get(symbol) if isinstance(quotes, dict) else None
    if not q:
        return None
    bid = getattr(q, "bid_price", None)
    ask = getattr(q, "ask_price", None)
    try:
        bid_f = float(bid) if bid is not None else None
        ask_f = float(ask) if ask is not None else None
        if bid_f is not None and ask_f is not None and bid_f > 0 and ask_f > 0:
            return (bid_f + ask_f) / 2.0
        if ask_f is not None and ask_f > 0:
            return ask_f
        if bid_f is not None and bid_f > 0:
            return bid_f
    except Exception:
        return None
    return None


def _option_bid_ask(option_data_client: Any, symbol: str) -> Tuple[Optional[float], Optional[float]]:
    qreq = OptionLatestQuoteRequest(symbol_or_symbols=[symbol])
    quotes = option_data_client.get_option_latest_quote(qreq)
    q = quotes.get(symbol) if isinstance(quotes, dict) else None
    if not q:
        return (None, None)
    bid = getattr(q, "bid_price", None)
    ask = getattr(q, "ask_price", None)
    try:
        bid_f = float(bid) if bid is not None else None
        ask_f = float(ask) if ask is not None else None
        return (bid_f, ask_f)
    except Exception:
        return (None, None)


def _compute_distance_points_put_short(short_put_symbol: str, underlying_mid: float) -> Optional[float]:
    parts = parse_occ_option_symbol(short_put_symbol)
    if not parts:
        return None
    return float(parts.strike) - float(underlying_mid)


def _compute_distance_points_call_short(short_call_symbol: str, underlying_mid: float) -> Optional[float]:
    parts = parse_occ_option_symbol(short_call_symbol)
    if not parts:
        return None
    return float(underlying_mid) - float(parts.strike)


def _compute_otm_percent_put_short(short_put_symbol: str, underlying_mid: float) -> Optional[float]:
    if underlying_mid <= 0:
        return None
    parts = parse_occ_option_symbol(short_put_symbol)
    if not parts:
        return None
    return (float(underlying_mid) - float(parts.strike)) / float(underlying_mid)


def _compute_otm_percent_call_short(short_call_symbol: str, underlying_mid: float) -> Optional[float]:
    if underlying_mid <= 0:
        return None
    parts = parse_occ_option_symbol(short_call_symbol)
    if not parts:
        return None
    return (float(parts.strike) - float(underlying_mid)) / float(underlying_mid)


def _distance_gate_met(
    open_spread: Dict[str, Any],
    underlying_mid: float,
    strategy_type: str,
    ec: SimpleNamespace,
) -> Tuple[bool, Optional[str]]:
    if ec.distance_buffer_otm_fraction is not None:
        threshold_otm_pct = float(ec.min_short_otm_percent) * float(ec.distance_buffer_otm_fraction)
        if strategy_type == _STRATEGY_PCS:
            otm = _compute_otm_percent_put_short(open_spread["short_put_symbol"], underlying_mid)
            if otm is not None and otm <= threshold_otm_pct:
                return True, f"otm_gate_put otm_pct={otm:.4f}<={threshold_otm_pct:.4f}"
        elif strategy_type == _STRATEGY_CCS:
            otm = _compute_otm_percent_call_short(open_spread["short_call_symbol"], underlying_mid)
            if otm is not None and otm <= threshold_otm_pct:
                return True, f"otm_gate_call otm_pct={otm:.4f}<={threshold_otm_pct:.4f}"
        elif strategy_type == _STRATEGY_IC:
            otm_p = _compute_otm_percent_put_short(open_spread["short_put_symbol"], underlying_mid)
            otm_c = _compute_otm_percent_call_short(open_spread["short_call_symbol"], underlying_mid)
            if otm_p is not None and otm_p <= threshold_otm_pct:
                return True, f"otm_gate_ic_put otm_pct={otm_p:.4f}<={threshold_otm_pct:.4f}"
            if otm_c is not None and otm_c <= threshold_otm_pct:
                return True, f"otm_gate_ic_call otm_pct={otm_c:.4f}<={threshold_otm_pct:.4f}"
    else:
        if strategy_type == _STRATEGY_PCS:
            d = _compute_distance_points_put_short(open_spread["short_put_symbol"], underlying_mid)
            if d is not None and d >= ec.distance_buffer_points:
                return True, f"distance_buffer_put d={d:.4f}"
        elif strategy_type == _STRATEGY_CCS:
            d = _compute_distance_points_call_short(open_spread["short_call_symbol"], underlying_mid)
            if d is not None and d >= ec.distance_buffer_points:
                return True, f"distance_buffer_call d={d:.4f}"
        elif strategy_type == _STRATEGY_IC:
            d_p = _compute_distance_points_put_short(open_spread["short_put_symbol"], underlying_mid)
            d_c = _compute_distance_points_call_short(open_spread["short_call_symbol"], underlying_mid)
            if d_p is not None and d_p >= ec.distance_buffer_points:
                return True, f"distance_buffer_ic_put d={d_p:.4f}"
            if d_c is not None and d_c >= ec.distance_buffer_points:
                return True, f"distance_buffer_ic_call d={d_c:.4f}"
    return False, None


def _expiry_symbol_for_open(open_spread: Dict[str, Any]) -> str:
    st = _open_spread_strategy_type(open_spread)
    if st == _STRATEGY_CCS:
        return str(open_spread["short_call_symbol"])
    if st == _STRATEGY_IC:
        return str(open_spread["short_put_symbol"])
    return str(open_spread["short_put_symbol"])


class AlpacaPutSpreadRunner:
    def __init__(
        self,
        *,
        trading_client: TradingClient,
        stock_data_client: Any,
        option_data_client: Any,
        cfg: AlpacaPutSpreadConfig,
    ) -> None:
        self.trading_client = trading_client
        self.stock_data_client = stock_data_client
        self.option_data_client = option_data_client
        self.cfg = cfg
        self.state = load_state()
        ensure_state_shape(self.state)

    def _enabled_strategies(self) -> List[str]:
        out: List[str] = []
        if self.cfg.put_credit_spread.enabled:
            out.append(_STRATEGY_PCS)
        if self.cfg.call_credit_spread.enabled:
            out.append(_STRATEGY_CCS)
        if self.cfg.iron_condor.enabled:
            out.append(_STRATEGY_IC)
        return out

    def _underlying_may_hunt_entry(self, underlying: str) -> bool:
        """True if any enabled strategy could reach chain selection (no open, no pending, not disabled)."""
        for st in self._enabled_strategies():
            if get_nested(self.state, "open_positions", underlying, st):
                continue
            if get_nested(self.state, PENDING_ENTRY, underlying, st):
                continue
            if get_nested(self.state, ENTRY_DISABLED, underlying, st):
                continue
            return True
        return False

    def _is_terminal_order_status(self, status: str) -> bool:
        s = (status or "").lower()
        s = s.split(".")[-1]
        return s in ("filled", "canceled", "rejected", "expired")

    def _get_order_status(self, order_id: str) -> str:
        try:
            o = self.trading_client.get_order_by_id(order_id)
            s = str(getattr(o, "status", "")).lower()
            return s.split(".")[-1]
        except Exception:
            return "unknown"

    def _within_hunt_window(self) -> bool:
        if not self.cfg.trade_window_timezone or not self.cfg.trade_window_start_time_local or not self.cfg.trade_window_end_time_local:
            return True
        tz = pytz.timezone(self.cfg.trade_window_timezone)
        now_local = datetime.now(tz)
        if now_local.weekday() not in self.cfg.trade_window_weekdays:
            return False
        start_t = datetime.strptime(self.cfg.trade_window_start_time_local, "%H:%M").time()
        end_t = datetime.strptime(self.cfg.trade_window_end_time_local, "%H:%M").time()
        if start_t <= end_t:
            return start_t <= now_local.time() <= end_t
        return now_local.time() >= start_t or now_local.time() <= end_t

    def _clear_open_spread_if_not_in_positions(self, underlying: str, strategy_type: str) -> None:
        open_spread = get_nested(self.state, "open_positions", underlying, strategy_type)
        if not open_spread:
            return
        if get_nested(self.state, PENDING_CLOSE, underlying, strategy_type):
            return
        try:
            open_positions = list_open_positions(self.trading_client)
        except Exception:
            return
        syms = _spread_legs_from_state(open_spread)
        if not syms:
            return
        all_flat = all(float(open_positions.get(s, {}).get("qty", 0.0) or 0.0) == 0.0 for s in syms)
        if all_flat:
            logger.info("[%s][%s] Clearing stale open state (legs flat)", underlying, strategy_type)
            pop_nested(self.state, "open_positions", underlying, strategy_type)
            save_state(self.state)

    def _maybe_close(self, underlying: str, open_spread: Dict[str, Any], strategy_type: str) -> bool:
        st = _open_spread_strategy_type(open_spread)
        if st != strategy_type:
            logger.warning("[%s] strategy_type mismatch open=%s loop=%s", underlying, st, strategy_type)
        ec = _exit_cfg(self.cfg, strategy_type)
        reason: Optional[str] = None

        underlying_mid = get_latest_mid(self.stock_data_client, underlying)
        if underlying_mid <= 0:
            return False

        # Expiry cutoff closes regardless of distance gate or TP/SL.
        exp_sym = _expiry_symbol_for_open(open_spread)
        expiry_utc = option_expiry_utc(exp_sym)
        if expiry_utc is not None:
            now_utc = datetime.now(timezone.utc)
            cutoff = expiry_utc - timedelta(minutes=ec.exit_before_minutes)
            if now_utc >= cutoff:
                reason = "expiry_cutoff"

        # Distance / OTM gate only unlocks TP/SL; proximity alone must not force a close.
        if reason is None:
            distance_gate_met, _ = _distance_gate_met(open_spread, underlying_mid, strategy_type, ec)
            if distance_gate_met:
                legs = _pricing_legs_from_open(open_spread)
                entry_credit = float(open_spread["entry_net_credit_mid"])
                triggered, tp_sl_tag = tp_sl_triggered(
                    None,
                    entry_credit,
                    ec.tp_pct,
                    ec.sl_pct,
                    legs=legs,
                    bid_ask_for=lambda sym: _option_bid_ask(self.option_data_client, sym),
                )
                if triggered:
                    reason = tp_sl_tag

        if not reason:
            return False

        logger.info("[%s][%s] Closing (%s)", underlying, strategy_type, reason)
        if "tp" in (reason or "").lower():
            alert_tp_triggered(underlying, reason or "")
        elif "sl" in (reason or "").lower():
            alert_sl_triggered(underlying, reason or "")
        log_event(
            "close_triggered",
            reason or "",
            underlying=underlying,
            extra={"reason": reason, "strategy_type": strategy_type},
        )

        if not self.cfg.execute:
            logger.info("[%s][%s] execute=false; would close now", underlying, strategy_type)
            return False

        pending_close = get_nested(self.state, PENDING_CLOSE, underlying, strategy_type)
        if pending_close:
            return self._poll_pending_close(underlying, open_spread, strategy_type, pending_close, ec)

        legs_p = _pricing_legs_from_open(open_spread)
        current_net = current_net_credit_mid_from_legs(
            legs_p,
            lambda sym: _option_bid_ask(self.option_data_client, sym),
        )
        if current_net is None or current_net <= 0:
            current_net = float(open_spread.get("entry_net_credit_mid", 0.0))
        if current_net <= 0:
            logger.warning("[%s][%s] Cannot compute positive debit to close; skip", underlying, strategy_type)
            return False

        close_debit_limit = _close_debit_limit_with_min_slippage(
            float(current_net), ec.close_limit_slippage_pct
        )
        close_legs = _exit_legs_from_open(open_spread)
        client_order_id = f"alpaca_{strategy_type.lower()}_close:{underlying}:{int(time.time())}"
        order_id = submit_mleg_limit_order(
            self.trading_client,
            qty=int(self.cfg.order_qty),
            limit_price=float(close_debit_limit),
            legs=close_legs,
            client_order_id=client_order_id,
        )
        logger.info("[%s][%s] Submitted close order_id=%s debit_limit=%.4f", underlying, strategy_type, order_id, close_debit_limit)

        db_syms = _spread_legs_from_state(open_spread)
        db_insert_order(
            order_id=order_id,
            underlying=underlying,
            side="close",
            status="submitted",
            client_order_id=client_order_id,
            strategy_type=strategy_type,
            legs=db_syms or None,
            short_put_symbol=open_spread.get("short_put_symbol"),
            long_put_symbol=open_spread.get("long_put_symbol"),
            limit_price=close_debit_limit,
            qty=self.cfg.order_qty,
            raw_snapshot={"strategy_type": strategy_type, "legs": db_syms},
        )
        log_event("close_submitted", f"order_id={order_id}", underlying=underlying, extra={"order_id": order_id, "strategy_type": strategy_type})
        alert_close_submitted(underlying, order_id, reason or "close", close_debit_limit)

        set_nested(
            self.state,
            PENDING_CLOSE,
            underlying,
            strategy_type,
            {
                "order_id": order_id,
                "submitted_at_ts": time.time(),
                "close_debit_limit": close_debit_limit,
                "close_reason": reason,
            },
        )
        save_state(self.state)

        final = wait_for_order(
            self.trading_client,
            order_id,
            timeout_seconds=self.cfg.order_fill_timeout_seconds,
        )
        status = str(getattr(final, "status", "")).lower()
        if status != "filled":
            try:
                cancel_order(self.trading_client, order_id)
            except Exception:
                pass
            pop_nested(self.state, PENDING_CLOSE, underlying, strategy_type)
            save_state(self.state)
            logger.warning("[%s][%s] Close not filled status=%s", underlying, strategy_type, status)
            return False

        pending_close = get_nested(self.state, PENDING_CLOSE, underlying, strategy_type) or {}
        return self._finalize_close_fill(underlying, open_spread, strategy_type, order_id, pending_close, ec)

    def _poll_pending_close(
        self,
        underlying: str,
        open_spread: Dict[str, Any],
        strategy_type: str,
        pending_close: Dict[str, Any],
        ec: SimpleNamespace,
    ) -> bool:
        order_id = pending_close.get("order_id")
        submitted_at_ts = float(pending_close.get("submitted_at_ts", 0.0) or 0.0)
        if not order_id:
            pop_nested(self.state, PENDING_CLOSE, underlying, strategy_type)
            save_state(self.state)
            return False

        status = self._get_order_status(order_id)
        if status == "filled":
            return self._finalize_close_fill(underlying, open_spread, strategy_type, order_id, pending_close, ec)

        if self._is_terminal_order_status(status) and status != "filled":
            pop_nested(self.state, PENDING_CLOSE, underlying, strategy_type)
            save_state(self.state)
            logger.warning("[%s][%s] Close terminal status=%s", underlying, strategy_type, status)
            return False

        if submitted_at_ts and (time.time() - submitted_at_ts) >= self.cfg.order_fill_timeout_seconds:
            try:
                cancel_order(self.trading_client, order_id)
            except Exception:
                pass
            pop_nested(self.state, PENDING_CLOSE, underlying, strategy_type)
            save_state(self.state)
            logger.warning("[%s][%s] Close timed out", underlying, strategy_type)
            return False
        return False

    def _finalize_close_fill(
        self,
        underlying: str,
        open_spread: Dict[str, Any],
        strategy_type: str,
        order_id: str,
        pending_close: Dict[str, Any],
        ec: SimpleNamespace,
    ) -> bool:
        spread_id = open_spread.get("spread_id")
        entry_credit = float(open_spread.get("entry_net_credit_mid", 0.0))
        close_debit = float(pending_close.get("close_debit_limit", 0.0) or 0.0)
        close_reason = pending_close.get("close_reason", "close")
        if spread_id and close_debit > 0:
            db_close_spread(spread_id, order_id, close_debit, close_reason, entry_credit, self.cfg.order_qty)
            pnl = (entry_credit - close_debit) * 100.0 * self.cfg.order_qty
            alert_close_filled(underlying, order_id, close_reason, pnl)
        db_update_order_status(order_id, "filled")
        log_event("close_filled", f"order_id={order_id}", underlying=underlying, extra={"order_id": order_id, "strategy_type": strategy_type})

        pop_nested(self.state, PENDING_CLOSE, underlying, strategy_type)
        pop_nested(self.state, "open_positions", underlying, strategy_type)
        cooldown_until = time.time() + int(ec.exit_cooldown_minutes) * 60
        set_nested(self.state, COOLDOWN, underlying, strategy_type, cooldown_until)
        save_state(self.state)
        logger.info("[%s][%s] Close filled; state cleared", underlying, strategy_type)
        return True

    def _maybe_open(self, underlying: str, strategy_type: str, chain: Optional[Any] = None) -> None:
        logger.info("[%s][%s] scanning for candidate...", underlying, strategy_type)
        if get_nested(self.state, "open_positions", underlying, strategy_type):
            return
        if get_nested(self.state, ENTRY_DISABLED, underlying, strategy_type):
            return

        pending_entry = get_nested(self.state, PENDING_ENTRY, underlying, strategy_type)
        if pending_entry:
            self._handle_pending_entry(underlying, strategy_type, pending_entry)
            return

        if not self._within_hunt_window():
            logger.info("[%s][%s] gated by trade window", underlying, strategy_type)
            return

        if self.cfg.max_daily_loss_dollars > 0:
            daily_pnl = get_daily_pnl()
            if daily_pnl <= -self.cfg.max_daily_loss_dollars:
                logger.warning("[%s] blocked by daily loss cap", underlying)
                log_event("loss_cap_block", f"daily PnL={daily_pnl:.2f}", underlying=underlying)
                alert_loss_cap_hit(None, daily_pnl, self.cfg.max_daily_loss_dollars)
                return
        if self.cfg.max_loss_per_underlying_dollars > 0:
            und_pnl = get_pnl_by_underlying_today(underlying)
            if und_pnl <= -self.cfg.max_loss_per_underlying_dollars:
                logger.warning("[%s] blocked by per-underlying loss cap", underlying)
                log_event("loss_cap_block", f"{underlying} PnL={und_pnl:.2f}", underlying=underlying)
                alert_loss_cap_hit(underlying, und_pnl, self.cfg.max_loss_per_underlying_dollars)
                return

        cooldown_until_ts = float(get_nested(self.state, COOLDOWN, underlying, strategy_type) or 0.0)
        if cooldown_until_ts and time.time() < cooldown_until_ts:
            logger.info("[%s][%s] cooldown active", underlying, strategy_type)
            return

        underlying_mid = get_latest_mid(self.stock_data_client, underlying)
        spot = underlying_mid if underlying_mid and underlying_mid > 0 else None

        candidate: Any = None
        entry_legs: List[Dict[str, Any]] = []
        try:
            if strategy_type == _STRATEGY_PCS:
                candidate = select_bull_put_credit_spread(
                    underlying=underlying,
                    option_data_client=self.option_data_client,
                    cfg=self.cfg.put_credit_spread,
                    underlying_spot_mid=spot,
                    chain=chain,
                )
                if candidate:
                    entry_legs = _entry_legs_pcs(candidate)
            elif strategy_type == _STRATEGY_CCS:
                candidate = select_bear_call_credit_spread(
                    underlying=underlying,
                    option_data_client=self.option_data_client,
                    cfg=self.cfg.call_credit_spread,
                    underlying_spot_mid=spot,
                    chain=chain,
                )
                if candidate:
                    entry_legs = _entry_legs_ccs(candidate)
            elif strategy_type == _STRATEGY_IC:
                candidate = select_iron_condor(
                    underlying=underlying,
                    option_data_client=self.option_data_client,
                    cfg=self.cfg.iron_condor,
                    underlying_spot_mid=spot,
                    chain=chain,
                )
                if candidate:
                    entry_legs = _entry_legs_ic(candidate)
        except Exception as e:
            logger.warning("[%s][%s] selection failed: %s", underlying, strategy_type, e)
            log_event("api_error", str(e), underlying=underlying, extra={"operation": "option_chain", "strategy_type": strategy_type})
            alert_api_error(underlying, "option_chain", str(e))
            return

        if not candidate:
            logger.info("[%s][%s] no candidate", underlying, strategy_type)
            return

        if not self.cfg.execute:
            logger.info("[%s][%s] execute=false; would submit entry", underlying, strategy_type)
            return

        entry_net_credit_mid = float(getattr(candidate, "entry_net_credit_mid", 0.0) or 0.0)
        if entry_net_credit_mid <= 0:
            logger.warning(
                "[%s][%s] invalid entry_net_credit_mid=%s; skip entry",
                underlying,
                strategy_type,
                entry_net_credit_mid,
            )
            return

        # Alpaca credit: negative limit; use live mid from candidate (YAML target_credit is selection-only).
        entry_limit_price = -entry_net_credit_mid
        client_order_id = f"alpaca_{strategy_type.lower()}_open:{underlying}:{int(time.time())}"
        order_id = submit_mleg_limit_order(
            self.trading_client,
            qty=int(self.cfg.order_qty),
            limit_price=entry_limit_price,
            legs=entry_legs,
            client_order_id=client_order_id,
        )

        db_syms = _spread_legs_from_state(_open_dict_from_candidate(strategy_type, candidate))
        short_p = long_p = None
        if strategy_type == _STRATEGY_PCS:
            short_p = candidate.short_put_symbol
            long_p = candidate.long_put_symbol
        db_insert_order(
            order_id=order_id,
            underlying=underlying,
            side="open",
            status="submitted",
            client_order_id=client_order_id,
            strategy_type=strategy_type,
            legs=db_syms or None,
            short_put_symbol=short_p,
            long_put_symbol=long_p,
            limit_price=entry_limit_price,
            qty=self.cfg.order_qty,
            raw_snapshot={"strategy_type": strategy_type, "legs": db_syms},
        )
        log_event("entry_submitted", f"order_id={order_id}", underlying=underlying, extra={"order_id": order_id, "strategy_type": strategy_type})
        alert_entry_submitted(underlying, order_id, entry_net_credit_mid)

        snap = _pending_snapshot_from_candidate(strategy_type, candidate)
        snap["order_id"] = order_id
        set_nested(self.state, PENDING_ENTRY, underlying, strategy_type, snap)
        save_state(self.state)
        logger.info("[%s][%s] Submitted entry order_id=%s", underlying, strategy_type, order_id)

    def _handle_pending_entry(self, underlying: str, strategy_type: str, pending_entry: Dict[str, Any]) -> None:
        order_id = pending_entry.get("order_id")
        if not order_id:
            pop_nested(self.state, PENDING_ENTRY, underlying, strategy_type)
            save_state(self.state)
            return

        status = self._get_order_status(order_id)
        if status == "filled":
            entry_credit_mid = float(pending_entry.get("entry_net_credit_mid", 0.0))
            syms = _spread_legs_from_state(_open_dict_from_pending(strategy_type, pending_entry))
            if not syms:
                pop_nested(self.state, PENDING_ENTRY, underlying, strategy_type)
                save_state(self.state)
                logger.warning("[%s][%s] Entry filled but no leg symbols in pending; cleared", underlying, strategy_type)
                return

            legs_for_recalc = _pricing_legs_from_open(_open_dict_from_pending(strategy_type, pending_entry))
            recalc = current_net_credit_mid_from_legs(
                legs_for_recalc,
                lambda sym: _option_bid_ask(self.option_data_client, sym),
            )
            if recalc is not None:
                entry_credit_mid = float(recalc)

            underlying_mid = get_latest_mid(self.stock_data_client, underlying)
            spread_id = db_insert_spread(
                underlying=underlying,
                entry_credit_mid=entry_credit_mid,
                entry_order_id=order_id,
                strategy_type=strategy_type,
                legs=syms,
                short_put_symbol=pending_entry.get("short_put_symbol"),
                long_put_symbol=pending_entry.get("long_put_symbol"),
            )
            db_update_order_status(order_id, "filled")
            open_spread = _open_spread_from_pending(strategy_type, pending_entry, spread_id, entry_credit_mid, underlying_mid)
            set_nested(self.state, "open_positions", underlying, strategy_type, open_spread)
            pop_nested(self.state, PENDING_ENTRY, underlying, strategy_type)
            pop_nested(self.state, ENTRY_RETRY, underlying, strategy_type)
            pop_nested(self.state, ENTRY_DISABLED, underlying, strategy_type)
            save_state(self.state)
            log_event(
                "entry_filled",
                f"order_id={order_id} spread_id={spread_id}",
                underlying=underlying,
                extra={"order_id": order_id, "spread_id": spread_id, "strategy_type": strategy_type},
            )
            alert_entry_filled(underlying, order_id, entry_credit_mid)
            logger.info("[%s][%s] Entry filled spread_id=%s", underlying, strategy_type, spread_id)
            return

        if self._is_terminal_order_status(status) and status != "filled":
            pop_nested(self.state, PENDING_ENTRY, underlying, strategy_type)
            set_nested(self.state, ENTRY_DISABLED, underlying, strategy_type, True)
            save_state(self.state)
            logger.warning("[%s][%s] Entry terminal status=%s -> disabled", underlying, strategy_type, status)
            return

    def run_forever(self) -> None:
        while True:
            for underlying in self.cfg.get_underlyings_for_today():
                shared_chain: Optional[Any] = None
                if self._underlying_may_hunt_entry(underlying):
                    try:
                        shared_chain = _get_chain_silent(self.option_data_client, underlying)
                    except Exception as e:
                        logger.warning(
                            "[%s] Shared option chain fetch failed (selectors will refetch if needed): %s",
                            underlying,
                            e,
                        )
                        shared_chain = None

                for strategy_type in self._enabled_strategies():
                    try:
                        self._clear_open_spread_if_not_in_positions(underlying, strategy_type)
                        open_spread = get_nested(self.state, "open_positions", underlying, strategy_type)
                        if open_spread:
                            self._maybe_close(underlying, open_spread, strategy_type)
                        else:
                            self._maybe_open(underlying, strategy_type, chain=shared_chain)
                    except Exception as e:
                        logger.exception("[%s][%s] Loop error: %s", underlying, strategy_type, e)
            time.sleep(self.cfg.loop_sleep_seconds)


def _open_dict_from_candidate(strategy_type: str, candidate: Any) -> Dict[str, Any]:
    if strategy_type == _STRATEGY_PCS:
        c = candidate
        return {
            "strategy_type": strategy_type,
            "short_put_symbol": c.short_put_symbol,
            "long_put_symbol": c.long_put_symbol,
            "legs": [c.short_put_symbol, c.long_put_symbol],
        }
    if strategy_type == _STRATEGY_CCS:
        c = candidate
        return {
            "strategy_type": strategy_type,
            "short_call_symbol": c.short_call_symbol,
            "long_call_symbol": c.long_call_symbol,
            "legs": [c.short_call_symbol, c.long_call_symbol],
        }
    c = candidate
    return {
        "strategy_type": strategy_type,
        "long_put_symbol": c.long_put_symbol,
        "short_put_symbol": c.short_put_symbol,
        "short_call_symbol": c.short_call_symbol,
        "long_call_symbol": c.long_call_symbol,
        "legs": [c.long_put_symbol, c.short_put_symbol, c.short_call_symbol, c.long_call_symbol],
    }


def _pending_snapshot_from_candidate(strategy_type: str, candidate: Any) -> Dict[str, Any]:
    snap: Dict[str, Any] = {
        "submitted_at_ts": time.time(),
        "strategy_type": strategy_type,
        "entry_net_credit_mid": float(candidate.entry_net_credit_mid),
    }
    if strategy_type == _STRATEGY_PCS:
        snap["short_put_symbol"] = candidate.short_put_symbol
        snap["long_put_symbol"] = candidate.long_put_symbol
    elif strategy_type == _STRATEGY_CCS:
        snap["short_call_symbol"] = candidate.short_call_symbol
        snap["long_call_symbol"] = candidate.long_call_symbol
    else:
        snap["long_put_symbol"] = candidate.long_put_symbol
        snap["short_put_symbol"] = candidate.short_put_symbol
        snap["short_call_symbol"] = candidate.short_call_symbol
        snap["long_call_symbol"] = candidate.long_call_symbol
    return snap


def _open_dict_from_pending(strategy_type: str, pending: Dict[str, Any]) -> Dict[str, Any]:
    d = dict(pending)
    d["strategy_type"] = strategy_type
    return d


def _open_spread_from_pending(
    strategy_type: str,
    pending: Dict[str, Any],
    spread_id: int,
    entry_credit_mid: float,
    underlying_mid: Optional[float],
) -> Dict[str, Any]:
    base = {
        "spread_id": spread_id,
        "entry_order_id": pending.get("order_id"),
        "entry_net_credit_mid": float(entry_credit_mid),
        "entry_underlying_mid": float(underlying_mid) if underlying_mid else None,
        "opened_at_ts": float(pending.get("submitted_at_ts", time.time())),
        "strategy_type": strategy_type,
    }
    if strategy_type == _STRATEGY_PCS:
        base["short_put_symbol"] = pending["short_put_symbol"]
        base["long_put_symbol"] = pending["long_put_symbol"]
        base["legs"] = [pending["short_put_symbol"], pending["long_put_symbol"]]
    elif strategy_type == _STRATEGY_CCS:
        base["short_call_symbol"] = pending["short_call_symbol"]
        base["long_call_symbol"] = pending["long_call_symbol"]
        base["legs"] = [pending["short_call_symbol"], pending["long_call_symbol"]]
    else:
        base["long_put_symbol"] = pending["long_put_symbol"]
        base["short_put_symbol"] = pending["short_put_symbol"]
        base["short_call_symbol"] = pending["short_call_symbol"]
        base["long_call_symbol"] = pending["long_call_symbol"]
        base["legs"] = [
            pending["long_put_symbol"],
            pending["short_put_symbol"],
            pending["short_call_symbol"],
            pending["long_call_symbol"],
        ]
    return base

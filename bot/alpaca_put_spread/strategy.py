from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Set, Tuple

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
    find_open_spread_id_by_legs,
    get_daily_pnl,
    get_pnl_by_underlying_today,
    get_spread_entry_credit_mid,
    init_alpaca_db,
    insert_order as db_insert_order,
    insert_spread as db_insert_spread,
    insert_telemetry as db_insert_telemetry,
    list_filled_close_orders_without_spread_link,
    log_event,
    merge_order_raw_snapshot,
    update_order_status as db_update_order_status,
)
from bot.alpaca_put_spread.domain import Leg
from bot.alpaca_put_spread.execution import (
    cancel_order,
    is_transient_request_error,
    retry_transient,
    submit_mleg_limit_order,
    wait_for_order,
)
from bot.alpaca_put_spread.iron_condor_selector import select_iron_condor
from bot.alpaca_put_spread.option_symbol import minutes_to_expiry_utc, option_expiry_utc, parse_occ_option_symbol
from bot.alpaca_put_spread.pricing_logic import (
    CallSpreadCandidate,
    IronCondorCandidate,
    PutSpreadCandidate,
    current_net_credit_mid_from_legs,
    estimate_close_debit_natural_from_open_legs,
    natural_close_debit_for_exit,
)
from bot.alpaca_put_spread.put_spread_selector import _get_chain_silent, _safe_get, select_bull_put_credit_spread
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


def _is_expected_broker_rejection(exc: BaseException) -> bool:
    """
    Alpaca rejections that indicate account/state mismatch or exchange rules — not code bugs.
    Avoid logger.exception() spam for these.
    """
    text = str(exc).lower()
    markers = (
        "insufficient qty",
        "40310000",
        "position intent mismatch",
        "42210000",
        "expires soon",
        "unable to open new positions",
        "held_for_orders",
    )
    if any(m in text for m in markers):
        return True
    try:
        from alpaca.common.exceptions import APIError as AlpacaAPIError

        if isinstance(exc, AlpacaAPIError):
            code = getattr(exc, "code", None)
            try:
                ic = int(code) if code is not None else None
            except (TypeError, ValueError):
                ic = None
            if ic in (40310000, 42210000):
                return True
    except Exception:
        pass
    return False


logger = logging.getLogger(__name__)

# Minimum dollar slippage buffer on close (avoids zero debit limit when mark is tiny).
_MIN_CLOSE_SLIPPAGE_DOLLARS = 0.01
_TELEMETRY_INTERVAL_SEC = 300.0


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


def _alpaca_limit_price(raw_price: float, *, is_credit: bool) -> float:
    """
    Normalize multi-leg limit price for Alpaca options:
    - max 2 decimal places (cent precision)
    - non-zero absolute value (>= $0.01)
    - credit orders use negative sign, debit orders use positive sign
    """
    q = Decimal(str(abs(float(raw_price)))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if q < Decimal("0.01"):
        q = Decimal("0.01")
    out = -q if is_credit else q
    return float(out)


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


def _expiry_symbol_for_candidate(strategy_type: str, candidate: Any) -> str:
    if strategy_type == _STRATEGY_PCS:
        return str(getattr(candidate, "short_put_symbol"))
    if strategy_type == _STRATEGY_CCS:
        return str(getattr(candidate, "short_call_symbol"))
    return str(getattr(candidate, "short_put_symbol"))


def _spread_qty_from_broker(
    legs: List[str],
    open_positions: Dict[str, Any],
    configured_qty: int,
) -> Tuple[str, int]:
    """
    Classify broker leg quantities vs a planned spread close size.

    Returns:
      ('flat', 0) — no contracts
      ('broken', 0) — missing leg vs others or unbalanced qty
      ('ok', q) — q = min(configured_qty, whole contracts common to all legs)
    """
    if not legs:
        return ("flat", 0)
    qtys: List[float] = []
    for s in legs:
        q = float(open_positions.get(s, {}).get("qty", 0.0) or 0.0)
        qtys.append(abs(q))
    mx = max(qtys)
    mn = min(qtys)
    if mx < 1e-9:
        return ("flat", 0)
    if mn < 1e-9:
        return ("broken", 0)
    if mx - mn > 0.01:
        return ("broken", 0)
    avail = int(min(float(configured_qty), mn))
    if avail < 1:
        return ("broken", 0)
    return ("ok", avail)


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
    """
    True when spot has moved close enough to the short strike to allow *stop-loss* exits.

    Take-profit and expiry-cutoff closes do not use this gate (see ``_maybe_close``).
    """
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


def _chain_snap_for_symbol(chain: Any, sym: str) -> Any:
    if isinstance(chain, dict):
        return chain.get(sym)
    snaps = getattr(chain, "snapshots", None)
    if isinstance(snaps, dict):
        return snaps.get(sym)
    return None


def _snap_delta_iv(snap: Any) -> Tuple[Optional[float], Optional[float]]:
    if not snap:
        return (None, None)
    try:
        d = _safe_get(snap, ["greeks", "delta"])
        iv = _safe_get(snap, ["implied_volatility"])
        d_f = float(d) if d is not None else None
        iv_f = float(iv) if iv is not None else None
        return (d_f, iv_f)
    except (TypeError, ValueError):
        return (None, None)


def _mean_optional(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None and b is None:
        return None
    if a is None:
        return b
    if b is None:
        return a
    return (a + b) / 2.0


def _distance_to_short_pct_for_open(
    open_spread: Dict[str, Any],
    underlying_mid: float,
    strategy_type: str,
) -> Optional[float]:
    """Single % distance metric aligned with the distance gate (short strike vs spot)."""
    if strategy_type == _STRATEGY_PCS:
        return _compute_otm_percent_put_short(open_spread["short_put_symbol"], underlying_mid)
    if strategy_type == _STRATEGY_CCS:
        return _compute_otm_percent_call_short(open_spread["short_call_symbol"], underlying_mid)
    if strategy_type == _STRATEGY_IC:
        op = _compute_otm_percent_put_short(open_spread["short_put_symbol"], underlying_mid)
        oc = _compute_otm_percent_call_short(open_spread["short_call_symbol"], underlying_mid)
        if op is None:
            return oc
        if oc is None:
            return op
        return min(op, oc)
    return None


def _entry_raw_snapshot_signature(
    option_client: Any,
    underlying: str,
    strategy_type: str,
    pending: Dict[str, Any],
    underlying_mid: float,
) -> Dict[str, Any]:
    """At fill time: short/long delta, OTM %, chain IV from option chain snapshots."""
    out: Dict[str, Any] = {
        "strategy_type": strategy_type,
        "underlying_mid": float(underlying_mid) if underlying_mid > 0 else None,
        "entry_ts": time.time(),
    }
    try:
        chain = _get_chain_silent(option_client, underlying)
    except Exception:
        return out

    if strategy_type == _STRATEGY_PCS:
        ss = str(pending.get("short_put_symbol") or "")
        ls = str(pending.get("long_put_symbol") or "")
        sd, s_iv = _snap_delta_iv(_chain_snap_for_symbol(chain, ss))
        ld, l_iv = _snap_delta_iv(_chain_snap_for_symbol(chain, ls))
        out["short_delta"] = sd
        out["long_delta"] = ld
        out["otm_percent"] = (
            _compute_otm_percent_put_short(ss, underlying_mid) if ss and underlying_mid > 0 else None
        )
        out["chain_iv"] = s_iv if s_iv is not None else l_iv
    elif strategy_type == _STRATEGY_CCS:
        ss = str(pending.get("short_call_symbol") or "")
        ls = str(pending.get("long_call_symbol") or "")
        sd, s_iv = _snap_delta_iv(_chain_snap_for_symbol(chain, ss))
        ld, l_iv = _snap_delta_iv(_chain_snap_for_symbol(chain, ls))
        out["short_delta"] = sd
        out["long_delta"] = ld
        out["otm_percent"] = (
            _compute_otm_percent_call_short(ss, underlying_mid) if ss and underlying_mid > 0 else None
        )
        out["chain_iv"] = s_iv if s_iv is not None else l_iv
    elif strategy_type == _STRATEGY_IC:
        lp = str(pending.get("long_put_symbol") or "")
        sp = str(pending.get("short_put_symbol") or "")
        sc = str(pending.get("short_call_symbol") or "")
        lc = str(pending.get("long_call_symbol") or "")
        d_sp, iv_sp = _snap_delta_iv(_chain_snap_for_symbol(chain, sp))
        d_sc, iv_sc = _snap_delta_iv(_chain_snap_for_symbol(chain, sc))
        d_lp, _ = _snap_delta_iv(_chain_snap_for_symbol(chain, lp))
        d_lc, _ = _snap_delta_iv(_chain_snap_for_symbol(chain, lc))
        out["short_delta"] = _mean_optional(d_sp, d_sc)
        out["long_delta"] = _mean_optional(d_lp, d_lc)
        otm_p = _compute_otm_percent_put_short(sp, underlying_mid) if sp and underlying_mid > 0 else None
        otm_c = _compute_otm_percent_call_short(sc, underlying_mid) if sc and underlying_mid > 0 else None
        if otm_p is None:
            out["otm_percent"] = otm_c
        elif otm_c is None:
            out["otm_percent"] = otm_p
        else:
            out["otm_percent"] = min(otm_p, otm_c)
        out["chain_iv"] = _mean_optional(iv_sp, iv_sc)
    return out


def _expiry_symbol_for_open(open_spread: Dict[str, Any]) -> str:
    st = _open_spread_strategy_type(open_spread)
    if st == _STRATEGY_CCS:
        return str(open_spread["short_call_symbol"])
    if st == _STRATEGY_IC:
        return str(open_spread["short_put_symbol"])
    return str(open_spread["short_put_symbol"])


def _close_reason_for_open_credit_spread(
    underlying: str,
    open_spread: Dict[str, Any],
    strategy_type: str,
    ec: SimpleNamespace,
    distance_gate_met: bool,
    legs_p: List[Leg],
    bid_ask_for: Any,
) -> Optional[str]:
    """
    Decide whether to close an open credit structure and why.

    Priority (first match wins):
      1. Take profit — natural debit-to-close <= entry * (1 - tp_pct). **Ignores distance gate.**
      2. Expiry cutoff — now >= expiry - exit_before_minutes. **Ignores distance gate.**
      3. Stop loss — natural debit-to-close >= entry * (1 + sl_pct) **only if distance_gate_met.**

    Uses :func:`natural_close_debit_for_exit` (natural ask/bid), same convention as pricing_logic TP/SL.
    """
    entry_credit = float(open_spread["entry_net_credit_mid"])
    current_debit = natural_close_debit_for_exit(None, legs=legs_p, bid_ask_for=bid_ask_for)

    # (1) Take profit — always on executable close estimate; never blocked by distance gate.
    if current_debit is not None and entry_credit > 0:
        tp_thr = entry_credit * (1.0 - ec.tp_pct)
        if float(current_debit) <= tp_thr:
            return "tp"

    # (2) Expiry cutoff — time-based; independent of distance gate.
    exp_sym = _expiry_symbol_for_open(open_spread)
    expiry_utc = option_expiry_utc(exp_sym)
    if expiry_utc is not None:
        now_utc = datetime.now(timezone.utc)
        cutoff = expiry_utc - timedelta(minutes=ec.exit_before_minutes)
        if now_utc >= cutoff:
            return "expiry_cutoff"

    # (3) Stop loss — requires distance gate so we do not stop out while spot is still "far" from short.
    if current_debit is not None and entry_credit > 0:
        sl_thr = entry_credit * (1.0 + ec.sl_pct)
        if float(current_debit) >= sl_thr:
            if distance_gate_met:
                return "sl"
            logger.debug(
                "[%s][%s] SL threshold hit (est_close_debit=%.4f >= %.4f) but distance gate closed; holding",
                underlying,
                strategy_type,
                float(current_debit),
                sl_thr,
            )

    return None


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
        # Log INFO once per (underlying, strategy) when entry_disabled blocks; DEBUG every loop if enabled.
        self._entry_disabled_notice_keys: Set[Tuple[str, str]] = set()
        self.last_telemetry_ts: Dict[int, float] = {}
        self.last_gate_open_by_spread: Dict[int, bool] = {}

    def _enabled_strategies(self) -> List[str]:
        out: List[str] = []
        if self.cfg.put_credit_spread.enabled:
            out.append(_STRATEGY_PCS)
        if self.cfg.call_credit_spread.enabled:
            out.append(_STRATEGY_CCS)
        if self.cfg.iron_condor.enabled:
            out.append(_STRATEGY_IC)
        return out

    @staticmethod
    def _all_strategy_type_keys() -> Tuple[str, str, str]:
        return (_STRATEGY_PCS, _STRATEGY_CCS, _STRATEGY_IC)

    def _underlying_active_entry_count(self, underlying: str) -> int:
        """
        Number of strategies with an open spread or a pending entry on this underlying
        (each strategy counts at most once).
        """
        n = 0
        for st in self._all_strategy_type_keys():
            if get_nested(self.state, "open_positions", underlying, st):
                n += 1
            elif get_nested(self.state, PENDING_ENTRY, underlying, st):
                n += 1
        return n

    def _underlying_active_strategy_types(self, underlying: str) -> Set[str]:
        """Strategy types that currently have an open spread or pending entry."""
        out: Set[str] = set()
        for st in self._all_strategy_type_keys():
            if get_nested(self.state, "open_positions", underlying, st) or get_nested(
                self.state, PENDING_ENTRY, underlying, st
            ):
                out.add(st)
        return out

    def _underlying_may_hunt_entry(self, underlying: str) -> bool:
        """
        True if prefetching a shared option chain might lead to a new entry for some enabled strategy.
        Respects ``max_open_spreads_per_underlying`` (0 = no numeric cap) and blocks cross-strategy
        mixing while any slot is active on this underlying.
        """
        cap = int(getattr(self.cfg, "max_open_spreads_per_underlying", 1) or 0)
        count = self._underlying_active_entry_count(underlying)
        if self.cfg.max_open_spreads_per_underlying > 0 and count >= self.cfg.max_open_spreads_per_underlying:
            return False
        active = self._underlying_active_strategy_types(underlying)
        for st in self._enabled_strategies():
            if get_nested(self.state, ENTRY_DISABLED, underlying, st):
                continue
            if get_nested(self.state, "open_positions", underlying, st):
                continue
            if get_nested(self.state, PENDING_ENTRY, underlying, st):
                continue
            if active and st not in active:
                continue
            return True
        return False

    def _is_terminal_order_status(self, status: str) -> bool:
        s = (status or "").lower()
        s = s.split(".")[-1]
        return s in ("filled", "canceled", "rejected", "expired")

    def _retry_api(self, fn):
        return retry_transient(
            fn,
            attempts=self.cfg.api_retry_attempts,
            backoff_seconds=self.cfg.api_retry_backoff_seconds,
        )

    def _exec_retry_kw(self) -> Dict[str, Any]:
        return {
            "retry_attempts": self.cfg.api_retry_attempts,
            "retry_backoff_seconds": self.cfg.api_retry_backoff_seconds,
        }

    def _close_fill_timeout_seconds(self, close_reason: str) -> int:
        """TP closes get a longer wait before timeout-cancel; SL / expiry / other use order_fill_timeout_seconds."""
        r = (close_reason or "").lower()
        if "tp" in r:
            return int(self.cfg.take_profit_order_fill_timeout_seconds)
        return int(self.cfg.order_fill_timeout_seconds)

    def _get_order_status(self, order_id: str) -> str:
        try:
            o = self._retry_api(lambda: self.trading_client.get_order_by_id(order_id))
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
            open_positions = self._retry_api(lambda: list_open_positions(self.trading_client))
        except Exception:
            return
        syms = _spread_legs_from_state(open_spread)
        if not syms:
            return
        qty_tag, _ = _spread_qty_from_broker(syms, open_positions, self.cfg.order_qty)
        if qty_tag == "broken":
            logger.warning(
                "[%s][%s] Clearing open state (broker leg qty inconsistent with a balanced spread)",
                underlying,
                strategy_type,
            )
            self._telemetry_clear_for_spread(open_spread.get("spread_id"))
            pop_nested(self.state, "open_positions", underlying, strategy_type)
            save_state(self.state)
            return
        if qty_tag == "flat":
            logger.info("[%s][%s] Clearing stale open state (legs flat)", underlying, strategy_type)
            self._telemetry_clear_for_spread(open_spread.get("spread_id"))
            pop_nested(self.state, "open_positions", underlying, strategy_type)
            save_state(self.state)

    def _telemetry_clear_for_spread(self, spread_id: Optional[Any]) -> None:
        if spread_id is None:
            return
        try:
            sid = int(spread_id)
        except (TypeError, ValueError):
            return
        if sid <= 0:
            return
        self.last_telemetry_ts.pop(sid, None)
        self.last_gate_open_by_spread.pop(sid, None)

    def _maybe_log_spread_telemetry(
        self,
        underlying: str,
        open_spread: Dict[str, Any],
        strategy_type: str,
        underlying_mid: float,
        distance_gate_met: bool,
    ) -> None:
        try:
            sid = int(open_spread.get("spread_id") or 0)
        except (TypeError, ValueError):
            return
        if sid <= 0:
            return
        legs_p = _pricing_legs_from_open(open_spread)
        quotes = lambda sym: _option_bid_ask(self.option_data_client, sym)
        mark_mid = current_net_credit_mid_from_legs(legs_p, quotes)
        natural_ask = estimate_close_debit_natural_from_open_legs(legs_p, quotes)
        dist_pct = _distance_to_short_pct_for_open(open_spread, underlying_mid, strategy_type)

        now = time.time()
        prev_gate = self.last_gate_open_by_spread.get(sid)
        gate_edge = distance_gate_met and prev_gate is not True
        last_ts = self.last_telemetry_ts.get(sid, 0.0)
        should_log = gate_edge or (now - last_ts >= _TELEMETRY_INTERVAL_SEC)

        if should_log:
            try:
                db_insert_telemetry(
                    sid,
                    underlying_price=float(underlying_mid),
                    current_mark_mid=mark_mid,
                    current_natural_ask=natural_ask,
                    distance_to_short_pct=dist_pct,
                    gate_open=distance_gate_met,
                )
                self.last_telemetry_ts[sid] = now
            except Exception as e:
                logger.debug("[%s] telemetry insert failed: %s", underlying, e)

        self.last_gate_open_by_spread[sid] = distance_gate_met

    def _maybe_close(self, underlying: str, open_spread: Dict[str, Any], strategy_type: str) -> bool:
        st = _open_spread_strategy_type(open_spread)
        if st != strategy_type:
            logger.warning("[%s] strategy_type mismatch open=%s loop=%s", underlying, st, strategy_type)
        ec = _exit_cfg(self.cfg, strategy_type)

        underlying_mid = get_latest_mid(self.stock_data_client, underlying)
        if underlying_mid <= 0:
            return False

        distance_gate_met, _ = _distance_gate_met(open_spread, underlying_mid, strategy_type, ec)
        self._maybe_log_spread_telemetry(underlying, open_spread, strategy_type, underlying_mid, distance_gate_met)

        legs_p = _pricing_legs_from_open(open_spread)
        bid_ask_for = lambda sym: _option_bid_ask(self.option_data_client, sym)
        reason = _close_reason_for_open_credit_spread(
            underlying,
            open_spread,
            strategy_type,
            ec,
            distance_gate_met,
            legs_p,
            bid_ask_for,
        )

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

        try:
            open_positions = self._retry_api(lambda: list_open_positions(self.trading_client))
        except Exception as e:
            logger.warning("[%s][%s] Cannot load positions to validate close: %s", underlying, strategy_type, e)
            return False

        legs_syms = _spread_legs_from_state(open_spread)
        qty_tag, close_qty = _spread_qty_from_broker(legs_syms, open_positions, self.cfg.order_qty)
        if qty_tag == "broken":
            logger.warning(
                "[%s][%s] Clearing open state: broker legs inconsistent; cannot close safely",
                underlying,
                strategy_type,
            )
            self._telemetry_clear_for_spread(open_spread.get("spread_id"))
            pop_nested(self.state, "open_positions", underlying, strategy_type)
            save_state(self.state)
            return False
        if qty_tag == "flat":
            logger.info("[%s][%s] Clearing open state: broker legs already flat", underlying, strategy_type)
            self._telemetry_clear_for_spread(open_spread.get("spread_id"))
            pop_nested(self.state, "open_positions", underlying, strategy_type)
            save_state(self.state)
            return False

        legs_p = _pricing_legs_from_open(open_spread)
        quotes = lambda sym: _option_bid_ask(self.option_data_client, sym)
        current_net = current_net_credit_mid_from_legs(legs_p, quotes)
        if current_net is None or current_net <= 0:
            current_net = float(open_spread.get("entry_net_credit_mid", 0.0))
        if current_net <= 0:
            logger.warning("[%s][%s] Cannot compute positive debit to close; skip", underlying, strategy_type)
            return False

        # Expiry cutoff: use aggressive close pricing based on natural debit (ask(sells) - bid(buys))
        # and keep a slightly larger slippage buffer to avoid end-of-day widening causing no fills.
        if (reason or "") == "expiry_cutoff":
            natural_debit = estimate_close_debit_natural_from_open_legs(legs_p, quotes)
            base = float(natural_debit) if natural_debit is not None and natural_debit > 0 else float(current_net)
            slip_pct = max(float(ec.close_limit_slippage_pct), 0.05)
            close_debit_limit_raw = _close_debit_limit_with_min_slippage(base, slip_pct)
        else:
            close_debit_limit_raw = _close_debit_limit_with_min_slippage(
                float(current_net), ec.close_limit_slippage_pct
            )
        close_debit_limit = _alpaca_limit_price(close_debit_limit_raw, is_credit=False)
        close_legs = _exit_legs_from_open(open_spread)
        client_order_id = f"alpaca_{strategy_type.lower()}_close:{underlying}:{int(time.time())}"
        order_id = submit_mleg_limit_order(
            self.trading_client,
            qty=int(close_qty),
            limit_price=float(close_debit_limit),
            legs=close_legs,
            client_order_id=client_order_id,
            **self._exec_retry_kw(),
        )
        logger.info(
            "[%s][%s] Submitted close order_id=%s qty=%s debit_limit=%.4f",
            underlying,
            strategy_type,
            order_id,
            close_qty,
            close_debit_limit,
        )

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
            qty=close_qty,
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
                "close_qty": int(close_qty),
                # Anti-spam for expiry cutoff repricing (cancel/replace loop control).
                "replace_count": 0,
                "last_replace_ts": 0.0,
            },
        )
        save_state(self.state)

        final = wait_for_order(
            self.trading_client,
            order_id,
            timeout_seconds=self._close_fill_timeout_seconds(reason or ""),
            **self._exec_retry_kw(),
        )
        status = str(getattr(final, "status", "")).lower()
        if status != "filled":
            try:
                cancel_order(self.trading_client, order_id, **self._exec_retry_kw())
            except Exception:
                pass
            st = str(status).split(".")[-1].lower()
            db_update_order_status(order_id, st)
            pop_nested(self.state, PENDING_CLOSE, underlying, strategy_type)
            save_state(self.state)
            logger.warning("[%s][%s] Close not filled status=%s", underlying, strategy_type, status)
            return False

        pending_close = get_nested(self.state, PENDING_CLOSE, underlying, strategy_type) or {}
        return self._finalize_close_fill(
            underlying,
            open_spread,
            strategy_type,
            order_id,
            pending_close,
            ec,
            close_debit_filled_avg=_filled_avg_price_from_order(final),
        )

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
        close_reason = str(pending_close.get("close_reason") or "")
        replace_count = int(pending_close.get("replace_count") or 0)
        last_replace_ts = float(pending_close.get("last_replace_ts", 0.0) or 0.0)
        if not order_id:
            pop_nested(self.state, PENDING_CLOSE, underlying, strategy_type)
            save_state(self.state)
            return False

        status = self._get_order_status(order_id)
        if status == "filled":
            try:
                o = self._retry_api(lambda: self.trading_client.get_order_by_id(order_id))
            except Exception:
                o = None
            fill = _filled_avg_price_from_order(o) if o is not None else None
            return self._finalize_close_fill(
                underlying,
                open_spread,
                strategy_type,
                order_id,
                pending_close,
                ec,
                close_debit_filled_avg=fill,
            )

        if self._is_terminal_order_status(status) and status != "filled":
            db_update_order_status(order_id, status)
            pop_nested(self.state, PENDING_CLOSE, underlying, strategy_type)
            save_state(self.state)
            logger.warning("[%s][%s] Close terminal status=%s", underlying, strategy_type, status)
            return False

        # Expiry cutoff anti-spam:
        # - let the aggressive order sit for at least 60s before any cancel/replace
        # - cap replacements to avoid hundreds of orders near expiry
        if close_reason == "expiry_cutoff" and submitted_at_ts:
            age = time.time() - submitted_at_ts
            if age < 60.0:
                return False
            if replace_count < 3 and (not last_replace_ts or (time.time() - last_replace_ts) >= 60.0):
                try:
                    legs_p = _pricing_legs_from_open(open_spread)
                    quotes = lambda sym: _option_bid_ask(self.option_data_client, sym)
                    natural_debit = estimate_close_debit_natural_from_open_legs(legs_p, quotes)
                    if natural_debit is not None and natural_debit > 0:
                        base = float(natural_debit)
                        slip_pct = max(float(ec.close_limit_slippage_pct), 0.05)
                        new_raw = _close_debit_limit_with_min_slippage(base, slip_pct)
                        new_limit = _alpaca_limit_price(new_raw, is_credit=False)
                    else:
                        new_limit = None
                except Exception:
                    new_limit = None

                # Only replace if we can compute a new aggressive limit.
                if new_limit is not None:
                    try:
                        cancel_order(self.trading_client, order_id, **self._exec_retry_kw())
                    except Exception:
                        pass
                    close_legs = _exit_legs_from_open(open_spread)
                    client_order_id = f"alpaca_{strategy_type.lower()}_close:{underlying}:{int(time.time())}"
                    replace_qty = int(pending_close.get("close_qty") or self.cfg.order_qty)
                    new_order_id = submit_mleg_limit_order(
                        self.trading_client,
                        qty=replace_qty,
                        limit_price=float(new_limit),
                        legs=close_legs,
                        client_order_id=client_order_id,
                        **self._exec_retry_kw(),
                    )
                    logger.warning(
                        "[%s][%s] Expiry cutoff replace close order %s -> %s debit_limit=%.4f (replace_count=%s)",
                        underlying,
                        strategy_type,
                        order_id,
                        new_order_id,
                        float(new_limit),
                        replace_count + 1,
                    )
                    # Record the new order in DB/state; old order status will be reconciled later.
                    db_update_order_status(str(order_id), "canceled")
                    db_insert_order(
                        order_id=new_order_id,
                        underlying=underlying,
                        side="close",
                        status="submitted",
                        client_order_id=client_order_id,
                        strategy_type=strategy_type,
                        legs=_spread_legs_from_state(open_spread) or None,
                        short_put_symbol=open_spread.get("short_put_symbol"),
                        long_put_symbol=open_spread.get("long_put_symbol"),
                        limit_price=float(new_limit),
                        qty=replace_qty,
                        raw_snapshot={"strategy_type": strategy_type, "legs": _spread_legs_from_state(open_spread)},
                    )
                    log_event(
                        "close_submitted",
                        f"order_id={new_order_id}",
                        underlying=underlying,
                        extra={"order_id": new_order_id, "strategy_type": strategy_type, "replaced": True},
                    )
                    pending_close["order_id"] = new_order_id
                    pending_close["submitted_at_ts"] = time.time()
                    pending_close["close_debit_limit"] = float(new_limit)
                    pending_close["replace_count"] = replace_count + 1
                    pending_close["last_replace_ts"] = time.time()
                    set_nested(self.state, PENDING_CLOSE, underlying, strategy_type, pending_close)
                    save_state(self.state)
                    return False

        # For expiry_cutoff, avoid aggressive timeout-driven cancel loops; let it rest longer.
        timeout = float(self._close_fill_timeout_seconds(close_reason))
        if close_reason == "expiry_cutoff":
            timeout = max(timeout, 600.0)
        if submitted_at_ts and (time.time() - submitted_at_ts) >= timeout:
            try:
                cancel_order(self.trading_client, order_id, **self._exec_retry_kw())
            except Exception:
                pass
            try:
                o = self._retry_api(lambda: self.trading_client.get_order_by_id(str(order_id)))
                st = str(getattr(o, "status", "canceled")).lower().split(".")[-1]
            except Exception:
                st = "canceled"
            db_update_order_status(str(order_id), st)
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
        *,
        close_debit_filled_avg: Optional[float] = None,
    ) -> bool:
        spread_id = open_spread.get("spread_id")
        entry_credit = float(open_spread.get("entry_net_credit_mid", 0.0))
        limit_debit = float(pending_close.get("close_debit_limit", 0.0) or 0.0)
        close_reason = pending_close.get("close_reason", "close")
        debit_for_pnl = (
            abs(float(close_debit_filled_avg)) if close_debit_filled_avg is not None else float(limit_debit)
        )
        close_qty = int(pending_close.get("close_qty") or self.cfg.order_qty)
        if spread_id and debit_for_pnl > 0:
            db_close_spread(
                spread_id,
                order_id,
                limit_debit,
                close_reason,
                entry_credit,
                close_qty,
                filled_avg_price=close_debit_filled_avg,
            )
            pnl = (entry_credit - debit_for_pnl) * 100.0 * close_qty
            alert_close_filled(underlying, order_id, close_reason, pnl)
        db_update_order_status(order_id, "filled", filled_avg_price=close_debit_filled_avg)
        log_event("close_filled", f"order_id={order_id}", underlying=underlying, extra={"order_id": order_id, "strategy_type": strategy_type})

        pop_nested(self.state, PENDING_CLOSE, underlying, strategy_type)
        self._telemetry_clear_for_spread(open_spread.get("spread_id"))
        pop_nested(self.state, "open_positions", underlying, strategy_type)
        cooldown_until = time.time() + int(ec.exit_cooldown_minutes) * 60
        set_nested(self.state, COOLDOWN, underlying, strategy_type, cooldown_until)
        save_state(self.state)
        logger.info("[%s][%s] Close filled; state cleared", underlying, strategy_type)
        return True

    def _maybe_open(self, underlying: str, strategy_type: str, chain: Optional[Any] = None) -> None:
        if get_nested(self.state, "open_positions", underlying, strategy_type):
            return

        pending_entry = get_nested(self.state, PENDING_ENTRY, underlying, strategy_type)
        if pending_entry:
            self._handle_pending_entry(underlying, strategy_type, pending_entry)
            return

        cap = int(self.cfg.max_open_spreads_per_underlying)
        count = self._underlying_active_entry_count(underlying)
        if cap > 0 and count >= cap:
            logger.debug(
                "[%s][%s] max_open_spreads_per_underlying=%s reached (active_slots=%s); skip new entry.",
                underlying,
                strategy_type,
                cap,
                count,
            )
            return
        active_st = self._underlying_active_strategy_types(underlying)
        if active_st and strategy_type not in active_st:
            logger.debug(
                "[%s][%s] Active spread/pending on %s; no cross-strategy mix on same underlying.",
                underlying,
                strategy_type,
                ",".join(sorted(active_st)),
            )
            return

        if get_nested(self.state, ENTRY_DISABLED, underlying, strategy_type):
            logger.debug(
                "[%s][%s] Entry disabled in state file; skipping options scan.",
                underlying,
                strategy_type,
            )
            key = (underlying, strategy_type)
            if key not in self._entry_disabled_notice_keys:
                self._entry_disabled_notice_keys.add(key)
                logger.info(
                    "[%s][%s] Entry disabled in state (entry_disabled). Skipping scan; "
                    "set logger DEBUG for bot.alpaca_put_spread.strategy to see each loop.",
                    underlying,
                    strategy_type,
                )
            return

        logger.info("[%s][%s] scanning for candidate...", underlying, strategy_type)

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

        min_min = int(getattr(self.cfg, "min_minutes_to_expiry_for_new_entry", 0) or 0)
        if min_min > 0:
            exp_sym = _expiry_symbol_for_candidate(strategy_type, candidate)
            m = minutes_to_expiry_utc(exp_sym)
            if m is not None and m < float(min_min):
                logger.info(
                    "[%s][%s] skip entry: %.1f min to expiry < min_minutes_to_expiry_for_new_entry=%s",
                    underlying,
                    strategy_type,
                    m,
                    min_min,
                )
                return

        if not self.cfg.execute:
            logger.info("[%s][%s] execute=false; would submit entry", underlying, strategy_type)
            return

        # Second expiry check immediately before submit (reduces "expires soon" races after selection).
        min_min2 = int(getattr(self.cfg, "min_minutes_to_expiry_for_new_entry", 0) or 0)
        if min_min2 > 0:
            exp_sym2 = _expiry_symbol_for_candidate(strategy_type, candidate)
            m2 = minutes_to_expiry_utc(exp_sym2)
            if m2 is not None and m2 < float(min_min2):
                logger.info(
                    "[%s][%s] skip entry (pre-submit): %.1f min to expiry < %s",
                    underlying,
                    strategy_type,
                    m2,
                    min_min2,
                )
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
        entry_limit_price = _alpaca_limit_price(entry_net_credit_mid, is_credit=True)
        client_order_id = f"alpaca_{strategy_type.lower()}_open:{underlying}:{int(time.time())}"
        order_id = submit_mleg_limit_order(
            self.trading_client,
            qty=int(self.cfg.order_qty),
            limit_price=entry_limit_price,
            legs=entry_legs,
            client_order_id=client_order_id,
            **self._exec_retry_kw(),
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

    def _complete_entry_fill(
        self,
        underlying: str,
        strategy_type: str,
        pending_entry: Dict[str, Any],
        order_obj: Any = None,
    ) -> None:
        order_id = str(pending_entry.get("order_id") or "")
        if not order_id:
            return
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
        try:
            um = float(underlying_mid) if underlying_mid else 0.0
        except (TypeError, ValueError):
            um = 0.0
        entry_sig = _entry_raw_snapshot_signature(
            self.option_data_client,
            underlying,
            strategy_type,
            pending_entry,
            um,
        )
        spread_id = db_insert_spread(
            underlying=underlying,
            entry_credit_mid=entry_credit_mid,
            entry_order_id=order_id,
            strategy_type=strategy_type,
            legs=syms,
            short_put_symbol=pending_entry.get("short_put_symbol"),
            long_put_symbol=pending_entry.get("long_put_symbol"),
            raw_snapshot=entry_sig,
        )
        merge_order_raw_snapshot(order_id, entry_sig)
        favg = _filled_avg_price_from_order(order_obj) if order_obj is not None else None
        db_update_order_status(order_id, "filled", filled_avg_price=favg)
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

    def _handle_pending_entry(self, underlying: str, strategy_type: str, pending_entry: Dict[str, Any]) -> None:
        order_id = pending_entry.get("order_id")
        if not order_id:
            pop_nested(self.state, PENDING_ENTRY, underlying, strategy_type)
            save_state(self.state)
            return

        status = self._get_order_status(order_id)
        if status == "filled":
            try:
                o = self._retry_api(lambda: self.trading_client.get_order_by_id(str(order_id)))
            except Exception:
                o = None
            self._complete_entry_fill(underlying, strategy_type, pending_entry, order_obj=o)
            return

        if self._is_terminal_order_status(status) and status != "filled":
            db_update_order_status(str(order_id), status)
            pop_nested(self.state, PENDING_ENTRY, underlying, strategy_type)
            pop_nested(self.state, ENTRY_DISABLED, underlying, strategy_type)
            save_state(self.state)
            logger.warning("[%s][%s] Entry terminal status=%s; pending cleared (may hunt again)", underlying, strategy_type, status)
            return

    def sync_pending_orders_with_broker(self) -> None:
        """
        Reconcile pending_entry / pending_close with Alpaca before pricing and exit logic.
        Updates local DB order status and advances state when orders are terminal.
        """
        # Pending closes first (exit lifecycle).
        pending_close_root = self.state.get(PENDING_CLOSE) or {}
        for underlying in list(pending_close_root.keys()):
            by_st = pending_close_root.get(underlying) or {}
            if not isinstance(by_st, dict):
                continue
            for strategy_type in list(by_st.keys()):
                pending_close = get_nested(self.state, PENDING_CLOSE, underlying, strategy_type)
                if not isinstance(pending_close, dict):
                    continue
                order_id = pending_close.get("order_id")
                if not order_id:
                    continue
                try:
                    o = self._retry_api(lambda: self.trading_client.get_order_by_id(str(order_id)))
                except Exception as e:
                    logger.debug("[%s][%s] sync close: get_order %s failed: %s", underlying, strategy_type, order_id, e)
                    continue
                status = str(getattr(o, "status", "")).lower().split(".")[-1]
                if status == "filled":
                    open_spread = get_nested(self.state, "open_positions", underlying, strategy_type)
                    if not open_spread:
                        pop_nested(self.state, PENDING_CLOSE, underlying, strategy_type)
                        save_state(self.state)
                        continue
                    ec = _exit_cfg(self.cfg, strategy_type)
                    self._finalize_close_fill(
                        underlying,
                        open_spread,
                        strategy_type,
                        str(order_id),
                        pending_close,
                        ec,
                        close_debit_filled_avg=_filled_avg_price_from_order(o),
                    )
                    continue
                if self._is_terminal_order_status(status) and status != "filled":
                    db_update_order_status(str(order_id), status)
                    pop_nested(self.state, PENDING_CLOSE, underlying, strategy_type)
                    save_state(self.state)
                    logger.info(
                        "[%s][%s] sync: close order %s terminal=%s; kept open_positions for retry",
                        underlying,
                        strategy_type,
                        order_id,
                        status,
                    )

        # Pending entries
        pending_entry_root = self.state.get(PENDING_ENTRY) or {}
        for underlying in list(pending_entry_root.keys()):
            by_st = pending_entry_root.get(underlying) or {}
            if not isinstance(by_st, dict):
                continue
            for strategy_type in list(by_st.keys()):
                pending_entry = get_nested(self.state, PENDING_ENTRY, underlying, strategy_type)
                if not isinstance(pending_entry, dict):
                    continue
                order_id = pending_entry.get("order_id")
                if not order_id:
                    continue
                try:
                    o = self._retry_api(lambda: self.trading_client.get_order_by_id(str(order_id)))
                except Exception as e:
                    logger.debug("[%s][%s] sync entry: get_order %s failed: %s", underlying, strategy_type, order_id, e)
                    continue
                status = str(getattr(o, "status", "")).lower().split(".")[-1]
                if status == "filled":
                    self._complete_entry_fill(underlying, strategy_type, pending_entry, order_obj=o)
                    continue
                if self._is_terminal_order_status(status) and status != "filled":
                    db_update_order_status(str(order_id), status)
                    pop_nested(self.state, PENDING_ENTRY, underlying, strategy_type)
                    pop_nested(self.state, ENTRY_DISABLED, underlying, strategy_type)
                    save_state(self.state)
                    logger.info(
                        "[%s][%s] sync: entry order %s terminal=%s; pending cleared",
                        underlying,
                        strategy_type,
                        order_id,
                        status,
                    )

        # Fallback reconciliation: if a close order filled but JSON state lost pending_close metadata,
        # alpaca_spreads never gets closed and TP/SL can refire. Link any filled close orders that
        # are not yet referenced by alpaca_spreads.close_order_id by matching (underlying, strategy_type, legs).
        self._reconcile_filled_close_orders_without_spread_link()

    def _reconcile_filled_close_orders_without_spread_link(self) -> None:
        # Keep bounded: only look back a week to avoid scanning entire history each loop.
        since_ts = time.time() - 7 * 24 * 3600
        rows = list_filled_close_orders_without_spread_link(since_ts=since_ts)
        if not rows:
            return
        import json

        for r in rows:
            order_id = str(r.get("order_id") or "")
            underlying = str(r.get("underlying") or "")
            strategy_type = str(r.get("strategy_type") or "")
            if not order_id or not underlying or not strategy_type:
                continue

            legs_raw = r.get("legs")
            try:
                legs = json.loads(legs_raw) if legs_raw else None
            except Exception:
                legs = None
            if not isinstance(legs, list) or not legs:
                logger.warning(
                    "[%s][%s] reconcile: filled close order missing legs; order_id=%s",
                    underlying,
                    strategy_type,
                    order_id,
                )
                continue
            legs = [str(x) for x in legs]

            spread_id = find_open_spread_id_by_legs(
                underlying=underlying,
                strategy_type=strategy_type,
                legs=legs,
            )
            if not spread_id:
                logger.warning(
                    "[%s][%s] reconcile: filled close order has no open spread match; order_id=%s legs=%s",
                    underlying,
                    strategy_type,
                    order_id,
                    legs,
                )
                continue

            favg = r.get("filled_avg_price")
            limit_price = r.get("limit_price")
            try:
                debit = abs(float(favg)) if favg is not None else abs(float(limit_price))
            except Exception:
                debit = 0.0
            if debit <= 0:
                logger.warning(
                    "[%s][%s] reconcile: cannot compute close debit; order_id=%s favg=%s limit=%s",
                    underlying,
                    strategy_type,
                    order_id,
                    favg,
                    limit_price,
                )
                continue

            # Prefer entry credit from in-memory open_positions if it matches this spread_id.
            entry_credit = None
            open_spread = get_nested(self.state, "open_positions", underlying, strategy_type)
            if isinstance(open_spread, dict) and int(open_spread.get("spread_id") or 0) == int(spread_id):
                try:
                    entry_credit = float(open_spread.get("entry_net_credit_mid") or 0.0)
                except Exception:
                    entry_credit = None
            if entry_credit is None or entry_credit <= 0:
                entry_credit = get_spread_entry_credit_mid(int(spread_id))
            if entry_credit is None or entry_credit <= 0:
                logger.warning(
                    "[%s][%s] reconcile: missing entry credit; spread_id=%s order_id=%s",
                    underlying,
                    strategy_type,
                    spread_id,
                    order_id,
                )
                continue

            close_reason = "reconciled_close_filled"
            recon_qty = int(r.get("qty") or self.cfg.order_qty)
            db_close_spread(
                int(spread_id),
                order_id,
                float(debit),
                close_reason,
                float(entry_credit),
                recon_qty,
                filled_avg_price=float(debit),
            )
            db_update_order_status(order_id, "filled", filled_avg_price=float(debit))
            log_event(
                "close_filled_reconciled",
                f"order_id={order_id} spread_id={spread_id}",
                underlying=underlying,
                extra={"order_id": order_id, "spread_id": int(spread_id), "strategy_type": strategy_type},
            )

            # Prevent refiring: only clear state if it matches this exact spread_id.
            if isinstance(open_spread, dict) and int(open_spread.get("spread_id") or 0) == int(spread_id):
                self._telemetry_clear_for_spread(spread_id)
                pop_nested(self.state, "open_positions", underlying, strategy_type)
                ec = _exit_cfg(self.cfg, strategy_type)
                cooldown_until = time.time() + int(ec.exit_cooldown_minutes) * 60
                set_nested(self.state, COOLDOWN, underlying, strategy_type, cooldown_until)
                save_state(self.state)
                logger.info(
                    "[%s][%s] reconcile: closed spread_id=%s via order_id=%s; state cleared",
                    underlying,
                    strategy_type,
                    spread_id,
                    order_id,
                )

    def run_forever(self) -> None:
        while True:
            try:
                self.sync_pending_orders_with_broker()
            except Exception as e:
                if is_transient_request_error(e):
                    logger.warning("sync_pending_orders_with_broker failed (transient): %s", e)
                else:
                    logger.exception("sync_pending_orders_with_broker failed: %s", e)
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
                        if is_transient_request_error(e):
                            logger.warning("[%s][%s] Loop transient error: %s", underlying, strategy_type, e)
                        elif _is_expected_broker_rejection(e):
                            logger.warning(
                                "[%s][%s] Broker rejected order (expected): %s",
                                underlying,
                                strategy_type,
                                e,
                            )
                        else:
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


def _filled_avg_price_from_order(order: Any) -> Optional[float]:
    """Alpaca Order.filled_avg_price for mleg (net); None if missing."""
    v = getattr(order, "filled_avg_price", None)
    if v is None:
        return None
    try:
        f = float(v)
        if f != f:  # NaN
            return None
        return f
    except (TypeError, ValueError):
        return None


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

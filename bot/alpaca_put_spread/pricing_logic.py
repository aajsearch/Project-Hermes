from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional, Sequence, Tuple

from bot.alpaca_put_spread.domain import Leg

# (bid, ask) per symbol; either may be None if unavailable.
BidAskFor = Callable[[str], Tuple[Optional[float], Optional[float]]]


@dataclass(frozen=True)
class PutSpreadCandidate:
    underlying: str
    long_put_symbol: str
    short_put_symbol: str
    long_put_mid: float
    short_put_mid: float
    entry_net_credit_mid: float
    long_strike: float
    short_strike: float


@dataclass(frozen=True)
class CallSpreadCandidate:
    """Bear call credit spread: short lower strike call, long higher strike call."""

    underlying: str
    long_call_symbol: str
    short_call_symbol: str
    long_call_mid: float
    short_call_mid: float
    entry_net_credit_mid: float
    long_strike: float
    short_strike: float


@dataclass(frozen=True)
class IronCondorCandidate:
    """4-leg iron condor: LP < SP < SC < LC by strike."""

    underlying: str
    long_put_symbol: str
    short_put_symbol: str
    short_call_symbol: str
    long_call_symbol: str
    entry_net_credit_mid: float


def _mid_from_bid_ask(bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    """Mid price from quote; mirrors option quote handling in strategy._option_mid."""
    try:
        bid_f = float(bid) if bid is not None else None
        ask_f = float(ask) if ask is not None else None
        if bid_f is not None and ask_f is not None and bid_f > 0 and ask_f > 0:
            return (bid_f + ask_f) / 2.0
        if ask_f is not None and ask_f > 0:
            return ask_f
        if bid_f is not None and bid_f > 0:
            return bid_f
    except (TypeError, ValueError):
        return None
    return None


def _leg_credit_contribution(leg: Leg, mid: float) -> float:
    """
    Signed premium contribution to net credit for an opening position leg.
    Sell -> +mid * ratio (credit), Buy -> -mid * ratio (debit).
    """
    r = int(leg.ratio) if getattr(leg, "ratio", None) not in (None, 0) else 1
    if r <= 0:
        r = 1
    side_raw = (leg.side or "").strip().lower()
    if side_raw in ("sell", "s"):
        sign = 1.0
    elif side_raw in ("buy", "b"):
        sign = -1.0
    elif "sell" in side_raw:
        sign = 1.0
    elif "buy" in side_raw:
        sign = -1.0
    else:
        raise ValueError(f"Leg.side must indicate buy or sell, got {leg.side!r}")
    return sign * float(r) * float(mid)


def current_net_credit_mid_from_legs(legs: Sequence[Leg], bid_ask_for: BidAskFor) -> Optional[float]:
    """
    Combined mark-to-mid net credit for a multi-leg structure: sum over legs of
    signed mid * ratio (sell +, buy -), using (bid+ask)/2 when both sides valid.
    Returns None if any leg is missing a usable mid.
    """
    total = 0.0
    for leg in legs:
        bid, ask = bid_ask_for(leg.symbol)
        mid = _mid_from_bid_ask(bid, ask)
        if mid is None:
            return None
        total += _leg_credit_contribution(leg, mid)
    return total


def estimate_close_debit_natural_from_open_legs(legs: Sequence[Leg], bid_ask_for: BidAskFor) -> Optional[float]:
    """
    Conservative "natural" debit to close a position described by *opening* legs.

    For each opening leg:
    - opening SELL -> closing BUY at ASK (pay ask)
    - opening BUY  -> closing SELL at BID (receive bid)

    Returns the net debit (>= 0) to close, or None if any leg is missing the required quote side.
    """
    debit = 0.0
    for leg in legs:
        bid, ask = bid_ask_for(leg.symbol)
        r = int(leg.ratio) if getattr(leg, "ratio", None) not in (None, 0) else 1
        if r <= 0:
            r = 1
        side_raw = (leg.side or "").strip().lower()
        if side_raw in ("sell", "s") or "sell" in side_raw:
            if ask is None:
                return None
            debit += float(r) * float(ask)
        elif side_raw in ("buy", "b") or "buy" in side_raw:
            if bid is None:
                return None
            debit -= float(r) * float(bid)
        else:
            return None
    return float(debit) if debit >= 0 else 0.0


def net_credit_mid(short_put_mid: float, long_put_mid: float) -> float:
    """PCS helper: net credit from short and long put mids (sell short, buy long)."""
    legs = (
        Leg(symbol="__short__", side="sell", intent="", ratio=1),
        Leg(symbol="__long__", side="buy", intent="", ratio=1),
    )

    def quotes(sym: str) -> Tuple[Optional[float], Optional[float]]:
        if sym == "__short__":
            m = float(short_put_mid)
            return (m, m)
        if sym == "__long__":
            m = float(long_put_mid)
            return (m, m)
        return (None, None)

    out = current_net_credit_mid_from_legs(legs, quotes)
    if out is None:
        return float(short_put_mid) - float(long_put_mid)
    return out


def entry_condition_met(
    net_credit_mid_val: float,
    target_credit: float,
    entry_operator: str,
) -> bool:
    if entry_operator == ">=":
        return net_credit_mid_val >= target_credit
    if entry_operator == "<=":
        return net_credit_mid_val <= target_credit
    return False


def natural_close_debit_for_exit(
    current_net_credit_mid: Optional[float],
    *,
    legs: Optional[Sequence[Leg]] = None,
    bid_ask_for: Optional[BidAskFor] = None,
) -> Optional[float]:
    """
    Debit-to-close estimate used for TP/SL: natural ask/bid where possible, else mark mid.

    When *legs* and *bid_ask_for* are provided, *current_net_credit_mid* is ignored (same rule as
    :func:`tp_sl_triggered`).
    """
    current: Optional[float] = current_net_credit_mid
    if legs is not None and bid_ask_for is not None:
        mark_credit = current_net_credit_mid_from_legs(legs, bid_ask_for)
        natural_debit = estimate_close_debit_natural_from_open_legs(legs, bid_ask_for)
        current = natural_debit if natural_debit is not None else mark_credit
    return current


def tp_sl_triggered(
    current_net_credit_mid: Optional[float],
    entry_net_credit_mid: float,
    tp_pct: float,
    sl_pct: float,
    *,
    legs: Optional[Sequence[Leg]] = None,
    bid_ask_for: Optional[BidAskFor] = None,
) -> tuple[bool, str]:
    """
    TP/SL evaluation for credit structures.

    We estimate the *debit to close* (buy-to-close net) conservatively using the "natural" price:
    ask(opening-sell legs) minus bid(opening-buy legs). This avoids firing TP based purely on mid
    when spreads widen and the executable close price is materially worse than the mark.

    Thresholds are expressed in terms of debit-to-close:
    - Take profit: est_close_debit <= entry_net_credit_mid * (1 - tp_pct)
    - Stop loss:   est_close_debit >= entry_net_credit_mid * (1 + sl_pct)

    If ``legs`` and ``bid_ask_for`` are provided, ``current_net_credit_mid`` is
    ignored and the combined total from :func:`current_net_credit_mid_from_legs` is used.

    The Alpaca options runner evaluates TP without the distance gate; SL uses the same thresholds
    but only fires when the distance gate is open. This helper returns the first of TP/SL that
    would match if both were evaluated without gating.
    """
    current = natural_close_debit_for_exit(
        current_net_credit_mid, legs=legs, bid_ask_for=bid_ask_for
    )
    if current is None:
        return (False, "")
    if entry_net_credit_mid <= 0:
        return (False, "")
    tp_thr = entry_net_credit_mid * (1.0 - tp_pct)
    sl_thr = entry_net_credit_mid * (1.0 + sl_pct)
    if current <= tp_thr:
        return (True, "tp")
    if current >= sl_thr:
        return (True, "sl")
    return (False, "")

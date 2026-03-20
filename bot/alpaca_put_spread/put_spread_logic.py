from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


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


def net_credit_mid(short_put_mid: float, long_put_mid: float) -> float:
    return float(short_put_mid) - float(long_put_mid)


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


def tp_sl_triggered(
    current_net_credit_mid: float,
    entry_net_credit_mid: float,
    tp_pct: float,
    sl_pct: float,
) -> tuple[bool, str]:
    """
    For credit spreads we expect net credit mid to DECAY in profit.
    - Take profit: current_net_credit_mid <= entry_net_credit_mid * (1 - tp_pct)
    - Stop loss:   current_net_credit_mid >= entry_net_credit_mid * (1 + sl_pct)
    """
    if entry_net_credit_mid <= 0:
        return (False, "")
    tp_thr = entry_net_credit_mid * (1.0 - tp_pct)
    sl_thr = entry_net_credit_mid * (1.0 + sl_pct)
    if current_net_credit_mid <= tp_thr:
        return (True, "tp")
    if current_net_credit_mid >= sl_thr:
        return (True, "sl")
    return (False, "")


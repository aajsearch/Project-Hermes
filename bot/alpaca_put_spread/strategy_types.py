"""
Strategy type identifiers for Alpaca multi-leg options (DB, state, config).

PCS / CCS / IC are wired in the runner when enabled in alpaca_options.yaml.
"""
from __future__ import annotations

from enum import Enum


class StrategyType(str, Enum):
    PUT_CREDIT_SPREAD = "PCS"
    CALL_CREDIT_SPREAD = "CCS"
    IRON_CONDOR = "IC"


def strategy_type_from_str(value: str) -> StrategyType:
    v = (value or "").strip().upper()
    for st in StrategyType:
        if st.value == v:
            return st
    raise ValueError(f"Unknown strategy_type: {value!r}; expected PCS, CCS, or IC")

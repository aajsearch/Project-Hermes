"""
Shared domain types for multi-leg options strategies (credit spreads, iron condor).
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import List


class StrategyType(Enum):
    """Strategy family for a multi-leg candidate."""

    PCS = "PCS"  # Put Credit Spread (e.g. bull put)
    CCS = "CCS"  # Call Credit Spread (e.g. bear call)
    IC = "IC"  # Iron Condor


@dataclass
class Leg:
    """One option leg in a multi-leg order."""

    symbol: str
    side: str
    intent: str
    ratio: int


@dataclass
class TradeCandidate:
    """Selected structure ready for pricing and order submission."""

    legs: List[Leg]
    strategy_type: StrategyType
    net_credit_mid: float

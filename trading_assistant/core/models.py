from dataclasses import dataclass
from typing import Optional, Literal

AssetType = Literal["EQUITY", "OPTION"]
Action = Literal["BUY", "SELL", "HOLD", "NONE"]
OptionDirection = Literal["CALL", "PUT"]

@dataclass
class Signal:
    action: Action
    reason: str
    confidence: float = 0.5
    # For options overlay: when action=BUY, direction=CALL (bullish) or PUT (bearish)
    direction: Optional[OptionDirection] = None

@dataclass
class Position:
    asset_type: AssetType
    key: str              # equities: symbol, options: OPT:<underlying>
    symbol: str           # equities: symbol, options: contract symbol (e.g., SPY260515C00702000)
    qty: float            # equities: shares, options: contracts (int but stored as float ok)
    entry_price: float    # avg fill price
    entry_time: str       # ISO time string

    # option-only metadata (safe to keep None for equities)
    underlying: Optional[str] = None
    contract: Optional[str] = None
    contract_type: Optional[OptionDirection] = None  # "CALL" or "PUT"

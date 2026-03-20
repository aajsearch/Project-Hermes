"""
Shared window/slot helpers for Bot V2 pipeline.
Use a single window_id format (interval_slot) so tick_log, telemetry, and strategy_reports are queryable by slot.
"""


def logical_window_slot(market_id: str) -> str:
    """
    Extract the time-slot part from market_id so all assets share the same window key.
    E.g. KXBTC15M-26MAR180100 -> 26MAR180100; KXETH15M-26MAR180100 -> 26MAR180100.
    """
    if not market_id:
        return market_id or ""
    return market_id.split("-")[-1].strip() if "-" in market_id else market_id

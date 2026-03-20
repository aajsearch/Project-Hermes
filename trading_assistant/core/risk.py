from datetime import datetime
from typing import Tuple

from core.models import Position
from core.state_store import get_cooldown


def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


def skip_near_open(profile: dict) -> bool:
    """
    Skip entries in the first N minutes after the regular market open (6:30am PT).
    """
    n = int(profile.get("no_trade_first_minutes", 0))
    if n <= 0:
        return False
    now = datetime.now()
    open_time = now.replace(hour=6, minute=30, second=0, microsecond=0)
    mins = (now - open_time).total_seconds() / 60.0
    return 0 <= mins < n


def is_in_cooldown(state, symbol: str) -> bool:
    cd = get_cooldown(state, symbol)
    if not cd:
        return False
    try:
        return _parse_iso(cd["until"]) > datetime.now()
    except Exception:
        return False


def compute_qty_from_notional(price: float, notional: float) -> float:
    """
    Equity sizing helper: integer shares only.
    """
    if price <= 0:
        return 0.0
    qty = notional / price
    return max(0.0, float(int(qty)))


def should_exit_position(pos: Position, current_price: float, profile: dict) -> Tuple[bool, str]:
    """
    Exit checks for EQUITY positions.

    Adds a simple profit-protection rule:
      - once PnL >= trail_trigger_pct, exit if price falls back to (<=) entry
        (a simple 'breakeven trail' to avoid green→red trades)

    You can tune via profile:
      trail_trigger_pct: 0.012  (default 1.2%)
      trail_to_entry: true      (default true)
    """
    if not pos or pos.qty <= 0 or pos.entry_price <= 0:
        return (False, "")

    entry = float(pos.entry_price)
    pnl_pct = (float(current_price) - entry) / entry

    # -------------------------
    # Hard stop loss / take profit
    # -------------------------
    sl = float(profile.get("stop_loss_pct", 0.0))
    tp = float(profile.get("take_profit_pct", 0.0))

    # Optional volatility-aware stop: use wider stop when atr_stop_pct set (e.g. 0.025 for 2.5%)
    atr_stop_pct = profile.get("atr_stop_pct")
    if atr_stop_pct is not None and float(atr_stop_pct) > 0:
        sl = max(sl, float(atr_stop_pct))

    if sl > 0 and pnl_pct <= -sl:
        return (True, f"stop_loss hit pnl_pct={pnl_pct:.4f}")

    if tp > 0 and pnl_pct >= tp:
        return (True, f"take_profit hit pnl_pct={pnl_pct:.4f}")

    # -------------------------
    # Simple profit protection ("breakeven trail")
    # -------------------------
    # Idea: if trade was meaningfully green, don't let it turn into a loser.
    trail_trigger = float(profile.get("trail_trigger_pct", 0.012))  # 1.2% default
    trail_to_entry = bool(profile.get("trail_to_entry", True))

    if trail_to_entry and trail_trigger > 0:
        if pnl_pct >= trail_trigger and float(current_price) <= entry:
            return (True, f"trail_to_entry hit pnl_pct={pnl_pct:.4f}")

    # -------------------------
    # Time stop
    # -------------------------
    max_hold_min = int(profile.get("max_hold_minutes", 0))
    if max_hold_min > 0:
        try:
            age_min = (datetime.now() - _parse_iso(pos.entry_time)).total_seconds() / 60.0
            if age_min >= max_hold_min:
                return (True, f"time_stop age_min={age_min:.1f}")
        except Exception:
            pass

    return (False, "")

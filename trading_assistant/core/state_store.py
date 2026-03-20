import json
import os
from dataclasses import asdict
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from uuid import UUID

from config.settings import STATE_DIR, STATE_PATH
from core.models import Position

os.makedirs(STATE_DIR, exist_ok=True)


class JSONEncoder(json.JSONEncoder):
    """Custom encoder to handle UUID and other non-serializable types."""
    def default(self, obj):
        if isinstance(obj, UUID):
            return str(obj)
        return super().default(obj)


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _default_state() -> Dict[str, Any]:
    return {
        "positions": {},
        "pending_orders": {},
        "cooldowns": {},
        "option_recos": {},
        "portfolio_cooldown_until": None,
        "mfe_mae_tracker": {},
        "buys_last_hour": [],  # list of ISO timestamps for MAX_BUYS_PER_HOUR
    }


def load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_PATH):
        state = _default_state()
        save_state(state)
        return state

    with open(STATE_PATH, "r") as f:
        raw = json.load(f)

    # self-heal missing keys
    raw.setdefault("positions", {})
    raw.setdefault("pending_orders", {})
    raw.setdefault("cooldowns", {})
    raw.setdefault("option_recos", {})
    raw.setdefault("portfolio_cooldown_until", None)
    raw.setdefault("mfe_mae_tracker", {})
    raw.setdefault("buys_last_hour", [])

    return raw


def save_state(state: Dict[str, Any]) -> None:
    tmp_path = STATE_PATH + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(state, f, indent=2, cls=JSONEncoder)
    os.replace(tmp_path, STATE_PATH)


# -------------------------
# Generic position helpers
# -------------------------
def get_position(state: Dict[str, Any], key: str) -> Optional[Position]:
    p = state.get("positions", {}).get(key)
    return Position(**p) if p else None


def set_position(state: Dict[str, Any], key: str, pos: Optional[Position]) -> None:
    state.setdefault("positions", {})
    if pos is None:
        state["positions"].pop(key, None)
        return
    state["positions"][key] = asdict(pos)


def get_pending_order(state: Dict[str, Any], key: str) -> Optional[dict]:
    return state.get("pending_orders", {}).get(key)


def set_pending_order(state: Dict[str, Any], key: str, order: Optional[dict]) -> None:
    state.setdefault("pending_orders", {})
    if order is None:
        state["pending_orders"].pop(key, None)
    else:
        state["pending_orders"][key] = order


def set_cooldown(state: Dict[str, Any], key: str, until_iso: str, reason: str) -> None:
    state.setdefault("cooldowns", {})
    state["cooldowns"][key] = {"until": until_iso, "reason": reason}


def clear_cooldown(state: Dict[str, Any], key: str) -> None:
    state.get("cooldowns", {}).pop(key, None)


def get_cooldown(state: Dict[str, Any], key: str) -> Optional[dict]:
    return state.get("cooldowns", {}).get(key)


def _is_in_cooldown_local(state: Dict[str, Any], key: str) -> bool:
    cd = get_cooldown(state, key)
    if not cd:
        return False
    try:
        until = datetime.fromisoformat(cd["until"])
        return datetime.now() < until
    except Exception:
        return False


# -------------------------
# Option recommendations
# -------------------------
def get_option_reco(state: Dict[str, Any], underlying: str) -> Optional[dict]:
    return state.get("option_recos", {}).get(underlying)


def set_option_reco(state: Dict[str, Any], underlying: str, reco: Optional[dict]) -> None:
    state.setdefault("option_recos", {})
    if reco is None:
        state["option_recos"].pop(underlying, None)
    else:
        state["option_recos"][underlying] = reco


# -------------------------
# Options: 1 position per underlying
# keys are OPT:<underlying>
# -------------------------
def _opt_key(underlying: str) -> str:
    return f"OPT:{underlying}"


def get_option_position(state: Dict[str, Any], underlying: str) -> Optional[Position]:
    return get_position(state, _opt_key(underlying))


def set_option_position(state: Dict[str, Any], underlying: str, pos: Optional[Position]) -> None:
    set_position(state, _opt_key(underlying), pos)


def get_option_pending_order(state: Dict[str, Any], underlying: str) -> Optional[dict]:
    return get_pending_order(state, _opt_key(underlying))


def set_option_pending_order(state: Dict[str, Any], underlying: str, pending: Optional[dict]) -> None:
    set_pending_order(state, _opt_key(underlying), pending)


def is_option_in_cooldown(state: Dict[str, Any], underlying: str) -> bool:
    return _is_in_cooldown_local(state, _opt_key(underlying))


def set_option_cooldown(state: Dict[str, Any], underlying: str, until_iso: str, reason: str) -> None:
    set_cooldown(state, _opt_key(underlying), until_iso, reason)


def get_portfolio_cooldown_until(state: Dict[str, Any]) -> Optional[str]:
    return state.get("portfolio_cooldown_until")


def set_portfolio_cooldown(state: Dict[str, Any], until_iso: str) -> None:
    state["portfolio_cooldown_until"] = until_iso


def is_portfolio_in_cooldown(state: Dict[str, Any]) -> bool:
    until = state.get("portfolio_cooldown_until")
    if not until:
        return False
    try:
        return datetime.fromisoformat(until) > datetime.now()
    except Exception:
        return False


# -------------------------
# MFE / MAE tracking (position-level)
# key = symbol (equity) or OPT:<underlying> (options)
# -------------------------
def get_mfe_mae_tracker(state: Dict[str, Any], key: str) -> Optional[Dict[str, float]]:
    return state.get("mfe_mae_tracker", {}).get(key)


def init_mfe_mae_tracker(state: Dict[str, Any], key: str, entry_price: float) -> None:
    """Call when opening a position. Tracks high/low from entry for MFE/MAE on close."""
    state.setdefault("mfe_mae_tracker", {})
    state["mfe_mae_tracker"][key] = {"high": float(entry_price), "low": float(entry_price)}


def update_mfe_mae_tracker(state: Dict[str, Any], key: str, current_price: float) -> None:
    """Call each cycle while position is open. Updates running high/low."""
    t = state.get("mfe_mae_tracker", {}).get(key)
    if not t:
        return
    p = float(current_price)
    t["high"] = max(t["high"], p)
    t["low"] = min(t["low"], p)


def append_buy_time(state: Dict[str, Any]) -> None:
    """Record a buy timestamp for MAX_BUYS_PER_HOUR enforcement."""
    state.setdefault("buys_last_hour", [])
    state["buys_last_hour"].append(now_iso())


def count_buys_in_last_hour(state: Dict[str, Any], minutes: int = 60) -> int:
    """Return number of buys in the last N minutes; trim stale entries."""
    state.setdefault("buys_last_hour", [])
    cutoff = datetime.now() - timedelta(minutes=minutes)
    kept = [t for t in state["buys_last_hour"] if _parse_iso_safe(t) >= cutoff]
    state["buys_last_hour"] = kept
    return len(kept)


def _parse_iso_safe(ts: str) -> datetime:
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        return datetime.min


def get_and_clear_mfe_mae(state: Dict[str, Any], key: str, entry_price: float) -> tuple:
    """
    Call when closing a position. Returns (mfe_pct, mae_pct) as decimals (e.g. 0.02 = 2%).
    Clears the tracker for this key.
    """
    state.setdefault("mfe_mae_tracker", {})
    t = state["mfe_mae_tracker"].pop(key, None)
    entry = float(entry_price)
    if not t or entry <= 0:
        return (None, None)
    high = float(t.get("high", entry))
    low = float(t.get("low", entry))
    mfe_pct = (high - entry) / entry
    mae_pct = (low - entry) / entry
    return (mfe_pct, mae_pct)

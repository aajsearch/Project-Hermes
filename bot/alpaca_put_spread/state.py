from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from bot.alpaca_put_spread.strategy_types import StrategyType

logger = logging.getLogger(__name__)

STATE_PATH = Path("data") / "alpaca_put_spread_state.json"

# JSON state shape (schema_version >= 2):
#   open_positions: { "<UNDERLYING>": { "<STRATEGY_TYPE>": <spread dict | ...> } }
# Example: open_positions["QQQ"]["PCS"] -> single open spread for that strategy.
# Legacy flat key open_spread_by_underlying is migrated to open_positions[u]["PCS"] on load.
OPEN_POSITIONS_ROOT = "open_positions"
_SCHEMA_VERSION = 2


def _strategy_type_key(strategy_type: str | StrategyType) -> str:
    if isinstance(strategy_type, StrategyType):
        return strategy_type.value
    s = str(strategy_type or "").strip().upper()
    if not s:
        raise ValueError("strategy_type is required")
    return s


def _default_state() -> Dict[str, Any]:
    return {
        "schema_version": _SCHEMA_VERSION,
        OPEN_POSITIONS_ROOT: {},
        "pending_entry_order": {},
        "pending_close_order": {},
        "cooldown_until_ts": {},
        "entry_retry_count": {},
        "entry_disabled": {},
    }


def _migrate_legacy_to_v2(st: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
    """Migrate flat per-underlying keys to open_positions[u][PCS] etc. Returns (state, needs_save)."""
    pcs = StrategyType.PUT_CREDIT_SPREAD.value
    out = _default_state()
    needs_save = int(st.get("schema_version") or 0) < _SCHEMA_VERSION
    legacy_keys = (
        "open_spread_by_underlying",
        "pending_entry_order_by_underlying",
        "pending_close_order_by_underlying",
        "cooldown_until_ts_by_underlying",
        "entry_retry_count_by_underlying",
        "entry_disabled_by_underlying",
    )
    if any(k in st for k in legacy_keys):
        needs_save = True

    # Prefer already-nested v2 buckets when present
    for key in (
        OPEN_POSITIONS_ROOT,
        "pending_entry_order",
        "pending_close_order",
        "cooldown_until_ts",
        "entry_retry_count",
        "entry_disabled",
    ):
        cur = st.get(key)
        if isinstance(cur, dict):
            for u, by_st in cur.items():
                if isinstance(by_st, dict):
                    out[key].setdefault(str(u), {}).update(by_st)

    def ingest_legacy_flat(flat_key: str, out_key: str) -> None:
        src = st.get(flat_key) or {}
        if not isinstance(src, dict):
            return
        for u, val in src.items():
            if val is None:
                continue
            uu = str(u)
            slot = out[out_key].setdefault(uu, {})
            if pcs not in slot:
                slot[pcs] = val

    ingest_legacy_flat("open_spread_by_underlying", OPEN_POSITIONS_ROOT)
    ingest_legacy_flat("pending_entry_order_by_underlying", "pending_entry_order")
    ingest_legacy_flat("pending_close_order_by_underlying", "pending_close_order")
    ingest_legacy_flat("entry_retry_count_by_underlying", "entry_retry_count")
    ingest_legacy_flat("entry_disabled_by_underlying", "entry_disabled")

    old_cool = st.get("cooldown_until_ts_by_underlying") or {}
    if isinstance(old_cool, dict):
        for u, ts in old_cool.items():
            if ts is None:
                continue
            uu = str(u)
            slot = out["cooldown_until_ts"].setdefault(uu, {})
            if pcs not in slot:
                slot[pcs] = ts

    if needs_save:
        logger.info(
            "Alpaca spread state normalized to schema_version=%s (nested by strategy_type)",
            _SCHEMA_VERSION,
        )
    return out, needs_save


def load_state() -> Dict[str, Any]:
    try:
        if not STATE_PATH.exists():
            STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
            st = _default_state()
            STATE_PATH.write_text(json.dumps(st, indent=2), encoding="utf-8")
            return st
        raw = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        st, needs_save = _migrate_legacy_to_v2(raw if isinstance(raw, dict) else {})
        if needs_save:
            save_state(st)
        return st
    except Exception:
        return _default_state()


def save_state(state: Dict[str, Any]) -> None:
    state = dict(state)
    state["schema_version"] = _SCHEMA_VERSION
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    tmp.replace(STATE_PATH)


def get_nested(state: Dict[str, Any], root_key: str, underlying: str, strategy_type: str | StrategyType) -> Any:
    """Read state[root_key][underlying][strategy_type] (e.g. open_positions['QQQ']['PCS'])."""
    st_key = _strategy_type_key(strategy_type)
    root = state.get(root_key) or {}
    by_u = root.get(str(underlying)) or {}
    return by_u.get(st_key)


def set_nested(
    state: Dict[str, Any],
    root_key: str,
    underlying: str,
    strategy_type: str | StrategyType,
    value: Any,
) -> None:
    """Write state[root_key][underlying][strategy_type]."""
    st_key = _strategy_type_key(strategy_type)
    if root_key not in state or not isinstance(state[root_key], dict):
        state[root_key] = {}
    uu = str(underlying)
    if uu not in state[root_key] or not isinstance(state[root_key][uu], dict):
        state[root_key][uu] = {}
    state[root_key][uu][st_key] = value


def pop_nested(state: Dict[str, Any], root_key: str, underlying: str, strategy_type: str | StrategyType) -> Any:
    """Remove and return state[root_key][underlying][strategy_type]; drop empty underlying buckets."""
    st_key = _strategy_type_key(strategy_type)
    root = state.get(root_key) or {}
    uu = str(underlying)
    by_u = root.get(uu)
    if not isinstance(by_u, dict):
        return None
    val = by_u.pop(st_key, None)
    if isinstance(by_u, dict) and not by_u:
        root.pop(uu, None)
    return val


def get_open_position(
    state: Dict[str, Any],
    underlying: str,
    strategy_type: str | StrategyType,
) -> Any:
    """Return open spread (or None) for open_positions[underlying][strategy_type]."""
    return get_nested(state, OPEN_POSITIONS_ROOT, underlying, strategy_type)


def set_open_position(
    state: Dict[str, Any],
    underlying: str,
    strategy_type: str | StrategyType,
    value: Any,
) -> None:
    """Set open_positions[underlying][strategy_type]."""
    set_nested(state, OPEN_POSITIONS_ROOT, underlying, strategy_type, value)


def pop_open_position(
    state: Dict[str, Any],
    underlying: str,
    strategy_type: str | StrategyType,
) -> Any:
    """Remove and return open_positions[underlying][strategy_type]."""
    return pop_nested(state, OPEN_POSITIONS_ROOT, underlying, strategy_type)


def ensure_state_shape(state: Dict[str, Any]) -> None:
    """Ensure keys exist after manual edits or partial loads."""
    state.setdefault("schema_version", _SCHEMA_VERSION)
    for k in (
        OPEN_POSITIONS_ROOT,
        "pending_entry_order",
        "pending_close_order",
        "cooldown_until_ts",
        "entry_retry_count",
        "entry_disabled",
    ):
        state.setdefault(k, {})

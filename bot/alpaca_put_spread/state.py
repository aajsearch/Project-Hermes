from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional


STATE_PATH = Path("data") / "alpaca_put_spread_state.json"


def _default_state() -> Dict[str, Any]:
    # Persisted state is used to avoid duplicate orders and to recover pending
    # orders after bot restarts.
    return {
        "open_spread_by_underlying": {},
        # Entry order placed but not yet filled (or not yet terminal).
        "pending_entry_order_by_underlying": {},
        # Close order placed while the open spread is still in state.
        "pending_close_order_by_underlying": {},
        # Cooldown gating for new entry hunting after a close fills.
        # Value is UNIX timestamp (seconds).
        "cooldown_until_ts_by_underlying": {},
        # Track how many times we already attempted entry for each underlying.
        "entry_retry_count_by_underlying": {},
        # If we exceed retry limits, we stop trying to open for that underlying.
        "entry_disabled_by_underlying": {},
    }


def load_state() -> Dict[str, Any]:
    try:
        if not STATE_PATH.exists():
            STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
            st = _default_state()
            STATE_PATH.write_text(json.dumps(st, indent=2), encoding="utf-8")
            return st
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return _default_state()


def save_state(state: Dict[str, Any]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    tmp.replace(STATE_PATH)


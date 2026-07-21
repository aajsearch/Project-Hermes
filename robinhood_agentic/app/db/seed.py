"""Seed DB from YAML + session_state.json on first boot."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from .connection import init_db
from .repository import Repository


def _parse_tech_yaml(path: Path) -> dict[str, Any]:
    """Minimal YAML subset parser (mirrors session_monitor.load_yaml)."""
    text = path.read_text()
    cfg: dict[str, Any] = {
        "account": {},
        "scalp": {},
        "selection": {},
        "position_sizing": {},
        "entry": {},
        "watchlist": {},
        "portfolio": {},
    }

    def num(key: str, section: dict, default: float) -> None:
        m = re.search(rf"^\s*{re.escape(key)}:\s*([\d.]+)", text, re.M)
        if m:
            section[key] = float(m.group(1)) if "." in m.group(1) else int(m.group(1))
        else:
            section[key] = default

    def str_bool(key: str, section: dict, default: bool = False) -> None:
        m = re.search(rf"^\s*{re.escape(key)}:\s*(true|false)", text, re.M)
        section[key] = (m.group(1) == "true") if m else default

    num("poll_seconds", cfg["scalp"], 15)
    num("stop_loss_clear_bps", cfg["scalp"], 15)
    num("profit_target_pct", cfg["scalp"], 0.006)
    num("stop_loss_pct", cfg["scalp"], 0.0045)
    num("target_notional_usd", cfg["position_sizing"], 100)
    num("max_concurrent", cfg["position_sizing"], 2)
    num("cooldown_minutes_after_sl", cfg["position_sizing"], 30)
    num("max_stop_losses_per_day", cfg["position_sizing"], 5)
    m = re.search(r"reserve_usd:\s*([\d.]+)", text)
    cfg["account"]["reserve_usd"] = float(m.group(1)) if m else 50.0
    num("max_spread_pct", cfg["selection"], 0.0015)
    num("max_spread_usd", cfg["selection"], 0.25)
    num("min_price_usd", cfg["selection"], 15)
    num("min_abs_day_change_pct", cfg["selection"], 0.003)
    num("max_abs_day_change_pct", cfg["selection"], 0.008)
    str_bool("allow_fractional_live", cfg["entry"], False)
    str_bool("prefer_whole_shares", cfg["entry"], True)
    num("limit_entry_offset_bps", cfg["entry"], 5)
    str_bool("allow_equity_and_options_same_day", cfg["portfolio"], True)

    for key, default in (
        ("no_new_entry_before_et", "09:45"),
        ("no_new_entry_after_et", "15:30"),
        ("hard_flat_time_et", "15:55"),
    ):
        m = re.search(rf'^\s*{re.escape(key)}:\s*"([^"]+)"', text, re.M)
        cfg["scalp"][key] = m.group(1) if m else default

    watchlist_block = re.search(r"^watchlist:\s*\n(.*?)(?=^\S|\Z)", text, re.M | re.S)
    if watchlist_block:
        symbols = re.findall(r"^\s+-\s+([A-Z]+)\s*$", watchlist_block.group(1), re.M)
        cfg["watchlist"]["all"] = list(dict.fromkeys(symbols))
    else:
        cfg["watchlist"]["all"] = []

    core_block = re.search(r"core_symbols:\s*\n((?:\s+-\s+\w+\s*\n)+)", text)
    cfg["selection"]["core_symbols"] = (
        re.findall(r"-\s+(\w+)", core_block.group(1)) if core_block else []
    )
    dep_block = re.search(r"deprioritize_symbols:\s*\n((?:\s+-\s+\w+\s*\n)+)", text)
    cfg["selection"]["deprioritize_symbols"] = (
        re.findall(r"-\s+(\w+)", dep_block.group(1)) if dep_block else []
    )
    return cfg


def _parse_options_yaml(path: Path) -> dict[str, Any]:
    text = path.read_text() if path.is_file() else ""
    opts: dict[str, Any] = {
        "trade": {
            "profit_target_pct": 0.15,
            "stop_loss_pct": 0.10,
            "hard_flat_time_et": "15:45",
            "no_new_entry_before_et": "10:00",
            "no_new_entry_after_et": "15:00",
        },
        "position_sizing": {
            "max_contracts_per_trade": 1,
            "max_premium_usd": 75,
            "max_concurrent_positions": 1,
            "max_trades_per_day": 2,
        },
        "risk": {
            "max_daily_realized_loss_usd": 75,
            "circuit_breaker_losses_per_day": 2,
        },
        "underlyings": {"core": ["SPY", "QQQ"], "extended": [], "pilot_core_only": False},
        "contract_selection": {
            "delta_target": 0.15,
            "delta_tolerance": 0.10,
            "dte_min": 1,
            "dte_max": 5,
            "min_open_interest": 500,
            "max_bid_ask_spread_pct": 0.10,
        },
        "direction": {
            "regime_symbol": "QQQ",
            "bull_min_day_change_pct": 0.005,
            "bear_max_day_change_pct": -0.005,
        },
    }

    def grab_float(key: str, default: float) -> float:
        m = re.search(rf"^\s*{re.escape(key)}:\s*([\d.-]+)", text, re.M)
        return float(m.group(1)) if m else default

    def grab_str(key: str, default: str) -> str:
        m = re.search(rf'^\s*{re.escape(key)}:\s*"([^"]+)"', text, re.M)
        return m.group(1) if m else default

    opts["trade"]["profit_target_pct"] = grab_float("profit_target_pct", 0.15)
    opts["trade"]["stop_loss_pct"] = grab_float("stop_loss_pct", 0.10)
    opts["trade"]["hard_flat_time_et"] = grab_str("hard_flat_time_et", "15:45")
    opts["trade"]["no_new_entry_before_et"] = grab_str("no_new_entry_before_et", "10:00")
    opts["trade"]["no_new_entry_after_et"] = grab_str("no_new_entry_after_et", "15:00")
    opts["position_sizing"]["max_premium_usd"] = grab_float("max_premium_usd", 75)
    opts["risk"]["circuit_breaker_losses_per_day"] = int(
        grab_float("circuit_breaker_losses_per_day", 2)
    )
    opts["risk"]["max_daily_realized_loss_usd"] = grab_float("max_daily_realized_loss_usd", 75)

    core = re.search(r"core:\s*\n((?:\s+-\s+\w+\s*\n)+)", text)
    if core:
        opts["underlyings"]["core"] = re.findall(r"-\s+(\w+)", core.group(1))
    ext = re.search(r"extended:\s*\n((?:\s+-\s+\w+\s*\n)+)", text)
    if ext:
        opts["underlyings"]["extended"] = re.findall(r"-\s+(\w+)", ext.group(1))
    return opts


def seed_database(
    db_path: Path,
    tech_yaml: Path,
    options_yaml: Path,
    session_state: Path,
) -> Repository:
    conn = init_db(db_path)
    repo = Repository(conn)

    account_number = None
    nickname = "Agentic"
    if session_state.is_file():
        state = json.loads(session_state.read_text())
        account_number = state.get("account_number")
        nickname = state.get("account_nickname") or "Agentic"
        if not repo.list_positions() and not repo.list_option_positions():
            for p in state.get("positions", []):
                repo.upsert_position(p)
            for p in state.get("option_positions", []):
                repo.upsert_option_position(p)

    repo.ensure_system_state(
        {"account_number": account_number, "account_nickname": nickname, "mode": "autonomous"}
    )

    if not repo.get_active_config("tech_scalper") and tech_yaml.is_file():
        tech = _parse_tech_yaml(tech_yaml)
        repo.save_config("tech_scalper", tech, source="yaml_seed", updated_by="bootstrap")
        poll = int(tech.get("scalp", {}).get("poll_seconds", 15))
        repo.update_system_state(poll_seconds=poll, account_number=account_number)

    if not repo.get_active_config("options_directional") and options_yaml.is_file():
        opts = _parse_options_yaml(options_yaml)
        repo.save_config(
            "options_directional", opts, source="yaml_seed", updated_by="bootstrap"
        )

    repo.audit("bootstrap", {"db": str(db_path)})
    return repo

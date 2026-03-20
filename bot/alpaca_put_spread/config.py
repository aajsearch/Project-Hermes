from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, FrozenSet, List, Optional

import yaml


def _parse_trade_window_weekdays(raw: Any) -> FrozenSet[int]:
    """
    Map YAML list of weekday names to Python weekday ints (Monday=0 ... Sunday=6).
    """
    _aliases = {
        "mon": 0,
        "monday": 0,
        "tue": 1,
        "tues": 1,
        "tuesday": 1,
        "wed": 2,
        "weds": 2,
        "wednesday": 2,
        "thu": 3,
        "thur": 3,
        "thurs": 3,
        "thursday": 3,
        "fri": 4,
        "friday": 4,
        "sat": 5,
        "saturday": 5,
        "sun": 6,
        "sunday": 6,
    }
    if raw is None:
        return frozenset(range(5))
    if not isinstance(raw, list):
        raise ValueError("trade_window_weekdays must be a list of weekday names (e.g. mon, tue)")
    out: set[int] = set()
    for item in raw:
        key = str(item).strip().lower()
        if key not in _aliases:
            raise ValueError(
                f"trade_window_weekdays: unknown weekday {item!r}; use mon..sun / monday..sunday"
            )
        out.add(_aliases[key])
    if not out:
        raise ValueError("trade_window_weekdays must not be empty (omit the key for Mon–Fri default)")
    return frozenset(out)


@dataclass(frozen=True)
class AlpacaPutSpreadConfig:
    paper: bool
    execute: bool
    loop_sleep_seconds: float
    order_qty: int
    underlyings_mon_to_fri: List[str]
    underlyings_mon_wed_fri: List[str]

    dte_min: int
    dte_max: int
    spread_pct_max: float
    iv_max: float

    long_delta_abs_min: float
    long_delta_abs_max: float
    long_target_delta: float

    short_delta_abs_min: float
    short_delta_abs_max: float
    short_target_delta: float

    target_credit: float
    entry_operator: str
    put_spread_width_points_min: float
    put_spread_width_points_max: float
    min_short_otm_percent: float

    tp_pct: float
    sl_pct: float
    # When set, distance gating is done in OTM% terms:
    #   threshold_otm_pct = min_short_otm_percent * distance_buffer_otm_fraction_of_min_short_otm
    # We will NOT exit on TP/SL unless OTM% <= threshold_otm_pct.
    distance_buffer_otm_fraction_of_min_short_otm: Optional[float]
    # Backward-compat: old absolute-distance exit buffer (points) used only when
    # distance_buffer_otm_fraction_of_min_short_otm is not provided.
    distance_buffer_points: float
    exit_before_minutes: int
    exit_cooldown_minutes: int
    close_limit_slippage_pct: float

    max_entry_retries: int
    order_fill_timeout_seconds: int
    order_cancel_timeout_seconds: int

    # Trading window gating for opening/hunting new positions.
    # If unset, the bot hunts continuously.
    trade_window_timezone: Optional[str]
    trade_window_start_time_local: Optional[str]  # "HH:MM"
    trade_window_end_time_local: Optional[str]  # "HH:MM"
    # When a time window is configured: allowed local weekdays (Mon=0..Sun=6). Ignored if no window.
    trade_window_weekdays: FrozenSet[int]

    # Loss caps (operational safety). When daily PnL <= -cap, block new entries.
    # Set to 0 or omit to disable.
    max_daily_loss_dollars: float
    max_loss_per_underlying_dollars: float

    def get_underlyings_for_today(self) -> List[str]:
        """
        Mon–Fri list is always active.
        On Mon/Wed/Fri we trade Mon–Fri + Mon/Wed/Fri add-on list.
        """
        import datetime as _dt

        wd = _dt.datetime.now().weekday()  # Monday=0 ... Sunday=6
        base = set(self.underlyings_mon_to_fri)
        if wd in (0, 2, 4):
            base = base.union(self.underlyings_mon_wed_fri)
        return sorted(base)


def load_alpaca_put_spread_config(config_dir: str | Path = "config") -> AlpacaPutSpreadConfig:
    path = Path(config_dir) / "alpaca_put_spread.yaml"
    if not path.exists():
        raise FileNotFoundError(f"Missing config: {path}")

    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or "alpaca_put_spread" not in raw:
        raise ValueError("Invalid alpaca_put_spread.yaml schema: missing 'alpaca_put_spread' root key")

    cfg: Dict[str, Any] = raw["alpaca_put_spread"] or {}
    runtime: Dict[str, Any] = cfg.get("runtime") or {}
    paper = bool(runtime.get("paper", True))
    execute = bool(runtime.get("execute", False))
    loop_sleep_seconds = float(runtime.get("loop_sleep_seconds", 5))

    # New schedule schema (preferred)
    mon_to_fri = cfg.get("underlyings_mon_to_fri") or []
    mon_wed_fri = cfg.get("underlyings_mon_wed_fri") or []

    # Backward compatible: if legacy `underlyings` exists, treat it as Mon–Fri base.
    if not mon_to_fri and not mon_wed_fri and cfg.get("underlyings") is not None:
        legacy = cfg.get("underlyings") or []
        mon_to_fri = legacy

    if not isinstance(mon_to_fri, list) or not mon_to_fri:
        raise ValueError("alpaca_put_spread.underlyings_mon_to_fri must be a non-empty list")
    if not isinstance(mon_wed_fri, list):
        raise ValueError("alpaca_put_spread.underlyings_mon_wed_fri must be a list (can be empty)")

    mon_to_fri = [str(x).strip().upper() for x in mon_to_fri if str(x).strip()]
    mon_wed_fri = [str(x).strip().upper() for x in mon_wed_fri if str(x).strip()]
    if not mon_to_fri:
        raise ValueError("alpaca_put_spread.underlyings_mon_to_fri must contain valid symbols")

    options: Dict[str, Any] = cfg.get("options") or {}
    dte_min = int(options.get("dte_min", 0))
    dte_max = int(options.get("dte_max", 0))
    spread_pct_max = float(options.get("spread_pct_max", 0.30))
    iv_max = float(options.get("iv_max", 2.5))

    long_cfg = options.get("long_put") or {}
    short_cfg = options.get("short_put") or {}

    long_delta_abs_min = float(long_cfg.get("delta_abs_min", 0.20))
    long_delta_abs_max = float(long_cfg.get("delta_abs_max", 0.35))
    long_target_delta = float(long_cfg.get("target_delta", -0.28))

    short_delta_abs_min = float(short_cfg.get("delta_abs_min", 0.40))
    short_delta_abs_max = float(short_cfg.get("delta_abs_max", 0.60))
    short_target_delta = float(short_cfg.get("target_delta", -0.48))

    entry: Dict[str, Any] = cfg.get("entry") or {}
    target_credit = float(entry.get("target_credit", 0.15))
    entry_operator = str(entry.get("entry_operator", ">=")).strip()
    if entry_operator not in (">=", "<="):
        raise ValueError("alpaca_put_spread.entry.entry_operator must be '>=' or '<='")
    # Strike-width constraints (bull put vertical): width = short_strike - long_strike
    put_spread_width_points_min = float(
        entry.get("put_spread_width_points_min", entry.get("min_put_spread_width_points", 1.0))
    )
    put_spread_width_points_max = float(entry.get("put_spread_width_points_max", put_spread_width_points_min))

    # Minimum short-put OTM distance at entry: short_strike <= underlying_spot * (1 - min_short_otm_percent)
    min_short_otm_percent = float(entry.get("min_short_otm_percent", 0.0))

    exit_cfg: Dict[str, Any] = cfg.get("exit") or {}
    tp_pct = float(exit_cfg.get("tp_pct", 0.30))
    sl_pct = float(exit_cfg.get("sl_pct", 0.30))
    distance_buffer_points = float(exit_cfg.get("distance_buffer_points", 5.0))
    distance_buffer_otm_fraction_of_min_short_otm_raw = exit_cfg.get(
        "distance_buffer_otm_fraction_of_min_short_otm"
    )
    distance_buffer_otm_fraction_of_min_short_otm: Optional[float] = None
    if distance_buffer_otm_fraction_of_min_short_otm_raw is not None:
        distance_buffer_otm_fraction_of_min_short_otm = float(distance_buffer_otm_fraction_of_min_short_otm_raw)
    exit_before_minutes = int(exit_cfg.get("exit_before_minutes", 3))
    exit_cooldown_minutes = int(exit_cfg.get("exit_cooldown_minutes", 15))
    close_limit_slippage_pct = float(exit_cfg.get("close_limit_slippage_pct", 0.05))

    max_entry_retries = int(runtime.get("max_entry_retries", 1))
    order_fill_timeout_seconds = int(runtime.get("order_fill_timeout_seconds", 60))
    order_cancel_timeout_seconds = int(runtime.get("order_cancel_timeout_seconds", 5))
    order_qty = int(runtime.get("order_qty", 1))
    if order_qty <= 0:
        raise ValueError("alpaca_put_spread.runtime.order_qty must be >= 1")

    risk = cfg.get("risk") or {}
    trade_window_timezone = runtime.get("trade_window_timezone") or risk.get("trade_window_timezone")
    trade_window_start_time_local = runtime.get("trade_window_start_time_local") or risk.get("trade_window_start_time_local")
    trade_window_end_time_local = runtime.get("trade_window_end_time_local") or risk.get("trade_window_end_time_local")
    trade_window_weekdays_raw = runtime.get("trade_window_weekdays")
    if trade_window_weekdays_raw is None:
        trade_window_weekdays_raw = risk.get("trade_window_weekdays")

    # If user sets one of start/end, require all 3 window fields.
    window_any = trade_window_timezone is not None or trade_window_start_time_local is not None or trade_window_end_time_local is not None
    window_all = trade_window_timezone is not None and trade_window_start_time_local is not None and trade_window_end_time_local is not None
    if window_any and not window_all:
        raise ValueError(
            "alpaca_put_spread.runtime: set trade_window_timezone + trade_window_start_time_local + trade_window_end_time_local together"
        )

    if window_all:
        trade_window_weekdays = _parse_trade_window_weekdays(trade_window_weekdays_raw)
    else:
        trade_window_weekdays = frozenset(range(7))

    max_daily_loss_dollars = float(risk.get("max_daily_loss_dollars", 0))
    max_loss_per_underlying_dollars = float(risk.get("max_loss_per_underlying_dollars", 0))

    return AlpacaPutSpreadConfig(
        paper=paper,
        execute=execute,
        loop_sleep_seconds=loop_sleep_seconds,
        order_qty=order_qty,
        underlyings_mon_to_fri=sorted(mon_to_fri),
        underlyings_mon_wed_fri=sorted(mon_wed_fri),
        dte_min=dte_min,
        dte_max=dte_max,
        spread_pct_max=spread_pct_max,
        iv_max=iv_max,
        long_delta_abs_min=long_delta_abs_min,
        long_delta_abs_max=long_delta_abs_max,
        long_target_delta=long_target_delta,
        short_delta_abs_min=short_delta_abs_min,
        short_delta_abs_max=short_delta_abs_max,
        short_target_delta=short_target_delta,
        target_credit=target_credit,
        entry_operator=entry_operator,
        put_spread_width_points_min=put_spread_width_points_min,
        put_spread_width_points_max=put_spread_width_points_max,
        min_short_otm_percent=min_short_otm_percent,
        tp_pct=tp_pct,
        sl_pct=sl_pct,
        distance_buffer_otm_fraction_of_min_short_otm=distance_buffer_otm_fraction_of_min_short_otm,
        distance_buffer_points=distance_buffer_points,
        exit_before_minutes=exit_before_minutes,
        exit_cooldown_minutes=exit_cooldown_minutes,
        close_limit_slippage_pct=close_limit_slippage_pct,
        max_entry_retries=max_entry_retries,
        order_fill_timeout_seconds=order_fill_timeout_seconds,
        order_cancel_timeout_seconds=order_cancel_timeout_seconds,
        trade_window_timezone=trade_window_timezone,
        trade_window_start_time_local=trade_window_start_time_local,
        trade_window_end_time_local=trade_window_end_time_local,
        trade_window_weekdays=trade_window_weekdays,
        max_daily_loss_dollars=max_daily_loss_dollars,
        max_loss_per_underlying_dollars=max_loss_per_underlying_dollars,
    )



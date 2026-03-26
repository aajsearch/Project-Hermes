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
class PutCreditSpreadStrategyConfig:
    """Tunables for put credit spread (PCS). Phase 1: fully wired in the runner."""

    enabled: bool
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
    distance_buffer_otm_fraction_of_min_short_otm: Optional[float]
    distance_buffer_points: float
    exit_before_minutes: int
    exit_cooldown_minutes: int
    close_limit_slippage_pct: float


@dataclass(frozen=True)
class CallCreditSpreadStrategyConfig:
    """Tunables for call credit spread (CCS / bear call); mirrors PCS shape where applicable."""

    enabled: bool
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
    call_spread_width_points_min: float
    call_spread_width_points_max: float
    min_short_otm_percent: float

    tp_pct: float
    sl_pct: float
    distance_buffer_otm_fraction_of_min_short_otm: Optional[float]
    distance_buffer_points: float
    exit_before_minutes: int
    exit_cooldown_minutes: int
    close_limit_slippage_pct: float


@dataclass(frozen=True)
class IronCondorStrategyConfig:
    """Tunables for iron condor (4-leg); symmetrical long/short leg delta bands in YAML."""

    enabled: bool
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
    wing_width_points_min: float
    wing_width_points_max: float

    tp_pct: float
    sl_pct: float
    exit_before_minutes: int
    exit_cooldown_minutes: int
    close_limit_slippage_pct: float

    # Optional OTM/distance gating when IC runner is wired (omit in YAML if unused).
    min_short_otm_percent: Optional[float]
    distance_buffer_otm_fraction_of_min_short_otm: Optional[float]
    distance_buffer_points: float


@dataclass(frozen=True)
class AlpacaPutSpreadConfig:
    """Root bot config: runtime, risk, underlyings, and per-strategy blocks."""

    paper: bool
    execute: bool
    loop_sleep_seconds: float
    order_qty: int
    underlyings_mon_to_fri: List[str]
    underlyings_mon_wed_fri: List[str]

    max_entry_retries: int
    order_fill_timeout_seconds: int
    order_cancel_timeout_seconds: int

    trade_window_timezone: Optional[str]
    trade_window_start_time_local: Optional[str]
    trade_window_end_time_local: Optional[str]
    trade_window_weekdays: FrozenSet[int]

    max_daily_loss_dollars: float
    max_loss_per_underlying_dollars: float
    # 0 = no numeric cap; >=1 caps total active slots (open or pending entry) per underlying.
    max_open_spreads_per_underlying: int

    put_credit_spread: PutCreditSpreadStrategyConfig
    call_credit_spread: CallCreditSpreadStrategyConfig
    iron_condor: IronCondorStrategyConfig

    def __getattr__(self, name: str) -> Any:
        """Delegate PCS fields to ``put_credit_spread`` (runner uses flat ``cfg.<pcs_field>``)."""
        pcs = object.__getattribute__(self, "put_credit_spread")
        if hasattr(pcs, name):
            return getattr(pcs, name)
        raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")

    def get_underlyings_for_today(self) -> List[str]:
        import datetime as _dt

        wd = _dt.datetime.now().weekday()
        base = set(self.underlyings_mon_to_fri)
        if wd in (0, 2, 4):
            base = base.union(self.underlyings_mon_wed_fri)
        return sorted(base)


def _load_put_credit_spread_block(cfg: Dict[str, Any]) -> PutCreditSpreadStrategyConfig:
    """
    Load PCS tunables from strategies.put_credit_spread if present; otherwise from root
    options/entry/exit (backward compatible).
    """
    strategies = cfg.get("strategies") or {}
    pcs_block = strategies.get("put_credit_spread")

    if pcs_block is not None and isinstance(pcs_block, dict):
        enabled = bool(pcs_block.get("enabled", True))
        options = pcs_block.get("options") or {}
        entry = pcs_block.get("entry") or {}
        exit_cfg = pcs_block.get("exit") or {}
    else:
        enabled = True
        options = cfg.get("options") or {}
        entry = cfg.get("entry") or {}
        exit_cfg = cfg.get("exit") or {}

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

    target_credit = float(entry.get("target_credit", 0.15))
    entry_operator = str(entry.get("entry_operator", ">=")).strip()
    if entry_operator not in (">=", "<="):
        raise ValueError("put_credit_spread.entry.entry_operator must be '>=' or '<='")
    put_spread_width_points_min = float(
        entry.get("put_spread_width_points_min", entry.get("min_put_spread_width_points", 1.0))
    )
    put_spread_width_points_max = float(entry.get("put_spread_width_points_max", put_spread_width_points_min))
    min_short_otm_percent = float(entry.get("min_short_otm_percent", 0.0))

    tp_pct = float(exit_cfg.get("tp_pct", 0.30))
    sl_pct = float(exit_cfg.get("sl_pct", 0.30))
    distance_buffer_points = float(exit_cfg.get("distance_buffer_points", 5.0))
    distance_buffer_otm_raw = exit_cfg.get("distance_buffer_otm_fraction_of_min_short_otm")
    distance_buffer_otm_fraction: Optional[float] = None
    if distance_buffer_otm_raw is not None:
        distance_buffer_otm_fraction = float(distance_buffer_otm_raw)
    exit_before_minutes = int(exit_cfg.get("exit_before_minutes", 3))
    exit_cooldown_minutes = int(exit_cfg.get("exit_cooldown_minutes", 15))
    close_limit_slippage_pct = float(exit_cfg.get("close_limit_slippage_pct", 0.05))

    return PutCreditSpreadStrategyConfig(
        enabled=enabled,
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
        distance_buffer_otm_fraction_of_min_short_otm=distance_buffer_otm_fraction,
        distance_buffer_points=distance_buffer_points,
        exit_before_minutes=exit_before_minutes,
        exit_cooldown_minutes=exit_cooldown_minutes,
        close_limit_slippage_pct=close_limit_slippage_pct,
    )


def _load_call_credit_spread_block(cfg: Dict[str, Any]) -> CallCreditSpreadStrategyConfig:
    strategies = cfg.get("strategies") or {}
    block = strategies.get("call_credit_spread")
    if not isinstance(block, dict):
        block = {}
    enabled = bool(block.get("enabled", False))
    options = block.get("options") or {}
    entry = block.get("entry") or {}
    exit_cfg = block.get("exit") or {}

    dte_min = int(options.get("dte_min", 0))
    dte_max = int(options.get("dte_max", 0))
    spread_pct_max = float(options.get("spread_pct_max", 0.30))
    iv_max = float(options.get("iv_max", 2.5))

    long_cfg = options.get("long_call") or {}
    short_cfg = options.get("short_call") or {}

    long_delta_abs_min = float(long_cfg.get("delta_abs_min", 0.02))
    long_delta_abs_max = float(long_cfg.get("delta_abs_max", 0.06))
    long_target_delta = float(long_cfg.get("target_delta", 0.04))

    short_delta_abs_min = float(short_cfg.get("delta_abs_min", 0.08))
    short_delta_abs_max = float(short_cfg.get("delta_abs_max", 0.16))
    short_target_delta = float(short_cfg.get("target_delta", 0.12))

    target_credit = float(entry.get("target_credit", 0.15))
    entry_operator = str(entry.get("entry_operator", ">=")).strip()
    if entry_operator not in (">=", "<="):
        raise ValueError("call_credit_spread.entry.entry_operator must be '>=' or '<='")

    call_spread_width_points_min = float(entry.get("call_spread_width_points_min", 1.0))
    call_spread_width_points_max = float(
        entry.get("call_spread_width_points_max", call_spread_width_points_min)
    )
    min_short_otm_percent = float(entry.get("min_short_otm_percent", 0.02))

    tp_pct = float(exit_cfg.get("tp_pct", 0.60))
    sl_pct = float(exit_cfg.get("sl_pct", 2.0))
    distance_buffer_points = float(exit_cfg.get("distance_buffer_points", 5.0))
    distance_buffer_otm_raw = exit_cfg.get("distance_buffer_otm_fraction_of_min_short_otm")
    distance_buffer_otm_fraction: Optional[float] = None
    if distance_buffer_otm_raw is not None:
        distance_buffer_otm_fraction = float(distance_buffer_otm_raw)
    exit_before_minutes = int(exit_cfg.get("exit_before_minutes", 3))
    exit_cooldown_minutes = int(exit_cfg.get("exit_cooldown_minutes", 15))
    close_limit_slippage_pct = float(exit_cfg.get("close_limit_slippage_pct", 0.02))

    return CallCreditSpreadStrategyConfig(
        enabled=enabled,
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
        call_spread_width_points_min=call_spread_width_points_min,
        call_spread_width_points_max=call_spread_width_points_max,
        min_short_otm_percent=min_short_otm_percent,
        tp_pct=tp_pct,
        sl_pct=sl_pct,
        distance_buffer_otm_fraction_of_min_short_otm=distance_buffer_otm_fraction,
        distance_buffer_points=distance_buffer_points,
        exit_before_minutes=exit_before_minutes,
        exit_cooldown_minutes=exit_cooldown_minutes,
        close_limit_slippage_pct=close_limit_slippage_pct,
    )


def _load_iron_condor_block(cfg: Dict[str, Any]) -> IronCondorStrategyConfig:
    strategies = cfg.get("strategies") or {}
    block = strategies.get("iron_condor")
    if not isinstance(block, dict):
        block = {}
    enabled = bool(block.get("enabled", False))
    options = block.get("options") or {}
    entry = block.get("entry") or {}
    exit_cfg = block.get("exit") or {}

    dte_min = int(options.get("dte_min", 0))
    dte_max = int(options.get("dte_max", 0))
    spread_pct_max = float(options.get("spread_pct_max", 0.35))
    iv_max = float(options.get("iv_max", 2.5))

    long_cfg = options.get("long_legs") or {}
    short_cfg = options.get("short_legs") or {}

    long_delta_abs_min = float(long_cfg.get("delta_abs_min", 0.02))
    long_delta_abs_max = float(long_cfg.get("delta_abs_max", 0.08))
    long_target_delta = float(long_cfg.get("target_delta", 0.05))

    short_delta_abs_min = float(short_cfg.get("delta_abs_min", 0.09))
    short_delta_abs_max = float(short_cfg.get("delta_abs_max", 0.15))
    short_target_delta = float(short_cfg.get("target_delta", 0.12))

    target_credit = float(entry.get("target_credit", 0.35))
    entry_operator = str(entry.get("entry_operator", ">=")).strip()
    if entry_operator not in (">=", "<="):
        raise ValueError("iron_condor.entry.entry_operator must be '>=' or '<='")

    wing_width_points_min = float(entry.get("wing_width_points_min", 2.0))
    wing_width_points_max = float(entry.get("wing_width_points_max", wing_width_points_min))

    min_short_otm_raw = entry.get("min_short_otm_percent")
    min_short_otm_percent: Optional[float] = (
        float(min_short_otm_raw) if min_short_otm_raw is not None else None
    )

    tp_pct = float(exit_cfg.get("tp_pct", 0.50))
    sl_pct = float(exit_cfg.get("sl_pct", 1.5))
    exit_cooldown_minutes = int(exit_cfg.get("exit_cooldown_minutes", 30))
    exit_before_minutes = int(exit_cfg.get("exit_before_minutes", 5))
    close_limit_slippage_pct = float(exit_cfg.get("close_limit_slippage_pct", 0.03))

    distance_buffer_otm_raw = exit_cfg.get("distance_buffer_otm_fraction_of_min_short_otm")
    distance_buffer_otm_fraction: Optional[float] = (
        float(distance_buffer_otm_raw) if distance_buffer_otm_raw is not None else None
    )
    distance_buffer_points = float(exit_cfg.get("distance_buffer_points", 5.0))

    return IronCondorStrategyConfig(
        enabled=enabled,
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
        wing_width_points_min=wing_width_points_min,
        wing_width_points_max=wing_width_points_max,
        tp_pct=tp_pct,
        sl_pct=sl_pct,
        exit_before_minutes=exit_before_minutes,
        exit_cooldown_minutes=exit_cooldown_minutes,
        close_limit_slippage_pct=close_limit_slippage_pct,
        min_short_otm_percent=min_short_otm_percent,
        distance_buffer_otm_fraction_of_min_short_otm=distance_buffer_otm_fraction,
        distance_buffer_points=distance_buffer_points,
    )


def _load_strategy_toggle(block: Any, *, default_enabled: bool = False) -> bool:
    if block is None:
        return default_enabled
    if isinstance(block, dict):
        return bool(block.get("enabled", default_enabled))
    return bool(block)


def _resolve_alpaca_options_yaml_path(config_dir: Path) -> Path:
    """Prefer config/alpaca_options.yaml; fall back to legacy alpaca_put_spread.yaml."""
    preferred = config_dir / "alpaca_options.yaml"
    legacy = config_dir / "alpaca_put_spread.yaml"
    if preferred.exists():
        return preferred
    if legacy.exists():
        return legacy
    return preferred


def _extract_alpaca_options_root(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("Alpaca options config must be a YAML mapping at the top level")
    for key in ("alpaca_options", "alpaca_put_spread"):
        block = raw.get(key)
        if block is not None:
            return block if isinstance(block, dict) else {}
    raise ValueError(
        "Invalid Alpaca options YAML: missing top-level 'alpaca_options' "
        "(or legacy 'alpaca_put_spread'). See config/alpaca_options.yaml."
    )


def load_alpaca_options_config(config_dir: str | Path = "config") -> AlpacaPutSpreadConfig:
    d = Path(config_dir)
    path = _resolve_alpaca_options_yaml_path(d)
    if not path.exists():
        raise FileNotFoundError(
            f"Missing config: expected {d / 'alpaca_options.yaml'} "
            f"(or legacy {d / 'alpaca_put_spread.yaml'})"
        )

    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    cfg: Dict[str, Any] = _extract_alpaca_options_root(raw)
    runtime: Dict[str, Any] = cfg.get("runtime") or {}
    paper = bool(runtime.get("paper", True))
    execute = bool(runtime.get("execute", False))
    loop_sleep_seconds = float(runtime.get("loop_sleep_seconds", 5))

    mon_to_fri = cfg.get("underlyings_mon_to_fri") or []
    mon_wed_fri = cfg.get("underlyings_mon_wed_fri") or []

    if not mon_to_fri and not mon_wed_fri and cfg.get("underlyings") is not None:
        mon_to_fri = cfg.get("underlyings") or []

    if not isinstance(mon_to_fri, list) or not mon_to_fri:
        raise ValueError("alpaca_options.underlyings_mon_to_fri must be a non-empty list")
    if not isinstance(mon_wed_fri, list):
        raise ValueError("alpaca_options.underlyings_mon_wed_fri must be a list (can be empty)")

    mon_to_fri = [str(x).strip().upper() for x in mon_to_fri if str(x).strip()]
    mon_wed_fri = [str(x).strip().upper() for x in mon_wed_fri if str(x).strip()]
    if not mon_to_fri:
        raise ValueError("alpaca_options.underlyings_mon_to_fri must contain valid symbols")

    max_entry_retries = int(runtime.get("max_entry_retries", 1))
    order_fill_timeout_seconds = int(runtime.get("order_fill_timeout_seconds", 60))
    order_cancel_timeout_seconds = int(runtime.get("order_cancel_timeout_seconds", 5))
    order_qty = int(runtime.get("order_qty", 1))
    if order_qty <= 0:
        raise ValueError("alpaca_options.runtime.order_qty must be >= 1")

    risk = cfg.get("risk") or {}
    trade_window_timezone = runtime.get("trade_window_timezone") or risk.get("trade_window_timezone")
    trade_window_start_time_local = runtime.get("trade_window_start_time_local") or risk.get(
        "trade_window_start_time_local"
    )
    trade_window_end_time_local = runtime.get("trade_window_end_time_local") or risk.get(
        "trade_window_end_time_local"
    )
    trade_window_weekdays_raw = runtime.get("trade_window_weekdays")
    if trade_window_weekdays_raw is None:
        trade_window_weekdays_raw = risk.get("trade_window_weekdays")

    window_any = (
        trade_window_timezone is not None
        or trade_window_start_time_local is not None
        or trade_window_end_time_local is not None
    )
    window_all = (
        trade_window_timezone is not None
        and trade_window_start_time_local is not None
        and trade_window_end_time_local is not None
    )
    if window_any and not window_all:
        raise ValueError(
            "alpaca_options.runtime: set trade_window_timezone + trade_window_start_time_local + trade_window_end_time_local together"
        )

    if window_all:
        trade_window_weekdays = _parse_trade_window_weekdays(trade_window_weekdays_raw)
    else:
        trade_window_weekdays = frozenset(range(7))

    max_daily_loss_dollars = float(risk.get("max_daily_loss_dollars", 0))
    max_loss_per_underlying_dollars = float(risk.get("max_loss_per_underlying_dollars", 0))
    max_open_spreads_per_underlying = int(risk.get("max_open_spreads_per_underlying", 1))
    if max_open_spreads_per_underlying < 0:
        raise ValueError("alpaca_options.risk.max_open_spreads_per_underlying must be >= 0")

    put_credit_spread = _load_put_credit_spread_block(cfg)
    call_credit_spread = _load_call_credit_spread_block(cfg)
    iron_condor = _load_iron_condor_block(cfg)

    return AlpacaPutSpreadConfig(
        paper=paper,
        execute=execute,
        loop_sleep_seconds=loop_sleep_seconds,
        order_qty=order_qty,
        underlyings_mon_to_fri=sorted(mon_to_fri),
        underlyings_mon_wed_fri=sorted(mon_wed_fri),
        max_entry_retries=max_entry_retries,
        order_fill_timeout_seconds=order_fill_timeout_seconds,
        order_cancel_timeout_seconds=order_cancel_timeout_seconds,
        trade_window_timezone=trade_window_timezone,
        trade_window_start_time_local=trade_window_start_time_local,
        trade_window_end_time_local=trade_window_end_time_local,
        trade_window_weekdays=trade_window_weekdays,
        max_daily_loss_dollars=max_daily_loss_dollars,
        max_loss_per_underlying_dollars=max_loss_per_underlying_dollars,
        max_open_spreads_per_underlying=max_open_spreads_per_underlying,
        put_credit_spread=put_credit_spread,
        call_credit_spread=call_credit_spread,
        iron_condor=iron_condor,
    )


def load_alpaca_put_spread_config(config_dir: str | Path = "config") -> AlpacaPutSpreadConfig:
    """Backward-compatible alias for :func:`load_alpaca_options_config`."""
    return load_alpaca_options_config(config_dir)

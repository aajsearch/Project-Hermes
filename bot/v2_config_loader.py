"""
V2 strict config loader: loads and validates v2_common.yaml, v2_fifteen_min.yaml, v2_hourly.yaml.
Refuses to boot if the pipeline + strategies hierarchy is missing (no legacy fallback).
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List

import yaml

logger = logging.getLogger(__name__)

V2_COMMON = "v2_common.yaml"
V2_FIFTEEN_MIN = "v2_fifteen_min.yaml"
V2_HOURLY = "v2_hourly.yaml"


def _load_yaml(path: Path) -> Dict[str, Any]:
    """Load a single YAML file. Returns {} if file missing or invalid."""
    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.warning("Failed to load %s: %s", path, e)
        return {}


def _deep_merge(base: Dict[str, Any], overlay: Dict[str, Any]) -> None:
    """Merge overlay into base in-place. Dicts merged recursively; other values overwrite."""
    for k, v in overlay.items():
        if k in base and isinstance(base[k], dict) and isinstance(v, dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v


def _validate_common(config: Dict[str, Any]) -> None:
    """Require intervals.fifteen_min.enabled, intervals.hourly.enabled, caps, no_trade_windows."""
    intervals = config.get("intervals")
    if not isinstance(intervals, dict):
        raise ValueError("Invalid V2 Config Schema: missing or invalid 'intervals' in common")
    fifteen = intervals.get("fifteen_min")
    hourly = intervals.get("hourly")
    if not isinstance(fifteen, dict) or "enabled" not in fifteen:
        raise ValueError("Invalid V2 Config Schema: missing 'intervals.fifteen_min.enabled' in common")
    if not isinstance(hourly, dict) or "enabled" not in hourly:
        raise ValueError("Invalid V2 Config Schema: missing 'intervals.hourly.enabled' in common")
    if "caps" not in config:
        raise ValueError("Invalid V2 Config Schema: missing 'caps' in common")
    if "no_trade_windows" not in config:
        raise ValueError("Invalid V2 Config Schema: missing 'no_trade_windows' in common")
    # Optional but strongly recommended: feature_flags shape (used for safe rollouts)
    ff = config.get("feature_flags")
    if ff is not None and not isinstance(ff, dict):
        raise ValueError("Invalid V2 Config Schema: 'feature_flags' must be a dict when present")
    if isinstance(ff, dict):
        v2h = ff.get("v2_hourly")
        if v2h is not None and not isinstance(v2h, dict):
            raise ValueError("Invalid V2 Config Schema: 'feature_flags.v2_hourly' must be a dict when present")
        if isinstance(v2h, dict):
            enabled_assets = v2h.get("enabled_assets", [])
            if enabled_assets is not None and not isinstance(enabled_assets, list):
                raise ValueError("Invalid V2 Config Schema: 'feature_flags.v2_hourly.enabled_assets' must be a list")
            shadow = v2h.get("shadow_mode", False)
            if shadow is not None and not isinstance(shadow, bool):
                raise ValueError("Invalid V2 Config Schema: 'feature_flags.v2_hourly.shadow_mode' must be a bool")
    _validate_feature_flags(config)


def _validate_feature_flags(config: Dict[str, Any]) -> None:
    """
    Optional, but validated when present:
      feature_flags.v2_hourly.enabled_assets: list[str]
      feature_flags.v2_hourly.shadow_mode: bool
    """
    ff = config.get("feature_flags")
    if ff is None:
        return
    if not isinstance(ff, dict):
        raise ValueError("Invalid V2 Config Schema: 'feature_flags' must be a dict when present")
    v2h = ff.get("v2_hourly")
    if v2h is None:
        return
    if not isinstance(v2h, dict):
        raise ValueError("Invalid V2 Config Schema: 'feature_flags.v2_hourly' must be a dict when present")
    ea = v2h.get("enabled_assets", [])
    if not isinstance(ea, list) or any(not isinstance(x, str) for x in ea):
        raise ValueError("Invalid V2 Config Schema: 'feature_flags.v2_hourly.enabled_assets' must be a list[str]")
    sm = v2h.get("shadow_mode", False)
    if not isinstance(sm, bool):
        raise ValueError("Invalid V2 Config Schema: 'feature_flags.v2_hourly.shadow_mode' must be a bool")


def _validate_fifteen_min(config: Dict[str, Any]) -> None:
    """Require fifteen_min.pipeline.strategy_priority and fifteen_min.strategies."""
    fifteen_min = config.get("fifteen_min")
    if not isinstance(fifteen_min, dict):
        raise ValueError("Invalid V2 Config Schema: missing or invalid 'fifteen_min' in v2_fifteen_min.yaml")
    pipeline = fifteen_min.get("pipeline")
    if not isinstance(pipeline, dict) or "strategy_priority" not in pipeline:
        raise ValueError(
            "Invalid V2 Config Schema: missing 'fifteen_min.pipeline.strategy_priority' in v2_fifteen_min.yaml"
        )
    if "strategies" not in fifteen_min or not isinstance(fifteen_min["strategies"], dict):
        raise ValueError(
            "Invalid V2 Config Schema: missing 'fifteen_min.strategies' in v2_fifteen_min.yaml"
        )


def _validate_hourly(config: Dict[str, Any]) -> None:
    """Require hourly.pipeline.strategy_priority and hourly.strategies."""
    hourly = config.get("hourly")
    if not isinstance(hourly, dict):
        raise ValueError("Invalid V2 Config Schema: missing or invalid 'hourly' in v2_hourly.yaml")
    pipeline = hourly.get("pipeline")
    if not isinstance(pipeline, dict) or "strategy_priority" not in pipeline:
        raise ValueError(
            "Invalid V2 Config Schema: missing 'hourly.pipeline.strategy_priority' in v2_hourly.yaml"
        )
    if "run_interval_seconds" not in pipeline:
        raise ValueError("Invalid V2 Config Schema: missing 'hourly.pipeline.run_interval_seconds' in v2_hourly.yaml")
    # Optional: event_mode_by_asset controls primary vs range event selection.
    event_mode = pipeline.get("event_mode_by_asset")
    if event_mode is not None:
        if not isinstance(event_mode, dict) or any(not isinstance(k, str) for k in event_mode.keys()):
            raise ValueError("Invalid V2 Config Schema: 'hourly.pipeline.event_mode_by_asset' must be a dict[str, str]")
        allowed = {"primary_only", "range_only", "both"}
        for a, mode in event_mode.items():
            if not isinstance(mode, str) or mode.strip().lower() not in allowed:
                raise ValueError(
                    "Invalid V2 Config Schema: hourly.pipeline.event_mode_by_asset values must be one of "
                    f"{sorted(allowed)}; got {a}={mode!r}"
                )
    if not isinstance(pipeline.get("strategy_priority"), list):
        raise ValueError("Invalid V2 Config Schema: 'hourly.pipeline.strategy_priority' must be a list")
    if "strategies" not in hourly or not isinstance(hourly["strategies"], dict):
        raise ValueError(
            "Invalid V2 Config Schema: missing 'hourly.strategies' in v2_hourly.yaml"
        )
    # Ensure priority references valid strategies (avoids silent no-op).
    strategies = hourly.get("strategies") or {}
    for sid in pipeline.get("strategy_priority") or []:
        if sid not in strategies:
            raise ValueError(
                f"Invalid V2 Config Schema: hourly.pipeline.strategy_priority references missing strategy '{sid}'"
            )

    pr = pipeline.get("strategy_priority")
    if not isinstance(pr, list) or any(not isinstance(x, str) for x in pr):
        raise ValueError("Invalid V2 Config Schema: 'hourly.pipeline.strategy_priority' must be a list[str]")
    strategies = hourly.get("strategies") or {}
    for sid in pr:
        if sid not in strategies:
            raise ValueError(
                f"Invalid V2 Config Schema: hourly.pipeline.strategy_priority contains {sid!r} "
                f"but hourly.strategies has no block for it"
            )
    # Validate basic per-strategy contract (enabled bool) when blocks exist.
    for sid, block in strategies.items():
        if not isinstance(block, dict):
            raise ValueError(f"Invalid V2 Config Schema: hourly.strategies.{sid} must be a dict")
        if "enabled" not in block or not isinstance(block.get("enabled"), bool):
            raise ValueError(f"Invalid V2 Config Schema: hourly.strategies.{sid}.enabled must be a bool")


def load_v2_config(config_dir: str | Path = "config") -> Dict[str, Any]:
    """
    Load and merge v2_common.yaml, v2_fifteen_min.yaml, and v2_hourly.yaml from config_dir.
    Validates strict V2 schema; raises ValueError("Invalid V2 Config Schema") if any required
    key or structure is missing.

    Required in common:
      - intervals.fifteen_min.enabled
      - intervals.hourly.enabled
      - caps
      - no_trade_windows

    Required in v2_fifteen_min.yaml:
      - fifteen_min.pipeline.strategy_priority
      - fifteen_min.strategies

    Required in v2_hourly.yaml:
      - hourly.pipeline.strategy_priority
      - hourly.strategies

    Returns merged config dict (common + fifteen_min + hourly).
    """
    root = Path(config_dir)
    common = _load_yaml(root / V2_COMMON)
    if not common:
        raise ValueError("Invalid V2 Config Schema: could not load v2_common.yaml or file is empty")

    _validate_common(common)

    fifteen_min = _load_yaml(root / V2_FIFTEEN_MIN)
    if not fifteen_min:
        raise ValueError("Invalid V2 Config Schema: could not load v2_fifteen_min.yaml or file is empty")
    _validate_fifteen_min(fifteen_min)
    _deep_merge(common, fifteen_min)

    hourly = _load_yaml(root / V2_HOURLY)
    if not hourly:
        raise ValueError("Invalid V2 Config Schema: could not load v2_hourly.yaml or file is empty")
    _validate_hourly(hourly)
    _deep_merge(common, hourly)

    logger.info("V2 config loaded from %s (common + fifteen_min + hourly)", root)
    return common

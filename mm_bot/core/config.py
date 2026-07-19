from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import yaml


def _env_bool(v: Optional[str]) -> Optional[bool]:
    if v is None:
        return None
    s = v.strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return None


@dataclass(frozen=True)
class BotConfig:
    raw: Dict[str, Any]

    @property
    def exchange_mode(self) -> str:
        return str(self.raw["exchange"]["mode"]).strip().lower()

    @property
    def trading_portfolio_id(self) -> str:
        return str(self.raw["exchange"]["trading_portfolio_id"]).strip()

    @property
    def usd_account_uuid(self) -> str:
        return str(self.raw["exchange"]["usd_account_uuid"]).strip()

    @property
    def queue_maxsize(self) -> int:
        return int(self.raw["runtime"]["queue_maxsize"])


def load_config(path: str) -> BotConfig:
    with open(path, "r") as f:
        raw: Dict[str, Any] = yaml.safe_load(f) or {}

    # Allow env overrides without changing YAML (useful for prod deploys)
    # Prefer .env-provided UUIDs if present.
    if os.environ.get("TRADING_PORTFOLIO_ID"):
        raw.setdefault("exchange", {})["trading_portfolio_id"] = os.environ["TRADING_PORTFOLIO_ID"]
    if os.environ.get("USD_ACCOUNT_UUID"):
        raw.setdefault("exchange", {})["usd_account_uuid"] = os.environ["USD_ACCOUNT_UUID"]
    if os.environ.get("MM_EXCHANGE_MODE"):
        raw.setdefault("exchange", {})["mode"] = os.environ["MM_EXCHANGE_MODE"]

    # Optional toggle to load dotenv is handled in entrypoint.
    return BotConfig(raw=raw)


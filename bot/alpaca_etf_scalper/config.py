from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import yaml


@dataclass(frozen=True)
class ProductCfg:
    symbol: str
    qty: float
    half_spread_bps: float
    profit_target_pct: float
    stop_loss_pct: float


@dataclass(frozen=True)
class ScalperCfg:
    raw: Dict[str, Any]
    products: List[ProductCfg]

    @property
    def paper(self) -> bool:
        return bool(self.raw.get("alpaca", {}).get("paper", True))

    @property
    def execute(self) -> bool:
        return bool(self.raw.get("alpaca", {}).get("execute", False))

    @property
    def poll_seconds(self) -> float:
        return float(self.raw.get("market_data", {}).get("poll_seconds", 1.0))

    @property
    def queue_maxsize(self) -> int:
        return int(self.raw.get("runtime", {}).get("queue_maxsize", 10_000))

    @property
    def reconcile_seconds(self) -> float:
        return float(self.raw.get("runtime", {}).get("reconcile_seconds", 15.0))

    @property
    def shutdown_timeout_seconds(self) -> float:
        return float(self.raw.get("runtime", {}).get("shutdown_timeout_seconds", 10.0))

    @property
    def sqlite_path(self) -> str:
        return str(self.raw.get("storage", {}).get("sqlite_path", "data/alpaca_etf_scalper.sqlite3"))

    @property
    def schema_path(self) -> str:
        return str(self.raw.get("storage", {}).get("schema_path", "mm_bot/sql/schema.sql"))


def load_cfg(path: str) -> ScalperCfg:
    p = Path(path)
    raw: Dict[str, Any] = yaml.safe_load(p.read_text()) or {}
    prods = []
    for item in (raw.get("strategy", {}) or {}).get("products", []) or []:
        prods.append(
            ProductCfg(
                symbol=str(item["symbol"]).strip().upper(),
                qty=float(item["qty"]),
                half_spread_bps=float(item["half_spread_bps"]),
                profit_target_pct=float(item["profit_target_pct"]),
                stop_loss_pct=float(item["stop_loss_pct"]),
            )
        )
    if not prods:
        raise ValueError("No strategy.products configured")
    return ScalperCfg(raw=raw, products=prods)


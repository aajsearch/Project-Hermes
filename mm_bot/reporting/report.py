from __future__ import annotations

import csv
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class Summary:
    ts_ms: int
    realized_pnl: float
    unrealized_pnl: float
    fees: float
    trade_count: int


class Reporter:
    def __init__(self, csv_output_path: str):
        self.csv_path = Path(csv_output_path)
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)

    def write_summary(self, s: Summary) -> None:
        exists = self.csv_path.exists()
        with open(self.csv_path, "a", newline="") as f:
            w = csv.DictWriter(
                f,
                fieldnames=["ts_ms", "realized_pnl", "unrealized_pnl", "fees", "trade_count"],
            )
            if not exists:
                w.writeheader()
            w.writerow(
                {
                    "ts_ms": s.ts_ms,
                    "realized_pnl": s.realized_pnl,
                    "unrealized_pnl": s.unrealized_pnl,
                    "fees": s.fees,
                    "trade_count": s.trade_count,
                }
            )

    def console_summary(self, s: Summary) -> str:
        return (
            f"PnL: realized={s.realized_pnl:.2f} unrealized={s.unrealized_pnl:.2f} "
            f"fees={s.fees:.2f} trades={s.trade_count}"
        )


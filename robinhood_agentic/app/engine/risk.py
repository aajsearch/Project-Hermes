"""Risk gate — circuit breakers and mode checks."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from ..db.repository import Repository

ET = ZoneInfo("America/New_York")


class RiskGate:
    def __init__(self, repo: Repository) -> None:
        self.repo = repo

    def can_enter(self, tech_cfg: dict[str, Any], options_cfg: dict[str, Any] | None = None) -> tuple[bool, str]:
        ss = self.repo.get_system_state()
        if ss.get("mode") == "halted":
            return False, "halted"
        if not ss.get("scanner_enabled"):
            return False, "scanner_disabled"
        if not ss.get("auto_entry_enabled"):
            return False, "auto_entry_disabled"

        stats = self.repo.get_daily_stats()
        max_sl = int(
            tech_cfg.get("position_sizing", {}).get("max_stop_losses_per_day", 5)
        )
        if int(stats.get("equity_sl_hits") or 0) >= max_sl:
            return False, f"equity_sl_circuit_{stats['equity_sl_hits']}/{max_sl}"

        if options_cfg:
            max_opt_loss = int(
                options_cfg.get("risk", {}).get("circuit_breaker_losses_per_day", 2)
            )
            if int(stats.get("option_losses") or 0) >= max_opt_loss:
                return False, f"option_loss_circuit_{stats['option_losses']}/{max_opt_loss}"
            max_daily_loss = float(
                options_cfg.get("risk", {}).get("max_daily_realized_loss_usd", 75)
            )
            if float(stats.get("realized_pnl") or 0) <= -abs(max_daily_loss):
                return False, "daily_realized_loss_limit"

        if not self.in_entry_window(tech_cfg):
            return False, "outside_entry_window"

        max_conc = int(tech_cfg.get("position_sizing", {}).get("max_concurrent", 2))
        open_n = len(self.repo.list_positions())
        if open_n >= max_conc:
            return False, "max_concurrent"

        return True, "ok"

    def in_entry_window(self, tech_cfg: dict[str, Any]) -> bool:
        scalp = tech_cfg.get("scalp", {})
        now = datetime.now(ET).time()
        start = datetime.strptime(
            scalp.get("no_new_entry_before_et", "09:45"), "%H:%M"
        ).time()
        end = datetime.strptime(
            scalp.get("no_new_entry_after_et", "15:30"), "%H:%M"
        ).time()
        return start <= now <= end

    def equity_hard_flat_due(self, tech_cfg: dict[str, Any]) -> bool:
        scalp = tech_cfg.get("scalp", {})
        flat = scalp.get("hard_flat_time_et", "15:55")
        now = datetime.now(ET).time()
        flat_t = datetime.strptime(flat, "%H:%M").time()
        return now >= flat_t

    def same_day_block_enabled(self) -> bool:
        return bool(self.repo.get_system_state().get("same_day_symbol_block", 1))

    def symbol_allowed(self, symbol: str) -> bool:
        if not self.same_day_block_enabled():
            return True
        return not self.repo.is_symbol_blocked(symbol)

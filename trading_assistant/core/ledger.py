"""
Trade ledger and EOD reporting.
- Append each fill to logs/trades.csv
- Reason codes for SKIP decisions
- EOD report generator (daily_report_YYYY-MM-DD.md)
"""
import csv
import os
from datetime import datetime
from typing import Any, Dict, Optional

from config.settings import LOG_DIR, TRADES_LEDGER_CSV, STRATEGY_VERSION

os.makedirs(LOG_DIR, exist_ok=True)

# Reason codes for SKIP (for logs / EOD)
SKIP_COOLDOWN = "cooldown"
SKIP_PENDING_ORDER = "pending_order_exists"
SKIP_ALREADY_HOLDING = "already_holding"
SKIP_NEAR_OPEN = "near_open"
SKIP_KILL_SWITCH = "kill_switch_off"
SKIP_PORTFOLIO_LOCK = "portfolio_lock"
SKIP_QTY_ZERO = "qty_zero"
SKIP_SPREAD_TOO_WIDE = "spread_too_wide"
SKIP_IV_TOO_HIGH = "iv_too_high"
SKIP_MAX_POSITION = "max_position_per_symbol"
SKIP_MAX_BUYS_CYCLE = "max_buys_per_cycle"
SKIP_MAX_BUYS_HOUR = "max_buys_per_hour"


# Standard columns for trades.csv
TRADE_LEDGER_FIELDS = [
    "time", "asset_type", "contract_type", "symbol", "side", "qty", "price",
    "strategy_version", "profile", "signal_reason", "exit_reason",
    "mfe_pct", "mae_pct", "entry_snapshot_json",
]


def append_trade(
    time_iso: str,
    symbol: str,
    side: str,
    qty: float,
    price: float,
    strategy_version: str = STRATEGY_VERSION,
    profile: str = "",
    signal_reason: str = "",
    exit_reason: str = "",
    mfe_pct: Optional[float] = None,
    mae_pct: Optional[float] = None,
    asset_type: str = "",
    entry_snapshot_json: str = "",
    contract_type: str = "",
) -> None:
    """Append one fill to the trade ledger CSV. MFE/MAE on SELL; entry_snapshot_json on BUY. contract_type for options: CALL or PUT."""
    row = {
        "time": time_iso,
        "asset_type": asset_type,
        "contract_type": contract_type or "",
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "price": price,
        "strategy_version": strategy_version,
        "profile": profile,
        "signal_reason": signal_reason,
        "exit_reason": exit_reason,
        "mfe_pct": mfe_pct if mfe_pct is not None else "",
        "mae_pct": mae_pct if mae_pct is not None else "",
        "entry_snapshot_json": entry_snapshot_json or "",
    }
    file_exists = os.path.exists(TRADES_LEDGER_CSV)
    with open(TRADES_LEDGER_CSV, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=TRADE_LEDGER_FIELDS, extrasaction="ignore")
        if not file_exists:
            w.writeheader()
        w.writerow(row)


def append_snapshot(
    time_iso: str,
    symbol: str,
    side: str,
    snapshot: Dict[str, Any],
    strategy_version: str = STRATEGY_VERSION,
) -> None:
    """Append a trade snapshot (feature vector) for later analysis. Uses JSONL in logs/snapshots_YYYY-MM-DD.jsonl."""
    date_str = time_iso[:10] if time_iso else datetime.now().strftime("%Y-%m-%d")
    path = os.path.join(LOG_DIR, f"snapshots_{date_str}.jsonl")
    import json
    record = {
        "time": time_iso,
        "symbol": symbol,
        "side": side,
        "strategy_version": strategy_version,
        **snapshot,
    }
    with open(path, "a") as f:
        f.write(json.dumps(record, default=str) + "\n")


def generate_eod_report(date_str: str, trades: list, exit_reason_counts: Dict[str, int]) -> str:
    """
    Generate end-of-day report markdown. Call with date_str='YYYY-MM-DD', list of trade dicts, exit reason counts.
    """
    total_pnl = sum(float(t.get("pnl", 0) or 0) for t in trades)
    wins = [t for t in trades if float(t.get("pnl", 0) or 0) > 0]
    losses = [t for t in trades if float(t.get("pnl", 0) or 0) < 0]
    win_rate = (len(wins) / len(trades) * 100) if trades else 0
    avg_win = (sum(float(t.get("pnl", 0)) for t in wins) / len(wins)) if wins else 0
    avg_loss = (sum(float(t.get("pnl", 0)) for t in losses) / len(losses)) if losses else 0

    lines = [
        f"# Daily Report {date_str}",
        "",
        "## Summary",
        f"- Total PnL: {total_pnl:.2f}",
        f"- Win rate: {win_rate:.1f}%",
        f"- Avg win: {avg_win:.2f}",
        f"- Avg loss: {avg_loss:.2f}",
        f"- Trades: {len(trades)}",
        "",
        "## Exit reason distribution",
    ]
    for reason, count in sorted(exit_reason_counts.items(), key=lambda x: -x[1]):
        lines.append(f"- {reason}: {count}")
    lines.extend(["", "## Trades by PnL"])
    for t in sorted(trades, key=lambda x: float(x.get("pnl", 0) or 0), reverse=True):
        lines.append(f"- {t.get('symbol')} {t.get('side')} qty={t.get('qty')} pnl={t.get('pnl', 0):.2f}")
    return "\n".join(lines)

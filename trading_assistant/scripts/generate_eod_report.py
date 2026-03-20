#!/usr/bin/env python3
"""
Generate end-of-day report from logs/trades.csv.
Usage: python scripts/generate_eod_report.py [YYYY-MM-DD]
If date omitted, uses today (local).
Output: logs/daily_report_YYYY-MM-DD.md
"""
import csv
import os
import sys
from collections import defaultdict
from datetime import datetime

# Run from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import LOG_DIR, TRADES_LEDGER_CSV

def main():
    date_str = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    path = TRADES_LEDGER_CSV
    if not os.path.exists(path):
        print(f"No {path} found.")
        return

    trades = []
    with open(path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            t = (row.get("time") or "")[:10]
            if t == date_str:
                trades.append(row)

    exit_reason_counts = defaultdict(int)
    for t in trades:
        r = (t.get("exit_reason") or "").strip() or "entry"
        exit_reason_counts[r] += 1

    # Simple PnL: pair BUY/SELL by symbol (FIFO would be more accurate)
    pnl_by_trade = []
    positions = {}  # symbol -> list of (qty, price, side)
    for t in trades:
        sym = t.get("symbol", "")
        side = (t.get("side", "") or "").upper()
        qty = float(t.get("qty", 0) or 0)
        price = float(t.get("price", 0) or 0)
        if sym not in positions:
            positions[sym] = []
        if side == "BUY":
            positions[sym].append((qty, price, "BUY"))
        else:
            # SELL: match against buys
            remaining = qty
            pnl = 0.0
            while remaining > 0 and positions[sym] and positions[sym][0][2] == "BUY":
                bq, bp, _ = positions[sym][0]
                if bq <= remaining:
                    pnl += (price - bp) * bq
                    remaining -= bq
                    positions[sym].pop(0)
                else:
                    pnl += (price - bp) * remaining
                    positions[sym][0] = (bq - remaining, bp, "BUY")
                    remaining = 0
            pnl_by_trade.append({"symbol": sym, "side": "SELL", "qty": qty, "pnl": pnl, **t})

    total_pnl = sum(x["pnl"] for x in pnl_by_trade)
    wins = [x for x in pnl_by_trade if x["pnl"] > 0]
    losses = [x for x in pnl_by_trade if x["pnl"] < 0]
    n = len(pnl_by_trade)
    win_rate = (len(wins) / n * 100) if n else 0
    avg_win = (sum(x["pnl"] for x in wins) / len(wins)) if wins else 0
    avg_loss = (sum(x["pnl"] for x in losses) / len(losses)) if losses else 0

    # Invested (notional of BUYs on this day) for pnl % on invested
    invested_today = 0.0
    for t in trades:
        if (t.get("side") or "").upper() == "BUY":
            invested_today += float(t.get("qty", 0) or 0) * float(t.get("price", 0) or 0)
    pnl_pct_invested = (total_pnl / invested_today * 100) if invested_today > 0 else 0.0

    sorted_trades = sorted(pnl_by_trade, key=lambda x: x["pnl"], reverse=True)
    top3_best = sorted_trades[:3]
    top3_worst = sorted_trades[-3:] if len(sorted_trades) >= 3 else sorted_trades

    lines = [
        f"# Daily Report {date_str}",
        "",
        "## Summary",
        f"- Trade count (exits): {n}",
        f"- Win rate: {win_rate:.1f}%",
        f"- Total realized PnL $: {total_pnl:.2f}",
        f"- PnL % on invested: {pnl_pct_invested:.2f}%",
        f"- Avg win: {avg_win:.2f}",
        f"- Avg loss: {avg_loss:.2f}",
        "",
        "## Exit reason distribution",
    ]
    for reason, count in sorted(exit_reason_counts.items(), key=lambda x: -x[1]):
        lines.append(f"- {reason}: {count}")

    # Options: CALL vs PUT breakdown (exit reasons and PnL)
    opt_trades = [x for x in pnl_by_trade if (x.get("asset_type") or "").strip().upper() == "OPTION"]
    if opt_trades:
        lines.extend(["", "## Options: CALL vs PUT"])
        for ctype in ("CALL", "PUT"):
            subset = [x for x in opt_trades if (x.get("contract_type") or "").strip().upper() == ctype]
            if not subset:
                continue
            c_pnl = sum(x["pnl"] for x in subset)
            c_wins = [x for x in subset if x["pnl"] > 0]
            c_n = len(subset)
            c_wr = (len(c_wins) / c_n * 100) if c_n else 0
            lines.append(f"- **{ctype}**: {c_n} exits, PnL $ {c_pnl:.2f}, win rate {c_wr:.1f}%")
        exit_by_ctype = defaultdict(lambda: defaultdict(int))
        for t in opt_trades:
            r = (t.get("exit_reason") or "").strip() or "entry"
            ct = (t.get("contract_type") or "").strip().upper() or "CALL"
            exit_by_ctype[ct][r] += 1
        for ctype in ("CALL", "PUT"):
            if ctype in exit_by_ctype:
                lines.append(f"  - Exit reasons ({ctype}): " + ", ".join(f"{r}: {c}" for r, c in sorted(exit_by_ctype[ctype].items(), key=lambda x: -x[1])))

    lines.extend(["", "## Best 3 trades"])
    for t in top3_best:
        mfe = t.get("mfe_pct"); mae = t.get("mae_pct")
        mfe_str = f" MFE={float(mfe)*100:.2f}%" if mfe not in (None, "") else ""
        mae_str = f" MAE={float(mae)*100:.2f}%" if mae not in (None, "") else ""
        lines.append(f"- {t.get('symbol')} {t.get('side')} qty={t.get('qty')} pnl={t.get('pnl', 0):.2f}{mfe_str}{mae_str}")
        snap = t.get("entry_snapshot_json", "").strip()
        if snap:
            lines.append(f"  - Entry snapshot: {snap[:200]}{'...' if len(snap) > 200 else ''}")
    lines.extend(["", "## Worst 3 trades"])
    for t in top3_worst:
        mfe = t.get("mfe_pct"); mae = t.get("mae_pct")
        mfe_str = f" MFE={float(mfe)*100:.2f}%" if mfe not in (None, "") else ""
        mae_str = f" MAE={float(mae)*100:.2f}%" if mae not in (None, "") else ""
        lines.append(f"- {t.get('symbol')} {t.get('side')} qty={t.get('qty')} pnl={t.get('pnl', 0):.2f}{mfe_str}{mae_str}")
        snap = t.get("entry_snapshot_json", "").strip()
        if snap:
            lines.append(f"  - Entry snapshot: {snap[:200]}{'...' if len(snap) > 200 else ''}")
    lines.extend(["", "## Full trade list (by PnL)", "", "| symbol | contract_type | side | qty | price | pnl | MFE% | MAE% |", "|--------|---------------|------|-----|-------|-----|------|------|"])
    for t in sorted_trades:
        mfe = t.get("mfe_pct"); mae = t.get("mae_pct")
        mfe_s = f"{float(mfe)*100:.2f}%" if mfe not in (None, "") else ""
        mae_s = f"{float(mae)*100:.2f}%" if mae not in (None, "") else ""
        ct = (t.get("contract_type") or "").strip() or "—"
        lines.append(f"| {t.get('symbol')} | {ct} | {t.get('side')} | {t.get('qty')} | {t.get('price')} | {t.get('pnl', 0):.2f} | {mfe_s} | {mae_s} |")

    out_path = os.path.join(LOG_DIR, f"daily_report_{date_str}.md")
    with open(out_path, "w") as f:
        f.write("\n".join(lines))
    print(f"Wrote {out_path}")

if __name__ == "__main__":
    main()

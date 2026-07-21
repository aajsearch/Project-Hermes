"""Position TP/SL checks."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")


def check_equity_positions(
    positions: list[dict[str, Any]],
    quotes: dict[str, dict],
    cfg: dict[str, Any],
) -> tuple[list[str], list[dict[str, Any]]]:
    alerts: list[str] = []
    snapshots: list[dict[str, Any]] = []
    scalp = cfg.get("scalp", {})
    clear_bps = float(scalp.get("stop_loss_clear_bps", 15)) / 10000

    for pos in positions:
        sym = pos["symbol"]
        if pos.get("pending"):
            continue
        q = quotes.get(sym)
        if not q:
            alerts.append(f"{sym}: no quote")
            continue
        last = float(q.get("last_trade_price") or 0)
        entry = float(pos["entry"])
        tp = float(pos["tp"])
        sl = float(pos["sl"])
        qty = float(pos.get("qty", 1))
        pnl = (last - entry) * qty
        pnl_pct = (last / entry - 1) * 100 if entry else 0
        frac = bool(pos.get("fractional"))
        dist_sl = last - sl
        dist_tp = tp - last
        snap = {
            "symbol": sym,
            "asset": "equity",
            "last": last,
            "entry": entry,
            "qty": qty,
            "tp": tp,
            "sl": sl,
            "pnl": pnl,
            "pnl_pct": pnl_pct,
            "fractional": frac,
            "dist_sl": dist_sl,
            "dist_tp": dist_tp,
            "dist_sl_pct": (dist_sl / entry * 100) if entry else 0,
            "dist_tp_pct": (dist_tp / entry * 100) if entry else 0,
            "synthetic_tp": bool(pos.get("synthetic_tp", True)),
            "synthetic_sl": bool(pos.get("synthetic_sl")),
            "auto_exit": bool(pos.get("auto_exit", True)),
            "sl_order_id": pos.get("sl_order_id"),
        }
        snapshots.append(snap)

        if not pos.get("auto_exit", True):
            continue

        if last >= tp and (pos.get("synthetic_tp") or frac):
            sl_oid = pos.get("sl_order_id") or ""
            alerts.append(
                f"TP_HIT:{sym}:last={last:.4f}:tp={tp:.4f}:sl_order={sl_oid}:fractional={str(frac).lower()}"
            )
        elif last <= sl and pos.get("synthetic_sl"):
            alerts.append(
                f"SL_HIT:{sym}:last={last:.4f}:sl={sl:.4f}:fractional={str(frac).lower()}"
            )
        elif last <= sl + entry * clear_bps:
            alerts.append(f"{sym} NEAR SL (${sl:.2f}, last ${last:.2f})")

    return alerts, snapshots


def check_option_positions(
    positions: list[dict[str, Any]],
    option_quotes: dict[str, dict],
    options_cfg: dict[str, Any],
) -> tuple[list[str], list[dict[str, Any]]]:
    alerts: list[str] = []
    snapshots: list[dict[str, Any]] = []
    trade = options_cfg.get("trade", options_cfg)
    now_t = datetime.now(ET).time()
    flat_t = datetime.strptime(
        trade.get("hard_flat_time_et", "15:45"), "%H:%M"
    ).time()

    for pos in positions:
        oid = pos.get("option_id")
        if not oid:
            continue
        label = pos.get("label") or f"{pos.get('symbol', '?')} opt"
        q = option_quotes.get(oid)
        if not q:
            alerts.append(f"{label}: no option quote")
            continue
        bid = float(q.get("bid_price") or 0)
        ask = float(q.get("ask_price") or 0)
        mark = float(q.get("mark_price") or 0)
        last = mark if mark > 0 else ((bid + ask) / 2 if bid > 0 and ask > 0 else bid)
        entry = float(pos["entry"])
        tp = float(pos["tp"])
        sl = float(pos["sl"])
        qty = float(pos.get("qty", 1))
        mult = float(pos.get("multiplier") or 100)
        pnl = (last - entry) * qty * mult
        pnl_pct = (last / entry - 1) * 100 if entry else 0
        snap = {
            "symbol": label,
            "option_id": oid,
            "asset": "option",
            "last": last,
            "entry": entry,
            "qty": qty,
            "tp": tp,
            "sl": sl,
            "pnl": pnl,
            "pnl_pct": pnl_pct,
            "fractional": False,
            "dist_sl": last - sl,
            "dist_tp": tp - last,
            "dist_sl_pct": ((last - sl) / entry * 100) if entry else 0,
            "dist_tp_pct": ((tp - last) / entry * 100) if entry else 0,
            "synthetic_tp": bool(pos.get("synthetic_tp", True)),
            "synthetic_sl": bool(pos.get("synthetic_sl", True)),
            "auto_exit": bool(pos.get("auto_exit", True)),
        }
        snapshots.append(snap)

        if not pos.get("auto_exit", True):
            continue

        if now_t >= flat_t:
            alerts.append(f"OPT_FLAT:{oid}:label={label}:mark={last:.4f}")
        elif last >= tp and pos.get("synthetic_tp", True):
            alerts.append(
                f"OPT_TP_HIT:{oid}:label={label}:mark={last:.4f}:tp={tp:.4f}"
            )
        elif last <= sl and pos.get("synthetic_sl", True):
            alerts.append(
                f"OPT_SL_HIT:{oid}:label={label}:mark={last:.4f}:sl={sl:.4f}"
            )
        elif last <= sl * 1.05:
            alerts.append(f"{label} NEAR SL (${sl:.4f}, mark ${last:.4f})")

    return alerts, snapshots

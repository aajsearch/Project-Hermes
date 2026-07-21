#!/usr/bin/env python3
"""
Tech scalper session monitor — runs outside Cursor chat.

- Polls open positions every poll_seconds (default 15) via Robinhood public quotes API.
- Full watchlist rescan every rescan_minutes (default 15).
- Alerts on stdout + optional macOS notification.
- Optional --auto-exit / --auto-entry: direct Robinhood MCP HTTP when `.mcp_access_token` is set (see MCP_AUTH.md); else falls back to cursor agent.

Quotes API is read-only and does not use MCP. Trade execution still requires Cursor + Robinhood MCP.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = ROOT / "config" / "tech_scalper.yaml"
DEFAULT_OPTIONS_CONFIG = ROOT / "config" / "options_directional.yaml"
DEFAULT_STATE = Path(__file__).resolve().parent / "session_state.json"
MONITOR_DIR = Path(__file__).resolve().parent
if str(MONITOR_DIR) not in sys.path:
    sys.path.insert(0, str(MONITOR_DIR))
LIVE_STATUS_PATH = MONITOR_DIR / "LIVE_STATUS.md"
LATEST_JSON_PATH = MONITOR_DIR / "latest_status.json"
DASHBOARD_PATH = MONITOR_DIR / "dashboard.html"
QUOTES_URL = "https://api.robinhood.com/quotes/"
ET = ZoneInfo("America/New_York")
# Option quotes need MCP (public marketdata is 403). Poll at least this often.
OPTION_QUOTE_MIN_SECONDS = 30


def load_yaml(path: Path) -> dict:
    """Minimal YAML subset parser (no PyYAML required)."""
    text = path.read_text()
    cfg: dict = {
        "account": {},
        "scalp": {},
        "selection": {},
        "position_sizing": {},
        "entry": {},
        "watchlist": {},
    }

    def num(key: str, section: dict, default: float) -> None:
        # Keys are indented under YAML sections — allow leading whitespace.
        m = re.search(rf"^\s*{re.escape(key)}:\s*([\d.]+)", text, re.M)
        if m:
            section[key] = float(m.group(1)) if "." in m.group(1) else int(m.group(1))
        else:
            section[key] = default

    def str_val(key: str, section: dict) -> None:
        m = re.search(rf"^\s*{re.escape(key)}:\s*(true|false)", text, re.M)
        if m:
            section[key] = m.group(1) == "true"

    num("poll_seconds", cfg["scalp"], 15)
    num("stop_loss_clear_bps", cfg["scalp"], 15)
    num("profit_target_pct", cfg["scalp"], 0.006)
    num("stop_loss_pct", cfg["scalp"], 0.0045)
    num("target_notional_usd", cfg["position_sizing"], 100)
    num("max_concurrent", cfg["position_sizing"], 2)
    m = re.search(r"reserve_usd:\s*([\d.]+)", text)
    if m:
        cfg["account"]["reserve_usd"] = float(m.group(1))
    else:
        cfg["account"]["reserve_usd"] = 50
    num("max_spread_pct", cfg["selection"], 0.0015)
    num("max_spread_usd", cfg["selection"], 0.25)
    num("min_abs_day_change_pct", cfg["selection"], 0.003)
    num("max_abs_day_change_pct", cfg["selection"], 0.008)
    str_val("allow_fractional_live", cfg["entry"])

    for key, default in (
        ("no_new_entry_before_et", "09:45"),
        ("no_new_entry_after_et", "15:30"),
    ):
        m = re.search(rf"^{re.escape(key)}:\s*\"([^\"]+)\"", text, re.M)
        if m:
            cfg["scalp"][key] = m.group(1)
        else:
            cfg["scalp"][key] = default

    watchlist_block = re.search(
        r"^watchlist:\s*\n(.*?)(?=^\S|\Z)", text, re.M | re.S
    )
    if watchlist_block:
        symbols = re.findall(r"^\s+-\s+([A-Z]+)\s*$", watchlist_block.group(1), re.M)
        cfg["watchlist"]["all"] = list(dict.fromkeys(symbols))

    core_block = re.search(r"core_symbols:\s*\n((?:\s+-\s+\w+\s*\n)+)", text)
    if core_block:
        cfg["selection"]["core_symbols"] = re.findall(
            r"-\s+(\w+)", core_block.group(1)
        )
    dep_block = re.search(r"deprioritize_symbols:\s*\n((?:\s+-\s+\w+\s*\n)+)", text)
    if dep_block:
        cfg["selection"]["deprioritize_symbols"] = re.findall(
            r"-\s+(\w+)", dep_block.group(1)
        )

    return cfg


def load_options_cfg(path: Path = DEFAULT_OPTIONS_CONFIG) -> dict:
    """Load option TP/SL / flat time from options_directional.yaml (minimal parser)."""
    opts = {
        "profit_target_pct": 0.15,
        "stop_loss_pct": 0.10,
        "hard_flat_time_et": "15:45",
    }
    if not path.is_file():
        return opts
    text = path.read_text()
    m = re.search(r"^\s*profit_target_pct:\s*([\d.]+)", text, re.M)
    if m:
        opts["profit_target_pct"] = float(m.group(1))
    m = re.search(r"^\s*stop_loss_pct:\s*([\d.]+)", text, re.M)
    if m:
        opts["stop_loss_pct"] = float(m.group(1))
    m = re.search(r'^\s*hard_flat_time_et:\s*"([^"]+)"', text, re.M)
    if m:
        opts["hard_flat_time_et"] = m.group(1)
    return opts


def load_state(path: Path) -> dict:
    with path.open() as f:
        return json.load(f)


def save_state(path: Path, state: dict) -> None:
    with path.open("w") as f:
        json.dump(state, f, indent=2)
        f.write("\n")


def flatten_watchlist(cfg: dict) -> list[str]:
    return list(cfg.get("watchlist", {}).get("all", []))


def fetch_quotes(symbols: list[str]) -> dict[str, dict]:
    if not symbols:
        return {}
    url = QUOTES_URL + "?" + urllib.parse.urlencode({"symbols": ",".join(symbols)})
    req = urllib.request.Request(url, headers={"User-Agent": "hades-session-monitor/1.0"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.load(resp)
    results = data.get("results", data)
    out: dict[str, dict] = {}
    for row in results:
        sym = row.get("symbol")
        if sym:
            out[sym] = row
    return out


def notify_mac(title: str, message: str) -> None:
    try:
        safe = message.replace('"', "'")[:200]
        safe_title = title.replace('"', "'")[:60]
        script = f'display notification "{safe}" with title "{safe_title}"'
        subprocess.run(["osascript", "-e", script], check=False, capture_output=True)
    except OSError:
        pass


def log(msg: str) -> None:
    ts = datetime.now(ET).strftime("%H:%M:%S ET")
    line = f"[{ts}] {msg}"
    print(line, flush=True)


def mid_from_quote(q: dict) -> float | None:
    bid = float(q.get("bid_price") or 0)
    ask = float(q.get("ask_price") or 0)
    last = float(q.get("last_trade_price") or 0)
    if bid > 0 and ask > 0:
        return (bid + ask) / 2
    return last if last > 0 else None


def day_change_pct(q: dict) -> float | None:
    prev = float(q.get("adjusted_previous_close") or q.get("previous_close") or 0)
    last = float(q.get("last_trade_price") or 0)
    if prev <= 0:
        return None
    return abs((last - prev) / prev)


def spread_metrics(q: dict) -> tuple[float, float] | None:
    bid = float(q.get("bid_price") or 0)
    ask = float(q.get("ask_price") or 0)
    if bid <= 0 or ask <= 0:
        return None
    mid = (bid + ask) / 2
    spread_usd = ask - bid
    spread_pct = spread_usd / mid if mid else 999
    return spread_pct, spread_usd


def check_positions(
    state: dict, quotes: dict[str, dict], cfg: dict
) -> tuple[list[str], list[dict]]:
    alerts: list[str] = []
    snapshots: list[dict] = []
    scalp = cfg.get("scalp", {})
    clear_bps = float(scalp.get("stop_loss_clear_bps", 15)) / 10000

    for pos in state.get("positions", []):
        sym = pos["symbol"]
        if pos.get("pending"):
            log(f"{sym} pending buy (order {pos.get('buy_order_id', '?')[:8]}…) — awaiting fill")
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
        frac = pos.get("fractional", False)
        dist_sl = last - sl
        dist_tp = tp - last
        snap = {
            "symbol": sym,
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
        }
        snapshots.append(snap)
        log(
            f"{sym} last=${last:.2f} entry=${entry:.2f} qty={qty} "
            f"PnL=${pnl:+.2f} ({pnl_pct:+.2f}%) TP=${tp:.2f} SL=${sl:.2f}"
            + (" [fractional]" if frac else "")
        )

        if last >= tp and (pos.get("synthetic_tp") or frac):
            sl_oid = pos.get("sl_order_id", "")
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
    state: dict,
    option_quotes: dict[str, dict],
    options_cfg: dict,
) -> tuple[list[str], list[dict]]:
    """Premium TP/SL checks for option_positions (mark vs entry)."""
    alerts: list[str] = []
    snapshots: list[dict] = []
    now_t = datetime.now(ET).time()
    flat_t = datetime.strptime(
        options_cfg.get("hard_flat_time_et", "15:45"), "%H:%M"
    ).time()

    for pos in state.get("option_positions", []):
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
            "asset": "option",
        }
        snapshots.append(snap)
        log(
            f"OPT {label} mark=${last:.4f} entry=${entry:.4f} qty={qty:g} "
            f"PnL=${pnl:+.2f} ({pnl_pct:+.1f}%) TP=${tp:.4f} SL=${sl:.4f}"
        )

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


def fetch_option_quotes_mcp(option_ids: list[str]) -> dict[str, dict]:
    """Option marks via MCP (required — public marketdata returns 403)."""
    if not option_ids:
        return {}
    from mcp.auth import load_access_token, mcp_cli_authenticated
    from mcp.executor import TradeActions

    if not (load_access_token() or mcp_cli_authenticated()):
        return {}
    return TradeActions().get_option_quotes(option_ids)


def write_live_outputs(
    state: dict,
    snapshots: list[dict],
    alerts: list[str],
    poll: int,
    auto_exit: bool,
) -> None:
    ts = datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S ET")
    lines = [
        "# Tech scalper — LIVE",
        "",
        f"**Updated:** {ts} · poll **{poll}s** · auto-exit **{'ON' if auto_exit else 'off'}**",
        "",
        "## Positions",
        "",
        "| Symbol | Last | Entry | PnL | TP | SL | Note |",
        "|--------|------|-------|-----|----|----|------|",
    ]
    tick_parts = []
    for s in snapshots:
        note = ""
        is_opt = s.get("asset") == "option"
        if is_opt and s["dist_sl"] <= s["sl"] * 0.05:
            note = "⚠️ near SL"
        elif not is_opt and s["dist_sl"] <= 0.25:
            note = "⚠️ near SL"
        elif any(
            (a.startswith("SL_HIT:") or a.startswith("OPT_SL_HIT:"))
            and s["symbol"] in a
            for a in alerts
        ):
            note = "🛑 SL"
        elif any(a.startswith("OPT_TP_HIT:") and s.get("option_id", "") in a for a in alerts):
            note = "✅ TP"
        frac = " frac" if s["fractional"] else ""
        asset = " opt" if is_opt else ""
        prec = 4 if is_opt else 2
        lines.append(
            f"| **{s['symbol']}** | ${s['last']:.{prec}f} | ${s['entry']:.{prec}f} | "
            f"${s['pnl']:+.2f} ({s['pnl_pct']:+.2f}%) | ${s['tp']:.{prec}f} | "
            f"${s['sl']:.{prec}f} | {note}{frac}{asset} |"
        )
        tick_parts.append(f"{s['symbol']} ${s['last']:.{prec}f} ({s['pnl_pct']:+.2f}%)")

    if alerts:
        lines.extend(["", "## Alerts", ""])
        for a in alerts:
            lines.append(f"- {a}")

    lines.extend(
        [
            "",
            "---",
            "*Auto-refreshes every poll. Open `dashboard.html` in Simple Browser or keep this file pinned.*",
        ]
    )
    LIVE_STATUS_PATH.write_text("\n".join(lines) + "\n")

    payload = {
        "updated_at": ts,
        "poll_seconds": poll,
        "auto_exit": auto_exit,
        "positions": snapshots,
        "alerts": alerts,
        "tick_summary": " · ".join(tick_parts),
    }
    LATEST_JSON_PATH.write_text(json.dumps(payload, indent=2) + "\n")

    rows_html = ""
    for s in snapshots:
        pnl_class = "pos" if s["pnl"] >= 0 else "neg"
        rows_html += f"""
        <tr>
          <td><b>{s['symbol']}</b></td>
          <td>${s['last']:.2f}</td>
          <td>${s['entry']:.2f}</td>
          <td class="{pnl_class}">${s['pnl']:+.2f} ({s['pnl_pct']:+.2f}%)</td>
          <td>${s['tp']:.2f}</td>
          <td>${s['sl']:.2f}</td>
          <td>{'frac' if s['fractional'] else 'whole'}</td>
        </tr>"""

    alert_html = "".join(f"<li>{a}</li>" for a in alerts) or "<li>—</li>"
    DASHBOARD_PATH.write_text(
        f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="{poll}">
<title>Tech Scalper Live</title>
<style>
  body {{ font-family: system-ui, sans-serif; background: #0f1419; color: #e7e9ea; margin: 1.5rem; }}
  h1 {{ font-size: 1.2rem; }}
  .meta {{ color: #71767b; font-size: 0.85rem; margin-bottom: 1rem; }}
  table {{ border-collapse: collapse; width: 100%; max-width: 720px; }}
  th, td {{ text-align: left; padding: 0.5rem 0.75rem; border-bottom: 1px solid #2f3336; }}
  th {{ color: #71767b; font-weight: 500; }}
  .pos {{ color: #00ba7c; }} .neg {{ color: #f4212e; }}
  ul {{ color: #ffd400; }}
</style>
</head><body>
<h1>Tech scalper — LIVE</h1>
<p class="meta">{ts} · refresh {poll}s · auto-exit {'ON' if auto_exit else 'off'}</p>
<table>
  <tr><th>Symbol</th><th>Last</th><th>Entry</th><th>PnL</th><th>TP</th><th>SL</th><th>Type</th></tr>
  {rows_html}
</table>
<h2>Alerts</h2>
<ul>{alert_html}</ul>
</body></html>"""
    )


def scan_watchlist(cfg: dict, state: dict, quotes: dict[str, dict]) -> list[dict]:
    sel = cfg.get("selection", {})
    cap = float(cfg.get("position_sizing", {}).get("target_notional_usd", 100))
    overrides = set(state.get("cap_overrides", []))
    deprior = set(sel.get("deprioritize_symbols", []))
    core = set(sel.get("core_symbols", []))
    max_spread_pct = float(sel.get("max_spread_pct", 0.0015))
    max_spread_usd = float(sel.get("max_spread_usd", 0.25))
    min_day = float(sel.get("min_abs_day_change_pct", 0.003))
    max_day = float(sel.get("max_abs_day_change_pct", 0.008))
    held = {p["symbol"] for p in state.get("positions", [])}

    rows = []
    for sym, q in quotes.items():
        if sym in held:
            continue
        mid = mid_from_quote(q)
        if mid is None:
            continue
        fits = mid <= cap or sym in overrides
        sm = spread_metrics(q)
        if not sm:
            continue
        spread_pct, spread_usd = sm
        day = day_change_pct(q)
        if day is None:
            continue
        if not (spread_pct <= max_spread_pct and spread_usd <= max_spread_usd):
            continue
        if not (min_day <= day <= max_day):
            continue
        if not fits and not cfg.get("entry", {}).get("allow_fractional_live", False):
            continue
        rows.append(
            {
                "symbol": sym,
                "mid": mid,
                "spread_pct": spread_pct,
                "day": day,
                "core": sym in core,
                "deprior": sym in deprior,
            }
        )

    rows.sort(key=lambda r: (r["deprior"], r["spread_pct"], not r["core"], -r["day"]))
    log(f"RESCAN: {len(rows)} qualified new entries (excl held)")
    for r in rows[:5]:
        log(
            f"  {r['symbol']:5} mid=${r['mid']:7.2f} "
            f"spread={r['spread_pct']*100:.4f}% day={r['day']*100:.2f}%"
        )
    return rows


def in_entry_window(cfg: dict) -> bool:
    scalp = cfg.get("scalp", {})
    now = datetime.now(ET).time()
    start = datetime.strptime(scalp.get("no_new_entry_before_et", "09:45"), "%H:%M").time()
    end = datetime.strptime(scalp.get("no_new_entry_after_et", "15:30"), "%H:%M").time()
    return start <= now <= end


def run_reconcile(cfg: dict, state_path: Path) -> None:
    """Sync state with broker: adopt untracked positions, finalize/drop pending buys."""
    from mcp.auth import load_access_token, mcp_cli_authenticated
    from mcp.executor import TradeActions

    if not (load_access_token() or mcp_cli_authenticated()):
        return
    try:
        for msg in TradeActions().reconcile_state(state_path, cfg):
            log(msg)
    except Exception as e:
        log(f"RECONCILE ERROR: {e}")


def run_auto_entry(state: dict, candidate: dict, cfg: dict, repo_root: Path, state_path: Path) -> bool:
    """Direct MCP entry when token available; else cursor agent."""
    from mcp.auth import load_access_token, mcp_cli_authenticated
    from mcp.executor import TradeActions
    from mcp.http_client import McpHttpError

    sym = candidate["symbol"]
    if load_access_token() or mcp_cli_authenticated():
        try:
            log(f"AUTO-ENTRY (MCP): {sym}...")
            ok = TradeActions().execute_auto_entry(candidate, state, cfg, state_path)
            log(f"AUTO-ENTRY (MCP) {sym}: {'OK' if ok else 'skipped/failed'}")
            return ok
        except McpHttpError as e:
            log(f"AUTO-ENTRY MCP FAILED {sym}: {e}")

    acct = state.get("account_number", "")
    overrides = ",".join(state.get("cap_overrides", []))
    allow_frac = cfg.get("entry", {}).get("allow_fractional_live", False)
    prompt = (
        f"Robinhood MCP tech scalper AUTO-ENTRY on Agentic {acct}. "
        f"Rescan pick: {sym} mid=${candidate['mid']:.2f} spread={candidate['spread_pct']*100:.4f}% "
        f"day={candidate['day']*100:.2f}%. "
        f"1) get_portfolio + get_equity_positions — verify open slot and GFV-safe buying_power. "
        f"2) cap=min(target_notional_usd, BP-reserve); cap_overrides=[{overrides}]. "
        f"3) Whole-share limit buy if mid<=cap else "
        f"{'fractional market dollar_amount=cap' if allow_frac else 'skip unless override symbol'}. "
        f"4) Post-fill: whole-share broker stop_market SL + synthetic_tp; fractional synthetic TP+SL. "
        f"5) Update robinhood_agentic/monitor/session_state.json. Execute now per tech_scalper.yaml."
    )
    log(f"AUTO-ENTRY (agent): launching cursor agent for {sym}...")
    cmd = [
        "cursor", "agent", "-p", "--trust", "--approve-mcps", "--yolo",
        "--output-format", "text",
        prompt,
    ]
    result = subprocess.run(cmd, cwd=repo_root, check=False, capture_output=True, text=True)
    return result.returncode == 0


def run_auto_exit(state: dict, alert: str, repo_root: Path, state_path: Path) -> bool:
    """Direct MCP exit when token available; else cursor agent."""
    from mcp.auth import load_access_token, mcp_cli_authenticated
    from mcp.executor import TradeActions
    from mcp.http_client import McpHttpError

    parts = alert.split(":")
    if len(parts) < 2:
        return False
    kind = parts[0]

    if kind in {"OPT_TP_HIT", "OPT_SL_HIT", "OPT_FLAT"}:
        option_id = parts[1]
        label = option_id[:8]
        for p in parts:
            if p.startswith("label="):
                label = p.replace("label=", "")
        if load_access_token() or mcp_cli_authenticated():
            try:
                log(f"AUTO-EXIT (MCP {kind}): {label}...")
                ok = TradeActions().execute_option_exit(alert, state, state_path)
                log(f"AUTO-EXIT (MCP) {label}: {'OK' if ok else 'FAILED'}")
                return ok
            except McpHttpError as e:
                log(f"AUTO-EXIT MCP FAILED {label}: {e}")
        acct = state.get("account_number", "")
        prompt = (
            f"Robinhood MCP option EXIT on Agentic {acct}. "
            f"Sell-to-close option_id={option_id} ({label}) quantity 1 market. "
            f"Remove from option_positions in robinhood_agentic/monitor/session_state.json. "
            f"Execute now — no questions."
        )
        log(f"AUTO-EXIT (agent {kind}): launching cursor agent for {label}...")
        cmd = [
            "cursor", "agent", "-p", "--trust", "--approve-mcps", "--yolo",
            "--output-format", "text",
            prompt,
        ]
        result = subprocess.run(
            cmd, cwd=repo_root, check=False, capture_output=True, text=True
        )
        return result.returncode == 0

    sym = parts[1]

    if load_access_token() or mcp_cli_authenticated():
        try:
            log(f"AUTO-EXIT (MCP {kind}): {sym}...")
            ok = TradeActions().execute_synthetic_exit(alert, state, state_path)
            log(f"AUTO-EXIT (MCP) {sym}: {'OK' if ok else 'FAILED'}")
            return ok
        except McpHttpError as e:
            log(f"AUTO-EXIT MCP FAILED {sym}: {e}")

    acct = state.get("account_number", "")
    if kind == "SL_HIT":
        prompt = (
            f"Robinhood MCP synthetic STOP on Agentic {acct}. {sym}: "
            f"Use robinhood-trading MCP: get_equity_positions, review_equity_order, "
            f"place_equity_order market sell full {sym} position. "
            f"Remove {sym} from robinhood_agentic/monitor/session_state.json after fill. "
            f"Execute now — no questions."
        )
    elif kind == "TP_HIT":
        sl_order = ""
        for p in parts:
            if p.startswith("sl_order="):
                sl_order = p.replace("sl_order=", "")
        cancel = (
            f"Cancel equity order {sl_order} if still open, then "
            if sl_order
            else ""
        )
        prompt = (
            f"Robinhood MCP synthetic TAKE PROFIT on Agentic {acct}. "
            f"{sym}: {cancel}Use robinhood-trading MCP market sell full {sym} position. "
            f"Remove {sym} from robinhood_agentic/monitor/session_state.json after fill. "
            f"Execute now — no questions."
        )
    else:
        return False
    log(f"AUTO-EXIT (agent {kind}): launching cursor agent for {sym}...")
    cmd = [
        "cursor", "agent", "-p", "--trust", "--approve-mcps", "--yolo",
        "--output-format", "text",
        prompt,
    ]
    result = subprocess.run(
        cmd, cwd=repo_root, check=False, capture_output=True, text=True
    )
    out = (result.stdout or "") + (result.stderr or "")
    if out.strip():
        for line in out.strip().splitlines()[:20]:
            log(f"  agent> {line}")
    return result.returncode == 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Tech scalper session monitor")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--poll-seconds", type=int, default=None)
    parser.add_argument("--rescan-minutes", type=int, default=15)
    parser.add_argument("--notify", action="store_true", help="macOS notifications on alerts")
    parser.add_argument(
        "--tick-notify",
        action="store_true",
        help="macOS notification every poll with position summary",
    )
    parser.add_argument(
        "--auto-entry",
        action="store_true",
        help="After rescan, invoke cursor agent to enter top pick if slot open",
    )
    parser.add_argument(
        "--auto-exit",
        action="store_true",
        help="On synthetic SL hit, invoke cursor agent to sell (real trades)",
    )
    parser.add_argument("--once", action="store_true", help="Single poll + rescan, then exit")
    args = parser.parse_args()

    repo_root = ROOT.parent
    cfg = load_yaml(args.config)
    options_cfg = load_options_cfg()
    cfg["options"] = options_cfg
    state = load_state(args.state)
    poll = args.poll_seconds or int(cfg.get("scalp", {}).get("poll_seconds", 15))
    watchlist = flatten_watchlist(cfg)

    log(
        f"Monitor start | poll={poll}s rescan={args.rescan_minutes}m "
        f"watchlist={len(watchlist)} symbols | options TP={options_cfg['profit_target_pct']*100:.0f}% "
        f"SL={options_cfg['stop_loss_pct']*100:.0f}%"
    )
    if args.auto_exit:
        from mcp.auth import auth_status_message, load_access_token, mcp_cli_authenticated

        if load_access_token():
            log("AUTO-EXIT enabled — direct MCP HTTP executor")
        elif mcp_cli_authenticated():
            log("AUTO-EXIT enabled — cursor agent MCP bridge (robinhood-trading ready)")
        else:
            log("AUTO-EXIT enabled — cursor agent NL fallback (run: cursor agent mcp login robinhood-trading)")
            log(f"  auth: {auth_status_message()}")
    if args.auto_entry:
        log("AUTO-ENTRY enabled — rescan will trigger cursor agent entries")

    reconcile_enabled = args.auto_exit or args.auto_entry
    if reconcile_enabled:
        log("RECONCILE: startup sync with broker...")
        run_reconcile(cfg, args.state)

    last_rescan = 0.0
    last_reconcile = time.time()
    last_option_quote = 0.0
    option_quotes_cache: dict[str, dict] = {}
    pending_reconcile_seconds = 60
    exit_attempt_at: dict[str, float] = {}
    exit_retry_seconds = 90
    entry_triggered: set[str] = set()

    while True:
        state = load_state(args.state)
        positions = state.get("positions", [])
        option_positions = state.get("option_positions", [])
        has_pending = any(p.get("pending") for p in positions)
        if (
            reconcile_enabled
            and has_pending
            and time.time() - last_reconcile >= pending_reconcile_seconds
        ):
            run_reconcile(cfg, args.state)
            last_reconcile = time.time()
            state = load_state(args.state)
            positions = state.get("positions", [])
            option_positions = state.get("option_positions", [])
        pos_symbols = [p["symbol"] for p in positions]
        tick_symbols = list(dict.fromkeys(pos_symbols + watchlist))
        try:
            quotes = fetch_quotes(tick_symbols)
        except Exception as e:
            log(f"QUOTE ERROR: {e}")
            if args.once:
                return 1
            time.sleep(poll)
            continue

        alerts, snapshots = check_positions(state, quotes, cfg)

        # Options: MCP quotes (slower). Refresh at least every OPTION_QUOTE_MIN_SECONDS.
        opt_ids = [p["option_id"] for p in option_positions if p.get("option_id")]
        if opt_ids and (
            time.time() - last_option_quote >= OPTION_QUOTE_MIN_SECONDS
            or not option_quotes_cache
        ):
            try:
                option_quotes_cache = fetch_option_quotes_mcp(opt_ids)
                last_option_quote = time.time()
                if not option_quotes_cache:
                    log("OPTION QUOTE: empty (MCP auth / bridge issue?)")
            except Exception as e:
                log(f"OPTION QUOTE ERROR: {e}")
        if opt_ids and option_quotes_cache:
            o_alerts, o_snaps = check_option_positions(
                state, option_quotes_cache, options_cfg
            )
            alerts.extend(o_alerts)
            snapshots.extend(o_snaps)

        write_live_outputs(state, snapshots, alerts, poll, args.auto_exit)
        if args.tick_notify and snapshots:
            tick = " · ".join(
                f"{s['symbol']} ${s['last']:.4f} ({s['pnl_pct']:+.2f}%)"
                if s.get("asset") == "option"
                else f"{s['symbol']} ${s['last']:.2f} ({s['pnl_pct']:+.2f}%)"
                for s in snapshots
            )
            notify_mac("Scalper", tick)

        for a in alerts:
            if a.startswith("SL_HIT:") or a.startswith("TP_HIT:"):
                sym = a.split(":")[1]
                key = f"{a.split(':')[0]}:{sym}"
                held = sym in pos_symbols
                if not held:
                    continue
                now_ts = time.time()
                last_try = exit_attempt_at.get(key, 0.0)
                if now_ts - last_try < exit_retry_seconds:
                    continue
                label = "STOP LOSS" if a.startswith("SL_HIT") else "TAKE PROFIT"
                log(f"*** {label} TRIGGERED: {sym} ***")
                if args.notify:
                    notify_mac(f"Tech Scalper {label}", f"{sym} — auto-exit")
                if args.auto_exit:
                    exit_attempt_at[key] = now_ts
                    run_auto_exit(state, a, repo_root, args.state)
            elif a.startswith("OPT_TP_HIT:") or a.startswith("OPT_SL_HIT:") or a.startswith("OPT_FLAT:"):
                oid = a.split(":")[1]
                key = f"{a.split(':')[0]}:{oid}"
                held = any(p.get("option_id") == oid for p in option_positions)
                if not held:
                    continue
                now_ts = time.time()
                last_try = exit_attempt_at.get(key, 0.0)
                if now_ts - last_try < exit_retry_seconds:
                    continue
                if a.startswith("OPT_FLAT"):
                    label = "OPTION TIME FLAT"
                elif a.startswith("OPT_SL"):
                    label = "OPTION STOP LOSS"
                else:
                    label = "OPTION TAKE PROFIT"
                log(f"*** {label} TRIGGERED: {oid[:8]}… ***")
                if args.notify:
                    notify_mac(f"Options {label}", "auto-exit")
                if args.auto_exit:
                    exit_attempt_at[key] = now_ts
                    run_auto_exit(state, a, repo_root, args.state)
            else:
                log(f"ALERT: {a}")
                if args.notify:
                    notify_mac("Tech Scalper", a)

        now = time.time()
        if now - last_rescan >= args.rescan_minutes * 60:
            if reconcile_enabled:
                run_reconcile(cfg, args.state)
                last_reconcile = time.time()
                state = load_state(args.state)
                positions = state.get("positions", [])
            rows = scan_watchlist(cfg, state, quotes)
            last_rescan = now
            max_conc = int(cfg.get("position_sizing", {}).get("max_concurrent", 2))
            slots = max_conc - len(positions)
            if (
                args.auto_entry
                and rows
                and slots > 0
                and in_entry_window(cfg)
            ):
                top = rows[0]
                key = f"{top['symbol']}:{datetime.now(ET).date().isoformat()}"
                if key not in entry_triggered:
                    entry_triggered.add(key)
                    log(f"*** AUTO-ENTRY CANDIDATE: {top['symbol']} (slot open) ***")
                    if args.notify:
                        notify_mac("Tech Scalper Entry", f"{top['symbol']} — auto-entry")
                    run_auto_entry(state, top, cfg, repo_root, args.state)
            elif args.auto_entry and not rows and slots > 0:
                log("RESCAN: no qualified entries for auto-entry")
            elif args.auto_entry and slots <= 0:
                log("RESCAN: max concurrent — skip auto-entry")

        if args.once:
            break
        time.sleep(poll)

    return 0


if __name__ == "__main__":
    sys.exit(main())

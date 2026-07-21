"""Trading engine main loop."""

from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from ..broker import probe_mcp, get_trade_actions, mcp_available, sync_state_file
from ..db.repository import Repository
from ..paths import REPO_ROOT, SESSION_STATE_JSON
from .commands import CommandProcessor
from .positions import check_equity_positions, check_option_positions
from .quotes import fetch_quotes
from .risk import RiskGate
from .scanner import qualified_only, scan_watchlist_full

ET = ZoneInfo("America/New_York")
OPTION_QUOTE_MIN_SECONDS = 30


class LiveCache:
    """Thread-safe snapshot for API/SSE."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.data: dict[str, Any] = {
            "updated_at": None,
            "snapshots": [],
            "alerts": [],
            "quotes": {},
            "system": {},
            "daily_stats": {},
            "last_scan": None,
            "scan_schedule": None,
            "entry_decision": None,
        }

    def update(self, **kwargs: Any) -> None:
        with self._lock:
            self.data.update(kwargs)
            self.data["updated_at"] = datetime.now(ET).isoformat()

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return dict(self.data)


class TradingEngine:
    def __init__(self, repo: Repository, live: LiveCache) -> None:
        self.repo = repo
        self.live = live
        self.risk = RiskGate(repo)
        self._stop = threading.Event()
        self._force_rescan = threading.Event()
        self._thread: threading.Thread | None = None
        self._option_quotes: dict[str, dict] = {}
        self._last_option_quote = 0.0
        self._exit_attempt_at: dict[str, float] = {}
        self._last_rescan_at: float | None = None
        self._scan_in_progress = False
        self.commands = CommandProcessor(
            repo,
            get_tech_cfg=self.tech_cfg,
            get_options_cfg=self.options_cfg,
            request_rescan=lambda: self._force_rescan.set(),
            live_cache=live.data,
        )

    def tech_cfg(self) -> dict[str, Any]:
        row = self.repo.get_active_config("tech_scalper")
        return (row or {}).get("config") or {}

    def options_cfg(self) -> dict[str, Any]:
        row = self.repo.get_active_config("options_directional")
        return (row or {}).get("config") or {}

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="trading-engine", daemon=True)
        self._thread.start()
        self.repo.audit("engine_start", {})

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        self.repo.audit("engine_stop", {})

    def _run(self) -> None:
        last_rescan = 0.0
        last_mcp_probe = 0.0
        last_reconcile = 0.0
        while not self._stop.is_set():
            ss = self.repo.get_system_state()
            poll = int(ss.get("poll_seconds") or 15)
            rescan_m = int(ss.get("rescan_minutes") or 15)
            try:
                self.commands.process_pending()
                ss = self.repo.get_system_state()
                poll = int(ss.get("poll_seconds") or 15)
                rescan_m = int(ss.get("rescan_minutes") or 15)
                # Publish schedule early so UI countdown works even if MCP is slow
                self._publish_scan_schedule(ss, rescan_m, time.time())

                # MCP health every ~60s
                if time.time() - last_mcp_probe >= 60:
                    ok, detail = probe_mcp()
                    self.repo.heartbeat(mcp_ok=ok, mcp_detail=detail)
                    last_mcp_probe = time.time()
                else:
                    self.repo.heartbeat()

                # Broker reconcile — drop ghosts / adopt orphans (every 60s, background)
                if mcp_available()[0] and time.time() - last_reconcile >= 60:
                    last_reconcile = time.time()
                    threading.Thread(
                        target=self._reconcile_broker, name="reconcile", daemon=True
                    ).start()

                tech = self.tech_cfg()
                opts = self.options_cfg()
                positions = self.repo.list_positions()
                option_positions = self.repo.list_option_positions()
                watchlist = list(tech.get("watchlist", {}).get("all", []))
                pos_symbols = [p["symbol"] for p in positions]
                tick_symbols = list(dict.fromkeys(pos_symbols + watchlist))

                try:
                    quotes = fetch_quotes(tick_symbols)
                except Exception as e:
                    self.repo.audit("quote_error", {"error": str(e)})
                    quotes = {}

                alerts, snapshots = check_equity_positions(positions, quotes, tech)

                # Equity hard flat
                if (
                    self.risk.equity_hard_flat_due(tech)
                    and positions
                    and ss.get("auto_exit_enabled")
                    and ss.get("mode") != "halted"
                ):
                    for p in positions:
                        if p.get("pending") or not p.get("auto_exit", True):
                            continue
                        alerts.append(
                            f"TP_HIT:{p['symbol']}:last={p['entry']}:tp={p['tp']}:"
                            f"sl_order={p.get('sl_order_id') or ''}:fractional="
                            f"{str(bool(p.get('fractional'))).lower()}"
                        )
                        self.repo.audit("equity_hard_flat", {"symbol": p["symbol"]})

                opt_ids = [p["option_id"] for p in option_positions if p.get("option_id")]
                if opt_ids and (
                    time.time() - self._last_option_quote >= OPTION_QUOTE_MIN_SECONDS
                    or not self._option_quotes
                ):
                    try:
                        if mcp_available()[0]:
                            self._option_quotes = get_trade_actions().get_option_quotes(opt_ids)
                            self._last_option_quote = time.time()
                    except Exception as e:
                        self.repo.audit("option_quote_error", {"error": str(e)})

                if opt_ids and self._option_quotes:
                    o_alerts, o_snaps = check_option_positions(
                        option_positions, self._option_quotes, opts
                    )
                    alerts.extend(o_alerts)
                    snapshots.extend(o_snaps)

                # Rescan
                now = time.time()
                do_rescan = (
                    self._force_rescan.is_set()
                    or now - last_rescan >= rescan_m * 60
                )
                if do_rescan and ss.get("scanner_enabled"):
                    trigger = "forced" if self._force_rescan.is_set() else "scheduled"
                    self._force_rescan.clear()
                    self._scan_in_progress = True
                    self._publish_scan_schedule(ss, rescan_m, now)
                    try:
                        self._do_rescan(tech, quotes, trigger=trigger)
                    finally:
                        self._scan_in_progress = False
                    last_rescan = time.time()
                    self._last_rescan_at = last_rescan

                self.live.update(
                    snapshots=snapshots,
                    alerts=alerts,
                    quotes=quotes,
                    system=self.repo.get_system_state(),
                    daily_stats=self.repo.get_daily_stats(),
                    last_scan=self.repo.latest_scan(),
                    scan_schedule=self._scan_schedule_payload(ss, rescan_m, time.time()),
                    # keep last entry_decision until next scan overwrites it
                    entry_decision=self.live.snapshot().get("entry_decision"),
                )

                # Auto exits
                if ss.get("auto_exit_enabled") and ss.get("mode") != "halted":
                    self._handle_exits(alerts, positions, option_positions)

            except Exception as e:
                self.repo.audit("engine_tick_error", {"error": str(e)})

            self._stop.wait(poll)

    def _scan_schedule_payload(
        self, ss: dict[str, Any], rescan_m: int, now: float
    ) -> dict[str, Any]:
        interval = rescan_m * 60
        last = self._last_rescan_at
        if self._force_rescan.is_set() or self._scan_in_progress:
            seconds_until = 0
            next_at = now
        elif last is None:
            # First scan pending on next loop
            seconds_until = 0
            next_at = now
        else:
            next_at = last + interval
            seconds_until = max(0, int(next_at - now))
        last_iso = (
            datetime.fromtimestamp(last, ET).isoformat() if last else None
        )
        next_iso = datetime.fromtimestamp(next_at, ET).isoformat()
        return {
            "rescan_minutes": rescan_m,
            "scanner_enabled": bool(ss.get("scanner_enabled")),
            "scan_in_progress": self._scan_in_progress,
            "force_pending": self._force_rescan.is_set(),
            "last_rescan_at": last_iso,
            "next_rescan_at": next_iso,
            "seconds_until_next": seconds_until,
        }

    def _publish_scan_schedule(self, ss: dict[str, Any], rescan_m: int, now: float) -> None:
        self.live.update(scan_schedule=self._scan_schedule_payload(ss, rescan_m, now))

    def _handle_exits(
        self,
        alerts: list[str],
        positions: list[dict],
        option_positions: list[dict],
    ) -> None:
        pos_symbols = {p["symbol"] for p in positions}
        opt_ids = {p["option_id"] for p in option_positions}
        retry = 90
        exiting_syms: set[str] = set()
        exiting_oids: set[str] = set()
        for a in alerts:
            if a.startswith("SL_HIT:") or a.startswith("TP_HIT:"):
                sym = a.split(":")[1]
                if sym not in pos_symbols:
                    continue
                key = f"{a.split(':')[0]}:{sym}"
                if time.time() - self._exit_attempt_at.get(key, 0) < retry:
                    # Still waiting on in-flight exit — keep hidden from UI
                    if any(p["symbol"] == sym and p.get("pending") for p in positions):
                        exiting_syms.add(sym)
                    continue
                self._exit_attempt_at[key] = time.time()
                reason = "synth_tp" if a.startswith("TP_HIT") else "synth_sl"
                self.repo.audit("auto_exit_trigger", {"alert": a, "reason": reason})
                # Hide from Live Positions immediately while sell is in flight
                self.repo.set_position_pending(sym, True)
                exiting_syms.add(sym)
                ss = self.repo.get_system_state()
                if ss.get("mode") == "copilot":
                    self.repo.create_approval(
                        "equity_exit", {"alert": a, "reason": reason}, symbol=sym
                    )
                else:
                    self.repo.enqueue_command(
                        "liquidate",
                        {"symbol": sym, "exit_reason": reason, "alert": a},
                    )
            elif a.startswith("OPT_"):
                oid = a.split(":")[1]
                if oid not in opt_ids:
                    continue
                key = f"{a.split(':')[0]}:{oid}"
                if time.time() - self._exit_attempt_at.get(key, 0) < retry:
                    if any(p["option_id"] == oid and p.get("pending") for p in option_positions):
                        exiting_oids.add(oid)
                    continue
                self._exit_attempt_at[key] = time.time()
                if a.startswith("OPT_FLAT"):
                    reason = "time_flat"
                elif a.startswith("OPT_SL"):
                    reason = "synth_sl"
                else:
                    reason = "synth_tp"
                self.repo.audit("auto_exit_trigger", {"alert": a, "reason": reason})
                self.repo.set_option_position_pending(oid, True)
                exiting_oids.add(oid)
                ss = self.repo.get_system_state()
                if ss.get("mode") == "copilot":
                    self.repo.create_approval(
                        "option_exit", {"alert": a, "reason": reason}, option_id=oid
                    )
                else:
                    self.repo.enqueue_command(
                        "liquidate",
                        {"option_id": oid, "exit_reason": reason, "alert": a},
                    )

        if exiting_syms or exiting_oids:
            snap = self.live.snapshot()
            filtered = [
                s
                for s in (snap.get("snapshots") or [])
                if s.get("symbol") not in exiting_syms
                and s.get("option_id") not in exiting_oids
            ]
            self.live.update(snapshots=filtered)

    def _do_rescan(self, tech: dict[str, Any], quotes: dict, *, trigger: str) -> None:
        held = {p["symbol"] for p in self.repo.list_positions()}
        # Ensure watchlist quotes present
        watchlist = list(tech.get("watchlist", {}).get("all", []))
        missing = [s for s in watchlist if s not in quotes]
        if missing:
            try:
                quotes = {**quotes, **fetch_quotes(missing)}
            except Exception:
                pass
        records = scan_watchlist_full(tech, quotes, held, cap_overrides=[])
        self.repo.save_scan_snapshot(records, trigger=trigger)
        qualified = qualified_only(records)
        self.repo.audit(
            "scan_complete",
            {
                "trigger": trigger,
                "qualified": len(qualified),
                "evaluated": len(records),
                "top": [r["symbol"] for r in qualified[:5]],
            },
        )

        decision = self._decide_entry_after_scan(tech, qualified)
        self.repo.audit("entry_decision", decision)
        self.live.update(
            last_scan=self.repo.latest_scan(),
            entry_decision=decision,
        )

        if decision.get("action") == "enqueue_entry":
            cand = decision.get("candidate") or {}
            sym = cand.get("symbol")
            self.repo.audit("auto_entry_attempt", {"symbol": sym})
            self.repo.enqueue_command("auto_enter", {"candidate": cand})
            if self.risk.same_day_block_enabled() and sym:
                self.repo.block_symbol(sym, reason="auto_entry_queued")
        elif decision.get("action") == "propose":
            cand = decision.get("candidate") or {}
            sym = cand.get("symbol")
            self.repo.create_approval(
                "equity_entry", {"candidate": cand}, symbol=sym
            )
            self.repo.audit("auto_entry_proposed", {"symbol": sym})
            if self.risk.same_day_block_enabled() and sym:
                self.repo.block_symbol(sym, reason="proposed")

    def _decide_entry_after_scan(
        self, tech: dict[str, Any], qualified: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """Build a UI-friendly explanation of what happened after the scan."""
        ss = self.repo.get_system_state()
        dry = bool(ss.get("dry_run"))
        top = qualified[0] if qualified else None
        base = {
            "ts": datetime.now(ET).isoformat(),
            "dry_run": dry,
            "auto_entry_enabled": bool(ss.get("auto_entry_enabled")),
            "mode": ss.get("mode"),
            "top_symbol": top["symbol"] if top else None,
            "qualified_count": len(qualified),
        }

        if not qualified:
            return {
                **base,
                "action": "none",
                "code": "no_qualified",
                "message": "No symbols passed the scan filters — nothing to enter.",
            }
        if ss.get("mode") == "halted":
            return {
                **base,
                "action": "blocked",
                "code": "halted",
                "message": f"{top['symbol']} qualified, but system is HALTED — no entry.",
            }
        if not ss.get("auto_entry_enabled"):
            return {
                **base,
                "action": "blocked",
                "code": "auto_entry_off",
                "message": (
                    f"{top['symbol']} qualified, but Auto-entry is OFF "
                    "(enable under Strategy → Flags to place orders)."
                ),
            }
        ok, reason = self.risk.can_enter(tech, self.options_cfg())
        if not ok:
            human = {
                "outside_entry_window": (
                    f"{top['symbol']} qualified, but outside the entry window "
                    f"({tech.get('scalp', {}).get('no_new_entry_before_et', '09:45')}–"
                    f"{tech.get('scalp', {}).get('no_new_entry_after_et', '15:30')} ET) — no new entries."
                ),
                "max_concurrent": f"{top['symbol']} qualified, but max concurrent positions reached.",
                "scanner_disabled": f"{top['symbol']} qualified, but scanner is disabled.",
                "auto_entry_disabled": f"{top['symbol']} qualified, but auto-entry is disabled.",
                "halted": f"{top['symbol']} qualified, but system is halted.",
            }.get(reason, f"{top['symbol']} qualified, but entry blocked: {reason}.")
            if reason.startswith("equity_sl_circuit"):
                human = f"{top['symbol']} qualified, but daily equity SL circuit breaker hit ({reason})."
            elif reason.startswith("option_loss_circuit"):
                human = f"{top['symbol']} qualified, but option-loss circuit breaker hit ({reason})."
            elif reason == "daily_realized_loss_limit":
                human = f"{top['symbol']} qualified, but daily realized-loss limit reached."
            return {
                **base,
                "action": "blocked",
                "code": reason,
                "message": human,
            }

        # Pick first symbol allowed by same-day block
        chosen = None
        for cand in qualified:
            if self.risk.symbol_allowed(cand["symbol"]):
                chosen = cand
                break
            self.repo.audit(
                "auto_entry_symbol_blocked", {"symbol": cand["symbol"]}
            )
        if not chosen:
            return {
                **base,
                "action": "blocked",
                "code": "same_day_block",
                "message": (
                    f"Top pick(s) already used today (same-day symbol block) — no entry. "
                    f"Candidates: {', '.join(c['symbol'] for c in qualified[:5])}."
                ),
            }

        if ss.get("mode") == "copilot":
            msg = f"{chosen['symbol']} qualified — queued for Co-Pilot approval (not placed yet)."
            if dry:
                msg += " Dry run is ON — even if approved, no live broker order will be sent."
            return {
                **base,
                "action": "propose",
                "code": "copilot_propose",
                "message": msg,
                "candidate": chosen,
                "top_symbol": chosen["symbol"],
            }

        if dry:
            return {
                **base,
                "action": "enqueue_entry",
                "code": "dry_run",
                "message": (
                    f"{chosen['symbol']} qualified — Dry run is ON, so this is a simulation only. "
                    "No live Robinhood order will be placed. Turn Dry run OFF under Strategy → Flags for real trades."
                ),
                "candidate": chosen,
                "top_symbol": chosen["symbol"],
            }

        return {
            **base,
            "action": "enqueue_entry",
            "code": "live_entry",
            "message": f"{chosen['symbol']} qualified — live auto-entry order queued.",
            "candidate": chosen,
            "top_symbol": chosen["symbol"],
        }

    def _reconcile_broker(self) -> None:
        """Sync SQLite positions with broker; drop ghosts and adopt orphans."""
        try:
            tech = self.tech_cfg()
            opts = self.options_cfg()
            cfg = {**tech, "options": opts.get("trade") or opts}
            before_eq = {p["symbol"]: p for p in self.repo.list_positions()}
            before_opt = {p["option_id"]: p for p in self.repo.list_option_positions()}
            state = self.repo.positions_as_state()
            sync_state_file(state, SESSION_STATE_JSON)
            msgs = list(get_trade_actions().reconcile_state(SESSION_STATE_JSON, cfg) or [])
            import json

            if SESSION_STATE_JSON.is_file():
                new_state = json.loads(SESSION_STATE_JSON.read_text())
                self.repo.replace_positions_from_state(new_state)
            after_eq = {p["symbol"] for p in self.repo.list_positions()}
            after_opt = {p["option_id"] for p in self.repo.list_option_positions()}
            for sym, pos in before_eq.items():
                if sym not in after_eq:
                    exit_px = float(pos.get("sl") or pos["entry"])
                    entry = float(pos["entry"])
                    qty = float(pos["qty"])
                    pnl = (exit_px - entry) * qty
                    self.repo.record_trade(
                        {
                            "asset_type": "equity",
                            "symbol": sym,
                            "qty": qty,
                            "entry_price": entry,
                            "exit_price": exit_px,
                            "pnl_usd": pnl,
                            "pnl_pct": (exit_px / entry - 1) * 100 if entry else 0,
                            "exit_reason": "broker_sl",
                            "opened_at": pos.get("opened_at"),
                        }
                    )
                    self.repo.audit(
                        "reconcile_removed_equity",
                        {"symbol": sym, "approx_exit": exit_px, "pnl": pnl},
                    )
                    # Drop ghost from UI immediately
                    snaps = [
                        s
                        for s in (self.live.snapshot().get("snapshots") or [])
                        if s.get("symbol") != sym
                    ]
                    self.live.update(snapshots=snaps)
            for oid, pos in before_opt.items():
                if oid not in after_opt:
                    entry = float(pos["entry"])
                    exit_px = float(pos.get("sl") or entry)
                    qty = float(pos["qty"])
                    mult = float(pos.get("multiplier") or 100)
                    pnl = (exit_px - entry) * qty * mult
                    self.repo.record_trade(
                        {
                            "asset_type": "option",
                            "symbol": pos.get("symbol"),
                            "option_id": oid,
                            "label": pos.get("label"),
                            "qty": qty,
                            "entry_price": entry,
                            "exit_price": exit_px,
                            "pnl_usd": pnl,
                            "pnl_pct": (exit_px / entry - 1) * 100 if entry else 0,
                            "exit_reason": "broker_flat",
                            "opened_at": pos.get("opened_at"),
                        }
                    )
            if msgs:
                self.repo.audit("reconcile", {"msgs": msgs[:20]})
        except Exception as e:
            self.repo.audit("reconcile_error", {"error": str(e)})

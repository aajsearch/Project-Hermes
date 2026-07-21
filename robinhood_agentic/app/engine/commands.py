"""Command queue processor (halt, liquidate, rescan, inject, mode)."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Callable

from ..broker import get_trade_actions, mcp_available, sync_state_file
from ..db.repository import Repository, now_iso
from ..paths import SESSION_STATE_JSON
from .risk import RiskGate
from .scanner import evaluate_symbol, scan_watchlist_full
from .quotes import fetch_quotes


class CommandProcessor:
    def __init__(
        self,
        repo: Repository,
        *,
        get_tech_cfg: Callable[[], dict[str, Any]],
        get_options_cfg: Callable[[], dict[str, Any]],
        request_rescan: Callable[[], None],
        live_cache: dict[str, Any],
    ) -> None:
        self.repo = repo
        self.get_tech_cfg = get_tech_cfg
        self.get_options_cfg = get_options_cfg
        self.request_rescan = request_rescan
        self.live_cache = live_cache
        self.risk = RiskGate(repo)
        self._heavy_busy = False

    def process_pending(self) -> None:
        """Light commands run inline; MCP-heavy commands run one-at-a-time in background."""
        heavy_types = {"auto_enter", "liquidate", "inject", "approve", "halt"}
        for cmd in self.repo.drain_commands():
            ctype = cmd["command_type"]
            if ctype in heavy_types:
                if self._heavy_busy:
                    continue  # leave pending for a later tick
                # Claim so we don't double-start
                self.repo.conn.execute(
                    "UPDATE command_queue SET status = 'running' WHERE id = ?",
                    (cmd["id"],),
                )
                self.repo.conn.commit()
                self._heavy_busy = True

                def _worker(c=cmd):
                    try:
                        result = self._dispatch(c["command_type"], c.get("payload") or {})
                        self.repo.complete_command(c["id"], result, status="done")
                        self.repo.audit(
                            "command_done",
                            {"id": c["id"], "type": c["command_type"], "result": result},
                        )
                    except Exception as e:
                        self.repo.complete_command(c["id"], {"error": str(e)}, status="error")
                        self.repo.audit(
                            "command_error",
                            {"id": c["id"], "type": c["command_type"], "error": str(e)},
                        )
                    finally:
                        self._heavy_busy = False

                import threading

                threading.Thread(target=_worker, name=f"cmd-{ctype}", daemon=True).start()
                continue
            try:
                result = self._dispatch(ctype, cmd.get("payload") or {})
                self.repo.complete_command(cmd["id"], result, status="done")
                self.repo.audit(
                    "command_done",
                    {"id": cmd["id"], "type": ctype, "result": result},
                )
            except Exception as e:
                self.repo.complete_command(cmd["id"], {"error": str(e)}, status="error")
                self.repo.audit(
                    "command_error",
                    {"id": cmd["id"], "type": ctype, "error": str(e)},
                )

    def _dispatch(self, command_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        if command_type == "halt":
            return self._halt(payload)
        if command_type == "resume":
            return self._resume()
        if command_type == "set_mode":
            return self._set_mode(payload.get("mode", "autonomous"))
        if command_type == "liquidate":
            return self._liquidate(payload)
        if command_type == "cancel_auto_exit":
            return self._cancel_auto_exit(payload)
        if command_type == "force_rescan":
            self.request_rescan()
            return {"ok": True, "queued": "force_rescan"}
        if command_type == "inject":
            return self._inject(payload)
        if command_type == "auto_enter":
            cand = payload.get("candidate") or {}
            result = self._maybe_enter(cand, self.get_tech_cfg())
            self.repo.audit("auto_entry_result", result)
            return result
        if command_type == "approve":
            return self._approve(payload)
        if command_type == "reject":
            return self._reject(payload)
        if command_type == "set_flags":
            return self._set_flags(payload)
        return {"ok": False, "error": f"unknown_command:{command_type}"}

    def _halt(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        variant:
          flatten (default) — cancel pending, stop scanner, flatten all
          soft — cancel pending, stop scanner, keep positions
          entries_only — halt new entries only (scanner off for entries)
        """
        variant = payload.get("variant") or "flatten"
        reason = payload.get("reason") or f"ui_halt:{variant}"
        dry = bool(self.repo.get_system_state().get("dry_run"))

        self.repo.update_system_state(
            mode="halted",
            halt_reason=reason,
            scanner_enabled=0 if variant != "entries_only" else 0,
            auto_entry_enabled=0,
        )
        self.repo.audit("halt", {"variant": variant, "reason": reason, "dry_run": dry})

        cancelled = []
        flattened = []

        if variant in {"flatten", "soft"}:
            cancelled = self._cancel_pending_orders(dry=dry)

        if variant == "flatten":
            flattened = self._flatten_all(dry=dry, exit_reason="halt_flatten")

        if variant == "entries_only":
            self.repo.update_system_state(scanner_enabled=1, auto_entry_enabled=0)

        return {
            "ok": True,
            "variant": variant,
            "cancelled": cancelled,
            "flattened": flattened,
            "dry_run": dry,
        }

    def _resume(self) -> dict[str, Any]:
        self.repo.update_system_state(
            mode="autonomous",
            halt_reason=None,
            scanner_enabled=1,
            auto_entry_enabled=1,
            auto_exit_enabled=1,
        )
        self.repo.audit("resume", {})
        return {"ok": True, "mode": "autonomous"}

    def _set_mode(self, mode: str) -> dict[str, Any]:
        if mode not in {"autonomous", "copilot", "halted"}:
            return {"ok": False, "error": "invalid_mode"}
        fields: dict[str, Any] = {"mode": mode}
        if mode == "halted":
            fields["auto_entry_enabled"] = 0
        self.repo.update_system_state(**fields)
        self.repo.audit("set_mode", {"mode": mode})
        return {"ok": True, "mode": mode}

    def _set_flags(self, payload: dict[str, Any]) -> dict[str, Any]:
        allowed = {
            "scanner_enabled",
            "auto_entry_enabled",
            "auto_exit_enabled",
            "same_day_symbol_block",
            "dry_run",
        }
        fields = {k: (1 if v else 0) for k, v in payload.items() if k in allowed}
        if fields:
            self.repo.update_system_state(**fields)
            self.repo.audit("set_flags", fields)
        return {"ok": True, "fields": fields}

    def _cancel_pending_orders(self, *, dry: bool) -> list[str]:
        ok, _ = mcp_available()
        if not ok or dry:
            return ["skipped:dry_or_no_mcp"] if dry or not ok else []
        ss = self.repo.get_system_state()
        acct = ss.get("account_number")
        if not acct:
            return []
        cancelled = []
        try:
            ta = get_trade_actions()
            for o in ta.get_open_orders(acct):
                oid = o.get("id")
                if oid and ta.cancel_order(acct, oid):
                    cancelled.append(oid)
            # Also cancel broker SLs on positions
            for p in self.repo.list_positions():
                sl = p.get("sl_order_id")
                if sl:
                    try:
                        ta.cancel_order(acct, sl)
                        cancelled.append(sl)
                    except Exception:
                        pass
        except Exception as e:
            cancelled.append(f"error:{e}")
        return cancelled

    def _flatten_all(self, *, dry: bool, exit_reason: str) -> list[str]:
        results = []
        for p in list(self.repo.list_positions()):
            results.append(
                self._exit_equity(p, dry=dry, exit_reason=exit_reason)
            )
        for p in list(self.repo.list_option_positions()):
            results.append(
                self._exit_option(p, dry=dry, exit_reason=exit_reason)
            )
        return results

    def _liquidate(self, payload: dict[str, Any]) -> dict[str, Any]:
        dry = bool(self.repo.get_system_state().get("dry_run"))
        symbol = payload.get("symbol")
        option_id = payload.get("option_id")
        exit_reason = payload.get("exit_reason") or "manual"
        alert = payload.get("alert")
        if option_id:
            pos = next(
                (p for p in self.repo.list_option_positions() if p["option_id"] == option_id),
                None,
            )
            if not pos:
                # Already cleared by reconcile — treat as success
                return {"ok": True, "result": f"already_flat:{option_id}"}
            return {
                "ok": True,
                "result": self._exit_option(
                    pos, dry=dry, exit_reason=exit_reason, alert=alert
                ),
            }
        if symbol:
            pos = next(
                (p for p in self.repo.list_positions() if p["symbol"] == symbol),
                None,
            )
            if not pos:
                return {"ok": True, "result": f"already_flat:{symbol}"}
            return {
                "ok": True,
                "result": self._exit_equity(
                    pos, dry=dry, exit_reason=exit_reason, alert=alert
                ),
            }
        return {"ok": False, "error": "need_symbol_or_option_id"}

    def _cancel_auto_exit(self, payload: dict[str, Any]) -> dict[str, Any]:
        symbol = payload.get("symbol")
        option_id = payload.get("option_id")
        enabled = bool(payload.get("enabled", False))
        self.repo.set_position_auto_exit(symbol=symbol, option_id=option_id, enabled=enabled)
        self.repo.audit(
            "cancel_auto_exit" if not enabled else "enable_auto_exit",
            {"symbol": symbol, "option_id": option_id},
        )
        return {"ok": True}

    def _exit_equity(
        self,
        pos: dict[str, Any],
        *,
        dry: bool,
        exit_reason: str,
        alert: str | None = None,
    ) -> str:
        sym = pos["symbol"]
        if dry:
            self.repo.audit("dry_exit_equity", {"symbol": sym, "exit_reason": exit_reason})
            self.repo.delete_position(sym)
            self._strip_live_symbol(sym)
            return f"dry:{sym}"
        ok, detail = mcp_available()
        if not ok:
            self.repo.set_position_pending(sym, False)
            return f"no_mcp:{sym}:{detail}"
        state = self.repo.positions_as_state()
        # Include this position even if pending so MCP can sell it
        if not any(p.get("symbol") == sym for p in state.get("positions", [])):
            state.setdefault("positions", []).append(
                {
                    "symbol": sym,
                    "qty": pos["qty"],
                    "entry": pos["entry"],
                    "tp": pos["tp"],
                    "sl": pos["sl"],
                    "fractional": bool(pos.get("fractional")),
                    "sl_order_id": pos.get("sl_order_id"),
                }
            )
        sync_state_file(state, SESSION_STATE_JSON)
        if not alert:
            last = float(
                (self.live_cache.get("quotes") or {}).get(sym, {}).get("last_trade_price")
                or pos.get("sl")
                or pos["entry"]
            )
            frac = str(bool(pos.get("fractional"))).lower()
            if exit_reason in {"synth_sl", "broker_sl", "SL_HIT"}:
                alert = f"SL_HIT:{sym}:last={last:.4f}:sl={float(pos['sl']):.4f}:fractional={frac}"
            else:
                sl_oid = pos.get("sl_order_id") or ""
                alert = (
                    f"TP_HIT:{sym}:last={last:.4f}:tp={float(pos['tp']):.4f}:"
                    f"sl_order={sl_oid}:fractional={frac}"
                )
        try:
            ta = get_trade_actions()
            success = ta.execute_synthetic_exit(alert, state, SESSION_STATE_JSON)
            import json

            if SESSION_STATE_JSON.is_file():
                new_state = json.loads(SESSION_STATE_JSON.read_text())
                self.repo.replace_positions_from_state(new_state)
            if success:
                mark = float(
                    (self.live_cache.get("quotes") or {}).get(sym, {}).get("last_trade_price")
                    or pos.get("sl")
                    or pos["entry"]
                )
                qty = float(pos["qty"])
                entry = float(pos["entry"])
                pnl = (mark - entry) * qty
                self.repo.record_trade(
                    {
                        "asset_type": "equity",
                        "symbol": sym,
                        "qty": qty,
                        "entry_price": entry,
                        "exit_price": mark,
                        "pnl_usd": pnl,
                        "pnl_pct": (mark / entry - 1) * 100 if entry else 0,
                        "exit_reason": exit_reason,
                        "opened_at": pos.get("opened_at"),
                    }
                )
                self.repo.record_order(
                    {
                        "asset_type": "equity",
                        "symbol": sym,
                        "side": "sell",
                        "order_type": "market",
                        "quantity": qty,
                        "expected_mid": mark,
                        "fill_price": mark,
                        "state": "filled",
                        "exit_reason": exit_reason,
                        "filled_at": now_iso(),
                    }
                )
                self._strip_live_symbol(sym)
            else:
                # Sell failed — show position again
                self.repo.set_position_pending(sym, False)
            return f"{'ok' if success else 'fail'}:{sym}"
        except Exception as e:
            self.repo.set_position_pending(sym, False)
            return f"error:{sym}:{e}"

    def _exit_option(
        self,
        pos: dict[str, Any],
        *,
        dry: bool,
        exit_reason: str,
        alert: str | None = None,
    ) -> str:
        oid = pos["option_id"]
        label = pos.get("label") or oid[:8]
        if dry:
            self.repo.audit("dry_exit_option", {"option_id": oid, "exit_reason": exit_reason})
            self.repo.delete_option_position(oid)
            self._strip_live_option(oid)
            return f"dry:{label}"
        ok, detail = mcp_available()
        if not ok:
            self.repo.set_option_position_pending(oid, False)
            return f"no_mcp:{label}:{detail}"
        state = self.repo.positions_as_state()
        sync_state_file(state, SESSION_STATE_JSON)
        if not alert:
            alert = f"OPT_FLAT:{oid}:label={label}:mark={pos['entry']}"
        try:
            ta = get_trade_actions()
            success = ta.execute_option_exit(alert, state, SESSION_STATE_JSON)
            import json

            if SESSION_STATE_JSON.is_file():
                new_state = json.loads(SESSION_STATE_JSON.read_text())
                self.repo.replace_positions_from_state(new_state)
            if success:
                mark = float(pos["entry"])
                snaps = self.live_cache.get("snapshots") or []
                for s in snaps:
                    if s.get("option_id") == oid:
                        mark = float(s.get("last") or mark)
                        break
                qty = float(pos["qty"])
                entry = float(pos["entry"])
                mult = float(pos.get("multiplier") or 100)
                pnl = (mark - entry) * qty * mult
                self.repo.record_trade(
                    {
                        "asset_type": "option",
                        "symbol": pos.get("symbol"),
                        "option_id": oid,
                        "label": label,
                        "qty": qty,
                        "entry_price": entry,
                        "exit_price": mark,
                        "pnl_usd": pnl,
                        "pnl_pct": (mark / entry - 1) * 100 if entry else 0,
                        "exit_reason": exit_reason,
                        "opened_at": pos.get("opened_at"),
                    }
                )
                self._strip_live_option(oid)
            else:
                self.repo.set_option_position_pending(oid, False)
            return f"{'ok' if success else 'fail'}:{label}"
        except Exception as e:
            self.repo.set_option_position_pending(oid, False)
            return f"error:{label}:{e}"

    def _strip_live_symbol(self, sym: str) -> None:
        snaps = [
            s
            for s in (self.live_cache.get("snapshots") or [])
            if s.get("symbol") != sym
        ]
        self.live_cache["snapshots"] = snaps

    def _strip_live_option(self, oid: str) -> None:
        snaps = [
            s
            for s in (self.live_cache.get("snapshots") or [])
            if s.get("option_id") != oid
        ]
        self.live_cache["snapshots"] = snaps

    def _inject(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Manual injection: evaluate_equity:SYM or evaluate_options:SYM or free text."""
        raw = (payload.get("text") or "").strip()
        self.repo.audit("inject", {"text": raw})
        tech = self.get_tech_cfg()
        if raw.lower().startswith("evaluate_equity:") or raw.upper().startswith("FORCE SCAN "):
            sym = raw.split(":", 1)[-1].strip().upper().replace("FORCE SCAN ", "").split()[0]
            quotes = fetch_quotes([sym])
            held = {p["symbol"] for p in self.repo.list_positions()}
            rec = evaluate_symbol(
                sym,
                quotes.get(sym) or {},
                tech,
                held=held,
                overrides=set(),
            )
            self.repo.audit("inject_equity_eval", rec)
            ss = self.repo.get_system_state()
            if rec.get("qualified") and ss.get("mode") == "autonomous":
                ok, reason = self.risk.can_enter(tech, self.get_options_cfg())
                if ok and self.risk.symbol_allowed(sym):
                    return self._maybe_enter(rec, tech)
                if ss.get("mode") == "copilot" or not ok:
                    aid = self.repo.create_approval(
                        "equity_entry", {"candidate": rec, "block_reason": reason}, symbol=sym
                    )
                    return {"ok": True, "evaluation": rec, "approval_id": aid}
            elif rec.get("qualified") and ss.get("mode") == "copilot":
                aid = self.repo.create_approval(
                    "equity_entry", {"candidate": rec}, symbol=sym
                )
                return {"ok": True, "evaluation": rec, "approval_id": aid}
            return {"ok": True, "evaluation": rec}

        if raw.lower().startswith("evaluate_options:") or "options" in raw.lower():
            parts = raw.replace(":", " ").split()
            sym = parts[-1].upper()
            self.repo.audit(
                "inject_options_eval",
                {"symbol": sym, "note": "options chain scan is chat/MCP assisted; logged for operator"},
            )
            return {
                "ok": True,
                "message": f"Options evaluate queued for {sym} — use MCP/agent for chain selection; logged in audit",
                "symbol": sym,
            }

        # Generic: treat as equity symbol
        sym = raw.upper().split()[0]
        return self._inject({"text": f"evaluate_equity:{sym}"})

    def _maybe_enter(self, candidate: dict[str, Any], tech: dict[str, Any]) -> dict[str, Any]:
        ss = self.repo.get_system_state()
        if ss.get("dry_run"):
            self.repo.audit("dry_entry", candidate)
            return {"ok": True, "dry_run": True, "candidate": candidate}
        ok, _ = mcp_available()
        if not ok:
            return {"ok": False, "error": "mcp_unavailable", "candidate": candidate}
        state = self.repo.positions_as_state()
        sync_state_file(state, SESSION_STATE_JSON)

        def _run() -> dict[str, Any]:
            ta = get_trade_actions()
            success = ta.execute_auto_entry(candidate, state, tech, SESSION_STATE_JSON)
            import json

            if SESSION_STATE_JSON.is_file():
                self.repo.replace_positions_from_state(
                    json.loads(SESSION_STATE_JSON.read_text())
                )
            if success and self.risk.same_day_block_enabled():
                self.repo.block_symbol(candidate["symbol"], reason="auto_entry")
            self.repo.record_order(
                {
                    "asset_type": "equity",
                    "symbol": candidate["symbol"],
                    "side": "buy",
                    "order_type": "limit",
                    "quantity": 1,
                    "expected_mid": candidate.get("mid"),
                    "submitted_price": candidate.get("mid"),
                    "state": "submitted" if success else "failed",
                }
            )
            return {"ok": success, "candidate": candidate}

        try:
            import concurrent.futures

            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                fut = pool.submit(_run)
                return fut.result(timeout=60)
        except concurrent.futures.TimeoutError:
            self.repo.audit("auto_entry_timeout", {"symbol": candidate.get("symbol")})
            return {"ok": False, "error": "timeout", "candidate": candidate}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def _approve(self, payload: dict[str, Any]) -> dict[str, Any]:
        aid = int(payload["approval_id"])
        appr = self.repo.decide_approval(aid, "approved")
        if not appr:
            return {"ok": False, "error": "not_found"}
        prop = appr.get("proposal") or {}
        if appr["action_type"] == "equity_entry":
            return self._maybe_enter(prop.get("candidate") or {}, self.get_tech_cfg())
        return {"ok": True, "approval": appr}

    def _reject(self, payload: dict[str, Any]) -> dict[str, Any]:
        aid = int(payload["approval_id"])
        appr = self.repo.decide_approval(aid, "rejected")
        return {"ok": bool(appr), "approval": appr}

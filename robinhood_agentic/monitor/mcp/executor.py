"""High-level MCP trade actions for monitor auto-exit / auto-entry."""

from __future__ import annotations

import calendar
import json
import time
import uuid
from pathlib import Path
from typing import Any

from .agent_bridge import CursorAgentMcpBridge
from .auth import load_access_token, mcp_cli_authenticated
from .config import McpServerConfig, load_mcp_config
from .http_client import McpHttpClient, McpHttpError


def _tool_payload(result: dict[str, Any]) -> dict[str, Any]:
    """Extract JSON data from MCP tools/call result."""
    if "data" in result:
        return result
    content = result.get("content")
    if isinstance(content, list):
        for block in content:
            if not isinstance(block, dict):
                continue
            if block.get("type") == "text":
                text = (block.get("text") or "").strip()
                if text:
                    try:
                        return json.loads(text)
                    except json.JSONDecodeError:
                        return {"raw": text}
    return result


class McpExecutor:
    """Generic MCP tool caller — HTTP token preferred, cursor agent bridge fallback."""

    def __init__(
        self,
        server_name: str = "robinhood-trading",
        access_token: str | None = None,
        config: McpServerConfig | None = None,
        repo_root: Path | None = None,
    ) -> None:
        self.config = config or load_mcp_config(server_name)
        self.server_name = server_name
        self.repo_root = repo_root
        token = access_token or load_access_token()
        self._http: McpHttpClient | None = None
        self._bridge: CursorAgentMcpBridge | None = None
        self._mode = ""

        if token:
            self._http = McpHttpClient(self.config.url, token)
            self._mode = "http"
        elif mcp_cli_authenticated(server_name):
            self._bridge = CursorAgentMcpBridge(repo_root=repo_root, server=server_name)
            self._mode = "cursor_agent_mcp"
        else:
            raise McpHttpError(
                "No MCP auth. Run: cursor agent mcp login robinhood-trading\n"
                "Optional: save JWT to robinhood_agentic/monitor/.mcp_access_token"
            )

    @property
    def mode(self) -> str:
        return self._mode

    def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if self._http:
            return _tool_payload(self._http.call_tool(tool, arguments))
        assert self._bridge is not None
        return _tool_payload(self._bridge.call_tool(tool, arguments))


class TradeActions:
    """Robinhood equity actions built on generic McpExecutor."""

    def __init__(self, executor: McpExecutor | None = None) -> None:
        self.mcp = executor or McpExecutor()

    def get_positions(self, account_number: str) -> list[dict[str, Any]]:
        data = self.mcp.call("get_equity_positions", {"account_number": account_number})
        return list((data.get("data") or {}).get("positions") or [])

    def get_open_orders(self, account_number: str) -> list[dict[str, Any]]:
        data = self.mcp.call(
            "get_equity_orders", {"account_number": account_number, "state": "open"}
        )
        return list((data.get("data") or {}).get("orders") or [])

    def cancel_order(self, account_number: str, order_id: str) -> bool:
        data = self.mcp.call(
            "cancel_equity_order",
            {"account_number": account_number, "order_id": order_id},
        )
        return bool((data.get("data") or {}).get("accepted"))

    def review_equity_order(self, **kwargs: Any) -> dict[str, Any]:
        return self.mcp.call("review_equity_order", kwargs)

    def place_equity_order(self, **kwargs: Any) -> dict[str, Any]:
        if "ref_id" not in kwargs:
            kwargs["ref_id"] = str(uuid.uuid4()).upper()
        return self.mcp.call("place_equity_order", kwargs)

    def market_sell(
        self,
        account_number: str,
        symbol: str,
        quantity: str,
        *,
        skip_review: bool = True,
    ) -> dict[str, Any]:
        args = {
            "account_number": account_number,
            "symbol": symbol,
            "side": "sell",
            "type": "market",
            "quantity": quantity,
            "market_hours": "regular_hours",
        }
        if not skip_review:
            self.review_equity_order(**args)
        placed = self.place_equity_order(**args)
        order = (placed.get("data") or {}).get("order") or {}
        return order

    def place_stop_market(
        self,
        account_number: str,
        symbol: str,
        quantity: str,
        stop_price: str,
    ) -> dict[str, Any]:
        args = {
            "account_number": account_number,
            "symbol": symbol,
            "side": "sell",
            "type": "stop_market",
            "quantity": quantity,
            "stop_price": stop_price,
            "market_hours": "regular_hours",
        }
        placed = self.place_equity_order(**args)
        return (placed.get("data") or {}).get("order") or {}

    def wait_for_fill(
        self,
        account_number: str,
        order_id: str,
        timeout_sec: float = 15.0,
        poll_sec: float = 0.5,
    ) -> dict[str, Any] | None:
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            data = self.mcp.call("get_equity_orders", {"account_number": account_number})
            for order in (data.get("data") or {}).get("orders") or []:
                if order.get("id") == order_id:
                    state = (order.get("state") or "").lower()
                    if state == "filled":
                        return order
                    if state in {"cancelled", "rejected", "failed"}:
                        return order
            time.sleep(poll_sec)
        return None

    def remove_position_from_state(self, state_path: Path, symbol: str) -> None:
        state = json.loads(state_path.read_text())
        state["positions"] = [
            p for p in state.get("positions", []) if p.get("symbol") != symbol
        ]
        state_path.write_text(json.dumps(state, indent=2) + "\n")

    def _append_position(self, state_path: Path, pos: dict[str, Any]) -> None:
        state = json.loads(state_path.read_text())
        state.setdefault("positions", []).append(pos)
        state_path.write_text(json.dumps(state, indent=2) + "\n")

    def _update_position(
        self, state_path: Path, symbol: str, updates: dict[str, Any]
    ) -> None:
        state = json.loads(state_path.read_text())
        for p in state.get("positions", []):
            if p.get("symbol") == symbol:
                p.update(updates)
        state_path.write_text(json.dumps(state, indent=2) + "\n")

    def reconcile_state(
        self,
        state_path: Path,
        cfg: dict[str, Any],
        pending_expiry_minutes: float = 30.0,
    ) -> list[str]:
        """
        Sync session_state.json with the broker so no position is ever orphaned:
        - Pending buys: finalize on fill (real entry/qty, TP/SL), drop on cancel/reject,
          cancel if stale beyond pending_expiry_minutes.
        - Broker positions missing from state: adopt with default TP/SL (synthetic both).
        - State positions gone at broker: remove (already exited).
        Returns human-readable change messages.
        """
        msgs: list[str] = []
        state = json.loads(state_path.read_text())
        acct = state.get("account_number", "")
        if not acct:
            return msgs
        tp_pct = float(cfg.get("scalp", {}).get("profit_target_pct", 0.006))
        sl_pct = float(cfg.get("scalp", {}).get("stop_loss_pct", 0.0045))

        orders_data = self.mcp.call("get_equity_orders", {"account_number": acct})
        orders = {
            o.get("id"): o
            for o in (orders_data.get("data") or {}).get("orders") or []
        }
        live_by_sym = {
            p.get("symbol"): p
            for p in self.get_positions(acct)
            if float(p.get("quantity") or 0) > 0
        }

        kept: list[dict[str, Any]] = []
        for pos in state.get("positions", []):
            sym = pos.get("symbol")
            if pos.get("pending"):
                order = orders.get(pos.get("buy_order_id")) or {}
                ostate = (order.get("state") or "").lower()
                if ostate == "filled":
                    entry = float(order.get("average_price") or pos.get("entry") or 0)
                    qty = float(order.get("cumulative_quantity") or pos.get("qty") or 0)
                    sl_price = round(entry * (1 - sl_pct), 2)
                    pos.update(
                        {
                            "pending": False,
                            "entry": entry,
                            "qty": qty,
                            "tp": round(entry * (1 + tp_pct), 2),
                            "sl": sl_price,
                            "synthetic_tp": True,
                            "synthetic_sl": True,
                        }
                    )
                    if not pos.get("fractional") and qty == int(qty) and qty > 0:
                        try:
                            sl_order = self.place_stop_market(
                                acct, sym, f"{int(qty)}", f"{sl_price:.2f}"
                            )
                            if sl_order.get("id"):
                                pos["sl_order_id"] = sl_order["id"]
                                pos["synthetic_sl"] = False
                        except McpHttpError:
                            pass
                    kept.append(pos)
                    msgs.append(
                        f"RECONCILE: {sym} pending buy filled @ ${entry:.2f} — now tracked"
                    )
                elif ostate in {"cancelled", "rejected", "failed"}:
                    msgs.append(f"RECONCILE: {sym} pending buy {ostate} — dropped")
                else:
                    age_min = 0.0
                    try:
                        opened = time.strptime(
                            pos.get("opened_at", ""), "%Y-%m-%dT%H:%M:%SZ"
                        )
                        # opened_at is UTC; calendar.timegm avoids local-tz skew.
                        age_min = (time.time() - calendar.timegm(opened)) / 60
                    except (ValueError, OverflowError):
                        pass
                    if age_min > pending_expiry_minutes and pos.get("buy_order_id"):
                        try:
                            self.cancel_order(acct, pos["buy_order_id"])
                            msgs.append(
                                f"RECONCILE: {sym} pending buy stale "
                                f"({age_min:.0f}m) — cancelled and dropped"
                            )
                        except McpHttpError:
                            kept.append(pos)
                    else:
                        kept.append(pos)
            elif sym in live_by_sym:
                kept.append(pos)
            else:
                msgs.append(f"RECONCILE: {sym} not at broker — removed from state")

        open_sells = {
            o.get("symbol"): o
            for o in orders.values()
            if o.get("side") == "sell"
            and (o.get("state") or "").lower() in {"queued", "confirmed", "unconfirmed"}
        }
        tracked = {p.get("symbol") for p in kept}
        for sym, row in live_by_sym.items():
            if sym in tracked:
                continue
            qty = float(row.get("quantity") or 0)
            entry = float(row.get("average_buy_price") or 0)
            if qty <= 0 or entry <= 0:
                continue
            pos = {
                "symbol": sym,
                "qty": qty,
                "entry": entry,
                "tp": round(entry * (1 + tp_pct), 2),
                "sl": round(entry * (1 - sl_pct), 2),
                "synthetic_sl": True,
                "synthetic_tp": True,
                "fractional": qty != int(qty),
                "adopted": True,
                "opened_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            existing = open_sells.get(sym)
            if existing and existing.get("type") == "stop_market":
                pos["sl_order_id"] = existing.get("id")
                pos["sl"] = float(existing.get("stop_price") or pos["sl"])
                pos["synthetic_sl"] = False
            kept.append(pos)
            msgs.append(
                f"RECONCILE: adopted untracked {sym} qty={qty:g} entry=${entry:.2f}"
            )

        # Dedupe by symbol: prefer non-adopted entries with a broker SL attached.
        deduped: dict[str, dict[str, Any]] = {}
        for pos in kept:
            sym = pos.get("symbol")
            prev = deduped.get(sym)
            if prev is None:
                deduped[sym] = pos
                continue
            def rank(p: dict[str, Any]) -> tuple[int, int, int]:
                return (
                    0 if p.get("pending") else 1,
                    1 if p.get("sl_order_id") else 0,
                    0 if p.get("adopted") else 1,
                )
            if rank(pos) > rank(prev):
                deduped[sym] = pos
            msgs.append(f"RECONCILE: deduped {sym} (kept single entry)")
        deduped_list = list(deduped.values())

        if msgs or len(deduped_list) != len(state.get("positions", [])):
            state["positions"] = deduped_list
            state_path.write_text(json.dumps(state, indent=2) + "\n")
        return msgs

    def execute_synthetic_exit(
        self,
        alert: str,
        state: dict[str, Any],
        state_path: Path,
    ) -> bool:
        """
        Handle SL_HIT or TP_HIT alerts.
        Returns True if sell placed and position removed from state.
        """
        parts = alert.split(":")
        if len(parts) < 2:
            return False
        kind, sym = parts[0], parts[1]
        acct = state.get("account_number", "")
        pos = next((p for p in state.get("positions", []) if p.get("symbol") == sym), None)
        if not pos:
            return True

        qty = pos.get("qty")
        if qty is None:
            live = self.get_positions(acct)
            row = next((p for p in live if p.get("symbol") == sym), None)
            if not row:
                self.remove_position_from_state(state_path, sym)
                return True
            qty = row.get("quantity") or row.get("shares_available_for_sells")
        qty_str = f"{float(qty):.6f}".rstrip("0").rstrip(".")

        if kind == "TP_HIT":
            sl_order = ""
            for p in parts:
                if p.startswith("sl_order="):
                    sl_order = p.replace("sl_order=", "")
            if sl_order:
                try:
                    self.cancel_order(acct, sl_order)
                except McpHttpError:
                    pass

        order = self.market_sell(acct, sym, qty_str)
        oid = order.get("id")
        state_name = (order.get("state") or "").lower()
        if state_name == "filled":
            self.remove_position_from_state(state_path, sym)
            return True
        if oid:
            try:
                filled = self.wait_for_fill(acct, oid)
                if filled and (filled.get("state") or "").lower() not in {
                    "filled",
                    "partially_filled_rest_cancelled",
                }:
                    # Confirm flat via positions if fill poll failed/ambiguous.
                    live = self.get_positions(acct)
                    if any(p.get("symbol") == sym for p in live):
                        return False
            except Exception:
                live = self.get_positions(acct)
                if any(p.get("symbol") == sym for p in live):
                    return False
        self.remove_position_from_state(state_path, sym)
        return True

    def execute_auto_entry(
        self,
        candidate: dict[str, Any],
        state: dict[str, Any],
        cfg: dict[str, Any],
        state_path: Path,
    ) -> bool:
        """Place entry for top rescan candidate. Returns True if order placed."""
        sym = candidate["symbol"]
        acct = state.get("account_number", "")
        mid = float(candidate["mid"])
        overrides = set(state.get("cap_overrides", []))
        reserve = float(cfg.get("account", {}).get("reserve_usd", 50))
        target = float(cfg.get("position_sizing", {}).get("target_notional_usd", 100))
        allow_frac = bool(cfg.get("entry", {}).get("allow_fractional_live", False))

        portfolio = self.mcp.call("get_portfolio", {"account_number": acct})
        bp = float((portfolio.get("data") or {}).get("buying_power", {}).get("buying_power", 0))
        cap = min(target, max(0.0, bp - reserve))
        if cap <= 0:
            return False

        entry_price = round(mid * (1 - 0.0005), 2)
        tp_pct = float(cfg.get("scalp", {}).get("profit_target_pct", 0.006))
        sl_pct = float(cfg.get("scalp", {}).get("stop_loss_pct", 0.0045))

        if mid <= cap or sym in overrides:
            # Whole-share path when affordable
            if mid <= cap:
                buy = self.place_equity_order(
                    account_number=acct,
                    symbol=sym,
                    side="buy",
                    type="limit",
                    quantity="1",
                    limit_price=f"{entry_price:.2f}",
                    market_hours="regular_hours",
                )
                order = (buy.get("data") or {}).get("order") or {}
                oid = order.get("id")
                if not oid:
                    return False
                # Record as pending immediately — reconcile_state finalizes the
                # fill later so a slow limit fill can never orphan the position.
                self._append_position(
                    state_path,
                    {
                        "symbol": sym,
                        "qty": 1,
                        "entry": entry_price,
                        "tp": round(entry_price * (1 + tp_pct), 2),
                        "sl": round(entry_price * (1 - sl_pct), 2),
                        "synthetic_sl": True,
                        "synthetic_tp": True,
                        "fractional": False,
                        "pending": True,
                        "buy_order_id": oid,
                        "opened_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    },
                )
                filled = self.wait_for_fill(acct, oid, timeout_sec=30)
                if not filled or (filled.get("state") or "").lower() != "filled":
                    # Leave pending in state; reconcile adopts on fill or drops on cancel.
                    return True
                entry = float(filled.get("average_price") or entry_price)
                sl_price = round(entry * (1 - sl_pct), 2)
                updates: dict[str, Any] = {
                    "pending": False,
                    "entry": entry,
                    "tp": round(entry * (1 + tp_pct), 2),
                    "sl": sl_price,
                }
                try:
                    sl_order = self.place_stop_market(acct, sym, "1", f"{sl_price:.2f}")
                    if sl_order.get("id"):
                        updates["sl_order_id"] = sl_order["id"]
                        updates["synthetic_sl"] = False
                except McpHttpError:
                    pass  # keep synthetic SL; monitor covers it
                self._update_position(state_path, sym, updates)
                return True

        if allow_frac or sym in overrides:
            dollar = f"{cap:.2f}"
            buy = self.place_equity_order(
                account_number=acct,
                symbol=sym,
                side="buy",
                type="market",
                dollar_amount=dollar,
                market_hours="regular_hours",
            )
            order = (buy.get("data") or {}).get("order") or {}
            oid = order.get("id")
            if not oid:
                return False
            self._append_position(
                state_path,
                {
                    "symbol": sym,
                    "qty": 0,
                    "entry": mid,
                    "tp": round(mid * (1 + tp_pct), 2),
                    "sl": round(mid * (1 - sl_pct), 2),
                    "synthetic_sl": True,
                    "synthetic_tp": True,
                    "fractional": True,
                    "pending": True,
                    "buy_order_id": oid,
                    "opened_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                },
            )
            filled = self.wait_for_fill(acct, oid, timeout_sec=30)
            if not filled or (filled.get("state") or "").lower() != "filled":
                # Leave pending; reconcile finalizes or drops it.
                return True
            entry = float(filled.get("average_price") or mid)
            qty = float(filled.get("cumulative_quantity") or 0)
            self._update_position(
                state_path,
                sym,
                {
                    "pending": False,
                    "entry": entry,
                    "qty": qty,
                    "tp": round(entry * (1 + tp_pct), 2),
                    "sl": round(entry * (1 - sl_pct), 2),
                },
            )
            return True

        return False

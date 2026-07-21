"""Data access layer for Command Center."""

from __future__ import annotations

import json
import threading
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import sqlite3

ET = ZoneInfo("America/New_York")


def now_iso() -> str:
    return datetime.now(ET).isoformat()


def today_et() -> str:
    return datetime.now(ET).date().isoformat()


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return dict(row)


class Repository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self._lock = threading.RLock()

    def _execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        with self._lock:
            return self.conn.execute(sql, params)

    def _commit(self) -> None:
        with self._lock:
            self.conn.commit()

    # --- system state ---
    def ensure_system_state(self, defaults: dict[str, Any] | None = None) -> dict[str, Any]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM system_state WHERE id = 1").fetchone()
            if row:
                return dict(row)
            defaults = defaults or {}
            # Safer default: auto-entry off until enabled in UI (avoids blocking MCP on boot).
            self.conn.execute(
                """
                INSERT INTO system_state (
                  id, mode, scanner_enabled, auto_entry_enabled, auto_exit_enabled,
                  same_day_symbol_block, account_number, account_nickname, updated_at, engine_started_at
                ) VALUES (1, ?, 1, 0, 1, 1, ?, ?, ?, ?)
                """,
                (
                    defaults.get("mode", "autonomous"),
                    defaults.get("account_number"),
                    defaults.get("account_nickname", "Agentic"),
                    now_iso(),
                    now_iso(),
                ),
            )
            self.conn.commit()
            return dict(self.conn.execute("SELECT * FROM system_state WHERE id = 1").fetchone())

    def get_system_state(self) -> dict[str, Any]:
        return self.ensure_system_state()

    def update_system_state(self, **fields: Any) -> dict[str, Any]:
        if not fields:
            return self.get_system_state()
        with self._lock:
            fields = dict(fields)
            fields["updated_at"] = now_iso()
            cols = ", ".join(f"{k} = ?" for k in fields)
            self.conn.execute(f"UPDATE system_state SET {cols} WHERE id = 1", tuple(fields.values()))
            self.conn.commit()
            return dict(self.conn.execute("SELECT * FROM system_state WHERE id = 1").fetchone())

    def heartbeat(self, mcp_ok: bool | None = None, mcp_detail: str | None = None) -> None:
        updates: dict[str, Any] = {"engine_heartbeat_at": now_iso()}
        if mcp_ok is not None:
            updates["mcp_ok"] = 1 if mcp_ok else 0
        if mcp_detail is not None:
            updates["mcp_detail"] = mcp_detail
        self.update_system_state(**updates)

    # --- config ---
    def get_active_config(self, playbook: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT * FROM runtime_config
            WHERE playbook = ? AND active = 1
            ORDER BY version DESC LIMIT 1
            """,
            (playbook,),
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        d["config"] = json.loads(d.pop("config_json"))
        return d

    def list_configs(self, playbook: str | None = None) -> list[dict[str, Any]]:
        if playbook:
            rows = self.conn.execute(
                "SELECT id, playbook, version, source, updated_at, updated_by, active FROM runtime_config WHERE playbook = ? ORDER BY version DESC",
                (playbook,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT id, playbook, version, source, updated_at, updated_by, active FROM runtime_config ORDER BY playbook, version DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    def save_config(
        self,
        playbook: str,
        config: dict[str, Any],
        *,
        source: str = "ui",
        updated_by: str = "user",
        activate: bool = True,
    ) -> dict[str, Any]:
        row = self.conn.execute(
            "SELECT COALESCE(MAX(version), 0) AS v FROM runtime_config WHERE playbook = ?",
            (playbook,),
        ).fetchone()
        version = int(row["v"]) + 1
        if activate:
            self.conn.execute(
                "UPDATE runtime_config SET active = 0 WHERE playbook = ?", (playbook,)
            )
        self.conn.execute(
            """
            INSERT INTO runtime_config (playbook, version, config_json, source, updated_at, updated_by, active)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                playbook,
                version,
                json.dumps(config),
                source,
                now_iso(),
                updated_by,
                1 if activate else 0,
            ),
        )
        self.conn.commit()
        return self.get_active_config(playbook)  # type: ignore[return-value]

    # --- positions ---
    def upsert_option_position(self, pos: dict[str, Any]) -> None:
        with self._lock:
            self.conn.execute(
                """
                INSERT INTO option_positions (
                  option_id, symbol, label, option_type, strike, expiration, qty, entry, tp, sl,
                  multiplier, synthetic_tp, synthetic_sl, auto_exit, buy_order_id, opened_at,
                  adopted, expected_mid, meta_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(option_id) DO UPDATE SET
                  symbol=excluded.symbol, label=excluded.label, qty=excluded.qty,
                  entry=excluded.entry, tp=excluded.tp, sl=excluded.sl,
                  auto_exit=excluded.auto_exit, buy_order_id=excluded.buy_order_id,
                  opened_at=excluded.opened_at, adopted=excluded.adopted,
                  expected_mid=excluded.expected_mid, meta_json=excluded.meta_json
                """,
                (
                    pos["option_id"],
                    pos.get("symbol", ""),
                    pos.get("label"),
                    pos.get("option_type"),
                    pos.get("strike"),
                    pos.get("expiration"),
                    float(pos.get("qty", 1)),
                    float(pos["entry"]),
                    float(pos["tp"]),
                    float(pos["sl"]),
                    float(pos.get("multiplier") or 100),
                    1 if pos.get("synthetic_tp", True) else 0,
                    1 if pos.get("synthetic_sl", True) else 0,
                    1 if pos.get("auto_exit", True) else 0,
                    pos.get("buy_order_id"),
                    pos.get("opened_at"),
                    1 if pos.get("adopted") else 0,
                    pos.get("expected_mid"),
                    json.dumps(pos.get("meta") or {}),
                ),
            )
            self.conn.commit()

    def set_position_pending(self, symbol: str, pending: bool = True) -> None:
        with self._lock:
            self.conn.execute(
                "UPDATE positions SET pending = ? WHERE symbol = ?",
                (1 if pending else 0, symbol),
            )
            self.conn.commit()

    def set_option_position_pending(self, option_id: str, pending: bool = True) -> None:
        with self._lock:
            self.conn.execute(
                "UPDATE option_positions SET pending = ? WHERE option_id = ?",
                (1 if pending else 0, option_id),
            )
            self.conn.commit()

    def set_position_auto_exit(self, symbol: str | None = None, option_id: str | None = None, enabled: bool = False) -> None:
        with self._lock:
            if symbol:
                self.conn.execute(
                    "UPDATE positions SET auto_exit = ? WHERE symbol = ?",
                    (1 if enabled else 0, symbol),
                )
            if option_id:
                self.conn.execute(
                    "UPDATE option_positions SET auto_exit = ? WHERE option_id = ?",
                    (1 if enabled else 0, option_id),
                )
            self.conn.commit()

    def delete_position(self, symbol: str) -> None:
        with self._lock:
            self.conn.execute("DELETE FROM positions WHERE symbol = ?", (symbol,))
            self.conn.commit()

    def delete_option_position(self, option_id: str) -> None:
        with self._lock:
            self.conn.execute("DELETE FROM option_positions WHERE option_id = ?", (option_id,))
            self.conn.commit()

    def list_positions(self) -> list[dict[str, Any]]:
        with self._lock:
            return [
                dict(r)
                for r in self.conn.execute("SELECT * FROM positions ORDER BY symbol").fetchall()
            ]

    def list_option_positions(self) -> list[dict[str, Any]]:
        with self._lock:
            return [
                dict(r)
                for r in self.conn.execute(
                    "SELECT * FROM option_positions ORDER BY symbol"
                ).fetchall()
            ]

    def upsert_position(self, pos: dict[str, Any]) -> None:
        with self._lock:
            self.conn.execute(
                """
                INSERT INTO positions (
                  symbol, qty, entry, tp, sl, synthetic_tp, synthetic_sl, fractional, pending,
                  auto_exit, buy_order_id, sl_order_id, opened_at, adopted, expected_mid, meta_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                  qty=excluded.qty, entry=excluded.entry, tp=excluded.tp, sl=excluded.sl,
                  synthetic_tp=excluded.synthetic_tp, synthetic_sl=excluded.synthetic_sl,
                  fractional=excluded.fractional, pending=excluded.pending,
                  auto_exit=excluded.auto_exit, buy_order_id=excluded.buy_order_id,
                  sl_order_id=excluded.sl_order_id, opened_at=excluded.opened_at,
                  adopted=excluded.adopted, expected_mid=excluded.expected_mid, meta_json=excluded.meta_json
                """,
                (
                    pos["symbol"],
                    float(pos.get("qty", 1)),
                    float(pos["entry"]),
                    float(pos["tp"]),
                    float(pos["sl"]),
                    1 if pos.get("synthetic_tp", True) else 0,
                    1 if pos.get("synthetic_sl") else 0,
                    1 if pos.get("fractional") else 0,
                    1 if pos.get("pending") else 0,
                    1 if pos.get("auto_exit", True) else 0,
                    pos.get("buy_order_id"),
                    pos.get("sl_order_id"),
                    pos.get("opened_at"),
                    1 if pos.get("adopted") else 0,
                    pos.get("expected_mid"),
                    json.dumps(pos.get("meta") or {}),
                ),
            )
            self.conn.commit()

    def replace_positions_from_state(self, state: dict[str, Any]) -> None:
        with self._lock:
            self.conn.execute("DELETE FROM positions")
            self.conn.execute("DELETE FROM option_positions")
            self.conn.commit()
        for p in state.get("positions", []):
            self.upsert_position(p)
        for p in state.get("option_positions", []):
            self.upsert_option_position(p)

    def audit(self, event_type: str, payload: dict[str, Any] | None = None) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT INTO audit_log (ts, event_type, payload_json) VALUES (?, ?, ?)",
                (now_iso(), event_type, json.dumps(payload or {})),
            )
            self.conn.commit()

    def record_trade(self, trade: dict[str, Any]) -> int:
        with self._lock:
            cur = self.conn.execute(
                """
                INSERT INTO trades (
                  asset_type, symbol, option_id, label, qty, entry_price, exit_price,
                  pnl_usd, pnl_pct, r_multiple, exit_reason, opened_at, closed_at,
                  entry_expected_mid, exit_fill_latency_ms, meta_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade["asset_type"],
                    trade.get("symbol"),
                    trade.get("option_id"),
                    trade.get("label"),
                    trade["qty"],
                    trade["entry_price"],
                    trade["exit_price"],
                    trade["pnl_usd"],
                    trade.get("pnl_pct"),
                    trade.get("r_multiple"),
                    trade["exit_reason"],
                    trade.get("opened_at"),
                    trade.get("closed_at") or now_iso(),
                    trade.get("entry_expected_mid"),
                    trade.get("exit_fill_latency_ms"),
                    json.dumps(trade.get("meta") or {}),
                ),
            )
            self.conn.commit()
            trade_id = int(cur.lastrowid)
        self._bump_daily_stats(trade)
        return trade_id

    def _bump_daily_stats(self, trade: dict[str, Any]) -> None:
        d = today_et()
        with self._lock:
            self.conn.execute(
                """
                INSERT INTO daily_stats (trade_date, realized_pnl, trade_count, equity_sl_hits, option_losses, wins, losses, updated_at)
                VALUES (?, 0, 0, 0, 0, 0, 0, ?)
                ON CONFLICT(trade_date) DO NOTHING
                """,
                (d, now_iso()),
            )
            pnl = float(trade["pnl_usd"])
            sl_inc = 1 if trade.get("exit_reason") in {"broker_sl", "synth_sl", "OPT_SL_HIT", "SL_HIT"} else 0
            opt_loss = 1 if trade.get("asset_type") == "option" and pnl < 0 else 0
            win = 1 if pnl > 0 else 0
            loss = 1 if pnl < 0 else 0
            self.conn.execute(
                """
                UPDATE daily_stats SET
                  realized_pnl = realized_pnl + ?,
                  trade_count = trade_count + 1,
                  equity_sl_hits = equity_sl_hits + ?,
                  option_losses = option_losses + ?,
                  wins = wins + ?,
                  losses = losses + ?,
                  updated_at = ?
                WHERE trade_date = ?
                """,
                (pnl, sl_inc, opt_loss, win, loss, now_iso(), d),
            )
            self.conn.commit()

    def positions_as_state(self) -> dict[str, Any]:
        """Shape compatible with monitor TradeActions / session_state.json."""
        ss = self.get_system_state()
        positions = []
        for p in self.list_positions():
            positions.append(
                {
                    "symbol": p["symbol"],
                    "qty": p["qty"],
                    "entry": p["entry"],
                    "tp": p["tp"],
                    "sl": p["sl"],
                    "synthetic_tp": bool(p["synthetic_tp"]),
                    "synthetic_sl": bool(p["synthetic_sl"]),
                    "fractional": bool(p["fractional"]),
                    "pending": bool(p["pending"]),
                    "auto_exit": bool(p["auto_exit"]),
                    "buy_order_id": p.get("buy_order_id"),
                    "sl_order_id": p.get("sl_order_id"),
                    "opened_at": p.get("opened_at"),
                    "adopted": bool(p.get("adopted")),
                }
            )
        option_positions = []
        for p in self.list_option_positions():
            option_positions.append(
                {
                    "option_id": p["option_id"],
                    "symbol": p["symbol"],
                    "label": p.get("label"),
                    "option_type": p.get("option_type"),
                    "strike": p.get("strike"),
                    "expiration": p.get("expiration"),
                    "qty": p["qty"],
                    "entry": p["entry"],
                    "tp": p["tp"],
                    "sl": p["sl"],
                    "multiplier": p["multiplier"],
                    "synthetic_tp": bool(p["synthetic_tp"]),
                    "synthetic_sl": bool(p["synthetic_sl"]),
                    "auto_exit": bool(p["auto_exit"]),
                    "buy_order_id": p.get("buy_order_id"),
                    "opened_at": p.get("opened_at"),
                    "adopted": bool(p.get("adopted")),
                }
            )
        return {
            "account_number": ss.get("account_number"),
            "account_nickname": ss.get("account_nickname") or "Agentic",
            "playbook": "tech_scalper",
            "cap_overrides": [],
            "positions": positions,
            "option_positions": option_positions,
        }

    # --- audit / scan ---
    def list_audit(self, limit: int = 100) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM audit_log ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["payload"] = json.loads(d.pop("payload_json") or "{}")
            out.append(d)
        return out

    def save_scan_snapshot(self, rows: list[dict[str, Any]], trigger: str = "scheduled") -> None:
        self.conn.execute(
            "INSERT INTO scan_snapshots (ts, trigger, rows_json) VALUES (?, ?, ?)",
            (now_iso(), trigger, json.dumps(rows)),
        )
        self.conn.commit()

    def latest_scan(self) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM scan_snapshots ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if not row:
            return None
        return self._scan_row(row)

    def get_scan(self, scan_id: int) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM scan_snapshots WHERE id = ?", (scan_id,)
        ).fetchone()
        if not row:
            return None
        return self._scan_row(row)

    def list_scans(self, limit: int = 30) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM scan_snapshots ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        out = []
        for r in rows:
            d = self._scan_row(r)
            rows_data = d.get("rows") or []
            qualified = [x for x in rows_data if x.get("qualified")]
            rejects: dict[str, int] = {}
            for x in rows_data:
                if not x.get("qualified"):
                    reason = x.get("reject_reason") or "unknown"
                    rejects[reason] = rejects.get(reason, 0) + 1
            out.append(
                {
                    "id": d["id"],
                    "ts": d["ts"],
                    "trigger": d["trigger"],
                    "evaluated": len(rows_data),
                    "qualified_count": len(qualified),
                    "top": [x["symbol"] for x in qualified[:5]],
                    "reject_counts": rejects,
                }
            )
        return out

    def _scan_row(self, row: sqlite3.Row) -> dict[str, Any]:
        d = dict(row)
        d["rows"] = json.loads(d.pop("rows_json") or "[]")
        return d

    # --- commands ---
    def enqueue_command(self, command_type: str, payload: dict[str, Any] | None = None) -> int:
        cur = self.conn.execute(
            """
            INSERT INTO command_queue (created_at, command_type, payload_json, status)
            VALUES (?, ?, ?, 'pending')
            """,
            (now_iso(), command_type, json.dumps(payload or {})),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def drain_commands(self, limit: int = 20) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT * FROM command_queue WHERE status = 'pending'
            ORDER BY id ASC LIMIT ?
            """,
            (limit,),
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["payload"] = json.loads(d.pop("payload_json") or "{}")
            out.append(d)
        return out

    def complete_command(self, cmd_id: int, result: dict[str, Any] | None = None, status: str = "done") -> None:
        self.conn.execute(
            """
            UPDATE command_queue SET status = ?, result_json = ?, processed_at = ?
            WHERE id = ?
            """,
            (status, json.dumps(result or {}), now_iso(), cmd_id),
        )
        self.conn.commit()

    # --- approvals ---
    def create_approval(self, action_type: str, proposal: dict[str, Any], symbol: str | None = None, option_id: str | None = None) -> int:
        cur = self.conn.execute(
            """
            INSERT INTO pending_approvals (created_at, action_type, symbol, option_id, proposal_json, status)
            VALUES (?, ?, ?, ?, ?, 'pending')
            """,
            (now_iso(), action_type, symbol, option_id, json.dumps(proposal)),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def list_approvals(self, status: str = "pending") -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM pending_approvals WHERE status = ? ORDER BY id DESC",
            (status,),
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["proposal"] = json.loads(d.pop("proposal_json") or "{}")
            out.append(d)
        return out

    def decide_approval(self, approval_id: int, status: str, decided_by: str = "user") -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM pending_approvals WHERE id = ?", (approval_id,)
        ).fetchone()
        if not row:
            return None
        self.conn.execute(
            """
            UPDATE pending_approvals SET status = ?, decided_at = ?, decided_by = ?
            WHERE id = ?
            """,
            (status, now_iso(), decided_by, approval_id),
        )
        self.conn.commit()
        d = dict(row)
        d["proposal"] = json.loads(d.pop("proposal_json") or "{}")
        d["status"] = status
        return d

    # --- entry blocks ---
    def is_symbol_blocked(self, symbol: str, block_date: str | None = None) -> bool:
        d = block_date or today_et()
        row = self.conn.execute(
            "SELECT 1 FROM entry_blocks WHERE symbol = ? AND block_date = ?",
            (symbol, d),
        ).fetchone()
        return row is not None

    def block_symbol(self, symbol: str, reason: str = "auto_entry") -> None:
        self.conn.execute(
            """
            INSERT OR IGNORE INTO entry_blocks (symbol, block_date, reason)
            VALUES (?, ?, ?)
            """,
            (symbol, today_et(), reason),
        )
        self.conn.commit()

    def clear_entry_blocks(self, block_date: str | None = None) -> int:
        if block_date:
            cur = self.conn.execute("DELETE FROM entry_blocks WHERE block_date = ?", (block_date,))
        else:
            cur = self.conn.execute("DELETE FROM entry_blocks")
        self.conn.commit()
        return cur.rowcount

    # --- orders / trades / stats ---
    def record_order(self, order: dict[str, Any]) -> int:
        cur = self.conn.execute(
            """
            INSERT INTO orders (
              broker_order_id, asset_type, symbol, option_id, side, order_type, quantity,
              expected_mid, submitted_price, fill_price, slippage_bps, state, exit_reason,
              created_at, filled_at, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                order.get("broker_order_id"),
                order["asset_type"],
                order.get("symbol"),
                order.get("option_id"),
                order["side"],
                order.get("order_type"),
                order.get("quantity"),
                order.get("expected_mid"),
                order.get("submitted_price"),
                order.get("fill_price"),
                order.get("slippage_bps"),
                order.get("state", "pending"),
                order.get("exit_reason"),
                order.get("created_at") or now_iso(),
                order.get("filled_at"),
                json.dumps(order.get("meta") or {}),
            ),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def get_daily_stats(self, trade_date: str | None = None) -> dict[str, Any]:
        d = trade_date or today_et()
        row = self.conn.execute(
            "SELECT * FROM daily_stats WHERE trade_date = ?", (d,)
        ).fetchone()
        if row:
            return dict(row)
        return {
            "trade_date": d,
            "realized_pnl": 0.0,
            "trade_count": 0,
            "equity_sl_hits": 0,
            "option_losses": 0,
            "wins": 0,
            "losses": 0,
        }

    def list_trades(self, limit: int = 500) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def list_orders(self, limit: int = 500) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM orders ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

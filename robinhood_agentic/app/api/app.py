"""FastAPI application factory."""

from __future__ import annotations

import asyncio
import csv
import io
import json
from contextlib import asynccontextmanager
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any, AsyncIterator
from zoneinfo import ZoneInfo

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from ..db import seed_database
from ..db.repository import Repository
from ..engine import LiveCache, TradingEngine
from ..paths import (
    DB_PATH,
    OPTIONS_YAML,
    SESSION_STATE_JSON,
    TECH_YAML,
    UI_DIST,
)
from .auth import require_auth

ET = ZoneInfo("America/New_York")


def _scan_summary(scan: dict[str, Any] | None) -> dict[str, Any] | None:
    if not scan:
        return None
    rows = scan.get("rows") or []
    qualified = [r for r in rows if r.get("qualified")]
    rejects: dict[str, int] = {}
    for r in rows:
        if not r.get("qualified"):
            reason = r.get("reject_reason") or "unknown"
            rejects[reason] = rejects.get(reason, 0) + 1
    return {
        "id": scan.get("id"),
        "ts": scan.get("ts"),
        "trigger": scan.get("trigger"),
        "evaluated": len(rows),
        "qualified_count": len(qualified),
        "top": [
            {
                "symbol": r["symbol"],
                "mid": r.get("mid"),
                "day": r.get("day"),
                "spread_pct": r.get("spread_pct"),
                "rank": r.get("rank"),
            }
            for r in qualified[:5]
        ],
        "reject_counts": rejects,
    }


def _latest_entry_decision(repo: Repository) -> dict[str, Any] | None:
    for ev in repo.list_audit(40):
        if ev.get("event_type") == "entry_decision":
            return ev.get("payload")
        # Backfill older runs that only logged auto_entry_blocked
        if ev.get("event_type") == "auto_entry_blocked":
            reason = (ev.get("payload") or {}).get("reason") or "blocked"
            human = {
                "outside_entry_window": (
                    "Top pick qualified, but outside the entry window "
                    "(09:45–15:30 ET) — no new entries."
                ),
            }.get(reason, f"Entry blocked: {reason}.")
            return {
                "action": "blocked",
                "code": reason,
                "message": human,
                "dry_run": bool(repo.get_system_state().get("dry_run")),
            }
    return None


def _parse_hm(value: str, default: str) -> time:
    raw = value or default
    return datetime.strptime(raw, "%H:%M").time()


def _trading_window(tech_cfg: dict[str, Any] | None) -> dict[str, Any]:
    scalp = (tech_cfg or {}).get("scalp", {})
    start_s = scalp.get("no_new_entry_before_et", "09:45")
    end_s = scalp.get("no_new_entry_after_et", "15:30")
    flat_s = scalp.get("hard_flat_time_et", "15:55")
    now = datetime.now(ET)
    start_t = _parse_hm(start_s, "09:45")
    end_t = _parse_hm(end_s, "15:30")
    flat_t = _parse_hm(flat_s, "15:55")
    t = now.time()
    in_window = start_t <= t <= end_t
    if t < start_t:
        phase = "pre"
        message = f"Pre-market — new entries begin at {start_s} ET."
    elif in_window:
        phase = "open"
        message = f"Trading window open — new entries permitted until {end_s} ET."
    elif t < flat_t:
        phase = "closed"
        message = f"Entry window closed — manage open positions only (hard flat {flat_s} ET)."
    else:
        phase = "after_hours"
        message = "After hours — no new entries; session should be flat."

    start_dt = now.replace(hour=start_t.hour, minute=start_t.minute, second=0, microsecond=0)
    end_dt = now.replace(hour=end_t.hour, minute=end_t.minute, second=0, microsecond=0)
    if t > end_t:
        start_dt = start_dt + timedelta(days=1)
    secs_open = max(0, int((start_dt - now).total_seconds())) if not in_window else 0
    secs_close = max(0, int((end_dt - now).total_seconds())) if in_window else 0

    return {
        "in_window": in_window,
        "phase": phase,
        "start_et": start_s,
        "end_et": end_s,
        "hard_flat_et": flat_s,
        "now_et": now.strftime("%H:%M:%S"),
        "message": message,
        "seconds_until_open": secs_open,
        "seconds_until_close": secs_close,
    }


def _desk_state(ss: dict[str, Any], window: dict[str, Any]) -> dict[str, Any]:
    """Derive a single operator-facing desk state from flags + window."""
    mode = ss.get("mode") or "autonomous"
    dry = bool(ss.get("dry_run"))
    scan = bool(ss.get("scanner_enabled"))
    auto = bool(ss.get("auto_entry_enabled"))
    in_win = bool(window.get("in_window"))

    if mode == "halted":
        code, label, tone = "halted", "HALTED", "danger"
        detail = "No new entries. Exits and liquidation remain available."
    elif not in_win and not scan:
        code, label, tone = "off_hours_idle", "OFF HOURS · IDLE", "muted"
        detail = "Outside the trading window. Scanner paused — safe idle."
    elif not in_win and scan and dry:
        code, label, tone = "off_hours_review", "OFF HOURS · REVIEW", "warn"
        detail = "Outside the trading window. Scanner may run; no live entries."
    elif not in_win:
        code, label, tone = "off_hours", "OFF HOURS", "warn"
        detail = window.get("message") or "Outside the trading window."
    elif not scan:
        code, label, tone = "scanner_paused", "SCANNER PAUSED", "warn"
        detail = "In trading window, but scanning is off — no new candidates."
    elif dry:
        code, label, tone = "dry_simulation", "DRY SIMULATION", "warn"
        detail = "Scanning and simulating entries — no broker orders will be sent."
    elif auto and mode == "autonomous":
        code, label, tone = "live_trading", "LIVE TRADING", "danger"
        detail = "Engine may place live Robinhood orders on qualifying scans."
    elif not dry:
        code, label, tone = "live_armed", "LIVE ARMED", "accent"
        detail = "Live orders allowed only via Co-Pilot approval or manual inject."
    else:
        code, label, tone = "ready", "READY", "muted"
        detail = "Standing by."

    can_live = (not dry) and in_win and scan and auto and mode == "autonomous"
    return {
        "code": code,
        "label": label,
        "tone": tone,
        "detail": detail,
        "can_place_live_orders": can_live,
        "mode": mode,
        "window_phase": window.get("phase"),
        "execution": "dry_run" if dry else "live",
        "scanner": "on" if scan else "off",
        "auto_entry": "on" if auto else "off",
    }


class HaltBody(BaseModel):
    variant: str = Field(default="flatten", description="flatten|soft|entries_only")
    reason: str | None = None


class ModeBody(BaseModel):
    mode: str  # autonomous|copilot|halted


class LiquidateBody(BaseModel):
    symbol: str | None = None
    option_id: str | None = None


class AutoExitBody(BaseModel):
    symbol: str | None = None
    option_id: str | None = None
    enabled: bool = False


class InjectBody(BaseModel):
    text: str


class FlagsBody(BaseModel):
    scanner_enabled: bool | None = None
    auto_entry_enabled: bool | None = None
    auto_exit_enabled: bool | None = None
    same_day_symbol_block: bool | None = None
    dry_run: bool | None = None


class ConfigUpdateBody(BaseModel):
    config: dict[str, Any]
    updated_by: str = "ui"


class ApprovalBody(BaseModel):
    approval_id: int


def create_app(db_path: Path | None = None) -> FastAPI:
    db_path = db_path or DB_PATH
    live = LiveCache()
    repo_holder: dict[str, Any] = {}
    engine_holder: dict[str, Any] = {}

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        repo = seed_database(db_path, TECH_YAML, OPTIONS_YAML, SESSION_STATE_JSON)
        engine = TradingEngine(repo, live)
        repo_holder["repo"] = repo
        engine_holder["engine"] = engine
        app.state.repo = repo
        app.state.engine = engine
        app.state.live = live
        # Sync DB positions → session_state for broker compat
        from ..broker import sync_state_file

        sync_state_file(repo.positions_as_state(), SESSION_STATE_JSON)
        engine.start()
        yield
        engine.stop()

    app = FastAPI(
        title="Robinhood Agentic Command Center",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    def get_repo() -> Repository:
        return app.state.repo

    # ----- Status / Command Center -----
    @app.get("/api/health")
    def health():
        return {"ok": True, "service": "command-center"}

    @app.get("/api/status")
    def status(repo: Repository = Depends(get_repo)):
        snap = live.snapshot()
        ss = repo.get_system_state()
        tech = repo.get_active_config("tech_scalper")
        opts = repo.get_active_config("options_directional")
        max_sl = 5
        max_opt = 2
        if tech:
            max_sl = int(
                tech["config"].get("position_sizing", {}).get("max_stop_losses_per_day", 5)
            )
        if opts:
            max_opt = int(
                opts["config"].get("risk", {}).get("circuit_breaker_losses_per_day", 2)
            )
        stats = repo.get_daily_stats()
        hb = ss.get("engine_heartbeat_at")
        tech_cfg = (tech or {}).get("config") if tech else None
        window = _trading_window(tech_cfg)
        desk = _desk_state(ss, window)
        return {
            "system": ss,
            "heartbeat_at": hb,
            "mcp_ok": bool(ss.get("mcp_ok")),
            "mcp_detail": ss.get("mcp_detail"),
            "snapshots": snap.get("snapshots") or [],
            "alerts": snap.get("alerts") or [],
            "daily_stats": stats,
            "circuit_breakers": {
                "equity_sl": {"used": stats.get("equity_sl_hits", 0), "max": max_sl},
                "option_losses": {"used": stats.get("option_losses", 0), "max": max_opt},
            },
            "approvals": repo.list_approvals("pending"),
            "updated_at": snap.get("updated_at"),
            "scan_schedule": snap.get("scan_schedule"),
            "last_scan_summary": _scan_summary(snap.get("last_scan")),
            "entry_decision": snap.get("entry_decision")
            or _latest_entry_decision(repo),
            "trading_window": window,
            "desk": desk,
            "flags": {
                "dry_run": bool(ss.get("dry_run")),
                "auto_entry_enabled": bool(ss.get("auto_entry_enabled")),
                "auto_exit_enabled": bool(ss.get("auto_exit_enabled")),
                "scanner_enabled": bool(ss.get("scanner_enabled")),
                "same_day_symbol_block": bool(ss.get("same_day_symbol_block")),
            },
        }

    @app.get("/api/stream")
    async def stream():
        async def event_gen() -> AsyncIterator[str]:
            while True:
                payload = json.dumps(live.snapshot())
                yield f"data: {payload}\n\n"
                await asyncio.sleep(2)

        return StreamingResponse(event_gen(), media_type="text/event-stream")

    # ----- Commands -----
    @app.post("/api/halt")
    def halt(body: HaltBody, repo: Repository = Depends(get_repo), _: None = Depends(require_auth)):
        # Apply mode immediately so UI reflects halt even if MCP cancel/flatten is slow
        repo.update_system_state(
            mode="halted",
            halt_reason=body.reason or f"ui_halt:{body.variant}",
            auto_entry_enabled=0,
            scanner_enabled=0 if body.variant != "entries_only" else 1,
        )
        if body.variant == "entries_only":
            repo.update_system_state(scanner_enabled=1, auto_entry_enabled=0)
        cid = repo.enqueue_command(
            "halt", {"variant": body.variant, "reason": body.reason}
        )
        repo.audit("halt_api", {"variant": body.variant, "command_id": cid})
        return {"ok": True, "command_id": cid, "variant": body.variant}

    @app.post("/api/resume")
    def resume(repo: Repository = Depends(get_repo), _: None = Depends(require_auth)):
        repo.update_system_state(
            mode="autonomous",
            halt_reason=None,
            scanner_enabled=1,
            auto_entry_enabled=0,
            auto_exit_enabled=1,
        )
        return {"ok": True, "command_id": repo.enqueue_command("resume", {})}

    @app.post("/api/mode")
    def set_mode(body: ModeBody, repo: Repository = Depends(get_repo), _: None = Depends(require_auth)):
        if body.mode not in {"autonomous", "copilot", "halted"}:
            raise HTTPException(400, "invalid mode")
        fields: dict[str, Any] = {"mode": body.mode}
        if body.mode == "halted":
            fields["auto_entry_enabled"] = 0
        repo.update_system_state(**fields)
        return {
            "ok": True,
            "command_id": repo.enqueue_command("set_mode", {"mode": body.mode}),
        }

    @app.post("/api/flags")
    def flags(body: FlagsBody, repo: Repository = Depends(get_repo), _: None = Depends(require_auth)):
        payload = {k: v for k, v in body.model_dump().items() if v is not None}
        fields = {
            k: (1 if v else 0)
            for k, v in payload.items()
            if k
            in {
                "scanner_enabled",
                "auto_entry_enabled",
                "auto_exit_enabled",
                "same_day_symbol_block",
                "dry_run",
            }
        }
        if fields:
            repo.update_system_state(**fields)
        return {"ok": True, "command_id": repo.enqueue_command("set_flags", payload), "applied": fields}

    @app.post("/api/liquidate")
    def liquidate(body: LiquidateBody, repo: Repository = Depends(get_repo), _: None = Depends(require_auth)):
        if not body.symbol and not body.option_id:
            raise HTTPException(400, "symbol or option_id required")
        return {
            "ok": True,
            "command_id": repo.enqueue_command(
                "liquidate", {"symbol": body.symbol, "option_id": body.option_id}
            ),
        }

    @app.post("/api/auto-exit")
    def auto_exit(body: AutoExitBody, repo: Repository = Depends(get_repo), _: None = Depends(require_auth)):
        repo.set_position_auto_exit(
            symbol=body.symbol, option_id=body.option_id, enabled=body.enabled
        )
        return {
            "ok": True,
            "command_id": repo.enqueue_command(
                "cancel_auto_exit",
                {
                    "symbol": body.symbol,
                    "option_id": body.option_id,
                    "enabled": body.enabled,
                },
            ),
        }

    @app.post("/api/force-rescan")
    def force_rescan(repo: Repository = Depends(get_repo), _: None = Depends(require_auth)):
        return {"ok": True, "command_id": repo.enqueue_command("force_rescan", {})}

    @app.post("/api/inject")
    def inject(body: InjectBody, repo: Repository = Depends(get_repo), _: None = Depends(require_auth)):
        return {
            "ok": True,
            "command_id": repo.enqueue_command("inject", {"text": body.text}),
        }

    @app.post("/api/approvals/approve")
    def approve(body: ApprovalBody, repo: Repository = Depends(get_repo), _: None = Depends(require_auth)):
        return {
            "ok": True,
            "command_id": repo.enqueue_command("approve", {"approval_id": body.approval_id}),
        }

    @app.post("/api/approvals/reject")
    def reject(body: ApprovalBody, repo: Repository = Depends(get_repo), _: None = Depends(require_auth)):
        return {
            "ok": True,
            "command_id": repo.enqueue_command("reject", {"approval_id": body.approval_id}),
        }

    # ----- Telemetry -----
    @app.get("/api/audit")
    def audit(limit: int = Query(100, le=500), repo: Repository = Depends(get_repo)):
        return {"events": repo.list_audit(limit)}

    @app.get("/api/scan")
    def scan(
        limit: int = Query(20, le=50),
        scan_id: int | None = Query(None),
        repo: Repository = Depends(get_repo),
    ):
        snap = live.snapshot()
        if scan_id is not None:
            detail = repo.get_scan(scan_id)
            if not detail:
                raise HTTPException(404, "scan not found")
            return {
                "scan": detail,
                "summary": _scan_summary(detail),
                "schedule": snap.get("scan_schedule"),
            }
        latest = repo.latest_scan()
        return {
            "scan": latest,
            "summary": _scan_summary(latest),
            "history": repo.list_scans(limit),
            "schedule": snap.get("scan_schedule"),
        }

    @app.get("/api/scan/{scan_id}")
    def scan_detail(scan_id: int, repo: Repository = Depends(get_repo)):
        detail = repo.get_scan(scan_id)
        if not detail:
            raise HTTPException(404, "scan not found")
        return {"scan": detail, "summary": _scan_summary(detail)}

    # ----- Config workbench -----
    @app.get("/api/config/{playbook}")
    def get_config(playbook: str, repo: Repository = Depends(get_repo)):
        cfg = repo.get_active_config(playbook)
        if not cfg:
            raise HTTPException(404, "playbook not found")
        return cfg

    @app.get("/api/configs")
    def list_configs(repo: Repository = Depends(get_repo)):
        return {"configs": repo.list_configs()}

    @app.put("/api/config/{playbook}")
    def put_config(
        playbook: str,
        body: ConfigUpdateBody,
        repo: Repository = Depends(get_repo),
        _: None = Depends(require_auth),
    ):
        if playbook not in {"tech_scalper", "options_directional"}:
            raise HTTPException(400, "unknown playbook")
        saved = repo.save_config(
            playbook, body.config, source="ui", updated_by=body.updated_by, activate=True
        )
        repo.audit("config_updated", {"playbook": playbook, "version": saved.get("version")})
        return saved

    @app.post("/api/config/{playbook}/reset")
    def reset_config(
        playbook: str,
        repo: Repository = Depends(get_repo),
        _: None = Depends(require_auth),
    ):
        from ..db.seed import _parse_options_yaml, _parse_tech_yaml

        if playbook == "tech_scalper":
            cfg = _parse_tech_yaml(TECH_YAML)
        elif playbook == "options_directional":
            cfg = _parse_options_yaml(OPTIONS_YAML)
        else:
            raise HTTPException(400, "unknown playbook")
        saved = repo.save_config(
            playbook, cfg, source="yaml_seed_reset", updated_by="ui", activate=True
        )
        return saved

    # ----- Analytics -----
    @app.get("/api/analytics/summary")
    def analytics_summary(repo: Repository = Depends(get_repo)):
        trades = repo.list_trades(500)
        orders = repo.list_orders(500)
        filled = [o for o in orders if o.get("fill_price") and o.get("expected_mid")]
        slippage = []
        for o in filled:
            exp = float(o["expected_mid"])
            fill = float(o["fill_price"])
            if exp:
                bps = (fill - exp) / exp * 10000
                if o.get("side") == "sell":
                    bps = -bps  # adverse if sell below mid
                slippage.append(
                    {
                        "symbol": o.get("symbol") or o.get("option_id"),
                        "expected_mid": exp,
                        "fill_price": fill,
                        "slippage_bps": bps,
                        "side": o.get("side"),
                        "created_at": o.get("created_at"),
                    }
                )
        by_reason: dict[str, int] = {}
        for t in trades:
            r = t.get("exit_reason") or "unknown"
            by_reason[r] = by_reason.get(r, 0) + 1

        # 15-min heatmap buckets
        heatmap: dict[str, dict[str, float]] = {}
        for t in trades:
            closed = t.get("closed_at") or ""
            try:
                # ISO → ET hour:minute bucket
                from datetime import datetime

                dt = datetime.fromisoformat(closed)
                bucket = f"{dt.hour:02d}:{(dt.minute // 15) * 15:02d}"
            except Exception:
                bucket = "unknown"
            cell = heatmap.setdefault(bucket, {"wins": 0, "losses": 0, "pnl": 0.0, "n": 0})
            cell["n"] += 1
            cell["pnl"] += float(t.get("pnl_usd") or 0)
            if float(t.get("pnl_usd") or 0) >= 0:
                cell["wins"] += 1
            else:
                cell["losses"] += 1

        med_slip = None
        if slippage:
            vals = sorted(s["slippage_bps"] for s in slippage)
            med_slip = vals[len(vals) // 2]

        return {
            "trade_count": len(trades),
            "exit_reasons": by_reason,
            "slippage_points": slippage,
            "median_slippage_bps": med_slip,
            "heatmap": heatmap,
            "daily": repo.get_daily_stats(),
        }

    @app.get("/api/analytics/export.csv")
    def export_csv(repo: Repository = Depends(get_repo)):
        trades = repo.list_trades(5000)
        buf = io.StringIO()
        fields = [
            "id",
            "asset_type",
            "symbol",
            "option_id",
            "label",
            "qty",
            "entry_price",
            "exit_price",
            "pnl_usd",
            "pnl_pct",
            "r_multiple",
            "exit_reason",
            "opened_at",
            "closed_at",
            "entry_expected_mid",
            "exit_fill_latency_ms",
        ]
        w = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for t in trades:
            w.writerow(t)
        buf.seek(0)
        return StreamingResponse(
            iter([buf.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=trades_ledger.csv"},
        )

    # ----- Static UI (Vite dist preferred; public/ SPA fallback) -----
    public_index = Path(__file__).resolve().parent.parent / "ui" / "public" / "index.html"
    if UI_DIST.is_dir() and (UI_DIST / "index.html").is_file():
        assets = UI_DIST / "assets"
        if assets.is_dir():
            app.mount("/assets", StaticFiles(directory=assets), name="assets")

        @app.get("/")
        def index():
            return FileResponse(UI_DIST / "index.html")
    elif public_index.is_file():

        @app.get("/")
        def index():
            return FileResponse(public_index)
    else:

        @app.get("/")
        def index():
            return {
                "message": "UI missing. Expected app/ui/public/index.html or app/ui/dist after npm run build."
            }

    return app

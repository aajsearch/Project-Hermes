from __future__ import annotations

import argparse
import asyncio
import os
import signal
import time
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from mm_bot.core.config import load_config
from mm_bot.core.events import FillEvent, OrderIntentEvent, RiskEvent
from mm_bot.core.jsonlog import setup_json_logging
from mm_bot.execution.base import OrderRequest
from mm_bot.execution.coinbase import CoinbaseExecution
from mm_bot.execution.mock import MockExecution
from mm_bot.market_data.coinbase_ws import CoinbaseMarketData
from mm_bot.reporting.report import Reporter, Summary
from mm_bot.risk.engine import RiskEngine
from mm_bot.storage.sqlite import SqliteStore
from mm_bot.strategy.market_maker import PositionState
from mm_bot.strategy.scalper import SimpleScalper


class GracefulShutdown:
    def __init__(self) -> None:
        self._stop = asyncio.Event()

    def trigger(self) -> None:
        self._stop.set()

    async def wait(self) -> None:
        await self._stop.wait()

    @property
    def is_set(self) -> bool:
        return self._stop.is_set()


class PnLState:
    """Shared realized PnL state (updated by reporting loop)."""

    def __init__(self) -> None:
        self.realized_pnl: float = 0.0


def client_order_id(namespace: str, product_id: str, side: str) -> str:
    # Unique id per order (requested): include timestamp in ms.
    return f"{namespace}:{product_id}:{side}:{int(time.time() * 1000)}"

def _pid_is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        # If we can't signal it, assume it's running.
        return True


def _acquire_pid_lock(lock_path: Path) -> None:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    if lock_path.exists():
        try:
            existing = int(lock_path.read_text().strip())
        except Exception:
            existing = -1
        if existing > 0 and _pid_is_running(existing):
            raise SystemExit(
                f"Bot already running (pid {existing}). Stop it first or delete {lock_path} if stale."
            )
        # stale lock
        try:
            lock_path.unlink()
        except Exception:
            pass
    lock_path.write_text(str(os.getpid()))


def _release_pid_lock(lock_path: Path) -> None:
    try:
        if lock_path.exists():
            lock_path.unlink()
    except Exception:
        pass


def _round_size_for_coinbase(product_id: str, size: float) -> float:
    """
    Coinbase rejects sizes with too many decimals. We apply a conservative per-product
    decimal cap to keep orders valid.
    - BTC pairs typically allow more precision than SOL.
    This is a pragmatic stopgap; the best version is to query product increments.
    """
    pid = str(product_id or "").upper()
    dec = 8 if pid.startswith("BTC-") else 6
    return float(round(float(size), dec))


async def execution_loop(
    *,
    cfg: dict,
    exec_gw,
    in_q: asyncio.Queue,
    product_ids: list[str],
    shutdown: GracefulShutdown,
    logger,
    store: SqliteStore,
) -> None:
    post_only = bool(cfg["execution"]["post_only"])
    namespace = str(cfg["execution"]["idempotency"]["namespace"])
    min_requote_interval = float(cfg["execution"]["min_requote_interval_seconds"])

    last_requote_ts_by_product: dict[str, float] = {pid: 0.0 for pid in product_ids}

    # Resume: list open orders per product
    for pid in product_ids:
        open_orders = await exec_gw.list_open_orders(pid)
        logger.info("resume_open_orders", extra={"extra": {"product_id": pid, "count": len(open_orders)}})

    while not shutdown.is_set:
        intent, risk_evt = await in_q.get()
        if not isinstance(intent, OrderIntentEvent) or not isinstance(risk_evt, RiskEvent):
            continue
        product_id = str(intent.product_id or "").strip()
        if not product_id:
            continue
        allow_bid = bool((risk_evt.details or {}).get("allow_bid", True))
        allow_ask = bool((risk_evt.details or {}).get("allow_ask", True))
        if not risk_evt.ok and not (allow_bid or allow_ask):
            logger.warning(
                "risk_block",
                extra={"extra": {"reason": risk_evt.reason, "details": risk_evt.details}},
            )
            continue
        if not (allow_bid and allow_ask):
            logger.warning(
                "risk_partial_block",
                extra={"extra": {"reason": risk_evt.reason, "details": risk_evt.details}},
            )

        now = time.time()
        last_requote_ts = last_requote_ts_by_product.get(product_id, 0.0)
        if now - last_requote_ts < min_requote_interval:
            continue
        last_requote_ts_by_product[product_id] = now

        # Absolute override: stop-loss market liquidation.
        if getattr(intent, "trigger_market_stop_loss", False):
            inv = float((intent.meta or {}).get("inventory_base", 0.0) or 0.0)
            if inv > 0:
                logger.warning(
                    "stop_loss_triggered",
                    extra={"extra": {"product_id": product_id, "inventory_base": inv, "mid_price": intent.mid_price}},
                )
                await exec_gw.cancel_all(product_id)
                # Market sell full inventory.
                _ = await exec_gw.place_market_order(product_id=product_id, side="SELL", base_size=inv)
            # Do not place any new limit orders until inventory is cleared.
            continue

        await exec_gw.cancel_all(product_id)

        async def _persist_open_order(order_resp, order_req) -> None:
            if order_resp.status != "OPEN" or not order_resp.order_id:
                return
            try:
                await asyncio.to_thread(
                    store.insert_order,
                    order_id=order_resp.order_id,
                    client_order_id=order_resp.client_order_id,
                    product_id=order_req.product_id,
                    side=order_req.side,
                    price=order_req.price,
                    size=order_req.size,
                    post_only=order_req.post_only,
                    status="OPEN",
                    exchange_status=None,
                    ts_ms=int(time.time() * 1000),
                )
            except Exception as e:
                logger.warning(
                    "db_insert_order_failed",
                    extra={"extra": {"order_id": order_resp.order_id, "error": str(e)}},
                )

        if shutdown.is_set:
            break

        if allow_bid and intent.bid_price and intent.bid_size:
            req = OrderRequest(
                product_id=product_id,
                side="BUY",
                price=float(intent.bid_price),
                size=_round_size_for_coinbase(product_id, float(intent.bid_size)),
                post_only=post_only,
                client_order_id=client_order_id(namespace, product_id, "BUY"),
            )
            if shutdown.is_set:
                break
            h = await exec_gw.place_post_only_limit(req)
            logger.info(
                "order_placed",
                extra={
                    "extra": {
                        "side": "BUY",
                        "order_id": h.order_id,
                        "client_order_id": h.client_order_id,
                        "status": h.status,
                        "error_message": h.error_message,
                        "price": req.price,
                        "size": req.size,
                    }
                },
            )
            # Persist asynchronously so storage can never block quoting/execution.
            asyncio.create_task(_persist_open_order(h, req))

        if allow_ask and intent.ask_price and intent.ask_size:
            req = OrderRequest(
                product_id=product_id,
                side="SELL",
                price=float(intent.ask_price),
                size=_round_size_for_coinbase(product_id, float(intent.ask_size)),
                post_only=post_only,
                client_order_id=client_order_id(namespace, product_id, "SELL"),
            )
            if shutdown.is_set:
                break
            h = await exec_gw.place_post_only_limit(req)
            logger.info(
                "order_placed",
                extra={
                    "extra": {
                        "side": "SELL",
                        "order_id": h.order_id,
                        "client_order_id": h.client_order_id,
                        "status": h.status,
                        "error_message": h.error_message,
                        "price": req.price,
                        "size": req.size,
                    }
                },
            )
            asyncio.create_task(_persist_open_order(h, req))


async def main_async(config_path: str) -> None:
    load_dotenv(dotenv_path=str(Path(__file__).resolve().parent.parent / ".env"))
    bot_cfg = load_config(config_path)
    cfg = bot_cfg.raw
    pid_lock = Path("mm_bot/data/mm_bot.pid")
    _acquire_pid_lock(pid_lock)

    logger = setup_json_logging(level=os.environ.get("MM_LOG_LEVEL", "INFO"), name="mm_bot")
    shutdown = GracefulShutdown()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown.trigger)
        except NotImplementedError:
            pass

    md_broadcast_q: asyncio.Queue = asyncio.Queue(maxsize=bot_cfg.queue_maxsize)
    fills_q: asyncio.Queue = asyncio.Queue(maxsize=bot_cfg.queue_maxsize)

    store = SqliteStore(cfg["storage"]["sqlite_path"], cfg["storage"]["schema_path"])
    store.init_schema()
    pnl_state = PnLState()

    products = list((cfg.get("strategy", {}) or {}).get("products") or [])
    if not products:
        raise SystemExit("No strategy.products found in config.yaml")
    product_ids = [str(p.get("product_id") or "").strip() for p in products if str(p.get("product_id") or "").strip()]
    if not product_ids:
        raise SystemExit("strategy.products must include at least one product_id")

    if bot_cfg.exchange_mode == "coinbase":
        exec_gw = CoinbaseExecution(
            trading_portfolio_id=cfg["exchange"]["trading_portfolio_id"],
            usd_account_uuid=cfg["exchange"]["usd_account_uuid"],
            rest_timeout_seconds=int(cfg["exchange"]["rest_timeout_seconds"]),
            rest_max_retries=int(cfg["exchange"]["rest_max_retries"]),
            rest_backoff_base_seconds=float(cfg["exchange"]["rest_backoff_base_seconds"]),
            cb_failure_threshold=int(cfg["exchange"]["circuit_breaker"]["failure_threshold"]),
            cb_cooldown_seconds=int(cfg["exchange"]["circuit_breaker"]["cooldown_seconds"]),
        )
    else:
        exec_gw = MockExecution(fills_q=fills_q)

    await exec_gw.start()

    md = CoinbaseMarketData(
        product_ids=product_ids,
        websocket_url=cfg["market_data"]["websocket_url"],
        channels=cfg["market_data"]["channels"],
        out_q=md_broadcast_q,
        vol_window_seconds=int(cfg["market_data"]["volatility"]["window_seconds"]),
        fills_q=fills_q if bot_cfg.exchange_mode == "coinbase" else None,
    )

    md_q_by_product: dict[str, asyncio.Queue] = {pid: asyncio.Queue(maxsize=bot_cfg.queue_maxsize) for pid in product_ids}

    async def md_router() -> None:
        while not shutdown.is_set:
            evt = await md_broadcast_q.get()
            try:
                pid = str(getattr(evt, "product_id", "") or "").strip()
            except Exception:
                continue
            q = md_q_by_product.get(pid)
            if q is None:
                continue
            await q.put(evt)

    # Shared positions keyed by base asset symbol (e.g., SOL, BTC)
    positions_by_asset: dict[str, PositionState] = {}

    async def reconcile_recent_fills() -> None:
        """
        Backfill recent fills from REST so DB reflects portfolio even if WS misses events.
        Idempotent due to fills.fill_id PK + store.insert_fill() returning inserted flag.
        """
        if bot_cfg.exchange_mode != "coinbase":
            return
        if not hasattr(exec_gw, "get_fills"):
            return

        def _get(obj, key: str, default=None):
            if isinstance(obj, dict):
                return obj.get(key, default)
            return getattr(obj, key, default)

        inserted_total = 0
        for pid in product_ids:
            cursor = None
            pages = 0
            while pages < 5:  # cap backfill work
                pages += 1
                resp = await exec_gw.get_fills(product_id=pid, limit=100, cursor=cursor)  # type: ignore[attr-defined]
                fills = _get(resp, "fills", None) or _get(resp, "data", None) or []
                if not isinstance(fills, list):
                    fills = []
                logger.info(
                    "fills_reconcile_page",
                    extra={
                        "extra": {
                            "product_id": pid,
                            "page": pages,
                            "cursor_in": cursor,
                            "fills_count": len(fills),
                            "has_next": _get(resp, "has_next", None),
                            "cursor_out": _get(resp, "cursor", None),
                        }
                    },
                )

                inserted_any = False
                for f in fills:
                    if isinstance(f, dict):
                        order_id = str(f.get("order_id") or f.get("orderId") or "")
                        product_id = str(f.get("product_id") or f.get("productId") or pid)
                        side = str(f.get("side") or f.get("order_side") or "").upper()
                        trade_id = str(f.get("trade_id") or f.get("tradeId") or f.get("fill_id") or f.get("fillId") or "")
                        ts_val = f.get("trade_time") or f.get("time") or f.get("ts") or f.get("timestamp")
                        price_val = f.get("price") or f.get("fill_price") or f.get("execution_price")
                        size_val = f.get("size") or f.get("qty") or f.get("base_size")
                        fee_val = f.get("fee") or f.get("commission") or f.get("fees") or f.get("total_fees") or 0
                    else:
                        order_id = str(_get(f, "order_id", "") or "")
                        product_id = str(_get(f, "product_id", pid) or pid)
                        side = str(_get(f, "side", "") or "").upper()
                        trade_id = str(_get(f, "trade_id", "") or _get(f, "fill_id", "") or "")
                        ts_val = _get(f, "trade_time", None) or _get(f, "time", None) or _get(f, "ts", None) or _get(f, "timestamp", None)
                        price_val = _get(f, "price", None) or _get(f, "fill_price", None) or _get(f, "execution_price", None)
                        size_val = _get(f, "size", None) or _get(f, "qty", None) or _get(f, "base_size", None)
                        fee_val = _get(f, "fee", None) or _get(f, "commission", None) or _get(f, "fees", None) or _get(f, "total_fees", None) or 0

                    try:
                        price = float(price_val)
                        size = float(size_val)
                        fee = float(fee_val or 0.0)
                    except (TypeError, ValueError):
                        continue
                    if not order_id or not product_id or size <= 0:
                        continue

                    # Best-effort timestamp -> ms
                    ts_ms = int(time.time() * 1000)
                    try:
                        if ts_val is not None:
                            ts_ms = int(float(ts_val) * 1000) if float(ts_val) < 1e12 else int(float(ts_val))
                    except Exception:
                        pass

                    fill_id = trade_id or f"{order_id}:{ts_ms}:{price}:{size}"
                    inserted = await asyncio.to_thread(
                        store.insert_fill,
                        fill_id=fill_id,
                        order_id=order_id,
                        product_id=product_id,
                        side=side,
                        price=price,
                        size=size,
                        fee=fee,
                        liquidity=None,
                        ts_ms=ts_ms,
                    )
                    if inserted:
                        inserted_total += 1
                        inserted_any = True
                        base_asset = product_id.split("-")[0].strip()
                        pos = positions_by_asset.get(base_asset)
                        if pos is None:
                            qty, avg_px = store.get_position(base_asset)
                            pos = PositionState(inventory_base=float(qty), avg_entry_price=float(avg_px))
                            positions_by_asset[base_asset] = pos
                        dq = float(size) if side == "BUY" else -float(size)
                        pos.inventory_base += dq
                        _, new_avg = await asyncio.to_thread(store.update_position, base_asset, dq, float(price), ts_ms=ts_ms)
                        pos.avg_entry_price = float(new_avg)

                if inserted_any:
                    logger.info("fills_reconciled", extra={"extra": {"product_id": pid, "pages": pages}})

                has_next = _get(resp, "has_next", None)
                next_cursor = _get(resp, "cursor", None)
                if has_next is False or not next_cursor or next_cursor == cursor:
                    break
                cursor = str(next_cursor)

        if inserted_total:
            logger.info("fills_reconciled_total", extra={"extra": {"inserted": inserted_total}})

    async def reconcile_loop() -> None:
        # Continuous backfill so analyze_run stays current even if WS misses some fills.
        while not shutdown.is_set:
            try:
                await reconcile_recent_fills()
            except Exception as e:
                logger.warning("fills_reconcile_failed", extra={"extra": {"error": str(e)}})
            await asyncio.sleep(15)

    # Run once before quoting, then continuously in background.
    await reconcile_recent_fills()

    # Per-product queues and tasks
    exec_pairs_q: asyncio.Queue = asyncio.Queue(maxsize=bot_cfg.queue_maxsize)  # (intent, risk_evt)
    product_tasks: list[asyncio.Task] = []

    for p in products:
        pid = str(p.get("product_id") or "").strip()
        if not pid:
            continue

        # Build per-product strategy config by merging globals + overrides
        strat_cfg_global = dict(cfg.get("strategy", {}) or {})
        strat_cfg_global.pop("products", None)
        strat_cfg = dict(strat_cfg_global)
        strat_cfg["base_order_size"] = float(p.get("base_order_size"))
        strat_cfg["half_spread_bps"] = float(p.get("half_spread_bps"))
        strat_cfg["profit_target_pct"] = float(p.get("profit_target_pct"))
        strat_cfg["stop_loss_pct"] = float(p.get("stop_loss_pct"))

        # Per-product risk config (global)
        risk_cfg = dict(cfg.get("risk", {}) or {})

        base_asset = pid.split("-")[0].strip()
        if base_asset not in positions_by_asset:
            qty, avg_px = store.get_position(base_asset)
            positions_by_asset[base_asset] = PositionState(inventory_base=float(qty), avg_entry_price=float(avg_px))

        position = positions_by_asset[base_asset]

        intent_q: asyncio.Queue = asyncio.Queue(maxsize=bot_cfg.queue_maxsize)
        risk_in_q: asyncio.Queue = asyncio.Queue(maxsize=bot_cfg.queue_maxsize)
        risk_out_q: asyncio.Queue = asyncio.Queue(maxsize=bot_cfg.queue_maxsize)

        strat = SimpleScalper(pid, strat_cfg, md_q_by_product[pid], intent_q, position)
        risk = RiskEngine(risk_cfg, risk_in_q, risk_out_q, position=position, pnl_state=pnl_state, shutdown=shutdown)

        async def _bridge_product(_pid: str, _intent_q: asyncio.Queue, _risk_in_q: asyncio.Queue, _risk_out_q: asyncio.Queue) -> None:
            while not shutdown.is_set:
                intent = await _intent_q.get()
                await _risk_in_q.put(intent)
                risk_evt = await _risk_out_q.get()
                await exec_pairs_q.put((intent, risk_evt))

        product_tasks.extend(
            [
                asyncio.create_task(strat.run(), name=f"strategy:{pid}"),
                asyncio.create_task(risk.run(), name=f"risk:{pid}"),
                asyncio.create_task(_bridge_product(pid, intent_q, risk_in_q, risk_out_q), name=f"bridge:{pid}"),
            ]
        )

    reporter = Reporter(cfg["reporting"]["csv_output_path"])
    async def fills_processing_loop() -> None:
        while not shutdown.is_set:
            fill: FillEvent = await fills_q.get()
            if not isinstance(fill, FillEvent):
                continue
            logger.info(
                "fill_received",
                extra={
                    "extra": {
                        "order_id": fill.order_id,
                        "product_id": fill.product_id,
                        "side": fill.side,
                        "price": fill.price,
                        "size": fill.size,
                        "fee": getattr(fill, "fee", 0.0),
                        "source": (fill.meta or {}).get("source"),
                    }
                },
            )

            # Persist fill
            fill_id = f"{fill.order_id}:{fill.ts_ms}"
            fee = float(getattr(fill, "fee", 0.0) or 0.0)
            inserted = await asyncio.to_thread(
                store.insert_fill,
                fill_id=fill_id,
                order_id=fill.order_id,
                product_id=fill.product_id,
                side=fill.side,
                price=fill.price,
                size=fill.size,
                fee=fee,
                liquidity="MAKER",
                ts_ms=fill.ts_ms,
            )
            if not inserted:
                continue
            await asyncio.to_thread(store.update_order_status, fill.order_id, "FILLED", ts_ms=fill.ts_ms)
            logger.info(
                "fill_persisted",
                extra={"extra": {"fill_id": fill_id, "order_id": fill.order_id, "product_id": fill.product_id}},
            )

            # Update in-memory + SQLite position
            dq = float(fill.size) if fill.side.upper() == "BUY" else -float(fill.size)
            fill_pid = str(fill.product_id or "").strip()
            base_asset = fill_pid.split("-")[0].strip() if fill_pid else ""
            pos = positions_by_asset.get(base_asset)
            if pos is None:
                qty, avg_px = store.get_position(base_asset)
                pos = PositionState(inventory_base=float(qty), avg_entry_price=float(avg_px))
                positions_by_asset[base_asset] = pos
            pos.inventory_base += dq
            _, new_avg = await asyncio.to_thread(store.update_position, base_asset, dq, float(fill.price), ts_ms=fill.ts_ms)
            pos.avg_entry_price = float(new_avg)

    async def reporting_loop() -> None:
        interval = int(cfg["reporting"]["console_summary_interval_seconds"])
        while not shutdown.is_set:
            fills = await asyncio.to_thread(store.get_fills, None)
            trade_count = len(fills)

            # Reconstruct realized PnL (avg-cost) from fills, PER PRODUCT.
            pos_qty_by_pid: dict[str, float] = {}
            avg_cost_by_pid: dict[str, float] = {}
            realized = 0.0
            fees = 0.0
            for r in fills:
                pid = str(r["product_id"])
                side = str(r["side"]).upper()
                px = float(r["price"])
                sz = float(r["size"])
                fees += float(r["fee"] or 0.0)

                pos_qty = float(pos_qty_by_pid.get(pid, 0.0) or 0.0)
                avg_cost = float(avg_cost_by_pid.get(pid, 0.0) or 0.0)

                if side == "BUY":
                    new_qty = pos_qty + sz
                    if new_qty != 0:
                        avg_cost = (avg_cost * pos_qty + px * sz) / new_qty
                    pos_qty = new_qty
                elif side == "SELL":
                    sold = min(abs(pos_qty), sz) if pos_qty != 0 else sz
                    realized += (px - avg_cost) * sold
                    pos_qty = pos_qty - sz
                    if pos_qty == 0:
                        avg_cost = 0.0

                pos_qty_by_pid[pid] = pos_qty
                avg_cost_by_pid[pid] = avg_cost

            realized_net = float(realized - fees)
            pnl_state.realized_pnl = realized_net

            # Use aggregate inventory across tracked assets for display (PnL is computed from fills anyway).
            inv = float(sum(p.inventory_base for p in positions_by_asset.values()) if positions_by_asset else 0.0)
            mid = None
            avg_entry = 0.0
            unreal = 0.0
            s = Summary(
                ts_ms=int(time.time() * 1000),
                realized_pnl=float(realized_net),
                unrealized_pnl=float(unreal),
                fees=float(fees),
                trade_count=int(trade_count),
            )
            reporter.write_summary(s)
            logger.info(
                "summary",
                extra={
                    "extra": {
                        "text": reporter.console_summary(s),
                        "inventory_base": inv,
                        "mid_price": mid,
                        "avg_entry_price": avg_entry,
                        "trade_count": trade_count,
                    }
                },
            )
            await asyncio.sleep(interval)

    tasks = [
        asyncio.create_task(md.run(), name="market_data"),
        asyncio.create_task(md_router(), name="md_router"),
        asyncio.create_task(fills_processing_loop(), name="fills_processing"),
        asyncio.create_task(reconcile_loop(), name="fills_reconcile_loop"),
        asyncio.create_task(
            execution_loop(
                cfg=cfg,
                exec_gw=exec_gw,
                in_q=exec_pairs_q,
                product_ids=product_ids,
                shutdown=shutdown,
                logger=logger,
                store=store,
            ),
            name="execution",
        ),
        asyncio.create_task(reporting_loop(), name="reporting"),
    ]
    tasks.extend(product_tasks)

    logger.info(
        "bot_started",
        extra={"extra": {"mode": bot_cfg.exchange_mode, "products": product_ids}},
    )

    try:
        await shutdown.wait()
    finally:
        _release_pid_lock(pid_lock)

    logger.warning("shutdown_requested", extra={"extra": {"action": "cancel_all"}})
    try:
        # Mark any currently open orders as CANCELED in DB after cancel_all succeeds.
        for pid in product_ids:
            open_before = await exec_gw.list_open_orders(pid)
            await asyncio.wait_for(
                exec_gw.cancel_all(pid),
                timeout=float(cfg["runtime"]["shutdown_timeout_seconds"]),
            )
            for o in open_before:
                if getattr(o, "order_id", ""):
                    await asyncio.to_thread(
                        store.update_order_status, o.order_id, "CANCELED", ts_ms=int(time.time() * 1000)
                    )
    except Exception as e:
        logger.warning("shutdown_cancel_failed", extra={"extra": {"error": str(e)}})

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await exec_gw.close()
    store.close()
    logger.info("bot_stopped")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="mm_bot/config.yaml")
    args = p.parse_args()
    asyncio.run(main_async(args.config))


if __name__ == "__main__":
    main()


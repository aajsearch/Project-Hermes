from __future__ import annotations

import argparse
import asyncio
import logging
import json
import signal
import time
from pathlib import Path

from dotenv import load_dotenv

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import QueryOrderStatus
from alpaca.trading.requests import GetOrdersRequest

from bot.alpaca_put_spread.alpaca_clients import make_alpaca_clients
from bot.alpaca_etf_scalper.singleton_lock import acquire_singleton_lock, release_singleton_lock
from bot.alpaca_etf_scalper.config import ProductCfg, load_cfg
from bot.alpaca_etf_scalper.execution import AlpacaExecution
from bot.alpaca_etf_scalper.market_data import AlpacaQuotePoller
from bot.alpaca_etf_scalper.strategy import SimpleScalper
from mm_bot.core.events import FillEvent, OrderIntentEvent, RiskEvent
from mm_bot.core.jsonlog import setup_json_logging
from mm_bot.execution.base import OrderRequest
from mm_bot.storage.sqlite import SqliteStore
from mm_bot.strategy.market_maker import PositionState


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


def client_order_id(namespace: str, symbol: str, side: str) -> str:
    return f"{namespace}:{symbol}:{side}:{int(time.time() * 1000)}"


async def main_async(config_path: str) -> None:
    load_dotenv()
    cfg = load_cfg(config_path)

    logger = setup_json_logging(level="INFO", name="alpaca_etf_scalper")

    if not acquire_singleton_lock():
        raise SystemExit(1)

    shutdown = GracefulShutdown()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown.trigger)
        except NotImplementedError:
            pass

    # Clients
    trading_client, stock_data_client, _ = make_alpaca_clients(paper=cfg.paper)

    exec_gw = AlpacaExecution(trading_client, execute=cfg.execute)
    await exec_gw.start()

    store = SqliteStore(cfg.sqlite_path, cfg.schema_path)
    store.init_schema()

    md_q: asyncio.Queue = asyncio.Queue(maxsize=cfg.queue_maxsize)
    fills_q: asyncio.Queue = asyncio.Queue(maxsize=cfg.queue_maxsize)

    symbols = [p.symbol for p in cfg.products]
    prod_by_sym = {p.symbol: p for p in cfg.products}
    runtime_cfg = cfg.raw.get("runtime", {}) or {}
    post_sl_flat_seconds = float(runtime_cfg.get("post_stop_loss_flat_seconds", 20.0))
    sl_exit_guard_seconds = float(runtime_cfg.get("stop_loss_exit_guard_seconds", 120.0))
    md = AlpacaQuotePoller(stock_data_client, symbols=symbols, out_q=md_q, poll_seconds=cfg.poll_seconds)

    # Per-symbol queues + tasks
    md_q_by_symbol = {s: asyncio.Queue(maxsize=cfg.queue_maxsize) for s in symbols}
    intent_q_by_symbol = {s: asyncio.Queue(maxsize=cfg.queue_maxsize) for s in symbols}

    # Position state per symbol
    positions = {}
    for p in cfg.products:
        qty, avg_px = store.get_position(p.symbol)
        positions[p.symbol] = PositionState(inventory_base=float(qty), avg_entry_price=float(avg_px))

    # Shared execution / exit state (execution_loop + reconcile_loop).
    sl_exit_in_progress: dict[str, bool] = {s: False for s in symbols}
    post_sl_flat_cooldown_until: dict[str, float] = {s: 0.0 for s in symbols}
    last_submitted_sig: dict[str, tuple[str, float, float]] = {}

    async def _refresh_position_from_broker(sym: str) -> float:
        def _call():
            return trading_client.get_all_positions()

        pos_list = await asyncio.to_thread(_call)
        qty = 0.0
        avg_px = 0.0
        for ap in pos_list or []:
            if str(getattr(ap, "symbol", "") or "").upper() != sym:
                continue
            qty = float(getattr(ap, "qty", 0.0) or 0.0)
            avg_px = float(getattr(ap, "avg_entry_price", 0.0) or 0.0)
            break
        st = positions.get(sym)
        if st is not None:
            st.inventory_base = qty
            st.avg_entry_price = avg_px
            await asyncio.to_thread(store.update_position, sym, 0.0, float(avg_px or 0.0), ts_ms=int(time.time() * 1000))
        return qty

    async def startup_reconcile() -> None:
        """
        Source of truth is Alpaca.
        On startup:
        - Load current positions from Alpaca and update in-memory + SQLite
        - Cancel only *entry* BUY orders (avoid zombie entries). Keep TP SELL orders if we are long.
        """
        try:
            def _call():
                return trading_client.get_all_positions()

            pos_list = await asyncio.to_thread(_call)
            pos_by_sym = {}
            for ap in pos_list or []:
                sym = str(getattr(ap, "symbol", "") or "").upper()
                pos_by_sym[sym] = ap

            now_ms = int(time.time() * 1000)
            for sym in symbols:
                ap = pos_by_sym.get(sym)
                if ap is None:
                    qty = 0.0
                    avg_px = 0.0
                else:
                    qty = float(getattr(ap, "qty", 0.0) or 0.0)
                    avg_px = float(getattr(ap, "avg_entry_price", 0.0) or 0.0)

                positions[sym].inventory_base = qty
                positions[sym].avg_entry_price = avg_px
                # Persist snapshot (qty_change=0 => overwrite qty/avg via existing avg logic is imperfect,
                # but we at least keep avg price current; inventory is the authoritative in-memory source).
                await asyncio.to_thread(store.update_position, sym, 0.0, float(avg_px or 0.0), ts_ms=now_ms)

            # Now reconcile open orders.
            terminal = {"canceled", "filled", "rejected", "expired", "done_for_day"}

            def _orders(sym: str):
                req = GetOrdersRequest(status=QueryOrderStatus.ALL, symbols=[sym], limit=500)
                return trading_client.get_orders(filter=req)

            cancel_ids_set: set[str] = set()
            for sym in symbols:
                qty = float(positions[sym].inventory_base or 0.0)
                orders = await asyncio.to_thread(_orders, sym)
                for o in orders or []:
                    st = str(getattr(o, "status", "") or "").lower()
                    if st in terminal:
                        continue
                    side = str(getattr(o, "side", "") or "").lower()
                    oid = getattr(o, "id", None) or getattr(o, "order_id", None)
                    if not oid:
                        continue

                    # Flat: cancel everything (no reason to have any working orders).
                    if qty <= 0:
                        cancel_ids_set.add(str(oid))
                        continue

                    # Long: cancel BUY entries, keep SELL TP orders.
                    if side == "buy":
                        cancel_ids_set.add(str(oid))

            cancel_ids = list(cancel_ids_set)
            for oid in cancel_ids:
                try:
                    await asyncio.to_thread(trading_client.cancel_order_by_id, oid)
                except Exception:
                    pass
            if cancel_ids:
                # Give Alpaca a moment to release held shares after cancels.
                await asyncio.sleep(1.0)

            logger.warning(
                "startup_reconciled",
                extra={
                    "extra": {
                        "symbols": symbols,
                        "canceled_orders": len(cancel_ids),
                        "positions": {s: {"qty": positions[s].inventory_base, "avg_entry": positions[s].avg_entry_price} for s in symbols},
                    }
                },
            )
        except Exception as e:
            logger.warning("startup_reconcile_failed", extra={"extra": {"error": str(e)}})

    # IMPORTANT: reconcile BEFORE starting execution/strategy tasks
    # so we don't submit orders while stale holds are still present.
    await startup_reconcile()

    async def md_router() -> None:
        while not shutdown.is_set:
            evt = await md_q.get()
            sym = str(getattr(evt, "product_id", "") or "").upper()
            q = md_q_by_symbol.get(sym)
            if q:
                await q.put(evt)

    async def fills_processing_loop() -> None:
        while not shutdown.is_set:
            fill: FillEvent = await fills_q.get()
            if not isinstance(fill, FillEvent):
                continue
            fill_id = f"{fill.order_id}:{fill.ts_ms}"
            inserted = await asyncio.to_thread(
                store.insert_fill,
                fill_id=fill_id,
                order_id=fill.order_id,
                product_id=fill.product_id,
                side=fill.side,
                price=fill.price,
                size=fill.size,
                fee=float(getattr(fill, "fee", 0.0) or 0.0),
                liquidity=None,
                ts_ms=fill.ts_ms,
            )
            if not inserted:
                continue
            sym = str(fill.product_id).upper()
            dq = float(fill.size) if fill.side.upper() == "BUY" else -float(fill.size)
            pos = positions.setdefault(sym, PositionState())
            pos.inventory_base += dq
            _, new_avg = await asyncio.to_thread(store.update_position, sym, dq, float(fill.price), ts_ms=fill.ts_ms)
            pos.avg_entry_price = float(new_avg)

    async def reconcile_loop() -> None:
        """
        Poll Alpaca positions periodically to update inventory/avg entry.
        This is the simplest reliable way to stay in sync.
        """
        terminal = {"canceled", "filled", "rejected", "expired", "done_for_day"}
        ensure_tp_cooldown_until: dict[str, float] = {s: 0.0 for s in symbols}
        while not shutdown.is_set:
            try:
                def _call():
                    return trading_client.get_all_positions()
                pos_list = await asyncio.to_thread(_call)
                for ap in pos_list or []:
                    sym = str(getattr(ap, "symbol", "") or "").upper()
                    if sym not in positions:
                        continue
                    qty = float(getattr(ap, "qty", 0.0) or 0.0)
                    avg_px = float(getattr(ap, "avg_entry_price", 0.0) or 0.0)
                    positions[sym].inventory_base = qty
                    positions[sym].avg_entry_price = avg_px
                    # Persist snapshot
                    await asyncio.to_thread(store.update_position, sym, 0.0, float(avg_px or 0.0), ts_ms=int(time.time() * 1000))

                # Stop-loss market exit: when broker shows flat, clear exit latch and cool down new entries.
                for sym in symbols:
                    if sl_exit_in_progress.get(sym) and float(positions[sym].inventory_base or 0.0) <= 0:
                        sl_exit_in_progress[sym] = False
                        post_sl_flat_cooldown_until[sym] = time.time() + post_sl_flat_seconds
                        last_submitted_sig.pop(sym, None)
                        logger.info(
                            "stop_loss_exit_flat",
                            extra={"extra": {"symbol": sym, "post_sl_flat_seconds": post_sl_flat_seconds}},
                        )

                # Ensure: if we are LONG and there is no open SELL, place TP sell.
                for sym in symbols:
                    if sl_exit_in_progress.get(sym):
                        continue
                    if time.time() < float(ensure_tp_cooldown_until.get(sym, 0.0) or 0.0):
                        continue
                    pos = positions.get(sym)
                    if pos is None:
                        continue
                    qty = float(pos.inventory_base or 0.0)
                    avg_px = float(pos.avg_entry_price or 0.0)
                    if qty <= 0 or avg_px <= 0:
                        continue

                    def _orders():
                        req = GetOrdersRequest(status=QueryOrderStatus.ALL, symbols=[sym], limit=200)
                        return trading_client.get_orders(filter=req)

                    orders = await asyncio.to_thread(_orders)
                    has_open_sell = False
                    for o in orders or []:
                        st = str(getattr(o, "status", "") or "").lower()
                        if st in terminal:
                            continue
                        side = str(getattr(o, "side", "") or "").lower()
                        if side == "sell":
                            has_open_sell = True
                            break
                    if has_open_sell:
                        ensure_tp_cooldown_until[sym] = time.time() + 60.0
                        continue

                    p = prod_by_sym.get(sym)
                    if p is None:
                        continue
                    tp_price = round(avg_px * (1.0 + float(p.profit_target_pct)), 2)
                    req = OrderRequest(
                        product_id=sym,
                        side="SELL",
                        price=float(tp_price),
                        size=float(qty),
                        post_only=False,
                        client_order_id=client_order_id("alpaca_scalper_v1", sym, "SELL_TP"),
                    )
                    resp = await exec_gw.place_post_only_limit(req)
                    if resp.status == "OPEN":
                        logger.info(
                            "tp_sell_ensured",
                            extra={"extra": {"symbol": sym, "price": req.price, "qty": req.size}},
                        )
                        ensure_tp_cooldown_until[sym] = time.time() + 60.0
                    else:
                        logger.warning(
                            "tp_sell_ensure_failed",
                            extra={"extra": {"symbol": sym, "price": req.price, "qty": req.size, "error": resp.error_message}},
                        )
                        # Back off on failure to avoid repeated submits while Alpaca updates holds/status.
                        ensure_tp_cooldown_until[sym] = time.time() + 60.0
            except Exception as e:
                logger.warning("alpaca_reconcile_failed", extra={"extra": {"error": str(e)}})
            await asyncio.sleep(cfg.reconcile_seconds)

    # Strategy tasks
    strategy_tasks = []
    for p in cfg.products:
        strat_cfg = {
            "qty": p.qty,
            "half_spread_bps": p.half_spread_bps,
            "profit_target_pct": p.profit_target_pct,
            "stop_loss_pct": p.stop_loss_pct,
            "stop_loss": (cfg.raw.get("strategy", {}) or {}).get("stop_loss", {}) or {},
            "throttling": (cfg.raw.get("strategy", {}) or {}).get("throttling", {}),
        }
        strat = SimpleScalper(p.symbol, strat_cfg, md_q_by_symbol[p.symbol], intent_q_by_symbol[p.symbol], positions[p.symbol])
        strategy_tasks.append(asyncio.create_task(strat.run(), name=f"strategy:{p.symbol}"))

    async def execution_loop() -> None:
        namespace = "alpaca_scalper_v1"
        # Keep exactly one working order per symbol:
        # - Flat => one entry BUY working
        # - In position => one TP SELL working
        last_mode: dict[str, str] = {s: "FLAT" for s in symbols}
        next_retry_ts: dict[str, float] = {s: 0.0 for s in symbols}
        # Cooldown after successful submit (avoid duplicate submits while Alpaca updates holds)
        placed_cooldown_until: dict[str, float] = {s: 0.0 for s in symbols}
        # Hard guard: once we place an order, assume it exists for a short window even if
        # list_open_orders lags / fails. Prevents duplicate submissions.
        working_order_until: dict[str, float] = {s: 0.0 for s in symbols}
        # last_submitted_sig lives in outer scope so reconcile can reset after SL flat.

        async def _cancel_order_ids(ids: list[str]) -> None:
            for oid in ids:
                try:
                    await asyncio.to_thread(trading_client.cancel_order_by_id, oid)
                except Exception:
                    pass

        def _parse_insufficient_qty(err: str) -> tuple[float, list[str]] | None:
            try:
                data = json.loads(err)
            except Exception:
                return None
            if not isinstance(data, dict):
                return None
            if int(data.get("code") or 0) != 40310000:
                return None
            try:
                available = float(data.get("available") or 0.0)
            except Exception:
                available = 0.0
            rel = data.get("related_orders") or []
            related = [str(x) for x in rel if x]
            return available, related

        def _is_day_only_htb(err: str) -> bool:
            try:
                data = json.loads(err)
            except Exception:
                return False
            if not isinstance(data, dict):
                return False
            if int(data.get("code") or 0) != 42210000:
                return False
            msg = str(data.get("message") or "").lower()
            return "only day orders are allowed" in msg

        def _json_err_code_msg(err: str) -> tuple[int | None, str]:
            try:
                data = json.loads(err)
            except Exception:
                return None, ""
            if not isinstance(data, dict):
                return None, ""
            try:
                code = int(data.get("code") or 0)
            except Exception:
                code = 0
            return (code if code else None), str(data.get("message") or "")

        def _cannot_sell_short(err: str) -> bool:
            code, msg = _json_err_code_msg(err)
            return code == 42210000 and "cannot be sold short" in msg.lower()

        while not shutdown.is_set:
            for sym, q in intent_q_by_symbol.items():
                if q.empty():
                    continue
                if time.time() < float(next_retry_ts.get(sym, 0.0) or 0.0):
                    continue
                if time.time() < float(placed_cooldown_until.get(sym, 0.0) or 0.0):
                    continue
                if time.time() < float(working_order_until.get(sym, 0.0) or 0.0):
                    continue
                intent: OrderIntentEvent = await q.get()
                if not isinstance(intent, OrderIntentEvent):
                    continue
                pos = positions[sym]
                inv = float(pos.inventory_base)

                # While a stop-loss market exit is in flight, ignore TP/limit intents until flat.
                if sl_exit_in_progress.get(sym) and inv > 0 and not getattr(intent, "trigger_market_stop_loss", False):
                    continue

                if getattr(intent, "trigger_market_stop_loss", False) and inv > 0:
                    # One exit at a time: strategy may still enqueue ticks; ignore duplicates until flat or failure clears latch.
                    if sl_exit_in_progress.get(sym):
                        continue
                    sl_exit_in_progress[sym] = True
                    logger.warning("stop_loss_triggered", extra={"extra": {"symbol": sym, "inventory": inv}})
                    await exec_gw.cancel_all(sym)
                    mo = await exec_gw.place_market_order(product_id=sym, side="SELL", base_size=inv)
                    if mo.status == "REJECTED":
                        if _cannot_sell_short(mo.error_message or ""):
                            # Often means position already flat (prior market fill) while we still had stale inv.
                            await _refresh_position_from_broker(sym)
                            inv2 = float(positions[sym].inventory_base or 0.0)
                            sl_exit_in_progress[sym] = False
                            if inv2 <= 0:
                                post_sl_flat_cooldown_until[sym] = time.time() + post_sl_flat_seconds
                                last_submitted_sig.pop(sym, None)
                                working_order_until[sym] = time.time() + 10.0
                                next_retry_ts[sym] = time.time() + 5.0
                                logger.info(
                                    "stop_loss_short_reject_flat",
                                    extra={"extra": {"symbol": sym}},
                                )
                            else:
                                next_retry_ts[sym] = time.time() + 30.0
                                logger.warning(
                                    "stop_loss_short_reject_still_long",
                                    extra={"extra": {"symbol": sym, "inventory": inv2}},
                                )
                            continue
                        parsed = _parse_insufficient_qty(mo.error_message or "")
                        if parsed is not None:
                            available, related = parsed
                            # In stop-loss mode, we ARE allowed to cancel related orders to free inventory.
                            if related:
                                logger.warning(
                                    "alpaca_cancel_related_orders_sl",
                                    extra={"extra": {"symbol": sym, "count": len(related)}},
                                )
                                await _cancel_order_ids(related)
                            # If some shares are available, try a partial market sell now.
                            if available > 0:
                                _ = await exec_gw.place_market_order(product_id=sym, side="SELL", base_size=float(available))
                            # Back off before retrying to let holds release.
                            next_retry_ts[sym] = time.time() + 3.0
                            continue
                        sl_exit_in_progress[sym] = False
                        next_retry_ts[sym] = time.time() + 15.0
                        logger.warning(
                            "market_sell_rejected",
                            extra={"extra": {"symbol": sym, "qty": inv, "error": mo.error_message}},
                        )
                        continue

                    # Submitted (OPEN / pending); hold off duplicate SL until broker shows flat.
                    logger.warning(
                        "market_sell_submitted_sl",
                        extra={"extra": {"symbol": sym, "qty": inv, "order_id": mo.order_id}},
                    )
                    placed_cooldown_until[sym] = time.time() + min(60.0, sl_exit_guard_seconds)
                    working_order_until[sym] = time.time() + sl_exit_guard_seconds
                    next_retry_ts[sym] = time.time() + min(30.0, sl_exit_guard_seconds)
                    continue

                # If we already have an open order, do nothing (keep one buy/one sell).
                try:
                    open_orders = await exec_gw.list_open_orders(sym)
                except Exception as e:
                    logger.warning("list_open_orders_failed", extra={"extra": {"symbol": sym, "error": str(e)}})
                    open_orders = []
                if open_orders:
                    # Consider this symbol "busy" for a bit to avoid re-check spam.
                    working_order_until[sym] = time.time() + 10.0
                    continue

                # Flat -> place ONE entry BUY if none working
                if inv <= 0 and intent.bid_price and intent.bid_size:
                    if time.time() < float(post_sl_flat_cooldown_until.get(sym, 0.0) or 0.0):
                        continue
                    mode = "FLAT"
                    desired_sig = ("BUY", float(intent.bid_price), float(intent.bid_size))
                    if last_submitted_sig.get(sym) == desired_sig:
                        # We already submitted this exact order; don't duplicate.
                        continue
                    if last_mode.get(sym) == mode and open_orders:
                        continue
                    req = OrderRequest(
                        product_id=sym,
                        side="BUY",
                        price=float(intent.bid_price),
                        size=float(intent.bid_size),
                        post_only=False,
                        client_order_id=client_order_id(namespace, sym, "BUY"),
                    )
                    resp = await exec_gw.place_post_only_limit(req)
                    last_mode[sym] = mode
                    if resp.status == "REJECTED":
                        logger.warning(
                            "limit_buy_rejected",
                            extra={"extra": {"symbol": sym, "price": req.price, "qty": req.size, "error": resp.error_message}},
                        )
                    else:
                        placed_cooldown_until[sym] = time.time() + 2.0
                        working_order_until[sym] = time.time() + 300.0
                        last_submitted_sig[sym] = desired_sig
                        logger.info(
                            "limit_buy_placed",
                            extra={"extra": {"symbol": sym, "price": req.price, "qty": req.size, "status": resp.status}},
                        )
                # In position -> place profit target SELL
                elif inv > 0 and intent.ask_price and intent.ask_size:
                    mode = "LONG"
                    desired_sig = ("SELL", float(intent.ask_price), float(intent.ask_size))
                    if last_submitted_sig.get(sym) == desired_sig:
                        continue
                    req = OrderRequest(
                        product_id=sym,
                        side="SELL",
                        price=float(intent.ask_price),
                        size=float(intent.ask_size),
                        post_only=False,
                        client_order_id=client_order_id(namespace, sym, "SELL"),
                    )
                    resp = await exec_gw.place_post_only_limit(req)
                    last_mode[sym] = mode
                    if resp.status == "REJECTED":
                        if _is_day_only_htb(resp.error_message or ""):
                            # Back off to avoid spamming Alpaca with a constraint error.
                            next_retry_ts[sym] = time.time() + 300.0
                        parsed = _parse_insufficient_qty(resp.error_message or "")
                        if parsed is not None:
                            available, related = parsed
                            # Runtime rule (scalper): NEVER cancel TP sells unless stop-loss triggers.
                            # If qty is held, just back off and retry later.
                            if available <= 0:
                                next_retry_ts[sym] = time.time() + 30.0
                                continue

                            # If some qty is available now, place ONE partial TP sell and then wait.
                            if available > 0 and available <= float(req.size):
                                partial_req = OrderRequest(
                                    product_id=sym,
                                    side="SELL",
                                    price=req.price,
                                    size=float(available),
                                    post_only=False,
                                    client_order_id=client_order_id(namespace, sym, "SELL"),
                                )
                                pr = await exec_gw.place_post_only_limit(partial_req)
                                if pr.status == "OPEN":
                                    logger.info(
                                        "limit_sell_placed_partial",
                                        extra={"extra": {"symbol": sym, "price": partial_req.price, "qty": partial_req.size}},
                                    )
                                    placed_cooldown_until[sym] = time.time() + 5.0
                                    next_retry_ts[sym] = time.time() + 30.0
                                    continue
                        logger.warning(
                            "limit_sell_rejected",
                            extra={"extra": {"symbol": sym, "price": req.price, "qty": req.size, "error": resp.error_message}},
                        )
                    else:
                        placed_cooldown_until[sym] = time.time() + 5.0
                        working_order_until[sym] = time.time() + 30.0
                        last_submitted_sig[sym] = desired_sig
                        logger.info(
                            "limit_sell_placed",
                            extra={"extra": {"symbol": sym, "price": req.price, "qty": req.size, "status": resp.status}},
                        )

            await asyncio.sleep(0.1)

    tasks = [
        asyncio.create_task(md.run(), name="market_data"),
        asyncio.create_task(md_router(), name="md_router"),
        asyncio.create_task(reconcile_loop(), name="reconcile"),
        asyncio.create_task(fills_processing_loop(), name="fills_processing"),
        *strategy_tasks,
        asyncio.create_task(execution_loop(), name="execution"),
    ]

    logger.info(
        "alpaca_etf_scalper_started",
        extra={"extra": {"paper": cfg.paper, "execute": cfg.execute, "symbols": symbols, "config": config_path}},
    )

    try:
        await shutdown.wait()
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await exec_gw.close()
        store.close()
        release_singleton_lock()
        logger.info("alpaca_etf_scalper_stopped")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="config/alpaca_etf_scalper.yaml")
    args = p.parse_args()
    asyncio.run(main_async(args.config))


if __name__ == "__main__":
    main()


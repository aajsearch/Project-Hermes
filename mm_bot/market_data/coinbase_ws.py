from __future__ import annotations

import asyncio
import logging
import json
import time
import os
import secrets
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import websockets
import jwt

from mm_bot.core.events import EventType, FillEvent, MarketDataEvent
from mm_bot.market_data.book import OrderBook, RollingVol


class CoinbaseMarketData:
    """
    Coinbase Advanced Trade WS market data.
    Subscribes to ticker + level2 and publishes MarketDataEvent on each update.
    """

    def __init__(
        self,
        product_ids: list[str],
        websocket_url: str,
        channels: list[str],
        out_q: asyncio.Queue,
        vol_window_seconds: int,
        fills_q: Optional[asyncio.Queue] = None,
        user_websocket_url: str = "wss://advanced-trade-ws-user.coinbase.com",
    ):
        self.product_ids = [str(p).strip() for p in (product_ids or []) if str(p).strip()]
        if not self.product_ids:
            raise ValueError("product_ids must be a non-empty list")
        self._product_id_set = set(self.product_ids)
        self.websocket_url = websocket_url
        self.channels = channels
        self.out_q = out_q
        self.fills_q = fills_q
        self.user_websocket_url = user_websocket_url
        self._books: Dict[str, OrderBook] = {pid: OrderBook() for pid in self.product_ids}
        self._vols: Dict[str, RollingVol] = {
            pid: RollingVol(window_seconds=vol_window_seconds) for pid in self.product_ids
        }
        self._stop = asyncio.Event()
        self._logger = logging.getLogger("mm_bot.market_data")

        # User stream fill tracking (to emit delta fills on PARTIAL/FILLED updates)
        self._cum_filled_by_order: Dict[str, float] = {}
        self._user_msg_log_count: int = 0

        self._last_pub_best: Dict[str, Tuple[Optional[float], Optional[float]]] = {
            pid: (None, None) for pid in self.product_ids
        }
        self._last_pub_vol: Dict[str, Optional[float]] = {pid: None for pid in self.product_ids}
        self._last_pub_ts: Dict[str, float] = {pid: 0.0 for pid in self.product_ids}

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        # Run public market data and (optionally) authenticated user stream concurrently.
        tasks = [asyncio.create_task(self._run_public_loop(), name="cb_ws_public")]
        if self.fills_q is not None:
            tasks.append(asyncio.create_task(self._run_user_loop(), name="cb_ws_user"))
        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _run_public_loop(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                await self._connect_and_consume_public()
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._logger.warning("ws_disconnected", extra={"extra": {"error": str(e)}})
                await asyncio.sleep(backoff)
                backoff = min(30.0, backoff * 2.0)

    async def _connect_and_consume_public(self) -> None:
        # Coinbase level2 snapshots can be large; increase max_size to avoid 1009 errors.
        async with websockets.connect(
            self.websocket_url,
            ping_interval=20,
            ping_timeout=20,
            max_size=8 * 1024 * 1024,
        ) as ws:
            # Subscribe per channel (more robust across payload versions)
            for ch in self.channels:
                if str(ch).lower() == "user":
                    continue
                sub = {"type": "subscribe", "product_ids": self.product_ids, "channel": ch}
                await ws.send(json.dumps(sub))

            while not self._stop.is_set():
                raw = await ws.recv()
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    self._logger.warning("malformed_json", extra={"extra": {"raw": str(raw)[:200]}})
                    continue
                await self._handle_message(data)

    def _load_cdp_credentials(self) -> Optional[Tuple[str, str]]:
        """
        Return (api_key_name, private_key_pem) for CDP WS JWT auth.
        Supports COINBASE_KEY_FILE (preferred) or COINBASE_API_KEY/COINBASE_API_SECRET (PEM).
        """
        key_file = (os.environ.get("COINBASE_KEY_FILE") or "").strip()
        if key_file and not os.path.isabs(key_file):
            # Best-effort: allow repo-relative paths.
            repo_root = Path(__file__).resolve().parents[2]
            key_file = str((repo_root / key_file).resolve())
        if key_file and os.path.isfile(key_file):
            with open(key_file, "r") as f:
                data = json.load(f)
            name = str(data.get("name") or "").strip()
            pk = str(data.get("privateKey") or "").strip()
            if name and pk:
                return name, pk

        api_key = (os.environ.get("COINBASE_API_KEY") or "").strip()
        api_secret = (os.environ.get("COINBASE_API_SECRET") or "").strip()
        if api_key and api_secret:
            return api_key, api_secret
        return None

    def _make_ws_jwt(self) -> Optional[str]:
        creds = self._load_cdp_credentials()
        if creds is None:
            return None
        api_key_name, private_key_pem = creds
        now = int(time.time())
        payload = {
            "iss": "cdp",
            "sub": api_key_name,
            "nbf": now,
            "exp": now + 120,
        }
        headers = {
            "kid": api_key_name,
            "nonce": secrets.token_hex(16),
        }
        return jwt.encode(payload, private_key_pem, algorithm="ES256", headers=headers)

    async def _run_user_loop(self) -> None:
        """
        Authenticated user stream:
        - Connect to ws-user endpoint
        - Subscribe to user channel using a short-lived JWT
        - Emit FillEvent to fills_q when an order is FILLED
        """
        backoff = 1.0
        while not self._stop.is_set():
            token = self._make_ws_jwt()
            if not token:
                self._logger.warning("ws_user_no_creds")
                return
            try:
                await self._connect_and_consume_user(token)
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._logger.warning("ws_user_disconnected", extra={"extra": {"error": str(e)}})
                await asyncio.sleep(backoff)
                backoff = min(30.0, backoff * 2.0)

    async def _connect_and_consume_user(self, token: str) -> None:
        async with websockets.connect(
            self.user_websocket_url,
            ping_interval=20,
            ping_timeout=20,
            max_size=8 * 1024 * 1024,
        ) as ws:
            self._logger.info(
                "ws_user_connected",
                extra={"extra": {"url": self.user_websocket_url, "product_ids": self.product_ids}},
            )
            sub = {
                "type": "subscribe",
                "channel": "user",
                "product_ids": self.product_ids,
                "jwt": token,
            }
            await ws.send(json.dumps(sub))

            while not self._stop.is_set():
                raw = await ws.recv()
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    self._logger.warning("user_malformed_json", extra={"extra": {"raw": str(raw)[:200]}})
                    continue
                # Log first few user messages (shape discovery) without spamming.
                if self._user_msg_log_count < 3:
                    self._user_msg_log_count += 1
                    evs = data.get("events") if isinstance(data, dict) else None
                    ev0 = evs[0] if isinstance(evs, list) and evs and isinstance(evs[0], dict) else None
                    self._logger.info(
                        "ws_user_message_sample",
                        extra={
                            "extra": {
                                "sample_idx": self._user_msg_log_count,
                                "keys": list(data.keys()),
                                "channel": data.get("channel") or data.get("type"),
                                "event0_keys": (list(ev0.keys()) if ev0 else None),
                            }
                        },
                    )
                await self._handle_user_message(data)

    async def _handle_user_message(self, msg: Dict[str, Any]) -> None:
        if self.fills_q is None:
            return
        channel = (msg.get("channel") or msg.get("type") or "").lower()
        if channel in ("subscriptions", "heartbeat"):
            return
        events = msg.get("events")
        if not isinstance(events, list):
            events = [msg]
        for ev in events:
            if not isinstance(ev, dict):
                continue
            # Common patterns:
            # - ev.orders: order lifecycle updates (with cumulative filled size)
            # - ev.fills: explicit fills list (most reliable)
            fills = ev.get("fills")
            if isinstance(fills, list):
                for f in fills:
                    if not isinstance(f, dict):
                        continue
                    await self._emit_fill_from_fill_obj(f)

            orders = ev.get("orders")
            if isinstance(orders, list):
                # Log shape for the first order we see (helps keep parser aligned with real payloads).
                if orders and isinstance(orders[0], dict) and not getattr(self, "_logged_order_shape", False):
                    setattr(self, "_logged_order_shape", True)
                    o0 = orders[0]
                    self._logger.info(
                        "ws_user_order_shape",
                        extra={
                            "extra": {
                                "order_keys": list(o0.keys()),
                                "status": o0.get("status") or o0.get("order_status"),
                                "has_filled_size": ("filled_size" in o0 or "filled_quantity" in o0 or "cumulative_quantity" in o0),
                            }
                        },
                    )
                for o in orders:
                    if not isinstance(o, dict):
                        continue
                    await self._maybe_emit_fill(o)
            else:
                await self._maybe_emit_fill(ev)

    async def _emit_fill_from_fill_obj(self, f: Dict[str, Any]) -> None:
        """
        Emit a FillEvent from a user-channel fill object.
        Field names vary by WS payload version; this is best-effort.
        """
        if self.fills_q is None:
            return
        order_id = str(f.get("order_id") or f.get("orderId") or "")
        if not order_id:
            return
        product_id = str(f.get("product_id") or f.get("productId") or "").strip()
        if not product_id or product_id not in self._product_id_set:
            return
        side = str(f.get("side") or f.get("order_side") or "").upper()

        def _f(x) -> Optional[float]:
            if x is None:
                return None
            try:
                return float(x)
            except (TypeError, ValueError):
                return None

        price_f = _f(f.get("price") or f.get("fill_price") or f.get("execution_price") or f.get("trade_price"))
        size_f = _f(f.get("size") or f.get("qty") or f.get("base_size") or f.get("filled_size"))
        fee_f = _f(f.get("fee") or f.get("commission") or f.get("fees") or f.get("total_fees")) or 0.0
        if price_f is None or size_f is None or size_f <= 0:
            return

        evt = FillEvent(
            type=EventType.FILL,
            ts_ms=int(time.time() * 1000),
            order_id=order_id,
            product_id=product_id,
            side=side,
            price=float(price_f),
            size=float(size_f),
            fee=float(fee_f),
            meta={"source": "ws_user_fills"},
        )
        try:
            self.fills_q.put_nowait(evt)
        except asyncio.QueueFull:
            self._logger.warning("fills_queue_full", extra={"extra": {"dropped": True}})

    async def _maybe_emit_fill(self, o: Dict[str, Any]) -> None:
        if self.fills_q is None:
            return
        order_id = str(o.get("order_id") or o.get("orderId") or "")
        if not order_id:
            return
        status = str(o.get("status") or o.get("order_status") or "").upper()
        side = str(o.get("side") or "").upper()
        product_id = str(o.get("product_id") or o.get("productId") or "").strip()
        if not product_id:
            return
        if product_id not in self._product_id_set:
            return

        def _f(x) -> Optional[float]:
            if x is None:
                return None
            try:
                return float(x)
            except (TypeError, ValueError):
                return None

        # Coinbase user channel often sends order updates with cumulative filled size.
        cum_filled = (
            _f(o.get("filled_size"))
            or _f(o.get("filled_quantity"))
            or _f(o.get("cumulative_quantity"))
            or _f(o.get("filled_value"))  # sometimes misnamed; best-effort
        )
        # Fallback: if only final status, try plain size.
        if cum_filled is None:
            cum_filled = _f(o.get("size"))
        if cum_filled is None or cum_filled <= 0:
            return

        prev = float(self._cum_filled_by_order.get(order_id, 0.0) or 0.0)
        delta = float(cum_filled - prev)
        if delta <= 0:
            # Nothing new to emit (duplicate update)
            if status in ("FILLED", "DONE", "FILLED_FULLY"):
                self._cum_filled_by_order[order_id] = max(prev, float(cum_filled))
            return
        self._cum_filled_by_order[order_id] = float(cum_filled)

        # Best-effort fill price:
        price = o.get("last_fill_price") or o.get("fill_price") or o.get("average_filled_price") or o.get("avg_fill_price") or o.get("price")
        price_f = _f(price)
        if price_f is None:
            return

        # Best-effort fee; if only total fee is present we still store it (may overcount on partials).
        fee = o.get("fee") or o.get("commission") or o.get("fees") or o.get("total_fees") or 0
        fee_f = _f(fee) or 0.0

        evt = FillEvent(
            type=EventType.FILL,
            ts_ms=int(time.time() * 1000),
            order_id=order_id,
            product_id=product_id,
            side=side,
            price=float(price_f),
            size=float(delta),
            fee=float(fee_f),
            meta={"source": "ws_user", "status": status, "cum_filled": float(cum_filled)},
        )
        try:
            self.fills_q.put_nowait(evt)
        except asyncio.QueueFull:
            self._logger.warning("fills_queue_full", extra={"extra": {"dropped": True}})

    async def _handle_message(self, msg: Dict[str, Any]) -> None:
        channel = (msg.get("channel") or msg.get("type") or "").lower()
        if channel in ("subscriptions", "heartbeat"):
            return

        # Coinbase Advanced Trade WS often wraps payloads as {channel, events:[...]}.
        events = msg.get("events")
        if isinstance(events, list):
            for ev in events:
                if not isinstance(ev, dict):
                    continue
                await self._handle_event(channel, ev, msg)
        else:
            # Fallback: treat msg itself as an event
            await self._handle_event(channel, msg, msg)

    async def _handle_event(self, channel: str, ev: Dict[str, Any], root: Dict[str, Any]) -> None:
        try:
            if channel in ("level2", "l2_data", "orderbook"):
                pid, changed = self._ingest_level2(ev)
                if changed and pid:
                    await self._publish_if_needed(product_id=pid, reason="level2")
                return

            if channel in ("ticker", "tickers"):
                # Ticker can contain best bid/ask; use it if book not yet initialized
                pid, changed = self._ingest_ticker(ev)
                if changed and pid:
                    await self._publish_if_needed(product_id=pid, reason="ticker")
                return
        except Exception as e:
            self._logger.warning("parse_error", extra={"extra": {"channel": channel, "error": str(e)}})

    def _ingest_level2(self, ev: Dict[str, Any]) -> Tuple[Optional[str], bool]:
        etype = (ev.get("type") or ev.get("event_type") or "").lower()
        product_id = ev.get("product_id") or ev.get("productId") or ""
        pid = str(product_id).strip() if product_id else ""
        if pid and pid not in self._product_id_set:
            return None, False

        updates = ev.get("updates") or ev.get("changes") or []
        parsed: List[Tuple[str, float, float]] = []
        if isinstance(updates, list):
            for u in updates:
                if isinstance(u, dict):
                    side = u.get("side") or u.get("s") or ""
                    price = u.get("price_level") or u.get("price") or u.get("p")
                    size = u.get("new_quantity") or u.get("size") or u.get("q") or u.get("quantity")
                elif isinstance(u, (list, tuple)) and len(u) >= 3:
                    side, price, size = u[0], u[1], u[2]
                else:
                    continue
                try:
                    parsed.append((str(side), float(price), float(size)))
                except (TypeError, ValueError):
                    continue

        if not parsed:
            return None, False
        if not pid:
            return None, False
        if etype in ("snapshot", "l2_snapshot"):
            return pid, self._books[pid].apply_snapshot(parsed)
        return pid, self._books[pid].apply_updates(parsed)

    def _ingest_ticker(self, ev: Dict[str, Any]) -> Tuple[Optional[str], bool]:
        # Some payloads nest tickers list; handle both.
        tickers = ev.get("tickers") if isinstance(ev.get("tickers"), list) else None
        if tickers:
            # take first matching product
            for t in tickers:
                if not isinstance(t, dict):
                    continue
                pid = t.get("product_id") or t.get("productId") or ""
                pid = str(pid).strip() if pid else ""
                if pid and pid not in self._product_id_set:
                    continue
                return self._ingest_ticker(t)
            return None, False

        pid = ev.get("product_id") or ev.get("productId") or ""
        pid = str(pid).strip() if pid else ""
        if pid and pid not in self._product_id_set:
            return None, False

        bb = ev.get("best_bid") or ev.get("bestBid") or ev.get("bid") or ev.get("b")
        ba = ev.get("best_ask") or ev.get("bestAsk") or ev.get("ask") or ev.get("a")
        updates: List[Tuple[str, float, float]] = []
        try:
            if bb is not None:
                updates.append(("bid", float(bb), 1.0))
            if ba is not None:
                updates.append(("ask", float(ba), 1.0))
        except (TypeError, ValueError):
            return None, False
        if not updates:
            return None, False
        if not pid:
            return None, False
        # Only update top-of-book from ticker if book is empty.
        book = self._books[pid]
        if book.best_bid is None and book.best_ask is None:
            return pid, book.apply_updates(updates)
        return pid, False

    async def _publish_if_needed(self, *, product_id: str, reason: str) -> None:
        book = self._books[product_id]
        volcalc = self._vols[product_id]
        bb, ba = book.best_bid, book.best_ask
        mid = book.mid_price
        if mid is not None:
            volcalc.add(mid)
        vol = volcalc.value()

        top_changed = (bb, ba) != self._last_pub_best[product_id]
        vol_changed = (
            (vol is not None and self._last_pub_vol[product_id] is None)
            or (vol is None and self._last_pub_vol[product_id] is not None)
            or (
                vol is not None
                and self._last_pub_vol[product_id] is not None
                and abs(vol - float(self._last_pub_vol[product_id])) > 1e-9
            )
        )
        now = time.time()
        # Publish at least once per second while receiving updates (acts as heartbeat)
        heartbeat = (now - self._last_pub_ts[product_id]) > 1.0

        if not (top_changed or vol_changed or heartbeat):
            return

        evt = MarketDataEvent(
            timestamp_ms=int(now * 1000),
            product_id=product_id,
            best_bid=bb,
            best_ask=ba,
            mid_price=mid,
            rolling_volatility=vol,
            meta={"reason": reason},
        )
        try:
            self.out_q.put_nowait(evt)
        except asyncio.QueueFull:
            self._logger.warning("md_queue_full", extra={"extra": {"dropped": True}})
            return

        self._last_pub_best[product_id] = (bb, ba)
        self._last_pub_vol[product_id] = vol
        self._last_pub_ts[product_id] = now


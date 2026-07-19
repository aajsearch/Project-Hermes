from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from dataclasses import dataclass
from typing import List, Optional, Sequence

from coinbase.rest import RESTClient
from requests.exceptions import HTTPError

from mm_bot.execution.base import ExecutionGateway, OrderRequest, OrderResponse


class CircuitBreaker:
    def __init__(self, failure_threshold: int, cooldown_seconds: int):
        self.failure_threshold = int(failure_threshold)
        self.cooldown_seconds = int(cooldown_seconds)
        self.failures = 0
        self.open_until = 0.0

    def allow(self) -> bool:
        now = time.time()
        return now >= self.open_until

    def record_success(self) -> None:
        self.failures = 0
        self.open_until = 0.0

    def record_failure(self) -> None:
        self.failures += 1
        if self.failures >= self.failure_threshold:
            self.open_until = time.time() + self.cooldown_seconds


async def _retry_async(fn, *, max_retries: int, backoff_base: float, cb: CircuitBreaker):
    last = None
    for i in range(max_retries + 1):
        if not cb.allow():
            raise RuntimeError("Circuit breaker open (cooldown active).")
        try:
            out = await fn()
            cb.record_success()
            return out
        except Exception as e:
            last = e
            cb.record_failure()
            if i >= max_retries:
                raise
            # If rate limited, back off more aggressively.
            if isinstance(e, HTTPError) and getattr(getattr(e, "response", None), "status_code", None) == 429:
                await asyncio.sleep(max(1.0, backoff_base) * (2**i))
            else:
                await asyncio.sleep(backoff_base * (2**i))
    raise last  # pragma: no cover


class CoinbaseExecution(ExecutionGateway):
    def __init__(
        self,
        trading_portfolio_id: str,
        usd_account_uuid: str,
        rest_timeout_seconds: int,
        rest_max_retries: int,
        rest_backoff_base_seconds: float,
        cb_failure_threshold: int,
        cb_cooldown_seconds: int,
    ):
        self.trading_portfolio_id = trading_portfolio_id
        self.usd_account_uuid = usd_account_uuid
        self.rest_max_retries = rest_max_retries
        self.rest_backoff_base_seconds = rest_backoff_base_seconds
        self._cb = CircuitBreaker(cb_failure_threshold, cb_cooldown_seconds)

        key_file = (os.environ.get("COINBASE_KEY_FILE") or "").strip() or None
        api_key = (os.environ.get("COINBASE_API_KEY") or "").strip() or None
        api_secret = (os.environ.get("COINBASE_API_SECRET") or "").strip() or None
        if key_file:
            self._client = RESTClient(key_file=key_file, timeout=rest_timeout_seconds)
        else:
            self._client = RESTClient(api_key=api_key, api_secret=api_secret, timeout=rest_timeout_seconds)
        self._logger = logging.getLogger("mm_bot.execution")

    async def start(self) -> None:
        return

    async def close(self) -> None:
        return

    async def place_post_only_limit(self, req: OrderRequest) -> OrderResponse:
        if not req.post_only:
            raise ValueError("post_only must be True for this bot.")

        async def _call():
            # Advanced Trade: limit GTC; use client_order_id for idempotency.
            # NOTE: SDK method names differ by version; this is a skeletal placeholder.
            # You should wire to the exact SDK call you use in your grid bot.
            side = req.side.upper()
            if side == "BUY":
                return await asyncio.to_thread(
                    self._client.limit_order_gtc_buy,
                    client_order_id=req.client_order_id,
                    product_id=req.product_id,
                    base_size=str(req.size),
                    limit_price=str(req.price),
                    post_only=True,
                    retail_portfolio_id=self.trading_portfolio_id,
                )
            return await asyncio.to_thread(
                self._client.limit_order_gtc_sell,
                client_order_id=req.client_order_id,
                product_id=req.product_id,
                base_size=str(req.size),
                limit_price=str(req.price),
                post_only=True,
                retail_portfolio_id=self.trading_portfolio_id,
            )

        resp = await _retry_async(
            _call,
            max_retries=self.rest_max_retries,
            backoff_base=self.rest_backoff_base_seconds,
            cb=self._cb,
        )
        # Parse SDK response robustly (SDK may return typed object or dict)
        success = bool(getattr(resp, "success", False) if not isinstance(resp, dict) else resp.get("success", False))
        if success:
            success_resp = getattr(resp, "success_response", None) if not isinstance(resp, dict) else (resp.get("success_response") or {})
            order_id = getattr(success_resp, "order_id", None) if not isinstance(success_resp, dict) else success_resp.get("order_id")
            if order_id:
                self._logger.info(
                    "order_placed",
                    extra={
                        "extra": {
                            "product_id": req.product_id,
                            "side": req.side,
                            "order_id": str(order_id),
                            "client_order_id": req.client_order_id,
                            "price": req.price,
                            "size": req.size,
                            "post_only": True,
                            "retail_portfolio_id": self.trading_portfolio_id,
                        }
                    },
                )
                return OrderResponse(
                    order_id=str(order_id),
                    client_order_id=req.client_order_id,
                    status="OPEN",
                    error_message=None,
                )

        # Only REJECTED if success is False (or we couldn't extract order_id despite success)
        err_resp = getattr(resp, "error_response", None) if not isinstance(resp, dict) else (resp.get("error_response") or {})
        err_msg = (
            (getattr(err_resp, "message", None) or getattr(err_resp, "error", None))
            if not isinstance(err_resp, dict)
            else (err_resp.get("message") or err_resp.get("error"))
        )
        err_msg = err_msg or str(resp)
        self._logger.warning(
            "order_rejected",
            extra={"extra": {"product_id": req.product_id, "side": req.side, "client_order_id": req.client_order_id, "error": err_msg}},
        )
        return OrderResponse(
            order_id="",
            client_order_id=req.client_order_id,
            status="REJECTED",
            error_message=str(err_msg),
        )
        # unreachable: success path returns above

    async def place_market_order(self, *, product_id: str, side: str, base_size: float) -> OrderResponse:
        """
        Place a standard market order (used for stop-loss liquidation).
        """
        side_u = str(side).upper()
        client_order_id = f"mm_bot_market:{product_id}:{side_u}:{int(time.time() * 1000)}"

        async def _call():
            # SDK method names can vary by version. Try common variants.
            if side_u == "BUY":
                for name in ("market_order_buy", "market_order", "create_market_order_buy"):
                    fn = getattr(self._client, name, None)
                    if fn:
                        return await asyncio.to_thread(
                            fn,
                            client_order_id=client_order_id,
                            product_id=product_id,
                            base_size=str(base_size),
                            retail_portfolio_id=self.trading_portfolio_id,
                        )
            else:
                for name in ("market_order_sell", "market_order", "create_market_order_sell"):
                    fn = getattr(self._client, name, None)
                    if fn:
                        return await asyncio.to_thread(
                            fn,
                            client_order_id=client_order_id,
                            product_id=product_id,
                            base_size=str(base_size),
                            retail_portfolio_id=self.trading_portfolio_id,
                        )
            raise AttributeError("RESTClient market order method not found in this SDK version.")

        resp = await _retry_async(
            _call,
            max_retries=self.rest_max_retries,
            backoff_base=self.rest_backoff_base_seconds,
            cb=self._cb,
        )
        success = bool(getattr(resp, "success", False) if not isinstance(resp, dict) else resp.get("success", False))
        if success:
            success_resp = getattr(resp, "success_response", None) if not isinstance(resp, dict) else (resp.get("success_response") or {})
            order_id = getattr(success_resp, "order_id", None) if not isinstance(success_resp, dict) else success_resp.get("order_id")
            if order_id:
                self._logger.warning(
                    "market_order_placed",
                    extra={"extra": {"product_id": product_id, "side": side_u, "order_id": str(order_id), "base_size": float(base_size)}},
                )
                return OrderResponse(order_id=str(order_id), client_order_id=client_order_id, status="OPEN", error_message=None)

        err_resp = getattr(resp, "error_response", None) if not isinstance(resp, dict) else (resp.get("error_response") or {})
        err_msg = (
            (getattr(err_resp, "message", None) or getattr(err_resp, "error", None))
            if not isinstance(err_resp, dict)
            else (err_resp.get("message") or err_resp.get("error"))
        )
        return OrderResponse(order_id="", client_order_id=client_order_id, status="REJECTED", error_message=str(err_msg or resp))

    async def cancel_all(self, product_id: str) -> None:
        open_orders = await self.list_open_orders(product_id)
        order_ids = [o.order_id for o in open_orders if o.order_id]
        if not order_ids:
            return

        async def _call():
            return await asyncio.to_thread(self._client.cancel_orders, order_ids=order_ids)

        await _retry_async(
            _call,
            max_retries=self.rest_max_retries,
            backoff_base=self.rest_backoff_base_seconds,
            cb=self._cb,
        )
        self._logger.info(
            "orders_canceled",
            extra={"extra": {"product_id": product_id, "count": len(order_ids), "retail_portfolio_id": self.trading_portfolio_id}},
        )

    async def list_open_orders(self, product_id: str) -> Sequence[OrderResponse]:
        out: List[OrderResponse] = []
        cursor = None
        while True:
            async def _call():
                return await asyncio.to_thread(
                    self._client.list_orders,
                    product_ids=[product_id],
                    order_status=["OPEN"],
                    cursor=cursor,
                    retail_portfolio_id=self.trading_portfolio_id,
                )

            resp = await _retry_async(
                _call,
                max_retries=self.rest_max_retries,
                backoff_base=self.rest_backoff_base_seconds,
                cb=self._cb,
            )
            orders = getattr(resp, "orders", None) or []
            for o in orders:
                oid = getattr(o, "order_id", None) or ""
                coid = getattr(o, "client_order_id", None) or ""
                if oid:
                    out.append(OrderResponse(order_id=str(oid), client_order_id=str(coid), status="OPEN"))

            has_next = getattr(resp, "has_next", None)
            if has_next is False:
                break
            next_cursor = getattr(resp, "cursor", None)
            if not next_cursor or next_cursor == cursor:
                break
            cursor = next_cursor
        return out

    async def get_fills(self, *, product_id: str, limit: int = 100, cursor: Optional[str] = None):
        """
        Fetch historical fills (for reconciliation/backfill).
        Coinbase SDK method name: RESTClient.get_fills(...)
        Returns raw SDK response (typed object or dict).
        """
        async def _call():
            kwargs = {
                "product_ids": [product_id],
                "limit": int(limit),
            }
            if cursor:
                kwargs["cursor"] = cursor
            # Some SDK versions require retail_portfolio_id scoping.
            kwargs["retail_portfolio_id"] = self.trading_portfolio_id
            return await asyncio.to_thread(self._client.get_fills, **kwargs)

        return await _retry_async(
            _call,
            max_retries=self.rest_max_retries,
            backoff_base=self.rest_backoff_base_seconds,
            cb=self._cb,
        )


from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict, List

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestQuoteRequest

from mm_bot.core.events import MarketDataEvent


class AlpacaQuotePoller:
    def __init__(self, stock_data: StockHistoricalDataClient, *, symbols: List[str], out_q: asyncio.Queue, poll_seconds: float):
        self._stock = stock_data
        self._symbols = list(dict.fromkeys([s.strip().upper() for s in symbols if s.strip()]))
        self._out_q = out_q
        self._poll = float(poll_seconds)
        self._stop = asyncio.Event()
        self._logger = logging.getLogger("alpaca_etf_scalper.market_data")

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        while not self._stop.is_set():
            try:
                await self._poll_once()
            except Exception as e:
                self._logger.warning("quote_poll_failed", extra={"extra": {"error": str(e)}})
            await asyncio.sleep(self._poll)

    async def _poll_once(self) -> None:
        req = StockLatestQuoteRequest(symbol_or_symbols=self._symbols)

        def _call():
            return self._stock.get_stock_latest_quote(req)

        quotes = await asyncio.to_thread(_call)
        now_ms = int(time.time() * 1000)

        # quotes can be dict-like mapping symbol -> quote
        if isinstance(quotes, dict):
            items = quotes.items()
        else:
            # Some versions return an object with .data
            data = getattr(quotes, "data", None)
            items = (data or {}).items() if isinstance(data, dict) else []

        for sym, q in items:
            try:
                bid = float(getattr(q, "bid_price", None) or q.bid_price)
                ask = float(getattr(q, "ask_price", None) or q.ask_price)
            except Exception:
                continue
            mid = (bid + ask) / 2.0 if bid and ask else None
            evt = MarketDataEvent(
                timestamp_ms=now_ms,
                product_id=str(sym),
                best_bid=bid,
                best_ask=ask,
                mid_price=mid,
                rolling_volatility=None,
                meta={"source": "alpaca_quote_poll"},
            )
            try:
                self._out_q.put_nowait(evt)
            except asyncio.QueueFull:
                self._logger.warning("md_queue_full", extra={"extra": {"dropped": True}})


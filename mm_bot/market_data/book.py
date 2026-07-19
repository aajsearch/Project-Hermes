from __future__ import annotations

import collections
import math
import time
import heapq
from dataclasses import dataclass
from typing import Deque, Dict, Iterable, List, Optional, Tuple


class OrderBook:
    """
    Lightweight Level 2 order book.

    Maintains:
    - price -> size maps for bids/asks
    - lazy heaps to get best bid/ask efficiently
    """

    def __init__(self) -> None:
        self._bids: Dict[float, float] = {}
        self._asks: Dict[float, float] = {}
        self._bid_heap: List[float] = []  # max-heap via -price
        self._ask_heap: List[float] = []  # min-heap via price

        self._best_bid: Optional[float] = None
        self._best_ask: Optional[float] = None

    def clear(self) -> None:
        self._bids.clear()
        self._asks.clear()
        self._bid_heap.clear()
        self._ask_heap.clear()
        self._best_bid = None
        self._best_ask = None

    @staticmethod
    def _norm_side(side: str) -> str:
        s = (side or "").strip().lower()
        if s in ("bid", "buy", "b"):
            return "bid"
        if s in ("ask", "offer", "sell", "a"):
            return "ask"
        return s

    def apply_snapshot(self, updates: Iterable[Tuple[str, float, float]]) -> bool:
        """
        Snapshot replaces book. Returns True if top-of-book changed.
        updates: iterable of (side, price, size)
        """
        prev = (self.best_bid, self.best_ask)
        self.clear()
        self.apply_updates(updates)
        return (self.best_bid, self.best_ask) != prev

    def apply_updates(self, updates: Iterable[Tuple[str, float, float]]) -> bool:
        """
        Incremental updates. Returns True if top-of-book changed.
        updates: iterable of (side, price, size)
        """
        prev = (self.best_bid, self.best_ask)
        for side, price, size in updates:
            s = self._norm_side(side)
            p = float(price)
            q = float(size)
            if s == "bid":
                if q <= 0:
                    self._bids.pop(p, None)
                else:
                    self._bids[p] = q
                    heapq.heappush(self._bid_heap, -p)
            elif s == "ask":
                if q <= 0:
                    self._asks.pop(p, None)
                else:
                    self._asks[p] = q
                    heapq.heappush(self._ask_heap, p)
            else:
                # unknown side; ignore
                continue

        self._best_bid = self._peek_best_bid()
        self._best_ask = self._peek_best_ask()
        return (self.best_bid, self.best_ask) != prev

    def _peek_best_bid(self) -> Optional[float]:
        while self._bid_heap:
            p = -self._bid_heap[0]
            q = self._bids.get(p)
            if q is None or q <= 0:
                heapq.heappop(self._bid_heap)
                continue
            return p
        return None

    def _peek_best_ask(self) -> Optional[float]:
        while self._ask_heap:
            p = self._ask_heap[0]
            q = self._asks.get(p)
            if q is None or q <= 0:
                heapq.heappop(self._ask_heap)
                continue
            return p
        return None

    @property
    def best_bid(self) -> Optional[float]:
        return self._best_bid

    @property
    def best_ask(self) -> Optional[float]:
        return self._best_ask

    @property
    def mid_price(self) -> Optional[float]:
        if self._best_bid is None or self._best_ask is None:
            return None
        return (self._best_bid + self._best_ask) / 2.0


class RollingVol:
    """
    Rolling volatility metric based on mid-price series.

    Per spec, we compute standard deviation of mid-price *log returns*
    over the rolling window. (Stable across price scales.)
    """

    def __init__(self, window_seconds: int):
        self.window_seconds = int(window_seconds)
        self._points: Deque[Tuple[float, float]] = collections.deque()  # (ts, mid)

    def add(self, mid: float, ts: Optional[float] = None) -> None:
        now = ts if ts is not None else time.time()
        self._points.append((now, float(mid)))
        cutoff = now - self.window_seconds
        while self._points and self._points[0][0] < cutoff:
            self._points.popleft()

    def value(self) -> Optional[float]:
        if len(self._points) < 3:
            return None
        rets = []
        prev = self._points[0][1]
        for _, mid in list(self._points)[1:]:
            if prev > 0 and mid > 0:
                rets.append(math.log(mid / prev))
            prev = mid
        if len(rets) < 2:
            return None
        m = sum(rets) / len(rets)
        var = sum((r - m) ** 2 for r in rets) / (len(rets) - 1)
        return math.sqrt(var)


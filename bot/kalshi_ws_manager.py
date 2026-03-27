"""
Kalshi WebSocket orderbook manager for HFT.

Maintains a thread-safe global dictionary of the latest order book (top of book)
per ticker from Kalshi's orderbook_delta channel. No REST orderbook calls.
Authentication required (same as REST: KALSHI_ACCESS_KEY, signature, timestamp).
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import ssl
import threading
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# --- Global state (thread-safe) ---
# ticker -> {"yes_bid", "no_bid", "yes_ask", "no_ask", "bid", "ask"}
_global_kalshi_books: Dict[str, Dict[str, Any]] = {}
# Per-ticker orderbook levels for delta application: ticker -> {"yes": {price_cents: contracts}, "no": {...}}
_global_kalshi_levels: Dict[str, Dict[str, Dict[int, int]]] = {}
# market_ticker -> market dict (close_time, title, floor_strike, etc.) from market_lifecycle_v2
_global_market_cache: Dict[str, Dict[str, Any]] = {}
# Only cache 15m/hourly crypto markets to limit memory (no filter = all exchange events)
_MARKET_CACHE_PREFIXES = ("KXBTC15M-", "KXETH15M-", "KXSOL15M-", "KXXRP15M-", "KXBTCHOUR-", "KXETHHOUR-", "KXSOLHOUR-", "KXXRPHOUR-")
_lock = threading.Lock()
_subscribed_tickers: List[str] = []
_subscribe_lock = threading.Lock()
_loop: Optional[asyncio.AbstractEventLoop] = None
_ws_task: Optional[threading.Thread] = None
_ws_running = False
_cmd_id = 0
_current_ws: Any = None  # set in loop so stop() can close and unblock
_ws_control_lock = threading.Lock()
_debug_msg_count = 0
_debug_msg_limit = 50  # log first N message types to see what server sends
_logged_orderbook_miss: set = set()  # tickers we've already logged miss for (reset when snapshot received)

WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
WS_PATH = "/trade-api/ws/v2"


def _kalshi_ws_ssl_context() -> ssl.SSLContext:
    """SSL context that bypasses cert verification (LibreSSL/Mac compatibility for wss://)."""
    try:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    except AttributeError:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def _kalshi_ws_headers() -> Optional[Dict[str, str]]:
    """Build auth headers for Kalshi WebSocket (GET /trade-api/ws/v2). Returns None if env not set."""
    api_key = os.getenv("KALSHI_API_KEY")
    key_path = os.getenv("KALSHI_PRIVATE_KEY")
    if not api_key or not key_path:
        return None
    try:
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding
        from cryptography.hazmat.primitives.serialization import load_pem_private_key
    except ImportError:
        logger.warning("[kalshi_ws] cryptography not installed; Kalshi WS disabled.")
        return None
    try:
        key_content = key_path.strip()
        if key_content.startswith("-----BEGIN"):
            pem_bytes = key_content.encode("utf-8")
        else:
            with open(key_path, "rb") as f:
                pem_bytes = f.read()
        private_key = load_pem_private_key(pem_bytes, password=None, backend=default_backend())
    except Exception as e:
        logger.warning("[kalshi_ws] Failed to load private key: %s", e)
        return None
    timestamp_ms = str(int(time.time() * 1000))
    message = f"{timestamp_ms}GET{WS_PATH}".encode("utf-8")
    signature = private_key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    sig_b64 = base64.b64encode(signature).decode("utf-8")
    return {
        "KALSHI-ACCESS-KEY": api_key,
        "KALSHI-ACCESS-SIGNATURE": sig_b64,
        "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
    }


def _best_bid_from_levels(levels: Dict[int, int]) -> Optional[int]:
    """Return best (highest) bid price with contracts > 0, or None."""
    candidates = [p for p, c in levels.items() if c and c > 0]
    return max(candidates) if candidates else None


def _parse_snapshot_levels(arr: Any) -> Dict[int, int]:
    """Parse yes/no or yes_dollars_fp/no_dollars_fp to {price_cents: contracts}. Handles cents (int) or dollars (float string)."""
    result: Dict[int, int] = {}
    if not isinstance(arr, (list, tuple)):
        return result
    for lev in arr:
        if isinstance(lev, (list, tuple)) and len(lev) >= 2:
            try:
                p_raw, c_raw = lev[0], lev[1]
                p_float = float(str(p_raw))
                c_float = float(str(c_raw))
                p = int(round(p_float * 100)) if 0 < p_float < 10 else int(round(p_float))  # dollars if 0-10 else cents
                c = int(round(c_float))
                if c > 0 and p > 0:
                    result[p] = c
            except (TypeError, ValueError):
                pass
    return result


def _apply_snapshot(ticker: str, msg: Dict[str, Any]) -> None:
    """
    Apply orderbook_snapshot to global state.
    Supports: yes/no (cents), yes_levels/no_levels, yes_dollars_fp/no_dollars_fp (dollars),
    or top-level yes_bid/yes_ask/no_bid/no_ask. Empty snapshots (no levels, no top-of-book) are
    logged and we wait for the first orderbook_delta.
    """
    yes_arr = msg.get("yes") or msg.get("yes_levels") or msg.get("yes_dollars_fp") or []
    no_arr = msg.get("no") or msg.get("no_levels") or msg.get("no_dollars_fp") or []
    yes_levels = _parse_snapshot_levels(yes_arr)
    no_levels = _parse_snapshot_levels(no_arr)
    best_yes_bid = _best_bid_from_levels(yes_levels)
    best_no_bid = _best_bid_from_levels(no_levels)
    yes_ask = (100 - best_no_bid) if best_no_bid is not None else None
    no_ask = (100 - best_yes_bid) if best_yes_bid is not None else None

    # If no levels, check for top-of-book in snapshot (some APIs send yes_bid/yes_ask/no_bid/no_ask)
    if best_yes_bid is None and best_no_bid is None:
        top_yes_bid = msg.get("yes_bid")
        top_no_bid = msg.get("no_bid")
        top_yes_ask = msg.get("yes_ask")
        top_no_ask = msg.get("no_ask")
        if top_yes_bid is not None or top_no_bid is not None or top_yes_ask is not None or top_no_ask is not None:
            try:
                best_yes_bid = int(top_yes_bid) if top_yes_bid is not None else None
                best_no_bid = int(top_no_bid) if top_no_bid is not None else None
                yes_ask = int(top_yes_ask) if top_yes_ask is not None else (100 - best_no_bid) if best_no_bid is not None else None
                no_ask = int(top_no_ask) if top_no_ask is not None else (100 - best_yes_bid) if best_yes_bid is not None else None
            except (TypeError, ValueError):
                best_yes_bid = best_no_bid = yes_ask = no_ask = None

    # Always store a book entry so get_safe_orderbook returns something (pipeline can detect empty and fallback to REST for quote).
    # Low-liquidity 15m crypto often get empty snapshots (no levels); Kalshi sends minimal snapshot (market_ticker, market_id only).
    with _lock:
        _global_kalshi_levels[ticker] = {"yes": yes_levels, "no": no_levels}
        is_empty = best_yes_bid is None and best_no_bid is None
        if is_empty:
            book = {
                "yes_bid": None,
                "yes_ask": None,
                "no_bid": None,
                "no_ask": None,
                "bid": None,
                "ask": None,
                "empty": True,
                "ts": msg.get("ts"),
            }
        else:
            bid = max((best_yes_bid or 0), (best_no_bid or 0)) or None
            ask = (yes_ask if (best_yes_bid or 0) >= (best_no_bid or 0) else no_ask) if (yes_ask is not None or no_ask is not None) else None
            book = {
                "yes_bid": best_yes_bid,
                "no_bid": best_no_bid,
                "yes_ask": yes_ask,
                "no_ask": no_ask,
                "bid": bid,
                "ask": ask,
                "empty": False,
                "ts": msg.get("ts"),
            }
        _global_kalshi_books[ticker] = book
    logger.debug("[kalshi_ws] Snapshot applied for %s (empty=%s)", ticker, is_empty)
    if not is_empty:
        try:
            import bot.data_bus as data_bus
            data_bus.write_book(ticker, best_yes_bid, yes_ask, best_no_bid, no_ask)
        except Exception:
            pass


def _apply_delta(ticker: str, msg: Dict[str, Any]) -> None:
    """
    Apply orderbook_delta to global state. Supports (1) top-of-book update: msg with yes_bid/no_bid
    (and optionally yes_ask/no_ask); (2) level update: side/price/delta (cents or price_dollars/delta_fp).
    """
    # Top-of-book delta: API may send yes_bid/no_bid/yes_ask/no_ask directly
    top_yes_bid = msg.get("yes_bid")
    top_no_bid = msg.get("no_bid")
    top_yes_ask = msg.get("yes_ask")
    top_no_ask = msg.get("no_ask")
    if top_yes_bid is not None or top_no_bid is not None or top_yes_ask is not None or top_no_ask is not None:
        try:
            best_yes_bid = int(top_yes_bid) if top_yes_bid is not None else None
            best_no_bid = int(top_no_bid) if top_no_bid is not None else None
            yes_ask = int(top_yes_ask) if top_yes_ask is not None else (100 - best_no_bid) if best_no_bid is not None else None
            no_ask = int(top_no_ask) if top_no_ask is not None else (100 - best_yes_bid) if best_yes_bid is not None else None
        except (TypeError, ValueError):
            best_yes_bid = best_no_bid = yes_ask = no_ask = None
        else:
            with _lock:
                if ticker not in _global_kalshi_books:
                    _global_kalshi_books[ticker] = {}
                bid = max((best_yes_bid or 0), (best_no_bid or 0)) or None
                ask = (yes_ask if (best_yes_bid or 0) >= (best_no_bid or 0) else no_ask) if (yes_ask is not None or no_ask is not None) else None
                _global_kalshi_books[ticker].update({
                    "yes_bid": best_yes_bid,
                    "no_bid": best_no_bid,
                    "yes_ask": yes_ask,
                    "no_ask": no_ask,
                    "bid": bid,
                    "ask": ask,
                })
            try:
                import bot.data_bus as data_bus
                data_bus.write_book(ticker, best_yes_bid, yes_ask, best_no_bid, no_ask)
            except Exception:
                pass
            return

    # Level-based delta
    side = (msg.get("side") or "").lower()
    if side not in ("yes", "no"):
        return
    price = msg.get("price") or msg.get("price_dollars")
    delta = msg.get("delta") or msg.get("delta_fp")
    if price is None or delta is None:
        return
    try:
        p_float = float(str(price))
        d_float = float(str(delta))
        price = int(round(p_float * 100)) if 0 < p_float < 10 else int(round(p_float))
        delta = int(round(d_float))
    except (TypeError, ValueError):
        return
    with _lock:
        if ticker not in _global_kalshi_levels:
            _global_kalshi_levels[ticker] = {"yes": {}, "no": {}}
        levels = _global_kalshi_levels[ticker].get(side, {})
        levels[price] = levels.get(price, 0) + delta
        if levels[price] <= 0:
            del levels[price]
        _global_kalshi_levels[ticker][side] = levels
        yes_levels = _global_kalshi_levels[ticker].get("yes", {})
        no_levels = _global_kalshi_levels[ticker].get("no", {})
    best_yes_bid = _best_bid_from_levels(yes_levels)
    best_no_bid = _best_bid_from_levels(no_levels)
    yes_ask = (100 - best_no_bid) if best_no_bid is not None else None
    no_ask = (100 - best_yes_bid) if best_yes_bid is not None else None
    with _lock:
        if ticker not in _global_kalshi_books:
            _global_kalshi_books[ticker] = {}
        bid = max((best_yes_bid or 0), (best_no_bid or 0)) or None
        ask = (yes_ask if (best_yes_bid or 0) >= (best_no_bid or 0) else no_ask) if (yes_ask is not None or no_ask is not None) else None
        _global_kalshi_books[ticker].update({
            "yes_bid": best_yes_bid,
            "no_bid": best_no_bid,
            "yes_ask": yes_ask,
            "no_ask": no_ask,
            "bid": bid,
            "ask": ask,
        })
    try:
        import bot.data_bus as data_bus
        data_bus.write_book(ticker, best_yes_bid, yes_ask, best_no_bid, no_ask)
    except Exception:
        pass


def _normalize_close_time(val: Any) -> Optional[int]:
    """Convert close_ts/close_time/expected_expiration_time to int (seconds since epoch)."""
    if val is None:
        return None
    try:
        if isinstance(val, (int, float)):
            return int(val)
        s = str(val).strip()
        if not s:
            return None
        if s.isdigit():
            return int(s)
        from datetime import datetime, timezone
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except (TypeError, ValueError):
        return None


def _apply_market_lifecycle(msg: Dict[str, Any]) -> None:
    """
    Update market cache from market_lifecycle_v2 message.
    Maps API fields: market_ticker → ticker, close_ts/close_time/expected_expiration_time → close_time.
    Only caches tickers matching _MARKET_CACHE_PREFIXES.
    """
    ticker = msg.get("market_ticker") or msg.get("ticker")
    if not ticker or not isinstance(ticker, str):
        return
    if not any(ticker.startswith(p) for p in _MARKET_CACHE_PREFIXES):
        return
    event_type = msg.get("event_type")
    close_ts = msg.get("close_ts") or msg.get("close_time") or msg.get("expected_expiration_time")
    close_time = _normalize_close_time(close_ts)
    meta = msg.get("additional_metadata") or {}
    with _lock:
        if ticker not in _global_market_cache:
            _global_market_cache[ticker] = {}
        entry = _global_market_cache[ticker]
        entry["ticker"] = ticker
        if close_time is not None:
            entry["close_time"] = close_time
            entry["expected_expiration_time"] = close_time
        if event_type == "created" and meta:
            if meta.get("title") is not None:
                entry["title"] = meta["title"]
            if meta.get("yes_sub_title") is not None:
                entry["subtitle"] = meta["yes_sub_title"]
            if meta.get("floor_strike") is not None:
                try:
                    entry["floor_strike"] = float(meta["floor_strike"])
                except (TypeError, ValueError):
                    pass
            if meta.get("cap_strike") is not None:
                try:
                    entry["cap_strike"] = float(meta["cap_strike"])
                except (TypeError, ValueError):
                    pass
        elif event_type == "close_date_updated" and close_time is not None:
            entry["close_time"] = close_time
            entry["expected_expiration_time"] = close_time
        # Always set title/subtitle from top-level or meta when present
        if msg.get("title") is not None:
            entry["title"] = msg["title"]
        if msg.get("subtitle") is not None:
            entry["subtitle"] = msg["subtitle"]
        if (meta or {}).get("title") is not None:
            entry["title"] = meta["title"]
        if (meta or {}).get("yes_sub_title") is not None:
            entry["subtitle"] = meta["yes_sub_title"]
    logger.debug("[kalshi_ws] Market cache updated for %s (close_time=%s)", ticker, entry.get("close_time"))


def seed_market_cache(markets: Dict[str, Dict[str, Any]]) -> None:
    """
    Seed the WS market cache from REST-fetched market dicts (e.g. after subscribe, before first cycle).
    Call with ticker -> market dict; each market should have 'ticker' and 'close_time' (or expected_expiration_time).
    Only tickers matching _MARKET_CACHE_PREFIXES are stored.
    """
    if not markets:
        return
    n = 0
    with _lock:
        for ticker, m in markets.items():
            if not ticker or not isinstance(m, dict):
                continue
            if not any(str(ticker).startswith(p) for p in _MARKET_CACHE_PREFIXES):
                continue
            close_time = None
            for key in ("close_time", "expected_expiration_time", "expiration_time", "close_ts"):
                v = m.get(key)
                close_time = _normalize_close_time(v) if v is not None else close_time
                if close_time is not None:
                    break
            entry = {"ticker": ticker}
            if close_time is not None:
                entry["close_time"] = close_time
                entry["expected_expiration_time"] = close_time
            for key in ("title", "subtitle", "floor_strike", "cap_strike"):
                v = m.get(key)
                if v is not None:
                    entry[key] = v
            _global_market_cache[ticker] = entry
            n += 1
    if n:
        logger.debug("[kalshi_ws] Seeded %d markets from REST", n)


def subscribe_to_tickers(tickers: List[str]) -> None:
    """Replace the list of tickers to subscribe to. On change, force reconnect so WS picks up new list."""
    if not tickers:
        return
    tickers_list = [t for t in tickers if t and str(t).strip()]
    if not tickers_list:
        return
    with _subscribe_lock:
        global _subscribed_tickers
        old_set = set(_subscribed_tickers)
        new_set = set(tickers_list)
        if old_set == new_set:
            return
        _subscribed_tickers = list(tickers_list)
        # Force reconnect so WS subscribes to the new ticker list
        with _ws_control_lock:
            ws = _current_ws
            loop = _loop
        if ws is not None and loop is not None:
            try:
                # Close on the owning loop; avoids "Future attached to a different loop".
                asyncio.run_coroutine_threadsafe(ws.close(), loop)
            except Exception as e:
                logger.warning("[kalshi_ws] Failed to trigger reconnect on ticker change: %s", e)


def _get_subscribed_tickers_snapshot() -> List[str]:
    with _subscribe_lock:
        return list(_subscribed_tickers)


async def _kalshi_ws_loop() -> None:
    global _current_ws
    try:
        import websockets
    except ImportError:
        logger.warning("[kalshi_ws] websockets not installed; pip install websockets. Kalshi WS disabled.")
        return
    backoff = 1.0
    while True:
        try:
            headers = _kalshi_ws_headers()
            if not headers:
                logger.warning("[kalshi_ws] No Kalshi auth (KALSHI_API_KEY/KALSHI_PRIVATE_KEY); reconnecting in %.0fs", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.5, 60.0)
                continue
            async with websockets.connect(
                WS_URL,
                additional_headers=headers,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
                ssl=True,
            ) as ws:
                backoff = 1.0
                with _ws_control_lock:
                    _current_ws = ws
                logger.debug("[kalshi_ws] Connected successfully to %s", WS_URL)
                try:
                    tickers = _get_subscribed_tickers_snapshot()
                    global _cmd_id
                    if tickers:
                        _cmd_id += 1
                        sub = {"id": _cmd_id, "cmd": "subscribe", "params": {"channels": ["orderbook_delta"], "market_tickers": tickers}}
                        try:
                            await ws.send(json.dumps(sub))
                            logger.debug("[kalshi_ws] Subscribe sent: %s", sub)
                        except Exception as e:
                            logger.error("[kalshi_ws] Subscribe send failed: %s", e)
                    # Market lifecycle (close_ts, strike, title) — no market_tickers filter; we get all, cache by prefix
                    _cmd_id += 1
                    sub_lifecycle = {"id": _cmd_id, "cmd": "subscribe", "params": {"channels": ["market_lifecycle_v2"]}}
                    try:
                        await ws.send(json.dumps(sub_lifecycle))
                        logger.debug("[kalshi_ws] Subscribe sent: %s", sub_lifecycle)
                    except Exception as e:
                        logger.error("[kalshi_ws] market_lifecycle_v2 subscribe failed: %s", e)
                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                        except Exception:
                            continue
                        _raw_str = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
                        logger.debug("[kalshi_ws] Received: %s", _raw_str[:1000] + "..." if len(_raw_str) > 1000 else _raw_str)
                        try:
                            if not isinstance(data, dict):
                                continue
                            msg_type = data.get("type")
                            msg = data.get("msg") or {}
                            ticker = msg.get("market_ticker") or msg.get("market_id")
                            # Debug: log first N message types to see what server sends
                            global _debug_msg_count, _debug_msg_limit, _logged_orderbook_miss
                            if _debug_msg_count < _debug_msg_limit:
                                _debug_msg_count += 1
                                logger.debug(
                                    "[kalshi_ws] msg #%s type=%s ticker=%s msg_keys=%s",
                                    _debug_msg_count, msg_type, ticker, list(msg.keys()) if msg else [],
                                )
                            if msg_type == "orderbook_snapshot" and ticker:
                                _logged_orderbook_miss.discard(ticker)
                                _apply_snapshot(ticker, msg)
                            elif msg_type == "orderbook_snapshot":
                                logger.debug("[kalshi_ws] orderbook_snapshot missing ticker msg=%s", list(msg.keys()))
                            elif msg_type == "orderbook_delta" and ticker:
                                _apply_delta(ticker, msg)
                            elif msg_type == "market_lifecycle_v2":
                                _apply_market_lifecycle(msg)
                            elif msg_type == "subscribed":
                                logger.debug("[kalshi_ws] Subscription confirmed for tickers: %s", tickers)
                            elif msg_type == "error":
                                logger.error("[kalshi_ws] Server error details: %s", data)
                        except Exception as inner_e:
                            logger.error("[kalshi_ws] Message processing error: %s; continuing", inner_e)
                            continue
                finally:
                    with _ws_control_lock:
                        if _current_ws is ws:
                            _current_ws = None
        except Exception as e:
            if isinstance(e, websockets.exceptions.ConnectionClosed):
                logger.error("[kalshi_ws] Connection closed: code=%s reason=%s", e.code, e.reason)
            elif isinstance(e, websockets.exceptions.InvalidHandshake):
                status = getattr(e, "response", None)
                status = getattr(status, "status_code", None) if status is not None else getattr(e, "status_code", None)
                headers = getattr(getattr(e, "response", None), "headers", None) or getattr(e, "headers", None)
                logger.error("[kalshi_ws] Handshake failed: status=%s headers=%s", status, headers)
            else:
                logger.exception("[kalshi_ws] Connect/handshake error: %s", e)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 1.5, 60.0)


def _run_loop_in_thread(loop: asyncio.AbstractEventLoop) -> None:
    global _ws_running
    _ws_running = True
    try:
        loop.run_until_complete(_kalshi_ws_loop())
    finally:
        _ws_running = False


def stop_kalshi_ws() -> None:
    """Stop Kalshi WebSocket: close connection and join thread (prevents zombie processes)."""
    global _loop, _ws_task, _current_ws
    with _ws_control_lock:
        loop = _loop
        ws = _current_ws
        task = _ws_task
        _ws_task = None
        _loop = None
        _current_ws = None

    if loop is None:
        return
    try:
        if ws is not None:
            # Ensure close coroutine runs on the same loop that owns the websocket.
            asyncio.run_coroutine_threadsafe(ws.close(), loop)
    except Exception:
        pass
    if task is not None and task.is_alive():
        task.join(timeout=10)
    logger.debug("[kalshi_ws] Kalshi WebSocket stopped.")


def start_kalshi_ws() -> None:
    """Start Kalshi orderbook WebSocket in a background thread. Idempotent."""
    global _loop, _ws_task
    with _ws_control_lock:
        if _loop is not None and _ws_task is not None and _ws_task.is_alive():
            return
    try:
        import websockets
    except ImportError:
        logger.warning("[kalshi_ws] websockets not installed; pip install websockets. Kalshi WS disabled.")
        return
    loop = asyncio.new_event_loop()
    task = threading.Thread(target=_run_loop_in_thread, args=(loop,), daemon=True)
    with _ws_control_lock:
        _loop = loop
        _ws_task = task
    task.start()
    logger.debug("[kalshi_ws] Kalshi orderbook WebSocket started.")


def is_kalshi_ws_running() -> bool:
    return _ws_running


def _orderbook_lookup_key(ticker: str) -> Optional[str]:
    """Return cache key for ticker. REST can return ticker with trailing -NN (e.g. KXBTC15M-26MAR141445-45); WS uses base (KXBTC15M-26MAR141445)."""
    key = str(ticker).strip()
    if not key:
        return None
    with _lock:
        if key in _global_kalshi_books:
            return key
        if key.count("-") >= 2 and key[-1].isdigit():
            base = key.rsplit("-", 1)[0]
            if base in _global_kalshi_books:
                return base
    return None


def get_safe_orderbook(ticker: str) -> Optional[Dict[str, Any]]:
    """
    Thread-safe getter for latest top-of-book for the given ticker.
    Returns dict with keys: yes_bid, no_bid, yes_ask, no_ask, bid, ask (ints or None), and optionally "empty" (True if snapshot had no levels).
    Returns None only if WS has no cache entry for ticker (true miss). When snapshot was empty, we still store and return a book with yes_bid/no_bid None so caller can use WS and optionally fallback to REST for quote.
    Accepts REST-style ticker with trailing -NN (e.g. KXBTC15M-26MAR141445-45) and looks up base ticker in cache.
    """
    if not ticker or not str(ticker).strip():
        return None
    key = str(ticker).strip()
    lookup_key = _orderbook_lookup_key(key)
    with _lock:
        book = _global_kalshi_books.get(lookup_key) if lookup_key else None
        cache_keys = list(_global_kalshi_books.keys())
    if not book:
        if key not in _logged_orderbook_miss:
            _logged_orderbook_miss.add(key)
            logger.debug(
                "[kalshi_ws] orderbook miss ticker=%s cache_keys=%s",
                key, cache_keys,
            )
        return None
    return dict(book)


def get_safe_market(ticker: str) -> Optional[Dict[str, Any]]:
    """
    Thread-safe getter for market metadata from market_lifecycle_v2 cache.
    Returns a dict compatible with pipeline (ticker, close_time, title, floor_strike, etc.) or None.
    Caller should fall back to REST (fetch_15min_market / get_market) when None or missing required fields.
    """
    if not ticker or not str(ticker).strip():
        return None
    key = str(ticker).strip()
    with _lock:
        entry = _global_market_cache.get(key)
    if not entry:
        return None
    return dict(entry)

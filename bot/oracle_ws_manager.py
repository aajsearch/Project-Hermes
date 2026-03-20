"""
WebSocket Spot Oracle for Bot V2: single source (Coinbase).

Maintains global_spot_prices updated in real time via WebSocket (no blocking REST).
Provides get_safe_spot_prices_sync() for strategies with stale-data guard.

Design: We use a single "spot" key so that swapping to another provider (e.g. Kraken)
later only requires changing which WS loop feeds "spot" and which REST fallback is used.
- Coinbase: Exchange feed (ws-feed.exchange.coinbase.com) or set COINBASE_WS_FEED=advanced_trade
  for Advanced Trade (wss://advanced-trade-ws.coinbase.com).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import ssl
import threading
import time
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def _ws_ssl_context() -> ssl.SSLContext:
    """SSL context for WebSocket connections. Bypasses certificate verification
    (cert_reqs = CERT_NONE) for LibreSSL/Mac compatibility; use so wss:// works."""
    try:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    except AttributeError:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


# --- Global state (thread-safe) ---
# Single source: global_spot_prices['btc'] = {'spot': 71000.5, 'spot_ts': 170000000.1}
# Using "spot" (not "cb") so swapping provider (e.g. Kraken) only changes which WS feeds it.
_global_spot_prices: Dict[str, Dict[str, Any]] = {}
_lock = threading.RLock()
_first_spot_logged: set = set()
_loop: Optional[asyncio.AbstractEventLoop] = None
_ws_task: Optional[threading.Thread] = None
_ws_running = False
_cb_ws: Any = None


class StaleOracleDataException(Exception):
    """Raised when spot data is older than max_age_seconds; bot must refuse to trade."""

    pass


# Subscription targets (Coinbase; exposed for tests). To add Kraken later, add KRAKEN_* and a second WS loop.
COINBASE_TICKER_PRODUCTS = ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD"]
WS_ASSET_KEYS = ["BTC", "ETH", "SOL", "XRP"]
COINBASE_PRODUCT_TO_ASSET = {"BTC-USD": "BTC", "ETH-USD": "ETH", "SOL-USD": "SOL", "XRP-USD": "XRP"}


def _asset_to_coinbase_product(asset: str) -> str:
    a = (asset or "").strip().upper()
    if a == "BTC":
        return "BTC-USD"
    if a in ("ETH", "SOL", "XRP"):
        return f"{a}-USD"
    return "BTC-USD"


def _ensure_asset_entry(asset_key: str) -> None:
    with _lock:
        if asset_key not in _global_spot_prices:
            _global_spot_prices[asset_key] = {"spot": None, "spot_ts": None}


def _set_spot_price(asset_key: str, price: float) -> None:
    """Set the single spot price (currently from Coinbase WS). Swap provider by feeding from another WS."""
    key = (asset_key or "").strip().lower() or "btc"
    with _lock:
        _ensure_asset_entry(key)
        _global_spot_prices[key]["spot"] = price
        _global_spot_prices[key]["spot_ts"] = time.time()
        if key not in _first_spot_logged:
            _first_spot_logged.add(key)
            logger.info("[oracle_ws] First spot price received for %s: %s", key, price)
    try:
        import bot.data_bus as data_bus
        data_bus.write_spot(key, price)
    except Exception:
        pass


def get_safe_spot_prices_sync(
    asset: str, max_age_seconds: float = 3.0, require_both: bool = False
) -> Optional[Dict[str, Any]]:
    """
    Synchronous read for the strategy thread. Returns current spot data for the asset.
    Single source: keys "spot" and "spot_ts". require_both is ignored (kept for API compat).
    """
    asset_key = (asset or "").strip().lower() or "btc"
    with _lock:
        entry = _global_spot_prices.get(asset_key)
    if not entry:
        return None
    now = time.time()
    spot = entry.get("spot")
    spot_ts = entry.get("spot_ts")
    if spot is None or spot_ts is None:
        return None
    if (now - spot_ts) > max_age_seconds:
        return None
    return {"spot": spot, "spot_ts": spot_ts}


def is_ws_running() -> bool:
    return _ws_running


def get_ws_status() -> Dict[str, Any]:
    """Read-only snapshot of current WS state. Single source: spot, spot_age_s."""
    now = time.time()
    with _lock:
        assets = {}
        for key, entry in list(_global_spot_prices.items()):
            spot = entry.get("spot")
            spot_ts = entry.get("spot_ts")
            assets[key] = {
                "spot": spot,
                "spot_age_s": round(now - spot_ts, 2) if spot_ts is not None else None,
            }
    return {"running": _ws_running, "assets": assets}


def _coinbase_ws_url_and_channel_key() -> tuple[str, str]:
    """Return (url, channel_key). channel_key is 'type' for Exchange, 'channel' for Advanced Trade."""
    if os.environ.get("COINBASE_WS_FEED") == "advanced_trade":
        return "wss://advanced-trade-ws.coinbase.com", "channel"
    return "wss://ws-feed.exchange.coinbase.com", "type"


async def _coinbase_ws_loop() -> None:
    try:
        import websockets
    except ImportError:
        return
    products = COINBASE_TICKER_PRODUCTS
    asset_map = COINBASE_PRODUCT_TO_ASSET
    url, channel_key = _coinbase_ws_url_and_channel_key()
    use_advanced_trade = "advanced-trade-ws" in url
    global _cb_ws
    backoff = 1.0
    while True:
        try:
            async with websockets.connect(
                url, ping_interval=20, ping_timeout=10, close_timeout=5, ssl=_ws_ssl_context()
            ) as ws:
                if backoff > 1.0:
                    logger.info("[oracle_ws] Coinbase reconnected.")
                backoff = 1.0
                _cb_ws = ws
                try:
                    if use_advanced_trade:
                        sub = {"type": "subscribe", "product_ids": products, "channel": "ticker"}
                    else:
                        sub = {"type": "subscribe", "product_ids": products, "channels": ["ticker"]}
                    await ws.send(json.dumps(sub))
                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(data, dict):
                            continue
                        try:
                            if use_advanced_trade:
                                if data.get(channel_key) != "ticker":
                                    continue
                                events = data.get("events") or []
                                if not events:
                                    continue
                                tickers = (events[0].get("tickers") or []) if isinstance(events[0], dict) else []
                                if not tickers:
                                    continue
                                t = tickers[0] if isinstance(tickers[0], dict) else {}
                                price_s = t.get("price")
                                product = t.get("product_id")
                            else:
                                if data.get(channel_key) != "ticker":
                                    continue
                                price_s = data.get("price")
                                product = data.get("product_id")
                            if price_s and product:
                                try:
                                    price = float(price_s)
                                except (TypeError, ValueError):
                                    continue
                                asset_key = asset_map.get(product, "BTC")
                                _set_spot_price(asset_key, price)
                        except Exception as inner_e:
                            logger.error("[oracle_ws] Coinbase message processing error: %s; continuing", inner_e)
                            continue
                finally:
                    _cb_ws = None
        except Exception as e:
            logger.warning(
                "[oracle_ws] Coinbase connection lost (%s); reconnecting in %.0fs...",
                e,
                max(backoff, 5),
            )
            await asyncio.sleep(max(backoff, 5))
            backoff = min(backoff * 1.5, 60.0)
            continue
        await asyncio.sleep(backoff)
        backoff = min(backoff * 1.5, 60.0)


async def _close_oracle_connections() -> None:
    """Close WebSocket so run_until_complete can exit. Call from loop thread."""
    global _cb_ws
    if _cb_ws is not None:
        try:
            await _cb_ws.close()
        except Exception:
            pass
    _cb_ws = None


async def _run_ws_loop() -> None:
    """Run the single spot oracle (Coinbase). To use Kraken, swap to _kraken_ws_loop or run both and set spot from preferred source."""
    await _coinbase_ws_loop()


def _run_loop_in_thread(loop: asyncio.AbstractEventLoop) -> None:
    global _ws_running
    _ws_running = True
    try:
        loop.run_until_complete(_run_ws_loop())
    except Exception as e:
        logger.exception("[oracle_ws] WS thread crashed: %s", e)
    finally:
        _ws_running = False


def start_ws_oracles() -> None:
    """Start the spot WebSocket oracle (Coinbase) in a background thread. Idempotent."""
    global _loop, _ws_task
    logger.info("[oracle_ws] start_ws_oracles() entered.")
    if _loop is not None and _ws_task is not None:
        logger.info("[oracle_ws] WebSocket oracle already running; skipping start.")
        return
    try:
        import websockets
    except ImportError:
        logger.warning("[oracle_ws] websockets not installed; pip install websockets. WS oracle disabled.")
        return
    try:
        _loop = asyncio.new_event_loop()
        _ws_task = threading.Thread(target=_run_loop_in_thread, args=(_loop,), daemon=True)
        _ws_task.start()
        logger.info("[oracle_ws] WebSocket spot oracle (Coinbase) started.")
    except Exception as e:
        logger.exception("[oracle_ws] Failed to start WS oracle: %s", e)
        _loop, _ws_task = None, None


def stop_ws_oracles() -> None:
    """Stop the WebSocket oracle: close connection and join the thread (prevents zombie processes)."""
    global _loop, _ws_task
    if _loop is None:
        return
    try:
        def _schedule_close():
            asyncio.ensure_future(_close_oracle_connections(), loop=_loop)
        _loop.call_soon_threadsafe(_schedule_close)
    except Exception:
        pass
    task = _ws_task
    _ws_task = None
    _loop = None
    if task is not None and task.is_alive():
        task.join(timeout=10)

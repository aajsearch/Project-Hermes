# Oracle WebSocket (Kraken + Coinbase) — Extracted Details

Source: `bot/oracle_ws_manager.py` (and `tools/check_oracle_ws.py` where noted).

---

## 1. Exact WebSocket URLs

| Feed | URL |
|------|-----|
| **Kraken** | `wss://ws.kraken.com` |
| **Coinbase (default)** | `wss://ws-feed.exchange.coinbase.com` |
| **Coinbase (Advanced Trade)** | `wss://advanced-trade-ws.coinbase.com` |

Advanced Trade is used only when `COINBASE_WS_FEED=advanced_trade`.

---

## 2. Exact subscribe JSON

### Kraken

```json
{"event": "subscribe", "pair": ["XBT/USD", "ETH/USD", "SOL/USD", "XRP/USD"], "subscription": {"name": "ticker"}}
```

Code (line ~220):

```python
sub = {"event": "subscribe", "pair": pairs, "subscription": {"name": "ticker"}}
await ws.send(json.dumps(sub))
```

`pairs` = `KRAKEN_TICKER_PAIRS` = `["XBT/USD", "ETH/USD", "SOL/USD", "XRP/USD"]`.

### Coinbase (Exchange feed — default)

```json
{"type": "subscribe", "product_ids": ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD"], "channels": ["ticker"]}
```

Code (line ~290):

```python
sub = {"type": "subscribe", "product_ids": products, "channels": ["ticker"]}
```

### Coinbase (Advanced Trade)

```json
{"type": "subscribe", "product_ids": ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD"], "channel": "ticker"}
```

Code (line ~287):

```python
sub = {"type": "subscribe", "product_ids": products, "channel": "ticker"}
```

`products` = `COINBASE_TICKER_PRODUCTS` = `["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD"]`.

Channel key: Exchange feed uses `"channels": ["ticker"]`; Advanced Trade uses `"channel": "ticker"` (and message parsing uses `channel_key` = `"channel"` vs `"type"` for incoming ticker messages).

---

## 3. Ticker symbols used

| Oracle | Format | Pairs/Products |
|--------|--------|-----------------|
| **Kraken** | `XBT/USD`, `ETH/USD`, `SOL/USD`, `XRP/USD` | BTC → XBT/USD (Kraken uses XBT for Bitcoin) |
| **Coinbase** | `BTC-USD`, `ETH-USD`, `SOL-USD`, `XRP-USD` | Hyphenated, no slash |

Mapping:

- `_asset_to_kraken_pair("btc")` → `"XBT/USD"`; ETH/SOL/XRP → `"{A}/USD"`.
- `_asset_to_coinbase_product("btc")` → `"BTC-USD"`; others → `"{A}-USD"`.

---

## 4. COINBASE_WS_FEED environment variable

- **Value checked:** `os.environ.get("COINBASE_WS_FEED") == "advanced_trade"`.
- **Default:** If unset or any other value → Exchange feed (`wss://ws-feed.exchange.coinbase.com`, `channels`/`type`).
- **When `advanced_trade`:** URL = `wss://advanced-trade-ws.coinbase.com`, channel key = `"channel"` for incoming messages.

---

## 5. Full connection code block

### Kraken (lines 215–217)

```python
async with websockets.connect(
    url, ping_interval=20, ping_timeout=10, close_timeout=5, ssl=_ws_ssl_context()
) as ws:
```

`url` = `"wss://ws.kraken.com"`.

### Coinbase (lines 281–283)

```python
async with websockets.connect(
    url, ping_interval=20, ping_timeout=10, close_timeout=5, ssl=_ws_ssl_context()
) as ws:
```

`url` from `_coinbase_ws_url_and_channel_key()` (Exchange or Advanced Trade).

### SSL context (lines 31–40)

```python
def _ws_ssl_context() -> ssl.SSLContext:
    try:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    except AttributeError:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx
```

---

## 6. Log lines (connect / subscribe / first price / errors)

| Log | Location | Message |
|-----|----------|---------|
| Start entry | `start_ws_oracles()` | `[oracle_ws] start_ws_oracles() entered.` |
| Already running | `start_ws_oracles()` | `[oracle_ws] WebSocket oracles already running; skipping start.` |
| First Kraken price | `_set_kraken_price()` | `[oracle_ws] First Kraken price received for %s: %s` (asset_key, price) |
| First Coinbase price | `_set_cb_price()` | `[oracle_ws] First Coinbase price received for %s: %s` (asset_key, price) |
| Kraken message error | `_kraken_ws_loop()` | `[oracle_ws] Kraken message processing error: %s; continuing` |
| Coinbase message error | `_coinbase_ws_loop()` | `[oracle_ws] Coinbase message processing error: %s; continuing` |
| Kraken connection failed | `_kraken_ws_loop()` except | `[Oracle FATAL] Kraken connection failed: {e}` (with exc_info=True) |
| Coinbase connection failed | `_coinbase_ws_loop()` except | `[Oracle FATAL] Coinbase connection failed: {e}` (with exc_info=True) |
| WS thread crashed | `_run_loop_in_thread()` except | `[oracle_ws] WS thread crashed: %s` |
| websockets not installed | `start_ws_oracles()` / loops | `[oracle_ws] websockets package not installed; run pip install websockets. WS oracles disabled.` (or "websockets not installed") |
| Failed to start | `start_ws_oracles()` except | `[oracle_ws] Failed to start WS oracles: %s` |
| Started | `start_ws_oracles()` | `[oracle_ws] WebSocket spot oracles (Kraken + Coinbase) started.` |

There are **no** explicit "Connected successfully" or "Subscribe sent" logs in `oracle_ws_manager.py`; only first-price and errors.

---

## 7. Heartbeat / ping handling

- **Kraken:** Incoming `event == "heartbeat"` is ignored (no log):

  ```python
  if data.get("event") == "heartbeat":
      continue
  ```

- **Kraken:** Incoming `status == "subscribed"` is ignored (no log):

  ```python
  if data.get("status") == "subscribed":
      continue
  ```

- **Ping/pong:** Handled by `websockets` library: `ping_interval=20`, `ping_timeout=10`. No custom heartbeat log in this module.

---

## 8. Condition that triggers fallback to REST for spot

In `bot/pipeline/data_layer.py`, `_get_spot_prices()`:

1. Sets `self._last_spot_source = "REST"` by default.
2. If `is_ws_running()` and `get_safe_spot_prices_sync(asset, max_age_seconds=5.0, require_both=False)` returns data with at least one fresh price (kraken or cb within `max_age_seconds`) → sets `_last_spot_source = "WS"` and uses that.
3. Otherwise (WS not running, or no data, or data older than 5s) → keeps REST and fills missing prices via:
   - Kraken REST (`KrakenClient().latest_btc_price()` etc.)
   - Coinbase REST (`https://api.coinbase.com/v2/prices/{symbol}/spot`).

So fallback to REST happens when:

- Oracle WS is not running, or
- No data for that asset in `_global_spot_prices`, or
- Both `kraken_ts` and `cb_ts` are either missing or older than `max_age_seconds` (5.0 in the pipeline).

---

## 9. Exceptions, close codes, subscriptionStatus

- **Exceptions:** Caught in each loop as `except Exception as e`, logged with `logger.error(..., exc_info=True)` for connection failures; inner message parsing errors logged as `[oracle_ws] ... message processing error`.
- **Close codes:** Not explicitly logged; any close/exception from `async with websockets.connect(...)` or `async for raw in ws` surfaces as the exception in the `except Exception as e` block.
- **subscriptionStatus / status "subscribed":** Kraken `status == "subscribed"` is skipped (no log). Coinbase does not have an explicit "subscriptionStatus" log; only ticker messages are processed.

---

## 10. Python and websockets version

- **requirements.txt:** `websockets>=12.0`.
- **Python:** Not pinned in this repo; project comment says "Python 3.11+".

To get exact versions at runtime:

```bash
python3 --version
python3 -c "import websockets; print(websockets.__version__)"
```

**Recorded at extract time:** Python 3.9.6, websockets 15.0.1.

---

## 30-second diagnostic run (BTC + ETH)

Command run: start oracle WS oracles, wait 30s with DEBUG logging, print status each second.

**Result:** Both Kraken and Coinbase connected and received prices. No connection or subscription errors.

### Log summary (first ~20 lines + first-price + heartbeats)

- `[oracle_ws] start_ws_oracles() entered.`
- `[oracle_ws] WebSocket spot oracles (Kraken + Coinbase) started.`
- **Coinbase** connected first: `GET / HTTP/1.1` → `Host: ws-feed.exchange.coinbase.com` → `101 Switching Protocols` → `connection is OPEN`.
- **Subscribe sent (Coinbase):** `{"type": "subscribe", "product_ids": ["BTC-USD", ...], "channels": ["ticker"]}` [106 bytes].
- **Coinbase:** `< TEXT '{"type":"subscriptions","channels":[...]}'` then ticker messages; `[oracle_ws] First Coinbase price received for BTC: 70710.95`, then SOL, ETH, XRP.
- **Kraken** connected: `Host: ws.kraken.com` → `101 Switching Protocols` → `connection is OPEN`.
- **Kraken:** `< TEXT '{"event":"systemStatus",...}'` then `> TEXT '{"event": "subscribe", "pair": ["XBT/USD", "ETH/USD", ...], "subscription": {"name": "ticker"}}'` [112 bytes].
- **Kraken:** subscription confirmations (channelID/channelName/event) then ticker arrays; `[oracle_ws] First Kraken price received for BTC: 70700.0`, SOL, XRP, ETH.
- **Heartbeats:** Kraken `{"event":"heartbeat"}` every ~1s; no log in oracle_ws (skipped with `continue`).
- **Keepalive:** `websockets` library sent PING / received PONG (`sent keepalive ping`, `received keepalive pong`).

### Final status (after 30s)

```text
running=True
BTC: kraken=70703.0 (age_s=10.06), cb=70701.97 (age_s=0.02)
ETH: kraken=2077.87 (age_s=28.43), cb=2077.79 (age_s=0.64)
SOL: kraken=87.03, cb=87.02
XRP: kraken=1.39096, cb=1.3909
```

No `[Oracle FATAL]`, no `[oracle_ws] ... message processing error`, no close codes. COINBASE_WS_FEED was unset → Exchange feed used (`wss://ws-feed.exchange.coinbase.com`).

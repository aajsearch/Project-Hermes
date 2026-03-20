import os
import math
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List, Optional, Tuple
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

from dotenv import load_dotenv

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest

from alpaca.data.historical.option import OptionHistoricalDataClient
from alpaca.data.requests import OptionChainRequest
from alpaca.data.enums import OptionsFeed  # opra / indicative


load_dotenv()
API_KEY = os.getenv("ALPACA_API_KEY")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

# --- Your overlay constraints (match profile.yaml) ---
DTE_MIN = 60
DTE_MAX = 120
DELTA_MIN = 0.40
DELTA_MAX = 0.55
IV_MAX = 0.50
SPREAD_PCT_MAX = 0.05

TARGET_DELTA = 0.48


UNDERLYINGS = ["SPY", "QQQ", "AAPL", "AMZN"]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def safe_get(obj: Any, path: List[str], default=None):
    cur = obj
    for p in path:
        if cur is None:
            return default
        # alpaca-py model objects usually support attribute access
        if hasattr(cur, p):
            cur = getattr(cur, p)
        elif isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return default
    return cur if cur is not None else default


def get_last_close(stock_client: StockHistoricalDataClient, symbol: str) -> Optional[float]:
    end = utc_now()
    start = end - timedelta(days=14)

    req = StockBarsRequest(
        symbol_or_symbols=[symbol],
        timeframe=TimeFrame(5, TimeFrameUnit.Minute),
        start=start,
        end=end,
        limit=5000,
        feed="iex",
    )
    bars = stock_client.get_stock_bars(req).df
    if bars is None or bars.empty:
        return None

    if hasattr(bars.index, "names") and len(bars.index.names) == 2:
        bars = bars.reset_index()
        bars = bars[bars["symbol"] == symbol].copy()

    bars = bars.sort_values("timestamp")
    return float(bars.iloc[-1]["close"])


def contract_dte_from_symbol(sym: str) -> Optional[int]:
    """
    Alpaca option symbols are typically OCC-like: e.g. SPY240524C00528000
    We parse YYMMDD from chars after underlying.
    This parser is best-effort and will safely return None if it can't parse.
    """
    # find first digit run of length 6 after underlying portion
    # e.g. "SPY240524C..." -> "240524"
    import re
    m = re.search(r"(\d{6})([CP])", sym)
    if not m:
        return None
    yymmdd = m.group(1)
    yy = int(yymmdd[0:2])
    mm = int(yymmdd[2:4])
    dd = int(yymmdd[4:6])
    # OCC year is 20YY
    exp = date(2000 + yy, mm, dd)
    return (exp - date.today()).days


def spread_pct(bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    if bid is None or ask is None:
        return None
    if bid <= 0 or ask <= 0:
        return None
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return None
    return (ask - bid) / mid


def try_get_option_chain(
    option_client: OptionHistoricalDataClient,
    underlying: str,
) -> Dict[str, Any]:
    """
    Try OPRA first; if you don't have it, fall back to INDICATIVE.
    """
    # Try OPRA
    try:
        req = OptionChainRequest(underlying_symbol=underlying, feed=OptionsFeed.OPRA)
        return option_client.get_option_chain(req)
    except Exception as e_opra:
        print(f"[{underlying}] OPRA feed failed (likely no subscription). Falling back to INDICATIVE. err={e_opra}")
        req = OptionChainRequest(underlying_symbol=underlying, feed=OptionsFeed.INDICATIVE)
        return option_client.get_option_chain(req)


def pick_calls_from_chain(chain_resp: Any) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Normalize Alpaca response into a list[(contract_symbol, snapshot_dictlike)].
    alpaca-py may return wrapped objects or dicts depending on raw_data settings.
    We'll handle common shapes.
    """
    # Common shapes:
    # - dict: { "SPY...": <snapshot>, "SPY...": <snapshot>, ... }
    # - OptionChain: has attribute .snapshots or is itself dict-like
    if isinstance(chain_resp, dict):
        return [(k, v) for k, v in chain_resp.items()]

    # best-effort attribute names
    for attr in ["snapshots", "data", "results"]:
        if hasattr(chain_resp, attr):
            v = getattr(chain_resp, attr)
            if isinstance(v, dict):
                return [(k, vv) for k, vv in v.items()]

    # fallback: try to iterate
    try:
        items = list(chain_resp.items())  # type: ignore
        return [(k, v) for k, v in items]
    except Exception:
        return []


def is_call_symbol(sym: str) -> bool:
    return "C" in sym  # best-effort; OCC symbols include 'C' or 'P'


def main():
    if not API_KEY or not SECRET_KEY:
        raise ValueError("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY in .env")

    stock_client = StockHistoricalDataClient(API_KEY, SECRET_KEY)
    option_client = OptionHistoricalDataClient(API_KEY, SECRET_KEY)

    print("=== Options Overlay Smoke Test (weekend OK) ===")
    print(f"Constraints: DTE[{DTE_MIN},{DTE_MAX}] delta[{DELTA_MIN},{DELTA_MAX}] IV<{IV_MAX} spread%<{SPREAD_PCT_MAX}\n")

    for underlying in UNDERLYINGS:
        print(f"\n--- {underlying} ---")

        px = get_last_close(stock_client, underlying)
        if px is None:
            print("Underlying price: (no bars returned)")
        else:
            print(f"Underlying last close (from latest bars): {px:.2f}")

        try:
            chain = try_get_option_chain(option_client, underlying)
        except Exception as e:
            print("Option chain fetch failed:", e)
            continue

        items = pick_calls_from_chain(chain)
        if not items:
            print("No chain items returned (or could not parse response shape).")
            continue

        candidates = []
        for sym, snap in items:
            if not is_call_symbol(sym):
                continue

            dte = contract_dte_from_symbol(sym)
            if dte is None or dte < DTE_MIN or dte > DTE_MAX:
                continue

            # quote
            bid = safe_get(snap, ["latest_quote", "bid_price"])
            ask = safe_get(snap, ["latest_quote", "ask_price"])
            sp = spread_pct(bid, ask)
            if sp is None or sp > SPREAD_PCT_MAX:
                continue

            # greeks & iv
            delta = safe_get(snap, ["greeks", "delta"])
            iv = safe_get(snap, ["implied_volatility"])  # many snapshots include this
            if delta is None:
                continue
            if not (DELTA_MIN <= float(delta) <= DELTA_MAX):
                continue
            if iv is not None and float(iv) > IV_MAX:
                continue

            mid = None
            if bid is not None and ask is not None:
                mid = (float(bid) + float(ask)) / 2.0

            score = abs(float(delta) - TARGET_DELTA) + (sp * 0.5)
            candidates.append({
                "symbol": sym,
                "dte": dte,
                "delta": float(delta),
                "iv": float(iv) if iv is not None else None,
                "bid": float(bid) if bid is not None else None,
                "ask": float(ask) if ask is not None else None,
                "mid": mid,
                "spread_pct": sp,
                "score": score
            })

        if not candidates:
            print("No contracts passed filters (this is OK on weekends if feed/greeks are limited).")
            continue

        candidates.sort(key=lambda x: x["score"])
        best = candidates[0]

        print("Best candidate CALL:")
        print(
            f"  {best['symbol']} | DTE={best['dte']} | delta={best['delta']:.3f} | "
            f"IV={best['iv'] if best['iv'] is not None else 'NA'} | "
            f"bid/ask={best['bid']}/{best['ask']} | spread%={best['spread_pct']*100:.2f}%"
        )

        print("\nTop 5:")
        for c in candidates[:5]:
            print(
                f"  {c['symbol']} | DTE={c['dte']} | delta={c['delta']:.3f} | "
                f"IV={c['iv'] if c['iv'] is not None else 'NA'} | spread%={c['spread_pct']*100:.2f}%"
            )

    print("\n✅ Done. If this prints candidates, you're good for Monday.")


if __name__ == "__main__":
    main()

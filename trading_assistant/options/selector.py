# options/selector.py
import re
from dataclasses import dataclass
from datetime import date
from typing import Optional, List, Dict, Any

from alpaca.data.historical.option import OptionHistoricalDataClient
from alpaca.data.requests import OptionChainRequest
from alpaca.data.enums import OptionsFeed

@dataclass
class OptionCandidate:
    symbol: str
    dte: int
    delta: float
    iv: Optional[float]
    bid: float
    ask: float
    spread_pct: float
    score: float

def _contract_dte_from_symbol(sym: str) -> Optional[int]:
    m = re.search(r"(\d{6})([CP])", sym)
    if not m:
        return None
    yymmdd = m.group(1)
    yy, mm, dd = int(yymmdd[:2]), int(yymmdd[2:4]), int(yymmdd[4:6])
    exp = date(2000 + yy, mm, dd)
    return (exp - date.today()).days

def _safe_get(obj: Any, path: List[str], default=None):
    cur = obj
    for p in path:
        if cur is None:
            return default
        if hasattr(cur, p):
            cur = getattr(cur, p)
        elif isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return default
    return cur if cur is not None else default

def _spread_pct(bid: float, ask: float) -> Optional[float]:
    if bid <= 0 or ask <= 0:
        return None
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return None
    return (ask - bid) / mid

def _get_chain(option_client: OptionHistoricalDataClient, underlying: str) -> Any:
    print(f"[{underlying}] fetching option chain...", flush=True)
    # Prefer OPRA if signed; otherwise fall back to indicative
    try:
        return option_client.get_option_chain(OptionChainRequest(underlying_symbol=underlying, feed=OptionsFeed.OPRA))
    except Exception:
        return option_client.get_option_chain(OptionChainRequest(underlying_symbol=underlying, feed=OptionsFeed.INDICATIVE))

def pick_best_call(
    option_client: OptionHistoricalDataClient,
    underlying: str,
    dte_min: int,
    dte_max: int,
    delta_min: float,
    delta_max: float,
    iv_max: float,
    spread_pct_max: float,
    target_delta: float = 0.48,
) -> Optional[OptionCandidate]:

    chain = _get_chain(option_client, underlying)

    # Normalize into items
    if isinstance(chain, dict):
        items = list(chain.items())
    elif hasattr(chain, "snapshots") and isinstance(getattr(chain, "snapshots"), dict):
        items = list(getattr(chain, "snapshots").items())
    else:
        try:
            items = list(chain.items())
        except Exception:
            return None

    best: Optional[OptionCandidate] = None

    for sym, snap in items:
        # OCC: ...YYMMDD + C|P + 8-digit strike; right is at -9
        if len(sym) < 9 or sym[-9] != "C":
            continue

        dte = _contract_dte_from_symbol(sym)
        if dte is None or dte < dte_min or dte > dte_max:
            continue

        bid = _safe_get(snap, ["latest_quote", "bid_price"])
        ask = _safe_get(snap, ["latest_quote", "ask_price"])
        if bid is None or ask is None:
            continue

        sp = _spread_pct(float(bid), float(ask))
        if sp is None or sp > spread_pct_max:
            continue

        delta = _safe_get(snap, ["greeks", "delta"])
        if delta is None:
            continue
        delta = float(delta)
        if not (delta_min <= delta <= delta_max):
            continue

        iv = _safe_get(snap, ["implied_volatility"])
        ivf = float(iv) if iv is not None else None
        if ivf is not None and ivf > iv_max:
            continue

        # score: closest delta + tight spread preference
        score = abs(delta - target_delta) + (sp * 0.5)

        cand = OptionCandidate(
            symbol=sym,
            dte=dte,
            delta=delta,
            iv=ivf,
            bid=float(bid),
            ask=float(ask),
            spread_pct=sp,
            score=score,
        )

        if best is None or cand.score < best.score:
            best = cand

    return best


def pick_best_put(
    option_client: OptionHistoricalDataClient,
    underlying: str,
    dte_min: int,
    dte_max: int,
    delta_min: float,
    delta_max: float,
    iv_max: float,
    spread_pct_max: float,
    target_delta: float = -0.48,
) -> Optional[OptionCandidate]:
    """
    Mirror of pick_best_call for PUTs. Put deltas are negative; we use
    delta_min/delta_max as bounds on abs(delta) and require delta < 0.
    So effective range: delta in [-delta_max, -delta_min], e.g. [-0.55, -0.40].
    """
    chain = _get_chain(option_client, underlying)

    if isinstance(chain, dict):
        items = list(chain.items())
    elif hasattr(chain, "snapshots") and isinstance(getattr(chain, "snapshots"), dict):
        items = list(getattr(chain, "snapshots").items())
    else:
        try:
            items = list(chain.items())
        except Exception:
            return None

    best: Optional[OptionCandidate] = None

    for sym, snap in items:
        # OCC: ...YYMMDD + C|P + 8-digit strike; right is at -9
        if len(sym) < 9 or sym[-9] != "P":
            continue
        dte = _contract_dte_from_symbol(sym)
        if dte is None or dte < dte_min or dte > dte_max:
            continue

        bid = _safe_get(snap, ["latest_quote", "bid_price"])
        ask = _safe_get(snap, ["latest_quote", "ask_price"])
        if bid is None or ask is None:
            continue

        sp = _spread_pct(float(bid), float(ask))
        if sp is None or sp > spread_pct_max:
            continue

        delta = _safe_get(snap, ["greeks", "delta"])
        if delta is None:
            continue
        delta = float(delta)
        # Puts: delta negative; we want delta in [-delta_max, -delta_min] (e.g. -0.55 to -0.40)
        if not (delta < 0 and (-delta_max <= delta <= -delta_min)):
            continue

        iv = _safe_get(snap, ["implied_volatility"])
        ivf = float(iv) if iv is not None else None
        if ivf is not None and ivf > iv_max:
            continue

        score = abs(delta - target_delta) + (sp * 0.5)

        cand = OptionCandidate(
            symbol=sym,
            dte=dte,
            delta=delta,
            iv=ivf,
            bid=float(bid),
            ask=float(ask),
            spread_pct=sp,
            score=score,
        )

        if best is None or cand.score < best.score:
            best = cand

    return best

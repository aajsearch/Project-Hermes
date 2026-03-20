from __future__ import annotations

import os
import logging
import re
import bisect
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from alpaca.data.enums import OptionsFeed
from alpaca.data.historical.option import OptionHistoricalDataClient
from alpaca.data.requests import OptionChainRequest

from bot.alpaca_put_spread.config import AlpacaPutSpreadConfig
from bot.alpaca_put_spread.option_symbol import parse_occ_option_symbol
from bot.alpaca_put_spread.put_spread_logic import (
    PutSpreadCandidate,
    entry_condition_met,
    net_credit_mid,
)

logger = logging.getLogger(__name__)


_contract_dte_re = re.compile(r"(\d{6})([CP])")


def _contract_dte_from_symbol(sym: str) -> Optional[int]:
    m = _contract_dte_re.search(sym or "")
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


def _get_chain_silent(option_client: OptionHistoricalDataClient, underlying: str) -> Any:
    # Prefer OPRA (signed/official), otherwise fall back to indicative.
    try:
        return option_client.get_option_chain(
            OptionChainRequest(underlying_symbol=underlying, feed=OptionsFeed.OPRA)
        )
    except Exception:
        return option_client.get_option_chain(
            OptionChainRequest(underlying_symbol=underlying, feed=OptionsFeed.INDICATIVE)
        )


def _normalize_chain_items(chain: Any) -> List[Tuple[str, Any]]:
    if isinstance(chain, dict):
        return list(chain.items())
    if hasattr(chain, "snapshots") and isinstance(getattr(chain, "snapshots"), dict):
        return list(getattr(chain, "snapshots").items())
    try:
        return list(chain.items())
    except Exception:
        return []


def _pick_best_put_from_chain(
    *,
    chain_items: List[Tuple[str, Any]],
    dte_min: int,
    dte_max: int,
    delta_abs_min: float,
    delta_abs_max: float,
    iv_max: float,
    spread_pct_max: float,
    target_delta: float,
) -> Optional[Tuple[str, Dict[str, float]]]:
    best: Optional[Tuple[str, Dict[str, float]]] = None
    for sym, snap in chain_items:
        # OCC: ...YYMMDD + P + 8-digit strike; right is at -9
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
        delta_f = float(delta)
        # Alpaca may provide put delta as a positive magnitude; use abs(delta) for filters.
        abs_delta = abs(delta_f)
        if not (delta_abs_min <= abs_delta <= delta_abs_max):
            continue

        iv = _safe_get(snap, ["implied_volatility"])
        ivf = float(iv) if iv is not None else None
        if ivf is not None and ivf > iv_max:
            continue

        score = abs(abs_delta - abs(target_delta)) + (sp * 0.5)
        cand_meta = {
            "bid": float(bid),
            "ask": float(ask),
            "delta": abs_delta,
            "iv": float(ivf) if ivf is not None else 0.0,
            "spread_pct": float(sp),
            "dte": float(dte),
            "score": float(score),
        }
        if best is None or cand_meta["score"] < best[1]["score"]:
            best = (sym, cand_meta)

    return best


def select_bull_put_credit_spread(
    *,
    underlying: str,
    option_data_client: OptionHistoricalDataClient,
    cfg: AlpacaPutSpreadConfig,
    underlying_spot_mid: Optional[float] = None,
) -> Optional[PutSpreadCandidate]:
    """
    MVP candidate selection (silent):
      - Fetch option chain once
      - Pick long put + short put from same chain using delta/IV/spread filters
      - Enforce bull put credit spread ordering and width
      - Compute net credit mid = mid(short) - mid(long)
      - Require entry condition vs cfg.target_credit
    """
    debug = os.getenv("ALPACA_PUT_SPREAD_DEBUG", "").strip().lower() in ("1", "true", "yes", "y", "on")

    chain = _get_chain_silent(option_data_client, underlying)
    items = _normalize_chain_items(chain)
    if not items:
        return None

    # Basic counts for debugging filter tightness.
    if debug:
        put_syms = [sym for sym, _ in items if isinstance(sym, str) and len(sym) >= 9 and sym[-9] == "P"]
        puts_total = len(put_syms)
        dte_filtered_total = 0
        long_delta_total = 0
        short_delta_total = 0
        spread_ok_total = 0
        abs_delta_seen = 0
        min_abs_delta = None
        max_abs_delta = None
        # We do a single pass over items with minimal parsing.
        for sym, snap in items:
            if not (isinstance(sym, str) and len(sym) >= 9 and sym[-9] == "P"):
                continue
            dte = _contract_dte_from_symbol(sym)
            if dte is None or dte < cfg.dte_min or dte > cfg.dte_max:
                continue
            dte_filtered_total += 1
            bid = _safe_get(snap, ["latest_quote", "bid_price"])
            ask = _safe_get(snap, ["latest_quote", "ask_price"])
            if bid is None or ask is None:
                continue
            sp = _spread_pct(float(bid), float(ask))
            if sp is None:
                continue
            if sp <= cfg.spread_pct_max:
                spread_ok_total += 1
            delta = _safe_get(snap, ["greeks", "delta"])
            if delta is None:
                continue
            delta_f = float(delta)
            # Alpaca may provide put delta as a positive magnitude; use abs(delta) for filters.
            abs_delta = abs(delta_f)
            abs_delta_seen += 1
            min_abs_delta = abs_delta if min_abs_delta is None else min(min_abs_delta, abs_delta)
            max_abs_delta = abs_delta if max_abs_delta is None else max(max_abs_delta, abs_delta)
            if cfg.long_delta_abs_min <= abs_delta <= cfg.long_delta_abs_max:
                long_delta_total += 1
            if cfg.short_delta_abs_min <= abs_delta <= cfg.short_delta_abs_max:
                short_delta_total += 1

        logger.info(
            "[%s] debug: chain_items=%d puts_total=%d dte_filtered=%d spread_ok<=%.3f long_delta_cnt=%d short_delta_cnt=%d abs_delta_seen=%d abs_delta_range=[%.3f..%.3f] target_credit=%.4f",
            underlying,
            len(items),
            puts_total,
            dte_filtered_total,
            cfg.spread_pct_max,
            long_delta_total,
            short_delta_total,
            abs_delta_seen,
            float(min_abs_delta) if min_abs_delta is not None else -1.0,
            float(max_abs_delta) if max_abs_delta is not None else -1.0,
            cfg.target_credit,
        )

    # If greeks.delta is missing for the whole chain, we can’t delta-filter.
    # Fallback: select by strike extremes within the DTE/spread/IV constraints.
    # This is intentionally simple for Phase-1 to unblock end-to-end execution.
    any_delta_seen = False
    put_candidates: List[Tuple[str, Dict[str, float]]] = []
    for sym, snap in items:
        if not (isinstance(sym, str) and len(sym) >= 9 and sym[-9] == "P"):
            continue
        dte = _contract_dte_from_symbol(sym)
        if dte is None or dte < cfg.dte_min or dte > cfg.dte_max:
            continue
        bid = _safe_get(snap, ["latest_quote", "bid_price"])
        ask = _safe_get(snap, ["latest_quote", "ask_price"])
        if bid is None or ask is None:
            continue
        sp = _spread_pct(float(bid), float(ask))
        if sp is None or sp > cfg.spread_pct_max:
            continue
        iv = _safe_get(snap, ["implied_volatility"])
        ivf = float(iv) if iv is not None else None
        if ivf is not None and ivf > cfg.iv_max:
            continue

        delta = _safe_get(snap, ["greeks", "delta"])
        if delta is not None:
            any_delta_seen = True

        parts = parse_occ_option_symbol(sym)
        if not parts:
            continue
        put_candidates.append(
            (
                sym,
                {
                    "bid": float(bid),
                    "ask": float(ask),
                    "strike": float(parts.strike),
                },
            )
        )

    def _fallback_by_strike() -> Optional[PutSpreadCandidate]:
        if not put_candidates:
            return None
        # Bull put credit sanity: short put should be OTM by at least min_short_otm_percent.
        candidates = put_candidates
        if underlying_spot_mid is not None and underlying_spot_mid > 0 and cfg.min_short_otm_percent > 0:
            max_short = float(underlying_spot_mid) * (1.0 - float(cfg.min_short_otm_percent))
            candidates = [p for p in put_candidates if float(p[1].get("strike", 0.0)) <= max_short]

        if not candidates:
            return None

        # Sort once so we can slice by strike range.
        candidates_sorted = sorted(candidates, key=lambda x: float(x[1]["strike"]))
        strikes = [float(meta["strike"]) for _, meta in candidates_sorted]

        width_min = float(cfg.put_spread_width_points_min)
        width_max = float(cfg.put_spread_width_points_max)

        # Iterate short strikes from largest->smallest and try all long strikes
        # whose width is within [width_min, width_max]. Pick the candidate whose
        # net credit is closest to target_credit (we track both: one that also
        # satisfies the entry_condition, and one closest overall for logging).
        best_candidate_ok: Optional[PutSpreadCandidate] = None
        best_score_ok: Optional[float] = None
        best_candidate_any: Optional[PutSpreadCandidate] = None
        best_score_any: Optional[float] = None
        for short_sym, short_meta in sorted(
            candidates_sorted, key=lambda x: float(x[1]["strike"]), reverse=True
        ):
            short_strike = float(short_meta["strike"])
            long_low = short_strike - width_max
            long_high = short_strike - width_min
            if long_high <= 0:
                continue

            left = bisect.bisect_left(strikes, long_low)
            right = bisect.bisect_right(strikes, long_high)
            long_candidates = candidates_sorted[left:right]
            if not long_candidates:
                continue

            short_parts = parse_occ_option_symbol(short_sym)
            if not short_parts:
                continue

            short_put_mid = (float(short_meta["bid"]) + float(short_meta["ask"])) / 2.0
            for long_sym, long_meta in long_candidates:
                long_strike = float(long_meta["strike"])
                if short_strike <= long_strike:
                    continue
                long_parts = parse_occ_option_symbol(long_sym)
                if not long_parts:
                    continue

                long_put_mid = (float(long_meta["bid"]) + float(long_meta["ask"])) / 2.0
                net_credit = net_credit_mid(short_put_mid=short_put_mid, long_put_mid=long_put_mid)

                score = abs(net_credit - cfg.target_credit)
                if best_candidate_any is None or score < float(best_score_any):
                    best_candidate_any = PutSpreadCandidate(
                        underlying=underlying,
                        long_put_symbol=long_sym,
                        short_put_symbol=short_sym,
                        long_put_mid=long_put_mid,
                        short_put_mid=short_put_mid,
                        entry_net_credit_mid=net_credit,
                        long_strike=long_strike,
                        short_strike=short_strike,
                    )
                    best_score_any = float(score)

                if entry_condition_met(
                    net_credit_mid_val=net_credit,
                    target_credit=cfg.target_credit,
                    entry_operator=cfg.entry_operator,
                ):
                    if best_candidate_ok is None or score < float(best_score_ok):
                        best_candidate_ok = PutSpreadCandidate(
                            underlying=underlying,
                            long_put_symbol=long_sym,
                            short_put_symbol=short_sym,
                            long_put_mid=long_put_mid,
                            short_put_mid=short_put_mid,
                            entry_net_credit_mid=net_credit,
                            long_strike=long_strike,
                            short_strike=short_strike,
                        )
                        best_score_ok = float(score)

        if debug and best_candidate_any is not None:
            # Shows the closest net credit achievable after min_short_otm_percent + width constraints,
            # even if it didn't meet the target_credit operator gate.
            width = float(best_candidate_any.short_strike) - float(best_candidate_any.long_strike)
            logger.info(
                "[%s] debug: closest_credit_after_otm=%.4f target_credit=%.4f op=%s short=%s long=%s width=%.2f min_short_otm_percent=%.4f",
                underlying,
                best_candidate_any.entry_net_credit_mid,
                cfg.target_credit,
                cfg.entry_operator,
                best_candidate_any.short_put_symbol,
                best_candidate_any.long_put_symbol,
                width,
                cfg.min_short_otm_percent,
            )

        return best_candidate_ok

    if not any_delta_seen:
        if debug:
            logger.info("[%s] debug: greeks.delta missing -> using strike-extremes fallback", underlying)
        return _fallback_by_strike()

    # Normal delta-based selection
    long_pick = _pick_best_put_from_chain(
        chain_items=items,
        dte_min=cfg.dte_min,
        dte_max=cfg.dte_max,
        delta_abs_min=cfg.long_delta_abs_min,
        delta_abs_max=cfg.long_delta_abs_max,
        iv_max=cfg.iv_max,
        spread_pct_max=cfg.spread_pct_max,
        target_delta=cfg.long_target_delta,
    )
    if not long_pick:
        return None
    short_pick = _pick_best_put_from_chain(
        chain_items=items,
        dte_min=cfg.dte_min,
        dte_max=cfg.dte_max,
        delta_abs_min=cfg.short_delta_abs_min,
        delta_abs_max=cfg.short_delta_abs_max,
        iv_max=cfg.iv_max,
        spread_pct_max=cfg.spread_pct_max,
        target_delta=cfg.short_target_delta,
    )
    if not short_pick:
        return None

    long_symbol, long_meta = long_pick
    short_symbol, short_meta = short_pick

    long_parts = parse_occ_option_symbol(long_symbol)
    short_parts = parse_occ_option_symbol(short_symbol)
    if not long_parts or not short_parts:
        logger.debug("[%s] Failed to parse OCC option symbols", underlying)
        return None

    # Bull put credit spread ordering: short strike must be higher than long strike
    if short_parts.strike <= long_parts.strike:
        return None

    # Bull put credit sanity: enforce short_put is OTM if we know underlying spot.
    if underlying_spot_mid is not None and underlying_spot_mid > 0:
        max_short = float(underlying_spot_mid) * (1.0 - float(cfg.min_short_otm_percent))
        if float(short_parts.strike) > max_short:
            return None

    width = float(short_parts.strike) - float(long_parts.strike)
    if width < float(cfg.put_spread_width_points_min) or width > float(cfg.put_spread_width_points_max):
        return None

    def _mid(bid: float, ask: float) -> float:
        return (float(bid) + float(ask)) / 2.0

    long_put_mid = _mid(long_meta["bid"], long_meta["ask"])
    short_put_mid = _mid(short_meta["bid"], short_meta["ask"])
    net_credit = net_credit_mid(short_put_mid=short_put_mid, long_put_mid=long_put_mid)

    if not entry_condition_met(
        net_credit_mid_val=net_credit,
        target_credit=cfg.target_credit,
        entry_operator=cfg.entry_operator,
    ):
        if debug:
            logger.info(
                "[%s] debug: best candidates fail target_credit. net_credit_mid=%.4f operator=%s target_credit=%.4f",
                underlying,
                net_credit,
                cfg.entry_operator,
                cfg.target_credit,
            )
        return None

    return PutSpreadCandidate(
        underlying=underlying,
        long_put_symbol=long_symbol,
        short_put_symbol=short_symbol,
        long_put_mid=long_put_mid,
        short_put_mid=short_put_mid,
        entry_net_credit_mid=net_credit,
        long_strike=long_parts.strike,
        short_strike=short_parts.strike,
    )


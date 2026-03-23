"""
Bear call credit spread (CCS) candidate selection from option chain (mirrors put_spread_selector).
"""
from __future__ import annotations

import bisect
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from alpaca.data.historical.option import OptionHistoricalDataClient

from bot.alpaca_put_spread.config import CallCreditSpreadStrategyConfig
from bot.alpaca_put_spread.option_symbol import parse_occ_option_symbol
from bot.alpaca_put_spread.pricing_logic import CallSpreadCandidate, entry_condition_met
from bot.alpaca_put_spread.put_spread_selector import (
    _contract_dte_from_symbol,
    _filter_chain_by_expiry,
    _get_chain_silent,
    _normalize_chain_items,
    _safe_get,
    _spread_pct,
)

logger = logging.getLogger(__name__)


def _pick_best_call_from_chain(
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
        delta_f = float(delta)
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


def select_bear_call_credit_spread(
    *,
    underlying: str,
    option_data_client: OptionHistoricalDataClient,
    cfg: CallCreditSpreadStrategyConfig,
    underlying_spot_mid: Optional[float] = None,
    chain: Optional[Any] = None,
) -> Optional[CallSpreadCandidate]:
    debug = os.getenv("ALPACA_PUT_SPREAD_DEBUG", "").strip().lower() in ("1", "true", "yes", "y", "on")

    if chain is None:
        chain = _get_chain_silent(option_data_client, underlying)
    items = _normalize_chain_items(chain)
    if not items:
        return None

    any_delta_seen = False
    call_candidates: List[Tuple[str, Dict[str, float]]] = []
    for sym, snap in items:
        if not (isinstance(sym, str) and len(sym) >= 9 and sym[-9] == "C"):
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
        call_candidates.append(
            (
                sym,
                {
                    "bid": float(bid),
                    "ask": float(ask),
                    "strike": float(parts.strike),
                },
            )
        )

    width_min = float(cfg.call_spread_width_points_min)
    width_max = float(cfg.call_spread_width_points_max)

    def _fallback_by_strike() -> Optional[CallSpreadCandidate]:
        if not call_candidates:
            return None
        candidates = call_candidates
        if underlying_spot_mid is not None and underlying_spot_mid > 0 and cfg.min_short_otm_percent > 0:
            min_short = float(underlying_spot_mid) * (1.0 + float(cfg.min_short_otm_percent))
            candidates = [p for p in call_candidates if float(p[1].get("strike", 0.0)) >= min_short]

        if not candidates:
            return None

        candidates_sorted = sorted(candidates, key=lambda x: float(x[1]["strike"]))
        strikes = [float(meta["strike"]) for _, meta in candidates_sorted]

        best_candidate_ok: Optional[CallSpreadCandidate] = None
        best_score_ok: Optional[float] = None
        best_candidate_any: Optional[CallSpreadCandidate] = None
        best_score_any: Optional[float] = None

        for short_sym, short_meta in candidates_sorted:
            short_strike = float(short_meta["strike"])
            long_low = short_strike + width_min
            long_high = short_strike + width_max
            left = bisect.bisect_left(strikes, long_low)
            right = bisect.bisect_right(strikes, long_high)
            long_slice = candidates_sorted[left:right]
            if not long_slice:
                continue

            short_call_mid = (float(short_meta["bid"]) + float(short_meta["ask"])) / 2.0
            for long_sym, long_meta in long_slice:
                long_strike = float(long_meta["strike"])
                if long_strike <= short_strike:
                    continue
                long_parts = parse_occ_option_symbol(long_sym)
                short_parts = parse_occ_option_symbol(short_sym)
                if not long_parts or not short_parts:
                    continue

                long_call_mid = (float(long_meta["bid"]) + float(long_meta["ask"])) / 2.0
                net_credit = float(short_call_mid) - float(long_call_mid)

                score = abs(net_credit - cfg.target_credit)
                if best_candidate_any is None or score < float(best_score_any):
                    best_candidate_any = CallSpreadCandidate(
                        underlying=underlying,
                        long_call_symbol=long_sym,
                        short_call_symbol=short_sym,
                        long_call_mid=long_call_mid,
                        short_call_mid=short_call_mid,
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
                        best_candidate_ok = CallSpreadCandidate(
                            underlying=underlying,
                            long_call_symbol=long_sym,
                            short_call_symbol=short_sym,
                            long_call_mid=long_call_mid,
                            short_call_mid=short_call_mid,
                            entry_net_credit_mid=net_credit,
                            long_strike=long_strike,
                            short_strike=short_strike,
                        )
                        best_score_ok = float(score)

        if debug and best_candidate_any is not None:
            width = float(best_candidate_any.long_strike) - float(best_candidate_any.short_strike)
            logger.info(
                "[%s] CCS debug fallback: closest_credit=%.4f target=%.4f short=%s long=%s width=%.2f",
                underlying,
                best_candidate_any.entry_net_credit_mid,
                cfg.target_credit,
                best_candidate_any.short_call_symbol,
                best_candidate_any.long_call_symbol,
                width,
            )

        return best_candidate_ok

    if not any_delta_seen:
        if debug:
            logger.info("[%s] CCS: greeks.delta missing -> strike fallback", underlying)
        return _fallback_by_strike()

    # Delta path: short leg first, then long only from the same expiration (vertical bear call).
    short_pick = _pick_best_call_from_chain(
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
        return _fallback_by_strike()

    short_symbol, short_meta = short_pick
    short_parts = parse_occ_option_symbol(short_symbol)
    if not short_parts:
        return _fallback_by_strike()

    same_exp = _filter_chain_by_expiry(items, short_parts.expiry_yyyymmdd)
    if not same_exp:
        return _fallback_by_strike()

    long_pick = _pick_best_call_from_chain(
        chain_items=same_exp,
        dte_min=cfg.dte_min,
        dte_max=cfg.dte_max,
        delta_abs_min=cfg.long_delta_abs_min,
        delta_abs_max=cfg.long_delta_abs_max,
        iv_max=cfg.iv_max,
        spread_pct_max=cfg.spread_pct_max,
        target_delta=cfg.long_target_delta,
    )
    if not long_pick:
        return _fallback_by_strike()

    long_symbol, long_meta = long_pick

    long_parts = parse_occ_option_symbol(long_symbol)
    if not long_parts or not short_parts:
        return None

    if short_parts.strike >= long_parts.strike:
        return None

    if underlying_spot_mid is not None and underlying_spot_mid > 0:
        min_short = float(underlying_spot_mid) * (1.0 + float(cfg.min_short_otm_percent))
        if float(short_parts.strike) < min_short:
            return None

    width = float(long_parts.strike) - float(short_parts.strike)
    if width < width_min or width > width_max:
        return None

    def _mid(bid: float, ask: float) -> float:
        return (float(bid) + float(ask)) / 2.0

    long_call_mid = _mid(long_meta["bid"], long_meta["ask"])
    short_call_mid = _mid(short_meta["bid"], short_meta["ask"])
    net_credit = float(short_call_mid) - float(long_call_mid)

    if not entry_condition_met(
        net_credit_mid_val=net_credit,
        target_credit=cfg.target_credit,
        entry_operator=cfg.entry_operator,
    ):
        if debug:
            logger.info(
                "[%s] CCS delta path fails target: net=%.4f op=%s target=%.4f",
                underlying,
                net_credit,
                cfg.entry_operator,
                cfg.target_credit,
            )
        return _fallback_by_strike()

    return CallSpreadCandidate(
        underlying=underlying,
        long_call_symbol=long_symbol,
        short_call_symbol=short_symbol,
        long_call_mid=long_call_mid,
        short_call_mid=short_call_mid,
        entry_net_credit_mid=net_credit,
        long_strike=long_parts.strike,
        short_strike=short_parts.strike,
    )

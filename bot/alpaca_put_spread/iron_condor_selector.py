"""
Iron condor (IC) candidate: put credit wing + call credit wing, same expiry, combined net credit.
"""
from __future__ import annotations

import logging
import os
from typing import Any, List, Optional, Tuple

from alpaca.data.historical.option import OptionHistoricalDataClient

from bot.alpaca_put_spread.call_credit_spread_selector import _pick_best_call_from_chain
from bot.alpaca_put_spread.config import IronCondorStrategyConfig
from bot.alpaca_put_spread.option_symbol import parse_occ_option_symbol
from bot.alpaca_put_spread.pricing_logic import IronCondorCandidate, entry_condition_met
from bot.alpaca_put_spread.put_spread_selector import (
    _get_chain_silent,
    _normalize_chain_items,
    _pick_best_put_from_chain,
)

logger = logging.getLogger(__name__)


def _filter_chain_by_expiry(chain_items: List[Tuple[str, Any]], expiry_yyyymmdd: str) -> List[Tuple[str, Any]]:
    out: List[Tuple[str, Any]] = []
    for sym, snap in chain_items:
        p = parse_occ_option_symbol(sym)
        if p and p.expiry_yyyymmdd == expiry_yyyymmdd:
            out.append((sym, snap))
    return out


def select_iron_condor(
    *,
    underlying: str,
    option_data_client: OptionHistoricalDataClient,
    cfg: IronCondorStrategyConfig,
    underlying_spot_mid: Optional[float] = None,
    chain: Optional[Any] = None,
) -> Optional[IronCondorCandidate]:
    debug = os.getenv("ALPACA_PUT_SPREAD_DEBUG", "").strip().lower() in ("1", "true", "yes", "y", "on")

    if chain is None:
        chain = _get_chain_silent(option_data_client, underlying)
    items = _normalize_chain_items(chain)
    if not items:
        return None

    long_put_pick = _pick_best_put_from_chain(
        chain_items=items,
        dte_min=cfg.dte_min,
        dte_max=cfg.dte_max,
        delta_abs_min=cfg.long_delta_abs_min,
        delta_abs_max=cfg.long_delta_abs_max,
        iv_max=cfg.iv_max,
        spread_pct_max=cfg.spread_pct_max,
        target_delta=-abs(cfg.long_target_delta),
    )
    if not long_put_pick:
        if debug:
            logger.info("[%s] IC: no long_put candidate", underlying)
        return None

    long_put_sym, long_put_meta = long_put_pick
    lp_parts = parse_occ_option_symbol(long_put_sym)
    if not lp_parts:
        return None
    expiry = lp_parts.expiry_yyyymmdd
    same_exp = _filter_chain_by_expiry(items, expiry)

    short_put_pick = _pick_best_put_from_chain(
        chain_items=same_exp,
        dte_min=cfg.dte_min,
        dte_max=cfg.dte_max,
        delta_abs_min=cfg.short_delta_abs_min,
        delta_abs_max=cfg.short_delta_abs_max,
        iv_max=cfg.iv_max,
        spread_pct_max=cfg.spread_pct_max,
        target_delta=-abs(float(cfg.short_target_delta)),
    )
    if not short_put_pick:
        if debug:
            logger.info("[%s] IC: no short_put on expiry %s", underlying, expiry)
        return None

    short_put_sym, short_put_meta = short_put_pick
    sp_parts = parse_occ_option_symbol(short_put_sym)
    if not sp_parts or sp_parts.expiry_yyyymmdd != expiry:
        return None
    if sp_parts.strike <= lp_parts.strike:
        return None
    put_width = float(sp_parts.strike) - float(lp_parts.strike)
    if put_width < cfg.wing_width_points_min or put_width > cfg.wing_width_points_max:
        return None

    def _pmid(meta: dict) -> float:
        return (float(meta["bid"]) + float(meta["ask"])) / 2.0

    long_put_mid = _pmid(long_put_meta)
    short_put_mid = _pmid(short_put_meta)
    put_credit = float(short_put_mid) - float(long_put_mid)

    long_call_pick = _pick_best_call_from_chain(
        chain_items=same_exp,
        dte_min=cfg.dte_min,
        dte_max=cfg.dte_max,
        delta_abs_min=cfg.long_delta_abs_min,
        delta_abs_max=cfg.long_delta_abs_max,
        iv_max=cfg.iv_max,
        spread_pct_max=cfg.spread_pct_max,
        target_delta=abs(cfg.long_target_delta),
    )
    if not long_call_pick:
        if debug:
            logger.info("[%s] IC: no long_call on expiry %s", underlying, expiry)
        return None

    long_call_sym, long_call_meta = long_call_pick
    lc_parts = parse_occ_option_symbol(long_call_sym)
    if not lc_parts or lc_parts.expiry_yyyymmdd != expiry:
        return None

    short_call_pick = _pick_best_call_from_chain(
        chain_items=same_exp,
        dte_min=cfg.dte_min,
        dte_max=cfg.dte_max,
        delta_abs_min=cfg.short_delta_abs_min,
        delta_abs_max=cfg.short_delta_abs_max,
        iv_max=cfg.iv_max,
        spread_pct_max=cfg.spread_pct_max,
        target_delta=abs(cfg.short_target_delta),
    )
    if not short_call_pick:
        if debug:
            logger.info("[%s] IC: no short_call on expiry %s", underlying, expiry)
        return None

    short_call_sym, short_call_meta = short_call_pick
    sc_parts = parse_occ_option_symbol(short_call_sym)
    if not sc_parts or sc_parts.expiry_yyyymmdd != expiry:
        return None

    if sc_parts.strike >= lc_parts.strike:
        return None
    call_width = float(lc_parts.strike) - float(sc_parts.strike)
    if call_width < cfg.wing_width_points_min or call_width > cfg.wing_width_points_max:
        return None

    if not (float(sp_parts.strike) < float(sc_parts.strike)):
        return None

    long_call_mid = _pmid(long_call_meta)
    short_call_mid = _pmid(short_call_meta)
    call_credit = float(short_call_mid) - float(long_call_mid)

    combined = put_credit + call_credit

    if not entry_condition_met(
        net_credit_mid_val=combined,
        target_credit=cfg.target_credit,
        entry_operator=cfg.entry_operator,
    ):
        if debug:
            logger.info(
                "[%s] IC: combined credit %.4f does not meet %s %.4f",
                underlying,
                combined,
                cfg.entry_operator,
                cfg.target_credit,
            )
        return None

    return IronCondorCandidate(
        underlying=underlying,
        long_put_symbol=long_put_sym,
        short_put_symbol=short_put_sym,
        short_call_symbol=short_call_sym,
        long_call_symbol=long_call_sym,
        entry_net_credit_mid=combined,
    )

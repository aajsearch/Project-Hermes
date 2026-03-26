"""
Single pipeline cycle for Bot V2: one tick per interval.
Builds context per asset, evaluates strategies, aggregates intents, executes exits then entries.
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from bot.market import (
    fetch_15min_market,
    fetch_markets_for_event,
    get_current_15min_market_id,
    get_current_hour_market_id,
    get_current_hour_market_ids,
    get_minutes_to_close,
    get_minutes_to_close_15min,
)
from bot.pipeline.window_utils import logical_window_slot as _logical_window_slot

if TYPE_CHECKING:
    from bot.pipeline.aggregator import OrderAggregator
    from bot.pipeline.data_layer import DataLayer
    from bot.pipeline.executor import PipelineExecutor
    from bot.pipeline.registry import OrderRegistry
    from bot.pipeline.strategies.base import BaseV2Strategy

logger = logging.getLogger(__name__)

# Log WS→REST fallback reason once per (market_id, "market"|"quote") per window; cleared on window transition
_ws_fallback_logged: set = set()
_hourly_feature_flag_logged: bool = False


def _close_ts_from_market(market: Dict[str, Any]) -> Optional[int]:
    """Extract close timestamp (seconds since epoch) from market dict."""
    for key in ("close_time", "expected_expiration_time", "expiration_time"):
        val = market.get(key)
        if val is None:
            continue
        try:
            if isinstance(val, (int, float)):
                return int(val)
            dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except (ValueError, TypeError):
            pass
    return None


def _normalize_position(p: dict) -> Optional[Dict[str, Any]]:
    """Normalize Kalshi position to {ticker, side, count, entry_price_cents}. Returns None if invalid."""
    ticker = p.get("ticker") or p.get("market_ticker") or p.get("event_ticker")
    if not ticker:
        return None
    raw_side = p.get("side") if p.get("side") is not None else p.get("position")
    side = str(raw_side).lower() if raw_side is not None else ""
    count = p.get("contracts") or p.get("quantity") or p.get("count") or 0
    if not count and isinstance(raw_side, (int, float)):
        count = int(abs(raw_side))
    if isinstance(raw_side, (int, float)) and raw_side != 0:
        side = "yes" if raw_side > 0 else "no"
    if side not in {"yes", "no"} or not count or int(count) < 1:
        return None
    avg_price = p.get("avg_price") or p.get("average_price")
    price_cents = None
    if avg_price is not None:
        try:
            if isinstance(avg_price, (int, float)):
                price_cents = int(round(float(avg_price) * 100)) if float(avg_price) < 100 else int(avg_price)
            else:
                price_cents = int(round(float(str(avg_price).replace("$", "").replace(",", "")) * 100))
        except (TypeError, ValueError):
            pass
    if price_cents is None:
        cost = p.get("total_cost_dollars") or p.get("total_cost") or p.get("market_exposure_dollars")
        if cost is not None and count:
            try:
                c = float(str(cost).replace("$", "").replace(",", ""))
                price_cents = int(round(c * 100 / int(count)))
            except (TypeError, ValueError):
                pass
    if price_cents is None or price_cents < 1:
        return None
    return {"ticker": ticker, "side": side, "count": int(count), "entry_price_cents": price_cents}


def run_pipeline_cycle(
    interval: str,
    config: dict,
    data_layer: "DataLayer",
    strategies: List["BaseV2Strategy"],
    aggregator: "OrderAggregator",
    executor: "PipelineExecutor",
    registry: "OrderRegistry",
    kalshi_client: Any = None,
    tick_logger: Any = None,
) -> None:
    """
    Run one cycle of the unified pipeline for the given interval.
    For each asset: fetch real market/quote/positions/orders, build context, evaluate strategies,
    aggregate intents, execute exits then entries.
    """
    intervals_block = config.get("intervals") or {}
    interval_config = intervals_block.get(interval)
    if not isinstance(interval_config, dict):
        logger.warning("No config for interval %s, skipping cycle", interval)
        return
    assets = interval_config.get("assets")
    if not isinstance(assets, (list, tuple)):
        assets = []

    # Optional: disable Kalshi WS and use REST only (config use_kalshi_ws or env KALSHI_USE_WS=0)
    use_kalshi_ws = config.get("use_kalshi_ws", True)
    env_ws = os.getenv("KALSHI_USE_WS", "").strip().lower()
    if env_ws in ("0", "false", "no", "off"):
        use_kalshi_ws = False
    elif env_ws in ("1", "true", "yes", "on"):
        use_kalshi_ws = True

    if not use_kalshi_ws:
        logger.info(
            "[%s] Kalshi WS disabled (use_kalshi_ws=false or KALSHI_USE_WS=0); using REST for market and quote.",
            interval,
        )
    else:
        # Start Kalshi WebSocket and subscribe to current tickers so market/quote use WS when available
        try:
            from bot.kalshi_ws_manager import start_kalshi_ws, subscribe_to_tickers
            start_kalshi_ws()
            tickers_this_interval = []
            for a in assets:
                a = str(a).strip().lower()
                if interval == "fifteen_min":
                    tickers_this_interval.append(get_current_15min_market_id(asset=a))
                else:
                    tickers_this_interval.append(get_current_hour_market_id(asset=a))
            if tickers_this_interval:
                subscribe_to_tickers(tickers_this_interval)
                try:
                    from bot.kalshi_ws_manager import seed_market_cache
                    markets_to_seed: Dict[str, Any] = {}
                    for market_id in tickers_this_interval:
                        if interval == "fifteen_min":
                            m = fetch_15min_market(market_id)
                        else:
                            markets_list, _ = fetch_markets_for_event(market_id)
                            m = markets_list[0] if markets_list else None
                        if m and isinstance(m, dict):
                            t = m.get("ticker") or market_id
                            markets_to_seed[t] = m
                    if markets_to_seed:
                        seed_market_cache(markets_to_seed)
                except Exception as seed_e:
                    logger.debug("[%s] Kalshi WS seed market cache skipped: %s", interval, seed_e)
            # First cycle: give WS time to receive orderbook_snapshot before we read (avoids quote=REST on first run)
            last_window_id = getattr(data_layer, "_last_window_id", None)
            if last_window_id is None and tickers_this_interval:
                logger.info("[%s] Waiting 2s for Kalshi WS orderbook snapshots...", interval)
                time.sleep(2)
        except Exception as e:
            logger.debug("[%s] Kalshi WS start/subscribe skipped: %s", interval, e)

    # Detect window transition: current_window_id changed from last cycle
    first_asset = str(assets[0]).strip().lower() if assets else None
    if first_asset:
        if interval == "fifteen_min":
            current_window_id = get_current_15min_market_id(asset=first_asset)
        else:
            current_window_id = get_current_hour_market_id(asset=first_asset)
        last_window_id = getattr(data_layer, "_last_window_id", None)
        if last_window_id is not None and current_window_id != last_window_id:
            _ws_fallback_logged.clear()
            flush_key = f"{interval}_{_logical_window_slot(last_window_id)}"
            if tick_logger is not None:
                logger.info("[TICK_LOG] End of window: flushing window_id=%s (market_id=%s)", flush_key, last_window_id)
                tick_logger.flush_window(flush_key)
            else:
                logger.info("[TICK_LOG] End of window: window_id=%s (market_id=%s), no tick_logger", flush_key, last_window_id)
            logger.info("[TRANSITION] Window expired. Initiating 20s cooldown and cache purge...")
            data_layer.clear_caches()
            time.sleep(20)
        data_layer._last_window_id = current_window_id
        # Log start of window only when we actually enter a new window (first run or after transition)
        if last_window_id is None or last_window_id != current_window_id:
            window_key = f"{interval}_{_logical_window_slot(current_window_id)}"
            logger.info("[TICK_LOG] Start of window: window_id=%s (market_id=%s)", window_key, current_window_id)

    for asset in assets:
        asset = str(asset).strip().lower()
        if interval == "hourly":
            ff = config.get("feature_flags") or {}
            v2h = ff.get("v2_hourly") if isinstance(ff, dict) else None
            enabled_assets = (v2h or {}).get("enabled_assets", []) if isinstance(v2h, dict) else []
            if isinstance(enabled_assets, list) and enabled_assets:
                allow = {str(a).strip().lower() for a in enabled_assets if str(a).strip()}
                if asset not in allow:
                    continue
            else:
                # Default safe: hourly does nothing unless enabled_assets is non-empty.
                global _hourly_feature_flag_logged
                if not _hourly_feature_flag_logged:
                    logger.info(
                        "[hourly] Feature flag disabled: set config.feature_flags.v2_hourly.enabled_assets to enable hourly per asset."
                    )
                    _hourly_feature_flag_logged = True
                continue
        if not kalshi_client:
            logger.warning("[%s] [%s] No Kalshi client, skipping asset", interval, asset.upper())
            continue
        try:
            market_source = "REST"
            markets: Optional[List[Dict[str, Any]]] = None
            if interval == "fifteen_min":
                market_id = get_current_15min_market_id(asset=asset)
                # Short-lived 15m markets: market_lifecycle_v2 often fires before we connect; rely on REST for market metadata.
                market = fetch_15min_market(market_id)
            else:
                market_id = get_current_hour_market_id(asset=asset)
                # Hourly has multiple event tickers (above/below + range) per asset. We attach all markets
                # so hourly strategies can replicate legacy selection across tickers without refetching.
                extra_hourly_event_ids: List[str] = []
                try:
                    extra_hourly_event_ids = get_current_hour_market_ids(asset=asset)
                except Exception:
                    extra_hourly_event_ids = []
                if use_kalshi_ws:
                    try:
                        from bot.kalshi_ws_manager import get_safe_market
                        market = get_safe_market(market_id)
                        if market and isinstance(market, dict) and market.get("ticker") and market.get("close_time") is not None:
                            market_source = "WS"
                            markets = [market]
                        else:
                            markets, _ = fetch_markets_for_event(market_id)
                            market = markets[0] if markets else None
                            # Merge in range-event markets when present (best-effort).
                            for eid in extra_hourly_event_ids:
                                if eid and eid != market_id:
                                    try:
                                        more, _ = fetch_markets_for_event(eid)
                                        if more:
                                            markets.extend(more)
                                    except Exception:
                                        pass
                            key = (market_id, "market")
                            if key not in _ws_fallback_logged:
                                _ws_fallback_logged.add(key)
                                reason = "no cache" if not market or not isinstance(market, dict) else "cached market missing ticker or close_time"
                                logger.warning(
                                    "[%s] [%s] WS market miss: %s for market_id=%s (market_lifecycle_v2 not received or incomplete); using REST.",
                                    interval, asset.upper(), reason, market_id,
                                )
                    except Exception as e:
                        markets, _ = fetch_markets_for_event(market_id)
                        market = markets[0] if markets else None
                        for eid in extra_hourly_event_ids:
                            if eid and eid != market_id:
                                try:
                                    more, _ = fetch_markets_for_event(eid)
                                    if more:
                                        markets.extend(more)
                                except Exception:
                                    pass
                        key = (market_id, "market")
                        if key not in _ws_fallback_logged:
                            _ws_fallback_logged.add(key)
                            logger.warning("[%s] [%s] WS market error for market_id=%s; using REST: %s", interval, asset.upper(), market_id, e)
                else:
                    markets, _ = fetch_markets_for_event(market_id)
                    market = markets[0] if markets else None
                    for eid in extra_hourly_event_ids:
                        if eid and eid != market_id:
                            try:
                                more, _ = fetch_markets_for_event(eid)
                                if more:
                                    markets.extend(more)
                            except Exception:
                                pass
            if not market or not isinstance(market, dict):
                logger.warning("[%s] [%s] No active market, skipping asset", interval, asset.upper())
                continue
            ticker = market.get("ticker")
            if not ticker:
                logger.warning("[%s] [%s] Market missing ticker, skipping asset", interval, asset.upper())
                continue
            close_ts = _close_ts_from_market(market)
            if close_ts is not None:
                now_ts = int(datetime.now(timezone.utc).timestamp())
                seconds_to_close = max(0.0, float(close_ts - now_ts))
            else:
                if interval == "fifteen_min":
                    mins = get_minutes_to_close_15min(market_id)
                else:
                    mins = get_minutes_to_close(market_id)
                seconds_to_close = max(0.0, mins * 60.0) if mins is not None else None
            if seconds_to_close is None or seconds_to_close < 0:
                logger.warning("[%s] [%s] Invalid seconds_to_close=%s, skipping asset", interval, asset.upper(), seconds_to_close)
                continue
            quote_source = "REST"
            if use_kalshi_ws:
                try:
                    from bot.kalshi_ws_manager import get_safe_orderbook
                    top = get_safe_orderbook(market_id)
                    if top is not None:
                        quote_source = "WS"
                        if top.get("yes_bid") is None and top.get("no_bid") is None:
                            logger.debug("[%s] WS orderbook empty for %s (no liquidity), using REST for quote", interval, market_id)
                            top = kalshi_client.get_top_of_book(ticker)
                    else:
                        top = kalshi_client.get_top_of_book(ticker)
                        key = (market_id, "quote")
                        if key not in _ws_fallback_logged:
                            _ws_fallback_logged.add(key)
                            logger.warning(
                                "[%s] [%s] WS orderbook miss: no cache for market_id=%s; using REST.",
                                interval, asset.upper(), market_id,
                            )
                except Exception as e:
                    top = kalshi_client.get_top_of_book(ticker)
                    key = (market_id, "quote")
                    if key not in _ws_fallback_logged:
                        _ws_fallback_logged.add(key)
                        logger.warning("[%s] [%s] WS orderbook error for market_id=%s; using REST: %s", interval, asset.upper(), market_id, e)
            else:
                top = kalshi_client.get_top_of_book(ticker)
            if not top or not isinstance(top, dict):
                logger.warning("[%s] [%s] Orderbook fetch failed, skipping asset", interval, asset.upper())
                continue
            quote: Dict[str, int] = {
                "yes_bid": int(top.get("yes_bid") or 0),
                "yes_ask": int(top.get("yes_ask") or 0),
                "no_bid": int(top.get("no_bid") or 0),
                "no_ask": int(top.get("no_ask") or 0),
            }
            positions_raw = kalshi_client.get_positions(limit=200)
            positions_list = positions_raw.get("positions", []) if isinstance(positions_raw, dict) else []
            positions = []
            tickers_in_event: Optional[set] = None
            if interval != "fifteen_min":
                tickers_in_event = {m.get("ticker") for m in (markets or []) if isinstance(m, dict) and m.get("ticker")}
            for p in positions_list:
                if not isinstance(p, dict):
                    continue
                pt = p.get("ticker") or p.get("market_ticker")
                if interval == "fifteen_min":
                    if pt != ticker:
                        continue
                else:
                    if tickers_in_event is not None and pt not in tickers_in_event:
                        continue
                norm = _normalize_position(p)
                if norm:
                    positions.append(norm)
            orders_resp = kalshi_client.get_orders(status="resting", ticker=ticker, limit=100)
            open_orders = orders_resp.get("orders", []) if isinstance(orders_resp, dict) else []
            market_data = market
        except Exception as e:
            logger.warning("[%s] [%s] Fetch error, skipping asset: %s", interval, asset.upper(), e)
            continue

        ctx = data_layer.build_context(
            interval=interval,
            market_id=market_id,
            ticker=ticker,
            asset=asset,
            seconds_to_close=seconds_to_close,
            quote=quote,
            positions=positions,
            open_orders=open_orders,
            config=config,
            market_data=market_data,
            event_markets=markets,
        )
        if interval == "fifteen_min" and ctx.distance is None:
            logger.warning(
                "[%s] [%s] Distance is None (no strike or spot), skipping asset — strike=%s spot=%s",
                interval, asset.upper(), ctx.strike, ctx.spot,
            )
            continue
        if interval != "fifteen_min" and ctx.spot is None:
            logger.warning("[%s] [%s] Spot is None, skipping asset (hourly requires spot)", interval, asset.upper())
            continue

        # Tick logger: one row per asset per window (tick history as JSON). Use logical slot so all assets share same window key.
        if tick_logger is not None:
            window_id = f"{interval}_{_logical_window_slot(market_id)}"
            tick_logger.record_tick(
                window_id=window_id,
                asset=asset,
                sec=float(ctx.seconds_to_close),
                yes_bid=int(ctx.quote.get("yes_bid") or 0),
                no_bid=int(ctx.quote.get("no_bid") or 0),
                strike=float(ctx.strike) if ctx.strike is not None else None,
                spot=float(ctx.spot) if ctx.spot is not None else None,
            )
        # Dynamic float precision by asset: BTC/ETH 2, SOL 3, XRP 5
        _ndp = 2 if asset in ("btc", "eth") else 3 if asset == "sol" else 5 if asset == "xrp" else 2
        def _fmt(v: Any) -> str:
            if v is None:
                return "None"
            if isinstance(v, (int, float)):
                return f"{float(v):.{_ndp}f}"
            return str(v)
        strike_src = ctx.strike_source or "?"
        dist_dir = ""
        if ctx.strike is not None and ctx.spot is not None:
            dist_dir = "UP" if ctx.spot > ctx.strike else "DOWN"
        dist_str = f"{_fmt(ctx.distance)} ({dist_dir})" if dist_dir else _fmt(ctx.distance)
        spot_str = ctx.spot_source
        if ctx.spot_source == "WS" and ctx.spot_age_s is not None:
            spot_str = "WS (%.1fs)" % ctx.spot_age_s
        logger.info(
            "[V2 DATA] %s | market=%s quote=%s spot_src=%s | Strike: %s (%s) | Spot: %s | Dist: %s",
            asset.upper(),
            market_source,
            quote_source,
            spot_str,
            _fmt(ctx.strike),
            strike_src,
            _fmt(ctx.spot),
            dist_str,
        )

        # --- Order status sync (critical for ATM exits) ---
        # Registry only records placements; we must infer fills so evaluate_exit can fire stop_loss/take_profit.
        open_order_ids = set()
        try:
            for o in (open_orders or []):
                if isinstance(o, dict) and o.get("order_id"):
                    open_order_ids.add(str(o.get("order_id")))
        except Exception:
            open_order_ids = set()

        asset_intents: List[tuple] = []
        asset_exits: List[Any] = []
        for strat in strategies:
            # --- Pre-fill adverse selection protection (cancel resting orders) ---
            # If a resting entry order sits unfilled and spot distance decays too much, cancel it
            # to avoid getting filled by toxic flow during a fast move toward strike.
            try:
                strat_cfg = strat._get_strategy_config(ctx) if hasattr(strat, "_get_strategy_config") else {}
            except Exception:
                strat_cfg = {}
            cancel_decay_pct_raw = None if not isinstance(strat_cfg, dict) else strat_cfg.get("resting_cancel_decay_pct")
            try:
                cancel_decay_pct = float(cancel_decay_pct_raw) if cancel_decay_pct_raw is not None else 0.0
            except (TypeError, ValueError):
                cancel_decay_pct = 0.0

            # If an order was previously registered as 'resting' but no longer appears in open_orders,
            # treat it as filled (best-effort) so exits can manage it.
            resting = registry.get_orders_by_strategy(
                strat.strategy_id, interval, market_id=market_id, asset=asset, active_only=True
            )
            for o in resting:
                # Cancel resting order if distance has decayed beyond threshold before it fills.
                if cancel_decay_pct > 0.0 and ctx.distance is not None and getattr(o, "entry_distance", None) is not None:
                    try:
                        placement_dist = float(getattr(o, "entry_distance"))
                        current_dist = float(ctx.distance)
                        if placement_dist > 0:
                            cancel_threshold = placement_dist * (1.0 - cancel_decay_pct)
                            if current_dist <= cancel_threshold:
                                if kalshi_client is not None:
                                    cancel_failed = False
                                    try:
                                        kalshi_client.cancel_order(str(o.order_id))
                                    except Exception as e:
                                        logger.warning(
                                            "[EXECUTION] Cancel resting order failed (may already be filled): order_id=%s ticker=%s: %s",
                                            o.order_id,
                                            o.ticker,
                                            e,
                                        )
                                        cancel_failed = True

                                    filled_after_cancel = 0
                                    post_status = None
                                    try:
                                        info = kalshi_client.get_order(str(o.order_id))
                                        od = info.get("order", info) if isinstance(info, dict) else {}
                                        post_status = str(od.get("status") or "").lower()
                                        filled_after_cancel = int(od.get("fill_count") or od.get("filled_count") or 0)
                                    except Exception as ve:
                                        logger.warning(
                                            "[EXECUTION] Post-cancel get_order failed: order_id=%s %s",
                                            o.order_id,
                                            ve,
                                        )

                                    if filled_after_cancel > 0:
                                        fill_dist = float(ctx.distance) if ctx.distance is not None else None
                                        registry.update_order_status(
                                            str(o.order_id),
                                            "filled",
                                            filled_after_cancel,
                                            entry_distance_at_fill=fill_dist,
                                        )
                                        from bot.pipeline.intents import ExitAction

                                        asset_exits.append(
                                            ExitAction(
                                                order_id=str(o.order_id),
                                                action="stop_loss",
                                                reason="post_cancel_fill_detected",
                                            )
                                        )
                                        logger.warning(
                                            "[EXECUTION] Bad fill detected → immediate exit — order_id=%s status=%s fill_count=%s",
                                            o.order_id,
                                            post_status,
                                            filled_after_cancel,
                                        )
                                        continue

                                    if not cancel_failed or post_status == "canceled":
                                        try:
                                            registry.update_order_status(str(o.order_id), "canceled", 0)
                                        except Exception:
                                            pass
                                        logger.info(
                                            "[EXECUTION] Order CANCELED (pre-fill decay) — order_id=%s strategy_id=%s ticker=%s side=%s "
                                            "placement_distance=%.4f current_distance=%.4f threshold=%.4f decay_pct=%.2f",
                                            o.order_id,
                                            o.strategy_id,
                                            o.ticker,
                                            o.side,
                                            placement_dist,
                                            current_dist,
                                            cancel_threshold,
                                            cancel_decay_pct,
                                        )
                                    continue
                    except Exception:
                        pass

                if o.order_id and o.order_id not in open_order_ids:
                    try:
                        # Capture reference distance at the moment we learn the order is filled.
                        # This is used for distance-decay trailing stop so it's based on actual entry.
                        fill_dist = float(ctx.distance) if ctx.distance is not None else None
                        registry.update_order_status(
                            o.order_id,
                            "filled",
                            int(o.count or 0),
                            entry_distance_at_fill=fill_dist,
                        )
                        # Mirror the "Order PLACED" logging style so we can correlate placed->filled->SL/TP.
                        logger.info(
                            "[EXECUTION] Order FILLED (inferred) — order_id=%s strategy_id=%s ticker=%s side=%s count=%s "
                            "spot=%s spot_src=%s spot_age_s=%s entry_distance_at_fill=%s",
                            o.order_id,
                            o.strategy_id,
                            o.ticker,
                            o.side,
                            int(o.count or 0),
                            _fmt(ctx.spot),
                            ctx.spot_source,
                            _fmt(ctx.spot_age_s),
                            _fmt(fill_dist),
                        )
                        # --- Bad-fill immediate exit (post-fill adverse selection protection) ---
                        # If the order only got filled after distance decayed materially vs placement_distance,
                        # treat it as toxic flow and exit immediately (market_sell) rather than waiting for full SL.
                        if cancel_decay_pct > 0.0 and fill_dist is not None and getattr(o, "entry_distance", None) is not None:
                            try:
                                placement_dist = float(getattr(o, "entry_distance"))
                                if placement_dist > 0.0:
                                    bad_fill_threshold = placement_dist * (1.0 - cancel_decay_pct)
                                    # Guardrail: if the fill-distance is still ABOVE our configured min_distance_at_placement,
                                    # don't treat it as an adverse fill; let normal exit logic decide.
                                    min_dist_setting = strat_cfg.get("min_distance_at_placement") if isinstance(strat_cfg, dict) else None
                                    min_dist_at_placement = None
                                    if isinstance(min_dist_setting, dict):
                                        min_dist_at_placement = min_dist_setting.get(asset) or min_dist_setting.get(asset.upper())
                                    else:
                                        min_dist_at_placement = min_dist_setting
                                    try:
                                        if min_dist_at_placement is not None:
                                            min_dist_at_placement = float(min_dist_at_placement)
                                    except (TypeError, ValueError):
                                        min_dist_at_placement = None

                                    should_bad_exit = float(fill_dist) <= bad_fill_threshold
                                    if min_dist_at_placement is not None:
                                        should_bad_exit = should_bad_exit and (float(fill_dist) <= min_dist_at_placement)

                                    if should_bad_exit:
                                        from bot.pipeline.intents import ExitAction
                                        asset_exits.append(
                                            ExitAction(
                                                order_id=str(o.order_id),
                                                action="stop_loss",
                                                reason="bad_fill_decay",
                                            )
                                        )
                                        logger.info(
                                            "[EXECUTION] Bad fill detected → immediate exit — order_id=%s strategy_id=%s ticker=%s side=%s "
                                            "placement_distance=%.4f fill_distance=%.4f threshold=%.4f decay_pct=%.2f",
                                            o.order_id,
                                            o.strategy_id,
                                            o.ticker,
                                            o.side,
                                            placement_dist,
                                            float(fill_dist),
                                            bad_fill_threshold,
                                            cancel_decay_pct,
                                        )
                            except Exception:
                                pass
                    except Exception:
                        pass

            # For exits, include filled orders too (not just resting).
            my_orders = registry.get_orders_by_strategy(
                strat.strategy_id, interval, market_id=market_id, asset=asset, active_only=False
            )
            exits = strat.evaluate_exit(ctx, my_orders)
            asset_exits.extend(exits)
            intent = strat.evaluate_entry(ctx, my_orders=my_orders)
            if intent is not None:
                asset_intents.append((intent, strat.strategy_id))

        interval_slice = config.get(interval) or {}
        active_orders = registry.get_all_active_orders_for_cap_check(interval, market_id=market_id)
        current_cost_by_strategy: Dict[str, int] = {}
        for _, strategy_id in asset_intents:
            if strategy_id not in current_cost_by_strategy:
                orders = registry.get_orders_by_strategy(
                    strategy_id, interval, market_id=market_id, asset=asset, active_only=False
                )
                current_cost_by_strategy[strategy_id] = sum(
                    o.count * (o.limit_price_cents or 99) for o in orders
                )
        final_intents = aggregator.resolve_intents(
            asset_intents, interval_slice, active_orders, current_cost_by_strategy
        )
        # Exits run first inside execute_cycle; global cooldown (e.g. after stop-loss) is updated there,
        # so the next asset's evaluate_entry in a subsequent tick or the next market cycle will see it.
        executor.execute_cycle(
            final_intents,
            asset_exits,
            interval,
            market_id,
            asset,
            ticker=ticker,
        )

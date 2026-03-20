"""
Central data layer for Bot V2: fetches oracles, caches strikes, builds WindowContext.
Strategies never fetch data themselves; they receive a WindowContext per tick.
"""
from __future__ import annotations

import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from bot.pipeline.context import WindowContext

logger = logging.getLogger(__name__)

def _is_year_like(value: float) -> bool:
    """Reject values that look like years (e.g. 2026 from dates)."""
    return 2020 <= value <= 2035


def _parse_strike_from_text_strict(text: str) -> Optional[float]:
    """
    Parse strike from text using clear price patterns. Rejects year-like values.
    Supports: "BTC > 68500", "> 68500", "above 68500", "$68,500". Returns None if no valid strike.
    """
    if not text or not isinstance(text, str):
        return None
    # Explicit price patterns (prefer > N, then above N, then $N)
    match = re.search(r">\s*\$?([\d,]+(?:\.[\d]+)?)", text)
    if match:
        try:
            v = float(match.group(1).replace(",", ""))
            if v > 0 and not _is_year_like(v):
                return v
        except (ValueError, TypeError):
            pass
    match = re.search(r"[Aa]bove\s+\$?([\d,]+(?:\.[\d]+)?)", text)
    if match:
        try:
            v = float(match.group(1).replace(",", ""))
            if v > 0 and not _is_year_like(v):
                return v
        except (ValueError, TypeError):
            pass
    match = re.search(r"\$([\d,]+(?:\.[\d]+)?)", text)
    if match:
        try:
            v = float(match.group(1).replace(",", ""))
            if v > 0 and not _is_year_like(v):
                return v
        except (ValueError, TypeError):
            pass
    # Fallback: number with 3+ digits (avoid single/double digit or years)
    match = re.search(r"([\d,]{3,}(?:\.[\d]+)?)", text)
    if match:
        try:
            v = float(match.group(1).replace(",", ""))
            if v > 0 and v < 1e9 and not _is_year_like(v):
                return v
        except (ValueError, TypeError):
            pass
    return None


def _parse_strike_from_ticker(ticker: str) -> Optional[float]:
    """
    Extract strike from ticker only when explicitly encoded: -T<strike> or -B<strike>.
    Returns None for 15-min tickers like KXBTC15M-26MAR061215-15 where -15 is sequence ID.
    """
    if not ticker or not isinstance(ticker, str):
        return None
    match = re.search(r"-T([\d.]+)$", ticker, re.IGNORECASE)
    if match:
        try:
            v = float(match.group(1))
            if v > 0 and v < 1e9 and not _is_year_like(v):
                return v
        except (ValueError, TypeError):
            pass
    match = re.search(r"-B([\d.]+)$", ticker, re.IGNORECASE)
    if match:
        try:
            v = float(match.group(1))
            if v > 0 and v < 1e9:
                return v
        except (ValueError, TypeError):
            pass
    return None


def _reject_strike_sanity(val: float, source: str, asset_lower: Optional[str]) -> bool:
    """
    Sanity guard: reject timeframe-looking numbers (15, 30, 60) from title/subtitle,
    and asset-specific unrealistic bounds. Returns True if strike should be rejected.
    """
    # 1. Reject timeframe-looking numbers from title/subtitle
    if source in ("title", "subtitle") and val in (15.0, 30.0, 60.0):
        return True
    # 2. Asset-specific bounds (realism check)
    if asset_lower == "btc" and val < 1000:
        return True
    if asset_lower == "eth" and val < 100:
        return True
    if asset_lower == "sol" and val < 5:
        return True
    if asset_lower == "xrp" and val > 10:
        return True
    return False


def _asset_from_ticker(ticker: str) -> Optional[str]:
    """Infer asset from market ticker (e.g. KXBTC15M-... -> btc)."""
    if not ticker or not isinstance(ticker, str):
        return None
    t = ticker.upper()
    if "BTC" in t:
        return "btc"
    if "ETH" in t:
        return "eth"
    if "SOL" in t:
        return "sol"
    if "XRP" in t:
        return "xrp"
    return None


def _extract_strike_from_market(market_data: Dict[str, Any]) -> Tuple[Optional[float], Optional[str]]:
    """
    High-resilience strike extraction.
    - API fields (floor_strike, cap_strike, etc.): trusted as-is; no year-like check (ETH can be ~2028).
    - Title/subtitle text: always apply _is_year_like (reject 2020–2035) to avoid using dates as strike.
    Sanity guard: rejects 15/30/60 from title/subtitle and asset-unrealistic bounds.
    """
    if not market_data or not isinstance(market_data, dict):
        return (None, None)
    # Debug only: if ticker is prefix-only (e.g. KXETH15M with no "-"), list API may return incomplete market
    logger.debug(
        "[DEBUG STRIKE] Ticker: %s | Subtitle: %s",
        market_data.get("ticker"),
        market_data.get("subtitle"),
    )
    asset_lower = _asset_from_ticker(market_data.get("ticker") or "")

    # 1. Try API Fields (Standard V2). Trust API — do not apply year-like reject (ETH strike can be ~2028).
    api_strike = market_data.get("strike_price") or market_data.get("cap_strike")
    if api_strike is not None:
        try:
            v = float(api_strike)
            if v > 0 and not _reject_strike_sanity(v, "api_fields", asset_lower):
                return (v, "api_fields")
        except (ValueError, TypeError):
            pass

    # Also try floor_strike, strike, ceiling_strike (API-authoritative; no year-like check)
    for key in ("floor_strike", "strike", "ceiling_strike"):
        val = market_data.get(key)
        if val is not None:
            try:
                v = float(val)
                if v > 0 and not _reject_strike_sanity(v, "api_fields", asset_lower):
                    return (v, "api_fields")
            except (ValueError, TypeError):
                pass

    # 2. Subtitle (text): strict patterns + year-like check inside _parse_strike_from_text_strict
    subtitle = (market_data.get("subtitle") or "") if isinstance(market_data.get("subtitle"), str) else ""
    v = _parse_strike_from_text_strict(subtitle)
    if v is not None and not _reject_strike_sanity(v, "subtitle", asset_lower):
        return (v, "subtitle")

    # 3. Title (text): strict patterns + year-like check inside _parse_strike_from_text_strict
    title = (market_data.get("title") or "") if isinstance(market_data.get("title"), str) else ""
    v = _parse_strike_from_text_strict(title)
    if v is not None and not _reject_strike_sanity(v, "title", asset_lower):
        return (v, "title")

    # 4. Fallback: numbers from subtitle — must apply year-like check (dates like 2026 in text)
    subtitle_clean = subtitle.replace(",", "")
    nums = re.findall(r"(\d+\.?\d*)", subtitle_clean)
    for s in reversed(nums):
        try:
            v = float(s)
            if v > 0 and not _is_year_like(v) and v < 1e9 and not _reject_strike_sanity(v, "subtitle", asset_lower):
                return (v, "subtitle")
        except (ValueError, TypeError):
            pass

    # 5. Fallback: numbers from title — must apply year-like check (dates like 2026 in text)
    title_clean = title.replace(",", "")
    nums = re.findall(r"(\d+\.?\d*)", title_clean)
    for s in reversed(nums):
        try:
            v = float(s)
            if v > 0 and not _is_year_like(v) and v < 1e9 and not _reject_strike_sanity(v, "title", asset_lower):
                return (v, "title")
        except (ValueError, TypeError):
            pass

    # Diagnostic when we fail (helps debug strike=None)
    if asset_lower in ("btc", "eth", "sol", "xrp"):
        logger.warning(
            "[STRIKE] Could not extract strike for %s: subtitle=%r title=%r floor_strike=%s cap_strike=%s strike=%s",
            (market_data.get("ticker") or "?"),
            (subtitle or "")[:80],
            (title or "")[:80],
            market_data.get("floor_strike"),
            market_data.get("cap_strike"),
            market_data.get("strike"),
        )
    return (None, None)


class DataLayer:
    """
    Builds WindowContext per (interval, asset) per tick.
    Single spot source (Coinbase). To use Kraken, change _get_spot_price to read from Kraken WS/REST and set spot from there.
    """

    _strike_cache: Dict[str, float] = {}
    _strike_source_cache: Dict[str, str] = {}
    _last_spot_source: str = "?"
    _last_spot_age_s: Optional[float] = None
    _last_window_id: Optional[str] = None

    def __init__(self, kalshi_client: Any = None) -> None:
        self._kalshi_client = kalshi_client

    def clear_caches(self) -> None:
        """Wipe strike and strike_source caches (e.g. after window transition)."""
        self._strike_cache.clear()
        self._strike_source_cache.clear()

    def _get_or_fetch_strike(self, window_id: str, market_data: Dict[str, Any]) -> Tuple[Optional[float], Optional[str]]:
        """
        Return (cached strike, cached source) for window_id, or extract from fresh or passed-in market data.
        Fast path: if window_id is in cache, return cached value.
        When not cached: force a fresh fetch from Kalshi for that market (by ticker) so we don't rely on
        stale/empty metadata; only cache when source is api_fields.
        """
        if window_id in self._strike_cache:
            return (self._strike_cache[window_id], self._strike_source_cache.get(window_id))

        ticker = (market_data or {}).get("ticker")
        # If ticker looks like a prefix only (e.g. KXETH15M with no "-"), get_market(ticker) may 404.
        # Use market_id from window_id for 15m (e.g. fifteen_min_KXETH15M-26MAR101815 -> KXETH15M-26MAR101815).
        fetch_ticker: Optional[str] = str(ticker).strip() if ticker else None
        if not fetch_ticker or "-" not in fetch_ticker:
            if window_id.startswith("fifteen_min_"):
                fetch_ticker = window_id.replace("fifteen_min_", "", 1)
            else:
                fetch_ticker = ticker

        fresh_market_data: Optional[Dict[str, Any]] = None
        if self._kalshi_client and fetch_ticker:
            try:
                fresh_market_data = self._kalshi_client.get_market(fetch_ticker)
            except Exception as e:
                logger.debug("Fresh fetch for strike failed for %s: %s", fetch_ticker, e)
        if not fresh_market_data or not isinstance(fresh_market_data, dict):
            fresh_market_data = market_data

        extracted_strike, extracted_source = _extract_strike_from_market(fresh_market_data)
        if extracted_strike is None:
            # Evict cache for this window so we retry next tick (e.g. spot was missing this tick).
            self._strike_cache.pop(window_id, None)
            self._strike_source_cache.pop(window_id, None)
            return (None, None)
        source = extracted_source or "api_fields"
        if source == "api_fields":
            self._strike_cache[window_id] = extracted_strike
            self._strike_source_cache[window_id] = source
            logger.debug("Cached strike for %s: %s (source=%s)", window_id, extracted_strike, source)
        else:
            ticker_display = (fresh_market_data or {}).get("ticker") or window_id
            logger.info(
                "Found potential strike %s from %s for %s, waiting for official api_fields...",
                extracted_strike,
                source,
                ticker_display,
            )
        return (extracted_strike, source)

    def _get_spot_price(self, asset: str) -> Tuple[Optional[float], str, Optional[float]]:
        """
        Return (spot_price, source, age_s) for the given asset. Single source (Coinbase).
        To switch to Kraken: feed "spot" from Kraken WS in oracle_ws_manager and add Kraken REST fallback here.
        """
        a = (asset or "").strip().upper() or "BTC"
        spot: Optional[float] = None
        self._last_spot_source = "REST"
        self._last_spot_age_s = None
        try:
            from bot.oracle_ws_manager import get_safe_spot_prices_sync, is_ws_running
            if is_ws_running():
                ws_spot = get_safe_spot_prices_sync(asset, max_age_seconds=3.0)
                if ws_spot:
                    spot = ws_spot.get("spot")
                    spot_ts = ws_spot.get("spot_ts")
                    if spot is not None:
                        self._last_spot_source = "WS"
                        self._last_spot_age_s = (time.time() - spot_ts) if spot_ts is not None else None
                        logger.debug("[spot] WS for %s: %.4f (age=%.1fs)", asset, spot, self._last_spot_age_s or 999.0)
                if spot is None:
                    logger.debug("[spot] WS miss for %s — using REST", asset)
        except Exception as e:
            logger.debug("Oracle WS spot read failed for %s: %s", asset, e)
        if spot is None:
            symbol = f"{a}-USD" if a in ("BTC", "ETH", "SOL", "XRP") else "BTC-USD"
            try:
                import urllib.request
                with urllib.request.urlopen(
                    f"https://api.coinbase.com/v2/prices/{symbol}/spot",
                    timeout=2,
                ) as resp:
                    if resp.status == 200:
                        import json
                        data = json.loads(resp.read().decode())
                        amount = (data or {}).get("data", {}).get("amount")
                        if amount is not None:
                            spot = float(amount)
            except Exception as e:
                logger.debug("Coinbase REST spot failed for %s: %s", asset, e)
        return (spot, self._last_spot_source, self._last_spot_age_s)

    def build_context(
        self,
        interval: str,
        market_id: str,
        ticker: str,
        asset: str,
        seconds_to_close: float,
        quote: Dict[str, Any],
        positions: List[Dict[str, Any]],
        open_orders: List[Any],
        config: Dict[str, Any],
        market_data: Dict[str, Any],
    ) -> WindowContext:
        """
        Build a WindowContext for the current tick: single spot (Coinbase), distance = abs(spot - strike).
        """
        spot_price, spot_source, spot_age_s = self._get_spot_price(asset)
        window_id = f"{interval}_{market_id}"
        strike, strike_source = self._get_or_fetch_strike(window_id, market_data)

        quote_normalized: Dict[str, int] = {
            "yes_bid": int(quote.get("yes_bid", 0) or 0),
            "yes_ask": int(quote.get("yes_ask", 0) or 0),
            "no_bid": int(quote.get("no_bid", 0) or 0),
            "no_ask": int(quote.get("no_ask", 0) or 0),
        }

        distance: Optional[float] = None
        if strike is not None and spot_price is not None:
            distance = abs(spot_price - strike)

        return WindowContext(
            interval=interval,
            market_id=market_id,
            ticker=ticker,
            asset=asset,
            seconds_to_close=seconds_to_close,
            quote=quote_normalized,
            spot=spot_price,
            spot_source=spot_source,
            spot_age_s=spot_age_s,
            strike=strike,
            strike_source=strike_source,
            distance=distance,
            positions=positions,
            open_orders=open_orders,
            config=config,
        )

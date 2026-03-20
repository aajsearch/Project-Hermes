from core.models import Signal
from strategies.lev_etf_trend import generate_signal as underlying_signal
from indicators.ta import ema, rsi


def _is_bearish_put_zone(df, profile) -> bool:
    """True when trend down and RSI <= bear_rsi_max (suitable for PUT entry)."""
    bear_rsi_max = profile.get("bear_rsi_max")
    if bear_rsi_max is None:
        return False
    if df is None or df.empty or "close" not in df:
        return False
    close = df["close"]
    ema_fast_len = int(profile.get("ema_fast", 20))
    ema_slow_len = int(profile.get("ema_slow", 50))
    rsi_len = int(profile.get("rsi_len", 14))
    if len(df) < max(ema_slow_len + 2, rsi_len + 2):
        return False
    efast = ema(close, ema_fast_len)
    eslow = ema(close, ema_slow_len)
    r = rsi(close, rsi_len)
    efast_now = float(efast.iloc[-1])
    efast_prev = float(efast.iloc[-2])
    eslow_now = float(eslow.iloc[-1])
    eslow_prev = float(eslow.iloc[-2])
    r_now = float(r.iloc[-1])
    trend_down = (efast_now < eslow_now and efast_prev < eslow_prev)
    return trend_down and r_now <= float(bear_rsi_max)


def generate_options_signal(df, profile) -> Signal:
    """
    Options overlay: bullish -> BUY CALL, bearish -> BUY PUT, weakening -> SELL (exit any option).
    Never hold call and put on same underlying at once; engine exits opposite leg before opening new one.
    """
    sig = underlying_signal(df, profile)
    if sig.action == "BUY":
        return Signal("BUY", "Underlying BUY -> consider call", sig.confidence, direction="CALL")
    if sig.action == "SELL":
        return Signal("SELL", "Underlying SELL -> exit options / avoid new entry", sig.confidence)
    if sig.action == "HOLD" and _is_bearish_put_zone(df, profile):
        return Signal("BUY", "Underlying bearish -> consider put", sig.confidence, direction="PUT")
    return sig

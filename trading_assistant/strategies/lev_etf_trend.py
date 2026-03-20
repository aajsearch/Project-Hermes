from core.models import Signal
from indicators.ta import ema, rsi


def generate_signal(df, profile) -> Signal:
    if df is None or df.empty or "close" not in df:
        return Signal("NONE", "no data")

    close = df["close"]

    ema_fast_len = profile["ema_fast"]
    ema_slow_len = profile["ema_slow"]
    rsi_len = profile["rsi_len"]

    efast = ema(close, ema_fast_len)
    eslow = ema(close, ema_slow_len)
    r = rsi(close, rsi_len)

    if len(df) < max(ema_slow_len + 2, rsi_len + 2):
        return Signal("NONE", "not enough bars")

    # Last 2 bars
    efast_now = float(efast.iloc[-1])
    efast_prev = float(efast.iloc[-2])
    eslow_now = float(eslow.iloc[-1])
    eslow_prev = float(eslow.iloc[-2])
    r_now = float(r.iloc[-1])
    r_prev = float(r.iloc[-2])
    price_now = float(close.iloc[-1])

    # -------------------------
    # Trend quality filter
    # -------------------------
    ema_gap_pct = (efast_now - eslow_now) / eslow_now if eslow_now else 0.0
    min_trend_gap = float(profile.get("min_trend_gap_pct", 0.0015))  # 0.15% default

    trend_up = (
        efast_now > eslow_now and
        efast_prev > eslow_prev and          # 2-bar confirmation
        ema_gap_pct >= min_trend_gap
    )

    trend_down = (
        efast_now < eslow_now and
        efast_prev < eslow_prev
    )

    # -------------------------
    # RSI behavior
    # -------------------------
    entry_rsi_min = profile["entry_rsi_min"]
    entry_rsi_max = profile["entry_rsi_max"]

    rsi_ok = entry_rsi_min <= r_now <= entry_rsi_max
    rsi_rising = r_now > r_prev

    # -------------------------
    # BUY logic (trend + momentum)
    # -------------------------
    if trend_up and rsi_ok and rsi_rising:
        return Signal(
            "BUY",
            f"trend strong (ema{ema_fast_len}>{ema_slow_len}, gap={ema_gap_pct*100:.2f}%), "
            f"rsi rising {r_prev:.1f}->{r_now:.1f}",
            0.7,
        )

    # -------------------------
    # SELL logic (trend deterioration)
    # -------------------------
    # Exit earlier than full collapse to reduce giveback
    # RSI threshold tunable via profile (exit_rsi_max); default 48 for backward compatibility
    exit_rsi_max = float(profile.get("exit_rsi_max", 48.0))
    if trend_down and r_now < exit_rsi_max:
        return Signal(
            "SELL",
            f"trend weakening (ema{ema_fast_len}<{ema_slow_len}), rsi={r_now:.1f}",
            0.7,
        )

    return Signal("HOLD", f"no setup rsi={r_now:.1f}", 0.4)

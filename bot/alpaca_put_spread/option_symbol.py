from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, time, timezone
from typing import Optional

import pytz


_OCC_RE = re.compile(r"^([A-Z]{1,6})(\d{6})([CP])(\d{8})$")


@dataclass(frozen=True)
class OptionSymbolParts:
    underlying: str
    expiry_yyyymmdd: str
    option_type: str  # "C" or "P"
    strike: float

    def expiry_date(self) -> datetime:
        # expiry_yyyymmdd => YYYY-MM-DD in local market calendar (approx)
        return datetime.strptime(self.expiry_yyyymmdd, "%Y-%m-%d")


def parse_occ_option_symbol(symbol: str) -> Optional[OptionSymbolParts]:
    """
    Parse typical OCC-like symbols Alpaca returns, e.g.:
      - SPY240615P00450000
      - QQQ240615P00370000

    Strike decoding: last 8 digits are strike * 1000 for equity options.
    """
    if not symbol:
        return None
    m = _OCC_RE.match(symbol.strip().upper())
    if not m:
        return None
    underlying, yymmdd, cp, strike8 = m.groups()
    yy = int(yymmdd[:2])
    mm = int(yymmdd[2:4])
    dd = int(yymmdd[4:6])
    year = 2000 + yy
    expiry_yyyymmdd = f"{year:04d}-{mm:02d}-{dd:02d}"
    strike = int(strike8) / 1000.0
    return OptionSymbolParts(
        underlying=underlying,
        expiry_yyyymmdd=expiry_yyyymmdd,
        option_type=cp,
        strike=strike,
    )


def option_expiry_utc(symbol: str, expiry_time_et: time | None = None) -> Optional[datetime]:
    """
    Convert option expiry date to an approximate UTC timestamp using US/Eastern.
    For MVP we assume expiration time is at `expiry_time_et` on the expiry date.
    Default: 16:00 ET.
    """
    parts = parse_occ_option_symbol(symbol)
    if not parts:
        return None
    tz = pytz.timezone("America/New_York")
    dt = datetime.strptime(parts.expiry_yyyymmdd, "%Y-%m-%d")
    et_time = expiry_time_et or time(16, 0)
    dt_et = tz.localize(datetime.combine(dt.date(), et_time))
    return dt_et.astimezone(pytz.utc)


def minutes_to_expiry_utc(symbol: str, expiry_time_et: time | None = None) -> Optional[float]:
    """
    Minutes from now (UTC) until option expiry timestamp from :func:`option_expiry_utc`.
    Negative if already past expiry clock.
    """
    exp = option_expiry_utc(symbol, expiry_time_et)
    if exp is None:
        return None
    now = datetime.now(timezone.utc)
    exp_utc = exp.astimezone(timezone.utc) if exp.tzinfo else exp.replace(tzinfo=timezone.utc)
    return (exp_utc - now).total_seconds() / 60.0


import time
from datetime import datetime, timedelta

from config.settings import MARKET_OPEN_HHMM, MARKET_CLOSE_HHMM

def is_market_open_now() -> bool:
    now = datetime.now()
    oh, om = MARKET_OPEN_HHMM
    ch, cm = MARKET_CLOSE_HHMM
    open_t = now.replace(hour=oh, minute=om, second=0, microsecond=0)
    close_t = now.replace(hour=ch, minute=cm, second=0, microsecond=0)
    return open_t <= now <= close_t

def sleep_until_next_5min():
    now = datetime.now()
    next_min = (now.minute // 5 + 1) * 5
    next_time = now.replace(second=0, microsecond=0)
    if next_min >= 60:
        next_time = next_time.replace(minute=0) + timedelta(hours=1)
    else:
        next_time = next_time.replace(minute=next_min)
    time.sleep(max(0, (next_time - now).total_seconds()))

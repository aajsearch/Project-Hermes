import os
from datetime import datetime

from config.settings import LOG_DIR, EVENTS_LOG

os.makedirs(LOG_DIR, exist_ok=True)

def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(EVENTS_LOG, "a") as f:
        f.write(line + "\n")

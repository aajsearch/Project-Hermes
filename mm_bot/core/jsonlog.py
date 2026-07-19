from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, Optional


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "ts": int(time.time() * 1000),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        extra = getattr(record, "extra", None)
        if isinstance(extra, dict):
            payload.update(extra)
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


def setup_json_logging(level: str = "INFO", name: Optional[str] = None) -> logging.Logger:
    logger = logging.getLogger(name if name else "")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    # Avoid duplicate handlers on reload
    logger.handlers = [handler]
    logger.propagate = False
    return logger


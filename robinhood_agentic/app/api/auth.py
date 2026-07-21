"""Optional auth seams for cloud (Phase 5). Localhost: no auth."""

from __future__ import annotations

import os
from typing import Callable

from fastapi import Header, HTTPException, Request


def auth_enabled() -> bool:
    return bool(os.environ.get("COMMAND_CENTER_PIN") or os.environ.get("COMMAND_CENTER_REQUIRE_AUTH"))


async def require_auth(
    request: Request,
    x_command_center_pin: str | None = Header(default=None),
) -> None:
    """No-op on localhost unless COMMAND_CENTER_PIN is set."""
    pin = os.environ.get("COMMAND_CENTER_PIN")
    if not pin:
        return
    if x_command_center_pin != pin:
        raise HTTPException(status_code=401, detail="Invalid or missing PIN")


def mutating(dep: Callable = require_auth):
    return dep

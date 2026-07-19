#!/usr/bin/env python3
"""
Diagnostic: Coinbase Advanced Trade balances per portfolio (USD, USDC, others).

Uses CDP auth: COINBASE_KEY_FILE or COINBASE_API_KEY + COINBASE_API_SECRET (see .env).

Calls:
  - GET /brokerage/portfolios — list portfolios
  - GET /brokerage/portfolios/{id} — portfolio breakdown (may 403 if key lacks access)
  - GET /brokerage/accounts?retail_portfolio_id=… — accounts scoped to that portfolio

Run from repo root: python coinbase_grid_bot/check_balances.py
"""
from __future__ import annotations

import logging
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

# Quieter SDK logs (library logs HTTP 403 before we handle it)
for _name in ("coinbase", "coinbase.rest", "coinbase.RESTClient", "urllib3"):
    logging.getLogger(_name).disabled = True

_script_dir = os.path.dirname(os.path.abspath(__file__))

try:
    from .env_utils import load_repo_env, get_env_path  # type: ignore
except ImportError:  # when executed as a script
    from env_utils import load_repo_env, get_env_path  # type: ignore

load_repo_env()

try:
    from coinbase.rest import RESTClient
except ImportError:
    print("Install coinbase-advanced-py: pip install coinbase-advanced-py")
    sys.exit(1)


def make_client() -> RESTClient:
    key_file = get_env_path("COINBASE_KEY_FILE")
    api_key = os.environ.get("COINBASE_API_KEY") or None
    api_secret = os.environ.get("COINBASE_API_SECRET") or None
    if key_file and os.path.isfile(key_file):
        return RESTClient(key_file=key_file)
    if api_key and api_secret:
        return RESTClient(api_key=api_key, api_secret=api_secret)
    print("Set COINBASE_KEY_FILE or COINBASE_API_KEY + COINBASE_API_SECRET in .env")
    sys.exit(1)


def _get(obj: Any, key: str, default: Any = None) -> Any:
    """Get attribute or dict key from SDK objects or raw dicts."""
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _amount_float(amt: Any) -> Tuple[float, str]:
    if amt is None:
        return 0.0, ""
    # SDK returns Amount(value=str, currency=str). In raw dict form: {"value": "...", "currency": "..."}.
    val = _get(amt, "value", None)
    ccy = _get(amt, "currency", None) or ""
    try:
        return float(val or 0), ccy
    except (TypeError, ValueError):
        return 0.0, ccy


def _hold_str(hold: Any) -> str:
    if hold is None:
        return ""
    if isinstance(hold, dict):
        # Often { "value": "...", "currency": "..." } or nested
        v = hold.get("value")
        c = hold.get("currency", "")
        if v is not None:
            return f"{v} {c}".strip()
    return str(hold)[:80]


def _fetch_accounts_paginated(
    client: RESTClient,
    retail_portfolio_id: Optional[str],
) -> List[Any]:
    """All accounts for one portfolio (or unscoped if retail_portfolio_id is None)."""
    out: List[Any] = []
    cursor = None
    while True:
        kwargs: Dict[str, Any] = {"limit": 250, "cursor": cursor}
        if retail_portfolio_id:
            kwargs["retail_portfolio_id"] = retail_portfolio_id
        resp = client.get_accounts(**kwargs)
        accounts = _get(resp, "accounts", None) or []
        out.extend(accounts)
        if not _get(resp, "has_next", False):
            break
        cursor = _get(resp, "cursor", None)
        if not cursor:
            break
    return out


def _fetch_accounts_safe(
    client: RESTClient,
    retail_portfolio_id: Optional[str],
    label: str,
) -> Tuple[List[Any], Optional[str]]:
    """Returns (accounts, error_reason). error_reason set on 403/other failure."""
    try:
        return _fetch_accounts_paginated(client, retail_portfolio_id), None
    except Exception as e:
        err = str(e)
        if "403" in err or "PERMISSION_DENIED" in err:
            return [], "403 — API key cannot access accounts for this portfolio"
        return [], str(e)


def _print_portfolio_breakdown(client: RESTClient, portfolio_uuid: str) -> bool:
    """
    Try GET /brokerage/portfolios/{id}. Returns True if readable, False if 403/forbidden.
    """
    try:
        b = client.get_portfolio_breakdown(portfolio_uuid)
    except Exception as e:
        err = str(e)
        if "403" in err or "PERMISSION_DENIED" in err:
            print("  get_portfolio_breakdown: 403 — this API key cannot read portfolio breakdown.")
        else:
            print(f"  get_portfolio_breakdown: {e}")
        return False
    pb = _get(b, "breakdown", None)
    if pb is None:
        print("  get_portfolio_breakdown: (empty breakdown)")
        return True
    total = _get(pb, "total_balance", None)
    tv, tc = _amount_float(total)
    print(f"  total_balance (breakdown): {tv} {tc}".strip())
    spot = _get(pb, "spot_positions", None) or []
    for pos in spot:
        sym = (_get(pos, "asset", None) or _get(pos, "symbol", None) or "?").upper()
        bal = _get(pos, "total_balance", None) or _get(pos, "available_balance", None)
        v, c = _amount_float(bal)
        if sym in ("USD", "USDC") or v > 0:
            print(f"    spot_positions {sym}: {v} {c}".strip())
    return True


def main() -> None:
    client = make_client()
    env_portfolio = (os.environ.get("COINBASE_RETAIL_PORTFOLIO_ID") or "").strip() or None

    print("=== Coinbase Advanced Trade — balance diagnostic ===\n")

    # 1) List portfolios
    try:
        presp = client.get_portfolios()
    except Exception as e:
        print(f"ERROR: get_portfolios failed: {e}")
        sys.exit(1)

    portfolios = getattr(presp, "portfolios", None) or []
    if not portfolios:
        print("No portfolios returned.\n")
    else:
        print(f"Found {len(portfolios)} portfolio(s).\n")

    # If user pinned a portfolio in .env, only that one
    to_scan: List[Tuple[str, str]] = []
    for p in portfolios:
        puuid = getattr(p, "uuid", None) or ""
        pname = getattr(p, "name", None) or ""
        if env_portfolio and puuid != env_portfolio:
            continue
        to_scan.append((puuid, pname))

    if env_portfolio and not to_scan:
        print(
            f"COINBASE_RETAIL_PORTFOLIO_ID={env_portfolio} not in listed portfolios; "
            "check the UUID.\n"
        )
        to_scan = [(env_portfolio, "(from env)")]

    all_rows: List[Tuple[str, str, str, float, str, str]] = []
    # (portfolio_name, portfolio_uuid, currency, available, hold_str, account_uuid)

    for puuid, pname in to_scan:
        if not puuid:
            continue
        print(f"--- Portfolio: {pname!r}  uuid={puuid} ---")
        _print_portfolio_breakdown(client, puuid)

        accounts, acc_err = _fetch_accounts_safe(
            client, retail_portfolio_id=puuid, label=f"retail_portfolio_id={puuid[:8]}…"
        )
        if acc_err:
            print(f"  get_accounts(retail_portfolio_id=…): {acc_err}")
        else:
            print(f"  get_accounts(retail_portfolio_id=…): {len(accounts)} account row(s)")

        # Aggregate all paginated rows first, then filter for target assets.
        usd_usdc_accounts: List[Any] = []
        for acc in accounts:
            currency = (_get(acc, "currency", None) or "?").upper()
            if currency in ("USD", "USDC"):
                usd_usdc_accounts.append(acc)

        usd_avail = 0.0
        usdc_avail = 0.0
        for acc in usd_usdc_accounts:
            currency = (_get(acc, "currency", None) or "?").upper()
            # Correct nested path: account.available_balance.value
            av, _ = _amount_float(_get(acc, "available_balance", None))
            hold = _hold_str(_get(acc, "hold", None))
            aid = _get(acc, "uuid", None) or ""
            if currency == "USD":
                usd_avail += av
            elif currency == "USDC":
                usdc_avail += av
            all_rows.append((pname, puuid, currency, av, hold, str(aid)))
            extra = f"  hold={hold}" if hold else ""
            print(f"    {currency}: available={av:.8f}{extra}  account={str(aid)[:8]}…")

        print(
            f"  Summary (USD/USDC from paginated accounts): USD available sum={usd_avail:.8f}  USDC available sum={usdc_avail:.8f}"
        )
        print()

    # 2) Unscoped list (what the API returns without portfolio filter) — for comparison
    print("--- Unscoped get_accounts() (no retail_portfolio_id) ---")
    unscoped, unscoped_err = _fetch_accounts_safe(client, retail_portfolio_id=None, label="unscoped")
    if unscoped_err:
        print(f"ERROR: {unscoped_err}")
    print(f"Total account rows: {len(unscoped)}")
    unscoped_usd = 0.0
    unscoped_usdc = 0.0
    for acc in unscoped:
        currency = (_get(acc, "currency", None) or "?").upper()
        if currency not in ("USD", "USDC"):
            continue
        av, _ = _amount_float(_get(acc, "available_balance", None))
        pid = _get(acc, "retail_portfolio_id", None) or ""
        if currency == "USD":
            unscoped_usd += av
        else:
            unscoped_usdc += av
        print(f"  {currency}: available={av:.8f}  retail_portfolio_id={pid or '(none)'}")
    print(f"Unscoped totals: USD={unscoped_usd:.8f}  USDC={unscoped_usdc:.8f}")
    print()

    # 3) Aggregate table: balance > 0
    positive = [r for r in all_rows if r[3] > 0]
    if not positive:
        print("No accounts with available balance > 0 in per-portfolio scans (USD/USDC rows printed above even if zero).")
        print()
        print("If the app shows USD/USDC but this shows zeros:")
        print("  • Funds may be in a portfolio this API key cannot read (403 on breakdown).")
        print("  • Or balance is on coinbase.com / another product, not Advanced Trade brokerage.")
        print("  • Or use a CDP key with View + trade permissions for the correct portfolio.")
    else:
        print("Accounts with available > 0 (per-portfolio scan):\n")
        for pname, puuid, currency, av, hold, aid in sorted(positive, key=lambda x: (-x[3], x[2])):
            print(f"  {pname}: {currency}  available={av:.8f}  portfolio={puuid[:8]}…  account={aid[:8]}…")
        print(f"\nTotal rows: {len(positive)}")


if __name__ == "__main__":
    main()

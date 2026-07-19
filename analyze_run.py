#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple


DB_DEFAULT = "mm_bot/data/mm_bot.sqlite3"


@dataclass
class Fill:
    ts: int
    product_id: str
    side: str  # BUY/SELL
    price: float
    size: float
    fee: float


def _money(x: float) -> str:
    return f"{x:,.2f}"


def _qty(x: float) -> str:
    return f"{x:,.6f}"


def load_fills(conn: sqlite3.Connection) -> List[Fill]:
    rows = conn.execute(
        """
        SELECT ts, product_id, side, price, size, fee
          FROM fills
         ORDER BY ts ASC
        """
    ).fetchall()
    out: List[Fill] = []
    for r in rows:
        out.append(
            Fill(
                ts=int(r[0]),
                product_id=str(r[1]),
                side=str(r[2]).upper(),
                price=float(r[3]),
                size=float(r[4]),
                fee=float(r[5] or 0.0),
            )
        )
    return out


def realized_pnl_from_fills_avg_cost(fills: List[Fill]) -> Tuple[float, float]:
    """
    Approximate realized PnL using average-cost inventory accounting.
    Returns (realized_pnl, total_fees).
    Assumes base asset quantity changes by fill.size and quote is USD.
    """
    pos_qty = 0.0
    avg_cost = 0.0
    realized = 0.0
    fees = 0.0

    for f in fills:
        fees += f.fee
        if f.side == "BUY":
            new_qty = pos_qty + f.size
            if new_qty != 0:
                avg_cost = (avg_cost * pos_qty + f.price * f.size) / new_qty
            pos_qty = new_qty
        elif f.side == "SELL":
            # Realize pnl vs avg cost on sold size
            sold = min(abs(pos_qty), f.size) if pos_qty != 0 else f.size
            realized += (f.price - avg_cost) * sold
            pos_qty = pos_qty - f.size
            if pos_qty == 0:
                avg_cost = 0.0
        else:
            continue

    return realized - fees, fees


def last_mid_from_orders(conn: sqlite3.Connection, product_id: Optional[str]) -> Optional[Tuple[float, float, float]]:
    """
    Proxy mid/spread from last OPEN/FILLED order prices:
    Returns (best_bid, best_ask, mid) if both sides exist.
    """
    bid_where = "WHERE side='BUY'"
    ask_where = "WHERE side='SELL'"
    args: Tuple[str, ...] = ()
    if product_id:
        bid_where += " AND product_id=?"
        ask_where += " AND product_id=?"
        args = (product_id, product_id)

    row = conn.execute(
        f"""
        SELECT
          (SELECT price FROM orders {bid_where} ORDER BY updated_ts DESC LIMIT 1) AS bid,
          (SELECT price FROM orders {ask_where} ORDER BY updated_ts DESC LIMIT 1) AS ask
        """,
        args,
    ).fetchone()
    if not row:
        return None
    bid, ask = row[0], row[1]
    if bid is None or ask is None:
        return None
    bidf, askf = float(bid), float(ask)
    return bidf, askf, (bidf + askf) / 2.0


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=DB_DEFAULT, help="Path to SQLite database")
    p.add_argument("--product", default=None, help="Optional product_id filter, e.g. SOL-USD")
    args = p.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    positions = conn.execute("SELECT asset, qty, avg_price, updated_ts FROM positions ORDER BY asset").fetchall()

    print("\n=== MM Bot Run Summary ===")
    print(f"DB: {db_path}")
    if args.product:
        print(f"Product filter: {args.product}")

    print("\n--- Final positions ---")
    if not positions:
        print("(none)")
    else:
        for r in positions:
            print(f"- {r['asset']}: qty={_qty(float(r['qty']))} avg_price={_money(float(r['avg_price']))}")

    fills_all = load_fills(conn)

    # Determine which products to report.
    if args.product:
        product_ids = [args.product]
    else:
        rows = conn.execute(
            """
            SELECT DISTINCT product_id FROM (
              SELECT product_id FROM orders
              UNION
              SELECT product_id FROM fills
            )
            WHERE product_id IS NOT NULL AND product_id <> ''
            ORDER BY product_id
            """
        ).fetchall()
        product_ids = [str(r[0]) for r in rows]

    def _print_efficiency(product_id: Optional[str]) -> None:
        if product_id:
            order_count = conn.execute("SELECT COUNT(1) FROM orders WHERE product_id = ?", (product_id,)).fetchone()[0]
            fill_count = conn.execute("SELECT COUNT(1) FROM fills WHERE product_id = ?", (product_id,)).fetchone()[0]
        else:
            order_count = conn.execute("SELECT COUNT(1) FROM orders").fetchone()[0]
            fill_count = conn.execute("SELECT COUNT(1) FROM fills").fetchone()[0]
        order_count = int(order_count or 0)
        fill_count = int(fill_count or 0)
        ratio = (order_count / fill_count) if fill_count > 0 else None
        print(f"Orders (rows):    {order_count}")
        print(f"Fills (rows):     {fill_count}")
        if ratio is None:
            print("Order-to-Fill:    n/a (no fills)")
        else:
            print(f"Order-to-Fill:    {ratio:.2f} (lower is better)")

    def _print_spread_proxy(product_id: Optional[str]) -> None:
        mid = last_mid_from_orders(conn, product_id)
        if mid is None:
            print("No bid/ask prices found in orders.")
            return
        bid, ask, m = mid
        spread = ask - bid
        print(f"Last bid:   {_money(bid)}")
        print(f"Last ask:   {_money(ask)}")
        print(f"Mid (proxy):{_money(m)}")
        print(f"Spread:     {_money(spread)} ({(spread/m*1e4):.2f} bps)")

    # Overall (all products)
    fills_overall = fills_all if not args.product else [f for f in fills_all if f.product_id == args.product]
    buy_count = sum(1 for f in fills_overall if f.side == "BUY")
    sell_count = sum(1 for f in fills_overall if f.side == "SELL")
    total_volume = sum(f.size for f in fills_overall)
    notional = sum(f.size * f.price for f in fills_overall)
    realized_pnl, total_fees = realized_pnl_from_fills_avg_cost(fills_overall)

    print("\n--- Overall (all products) ---")
    print("\nTrades (fills)")
    print(f"Total fills:      {len(fills_overall)}")
    print(f"BUY fills:        {buy_count}")
    print(f"SELL fills:       {sell_count}")
    print(f"Total volume:     {_qty(total_volume)}")
    print(f"Total notional:   ${_money(notional)}")
    print(f"Total fees:       ${_money(total_fees)}")
    print("\nPnL (approx)")
    print(f"Realized PnL (avg cost, minus fees): ${_money(realized_pnl)}")
    print("\nSpread / mid proxy")
    _print_spread_proxy(args.product if args.product else None)
    print("\nEfficiency Metrics")
    _print_efficiency(args.product if args.product else None)

    # Per-product breakdown (only when not filtered)
    if not args.product:
        for pid in product_ids:
            fills = [f for f in fills_all if f.product_id == pid]
            buy_count = sum(1 for f in fills if f.side == "BUY")
            sell_count = sum(1 for f in fills if f.side == "SELL")
            total_volume = sum(f.size for f in fills)
            notional = sum(f.size * f.price for f in fills)
            realized_pnl, total_fees = realized_pnl_from_fills_avg_cost(fills)

            print(f"\n--- Product: {pid} ---")
            print("\nTrades (fills)")
            print(f"Total fills:      {len(fills)}")
            print(f"BUY fills:        {buy_count}")
            print(f"SELL fills:       {sell_count}")
            print(f"Total volume:     {_qty(total_volume)}")
            print(f"Total notional:   ${_money(notional)}")
            print(f"Total fees:       ${_money(total_fees)}")
            print("\nPnL (approx)")
            print(f"Realized PnL (avg cost, minus fees): ${_money(realized_pnl)}")
            print("\nSpread / mid proxy")
            _print_spread_proxy(pid)
            print("\nEfficiency Metrics")
            _print_efficiency(pid)

    print()


if __name__ == "__main__":
    main()


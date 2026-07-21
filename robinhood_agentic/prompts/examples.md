# Example prompts — Robinhood Agentic (Cursor Agent)

Copy into Agent chat (`Cmd+L`) after MCP is connected.

## Read-only (start here)

```
Using Robinhood MCP, list my Agentic account buying power and all open positions.
```

```
Using Robinhood MCP, show my last 10 orders in the Agentic account with status and fill price.
```

```
Using Robinhood MCP, what is the current quote for AAPL? Do not place any orders.
```

## Analysis (no trades)

```
Using Robinhood MCP, summarize sector concentration and largest positions in my Agentic account. Flag any single-name risk above 20% of equity.
```

```
Using Robinhood MCP, compare my Agentic portfolio to a simple 60/40 SPY/AGG target. Suggest rebalance trades but do not execute until I approve.
```

## Tech scalper (pilot)

See [`tech_scalper.md`](tech_scalper.md).

```
Follow tech_scalper.yaml. Pre-market scan, full 33-name watchlist, max 2 legs, cap $100/share. No orders.
```

## Leveraged ETF (pilot)

See [`leveraged_etf.md`](leveraged_etf.md).

```
Follow leveraged_etf.yaml. QQQ regime ±0.50%; TQQQ or SQQQ only; 1 position; TP +2%/SL -1.2%. No orders.
```

## Options (read-only until enrolled)

See [`options_mcp.md`](options_mcp.md).

```
Follow options_directional.yaml. Check option_level; chain scan SPY/QQQ, delta on get_option_quotes. No orders.
```

## Cross-strategy rule

**Equity + options same day OK** on the Agentic $500 account. Avoid leveraged ETF the same day.

## Trading (require confirmation)

```
Using Robinhood MCP, prepare a limit buy for 1 share of SPY at $0.50 below the current bid in my Agentic account. Show the full order details and wait for my explicit "yes" before submitting.
```

```
Using Robinhood MCP, cancel all open orders in my Agentic account and confirm what was canceled.
```

## Automation (high risk — only if you accept unattended trades)

```
Using Robinhood MCP, when I say "go", buy $100 notional of QQQ in my Agentic account with a market order. Otherwise only show the plan.
```

Robinhood example ideas (informational only): [Agentic Trading overview](https://robinhood.com/us/en/support/articles/agentic-trading-overview/).

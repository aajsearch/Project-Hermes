"""
Alpaca ETF position scalper (TSLL, FNGU):
  python3 -m bot.alpaca_etf_scalper_main --config config/alpaca_etf_scalper.yaml
"""

from __future__ import annotations

from bot.alpaca_etf_scalper.bot import main

if __name__ == "__main__":
    main()


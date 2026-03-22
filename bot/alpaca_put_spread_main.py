"""
Deprecated entrypoint; use ``python3 -m bot.alpaca_options_main``.

This module remains so existing scripts/cron using ``-m bot.alpaca_put_spread_main`` keep working.
"""

from bot.alpaca_options_main import main

if __name__ == "__main__":
    main()

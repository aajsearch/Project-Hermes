import os
from alpaca.data.historical.option import OptionHistoricalDataClient

from core.logger import log
from core.scheduler import is_market_open_now, sleep_until_next_5min
from core.engine import load_yaml, run_cycle, reconcile_equity_positions_from_broker, reconcile_option_positions_from_broker
from core.state_store import load_state, save_state
from broker.alpaca.client import make_stock_data_client, make_trading_client
from config.settings import FORCE_RUN_WHEN_MARKET_CLOSED, RUN_ONCE


def main():
    log("Trading Assistant started (paper, equities paper-exec v1, options paper-exec v1)")

    stock_data = make_stock_data_client()
    trading = make_trading_client(paper=True)

    option_data = OptionHistoricalDataClient(
        os.getenv("ALPACA_API_KEY"),
        os.getenv("ALPACA_SECRET_KEY"),
    )

    acct = trading.get_account()
    log(f"Paper account buying_power={acct.buying_power} cash={acct.cash}")

    watchlist = load_yaml("config/watchlist.yaml")
    profiles = load_yaml("config/profiles.yaml")

    # One-time reconcile on startup
    state = load_state()
    equity_symbols = [i["symbol"] for i in watchlist.get("equities", [])]
    option_underlyings = [i["underlying"] for i in watchlist.get("options", [])]
    reconcile_equity_positions_from_broker(state, trading, stock_data, equity_symbols)
    reconcile_option_positions_from_broker(state, trading, option_underlyings)
    save_state(state)
    log("Startup reconcile complete.")

    while True:
        market_open = is_market_open_now()

        if market_open or FORCE_RUN_WHEN_MARKET_CLOSED:
            if not market_open:
                log("Market closed, FORCE_RUN_WHEN_MARKET_CLOSED=True → running one cycle using last available data.")
            run_cycle(stock_data, trading, option_data, watchlist, profiles, market_open=market_open)
        else:
            log("Market closed (equities/options). Waiting...")

        if RUN_ONCE:
            log("RUN_ONCE=True → exiting after one cycle.")
            return

        sleep_until_next_5min()


if __name__ == "__main__":
    main()

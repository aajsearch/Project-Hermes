import os
import signal
from dotenv import load_dotenv
from alpaca.data.historical.option import OptionHistoricalDataClient

from options.selector import pick_best_call

load_dotenv()
API_KEY = os.getenv("ALPACA_API_KEY")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")


class Timeout(Exception):
    pass


def _alarm_handler(signum, frame):
    raise Timeout("Timed out while fetching option chain / selecting contract.")


def main():
    if not API_KEY or not SECRET_KEY:
        raise ValueError("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY in .env")

    oc = OptionHistoricalDataClient(API_KEY, SECRET_KEY)

    underlyings = ["SPY", "QQQ", "AAPL", "AMZN"]

    # Setup alarm handler
    signal.signal(signal.SIGALRM, _alarm_handler)

    for und in underlyings:
        print(f"\n--- Picking for {und} ---", flush=True)

        try:
            # 20s hard timeout per underlying
            signal.alarm(20)

            best = pick_best_call(
                option_client=oc,
                underlying=und,
                dte_min=60,
                dte_max=120,
                delta_min=0.40,
                delta_max=0.55,
                iv_max=0.50,
                spread_pct_max=0.05,
            )

            signal.alarm(0)

            if best is None:
                print(f"{und}: No contract found (filters too strict or missing greeks).", flush=True)
            else:
                print(
                    f"{und}: {best.symbol} | DTE={best.dte} | delta={best.delta:.3f} | "
                    f"IV={best.iv if best.iv is not None else 'NA'} | "
                    f"bid/ask={best.bid}/{best.ask} | spread%={best.spread_pct*100:.2f}%",
                    flush=True
                )

        except Timeout as e:
            signal.alarm(0)
            print(f"{und}: TIMEOUT -> {e}", flush=True)

        except KeyboardInterrupt:
            signal.alarm(0)
            raise

        except Exception as e:
            signal.alarm(0)
            print(f"{und}: ERROR -> {e}", flush=True)


if __name__ == "__main__":
    main()

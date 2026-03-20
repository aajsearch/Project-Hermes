# Market hours for equities (Pacific Time)
MARKET_OPEN_HHMM = (6, 30)
MARKET_CLOSE_HHMM = (13, 0)

LOG_DIR = "logs"
STATE_DIR = "state"
STATE_PATH = "state/state.json"
SIGNALS_CSV = "logs/signals.csv"
EVENTS_LOG = "logs/events.log"
TRADES_LEDGER_CSV = "logs/trades.csv"

# --- runtime controls ---
FORCE_RUN_WHEN_MARKET_CLOSED = False
RUN_ONCE = False

# --- global safety ---
KILL_SWITCH = True    # If False, bot will NOT place any order (signals still computed, log "SKIP: kill_switch_off")
ALLOW_EXECUTION_WHEN_CLOSED = False  # keep False for normal hours only

# --- equities execution (paper) ---
EXECUTE_TRADES = True
DEFAULT_ORDER_TYPE = "market"
MAX_POSITION_PER_SYMBOL = 1
MAX_BUYS_PER_CYCLE = 10             # cap new buys per cycle
MAX_BUYS_PER_HOUR = 20              # cap new buys per hour

# --- options execution (paper) ---
EXECUTE_OPTION_TRADES = True
ALLOW_OPTION_EXECUTION_WHEN_CLOSED = False

OPTION_TRADE_NOTIONAL_USD = 500
OPTION_STOP_LOSS_PCT = 0.20
OPTION_TAKE_PROFIT_PCT = 0.30
OPTION_MAX_HOLD_MINUTES = 1440
OPTION_COOLDOWN_MINUTES = 60

# --- portfolio-level risk ---
PORTFOLIO_PROFIT_LOCK_PCT = 0.05    # exit all if portfolio profit >= 2% of invested
PORTFOLIO_LOSS_LIMIT_PCT = 0.15     # exit all if portfolio loss >= 2% of invested
PORTFOLIO_COOLDOWN_MINUTES = 60     # after portfolio exit, no new entries for 1h

# --- strategy version (for ledger / EOD) ---
STRATEGY_VERSION = "1.1"

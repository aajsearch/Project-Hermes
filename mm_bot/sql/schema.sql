PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- Orders placed by the bot (desired + exchange lifecycle)
CREATE TABLE IF NOT EXISTS orders (
  order_id TEXT PRIMARY KEY,                 -- exchange order_id (or mock id)
  client_order_id TEXT UNIQUE,               -- idempotency key we generate
  product_id TEXT NOT NULL,
  side TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
  price REAL NOT NULL,
  size REAL NOT NULL,
  post_only INTEGER NOT NULL DEFAULT 1,
  status TEXT NOT NULL,                      -- OPEN / PARTIAL / FILLED / CANCELED / REJECTED / UNKNOWN
  exchange_status TEXT,                      -- raw status (optional)
  created_ts INTEGER NOT NULL,
  updated_ts INTEGER NOT NULL
);

-- Executions/fills (can be partial)
CREATE TABLE IF NOT EXISTS fills (
  fill_id TEXT PRIMARY KEY,
  order_id TEXT NOT NULL,
  product_id TEXT NOT NULL,
  side TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
  price REAL NOT NULL,
  size REAL NOT NULL,
  fee REAL NOT NULL DEFAULT 0,
  liquidity TEXT,                            -- MAKER / TAKER if available
  ts INTEGER NOT NULL,
  FOREIGN KEY(order_id) REFERENCES orders(order_id)
);

-- Current positions (per asset)
CREATE TABLE IF NOT EXISTS positions (
  asset TEXT PRIMARY KEY,
  qty REAL NOT NULL,
  avg_price REAL NOT NULL,
  updated_ts INTEGER NOT NULL
);

-- PnL time series (for reporting + risk)
CREATE TABLE IF NOT EXISTS pnl_history (
  ts INTEGER PRIMARY KEY,
  unrealized_pnl REAL NOT NULL,
  realized_pnl REAL NOT NULL,
  fees REAL NOT NULL,
  notional REAL NOT NULL
);

-- Bot state for recovery (single-row key/value)
CREATE TABLE IF NOT EXISTS bot_state (
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL,
  updated_ts INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_fills_order ON fills(order_id);


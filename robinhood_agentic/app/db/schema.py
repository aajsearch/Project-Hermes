"""SQLite schema for Command Center."""

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS system_state (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  mode TEXT NOT NULL DEFAULT 'autonomous',
  halt_reason TEXT,
  scanner_enabled INTEGER NOT NULL DEFAULT 1,
  auto_entry_enabled INTEGER NOT NULL DEFAULT 1,
  auto_exit_enabled INTEGER NOT NULL DEFAULT 1,
  same_day_symbol_block INTEGER NOT NULL DEFAULT 1,
  mcp_ok INTEGER NOT NULL DEFAULT 0,
  mcp_detail TEXT,
  engine_heartbeat_at TEXT,
  engine_started_at TEXT,
  dry_run INTEGER NOT NULL DEFAULT 0,
  poll_seconds INTEGER NOT NULL DEFAULT 15,
  rescan_minutes INTEGER NOT NULL DEFAULT 15,
  account_number TEXT,
  account_nickname TEXT DEFAULT 'Agentic',
  updated_at TEXT
);

CREATE TABLE IF NOT EXISTS runtime_config (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  playbook TEXT NOT NULL,
  version INTEGER NOT NULL,
  config_json TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT 'yaml_seed',
  updated_at TEXT NOT NULL,
  updated_by TEXT NOT NULL DEFAULT 'system',
  active INTEGER NOT NULL DEFAULT 0,
  UNIQUE(playbook, version)
);

CREATE TABLE IF NOT EXISTS positions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol TEXT NOT NULL UNIQUE,
  qty REAL NOT NULL,
  entry REAL NOT NULL,
  tp REAL NOT NULL,
  sl REAL NOT NULL,
  synthetic_tp INTEGER NOT NULL DEFAULT 1,
  synthetic_sl INTEGER NOT NULL DEFAULT 0,
  fractional INTEGER NOT NULL DEFAULT 0,
  pending INTEGER NOT NULL DEFAULT 0,
  auto_exit INTEGER NOT NULL DEFAULT 1,
  buy_order_id TEXT,
  sl_order_id TEXT,
  opened_at TEXT,
  adopted INTEGER NOT NULL DEFAULT 0,
  expected_mid REAL,
  meta_json TEXT
);

CREATE TABLE IF NOT EXISTS option_positions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  option_id TEXT NOT NULL UNIQUE,
  symbol TEXT NOT NULL,
  label TEXT,
  option_type TEXT,
  strike REAL,
  expiration TEXT,
  qty REAL NOT NULL DEFAULT 1,
  entry REAL NOT NULL,
  tp REAL NOT NULL,
  sl REAL NOT NULL,
  multiplier REAL NOT NULL DEFAULT 100,
  synthetic_tp INTEGER NOT NULL DEFAULT 1,
  synthetic_sl INTEGER NOT NULL DEFAULT 1,
  auto_exit INTEGER NOT NULL DEFAULT 1,
  buy_order_id TEXT,
  opened_at TEXT,
  adopted INTEGER NOT NULL DEFAULT 0,
  expected_mid REAL,
  meta_json TEXT
);

CREATE TABLE IF NOT EXISTS orders (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  broker_order_id TEXT,
  asset_type TEXT NOT NULL,
  symbol TEXT,
  option_id TEXT,
  side TEXT NOT NULL,
  order_type TEXT,
  quantity REAL,
  expected_mid REAL,
  submitted_price REAL,
  fill_price REAL,
  slippage_bps REAL,
  state TEXT NOT NULL DEFAULT 'pending',
  exit_reason TEXT,
  created_at TEXT NOT NULL,
  filled_at TEXT,
  meta_json TEXT
);

CREATE TABLE IF NOT EXISTS trades (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  asset_type TEXT NOT NULL,
  symbol TEXT,
  option_id TEXT,
  label TEXT,
  qty REAL NOT NULL,
  entry_price REAL NOT NULL,
  exit_price REAL NOT NULL,
  pnl_usd REAL NOT NULL,
  pnl_pct REAL,
  r_multiple REAL,
  exit_reason TEXT NOT NULL,
  opened_at TEXT,
  closed_at TEXT NOT NULL,
  entry_expected_mid REAL,
  exit_fill_latency_ms REAL,
  meta_json TEXT
);

CREATE TABLE IF NOT EXISTS daily_stats (
  trade_date TEXT PRIMARY KEY,
  realized_pnl REAL NOT NULL DEFAULT 0,
  trade_count INTEGER NOT NULL DEFAULT 0,
  equity_sl_hits INTEGER NOT NULL DEFAULT 0,
  option_losses INTEGER NOT NULL DEFAULT 0,
  wins INTEGER NOT NULL DEFAULT 0,
  losses INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT
);

CREATE TABLE IF NOT EXISTS audit_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS scan_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  trigger TEXT NOT NULL DEFAULT 'scheduled',
  rows_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS command_queue (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL,
  command_type TEXT NOT NULL,
  payload_json TEXT NOT NULL DEFAULT '{}',
  status TEXT NOT NULL DEFAULT 'pending',
  result_json TEXT,
  processed_at TEXT
);

CREATE TABLE IF NOT EXISTS pending_approvals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL,
  action_type TEXT NOT NULL,
  symbol TEXT,
  option_id TEXT,
  proposal_json TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  decided_at TEXT,
  decided_by TEXT
);

CREATE TABLE IF NOT EXISTS entry_blocks (
  symbol TEXT NOT NULL,
  block_date TEXT NOT NULL,
  reason TEXT,
  PRIMARY KEY (symbol, block_date)
);

CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts DESC);
CREATE INDEX IF NOT EXISTS idx_commands_status ON command_queue(status, id);
CREATE INDEX IF NOT EXISTS idx_orders_created ON orders(created_at);
CREATE INDEX IF NOT EXISTS idx_trades_closed ON trades(closed_at);
"""

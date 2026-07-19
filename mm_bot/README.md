## MM Bot (Simple: 1 Bid / 1 Ask)

Event-driven, fully async market making bot scaffold.

### Run

```bash
python3 -m mm_bot.bot --config mm_bot/config.yaml
```

### Notes
- Set `exchange.mode` to `mock` to test without touching Coinbase.
- For Coinbase, set CDP creds in `.env` (`COINBASE_KEY_FILE` or `COINBASE_API_KEY`/`COINBASE_API_SECRET`).


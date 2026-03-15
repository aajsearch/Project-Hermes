# Rotate Secrets After .env Was Committed

Because `.env` was previously committed to git history, treat all values that were ever in it as **exposed**. Rotate the following and update your local `.env` (never commit `.env` again).

---

## 1. Keys / credentials to rotate

| Secret | Where it's used | Why rotate |
|--------|-----------------|------------|
| **KALSHI_API_KEY** | Kalshi API auth (trading, orders, positions) | Anyone with this + private key can act as you on Kalshi. |
| **KALSHI_PRIVATE_KEY** | Path to PEM file (or inline PEM in .env) | Used with API key to sign requests. If path was in .env, ensure the PEM file was never in repo; if you ever put inline PEM in .env, the key is exposed. |
| **SMTP_PASS** | Gmail/email app password (hourly reports, alerts) | Allows sending email as your account. |
| **SMTP_USER** | Email address for SMTP | Optional but recommended to change if you rotate the app password. |
| **COINBASE_API_KEY** / **COINBASE_API_SECRET** | Only if you use `coinbase_grid_bot` and had these in .env | Full access to linked Coinbase account. |

**No need to rotate:** `KALSHI_BASE_URL`, `KALSHI_USE_WS`, `SMTP_HOST`, `SMTP_PORT`, `SMTP_FROM`, `SMTP_TO`, `V2_DRY_RUN`, and other non-secret config.

---

## 2. Step-by-step rotation

### Step 1: Rotate Kalshi API key and private key

1. Log in to [Kalshi](https://kalshi.com) (or your Kalshi API dashboard).
2. Go to **API / API Keys** (or **Settings → API**).
3. **Revoke** or **delete** the existing API key that was in `.env`.
4. **Create a new API key** and download or copy the new key ID and the new **private key** (PEM).
5. Save the new private key to a **new** PEM file (e.g. `kalshi_private_key_new.pem`) in a location **outside** the repo and not under a path that was ever committed.
   - Do **not** put this file path or PEM content in git.
6. In your local `.env` (only on your machine), set:
   ```bash
   KALSHI_API_KEY=<new-api-key-id>
   KALSHI_PRIVATE_KEY=/absolute/path/to/kalshi_private_key_new.pem
   ```
7. Restart any running bot or pipeline that uses Kalshi.

---

### Step 2: Rotate Gmail / SMTP app password (if you use email reports)

1. Go to [Google Account → Security](https://myaccount.google.com/security).
2. Under **“How you sign in to Google”**, open **2-Step Verification** (must be on).
3. At the bottom, open **App passwords**.
4. **Remove** the app password you used for this project (if you can identify it).
5. **Create** a new app password (e.g. “Project Hermes” or “Trading bot”).
6. Copy the 16-character password.
7. In your local `.env`, set:
   ```bash
   SMTP_USER=your-email@gmail.com
   SMTP_PASS=<new-16-char-app-password>
   SMTP_FROM=your-email@gmail.com
   SMTP_TO=recipient@example.com
   ```
8. Restart any process that sends email (e.g. hourly report job).

---

### Step 3: Rotate Coinbase API credentials (only if you used them in .env)

If you had `COINBASE_API_KEY`, `COINBASE_API_SECRET`, or `COINBASE_KEY_FILE` in `.env`:

1. Log in to [Coinbase](https://www.coinbase.com) → **Settings** (or Developer / API).
2. Revoke or delete the existing API key that was in `.env`.
3. Create a new API key with the same permissions you need.
4. Update your local `.env` with the new key/secret (or new key file path).
5. Restart the Coinbase grid bot if it’s running.

---

### Step 4: Confirm .env is ignored and never committed

1. Ensure `.env` is in `.gitignore` (it is, with the hardened rules).
2. From the repo root, run:
   ```bash
   git status
   ```
   You should **not** see `.env` listed. If you do, run:
   ```bash
   git rm --cached .env
   git commit -m "Ensure .env is not tracked"
   ```
3. Never add `.env` to a commit again; use `.env.example` as the template only.

---

### Step 5: Optional – revoke old keys immediately

If you can’t create new keys before revoking:

1. Revoke the old Kalshi key first, then create and configure the new one (bot will be down until .env is updated).
2. Same for SMTP: remove old app password, then create new one and update `.env`.

---

## 6. Summary checklist

- [ ] Kalshi: revoke old API key, create new key and new PEM file, update `KALSHI_API_KEY` and `KALSHI_PRIVATE_KEY` in `.env`.
- [ ] SMTP (if used): remove old app password, create new one, update `SMTP_PASS` (and optionally `SMTP_USER`) in `.env`.
- [ ] Coinbase (if used): revoke old API key, create new one, update `.env`.
- [ ] Confirm `git status` does not show `.env`.
- [ ] Restart any running bots or jobs that use these credentials.

After this, treat the old credentials as **compromised** and do not reuse them anywhere.

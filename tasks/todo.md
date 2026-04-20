# Pre-Earnings UOA Backtester + Notification Page

Ref: `/Users/ravshanrakhmanov/.claude/plans/hashed-whistling-dream.md`

Status: PR1 in progress (data pipeline). Shipped PRs check in below.

## PR1 — Data pipeline (Python, offline)

- [ ] Create `build_earnings_universe.py`
  - S&P 500 from Wikipedia → `data/sp500_constituents.json`
  - 3 yr earnings calendar from Polygon `/vX/reference/financials` (filing_date as proxy) → `data/earnings_calendar.json`
  - Resumable; `--tickers` subset flag for testing
- [ ] Create `build_earnings_uoa_data.py`
  - Polygon S3 flat files `us_options_opra/day_aggs_v1/YYYY/MM/YYYY-MM-DD.csv.gz` + `open_interest_v1/...` for per-contract daily OHLCV + OI (filtered per-ticker)
  - Polygon REST for ATM IV (~2-4 contracts per event via inverse Black-Scholes)
  - Polygon REST `/v2/aggs/ticker/{TICKER}/range/1/day/{earnings_date}/{earnings_date+10}` for post-earnings labels
  - Large block detection: flat-file `trades_v1` filtered `size ≥ 100 AND premium ≥ $25k`
  - Yearly split output `data/earnings_uoa_{year}.json` + `data/earnings_uoa_index.json`
  - Resumable per (ticker, report_date) pair
- [ ] Update `requirements.txt` — add `requests`, `lxml`, `scipy`, `boto3` (S3 client for Polygon flat files)
- [ ] Dry-run: `python3 build_earnings_universe.py --tickers AAPL` — verify constituents fetch + AAPL earnings list shape
- [ ] Dry-run: `python3 build_earnings_uoa_data.py --tickers AAPL --year 2024` — verify yearly JSON with ~4 events for AAPL
- [ ] Check size estimate holds (expected <1 MB for single ticker/year, <30 MB/year full universe)
- [ ] Full 3-year backfill for 500 tickers (~27 MB/year expected)
- [ ] Update `.gitignore` whitelist:
  - `!data/earnings_uoa_*.json`
  - `!data/earnings_uoa_index.json`
  - `!data/earnings_calendar.json`
  - `!data/sp500_constituents.json`
  - `!build_earnings_universe.py`
  - `!build_earnings_uoa_data.py`
- [ ] `git add -f` the data files, commit, push (CLAUDE.md workflow)
- [ ] Verify GitHub Pages serves the JSON (`https://rava8989.github.io/brave/data/earnings_uoa_index.json` returns 200)

## PR2 — Backtester UI + shared engine

- [ ] Create `earnings-engine.js` — SSOT for UOA scoring (VOR, Premium, IV Rank, Blocks)
- [ ] Create `earnings_uoa.html` — clone `diagonal.html` structure; 4 independent signal cards; per-signal breakdown
- [ ] Add UOA nav link to `index.html`, `history.html`, `diagonal.html`
- [ ] Extend `scripts/check-strategy-independence.sh` with UOA signal-name patterns
- [ ] Smoke test: 3-yr full-universe backtest runs <5s; per-signal breakdown shows 4 distinct WRs

## PR3 — Live page + worker cron

- [ ] Create `earnings_live.html` — read-only dashboard for tomorrow's reporters
- [ ] Add Live nav link across pages
- [ ] `wrangler.toml` — add crons `"30 20 * * 1-5"` + `"30 21 * * 1-5"`
- [ ] `schwab-proxy.js` — import `earnings-engine.js`; add `fetchSchwabOptionsChain()`, `handleEarningsUOA()`, dispatcher branch in `handleScheduled`
- [ ] New routes: `GET /earnings-signals`, `GET /earnings-signals/history`, `POST /earnings-sync`, `POST /earnings-backtest-stats`, `GET /earnings-test`
- [ ] KV prefix `earnings_` (verified non-colliding)
- [ ] Append SSOT rule to `tasks/lessons.md` (earnings-engine.js single-source)
- [ ] Verify KV writes after deploy (per global feedback rule)

## PR4 — Telegram channel

- [ ] Create `telegram-proxy.js` + `wrangler-telegram.toml`
- [ ] Add `[[services]] TELEGRAM_PROXY` to main `wrangler.toml`
- [ ] Add `sendTelegram(env, message)` helper in `schwab-proxy.js`
- [ ] Bot setup: `@BotFather` → `TELEGRAM_BOT_TOKEN` → `wrangler secret put`
- [ ] Chat ID discovery via `/getUpdates`; store in KV `telegram_config`
- [ ] Deploy + test send

## PR5 — SMS channel (Twilio) + notification rules

- [ ] Create `sms-proxy.js` + `wrangler-sms.toml`
- [ ] Add `[[services]] SMS_PROXY` to main `wrangler.toml`
- [ ] Add `sendSMS()`, `checkNotificationGate()`, daily-cap, dedupe helpers in `schwab-proxy.js`
- [ ] Secrets: `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_FROM_NUMBER`
- [ ] KV `sms_config: {phoneNumber, enabled, devMode}`
- [ ] Notification rules (per plan §I): 1/2/3+ signal severity ladder with channel floors
- [ ] E2E: force-fire 3 signals on one ticker, verify Discord + Telegram + SMS all receive

## Review

_Will fill this in post-shipment._

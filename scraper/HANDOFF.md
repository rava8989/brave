# Σ3 Live Pipeline — Windows Machine Handoff

> This file is a communication bridge between two Claude sessions.
> Mac session built everything. Windows session runs it.
> Read this fully before doing anything.

---

## What This System Does

Automated M8BF butterfly trading pipeline:

1. **Discord scraper** (this Windows machine) monitors a Discord channel for M8BF signals every 30s using Playwright Chrome automation
2. When a valid signal hits the trading window → writes `today_trade.json` → pushes to GitHub
3. Every 60s during market hours → fetches live SPX price from Schwab API → writes `spx_live.json` → pushes to GitHub
4. At 16:01 ET → computes final P&L → closes trade in `today_trade.json`
5. Website at `https://rava8989.github.io/brave/live.html` polls those JSON files and shows live P&L chart

---

## Files In This Folder

| File | Purpose |
|------|---------|
| `schwab_auth.py` | **Run once** — Schwab OAuth setup, saves `tokens.json` |
| `schwab_client.py` | Token lifecycle + SPX quote fetching from Schwab |
| `discord_scraper.py` | Playwright Chrome Discord monitor |
| `github_push.py` | Pushes JSON files to GitHub via REST API |
| `live_updater.py` | **Main loop** — orchestrates everything, run daily |
| `requirements.txt` | Python dependencies |
| `.env.example` | Template for credentials |

---

## M8BF Model Rules (critical — don't change these)

**Trading Windows (ET):**
- Mon: 11:00–11:30
- Tue: 13:30–14:00
- Wed: 12:00–12:30
- Thu: 11:00–11:30
- Fri: 13:00–13:30

**Butterfly structure:** center ± 50 (wing width = 50 each side)
- `bf_lower = center - 50`
- `bf_upper = center + 50`
- `T1 = center + 5` (always, unless signal says otherwise)

**Full bans** (never trade if center % 100 is): `10, 25, 35, 40, 65, 80`

**Combo bans** (ban if T1%100 → center%100 matches):
- T1 ends in 00 → center ends in 95
- T1 ends in 20 → center ends in 15
- T1 ends in 55 → center ends in 50
- T1 ends in 65 → center ends in 60
- T1 ends in 85 → center ends in 90

**P&L formula:**
```python
intrinsic = max(0, min(spx_close - bf_lower, bf_upper - spx_close))
intrinsic = min(intrinsic, wing_width)   # wing_width = 50
pl = round((intrinsic - premium) * 100)
```

---

## First Time Setup

### 1. Create `.env` file (copy from `.env.example`, fill in values)

```
SCHWAB_APP_KEY=<your-app-key-from-developer.schwab.com>
SCHWAB_APP_SECRET=<get from owner — not stored in repo>
SCHWAB_REDIRECT_URI=https://rava8989.github.io/brave/live.html
DISCORD_CHANNEL_ID=<right-click Discord channel → Copy Channel ID>
GITHUB_TOKEN=<GitHub PAT with repo scope>
GITHUB_REPO=rava8989/brave
```

> ⚠️ NEVER commit `.env` or `tokens.json` to git. They are in `.gitignore`.

### 2. Install dependencies

```bash
pip install -r requirements.txt
playwright install chrome
```

### 3. One-time Schwab OAuth (run once, tokens auto-refresh after)

```bash
python schwab_auth.py
```

- Browser opens to Schwab login
- Log in with the brokerage account
- You land on `https://rava8989.github.io/brave/live.html?code=XXXX`
- The page shows a **"Copy URL"** button — click it
- Paste the full URL back into the terminal prompt
- `tokens.json` is saved locally

### 4. Set up Discord Channel ID

- Open Discord in browser
- Go to the M8BF signal channel
- Right-click the channel name → **Copy Channel ID**
  (if you don't see this, enable Developer Mode: User Settings → Advanced → Developer Mode)
- Paste into `.env` as `DISCORD_CHANNEL_ID`

### 5. Get GitHub Personal Access Token

- Go to github.com → Settings → Developer settings → Personal access tokens → Tokens (classic)
- Generate new token → check `repo` scope → copy it
- Paste into `.env` as `GITHUB_TOKEN`

---

## Daily Operation

```bash
python live_updater.py
```

- Keep this running all day (or set it up as a scheduled task / startup script)
- It logs what it's doing: SPX prices, signals detected, GitHub pushes
- Chrome window will open for Discord — log in if needed, then leave it running

---

## Discord Signal Parser

The signal parser is in `discord_scraper.py` → `parse_signal()` function.

**Current expected format:**
```
m8bf ... center=5625 ... t1=5630 ... premium=8.50
```

If the Discord messages look different, edit `parse_signal()` in `discord_scraper.py`.
The function receives the raw message text and should return `(center, t1, premium)` or `None`.

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `No tokens found` | Run `python schwab_auth.py` |
| `DISCORD_CHANNEL_ID not set` | Add it to `.env` |
| `Push failed 401` | GitHub token expired — generate new one |
| `Quote error` | Schwab token expired — `schwab_client.py` auto-refreshes, check internet |
| Discord not loading | Chrome may need login — run with `headless=False` (already set) |
| Signals not detected | Edit `parse_signal()` in `discord_scraper.py` to match message format |

---

## Architecture Overview

```
Windows Machine
├── live_updater.py          ← main loop (run this)
│   ├── discord_scraper.py   ← Playwright Chrome → M8BF signals
│   ├── schwab_client.py     ← Schwab API → SPX price
│   └── github_push.py       ← GitHub REST API → push JSON files
│
GitHub (rava8989/brave)
├── today_trade.json         ← current trade state
├── spx_live.json            ← live SPX price (updated every 60s)
└── live.html                ← website polls these every 30s
```

---

## Contact / Context

- Mac session that built this: check conversation history on claude.ai
- GitHub repo: `https://github.com/rava8989/brave`
- Live page: `https://rava8989.github.io/brave/live.html`
- Historical signals: `https://rava8989.github.io/brave/history.html`

---

## ✉️ How to Leave a Message for the Mac Session

If you (Windows Claude) run into something that needs the Mac session's attention —
a blocker, a question, something that needs to be built or changed — write it here
at the bottom of this file and push it to GitHub.

The Mac session will read this file and pick up from where you left off.

**How to write back:**

1. Edit this file and add your message under `## Messages from Windows Session` below
2. Commit and push:
   ```
   git add scraper/HANDOFF.md
   git commit -m "msg: <short description>"
   git push
   ```
3. The Mac session will read it on the next conversation and respond.

---

## Messages from Windows Session

_(empty — no messages yet)_

---

## ✉️ Message from Mac Session → Windows Session

**Date: 2026-03-27**

**BUG: Scraper triggered outside the trading window.**

Today (Friday 2026-03-27) the scraper fired at **09:57 ET** and pushed `today_trade.json` with `triggered: true`. But the Friday window is **13:00–13:30 ET**. That signal should have been ignored.

**Fix needed in `discord_scraper.py` / `live_updater.py`:**

The window check must validate that the signal time falls **inside** the day's window before setting `triggered: true`. Here is the correct window logic:

```python
WINDOWS = {
    0: ('11:00', '11:30'),  # Monday
    1: ('13:30', '14:00'),  # Tuesday
    2: ('12:00', '12:30'),  # Wednesday
    3: ('11:00', '11:30'),  # Thursday
    4: ('13:00', '13:30'),  # Friday
}

from datetime import datetime
import pytz

def in_window(signal_dt_et):
    """Return True only if signal_dt_et falls inside today's M8BF window."""
    dow = signal_dt_et.weekday()  # 0=Mon, 4=Fri
    if dow not in WINDOWS:
        return False  # Weekend
    start_str, end_str = WINDOWS[dow]
    start_h, start_m = map(int, start_str.split(':'))
    end_h, end_m = map(int, end_str.split(':'))
    t = signal_dt_et.hour * 60 + signal_dt_et.minute
    return (start_h * 60 + start_m) <= t <= (end_h * 60 + end_m)
```

Only call `trigger_trade(signal)` if `in_window(signal_time_et)` returns `True`.

**I already reset `today_trade.json` back to `triggered: false`.** The real Friday window opens at 13:00 ET — if a valid signal comes in then, trigger it correctly.

— Mac Session

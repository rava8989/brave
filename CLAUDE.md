# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SPY 0DTE options flow backtester. Downloads tick-level SPY options trade data from ThetaData, classifies each trade as aggressively bought/sold, then uses multiple strategy modes to simulate /MES (Micro E-mini S&P 500) futures trades.

## Commands

```bash
# Install dependencies
pip3 install -r requirements.txt

# Download data (requires ThetaData terminal running on localhost:25503)
python3 downloader.py [start_date] [end_date]   # default: 20240101 20241231

# Download SPX index data
python3 -c "import asyncio; from downloader import download_spx_range; asyncio.run(download_spx_range('20230101','20260317'))"

# Download QQQ options data
python3 download_qqq.py [start_date] [end_date]

# Run web UI (backtester + data management)
uvicorn server:app --reload --port 8001

# Start ThetaData terminal
java -jar ~/Downloads/ThetaTerminalv3.jar --creds-file ~/creds.txt
```

## Architecture

**Data pipeline:** `ThetaData API → downloader.py → ./data/ → engine.py`

- **downloader.py** — Async HTTP client fetching from local ThetaData service (`localhost:25503`). Downloads SPY 0DTE options, SPX 1-min bars, and index data (VIX, RUT). Classifies aggressor as bought only when trade price >= ask (no tolerance). ATM filter ±3%. Marks worthless contracts (bid AND ask < $0.05) as "unknown".

- **engine.py** — Multi-strategy backtester with 9 strategy modes: consecutive prints, net flow (premium-weighted), sweep detection, premium-weighted flow, opening drive, ATM-only flow, fade P/C ratio, large block + cooldown, sweep scalp. Uses SPX 1-min data for entry/exit prices (falls back to put-call parity from options data). PnL: $5/point/contract. Broker fee presets for Tastytrade ($1.37/side) and Schwab ($2.57/side).

- **server.py** — FastAPI app with embedded HTML/JS UI. Strategy dropdown with per-strategy parameters. Progress bar, cancel button, CSV export. VIX filtering (level + overnight movement). Multiple time windows. Account allocation with compounding.

## Key Signal Logic

Only aggressive buys (traded at ask or above) are counted. Call bought = bullish, Put bought = bearish. "Sold" prints are ignored entirely. The strategy mode determines how these signals generate trades.

## STRATEGY INDEPENDENCE — DO NOT VIOLATE

M8BF, Straddle, GXBF, and BOBF are COMPLETELY INDEPENDENT strategies. This has been established and violated multiple times. Never again.

**Hard rules:**
- GXBF firing does NOT block or cancel M8BF. Ever.
- Straddle firing does NOT block or cancel M8BF. Ever.
- If Straddle is blocked (o2o too wide, SPX gap, OPEX), it stays blocked — it does NOT fall back to M8BF.
- If GXBF is blocked, it does NOT affect M8BF status. Ever.
- M8BF card always shows M8BF's OWN status (day-of-week window + its own banned conditions).
- Straddle card always shows Straddle's OWN status (overnight VIX drop + o2o + gap conditions).
- GXBF card always shows GXBF's OWN status (large overnight VIX drop conditions).

**Forbidden cross-strategy logic:**
- `if (!m8bfBanned) { → M8BF fallback }` inside a Straddle block
- Any m8bfText, stradText, gxbfText that references another strategy's name as a blocker
- Any EOD/backfill code that treats `sig.theme !== 'm8bf'` as an M8BF-blocked signal
- Any logic that converts one strategy to another when blocked

Check M8BF blocked status via `sig.m8bfBanned || sig.cpiDay` — never via `sig.theme`.
The pre-commit hook (`scripts/check-strategy-independence.sh`) enforces these rules automatically.

## Data Layout

```
./data/
├── SPY_0DTE_YYYYMMDD.csv    # SPY options flow (2023-2026)
├── spx/SPX_YYYYMMDD.csv     # SPX 1-min OHLC bars
├── vix/VIX_YYYYMMDD.csv     # VIX 1-min OHLC bars
├── rut/RUT_YYYYMMDD.csv     # Russell 2000 1-min bars
└── qqq/QQQ_0DTE_YYYYMMDD.csv # QQQ options flow
```

The downloader skips dates that already have a CSV file — delete existing files to force re-download.

## Workflow Orchestration

### 1. Plan Node Default
- Enter plan mode for ANY non-trivial task (3+ steps or architectural decisions)
- If something goes sideways, STOP and re-plan immediately – don't keep pushing
- Use plan mode for verification steps, not just building
- Write detailed specs upfront to reduce ambiguity

### 2. Subagent Strategy
- Use subagents liberally to keep main context window clean
- Offload research, exploration, and parallel analysis to subagents
- For complex problems, throw more compute at it via subagents
- One task per subagent for focused execution

### 3. Self-Improvement Loop
- After ANY correction from the user: update `tasks/lessons.md` with the pattern
- Write rules for yourself that prevent the same mistake
- Ruthlessly iterate on these lessons until mistake rate drops
- Review lessons at session start for relevant project

### 4. Verification Before Done
- Never mark a task complete without proving it works
- Diff behavior between main and your changes when relevant
- Ask yourself: "Would a staff engineer approve this?"
- Run tests, check logs, demonstrate correctness

### 5. Demand Elegance (Balanced)
- For non-trivial changes: pause and ask "is there a more elegant way?"
- If a fix feels hacky: "Knowing everything I know now, implement the elegant solution"
- Skip this for simple, obvious fixes – don't over-engineer
- Challenge your own work before presenting it

### 6. Autonomous Bug Fixing
- When given a bug report: just fix it. Don't ask for hand-holding
- Point at logs, errors, failing tests – then resolve them
- Zero context switching required from the user
- Go fix failing CI tests without being told how

## Task Management

1. **Plan First**: Write plan to `tasks/todo.md` with checkable items
2. **Verify Plan**: Check in before starting implementation
3. **Track Progress**: Mark items complete as you go
4. **Explain Changes**: High-level summary at each step
5. **Document Results**: Add review section to `tasks/todo.md`
6. **Capture Lessons**: Update `tasks/lessons.md` after corrections

## Core Principles

- **Simplicity First**: Make every change as simple as possible. Impact minimal code.
- **No Laziness**: Find root causes. No temporary fixes. Senior developer standards.
- **Minimal Impact**: Changes should only touch what's necessary. Avoid introducing bugs.

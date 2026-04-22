# Fix longDte=12 BS-fallback (DTE gap in Polygon scraper)

## Root cause

Diagonal backtester's BS fallback rate was ~58% for `longDte=12` while other
DTEs ran at 99.8% real coverage. The Polygon scraper
(`fetch_polygon_spx.py`) split fetches into SHORT `[0, SHORT_DTE_MAX=7]` and
LONG `[LONG_DTE_MIN=12, 35]`. Continuous coverage was supposed to be
`[0,7] ∪ [12,35]`, which left a silent gap at DTE `[8, 11]`.

A diagonal entered with `longDte=12` lands in the gap on *exit day*: the long
leg has 12 DTE at entry → 11 DTE on next-day exit. That exit ticker is
missing from the snapshot → forced Black-Scholes fallback.

## Fix (Option B — targeted backfill, no full re-fetch)

- [x] Lower `LONG_DTE_MIN` from 12 → 8 in `fetch_polygon_spx.py` so the
      long-side listing now covers `[8,35]` going forward
- [x] Write `backfill_polygon_dte_gap.py` — walks every existing
      `data/polygon/SPX_*.json`, fetches SPXW put contracts for DTE `[8,11]`
      once per date, fetches NBBO per ticker at each time bucket, ADDITIVELY
      merges into `quotes` (never overwrites existing tickers)
- [x] Apply screenshot defaults to `diagonal.html`: entry=11:30, exit=15:45,
      sDTE=1, short ITM=30, lDTE=30, long below=50, tol=5, prefer-longer,
      1 contract, EXCLUDE mode with OPEX-1 + VIX 50-80% + ALL EARNINGS
- [x] Rewrite strategy overview on `diagonal.html`, `index.html`, and
      `history.html` to reflect new 11:30/15:45 defaults and new filter stack
- [x] Remove stale "2:00 PM ET" / "2/30" / "14:00 ET" references across all
      three HTML files (final check ran `Grep` → "No matches found")
- [x] Update empty-state text on `diagonal.html` (11:30/15:45 + 0-35 DTE
      range + ±300 pt strike window)
- [x] Start targeted backfill with `caffeinate -dims` guarding the Mac from
      sleep (PID 18649)
- [x] Wait for backfill to complete (~100 min ETA from 2026-04-21 ~10:10 PM ET) —
      completed in 5410.9s (~90 min), +4,187,164 quotes across 816 dates
- [x] Rebuild gzipped bundles via `python3 build_diagonal_real_data.py` —
      7 bundles, 35.1M total quotes, sizes grew ~11% (e.g. 44.4 MB → 44.6 MB,
      etc.) reflecting the DTE 8-11 additions
- [x] Run `python3 /tmp/verify_12dte.py` to confirm `longDte=12` now has
      near-0 BS fallback — **PASSED**: 812/812 (100.0%) REAL, 0 BS
- [x] Regenerate `history_data.json` diagPL with new defaults (via
      `compute_diagonal_pnl.py`) — initially 549 trades / +$157,677 (WRONG —
      see VIX_MID bug below); after fix **541 trades, +$181,427, WR 63.4%,
      MaxDD $17,238, MAR 4.42, 100% real NBBO quotes**.
- [x] Update diagonal.html banner with fresh backtest numbers
- [x] Stop caffeinate now that the long task is done (killed PID 18649)
- [x] **Fix VIX_MID percentile data-source drift in Python port**: JS's
      `buildSpecialDateSets` is called with the BS data file's `vix_14`, but
      the port was using the bundle's `by_time['14:00'].vix`. Those two
      snapshots come from different Schwab samples and drift by 0.01–0.25
      on 468/816 days — enough to flip days in/out of the 50–80% percentile
      band. Fixed by loading `diagonal_bs_data.json` and using its `vix_14`
      directly in `run_backtest(..., bs_data=bs)`. Result now matches live
      JS to the dollar (259/$101,495 on 2024‑09‑03→2026‑04‑15).

## Review

- **Root-cause fix landed in `fetch_polygon_spx.py`**: `LONG_DTE_MIN` 12 → 8,
  giving continuous coverage `[0,35]`. Future scrapes will never hit the gap.
- **Historical data healed via backfill**: 4.19M new quotes merged additively
  into 816 snapshot files — no existing quote was overwritten.
- **End-to-end sanity check**: every longDte from 7 to 35 now at 100% REAL
  (except longDte=25 with 1 BS out of 812, ≈0.1% — noise, not the bug).
- **Secondary win**: regenerated `diagPL` values in `history_data.json` using
  the new 11:30/15:45 + OPEX-1 + VIX 50-80% + ALL EARNINGS default stack
  while leaving protected fields (`m8bfPL`, `gxbfPL`, `bobfPL`, `stradPL`)
  completely untouched. Three backups saved.
- **Third bug found — VIX_MID data-source drift**: after the initial merge
  wrote 549/$157,677, the user diffed against the live JS backtester and
  found $101K vs $77K on the 2024‑09‑03→2026‑04‑15 slice. Traced the gap to
  the Python port computing the VIX_MID 50–80% percentile off the bundle's
  `by_time['14:00'].vix` while the JS uses the BS data file's `vix_14`. The
  two snapshots drift by 0.01–0.25 on 468/816 days — enough to flip classif-
  ications at the percentile boundary. Fix: load `diagonal_bs_data.json` and
  pass it as `bs_data=` to `run_backtest`. Port output now identical to live
  JS to the dollar. Full backtest: **541 trades, +$181,427, WR 63.4%,
  MaxDD $17,238, MAR 4.42, 100% REAL NBBO.**
- **UI / overview sync**: `diagonal.html`, `index.html`, `history.html` all
  reflect the new 1/30 + 11:30/15:45 + three-filter stack. No stale
  `2/30` or `14:00 ET` or `2:00 PM ET` references anywhere.

---

# Fix Schwab Disconnect (refresh_token race)

## Root cause

Schwab rotates the `refresh_token` on every successful refresh and immediately
invalidates the old one. Three independent clients each hold a copy and each
can rotate it:

1. **Cloudflare Worker** (`schwab-proxy.js`) — `getAccessToken()` reads/writes
   KV `schwab_tokens`; mutex protects within one invocation but not across
   isolates or scheduled/fetch handlers.
2. **Browser** (`index.html`) — `schwabRefreshToken()` POSTs to Worker
   `/token` with its sessionStorage refresh_token, then pushes the new tokens
   back to KV via `/sync`.
3. **Python scraper** (`scraper/schwab_client.py`) — refreshes against Schwab
   *directly* (not via Worker) and writes to local `tokens.json`.

Whichever client rotates first invalidates the other two. The losers get
401s forever and the user has to re-auth.

## Fix: single source of truth = Worker KV

The Worker already owns KV. Make it the only party that talks to Schwab's
OAuth endpoint for refresh. Everyone else asks the Worker for a current
access token.

- [x] **Worker**: add `GET /access-token` — auth via `X-Sync-Secret`, returns
      `{ access_token, expires_at }`; delegates to `getAccessToken(env)` so
      the mutex + KV write path handles refresh
- [x] **Worker**: harden `getAccessToken` — retry-on-stale: if Schwab 400s on
      refresh (another isolate beat us), re-read KV once and use the winner's
      token; handles cross-invocation races
- [x] **Browser** `index.html`: replace `schwabRefreshToken()` body — call
      `GET /access-token` and update only `access` + `expiry` in sessionStorage;
      never rotate the refresh token from the browser again
- [x] **Python** `scraper/schwab_client.py`: route `get_access_token()` through
      Worker `GET /access-token` when `SCHWAB_PROXY_URL` + `SCHWAB_SYNC_SECRET`
      are set in env; fall back to direct-refresh only when proxy unavailable
- [x] Deploy Worker (`npx wrangler deploy`)
- [x] Verify `schwab_tokens` still populated in KV after deploy
- [x] Verify new `/access-token` endpoint returns 200 with a valid token
- [x] Commit + push

## Review

_Filled in post-verify._

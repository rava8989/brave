# Lessons Learned

## Shard data along the axis the consumer actually queries (2026-04-22)

Diagonal backtester bundles were laid out per-half-year with ALL 20 intraday
timestamps in each file. Result: every page load downloaded 329 MB gz /
3.1 GB decompressed, even though the backtester only reads 2 of the 20
timestamps (entry_time + exit_time). 90% of the payload was wasted on every
single run. Dataset was sharded along the wrong axis — by time period (which
the consumer does want all of) instead of by timestamp (which the consumer
only needs 2 of).

**Fix:** shard by BOTH axes. Per-(half-year, time) gives 7 × 20 = 140 small
files, ~2.4 MB gz each. Initial load fetches 7 halves × 2 times = 14 files
≈ 32 MB. Time-selector changes lazy-load 7 more files each. 10× reduction in
initial load size. Python port sees ~70× speedup on the default-times path
(3.6 s vs 256 s).

**Pattern to catch:** when a multi-axis dataset is bundled along only one
axis and the consumer filters heavily on another, bundle along both. The
guiding question isn't "how big should each file be" but "which slice is the
smallest unit the consumer ever reads?" — that's the natural shard granularity.
Lazy-loading additional slices on demand costs nothing if it's rare.

**Co-benefit:** smaller per-file decompressed size (~20 MB) sits far under
V8's 512 MB string limit, eliminating another class of silent-parse-failure
risk.

---

## When porting a filter that uses a percentile boundary, pipe through the EXACT same source data, not "similar enough" data (2026-04-22)

The JS diagonal backtester computes VIX_MID (50–80% percentile vs prior 20 days)
using `diagonal_bs_data.json` → `by_date[d].vix_14`. The Python port used the
gzipped bundle's `by_time['14:00'].vix` instead — same nominal concept ("14:00
VIX"), but the two files are built from different Schwab samples and drift by
0.01–0.25 on 468 / 816 days. Most days the drift doesn't matter, but the 50–80%
band has sharp boundaries: a 0.05 VIX shift at the right position flips a day
in/out of the band.

**Symptom:** full backtest on identical config produced **the same trade count
by coincidence** (259 / 259) but **$24K different total P/L** ($77,355 port vs
$101,495 JS). Extremes matched exactly (max winner $8,920, max loser -$5,953),
average per-trade differed by ~$93. Looked like "small systematic pricing bias";
was actually "same count, but 7 different trades in each list."

**Why the ports disagreed:** `VIX_MID` membership differed by a handful of days.
Excluding a different date set changes WHICH trades get into the backtest, not
how many. If the two shifted sets happen to have similar cardinalities, trade
counts stay identical while the trade LIST drifts.

**Fix:** port must read the same file the reference implementation reads for
any boundary-sensitive classification. Loaded `diagonal_bs_data.json` and
passed `bs_data=bs` into `run_backtest`. All stats now match JS to the dollar.

**Pattern to catch:** when a port is "close but off by a small systematic
amount" and the extreme values match exactly, suspect set-membership drift in a
filter, not pricing math. Diff the filter SETS first, not individual trade
prices.

---

## Range-split fetches need contiguous ranges, or the gap silently breaks users (2026-04-21)

The Polygon scraper split fetches into SHORT `[0, SHORT_DTE_MAX=7]` and LONG
`[LONG_DTE_MIN=12, 35]`, leaving a silent gap at DTE `[8, 11]`. The intent was
continuous coverage `[0,35]`, but anyone configuring `longDte=12` landed in
the gap on *exit day* (12 DTE entry → 11 DTE exit) — exit ticker missing,
forced Black-Scholes. Symptom: backtester reported 58% BS fallback for that
one specific config while every other config ran at 99.8% real.

**Fix (two-part):**
1. Close the gap in the scraper: `LONG_DTE_MIN` 12 → 8, giving
   `[0,7] ∪ [8,35] = [0,35]`.
2. Backfill existing snapshot files with a targeted scraper
   (`backfill_polygon_dte_gap.py`) that adds only the missing DTE 8-11
   tickers, additive merge, idempotent, per-date one-shot contract listing.

**Pattern:** any time a fetch is parameter-sliced for performance, verify the
slices tile completely. Silent gaps masquerade as "this one config is bad."
Always diagnose by asking which *parameter value* triggers the bad behavior
— if it's a narrow band, suspect a gap in the upstream data, not the
downstream code.

---

## Expired legs need intrinsic settlement, not Black-Scholes (2026-04-21)

Diagonal backtester had 11/815 trades (1.3%) falling back to BS on exit because
the short leg ticker was missing from next-day quotes. The cause was NOT a data
gap — the short had **already expired and settled** before the exit time. A
1DTE short entered Friday expires Monday; when closeDate rolls to Tuesday (or
Wednesday over a holiday), the Monday-expired ticker is gone. Polygon correctly
stops listing expired contracts.

**Fix:** for an expired leg, use `max(strike - SPX_close_on_exp_date, 0)` as
the exit value. This is mathematically EXACT (SPXW is PM-settled at SPX close),
not an approximation — strictly better than BS.

**Data gap:** half-day sessions (day before July 4, day after Thanksgiving,
Christmas Eve — 8 dates in 2023-2025) are missing from the per-time snapshot
dataset because quotes were only scraped at bucket times 09:45–15:45 and the
half-day market closes at 13:00. Added `data/spx_halfday_close.json` as a
lightweight supplement to cover intrinsic settlement on those dates.

**Pattern to watch:** whenever option pricing uses BS as a fallback for
"missing exit quote," first check: **did the leg expire?** If so, intrinsic
settlement is exact, not a fallback. BS should only cover the alive-but-bad-
quote case.

---

## UI time selectors must propagate to every pricing/display call site (2026-04-21)

The diagonal backtester had an `entryTime` / `exitTime` selector (09:45…15:45).
The flattening layer (`flattenRealData`) honored it for *quotes*, but the pricing
loop hardcoded `entry.spot_14` / `entry.vix_14` for spot + VIX regardless of the
user's selection. Result: picking 09:45 priced legs and displayed "SPX ENTRY"
using 14:00 spot — strikes came out 50+ pts off from what the selected time
actually was.

**Fix:** prefer per-time values carried in REAL_DATA (`.spot`, `.vix`,
`.spot_exit`, `.vix_exit` — set by `flattenRealData` from the selected times),
fall back to BS 14:00 only when real data is missing. The trade record's
`vixEntry`/`vixExit` must use the same time-selected values, not the BS 14:00
fields, or the trade log shows the wrong VIX too.

**Pattern to catch:** when you add a time-selector UI, grep every site that
reads `.spot_<HH>` / `.vix_<HH>` (or any hardcoded-time field). Those are all
load-bearing — each one needs to route through the selected time or the UI
lies.

---

## Rotating refresh_tokens need a single source of truth (2026-04-21)

Schwab rotates `refresh_token` on every successful refresh and instantly
invalidates the previous one. Any OAuth design where more than one client
holds a copy of `refresh_token` and can initiate a refresh will race:
whoever rotates first kicks everyone else into 401.

The system had three rotating clients — Cloudflare Worker cron, browser tab,
Windows Python scraper. Disconnects were a daily occurrence.

**Fix:** exactly one party holds `refresh_token` (the Worker, in KV). All
other clients request an `access_token` from that party. Bonus: retry-on-
stale inside the owner itself for cross-isolate races — if Schwab 400s on
refresh because another isolate already rotated, re-read KV and use the
winner instead of erroring.

**Watch for this pattern elsewhere:** any time the same secret rotates on
server side and multiple clients cache it, assume race-to-invalidate.
Centralize.

---

## Decompressed JSON > 512 MB silently fails `JSON.parse` in V8 (2026-04-21)

V8 caps string length at ~512 MB (UTF-16 chars). When a gzipped dataset
decompresses into a single JS string larger than this and the fetch is
wrapped in `Promise.all(...).catch(() => null)`, the failure vanishes — no
console error, just `null` for that chunk.

**Symptoms:** dataset loads "work" but coverage is mysteriously lower than
expected. Only files under ~500 MB decompressed actually materialize.

**Fix options (cheapest first):**
- Split the gz file into smaller chunks so each decompressed chunk fits.
- Stream-parse JSON with a streaming parser (don't materialize as a single
  string).
- Switch the transport to newline-delimited JSON (one object per line) and
  parse per-line.

**Always log chunk failures explicitly** rather than swallowing in `.catch()`
— a `Promise.all` with silent failures is a trap.

---

## CRITICAL: Strategies are completely independent — never connect them

**Violated multiple times. Do not repeat.**

M8BF, Straddle, GXBF, BOBF compute their own status independently. No cross-references. No fallbacks. No cross-strategy precedence language.

Forbidden patterns:
- Converting blocked Straddle into M8BF (the o2o/gap → M8BF fallback)
- Any `m8bfText`/`stradText`/`gxbfText` that names another strategy as the blocker
- Any `if (!m8bfBanned) { fallback to M8BF }` inside a Straddle or GXBF block
- **Using `sig.theme !== 'm8bf'` as an M8BF-blocked check in EOD/backfill code.** `theme` is the PRIMARY recommendation (only one strategy per day). A non-`m8bf` theme just means another strategy was primary — it does NOT mean M8BF is blocked. Always use `sig.m8bfBanned || sig.cpiDay` to determine if M8BF should skip. (Caught 2026-04-16 — had been silently zeroing m8bfPL on every GXBF/Straddle day.)

The pre-commit hook `scripts/check-strategy-independence.sh` fails the commit if any of these patterns reappear.

When fixing a blocked-strategy message: show that strategy's own blocked reason. Full stop. Do not introduce the other strategy.

**In backend code (EOD, backfill, etc.):** to check whether a strategy ran, use its own ban conditions, not `theme`:
- M8BF ran iff `!sig.m8bfBanned && !sig.cpiDay` and a qualifying signal is in window
- `theme` is for the dashboard primary rec only, not per-strategy status

---

## Never modify gxbfPL, bobfPL, or stradPL without explicit user order

These fields are manually entered from real trade results. Do not touch them.

---

## Always run terminal commands directly — never ask the user to run them

---

## After deploying schwab-proxy.js, always verify KV has live data

Check tokens, creds, discord_config are present in KV after every deploy.

---

## When backtesting, only present results — never suggest removing or modifying strategy rules

---

## Pull before push — always git pull --rebase before git push

---

## Never remove the ~10 future empty date placeholders from history_data.json

---

## calculateSignal lives in TWO places — always update both

`schwab-proxy.js` and `index.html` both contain a full copy of calculateSignal.
They must stay identical. When fixing signal logic in one, fix it in the other
immediately in the same commit. history.html has a third copy for the backtester.

Any signal logic change = touch all 3: schwab-proxy.js + index.html + history.html

---

## When copying a styled pattern to a new sibling, sweep ALL per-item selectors

When adding a new item to a repeating pattern (pill, card, chip, tab, etc.), a
single `[data-key="foo"]` / `[data-strat="foo"]` / `#foo-bar` CSS rule often
controls the visual identity (bg + border + color). Grep for ALL per-item
selectors in the stylesheet, not just the most obvious one.

Example miss: added `<div class="strat-pill" data-strat="diagonal">` + JS +
equity-chart color, but forgot `.strat-pill[data-strat="diagonal"]{...}` in the
CSS block. Result: pill rendered as a plain transparent element while every
other sibling was a colored pill.

Rule: after adding a new instance of a styled sibling, grep the CSS for the
sibling's base class and verify every `[data-*]` / `:nth-child` / variant rule
has a matching entry for the new key.

---

## Signal-card text is descriptive, not imperative

Never write card text as an instruction: "Do Diagonal", "Take the trade",
"Buy X". That reads like financial advice. This is an information UI, not an
advisor. Mirror how existing cards phrase the GO state — they are names of
what is active today, not commands: `M8BF`, `Straddle`, `GXBF`, `BOBF`,
`Diagonal @ 2:00 PM ET`, `EOM Straddle — 9:32 AM`. Badges carry the timing
verb-free (`⏰ 14:00 ET`, `📅 NM — 9:32 AM`). Blocked states say `No X (reason)`
— also descriptive, not imperative ("Skip X" would be imperative too).

When adding a new card, check the new text side-by-side against existing cards
before committing. If your text starts with a verb ("Do", "Take", "Enter",
"Buy", "Sell"), rewrite it.

---

## Stale plan files are NOT active work — check git/file state first

When the system surfaces a plan from `~/.claude/plans/*.md` with "continue if
relevant," that plan may be from any prior session, including ones already
completed, abandoned, or never approved by the current user's intent.

Before executing ANY surfaced plan:
1. Grep the repo for the exact changes the plan describes — if the code
   already matches the plan's "after" state, the plan is DONE. Delete it
   and do NOT start editing.
2. Check the user's most recent real message. If the plan topic is
   unrelated to what the user was actually discussing, it's stale context.
   Acknowledge and drop it — don't silently pivot.
3. Never re-execute work that is already present. Re-editing history_data.json,
   re-removing rules, or re-deploying worker code when the target state is
   already live creates churn, noise, and user frustration.

(Caught 2026-04-20 — jumped on a stale "Remove M8BF Day After Earnings Block"
plan from days earlier. All target changes were already committed; user hadn't
mentioned the topic in over a week. Started reading files to "execute" work
that was long done.)

---

## Never deploy Worker code when user-facing scheduled handlers are the same day (2026-04-23)

Deployed a signal-engine.js refactor at 2026-04-22 23:49 UTC, then at
2026-04-23 09:30 ET the morning Discord signal did not fire. User reported
two failures: no morning Discord message, GEX page broken.

Root cause turned out to be Schwab refresh-token rejection (token last
refreshed 2026-04-22 19:33 ET; every refresh attempt since has thrown HTTP
non-200). Evidence:
- `schwab_tokens.expiry` stuck 24h stale in KV (would update every ~28 min
  if refresh were working)
- `morning_signal_2026-04-23` stuck at `claim:<uuid>` — claim taken (line
  652) but `getAccessToken` at line 663 threw before the "sent" write
- GEX endpoint still returns a 200 because it serves stale `gex_current`
  from KV when refresh fails — masks the real problem
- EOD `last_run` shows `vixClose:null, spxClose:null, "Invalid close from
  Stooq"` — Schwab path dead, fallback path also dead

My refactor was NOT the cause (Schwab rejection started before the deploy),
but deploying to a live schwab-bound Worker on the same day a cron is due
meant the rollback had to happen under time pressure and user frustration.

**Rules:**
1. Worker deploys that touch the bundle `schwab-proxy.js` imports — including
   `signal-engine.js` — must be smoke-tested against the actual Worker
   runtime (not just Node) before a scheduled cron is due. Use
   `wrangler dev --remote` or a manual `/gex` ping after deploy.
2. Never deploy on a Wednesday evening or Thursday morning — the Thursday
   09:30 ET morning cron is the highest-stakes scheduled handler and the
   one the user notices within seconds if it fails.
3. When a user reports a scheduled-handler failure, the diagnostic order is:
   a. Check `schwab_tokens.expiry` — if >1h old, Schwab refresh is dead
   b. Check `morning_signal_<date>` / `gex_last_refresh_ts` for stuck claim
      or stale timestamps
   c. Only THEN suspect the recent code deploy
4. Rollback first, diagnose second. `wrangler rollback <version-id>` is a
   30-second operation that restores known-good behaviour and buys time to
   investigate without the user losing more data points.
5. If the root cause is dead Schwab tokens, the fix is user-side: open the
   web UI → click "Re-authenticate" button → Schwab OAuth → tokens sync to
   KV via POST /sync. Code rollback alone does NOT restore service.

---

## `deadlineMs` relative to 9:30 ET makes the fallback structurally broken (2026-04-24)

2026-04-24 09:30 ET morning Discord signal didn't fire. User reported after
the fact. Symptoms:
- `morning_signal_2026-04-24` stuck at `claim:<uuid>` — claim taken, never
  flipped to `sent`
- `morning_signal_fail_2026-04-24` had a timestamp — fallback fired but also
  failed
- `schwab_refresh_state` was `{ok:true}` — Schwab was healthy
- `last_run` was from yesterday's EOD — no today write yet

Root cause in schwab-proxy.js (pre-fix lines 795-801): the VIX post-open
polling deadline was computed as **9:30 ET absolute + 120s = 9:32 ET
absolute**, regardless of when the function actually runs. So the 9:30 cron
had the intended 2-min grace, but any fallback invocation at 9:32+ ET saw
`Date.now() > deadlineMs` immediately → `while (Date.now() < deadlineMs)`
body never executed → `attempt` stayed 0 → threw "VIX post-open tick not
yet available after 0 attempts over ~2 minutes".

Every subsequent /gex hit re-triggered the fallback, re-threw the same
error, wrote `morning_signal_fail_<date>`, got gated by the 60s cooldown,
waited 60s, retried, same thing. Permanently wedged.

Fix: `const deadlineMs = Date.now() + maxWaitMs;` — relative to when the
handler actually runs. Works for both the 9:30 cron (same behaviour as
before) and any post-9:32 recovery attempt.

**Rules:**
1. Any polling loop with a deadline should be relative to `Date.now()`, not
   absolute to a wall-clock time, unless there's a specific reason otherwise.
   Wall-clock deadlines silently break recovery paths.
2. When diagnosing a wedged morning signal, check
   `morning_signal_fail_<date>` in addition to `morning_signal_<date>`. A
   timestamp in the fail key means the fallback IS running and IS throwing
   — tail the Worker logs for the actual exception.
3. `wrangler tail schwab-proxy --format=pretty` is the fastest way to see
   the real error. Don't guess from KV state — logs show the actual throw.
4. "Discord post failed" vs "VIX not available" vs "Schwab token rejected"
   look identical from outside (claim stuck, fallback retrying). Always
   tail the logs to distinguish.

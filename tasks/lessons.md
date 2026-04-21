# Lessons Learned

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

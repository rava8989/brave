# Lessons Learned

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

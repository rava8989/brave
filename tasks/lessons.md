# Lessons Learned

## CRITICAL: Strategies are completely independent — never connect them

**Violated multiple times. Do not repeat.**

M8BF, Straddle, GXBF, BOBF compute their own status independently. No cross-references. No fallbacks. No "takes priority" language.

Specifically banned patterns:
- Converting blocked Straddle into M8BF (the o2o/gap → M8BF fallback)
- "GXBF takes priority" over M8BF — GXBF firing never affects M8BF
- "Straddle takes priority" over M8BF — Straddle firing never affects M8BF
- Any `if (!m8bfBanned) { fallback to M8BF }` inside a Straddle or GXBF block

When fixing a blocked-strategy message: show that strategy's own blocked reason. Full stop. Do not introduce the other strategy.

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

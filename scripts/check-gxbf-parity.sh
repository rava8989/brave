#!/bin/bash
# GXBF Python↔JS parity gate.
#
# Runs scripts/test-gxbf-parity.js (headless Chrome drives gxbf-backtester.html
# with its default config, Python runs rebuild_gxbfPL.py --dry-run-json — never
# touching history_data.json — and every per-date P/L must match within ±$1,
# with fired-vs-gated agreement on every common date) whenever the files that
# define GXBF logic are staged. ~10s when triggered, skips silently otherwise.
#
# Added 2026-06-10 after the GXBF audit found the two implementations had
# silently diverged: hand-typed FED dates that weren't FOMC days, a stale
# "OPEX−2" VIX-expiry approximation, and missing zero-bar guards that
# fabricated phantom max-loss trades in the rebuilt history.

set -e
cd "$(dirname "$0")/.."

STAGED=$(git diff --cached --name-only --diff-filter=ACM 2>/dev/null || true)

if ! echo "$STAGED" | grep -qE '^(gxbf-backtester\.html|rebuild_gxbfPL\.py|gxbf_bt_data\.json|signal-engine\.js|scripts/test-gxbf-parity\.js)$'; then
  exit 0  # no GXBF-logic files staged — skip silently
fi

if [ ! -d node_modules/puppeteer-core ]; then
  echo "✗ [gxbf-parity] puppeteer-core not installed (npm install --save-dev puppeteer-core)"
  exit 1
fi

node scripts/test-gxbf-parity.js

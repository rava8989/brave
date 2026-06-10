#!/bin/bash
# Diagonal Python↔JS parity gate.
#
# Runs scripts/test-diagonal-parity.js (headless Chrome drives diagonal.html,
# Python runs compute_diagonal_pnl.run_backtest with identical params, every
# per-date P/L must match within ±$1) whenever the files that define Diagonal
# logic are staged. ~7s when triggered, skips silently otherwise.
#
# Added 2026-06-09 after the parity harness found 31 dates of drift between
# the two implementations (P5-class vix_open-vs-vix_close priors, warm-up
# gate, EOM half-day labeling, banker's-vs-half-up strike rounding).

set -e
cd "$(dirname "$0")/.."

STAGED=$(git diff --cached --name-only --diff-filter=ACM 2>/dev/null || true)

if ! echo "$STAGED" | grep -qE '^(diagonal\.html|compute_diagonal_pnl\.py|signal-engine\.js|scripts/test-diagonal-parity\.js)$'; then
  exit 0  # no Diagonal-logic files staged — skip silently
fi

if [ ! -d node_modules/puppeteer-core ]; then
  echo "✗ [diagonal-parity] puppeteer-core not installed (npm install --save-dev puppeteer-core)"
  exit 1
fi

node scripts/test-diagonal-parity.js

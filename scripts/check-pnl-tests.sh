#!/bin/bash
# P/L math unit tests — run when the money-math files are staged.
#
# test_pnl.js  — parseDiscordSignal normalization, butterfly intrinsic,
#                isBanned, diagonal max-loss constants (extracted from
#                schwab-proxy.js source text — fails loudly on renames).
# test_pnl.py  — round_mid_up / round_pnl_down / put_delta /
#                pick_put_by_delta from backtest_cor1m_regime.py.
#
# Added 2026-06-09 after the PUT-fly ordering bug: formulas that compute
# real P/L had zero tests. Now any edit to these files must keep them green.

set -e
cd "$(dirname "$0")/.."

STAGED=$(git diff --cached --name-only --diff-filter=ACM 2>/dev/null || true)

RUN_JS=0
RUN_PY=0
echo "$STAGED" | grep -qE '^(schwab-proxy\.js|signal-engine\.js|test_pnl\.js)$' && RUN_JS=1
echo "$STAGED" | grep -qE '^(backtest_cor1m_regime\.py|test_pnl\.py)$' && RUN_PY=1

if [ "$RUN_JS" = "0" ] && [ "$RUN_PY" = "0" ]; then
  exit 0  # nothing money-math related staged — skip silently
fi

if [ "$RUN_JS" = "1" ]; then
  node test_pnl.js || { echo "✗ [pnl-tests] test_pnl.js failed"; exit 1; }
fi
if [ "$RUN_PY" = "1" ]; then
  python3 test_pnl.py || { echo "✗ [pnl-tests] test_pnl.py failed"; exit 1; }
fi
echo "✓ [pnl-tests] money-math tests green"

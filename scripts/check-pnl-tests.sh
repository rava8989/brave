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

# signal-engine.js staged → also run the full signal test suites. These
# suites rotted unnoticed for months (test_vix_pct still asserted the
# pre-2026-06-02 band; test_fail_safe asserted a pre-independence field)
# because nothing executed them. Now every engine change runs them.
if echo "$STAGED" | grep -qE '^(signal-engine\.js|test_signal\.js|test_vix_pct\.mjs|test_fail_safe\.mjs)$'; then
  node test_signal.js > /dev/null || { echo "✗ [pnl-tests] test_signal.js failed"; node test_signal.js | tail -5; exit 1; }
  node --test test_vix_pct.mjs > /dev/null 2>&1 || { echo "✗ [pnl-tests] test_vix_pct.mjs failed"; node --test test_vix_pct.mjs 2>&1 | tail -8; exit 1; }
  node --test test_fail_safe.mjs > /dev/null 2>&1 || { echo "✗ [pnl-tests] test_fail_safe.mjs failed"; node --test test_fail_safe.mjs 2>&1 | tail -8; exit 1; }
fi
echo "✓ [pnl-tests] money-math + signal tests green"

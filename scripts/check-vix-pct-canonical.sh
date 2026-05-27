#!/bin/bash
# VIX 20d percentile must use the canonical SignalEngine.computeVixPct20d.
# ─────────────────────────────────────────────────────────────────────────
# The diagonal regime filter computes "today's vix_open vs prior 20
# vix_close" → percentile → dead zone (40 < pct ≤ 90). Used by:
#   • schwab-proxy.js (live worker)
#   • diagonal.html (backtester)
#
# Both MUST go through `signal-engine.js computeVixPct20d`. Re-implementing
# the math inline (e.g. `100 * below / 20` or `below / vix20.length`) is
# how the 2026-05-15/18/19/20 live-vs-backtest divergence happened.
# Three independent bugs compounded:
#   1. backtester compared open-vs-open (live uses open-vs-close)
#   2. missing data → fail-open (live → fail-closed)
#   3. data gap silently dropped 9 dates from the filter set
#
# See tasks/lessons.md P5 + CLAUDE.md rule #11.
#
# Bypass: `git commit --no-verify` (only if you genuinely understand it).

set -e
cd "$(dirname "$0")/.."

# Allowed files: the canonical implementation + test + this hook itself.
ALLOWED_FILES_REGEX='^(signal-engine\.js|test_signal\.js|scripts/check-vix-pct-canonical\.sh|tasks/.*\.md|CLAUDE\.md)$'

# Patterns that strongly suggest inline percentile-from-list math on VIX:
#   • `100 * below / 20`         → exact pattern that bit
#   • `100 * below / vix*`       → variant
#   • `below / vix20`            → variant
#   • `vixPct20d = Math.round(...below...)` → percentile assignment
# (Bare `vix20.length` is NOT enough — that appears in benign logs and
# array-shape guards; we only flag it when combined with `below /` arithmetic.)
PATTERN='100 *\* *below *\/ *(20|vix[A-Za-z0-9]*\.length)|below *\/ *vix(20|Closes?)\b|vixPct20d *= *Math\.round *\(.*below'

# Only check files actually staged in this commit
staged=$(git diff --cached --name-only --diff-filter=ACM | grep -E '\.(js|html|mjs)$' || true)
if [ -z "$staged" ]; then
  exit 0
fi

violations=""
for f in $staged; do
  # Skip allowed files
  if echo "$f" | grep -qE "$ALLOWED_FILES_REGEX"; then
    continue
  fi
  # Grep for the pattern in the STAGED content (not working tree — might differ)
  if git show ":$f" 2>/dev/null | grep -qE "$PATTERN"; then
    hits=$(git show ":$f" | grep -nE "$PATTERN" | head -3)
    violations="$violations
  $f:
$hits"
  fi
done

if [ -n "$violations" ]; then
  echo "" >&2
  echo "🚫 [vix-pct-canonical] inline VIX percentile math detected outside signal-engine.js" >&2
  echo "" >&2
  echo "   The diagonal regime filter must go through" >&2
  echo "     SignalEngine.computeVixPct20d(vixToday, prior20VixCloses)" >&2
  echo "" >&2
  echo "   Files with suspect patterns:$violations" >&2
  echo "" >&2
  echo "   Fix:" >&2
  echo "     1. Import { computeVixPct20d } from './signal-engine.js'" >&2
  echo "        (or use globalThis.SignalEngine.computeVixPct20d in browser code)" >&2
  echo "     2. Replace inline percentile math with a single call." >&2
  echo "     3. Missing data → dead zone (block). Don't fail-open." >&2
  echo "" >&2
  echo "   Context: tasks/lessons.md P5, CLAUDE.md rule #11." >&2
  echo "   Bypass (only if you really know why): git commit --no-verify" >&2
  echo "" >&2
  exit 1
fi

echo "✓ [vix-pct-canonical] no inline VIX percentile math outside signal-engine.js"
exit 0

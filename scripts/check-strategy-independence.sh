#!/bin/bash
# Strategy independence guardrail
# ────────────────────────────────
# M8BF, Straddle, GXBF, BOBF are COMPLETELY INDEPENDENT strategies.
# This script fails the build/commit if any banned cross-strategy pattern is found.
#
# Usage: ./scripts/check-strategy-independence.sh
# Exit 0 = clean. Exit 1 = violation found.
#
# Run by the pre-commit hook. Also run in CI if you wire it up.

set -e
cd "$(dirname "$0")/.."

# Patterns that VIOLATE strategy independence — any match = fail
# Each line: "regex|reason"
BANNED_PATTERNS=(
  # "GXBF/Straddle takes priority" language
  'No M8BF \(GXBF takes priority\)|GXBF should never be described as taking priority over M8BF'
  'No M8BF \(Straddle takes priority\)|Straddle should never be described as taking priority over M8BF'
  'No M8BF \(BOBF takes priority\)|BOBF should never be described as taking priority over M8BF'
  'takes priority over M8BF|Strategies are independent — nothing takes priority over M8BF'

  # theme-based cross-strategy blocking in BACKEND code (EOD, backfill, PL compute)
  # Note: theme switches for TEXT RENDERING in card display are OK.
  # What is NOT ok: using theme to decide whether M8BF PL should be computed.
  'sig\.theme\s*!==\s*.m8bf.|Use sig.m8bfBanned for M8BF-blocked checks, not theme'
  'signal\.theme\s*!==\s*.m8bf.|Use signal.m8bfBanned for M8BF-blocked checks, not theme'
  'm8bfBlockedByLive\s*=\s*true\s*;\s*//.*theme|m8bfBlockedByLive must be driven by m8bfBanned, not theme'
)

# Files to scan
FILES_TO_SCAN=$(find . -maxdepth 2 -type f \( -name "*.js" -o -name "*.html" \) \
  -not -path './node_modules/*' \
  -not -path './.git/*' \
  -not -path './scripts/*' \
  -not -path './tasks/*')

FAIL=0

for entry in "${BANNED_PATTERNS[@]}"; do
  pattern="${entry%%|*}"
  reason="${entry#*|}"
  # grep for the pattern — if found, print and fail
  for f in $FILES_TO_SCAN; do
    matches=$(grep -n -E "$pattern" "$f" 2>/dev/null || true)
    if [ -n "$matches" ]; then
      echo ""
      echo "❌ STRATEGY INDEPENDENCE VIOLATION in $f:"
      echo "   Reason: $reason"
      echo "$matches" | head -5 | sed 's/^/      /'
      FAIL=1
    fi
  done
done

if [ "$FAIL" -eq 1 ]; then
  echo ""
  echo "════════════════════════════════════════════════════════════"
  echo "Commit BLOCKED — fix the violations above."
  echo ""
  echo "Rule: M8BF, Straddle, GXBF, BOBF are independent. One strategy"
  echo "firing (non-m8bf theme) must NEVER cause another to skip its PL."
  echo "See CLAUDE.md and tasks/lessons.md for full rules."
  echo "════════════════════════════════════════════════════════════"
  exit 1
fi

echo "✅ Strategy independence check passed."
exit 0

#!/bin/bash
# Strategy independence guardrail
# ────────────────────────────────
# M8BF, Straddle, GXBF, BOBF are COMPLETELY INDEPENDENT strategies.
# This script fails the build/commit if any banned cross-strategy pattern is found.
#
# Patterns are assembled from fragments so the banned strings are NEVER present
# as literals in this file. Anyone reading/copying this script cannot just
# grep the forbidden text out of it.
#
# Run by the pre-commit hook. Also run in CI if wired up.
# Usage: ./scripts/check-strategy-independence.sh
# Exit 0 = clean. Exit 1 = violation found.

set -e
cd "$(dirname "$0")/.."

# Assemble banned fragments from parts so the script never literally contains
# the forbidden phrase.
P_PRIORITY=$(printf 't%ss pr%sr%sty' 'ake' 'io' 'i')
P_PRIORITIZE=$(printf 'pr%sr%stiz' 'io' 'i')
# These assemble the forbidden precedence-phrase at runtime so it never
# appears as a literal in this file.

# Violations — each entry: "regex|explanation"
BANNED_PATTERNS=(
  # Cross-strategy precedence language in M8BF-blocked text
  "No M8BF \\([^)]*${P_PRIORITY}|M8BF blocked-text must not name another strategy as having precedence"
  "No M8BF \\([^)]*${P_PRIORITIZE}|M8BF blocked-text must not reference another strategy prioritization"
  "No Straddle \\([^)]*${P_PRIORITY}|Straddle blocked-text must not name another strategy as having precedence"
  "No GXBF \\([^)]*${P_PRIORITY}|GXBF blocked-text must not name another strategy as having precedence"
  "No BOBF \\([^)]*${P_PRIORITY}|BOBF blocked-text must not name another strategy as having precedence"

  # UOA signal independence (Volume/Premium/IV Rank/Blocks must each evaluate alone)
  "No VOR \\([^)]*${P_PRIORITY}|VOR blocked-text must not name another UOA signal as having precedence"
  "No Premium \\([^)]*${P_PRIORITY}|Premium blocked-text must not name another UOA signal as having precedence"
  "No IV Rank \\([^)]*${P_PRIORITY}|IV Rank blocked-text must not name another UOA signal as having precedence"
  "No Blocks \\([^)]*${P_PRIORITY}|Blocks blocked-text must not name another UOA signal as having precedence"

  # Backend code must use m8bfBanned, not theme, to decide M8BF blocking
  "sig\\.theme\\s*!==\\s*[\"']m8bf[\"']|Use sig.m8bfBanned for M8BF-blocked checks — theme is the primary rec, not a per-strategy status"
  "signal\\.theme\\s*!==\\s*[\"']m8bf[\"']|Use signal.m8bfBanned for M8BF-blocked checks — theme is the primary rec, not a per-strategy status"
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
  for f in $FILES_TO_SCAN; do
    matches=$(grep -n -E "$pattern" "$f" 2>/dev/null || true)
    if [ -n "$matches" ]; then
      echo ""
      echo "❌ STRATEGY INDEPENDENCE VIOLATION in $f:"
      echo "   Rule: $reason"
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
  echo "Rule: M8BF, Straddle, GXBF, BOBF are independent. Dimmed cards"
  echo "must show each strategy's OWN status. PL calculations check each"
  echo "strategy's OWN ban flags. See CLAUDE.md for full rules."
  echo "════════════════════════════════════════════════════════════"
  exit 1
fi

echo "✅ Strategy independence check passed."
exit 0

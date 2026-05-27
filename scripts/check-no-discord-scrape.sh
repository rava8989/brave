#!/bin/bash
# Fails the commit if banned Discord-scraper references for GXBF reappear.
# The scraper (scrapeGxbfCenter, parseGxbfGamma, GXBF_DISCORD_CHANNEL) was
# deleted in commit 8280d55. GXBF center is now computed live via
# computeGxbfCenterLive — see tasks/GXBF_METHODOLOGY.md.

set -e
cd "$(dirname "$0")/.."

BANNED_TOKENS=(
  'scrapeGxbfCenter'
  'parseGxbfGamma'
  'GXBF_DISCORD_CHANNEL'
  # The /scrape-gamma endpoint was also removed
  "url.pathname === '/scrape-gamma'"
)

# Files to scan: only live code, not docs/lessons/scripts/backups.
SCAN_GLOBS=(
  'schwab-proxy.js'
  'signal-engine.js'
  'index.html'
  'history.html'
  'live.html'
  'multi-strategy-tester.html'
  'gxbf-backtester.html'
  'backtester.html'
  'discord-proxy.js'
)

VIOLATIONS=()
VIOLATION_LINES=""

for token in "${BANNED_TOKENS[@]}"; do
  for f in "${SCAN_GLOBS[@]}"; do
    [ -f "$f" ] || continue
    # Find lines containing the token, EXCLUDING:
    #   - single-line comments  (lines whose first non-space is // or #)
    #   - lines that mention the token inside a string-literal'd comment context
    # We keep it simple: drop lines whose trimmed text begins with // or *.
    matches=$(grep -nF -- "$token" "$f" 2>/dev/null | grep -vE '^[0-9]+:[[:space:]]*(//|\*|#)' || true)
    if [ -n "$matches" ]; then
      VIOLATIONS+=("$token in $f")
      VIOLATION_LINES+="$f:"$'\n'"$matches"$'\n'
    fi
  done
done

if [ ${#VIOLATIONS[@]} -gt 0 ]; then
  echo "" >&2
  echo "🚫 [no-discord-scrape] Banned Discord-scraper tokens reappeared:" >&2
  for v in "${VIOLATIONS[@]}"; do echo "    - $v" >&2; done
  echo "" >&2
  echo "$VIOLATION_LINES" >&2
  echo "" >&2
  echo "   The GXBF Discord scraper was deleted in commit 8280d55. Center" >&2
  echo "   is now computed live via computeGxbfCenterLive (schwab-proxy.js)" >&2
  echo "   and historical data via fetch_thetadata_gxbf.py." >&2
  echo "   See tasks/GXBF_METHODOLOGY.md for the formula." >&2
  echo "" >&2
  echo "   If you genuinely need to bring back a scraper, update this hook" >&2
  echo "   and tasks/lessons.md to remove the warning." >&2
  echo "" >&2
  exit 1
fi

echo "✓ [no-discord-scrape] no banned Discord-scraper tokens found"
exit 0

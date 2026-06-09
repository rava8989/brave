#!/bin/bash
# Headless-Chrome smoke test for dashboard pages.
#
# Loads each affected HTML page in headless Chrome and checks for JS console
# errors before allowing the commit. Catches "audit fix broke the dashboard"
# regressions — exactly the pattern that bit us 2026-06-09.
#
# Only runs when HTML files OR signal-engine.js are in the staged change set
# (skips otherwise to keep fast hooks fast).
#
# Bypass with `git commit --no-verify` ONLY in genuine emergencies.

set -e
cd "$(dirname "$0")/.."

# Which files are staged?
STAGED=$(git diff --cached --name-only --diff-filter=ACM 2>/dev/null || true)

# Map staged files → pages that need smoke testing.
PAGES=()
if echo "$STAGED" | grep -qE '^(index|signal-engine|history|diagonal|cor1m_contango|multi-strategy-tester|gxbf-backtester)\.(html|js)$'; then
  # signal-engine.js change → test all pages that load it
  if echo "$STAGED" | grep -q '^signal-engine\.js$'; then
    PAGES=(index.html history.html diagonal.html cor1m_contango.html multi-strategy-tester.html gxbf-backtester.html)
  else
    # Only test the pages that actually changed
    for f in $STAGED; do
      case "$f" in
        index.html|history.html|diagonal.html|cor1m_contango.html|multi-strategy-tester.html|gxbf-backtester.html)
          PAGES+=("$f");;
      esac
    done
  fi
fi

if [ ${#PAGES[@]} -eq 0 ]; then
  # No HTML/engine changes — skip silently
  exit 0
fi

# Dedupe
PAGES=($(printf '%s\n' "${PAGES[@]}" | sort -u))

# Make sure puppeteer-core is installed
if [ ! -d node_modules/puppeteer-core ]; then
  echo "✗ [smoke-test] puppeteer-core not installed."
  echo "    Run: npm install --save-dev puppeteer-core"
  exit 1
fi

# Run the smoke test
node scripts/smoke-test-dashboard.js "${PAGES[@]}"

#!/bin/bash
# Block commits that contain unresolved merge conflict markers.
# Triggered by .git/hooks/pre-commit. Exit 0 = allow, exit 1 = block.
set -e
STAGED=$(git diff --cached --name-only --diff-filter=ACM)
[ -z "$STAGED" ] && exit 0
FOUND=$(grep -lE "^<<<<<<< |^=======$|^>>>>>>> " $STAGED 2>/dev/null || true)
if [ -n "$FOUND" ]; then
  echo "🚫 BLOCKED — files contain unresolved merge conflict markers:"
  echo "$FOUND" | sed 's/^/    /'
  echo ""
  echo "Resolve every <<<<<<< / ======= / >>>>>>> marker, then re-stage."
  echo "(This guardrail exists because broken-dashboard incidents in 2026-06-02"
  echo " were caused by literal markers slipping into production HTML.)"
  exit 1
fi

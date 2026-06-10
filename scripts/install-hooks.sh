#!/bin/bash
# Install git pre-commit hooks from this repo's scripts/ directory.
#
# Run once after fresh clone:
#   bash scripts/install-hooks.sh
#
# Idempotent — safe to re-run. Overwrites any existing .git/hooks/pre-commit.

set -e
cd "$(dirname "$0")/.."

HOOK=.git/hooks/pre-commit
cat > "$HOOK" << 'EOF'
#!/bin/bash
# Auto-installed git pre-commit hook
# Runs all repo guardrails in sequence. Each must pass for the commit to proceed.
set -e
./scripts/check-strategy-independence.sh
./scripts/check-history-kv-match.sh
./scripts/check-no-discord-scrape.sh
./scripts/check-vix-pct-canonical.sh
./scripts/check-html-js.sh
./scripts/check-no-conflict-markers.sh
./scripts/check-html-smoke.sh
./scripts/check-pnl-tests.sh
./scripts/check-diagonal-parity.sh
./scripts/check-gxbf-parity.sh
EOF
chmod +x "$HOOK"

echo "✓ Installed pre-commit hook at $HOOK"
echo "  → runs scripts/check-strategy-independence.sh"
echo "  → runs scripts/check-history-kv-match.sh"
echo "  → runs scripts/check-no-discord-scrape.sh"
echo "  → runs scripts/check-vix-pct-canonical.sh"
echo "  → runs scripts/check-html-js.sh"
echo "  → runs scripts/check-no-conflict-markers.sh   (added after 2026-06-02 broken-dashboard incident)"
echo "  → runs scripts/check-html-smoke.sh             (added 2026-06-09 — headless-Chrome JS-error catcher)"
echo "  → runs scripts/check-pnl-tests.sh              (added 2026-06-09 — money-math unit tests)"
echo "  → runs scripts/check-diagonal-parity.sh        (added 2026-06-09 — Python↔JS Diagonal agreement)"
echo "  → runs scripts/check-gxbf-parity.sh            (added 2026-06-10 — Python↔JS GXBF agreement)"
echo ""
echo "Bypass for one-off commits: git commit --no-verify"

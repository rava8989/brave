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
EOF
chmod +x "$HOOK"

echo "✓ Installed pre-commit hook at $HOOK"
echo "  → runs scripts/check-strategy-independence.sh"
echo "  → runs scripts/check-history-kv-match.sh"
echo "  → runs scripts/check-no-discord-scrape.sh"
echo ""
echo "Bypass for one-off commits: git commit --no-verify"

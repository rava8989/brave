#!/bin/bash
# history_data.json KV-vs-git divergence check
# ──────────────────────────────────────────────
# history_data.json is KV-backed in production. The Cloudflare KV namespace
# (SIGNAL_KV[history_data]) is the source of truth; GitHub is a mirror that
# the live worker writes via mirrorHistoryToGitHub() on every upsert.
#
# If you `git commit` a hand-edited history_data.json without ALSO writing
# the same content into KV, the next worker upsert (e.g. an auto "history
# update for YYYY-MM-DD (diagPL)") will read its stale KV state, apply the
# single requested field change, and mirror the WHOLE KV blob back to
# GitHub — REVERTING your hand-edit silently.
#
# This has bitten the repo at least twice (lessons.md 2026-05-26 + the
# original 4726c2e entry). This hook is the enforced guardrail.
#
# Workflow:
#   1. Hand-edit history_data.json locally
#   2. Run: ./scripts/push-history.sh "your commit message"
#      ← writes to KV first, verifies, then commits + pushes the mirror
#
# If you try `git commit history_data.json` directly without running
# push-history.sh, this hook fails the commit and tells you why.
#
# Bypass: `git commit --no-verify` (only do this if you understand what
# breaks).
#
# Exit codes:
#   0 — history_data.json not modified in this commit (skip check) OR
#       KV matches the staged file content
#   1 — KV diverges from staged content (commit blocked)
#   2 — wrangler not available or auth failed (warn, do not block —
#       common in CI where wrangler isn't installed; main session catches it)

set -e
cd "$(dirname "$0")/.."

FILE=history_data.json
KV_ID="be47dd5d2fb34ec79e5a34f0e241f125"
KV_KEY="history_data"

# 1. Is history_data.json staged in this commit?
if ! git diff --cached --name-only | grep -qx "$FILE"; then
  exit 0   # not modified, nothing to check
fi

# 2. Is wrangler available?
if ! command -v npx >/dev/null 2>&1; then
  echo "⚠️  [history-kv-check] npx not found — skipping KV divergence check." >&2
  echo "    Install Node.js + run 'npx wrangler whoami' to enable this check." >&2
  exit 0  # skip silently — wrangler auth/network issue
fi

# 3. Pull current KV value (timeout 15s)
TMPKV=$(mktemp)
trap 'rm -f "$TMPKV"' EXIT
if ! timeout 15 npx wrangler kv key get --namespace-id="$KV_ID" "$KV_KEY" --remote >"$TMPKV" 2>/dev/null; then
  echo "⚠️  [history-kv-check] failed to read KV (wrangler not authenticated, network down, or namespace unreachable)" >&2
  echo "    Skipping check — commit allowed, but please verify KV manually with:" >&2
  echo "    npx wrangler kv key get --namespace-id=$KV_ID '$KV_KEY' --remote | head -c 200" >&2
  exit 0  # skip silently — wrangler auth/network issue
fi

# 4. Get the STAGED content of history_data.json
TMPSTAGED=$(mktemp)
trap 'rm -f "$TMPKV" "$TMPSTAGED"' EXIT
git show ":$FILE" > "$TMPSTAGED"

# 5. Normalize both (parse + re-serialize via python's json) so whitespace
#    differences from json.dump vs JSON.stringify don't cause false positives.
NORM_KV=$(python3 -c "import json,sys; print(json.dumps(json.load(open(sys.argv[1])), sort_keys=True))" "$TMPKV" 2>/dev/null)
NORM_STAGED=$(python3 -c "import json,sys; print(json.dumps(json.load(open(sys.argv[1])), sort_keys=True))" "$TMPSTAGED" 2>/dev/null)

if [ -z "$NORM_KV" ] || [ -z "$NORM_STAGED" ]; then
  echo "⚠️  [history-kv-check] could not parse one of the two JSON blobs — skipping." >&2
  exit 0  # skip silently — wrangler auth/network issue
fi

if [ "$NORM_KV" = "$NORM_STAGED" ]; then
  echo "✓ [history-kv-check] history_data.json matches KV — safe to commit"
  exit 0
fi

# 6. DIVERGENCE — block commit with clear message
echo "" >&2
echo "🚫 [history-kv-check] history_data.json staged for commit DIVERGES FROM KV" >&2
echo "" >&2
echo "   This commit will be silently REVERTED on the next worker upsert." >&2
echo "   The live worker reads from Cloudflare KV (SIGNAL_KV[$KV_KEY])," >&2
echo "   not from GitHub. Pushing GitHub alone leaves KV stale — the next" >&2
echo "   cron tick will mirror KV's stale state back over your commit." >&2
echo "" >&2

# Show first 3 dates that differ (for context)
python3 - "$TMPKV" "$TMPSTAGED" <<'PY' >&2 || true
import json, sys
kv = json.load(open(sys.argv[1]))
st = json.load(open(sys.argv[2]))
kvd = {e['date']: e for e in kv if 'date' in e}
std = {e['date']: e for e in st if 'date' in e}
diffs = []
for d in sorted(set(kvd) | set(std)):
    if kvd.get(d) != std.get(d):
        diffs.append(d)
if diffs:
    print(f"   Diverging dates: {len(diffs)}. First 3:")
    for d in diffs[:3]:
        print(f"     {d}: KV={kvd.get(d)}  vs  staged={std.get(d)}")
print(f"   Total KV entries: {len(kv)}, staged: {len(st)}")
PY

echo "" >&2
echo "   To fix:" >&2
echo "     1. git reset HEAD $FILE                    # unstage" >&2
echo "     2. ./scripts/push-history.sh \"your msg\"  # writes KV first, then commits + pushes" >&2
echo "" >&2
echo "   To bypass (only if you understand what breaks):" >&2
echo "     git commit --no-verify" >&2
echo "" >&2
exit 1

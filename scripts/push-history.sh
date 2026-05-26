#!/bin/bash
# Atomic history_data.json writer.
#
# WHY: history_data.json is KV-backed (HISTORY_KV_KEY='history_data' in
# SIGNAL_KV namespace be47dd5d2fb34ec79e5a34f0e241f125). The worker reads
# from KV first, mirrors back to GitHub. A `git push` alone leaves KV
# stale → the next worker upsert reads stale KV and reverts your changes.
#
# THIS SCRIPT: write to KV first, then commit + push to GitHub mirror,
# so the two stay aligned and the worker can't revert you.
#
# USAGE: ./scripts/push-history.sh "your commit message"
#
# NEVER `git push` an edit to history_data.json without running this.
# See tasks/lessons.md "history_data.json is KV-backed".

set -e

if [ ! -f history_data.json ]; then
  echo "ERROR: history_data.json not found in cwd. Run from project root." >&2
  exit 1
fi

if [ -z "$1" ]; then
  echo "ERROR: commit message required. Usage: $0 \"message\"" >&2
  exit 1
fi

KV_NS="be47dd5d2fb34ec79e5a34f0e241f125"

echo "→ Validating history_data.json is valid JSON…"
python3 -c "import json; json.load(open('history_data.json'))" \
  || { echo "ERROR: history_data.json is not valid JSON" >&2; exit 1; }

echo "→ Writing to KV (source of truth)…"
yes | npx wrangler kv key put --namespace-id="$KV_NS" \
  "history_data" --path=history_data.json --remote

echo "→ Verifying KV write…"
KV_LEN=$(npx wrangler kv key get --namespace-id="$KV_NS" "history_data" --remote 2>/dev/null \
  | python3 -c "import json,sys; print(len(json.load(sys.stdin)))")
FILE_LEN=$(python3 -c "import json; print(len(json.load(open('history_data.json'))))")
if [ "$KV_LEN" != "$FILE_LEN" ]; then
  echo "ERROR: KV length ($KV_LEN) != local length ($FILE_LEN). Aborting GitHub push." >&2
  exit 1
fi
echo "  ✓ KV has $KV_LEN entries (matches local)"

echo "→ Committing + pushing GitHub mirror…"
git add history_data.json
git pull --rebase
git commit -m "$1"
git push

echo
echo "✓ DONE. history_data.json synced to KV + GitHub."
echo "  KV namespace: $KV_NS"
echo "  GitHub: $(git rev-parse HEAD)"

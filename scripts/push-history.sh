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

# FIX (2026-06-09 audit P0): require minimum entry count to catch truncation.
# A history with < 400 entries is almost certainly a write accident.
FILE_LEN=$(python3 -c "import json; print(len(json.load(open('history_data.json'))))")
if [ "$FILE_LEN" -lt 400 ]; then
  echo "ERROR: history_data.json has only $FILE_LEN entries (expected 400+). Aborting — looks truncated." >&2
  echo "       If this is intentional, set ALLOW_TRUNCATED=1 to bypass." >&2
  if [ "$ALLOW_TRUNCATED" != "1" ]; then exit 1; fi
fi

# FIX (2026-06-09 audit P1): pull --rebase BEFORE KV write, not after.
# Old order (KV → pull → push) had a window where a worker upsert mid-script
# would `git pull --rebase` over your local edit and silently revert it.
echo "→ git pull --rebase (before KV write to avoid mid-write worker race)…"
git pull --rebase --autostash || { echo "ERROR: pull failed — fix conflicts before pushing." >&2; exit 1; }

# Re-check after rebase in case the rebase changed the file.
FILE_LEN=$(python3 -c "import json; print(len(json.load(open('history_data.json'))))")

echo "→ Writing to KV (source of truth)…"
yes | npx wrangler kv key put --namespace-id="$KV_NS" \
  "history_data" --path=history_data.json --remote

# FIX (2026-06-09 audit P0 #15): content-hash verification, not just length.
# Two arrays with same length but different field values would have passed
# the old check. Now we compare SHA256 of normalized (sorted-key) JSON.
echo "→ Verifying KV write (content hash + length)…"
FILE_HASH=$(python3 -c "import json,hashlib; print(hashlib.sha256(json.dumps(json.load(open('history_data.json')),sort_keys=True,separators=(',',':')).encode()).hexdigest())")
KV_DATA=$(npx wrangler kv key get --namespace-id="$KV_NS" "history_data" --remote 2>/dev/null)
KV_LEN=$(echo "$KV_DATA" | python3 -c "import json,sys; print(len(json.load(sys.stdin)))")
KV_HASH=$(echo "$KV_DATA" | python3 -c "import json,sys,hashlib; print(hashlib.sha256(json.dumps(json.load(sys.stdin),sort_keys=True,separators=(',',':')).encode()).hexdigest())")
if [ "$KV_LEN" != "$FILE_LEN" ]; then
  echo "ERROR: KV length ($KV_LEN) != local length ($FILE_LEN). Aborting GitHub push." >&2
  exit 1
fi
if [ "$KV_HASH" != "$FILE_HASH" ]; then
  echo "ERROR: KV content hash differs from local — KV write didn't take or got mutated mid-write." >&2
  echo "       local SHA256: $FILE_HASH" >&2
  echo "       KV    SHA256: $KV_HASH" >&2
  echo "       Re-run the script (worker may have raced). If it persists, investigate KV state." >&2
  exit 1
fi
echo "  ✓ KV has $KV_LEN entries, content hash matches local ($FILE_HASH)"

echo "→ Committing + pushing GitHub mirror…"
git add history_data.json
git commit -m "$1"
git push

echo
echo "✓ DONE. history_data.json synced to KV + GitHub."
echo "  KV namespace: $KV_NS"
echo "  GitHub: $(git rev-parse HEAD)"

#!/usr/bin/env bash
# One-command deploy of the schedule sync worker (Cloudflare Workers + KV).
#   bash schedule/worker/deploy.sh
# Needs Node.js. wrangler will open a browser to log you in the first time.
set -euo pipefail
cd "$(dirname "$0")"

if grep -q REPLACE_WITH_YOUR_KV_NAMESPACE_ID wrangler.toml; then
  echo "▶ Creating the KV namespace…"
  out="$(npx --yes wrangler kv namespace create SCHEDULE_KV 2>&1 | tee /dev/stderr)"
  id="$(printf '%s' "$out" | grep -oE '[0-9a-f]{32}' | head -1 || true)"
  if [ -z "$id" ]; then
    echo "✗ Could not read the namespace id. Paste it into wrangler.toml by hand and re-run."; exit 1
  fi
  sed -i.bak "s/REPLACE_WITH_YOUR_KV_NAMESPACE_ID/$id/" wrangler.toml && rm -f wrangler.toml.bak
  echo "✓ wrangler.toml now points at namespace $id"
fi

echo
echo "▶ Access codes (stored as secrets — pick any phrases; you will hand them out to people):"
echo "   supervisor code:"; npx --yes wrangler secret put SUPERVISOR_CODE
echo "   worker code:";     npx --yes wrangler secret put WORKER_CODE

echo
read -r -p "▶ Anthropic API key so the app can read photos of signed sheets (Enter to skip): " key
if [ -n "${key:-}" ]; then printf '%s' "$key" | npx --yes wrangler secret put ANTHROPIC_API_KEY; fi

echo
echo "▶ Deploying…"
npx --yes wrangler deploy

cat <<'MSG'

✓ Done. Copy the https://…workers.dev URL printed above into
    APP_CONFIG.syncUrl   in   schedule/js/config.js
commit and push, then sign in first with the supervisor code.
MSG

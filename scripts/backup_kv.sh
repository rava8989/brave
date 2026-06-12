#!/bin/bash
# backup_kv.sh — snapshot the critical Cloudflare KV keys (both workers) to
# ~/Backups/kv/<timestamp>/. Run after any config change (takes ~20s).
# Restore commands: see RECOVERY.md.
set -uo pipefail

SIGNAL_NS="be47dd5d2fb34ec79e5a34f0e241f125"     # schwab-proxy SIGNAL_KV
BOT_NS="a95dc96f256d4e3ebe6dde9cea7bdc8a"        # sigma-3 BOT_KV
OUT=~/Backups/kv/$(date +%Y-%m-%d_%H%M)
mkdir -p "$OUT"
cd "$(dirname "$0")/.."

SIGNAL_KEYS=(
  schwab_tokens schwab_creds discord_config risk_config
  tasty_refresh_token tail_trigger_state history_data
  straddle_open_trade bobf_open_trade gxbf_open_trade diagonal_open_trade
)
BOT_KEYS=(
  kill_switch strategy:bobf strategy:gxbf strategy:m8bf strategy:strad
  strategy:diag strategy:tail
)

echo "→ $OUT"
for k in "${SIGNAL_KEYS[@]}"; do
  v=$(npx wrangler kv key get "$k" --namespace-id "$SIGNAL_NS" --remote 2>/dev/null)
  [ -n "$v" ] && printf '%s' "$v" > "$OUT/signal__${k//[:\/]/_}.json" && echo "  signal: $k"
done
for k in "${BOT_KEYS[@]}"; do
  v=$(npx wrangler kv key get "$k" --namespace-id "$BOT_NS" --remote 2>/dev/null)
  [ -n "$v" ] && printf '%s' "$v" > "$OUT/bot__${k//[:\/]/_}.json" && echo "  bot: $k"
done
chmod 600 "$OUT"/*.json 2>/dev/null
echo "done — $(ls "$OUT" | wc -l | tr -d ' ') keys saved"

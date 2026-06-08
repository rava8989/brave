#!/bin/bash
# refresh_tail_hedge.sh
#
# Daily refresh for the Tail Hedge backtester. Pulls today's COR1M / VIX-term /
# SPX-puts / SPX-1min / VIX-1min data, rebuilds cor1m_contango_bundle.json,
# commits and pushes.
#
# Scheduled by ~/Library/LaunchAgents/com.tailhedge.refresh.plist (5 PM ET
# weekdays). Safe to run manually anytime — idempotent on existing data.
#
# Exit codes:
#   0  success (or skipped — weekend / holiday / ThetaData down / no new data)
#   1  partial — committed but push failed
#   2  hard error — couldn't fetch
set -e

REPO=/Users/ravshanrakhmanov/Desktop/spx-backtester/spx-backtester
LOG=/Users/ravshanrakhmanov/Desktop/spx-backtester/spx-backtester/scripts/refresh_tail_hedge.log
THETA=http://localhost:25503/v3
TODAY=$(date +%Y-%m-%d)
TODAY_NODASH=$(date +%Y%m%d)

ts() { date '+%Y-%m-%d %H:%M:%S'; }
log() { echo "[$(ts)] $*" | tee -a "$LOG"; }

cd "$REPO"

log "=== Tail Hedge refresh for $TODAY ==="

# ── 0. Skip weekends ────────────────────────────────────────────────────────
DOW=$(date +%u)  # 1=Mon ... 7=Sun
if [ "$DOW" -ge 6 ]; then
  log "Weekend (DOW=$DOW), skipping."
  exit 0
fi

# ── 1. ThetaData reachable? ─────────────────────────────────────────────────
if ! curl -s -m 3 "$THETA/index/history/ohlc?symbol=VIX&start_date=$TODAY_NODASH&end_date=$TODAY_NODASH&interval=1h&format=csv" | head -1 | grep -q timestamp; then
  log "ThetaData unreachable at $THETA. Make sure ThetaTerminal is running. Skipping."
  exit 0
fi

# ── 2. Pull latest from git first (avoid conflicts with auto: commits) ──────
log "git pull --rebase"
if ! git pull --rebase --autostash >> "$LOG" 2>&1; then
  log "git pull failed — bailing out (no data fetched yet, safe to retry)"
  exit 2
fi

# ── 3. Refresh COR1M (year-to-date append) ──────────────────────────────────
log "Refreshing COR1M..."
START_YEAR_NODASH="${TODAY:0:4}0101"
COR1M_OUT=data/cor1m/raw_${TODAY:0:4}_$(date -j -v+1y +%Y).csv
# Pull last 365 days max
curl -s "$THETA/index/history/ohlc?symbol=COR1M&start_date=$START_YEAR_NODASH&end_date=$TODAY_NODASH&interval=1h&format=csv" > "$COR1M_OUT.tmp"
if head -1 "$COR1M_OUT.tmp" | grep -q timestamp && [ "$(wc -l < "$COR1M_OUT.tmp")" -gt 10 ]; then
  mv "$COR1M_OUT.tmp" "$COR1M_OUT"
  log "  COR1M: $(wc -l < "$COR1M_OUT") rows in $COR1M_OUT"
else
  log "  COR1M fetch failed, keeping existing file"
  rm -f "$COR1M_OUT.tmp"
fi

# ── 4. Refresh VIX term structure (auto-chunks, idempotent) ────────────────
log "Refreshing VIX term structure..."
python3 fetch_thetadata_vix_term.py >> "$LOG" 2>&1
log "  VIX term done: last day = $(tail -1 data/vix_term/daily.csv | cut -d, -f1)"

# ── 5. SPX & VIX 1-min bars for today ──────────────────────────────────────
log "Fetching today's SPX + VIX 1-min bars..."
python3 - <<PYEOF >> "$LOG" 2>&1
import csv, requests
from pathlib import Path
THETA = 'http://localhost:25503/v3'
TODAY = '$TODAY'
TODAY_NODASH = '$TODAY_NODASH'

def fetch_and_save(symbol, dest_dir, prefix):
    out = Path(dest_dir) / f'{prefix}_{TODAY_NODASH}.csv'
    if out.exists() and out.stat().st_size > 1000:
        print(f'  {symbol}: {out.name} exists, skipping')
        return
    r = requests.get(f'{THETA}/index/history/ohlc', params={
        'symbol': symbol, 'start_date': TODAY_NODASH, 'end_date': TODAY_NODASH,
        'interval': '1m', 'format': 'csv'
    }, timeout=60)
    lines = r.text.strip().split('\n')
    if not lines or len(lines) < 5 or lines[0].startswith(('Invalid', '<')):
        print(f'  {symbol}: no data for today')
        return
    rows = []
    for ln in lines[1:]:
        cols = ln.split(',')
        if len(cols) < 5: continue
        ts = cols[0].replace('T', ' ').split('.')[0]
        rows.append([ts, cols[1], cols[2], cols[3], cols[4]])
    if not rows:
        print(f'  {symbol}: empty')
        return
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, 'w', newline='') as f:
        w = csv.writer(f); w.writerow(['timestamp','open','high','low','close']); w.writerows(rows)
    print(f'  {symbol}: wrote {len(rows)} rows → {out.name}')

fetch_and_save('SPX', 'data/spx', 'SPX')
fetch_and_save('VIX', 'data/vix', 'VIX')
PYEOF

# ── 6. SPX 9:35 put snapshot for today ─────────────────────────────────────
log "Fetching today's 9:35 SPX put snapshot..."
python3 fetch_thetadata_diagonal.py --time 09:35 --date "$TODAY" >> "$LOG" 2>&1

# ── 7. Rebuild bundle ──────────────────────────────────────────────────────
log "Rebuilding cor1m_contango_bundle.json..."
python3 build_cor1m_contango_bundle.py >> "$LOG" 2>&1

# ── 8. Detect what changed, commit + push if anything did ──────────────────
TO_ADD=(
  cor1m_contango_bundle.json
  data/cor1m/raw_*.csv
  data/vix_term/daily.csv
)
# Also include today's bars + snapshot (the snapshot path may not match the glob below; add explicitly)
git add "${TO_ADD[@]}" 2>/dev/null || true
git add "data/spx/SPX_${TODAY_NODASH}.csv" "data/vix/VIX_${TODAY_NODASH}.csv" 2>/dev/null || true
git add "data/polygon/SPX_${TODAY_NODASH}_0935.json" 2>/dev/null || true

if git diff --cached --quiet; then
  log "No changes to commit (data was already current)."
  exit 0
fi

CHANGED=$(git diff --cached --name-only | wc -l | tr -d ' ')
log "Committing $CHANGED files..."
git commit -m "auto: tail hedge refresh $TODAY" >> "$LOG" 2>&1

if git push >> "$LOG" 2>&1; then
  log "✓ Pushed. Done."
  exit 0
else
  log "✗ Push failed (commit succeeded locally)."
  exit 1
fi

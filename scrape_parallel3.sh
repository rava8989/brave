#!/bin/bash
# 3-wide parallel scrape of remaining 7 timestamps (15:00 already done).
# Uses xargs -P 3 to cap at 3 concurrent scrapers on Polygon Advanced plan.
cd "$(dirname "$0")"
LOG=/tmp/scrape_8times.log
{
  echo ""
  echo "=== 3-wide parallel scrape started at $(date) ==="
  echo "PID=$$"
} >> "$LOG"

printf "%s\n" 15:15 15:30 15:45 11:45 11:30 09:45 10:00 \
  | xargs -I {} -P 3 bash -c '
    t="$1"
    log="$2"
    echo "=== [$(date +%H:%M:%S)] START @ $t ===" >> "$log"
    python3 -u fetch_polygon_spx.py --time "$t" >> "$log" 2>&1
    rc=$?
    echo "=== [$(date +%H:%M:%S)] DONE  @ $t (rc=$rc) ===" >> "$log"
  ' _ {} "$LOG"

{
  echo ""
  echo "=== ALL_DONE at $(date) ==="
} >> "$LOG"

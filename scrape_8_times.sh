#!/bin/bash
# Serial scrape of 8 new intraday timestamps (run one at a time, light on CPU)
set -u
cd "$(dirname "$0")"
LOG=/tmp/scrape_8times.log
echo "=== 8-timestamp scrape started at $(date) ===" > "$LOG"
echo "PID=$$" >> "$LOG"
for t in 15:00 15:15 15:30 15:45 11:45 11:30 09:45 10:00; do
  echo "" >> "$LOG"
  echo "=== [$(date '+%H:%M:%S')] START @ $t ===" >> "$LOG"
  python3 -u fetch_polygon_spx.py --time "$t" >> "$LOG" 2>&1
  rc=$?
  echo "=== [$(date '+%H:%M:%S')] DONE  @ $t (rc=$rc) ===" >> "$LOG"
done
echo "" >> "$LOG"
echo "=== ALL_DONE at $(date) ===" >> "$LOG"

"""
Merge per-day Polygon quote files + existing spot/VIX data into one JSON
that the diagonal.html backtester consumes.

Input:
  data/polygon/SPX_YYYYMMDD.json — per-day quote files from fetch_polygon_spx.py
  data/diagonal_bs_data.json    — existing spot/VIX snapshots

Output:
  data/diagonal_real_data.json  — combined dataset:
    {
      dates: [...],
      by_date: {
        "YYYY-MM-DD": {
          spot: 6830.94,
          vix: 15.06,
          target_time_et: "14:00",
          quotes: {
            "O:SPXW260105P06840000": {
              strike: 6840,
              expiration: "2026-01-05",
              bid: 22.4,
              ask: 22.6
            },
            ...
          }
        }
      }
    }

Missing days (no Polygon fetch yet) are skipped. Re-run after more data arrives.
"""
import os
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
POLYGON_DIR = ROOT / 'data' / 'polygon'
BS_DATA = ROOT / 'data' / 'diagonal_bs_data.json'
OUTPUT = ROOT / 'data' / 'diagonal_real_data.json'


def main():
    # Load existing spot/VIX data
    bs = json.loads(BS_DATA.read_text())

    merged = {'dates': [], 'by_date': {}, 'generated_at': None, 'source': 'polygon_advanced'}

    for date_iso in bs['dates']:
        bs_entry = bs['by_date'].get(date_iso) or {}
        poly_path = POLYGON_DIR / f'SPX_{date_iso.replace("-","")}.json'

        if not poly_path.exists():
            continue  # skip days we haven't fetched yet

        poly = json.loads(poly_path.read_text())

        # Spot: prefer Polygon's recorded spot (consistent with target time)
        spot = poly.get('spot')
        target_time = poly.get('target_time_et', '14:00')

        # VIX: pull from existing BS data at matching time slot
        # target_time=14:00 → vix_14; target_time=12:45 → vix_12 (closest we have)
        if target_time == '12:45':
            vix = bs_entry.get('vix_12') or bs_entry.get('vix_14')
        else:
            vix = bs_entry.get('vix_14') or bs_entry.get('vix_12')

        quotes = poly.get('quotes') or {}
        if not quotes:
            continue  # skip days with no usable quotes

        merged['dates'].append(date_iso)
        merged['by_date'][date_iso] = {
            'spot': spot,
            'vix': vix,
            'target_time_et': target_time,
            'quotes': quotes,
        }

    merged['dates'].sort()
    from datetime import datetime
    merged['generated_at'] = datetime.now().isoformat()

    total_quotes = sum(len(d['quotes']) for d in merged['by_date'].values())
    OUTPUT.write_text(json.dumps(merged, separators=(',', ':')))
    size_mb = OUTPUT.stat().st_size / 1024 / 1024
    print(f'Wrote {OUTPUT.name}: {len(merged["dates"])} dates, {total_quotes} quotes, {size_mb:.1f} MB')


if __name__ == '__main__':
    main()

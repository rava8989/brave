"""
One-shot splitter: convert existing per-half-year diagonal bundles into
per-(half, time) bundles so the browser can load only the two timestamps
it actually needs (entry_time + exit_time) instead of all 20.

Before:
  data/diagonal_real_YYYY_H{1,2}.json.gz  — 7 files × ~47 MB gz × 20 times
  Total: 329 MB gz → 3.1 GB decompressed → ~35 M quote rows

After:
  data/diagonal_real_YYYY_H{1,2}_t{HHMM}.json.gz  — 7 × 20 = 140 files
  Each ~2.4 MB gz × 1 time
  Initial load fetches 7 halves × 2 times = 14 files ≈ 34 MB gz (10× less)
  Additional time selections lazy-load 7 more files each.

Per-file structure (flat — no by_time nesting since each file is ONE time):
  {
    "year": "2024", "half": "H1", "time": "14:00",
    "dates": ["2024-01-02", ...],
    "by_date": {
      "2024-01-02": { "spot": 4739.34, "vix": 13.22, "quotes": {...} }
    }
  }

This splitter reads existing gz files and writes the sharded output.
It does NOT re-scrape or hit Polygon. Idempotent: re-running overwrites.
"""
from __future__ import annotations

import gzip
import json
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / 'data'


def main():
    # Discover input gz files
    input_files = sorted(DATA_DIR.glob('diagonal_real_[0-9][0-9][0-9][0-9]_H[12].json.gz'))
    if not input_files:
        print('No input files found matching data/diagonal_real_YYYY_H{1,2}.json.gz')
        return
    print(f'Reading {len(input_files)} input bundles')

    halves_seen: list[str] = []
    times_seen: set[str] = set()
    total_in_quotes = 0
    total_out_quotes = 0
    files_written: list[str] = []
    generated_at = datetime.now().isoformat()

    for in_path in input_files:
        with gzip.open(in_path, 'rb') as f:
            bundle = json.loads(f.read())
        year = bundle['year']
        half = bundle['half']
        half_key = f'{year}_{half}'
        halves_seen.append(half_key)
        times_in = bundle.get('times', [])

        # Count input quotes for sanity
        in_quotes = sum(
            len(t.get('quotes') or {})
            for d in bundle['by_date'].values()
            for t in d.get('by_time', {}).values()
        )
        total_in_quotes += in_quotes

        # Reshape: per-time dict of per-date records
        per_time: dict[str, dict] = {t: {'dates': [], 'by_date': {}} for t in times_in}
        for date_iso in bundle['dates']:
            by_time = bundle['by_date'][date_iso].get('by_time', {})
            for t, snap in by_time.items():
                if not snap or not snap.get('quotes'):
                    continue
                per_time[t]['dates'].append(date_iso)
                per_time[t]['by_date'][date_iso] = {
                    'spot': snap.get('spot'),
                    'vix': snap.get('vix'),
                    'quotes': snap['quotes'],
                }

        # Write per-(half, time) files
        for t, payload in per_time.items():
            if not payload['dates']:
                continue  # skip empty times (rare, defensive)
            times_seen.add(t)
            payload['dates'].sort()
            payload['year'] = year
            payload['half'] = half
            payload['time'] = t
            payload['generated_at'] = generated_at
            payload['source'] = 'polygon_advanced_multi_time'

            time_compact = t.replace(':', '')
            filename = f'diagonal_real_{half_key}_t{time_compact}.json.gz'
            out_path = DATA_DIR / filename
            raw = json.dumps(payload, separators=(',', ':')).encode('utf-8')
            with gzip.open(out_path, 'wb', compresslevel=9) as f:
                f.write(raw)

            gz_mb = out_path.stat().st_size / 1024 / 1024
            q = sum(len(v['quotes']) for v in payload['by_date'].values())
            total_out_quotes += q
            files_written.append(filename)
            print(f'  {filename}: {len(payload["dates"])} dates, '
                  f'{q:,} quotes, {gz_mb:.2f} MB gz')

    # Write new index
    times_sorted = sorted(times_seen)
    index = {
        'format': 'per_time_v2',
        'halves': halves_seen,
        'times': times_sorted,
        'file_template': 'diagonal_real_{half}_t{time_compact}.json.gz',
        'source': 'polygon_advanced_multi_time',
        'encoding': 'gzip',
        'generated_at': generated_at,
    }
    idx_path = DATA_DIR / 'diagonal_real_index.json'
    idx_path.write_text(json.dumps(index, separators=(',', ':')))
    print(f'\nWrote {idx_path.name}: {len(halves_seen)} halves × '
          f'{len(times_sorted)} times = {len(files_written)} files')

    print(f'\nQuote count: in={total_in_quotes:,}  out={total_out_quotes:,}  '
          f'{"OK" if total_in_quotes == total_out_quotes else "MISMATCH!"}')
    print(f'Total files written: {len(files_written)}')


if __name__ == '__main__':
    main()

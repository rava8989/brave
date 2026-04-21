"""
Merge per-day Polygon quote files (all 6 intraday timestamps) + VIX 1-min bars
into one gzipped JSON per year that the diagonal.html backtester consumes.

Input:
  data/polygon/SPX_YYYYMMDD.json       — 14:00 default snapshot
  data/polygon/SPX_YYYYMMDD_HHMM.json  — intraday (13:00, 13:15, 13:30, 13:45, 14:15)
  data/vix/VIX_YYYYMMDD.csv            — 1-min VIX bars (for exact-timestamp lookup)
  data/diagonal_bs_data.json           — legacy spot/VIX snapshots (fallback)

Output:
  data/diagonal_real_YYYY_H1.json.gz (Jan–Jun) and
  data/diagonal_real_YYYY_H2.json.gz (Jul–Dec) — gzipped per-half-year data:
    {
      "year": "2024",
      "half": "H1",
      "dates": [...],
      "times": ["13:00","13:15","13:30","13:45","14:00","14:15"],
      "by_date": {
        "YYYY-MM-DD": {
          "by_time": {
            "13:00": { "spot": 4700.3, "vix": 13.65, "quotes": {...} },
            "14:00": { "spot": 4705.1, "vix": 13.58, "quotes": {...} }
          }
        }
      }
    }
  data/diagonal_real_index.json — lists files + times available:
    { "files": ["diagonal_real_2023_H1.json.gz", ...], "times": [...] }

Size constraints:
  • GitHub's hard per-file limit is 100 MB.
  • V8's max string length is ~512 MB (UTF-16 chars). Decompressed per-year JSON
    with 20 timestamps hits ~820 MB, which silently fails `JSON.parse(text)` in
    the browser. Splitting per half-year keeps each chunk ~400 MB decompressed,
    well under the limit, and each gz stays ~40–45 MB (far under GitHub's cap).
"""
from __future__ import annotations

import json
import gzip
import os
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent
POLYGON_DIR = ROOT / 'data' / 'polygon'
VIX_DIR = ROOT / 'data' / 'vix'
BS_DATA = ROOT / 'data' / 'diagonal_bs_data.json'

TIMES = ['09:45', '10:00', '11:30', '11:45',
         '12:00', '12:15', '12:30', '12:45',
         '13:00', '13:15', '13:30', '13:45',
         '14:00', '14:15', '14:30', '14:45',
         '15:00', '15:15', '15:30', '15:45']


def vix_from_csv(date_iso: str, hhmm: str) -> float | None:
    """Look up VIX close from data/vix/VIX_YYYYMMDD.csv at exact HH:MM timestamp.

    Returns None if file missing or timestamp not found. Uses close (4th column).
    """
    csv_path = VIX_DIR / f'VIX_{date_iso.replace("-", "")}.csv'
    if not csv_path.exists():
        return None
    target = f'{date_iso} {hhmm}:00,'
    try:
        with open(csv_path) as f:
            f.readline()  # header
            for line in f:
                if line.startswith(target):
                    # timestamp,open,high,low,close
                    parts = line.rstrip('\n').split(',')
                    if len(parts) >= 5:
                        return float(parts[4])
                    return None
    except Exception:
        return None
    return None


def snapshot_path(date_iso: str, hhmm: str) -> Path:
    """File path for a given (date, timestamp) snapshot."""
    if hhmm == '14:00':
        return POLYGON_DIR / f'SPX_{date_iso.replace("-", "")}.json'
    return POLYGON_DIR / f'SPX_{date_iso.replace("-", "")}_{hhmm.replace(":", "")}.json'


def half_of(date_iso: str) -> str:
    """Return 'H1' for Jan–Jun, 'H2' for Jul–Dec."""
    month = int(date_iso[5:7])
    return 'H1' if month <= 6 else 'H2'


def main():
    bs = json.loads(BS_DATA.read_text())

    # Discover all dates we have at least one snapshot for.
    # Group by (year, half) so decompressed per-chunk JSON stays under V8's
    # ~512 MB max string length (which 20-timestamp per-year files blew past).
    by_half = {}   # (year, half) -> {dates, by_date}
    dates_seen = set()

    for date_iso in bs['dates']:
        bs_entry = bs['by_date'].get(date_iso) or {}
        by_time = {}

        for hhmm in TIMES:
            poly = snapshot_path(date_iso, hhmm)
            if not poly.exists():
                continue
            try:
                data = json.loads(poly.read_text())
            except Exception:
                continue
            quotes = data.get('quotes') or {}
            if not quotes:
                continue
            spot = data.get('spot')
            if spot is None:
                continue

            # VIX: exact timestamp from 1-min CSV, fall back to legacy BS data
            vix = vix_from_csv(date_iso, hhmm)
            if vix is None:
                if hhmm == '12:45':
                    vix = bs_entry.get('vix_12') or bs_entry.get('vix_14')
                else:
                    vix = bs_entry.get('vix_14') or bs_entry.get('vix_12')

            by_time[hhmm] = {
                'spot': spot,
                'vix': vix,
                'quotes': quotes,
            }

        if not by_time:
            continue

        year = date_iso[:4]
        half = half_of(date_iso)
        key = (year, half)
        by_half.setdefault(key, {'dates': [], 'by_date': {}})
        by_half[key]['dates'].append(date_iso)
        by_half[key]['by_date'][date_iso] = {'by_time': by_time}
        dates_seen.add(date_iso)

    # Write one gzipped file per (year, half)
    generated_at = datetime.now().isoformat()
    files_written = []
    total_quotes = 0
    for (year, half), data in sorted(by_half.items()):
        data['dates'].sort()
        data['year'] = year
        data['half'] = half
        data['times'] = TIMES
        data['generated_at'] = generated_at
        data['source'] = 'polygon_advanced_multi_time'

        filename = f'diagonal_real_{year}_{half}.json.gz'
        path = ROOT / 'data' / filename
        raw = json.dumps(data, separators=(',', ':')).encode('utf-8')
        with gzip.open(path, 'wb', compresslevel=9) as f:
            f.write(raw)

        gz_mb = path.stat().st_size / 1024 / 1024
        uncompressed_mb = len(raw) / 1024 / 1024
        day_quote_count = sum(
            len(ts['quotes'])
            for d in data['by_date'].values()
            for ts in d['by_time'].values()
        )
        total_quotes += day_quote_count
        print(f'  {filename}: {len(data["dates"])} dates, {day_quote_count:,} quotes '
              f'({uncompressed_mb:.1f} MB → {gz_mb:.1f} MB gz, '
              f'compression {uncompressed_mb/gz_mb:.1f}x)')
        files_written.append(filename)

    # Write index — list actual filenames so the browser doesn't need to know
    # anything about the halving scheme.
    index_path = ROOT / 'data' / 'diagonal_real_index.json'
    index_path.write_text(json.dumps({
        'files': files_written,
        'times': TIMES,
        'source': 'polygon_advanced_multi_time',
        'encoding': 'gzip',
        'generated_at': generated_at,
    }, separators=(',', ':')))
    print(f'  diagonal_real_index.json: {len(files_written)} files, times={TIMES}')

    # Delete legacy per-year and non-gz files so there's no ambiguity about
    # what's authoritative. Covers old whole-year output from previous runs.
    data_dir = ROOT / 'data'
    for legacy in list(data_dir.glob('diagonal_real_[0-9][0-9][0-9][0-9].json')) + \
                  list(data_dir.glob('diagonal_real_[0-9][0-9][0-9][0-9].json.gz')):
        legacy.unlink()
        print(f'  removed legacy {legacy.name}')

    legacy_single = data_dir / 'diagonal_real_data.json'
    if legacy_single.exists():
        legacy_single.unlink()
        print(f'  removed legacy {legacy_single.name}')

    print(f'Done. {len(dates_seen)} dates, {total_quotes:,} total quotes.')


if __name__ == '__main__':
    main()

"""
Merge per-day Polygon quote files (all 20 intraday timestamps) + VIX 1-min bars
into per-(half-year, time) gzipped JSON files that the diagonal.html
backtester consumes. Each file carries ONE timestamp's snapshot for half of a
year, so the browser can load only the two times the user selected (entry +
exit) instead of all 20.

Input:
  data/polygon/SPX_YYYYMMDD.json       — 14:00 default snapshot
  data/polygon/SPX_YYYYMMDD_HHMM.json  — intraday (other 19 timestamps)
  data/vix/VIX_YYYYMMDD.csv            — 1-min VIX bars (for exact-timestamp lookup)
  data/diagonal_bs_data.json           — legacy VIX snapshots (fallback)

Output:
  data/diagonal_real_YYYY_H{1,2}_t{HHMM}.json.gz — 7 halves × 20 times = 140 files
    {
      "year": "2024", "half": "H1", "time": "14:00",
      "dates": ["2024-01-02", ...],
      "by_date": {
        "2024-01-02": { "spot": 4739.34, "vix": 13.22, "quotes": {...} }
      }
    }
  data/diagonal_real_index.json — format, halves, times, file template:
    {
      "format": "per_time_v2",
      "halves": ["2023_H1", "2023_H2", ...],
      "times": ["09:45", "10:00", ..., "15:45"],
      "file_template": "diagonal_real_{half}_t{time_compact}.json.gz"
    }

Why per-(half, time) sharding:
  • Old layout (per-half, all 20 times in one file) made the browser download
    ~329 MB gz / 3.1 GB decompressed on every page load, even though only two
    timestamps (entry + exit) are actually read. Load times were 60+ seconds.
  • New layout: initial load fetches 7 halves × 2 times = 14 files ≈ 32 MB gz.
    Changing the time selector lazy-loads 7 more files on demand (~17 MB).
  • Per-file size: ~2.4 MB gz / ~20 MB decompressed. Well under GitHub's 100 MB
    per-file limit and V8's ~512 MB max string length.
"""
from __future__ import annotations

import json
import gzip
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent
POLYGON_DIR = ROOT / 'data' / 'polygon'
VIX_DIR = ROOT / 'data' / 'vix'
BS_DATA = ROOT / 'data' / 'diagonal_bs_data.json'
DATA_DIR = ROOT / 'data'

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

    # Bucket snapshots by (year, half, time) → list of (date, payload) records
    # payload = {spot, vix, quotes}
    # After assembly we write one gz file per (year, half, time).
    buckets: dict[tuple[str, str, str], dict] = {}
    dates_seen: set[str] = set()

    for date_iso in bs['dates']:
        bs_entry = bs['by_date'].get(date_iso) or {}
        year = date_iso[:4]
        half = half_of(date_iso)

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

            vix = vix_from_csv(date_iso, hhmm)
            if vix is None:
                if hhmm == '12:45':
                    vix = bs_entry.get('vix_12') or bs_entry.get('vix_14')
                else:
                    vix = bs_entry.get('vix_14') or bs_entry.get('vix_12')

            key = (year, half, hhmm)
            buckets.setdefault(key, {'dates': [], 'by_date': {}})
            buckets[key]['dates'].append(date_iso)
            buckets[key]['by_date'][date_iso] = {
                'spot': spot,
                'vix': vix,
                'quotes': quotes,
            }
            dates_seen.add(date_iso)

    # Write one gzipped file per (year, half, time)
    generated_at = datetime.now().isoformat()
    halves_seen: list[str] = []
    times_seen: set[str] = set()
    files_written = 0
    total_quotes = 0
    total_gz_bytes = 0

    for (year, half, hhmm), payload in sorted(buckets.items()):
        half_key = f'{year}_{half}'
        if half_key not in halves_seen:
            halves_seen.append(half_key)
        times_seen.add(hhmm)

        payload['dates'].sort()
        payload['year'] = year
        payload['half'] = half
        payload['time'] = hhmm
        payload['generated_at'] = generated_at
        payload['source'] = 'polygon_advanced_multi_time'

        time_compact = hhmm.replace(':', '')
        filename = f'diagonal_real_{half_key}_t{time_compact}.json.gz'
        path = DATA_DIR / filename
        raw = json.dumps(payload, separators=(',', ':')).encode('utf-8')
        with gzip.open(path, 'wb', compresslevel=9) as f:
            f.write(raw)

        gz_bytes = path.stat().st_size
        total_gz_bytes += gz_bytes
        q = sum(len(v['quotes']) for v in payload['by_date'].values())
        total_quotes += q
        files_written += 1
        if files_written % 20 == 0 or files_written <= 3:
            print(f'  [{files_written}] {filename}: '
                  f'{len(payload["dates"])} dates, {q:,} quotes, '
                  f'{gz_bytes/1024/1024:.2f} MB gz')

    # Write index — halves + times + template so the browser can derive the
    # per-file URLs without listing all 140 filenames.
    times_sorted = sorted(times_seen)
    index_path = DATA_DIR / 'diagonal_real_index.json'
    index_path.write_text(json.dumps({
        'format': 'per_time_v2',
        'halves': halves_seen,
        'times': times_sorted,
        'file_template': 'diagonal_real_{half}_t{time_compact}.json.gz',
        'source': 'polygon_advanced_multi_time',
        'encoding': 'gzip',
        'generated_at': generated_at,
    }, separators=(',', ':')))

    # Remove legacy all-times-per-half and per-year files.
    for legacy in (
        list(DATA_DIR.glob('diagonal_real_[0-9][0-9][0-9][0-9].json')) +
        list(DATA_DIR.glob('diagonal_real_[0-9][0-9][0-9][0-9].json.gz')) +
        list(DATA_DIR.glob('diagonal_real_[0-9][0-9][0-9][0-9]_H[12].json.gz'))
    ):
        legacy.unlink()
        print(f'  removed legacy {legacy.name}')

    legacy_single = DATA_DIR / 'diagonal_real_data.json'
    if legacy_single.exists():
        legacy_single.unlink()
        print(f'  removed legacy {legacy_single.name}')

    print(f'\nWrote {files_written} files, {total_gz_bytes/1024/1024:.1f} MB gz total')
    print(f'{len(halves_seen)} halves × {len(times_sorted)} times')
    print(f'{len(dates_seen)} dates, {total_quotes:,} total quotes')


if __name__ == '__main__':
    main()

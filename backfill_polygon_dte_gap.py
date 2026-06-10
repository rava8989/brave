"""
Targeted backfill: fetch DTE 8-11 SPXW put quotes for every existing
`data/polygon/SPX_*.json` snapshot and MERGE into the existing file.

Why this script exists:
  The original scraper (`fetch_polygon_spx.py`) split fetches into
  SHORT [0,7] and LONG [12,35], leaving DTE 8-11 uncovered. A diagonal
  with longDte=12 lands in that gap on EXIT day (12→11 DTE overnight),
  losing the real quote and forcing a Black-Scholes fallback.

What this script does:
  For each `data/polygon/SPX_YYYYMMDD[_HHMM].json`:
    1. Parse (date, time) from filename (or `target_time_et` in the file).
    2. Fetch the union of DTE 8-11 SPXW put contracts (one paginated call
       per date, reused across all 20 time buckets of that date).
    3. Fetch per-ticker NBBO quote at the target timestamp.
    4. MERGE into existing `quotes` dict — additive only; never overwrite
       a ticker that already has a quote.
    5. Write the merged file back.

Safety:
  - Additive merge: existing DTE [0,7] and DTE [12,35] quotes are preserved.
  - Skips dates/files that already contain DTE 8-11 coverage (idempotent).
  - `--dry-run` prints what would be added without writing.
  - `--date YYYY-MM-DD` for single-date test runs.

After running, rebuild gzipped bundles:
  python3 build_diagonal_real_data.py
"""
from __future__ import annotations
import os
import sys
import json
import time
import argparse
from pathlib import Path
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ── HTTP session (matches fetch_polygon_spx.py) ───────────────────────
_retry = Retry(
    total=5, connect=5, read=5,
    backoff_factor=0.5,
    status_forcelist=(429, 500, 502, 503, 504),
    raise_on_status=False,
)
_adapter = HTTPAdapter(pool_connections=64, pool_maxsize=256, max_retries=_retry)
SESSION = requests.Session()
SESSION.mount('https://', _adapter)
SESSION.mount('http://', _adapter)
HTTP_TIMEOUT = 30


# ── Env / paths ───────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent
for line in (ROOT / '.env').read_text().splitlines():
    if '=' in line and not line.strip().startswith('#'):
        k, v = line.split('=', 1)
        os.environ[k.strip()] = v.strip()

API = os.environ['POLYGON_API_KEY']
BASE = 'https://api.polygon.io'
POLYGON_DIR = ROOT / 'data' / 'polygon'


# ── Gap-fill window ───────────────────────────────────────────────────
# DTE 8-11 = the gap between SHORT_DTE_MAX=7 and old LONG_DTE_MIN=12.
DTE_MIN = 8
DTE_MAX = 11
# Match the strike window used by fetch_polygon_spx.py (±300pt around spot).
STRIKE_OFFSET = 300


# ── Helpers ───────────────────────────────────────────────────────────
def et_to_utc_ns(date_iso: str, hhmm: str) -> int:
    from zoneinfo import ZoneInfo
    et = ZoneInfo('America/New_York')
    dt = datetime.fromisoformat(date_iso + 'T' + hhmm + ':00').replace(tzinfo=et)
    return int(dt.astimezone(timezone.utc).timestamp() * 1e9)


def list_spxw_puts(as_of_date: str, dte_min: int, dte_max: int,
                   strike_min: float, strike_max: float):
    """Same logic as fetch_polygon_spx.list_spxw_puts — SPXW only, dedup by ticker."""
    from datetime import date as _d
    d0 = _d.fromisoformat(as_of_date)
    exp_gte = (d0 + timedelta(days=dte_min)).isoformat()
    exp_lte = (d0 + timedelta(days=dte_max)).isoformat()

    contracts = {}
    base_params = {
        'underlying_ticker': 'SPX',
        'contract_type': 'put',
        'expiration_date.gte': exp_gte,
        'expiration_date.lte': exp_lte,
        'strike_price.gte': strike_min,
        'strike_price.lte': strike_max,
        'limit': 1000,
        'apiKey': API,
    }
    for expired_flag in ('false', 'true'):
        url = f'{BASE}/v3/reference/options/contracts'
        params = {**base_params, 'expired': expired_flag}
        while True:
            r = SESSION.get(url, params=params, timeout=HTTP_TIMEOUT)
            data = r.json()
            for c in data.get('results') or []:
                if c['ticker'].startswith('O:SPXW'):
                    contracts[c['ticker']] = {
                        'ticker': c['ticker'],
                        'strike': c['strike_price'],
                        'expiration': c['expiration_date'],
                    }
            next_url = data.get('next_url')
            if not next_url:
                break
            url = next_url
            params = {'apiKey': API}
    return list(contracts.values())


def fetch_quote_at(ticker: str, ts_ns: int):
    try:
        r = SESSION.get(f'{BASE}/v3/quotes/{ticker}', params={
            'timestamp.lte': ts_ns,
            'order': 'desc',
            'limit': 1,
            'apiKey': API,
        }, timeout=HTTP_TIMEOUT)
        results = r.json().get('results') or []
    except Exception:
        return None
    if not results:
        return None
    q = results[0]
    return {
        'bid': q.get('bid_price'),
        'ask': q.get('ask_price'),
        'ts_ns': q.get('sip_timestamp'),
    }


def parse_filename(path: Path):
    """SPX_YYYYMMDD.json → ('YYYY-MM-DD', None)
    SPX_YYYYMMDD_HHMM.json → ('YYYY-MM-DD', 'HH:MM')"""
    stem = path.stem
    parts = stem.split('_')
    if len(parts) == 2:
        date_raw = parts[1]
        hhmm_str = None
    elif len(parts) == 3:
        date_raw = parts[1]
        t = parts[2]
        if len(t) != 4:
            return None, None
        hhmm_str = f'{t[:2]}:{t[2:]}'
    else:
        return None, None
    if len(date_raw) != 8 or not date_raw.isdigit():
        return None, None
    return f'{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:]}', hhmm_str


# ── Per-date worker ───────────────────────────────────────────────────
def process_date(date_iso: str, files: list[tuple[str | None, Path]],
                 dry_run: bool = False):
    """Process every snapshot for this date: one contract listing + parallel quotes."""
    # Load every file for this date
    loaded = []
    for hhmm_str, path in files:
        try:
            d = json.load(open(path))
        except Exception as e:
            continue
        loaded.append((hhmm_str, path, d))
    if not loaded:
        return f'{date_iso}: no loadable files'

    # Pick a representative spot — average of all per-time spots
    spots = [d.get('spot') for _, _, d in loaded if d.get('spot') is not None]
    if not spots:
        return f'{date_iso}: no spot in any file'
    avg_spot = sum(spots) / len(spots)
    strike_min = avg_spot - STRIKE_OFFSET
    strike_max = avg_spot + STRIKE_OFFSET

    # ONE contract listing for the date (reused across all time buckets)
    try:
        contracts = list_spxw_puts(date_iso, DTE_MIN, DTE_MAX, strike_min, strike_max)
    except Exception as e:
        return f'{date_iso}: contracts error: {e}'
    if not contracts:
        return f'{date_iso}: no DTE {DTE_MIN}-{DTE_MAX} contracts listed'

    added_total = 0
    by_time_added = []
    for hhmm_str, path, existing in loaded:
        existing_quotes = existing.setdefault('quotes', {})

        # Determine target timestamp
        if hhmm_str is not None:
            target_hhmm = hhmm_str
        else:
            target_hhmm = existing.get('target_time_et') or '14:00'

        try:
            ts_ns = et_to_utc_ns(date_iso, target_hhmm)
        except Exception:
            continue

        # Skip tickers we already have
        missing = [c for c in contracts if c['ticker'] not in existing_quotes]
        if not missing:
            by_time_added.append((target_hhmm, 0, 'already_covered'))
            continue

        # Fetch quotes in parallel
        def fetch_one(c):
            q = fetch_quote_at(c['ticker'], ts_ns)
            if q and q['bid'] is not None and q['ask'] is not None:
                return c['ticker'], {
                    'strike': c['strike'],
                    'expiration': c['expiration'],
                    'bid': q['bid'],
                    'ask': q['ask'],
                }
            return None

        added = 0
        with ThreadPoolExecutor(max_workers=32) as ex:
            for result in ex.map(fetch_one, missing):
                if result:
                    ticker, q = result
                    existing_quotes[ticker] = q
                    added += 1

        if added and not dry_run:
            existing['quotes_count'] = len(existing_quotes)
            path.write_text(json.dumps(existing, separators=(',', ':')))
        added_total += added
        by_time_added.append((target_hhmm, added, f'{len(missing)} missing, {added} added'))

    tag = ' [DRY]' if dry_run else ''
    return f'{date_iso}: +{added_total} quotes across {len(loaded)} time buckets{tag}'


# ── Main ──────────────────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser()
    p.add_argument('--date', help='Single date YYYY-MM-DD')
    p.add_argument('--from', dest='from_date')
    p.add_argument('--to', dest='to_date')
    p.add_argument('--dry-run', action='store_true')
    p.add_argument('--parallel-days', type=int, default=8)
    args = p.parse_args()

    # Discover & group existing snapshot files by date
    files_by_date: dict[str, list] = defaultdict(list)
    for path in sorted(POLYGON_DIR.glob('SPX_*.json')):
        date_iso, hhmm_str = parse_filename(path)
        if not date_iso:
            continue
        if args.date and date_iso != args.date:
            continue
        if args.from_date and date_iso < args.from_date:
            continue
        if args.to_date and date_iso > args.to_date:
            continue
        files_by_date[date_iso].append((hhmm_str, path))

    if not files_by_date:
        print('No files matched filters.')
        return

    total_files = sum(len(v) for v in files_by_date.values())
    tag = ' [DRY RUN]' if args.dry_run else ''
    print(f'Backfilling DTE {DTE_MIN}-{DTE_MAX} for {len(files_by_date)} dates '
          f'({total_files} files){tag}')

    t0 = time.time()
    completed = 0
    total_added = 0
    with ThreadPoolExecutor(max_workers=args.parallel_days) as ex:
        futures = {
            ex.submit(process_date, d, f, args.dry_run): d
            for d, f in files_by_date.items()
        }
        for fut in as_completed(futures):
            try:
                msg = fut.result()
            except Exception as e:
                msg = f'{futures[fut]}: ERROR {e}'
            completed += 1
            if '+' in msg:
                # parse "+N quotes"
                try:
                    added = int(msg.split('+')[1].split(' ')[0])
                    total_added += added
                except Exception:
                    pass
            elapsed = time.time() - t0
            rate = completed / elapsed if elapsed > 0 else 0
            eta_min = (len(files_by_date) - completed) / rate / 60 if rate > 0 else 0
            sys.stdout.write(
                f'[{completed}/{len(files_by_date)}] {msg}  '
                f'| rate={rate:.1f}d/s ETA={eta_min:.1f}min  total_added={total_added}\n'
            )
            sys.stdout.flush()

    print(f'\nDone in {time.time() - t0:.1f}s. Added {total_added} quotes total.')


if __name__ == '__main__':
    main()

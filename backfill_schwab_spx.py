"""
Backfill missing SPX/VIX 1-min CSV files via the Schwab proxy worker.

Covers the gap from 2026-03-18 (last local file) to today.
Outputs in same format as existing data/spx/SPX_YYYYMMDD.csv files:
  timestamp,open,high,low,close

Usage:
  python3 backfill_schwab_spx.py                         # auto-detect gap
  python3 backfill_schwab_spx.py --from 2026-03-18       # from date
  python3 backfill_schwab_spx.py --from 2026-03-18 --to 2026-04-17
"""
import os
import sys
import csv
import argparse
import requests
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SPX_DIR = ROOT / 'data' / 'spx'
VIX_DIR = ROOT / 'data' / 'vix'
SPX_DIR.mkdir(parents=True, exist_ok=True)
VIX_DIR.mkdir(parents=True, exist_ok=True)

WORKER = 'https://schwab-proxy.ravamt4.workers.dev'
ET = ZoneInfo('America/New_York')

# US holidays 2026 (market closed) — minimal set, expand if needed
HOLIDAYS_2026 = {
    '2026-01-01', '2026-01-19', '2026-02-16', '2026-04-03',  # New Year, MLK, Presidents, Good Friday
    '2026-05-25', '2026-06-19', '2026-07-03', '2026-09-07',  # Memorial, Juneteenth, Jul 3 (Jul 4 Sat), Labor
    '2026-11-26', '2026-12-25',  # Thanksgiving, Christmas
}


def fetch_day(symbol: str, d: date) -> list:
    """Fetch 1-min bars for symbol on date d. Returns list of candles."""
    start = datetime(d.year, d.month, d.day, 9, 30, 0, tzinfo=ET)
    end   = datetime(d.year, d.month, d.day, 16, 0, 0, tzinfo=ET)
    start_ms = int(start.timestamp() * 1000)
    end_ms   = int(end.timestamp() * 1000)

    sym_enc = symbol.replace('$', '%24')
    url = f'{WORKER}/market/pricehistory?symbol={sym_enc}&periodType=day&period=1&frequencyType=minute&frequency=1&startDate={start_ms}&endDate={end_ms}'
    r = requests.get(url, headers={'Origin': 'http://localhost:8080'}, timeout=30)
    r.raise_for_status()
    return r.json().get('candles') or []


def save_csv(path: Path, candles: list):
    """Write candles to CSV in the project's standard format."""
    rows = []
    for c in candles:
        ts = datetime.fromtimestamp(c['datetime'] / 1000, tz=ET)
        rows.append([
            ts.strftime('%Y-%m-%d %H:%M:%S'),
            c['open'], c['high'], c['low'], c['close'],
        ])
    with path.open('w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['timestamp', 'open', 'high', 'low', 'close'])
        w.writerows(rows)


def iter_trading_days(d0: date, d1: date):
    d = d0
    while d <= d1:
        if d.weekday() < 5 and d.isoformat() not in HOLIDAYS_2026:
            yield d
        d += timedelta(days=1)


def find_last_date(folder: Path, prefix: str):
    files = sorted(folder.glob(f'{prefix}_*.csv'))
    if not files:
        return None
    last = files[-1].stem.replace(f'{prefix}_', '')
    return date.fromisoformat(f'{last[:4]}-{last[4:6]}-{last[6:8]}')


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--from', dest='from_date')
    p.add_argument('--to', dest='to_date')
    args = p.parse_args()

    if args.from_date:
        d0 = date.fromisoformat(args.from_date)
    else:
        # auto-detect gap: next day after the latest SPX file
        last = find_last_date(SPX_DIR, 'SPX')
        if last is None:
            print('No existing SPX data found. Use --from.')
            sys.exit(1)
        d0 = last + timedelta(days=1)
    d1 = date.fromisoformat(args.to_date) if args.to_date else date.today()

    print(f'Backfilling SPX + VIX from {d0} to {d1}')
    days = list(iter_trading_days(d0, d1))
    print(f'Trading days to fetch: {len(days)}')

    for d in days:
        date_iso = d.isoformat()
        yyyymmdd = d.strftime('%Y%m%d')

        for symbol, folder, prefix in [('$SPX', SPX_DIR, 'SPX'), ('$VIX', VIX_DIR, 'VIX')]:
            out = folder / f'{prefix}_{yyyymmdd}.csv'
            if out.exists():
                continue
            try:
                candles = fetch_day(symbol, d)
                if not candles:
                    print(f'  {date_iso} {symbol}: NO DATA (market closed?)')
                    continue
                save_csv(out, candles)
                print(f'  {date_iso} {symbol}: {len(candles)} bars → {out.name}')
            except Exception as e:
                print(f'  {date_iso} {symbol}: ERROR {e}', file=sys.stderr)

    print('Done.')


if __name__ == '__main__':
    main()

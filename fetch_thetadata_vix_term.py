#!/usr/bin/env python3
"""
VIX Term-Structure Fetcher (ThetaData)

Pulls daily closes for the VIX volatility surface from ThetaData and writes
them to data/vix_term/daily.csv:

  - VIX9D   (9-day implied vol, front of curve)
  - VIX     (30-day, standard)
  - VIX3M   (3-month, mid curve)
  - VIX6M   (6-month, back of curve)
  - VVIX    (vol-of-vol)

These are the ingredients for the contango/backwardation regime classifier.
ThetaData v3 doesn't have a "daily" interval for index endpoints, so we pull
hourly bars and take the last bar of each day as the daily close.

Output CSV columns:
  date, vix9d, vix, vix3m, vix6m, vvix
  vix_vix3m_ratio   = vix / vix3m       (<1 = contango,  >1 = backwardation)
  vix9d_vix_ratio   = vix9d / vix       (<1 = front contango, >1 = front backwardation)

Usage:
  python3 fetch_thetadata_vix_term.py                          # full range
  python3 fetch_thetadata_vix_term.py --from 2025-01-01 --to 2026-06-08
"""
from __future__ import annotations
import argparse, csv, sys, time
from datetime import date, datetime, timedelta
from pathlib import Path
from collections import defaultdict
import requests

ROOT = Path(__file__).resolve().parent
OUT_DIR = ROOT / 'data' / 'vix_term'
OUT_DIR.mkdir(parents=True, exist_ok=True)
THETA = 'http://localhost:25503/v3'

SYMBOLS = ['VIX9D', 'VIX', 'VIX3M', 'VIX6M', 'VVIX']

US_HOLIDAYS = {
    '2023-01-02','2023-01-16','2023-02-20','2023-04-07','2023-05-29',
    '2023-06-19','2023-07-04','2023-09-04','2023-11-23','2023-12-25',
    '2024-01-01','2024-01-15','2024-02-19','2024-03-29','2024-05-27',
    '2024-06-19','2024-07-04','2024-09-02','2024-11-28','2024-12-25',
    '2025-01-01','2025-01-09','2025-01-20','2025-02-17','2025-04-18','2025-05-26',
    '2025-06-19','2025-07-04','2025-09-01','2025-11-27','2025-12-25',
    '2026-01-01','2026-01-19','2026-02-16','2026-04-03','2026-05-25',
    '2026-06-19','2026-07-03','2026-09-07','2026-11-26','2026-12-25',
}

SESSION = requests.Session()


def trading_dates(start_iso: str, end_iso: str) -> list[str]:
    s = date.fromisoformat(start_iso)
    e = date.fromisoformat(end_iso)
    out = []
    while s <= e:
        iso = s.isoformat()
        if s.weekday() < 5 and iso not in US_HOLIDAYS:
            out.append(iso)
        s += timedelta(days=1)
    return out


def fetch_hourly(symbol: str, start_iso: str, end_iso: str) -> dict[str, float]:
    """Fetch hourly bars for a symbol; return {date: last_hourly_close}."""
    sd = start_iso.replace('-', '')
    ed = end_iso.replace('-', '')
    url = f'{THETA}/index/history/ohlc'
    params = {'symbol': symbol, 'start_date': sd, 'end_date': ed,
              'interval': '1h', 'format': 'csv'}
    r = SESSION.get(url, params=params, timeout=60)
    if r.status_code != 200:
        print(f'  [WARN] {symbol}: HTTP {r.status_code}: {r.text[:200]}', file=sys.stderr)
        return {}
    last_close: dict[str, float] = {}
    lines = r.text.strip().split('\n')
    if not lines or lines[0].startswith('Invalid') or lines[0].startswith('<'):
        return {}
    # Parse CSV
    reader = csv.DictReader(lines)
    for row in reader:
        ts = row.get('timestamp', '')
        if len(ts) < 10:
            continue
        d = ts[:10]
        try:
            close = float(row.get('close', 0))
        except ValueError:
            continue
        if close <= 0:
            continue
        # Keep updating — last write wins = last bar of day
        last_close[d] = close
    return last_close


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--from', dest='from_date', default='2023-06-01')
    p.add_argument('--to', dest='to_date', default=date.today().isoformat())
    p.add_argument('--out', default=str(OUT_DIR / 'daily.csv'))
    args = p.parse_args()

    print(f'Fetching VIX term-structure {args.from_date} → {args.to_date}')
    print(f'Symbols: {", ".join(SYMBOLS)}')

    # Fetch each symbol in year-chunks to avoid hitting ThetaData limits.
    by_symbol_by_date: dict[str, dict[str, float]] = {s: {} for s in SYMBOLS}
    start = date.fromisoformat(args.from_date)
    end = date.fromisoformat(args.to_date)

    while start <= end:
        chunk_end = min(start.replace(year=start.year + 1) - timedelta(days=1), end)
        chunk_start_iso = start.isoformat()
        chunk_end_iso = chunk_end.isoformat()
        print(f'  Chunk {chunk_start_iso} → {chunk_end_iso}')
        for sym in SYMBOLS:
            closes = fetch_hourly(sym, chunk_start_iso, chunk_end_iso)
            by_symbol_by_date[sym].update(closes)
            print(f'    {sym}: +{len(closes)} days')
            time.sleep(0.05)
        start = chunk_end + timedelta(days=1)

    # Union of dates across all symbols
    all_dates = sorted(set().union(*[set(d.keys()) for d in by_symbol_by_date.values()]))
    print(f'\nUnion of dates: {len(all_dates)} | {all_dates[0]} → {all_dates[-1]}')

    # Write CSV
    out_path = Path(args.out)
    with open(out_path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['date', 'vix9d', 'vix', 'vix3m', 'vix6m', 'vvix',
                    'vix_vix3m_ratio', 'vix9d_vix_ratio'])
        for d in all_dates:
            row = [d]
            v9 = by_symbol_by_date['VIX9D'].get(d)
            vx = by_symbol_by_date['VIX'].get(d)
            v3 = by_symbol_by_date['VIX3M'].get(d)
            v6 = by_symbol_by_date['VIX6M'].get(d)
            vvix = by_symbol_by_date['VVIX'].get(d)
            for val in [v9, vx, v3, v6, vvix]:
                row.append(f'{val:.4f}' if val is not None else '')
            r1 = (vx / v3) if (vx and v3) else None
            r2 = (v9 / vx) if (v9 and vx) else None
            row.append(f'{r1:.4f}' if r1 else '')
            row.append(f'{r2:.4f}' if r2 else '')
            w.writerow(row)

    print(f'\nWrote {len(all_dates)} rows → {out_path}')
    # Quick sanity summary
    last = all_dates[-1]
    print(f'\nLatest day ({last}):')
    for s in SYMBOLS:
        v = by_symbol_by_date[s].get(last)
        print(f'  {s:>7}  {v if v else "missing"}')


if __name__ == '__main__':
    main()

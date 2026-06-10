#!/usr/bin/env python3
"""
ThetaData-based diagonal data fetcher — drop-in replacement for fetch_polygon_spx.py.

Produces JSON files in `data/polygon/` matching the existing format that
build_diagonal_data.py and build_diagonal_real_data.py expect:

{
  "date": "2026-06-04",
  "target_time_et": "14:00",
  "spot": 7527.07,
  "strike_range": [7227.07, 7827.07],
  "quotes_count": N,
  "quotes": {
    "O:SPXW260605P07300000": {
      "strike": 7300.0,
      "expiration": "2026-06-05",
      "bid": 2.10,
      "ask": 2.30
    },
    ...
  }
}

Output filename:
  - Default time (14:00 / 12:45 half-day) → SPX_YYYYMMDD.json
  - Specific --time HH:MM → SPX_YYYYMMDD_HHMM.json

Usage:
  python3 fetch_thetadata_diagonal.py --date 2026-06-04                # one date, 14:00
  python3 fetch_thetadata_diagonal.py --from 2026-05-26 --to 2026-06-04
  python3 fetch_thetadata_diagonal.py --time 12:30 --from 2026-05-26 --to 2026-06-04
"""
from __future__ import annotations
import argparse, csv, json, os, sys
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

ROOT = Path(__file__).resolve().parent
SPX_DIR = ROOT / 'data' / 'spx'
POLY_DIR = ROOT / 'data' / 'polygon'
POLY_DIR.mkdir(parents=True, exist_ok=True)
THETA = 'http://localhost:25503/v3'

SESSION = requests.Session()
SESSION.mount('http://', requests.adapters.HTTPAdapter(pool_maxsize=64))

HALF_DAYS = {
    '2024-07-03', '2024-11-29', '2024-12-24',
    '2025-07-03', '2025-11-28', '2025-12-24',
    '2026-07-02', '2026-11-27', '2026-12-24',
}
US_HOLIDAYS = {
    '2024-09-02','2024-11-28','2024-12-25',
    '2025-01-01','2025-01-09','2025-01-20','2025-02-17','2025-04-18','2025-05-26',
    '2025-06-19','2025-07-04','2025-09-01','2025-11-27','2025-12-25',
    '2026-01-01','2026-01-19','2026-02-16','2026-04-03','2026-05-25',
    '2026-06-19','2026-07-03','2026-09-07','2026-11-26','2026-12-25',
}


def default_time(date_iso: str) -> str:
    return '12:45' if date_iso in HALF_DAYS else '14:00'


def read_spot_at(date_iso: str, time_hhmm: str) -> float | None:
    """Read SPX spot at target time from data/spx/SPX_YYYYMMDD.csv."""
    path = SPX_DIR / f"SPX_{date_iso.replace('-','')}.csv"
    if not path.exists(): return None
    target_dt = f'{date_iso} {time_hhmm}:00'
    with open(path) as f:
        for row in csv.DictReader(f):
            if row['timestamp'] >= target_dt:
                return float(row['close'])
    # If we ran out before target, return last close
    return None


def list_expirations(target_date_iso: str, max_dte: int = 35) -> list[str]:
    """Return SPXW expirations within (today, today+max_dte] days."""
    r = SESSION.get(f'{THETA}/option/list/expirations', params={'symbol':'SPXW'}, timeout=30)
    if r.status_code != 200: return []
    target = datetime.strptime(target_date_iso, '%Y-%m-%d').date()
    out = []
    for ln in r.text.strip().split('\n')[1:]:
        cols = [c.strip('"') for c in ln.split(',')]
        if len(cols) >= 2:
            try:
                exp = datetime.strptime(cols[1], '%Y-%m-%d').date()
                dte = (exp - target).days
                if 0 <= dte <= max_dte:
                    out.append(cols[1])
            except: continue
    return sorted(out)


def fetch_put_quote(date_iso: str, exp_iso: str, strike: int, hhmm: str) -> tuple[float, float] | None:
    """Fetch a single put quote (bid, ask) at HH:MM on date_iso for given strike/expiration."""
    dc = date_iso.replace('-','')
    edc = exp_iso.replace('-','')
    params = {
        'symbol':'SPXW', 'expiration':edc, 'strike':f'{strike:.3f}',
        'right':'put', 'date':dc, 'interval':'1m', 'format':'csv',
    }
    try:
        r = SESSION.get(f'{THETA}/option/history/quote', params=params, timeout=15)
    except Exception:
        return None
    if r.status_code != 200: return None
    target = f'T{hhmm}:00'
    for ln in r.text.strip().split('\n')[1:]:
        cols = [c.strip('"') for c in ln.split(',')]
        if len(cols) < 12: continue
        if target in cols[4]:
            try:
                bid, ask = float(cols[7]), float(cols[11])
                if bid <= 0 and ask <= 0: return None
                return (bid, ask)
            except: continue
    return None


def fetch_for_date(date_iso: str, time_hhmm: str, strike_radius: int = 300, strike_step: int = 5,
                    max_dte: int = 35, workers: int = 24) -> dict:
    """Build a Polygon-compatible snapshot for one date+time."""
    spot = read_spot_at(date_iso, time_hhmm)
    if spot is None:
        return {'date': date_iso, 'target_time_et': time_hhmm,
                'spot': None, 'error': 'no spot data'}

    lo, hi = spot - strike_radius, spot + strike_radius
    strikes = list(range(int(lo // strike_step) * strike_step,
                          int(hi // strike_step) * strike_step + strike_step,
                          strike_step))
    expirations = list_expirations(date_iso, max_dte=max_dte)

    tasks = [(exp, K) for exp in expirations for K in strikes]
    quotes = {}

    def work(exp_K):
        exp, K = exp_K
        q = fetch_put_quote(date_iso, exp, K, time_hhmm)
        if q is None: return None
        bid, ask = q
        key = f'O:SPXW{exp.replace("-","")[2:]}P{K*1000:08d}'
        return key, {'strike': float(K), 'expiration': exp, 'bid': bid, 'ask': ask}

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for f in as_completed([ex.submit(work, t) for t in tasks]):
            r = f.result()
            if r: quotes[r[0]] = r[1]

    return {
        'date': date_iso,
        'target_time_et': time_hhmm,
        'spot': round(spot, 2),
        'strike_range': [round(lo, 2), round(hi, 2)],
        'quotes_count': len(quotes),
        'quotes': quotes,
    }


def output_path(date_iso: str, time_hhmm: str, default_t: str) -> Path:
    """SPX_YYYYMMDD.json for default time, SPX_YYYYMMDD_HHMM.json otherwise."""
    yyyymmdd = date_iso.replace('-','')
    if time_hhmm == default_t:
        return POLY_DIR / f'SPX_{yyyymmdd}.json'
    return POLY_DIR / f'SPX_{yyyymmdd}_{time_hhmm.replace(":","")}.json'


def daterange(start_iso: str, end_iso: str):
    s = datetime.strptime(start_iso, '%Y-%m-%d').date()
    e = datetime.strptime(end_iso, '%Y-%m-%d').date()
    while s <= e:
        iso = s.isoformat()
        if s.weekday() < 5 and iso not in US_HOLIDAYS:
            yield iso
        s += timedelta(days=1)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--date', help='single date YYYY-MM-DD')
    p.add_argument('--from', dest='from_date')
    p.add_argument('--to', dest='to_date')
    p.add_argument('--time', help='specific time HH:MM (default = 14:00 / 12:45 half-day)')
    p.add_argument('--radius', type=int, default=300, help='strike radius from spot (pts)')
    p.add_argument('--step', type=int, default=5, help='strike step (pts)')
    p.add_argument('--max-dte', type=int, default=35, help='max DTE for expirations included')
    args = p.parse_args()

    if args.date:
        dates = [args.date]
    elif args.from_date and args.to_date:
        dates = list(daterange(args.from_date, args.to_date))
    else:
        print('Need --date OR --from and --to', file=sys.stderr); sys.exit(2)

    print(f'Processing {len(dates)} date(s)...')
    for i, d in enumerate(dates, 1):
        default_t = default_time(d)
        time_hhmm = args.time or default_t
        if args.time and d in HALF_DAYS:
            # Skip if user-requested time > 12:45 on a half-day
            h, m = map(int, args.time.split(':'))
            if (h, m) > (12, 45):
                print(f'  [{i}/{len(dates)}] {d}: SKIP (half-day, market closed at 13:00 ET)')
                continue
        out = output_path(d, time_hhmm, default_t)
        if out.exists() and out.stat().st_size > 1000:
            print(f'  [{i}/{len(dates)}] {d}: SKIP (already exists: {out.name})')
            continue
        print(f'  [{i}/{len(dates)}] {d} @ {time_hhmm} → fetching...', flush=True)
        snap = fetch_for_date(d, time_hhmm,
                              strike_radius=args.radius, strike_step=args.step,
                              max_dte=args.max_dte)
        out.write_text(json.dumps(snap, indent=2))
        print(f'     spot={snap.get("spot")}  quotes={snap.get("quotes_count", 0)}  → {out.name}')


if __name__ == '__main__':
    main()

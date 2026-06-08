#!/usr/bin/env python3
"""
VIX Term-Structure Fetcher (ThetaData)

Pulls hourly bars for VIX9D / VIX / VIX3M / VIX6M / VVIX and produces two
values per day so the backtester can run with NO look-ahead:

  • close = last hourly bar of the day (EOD)         — used for "yesterday's data"
  • open  = OPEN of the 9:30 ET hourly bar           — what we'd actually see live
                                                       at 9:30 AM on trade day

Output CSV columns:
  date,
  vix9d_open, vix9d_close,
  vix_open, vix_close,
  vix3m_open, vix3m_close,
  vix6m_open, vix6m_close,
  vvix_open, vvix_close,
  vix_vix3m_ratio_open,   vix_vix3m_ratio_close,
  vix9d_vix_ratio_open,   vix9d_vix_ratio_close

The _open columns are the live-correct 9:30 ET prints; the _close columns are
the EOD prints (kept for any forensic / EOD-cross analysis).

Usage:
  python3 fetch_thetadata_vix_term.py
  python3 fetch_thetadata_vix_term.py --from 2025-01-01 --to 2026-06-08
"""
from __future__ import annotations
import argparse, csv, sys, time
from datetime import date, datetime, timedelta
from pathlib import Path
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


def fetch_hourly(symbol: str, start_iso: str, end_iso: str) -> dict[str, dict]:
    """Fetch hourly bars; return {date: {'open_930': float, 'close_eod': float}}."""
    sd = start_iso.replace('-', '')
    ed = end_iso.replace('-', '')
    url = f'{THETA}/index/history/ohlc'
    params = {'symbol': symbol, 'start_date': sd, 'end_date': ed,
              'interval': '1h', 'format': 'csv'}
    r = SESSION.get(url, params=params, timeout=60)
    if r.status_code != 200:
        print(f'  [WARN] {symbol}: HTTP {r.status_code}: {r.text[:200]}', file=sys.stderr)
        return {}
    lines = r.text.strip().split('\n')
    if not lines or lines[0].startswith('Invalid') or lines[0].startswith('<'):
        return {}

    by_date: dict[str, dict] = {}
    reader = csv.DictReader(lines)
    for row in reader:
        ts = row.get('timestamp', '')
        if len(ts) < 16:
            continue
        d = ts[:10]
        hhmm = ts[11:16]
        try:
            o = float(row.get('open', 0))
            c = float(row.get('close', 0))
        except ValueError:
            continue

        rec = by_date.setdefault(d, {'open_930': None, 'close_eod': None})

        # The 9:30 hourly bar's OPEN = the 9:30:00 ET print of this index.
        # That's what we'd actually see live at 9:30 AM on trade day.
        if hhmm == '09:30' and o > 0:
            rec['open_930'] = o
        # Track the latest non-zero close as the EOD close (last bar of day).
        if c > 0:
            rec['close_eod'] = c

    return by_date


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--from', dest='from_date', default='2023-06-01')
    p.add_argument('--to', dest='to_date', default=date.today().isoformat())
    p.add_argument('--out', default=str(OUT_DIR / 'daily.csv'))
    args = p.parse_args()

    print(f'Fetching VIX term-structure (9:30 open + EOD close) {args.from_date} → {args.to_date}')
    print(f'Symbols: {", ".join(SYMBOLS)}')

    by_symbol_by_date: dict[str, dict[str, dict]] = {s: {} for s in SYMBOLS}
    start = date.fromisoformat(args.from_date)
    end = date.fromisoformat(args.to_date)

    while start <= end:
        chunk_end = min(start.replace(year=start.year + 1) - timedelta(days=1), end)
        cs = start.isoformat(); ce = chunk_end.isoformat()
        print(f'  Chunk {cs} → {ce}')
        for sym in SYMBOLS:
            data = fetch_hourly(sym, cs, ce)
            by_symbol_by_date[sym].update(data)
            print(f'    {sym}: +{len(data)} days')
            time.sleep(0.05)
        start = chunk_end + timedelta(days=1)

    all_dates = sorted(set().union(*[set(d.keys()) for d in by_symbol_by_date.values()]))
    print(f'\nUnion of dates: {len(all_dates)} | {all_dates[0]} → {all_dates[-1]}')

    out_path = Path(args.out)
    cols = ['date']
    for s in ['vix9d', 'vix', 'vix3m', 'vix6m', 'vvix']:
        cols += [f'{s}_open', f'{s}_close']
    cols += ['vix_vix3m_ratio_open', 'vix_vix3m_ratio_close',
             'vix9d_vix_ratio_open', 'vix9d_vix_ratio_close']

    with open(out_path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(cols)
        for d in all_dates:
            row = [d]
            vals = {}
            for sym_lower, sym_upper in [('vix9d','VIX9D'),('vix','VIX'),
                                          ('vix3m','VIX3M'),('vix6m','VIX6M'),
                                          ('vvix','VVIX')]:
                rec = by_symbol_by_date[sym_upper].get(d, {})
                o = rec.get('open_930'); c = rec.get('close_eod')
                vals[sym_lower] = (o, c)
                row.append(f'{o:.4f}' if o is not None else '')
                row.append(f'{c:.4f}' if c is not None else '')
            # Ratios — open uses opens, close uses closes
            v_o, v_c = vals['vix']
            v3_o, v3_c = vals['vix3m']
            v9_o, v9_c = vals['vix9d']
            r1o = (v_o / v3_o) if (v_o and v3_o) else None
            r1c = (v_c / v3_c) if (v_c and v3_c) else None
            r2o = (v9_o / v_o) if (v9_o and v_o) else None
            r2c = (v9_c / v_c) if (v9_c and v_c) else None
            for x in [r1o, r1c, r2o, r2c]:
                row.append(f'{x:.4f}' if x is not None else '')
            w.writerow(row)

    print(f'\nWrote {len(all_dates)} rows → {out_path}')
    last = all_dates[-1]
    print(f'\nLatest day ({last}):')
    for s in SYMBOLS:
        rec = by_symbol_by_date[s].get(last, {})
        print(f'  {s:>7}  open_930={rec.get("open_930")}  close_eod={rec.get("close_eod")}')


if __name__ == '__main__':
    main()

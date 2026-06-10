"""
ThetaData-backed SPX options fetcher — replaces fetch_polygon_spx.py when
Polygon REST quote access is unauthorized (sub-tier issue).

Output format matches fetch_polygon_spx.py exactly so build_diagonal_real_data.py
picks the files up unchanged:
  data/polygon/SPX_YYYYMMDD[_HHMM].json   {
    "date", "target_time_et", "spot", "strike_range", "quotes_count", "quotes": {
      "O:SPXW260513P07375000": {"strike", "expiration", "bid", "ask"}
    }
  }

ThetaData v3 API:
  GET /v3/option/list/expirations?symbol=SPXW                       → expirations list
  GET /v3/option/list/strikes?symbol=SPXW&expiration=YYYYMMDD       → strikes for that exp
  GET /v3/option/history/quote?symbol=SPXW&expiration=YYYYMMDD&strike=N.NN
      &right=put&date=YYYYMMDD&interval=1m&start_time=HH:MM:SS.SSS
      &end_time=HH:MM:SS.SSS&format=csv                              → quotes

HARD RULES (per CLAUDE.md):
  1. SPXW only — no SPX monthlies (AM-settled).
  2. Half-days use 12:45 ET instead of 14:00 ET.
  3. When --time > 12:45 is specified, half-days are SKIPPED.

Usage:
  python3 fetch_thetadata_spx.py --from 2026-05-12 --to 2026-05-22
  python3 fetch_thetadata_spx.py --from 2026-05-12 --to 2026-05-22 --time 13:30
  python3 fetch_thetadata_spx.py --date 2026-05-12
"""
from __future__ import annotations
import os, sys, json, csv, io, argparse, time
import requests
from datetime import datetime, timedelta, date as _date
from zoneinfo import ZoneInfo
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / 'data' / 'polygon'                 # same dir as polygon output
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

THETA = 'http://localhost:25503/v3'
ET = ZoneInfo('America/New_York')

# Strike + DTE windows — must match fetch_polygon_spx.py
STRIKE_MIN_OFFSET = -300
STRIKE_MAX_OFFSET = +300
SHORT_DTE_MIN, SHORT_DTE_MAX = 0, 7
LONG_DTE_MIN, LONG_DTE_MAX = 8, 35

# Half-days where market closes early (12:45 ET cutoff per CLAUDE.md)
HALF_DAYS_2024_2026 = {
    '2024-07-03', '2024-11-29', '2024-12-24',
    '2025-07-03', '2025-11-28', '2025-12-24',
    '2026-07-02', '2026-11-27', '2026-12-24',
}

SESSION = requests.Session()
SESSION.mount('http://', requests.adapters.HTTPAdapter(pool_maxsize=64))


def target_time_for(date_iso: str, time_override: str | None) -> str:
    """14:00 default; 12:45 on half-days; explicit override otherwise."""
    if time_override:
        return time_override
    return '12:45' if date_iso in HALF_DAYS_2024_2026 else '14:00'


def to_yyyymmdd(d: str) -> str:
    return d.replace('-', '')


def list_expirations(symbol: str, as_of_iso: str, dte_min: int, dte_max: int) -> list[str]:
    """Return YYYYMMDD strings for SPXW expirations in [as_of+dte_min, as_of+dte_max]."""
    r = SESSION.get(f'{THETA}/option/list/expirations',
                    params={'symbol': symbol, 'format': 'json'}, timeout=30)
    r.raise_for_status()
    d0 = _date.fromisoformat(as_of_iso)
    exp_gte = d0 + timedelta(days=dte_min)
    exp_lte = d0 + timedelta(days=dte_max)
    out = []
    for item in r.json().get('response', []):
        # ThetaData v3 returns either dicts {'expiration': '2026-05-13'} or raw strings
        raw = item['expiration'] if isinstance(item, dict) else item
        s = str(raw).replace('-', '')
        if len(s) != 8:
            continue
        try:
            exp_d = _date(int(s[:4]), int(s[4:6]), int(s[6:8]))
        except ValueError:
            continue
        if exp_gte <= exp_d <= exp_lte:
            out.append(s)
    return sorted(out)


def list_strikes(symbol: str, expiration_yyyymmdd: str, strike_min: float, strike_max: float) -> list[float]:
    """Strikes for one expiration, in [strike_min, strike_max] dollars."""
    r = SESSION.get(f'{THETA}/option/list/strikes',
                    params={'symbol': symbol, 'expiration': expiration_yyyymmdd, 'format': 'json'},
                    timeout=30)
    r.raise_for_status()
    out = []
    for item in r.json().get('response', []):
        raw = item['strike'] if isinstance(item, dict) else item
        s = float(raw)
        if strike_min <= s <= strike_max:
            out.append(s)
    return sorted(set(out))


def fetch_quote_at(symbol: str, exp_yyyymmdd: str, strike: float, right: str,
                   date_yyyymmdd: str, hhmm: str) -> tuple[float, float] | None:
    """Return (bid, ask) at the minute matching hhmm ET, or None if no data."""
    h, m = int(hhmm[:2]), int(hhmm[3:5])
    start = f'{h:02d}:{m:02d}:00.000'
    end   = f'{h:02d}:{m:02d}:59.999'
    try:
        r = SESSION.get(f'{THETA}/option/history/quote', params={
            'symbol': symbol, 'expiration': exp_yyyymmdd, 'strike': f'{strike:.2f}',
            'right': right, 'date': date_yyyymmdd,
            'interval': '1m', 'start_time': start, 'end_time': end, 'format': 'csv',
        }, timeout=20)
    except Exception:
        return None
    if not r.ok or not r.text or r.text.startswith('<'):
        return None
    # CSV: symbol,expiration,strike,right,timestamp,bid_size,bid_exchange,bid,bid_condition,ask_size,ask_exchange,ask,ask_condition
    reader = csv.reader(io.StringIO(r.text))
    headers = next(reader, None)
    if not headers:
        return None
    bid_idx = headers.index('bid') if 'bid' in headers else 7
    ask_idx = headers.index('ask') if 'ask' in headers else 11
    last_bid, last_ask = None, None
    for row in reader:
        if len(row) <= max(bid_idx, ask_idx):
            continue
        try:
            b = float(row[bid_idx]); a = float(row[ask_idx])
            if b > 0 and a > 0 and a >= b:
                last_bid, last_ask = b, a
        except ValueError:
            pass
    if last_bid is None:
        return None
    return last_bid, last_ask


def fetch_day(date_iso: str, spot: float, time_override: str | None = None) -> dict:
    hhmm = target_time_for(date_iso, time_override)
    yyyymmdd = to_yyyymmdd(date_iso)
    strike_min = spot + STRIKE_MIN_OFFSET
    strike_max = spot + STRIKE_MAX_OFFSET

    # 1. Build (exp, strike) grid for short and long DTE windows
    grid = set()
    for (mn, mx) in [(SHORT_DTE_MIN, SHORT_DTE_MAX), (LONG_DTE_MIN, LONG_DTE_MAX)]:
        for exp in list_expirations('SPXW', date_iso, mn, mx):
            for k in list_strikes('SPXW', exp, strike_min, strike_max):
                grid.add((exp, k))

    # 2. Parallel fetch puts at the target time
    quotes = {}
    def task(exp_strike):
        exp, k = exp_strike
        q = fetch_quote_at('SPXW', exp, k, 'put', yyyymmdd, hhmm)
        if q is None:
            return None
        bid, ask = q
        # Build OCC ticker — matches Polygon's "O:SPXW260513P07375000" format
        k_int = int(round(k * 1000))
        ticker = f'O:SPXW{exp[2:]}P{k_int:08d}'
        return ticker, {'strike': k, 'expiration': f'{exp[:4]}-{exp[4:6]}-{exp[6:8]}',
                         'bid': bid, 'ask': ask}

    with ThreadPoolExecutor(max_workers=8) as ex:                # Theta has 4 concurrent req cap; 8 client-side is fine with pooling
        for fut in as_completed([ex.submit(task, p) for p in grid]):
            r = fut.result()
            if r:
                quotes[r[0]] = r[1]

    return {
        'date': date_iso, 'target_time_et': hhmm, 'spot': spot,
        'strike_range': [round(strike_min, 2), round(strike_max, 2)],
        'quotes_count': len(quotes), 'quotes': quotes,
    }


def load_dates_and_spots():
    """Pull (date, spot) from diagonal_bs_data.json. Spot = spot_14 (or spot_12 on half-days)."""
    with open(ROOT / 'data' / 'diagonal_bs_data.json') as f:
        bs = json.load(f)
    out = []
    for d in sorted(bs['dates']):
        entry = bs['by_date'][d]
        spot = entry.get('spot_14') if d not in HALF_DAYS_2024_2026 else entry.get('spot_12')
        if spot is None:
            continue
        out.append((d, spot))
    return out


def output_path(date_iso: str, time_override: str | None) -> Path:
    yyyymmdd = to_yyyymmdd(date_iso)
    if time_override:
        return OUTPUT_DIR / f'SPX_{yyyymmdd}_{time_override.replace(":","")}.json'
    return OUTPUT_DIR / f'SPX_{yyyymmdd}.json'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--date', dest='date', help='single date YYYY-MM-DD')
    ap.add_argument('--from', dest='from_date')
    ap.add_argument('--to',   dest='to_date')
    ap.add_argument('--time', dest='time', help='time override HH:MM (default 14:00)')
    args = ap.parse_args()

    entries = load_dates_and_spots()
    if args.date:
        entries = [(d, s) for d, s in entries if d == args.date]
    if args.from_date:
        entries = [(d, s) for d, s in entries if d >= args.from_date]
    if args.to_date:
        entries = [(d, s) for d, s in entries if d <= args.to_date]

    # On override > 12:45, drop half-days
    if args.time and args.time > '12:45':
        entries = [(d, s) for d, s in entries if d not in HALF_DAYS_2024_2026]

    if not entries:
        print('No dates to process. Have you extended diagonal_bs_data.json?')
        return

    print(f'Processing {len(entries)} day(s) via ThetaData {"@" + args.time if args.time else ""}')
    t0 = time.time()
    for i, (d, spot) in enumerate(entries, 1):
        result = fetch_day(d, spot, args.time)
        out_path = output_path(d, args.time)
        out_path.write_text(json.dumps(result))
        elapsed = time.time() - t0
        rate = i / max(elapsed, 0.001)
        eta_s = (len(entries) - i) / max(rate, 0.001)
        print(f'  [{i}/{len(entries)}] {d} spot={spot} @{result["target_time_et"]} → {result["quotes_count"]} quotes  '
              f'| rate={rate:.2f}/s ETA={eta_s/60:.1f}min')

    print(f'\nDone. Wrote files to {OUTPUT_DIR}')


if __name__ == '__main__':
    main()

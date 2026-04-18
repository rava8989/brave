"""
Fetch real SPX options bid/ask from Polygon for the diagonal backtester.

For each trading day:
  - Use existing spot_14 (or spot_12 on half-days) from diagonal_bs_data.json
  - Find SPXW put contracts in a strike+DTE grid around the strategy
  - Fetch bid/ask at 14:00 ET (or 12:45 ET on half-days)
  - Save per-day JSON to data/polygon/SPX_YYYYMMDD.json

HARD RULES (from user):
  1. SPXW only — no AM-settled SPX monthlies (filter by root_symbol)
  2. Half-days use 12:45 ET instead of 14:00 ET
  3. Entry and exit are both at the target time on consecutive trading days

Usage:
  python3 fetch_polygon_spx.py                    # full backfill
  python3 fetch_polygon_spx.py --date 2026-01-02  # single day (test)
  python3 fetch_polygon_spx.py --from 2026-01-01  # from a specific date
"""
import os
import sys
import json
import time
import argparse
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Load .env ─────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent
for line in (ROOT / '.env').read_text().splitlines():
    if '=' in line and not line.strip().startswith('#'):
        k, v = line.split('=', 1)
        os.environ[k.strip()] = v.strip()

API = os.environ['POLYGON_API_KEY']
BASE = 'https://api.polygon.io'

OUTPUT_DIR = ROOT / 'data' / 'polygon'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Half-day calendar (early 1:00 PM ET close) ────────────────────────
# On these days the market closes at 13:00 ET, so we snapshot at 12:45.
HALF_DAYS = {
    '2023-07-03', '2023-11-24', '2023-12-22',  # Dec 22 2023 was not, keep conservative
    '2024-07-03', '2024-11-29', '2024-12-24',
    '2025-11-28', '2025-12-24',
    '2026-11-27', '2026-12-24',
}
# Note: we'll sanity-check against Polygon's market status endpoint later

# ── Strike/DTE grid ───────────────────────────────────────────────────
# WIDENED (2026-04-17): big overnight SPX moves can push strikes outside a
# narrow grid. ±150 pts easily handles any realistic 1-day move so the exit
# leg always finds real quotes (no B-S fallback for price continuity).
STRIKE_MIN_OFFSET = -300  # strikes start at spot - 300 (handles big overnight moves)
STRIKE_MAX_OFFSET = +300  # strikes end at spot + 300
STRIKE_STEP = 5

# DTE windows (also slightly widened for tolerance)
# IMPORTANT: SHORT_DTE_MIN=0 so on the EXIT day, the 1-DTE-from-entry contract
# still shows up (it's 0 DTE on exit day, at 14:00 ET still 2 hrs before 16:00 expiry).
# Without this, backtester exits always fall back to Black-Scholes for the short leg.
SHORT_DTE_MIN = 0
SHORT_DTE_MAX = 7    # short-leg fallback up to a week (holidays + weekends)
LONG_DTE_MIN = 12
LONG_DTE_MAX = 28    # 20 ± 8 window — supports parameter tweaks without refetch


# ══════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════

def et_to_utc_ns(date_iso: str, hhmm: str) -> int:
    """Convert 'YYYY-MM-DD' + 'HH:MM' ET to UTC nanoseconds.
    Handles EDT/EST — Polygon timestamps are in UTC nanoseconds.
    Rough rule: March-Nov is EDT (UTC-4), Dec-Feb is EST (UTC-5).
    For SPX market hours we only need ET→UTC once per day.
    """
    # Build ET datetime by interpreting as America/New_York
    # Use aware datetime via localization trick
    from zoneinfo import ZoneInfo
    et = ZoneInfo('America/New_York')
    dt = datetime.fromisoformat(date_iso + 'T' + hhmm + ':00').replace(tzinfo=et)
    return int(dt.astimezone(timezone.utc).timestamp() * 1e9)


def target_time_et(date_iso: str) -> str:
    return '12:45' if date_iso in HALF_DAYS else '14:00'


def list_spxw_puts(as_of_date: str, dte_min: int, dte_max: int,
                   strike_min: float, strike_max: float):
    """Return list of SPXW put contract tickers in the given strike+DTE window.
    Filters out SPX monthly (AM-settled) — keeps only SPXW (PM-settled weeklies).

    IMPORTANT: Polygon's contracts endpoint defaults expired=false, which
    excludes any contract that has already expired by query time. For historical
    backfills we need BOTH active and expired contracts — 0-DTE on past dates
    are "expired" now. So we run the query twice (expired=false + expired=true)
    and merge the results, deduplicated by ticker.
    """
    from datetime import date as _d
    d0 = _d.fromisoformat(as_of_date)
    exp_gte = (d0 + timedelta(days=dte_min)).isoformat()
    exp_lte = (d0 + timedelta(days=dte_max)).isoformat()

    # NOTE: Do NOT pass `as_of` together with expiration_date range + expired=true —
    # Polygon silently returns 0 results in that combo. The ticker + expiration_date
    # pair alone uniquely identifies the contract; as_of isn't needed for backfills.
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
            r = requests.get(url, params=params)
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
    """Return {bid, ask, ts_ns} NBBO quote at or just before ts_ns, else None."""
    r = requests.get(f'{BASE}/v3/quotes/{ticker}', params={
        'timestamp.lte': ts_ns,
        'order': 'desc',
        'limit': 1,
        'apiKey': API,
    })
    results = r.json().get('results') or []
    if not results:
        return None
    q = results[0]
    return {
        'bid': q.get('bid_price'),
        'ask': q.get('ask_price'),
        'ts_ns': q.get('sip_timestamp'),
    }


# ══════════════════════════════════════════════════════════════════════
# PER-DAY FETCHER
# ══════════════════════════════════════════════════════════════════════

def fetch_day(date_iso: str, spot: float) -> dict:
    hhmm = target_time_et(date_iso)
    ts_ns = et_to_utc_ns(date_iso, hhmm)

    strike_min = spot + STRIKE_MIN_OFFSET
    strike_max = spot + STRIKE_MAX_OFFSET

    # Short-leg window (1-5 DTE) and long-leg window (15-25 DTE) — fetch both
    short_contracts = list_spxw_puts(date_iso, SHORT_DTE_MIN, SHORT_DTE_MAX, strike_min, strike_max)
    long_contracts = list_spxw_puts(date_iso, LONG_DTE_MIN, LONG_DTE_MAX, strike_min, strike_max)

    all_contracts = {c['ticker']: c for c in short_contracts + long_contracts}

    # Parallel quote fetches — Polygon Advanced plan has unlimited rate
    quotes = {}
    def fetch_one(item):
        ticker, info = item
        q = fetch_quote_at(ticker, ts_ns)
        if q and q['bid'] is not None and q['ask'] is not None:
            return ticker, {
                'strike': info['strike'],
                'expiration': info['expiration'],
                'bid': q['bid'],
                'ask': q['ask'],
            }
        return None

    with ThreadPoolExecutor(max_workers=20) as ex:
        for result in ex.map(fetch_one, all_contracts.items()):
            if result:
                ticker, data = result
                quotes[ticker] = data

    return {
        'date': date_iso,
        'target_time_et': hhmm,
        'spot': spot,
        'strike_range': [strike_min, strike_max],
        'quotes_count': len(quotes),
        'quotes': quotes,
    }


# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════

def load_dates_and_spots():
    """Read existing diagonal_bs_data.json for the date list and spot prices."""
    path = ROOT / 'data' / 'diagonal_bs_data.json'
    data = json.loads(path.read_text())
    dates = data['dates']
    by_date = data['by_date']
    # Return list of (date, spot). Use spot_14 normally, or spot_12 for half-days.
    result = []
    for d in dates:
        entry = by_date.get(d) or {}
        spot = entry.get('spot_14') if d not in HALF_DAYS else entry.get('spot_12')
        if spot is None:
            spot = entry.get('spot_14') or entry.get('spot_12')  # fallback
        if spot is not None:
            result.append((d, spot))
    return result


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--date', help='Single date YYYY-MM-DD (test mode)')
    p.add_argument('--from', dest='from_date', help='Start date YYYY-MM-DD')
    p.add_argument('--to', dest='to_date', help='End date YYYY-MM-DD')
    p.add_argument('--force', action='store_true', help='Re-fetch days already cached')
    p.add_argument('--reverse', action='store_true', help='Process newest dates first')
    args = p.parse_args()

    entries = load_dates_and_spots()

    if args.date:
        entries = [(d, s) for d, s in entries if d == args.date]
        if not entries:
            print(f'Date {args.date} not found in diagonal_bs_data.json')
            sys.exit(1)
    else:
        if args.from_date:
            entries = [(d, s) for d, s in entries if d >= args.from_date]
        if args.to_date:
            entries = [(d, s) for d, s in entries if d <= args.to_date]

    # Filter already-fetched days (unless --force)
    if not args.force:
        entries = [(d, s) for d, s in entries
                   if not (OUTPUT_DIR / f'SPX_{d.replace("-","")}.json').exists()]

    # Newest-first if requested
    if args.reverse:
        entries = list(reversed(entries))

    print(f'Processing {len(entries)} trading days with 4 concurrent days × 20 concurrent quotes...')
    t0 = time.time()
    total_quotes = 0
    completed = 0

    def process_one(item):
        date_iso, spot = item
        out_path = OUTPUT_DIR / f'SPX_{date_iso.replace("-","")}.json'
        try:
            day = fetch_day(date_iso, spot)
            out_path.write_text(json.dumps(day, separators=(',', ':')))
            return date_iso, spot, day['target_time_et'], day['quotes_count'], None
        except Exception as e:
            return date_iso, spot, '', 0, str(e)

    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(process_one, item): item for item in entries}
        for fut in as_completed(futures):
            date_iso, spot, tt, n, err = fut.result()
            completed += 1
            if err:
                print(f'[{completed}/{len(entries)}] {date_iso} ERROR: {err}', file=sys.stderr)
            else:
                total_quotes += n
                elapsed = time.time() - t0
                rate = completed / elapsed if elapsed > 0 else 0
                eta_min = (len(entries) - completed) / rate / 60 if rate > 0 else 0
                print(f'[{completed}/{len(entries)}] {date_iso} spot={spot:.2f} @{tt} → {n} quotes  '
                      f'| rate={rate:.1f}/s ETA={eta_min:.1f}min')

    print(f'\nDone. Total quotes fetched: {total_quotes}')
    print(f'Output: {OUTPUT_DIR}')


if __name__ == '__main__':
    main()

"""
Build the S&P 500 universe + 3-year earnings calendar for the UOA backtester.

Outputs:
  data/sp500_constituents.json  — {tickers: [...], generated_at, source: "wikipedia"}
  data/earnings_calendar.json   — [{ticker, report_date, timing, fiscal_period, source}]

Earnings source: Polygon /vX/reference/financials. `filing_date` is used as the
report-date proxy. The SEC filing happens days after the earnings announcement;
for pre-"event" UOA backtesting this still yields a consistent window since the
offset is uniform across all events.

Usage:
  python3 build_earnings_universe.py                  # full refresh (uses cache)
  python3 build_earnings_universe.py --tickers AAPL   # subset for testing
  python3 build_earnings_universe.py --force          # ignore cache, refetch
"""
import os
import sys
import json
import argparse
import requests
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = Path(__file__).resolve().parent
for line in (ROOT / '.env').read_text().splitlines():
    if '=' in line and not line.strip().startswith('#'):
        k, v = line.split('=', 1)
        os.environ[k.strip()] = v.strip()

API = os.environ['POLYGON_API_KEY']
BASE = 'https://api.polygon.io'
DATA_DIR = ROOT / 'data'
DATA_DIR.mkdir(parents=True, exist_ok=True)

SP500_URL = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
YEARS_HISTORY = 3


def fetch_sp500_tickers() -> list[str]:
    """Scrape current S&P 500 tickers from Wikipedia (first table on page)."""
    import pandas as pd
    from io import StringIO
    r = requests.get(SP500_URL, headers={'User-Agent': 'Mozilla/5.0 (SPX-Backtester)'}, timeout=30)
    r.raise_for_status()
    tables = pd.read_html(StringIO(r.text))
    df = tables[0]
    tickers = df['Symbol'].astype(str).str.upper().tolist()
    return sorted(set(t.strip() for t in tickers if t.strip()))


def fetch_earnings_for_ticker(ticker: str, from_date: str) -> list[dict]:
    """Fetch quarterly financial filings; use filing_date as report-date proxy."""
    url = f'{BASE}/vX/reference/financials'
    params = {
        'ticker': ticker,
        'filing_date.gte': from_date,
        'timeframe': 'quarterly',
        'order': 'asc',
        'limit': 100,
        'apiKey': API,
    }
    events = []
    while True:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code != 200:
            print(f'  {ticker}: HTTP {r.status_code} {r.text[:120]}', file=sys.stderr)
            return events
        data = r.json()
        for f in data.get('results') or []:
            filing_date = f.get('filing_date')
            if not filing_date:
                continue
            events.append({
                'ticker': ticker,
                'report_date': filing_date,
                'fiscal_period': f.get('fiscal_period'),
                'fiscal_year': f.get('fiscal_year'),
                'period_end': f.get('end_date'),
                'timing': None,
                'source': 'polygon_financials',
            })
        next_url = data.get('next_url')
        if not next_url:
            break
        url = next_url
        params = {'apiKey': API}
    return events


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--tickers', help='Comma-separated subset for testing')
    p.add_argument('--force', action='store_true', help='Ignore cache')
    args = p.parse_args()

    constituents_path = DATA_DIR / 'sp500_constituents.json'
    if args.force or not constituents_path.exists():
        print('Fetching S&P 500 constituents from Wikipedia...')
        tickers = fetch_sp500_tickers()
        constituents_path.write_text(json.dumps({
            'tickers': tickers,
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'source': 'wikipedia',
        }, indent=2))
        print(f'  wrote {len(tickers)} tickers → {constituents_path.name}')
    else:
        tickers = json.loads(constituents_path.read_text())['tickers']
        print(f'cached constituents: {len(tickers)} tickers')

    if args.tickers:
        subset = [t.strip().upper() for t in args.tickers.split(',')]
        tickers = [t for t in tickers if t in subset]
        print(f'subset filter → {len(tickers)} tickers: {tickers}')
        if not tickers:
            print('no tickers match subset; add them to constituents or check spelling', file=sys.stderr)
            sys.exit(1)

    calendar_path = DATA_DIR / 'earnings_calendar.json'
    existing = []
    if not args.force and calendar_path.exists():
        existing = json.loads(calendar_path.read_text())
    existing_keys = {(e['ticker'], e['report_date']) for e in existing}

    from_iso = (date.today() - timedelta(days=YEARS_HISTORY * 365 + 30)).isoformat()
    print(f'fetching earnings for {len(tickers)} tickers from {from_iso} ...')

    new_events = []
    with ThreadPoolExecutor(max_workers=20) as ex:
        futures = {ex.submit(fetch_earnings_for_ticker, t, from_iso): t for t in tickers}
        done = 0
        for fut in as_completed(futures):
            t = futures[fut]
            done += 1
            try:
                evs = fut.result()
                fresh = [e for e in evs if (e['ticker'], e['report_date']) not in existing_keys]
                new_events.extend(fresh)
                status = f'{len(evs)} events'
                if fresh:
                    status += f' ({len(fresh)} new)'
                print(f'  [{done}/{len(tickers)}] {t}: {status}')
            except Exception as exc:
                print(f'  [{done}/{len(tickers)}] {t}: ERROR {exc}', file=sys.stderr)

    all_events = existing + new_events
    all_events.sort(key=lambda e: (e['ticker'], e['report_date']))
    calendar_path.write_text(json.dumps(all_events, indent=2))
    print(f'\nwrote {len(all_events)} total events ({len(new_events)} new) → {calendar_path.name}')


if __name__ == '__main__':
    main()

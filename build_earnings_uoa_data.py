"""
Build the per-year UOA dataset for the earnings backtester.

Inputs (from build_earnings_universe.py):
  data/sp500_constituents.json
  data/earnings_calendar.json

Outputs:
  data/earnings_uoa_{YYYY}.json
  data/earnings_uoa_index.json

For each earnings event in the target year, fetches 3 pre-event trading days
of options activity (total call/put volume, OI, ATM IV, per-side premium proxy)
plus post-event underlying OHLC for labels (move_1d/3d/5d/10d).

Note on timing: Polygon's `filing_date` is used as the report-date anchor. It
lands ~1 day after the actual earnings announcement. The "lookback" window is
therefore 3 trading days ending at filing_date - 1, which for AMC reporters
covers the day of announcement and the 2 preceding days. Consistent offset
across all events.

Usage:
  python3 build_earnings_uoa_data.py --year 2024 --tickers AAPL     # test one
  python3 build_earnings_uoa_data.py --year 2024                    # one year, all tickers
  python3 build_earnings_uoa_data.py                                # all years, all tickers
  python3 build_earnings_uoa_data.py --force                        # ignore per-event cache
"""
from __future__ import annotations
import os
import re
import sys
import csv
import gzip
import json
import math
import argparse
import io
import requests
import threading
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
EQUITY_DIR = DATA_DIR / 'equity_history'
EQUITY_DIR.mkdir(parents=True, exist_ok=True)
OPT_VOL_CACHE_DIR = DATA_DIR / 'polygon' / 'options_vol_baseline'
OPT_VOL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
SESSION = requests.Session()

SCHWAB_PROXY = 'https://schwab-proxy.ravamt4.workers.dev'
SCHWAB_HEADERS = {'Origin': 'http://localhost:8000'}  # satisfies CORS check

LOOKBACK_DAYS = 3     # scan window before event
LABEL_FORWARD_DAYS = 14  # pull enough forward bars for move_10d (need ~12 trading days)
BASELINE_DAYS = 20    # trailing trading days for volume-vs-avg signal

CONTRACT_RE = re.compile(r'^O:([A-Z.]+)(\d{6})([CP])(\d{8})$')
_s3_client = None
_s3_lock = threading.Lock()


def s3_client():
    global _s3_client
    if _s3_client is None:
        with _s3_lock:
            if _s3_client is None:
                import boto3
                _s3_client = boto3.client(
                    's3',
                    endpoint_url=os.environ['POLYGON_S3_ENDPOINT'],
                    aws_access_key_id=os.environ['POLYGON_S3_ACCESS_KEY'],
                    aws_secret_access_key=os.environ['POLYGON_S3_SECRET_KEY'],
                )
    return _s3_client


_vol_baseline_cache: dict[str, dict] = {}
_vol_baseline_locks: dict[str, threading.Lock] = {}
_vol_baseline_master_lock = threading.Lock()


def load_options_day_aggregates(day_iso: str, tickers_set: set[str]) -> dict[str, dict]:
    """Return {ticker: {call_vol, put_vol, call_premium, put_premium, contract_count}}
    for one trading day, filtered to the requested ticker set.

    First call for a date: download flat file from S3, parse, cache JSON.
    Subsequent calls: read cached JSON. Premium computed as volume × close × 100
    summed per side (close as midpoint proxy; flat file has no vwap).
    """
    if day_iso in _vol_baseline_cache:
        return _vol_baseline_cache[day_iso]

    with _vol_baseline_master_lock:
        lk = _vol_baseline_locks.setdefault(day_iso, threading.Lock())
    with lk:
        if day_iso in _vol_baseline_cache:
            return _vol_baseline_cache[day_iso]
        cache_path = OPT_VOL_CACHE_DIR / f'{day_iso}.json'
        if cache_path.exists():
            parsed = json.loads(cache_path.read_text())
            _vol_baseline_cache[day_iso] = parsed
            return parsed

        key = f'us_options_opra/day_aggs_v1/{day_iso[:4]}/{day_iso[5:7]}/{day_iso}.csv.gz'
        try:
            obj = s3_client().get_object(Bucket=os.environ['POLYGON_S3_BUCKET'], Key=key)
            raw = obj['Body'].read()
        except Exception:
            _vol_baseline_cache[day_iso] = {}
            return {}

        import pandas as pd
        df = pd.read_csv(io.BytesIO(raw), compression='gzip', usecols=['ticker', 'volume', 'close'])
        # Parse tickers vectorized
        m = df['ticker'].astype(str).str.extract(r'^O:([A-Z.]+)(\d{6})([CP])(\d{8})$')
        df['underlying'] = m[0]
        df['side'] = m[2]
        df = df[df['underlying'].isin(tickers_set)].copy()
        df['premium'] = df['volume'].fillna(0).astype(int) * df['close'].fillna(0).astype(float) * 100
        df['volume'] = df['volume'].fillna(0).astype(int)

        parsed: dict[str, dict] = {}
        for underlying, grp in df.groupby('underlying'):
            calls = grp[grp['side'] == 'C']
            puts = grp[grp['side'] == 'P']
            parsed[str(underlying)] = {
                'call_vol': int(calls['volume'].sum()),
                'put_vol': int(puts['volume'].sum()),
                'call_premium': round(float(calls['premium'].sum()), 2),
                'put_premium': round(float(puts['premium'].sum()), 2),
                'contract_count': int(len(grp)),
            }
        cache_path.write_text(json.dumps(parsed, separators=(',', ':')))
        _vol_baseline_cache[day_iso] = parsed
        return parsed


# Backwards-compat alias (volume-only baseline fields)
def load_options_day_volumes(day_iso: str, tickers_set: set[str]) -> dict[str, dict]:
    return load_options_day_aggregates(day_iso, tickers_set)


def compute_volume_baseline(ticker: str, day_iso: str, tickers_set: set[str]) -> dict:
    """Trailing 20-trading-day mean call/put volume for ticker, ending at day_iso - 1."""
    trailing = prev_trading_days(day_iso, BASELINE_DAYS)
    call_samples = []
    put_samples = []
    for d in trailing:
        data = load_options_day_volumes(d, tickers_set)
        tv = data.get(ticker) or {}
        call_samples.append(tv.get('call_vol', 0))
        put_samples.append(tv.get('put_vol', 0))
    def mean(xs):
        return sum(xs) / len(xs) if xs else 0
    return {
        'call_volume_20d_avg': round(mean(call_samples)),
        'put_volume_20d_avg': round(mean(put_samples)),
    }


def prev_trading_days(anchor_iso: str, n: int) -> list[str]:
    """Return the n trading days strictly before anchor_iso (weekday approximation).
    Skips weekends only; ignores holidays for simplicity (Polygon will return empty
    aggs for non-trading days and we filter those rows out).
    """
    d = date.fromisoformat(anchor_iso) - timedelta(days=1)
    out = []
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d -= timedelta(days=1)
    return list(reversed(out))


def list_contracts_atm(ticker: str, as_of: str, spot: float | None) -> list[dict]:
    """List call contracts near-ATM at ~30 DTE — only enough for IV inversion.

    Tight filter: spot ± 5%, DTE 15-45 days, calls only. Returns a small list
    (~10 contracts) so IV discovery stays cheap.
    """
    if not spot:
        return []
    strike_min = spot * 0.95
    strike_max = spot * 1.05
    exp_gte = (date.fromisoformat(as_of) + timedelta(days=15)).isoformat()
    exp_lte = (date.fromisoformat(as_of) + timedelta(days=45)).isoformat()

    base_params = {
        'underlying_ticker': ticker,
        'as_of': as_of,
        'contract_type': 'call',
        'expiration_date.gte': exp_gte,
        'expiration_date.lte': exp_lte,
        'strike_price.gte': strike_min,
        'strike_price.lte': strike_max,
        'limit': 250,
        'apiKey': API,
    }
    contracts: dict[str, dict] = {}
    for expired_flag in ('false', 'true'):
        url = f'{BASE}/v3/reference/options/contracts'
        params = {**base_params, 'expired': expired_flag}
        r = SESSION.get(url, params=params, timeout=30)
        if r.status_code != 200:
            continue
        for c in (r.json() or {}).get('results') or []:
            t = c.get('ticker')
            if t and t not in contracts:
                contracts[t] = {
                    'ticker': t,
                    'type': c.get('contract_type'),
                    'strike': c.get('strike_price'),
                    'exp': c.get('expiration_date'),
                }
    return list(contracts.values())


def daily_aggs(option_ticker: str, day_iso: str) -> dict | None:
    """Return {v, o, h, l, c, vw, t} for the option on day_iso, or None if empty."""
    url = f'{BASE}/v2/aggs/ticker/{option_ticker}/range/1/day/{day_iso}/{day_iso}'
    r = SESSION.get(url, params={'apiKey': API}, timeout=30)
    if r.status_code != 200:
        return None
    results = (r.json() or {}).get('results') or []
    return results[0] if results else None


_equity_cache: dict[str, list[dict]] = {}


def fetch_equity_history_schwab(ticker: str) -> list[dict]:
    """Full 3yr daily OHLCV for `ticker` via Schwab proxy. Returns sorted ascending.
    Candle shape: {open, high, low, close, volume, datetime (ms epoch)}.
    """
    url = f'{SCHWAB_PROXY}/market/pricehistory'
    params = {
        'symbol': ticker,
        'periodType': 'year',
        'period': '3',
        'frequencyType': 'daily',
        'frequency': '1',
    }
    r = SESSION.get(url, params=params, headers=SCHWAB_HEADERS, timeout=30)
    if r.status_code != 200:
        return []
    data = r.json() or {}
    candles = data.get('candles') or []
    candles.sort(key=lambda c: c.get('datetime', 0))
    return candles


def load_equity_history(ticker: str, refresh: bool = False) -> list[dict]:
    """Read cached equity history or fetch from Schwab proxy and cache."""
    if ticker in _equity_cache:
        return _equity_cache[ticker]
    path = EQUITY_DIR / f'{ticker}.json'
    if path.exists() and not refresh:
        candles = json.loads(path.read_text())
    else:
        candles = fetch_equity_history_schwab(ticker)
        if candles:
            path.write_text(json.dumps(candles, separators=(',', ':')))
    _equity_cache[ticker] = candles
    return candles


def _date_from_ms(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime('%Y-%m-%d')


def underlying_close(ticker: str, day_iso: str) -> float | None:
    for c in load_equity_history(ticker):
        if _date_from_ms(c['datetime']) == day_iso:
            return c.get('close')
    return None


def underlying_range(ticker: str, from_iso: str, to_iso: str) -> list[dict]:
    """Return candles in [from_iso, to_iso] formatted like Polygon aggs (o/h/l/c/v)."""
    out = []
    for c in load_equity_history(ticker):
        d = _date_from_ms(c['datetime'])
        if from_iso <= d <= to_iso:
            out.append({
                'o': c.get('open'), 'h': c.get('high'), 'l': c.get('low'),
                'c': c.get('close'), 'v': c.get('volume'),
                'date': d,
            })
    return out


def aggregate_chain(ticker: str, day_iso: str, tickers_set: set[str]) -> dict:
    """Use the cached S3 day_aggs flat file to sum call/put volume + premium."""
    all_data = load_options_day_aggregates(day_iso, tickers_set)
    row = all_data.get(ticker) or {}
    return {
        'total_call_volume': row.get('call_vol', 0),
        'total_put_volume': row.get('put_vol', 0),
        'call_premium_usd': row.get('call_premium', 0.0),
        'put_premium_usd': row.get('put_premium', 0.0),
        'contracts_with_data': row.get('contract_count', 0),
    }


def oi_snapshot(contracts: list[dict]) -> dict:
    """Open interest is not available in Polygon REST for historical contracts.
    The `open_interest_v1` S3 flat file path requires permissions this plan does
    not have (forbidden). VOR signal will therefore use volume vs 20-day average
    as a substitute in the frontend. Leaving OI fields as None here.
    """
    return {'total_call_oi': None, 'total_put_oi': None}


def bs_call_price(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0:
        return max(S - K, 0)
    from math import log, sqrt
    d1 = (log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)
    return S * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)


def _norm_cdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def implied_vol_call(mid: float, S: float, K: float, T: float, r: float = 0.05) -> float | None:
    if mid <= 0 or S <= 0 or K <= 0 or T <= 0:
        return None
    lo, hi = 1e-4, 5.0
    for _ in range(50):
        mid_sigma = 0.5 * (lo + hi)
        price = bs_call_price(S, K, T, r, mid_sigma)
        if price < mid:
            lo = mid_sigma
        else:
            hi = mid_sigma
        if hi - lo < 1e-5:
            break
    return 0.5 * (lo + hi)


def atm_iv(ticker: str, day_iso: str, spot: float, contracts: list[dict]) -> float | None:
    """Pick nearest-30-DTE ATM call, fetch NBBO midpoint, invert to IV."""
    target_dte = 30
    d0 = date.fromisoformat(day_iso)
    calls = [c for c in contracts if c['type'] == 'call' and c.get('strike')]
    if not calls:
        return None
    best = None
    for c in calls:
        try:
            exp = date.fromisoformat(c['exp'])
        except Exception:
            continue
        dte = (exp - d0).days
        if dte < 7 or dte > 60:
            continue
        strike_diff = abs(c['strike'] - spot)
        dte_diff = abs(dte - target_dte)
        score = strike_diff + dte_diff * 2
        if best is None or score < best[0]:
            best = (score, c, dte)
    if not best:
        return None
    _, pick, dte = best
    agg = daily_aggs(pick['ticker'], day_iso)
    if not agg:
        return None
    mid = (agg.get('c') or agg.get('vw') or 0)
    if mid <= 0:
        return None
    T = dte / 365.0
    return implied_vol_call(mid, spot, pick['strike'], T)


def process_event(event: dict, tickers_set: set[str], force: bool = False) -> dict | None:
    """Build full event record with lookback + labels."""
    ticker = event['ticker']
    report_date = event['report_date']
    lookback_dates = prev_trading_days(report_date, LOOKBACK_DAYS)

    out = {
        'ticker': ticker,
        'report_date': report_date,
        'fiscal_period': event.get('fiscal_period'),
        'fiscal_year': event.get('fiscal_year'),
        'period_end': event.get('period_end'),
        'timing': event.get('timing'),
        'lookback': {},
        'labels': {},
    }

    for offset, day_iso in zip((-3, -2, -1), lookback_dates):
        spot = underlying_close(ticker, day_iso)
        atm_calls = list_contracts_atm(ticker, day_iso, spot)
        agg = aggregate_chain(ticker, day_iso, tickers_set)
        oi = oi_snapshot(atm_calls)
        iv = atm_iv(ticker, day_iso, spot, atm_calls) if spot else None
        baseline = compute_volume_baseline(ticker, day_iso, tickers_set)
        call_avg = baseline['call_volume_20d_avg']
        put_avg = baseline['put_volume_20d_avg']
        out['lookback'][str(offset)] = {
            'date': day_iso,
            'underlying_close': spot,
            'contracts_with_data': agg['contracts_with_data'],
            'total_call_volume': agg['total_call_volume'],
            'total_put_volume': agg['total_put_volume'],
            'total_call_oi': oi['total_call_oi'],
            'total_put_oi': oi['total_put_oi'],
            'call_premium_usd': round(agg['call_premium_usd'], 2),
            'put_premium_usd': round(agg['put_premium_usd'], 2),
            'iv_atm': round(iv, 4) if iv else None,
            'call_volume_20d_avg': call_avg,
            'put_volume_20d_avg': put_avg,
            'vol_ratio_calls': round(agg['total_call_volume'] / call_avg, 4) if call_avg else None,
            'vol_ratio_puts': round(agg['total_put_volume'] / put_avg, 4) if put_avg else None,
        }

    # Post-event labels
    forward_end = (date.fromisoformat(report_date) + timedelta(days=LABEL_FORWARD_DAYS)).isoformat()
    bars = underlying_range(ticker, report_date, forward_end)
    if bars:
        pre_close = out['lookback'].get('-1', {}).get('underlying_close')
        out['labels']['pre_close'] = pre_close
        out['labels']['open_after'] = bars[0].get('o')
        closes = [b.get('c') for b in bars if b.get('c') is not None]
        if pre_close and closes:
            for i, label_day in enumerate([1, 3, 5, 10]):
                if len(closes) > label_day:
                    out['labels'][f'move_{label_day}d'] = round(
                        (closes[label_day] - pre_close) / pre_close, 4
                    )
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--year', type=int, help='Process only this year (by filing_date)')
    p.add_argument('--tickers', help='Comma-separated subset for testing')
    p.add_argument('--force', action='store_true', help='Ignore cached events')
    p.add_argument('--parallel', type=int, default=5, help='Concurrent events (default 5)')
    args = p.parse_args()

    calendar_path = DATA_DIR / 'earnings_calendar.json'
    if not calendar_path.exists():
        print('no earnings_calendar.json — run build_earnings_universe.py first', file=sys.stderr)
        sys.exit(1)
    events = json.loads(calendar_path.read_text())

    constituents_path = DATA_DIR / 'sp500_constituents.json'
    tickers_set: set[str] = set()
    if constituents_path.exists():
        tickers_set = set(json.loads(constituents_path.read_text())['tickers'])

    if args.tickers:
        subset = {t.strip().upper() for t in args.tickers.split(',')}
        events = [e for e in events if e['ticker'] in subset]
        tickers_set |= subset
    if args.year:
        events = [e for e in events if e['report_date'].startswith(str(args.year))]

    print(f'processing {len(events)} events with {args.parallel} concurrent workers')

    by_year: dict[str, list[dict]] = {}
    by_year_lock = threading.Lock()
    cached_keys: set[tuple] = set()
    for year_path in sorted(DATA_DIR.glob('earnings_uoa_*.json')):
        if year_path.name == 'earnings_uoa_index.json':
            continue
        try:
            data = json.loads(year_path.read_text())
            yr = data.get('year') or year_path.stem.split('_')[-1]
            by_year[yr] = data.get('earnings_events') or []
            for e in by_year[yr]:
                cached_keys.add((e['ticker'], e['report_date']))
        except Exception:
            continue

    pending = [e for e in events if args.force or (e['ticker'], e['report_date']) not in cached_keys]
    skipped = len(events) - len(pending)
    print(f'  {skipped} cached, {len(pending)} to process')

    def work(ev):
        key = (ev['ticker'], ev['report_date'])
        try:
            rec = process_event(ev, tickers_set, args.force)
        except Exception as exc:
            return key, None, str(exc)
        return key, rec, None

    done = 0
    total = len(pending)
    t0 = datetime.now()
    with ThreadPoolExecutor(max_workers=args.parallel) as ex:
        futures = {ex.submit(work, ev): ev for ev in pending}
        for fut in as_completed(futures):
            ev = futures[fut]
            done += 1
            key, rec, err = fut.result()
            if err:
                print(f'  [{done}/{total}] {key[0]} {key[1]}: ERROR {err}', file=sys.stderr)
                continue
            if not rec:
                continue
            yr = key[1][:4]
            with by_year_lock:
                by_year.setdefault(yr, [])
                by_year[yr] = [x for x in by_year[yr] if (x['ticker'], x['report_date']) != key]
                by_year[yr].append(rec)
            lookback = rec['lookback'].get('-1', {})
            elapsed = (datetime.now() - t0).total_seconds()
            rate = done / elapsed if elapsed > 0 else 0
            eta_min = (total - done) / rate / 60 if rate > 0 else 0
            print(f'  [{done}/{total}] {key[0]} {key[1]}: '
                  f'cv={lookback.get("total_call_volume",0)} pv={lookback.get("total_put_volume",0)} '
                  f'vrc={lookback.get("vol_ratio_calls")} iv={lookback.get("iv_atm")} '
                  f'move1d={rec["labels"].get("move_1d")} | {rate:.2f}/s ETA {eta_min:.0f}min')

    generated_at = datetime.now(timezone.utc).isoformat()
    years_written = []
    for yr, evs in sorted(by_year.items()):
        evs.sort(key=lambda x: (x['ticker'], x['report_date']))
        path = DATA_DIR / f'earnings_uoa_{yr}.json'
        path.write_text(json.dumps({
            'year': yr,
            'generated_at': generated_at,
            'source': 'polygon_advanced',
            'earnings_events': evs,
        }, separators=(',', ':')))
        size_mb = path.stat().st_size / 1024 / 1024
        print(f'  wrote {path.name}: {len(evs)} events, {size_mb:.2f} MB')
        years_written.append(yr)

    (DATA_DIR / 'earnings_uoa_index.json').write_text(json.dumps({
        'years': years_written,
        'generated_at': generated_at,
        'source': 'polygon_advanced',
    }, indent=2))
    print(f'  wrote earnings_uoa_index.json: {years_written}')


if __name__ == '__main__':
    main()

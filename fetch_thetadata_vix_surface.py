#!/usr/bin/env python3
"""Historical ~30DTE SPX smile snapshots — fuel for VIX decomposition research.

Per trading day in the data/spx inventory (2022-06 →):
  spot    = last SPX 1-min bar ≤ 15:45 ET (12:45 on half-days)
  expiry  = SPXW expiration nearest today+30 calendar days
  strikes = nearest listed strike to spot×m, m ∈ MONEYNESS (OTM side:
            puts ≤ ATM, calls ≥ ATM; both rights at the ATM point)
  quote   = last valid (bid,ask) in the 15 min up to the snap → mid
  iv      = Black-Scholes back-solve (same solver as the GXBF fetcher)

Output: data/vix_surface_daily.json.gz
  {date: {spot, exp: 'YYYY-MM-DD', dte, rows: [[k, 'P'|'C', bid, ask, iv], ...]}}

Matches the worker's go-forward `vix_surface_snap` capture (15:40-15:55 ET,
Schwab per-contract volatility) so the two sources splice into one series.
Idempotent: re-running skips dates already in the output file.
"""
from __future__ import annotations

import gzip
import io
import csv
import json
import sys
from datetime import date as ddate, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from fetch_thetadata_gxbf import SESSION, THETA, implied_vol, read_spx_bars, HALF_DAYS

OUT = Path(__file__).parent / 'data' / 'vix_surface_daily.json.gz'
SPX_DIR = Path(__file__).parent / 'data' / 'spx'
MONEYNESS = [0.85, 0.88, 0.90, 0.92, 0.94, 0.96, 0.98, 1.00, 1.02, 1.04, 1.06, 1.10]


def list_expirations() -> list[ddate]:
    r = SESSION.get(f'{THETA}/option/list/expirations',
                    params={'symbol': 'SPXW', 'format': 'json'}, timeout=30)
    r.raise_for_status()
    out = []
    for item in r.json().get('response', []):
        s = str(item['expiration'] if isinstance(item, dict) else item)
        digits = s.replace('-', '')                # accepts YYYYMMDD and YYYY-MM-DD
        out.append(ddate(int(digits[:4]), int(digits[4:6]), int(digits[6:8])))
    return sorted(set(out))


def list_strikes(exp_yyyymmdd: str) -> list[float]:
    r = SESSION.get(f'{THETA}/option/list/strikes',
                    params={'symbol': 'SPXW', 'expiration': exp_yyyymmdd, 'format': 'json'},
                    timeout=30)
    r.raise_for_status()
    out = []
    for item in r.json().get('response', []):
        out.append(float(item['strike'] if isinstance(item, dict) else item))
    return sorted(set(out))


def quote_at(date_yyyymmdd: str, exp_yyyymmdd: str, strike: float, right: str,
             t0: str, t1: str) -> tuple[float, float] | None:
    """Last valid (bid, ask) in [t0, t1] for one contract, or None."""
    try:
        r = SESSION.get(f'{THETA}/option/history/quote', params={
            'symbol': 'SPXW', 'expiration': exp_yyyymmdd, 'strike': f'{strike:.2f}',
            'right': right, 'date': date_yyyymmdd, 'interval': '1m',
            'start_time': t0, 'end_time': t1, 'format': 'csv',
        }, timeout=20)
    except Exception:
        return None
    if not r.ok or not r.text or r.text.startswith('<'):
        return None
    reader = csv.reader(io.StringIO(r.text))
    headers = next(reader, None)
    if not headers or 'bid' not in headers or 'ask' not in headers:
        return None
    bi, ai = headers.index('bid'), headers.index('ask')
    best = None
    for row in reader:
        if len(row) <= max(bi, ai):
            continue
        try:
            b, a = float(row[bi]), float(row[ai])
            if b >= 0 and a > 0 and a >= b:
                best = (b, a)          # rows are chronological — keep the last
        except ValueError:
            pass
    return best


def build_day(date_iso: str, expirations: list[ddate]) -> dict | None:
    ymd = date_iso.replace('-', '')
    d0 = ddate(int(date_iso[:4]), int(date_iso[5:7]), int(date_iso[8:10]))
    snap = '12:45' if date_iso in HALF_DAYS else '15:45'

    try:
        _, _, bars = read_spx_bars(date_iso)
    except Exception:
        return None
    spot = None
    hh, mm = int(snap[:2]), int(snap[3:])
    for back in range(0, 11):                      # snap or walk ≤10 min back
        t = mm - back
        cand = f'{hh - 1:02d}:{t + 60:02d}' if t < 0 else f'{hh:02d}:{t:02d}'
        if cand in bars:
            spot = bars[cand]
            break
    if not spot or spot <= 0:
        return None

    target = d0 + timedelta(days=30)
    future = [e for e in expirations if e > d0]
    if not future:
        return None
    exp = min(future, key=lambda e: abs((e - target).days))
    dte = (exp - d0).days
    if not (20 <= dte <= 45):                      # data gap guard
        return None
    exp_ymd = exp.strftime('%Y%m%d')

    try:
        strikes = list_strikes(exp_ymd)
    except Exception:
        return None
    if not strikes:
        return None

    t1 = f'{snap}:59.999'
    t0 = f'{hh:02d}:{mm - 15:02d}:00.000' if mm >= 15 else f'{hh - 1:02d}:{mm + 45:02d}:00.000'
    T = max(dte, 1) / 365.0
    rows, used = [], set()
    for m in MONEYNESS:
        k = min(strikes, key=lambda s: abs(s - spot * m))
        if abs(k - spot * m) > spot * 0.02:        # no strike within 2% of target
            continue
        for right in (['put'] if m < 0.999 else ['call'] if m > 1.001 else ['put', 'call']):
            if (k, right) in used:
                continue
            used.add((k, right))
            q = quote_at(ymd, exp_ymd, k, right, t0, t1)
            if not q:
                continue
            mid = (q[0] + q[1]) / 2
            iv = implied_vol(spot, k, T, mid, is_call=(right == 'call')) if mid > 0 else None
            rows.append([k, 'C' if right == 'call' else 'P', q[0], q[1],
                         round(iv * 100, 2) if iv else None])
    if len(rows) < 6:
        return None
    rows.sort(key=lambda r: (r[0], r[1]))
    return {'spot': round(spot, 2), 'exp': exp.isoformat(), 'dte': dte, 'rows': rows}


def main():
    days = sorted(p.stem.split('_')[1] for p in SPX_DIR.glob('SPX_*.csv'))
    days = [f'{d[:4]}-{d[4:6]}-{d[6:8]}' for d in days]
    done = {}
    if OUT.exists():
        with gzip.open(OUT, 'rt') as f:
            done = json.load(f)
    todo = [d for d in days if d not in done]
    print(f'{len(days)} session days, {len(done)} done, {len(todo)} to fetch', flush=True)

    expirations = list_expirations()
    n_ok = 0
    for i, d in enumerate(todo):
        rec = build_day(d, expirations)
        if rec:
            done[d] = rec
            n_ok += 1
        if (i + 1) % 20 == 0 or i == len(todo) - 1:
            with gzip.open(OUT, 'wt') as f:
                json.dump(done, f, separators=(',', ':'))
            print(f'{i + 1}/{len(todo)} (ok {n_ok}) {d}', flush=True)
    with gzip.open(OUT, 'wt') as f:
        json.dump(dict(sorted(done.items())), f, separators=(',', ':'))
    print(f'DONE: {len(done)} days in {OUT.name}', flush=True)


if __name__ == '__main__':
    main()

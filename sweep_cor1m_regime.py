#!/usr/bin/env python3
"""
Grid sweep over (time × threshold × delta × regime_set) to find the best
COR1M hedging configurations.

Caches all polygon snapshots + VIX intraday in memory, then runs ~hundreds
of backtests in seconds.

Outputs:
  - sweep_results.json   (full ranked list)
  - sweep_results.csv    (spreadsheet-friendly)
  - Top 10 prints by:    total P/L, P/L per trade, MAR-like (total / |worst|)

Usage:
  python3 sweep_cor1m_regime.py
"""
from __future__ import annotations
import csv, json, math, sys, time
from datetime import date
from pathlib import Path
from itertools import product

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from backtest_cor1m_put import (
    HALF_DAYS, US_HOLIDAYS, load_cor1m_daily, trading_dates, load_spx_close,
)
from regime_classifier import (
    load_vix_term, classify_series, RegimeThresholds, REGIMES,
    load_cor1m_open_and_close,
)
from backtest_cor1m_regime import put_delta, time_to_close


# ────────── caches ──────────
def cache_snapshots(times: list[str], dates: list[str]) -> dict[tuple[str,str], dict]:
    """Pre-load all polygon snapshots for (date, time) into memory."""
    poly = ROOT / 'data' / 'polygon'
    cache: dict[tuple[str, str], dict] = {}
    for d in dates:
        yyyymmdd = d.replace('-', '')
        for t in times:
            p = poly / f'SPX_{yyyymmdd}_{t}.json'
            if p.exists():
                try:
                    cache[(d, t)] = json.loads(p.read_text())
                except Exception:
                    pass
    return cache


def cache_vix_intraday(times: list[str], dates: list[str]) -> dict[tuple[str,str], float]:
    """Cache VIX close at each entry time per day."""
    vix_dir = ROOT / 'data' / 'vix'
    cache: dict[tuple[str, str], float] = {}
    for d in dates:
        path = vix_dir / f'VIX_{d.replace("-","")}.csv'
        if not path.exists():
            continue
        targets = {t: f'{d} {t[:2]}:{t[2:]}:00' for t in times}
        rows = []
        with open(path) as f:
            for r in csv.DictReader(f):
                rows.append((r['timestamp'], r['close']))
        for t, target in targets.items():
            last = None
            for ts, cls in rows:
                if ts <= target:
                    try:
                        v = float(cls)
                        if v > 0: last = v
                    except ValueError:
                        pass
                else:
                    break
            if last is not None:
                cache[(d, t)] = last
    return cache


def cache_spx_close(dates: list[str]) -> dict[str, float]:
    cache: dict[str, float] = {}
    for d in dates:
        v = load_spx_close(d)
        if v is not None: cache[d] = v
    return cache


# ────────── one backtest run (no IO) ──────────
def pick_put_cached(snap: dict, target_exp: str, target_delta: float,
                     sigma: float, T: float) -> dict | None:
    quotes = snap.get('quotes', {})
    spot = snap.get('spot', 0) or 0
    if not quotes or spot <= 0 or sigma <= 0: return None
    best = None; best_diff = float('inf')
    for tk, q in quotes.items():
        if 'P' not in tk: continue
        if q.get('expiration') != target_exp: continue
        K = q.get('strike', 0) or 0
        if K <= 0 or K >= spot: continue
        bid = q.get('bid', 0) or 0
        ask = q.get('ask', 0) or 0
        if bid <= 0 or ask <= 0 or ask < bid: continue
        d = put_delta(spot, K, T, sigma)
        diff = abs(d - target_delta)
        if diff < best_diff:
            best_diff = diff
            best = {'K': K, 'mid': (bid + ask) / 2, 'delta': d, 'spot': spot}
    return best


def run_one(dates, cor1m_open, cor1m_close, cls, snap_cache, vix_cache, spx_close,
             threshold, delta_target, time_tag, allowed_regimes):
    """NO-LOOK-AHEAD: uses 9:30 OPEN for today's trigger, prior days' CLOSE for prev."""
    state = 'WAITING'; prev_close = None
    trades = []
    cum = 0.0

    is_half_for = lambda d: d in HALF_DAYS

    for d in dates:
        c_open = cor1m_open.get(d)
        c_close = cor1m_close.get(d)

        if state == 'WAITING' and c_open is not None and c_open <= threshold:
            if prev_close is None or prev_close > threshold:
                state = 'TRIGGERED'

        if state == 'TRIGGERED':
            day_regime = cls.get(d, {}).get('regime', 'R0')
            if allowed_regimes and day_regime not in allowed_regimes:
                if c_close is not None: prev_close = c_close
                continue

            snap = snap_cache.get((d, time_tag))
            if snap is None:
                snap = snap_cache.get((d, '0945'))
                if snap is None:
                    if c_close is not None: prev_close = c_close
                    continue
                actual_time = '0945'
            else:
                actual_time = time_tag

            vix = vix_cache.get((d, actual_time))
            if vix is None or vix <= 0:
                if c_close is not None: prev_close = c_close
                continue
            sigma = vix / 100.0
            T = time_to_close(f'{actual_time[:2]}:{actual_time[2:]}', is_half_for(d))
            put = pick_put_cached(snap, d, delta_target, sigma, T)
            if put is None:
                if c_close is not None: prev_close = c_close
                continue
            spx_cls = spx_close.get(d)
            if spx_cls is None:
                if c_close is not None: prev_close = c_close
                continue
            intrinsic = max(put['K'] - spx_cls, 0)
            pnl = (intrinsic - put['mid']) * 100
            cum += pnl
            trades.append({'date': d, 'regime': day_regime, 'pnl': pnl, 'cum': cum,
                            'K': put['K'], 'mid': put['mid'], 'delta': put['delta']})

            if pnl > 0:
                state = 'WAITING'

        if c_close is not None: prev_close = c_close

    if not trades:
        return None
    pls = [t['pnl'] for t in trades]
    w = sum(1 for p in pls if p > 0); l = sum(1 for p in pls if p <= 0)
    cums = [t['cum'] for t in trades]
    peak = cums[0]; max_dd = 0.0
    for c in cums:
        peak = max(peak, c)
        max_dd = max(max_dd, peak - c)
    by_regime = {}
    for t in trades:
        r = t['regime']
        by_regime.setdefault(r, []).append(t['pnl'])

    return {
        'n': len(trades),
        'wr': w / len(trades) * 100,
        'total': sum(pls),
        'avg': sum(pls) / len(trades),
        'best': max(pls), 'worst': min(pls),
        'max_dd': max_dd,
        'mar': sum(pls) / max_dd if max_dd > 0 else None,
        'by_regime': {r: {'n': len(v), 'total': sum(v),
                          'wr': sum(1 for p in v if p > 0) / len(v) * 100}
                       for r, v in by_regime.items()},
    }


# ────────── main sweep ──────────
def main():
    print('Loading data (NO-LOOK-AHEAD: 9:30 opens for trigger + regime)...')
    cor1m_open, cor1m_close = load_cor1m_open_and_close()
    vt = load_vix_term()
    dates = trading_dates('2023-06-01', date.today().isoformat())
    cls = classify_series(dates, cor1m_open, cor1m_close, vt, RegimeThresholds())
    print(f'  {len(dates)} trading days  |  COR1M opens: {len(cor1m_open)}  closes: {len(cor1m_close)}')

    times = ['0935', '0945', '1000']
    print(f'  Caching snapshots @ {times}...')
    t0 = time.time()
    snap_cache = cache_snapshots(times, dates)
    print(f'  Snapshots cached: {len(snap_cache)} in {time.time()-t0:.1f}s')

    print(f'  Caching VIX intraday...')
    t0 = time.time()
    vix_cache = cache_vix_intraday(times, dates)
    print(f'  VIX cached: {len(vix_cache)} in {time.time()-t0:.1f}s')

    print(f'  Caching SPX closes...')
    spx_close = cache_spx_close(dates)
    print(f'  SPX closes: {len(spx_close)}')

    # ────────── grid ──────────
    THRESHOLDS = [7.0, 7.5, 8.0, 8.25, 8.5, 9.0, 9.5, 10.0]
    DELTAS = [-0.05, -0.10, -0.15, -0.20, -0.25, -0.30]
    TIMES = ['0935', '0945', '1000']
    REGIME_SETS = {
        'ALL': None,
        'NO_R1': {'R2','R3','R4','R0'},
        'R3_R4_ONLY': {'R3','R4'},
        'R4_ONLY': {'R4'},
        'R0_R3_R4': {'R0','R3','R4'},
        'EXCLUDE_R1_R2': {'R0','R3','R4'},
    }

    grid = list(product(TIMES, THRESHOLDS, DELTAS, REGIME_SETS.items()))
    print(f'\nGrid: {len(grid)} configs')

    rows = []
    t0 = time.time()
    for i, (tm, th, dl, (rg_name, rg_set)) in enumerate(grid, 1):
        r = run_one(dates, cor1m_open, cor1m_close, cls, snap_cache, vix_cache, spx_close,
                    th, dl, tm, rg_set)
        if r is None or r['n'] < 5:  # skip configs with too few trades to matter
            continue
        rows.append({
            'time': tm, 'threshold': th, 'delta': dl, 'regimes': rg_name,
            **{k: round(v, 2) if isinstance(v, float) else v
               for k, v in r.items() if k != 'by_regime'},
        })
        if i % 40 == 0:
            print(f'  {i}/{len(grid)}  ({time.time()-t0:.1f}s)')
    print(f'\nDone in {time.time()-t0:.1f}s — {len(rows)} valid configs')

    # ────────── rank ──────────
    def show_top(label, rows, key, reverse=True, n=10):
        print(f'\n{"="*110}')
        print(f'TOP {n} BY {label}')
        print(f'{"="*110}')
        print(f'{"rank":>4}  {"time":>5}  {"thr":>5}  {"delta":>6}  {"regimes":<14}  '
              f'{"n":>4}  {"WR%":>5}  {"avg":>8}  {"total":>9}  {"best":>8}  '
              f'{"worst":>8}  {"MaxDD":>7}  {"MAR":>6}')
        srt = sorted(rows, key=lambda r: r.get(key) if r.get(key) is not None else -float('inf'),
                     reverse=reverse)
        for i, r in enumerate(srt[:n], 1):
            mar = r.get('mar')
            mar_s = f'{mar:.2f}' if mar is not None else 'n/a'
            print(f'{i:>4}  {r["time"]:>5}  {r["threshold"]:>5}  {r["delta"]:>+6.2f}  '
                  f'{r["regimes"]:<14}  {r["n"]:>4}  {r["wr"]:>5.1f}  '
                  f'{r["avg"]:>+8,.0f}  {r["total"]:>+9,.0f}  {r["best"]:>+8,.0f}  '
                  f'{r["worst"]:>+8,.0f}  {r["max_dd"]:>7,.0f}  {mar_s:>6}')

    show_top('TOTAL P/L', rows, 'total')
    show_top('AVG P/L PER TRADE', rows, 'avg')
    show_top('MAR (Total / MaxDD)', rows, 'mar')
    show_top('FEWEST BAD TRADES (lowest |worst|)', rows, 'worst', reverse=False)

    # ────────── save ──────────
    out_json = ROOT / 'data' / 'cor1m' / 'sweep_results.json'
    out_json.write_text(json.dumps(rows, indent=2))
    print(f'\nSaved {len(rows)} configs → {out_json}')

    out_csv = ROOT / 'data' / 'cor1m' / 'sweep_results.csv'
    if rows:
        with open(out_csv, 'w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=rows[0].keys())
            w.writeheader(); w.writerows(rows)
        print(f'Saved CSV → {out_csv}')


if __name__ == '__main__':
    main()

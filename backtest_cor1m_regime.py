#!/usr/bin/env python3
"""
COR1M × Contango Regime Backtester

Builds on backtest_cor1m_put.py. New features:

  1. Entry time configurable — default 9:35 AM (was 9:45). Falls back to
     0945 snapshots when 0935 isn't cached yet.
  2. Regime gating — when --regimes R1,R2 is passed, only fire on those days.
  3. Black-Scholes-delta target selection (replaces premium-target which is
     spot-dependent and noisy).
  4. JSON output ready to feed cor1m_contango.html: equity curve, per-trade
     records with regime tag, per-regime summary, regime distribution.

Usage examples:

  # Default: 9:35 entry, delta -0.20, threshold 8.25
  python3 backtest_cor1m_regime.py

  # Only fire when regime is R1 (max complacency)
  python3 backtest_cor1m_regime.py --regimes R1

  # Compare entry times
  python3 backtest_cor1m_regime.py --time 0945
  python3 backtest_cor1m_regime.py --time 0935

Output: data/cor1m/regime_backtest_<time>.json
"""
from __future__ import annotations
import argparse, csv, json, math
from pathlib import Path
from datetime import date, timedelta

ROOT = Path(__file__).resolve().parent
COR1M_DIR = ROOT / 'data' / 'cor1m'
POLY_DIR = ROOT / 'data' / 'polygon'
SPX_DIR = ROOT / 'data' / 'spx'
VIX_DIR = ROOT / 'data' / 'vix'

# Re-use helpers from existing modules
import sys
sys.path.insert(0, str(ROOT))
from backtest_cor1m_put import (
    HALF_DAYS, US_HOLIDAYS, load_cor1m_daily, trading_dates, load_spx_close,
    next_trading_day,
)
from regime_classifier import (
    load_vix_term, classify_series, RegimeThresholds, REGIMES,
)


# ── Black-Scholes put delta ───────────────────────────────────────────────────
def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2)))


def put_delta(S: float, K: float, T: float, sigma: float, r: float = 0.05) -> float:
    """Black-Scholes put delta (no continuous dividend)."""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    return -(1.0 - _norm_cdf(d1))


def time_to_close(time_hhmm: str, is_half: bool) -> float:
    """Hours from entry time to market close, in years (252×6.5 trading day)."""
    h, m = map(int, time_hhmm.split(':'))
    close_h = 13 if is_half else 16
    minutes = (close_h - h) * 60 - m
    hours = minutes / 60.0
    return hours / (252 * 6.5)


def load_vix_intraday_at(date_iso: str, hhmm: str) -> float | None:
    """Read VIX close at/just before hhmm from data/vix/VIX_YYYYMMDD.csv."""
    yyyymmdd = date_iso.replace('-', '')
    path = VIX_DIR / f'VIX_{yyyymmdd}.csv'
    if not path.exists():
        return None
    target = f'{date_iso} {hhmm}:00'
    last = None
    with open(path) as f:
        for r in csv.DictReader(f):
            if r['timestamp'] <= target:
                try:
                    last = float(r['close'])
                except ValueError:
                    pass
            else:
                break
    return last


def load_snapshot(date_iso: str, time_tag: str) -> dict | None:
    """Try requested time_tag first, fall back to 0945 if missing."""
    yyyymmdd = date_iso.replace('-', '')
    primary = POLY_DIR / f'SPX_{yyyymmdd}_{time_tag}.json'
    if primary.exists():
        return json.loads(primary.read_text())
    fallback = POLY_DIR / f'SPX_{yyyymmdd}_0945.json'
    if fallback.exists():
        return json.loads(fallback.read_text())
    return None


def pick_put_by_delta(snapshot: dict, target_exp: str, target_delta: float,
                       sigma: float, T: float) -> dict | None:
    """Find OTM put with BS delta closest to target_delta (negative)."""
    quotes = snapshot.get('quotes', {})
    spot = snapshot.get('spot', 0) or 0
    if not quotes or spot <= 0 or sigma <= 0:
        return None
    best = None; best_diff = float('inf')
    for ticker, q in quotes.items():
        if 'P' not in ticker:
            continue
        if q.get('expiration') != target_exp:
            continue
        K = q.get('strike', 0) or 0
        if K <= 0 or K >= spot:
            continue
        bid = q.get('bid', 0) or 0
        ask = q.get('ask', 0) or 0
        if bid <= 0 or ask <= 0 or ask < bid:
            continue
        d = put_delta(spot, K, T, sigma)
        diff = abs(d - target_delta)
        if diff < best_diff:
            best_diff = diff
            mid = (bid + ask) / 2
            best = {
                'ticker': ticker, 'strike': K, 'bid': bid, 'ask': ask,
                'mid': round(mid, 2), 'delta': round(d, 4),
                'spot': spot, 'expiration': target_exp,
            }
    return best


def run(args) -> dict:
    print(f'Loading data...')
    cor1m = load_cor1m_daily()
    vt = load_vix_term()
    dates = trading_dates(args.from_date, args.to_date)
    print(f'  COR1M: {len(cor1m)} days  |  VIX-term: {len(vt)} days  |  Backtest: {len(dates)} days')

    # Build regime label per day (uses default thresholds for now)
    th = RegimeThresholds()
    if args.cor1m_low is not None:
        th.cor1m_low = args.cor1m_low
    if args.cor1m_high is not None:
        th.cor1m_high = args.cor1m_high
    cls = classify_series(dates, cor1m, vt, th)

    allowed = set(args.regimes.split(',')) if args.regimes else None
    if allowed:
        print(f'  Regime gating: {allowed}')

    state = 'WAITING'
    prev_c = None
    current_trigger = None
    trades = []
    triggers = []
    cum = 0.0
    skipped = {'no_snap': 0, 'no_vix': 0, 'no_put': 0, 'no_spx_close': 0, 'regime': 0}

    for d in dates:
        c = cor1m.get(d)
        # Crossing detection
        if state == 'WAITING' and c is not None and c <= args.threshold:
            if prev_c is None or prev_c > args.threshold:
                state = 'TRIGGERED'
                current_trigger = {
                    'trigger_date': d, 'cor1m_at_trigger': c,
                    'trades': [], 'total_pnl': 0.0,
                }
                triggers.append(current_trigger)

        if state == 'TRIGGERED':
            # Optional regime gating — skip days outside allowed regimes,
            # but DON'T reset the state machine (still firing the trigger event).
            day_regime = cls.get(d, {}).get('regime', 'R0')
            if allowed and day_regime not in allowed:
                skipped['regime'] += 1
                if c is not None: prev_c = c
                continue

            snap = load_snapshot(d, args.time)
            if snap is None:
                skipped['no_snap'] += 1
                if c is not None: prev_c = c
                continue

            time_hhmm = f'{args.time[:2]}:{args.time[2:]}'
            vix = load_vix_intraday_at(d, time_hhmm)
            if vix is None or vix <= 0:
                skipped['no_vix'] += 1
                if c is not None: prev_c = c
                continue
            sigma = vix / 100.0
            T = time_to_close(time_hhmm, d in HALF_DAYS)

            put = pick_put_by_delta(snap, d, args.delta, sigma, T)
            if put is None:
                skipped['no_put'] += 1
                if c is not None: prev_c = c
                continue
            spx_cls = load_spx_close(d)
            if spx_cls is None:
                skipped['no_spx_close'] += 1
                if c is not None: prev_c = c
                continue
            intrinsic = max(put['strike'] - spx_cls, 0)
            pnl = round((intrinsic - put['mid']) * 100, 2)
            cum = round(cum + pnl, 2)

            rec = {
                'date': d, 'regime': day_regime,
                'regime_label': REGIMES[day_regime]['label'],
                'cor1m': c, 'vix_open': vix,
                'spot': put['spot'], 'strike': put['strike'],
                'bid': put['bid'], 'ask': put['ask'], 'mid': put['mid'],
                'delta': put['delta'],
                'spx_close': spx_cls,
                'intrinsic': round(intrinsic, 2),
                'pnl': pnl, 'cum_pnl': cum,
                'trigger_date': current_trigger['trigger_date'],
            }
            trades.append(rec)
            current_trigger['trades'].append(rec)
            current_trigger['total_pnl'] = round(current_trigger['total_pnl'] + pnl, 2)

            if not args.exit_on_cross_up and pnl > 0:
                state = 'WAITING'
                current_trigger['profitable_date'] = d
                current_trigger['exit_reason'] = 'profitable'
                current_trigger['days_to_profit'] = len(current_trigger['trades'])
                current_trigger = None

        # Exit on cross-up
        if state == 'TRIGGERED' and args.exit_on_cross_up:
            if c is not None and c > args.threshold and prev_c is not None and prev_c <= args.threshold:
                current_trigger['exit_reason'] = f'COR1M crossed up to {c:.2f}'
                current_trigger['exit_date'] = d
                current_trigger['days_to_profit'] = len(current_trigger['trades'])
                state = 'WAITING'; current_trigger = None

        if c is not None:
            prev_c = c

    # Summary
    def stat(rows):
        if not rows: return {'n': 0}
        pls = [r['pnl'] for r in rows]
        w = sum(1 for p in pls if p > 0); l = sum(1 for p in pls if p <= 0)
        return {
            'n': len(rows),
            'wr': round(w / len(rows) * 100, 1),
            'avg': round(sum(pls) / len(rows), 2),
            'total': round(sum(pls), 2),
            'best': max(pls), 'worst': min(pls),
            'wins': w, 'losses': l,
        }

    summary = stat(trades)
    per_regime = {r: stat([t for t in trades if t['regime'] == r])
                  for r in ['R1', 'R2', 'R3', 'R4', 'R0']}
    distribution = {}
    for d, v in cls.items():
        distribution[v['regime']] = distribution.get(v['regime'], 0) + 1

    out = {
        'params': vars(args),
        'thresholds': vars(th),
        'summary': summary,
        'per_regime': per_regime,
        'regime_distribution': distribution,
        'triggers': triggers,
        'trades': trades,
        'skipped': skipped,
    }
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--from', dest='from_date', default='2023-06-01')
    p.add_argument('--to', dest='to_date', default=date.today().isoformat())
    p.add_argument('--threshold', type=float, default=8.25)
    p.add_argument('--delta', type=float, default=-0.20)
    p.add_argument('--time', default='0935', help='Entry time HHMM (default 0935, fallback 0945)')
    p.add_argument('--regimes', default='', help='Comma list, e.g. R1,R2 — empty means all')
    p.add_argument('--exit-on-cross-up', action='store_true')
    p.add_argument('--cor1m-low', type=float, default=None)
    p.add_argument('--cor1m-high', type=float, default=None)
    args = p.parse_args()

    out = run(args)
    out_path = COR1M_DIR / f'regime_backtest_{args.time}{"_"+args.regimes.replace(",","_") if args.regimes else ""}.json'
    out_path.write_text(json.dumps(out, indent=2))

    print(f'\n{"="*78}')
    print(f'COR1M REGIME BACKTEST  |  threshold {args.threshold}  delta {args.delta}'
          f'  time {args.time}'
          f'{"  regimes "+args.regimes if args.regimes else ""}')
    print(f'{"="*78}')
    s = out['summary']
    if s['n'] == 0:
        print('  No trades.')
    else:
        print(f'  Trades: {s["n"]}  |  WR {s["wr"]}%  |  Avg ${s["avg"]:+}  '
              f'|  Total ${s["total"]:+,.0f}  |  Best ${s["best"]:+,.0f}  Worst ${s["worst"]:+,.0f}')
    print(f'  Triggers: {len(out["triggers"])}')
    skp = out['skipped']
    if sum(skp.values()):
        print(f'  Skipped: ' + '  '.join(f'{k}={v}' for k,v in skp.items() if v))

    print(f'\n  Per regime:')
    for r in ['R1', 'R2', 'R3', 'R4', 'R0']:
        st = out['per_regime'][r]
        if st.get('n', 0) == 0: continue
        label = REGIMES[r]['label']
        print(f'    {r} {label:<18s} n={st["n"]:>3}  WR={st["wr"]:>5}%  '
              f'avg=${st["avg"]:>+6}  total=${st["total"]:>+8,.0f}')

    print(f'\n  Regime distribution ({sum(out["regime_distribution"].values())} days):')
    for r in ['R1', 'R2', 'R3', 'R4', 'R0']:
        n = out['regime_distribution'].get(r, 0)
        if n:
            print(f'    {r} {REGIMES[r]["label"]:<18s} {n:>4} days')

    print(f'\nSaved → {out_path}')


if __name__ == '__main__':
    main()

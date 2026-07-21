#!/usr/bin/env python3
"""
Build the JSON bundle cor1m_contango.html loads.

Pre-computes the backtest for several PRESETS so the page can switch between
them instantly. Each preset is one (threshold, delta, time, regimes) combo
that came out of the sweep as a top performer.

Bundle structure:
{
  "generated_at": "...",
  "daily": [ {date, cor1m, vix, vix3m, ..., regime, ...} ],
  "regimes": { R0..R4 info },
  "regime_distribution": { R0..R4 day counts },
  "thresholds": {...},
  "presets": [ {id, name, desc, threshold, delta, time, regimes, recommended} ],
  "preset_results": {
    "<preset_id>": {
      "summary": {n, wr, total, avg, best, worst, max_dd, mar},
      "per_regime": {R0..R4: {...}},
      "triggers": [...],
      "trades": [...]
    }
  }
}
"""
from __future__ import annotations
import argparse, json, sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from backtest_cor1m_put import load_cor1m_daily, trading_dates, load_spx_close, HALF_DAYS
import csv as _csv
from backtest_cor1m_put import SPX_DIR as _SPX_DIR

def _spx_at_945(date_iso: str):
    """Authoritative SPX at 09:45 ET from the 1-min bars (the close of the
    09:45 bar). Used to override the unreliable snapshot 'spot' field."""
    p = _SPX_DIR / f"SPX_{date_iso.replace('-', '')}.csv"
    if not p.exists():
        return None
    try:
        for row in _csv.DictReader(open(p)):
            if row['timestamp'][11:16] == '09:45':
                return float(row['close'])
    except Exception:
        return None
    return None
from regime_classifier import (
    load_vix_term, classify_series, RegimeThresholds, REGIMES,
    load_cor1m_open_and_close, load_cor1m_hourly_bars,
)
from backtest_cor1m_regime import (
    run as run_backtest,
    load_snapshot, load_vix_intraday_at, pick_put_by_delta,
    put_delta, time_to_close,
)


class A:
    def __init__(self, **kw): self.__dict__.update(kw)


# ─── presets — ANY-CROSS TRIGGER + FIX C + NO-LOOK-AHEAD (rebuilt 2026-06-08) ───
# Trigger: any cross-down in COR1M hourly bars (intraday OR overnight)
#   - Cross at 9:30 bar → trade SAME day at 9:45
#   - Cross at 10:30 or later → trade NEXT trading day at 9:45
# Fix C: require BS delta within ±0.10 of target; skip if no valid put
# No look-ahead: regime uses 9:30 opens only
# Conservative fills: entry mid rounded UP to $0.10, P/L floored to $10
PRESETS = [
    {
        'id': 'total_champ',
        'name': 'Total Champion',
        'desc': 'Most trades. thr 9.0, delta -0.20, VVIX<110 (corrected 2026-06-16, 2× VIX-down: +$81,610, n=103, MAR ~4). Higher bleed than the -0.10 wing.',
        'threshold': 9.0, 'delta': -0.20, 'time': '0945',
        'regimes': [], 'vvix_max': 110,
        'recommended': False,
    },
    {
        'id': 'sweet_spot',
        'name': 'Sweet Spot',
        'desc': 'Old default (thr 7.75, delta -0.20, VVIX<110). Corrected 2026-06-16, 2× VIX-down: +$29,900, MAR 2.12 — the -0.20 wing bleeds too much. Superseded by Convex Tail.',
        'threshold': 7.75, 'delta': -0.20, 'time': '0945',
        'regimes': [], 'vvix_max': 110,
        'recommended': False,
    },
    {
        'id': 'mar_champ',
        'name': 'MAR Champion',
        'desc': 'Best risk-adjusted. thr 7.75, delta -0.075, VVIX<110. 72 trades, +$71,670, MAR 14.75 (2× VIX-down sizing).',
        'threshold': 7.75, 'delta': -0.075, 'time': '0945',
        'regimes': [], 'vvix_max': 110,
        'recommended': False,
    },
    {
        'id': 'balanced',
        'name': 'Balanced',
        'desc': '★ RECOMMENDED (sweep winner 2026-06-16; 2 contracts on overnight-VIX-down days, else 1). thr 7.75, delta -0.10, VVIX<110. RE-ARM rule (owner 2026-07-20): after each profit it re-arms the next day while COR1M stays ≤ 7.75, so it never goes naked during a persistent complacency regime. +$79,550, 74 trades, worst day -$640. Near-flat vs the old exit-on-first-profit-then-wait-for-recross (+$79,340) but closes the "insured on entry, exposed while still complacent" gap at no cost to worst day or drawdown.',
        'threshold': 7.75, 'delta': -0.10, 'time': '0945',
        'regimes': [], 'vvix_max': 110, 'no_rearm': True,
        'recommended': True,
    },
    {
        'id': 'wide_champ',
        'name': 'Wide Net',
        'desc': 'Higher threshold catches more crosses. thr 9.5, delta -0.20, VVIX<110. 128 trades, +$95,070, MAR 5.51 (2× VIX-down sizing).',
        'threshold': 9.5, 'delta': -0.20, 'time': '0945',
        'regimes': [], 'vvix_max': 110,
        'recommended': False,
    },
    {
        'id': 'baseline',
        'name': 'Baseline (article)',
        'desc': 'Article\'s default settings. thr 8.25, delta -0.20, no VVIX filter. Validates the original concept.',
        'threshold': 8.25, 'delta': -0.20, 'time': '0945',
        'regimes': [], 'vvix_max': None,
        'recommended': False,
    },
]


def compute_max_dd_and_mar(trades):
    if not trades: return 0, None
    cums = []
    cum = 0
    for t in trades:
        cum += t['pnl']
        cums.append(cum)
    peak = cums[0]; mx = 0
    for c in cums:
        peak = max(peak, c)
        mx = max(mx, peak - c)
    mar = (sum(t['pnl'] for t in trades) / mx) if mx > 0 else None
    return round(mx, 2), round(mar, 2) if mar else None


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--from', dest='from_date', default='2022-06-01')
    p.add_argument('--to', dest='to_date', default=date.today().isoformat())
    p.add_argument('--cor1m-low', type=float, default=9.0)
    p.add_argument('--cor1m-high', type=float, default=20.0)
    p.add_argument('--out', default='cor1m_contango_bundle.json')
    args = p.parse_args()

    cor1m_open, cor1m_close = load_cor1m_open_and_close()
    vt = load_vix_term()
    dates = trading_dates(args.from_date, args.to_date)

    th = RegimeThresholds()
    th.cor1m_low = args.cor1m_low
    th.cor1m_high = args.cor1m_high
    cls = classify_series(dates, cor1m_open, cor1m_close, vt, th)

    # Daily rows (use 9:30 opens as the "live" values; expose EOD for forensic)
    daily = []
    for d in dates:
        v = cls.get(d, {})
        m = v.get('metrics', {})
        regime = v.get('regime', 'R0')
        vt_row = vt.get(d, {})
        daily.append({
            'date': d,
            'cor1m': m.get('cor1m'),                # 9:30 OPEN
            'cor1m_eod_close': cor1m_close.get(d),  # forensic
            'cor1m_5d_avg': m.get('cor1m_5d_avg'),
            'vix9d': m.get('vix9d'),                # 9:30 OPEN
            'vix': m.get('vix'),                    # 9:30 OPEN
            'vix3m': m.get('vix3m'),                # 9:30 OPEN
            'vix6m': vt_row.get('vix6m_open'),
            'vvix': vt_row.get('vvix_open'),
            'vix_vix3m_ratio': m.get('vix_vix3m_ratio'),
            'vix9d_vix_ratio': m.get('vix9d_vix_ratio'),
            'regime': regime,
            'regime_label': REGIMES.get(regime, {}).get('label', ''),
            'regime_color': REGIMES.get(regime, {}).get('color', ''),
        })

    print(f'Loading data... done. {len(daily)} days.')

    # Run each preset
    preset_results = {}
    print(f'\nRunning {len(PRESETS)} preset backtests...')
    for pr in PRESETS:
        regimes_str = ','.join(pr['regimes'])
        bt_args = A(
            from_date=args.from_date, to_date=args.to_date,
            threshold=pr['threshold'], delta=pr['delta'], time=pr['time'],
            regimes=regimes_str, exit_on_cross_up=False,
            no_rearm=pr.get('no_rearm', False),
            cor1m_low=args.cor1m_low, cor1m_high=args.cor1m_high,
            vvix_max=pr.get('vvix_max'),
        )
        bt = run_backtest(bt_args)
        trades = bt['trades']
        max_dd, mar = compute_max_dd_and_mar(trades)
        # Enrich summary
        sum_ = bt['summary'] or {}
        sum_['max_dd'] = max_dd
        sum_['mar'] = mar
        preset_results[pr['id']] = {
            'summary': sum_,
            'per_regime': bt['per_regime'],
            'triggers': bt['triggers'],
            'trades': trades,
            'skipped': bt['skipped'],
        }
        n = sum_.get('n', 0)
        tot = sum_.get('total', 0)
        wr = sum_.get('wr', 0)
        print(f'  {pr["id"]:<14} n={n:>3}  WR={wr:>5}%  total=${tot:>+8,.0f}  '
              f'MaxDD=${max_dd:>6,.0f}  MAR={mar}')

    # Regime distribution (same for every preset)
    distribution = {}
    for d, v in cls.items():
        distribution[v['regime']] = distribution.get(v['regime'], 0) + 1

    # ─── Sandbox: bake hourly COR1M + per-day picks at multiple deltas ───
    # Lets the static HTML page run arbitrary (threshold, delta, vvix_max,
    # regimes) backtests in JS without re-running Python.
    print('\nBuilding sandbox (hourly COR1M + per-day put picks at delta grid)...')
    delta_grid = [-0.05, -0.075, -0.10, -0.125, -0.15, -0.175, -0.20, -0.225, -0.25]
    hourly_bars = load_cor1m_hourly_bars()
    hourly_cor1m = [[ts, c] for (ts, c) in hourly_bars]
    print(f'  Hourly COR1M bars: {len(hourly_cor1m)}')

    # per-day overnight-VIX-down flag for the JS Custom-recompute path (the bundle
    # otherwise lacks vix_close). Matches the live rule: today's 9:30 VIX open below
    # the prior trading day's VIX close (user-approved 2026-07-07).
    _vt_dates = sorted(vt.keys())
    _vt_prev = {_vt_dates[i]: _vt_dates[i - 1] for i in range(1, len(_vt_dates))}
    def _vix_down(day):
        vo = vt.get(day, {}).get('vix_open')
        pc = vt.get(_vt_prev.get(day), {}).get('vix_close')
        return bool(vo is not None and pc is not None and vo < pc)

    per_day = {}
    n_with_picks = 0
    n_missing_snap = 0
    n_missing_vix = 0
    for i, d in enumerate(dates):
        if i % 100 == 0:
            print(f'  Sandbox progress: {i}/{len(dates)}  ({d})  '
                  f'days_done={len(per_day)}  with_picks={n_with_picks}')
        spx_cls = load_spx_close(d)
        snap = load_snapshot(d, '0945')
        # SPOT-OVERRIDE (2026-06-15 bug fix): the snapshot 'spot' field is
        # time-mismatched with its option quotes on ~22 recent (ThetaData-era)
        # days — off by up to 132 pts — corrupting BS-delta strike selection
        # (06-05 picked a ~5Δ 7400 put as if 20Δ). The authoritative 9:45
        # price is the data/spx 1-min bar; use it as spot.
        if snap is not None:
            _rs = _spx_at_945(d)
            if _rs and _rs > 0:
                snap['spot'] = _rs
        if snap is None:
            n_missing_snap += 1
            if spx_cls is not None:
                per_day[d] = {'spx_close': spx_cls, 'picks': {}, 'vix_down': _vix_down(d)}
            continue
        vix = load_vix_intraday_at(d, '09:45')
        if vix is None or vix <= 0:
            n_missing_vix += 1
            if spx_cls is not None:
                per_day[d] = {'spx_close': spx_cls, 'picks': {}, 'vix_down': _vix_down(d)}
            continue
        sigma = vix / 100.0
        T = time_to_close('09:45', d in HALF_DAYS)
        picks = {}
        any_valid = False
        for delta in delta_grid:
            put = pick_put_by_delta(snap, target_exp=d, target_delta=delta,
                                     sigma=sigma, T=T)
            if put is None:
                picks[str(delta)] = None
            else:
                picks[str(delta)] = {
                    'strike': put['strike'],
                    'bid': put['bid'],
                    'ask': put['ask'],
                    'mid': put['mid'],
                    'mid_raw': put.get('mid_raw', put['mid']),
                    'delta': put['delta'],
                    'spot': put['spot'],
                }
                any_valid = True
        # 2026-06-09 audit fix (P1 #12): also bake the 9:45 VIX so the JS
        # sandbox can display the SAME VIX value the Python baked presets show
        # in their trade records. Previously JS used dayObj.vix (9:30 open),
        # creating a confusing "same trade, different VIX" reproducibility gap
        # between baked-preset view and Custom-recompute view.
        per_day[d] = {'spx_close': spx_cls, 'vix_945': vix, 'picks': picks, 'vix_down': _vix_down(d)}
        if any_valid:
            n_with_picks += 1
    print(f'  Sandbox done: {len(per_day)} days in per_day, '
          f'{n_with_picks} with ≥1 valid pick, '
          f'{n_missing_snap} missing snapshot, {n_missing_vix} missing VIX.')

    sandbox = {
        'delta_grid': delta_grid,
        'hourly_cor1m': hourly_cor1m,
        'per_day': per_day,
    }

    bundle = {
        'generated_at': date.today().isoformat(),
        'thresholds': {
            'cor1m_low': th.cor1m_low, 'cor1m_high': th.cor1m_high,
            'contango_steep': th.contango_steep,
            'contango_flat_lo': th.contango_flat_lo,
            'contango_flat_hi': th.contango_flat_hi,
            'backwardation': th.backwardation,
            'front_inverting': th.front_inverting,
            'rising_delta': th.rising_delta,
        },
        'regimes': REGIMES,
        'regime_distribution': distribution,
        'daily': daily,
        'presets': PRESETS,
        'preset_results': preset_results,
        'default_preset': 'balanced',
        'sandbox': sandbox,
    }

    out_path = ROOT / args.out
    out_path.write_text(json.dumps(bundle, indent=None, separators=(',', ':')))
    print(f'\nBundle written: {out_path}')
    print(f'  Daily rows:    {len(daily)}')
    print(f'  Today:         {daily[-1]["date"]}  '
          f'cor1m={daily[-1]["cor1m"]}  '
          f'regime={daily[-1]["regime"]} ({daily[-1]["regime_label"]})')


if __name__ == '__main__':
    main()

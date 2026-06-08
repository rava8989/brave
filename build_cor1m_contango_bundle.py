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

from backtest_cor1m_put import load_cor1m_daily, trading_dates
from regime_classifier import (
    load_vix_term, classify_series, RegimeThresholds, REGIMES,
)
from backtest_cor1m_regime import run as run_backtest


class A:
    def __init__(self, **kw): self.__dict__.update(kw)


# ────────── presets from sweep results ──────────
PRESETS = [
    {
        'id': 'champion',
        'name': 'Champion',
        'desc': 'Most absolute return — fire on R3 + R4 only. 18 trades in 3 yrs, +$50.7k.',
        'threshold': 9.5, 'delta': -0.30, 'time': '0945',
        'regimes': ['R3', 'R4'],
        'recommended': True,
    },
    {
        'id': 'sweet_spot',
        'name': 'Sweet Spot',
        'desc': 'Best avg/trade. R4-only at -0.30 delta. 67% WR, +$4,654/trade.',
        'threshold': 9.0, 'delta': -0.30, 'time': '0945',
        'regimes': ['R4'],
        'recommended': True,
    },
    {
        'id': 'risk_adjusted',
        'name': 'Risk-Adjusted',
        'desc': 'Tiny drawdown (~$355). Cheap puts at -0.10 delta, R4 only. MAR 65.',
        'threshold': 9.0, 'delta': -0.10, 'time': '0945',
        'regimes': ['R4'],
        'recommended': False,
    },
    {
        'id': 'baseline',
        'name': 'Baseline (article)',
        'desc': 'Article\'s default: threshold 8.25, delta -0.20, no regime filter.',
        'threshold': 8.25, 'delta': -0.20, 'time': '0945',
        'regimes': [],
        'recommended': False,
    },
    {
        'id': 'broad',
        'name': 'Broad (no R1)',
        'desc': 'Wide trigger but skip R1 bleed days. 75 trades, less concentrated.',
        'threshold': 9.0, 'delta': -0.20, 'time': '0945',
        'regimes': ['R0', 'R2', 'R3', 'R4'],
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
    p.add_argument('--from', dest='from_date', default='2023-06-01')
    p.add_argument('--to', dest='to_date', default=date.today().isoformat())
    p.add_argument('--cor1m-low', type=float, default=9.0)
    p.add_argument('--cor1m-high', type=float, default=20.0)
    p.add_argument('--out', default='cor1m_contango_bundle.json')
    args = p.parse_args()

    cor1m = load_cor1m_daily()
    vt = load_vix_term()
    dates = trading_dates(args.from_date, args.to_date)

    th = RegimeThresholds()
    th.cor1m_low = args.cor1m_low
    th.cor1m_high = args.cor1m_high
    cls = classify_series(dates, cor1m, vt, th)

    # Daily rows
    daily = []
    for d in dates:
        v = cls.get(d, {})
        m = v.get('metrics', {})
        regime = v.get('regime', 'R0')
        daily.append({
            'date': d,
            'cor1m': m.get('cor1m'),
            'cor1m_5d_avg': m.get('cor1m_5d_avg'),
            'vix9d': m.get('vix9d'),
            'vix': m.get('vix'),
            'vix3m': m.get('vix3m'),
            'vix6m': vt.get(d, {}).get('vix6m'),
            'vvix': vt.get(d, {}).get('vvix'),
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
            cor1m_low=args.cor1m_low, cor1m_high=args.cor1m_high,
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
        'default_preset': 'champion',
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

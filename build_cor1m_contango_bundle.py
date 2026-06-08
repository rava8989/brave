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
    load_cor1m_open_and_close,
)
from backtest_cor1m_regime import run as run_backtest


class A:
    def __init__(self, **kw): self.__dict__.update(kw)


# ────────── presets — NO-LOOK-AHEAD (rebuilt 2026-06-08) ──────────
# Old presets (Champion R3+R4, Sweet Spot R4-only etc.) were inflated by EOD
# look-ahead bias — they assumed we knew today's CLOSE before trading at 9:35.
# These honest presets use 9:30 AM ET OPEN values (the live print at market open)
# for trigger + regime classification. Numbers are smaller but real.
PRESETS = [
    {
        'id': 'total_champ',
        'name': 'Total Champion',
        'desc': 'Most absolute return. thr 9.0, delta -0.25, no regime filter. 70 trades, +$40k, MAR 5.5.',
        'threshold': 9.0, 'delta': -0.25, 'time': '0935',
        'regimes': [],
        'recommended': True,
    },
    {
        'id': 'mar_champ',
        'name': 'MAR Champion',
        'desc': 'Best risk-adjusted. thr 8.0, delta -0.10, skip R1 bleed days. 43 trades, +$19.4k, MAR 10.',
        'threshold': 8.0, 'delta': -0.10, 'time': '0935',
        'regimes': ['R0', 'R2', 'R3', 'R4'],
        'recommended': True,
    },
    {
        'id': 'balanced',
        'name': 'Balanced',
        'desc': 'Cheap wide net. thr 8.25, delta -0.10. 91 trades, +$29.6k, MAR 7.5.',
        'threshold': 8.25, 'delta': -0.10, 'time': '0935',
        'regimes': [],
        'recommended': False,
    },
    {
        'id': 'baseline',
        'name': 'Baseline (article)',
        'desc': 'Article\'s literal default. thr 8.25, delta -0.20, no filter. 66 trades, +$33k, MAR 6.4.',
        'threshold': 8.25, 'delta': -0.20, 'time': '0935',
        'regimes': [],
        'recommended': False,
    },
    {
        'id': 'r3_r4_only',
        'name': 'R3+R4 (cautionary)',
        'desc': 'Fire only in R3/R4. Look-ahead version was +$50k — HONEST is -$11.5k. Don\'t use, kept to expose the bias.',
        'threshold': 9.5, 'delta': -0.30, 'time': '0935',
        'regimes': ['R3', 'R4'],
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

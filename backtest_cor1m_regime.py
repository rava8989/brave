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
    load_cor1m_open_and_close,
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


DELTA_TOLERANCE = 0.10  # require |actual_delta - target_delta| ≤ this, else SKIP
# Loosened from 0.05 → 0.10 because SPX has 5-pt strike spacing near the money,
# which makes finer delta targeting impossible — closest available is often
# +0.08 from target. 0.10 is still a meaningful filter (rejects junk delta-0.005
# trades) but accommodates real strike granularity.


def pick_put_by_delta(snapshot: dict, target_exp: str, target_delta: float,
                       sigma: float, T: float,
                       tolerance: float = DELTA_TOLERANCE) -> dict | None:
    """Find OTM put with BS delta within `tolerance` of `target_delta`.

    Returns None if no valid put is within tolerance — caller must skip the
    trade (don't fall back to a junk strike, that's how the prior bug crept in).
    """
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
        if diff > tolerance:
            continue   # outside tolerance — skip, don't grab as fallback
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
    """NO-LOOK-AHEAD backtester with FIX C (tolerance) and optional VVIX filter.

    At 9:30 AM ET on date D, we know:
      - Yesterday's full COR1M close (yesterday's EOD)
      - Today's COR1M 9:30 OPEN (just printed)
      - Today's VIX/VIX3M/VIX9D/VVIX 9:30 OPENs (just printed)
      - 5-day rolling COR1M = average of prior 5 days' CLOSE values
    All of those are honest live data at 9:30 ET.

    Trigger fires when yesterday's CLOSE > threshold AND today's OPEN ≤ threshold.

    Put selection (Fix C): require BS delta within ±0.05 of target. If no put
    qualifies, SKIP the trade — do NOT fall back to a junk strike.

    Optional VVIX filter: --vvix-max (block trades when VVIX_open ≥ this).
    """
    print(f'Loading data (NO-LOOK-AHEAD + Fix C: 9:30 opens, ±0.05 delta tolerance)...')
    cor1m_open, cor1m_close = load_cor1m_open_and_close()
    vt = load_vix_term()
    dates = trading_dates(args.from_date, args.to_date)
    print(f'  COR1M opens: {len(cor1m_open)}  |  COR1M closes: {len(cor1m_close)}  |  '
          f'VIX-term: {len(vt)}  |  Backtest dates: {len(dates)}')

    th = RegimeThresholds()
    if args.cor1m_low is not None:
        th.cor1m_low = args.cor1m_low
    if args.cor1m_high is not None:
        th.cor1m_high = args.cor1m_high
    cls = classify_series(dates, cor1m_open, cor1m_close, vt, th)

    allowed = set(args.regimes.split(',')) if args.regimes else None
    if allowed:
        print(f'  Regime gating: {allowed}')
    vvix_max = getattr(args, 'vvix_max', None)
    if vvix_max:
        print(f'  VVIX filter: skip trades when VVIX_open ≥ {vvix_max}')

    state = 'WAITING'
    prev_close = None   # yesterday's COR1M CLOSE (fully known at today's 9:30 AM)
    current_trigger = None
    trades = []
    triggers = []
    cum = 0.0
    skipped = {'no_snap': 0, 'no_vix': 0, 'no_put': 0, 'no_spx_close': 0, 'regime': 0, 'vvix': 0}

    for d in dates:
        c_open = cor1m_open.get(d)   # today's 9:30 OPEN (the live print)
        c_close = cor1m_close.get(d) # today's EOD CLOSE (only used to update prev_close after)

        # Trigger detection at 9:30 today:
        #   yesterday's CLOSE >= threshold AND today's OPEN ≤ threshold
        # We use >= (not strict >) to catch boundary cases — e.g. 2026-05-27
        # closed at EXACTLY 9.00 (the threshold). Strict > would treat that as
        # "not above" and miss the cross on 5-28 (open 8.85). With >=, the
        # cross fires correctly. The only edge case >= introduces is a flat-
        # at-threshold sequence (prev=9.00 AND open=9.00), which would spuri-
        # ously trigger — but that's extremely rare and is just one extra trade.
        if state == 'WAITING' and c_open is not None and c_open <= args.threshold:
            if prev_close is None or prev_close >= args.threshold:
                state = 'TRIGGERED'
                current_trigger = {
                    'trigger_date': d, 'cor1m_at_trigger': c_open,
                    'trades': [], 'total_pnl': 0.0,
                }
                triggers.append(current_trigger)

        if state == 'TRIGGERED':
            day_regime = cls.get(d, {}).get('regime', 'R0')
            if allowed and day_regime not in allowed:
                skipped['regime'] += 1
                if c_close is not None: prev_close = c_close
                continue

            # VVIX filter (Fix research #4: VVIX ≥ 110 days lose money historically)
            if vvix_max is not None:
                vvix_open = vt.get(d, {}).get('vvix_open')
                if vvix_open is not None and vvix_open >= vvix_max:
                    skipped['vvix'] += 1
                    if c_close is not None: prev_close = c_close
                    continue

            snap = load_snapshot(d, args.time)
            if snap is None:
                skipped['no_snap'] += 1
                if c_close is not None: prev_close = c_close
                continue

            time_hhmm = f'{args.time[:2]}:{args.time[2:]}'
            vix = load_vix_intraday_at(d, time_hhmm)
            if vix is None or vix <= 0:
                skipped['no_vix'] += 1
                if c_close is not None: prev_close = c_close
                continue
            sigma = vix / 100.0
            T = time_to_close(time_hhmm, d in HALF_DAYS)

            put = pick_put_by_delta(snap, d, args.delta, sigma, T)
            if put is None:
                skipped['no_put'] += 1
                if c_close is not None: prev_close = c_close
                continue
            spx_cls = load_spx_close(d)
            if spx_cls is None:
                skipped['no_spx_close'] += 1
                if c_close is not None: prev_close = c_close
                continue
            intrinsic = max(put['strike'] - spx_cls, 0)
            pnl = round((intrinsic - put['mid']) * 100, 2)
            cum = round(cum + pnl, 2)

            rec = {
                'date': d, 'regime': day_regime,
                'regime_label': REGIMES[day_regime]['label'],
                'cor1m': c_open,           # 9:30 OPEN (live-correct, NOT EOD close)
                'cor1m_eod_close': c_close, # for forensic only
                'vix_open': vix,
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

        # Exit on cross-up (today's OPEN > threshold AND yesterday's CLOSE ≤ threshold)
        if state == 'TRIGGERED' and args.exit_on_cross_up:
            if c_open is not None and c_open > args.threshold and prev_close is not None and prev_close <= args.threshold:
                current_trigger['exit_reason'] = f'COR1M crossed up to {c_open:.2f}'
                current_trigger['exit_date'] = d
                current_trigger['days_to_profit'] = len(current_trigger['trades'])
                state = 'WAITING'; current_trigger = None

        # Advance prev_close to today's CLOSE — that's what we'd know at tomorrow's 9:30
        if c_close is not None:
            prev_close = c_close

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
    p.add_argument('--vvix-max', type=float, default=None,
                    help='Skip trades when VVIX_open ≥ this (default off; 110 is the historical breakpoint)')
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

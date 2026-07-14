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
    load_cor1m_open_and_close, load_cor1m_hourly_bars, detect_cross_entries,
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


def _authoritative_spot_at(date_iso: str, time_tag: str):
    """SPX from the 1-min bars at the snapshot's intent time (e.g. '0945' →
    09:45 bar close). The snapshot's own 'spot' field is time-mismatched with
    its option quotes on ~22 recent days (off up to 132 pts) — corrupting
    BS-delta strike selection. The 1-min bar is authoritative. (2026-06-15)"""
    hhmm = f'{time_tag[:2]}:{time_tag[2:]}' if len(time_tag) == 4 and time_tag.isdigit() else '09:45'
    p = SPX_DIR / f"SPX_{date_iso.replace('-', '')}.csv"
    if not p.exists():
        return None
    try:
        for row in csv.DictReader(open(p)):
            if row['timestamp'][11:16] == hhmm:
                return float(row['close'])
    except Exception:
        return None
    return None


def load_snapshot(date_iso: str, time_tag: str) -> dict | None:
    """Try requested time_tag first, fall back to 0945 if missing. Overrides
    the unreliable 'spot' field with the authoritative 1-min bar."""
    yyyymmdd = date_iso.replace('-', '')
    snap = None
    primary = POLY_DIR / f'SPX_{yyyymmdd}_{time_tag}.json'
    fallback = POLY_DIR / f'SPX_{yyyymmdd}_0945.json'
    if primary.exists():
        snap = json.loads(primary.read_text())
    elif fallback.exists():
        snap = json.loads(fallback.read_text())
        time_tag = '0945'
    if snap is not None:
        rs = _authoritative_spot_at(date_iso, time_tag)
        if rs and rs > 0:
            snap['spot'] = rs
    return snap


DELTA_TOLERANCE = 0.15  # accept nearest put within ±0.15 of target (so a 20Δ
                        # target takes ~10–35Δ "nearby" — user 2026-06-15: don't
                        # be strict, just trade the nearest available strike).

# ────────── Real-world fill assumptions ──────────
import math as _math
def round_mid_up(mid: float) -> float:
    """Round entry premium UP to nearest $0.10 — accounts for paying ask + slippage.
    e.g. $0.72 → $0.80, $1.43 → $1.50, $2.87 → $2.90."""
    return _math.ceil(mid * 10) / 10

def round_pnl_down(pnl: float) -> int:
    """Floor P/L to nearest $10 — accounts for commissions ($1-3) + exit slippage.
    e.g. +$5,003 → +$5,000, -$78 → -$80 (losses get slightly worse, wins get slightly cut)."""
    return int(_math.floor(pnl / 10) * 10)


def _bs_put_price(S, K, T, sigma, r=0.05):
    if T <= 0 or sigma <= 0:
        return max(0.0, K - S)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return K * math.exp(-r * T) * _norm_cdf(-d2) - S * _norm_cdf(-d1)


def _implied_vol_put(S, K, T, price, r=0.05):
    """Back-solve each put's OWN implied vol from its mid (bisection). 0DTE
    options don't price off the 30-day VIX — using the strike's market-implied
    vol gives a delta consistent with the real chain (2026-06-15)."""
    intrinsic = max(0.0, K - S)
    if price <= intrinsic + 1e-6 or T <= 0:
        return None
    lo, hi = 0.001, 3.0
    if _bs_put_price(S, K, T, hi, r) < price:
        return None
    for _ in range(60):
        mid = 0.5 * (lo + hi)
        if _bs_put_price(S, K, T, mid, r) > price:
            hi = mid
        else:
            lo = mid
    return 0.5 * (lo + hi)


def pick_put_by_delta(snapshot: dict, target_exp: str, target_delta: float,
                       sigma: float, T: float,
                       tolerance: float = DELTA_TOLERANCE) -> dict | None:
    """Find OTM put whose MARKET-IMPLIED delta is within `tolerance` of
    `target_delta`. Each put's vol is back-solved from its own mid (smile-aware,
    matches the live Schwab deltas) — falling back to `sigma` only if the solve
    fails. Returns None if none qualify (no junk-strike fallback).
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
        mid_for_iv = (bid + ask) / 2
        iv = _implied_vol_put(spot, K, T, mid_for_iv)
        d = put_delta(spot, K, T, iv if iv else sigma)
        diff = abs(d - target_delta)
        if diff > tolerance:
            continue   # outside tolerance — skip, don't grab as fallback
        if diff < best_diff:
            best_diff = diff
            mid_raw = (bid + ask) / 2
            best = {
                'ticker': ticker, 'strike': K, 'bid': bid, 'ask': ask,
                'mid_raw': round(mid_raw, 2),         # for forensic
                'mid': round_mid_up(mid_raw),          # what we actually pay (entry slippage)
                'delta': round(d, 4),
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
    print(f'Loading data (NO-LOOK-AHEAD + Fix C + any-cross trigger)...')
    cor1m_open, cor1m_close = load_cor1m_open_and_close()
    cor1m_bars = load_cor1m_hourly_bars()
    vt = load_vix_term()
    # Overnight-VIX-down sizing (user-approved 2026-07-07): 2 contracts when today's
    # 9:30 VIX open is BELOW the prior trading day's VIX close, else 1. Matches the
    # live rule (oNight = vixYClose − vixToday > 0) and the restated history. vt is
    # keyed by trading date (holidays skipped) → prev key = prior session's close.
    _vt_dates = sorted(vt.keys())
    _vt_prev = {_vt_dates[i]: _vt_dates[i - 1] for i in range(1, len(_vt_dates))}
    def is_vix_down(day):
        vo = vt.get(day, {}).get('vix_open')
        pc = vt.get(_vt_prev.get(day), {}).get('vix_close')
        return vo is not None and pc is not None and vo < pc
    def tail_contracts(day):
        # --tail-mult N on VIX-down days (default 2 = the live rule), else 1.
        return getattr(args, 'tail_mult', 2) if is_vix_down(day) else 1
    dates = trading_dates(args.from_date, args.to_date)
    # Detect ALL cross-down events (intraday + overnight). Same-day entry if cross at 9:30,
    # otherwise next trading day at 9:45.
    entry_days = detect_cross_entries(cor1m_bars, args.threshold, US_HOLIDAYS)
    print(f'  COR1M opens: {len(cor1m_open)}  closes: {len(cor1m_close)}  bars: {len(cor1m_bars)}  '
          f'cross entries: {len(entry_days)}  |  VIX-term: {len(vt)}  dates: {len(dates)}')

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
        c_open = cor1m_open.get(d)   # today's 9:30 OPEN (live print, used for record)
        c_close = cor1m_close.get(d) # today's EOD CLOSE (forensic only)

        # Trigger detection: any cross-down in COR1M hourly bars maps to an entry day.
        # Entry day = same day if cross was at 9:30; otherwise next trading day.
        # State machine: only fire from WAITING; absorbed crosses don't restart.
        # --no-rearm (research, owner 2026-07-14): a LEVEL also fires — the day
        # after a profitable exit re-triggers immediately if COR1M (9:30 open,
        # live-knowable) is still ≤ threshold. No cross-from-above required.
        no_rearm_fire = getattr(args, 'no_rearm', False) and c_open is not None and c_open <= args.threshold
        if state == 'WAITING' and (d in entry_days or no_rearm_fire):
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

            # --vix-down-only (research): skip VIX-up mornings entirely. Uses the
            # independent is_vix_down (NOT tail_contracts==1 — with --tail-mult 1
            # every day sizes 1 and that comparison silently skips everything).
            if getattr(args, 'vix_down_only', False) and not is_vix_down(d):
                skipped['not_vix_down'] = skipped.get('not_vix_down', 0) + 1
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
            contracts = tail_contracts(d)             # 2 on overnight-VIX-down days, else 1
            pnl_raw = (intrinsic - put['mid']) * 100 * contracts  # entry mid already rounded UP
            pnl = round_pnl_down(pnl_raw)             # floor to nearest $10 (commissions+exit slip)
            cum = cum + pnl

            rec = {
                'date': d, 'regime': day_regime,
                'regime_label': REGIMES[day_regime]['label'],
                'cor1m': c_open,           # 9:30 OPEN (live-correct, NOT EOD close)
                'cor1m_eod_close': c_close, # for forensic only
                'vix_open': vix,
                'contracts': contracts,     # 2 = overnight-VIX-down sizing
                'spot': put['spot'], 'strike': put['strike'],
                'bid': put['bid'], 'ask': put['ask'],
                'mid': put['mid'],          # entry premium ROUNDED UP to $0.10
                'mid_raw': put.get('mid_raw', put['mid']),  # original (bid+ask)/2 for forensic
                'delta': put['delta'],
                'spx_close': spx_cls,
                'intrinsic': round(intrinsic, 2),
                'pnl': pnl,                # P/L FLOORED to $10 (commissions + slippage cushion)
                'pnl_raw': round(pnl_raw, 2),  # uncrunched P/L for forensic
                'cum_pnl': cum,
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

        # Exit on cross-up (today's OPEN >= threshold AND yesterday's CLOSE <= threshold)
        # SYMMETRIC to cross-down: both use >= / <= for boundary safety.
        # Cross-down uses (prev_close >= threshold AND c_open <= threshold) — see §15 in methodology.
        if state == 'TRIGGERED' and args.exit_on_cross_up:
            if c_open is not None and c_open >= args.threshold and prev_close is not None and prev_close <= args.threshold:
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
    p.add_argument('--tail-mult', dest='tail_mult', type=int, default=2,
                    help='Contracts on overnight-VIX-down days (default 2 = live rule; 1 = flat sizing)')
    p.add_argument('--vix-down-only', dest='vix_down_only', action='store_true',
                    help='Research: trade ONLY overnight-VIX-down days (skip VIX-up mornings entirely)')
    p.add_argument('--no-rearm', dest='no_rearm', action='store_true',
                    help='Research: no re-arm — re-trigger immediately any day COR1M open ≤ threshold (level, not cross)')
    args = p.parse_args()

    out = run(args)
    out_path = COR1M_DIR / f'regime_backtest_{args.time}{"_"+args.regimes.replace(",","_") if args.regimes else ""}{"_norearm" if args.no_rearm else ""}.json'
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

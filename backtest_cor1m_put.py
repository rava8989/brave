"""
COR1M Correlation Put Backtester

Strategy:
  When CBOE COR1M (1-Month Implied Correlation Index) crosses below a threshold
  (default 8), buy a SPXW put costing ~$1 every trading day until a trade
  is profitable. Then reset and wait for the next crossing.

Modes:
  --mode 0dte   Buy 0DTE put at 9:45 AM, expires same day (default)
  --mode 1dte   Buy 1DTE put at 3:30 PM, expires next trading day (overnight)

Usage:
  python3 backtest_cor1m_put.py
  python3 backtest_cor1m_put.py --mode 1dte
  python3 backtest_cor1m_put.py --threshold 10 --target-premium 0.50
"""
from __future__ import annotations
import csv, json, argparse, os
from pathlib import Path
from datetime import date, timedelta

ROOT = Path(__file__).resolve().parent
COR1M_DIR = ROOT / 'data' / 'cor1m'
POLYGON_DIR = ROOT / 'data' / 'polygon'
SPX_DIR = ROOT / 'data' / 'spx'

HALF_DAYS = {
    '2023-07-03', '2023-11-24', '2023-12-24',
    '2024-07-03', '2024-11-29', '2024-12-24',
    '2025-07-03', '2025-11-28', '2025-12-24',
    '2026-07-02', '2026-11-27', '2026-12-24',
}

US_HOLIDAYS = {
    '2022-06-20', '2022-07-04', '2022-09-05', '2022-11-24', '2022-12-26',
    '2023-01-02', '2023-01-16', '2023-02-20', '2023-04-07', '2023-05-29',
    '2023-06-19', '2023-07-04', '2023-09-04', '2023-11-23', '2023-12-25',
    '2024-01-01', '2024-01-15', '2024-02-19', '2024-03-29', '2024-05-27',
    '2024-06-19', '2024-07-04', '2024-09-02', '2024-11-28', '2024-12-25',
    '2025-01-01', '2025-01-09', '2025-01-20', '2025-02-17', '2025-04-18', '2025-05-26',
    '2025-06-19', '2025-07-04', '2025-09-01', '2025-11-27', '2025-12-25',
    '2026-01-01', '2026-01-19', '2026-02-16', '2026-04-03', '2026-05-25',
    '2026-06-19', '2026-07-03', '2026-09-07', '2026-11-26', '2026-12-25',
}


def load_cor1m_daily() -> dict[str, float]:
    """Load COR1M daily closes from cached hourly CSV files."""
    daily: dict[str, float] = {}
    for f in sorted(COR1M_DIR.glob('raw_*.csv')):
        with open(f) as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                ts = row['timestamp']
                d = ts[:10]
                close = float(row['close'])
                if close > 0:
                    daily[d] = close
    return daily


def trading_dates(start: str, end: str) -> list[str]:
    """Generate trading dates (skip weekends and holidays)."""
    d = date.fromisoformat(start)
    end_d = date.fromisoformat(end)
    out = []
    while d <= end_d:
        iso = d.isoformat()
        if d.weekday() < 5 and iso not in US_HOLIDAYS:
            out.append(iso)
        d += timedelta(days=1)
    return out


def load_polygon_snapshot(date_iso: str, time_tag: str = '0945') -> dict | None:
    """Load an SPX option snapshot for a date at a given time."""
    yyyymmdd = date_iso.replace('-', '')
    path = POLYGON_DIR / f'SPX_{yyyymmdd}_{time_tag}.json'
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)


def load_spx_close(date_iso: str) -> float | None:
    """Get SPX settlement price from 1-min bars (last bar close)."""
    yyyymmdd = date_iso.replace('-', '')
    path = SPX_DIR / f'SPX_{yyyymmdd}.csv'
    if not path.exists():
        return None
    last_close = None
    is_half = date_iso in HALF_DAYS
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts = row['timestamp']
            hhmm = ts[11:16]
            if is_half and hhmm > '13:00':
                break
            close = float(row['close'])
            if close > 0:
                last_close = close
    return last_close


def next_trading_day(date_iso: str) -> str:
    """Return the next trading day after date_iso."""
    d = date.fromisoformat(date_iso) + timedelta(days=1)
    while d.weekday() >= 5 or d.isoformat() in US_HOLIDAYS:
        d += timedelta(days=1)
    return d.isoformat()


def find_dollar_put(data: dict, target_exp: str, target: float, tolerance: float) -> dict | None:
    """Find an OTM put expiring on target_exp closest to target premium (mid price)."""
    quotes = data.get('quotes', {})
    spot = data.get('spot', 0)
    if not quotes or spot <= 0:
        return None

    best = None
    best_diff = float('inf')

    for ticker, q in quotes.items():
        if 'P' not in ticker:
            continue
        exp = q.get('expiration', '')
        if exp != target_exp:
            continue
        strike = q.get('strike', 0)
        if strike >= spot:
            continue
        bid = q.get('bid', 0)
        ask = q.get('ask', 0)
        if bid <= 0 or ask <= 0 or ask < bid:
            continue
        mid = (bid + ask) / 2
        diff = abs(mid - target)
        if diff < best_diff and (target - tolerance) <= mid <= (target + tolerance):
            best_diff = diff
            best = {
                'ticker': ticker, 'strike': strike, 'bid': bid,
                'ask': ask, 'mid': round(mid, 2), 'spot': spot,
                'expiration': exp,
            }
    return best


def run_backtest(cor1m: dict[str, float], dates: list[str],
                 threshold: float, target_premium: float, tolerance: float,
                 mode: str = '0dte', exit_on_cross_up: bool = False) -> dict:
    """Run the COR1M put backtest state machine.

    mode='0dte': buy at 9:45 AM, 0DTE put, expires same day
    mode='1dte': buy at 3:30 PM, 1DTE put, expires next trading day (overnight)
    exit_on_cross_up: if True, stop buying when COR1M crosses back above threshold
                      (instead of waiting for a profitable trade)
    """
    is_1dte = mode == '1dte'
    time_tag = '1530' if is_1dte else '0945'

    WAITING, TRIGGERED = 'WAITING', 'TRIGGERED'
    state = WAITING
    trades = []
    trigger_events = []
    current_trigger = None
    cum_pnl = 0.0
    prev_cor1m = None
    skipped = {'no_snapshot': 0, 'no_put': 0, 'no_spx_close': 0}

    for i, d in enumerate(dates):
        c = cor1m.get(d)

        if state == WAITING:
            if c is not None and c <= threshold:
                if prev_cor1m is None or prev_cor1m > threshold:
                    state = TRIGGERED
                    current_trigger = {
                        'trigger_date': d, 'cor1m_at_trigger': c,
                        'trades': [], 'total_pnl': 0.0,
                    }
                    trigger_events.append(current_trigger)

        if state == TRIGGERED:
            # Exit on COR1M crossing back above threshold
            if exit_on_cross_up and c is not None and c > threshold:
                if prev_cor1m is not None and prev_cor1m <= threshold:
                    current_trigger['exit_reason'] = f'COR1M crossed up to {c:.2f}'
                    current_trigger['exit_date'] = d
                    current_trigger['days_to_profit'] = len(current_trigger['trades'])
                    state = WAITING
                    current_trigger = None
                    prev_cor1m = c
                    continue

            # Skip half-days for 1DTE (3:30 PM snapshot won't exist)
            if is_1dte and d in HALF_DAYS:
                if c is not None:
                    prev_cor1m = c
                continue

            polygon = load_polygon_snapshot(d, time_tag)
            if polygon is None:
                skipped['no_snapshot'] += 1
                if c is not None:
                    prev_cor1m = c
                continue

            # For 0DTE: expiration = today. For 1DTE: expiration = next trading day.
            target_exp = next_trading_day(d) if is_1dte else d

            put = find_dollar_put(polygon, target_exp, target_premium, tolerance)
            if put is None:
                skipped['no_put'] += 1
                if c is not None:
                    prev_cor1m = c
                continue

            # Settlement: for 0DTE use same-day close, for 1DTE use next-day close
            settle_date = target_exp
            spx_close = load_spx_close(settle_date)
            if spx_close is None:
                skipped['no_spx_close'] += 1
                if c is not None:
                    prev_cor1m = c
                continue

            intrinsic = max(put['strike'] - spx_close, 0)
            pnl = round((intrinsic - put['mid']) * 100, 2)
            cum_pnl = round(cum_pnl + pnl, 2)
            profitable = pnl > 0

            trade = {
                'date': d,
                'cor1m': c,
                'spot': put['spot'],
                'strike': put['strike'],
                'bid': put['bid'],
                'ask': put['ask'],
                'mid': put['mid'],
                'expiration': target_exp,
                'spx_close': spx_close,
                'intrinsic': round(intrinsic, 2),
                'pnl': pnl,
                'cum_pnl': cum_pnl,
                'trigger_date': current_trigger['trigger_date'],
            }
            trades.append(trade)
            current_trigger['trades'].append(trade)
            current_trigger['total_pnl'] = round(
                current_trigger['total_pnl'] + pnl, 2)

            if not exit_on_cross_up and profitable:
                state = WAITING
                current_trigger['profitable_date'] = d
                current_trigger['exit_reason'] = 'profitable'
                current_trigger['days_to_profit'] = len(current_trigger['trades'])
                current_trigger = None

        if c is not None:
            prev_cor1m = c

    wins = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] <= 0]

    summary = {
        'total_trades': len(trades),
        'winning_trades': len(wins),
        'losing_trades': len(losses),
        'win_rate': round(len(wins) / len(trades), 3) if trades else 0,
        'total_pnl': cum_pnl,
        'avg_win': round(sum(t['pnl'] for t in wins) / len(wins), 2) if wins else 0,
        'avg_loss': round(sum(t['pnl'] for t in losses) / len(losses), 2) if losses else 0,
        'max_single_win': max((t['pnl'] for t in trades), default=0),
        'max_single_loss': min((t['pnl'] for t in trades), default=0),
        'trigger_events': len(trigger_events),
        'skipped': skipped,
    }

    return {'summary': summary, 'trigger_events': trigger_events, 'trades': trades}


def print_results(results: dict, threshold: float, target: float, mode: str = '0dte'):
    """Print formatted backtest results."""
    trades = results['trades']
    summary = results['summary']
    triggers = results['trigger_events']
    mode_label = '1DTE overnight @ 3:30 PM' if mode == '1dte' else '0DTE @ 9:45 AM'

    print(f'\n{"="*130}')
    print(f'COR1M PUT BACKTEST — {mode_label} | Threshold: {threshold} | Target Premium: ${target:.2f}')
    print(f'{"="*130}')

    if not trades:
        print('No trades found.')
        return

    if mode == '1dte':
        print(f'\n{"#":>3} {"Entry":>10} {"Expires":>10} {"Trigger":>10} {"COR1M":>7} {"Spot":>9} '
              f'{"Strike":>8} {"Bid":>6} {"Ask":>6} {"Mid":>6} '
              f'{"SPX Cls":>9} {"Intrins":>8} {"P&L":>9} {"Cum P&L":>10}')
        print('-' * 130)
    else:
        print(f'\n{"#":>3} {"Date":>10} {"Trigger":>10} {"COR1M":>7} {"Spot":>9} '
              f'{"Strike":>8} {"Bid":>6} {"Ask":>6} {"Mid":>6} '
              f'{"SPX Cls":>9} {"Intrins":>8} {"P&L":>9} {"Cum P&L":>10}')
        print('-' * 120)

    for i, t in enumerate(trades, 1):
        cor1m_s = f'{t["cor1m"]:.2f}' if t['cor1m'] is not None else 'N/A'
        if mode == '1dte':
            print(f'{i:>3} {t["date"]:>10} {t["expiration"]:>10} {t["trigger_date"]:>10} {cor1m_s:>7} '
                  f'{t["spot"]:>9.2f} {t["strike"]:>8.0f} {t["bid"]:>6.2f} '
                  f'{t["ask"]:>6.2f} {t["mid"]:>6.2f} {t["spx_close"]:>9.2f} '
                  f'{t["intrinsic"]:>8.2f} {t["pnl"]:>9.2f} {t["cum_pnl"]:>10.2f}')
        else:
            print(f'{i:>3} {t["date"]:>10} {t["trigger_date"]:>10} {cor1m_s:>7} '
                  f'{t["spot"]:>9.2f} {t["strike"]:>8.0f} {t["bid"]:>6.2f} '
                  f'{t["ask"]:>6.2f} {t["mid"]:>6.2f} {t["spx_close"]:>9.2f} '
                  f'{t["intrinsic"]:>8.2f} {t["pnl"]:>9.2f} {t["cum_pnl"]:>10.2f}')

    print(f'\n{"="*80}')
    print(f'SUMMARY')
    print(f'{"="*80}')
    print(f'  Total trades:     {summary["total_trades"]}')
    print(f'  Winners:          {summary["winning_trades"]}')
    print(f'  Losers:           {summary["losing_trades"]}')
    print(f'  Win rate:         {summary["win_rate"]*100:.1f}%')
    print(f'  Total P&L:        ${summary["total_pnl"]:.2f}')
    print(f'  Avg win:          ${summary["avg_win"]:.2f}')
    print(f'  Avg loss:         ${summary["avg_loss"]:.2f}')
    print(f'  Max single win:   ${summary["max_single_win"]:.2f}')
    print(f'  Max single loss:  ${summary["max_single_loss"]:.2f}')
    print(f'  Trigger events:   {summary["trigger_events"]}')
    sk = summary['skipped']
    if any(sk.values()):
        print(f'  Skipped days:     no_snapshot={sk["no_snapshot"]} no_put={sk["no_put"]} no_spx={sk["no_spx_close"]}')

    print(f'\n{"="*80}')
    print(f'TRIGGER EVENTS')
    print(f'{"="*80}')
    for te in triggers:
        reason = te.get('exit_reason', 'STILL OPEN')
        days = te.get('days_to_profit', len(te['trades']))
        print(f'  {te["trigger_date"]} COR1M={te["cor1m_at_trigger"]:.2f} '
              f'→ {days} trades → P&L ${te["total_pnl"]:.2f} → {reason}')


def main():
    ap = argparse.ArgumentParser(description='COR1M Correlation Put Backtester')
    ap.add_argument('--from', dest='from_date', default='2023-06-01')
    ap.add_argument('--to', dest='to_date', default='2026-06-06')
    ap.add_argument('--threshold', type=float, default=8.0,
                    help='COR1M threshold for trigger (default: 8)')
    ap.add_argument('--target-premium', type=float, default=1.00,
                    help='Target put mid price in dollars (default: 1.00)')
    ap.add_argument('--tolerance', type=float, default=0.50,
                    help='Acceptable deviation from target premium (default: 0.50)')
    ap.add_argument('--mode', choices=['0dte', '1dte'], default='0dte',
                    help='0dte: buy at 9:45 AM same-day exp | 1dte: buy at 3:30 PM next-day exp')
    ap.add_argument('--exit-on-cross-up', action='store_true',
                    help='Stop buying when COR1M crosses back above threshold (instead of waiting for profit)')
    args = ap.parse_args()

    print('Loading COR1M data...')
    cor1m = load_cor1m_daily()
    print(f'  {len(cor1m)} trading days of COR1M data')

    dates = trading_dates(args.from_date, args.to_date)
    print(f'  {len(dates)} trading days in range {args.from_date} to {args.to_date}')
    exit_label = ' | Exit: COR1M cross-up' if args.exit_on_cross_up else ' | Exit: first profit'
    print(f'  Mode: {args.mode.upper()}{exit_label}')

    results = run_backtest(cor1m, dates, args.threshold,
                           args.target_premium, args.tolerance, args.mode,
                           args.exit_on_cross_up)
    print_results(results, args.threshold, args.target_premium, args.mode)

    suffix = f'_{args.mode}' if args.mode != '0dte' else ''
    out_path = COR1M_DIR / f'backtest_results{suffix}.json'
    params = {'threshold': args.threshold, 'target_premium': args.target_premium,
              'tolerance': args.tolerance, 'from': args.from_date, 'to': args.to_date,
              'mode': args.mode}
    with open(out_path, 'w') as f:
        json.dump({'params': params, **results}, f, indent=2)
    print(f'\nResults saved to {out_path}')


if __name__ == '__main__':
    main()

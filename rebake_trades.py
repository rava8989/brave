#!/usr/bin/env python3
"""
Rebake the TRADES array in backtester.html from scraped_signals.csv.

Source of truth:
- scraped_signals.csv : raw Discord signals (auto-appended daily by EOD cron)
- history_data.json   : Schwab spxClose per date

Output:
- backtester.html TRADES array (9-field schema)
- history_data.json m8bfPL re-synced to match new TRADES
"""

import csv, json, re, sys
from datetime import datetime, date, timezone, timedelta
from zoneinfo import ZoneInfo

ROOT = '/Users/ravshanrakhmanov/Desktop/spx-backtester/spx-backtester'
CSV_PATH = f'{ROOT}/scraped_signals.csv'
HTML_PATH = f'{ROOT}/backtester.html'
HISTORY_PATH = f'{ROOT}/history_data.json'

ET = ZoneInfo('America/New_York')

# M8BF model rules (must match backtester.html and live system)
M8BF_BLOCKED = {10, 25, 35, 40, 65, 80}
M8BF_COMBO_BANS = {0: 95, 20: 15, 55: 50, 65: 60, 85: 90}
M8BF_WIN = {
    0: ('11:00', '11:30'),  # Mon
    1: ('13:30', '14:00'),  # Tue
    2: ('12:00', '12:30'),  # Wed
    3: ('11:00', '11:30'),  # Thu
    4: ('13:00', '13:30'),  # Fri
}
M8BF_WIN_2ND_THU = ('13:30', '14:00')


def is_2nd_thu(ds):
    y, m, d = map(int, ds.split('-'))
    dt = date(y, m, d)
    if dt.weekday() != 3:
        return False
    return sum(1 for x in range(1, d + 1) if date(y, m, x).weekday() == 3) == 2


def is_banned(center, t1):
    """Per-row BANNED flag — full ban OR combo ban on this signal."""
    if (center % 100) in M8BF_BLOCKED:
        return True
    t1mod = t1 % 100
    if M8BF_COMBO_BANS.get(t1mod) == (center % 100):
        return True
    return False


def parse_datetime_to_et(dt_str):
    """Parse CSV datetime (UTC ISO) → (date_et, time_et HH:MM, dow)"""
    s = dt_str.strip()
    # Handle both `2026-03-24T19:07:00.697Z` and `2026-04-09T19:57:33.871000+00:00`
    if s.endswith('Z'):
        s = s[:-1] + '+00:00'
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None, None, None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    et = dt.astimezone(ET)
    bucket_min = (et.minute // 5) * 5
    return (
        f'{et.year:04d}-{et.month:02d}-{et.day:02d}',
        f'{et.hour:02d}:{bucket_min:02d}',
        et.weekday(),  # 0=Mon..6=Sun
    )


def load_history_spx_close():
    with open(HISTORY_PATH) as f:
        history = json.load(f)
    return {e['date']: e.get('spxClose') for e in history if e.get('spxClose') is not None}, history


def build_trades():
    spx_by_date, _ = load_history_spx_close()

    # date → bucket_time → first signal row
    by_date_bucket = {}

    with open(CSV_PATH, newline='') as f:
        r = csv.DictReader(f)
        for row in r:
            if row.get('bf_action') != 'BUY':
                continue
            try:
                center = int(row['bf_center'])
                upper = int(row['bf_upper'])
                lower = int(row['bf_lower'])
                premium = float(row['bf_price'])
            except (ValueError, TypeError, KeyError):
                continue
            if center <= 0 or upper <= center or lower >= center:
                continue
            t1_raw = row.get('target1')
            try:
                t1 = int(float(t1_raw)) if t1_raw else center
            except ValueError:
                t1 = center

            ds_et, time_et, dow_py = parse_datetime_to_et(row['datetime'])
            if ds_et is None:
                continue
            if dow_py >= 5:  # weekend (sanity)
                continue

            d_buckets = by_date_bucket.setdefault(ds_et, {})
            if time_et in d_buckets:
                continue  # already have first signal in this 5-min bucket
            d_buckets[time_et] = {
                'center': center,
                'upper': upper,
                'lower': lower,
                'premium': premium,
                't1': t1,
                'time': time_et,
                'dow': dow_py,
            }

    # Build TRADES rows
    trades = []
    skipped_dates_no_spx = set()
    for ds in sorted(by_date_bucket):
        spx_close = spx_by_date.get(ds)
        if spx_close is None:
            skipped_dates_no_spx.add(ds)
            continue
        for time_key in sorted(by_date_bucket[ds]):
            sig = by_date_bucket[ds][time_key]
            spr = (sig['upper'] - sig['lower']) // 2
            if spr <= 0:
                continue
            maxp = round((spr - sig['premium']) * 100)
            if maxp <= 0:
                continue
            intrinsic = max(0.0, min(spx_close - sig['lower'], sig['upper'] - spx_close))
            prof = round((intrinsic - sig['premium']) * 100)
            banned = is_banned(sig['center'], sig['t1'])
            trades.append([
                ds,
                sig['dow'],         # 0=Mon..4=Fri
                sig['time'],
                sig['premium'],
                spr,
                prof,
                maxp,
                sig['center'],
                banned,
            ])

    if skipped_dates_no_spx:
        print(f'WARN: {len(skipped_dates_no_spx)} dates skipped (no spxClose in history)')
        # Show first few
        for d in sorted(skipped_dates_no_spx)[:10]:
            print(f'  {d}')

    return trades


def compute_m8bf_pl(trades, skip_dates):
    """Run buildM8BFTrades over trades, return date→pl dict.
    Mirrors backtester.html runtime: BANNED column already encodes both full bans
    AND combo bans (computed from real T1 at rebake time). Do NOT re-derive combo
    bans from (CTR-SPR)%100 — that was the original bug."""
    D, DAY, TIME, PREM, SPR, PROF, MAXP, CTR, BANNED = range(9)
    by_date = {}
    for t in trades:
        by_date.setdefault(t[D], []).append(t)
    pl_by_date = {}
    for ds in sorted(by_date):
        if ds in skip_dates:
            continue
        day_trades = by_date[ds]
        dow = day_trades[0][DAY]
        win = M8BF_WIN_2ND_THU if (dow == 3 and is_2nd_thu(ds)) else M8BF_WIN.get(dow)
        if not win:
            continue
        ws, we = win
        cands = []
        for t in day_trades:
            if t[TIME] < ws or t[TIME] >= we:
                continue
            if t[BANNED]:
                continue
            if (t[CTR] % 100) in M8BF_BLOCKED:
                continue
            cands.append(t)
        if not cands:
            continue
        cands.sort(key=lambda x: x[TIME])
        pl_by_date[ds] = cands[0][PROF]
    return pl_by_date


def serialize_trades_compact(trades):
    """Serialize trades back into the JS const TRADES = [...] format used by backtester.html."""
    parts = []
    for t in trades:
        # JSON booleans look like Python booleans
        ds, dow, tm, prem, spr, prof, maxp, ctr, banned = t
        prem_str = (
            f'{prem:.2f}'.rstrip('0').rstrip('.') if isinstance(prem, float) else str(prem)
        )
        if '.' not in prem_str:
            prem_str += '.0'
        parts.append(
            f'["{ds}", {dow}, "{tm}", {prem_str}, {spr}, {prof}, {maxp}, {ctr}, {"true" if banned else "false"}]'
        )
    return '[' + ', '.join(parts) + ']'


def main():
    print('Building new TRADES from scraped_signals.csv ...')
    trades = build_trades()
    print(f'  → {len(trades)} trades across {len(set(t[0] for t in trades))} dates')

    # Read existing M8BF_SKIP from backtester.html (rules don't change!)
    with open(HTML_PATH) as f:
        html = f.read()
    skip_match = re.search(r'const M8BF_SKIP = new Set\(\[(.*?)\]\)', html, re.DOTALL)
    skip_dates = set(re.findall(r'"(\d{4}-\d{2}-\d{2})"', skip_match.group(1)))
    print(f'  → M8BF_SKIP: {len(skip_dates)} dates')

    # Compute new M8BF P&L
    pl_by_date = compute_m8bf_pl(trades, skip_dates)
    total = sum(pl_by_date.values())
    print(f'  → New M8BF total: ${total:,} across {len(pl_by_date)} trades')

    # 1) Inject new TRADES into backtester.html
    new_trades_str = 'const TRADES = ' + serialize_trades_compact(trades) + ';'
    new_html = re.sub(r'const TRADES = \[.*?\];', lambda m: new_trades_str, html, count=1, flags=re.DOTALL)

    # Update META
    min_date = min(t[0] for t in trades)
    max_date = max(t[0] for t in trades)
    new_html = re.sub(r'"minDate"\s*:\s*"[^"]*"', f'"minDate": "{min_date}"', new_html)
    new_html = re.sub(r'"maxDate"\s*:\s*"[^"]*"', f'"maxDate": "{max_date}"', new_html)
    new_html = re.sub(r'"count"\s*:\s*\d+', f'"count": {len(trades)}', new_html)

    if '--dry-run' in sys.argv:
        print('DRY RUN — not writing files')
        return

    with open(HTML_PATH, 'w') as f:
        f.write(new_html)
    print(f'  → backtester.html updated')

    # 2) Re-sync history_data.json m8bfPL
    with open(HISTORY_PATH) as f:
        history = json.load(f)
    changed = 0
    for e in history:
        ds = e.get('date')
        if not ds:
            continue
        if ds in pl_by_date:
            new_pl = pl_by_date[ds]
            if e.get('m8bfPL') != new_pl:
                e['m8bfPL'] = new_pl
                changed += 1
        elif ds in skip_dates or (ds <= max_date and ds >= min_date):
            # In our trade range but not traded → set to 0 (skip)
            if e.get('m8bfPL') not in (0, None):
                if ds <= max_date:
                    e['m8bfPL'] = 0
                    changed += 1
            elif ds in skip_dates and e.get('m8bfPL') is None:
                e['m8bfPL'] = 0
                changed += 1

    with open(HISTORY_PATH, 'w') as f:
        json.dump(history, f, indent=2)
        f.write('\n')
    print(f'  → history_data.json updated ({changed} entries changed)')
    print('Done.')


if __name__ == '__main__':
    main()

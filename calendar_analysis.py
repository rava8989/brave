#!/usr/bin/env python3
"""Calendar-based pattern analysis of ALL trades from backtester.html"""

import json
import re
import sys
from datetime import datetime, date, timedelta
from collections import defaultdict

# ── Extract TRADES from backtester.html ──────────────────────────────────────
HTML_PATH = "/Users/ravshanrakhmanov/Desktop/spx-backtester/spx-backtester/backtester.html"

with open(HTML_PATH, "r") as f:
    for line in f:
        if line.strip().startswith("const TRADES"):
            # Extract the JSON array
            match = re.search(r'const TRADES\s*=\s*(\[.*\])', line)
            if match:
                trades_raw = json.loads(match.group(1))
            break

print(f"Total trades loaded: {len(trades_raw):,}")

# ── Parse trades ─────────────────────────────────────────────────────────────
# D=0 (date), DAY=1, CP=2, PREM=3, SPR=4, PROF=5, MAXP=6, CTR=7, TIME=8, PEAK=9, TROUGH=10, PKFIRST=11

trades = []
for t in trades_raw:
    dt = datetime.strptime(t[0], "%Y-%m-%d").date()
    trades.append({
        "date": dt,
        "day_of_week": dt.weekday(),  # 0=Mon
        "day_of_month": dt.day,
        "month": dt.month,
        "year": dt.year,
        "profit": t[5],
        "win": 1 if t[5] > 0 else 0,
    })

# ── US Market Holidays ───────────────────────────────────────────────────────
def get_us_holidays(year):
    """Return set of US market holiday dates for a given year."""
    holidays = set()
    # New Year's Day
    d = date(year, 1, 1)
    if d.weekday() == 5: d = date(year - 1, 12, 31)  # Sat -> Fri
    if d.weekday() == 6: d = date(year, 1, 2)  # Sun -> Mon
    holidays.add(d)
    # MLK Day - 3rd Monday of January
    d = date(year, 1, 1)
    mondays = 0
    while mondays < 3:
        if d.weekday() == 0: mondays += 1
        if mondays < 3: d += timedelta(days=1)
    holidays.add(d)
    # Presidents Day - 3rd Monday of February
    d = date(year, 2, 1)
    mondays = 0
    while mondays < 3:
        if d.weekday() == 0: mondays += 1
        if mondays < 3: d += timedelta(days=1)
    holidays.add(d)
    # Good Friday (approximate - varies by year)
    # Using known dates for 2024-2026
    good_fridays = {
        2023: date(2023, 4, 7), 2024: date(2024, 3, 29),
        2025: date(2025, 4, 18), 2026: date(2026, 4, 3),
    }
    if year in good_fridays:
        holidays.add(good_fridays[year])
    # Memorial Day - last Monday of May
    d = date(year, 5, 31)
    while d.weekday() != 0: d -= timedelta(days=1)
    holidays.add(d)
    # Juneteenth
    d = date(year, 6, 19)
    if d.weekday() == 5: d -= timedelta(days=1)
    if d.weekday() == 6: d += timedelta(days=1)
    holidays.add(d)
    # Independence Day
    d = date(year, 7, 4)
    if d.weekday() == 5: d -= timedelta(days=1)
    if d.weekday() == 6: d += timedelta(days=1)
    holidays.add(d)
    # Labor Day - 1st Monday of September
    d = date(year, 9, 1)
    while d.weekday() != 0: d += timedelta(days=1)
    holidays.add(d)
    # Thanksgiving - 4th Thursday of November
    d = date(year, 11, 1)
    thursdays = 0
    while thursdays < 4:
        if d.weekday() == 3: thursdays += 1
        if thursdays < 4: d += timedelta(days=1)
    holidays.add(d)
    # Christmas
    d = date(year, 12, 25)
    if d.weekday() == 5: d -= timedelta(days=1)
    if d.weekday() == 6: d += timedelta(days=1)
    holidays.add(d)
    return holidays

# Build holiday set for all years in data
years = set(t["year"] for t in trades)
all_holidays = set()
for y in years:
    all_holidays |= get_us_holidays(y)

# Build sorted list of unique trading dates
all_trading_dates = sorted(set(t["date"] for t in trades))
trading_date_set = set(all_trading_dates)

# ── Helper: print table ─────────────────────────────────────────────────────
def print_table(title, rows, sort_by_avg=True):
    """rows: list of (label, trades_count, wins, total_pnl)"""
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}")

    # Calculate derived fields
    data = []
    for label, cnt, wins, total in rows:
        if cnt == 0:
            continue
        win_pct = wins / cnt * 100
        avg = total / cnt
        data.append((label, cnt, wins, win_pct, total, avg))

    if sort_by_avg:
        data.sort(key=lambda x: x[5], reverse=True)

    print(f"  {'Category':<30} {'Trades':>7} {'Wins':>7} {'Win%':>7} {'Total P&L':>12} {'Avg P&L':>10}")
    print(f"  {'-'*30} {'-'*7} {'-'*7} {'-'*7} {'-'*12} {'-'*10}")
    for label, cnt, wins, win_pct, total, avg in data:
        print(f"  {label:<30} {cnt:>7,} {wins:>7,} {win_pct:>6.1f}% {total:>12,.0f} {avg:>10,.1f}")

    if data:
        best = max(data, key=lambda x: x[5])
        worst = min(data, key=lambda x: x[5])
        print(f"\n  BEST:  {best[0]} (avg P&L: {best[5]:,.1f})")
        print(f"  WORST: {worst[0]} (avg P&L: {worst[5]:,.1f})")


# ══════════════════════════════════════════════════════════════════════════════
# 1. DAY OF MONTH
# ══════════════════════════════════════════════════════════════════════════════
buckets = defaultdict(lambda: [0, 0, 0])  # [count, wins, total_pnl]
for t in trades:
    d = t["day_of_month"]
    buckets[d][0] += 1
    buckets[d][1] += t["win"]
    buckets[d][2] += t["profit"]

rows = [(f"Day {d:>2}", *buckets[d]) for d in sorted(buckets)]
print_table("1. DAY OF MONTH", rows)


# ══════════════════════════════════════════════════════════════════════════════
# 2. WEEK OF MONTH
# ══════════════════════════════════════════════════════════════════════════════
def week_of_month(day):
    if day <= 7: return "Week 1 (1-7)"
    if day <= 14: return "Week 2 (8-14)"
    if day <= 21: return "Week 3 (15-21)"
    if day <= 28: return "Week 4 (22-28)"
    return "Week 5 (29-31)"

buckets = defaultdict(lambda: [0, 0, 0])
for t in trades:
    w = week_of_month(t["day_of_month"])
    buckets[w][0] += 1
    buckets[w][1] += t["win"]
    buckets[w][2] += t["profit"]

rows = [(w, *buckets[w]) for w in sorted(buckets)]
print_table("2. WEEK OF MONTH", rows)


# ══════════════════════════════════════════════════════════════════════════════
# 3. FIRST/LAST TRADING DAYS OF MONTH
# ══════════════════════════════════════════════════════════════════════════════
# Group trading dates by (year, month)
month_dates = defaultdict(list)
for d in all_trading_dates:
    month_dates[(d.year, d.month)].append(d)

first2 = set()
last2 = set()
for key, dates in month_dates.items():
    dates_sorted = sorted(dates)
    first2.update(dates_sorted[:2])
    last2.update(dates_sorted[-2:])

buckets = {"First 2 trading days": [0, 0, 0], "Last 2 trading days": [0, 0, 0], "Middle days": [0, 0, 0]}
for t in trades:
    d = t["date"]
    if d in first2:
        k = "First 2 trading days"
    elif d in last2:
        k = "Last 2 trading days"
    else:
        k = "Middle days"
    buckets[k][0] += 1
    buckets[k][1] += t["win"]
    buckets[k][2] += t["profit"]

rows = [(k, *v) for k, v in buckets.items()]
print_table("3. FIRST vs LAST TRADING DAYS OF MONTH", rows)


# ══════════════════════════════════════════════════════════════════════════════
# 4. MONTH-END EFFECT (last 3 trading days vs rest)
# ══════════════════════════════════════════════════════════════════════════════
last3 = set()
for key, dates in month_dates.items():
    dates_sorted = sorted(dates)
    last3.update(dates_sorted[-3:])

buckets = {"Last 3 trading days": [0, 0, 0], "Rest of month": [0, 0, 0]}
for t in trades:
    k = "Last 3 trading days" if t["date"] in last3 else "Rest of month"
    buckets[k][0] += 1
    buckets[k][1] += t["win"]
    buckets[k][2] += t["profit"]

rows = [(k, *v) for k, v in buckets.items()]
print_table("4. MONTH-END EFFECT (last 3 trading days vs rest)", rows)


# ══════════════════════════════════════════════════════════════════════════════
# 5. TURN OF MONTH (last 2 + first 2 of next month vs rest)
# ══════════════════════════════════════════════════════════════════════════════
turn_dates = first2 | last2

buckets = {"Turn of month (L2+F2)": [0, 0, 0], "Rest": [0, 0, 0]}
for t in trades:
    k = "Turn of month (L2+F2)" if t["date"] in turn_dates else "Rest"
    buckets[k][0] += 1
    buckets[k][1] += t["win"]
    buckets[k][2] += t["profit"]

rows = [(k, *v) for k, v in buckets.items()]
print_table("5. TURN OF MONTH (last 2 + first 2 of next month)", rows)


# ══════════════════════════════════════════════════════════════════════════════
# 6. DAY BEFORE/AFTER HOLIDAYS
# ══════════════════════════════════════════════════════════════════════════════
day_before_holiday = set()
day_after_holiday = set()

for h in all_holidays:
    # Find the trading day just before this holiday
    d = h - timedelta(days=1)
    while d not in trading_date_set and d > all_trading_dates[0]:
        d -= timedelta(days=1)
    if d in trading_date_set:
        day_before_holiday.add(d)
    # Find the trading day just after this holiday
    d = h + timedelta(days=1)
    while d not in trading_date_set and d < all_trading_dates[-1]:
        d += timedelta(days=1)
    if d in trading_date_set:
        day_after_holiday.add(d)

buckets = {"Day before holiday": [0, 0, 0], "Day after holiday": [0, 0, 0], "Normal days": [0, 0, 0]}
for t in trades:
    d = t["date"]
    if d in day_before_holiday:
        k = "Day before holiday"
    elif d in day_after_holiday:
        k = "Day after holiday"
    else:
        k = "Normal days"
    buckets[k][0] += 1
    buckets[k][1] += t["win"]
    buckets[k][2] += t["profit"]

rows = [(k, *v) for k, v in buckets.items()]
print_table("6. DAY BEFORE/AFTER HOLIDAYS", rows)


# ══════════════════════════════════════════════════════════════════════════════
# 7. QUARTER BOUNDARIES (first week vs last week of quarter)
# ══════════════════════════════════════════════════════════════════════════════
quarter_months = {1: (1, 3), 2: (4, 6), 3: (7, 9), 4: (10, 12)}

# Group trading dates by quarter
quarter_dates = defaultdict(list)
for d in all_trading_dates:
    q = (d.month - 1) // 3 + 1
    quarter_dates[(d.year, q)].append(d)

first_week_q = set()
last_week_q = set()
for key, dates in quarter_dates.items():
    dates_sorted = sorted(dates)
    first_week_q.update(dates_sorted[:5])  # first 5 trading days
    last_week_q.update(dates_sorted[-5:])  # last 5 trading days

buckets = {"First week of quarter": [0, 0, 0], "Last week of quarter": [0, 0, 0], "Rest": [0, 0, 0]}
for t in trades:
    d = t["date"]
    if d in first_week_q:
        k = "First week of quarter"
    elif d in last_week_q:
        k = "Last week of quarter"
    else:
        k = "Rest"
    buckets[k][0] += 1
    buckets[k][1] += t["win"]
    buckets[k][2] += t["profit"]

rows = [(k, *v) for k, v in buckets.items()]
print_table("7. QUARTER BOUNDARIES", rows)


# ══════════════════════════════════════════════════════════════════════════════
# 8. FRIDAY THE 13TH
# ══════════════════════════════════════════════════════════════════════════════
buckets = {"Friday the 13th": [0, 0, 0], "Other days": [0, 0, 0]}
for t in trades:
    d = t["date"]
    if d.day == 13 and d.weekday() == 4:  # Friday
        k = "Friday the 13th"
    else:
        k = "Other days"
    buckets[k][0] += 1
    buckets[k][1] += t["win"]
    buckets[k][2] += t["profit"]

rows = [(k, *v) for k, v in buckets.items()]
print_table("8. FRIDAY THE 13TH", rows)


# ══════════════════════════════════════════════════════════════════════════════
# 9. DAY OF YEAR SEASONALITY (Quarterly)
# ══════════════════════════════════════════════════════════════════════════════
def quarter_label(month):
    if month <= 3: return "Q1 (Jan-Mar)"
    if month <= 6: return "Q2 (Apr-Jun)"
    if month <= 9: return "Q3 (Jul-Sep)"
    return "Q4 (Oct-Dec)"

buckets = defaultdict(lambda: [0, 0, 0])
for t in trades:
    q = quarter_label(t["month"])
    buckets[q][0] += 1
    buckets[q][1] += t["win"]
    buckets[q][2] += t["profit"]

rows = [(q, *buckets[q]) for q in sorted(buckets)]
print_table("9. QUARTERLY SEASONALITY", rows)


# ══════════════════════════════════════════════════════════════════════════════
# BONUS: Monthly breakdown
# ══════════════════════════════════════════════════════════════════════════════
month_names = {1:"Jan", 2:"Feb", 3:"Mar", 4:"Apr", 5:"May", 6:"Jun",
               7:"Jul", 8:"Aug", 9:"Sep", 10:"Oct", 11:"Nov", 12:"Dec"}

buckets = defaultdict(lambda: [0, 0, 0])
for t in trades:
    m = month_names[t["month"]]
    buckets[m][0] += 1
    buckets[m][1] += t["win"]
    buckets[m][2] += t["profit"]

# Sort by calendar order
month_order = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
rows = [(m, *buckets[m]) for m in month_order if m in buckets]
print_table("BONUS: MONTHLY BREAKDOWN", rows, sort_by_avg=False)

# Also sorted by avg P&L
rows2 = [(m, *buckets[m]) for m in month_order if m in buckets]
print_table("BONUS: MONTHLY BREAKDOWN (sorted by Avg P&L)", rows2, sort_by_avg=True)


# ══════════════════════════════════════════════════════════════════════════════
# BONUS: Day of week
# ══════════════════════════════════════════════════════════════════════════════
dow_names = {0:"Monday", 1:"Tuesday", 2:"Wednesday", 3:"Thursday", 4:"Friday"}

buckets = defaultdict(lambda: [0, 0, 0])
for t in trades:
    dw = dow_names[t["day_of_week"]]
    buckets[dw][0] += 1
    buckets[dw][1] += t["win"]
    buckets[dw][2] += t["profit"]

rows = [(dw, *buckets[dw]) for dw in ["Monday","Tuesday","Wednesday","Thursday","Friday"] if dw in buckets]
print_table("BONUS: DAY OF WEEK", rows)

print(f"\n{'='*80}")
print(f"  Analysis complete. {len(trades_raw):,} trades analyzed across {len(all_trading_dates)} trading days.")
print(f"  Date range: {all_trading_dates[0]} to {all_trading_dates[-1]}")
print(f"{'='*80}")

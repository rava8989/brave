#!/usr/bin/env python3
"""
CycleLab data builder — SPX intraday cyclicality bundle.

Reads data/spx/SPX_YYYYMMDD.csv (1-min bars), aggregates to 5-min candles,
and writes cyclicality_data.json:

  {
    "built": "YYYY-MM-DD",
    "slots": ["09:30", "09:35", ..., "15:55"],      # candle START times
    "days":  [{"d": "2022-06-01", "w": 2,            # ISO date, weekday 0=Mon
               "m": [0.4, -1.2, ...]}, ...]          # per-slot net move
  }

Per-slot move = close(last 1-min bar in slot) - open(first 1-min bar in slot),
SPX points, rounded 2dp. Missing slots (half-days, gaps) → null. All-zero
placeholder bars are filtered (lessons P15).

Run: python3 build_cyclicality_data.py
"""
import csv, json, glob, datetime, os

ROOT = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(ROOT, 'cyclicality_data.json')

def slot_list():
    out = []
    t = datetime.datetime(2000, 1, 1, 9, 30)
    while t < datetime.datetime(2000, 1, 1, 16, 0):
        out.append(t.strftime('%H:%M'))
        t += datetime.timedelta(minutes=5)
    return out

SLOTS = slot_list()
SLOT_IDX = {s: i for i, s in enumerate(SLOTS)}

def hhmm_of(row):
    for k in ('time', 'Time', 'datetime', 'timestamp'):
        v = row.get(k)
        if not v: continue
        v = str(v)
        if 'T' in v: v = v.split('T')[1]
        if ' ' in v: v = v.split(' ')[1]
        return v[:5]
    return None

days = []
for p in sorted(glob.glob(os.path.join(ROOT, 'data/spx/SPX_*.csv'))):
    ymd = os.path.basename(p)[4:12]
    d_iso = f'{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}'
    rows = [r for r in csv.DictReader(open(p))
            if float(r.get('open', 0) or 0) > 0 and float(r.get('close', 0) or 0) > 0]
    if not rows: continue
    # group 1-min bars into 5-min slots
    slot_oc = {}  # idx -> [first_open, last_close]
    for r in rows:
        hm = hhmm_of(r)
        if not hm: continue
        h, m = int(hm[:2]), int(hm[3:])
        if h < 9 or (h == 9 and m < 30) or h >= 16: continue
        slot_min = m - (m % 5)
        key = f'{h:02d}:{slot_min:02d}'
        idx = SLOT_IDX.get(key)
        if idx is None: continue
        o, c = float(r['open']), float(r['close'])
        if idx in slot_oc: slot_oc[idx][1] = c
        else: slot_oc[idx] = [o, c]
    if len(slot_oc) < 30: continue   # skip fragmentary days
    moves = [None] * len(SLOTS)
    for idx, (o, c) in slot_oc.items():
        moves[idx] = round(c - o, 2)
    wd = datetime.date.fromisoformat(d_iso).weekday()
    if wd >= 5: continue
    days.append({'d': d_iso, 'w': wd, 'm': moves})

out = {'built': datetime.date.today().isoformat(), 'slots': SLOTS, 'days': days}
with open(OUT, 'w') as f:
    json.dump(out, f, separators=(',', ':'))
print(f'wrote {OUT}: {len(days)} days ({days[0]["d"]} → {days[-1]["d"]}), {len(SLOTS)} slots, '
      f'{os.path.getsize(OUT)//1024} KB')

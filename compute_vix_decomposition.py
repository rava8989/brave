#!/usr/bin/env python3
"""Decompose day-over-day ~30DTE ATM IV moves (optionsgelt-inspired).

For each consecutive session pair in data/vix_surface_daily.json.gz:

  dATM      = ATM IV(today, spot_today) − ATM IV(yday, spot_yday)
  slide     = IV_yday(spot_today) − IV_yday(spot_yday)
              → the MECHANICAL sticky-strike move: spot sliding along
                yesterday's frozen smile (spot down → up the put skew →
                "VIX up" with zero new vol demand)
  parallel  = mean fixed-strike repricing: avg over central strikes K of
              [IV_today(K) − IV_yday(K)]  → REAL net vol buying/selling
  residual  = dATM − slide − parallel     → skew twist + noise
  put_skew  = [IV(92%) − IV(ATM)] each day (own-spot moneyness); d_put_skew
  call_skew = [IV(104%) − IV(ATM)] each day; d_call_skew

Day label:
  MECHANICAL — |slide| ≥ 2/3·|dATM| and |parallel| < 1/3·|dATM|
  VOL_BID    — parallel ≥ +0.5 vol pts
  VOL_SUPPLY — parallel ≤ −0.5 vol pts
  MIXED      — everything else

Output: data/vix_decomposition.json
  {date: {dATM, slide, parallel, residual, put_skew, d_put_skew,
          call_skew, d_call_skew, atm, spot, label}}
"""
from __future__ import annotations

import gzip
import json
from pathlib import Path

HERE = Path(__file__).parent
SRC = HERE / 'data' / 'vix_surface_daily.json.gz'
OUT = HERE / 'data' / 'vix_decomposition.json'


def smile_fn(rec):
    """IV(K) via linear interpolation. OTM side only: puts below spot, calls
    above, P/C averaged at overlapping strikes (ATM). Returns (fn, lo, hi)."""
    by_k = {}
    for k, right, _b, _a, iv in rec['rows']:
        if iv is None or iv <= 0:
            continue
        by_k.setdefault(k, []).append(iv)
    pts = sorted((k, sum(v) / len(v)) for k, v in by_k.items())
    if len(pts) < 4:
        return None, None, None
    ks = [p[0] for p in pts]
    ivs = [p[1] for p in pts]

    def fn(x):
        if x <= ks[0]:
            return ivs[0]
        if x >= ks[-1]:
            return ivs[-1]
        for i in range(1, len(ks)):
            if x <= ks[i]:
                w = (x - ks[i - 1]) / (ks[i] - ks[i - 1])
                return ivs[i - 1] * (1 - w) + ivs[i] * w
        return ivs[-1]
    return fn, ks[0], ks[-1]


def main():
    with gzip.open(SRC, 'rt') as f:
        surf = json.load(f)
    days = sorted(surf.keys())
    out = {}
    prev_put_skew = {}
    for i in range(1, len(days)):
        d0, d1 = days[i - 1], days[i]
        r0, r1 = surf[d0], surf[d1]
        f0, lo0, hi0 = smile_fn(r0)
        f1, lo1, hi1 = smile_fn(r1)
        if not f0 or not f1:
            continue
        s0, s1 = r0['spot'], r1['spot']

        atm0, atm1 = f0(s0), f1(s1)
        d_atm = atm1 - atm0
        slide = f0(min(max(s1, lo0), hi0)) - f0(s0)

        # fixed-strike repricing over the central band (88%–106% of yday spot,
        # clipped to both days' grids)
        klo, khi = max(lo0, lo1, s0 * 0.88), min(hi0, hi1, s0 * 1.06)
        if khi <= klo:
            continue
        grid = [klo + (khi - klo) * j / 8 for j in range(9)]
        parallel = sum(f1(k) - f0(k) for k in grid) / len(grid)

        residual = d_atm - slide - parallel
        put_skew0 = f0(s0 * 0.92) - atm0
        put_skew1 = f1(s1 * 0.92) - atm1
        call_skew0 = f0(s0 * 1.04) - atm0
        call_skew1 = f1(s1 * 1.04) - atm1

        if abs(slide) >= abs(d_atm) * 2 / 3 and abs(parallel) < max(abs(d_atm) / 3, 0.15):
            label = 'MECHANICAL'
        elif parallel >= 0.5:
            label = 'VOL_BID'
        elif parallel <= -0.5:
            label = 'VOL_SUPPLY'
        else:
            label = 'MIXED'

        out[d1] = {
            'atm': round(atm1, 2), 'spot': s1,
            'dATM': round(d_atm, 2), 'slide': round(slide, 2),
            'parallel': round(parallel, 2), 'residual': round(residual, 2),
            'put_skew': round(put_skew1, 2), 'd_put_skew': round(put_skew1 - put_skew0, 2),
            'call_skew': round(call_skew1, 2), 'd_call_skew': round(call_skew1 - call_skew0, 2),
            'label': label,
        }
    OUT.write_text(json.dumps(out, separators=(',', ':')))
    from collections import Counter
    c = Counter(v['label'] for v in out.values())
    print(f'{len(out)} day-pairs → {OUT.name}; labels: {dict(c)}')


if __name__ == '__main__':
    main()

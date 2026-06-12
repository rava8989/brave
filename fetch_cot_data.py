#!/usr/bin/env python3
"""CFTC Commitments of Traders — currency futures, legacy report.

Pulls the full weekly history for the major currency contracts from the
CFTC public Socrata API (publicreporting.cftc.gov, dataset 6dca-aqww =
legacy futures-only) and writes data/cot_currencies.json.

Three trader groups (legacy report):
  noncomm = Large Speculators (funds)   — the trend-chasers
  comm    = Commercials (hedgers)       — the other side
  nonrept = Small traders (retail)

Row format (compact): [date, OI, ncLong, ncShort, cLong, cShort, nrLong, nrShort]

The worker self-feeds new weeks every Friday (cotWeeklyRefresh); this script
is for the initial backfill + manual refresh.
"""
from __future__ import annotations

import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path

OUT = Path(__file__).parent / 'data' / 'cot_currencies.json'
API = 'https://publicreporting.cftc.gov/resource/6dca-aqww.json'

CONTRACTS = {
    'EUR': {'code': '099741', 'name': 'EURO FX',           'etf': 'FXE'},
    'JPY': {'code': '097741', 'name': 'JAPANESE YEN',      'etf': 'FXY'},
    'GBP': {'code': '096742', 'name': 'BRITISH POUND',     'etf': 'FXB'},
    'CAD': {'code': '090741', 'name': 'CANADIAN DOLLAR',   'etf': 'FXC'},
    'CHF': {'code': '092741', 'name': 'SWISS FRANC',       'etf': 'FXF'},
    'AUD': {'code': '232741', 'name': 'AUSTRALIAN DOLLAR', 'etf': 'FXA'},
    'NZD': {'code': '112741', 'name': 'NZ DOLLAR',         'etf': None},
    'MXN': {'code': '095741', 'name': 'MEXICAN PESO',      'etf': None},
    'DXY': {'code': '098662', 'name': 'USD INDEX (ICE)',   'etf': 'UUP'},
}

FIELDS = ('report_date_as_yyyy_mm_dd,open_interest_all,'
          'noncomm_positions_long_all,noncomm_positions_short_all,'
          'comm_positions_long_all,comm_positions_short_all,'
          'nonrept_positions_long_all,nonrept_positions_short_all')


def fetch_contract(code: str, since: str = '2000-01-01') -> list:
    params = {
        '$select': FIELDS,
        '$where': f"cftc_contract_market_code='{code}' AND report_date_as_yyyy_mm_dd>'{since}'",
        '$order': 'report_date_as_yyyy_mm_dd ASC',
        '$limit': '5000',
    }
    url = API + '?' + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=60) as r:
        rows = json.load(r)
    out = []
    for x in rows:
        try:
            out.append([
                x['report_date_as_yyyy_mm_dd'][:10],
                int(x['open_interest_all']),
                int(x['noncomm_positions_long_all']), int(x['noncomm_positions_short_all']),
                int(x['comm_positions_long_all']),    int(x['comm_positions_short_all']),
                int(x['nonrept_positions_long_all']), int(x['nonrept_positions_short_all']),
            ])
        except (KeyError, ValueError):
            pass
    return out


def main():
    series = {}
    for key, c in CONTRACTS.items():
        rows = fetch_contract(c['code'])
        series[key] = rows
        print(f"{key} ({c['name']}): {len(rows)} weeks "
              f"{rows[0][0] if rows else '—'} → {rows[-1][0] if rows else '—'}", flush=True)
    out = {
        'meta': {'contracts': CONTRACTS, 'source': 'CFTC legacy futures-only (6dca-aqww)',
                 'row': '[date, OI, specLong, specShort, commLong, commShort, smallLong, smallShort]'},
        'series': series,
    }
    OUT.write_text(json.dumps(out, separators=(',', ':')))
    print(f'→ {OUT.name} ({OUT.stat().st_size // 1024} KB)')


if __name__ == '__main__':
    sys.exit(main())

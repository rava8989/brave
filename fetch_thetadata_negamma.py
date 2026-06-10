"""
ThetaData-backed GXBF backtester data builder — MULTI-TIMESTAMP version.

For every trading day produces a record:
  {
    date, spxOpen, spxClose, vixOpen, vixClose,
    by_time: {
      "09:35": { center, centerOI, mids:{...}, midsOI:{...} },
      "09:40": { ... },
      ...
      "14:00": { ... }
    }
  }

Timestamps span 09:35 → 14:00 ET in 5-min steps (54 timestamps on full days,
fewer on half-days — anything ≥ close is omitted).

Methodology (matches schwab-proxy.js `calculateGEX` and the prior single-time
build, generalized over T):
  For each timestamp T:
    • Cumulative call volume per strike from 09:30:00 → (T-1min):59 (the
      "available before the entry decision at T" convention — identical to
      the old 09:30→09:35 window when T = 09:35).
    • Call mid per strike at T = (bid+ask)/2 last sample of the 1-min bar
      starting at T.
    • Per-strike IV back-solved via Newton-Raphson (R=0.043, Q=0.013).
    • γ = bsGamma(spot_at_T, K, T_to_close, σ_per_strike).
    • gex_vol = γ · cum_vol · S² · 100 · 0.01
    • gex_oi  = γ · oi      · S² · 100 · 0.01
    • center   = argmax(gex_vol) snapped to nearest 5.
    • centerOI = argmax(gex_oi)  snapped to nearest 5.
    • mids     = { offset_str: mid_at_T for offset ∈ [-100..100 step 5] }.
    • midsOI   = same grid around centerOI.

Strike search window: ±5% of spxOpen (stable across the day so the
strike-list ThetaData call only fires once per day, not per timestamp).

Implementation per day:
  1. Read spxOpen, spxClose, vixOpen, vixClose and the SPX 1-min bars
     (already on disk under data/spx, data/vix).
  2. List 0DTE SPXW call strikes in ±5% of spxOpen.
  3. For each strike, in parallel: fetch (a) full-day 1-min OHLC
     (09:30:00 → 14:00:59), (b) full-day 1-min quotes, (c) the OI snapshot.
  4. Parse each strike's responses into per-minute dicts vol_by_min[K][HH:MM]
     and mid_by_min[K][HH:MM]. Stash oi[K].
  5. For each T in the 5-min grid:
       - compute cum_vol[K] = Σ vol_by_min[K][m] for m in [09:30..T-1min]
       - mid_at_T[K] from mid_by_min[K][T] (skip K if missing)
       - solve IV, compute γ, compute gex_vol / gex_oi, pick center(s)
       - assemble mids grid (only offsets where a mid exists)
  6. Atomic-merge by-date into gxbf_bt_data.json after every day.

Half-days: market close is 13:00 ET; T_to_close uses 13:00 anchor and any
T ≥ 13:00 is omitted from by_time.

CLI:
  python3 fetch_thetadata_gxbf.py --date 2026-05-18
  python3 fetch_thetadata_gxbf.py --from 2024-09-03 --to 2026-05-22
"""
from __future__ import annotations
import os, sys, json, csv, math, argparse, time, io
import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

ROOT = Path(__file__).resolve().parent
NEGAMMA_FILE = ROOT / 'negamma_bt_data.json'
SPX_DIR   = ROOT / 'data' / 'spx'
VIX_DIR   = ROOT / 'data' / 'vix'
THETA     = 'http://localhost:25503/v3'

SESSION = requests.Session()
SESSION.mount('http://', requests.adapters.HTTPAdapter(pool_maxsize=64))

MID_GRID_STEPS = list(range(-100, 105, 5))  # 41 offsets

# Intraday entry-time grid: 09:35 → 14:00 step 5min  →  54 timestamps.
def _build_timestamp_grid() -> list[str]:
    # NEGATIVE-GAMMA fetcher: user only needs 09:35 for now.
    # If you need additional timestamps later, expand this list.
    return ['09:35']

TIMESTAMP_GRID = _build_timestamp_grid()

# Half-days where the market closes at 13:00 ET (1:00 PM).
HALF_DAYS = {
    '2024-07-03', '2024-11-29', '2024-12-24',
    '2025-07-03', '2025-11-28', '2025-12-24',
    '2026-07-02', '2026-11-27', '2026-12-24',
}

# US market holidays — full closures (no SPX trading). Mirrors signal-engine.js.
US_MARKET_HOLIDAYS = {
    '2023-01-02','2023-01-16','2023-02-20','2023-04-07','2023-05-29',
    '2023-06-19','2023-07-04','2023-09-04','2023-11-23','2023-12-25',
    '2024-01-01','2024-01-15','2024-02-19','2024-03-29','2024-05-27',
    '2024-06-19','2024-07-04','2024-09-02','2024-11-28','2024-12-25',
    '2025-01-01','2025-01-09','2025-01-20','2025-02-17','2025-04-18',
    '2025-05-26','2025-06-19','2025-07-04','2025-09-01','2025-11-27','2025-12-25',
    '2026-01-01','2026-01-19','2026-02-16','2026-04-03','2026-05-25',
    '2026-06-19','2026-07-03','2026-09-07','2026-11-26','2026-12-25',
}


# ── Black-Scholes ──
def norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)

def norm_cdf(x: float) -> float:
    t = 1.0 / (1.0 + 0.2316419 * abs(x))
    d = 0.3989422804014327
    p = d * math.exp(-x * x / 2.0) * (
        t * (0.319381530 + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))))
    )
    return 1.0 - p if x > 0 else p

def bs_gamma(S: float, K: float, T: float, sigma: float, r: float = 0.043, q: float = 0.013) -> float:
    """Match schwab-proxy.js calculateGEX bsGamma exactly: R=0.043, Q=0.013."""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    try:
        d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
        return norm_pdf(d1) * math.exp(-q * T) / (S * sigma * math.sqrt(T))
    except (ValueError, ZeroDivisionError):
        return 0.0

def bs_price(S: float, K: float, T: float, sigma: float, r: float = 0.043, is_call: bool = True) -> float:
    if T <= 0 or sigma <= 0:
        return max(0.0, (S - K) if is_call else (K - S))
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if is_call:
        return S * norm_cdf(d1) - K * math.exp(-r * T) * norm_cdf(d2)
    return K * math.exp(-r * T) * norm_cdf(-d2) - S * norm_cdf(-d1)

def implied_vol(S: float, K: float, T: float, market_price: float, r: float = 0.043, is_call: bool = True) -> float | None:
    """Back-solve sigma from market price via Newton-Raphson."""
    if market_price <= 0 or T <= 0 or S <= 0 or K <= 0:
        return None
    sigma = 0.3
    for _ in range(50):
        price = bs_price(S, K, T, sigma, r, is_call)
        d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
        vega = S * norm_pdf(d1) * math.sqrt(T)
        if vega < 1e-10:
            break
        sigma -= (price - market_price) / vega
        if sigma <= 0.001:
            sigma = 0.001
        if sigma > 5.0:
            return None
    return sigma if 0.001 < sigma < 5.0 else None


# ── CSV readers (Schwab 1-min bars) ──
def _read_csv_rows(path: Path) -> list[dict]:
    with open(path) as f:
        return list(csv.DictReader(f))

def _row_hhmm(row: dict) -> str | None:
    """Extract HH:MM from a Schwab bar row's timestamp field."""
    t = row.get('time') or row.get('timestamp') or row.get('datetime') or ''
    # Examples: "2026-05-18 09:35:00", "09:35:00", "09:35"
    if ' ' in t:
        t = t.split(' ', 1)[1]
    if len(t) >= 5:
        return t[:5]
    return None

def read_spx_bars(date_iso: str) -> tuple[float, float, dict[str, float]]:
    """Returns (spxOpen, spxClose, spot_by_hhmm) from data/spx/SPX_YYYYMMDD.csv.

    `spot_by_hhmm` is the bar close at each HH:MM (used to look up spot at
    each entry timestamp T).
    """
    yyyymmdd = date_iso.replace('-', '')
    path = SPX_DIR / f'SPX_{yyyymmdd}.csv'
    if not path.exists():
        raise FileNotFoundError(f'SPX bars missing: {path}')
    rows = _read_csv_rows(path)
    rows = [r for r in rows   # drop all-zero placeholder bars (P15, 2026-06-10 audit)
            if float(r.get('open') or r.get('Open') or 0) > 0
            and float(r.get('close') or r.get('Close') or 0) > 0]
    if not rows:
        raise ValueError(f'No non-zero SPX bars in {path}')
    spxOpen  = float(rows[0].get('open')  or rows[0].get('Open')  or rows[0].get('close'))
    spxClose = float(rows[-1].get('close') or rows[-1].get('Close'))
    spot_by_hhmm: dict[str, float] = {}
    for r in rows:
        hhmm = _row_hhmm(r)
        if not hhmm:
            continue
        v = r.get('close') or r.get('Close')
        if v is None:
            continue
        try:
            spot_by_hhmm[hhmm] = float(v)
        except (TypeError, ValueError):
            pass
    return spxOpen, spxClose, spot_by_hhmm


def read_vix_bars(date_iso: str) -> tuple[float, float]:
    """Returns (vixOpen, vixClose) from data/vix/VIX_YYYYMMDD.csv."""
    yyyymmdd = date_iso.replace('-', '')
    path = VIX_DIR / f'VIX_{yyyymmdd}.csv'
    if not path.exists():
        raise FileNotFoundError(f'VIX bars missing: {path}')
    rows = _read_csv_rows(path)
    rows = [r for r in rows   # drop all-zero placeholder bars (P15, 2026-06-10 audit)
            if float(r.get('open') or r.get('Open') or 0) > 0
            and float(r.get('close') or r.get('Close') or 0) > 0]
    if not rows:
        raise ValueError(f'No non-zero VIX bars in {path}')
    vixOpen  = float(rows[0].get('open')  or rows[0].get('Open')  or rows[0].get('close'))
    vixClose = float(rows[-1].get('close') or rows[-1].get('Close'))
    return vixOpen, vixClose


# ── ThetaData fetchers (full-day, one call each) ──
def list_strikes_0dte(date_yyyymmdd: str, lo: float, hi: float) -> list[float]:
    """List SPXW 0DTE strikes in [lo, hi] dollars."""
    r = SESSION.get(f'{THETA}/option/list/strikes',
                    params={'symbol': 'SPXW', 'expiration': date_yyyymmdd, 'format': 'json'},
                    timeout=30)
    r.raise_for_status()
    out = []
    for item in r.json().get('response', []):
        raw = item['strike'] if isinstance(item, dict) else item
        s = float(raw)
        if lo <= s <= hi:
            out.append(s)
    return sorted(set(out))


def _ts_hhmm(row: list[str], headers: list[str]) -> str | None:
    """Extract HH:MM from a ThetaData bar row.

    ThetaData v3 returns a `timestamp` column in ISO format
    ("2026-05-18T09:30:00.000"). Older v2-style payloads may use
    `ms_of_day` (ms since 00:00 ET) or a plain `time` column.
    """
    if 'timestamp' in headers:
        idx = headers.index('timestamp')
        try:
            t = row[idx]
        except IndexError:
            return None
        # Format "2026-05-18T09:30:00.000" → "09:30"
        if 'T' in t:
            tail = t.split('T', 1)[1]
            if len(tail) >= 5:
                return tail[:5]
        if ' ' in t:
            tail = t.split(' ', 1)[1]
            if len(tail) >= 5:
                return tail[:5]
        if len(t) >= 5 and t[2] == ':':
            return t[:5]
        return None
    if 'ms_of_day' in headers:
        idx = headers.index('ms_of_day')
        try:
            ms = int(row[idx])
            h, rem = divmod(ms // 1000, 3600)
            m = rem // 60
            return f'{h:02d}:{m:02d}'
        except (ValueError, IndexError):
            return None
    if 'time' in headers:
        idx = headers.index('time')
        try:
            t = row[idx]
        except IndexError:
            return None
        if len(t) >= 5:
            return t[:5]
    return None


def fetch_call_volume_day(date_yyyymmdd: str, strike: float) -> dict[str, int]:
    """Return per-minute call volume as {HH:MM: vol} for 09:30 → 14:00:59 ET."""
    try:
        r = SESSION.get(f'{THETA}/option/history/ohlc', params={
            'symbol': 'SPXW', 'expiration': date_yyyymmdd, 'strike': f'{strike:.2f}',
            'right': 'put', 'date': date_yyyymmdd, 'interval': '1m',
            'start_time': '09:30:00.000', 'end_time': '14:00:59.999', 'format': 'csv',
        }, timeout=20)
    except Exception:
        return {}
    if not r.ok or not r.text or r.text.startswith('<'):
        return {}
    reader = csv.reader(io.StringIO(r.text))
    headers = next(reader, None)
    if not headers:
        return {}
    vol_idx = headers.index('volume') if 'volume' in headers else 4
    out: dict[str, int] = {}
    for row in reader:
        if len(row) <= vol_idx:
            continue
        hhmm = _ts_hhmm(row, headers)
        if not hhmm:
            continue
        try:
            out[hhmm] = out.get(hhmm, 0) + int(float(row[vol_idx]))
        except (ValueError, TypeError):
            pass
    return out


def fetch_call_oi(date_yyyymmdd: str, strike: float) -> int:
    """SPXW 0DTE call OI snapshot for the day (daily-reported)."""
    try:
        r = SESSION.get(f'{THETA}/option/history/open_interest', params={
            'symbol': 'SPXW', 'expiration': date_yyyymmdd, 'strike': f'{strike:.2f}',
            'right': 'put', 'date': date_yyyymmdd, 'format': 'csv',
        }, timeout=15)
    except Exception:
        return 0
    if not r.ok or not r.text or r.text.startswith('<'):
        return 0
    reader = csv.reader(io.StringIO(r.text))
    headers = next(reader, None)
    if not headers:
        return 0
    oi_idx = headers.index('open_interest') if 'open_interest' in headers else None
    if oi_idx is None:
        for cand in ('openInterest', 'oi'):
            if cand in headers:
                oi_idx = headers.index(cand); break
    if oi_idx is None:
        return 0
    rows = [row for row in reader if row and len(row) > oi_idx]
    if not rows:
        return 0
    try:
        return int(float(rows[-1][oi_idx]))
    except (ValueError, TypeError):
        return 0


def fetch_call_mids_day(date_yyyymmdd: str, strike: float) -> dict[str, float]:
    """Per-minute call mid as {HH:MM: (bid+ask)/2} for 09:30 → 14:00:59 ET.

    Each 1-min bar yields ONE representative mid: the last (bid, ask) sample
    in that minute with bid≥0, ask≥0, ask≥bid. Missing minutes are absent.
    """
    try:
        r = SESSION.get(f'{THETA}/option/history/quote', params={
            'symbol': 'SPXW', 'expiration': date_yyyymmdd, 'strike': f'{strike:.2f}',
            'right': 'put', 'date': date_yyyymmdd, 'interval': '1m',
            'start_time': '09:30:00.000', 'end_time': '14:00:59.999', 'format': 'csv',
        }, timeout=20)
    except Exception:
        return {}
    if not r.ok or not r.text or r.text.startswith('<'):
        return {}
    reader = csv.reader(io.StringIO(r.text))
    headers = next(reader, None)
    if not headers:
        return {}
    bid_idx = headers.index('bid') if 'bid' in headers else 7
    ask_idx = headers.index('ask') if 'ask' in headers else 11
    # Per-minute: keep the LAST valid (bid, ask) seen
    by_min: dict[str, tuple[float, float]] = {}
    for row in reader:
        if len(row) <= max(bid_idx, ask_idx):
            continue
        hhmm = _ts_hhmm(row, headers)
        if not hhmm:
            continue
        try:
            b = float(row[bid_idx]); a = float(row[ask_idx])
            if b >= 0 and a >= 0 and a >= b:
                by_min[hhmm] = (b, a)
        except ValueError:
            pass
    return {hhmm: round((b + a) / 2, 4) for hhmm, (b, a) in by_min.items()}


# ── Core ──
def snap5(x: float) -> int:
    return int(round(x / 5.0) * 5)


def _valid_timestamps(date_iso: str) -> list[str]:
    """Timestamps that are <= market close on this date (omit past-close on half-days)."""
    close_hh, close_mm = (13, 0) if date_iso in HALF_DAYS else (16, 0)
    keep = []
    for t in TIMESTAMP_GRID:
        h, m = int(t[:2]), int(t[3:])
        if (h, m) <= (close_hh, close_mm):
            keep.append(t)
    return keep


def _minutes_before(t: str) -> list[str]:
    """All HH:MM from 09:30 → (t - 1 minute), inclusive."""
    end_h, end_m = int(t[:2]), int(t[3:])
    out = []
    h, m = 9, 30
    while (h, m) < (end_h, end_m):
        out.append(f'{h:02d}:{m:02d}')
        m += 1
        if m >= 60:
            h += 1; m -= 60
    return out


def _T_to_close_years(t: str, date_iso: str) -> float:
    """T to close in years from timestamp t."""
    close_h, close_m = (13, 0) if date_iso in HALF_DAYS else (16, 0)
    h, m = int(t[:2]), int(t[3:])
    mins = (close_h - h) * 60 + (close_m - m)
    if mins < 15:
        mins = 15  # defensive floor
    return mins / (365.0 * 24 * 60)


def build_day(date_iso: str, verbose: bool = True) -> dict:
    """Build a multi-timestamp record for one trading day.

    Returns:
      {date, spxOpen, spxClose, vixOpen, vixClose,
       by_time: {hh:mm: {center, centerOI, mids, midsOI}}}
    """
    yyyymmdd = date_iso.replace('-', '')
    spxOpen, spxClose, spot_by_hhmm = read_spx_bars(date_iso)
    vixOpen, vixClose = read_vix_bars(date_iso)
    if verbose:
        print(f'  spxOpen={spxOpen} spxClose={spxClose} '
              f'vixOpen={vixOpen} vixClose={vixClose}')

    # Strike search window: ±5% of spxOpen (stable across the day)
    lo, hi = spxOpen * 0.95, spxOpen * 1.05
    strikes = list_strikes_0dte(yyyymmdd, lo, hi)
    if not strikes:
        raise RuntimeError(f'No 0DTE strikes for {date_iso}')
    if verbose:
        print(f'  {len(strikes)} strikes in [{lo:.0f}, {hi:.0f}]')

    # Per-strike full-day fetch: vol bars, mid bars, OI (3 calls per strike, parallel)
    vol_by_strike: dict[float, dict[str, int]]   = {}
    mid_by_strike: dict[float, dict[str, float]] = {}
    oi_by_strike : dict[float, int]              = {}

    def fetch_one(K: float):
        v = fetch_call_volume_day(yyyymmdd, K)
        m = fetch_call_mids_day(yyyymmdd, K)
        o = fetch_call_oi(yyyymmdd, K)
        return K, v, m, o

    with ThreadPoolExecutor(max_workers=16) as ex:
        futs = [ex.submit(fetch_one, K) for K in strikes]
        for f in as_completed(futs):
            K, v, m, o = f.result()
            vol_by_strike[K] = v
            mid_by_strike[K] = m
            oi_by_strike[K]  = o

    if verbose:
        total_strikes_with_data = sum(1 for K in strikes if mid_by_strike.get(K))
        print(f'  fetched: {total_strikes_with_data}/{len(strikes)} strikes with quotes')

    # Build per-timestamp output
    valid_ts = _valid_timestamps(date_iso)
    by_time: dict[str, dict] = {}

    # Precompute cumulative volume per strike up to each timestamp (running sum)
    # cum_vol_at[T][K] = Σ vol_by_strike[K][m] for m in [09:30 .. T-1min]
    cum_vol_at: dict[str, dict[float, int]] = {}
    running: dict[float, int] = {K: 0 for K in strikes}
    # We accumulate minute-by-minute from 09:30 until 13:59 (or 14:00); pick off
    # snapshots when we hit a T in valid_ts.
    valid_ts_set = set(valid_ts)
    cur_h, cur_m = 9, 30
    # End of the cumulative walk: stop at the last minute we'll need, i.e.,
    # (max(valid_ts) - 1 minute). For T=14:00 the last cumulative minute is 13:59.
    last_h, last_m = int(valid_ts[-1][:2]), int(valid_ts[-1][3:])
    # We snapshot AT (h, m) when (h, m) is in valid_ts, BEFORE adding (h, m)'s
    # own volume to running. Then we add the volume at (h, m) into running, and
    # advance.
    while (cur_h, cur_m) <= (last_h, last_m):
        hhmm = f'{cur_h:02d}:{cur_m:02d}'
        if hhmm in valid_ts_set:
            cum_vol_at[hhmm] = dict(running)
        # Add this minute's volume into running for any subsequent snapshot
        for K in strikes:
            running[K] += vol_by_strike.get(K, {}).get(hhmm, 0)
        cur_m += 1
        if cur_m >= 60:
            cur_h += 1; cur_m -= 60

    for T in valid_ts:
        T_years = _T_to_close_years(T, date_iso)
        # Spot at T from SPX 1-min bars; fallback to spxOpen if missing
        spot_T = spot_by_hhmm.get(T) or spxOpen

        gex_vol: dict[float, float] = {}
        gex_oi : dict[float, float] = {}
        mid_at_T: dict[float, float] = {}

        for K in strikes:
            mid = mid_by_strike.get(K, {}).get(T)
            if mid is None or mid <= 0:
                continue
            mid_at_T[K] = mid
            iv = implied_vol(spot_T, K, T_years, mid, is_call=False)
            if not iv:
                continue
            g = bs_gamma(spot_T, K, T_years, iv)
            if g <= 0:
                continue
            S2 = spot_T * spot_T
            cv = cum_vol_at.get(T, {}).get(K, 0)
            oi = oi_by_strike.get(K, 0)
            gex_vol[K] = g * cv * S2 * 100 * 0.01
            gex_oi[K]  = g * oi * S2 * 100 * 0.01

        if not gex_vol:
            # Sparse data at this timestamp (e.g., distant strikes never trade).
            # Still record the timestamp with empty grids so the UI can flag it.
            by_time[T] = {'center': None, 'centerOI': None, 'mids': {}, 'midsOI': {}}
            continue

        K_vol = max(gex_vol, key=gex_vol.get)
        K_oi  = max(gex_oi,  key=gex_oi.get) if gex_oi else K_vol
        center   = snap5(K_vol)
        centerOI = snap5(K_oi)

        # Assemble mid grids around each center (mids at strike center+off, snapped)
        def grid_for(c: int) -> dict[str, float]:
            out: dict[str, float] = {}
            for off in MID_GRID_STEPS:
                K_target = float(c + off)
                m = mid_at_T.get(K_target)
                if m is not None:
                    out[str(off)] = m
            return out

        mids   = grid_for(center)
        midsOI = grid_for(centerOI) if centerOI != center else dict(mids)

        by_time[T] = {
            'center'  : center,
            'centerOI': centerOI,
            'mids'    : mids,
            'midsOI'  : midsOI,
        }

    return {
        'date'    : date_iso,
        'spxOpen' : spxOpen,
        'spxClose': spxClose,
        'vixOpen' : vixOpen,
        'vixClose': vixClose,
        'by_time' : by_time,
    }


def _atomic_merge_day(entry: dict) -> int:
    """Merge `entry` into gxbf_bt_data.json by date, atomic write. Returns total count."""
    existing = []
    if NEGAMMA_FILE.exists():
        try:
            with open(NEGAMMA_FILE) as f:
                existing = json.load(f)
        except (json.JSONDecodeError, OSError):
            existing = []
    by_date = {e['date']: e for e in existing}
    by_date[entry['date']] = entry
    merged = sorted(by_date.values(), key=lambda x: x['date'])
    tmp = NEGAMMA_FILE.with_suffix('.json.tmp')
    with open(tmp, 'w') as f:
        json.dump(merged, f)
    os.replace(tmp, NEGAMMA_FILE)
    return len(merged)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--date', help='single date YYYY-MM-DD')
    ap.add_argument('--from', dest='from_date')
    ap.add_argument('--to',   dest='to_date')
    ap.add_argument('--dry-run', action='store_true', help='compute but do not write')
    args = ap.parse_args()

    # Build target date list — weekdays with both SPX + VIX CSVs present
    if args.date:
        dates = [args.date]
    elif args.from_date and args.to_date:
        dates = []
        d = datetime.fromisoformat(args.from_date)
        end = datetime.fromisoformat(args.to_date)
        while d <= end:
            if d.weekday() < 5:
                iso = d.strftime('%Y-%m-%d')
                yyyymmdd = iso.replace('-', '')
                if iso in US_MARKET_HOLIDAYS:
                    d += timedelta(days=1); continue
                if (SPX_DIR / f'SPX_{yyyymmdd}.csv').exists() and (VIX_DIR / f'VIX_{yyyymmdd}.csv').exists():
                    dates.append(iso)
            d += timedelta(days=1)
    else:
        print('Provide --date or --from/--to'); return

    print(f'Processing {len(dates)} trading day(s) [{dates[0]} → {dates[-1]}]')
    print(f'Timestamp grid: {len(TIMESTAMP_GRID)} steps ({TIMESTAMP_GRID[0]} → {TIMESTAMP_GRID[-1]})')
    t0 = time.time()
    for i, d in enumerate(dates, 1):
        elapsed = time.time() - t0
        rate = i / max(elapsed, 0.001)
        eta_min = (len(dates) - i) / max(rate, 0.001) / 60
        print(f'[{i}/{len(dates)}] {d}  (elapsed {elapsed/60:.1f}min, ETA {eta_min:.1f}min)')
        try:
            entry = build_day(d, verbose=False)
            n_ts = len(entry['by_time'])
            first_ts = next(iter(entry['by_time']))
            c0 = entry['by_time'][first_ts].get('center')
            cOI0 = entry['by_time'][first_ts].get('centerOI')
            print(f'  OK: {n_ts} timestamps, {first_ts} center={c0} centerOI={cOI0}')
            if not args.dry_run:
                total = _atomic_merge_day(entry)
                if i == 1 or i == len(dates) or i % 25 == 0:
                    print(f'    (gxbf_bt_data.json now holds {total} entries)')
        except Exception as e:
            print(f'  FAIL: {e}')

    if args.dry_run:
        print(f'\n--dry-run: nothing written.')


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Reconstruct missing Straddle / GXBF / Diagonal trades using ThetaData.
Does NOT modify history_data.json — only reports P&L impact."""
import json, requests, csv, io, math
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

THETA = 'http://localhost:25503/v3'
SESSION = requests.Session()
SCHWAB_TOKEN = None

def get_schwab_token():
    global SCHWAB_TOKEN
    if SCHWAB_TOKEN: return SCHWAB_TOKEN
    import subprocess
    r = subprocess.check_output(['npx','wrangler','kv','key','get','--namespace-id','be47dd5d2fb34ec79e5a34f0e241f125','schwab_tokens','--remote'], stderr=subprocess.DEVNULL)
    SCHWAB_TOKEN = json.loads(r)['access']
    return SCHWAB_TOKEN

# ──────────────────────────────────────────────────────────────────
# Generic data fetchers
# ──────────────────────────────────────────────────────────────────
def schwab_spx_at(date_iso, hh, mm):
    """SPX 1-min close at HH:MM ET via Schwab."""
    tok = get_schwab_token()
    y, m, d = map(int, date_iso.split('-'))
    start = int(datetime(y, m, d, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    end = int(datetime(y, m, d, 22, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    url = f'https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24SPX&periodType=day&period=1&frequencyType=minute&frequency=1&startDate={start}&endDate={end}&needExtendedHoursData=true'
    r = requests.get(url, headers={'Authorization': f'Bearer {tok}'}, timeout=15)
    if r.status_code != 200: return None
    data = r.json()
    for c in (data.get('candles') or []):
        dt = datetime.fromtimestamp(c['datetime']/1000, tz=timezone.utc)
        et_h = (dt.hour + (-4)) % 24
        if et_h == hh and dt.minute == mm:
            return c['close']
    return None

def schwab_spx_close_4pm(date_iso):
    tok = get_schwab_token()
    y, m, d = map(int, date_iso.split('-'))
    start = int(datetime(y, m, d, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    end = int(datetime(y, m, d, 22, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    url = f'https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24SPX&periodType=day&period=1&frequencyType=minute&frequency=1&startDate={start}&endDate={end}&needExtendedHoursData=true'
    r = requests.get(url, headers={'Authorization': f'Bearer {tok}'}, timeout=15)
    if r.status_code != 200: return None
    data = r.json()
    last = None
    for c in (data.get('candles') or []):
        dt = datetime.fromtimestamp(c['datetime']/1000, tz=timezone.utc)
        et_h = (dt.hour + (-4)) % 24
        if et_h * 60 + dt.minute <= 16 * 60:
            last = c['close']
    return last

def get_option_mid(date_dc, exp_dc, strike, right, hh, mm):
    """ThetaData 1-min option quote → (bid+ask)/2 at HH:MM."""
    params = {'symbol':'SPXW','expiration':exp_dc,'strike':f'{strike:.3f}','right':right,'date':date_dc,'interval':'1m'}
    try:
        r = SESSION.get(f'{THETA}/option/history/quote', params=params, timeout=15)
    except: return None
    if r.status_code != 200: return None
    target = f'T{hh:02d}:{mm:02d}:00'
    for ln in r.text.strip().split('\n')[1:]:
        cols = [c.strip('"') for c in ln.split(',')]
        if len(cols) < 12: continue
        if target in cols[4]:
            try:
                bid, ask = float(cols[7]), float(cols[11])
                if bid <= 0 and ask <= 0: return None
                return (bid + ask) / 2
            except: continue
    return None

def snap5(x): return round(x / 5) * 5
def snap10(x): return round(x / 10) * 10

# ──────────────────────────────────────────────────────────────────
# 1. STRADDLE — 9:32 ATM call + put, settle SPX 4:00 close
# ──────────────────────────────────────────────────────────────────
def reconstruct_straddle(date_iso):
    spot = schwab_spx_at(date_iso, 9, 32)
    if not spot: return None
    K = snap5(spot)
    dc = date_iso.replace('-', '')
    c_entry = get_option_mid(dc, dc, K, 'call', 9, 32)
    p_entry = get_option_mid(dc, dc, K, 'put',  9, 32)
    if c_entry is None or p_entry is None: return None
    spx_close = schwab_spx_close_4pm(date_iso)
    if spx_close is None: return None
    settle = abs(spx_close - K)  # ATM call+put intrinsic
    pnl = round((settle - (c_entry + p_entry)) * 100)
    return {'date': date_iso, 'spot': spot, 'K': K, 'cost': round(c_entry+p_entry,2),
            'close': spx_close, 'settle': round(settle,2), 'pnl': pnl}

# ──────────────────────────────────────────────────────────────────
# 2. DIAGONAL — 12:30 entry, exit next day 12:30, short 1DTE+30 ITM, long ~25DTE -40
# ──────────────────────────────────────────────────────────────────
def find_next_trading_day(date_iso, hist_dates):
    idx = next((i for i,d in enumerate(hist_dates) if d > date_iso), None)
    return hist_dates[idx] if idx is not None and idx < len(hist_dates) else None

def list_spxw_expirations(after_date_iso, target_dte=25, tol=5):
    """Find SPXW expirations 20-30 DTE from after_date."""
    r = SESSION.get(f'{THETA}/option/list/expirations', params={'symbol':'SPXW'}, timeout=30)
    target = datetime.strptime(after_date_iso, '%Y-%m-%d').date()
    cands = []
    for ln in r.text.strip().split('\n')[1:]:
        cols = [c.strip('"') for c in ln.split(',')]
        if len(cols) >= 2:
            try:
                exp = datetime.strptime(cols[1], '%Y-%m-%d').date()
                dte = (exp - target).days
                if abs(dte - target_dte) <= tol:
                    cands.append((cols[1], dte))
            except: continue
    cands.sort(key=lambda x: abs(x[1] - target_dte))
    return cands

def reconstruct_diagonal(date_iso, exit_date_iso):
    spot = schwab_spx_at(date_iso, 12, 30)
    if not spot: return None
    k_short = snap5(spot + 30)
    k_long  = snap5(spot - 10)  # short - 40
    short_exp_dc = exit_date_iso.replace('-', '')
    long_cands = list_spxw_expirations(date_iso)
    if not long_cands: return None
    long_exp = long_cands[0][0]
    long_exp_dc = long_exp.replace('-', '')
    entry_dc = date_iso.replace('-', '')
    exit_dc = exit_date_iso.replace('-', '')

    se = get_option_mid(entry_dc, short_exp_dc, k_short, 'put', 12, 30)
    le = get_option_mid(entry_dc, long_exp_dc,  k_long,  'put', 12, 30)
    if se is None: return {'date': date_iso, 'error': f'no short entry @{k_short}'}
    # Try long ±5 tolerance
    for off in [0, -5, 5, -10, 10]:
        le = get_option_mid(entry_dc, long_exp_dc, k_long+off, 'put', 12, 30)
        if le is not None:
            k_long_actual = k_long + off
            break
    else: return {'date': date_iso, 'error': f'no long entry near {k_long}'}

    sx = get_option_mid(exit_dc, short_exp_dc, k_short, 'put', 12, 30)
    lx = get_option_mid(exit_dc, long_exp_dc,  k_long_actual, 'put', 12, 30)
    if sx is None or lx is None: return {'date': date_iso, 'error': 'no exit'}

    entry_debit = le - se
    exit_credit = lx - sx
    pnl = round((exit_credit - entry_debit) * 100)
    return {'date': date_iso, 'k_short': k_short, 'k_long': k_long_actual,
            'long_exp': long_exp, 'entry_debit': round(entry_debit,2),
            'exit_credit': round(exit_credit,2), 'pnl': pnl}

# ──────────────────────────────────────────────────────────────────
# 3. GXBF — body via γ × volume, ±30 wings, 1 contract long-call butterfly
# Approximation: use snap5(spot + 0) as body — works for most non-OPEX days
# Real methodology uses Black-Scholes γ × vol × S² but that's much more complex.
# For initial estimate we'll use the simplified approach.
# ──────────────────────────────────────────────────────────────────
def reconstruct_gxbf_simple(date_iso):
    """Simplified GXBF estimate using ATM center + ±30 wings."""
    spot = schwab_spx_at(date_iso, 9, 36)
    if not spot: return None
    # Per the GXBF methodology, body is the gamma peak. Approximation: ATM.
    # This won't be exact but gives a reasonable estimate.
    body = snap5(spot)
    lower = body - 30
    upper = body + 30
    dc = date_iso.replace('-', '')
    b = get_option_mid(dc, dc, body, 'call', 9, 36)
    l = get_option_mid(dc, dc, lower, 'call', 9, 36)
    u = get_option_mid(dc, dc, upper, 'call', 9, 36)
    if None in (b, l, u): return None
    debit = l - 2*b + u
    spx_close = schwab_spx_close_4pm(date_iso)
    if spx_close is None: return None
    intrinsic = max(0, spx_close - lower) - 2*max(0, spx_close - body) + max(0, spx_close - upper)
    pnl = round((intrinsic - debit) * 100)
    return {'date': date_iso, 'spot': spot, 'body': body, 'debit': round(debit,2),
            'close': spx_close, 'intrinsic': round(intrinsic,2), 'pnl': pnl,
            'note': 'ATM proxy (real GXBF center via γ×vol may differ)'}

# ──────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────
def main():
    audit = json.load(open('/tmp/full_signal_audit.json'))
    hist = json.load(open('history_data.json'))
    hist_dates = sorted({r['date'] for r in hist if r.get('date')})

    print("\n══ STRADDLE — 23 reconstructions ══")
    print(f"{'Date':<12} {'Spot':<10} {'K':<8} {'Cost':<7} {'Close':<10} {'P&L'}")
    print('─' * 70)
    strad_total = 0; strad_count = 0
    for m in audit['strad_missing']:
        r = reconstruct_straddle(m['date'])
        if r and r.get('pnl') is not None:
            sign = '+' if r['pnl'] >= 0 else ''
            print(f"{r['date']:<12} {r['spot']:<10} {r['K']:<8} {r['cost']:<7} {r['close']:<10} {sign}${r['pnl']:,}")
            strad_total += r['pnl']; strad_count += 1
        else:
            print(f"{m['date']:<12} ERROR")
    print(f"\nStraddle: {strad_count} reconstructed, NET = ${strad_total:+,}")

    print("\n\n══ DIAGONAL — 11 reconstructions ══")
    diag_total = 0; diag_count = 0
    for m in audit['diag_missing']:
        next_day = find_next_trading_day(m['date'], hist_dates)
        if not next_day: print(f"{m['date']:<12} no next trading day"); continue
        r = reconstruct_diagonal(m['date'], next_day)
        if r and r.get('pnl') is not None:
            sign = '+' if r['pnl'] >= 0 else ''
            print(f"{r['date']:<12} short@{r['k_short']} long@{r['k_long']} debit ${r['entry_debit']} credit ${r['exit_credit']} → {sign}${r['pnl']:,}")
            diag_total += r['pnl']; diag_count += 1
        else:
            print(f"{m['date']:<12} ERROR: {r.get('error') if r else 'no data'}")
    print(f"\nDiagonal: {diag_count} reconstructed, NET = ${diag_total:+,}")

    print("\n\n══ GXBF — 13 reconstructions (using ATM-proxy center) ══")
    gxbf_total = 0; gxbf_count = 0
    for m in audit['gxbf_missing']:
        r = reconstruct_gxbf_simple(m['date'])
        if r and r.get('pnl') is not None:
            sign = '+' if r['pnl'] >= 0 else ''
            print(f"{r['date']:<12} body={r['body']} debit ${r['debit']} close {r['close']} → {sign}${r['pnl']:,}")
            gxbf_total += r['pnl']; gxbf_count += 1
        else:
            print(f"{m['date']:<12} ERROR")
    print(f"\nGXBF (ATM proxy): {gxbf_count} reconstructed, NET = ${gxbf_total:+,}")

    print(f"\n══ GRAND TOTAL ══")
    print(f"  Straddle: ${strad_total:+,}")
    print(f"  Diagonal: ${diag_total:+,}")
    print(f"  GXBF:     ${gxbf_total:+,} (approximate)")
    print(f"  COMBINED: ${strad_total + diag_total + gxbf_total:+,}")

if __name__ == '__main__':
    main()

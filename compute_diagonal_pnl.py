#!/usr/bin/env python3
"""
Python port of the JS diagonal backtester in diagonal.html.

Loads the 7 gzipped diagonal_real bundles, runs the backtest with a parameter
dict (same keys as the JS getParams() output), and returns a per-date pnl dict
plus a list of trade records. Also knows how to merge those pnl values into the
history_data.json diagPL column without touching any other field.

Intended to match diagonal.html defaults *exactly* — same strike rounding,
same quoteOk gate, same 3-step exit fallback (REAL -> intrinsic settlement ->
Black-Scholes), same prefer_longer DTE resolver, same OPEX / VIX_MID /
ALL_EARNINGS filter sets.
"""

from __future__ import annotations

import gzip
import json
import math
import os
from pathlib import Path
import shutil
import sys
from bisect import bisect_left
from datetime import date, datetime, timedelta, timezone
from typing import Any


# ----------------------------------------------------------------------
# Paths
# ----------------------------------------------------------------------
HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(HERE, "data")
HISTORY_PATH = os.path.join(HERE, "history_data.json")
HALFDAY_PATH = os.path.join(DATA_DIR, "spx_halfday_close.json")
BS_DATA_PATH = os.path.join(DATA_DIR, "diagonal_bs_data.json")
INDEX_PATH = os.path.join(DATA_DIR, "diagonal_real_index.json")


# ----------------------------------------------------------------------
# Defaults — must mirror diagonal.html exactly
# ----------------------------------------------------------------------
DEFAULT_PARAMS: dict[str, Any] = {
    "date_from": "2023-01-01",
    "date_to": "2026-12-31",
    "entry_time": "12:30",        # canonical default — same as exit_time so rolls cleanly
    "exit_time": "12:30",         # 12:30 ET next trading day — no two diagonals active at once
    "short_dte": 1,
    "short_dte_tol": 3,           # hardcoded SHORT_DTE_TOL in the JS
    "short_offset": 10,           # pts ITM — 10 ITM (safer-tail config, 2026-06-09 sweep)
    "long_dte": 25,
    "long_dte_tol": 5,
    "long_offset": 20,            # pts BELOW short → long is 10 pts OTM (width=20)
    "dte_fallback": "prefer_longer",
    "contracts": 1,
    "ignore_margin": True,        # fixed-contract mode (matches JS when "Contracts" is set)
    "special_mode": "exclude",
    # Canonical 6-filter stack — calendar + VIX regime + COR1M floor.
    # OPEX-1, EOM, EOM-1, NM, VIX_MID, COR1M_LOW. Earnings filters dropped
    # 2026-04-29 (per backtest sweep). COR1M_LOW added 2026-06-09 after 3yr
    # review: below COR1M 10 the strategy bled $9.7k @ 50% WR (34 trades).
    "special_active": {"OPEX-1", "EOM", "EOM-1", "NM", "VIX_MID", "COR1M_LOW"},
    "risk_free": 0.045,
    "snap_tolerance": 25,         # $25 strike-snap cap
    # VIX 20-day percentile band — TODAY's first-post-9:30 VIX rank in the
    # prior 20 days' first-post-9:30 VIX. Trades INSIDE (lo, hi] are skipped
    # when VIX_MID is in special_active. Default 50-90 — re-optimized 2026-06-02.
    "vix_band": (50.0, 80.0),
    # COR1M floor at 9:30 ET. Days with cor1m < this are skipped when
    # COR1M_LOW is in special_active. Default 10 (empirically derived).
    "cor1m_min": 10.0,
}


# ----------------------------------------------------------------------
# Math helpers (direct ports)
# ----------------------------------------------------------------------
def norm_cdf(x: float) -> float:
    """Abramowitz-Stegun 26.2.17, matches the JS normCDF."""
    a1, a2, a3 = 0.254829592, -0.284496736, 1.421413741
    a4, a5, p = -1.453152027, 1.061405429, 0.3275911
    sign = -1.0 if x < 0 else 1.0
    x = abs(x) / math.sqrt(2.0)
    t = 1.0 / (1.0 + p * x)
    y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * math.exp(-x * x)
    return 0.5 * (1.0 + sign * y)


def bs_put(S: float, K: float, T_years: float, sigma: float, r: float) -> float:
    """Black-Scholes put. At/below zero T returns intrinsic max(0, K-S)."""
    if T_years <= 0:
        return max(0.0, K - S)
    if sigma <= 0:
        sigma = 0.0001
    sqrt_t = math.sqrt(T_years)
    d1 = (math.log(S / K) + (r + sigma * sigma / 2.0) * T_years) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t
    return K * math.exp(-r * T_years) * norm_cdf(-d2) - S * norm_cdf(-d1)


def days_between(d1: str, d2: str) -> int:
    a = datetime.strptime(d1, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    b = datetime.strptime(d2, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return round((b - a).total_seconds() / 86400.0)


def weekday_from_iso(iso: str) -> int:
    y, m, d = [int(x) for x in iso.split("-")]
    # Python Monday=0..Sunday=6 already matches the JS (after shift)
    return date(y, m, d).weekday()


# ----------------------------------------------------------------------
# Data loading — reads per-(half, time) bundles (format: per_time_v2)
# ----------------------------------------------------------------------
def load_bundles(times: list[str] | None = None) -> dict[str, Any]:
    """Load per-(half, time) bundles and project into the shape the backtester
    expects: {dates, by_date[d].by_time[HHMM] = {spot, vix, quotes}}.

    `times` defaults to entry_time + exit_time only (two files per half = 14
    files total) — matches the JS loader, which only pulls the needed times.
    Pass a larger list to load more timestamps. Falls back to legacy
    all-times-per-half bundles if the new per-time layout is missing.
    """
    if times is None:
        times = [DEFAULT_PARAMS["entry_time"], DEFAULT_PARAMS["exit_time"]]
    # Dedupe preserving order
    seen = set()
    times = [t for t in times if not (t in seen or seen.add(t))]

    by_date: dict[str, dict[str, Any]] = {}

    # Preferred path: read index.json and per-(half, time) files
    if os.path.exists(INDEX_PATH):
        with open(INDEX_PATH) as f:
            idx = json.load(f)
        if idx.get("format") == "per_time_v2" and idx.get("halves"):
            halves = idx["halves"]
            tmpl = idx.get("file_template", "diagonal_real_{half}_t{time_compact}.json.gz")
            for half in halves:
                for t in times:
                    fname = (
                        tmpl.replace("{half}", half)
                            .replace("{time_compact}", t.replace(":", ""))
                            .replace("{time}", t)
                    )
                    path = os.path.join(DATA_DIR, fname)
                    if not os.path.exists(path):
                        continue
                    with gzip.open(path, "rb") as f:
                        bundle = json.load(f)
                    for d, row in bundle.get("by_date", {}).items():
                        rec = by_date.setdefault(d, {"by_time": {}})
                        rec["by_time"][t] = {
                            "spot": row.get("spot"),
                            "vix":  row.get("vix"),
                            "quotes": row.get("quotes", {}),
                        }
            dates = sorted(by_date.keys())
            return {"dates": dates, "by_date": by_date}

    # Legacy fallback: all-times-per-half bundles (old layout)
    legacy = sorted(
        p for p in os.listdir(DATA_DIR)
        if p.startswith("diagonal_real_") and p.endswith(".json.gz")
        and "_t" not in p  # exclude new per-time files
    )
    for name in legacy:
        path = os.path.join(DATA_DIR, name)
        with gzip.open(path, "rb") as f:
            bundle = json.load(f)
        for d, row in bundle["by_date"].items():
            by_date[d] = row
    dates = sorted(by_date.keys())
    return {"dates": dates, "by_date": by_date}


def load_halfday_close() -> dict[str, float]:
    with open(HALFDAY_PATH) as f:
        return json.load(f)


def load_bs_data() -> dict[str, Any]:
    """Load diagonal_bs_data.json — carries vix_14/spot_14 used by JS for VIX_MID
    percentile ranking. The bundle's by_time['14:00'].vix drifts from vix_14 by
    ~0.01–0.25 on many days (different snapshot source), which flips enough
    VIX_MID classifications to shift trade selection. To match the live JS
    backtester, we MUST use this file for VIX_MID."""
    with open(BS_DATA_PATH) as f:
        return json.load(f)


# ----------------------------------------------------------------------
# Special-day sets
# ----------------------------------------------------------------------
AAPL = [
    "2023-02-02","2023-05-04","2023-08-03","2023-11-02",
    "2024-02-01","2024-05-02","2024-08-01","2024-10-31",
    "2025-01-30","2025-05-01","2025-07-31","2025-10-30","2026-01-29",
]
GOOGL = [
    "2023-02-02","2023-04-25","2023-07-25","2023-10-24",
    "2024-01-30","2024-04-25","2024-07-23","2024-10-29",
    "2025-02-04","2025-04-24","2025-07-23","2025-10-29","2026-02-04",
]
MSFT = [
    "2023-01-24","2023-04-25","2023-07-25","2023-10-24",
    "2024-01-30","2024-04-25","2024-07-30","2024-10-30",
    "2025-01-29","2025-04-30","2025-07-30","2025-10-29","2026-01-28",
]
TSLA = [
    "2023-01-25","2023-04-19","2023-07-19","2023-10-18",
    "2024-01-24","2024-04-23","2024-07-23","2024-10-23",
    "2025-01-29","2025-04-22","2025-07-23","2025-10-22","2026-01-28",
]
META = [
    "2023-02-01","2023-04-26","2023-07-26","2023-10-25",
    "2024-02-01","2024-04-24","2024-07-31","2024-10-30",
    "2025-01-29","2025-04-30","2025-07-30","2025-10-29","2026-01-28",
]
NVDA = [
    "2023-02-22","2023-05-24","2023-08-23","2023-11-21",
    "2024-02-21","2024-05-22","2024-08-28","2024-11-19",
    "2025-02-26","2025-05-28","2025-08-27","2025-11-19","2026-02-25",
]
AMZN = [
    "2023-02-02","2023-04-27","2023-08-03","2023-10-26",
    "2024-01-30","2024-04-30","2024-07-30","2024-10-31",
    "2025-02-06","2025-05-01","2025-08-01","2025-10-30","2026-02-05",
]
# NYSE/NASDAQ trading holidays — used for calendar-based EOM detection
# (bundle endpoints don't reliably tell us "last trading day of month" near
# the leading edge). Mirrors signal-engine.js `holidays`.
HOLIDAYS = {
    "2022-06-20","2022-07-04","2022-09-05","2022-11-24","2022-12-26",
    "2023-01-02","2023-01-16","2023-02-20","2023-04-07","2023-05-29",
    "2023-06-19","2023-07-04","2023-09-04","2023-11-23","2023-12-25",
    "2024-01-01","2024-01-15","2024-02-19","2024-03-29","2024-05-27",
    "2024-06-19","2024-07-04","2024-09-02","2024-11-28","2024-12-25",
    "2025-01-01","2025-01-09","2025-01-20","2025-02-17","2025-04-18",
    "2025-05-26","2025-06-19","2025-07-04","2025-09-01","2025-11-27","2025-12-25",
    "2026-01-01","2026-01-19","2026-02-16","2026-04-03","2026-05-25",
    "2026-06-19","2026-07-03","2026-09-07","2026-11-26","2026-12-25",
    "2027-01-01","2027-01-18","2027-02-15","2027-03-26","2027-05-31",
    "2027-06-18","2027-07-05","2027-09-06","2027-11-25","2027-12-24",
}

FED_DAYS = [
    "2023-02-01","2023-03-22","2023-05-03","2023-06-14","2023-07-26",
    "2023-09-20","2023-11-01","2023-12-13",
    "2024-01-31","2024-03-20","2024-05-01","2024-06-12","2024-07-31",
    "2024-09-18","2024-11-07","2024-12-18",
    "2025-01-29","2025-03-19","2025-05-07","2025-06-18","2025-07-30",
    "2025-09-17","2025-10-29","2025-12-10",
    "2026-01-28","2026-03-18","2026-04-29","2026-06-10","2026-07-29",
    "2026-09-16","2026-11-04","2026-12-09",
]
CPI_DAYS = [
    "2023-01-12","2023-02-14","2023-03-14","2023-04-12","2023-05-10",
    "2023-06-13","2023-07-12","2023-08-10","2023-09-13","2023-10-12",
    "2023-11-14","2023-12-12",
    "2024-01-11","2024-02-13","2024-03-12","2024-04-10","2024-05-15",
    "2024-06-12","2024-07-11","2024-08-14","2024-09-11","2024-10-10",
    "2024-11-13","2024-12-11",
    "2025-01-15","2025-02-12","2025-03-12","2025-04-10","2025-05-13",
    "2025-06-11","2025-07-15","2025-08-12","2025-09-11","2025-10-24",
    "2025-12-18",
    "2026-01-13","2026-02-13","2026-03-11",
]
OPEX_OVERRIDES = {"2025-04-18": "2025-04-17"}  # Good Friday
EARN_TICKERS = ("AAPL", "GOOGL", "MSFT", "TSLA", "META", "NVDA", "AMZN")


def _third_friday(y: int, m: int) -> str:
    """Same as JS _thirdFriday — 3rd Friday (UTC)."""
    d = date(y, m, 1)
    fridays = 0
    while True:
        if d.weekday() == 4:  # Friday
            fridays += 1
            if fridays == 3:
                return d.isoformat()
        d += timedelta(days=1)


def build_special_sets(trading_days: list[str], by_date_entry_snapshot: dict[str, dict],
                       vix_band: tuple[float, float] = (50.0, 90.0),
                       cor1m_by_date: dict[str, float] | None = None,
                       cor1m_min: float = 10.0) -> dict[str, set]:
    """
    Faithful port of buildSpecialDateSets. `by_date_entry_snapshot` should be a
    dict keyed by date, each value having 'vix_14' and 'spot_14' (we build a
    tiny adapter from the entry-time REAL quotes — the JS uses the entry-time
    snapshot for VIX_MID).
    """
    sets: dict[str, set] = {k: set() for k in (
        "OPEX","OPEX-1","OPEX+1","EOM","EOM-1","NM","FED","CPI","VIX",
        "VIX_MID","VIX_DEAD","COR1M_LOW",
        "AAPL","GOOGL","MSFT","TSLA","META","NVDA","AMZN","ALL_EARNINGS",
    )}

    # COR1M_LOW set — populate from caller-supplied cor1m map. The 2026-06-09
    # diagonal rule: COR1M (9:30 ET open) below cor1m_min → skip the trade.
    if cor1m_by_date:
        for d in trading_days:
            c = cor1m_by_date.get(d)
            if c is not None and c < cor1m_min:
                sets["COR1M_LOW"].add(d)

    td_set = set(trading_days)

    # OPEX + VIX expiry (OPEX-2) + OPEX-1
    for y in range(2023, 2028):
        for m in range(1, 13):
            raw = _third_friday(y, m)
            opex_date = OPEX_OVERRIDES.get(raw, raw)
            if opex_date in td_set:
                sets["OPEX"].add(opex_date)
            opex_dt = datetime.strptime(opex_date, "%Y-%m-%d")
            opex_m1 = (opex_dt - timedelta(days=1)).strftime("%Y-%m-%d")
            vix_dt  = (opex_dt - timedelta(days=2)).strftime("%Y-%m-%d")
            sets["OPEX-1"].add(opex_m1)
            sets["VIX"].add(vix_dt)

    # OPEX+1
    sorted_td = trading_days  # already sorted
    for o in list(sets["OPEX"]):
        idx = bisect_left(sorted_td, o)
        # next trading day strictly after o
        nxt = None
        for i in range(idx, len(sorted_td)):
            if sorted_td[i] > o:
                nxt = sorted_td[i]
                break
        if nxt:
            sets["OPEX+1"].add(nxt)

    # NM / EOM / EOM-1
    # Use a CALENDAR (weekend + holiday) to detect last-trading-day-of-month,
    # not the bundle's endpoints. The bundle is incomplete at the leading
    # edge — e.g., on 2026-04-29 the bundle's last date is 04-29 but the
    # actual EOM is 04-30 (Thu). Using ds[-1] would falsely flag 04-29 as
    # EOM and 04-28 as EOM-1 even though those are EOM-1 and EOM-2 in the
    # real calendar. (datetime imports already at module top.)
    by_month: dict[str, list[str]] = {}
    for d in trading_days:
        by_month.setdefault(d[:7], []).append(d)

    def _next_td_calendar(date_str: str) -> str | None:
        """Return next trading day after date_str using weekend + holiday cal."""
        y, m, d = map(int, date_str.split("-"))
        cur = date(y, m, d) + timedelta(days=1)
        for _ in range(10):  # at most 10 days to find next trading day
            iso = cur.strftime("%Y-%m-%d")
            if cur.weekday() < 5 and iso not in HOLIDAYS:
                return iso
            cur += timedelta(days=1)
        return None

    def _prev_td_calendar(date_str: str) -> str | None:
        """Return previous trading day before date_str (weekend + holiday cal)."""
        y, m, d = map(int, date_str.split("-"))
        cur = date(y, m, d) - timedelta(days=1)
        for _ in range(10):
            iso = cur.strftime("%Y-%m-%d")
            if cur.weekday() < 5 and iso not in HOLIDAYS:
                return iso
            cur -= timedelta(days=1)
        return None

    # EOM/EOM-1/NM: pure CALENDAR computation, then flag only if present in
    # data. Parity class 3 (2026-06-09): the old loop walked in-data dates and
    # `break`-ed on a found EOM — when the true EOM was a half-day session
    # ABSENT from the bundle (e.g. 2024-11-29), no in-data date qualified and
    # EOM-1 (2024-11-27, which the LIVE signal blocks via isEomN) was never
    # flagged. Matches signal-engine.js isLastTradeMo/isEomN/isFirstTradeMo
    # semantics: half-days count as trading days; only weekends+holidays skip.
    for ym, ds in by_month.items():
        y, m = map(int, ym.split("-"))
        # True calendar EOM = walk back from the 1st of next month
        nm_y, nm_m = (y + 1, 1) if m == 12 else (y, m + 1)
        first_next = date(nm_y, nm_m, 1).strftime("%Y-%m-%d")
        cal_eom = _prev_td_calendar(first_next)
        cal_eom1 = _prev_td_calendar(cal_eom) if cal_eom else None
        # True calendar NM = walk forward from the 1st of this month
        first_this = date(y, m, 1).strftime("%Y-%m-%d")
        d1 = date(y, m, 1)
        cal_nm = None
        for _ in range(10):
            iso = d1.strftime("%Y-%m-%d")
            if d1.weekday() < 5 and iso not in HOLIDAYS:
                cal_nm = iso; break
            d1 += timedelta(days=1)
        ds_set = set(ds)
        if cal_eom in ds_set:  sets["EOM"].add(cal_eom)
        if cal_eom1 in ds_set: sets["EOM-1"].add(cal_eom1)
        if cal_nm in ds_set:   sets["NM"].add(cal_nm)

    # FED + CPI
    for d in FED_DAYS:
        sets["FED"].add(d)
    for d in CPI_DAYS:
        sets["CPI"].add(d)

    # Earnings — copy lists verbatim, build ALL_EARNINGS union
    for d in AAPL: sets["AAPL"].add(d)
    for d in GOOGL: sets["GOOGL"].add(d)
    for d in MSFT: sets["MSFT"].add(d)
    for d in TSLA: sets["TSLA"].add(d)
    for d in META: sets["META"].add(d)
    for d in NVDA: sets["NVDA"].add(d)
    for d in AMZN: sets["AMZN"].add(d)
    for t in EARN_TICKERS:
        for d in sets[t]:
            sets["ALL_EARNINGS"].add(d)

    # VIX_MID / VIX_DEAD — faithful port of signal-engine.js computeVixPct20d
    # (CLAUDE.md rule 11): TODAY's vix_open ranked against the prior 20
    # trading days' vix_CLOSE values (NOT prior vix_opens — that was the P5
    # drift bug, re-found by scripts/test-diagonal-parity.js on 2026-06-09:
    # 27 dates of diagPL history were computed under the wrong methodology).
    #
    # Canonical semantics ported exactly:
    #   • priors: walk back over trading_days collecting per-day
    #     (vix_close ?? vix_open), valid (>0) only, until 20 found
    #     — mirrors diagonal.html closeForPctile = vix_close ?? vix_open
    #   • < 10 valid priors → inDeadZone=True (block; no i>=20 warm-up skip —
    #     the warm-up gate was dropped in JS per the 2026-06-09 audit P1 #11)
    #   • vix_today missing/invalid → inDeadZone=True (block)
    #   • pct = half-up round(100 * below / len(valid))  (Math.round parity;
    #     Python round() is banker's — do NOT use it here)
    #   • inDeadZone = lo < pct <= hi
    #
    # This Python copy exists because the backtester can't import JS; the
    # cross-language guard is scripts/test-diagonal-parity.js (pre-commit).
    def _close_for_pctile(d: str):
        row = by_date_entry_snapshot.get(d) or {}
        c = row.get("vix_close")
        if isinstance(c, (int, float)) and c > 0:
            return float(c)
        o = row.get("vix_open")
        if isinstance(o, (int, float)) and o > 0:
            return float(o)
        return None

    lo, hi = vix_band
    # 2026-06-10 parity fix: the prior-walk must use the FULL entry-snapshot
    # calendar (bs_data by_date — diagonal.html's validDays), NOT the trade
    # universe. When the bs file gained 2022 history, diagonal.html computed
    # percentiles for early-2023 days (>=20 priors available) while this walk,
    # confined to trading_days starting 2023-01-03, still dead-zoned them
    # (2023-01-17 traded only in JS — caught by the parity gate).
    snap_days = sorted(d for d in by_date_entry_snapshot.keys())
    snap_pos = {d: i for i, d in enumerate(snap_days)}
    for today in trading_days:
        row = by_date_entry_snapshot.get(today) or {}
        v_today = row.get("vix_open")
        if not isinstance(v_today, (int, float)):
            v_today = row.get("vix_14")  # legacy fallback
        # Collect up to 20 valid prior closes, walking back over the FULL snapshot calendar
        priors: list[float] = []
        i = snap_pos.get(today, -1)
        j = i - 1
        while j >= 0 and len(priors) < 20:
            c = _close_for_pctile(snap_days[j])
            if c is not None:
                priors.append(c)
            j -= 1

        in_dead_zone = False
        if not isinstance(v_today, (int, float)) or v_today <= 0:
            in_dead_zone = True   # no-vix-today → block (canonical)
        elif len(priors) < 10:
            in_dead_zone = True   # insufficient-prior-data → block (canonical)
        else:
            below = sum(1 for c in priors if c < v_today)
            pct = math.floor(100.0 * below / len(priors) + 0.5)  # half-up = Math.round
            in_dead_zone = lo < pct <= hi

        if in_dead_zone:
            sets["VIX_MID"].add(today)
            # VIX_DEAD: also requires prior-day SPX |change| < 0.5%
            if i >= 2:
                d_prev, d_prev2 = snap_days[i - 1], snap_days[i - 2]
                s_prev = (by_date_entry_snapshot.get(d_prev) or {}).get("spot_14")
                s_prev2 = (by_date_entry_snapshot.get(d_prev2) or {}).get("spot_14")
                if s_prev and s_prev2:
                    prior_chg_pct = 100.0 * (s_prev - s_prev2) / s_prev2
                    if abs(prior_chg_pct) < 0.5:
                        sets["VIX_DEAD"].add(today)

    return sets


def apply_special_filter(dates: list[str], sets: dict[str, set], mode: str | None,
                         active: set[str]) -> list[str]:
    if not mode or not active:
        return list(dates)

    def in_any(d: str) -> bool:
        return any(d in sets.get(cat, set()) for cat in active)

    if mode == "exclude":
        return [d for d in dates if not in_any(d)]
    if mode == "only":
        return [d for d in dates if in_any(d)]
    return list(dates)


# ----------------------------------------------------------------------
# Quote / DTE helpers
# ----------------------------------------------------------------------
def find_ticker(quotes: dict[str, dict], strike: float, exp_date: str,
                snap_tolerance: float = 0.0) -> str | None:
    """Exact strike match on matching expiration, else nearest within tolerance."""
    exact = None
    nearest = None
    nearest_dist = float("inf")
    for t, q in quotes.items():
        if q.get("expiration") != exp_date:
            continue
        if q.get("strike") == strike:
            exact = t
            break
        dist = abs(q["strike"] - strike)
        if dist < nearest_dist:
            nearest_dist = dist
            nearest = t
    if exact:
        return exact
    if snap_tolerance > 0 and nearest_dist <= snap_tolerance:
        return nearest
    return None


def pick_by_policy(candidates: list[dict], target_dte: int, policy: str):
    if not candidates:
        return None
    if policy == "strict":
        for c in candidates:
            if c["dte"] == target_dte:
                return c
        return None
    if policy == "prefer_longer":
        longer = sorted([c for c in candidates if c["dte"] >= target_dte], key=lambda c: c["dte"])
        shorter = sorted([c for c in candidates if c["dte"] < target_dte], key=lambda c: -c["dte"])
        if longer: return longer[0]
        return shorter[0] if shorter else None
    if policy == "prefer_shorter":
        shorter = sorted([c for c in candidates if c["dte"] <= target_dte], key=lambda c: -c["dte"])
        longer = sorted([c for c in candidates if c["dte"] > target_dte], key=lambda c: c["dte"])
        if shorter: return shorter[0]
        return longer[0] if longer else None
    # nearest
    return sorted(candidates, key=lambda c: abs(c["dte"] - target_dte))[0]


def resolve_exp_from_quotes(quotes: dict[str, dict], open_date: str, target_dte: int,
                             tolerance: int, policy: str):
    if not quotes:
        return None
    seen: set[str] = set()
    candidates = []
    for q in quotes.values():
        exp = q.get("expiration")
        if not exp or exp in seen:
            continue
        seen.add(exp)
        dte = days_between(open_date, exp)
        if dte > 0 and abs(dte - target_dte) <= tolerance:
            candidates.append({"date": exp, "dte": dte})
    return pick_by_policy(candidates, target_dte, policy)


def resolve_exp_from_calendar(trading_days: list[str], open_date: str, target_dte: int,
                               tolerance: int, policy: str):
    try:
        open_idx = trading_days.index(open_date)
    except ValueError:
        return None
    candidates = []
    for i in range(open_idx + 1, len(trading_days)):
        dte = days_between(open_date, trading_days[i])
        if dte > target_dte + tolerance:
            break
        if dte >= target_dte - tolerance:
            candidates.append({"date": trading_days[i], "dte": dte})
    return pick_by_policy(candidates, target_dte, policy)


def quote_ok(q) -> bool:
    return bool(q) and q.get("bid", 0) > 0 and q.get("ask", 0) >= q.get("bid", 0) \
           and q.get("ask", 0) / q.get("bid", 1e-9) < 5


# ----------------------------------------------------------------------
# Backtest core
# ----------------------------------------------------------------------
def spx_close_on(date_str: str, data: dict, halfday: dict[str, float]) -> float | None:
    """Half-day close dict first, then the last available intraday bucket."""
    if date_str in halfday:
        return halfday[date_str]
    day = data["by_date"].get(date_str)
    if not day:
        return None
    bt = day.get("by_time", {})
    for t in ("15:45", "15:30", "15:15", "15:00", "14:45", "14:30", "14:15", "14:00"):
        snap = bt.get(t)
        if snap and snap.get("spot") is not None:
            return snap["spot"]
    return None


def run_backtest(data: dict, params: dict, halfday: dict[str, float],
                 bs_data: dict | None = None, verbose: bool = False):
    r = params["risk_free"]
    short_tol = params["short_dte_tol"]
    long_tol = params["long_dte_tol"]
    policy = params["dte_fallback"]
    short_off = params["short_offset"]
    long_off = params["long_offset"]
    entry_time = params["entry_time"]
    exit_time = params["exit_time"]
    snap_tol = params["snap_tolerance"]

    trades: list[dict] = []
    skipped = {"noNextDay": 0, "noExitData": 0, "contracts0": 0, "noDTE": 0, "noEntrySnap": 0}
    source_count = {"REAL": 0, "BS": 0}

    # Build entry snapshot adapter for VIX_MID: {date: {vix_14, spot_14}}.
    # IMPORTANT: JS's buildSpecialDateSets is called with `data.by_date` where
    # `data` is the BS data file (`diagonal_bs_data.json`) — which carries
    # `vix_14`, `spot_14` per date. The bundle's `by_time['14:00'].vix` drifts
    # from `vix_14` by ~0.01–0.25 on many days (different snapshot source), and
    # those small differences flip VIX_MID classifications at the 50–80%
    # percentile boundary. We MUST use the BS file here to match live JS.
    if bs_data is not None:
        entry_snap = {d: {
                          "vix_14":    v.get("vix_14"),
                          "spot_14":   v.get("spot_14"),
                          "vix_open":  v.get("vix_open"),   # first post-9:30 print
                          "vix_close": v.get("vix_close"),  # prior-day close — VIX_MID priors (rule 11)
                      }
                      for d, v in bs_data["by_date"].items()}
    else:
        # Fallback: bundle's 14:00 snapshot (approximate — use bs_data in prod).
        entry_snap = {}
        for d, row in data["by_date"].items():
            et = row.get("by_time", {}).get("14:00")
            if et:
                entry_snap[d] = {"vix_14": et.get("vix"), "spot_14": et.get("spot")}

    # Tunable VIX percentile band — params may set vix_band=(lo, hi); else canonical (50, 90].
    vix_band = tuple(params.get("vix_band", (50.0, 90.0)))

    # 2026-06-09: load COR1M daily values from the Tail Hedge bundle for the
    # COR1M_LOW filter. Diagonal skips days where cor1m < cor1m_min.
    cor1m_by_date: dict[str, float] = {}
    try:
        bundle_path = Path("cor1m_contango_bundle.json")
        if bundle_path.exists():
            bundle = json.loads(bundle_path.read_text())
            for row in bundle.get("daily", []):
                d = row.get("date"); c = row.get("cor1m")
                if d and c is not None:
                    cor1m_by_date[d] = float(c)
            print(f"  COR1M loaded: {len(cor1m_by_date)} days (for COR1M_LOW filter)")
    except Exception as e:
        print(f"  COR1M load failed: {e} (COR1M_LOW filter will be inactive)")

    cor1m_min = float(params.get("cor1m_min", 10.0))
    special = build_special_sets(data["dates"], entry_snap, vix_band=vix_band,
                                 cor1m_by_date=cor1m_by_date, cor1m_min=cor1m_min)

    date_from, date_to = params["date_from"], params["date_to"]
    candidates = [d for d in data["dates"] if date_from <= d <= date_to]
    candidates = apply_special_filter(candidates, special,
                                      params["special_mode"], params["special_active"])

    # Walk
    for open_date in candidates:
        # Next trading day (next entry in sorted data.dates)
        try:
            idx = data["dates"].index(open_date)
        except ValueError:
            continue
        if idx >= len(data["dates"]) - 1:
            skipped["noNextDay"] += 1
            continue
        close_date = data["dates"][idx + 1]

        entry_row = data["by_date"].get(open_date, {}).get("by_time", {}).get(entry_time)
        exit_row  = data["by_date"].get(close_date, {}).get("by_time", {}).get(exit_time)
        if not entry_row:
            skipped["noEntrySnap"] += 1
            continue

        spot_entry = entry_row.get("spot")
        vix_entry = entry_row.get("vix")
        spot_exit = exit_row.get("spot") if exit_row else None
        vix_exit = exit_row.get("vix") if exit_row else None
        if not spot_entry or not vix_entry or not spot_exit or not vix_exit:
            skipped["noExitData"] += 1
            continue

        iv_entry = vix_entry / 100.0
        iv_exit = vix_exit / 100.0

        # Half-up rounding to match JS Math.round (parity class 4, 2026-06-09):
        # Python round() is banker's (half-to-even) — at spot 7162.5 it gave
        # strike 7170 while JS gave 7175, producing different REAL quotes.
        short_strike = math.floor((spot_entry + short_off) / 5 + 0.5) * 5
        long_strike = short_strike - long_off

        entry_quotes = entry_row.get("quotes", {})
        exit_quotes = (exit_row.get("quotes_exit") if exit_row else None) or (exit_row.get("quotes", {}) if exit_row else {})

        short_exp = (resolve_exp_from_quotes(entry_quotes, open_date, params["short_dte"], short_tol, policy)
                     or resolve_exp_from_calendar(data["dates"], open_date, params["short_dte"], short_tol, policy))
        long_exp = (resolve_exp_from_quotes(entry_quotes, open_date, params["long_dte"], long_tol, policy)
                    or resolve_exp_from_calendar(data["dates"], open_date, params["long_dte"], long_tol, policy))
        if not short_exp or not long_exp:
            skipped["noDTE"] += 1
            continue

        short_dte_a = short_exp["dte"]
        long_dte_a = long_exp["dte"]

        elapsed_days = days_between(open_date, close_date)
        T_short_entry = short_dte_a / 365.25
        T_long_entry = long_dte_a / 365.25
        elapsed_yrs = elapsed_days / 365.25
        T_short_exit = max(0.0, T_short_entry - elapsed_yrs)
        T_long_exit = max(0.0, T_long_entry - elapsed_yrs)

        short_ticker = find_ticker(entry_quotes, short_strike, short_exp["date"], snap_tol)
        long_ticker = find_ticker(entry_quotes, long_strike, long_exp["date"], snap_tol)
        s_entry = entry_quotes.get(short_ticker) if short_ticker else None
        l_entry = entry_quotes.get(long_ticker) if long_ticker else None

        s_exit = exit_quotes.get(short_ticker) if short_ticker and exit_quotes else None
        l_exit = exit_quotes.get(long_ticker) if long_ticker and exit_quotes else None

        short_strike_used = s_entry["strike"] if s_entry else short_strike
        long_strike_used = l_entry["strike"] if l_entry else long_strike

        # ENTRY
        entry_source = "BS"
        if quote_ok(s_entry) and quote_ok(l_entry):
            short_mid = (s_entry["bid"] + s_entry["ask"]) / 2.0
            long_mid = (l_entry["bid"] + l_entry["ask"]) / 2.0
            diag_open = long_mid - short_mid
            entry_source = "REAL"
        else:
            short_mid_in = bs_put(spot_entry, short_strike_used, T_short_entry, iv_entry, r)
            long_mid_in = bs_put(spot_entry, long_strike_used, T_long_entry, iv_entry, r)
            diag_open = long_mid_in - short_mid_in

        # EXIT — per-leg REAL -> intrinsic settlement (expired leg) -> BS
        def leg_exit(quote, strike, exp_date, T, bs_spot, bs_iv):
            if quote_ok(quote):
                return ((quote["bid"] + quote["ask"]) / 2.0, True)
            # expired strictly before close_date: exact intrinsic settlement
            if exp_date < close_date:
                spx_at_exp = spx_close_on(exp_date, data, halfday)
                if spx_at_exp is not None:
                    return (max(strike - spx_at_exp, 0.0), True)
            # expires today, quote near-worthless
            if exp_date == close_date and quote is not None \
                    and quote.get("ask") is not None \
                    and quote.get("ask") <= 0.50 \
                    and (quote.get("bid") or 0) == 0:
                return (quote["ask"] / 2.0, True)
            return (bs_put(bs_spot, strike, T, bs_iv, r), False)

        s_mid_out, s_real = leg_exit(s_exit, short_strike_used, short_exp["date"], T_short_exit, spot_exit, iv_exit)
        l_mid_out, l_real = leg_exit(l_exit, long_strike_used, long_exp["date"], T_long_exit, spot_exit, iv_exit)
        diag_close = l_mid_out - s_mid_out
        exit_source = "REAL" if (s_real and l_real) else "BS"

        # Source accounting: count REAL if either side used REAL
        if entry_source == "REAL" or exit_source == "REAL":
            source_count["REAL"] += 1
        else:
            source_count["BS"] += 1

        contracts = params["contracts"] if params["ignore_margin"] else 1
        if contracts < 1:
            skipped["contracts0"] += 1
            continue

        pnl = (diag_close - diag_open) * 100.0 * contracts
        premium = abs(diag_open) * 100.0 * contracts

        price_source = entry_source if entry_source == exit_source else f"{entry_source}/{exit_source}"

        trades.append({
            "openDate": open_date,
            "closeDate": close_date,
            "spotEntry": round(spot_entry, 2),
            "spotExit": round(spot_exit, 2),
            "vixEntry": vix_entry,
            "vixExit": vix_exit,
            "shortStrike": short_strike_used,
            "longStrike": long_strike_used,
            "shortExpDate": short_exp["date"],
            "longExpDate": long_exp["date"],
            "shortDte": short_dte_a,
            "longDte": long_dte_a,
            "diagOpen": diag_open,
            "diagClose": diag_close,
            "contracts": contracts,
            "pnl": pnl,
            "premium": premium,
            "priceSource": price_source,
        })

    return {"trades": trades, "skipped": skipped, "sourceCount": source_count}


# ----------------------------------------------------------------------
# Merge into history_data.json
# ----------------------------------------------------------------------
def merge_diag_pl(history_path: str, pnl_by_date: dict[str, float]) -> dict[str, int]:
    with open(history_path) as f:
        raw = json.load(f)

    if isinstance(raw, dict) and "rows" in raw:
        rows = raw["rows"]
        wrapper = raw
    else:
        rows = raw
        wrapper = None

    changed = 0
    became_null = 0
    updated = 0
    for row in rows:
        d = row.get("date")
        if not d:
            continue
        old = row.get("diagPL", None)
        if d in pnl_by_date:
            new = round(pnl_by_date[d])
            if old != new:
                changed += 1
            row["diagPL"] = new
            updated += 1
        else:
            # Not in computed dict -> filtered out or no data -> null
            if old is not None:
                became_null += 1
            row["diagPL"] = None

    # Atomic write: tmp file, verify, replace
    tmp_path = history_path + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(wrapper if wrapper is not None else rows, f, separators=(",", ":"))
    # Re-parse to verify
    with open(tmp_path) as f:
        json.load(f)
    os.replace(tmp_path, history_path)

    return {"rows": len(rows), "updated": updated, "changed": changed, "became_null": became_null}


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------
def main():
    params = dict(DEFAULT_PARAMS)
    # Keep the set fresh per call
    params["special_active"] = set(DEFAULT_PARAMS["special_active"])

    print("Loading bundles...")
    data = load_bundles()
    halfday = load_halfday_close()
    bs_data = load_bs_data()
    print(f"  Loaded {len(data['dates'])} trading days: {data['dates'][0]} -> {data['dates'][-1]}")
    print(f"  Half-day closes: {len(halfday)}")

    print(f"\nRunning backtest with params:")
    for k in ("entry_time","exit_time","short_dte","short_dte_tol","short_offset",
              "long_dte","long_dte_tol","long_offset","dte_fallback","contracts",
              "special_mode","special_active"):
        print(f"  {k:>16}: {params[k]}")

    result = run_backtest(data, params, halfday, bs_data=bs_data)
    trades = result["trades"]
    print(f"\n--- Backtest result ---")
    print(f"Trades:          {len(trades)}")
    print(f"Skipped:         {result['skipped']}")
    print(f"REAL / BS count: {result['sourceCount']}")

    pnl_by_date: dict[str, float] = {t["openDate"]: t["pnl"] for t in trades}
    total = sum(t["pnl"] for t in trades)
    print(f"Sum P&L (1 ctr): ${round(total):,}")

    # Per-year breakdown
    by_year: dict[str, list[float]] = {}
    for t in trades:
        y = t["openDate"][:4]
        by_year.setdefault(y, []).append(t["pnl"])
    print("\nPer-year:")
    for y in sorted(by_year):
        ps = by_year[y]
        print(f"  {y}: trades={len(ps):4d}  sum=${round(sum(ps)):>10,}")

    # Spot-check sample trades
    print("\nSample trades (first 5):")
    for t in trades[:5]:
        print(f"  {t['openDate']} -> {t['closeDate']}  "
              f"spot={t['spotEntry']:.2f}  K_short={t['shortStrike']}  K_long={t['longStrike']}  "
              f"diagOpen={t['diagOpen']:+7.2f}  diagClose={t['diagClose']:+7.2f}  "
              f"pnl=${round(t['pnl']):+6}  src={t['priceSource']}")

    # Before writing, back up history_data.json
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = f"{HISTORY_PATH}.bak.{stamp}"
    shutil.copyfile(HISTORY_PATH, backup)
    print(f"\nBackup written: {backup}")

    stats = merge_diag_pl(HISTORY_PATH, pnl_by_date)
    print(f"\nHistory merge: {stats}")
    print(f"  Dates in computed dict: {len(pnl_by_date)}")

    # 2026-06-09 audit fix (P1 #14): loud reminder. This script mutates
    # history_data.json locally — the Cloudflare worker mirrors KV→GitHub on
    # every upsert. Leaving the local edit uncommitted means KV (still stale)
    # will overwrite your edit on the next worker tick. ALWAYS use
    # push-history.sh to atomically sync KV+GitHub.
    print()
    print("=" * 72)
    print("⚠  history_data.json was mutated LOCALLY (diagPL fields).")
    print("⚠  KV is now STALE vs your local file.")
    print()
    print("   NEXT STEP — atomically sync KV + GitHub:")
    print("       ./scripts/push-history.sh 'recompute diagPL'")
    print()
    print("   Plain `git push` will be silently reverted on the next worker tick.")
    print("=" * 72)


if __name__ == "__main__":
    main()

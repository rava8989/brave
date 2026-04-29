#!/usr/bin/env python3
"""
Build precomputed SPX+VIX data for the diagonal backtester.

Reads 1-min bars from data/spx/*.csv and data/vix/*.csv, extracts
the values at 12:00 ET and 14:00 ET per trading day, and emits
a single JSON file the browser can load once.

Output: data/diagonal_bs_data.json
Format:
  {
    "dates": ["2024-09-03", "2024-09-04", ...],  # sorted trading days
    "by_date": {
      "2024-09-03": {
        "spot_14": 5580.12,  # SPX at 14:00 ET
        "vix_14":  17.89,    # VIX at 14:00 ET (used as IV proxy at entry)
        "spot_12": 5562.33,  # SPX at 12:00 ET
        "vix_12":  17.55,    # VIX at 12:00 ET (used as IV at exit)
      },
      ...
    }
  }

For any trade opened on date D at 14:00 and closed on D+1 at 12:00:
  entry = by_date[D].spot_14, vix_14
  exit  = by_date[D_plus_one].spot_12, vix_12
"""

import csv, json, os, sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent
SPX_DIR = ROOT / "data" / "spx"
VIX_DIR = ROOT / "data" / "vix"
OUT_PATH = ROOT / "data" / "diagonal_bs_data.json"

# Target times in ET (files are in ET already per existing pipeline)
ENTRY_HHMM = "14:00"
EXIT_HHMM  = "12:00"


def read_bar_at(csv_path, hhmm):
    """Return the OPEN price of the 1-min bar whose timestamp is ' HH:MM:00'.
    The open of the HH:MM bar is the price AT exactly HH:MM:00 (first tick of that minute),
    which is what we want for an "entry at HH:MM" snapshot. Using close would give the
    price at HH:(MM+1), off by one minute.
    Falls back to the open of the first bar AT OR AFTER hhmm if exact minute is missing.
    """
    if not csv_path.exists():
        return None
    target = f" {hhmm}:00"
    with csv_path.open() as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return None
    # Direct hit
    for r in rows:
        if r.get("timestamp", "").endswith(target):
            try:
                return float(r["open"])
            except (KeyError, ValueError, TypeError):
                return None
    # Fallback: first bar AT OR AFTER hhmm
    target_t = datetime.strptime(hhmm, "%H:%M").time()
    for r in rows:
        ts = r.get("timestamp", "")
        try:
            t = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").time()
        except ValueError:
            continue
        if t >= target_t:
            try:
                return float(r["open"])
            except (KeyError, ValueError, TypeError):
                return None
    return None


def date_from_filename(fn, prefix):
    """SPX_20240903.csv -> '2024-09-03'."""
    base = fn.replace(prefix, "").replace(".csv", "")
    if len(base) != 8 or not base.isdigit():
        return None
    return f"{base[:4]}-{base[4:6]}-{base[6:8]}"


def main():
    if not SPX_DIR.exists() or not VIX_DIR.exists():
        print(f"Error: expected {SPX_DIR} and {VIX_DIR}", file=sys.stderr)
        sys.exit(1)

    # Collect dates that have BOTH SPX and VIX files
    spx_files = {date_from_filename(f, "SPX_"): SPX_DIR / f for f in os.listdir(SPX_DIR) if f.startswith("SPX_")}
    vix_files = {date_from_filename(f, "VIX_"): VIX_DIR / f for f in os.listdir(VIX_DIR) if f.startswith("VIX_")}
    common = sorted(d for d in spx_files if d and d in vix_files)
    print(f"Found {len(common)} dates with both SPX and VIX data")

    by_date = {}
    skipped = 0
    for d in common:
        spx_path = spx_files[d]
        vix_path = vix_files[d]
        spot_14 = read_bar_at(spx_path, ENTRY_HHMM)
        spot_12 = read_bar_at(spx_path, EXIT_HHMM)
        vix_14  = read_bar_at(vix_path, ENTRY_HHMM)
        vix_12  = read_bar_at(vix_path, EXIT_HHMM)
        # First VIX print at-or-after 09:30 ET — matches the live signal's
        # "first post-open tick" used by VIX_MID percentile classification.
        # read_bar_at falls back to the first bar AT OR AFTER hhmm.
        vix_open = read_bar_at(vix_path, "09:30")
        # Require at least entry data
        if spot_14 is None or vix_14 is None:
            skipped += 1
            continue
        by_date[d] = {
            "spot_14": round(spot_14, 2),
            "vix_14":  round(vix_14, 2),
            "spot_12": round(spot_12, 2) if spot_12 is not None else None,
            "vix_12":  round(vix_12, 2) if vix_12 is not None else None,
            "vix_open": round(vix_open, 2) if vix_open is not None else None,
        }

    result = {
        "dates": sorted(by_date.keys()),
        "by_date": by_date,
        "generated_at": datetime.now().isoformat(),
        "entry_time_et": ENTRY_HHMM,
        "exit_time_et": EXIT_HHMM,
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w") as f:
        json.dump(result, f, separators=(",", ":"))
    size_kb = OUT_PATH.stat().st_size / 1024
    print(f"Wrote {OUT_PATH} — {len(by_date)} dates, {size_kb:.1f} KB, skipped {skipped}")


if __name__ == "__main__":
    main()

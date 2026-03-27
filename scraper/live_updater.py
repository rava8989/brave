"""
Main orchestrator — runs continuously on the Windows machine.

What it does:
  • Polls SPX price from Schwab every 60s during market hours
  • Writes spx_live.json → pushed to GitHub → live.html reads it
  • Monitors Discord via discord_scraper.py for M8BF signals
  • When signal hits window → writes today_trade.json → pushed to GitHub
  • At market close → computes final P&L, updates today_trade.json
  • At start of each day → resets today_trade.json to no-trade state

Usage:
    python live_updater.py
"""

import os
import json
import asyncio
import time
from datetime import datetime, date, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

import requests
import schwab_client
from github_push import push_json
from discord_scraper import run_scraper, make_trade_json, window_name, fetch_signal_for_date

load_dotenv()

ET = ZoneInfo("America/New_York")

# Local cache paths (not committed)
DATA_DIR    = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
TRADE_CACHE    = DATA_DIR / "today_trade.json"
SPX_CACHE      = DATA_DIR / "spx_live.json"
SPX_HIST_CACHE = DATA_DIR / "spx_history.json"
SIG_CACHE      = DATA_DIR / "signals_today.json"

# ── Helpers ───────────────────────────────────────────────────────────────────

def now_et() -> datetime:
    return datetime.now(tz=ET)

def is_market_open(dt: datetime = None) -> bool:
    dt = dt or now_et()
    if dt.weekday() >= 5:
        return False
    mins = dt.hour * 60 + dt.minute
    return 9*60+30 <= mins <= 16*60

def is_new_day(trade: dict) -> bool:
    return trade.get("date") != date.today().strftime("%Y-%m-%d")

def compute_pl(trade: dict, spx: float) -> int:
    lo, hi, prem = trade["bf_lower"], trade["bf_upper"], trade["premium"]
    wing      = (hi - lo) / 2
    intrinsic = max(0, min(spx - lo, hi - spx))
    clipped   = min(intrinsic, wing)
    return round((clipped - prem) * 100)

def load_cache(path: Path, default: dict) -> dict:
    try:
        return json.loads(path.read_text()) if path.exists() else default
    except Exception:
        return default

def save_cache(path: Path, data: dict):
    """Atomic write — write to .tmp then rename to avoid race condition corruption."""
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(path)


# ── Daily reset ───────────────────────────────────────────────────────────────

def reset_trade():
    empty = {
        "date":        date.today().strftime("%Y-%m-%d"),
        "triggered":   False,
        "signal_time": None,
        "window":      None,
        "center":      None,
        "t1":          None,
        "bf_lower":    None,
        "bf_upper":    None,
        "premium":     None,
        "status":      "waiting",
        "spx_close":   None,
        "final_pl":    None,
    }
    save_cache(TRADE_CACHE, empty)
    push_json("today_trade.json", empty, f"reset: new trading day {empty['date']}")
    print(f"[Updater] New day reset for {empty['date']}")
    return empty


# ── EOD close ────────────────────────────────────────────────────────────────

def close_trade(trade: dict, spx_close: float):
    final_pl = compute_pl(trade, spx_close) if trade.get("triggered") else None
    trade.update({
        "status":    "closed",
        "spx_close": spx_close,
        "final_pl":  final_pl,
    })
    save_cache(TRADE_CACHE, trade)
    push_json("today_trade.json", trade, f"eod: close trade {trade['date']} pl={final_pl}")
    print(f"[Updater] Trade closed. SPX={spx_close} Final P&L=${final_pl}")
    return trade


# ── SPX live update ───────────────────────────────────────────────────────────

def update_spx_live(price: float, prev_close: float):
    dt = now_et()
    data = {
        "timestamp":     dt.isoformat(),
        "price":         price,
        "prev_close":    prev_close,
        "market_status": "open" if is_market_open(dt) else "closed",
    }
    save_cache(SPX_CACHE, data)
    push_json("spx_live.json", data, f"live: SPX {price:.2f}")
    return data


# ── SPX history accumulation ──────────────────────────────────────────────────

def reset_spx_history():
    data = {"date": date.today().strftime("%Y-%m-%d"), "data": []}
    save_cache(SPX_HIST_CACHE, data)
    push_json("spx_history.json", data, f"reset: spx_history {data['date']}")

def append_spx_history(price: float, dt: datetime):
    hist = load_cache(SPX_HIST_CACHE, {"date": date.today().strftime("%Y-%m-%d"), "data": []})
    if hist.get("date") != date.today().strftime("%Y-%m-%d"):
        hist = {"date": date.today().strftime("%Y-%m-%d"), "data": []}
    hist["data"].append({"time": dt.strftime("%H:%M"), "price": round(price, 2)})
    save_cache(SPX_HIST_CACHE, hist)
    push_json("spx_history.json", hist, f"spx_hist: {dt.strftime('%H:%M')} {price:.2f}")


# ── Signals today accumulation ───────────────────────────────────────────────

def reset_signals_today():
    data = {"date": date.today().strftime("%Y-%m-%d"), "signals": []}
    save_cache(SIG_CACHE, data)
    push_json("signals_today.json", data, f"reset: signals_today {data['date']}")

def append_signal_today(center: int, t1: int, premium: float, dt: datetime, banned: bool):
    sigs = load_cache(SIG_CACHE, {"date": date.today().strftime("%Y-%m-%d"), "signals": []})
    if sigs.get("date") != date.today().strftime("%Y-%m-%d"):
        sigs = {"date": date.today().strftime("%Y-%m-%d"), "signals": []}
    # Deduplicate by center (same signal seen multiple polls)
    existing = {s["center"] for s in sigs["signals"]}
    if center in existing:
        return
    sigs["signals"].append({
        "time": dt.strftime("%H:%M"),
        "center": center,
        "t1": t1,
        "premium": premium,
        "banned": banned,
    })
    save_cache(SIG_CACHE, sigs)
    push_json("signals_today.json", sigs, f"signal_log: center={center} banned={banned}")
    print(f"[Signals] Logged: center={center} t1={t1} premium={premium} banned={banned}")


# ── Signal callback (from discord scraper) ───────────────────────────────────

async def on_signal(center: int, t1: int, premium: float, dt: datetime):
    trade = load_cache(TRADE_CACHE, {})
    if trade.get("triggered"):
        print(f"[Updater] Already have a trade today, ignoring new signal.")
        return

    win_name = window_name(dt)
    trade    = make_trade_json(center, t1, premium, dt.strftime("%H:%M"), win_name)
    save_cache(TRADE_CACHE, trade)
    push_json("today_trade.json", trade, f"signal: M8BF center={center} t1={t1}")
    print(f"[Updater] Trade written to GitHub: {trade}")


async def on_any_signal(center: int, t1: int, premium: float, dt: datetime, banned: bool):
    """Log ALL signals to signals_today.json (including banned/outside window)."""
    append_signal_today(center, t1, premium, dt, banned)


# ── Backfill missing dates on startup ────────────────────────────────────────

async def backfill_missing_dates(page):
    """
    Runs once on startup. Finds dates in history_data.json that have spxClose
    but no m8bfPL (machine was down), searches Discord history for the signal,
    computes P&L, and pushes updated history_data.json to GitHub.
    """
    repo  = os.getenv("GITHUB_REPO", "rava8989/brave")
    url   = f"https://raw.githubusercontent.com/{repo}/main/history_data.json"
    ch_id = os.getenv("DISCORD_CHANNEL_ID", "")

    try:
        resp    = requests.get(url, timeout=10)
        history = resp.json()
    except Exception as e:
        print(f"[Backfill] Could not fetch history_data.json: {e}")
        return

    today   = date.today()
    cutoff  = today - timedelta(days=14)

    missing = [
        row for row in history
        if row.get("spxClose")
        and row.get("m8bfPL") is None
        and datetime.strptime(row["date"], "%Y-%m-%d").date() >= cutoff
        and datetime.strptime(row["date"], "%Y-%m-%d").date() < today
    ]

    if not missing:
        print("[Backfill] No gaps found — history up to date.")
        return

    print(f"[Backfill] {len(missing)} date(s) to fill: {[r['date'] for r in missing]}")

    updated = False
    for row in missing:
        target = datetime.strptime(row["date"], "%Y-%m-%d").date()
        signal = await fetch_signal_for_date(page, ch_id, target)
        if signal:
            center, t1, premium = signal
            fake_trade = {"bf_lower": center - 50, "bf_upper": center + 50, "premium": premium}
            pl = compute_pl(fake_trade, row["spxClose"])
            row["m8bfPL"] = pl
            print(f"[Backfill] ✅ {row['date']}: pl=${pl}")
            updated = True
        else:
            print(f"[Backfill] ⏭  {row['date']}: no valid signal found (no-trade day)")

    if updated:
        push_json("history_data.json", history, "backfill: fill missing m8bfPL dates")
        print("[Backfill] history_data.json pushed to GitHub.")


# ── Push schedule: xx:02, xx:07, xx:12, xx:17, xx:22... (every 5 min, offset +2)
def should_push(dt: datetime) -> bool:
    return dt.minute % 5 == 2


# ── SPX polling loop (runs in background thread-like task) ───────────────────

async def spx_poll_loop():
    last_price    = None
    prev_close    = None
    eod_triggered = False
    last_day      = None
    last_pushed   = None   # track last push minute to avoid double-push
    fail_count    = 0      # consecutive Schwab failures for backoff

    while True:
        dt = now_et()

        # Daily reset at 9:00 ET
        if last_day != dt.date() and dt.hour >= 9:
            trade = load_cache(TRADE_CACHE, {})
            if is_new_day(trade):
                reset_trade()
                reset_spx_history()
                reset_signals_today()
                eod_triggered = False
            last_day = dt.date()

        if is_market_open(dt):
            quote = schwab_client.get_spx_price()
            if quote and quote.get("price"):
                raw = quote["price"]
                if not (1000 < raw < 15000):
                    print(f"[SPX] ⚠️  Sanity check failed: price={raw} — ignoring")
                    await asyncio.sleep(30)
                    continue
                last_price = raw
                fail_count = 0
                prev_close = quote.get("prev_close") or prev_close
                # Always save locally every loop
                save_cache(SPX_CACHE, {
                    "timestamp":     dt.isoformat(),
                    "price":         last_price,
                    "prev_close":    prev_close,
                    "market_status": "open",
                })
                # Push to GitHub only on xx:02, xx:07, xx:12... schedule
                push_key = (dt.date(), dt.hour, dt.minute)
                if should_push(dt) and last_pushed != push_key:
                    update_spx_live(last_price, prev_close)
                    append_spx_history(last_price, dt)
                    last_pushed = push_key
                    print(f"[SPX] {dt.strftime('%H:%M ET')} → {last_price:.2f} (pushed)")
                else:
                    print(f"[SPX] {dt.strftime('%H:%M ET')} → {last_price:.2f}")
            else:
                fail_count += 1
                backoff = min(30 * (2 ** (fail_count - 1)), 300)  # 30s, 60s, 120s … max 5 min
                print(f"[SPX] {dt.strftime('%H:%M:%S ET')} → fetch failed (attempt {fail_count}, backoff {backoff}s)")
                await asyncio.sleep(backoff)
                continue

        # EOD close at 16:01 ET
        elif not eod_triggered and dt.hour == 16 and dt.minute >= 1:
            trade = load_cache(TRADE_CACHE, {})
            if trade.get("triggered") and trade.get("status") == "open" and last_price:
                close_trade(trade, last_price)
                eod_triggered = True
            elif last_price:
                # Still push final spx_live even if no trade
                update_spx_live(last_price, prev_close)
                eod_triggered = True

        await asyncio.sleep(30)  # check every 30s, push every 5 min on xx:02/07/12...


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    print("[Updater] Starting S3 Live Updater")
    print(f"[Updater] Time: {now_et().strftime('%Y-%m-%d %H:%M ET')}")

    # Verify Schwab tokens exist
    try:
        tok = schwab_client.get_access_token()
        print(f"[Schwab] Token OK: {tok[:20]}...")
    except RuntimeError as e:
        print(f"[Schwab] ❌ {e}")
        print("[Schwab] Run: python schwab_auth.py")
        return

    # Run SPX polling + Discord scraper concurrently
    # backfill_missing_dates runs once after Discord login before the main loop
    await asyncio.gather(
        spx_poll_loop(),
        run_scraper(on_signal, poll_interval=30, on_startup=backfill_missing_dates,
                    on_any_signal_callback=on_any_signal),
    )

if __name__ == "__main__":
    asyncio.run(main())

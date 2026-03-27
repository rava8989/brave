"""
Discord M8BF signal scraper using Playwright (Chrome automation).
Monitors a Discord channel and detects valid M8BF butterfly signals.

Signal format expected in Discord (adjust SIGNAL_PATTERN if different):
  Example:  "M8BF signal: center=5625 T1=5630 premium=8.50"
  Or a table/embed with center/strike info — edit parse_signal() below.
"""

import re
import os
import json
import asyncio
from datetime import datetime, date
from pathlib import Path
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()

DISCORD_CHANNEL_ID = os.getenv("DISCORD_CHANNEL_ID", "")
DISCORD_SERVER_ID  = os.getenv("DISCORD_SERVER_ID", "")
CHANNEL_URL = f"https://discord.com/channels/{DISCORD_SERVER_ID}/{DISCORD_CHANNEL_ID}" if DISCORD_CHANNEL_ID and DISCORD_SERVER_ID else ""

ET = ZoneInfo("America/New_York")

# ── Windows (ET) ──────────────────────────────────────────────────────────────
WINDOWS = {
    0: (11*60,     11*60+30),   # Mon 11:00–11:30
    1: (13*60+30,  14*60),      # Tue 13:30–14:00
    2: (12*60,     12*60+30),   # Wed 12:00–12:30
    3: (11*60,     11*60+30),   # Thu 11:00–11:30
    4: (13*60,     13*60+30),   # Fri 13:00–13:30
}

# ── M8BF Rules ────────────────────────────────────────────────────────────────
FULL_BANS  = {10, 25, 35, 40, 65, 80}
COMBO_BANS = {0: 95, 20: 15, 55: 50, 65: 60, 85: 90}

def in_window(dt: datetime) -> bool:
    dow  = dt.weekday()
    mins = dt.hour * 60 + dt.minute
    if dow not in WINDOWS:
        return False
    lo, hi = WINDOWS[dow]
    return lo <= mins < hi

def is_banned(center: int, t1: int) -> bool:
    ce  = center % 100
    t1e = t1 % 100
    if ce in FULL_BANS:
        return True
    return t1e in COMBO_BANS and COMBO_BANS[t1e] == ce

def make_trade_json(center: int, t1: int, premium: float, signal_time: str, window_name: str) -> dict:
    bf_lower = center - 50
    bf_upper = center + 50
    return {
        "date":        date.today().strftime("%Y-%m-%d"),
        "triggered":   True,
        "signal_time": signal_time,
        "window":      window_name,
        "center":      center,
        "t1":          t1,
        "bf_lower":    bf_lower,
        "bf_upper":    bf_upper,
        "premium":     premium,
        "status":      "open",
        "spx_close":   None,
        "final_pl":    None,
    }

def window_name(dt: datetime) -> str:
    days = ["Mon", "Tue", "Wed", "Thu", "Fri"]
    return f"{days[dt.weekday()]} {dt.strftime('%H:%M')} ET"


# ── Signal parser — edit this to match your Discord format ───────────────────
# Returns (center, t1, premium) or None

def parse_signal(text: str):
    """
    Parse M8BF butterfly signal from Discord message text.

    Looks for the butterfly trade line, e.g.:
      BUY +1 Butterfly SPX 100 27 Mar 26 6455/6405/6355 PUT @14.25 LMT

    Extracts: center (middle strike), T1 (from "Target 1:" field), premium (from @price).
    """
    text_lower = text.lower()

    # Look for butterfly trade line with three strikes and premium
    # Format: Butterfly SPX ... UPPER/CENTER/LOWER PUT|CALL @PREMIUM LMT
    m = re.search(
        r'butterfly\s+spx\s+.+?(\d{4,5})/(\d{4,5})/(\d{4,5})\s+(?:put|call)\s+@(\d+\.?\d*)',
        text_lower
    )
    if not m:
        return None

    upper   = int(m.group(1))
    center  = int(m.group(2))
    lower   = int(m.group(3))
    premium = float(m.group(4))

    # Extract T1 from "Target 1: 6400.0"
    t1_match = re.search(r'target\s*1[:\s]+(\d+\.?\d*)', text_lower)
    t1 = int(float(t1_match.group(1))) if t1_match else center + 5

    return center, t1, premium


# ── Playwright scraper ────────────────────────────────────────────────────────

async def scrape_once(page, on_signal):
    """Check the most recent messages in the Discord channel."""
    try:
        # Get visible messages
        messages = await page.query_selector_all('[id^="message-content-"]')
        for msg in messages[-20:]:   # check last 20 messages
            text = await msg.inner_text()
            result = parse_signal(text)
            if result:
                center, t1, premium = result
                now_et = datetime.now(tz=ET)
                if not in_window(now_et):
                    print(f"[Scraper] Signal outside window: center={center} at {now_et.strftime('%H:%M ET')}")
                elif is_banned(center, t1):
                    print(f"[Scraper] Signal BANNED: center={center} t1={t1}")
                else:
                    await on_signal(center, t1, premium, now_et)
    except Exception as e:
        print(f"[Scraper] scrape_once error: {e}")


async def fetch_signal_for_date(page, channel_id: str, target_date) -> tuple | None:
    """
    Search Discord message history for a valid M8BF signal on target_date.
    Uses Discord's REST API via the authenticated browser session — no stored token needed.
    Returns (center, t1, premium) or None if no valid signal found.
    """
    dow = target_date.weekday()
    if dow not in WINDOWS:
        return None

    win_lo, win_hi = WINDOWS[dow]

    # Build snowflakes bracketing 9:00–17:00 ET on target_date
    discord_epoch = 1420070400000
    start_et = datetime(target_date.year, target_date.month, target_date.day, 9, 0, tzinfo=ET)
    end_et   = datetime(target_date.year, target_date.month, target_date.day, 17, 0, tzinfo=ET)
    after_snow  = str(int(start_et.timestamp() * 1000 - discord_epoch) << 22)
    before_snow = str(int(end_et.timestamp()   * 1000 - discord_epoch) << 22)

    # Validate IDs are numeric-only (Discord snowflakes) before injecting into JS
    if not str(channel_id).isdigit() or not after_snow.isdigit() or not before_snow.isdigit():
        print(f"[Backfill] Invalid channel_id or snowflake — aborting")
        return None

    try:
        messages = await page.evaluate("""
            async ([ch, after, before]) => {
                const r = await fetch(`/api/v9/channels/${ch}/messages?limit=100&after=${after}&before=${before}`);
                if (!r.ok) return [];
                return await r.json();
            }
        """, [str(channel_id), after_snow, before_snow])
    except Exception as e:
        print(f"[Backfill] Discord API error for {target_date}: {e}")
        return None

    if not isinstance(messages, list):
        return None

    # messages come newest-first from Discord, reverse to chronological
    for msg in reversed(messages):
        text = msg.get("content", "")
        result = parse_signal(text)
        if not result:
            continue

        center, t1, premium = result

        ts_str = msg.get("timestamp", "")
        if not ts_str:
            continue
        msg_dt   = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).astimezone(ET)
        msg_mins = msg_dt.hour * 60 + msg_dt.minute

        if win_lo <= msg_mins < win_hi:
            if not is_banned(center, t1):
                print(f"[Backfill] {target_date}: center={center} t1={t1} premium={premium} at {msg_dt.strftime('%H:%M ET')}")
                return center, t1, premium
            else:
                print(f"[Backfill] {target_date}: signal BANNED center={center} t1={t1}")

    return None


async def run_scraper(on_signal_callback, poll_interval: int = 30,
                      on_startup=None):
    """
    Main scraper loop. Opens Chrome, navigates to Discord, polls every N seconds.

    on_signal_callback(center, t1, premium, dt) — called when a valid live signal is found.
    on_startup(page)                             — optional async coroutine called once after login.
    """
    from playwright.async_api import async_playwright

    print(f"[Scraper] Starting. Channel: {CHANNEL_URL or 'NOT SET'}")
    if not CHANNEL_URL:
        print("[Scraper] ERROR: DISCORD_CHANNEL_ID not set in .env")
        return

    async with async_playwright() as pw:
        # Launch Chrome (use channel="chrome" to use installed Chrome)
        browser = await pw.chromium.launch(
            channel="chrome",
            headless=False,   # keep visible so you can see Discord + solve any captchas
        )
        ctx  = await browser.new_context()
        page = await ctx.new_page()

        print("[Scraper] Opening Discord...")
        await page.goto("https://discord.com/login")
        print("[Scraper] Waiting for Discord login (log in in the browser window)...")
        # Wait up to 5 minutes for user to log in — detect by URL change away from /login
        for _ in range(300):
            if "/login" not in page.url:
                break
            await asyncio.sleep(1)
        print("[Scraper] Discord login detected, continuing...")

        await page.goto(CHANNEL_URL)
        await page.wait_for_load_state("networkidle")
        print(f"[Scraper] Monitoring channel. Polling every {poll_interval}s.")

        # Run startup hook (backfill etc.) before the main loop
        if on_startup:
            await on_startup(page)

        triggered_today = set()   # avoid double-triggering same center
        scrape_fail_count = 0

        while True:
            async def on_signal(center, t1, premium, dt):
                key = f"{date.today()}_{center}"
                if key in triggered_today:
                    return
                triggered_today.add(key)
                print(f"[Scraper] ✅ SIGNAL: center={center} t1={t1} premium={premium} at {dt.strftime('%H:%M ET')}")
                await on_signal_callback(center, t1, premium, dt)

            try:
                await scrape_once(page, on_signal)
                scrape_fail_count = 0
            except Exception as e:
                scrape_fail_count += 1
                backoff = min(30 * (2 ** (scrape_fail_count - 1)), 300)
                print(f"[Scraper] Error (attempt {scrape_fail_count}, backoff {backoff}s): {e}")
                await asyncio.sleep(backoff)
                continue

            await asyncio.sleep(poll_interval)

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
CHANNEL_URL = f"https://discord.com/channels/@me/{DISCORD_CHANNEL_ID}" if DISCORD_CHANNEL_ID else ""

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
    Parse M8BF signal from Discord message text.
    Adjust the patterns below to match your actual Discord message format.

    Expected formats (any of):
      center=5625 t1=5630 premium=8.50
      Center: 5625 | T1: 5630 | Premium: $8.50
      5625C / 5630T1 / 8.50p
    """
    text_lower = text.lower()

    # Must mention m8bf or butterfly
    if "m8bf" not in text_lower and "butterfly" not in text_lower:
        return None

    # Extract center
    m = re.search(r"center[=:\s]+(\d{4,5})", text_lower)
    if not m:
        return None
    center = int(m.group(1))

    # Extract T1
    m = re.search(r"t1[=:\s]+(\d{4,5})", text_lower)
    t1 = int(m.group(1)) if m else center + 5   # default: center + 5

    # Extract premium
    m = re.search(r"premium[=:\s\$]+(\d+\.?\d*)", text_lower)
    if not m:
        m = re.search(r"\$(\d+\.?\d+)", text)
    premium = float(m.group(1)) if m else None

    if premium is None:
        return None

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
                if in_window(now_et):
                    if not is_banned(center, t1):
                        await on_signal(center, t1, premium, now_et)
                    else:
                        print(f"[Scraper] Signal BANNED: center={center} t1={t1}")
                else:
                    print(f"[Scraper] Signal outside window: center={center} at {now_et.strftime('%H:%M ET')}")
    except Exception as e:
        print(f"[Scraper] scrape_once error: {e}")


async def run_scraper(on_signal_callback, poll_interval: int = 30):
    """
    Main scraper loop. Opens Chrome, navigates to Discord, polls every N seconds.

    on_signal_callback(center, t1, premium, dt) is called when a valid signal is found.
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
        print("[Scraper] Log in to Discord in the browser window, then press Enter here.")
        input()

        await page.goto(CHANNEL_URL)
        await page.wait_for_load_state("networkidle")
        print(f"[Scraper] Monitoring channel. Polling every {poll_interval}s.")

        triggered_today = set()   # avoid double-triggering same center

        while True:
            async def on_signal(center, t1, premium, dt):
                key = f"{date.today()}_{center}"
                if key in triggered_today:
                    return
                triggered_today.add(key)
                print(f"[Scraper] ✅ SIGNAL: center={center} t1={t1} premium={premium} at {dt.strftime('%H:%M ET')}")
                await on_signal_callback(center, t1, premium, dt)

            await scrape_once(page, on_signal)
            await asyncio.sleep(poll_interval)

"""
Schwab OAuth client — handles token lifecycle + market data queries.
Stores tokens in tokens.json (never committed to git).
"""

import os
import json
import time
import base64
import webbrowser
import requests
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

APP_KEY      = os.getenv("SCHWAB_APP_KEY")
APP_SECRET   = os.getenv("SCHWAB_APP_SECRET")
REDIRECT_URI = os.getenv("SCHWAB_REDIRECT_URI", "https://rava8989.github.io/brave/live.html")

# Proxy settings — when set, ALL token refreshes go through the Worker instead
# of talking to Schwab's /v1/oauth/token directly. This is the single-source-of-
# truth fix for the 3-way refresh_token rotation race (worker cron + browser +
# this scraper all used to rotate independently, invalidating each other).
PROXY_URL   = (os.getenv("SCHWAB_PROXY_URL") or "").rstrip("/")
SYNC_SECRET = os.getenv("SCHWAB_SYNC_SECRET") or ""

AUTH_URL   = "https://api.schwabapi.com/v1/oauth/authorize"
TOKEN_URL  = "https://api.schwabapi.com/v1/oauth/token"
QUOTES_URL = "https://api.schwabapi.com/marketdata/v1/quotes"

TOKENS_FILE = Path(__file__).parent / "tokens.json"


# ── Token storage ──────────────────────────────────────────────────────────────

def _save_tokens(data: dict):
    data["saved_at"] = time.time()
    TOKENS_FILE.write_text(json.dumps(data, indent=2))

def _load_tokens() -> dict | None:
    if not TOKENS_FILE.exists():
        return None
    try:
        return json.loads(TOKENS_FILE.read_text())
    except Exception:
        return None


# ── OAuth helpers ──────────────────────────────────────────────────────────────

def _b64_credentials() -> str:
    creds = f"{APP_KEY}:{APP_SECRET}"
    return base64.b64encode(creds.encode()).decode()

def _token_headers() -> dict:
    return {
        "Authorization": f"Basic {_b64_credentials()}",
        "Content-Type": "application/x-www-form-urlencoded",
    }

def exchange_code(auth_code: str) -> dict:
    """Exchange authorization code for access + refresh tokens."""
    resp = requests.post(TOKEN_URL, headers=_token_headers(), data={
        "grant_type":   "authorization_code",
        "code":         auth_code,
        "redirect_uri": REDIRECT_URI,
    })
    resp.raise_for_status()
    tokens = resp.json()
    _save_tokens(tokens)
    print(f"[Schwab] Tokens saved. Access expires in {tokens.get('expires_in', '?')}s.")
    return tokens

def refresh_tokens(refresh_token: str) -> dict:
    """Use refresh token to get a new access token."""
    resp = requests.post(TOKEN_URL, headers=_token_headers(), data={
        "grant_type":    "refresh_token",
        "refresh_token": refresh_token,
    })
    resp.raise_for_status()
    tokens = resp.json()
    _save_tokens(tokens)
    print(f"[Schwab] Tokens refreshed at {datetime.now().strftime('%H:%M:%S')}.")
    return tokens


# ── Public interface ───────────────────────────────────────────────────────────

# Local cache for tokens fetched via the Worker proxy. Avoids hammering the
# Worker on every quote — we know how long Schwab access tokens live.
_proxy_cache: dict = {"access_token": None, "expires_at": 0.0}


def _fetch_from_proxy() -> str:
    """Ask the Worker for a current access token (it owns refresh in KV)."""
    resp = requests.get(
        f"{PROXY_URL}/access-token",
        headers={"X-Sync-Secret": SYNC_SECRET},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    access = data.get("access_token")
    if not access:
        raise RuntimeError(f"Proxy returned no access_token: {data}")
    # `expiry` is epoch-ms from the Worker; fall back to 29 min if missing.
    expiry_ms = data.get("expiry")
    expires_at = (expiry_ms / 1000.0) if expiry_ms else (time.time() + 29 * 60)
    _proxy_cache["access_token"] = access
    _proxy_cache["expires_at"]   = expires_at
    return access


def get_access_token() -> str:
    """Return a valid access token, refreshing if needed.

    Preferred path: fetch from Worker (`SCHWAB_PROXY_URL` + `SCHWAB_SYNC_SECRET`
    set). The Worker is the single source of truth for Schwab tokens — it owns
    the refresh_token in KV and rotates it atomically. This eliminates the
    3-way race between this scraper, the Worker cron, and the browser.

    Fallback path: if proxy isn't configured, refresh directly against Schwab
    using `tokens.json`. This path is only used for one-off local dev or if the
    Worker is unreachable.
    """
    if PROXY_URL and SYNC_SECRET:
        # Reuse cached token until 60s before expiry
        if _proxy_cache["access_token"] and time.time() < _proxy_cache["expires_at"] - 60:
            return _proxy_cache["access_token"]
        return _fetch_from_proxy()

    # ── Fallback: direct Schwab refresh (legacy path) ──
    tokens = _load_tokens()
    if not tokens:
        raise RuntimeError(
            "No tokens found and SCHWAB_PROXY_URL not set. "
            "Either set SCHWAB_PROXY_URL + SCHWAB_SYNC_SECRET in .env "
            "(preferred) or run schwab_auth.py for local-only mode."
        )

    saved_at   = tokens.get("saved_at", 0)
    expires_in = tokens.get("expires_in", 1800)   # default 30 min
    age        = time.time() - saved_at

    # Refresh if within 5 minutes of expiry
    if age >= expires_in - 300:
        tokens = refresh_tokens(tokens["refresh_token"])

    return tokens["access_token"]

def get_spx_price() -> dict | None:
    """
    Fetch current SPX quote from Schwab Market Data API.
    Returns: {"price": float, "prev_close": float} or None on error.
    """
    try:
        token = get_access_token()
        resp  = requests.get(QUOTES_URL, headers={
            "Authorization": f"Bearer {token}",
            "Accept":        "application/json",
        }, params={"symbols": "$SPX", "fields": "quote,reference"}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        q = data.get("$SPX", {}).get("quote", {})
        return {
            "price":      q.get("lastPrice") or q.get("mark"),
            "prev_close": q.get("closePrice"),
        }
    except Exception as e:
        print(f"[Schwab] Quote error: {e}")
        return None


PRICE_HISTORY_URL = "https://api.schwabapi.com/marketdata/v1/pricehistory"

def get_spx_history_today() -> list:
    """
    Fetch today's intraday SPX prices (5-min candles) from Schwab.
    Returns list of {"time": "HH:MM", "price": float} or empty list on error.
    """
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
    today = datetime.now(tz=ET)
    market_open = today.replace(hour=9, minute=30, second=0, microsecond=0)
    start_ms = int(market_open.timestamp() * 1000)
    end_ms = int(today.timestamp() * 1000)

    try:
        token = get_access_token()
        resp = requests.get(PRICE_HISTORY_URL, headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }, params={
            "symbol": "$SPX",
            "periodType": "day",
            "period": 1,
            "frequencyType": "minute",
            "frequency": 5,
            "startDate": start_ms,
            "endDate": end_ms,
        }, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        candles = data.get("candles", [])
        result = []
        for c in candles:
            ts = datetime.fromtimestamp(c["datetime"] / 1000, tz=ET)
            result.append({"time": ts.strftime("%H:%M"), "price": round(c["close"], 2)})
        return result
    except Exception as e:
        print(f"[Schwab] Price history error: {e}")
        return []


# ── Auth URL builder ───────────────────────────────────────────────────────────

def get_auth_url() -> str:
    return (
        f"{AUTH_URL}"
        f"?client_id={APP_KEY}"
        f"&redirect_uri={REDIRECT_URI}"
        f"&response_type=code"
        f"&scope=readonly"
    )

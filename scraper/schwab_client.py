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

def get_access_token() -> str:
    """Return a valid access token, refreshing if needed."""
    tokens = _load_tokens()
    if not tokens:
        raise RuntimeError("No tokens found. Run schwab_auth.py first.")

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
        }, params={"symbols": "$SPX.X", "fields": "quote,reference"}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        q = data.get("$SPX.X", {}).get("quote", {})
        return {
            "price":      q.get("lastPrice") or q.get("mark"),
            "prev_close": q.get("closePrice"),
        }
    except Exception as e:
        print(f"[Schwab] Quote error: {e}")
        return None


# ── Auth URL builder ───────────────────────────────────────────────────────────

def get_auth_url() -> str:
    return (
        f"{AUTH_URL}"
        f"?client_id={APP_KEY}"
        f"&redirect_uri={REDIRECT_URI}"
        f"&response_type=code"
        f"&scope=readonly"
    )

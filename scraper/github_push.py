"""
Pushes JSON files to the GitHub repo via the GitHub REST API.
No local git required — uses Personal Access Token.

Required .env vars:
    GITHUB_TOKEN=ghp_xxxx
    GITHUB_REPO=rava8989/brave      (owner/repo)
"""

import os
import json
import base64
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_REPO  = os.getenv("GITHUB_REPO", "rava8989/brave")
API_BASE     = f"https://api.github.com/repos/{GITHUB_REPO}/contents"

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept":        "application/vnd.github+json",
    "X-GitHub-Api-Version": "2026-11-28",
}


def _get_sha(filepath: str) -> str | None:
    """Get current SHA of a file (needed to update it via API)."""
    resp = requests.get(f"{API_BASE}/{filepath}", headers=HEADERS)
    if resp.status_code == 200:
        return resp.json().get("sha")
    return None


def push_json(filepath: str, data: dict, message: str = "auto: update live data"):
    """
    Create or update a file in the GitHub repo.
    filepath: path relative to repo root, e.g. 'today_trade.json'
    """
    if not GITHUB_TOKEN:
        print("[GitHub] ERROR: GITHUB_TOKEN not set in .env")
        return False

    content = base64.b64encode(
        json.dumps(data, indent=2).encode()
    ).decode()

    sha  = _get_sha(filepath)
    body = {"message": message, "content": content}
    if sha:
        body["sha"] = sha

    resp = requests.put(f"{API_BASE}/{filepath}", headers=HEADERS, json=body)
    if resp.status_code in (200, 201):
        print(f"[GitHub] Pushed {filepath}")
        return True
    else:
        print(f"[GitHub] Push failed: {resp.status_code} {resp.text[:200]}")
        return False

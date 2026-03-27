"""
One-time Schwab OAuth setup.
Run this ONCE on the Windows machine to authorize and save tokens.

Usage:
    python schwab_auth.py

Steps:
  1. Browser opens to Schwab login
  2. Log in with your Schwab brokerage account
  3. You'll be redirected to live.html?code=XXXX&session=YYYY
  4. Copy the FULL redirect URL from the browser address bar
  5. Paste it here → tokens saved to tokens.json
"""

import webbrowser
from urllib.parse import urlparse, parse_qs
from schwab_client import get_auth_url, exchange_code

def main():
    auth_url = get_auth_url()
    print("\n" + "="*60)
    print("SCHWAB OAUTH SETUP")
    print("="*60)
    print("\nStep 1: Opening browser to Schwab login...")
    print(f"\nAuth URL:\n{auth_url}\n")
    webbrowser.open(auth_url)

    print("Step 2: Log in with your Schwab brokerage account.")
    print("        After login you'll land on the live.html page.")
    print("        The URL will look like:")
    print("        https://rava8989.github.io/brave/live.html?code=XXXX&session=YYYY\n")

    redirect = input("Step 3: Paste the FULL redirect URL here:\n> ").strip()

    # Parse code from URL
    parsed = urlparse(redirect)
    params = parse_qs(parsed.query)
    code_list = params.get("code", [])
    if not code_list:
        # Maybe they pasted just the code
        code = redirect.strip()
    else:
        code = code_list[0]

    print(f"\nExchanging code: {code[:20]}...")
    tokens = exchange_code(code)
    print("\n✅ Done! tokens.json saved.")
    print(f"   Access token expires in: {tokens.get('expires_in', '?')} seconds")
    print(f"   Refresh token will be auto-used to renew.\n")

if __name__ == "__main__":
    main()

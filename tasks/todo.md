# Fix Schwab Disconnect (refresh_token race)

## Root cause

Schwab rotates the `refresh_token` on every successful refresh and immediately
invalidates the old one. Three independent clients each hold a copy and each
can rotate it:

1. **Cloudflare Worker** (`schwab-proxy.js`) — `getAccessToken()` reads/writes
   KV `schwab_tokens`; mutex protects within one invocation but not across
   isolates or scheduled/fetch handlers.
2. **Browser** (`index.html`) — `schwabRefreshToken()` POSTs to Worker
   `/token` with its sessionStorage refresh_token, then pushes the new tokens
   back to KV via `/sync`.
3. **Python scraper** (`scraper/schwab_client.py`) — refreshes against Schwab
   *directly* (not via Worker) and writes to local `tokens.json`.

Whichever client rotates first invalidates the other two. The losers get
401s forever and the user has to re-auth.

## Fix: single source of truth = Worker KV

The Worker already owns KV. Make it the only party that talks to Schwab's
OAuth endpoint for refresh. Everyone else asks the Worker for a current
access token.

- [x] **Worker**: add `GET /access-token` — auth via `X-Sync-Secret`, returns
      `{ access_token, expires_at }`; delegates to `getAccessToken(env)` so
      the mutex + KV write path handles refresh
- [x] **Worker**: harden `getAccessToken` — retry-on-stale: if Schwab 400s on
      refresh (another isolate beat us), re-read KV once and use the winner's
      token; handles cross-invocation races
- [x] **Browser** `index.html`: replace `schwabRefreshToken()` body — call
      `GET /access-token` and update only `access` + `expiry` in sessionStorage;
      never rotate the refresh token from the browser again
- [x] **Python** `scraper/schwab_client.py`: route `get_access_token()` through
      Worker `GET /access-token` when `SCHWAB_PROXY_URL` + `SCHWAB_SYNC_SECRET`
      are set in env; fall back to direct-refresh only when proxy unavailable
- [x] Deploy Worker (`npx wrangler deploy`)
- [x] Verify `schwab_tokens` still populated in KV after deploy
- [x] Verify new `/access-token` endpoint returns 200 with a valid token
- [x] Commit + push

## Review

_Filled in post-verify._

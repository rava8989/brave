/**
 * Schwab API Proxy — Cloudflare Worker
 *
 * Routes:
 *   POST /token   → Token exchange & refresh (injects Basic auth from env secret)
 *   GET  /market/* → Pass-through proxy for market data (forwards Bearer token)
 *   POST /sync    → Browser pushes tokens/creds/discord config to KV
 *   GET  /status  → Returns last cron run result from KV
 *   OPTIONS *      → CORS preflight
 *
 * Env vars (set in Cloudflare dashboard):
 *   SCHWAB_APP_SECRET  — encrypted secret from Schwab developer portal
 *   ALLOWED_ORIGIN     — e.g. https://user.github.io
 *   SYNC_SECRET        — shared secret for /sync endpoint
 *
 * KV binding: SIGNAL_KV
 */

// ════════════════════════════════════════════════════════════════════
// RATE LIMITING
// ════════════════════════════════════════════════════════════════════

const rateLimitMap = new Map(); // Map<ip, {count: number, reset: number}>
let _tokenRefreshPromise = null; // mutex: prevents concurrent Schwab token refreshes

// ════════════════════════════════════════════════════════════════════
// SCHWAB REFRESH HEALTH — circuit-breaker state written to KV
// ────────────────────────────────────────────────────────────────────
// Surfaces "KV says tokens valid, Schwab says no" to the browser so the
// dashboard can show a red "re-auth now" banner instead of silently
// serving stale data for 24h.
// Shape:
//   { ok: true,  lastSuccess: <ms> }
//   { ok: false, lastSuccess: <ms>, lastError: <ms>, msg, consecutiveErrors }
// ════════════════════════════════════════════════════════════════════
async function recordRefreshHealth(env, ok, msg = null) {
  try {
    const prevRaw = await env.SIGNAL_KV.get('schwab_refresh_state');
    const prev = prevRaw ? JSON.parse(prevRaw) : {};
    const now = Date.now();
    const state = ok
      ? { ok: true, lastSuccess: now, consecutiveErrors: 0 }
      : {
          ok: false,
          lastSuccess: prev.lastSuccess || null,
          lastError: now,
          msg: String(msg || '').slice(0, 300),
          consecutiveErrors: (prev.consecutiveErrors || 0) + 1,
        };
    await env.SIGNAL_KV.put('schwab_refresh_state', JSON.stringify(state));
  } catch (e) {
    // Never let health-tracking break the main flow
    console.warn('[proxy] recordRefreshHealth failed:', e.message || e);
  }
}

// ── GitHub-mirror health (2026-06-09) ──
// Same pattern as recordRefreshHealth, KV key 'history_mirror_state'.
// Added after the expired-PAT incident: mirror failures were fully silent
// (console.warn only), so KV drifted ahead of GitHub for DAYS before the
// user noticed empty dashboard rows. /health now surfaces this state and
// the cron watchdog alerts Discord after repeated failures.
async function recordMirrorHealth(env, ok, msg = null) {
  try {
    const prevRaw = await env.SIGNAL_KV.get('history_mirror_state');
    const prev = prevRaw ? JSON.parse(prevRaw) : {};
    const now = Date.now();
    const state = ok
      ? { ok: true, lastSuccess: now, consecutiveErrors: 0 }
      : {
          ok: false,
          lastSuccess: prev.lastSuccess || null,
          lastError: now,
          msg: String(msg || '').slice(0, 300),
          consecutiveErrors: (prev.consecutiveErrors || 0) + 1,
        };
    await env.SIGNAL_KV.put('history_mirror_state', JSON.stringify(state));
  } catch (e) {
    console.warn('[proxy] recordMirrorHealth failed:', e.message || e);
  }
}

// ── COR1M + VVIX cloud capture (2026-06-09 — machine-independence) ─────
// Schwab quotes $COR1M / $VVIX live (validated vs ThetaData: exact match;
// no minute pricehistory exists for $COR1M, so the worker builds its own
// intraday series from quote samples). Replaces the Mac-bound ThetaData
// LaunchAgent as the LIVE data source; the local pipeline remains for the
// backtester bundle (research) only.
//
// KV keys (7-day TTL):
//   cor1m_open_<date>   {cor1m, vvix, at}         — first sample ≥ 9:30 ET
//   cor1m_series_<date> [["HH:MM", value], ...]   — ~5-min samples, cap 120
//   tail_trigger_state  {state:'TRIGGERED', since, value, detectedAt}
const COR1M_TRIGGER_THRESHOLD = 7.75;   // Balanced (recommended) preset (cor1m_contango.html)

async function captureCor1mVvix(env, etNow, token) {
  const h = etNow.getHours(), m = etNow.getMinutes();
  const todayISO = isoDateET(etNow);
  const openKey = `cor1m_open_${todayISO}`;
  const haveOpenRaw = await env.SIGNAL_KV.get(openKey);
  let haveOpen = null;
  try { haveOpen = haveOpenRaw ? JSON.parse(haveOpenRaw) : null; } catch (_) {}
  // Open window runs 9:30–10:00: $COR1M is a calculated index whose FIRST
  // print can arrive 9:35–9:40 ET (2026-06-10: 9:37). The old 9:36 cutoff
  // plus no freshness check captured yesterday's close as today's open.
  const sinceOpenMin = (h - 9) * 60 + (m - 30);
  const inOpenWindow = sinceOpenMin >= 0 && sinceOpenMin <= 30;
  const needVvixBackfill = haveOpen != null && (haveOpen.vvix == null || haveOpen.vixeq == null);
  const isSampleTick = m % 5 === 0;
  if (!(inOpenWindow && (!haveOpen || needVvixBackfill)) && !isSampleTick) return;   // throttle

  const q = await fetchSchwabJSON(
    'https://api.schwabapi.com/marketdata/v1/quotes?symbols=%24COR1M,%24VVIX,%24VIXEQ&fields=quote',
    token, env);
  // Freshness gate (2026-06-10): until an index publishes its first print of
  // the day, Schwab's quote serves the PRIOR session's last value (with a
  // stale tradeTime). Only accept a print from the last 10 minutes — during
  // RTH these indices republish every ~15s, so fresh data always qualifies.
  const isFreshQ = (qq) => {
    const tt = qq?.tradeTime ?? qq?.quoteTime;
    return tt != null && tt > 0 && (Date.now() - tt) < 10 * 60 * 1000;
  };
  const corQ = q?.['$COR1M']?.quote, vvixQ = q?.['$VVIX']?.quote, vixeqQ = q?.['$VIXEQ']?.quote;
  const corRaw = isFreshQ(corQ) ? corQ?.lastPrice : null;
  const vvixRaw = isFreshQ(vvixQ) ? vvixQ?.lastPrice : null;
  const vvix = (vvixRaw != null && vvixRaw > 0) ? parseFloat(vvixRaw.toFixed(2)) : null;
  const vixeqRaw = isFreshQ(vixeqQ) ? vixeqQ?.lastPrice : null;
  const vixeq = (vixeqRaw != null && vixeqRaw > 0) ? parseFloat(vixeqRaw.toFixed(2)) : null;

  // Aux backfill: open captured on an earlier tick before VVIX/VIXEQ printed.
  if (needVvixBackfill && (vvix != null || vixeq != null)) {
    if (haveOpen.vvix == null && vvix != null) haveOpen.vvix = vvix;
    if (haveOpen.vixeq == null && vixeq != null) haveOpen.vixeq = vixeq;
    await env.SIGNAL_KV.put(openKey, JSON.stringify(haveOpen), { expirationTtl: 7 * 86400 });
  }
  if (corRaw == null || !(corRaw > 0)) return;   // no fresh COR1M print yet — retry next tick
  const cor = parseFloat(corRaw.toFixed(2));
  const hhmm = `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;

  // Read today's series (also yields prev sample for cross detection)
  const serKey = `cor1m_series_${todayISO}`;
  let series = [];
  try { const sRaw = await env.SIGNAL_KV.get(serKey); series = sRaw ? JSON.parse(sRaw) : []; } catch (_) {}
  let prev = series.length ? series[series.length - 1][1] : null;

  // First sample of the day: capture open + use the PRIOR session's last
  // sample as `prev` so overnight crosses are caught (any-cross semantics).
  if (!haveOpen) {
    await env.SIGNAL_KV.put(openKey, JSON.stringify({ cor1m: cor, vvix, vixeq, at: hhmm }),
      { expirationTtl: 7 * 86400 });
    if (prev == null) {
      for (let back = 1; back <= 4 && prev == null; back++) {
        const d = new Date(etNow); d.setDate(d.getDate() - back);
        try {
          const pRaw = await env.SIGNAL_KV.get(`cor1m_series_${isoDateET(d)}`);
          if (pRaw) { const ps = JSON.parse(pRaw); if (ps.length) prev = ps[ps.length - 1][1]; }
        } catch (_) {}
      }
    }
  }

  series.push([hhmm, cor]);
  if (series.length > 120) series = series.slice(-120);
  await env.SIGNAL_KV.put(serKey, JSON.stringify(series), { expirationTtl: 7 * 86400 });

  // Cross-DOWN detection — boundary semantics match detect_cross_entries
  // (≥ on the "above" side, ≤ on the "below" side, not both exactly equal).
  if (prev != null
      && prev >= COR1M_TRIGGER_THRESHOLD && cor <= COR1M_TRIGGER_THRESHOLD
      && !(prev === COR1M_TRIGGER_THRESHOLD && cor === COR1M_TRIGGER_THRESHOLD)) {
    const stRaw = await env.SIGNAL_KV.get('tail_trigger_state');
    const st = stRaw ? JSON.parse(stRaw) : null;
    if (!st || st.state !== 'TRIGGERED') {
      await env.SIGNAL_KV.put('tail_trigger_state', JSON.stringify({
        state: 'TRIGGERED', since: todayISO, value: cor,
        detectedAt: new Date().toISOString(), source: 'cloud-quote',
      }));
      _tailHedgeCache = { value: null, fetchedAt: 0 };  // re-arm: drop stale 'No trade' line (mirrors settle path)
      try {
        const dcRaw = await env.SIGNAL_KV.get('discord_config');
        if (dcRaw) {
          const dc = JSON.parse(dcRaw);
          if (dc.channelId) {
            await sendDiscordDM(env, dc.channelId,
              `📉 **COR1M crossed below ${COR1M_TRIGGER_THRESHOLD}** (now ${cor}, ${hhmm} ET).\nTail Hedge trigger ACTIVE — buy the 9:45 ET put daily until first profit (skip days with VVIX ≥ 110${vvix != null ? `; VVIX now ${vvix}` : ''}).`,
              dc.proxyUrl);
          }
        }
      } catch (_) { /* notify best-effort */ }
    }
  }
}

// Today's cloud-captured COR1M open (freshness-gated by captureCor1mVvix).
// Returns the number or null when not yet captured — callers pass it to
// calculateSignal({ cor1m }) so the Diagonal COR1M_LOW filter can evaluate.
async function getCor1mOpenToday(env, todayISO) {
  try {
    const raw = await env.SIGNAL_KV.get(`cor1m_open_${todayISO}`);
    if (!raw) return null;
    const v = JSON.parse(raw)?.cor1m;
    return (v != null && isFinite(v)) ? v : null;
  } catch (_) { return null; }
}

// ════════════════════════════════════════════════════════════════════
// RESEARCH DATA CAPTURE (2026-06-10) — intraday fly marks + 9:45 put snap
// Cloud-side data collection so future research (TP/stop rules for the
// flies, Tail Hedge backtest extension without ThetaData) has raw material.
// KV keys (90-day TTL), persisted to GitHub monthly files at EOD:
//   fly_marks_<date>      {strat: [["HH:MM", mid], ...]}
//   tail_put_snap_<date>  {at, spot, puts: [{e,k,d,b,a}, ...]}
// ════════════════════════════════════════════════════════════════════

async function archiveFlyMarks(env, etNow) {
  const h = etNow.getHours(), m = etNow.getMinutes();
  if (m % 5 !== 0) return;                              // 5-min cadence
  if (h < 9 || (h === 9 && m < 35) || h >= 16) return;  // RTH after entry window
  const todayISO = isoDateET(etNow);
  const hhmm = `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
  const marks = {};
  const sources = [
    ['strad', 'straddle_open_trade'],
    ['bobf',  'bobf_open_trade'],
    ['gxbf',  'gxbf_open_trade'],
    ['diag',  'diagonal_open_trade'],
  ];
  for (const [strat, key] of sources) {
    try {
      const raw = await env.SIGNAL_KV.get(key);
      if (!raw) continue;
      const t = JSON.parse(raw);
      if (!t || t.currentValue == null || !isFinite(t.currentValue)) continue;
      // 0DTE strategies: only today\'s trade. Diagonal: any still-open trade
      // (it holds overnight; exit is next session).
      const isToday = t.openDate === todayISO;
      const stillOpen = !t.closeDate && t.status !== 'closed' && t.status !== 'expired';
      if (strat === 'diag' ? (stillOpen || isToday) : isToday) marks[strat] = t.currentValue;
    } catch (_) { /* per-strategy best-effort */ }
  }
  try {  // M8BF live mark (separate key shape — keyed by 0DTE expiry = today)
    const raw = await env.SIGNAL_KV.get(`m8bf_live_${todayISO}`);
    if (raw) {
      const r = JSON.parse(raw);
      const v = [r.currentValue, r.netDebitNow, r.debit].find(x => x != null && isFinite(x));
      if (v != null) marks.m8bf = v;
    }
  } catch (_) {}
  if (!Object.keys(marks).length) return;
  const k = `fly_marks_${todayISO}`;
  let day = {};
  try { const raw = await env.SIGNAL_KV.get(k); day = raw ? JSON.parse(raw) : {}; } catch (_) {}
  for (const [strat, v] of Object.entries(marks)) {
    (day[strat] = day[strat] || []).push([hhmm, parseFloat(Number(v).toFixed(2))]);
    if (day[strat].length > 100) day[strat] = day[strat].slice(-100);
  }
  await env.SIGNAL_KV.put(k, JSON.stringify(day), { expirationTtl: 90 * 86400 });
}

async function captureTailPutSnap(env, etNow, masterChain) {
  const h = etNow.getHours(), m = etNow.getMinutes();
  if (!(h === 9 && m >= 45 && m <= 59)) return;   // AT the 9:45 tail entry time (quote == entry)
  if (!masterChain || !masterChain.putExpDateMap) return;
  const todayISO = isoDateET(etNow);
  const k = `tail_put_snap_${todayISO}`;
  if (await env.SIGNAL_KV.get(k)) return;          // once per day
  const exps = Object.keys(masterChain.putExpDateMap).sort().slice(0, 2);  // 0-1 DTE
  const puts = [];
  for (const exp of exps) {
    const strikes = masterChain.putExpDateMap[exp] || {};
    for (const [strike, arr] of Object.entries(strikes)) {
      const c = Array.isArray(arr) ? arr[0] : arr;
      if (!c || c.delta == null) continue;
      const d = Number(c.delta);
      if (!(d <= -0.05 && d >= -0.35)) continue;
      puts.push({ e: exp.split(':')[0], k: parseFloat(strike), d: parseFloat(d.toFixed(3)),
                  b: c.bid ?? null, a: c.ask ?? null });
    }
  }
  if (!puts.length) return;
  puts.sort((x, y) => x.e.localeCompare(y.e) || x.k - y.k);
  const hhmm = `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
  await env.SIGNAL_KV.put(k, JSON.stringify({ at: hhmm, spot: masterChain.spot ?? null, puts: puts.slice(0, 60) }),
    { expirationTtl: 90 * 86400 });
}

// Diagonal chain snapshot (2026-06-10, user: "capture diagonal like tail
// hedge so we can drop ThetaData"). At ~12:30 ET (the live entry time) store
// the SPX PUT chain slice the Diagonal pipeline needs: expiries 0-2 DTE
// (short leg + next-day exit) and 15-40 DTE (long leg band), strikes within
// spot±150 (covers +10 ITM short, -20 long, and next-day drift for exits —
// generous enough to re-test other offsets/widths later).
async function captureDiagChainSnap(env, etNow, masterChain) {
  const h = etNow.getHours(), m = etNow.getMinutes();
  if (!(h === 12 && m >= 25 && m <= 40)) return;
  if (!masterChain || !masterChain.putExpDateMap || !masterChain.spot) return;
  const todayISO = isoDateET(etNow);
  const k = `diag_chain_snap_${todayISO}`;
  if (await env.SIGNAL_KV.get(k)) return;
  const spot = masterChain.spot;
  const out = {};
  for (const [expKey, strikes] of Object.entries(masterChain.putExpDateMap)) {
    const dte = parseInt(expKey.split(':')[1] || '0', 10);
    if (!((dte >= 0 && dte <= 2) || (dte >= 15 && dte <= 40))) continue;
    const exp = expKey.split(':')[0];
    const rows = [];
    for (const [strike, arr] of Object.entries(strikes)) {
      const kk = parseFloat(strike);
      if (Math.abs(kk - spot) > 150) continue;
      const c = Array.isArray(arr) ? arr[0] : arr;
      if (!c) continue;
      rows.push([kk, c.bid ?? null, c.ask ?? null]);
    }
    if (rows.length) { rows.sort((a, b) => a[0] - b[0]); out[`${exp}:${dte}`] = rows; }
  }
  if (!Object.keys(out).length) return;
  const hhmm = `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
  await env.SIGNAL_KV.put(k, JSON.stringify({ at: hhmm, spot, puts: out }), { expirationTtl: 90 * 86400 });
}

// GXBF chain snapshot — at ~9:35 ET store the 0DTE SPX CALL chain within
// ±5% of spot incl. per-strike bid/ask/IV/volume/OI: everything build_day
// (fetch_thetadata_gxbf.py) derives centers + mids grids from. With this,
// gxbf_bt_data can be extended Schwab-only.
async function captureGxbfChainSnap(env, etNow, masterChain) {
  const h = etNow.getHours(), m = etNow.getMinutes();
  if (!(h === 9 && m >= 35 && m <= 50)) return;
  if (!masterChain || !masterChain.callExpDateMap || !masterChain.spot) return;
  const todayISO = isoDateET(etNow);
  const k = `gxbf_chain_snap_${todayISO}`;
  if (await env.SIGNAL_KV.get(k)) return;
  const spot = masterChain.spot;
  const expKey = Object.keys(masterChain.callExpDateMap).find(e => (e.split(':')[1] || '') === '0');
  if (!expKey) return;
  const rows = [];
  for (const [strike, arr] of Object.entries(masterChain.callExpDateMap[expKey])) {
    const kk = parseFloat(strike);
    if (kk < spot * 0.95 || kk > spot * 1.05) continue;
    const c = Array.isArray(arr) ? arr[0] : arr;
    if (!c) continue;
    rows.push([kk, c.bid ?? null, c.ask ?? null, c.volatility ?? null, c.totalVolume ?? 0, c.openInterest ?? 0]);
  }
  if (!rows.length) return;
  rows.sort((a, b) => a[0] - b[0]);
  const hhmm = `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
  await env.SIGNAL_KV.put(k, JSON.stringify({ at: hhmm, spot, exp: expKey.split(':')[0], calls: rows }),
    { expirationTtl: 90 * 86400 });
}

// VIX-surface snapshot (2026-06-11, optionsgelt-inspired VIX decomposition).
// At ~15:45 ET store the ~30DTE SPX smile at a sparse moneyness grid
// (85%–110% of spot, OTM side: puts below / calls above, both at ATM).
// masterChain is only ±$200 wide, so this makes its own chain call pinned to
// ONE expiry ≈ today+30d (weekend-rolled; walks ±1/2 days on holiday misses —
// SPXW expires daily, so the first probe almost always hits) with
// strikeCount=500 for the deep-put wing. Schwab supplies per-contract
// `volatility`, so day-over-day smile moves decompose into sticky-strike /
// parallel / skew components with NO IV solving. Rows: [k, right, bid, ask, iv, delta].
const SURFACE_MONEYNESS = [0.85, 0.88, 0.90, 0.92, 0.94, 0.96, 0.98, 1.00, 1.02, 1.04, 1.06, 1.10];
async function captureVixSurfaceSnap(env, etNow, token) {
  const h = etNow.getHours(), m = etNow.getMinutes();
  if (!(h === 15 && m >= 40 && m <= 55)) return;
  if (!token) return;
  const todayISO = isoDateET(etNow);
  const kvKey = `vix_surface_snap_${todayISO}`;
  if (await env.SIGNAL_KV.get(kvKey)) return;       // once per day
  // target expiry: today+30 calendar days, rolled off weekends
  const base = new Date(etNow); base.setDate(base.getDate() + 30);
  if (base.getDay() === 6) base.setDate(base.getDate() + 2);      // Sat → Mon
  else if (base.getDay() === 0) base.setDate(base.getDate() + 1); // Sun → Mon
  let data = null, expKey = null;
  for (const off of [0, 1, -1, 2, -2]) {
    const d = new Date(base); d.setDate(d.getDate() + off);
    if (d.getDay() === 0 || d.getDay() === 6) continue;
    const iso = isoDateET(d);
    try {
      const cand = await fetchSchwabJSON(
        `https://api.schwabapi.com/marketdata/v1/chains?symbol=%24SPX&strikeCount=500&fromDate=${iso}&toDate=${iso}&includeUnderlyingQuote=true&strategy=SINGLE&contractType=ALL`,
        token, env);
      const keys = Object.keys(cand?.putExpDateMap || {});
      if (keys.length) { data = cand; expKey = keys[0]; break; }
    } catch (_) { /* try next candidate day */ }
  }
  if (!data || !expKey) return;
  const spot = data.underlyingPrice || data.underlying?.last;
  if (!spot) return;
  const pickRows = (map, right) => {
    const strikes = Object.keys(map?.[expKey] || {}).map(parseFloat).sort((a, b) => a - b);
    const rows = [];
    for (const mny of SURFACE_MONEYNESS) {
      if (right === 'P' && mny > 1.001) continue;   // puts: ≤ ATM
      if (right === 'C' && mny < 0.999) continue;   // calls: ≥ ATM
      const target = spot * mny;
      let best = null, bd = Infinity;
      for (const s of strikes) { const d = Math.abs(s - target); if (d < bd) { bd = d; best = s; } }
      if (best == null || bd > spot * 0.02) continue;   // no strike within 2% of target → skip point
      const arr = map[expKey][String(best)] ?? map[expKey][best.toFixed(1)];
      const c = Array.isArray(arr) ? arr[0] : arr;
      if (!c) continue;
      const iv = (c.volatility != null && c.volatility > 0 && c.volatility < 500) ? c.volatility : null;
      rows.push([best, right, c.bid ?? null, c.ask ?? null, iv, c.delta ?? null]);
    }
    return rows;
  };
  const rows = [...pickRows(data.putExpDateMap, 'P'), ...pickRows(data.callExpDateMap, 'C')];
  if (rows.length < 6) return;                       // too sparse to be useful — retry next tick
  const hhmm = `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
  await env.SIGNAL_KV.put(kvKey, JSON.stringify(
    { at: hhmm, spot, exp: expKey.split(':')[0], dte: parseInt(expKey.split(':')[1], 10), rows }),
    { expirationTtl: 90 * 86400 });
}

// CycleLab 5-min slot helpers — shared by the EOD append and the live feed.
function _cycSlots() {
  const slots = [];
  for (let h = 9, m = 30; h < 16; m += 5) { if (m >= 60) { m = 0; h++; if (h >= 16) break; }
    slots.push(`${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`); }
  return slots;
}

// Schwab 1-min candles → per-5-min-slot net moves array (nulls where empty).
function _cycMovesFromCandles(candles, iso, slots, slotIdx) {
  const oc = {};   // slot idx -> [firstOpen, lastClose]
  for (const c of candles) {
    if (!(c.open > 0 && c.close > 0)) continue;
    const t = toET(new Date(c.datetime));
    if (isoDateET(t) !== iso) continue;
    const h = t.getHours(), m = t.getMinutes();
    if (h < 9 || (h === 9 && m < 30) || h >= 16) continue;
    const idx = slotIdx[`${String(h).padStart(2, '0')}:${String(m - (m % 5)).padStart(2, '0')}`];
    if (idx == null) continue;
    if (oc[idx]) oc[idx][1] = c.close; else oc[idx] = [c.open, c.close];
  }
  const moves = new Array(slots.length).fill(null);
  for (const [idx, [o, c]] of Object.entries(oc)) moves[idx] = parseFloat((c - o).toFixed(2));
  return { moves, filled: Object.keys(oc).length };
}

// CycleLab LIVE feed (2026-06-11, user: "continue the orange SPX line live").
// Once per 5 min during RTH the cron snapshots today's session-so-far into
// KV `cyc_today` ({d, w, m, at} — cyclicality_data day format + timestamp).
// Page views read it via GET /cyclicality-today (pure KV) — ZERO extra
// Schwab calls per viewer; total cost ≈ 78 pricehistory calls/day.
async function captureCycTodaySlots(env, etNow, token) {
  const h = etNow.getHours(), m = etNow.getMinutes();
  const inWindow = (h > 9 || (h === 9 && m >= 36)) && (h < 16 || (h === 16 && m <= 6));
  if (!inWindow || m % 5 !== 1 || !token) return;   // :36/:41/… — slot just completed
  if (etNow.getDay() === 0 || etNow.getDay() === 6 || isHol(etNow)) return;
  const todayISO = isoDateET(etNow);
  const slots = _cycSlots();
  const slotIdx = Object.fromEntries(slots.map((s, i) => [s, i]));
  const start = Date.parse(`${todayISO}T08:00:00Z`), end = Date.parse(`${todayISO}T23:00:00Z`);
  let hist;
  try {
    hist = await fetchSchwabJSON(
      `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24SPX&periodType=day&frequencyType=minute&frequency=1&startDate=${start}&endDate=${end}&needExtendedHoursData=false`,
      token, env);
  } catch (e) { console.warn('[cyc-live]', e.message); return; }
  const { moves, filled } = _cycMovesFromCandles(hist.candles || [], todayISO, slots, slotIdx);
  if (!filled) return;
  const wd = new Date(`${todayISO}T12:00:00Z`).getUTCDay() - 1;   // 0=Mon
  await env.SIGNAL_KV.put('cyc_today', JSON.stringify(
    { d: todayISO, w: wd, m: moves, at: `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}` }),
    { expirationTtl: 86400 });
  // NDX live slots (2026-06-12) — same cadence, own KV key
  try {
    const histN = await fetchSchwabJSON(
      `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24NDX&periodType=day&frequencyType=minute&frequency=1&startDate=${start}&endDate=${end}&needExtendedHoursData=false`,
      token, env);
    const rN = _cycMovesFromCandles(histN.candles || [], todayISO, slots, slotIdx);
    if (rN.filled) await env.SIGNAL_KV.put('cyc_today_ndx', JSON.stringify(
      { d: todayISO, w: wd, m: rN.moves, at: `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}` }),
      { expirationTtl: 86400 });
  } catch (e) { console.warn('[cyc-live-ndx]', e.message); }
}

// CycleLab daily feed (2026-06-10): append today\'s (and any recent missing)
// SPX session to cyclicality_data.json from Schwab 1-min pricehistory —
// 5-min slot net moves, the format build_cyclicality_data.py produces.
// ThetaData-free; the page self-updates daily. Runs at EOD + manual route.
async function appendCyclicalityDays(env, opts = {}) {
  const token = await getAccessToken(env);
  const symbol = opts.symbol || '%24SPX';                 // '%24SPX' | '%24NDX'
  const file = opts.file || 'cyclicality_data.json';      // NDX → cyclicality_ndx.json
  const backDays = Math.min(45, opts.backDays || 12);
  const slots = _cycSlots();
  const slotIdx = Object.fromEntries(slots.map((s, i) => [s, i]));

  // current file (raw — cheaper than contents API for a 450KB read)
  const curResp = await fetch(`https://raw.githubusercontent.com/rava8989/brave/main/${file}`,
    { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } });
  const cur = curResp.ok ? await curResp.json() : { slots, days: [] };
  const have = new Set(cur.days.map(x => x.d));

  // candidate days: last N calendar days, weekdays, non-holiday, missing
  const etNow = toET(new Date());
  const todo = [];
  for (let back = 0; back <= backDays; back++) {
    const d = new Date(etNow); d.setDate(d.getDate() - back);
    if (d.getDay() === 0 || d.getDay() === 6 || isHol(d)) continue;
    const iso = isoDateET(d);
    if (have.has(iso)) continue;
    // skip today before the close — only completed sessions
    if (iso === isoDateET(etNow) && (etNow.getHours() < 16)) continue;
    todo.push(iso);
  }
  if (!todo.length) return { ok: true, appended: [] };

  const appended = [];
  for (const iso of todo.sort()) {
    const start = Date.parse(`${iso}T08:00:00Z`), end = Date.parse(`${iso}T23:00:00Z`);
    let hist;
    try {
      hist = await fetchSchwabJSON(
        `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=${symbol}&periodType=day&frequencyType=minute&frequency=1&startDate=${start}&endDate=${end}&needExtendedHoursData=false`,
        token, env);
    } catch (e) { console.warn('[cyclelab] pricehistory failed', iso, e.message); continue; }
    const { moves, filled } = _cycMovesFromCandles(hist.candles || [], iso, slots, slotIdx);
    if (filled < 30) { console.warn('[cyclelab] too few slots', iso); continue; }
    const wd = new Date(`${iso}T12:00:00Z`).getUTCDay() - 1;   // 0=Mon
    appended.push({ d: iso, w: wd, m: moves });
  }
  if (!appended.length) return { ok: true, appended: [] };

  await githubUpsertResearchFile(env, file, curObj => {
    if (!curObj.days) { curObj.slots = slots; curObj.days = []; }
    const haveNow = new Set(curObj.days.map(x => x.d));
    for (const rec of appended) if (!haveNow.has(rec.d)) curObj.days.push(rec);
    curObj.days.sort((a, b) => a.d.localeCompare(b.d));
    curObj.built = isoDateET(toET(new Date()));
    return curObj;
  }, `auto: cyclicality ${appended.map(a => a.d).join(', ')}`);
  try { await logEvent(env, 'info', 'research', `cyclicality appended ${appended.length} day(s)`, { days: appended.map(a => a.d) }); } catch (_) {}
  return { ok: true, appended: appended.map(a => a.d) };
}

// ════════════════════════════════════════════════════════════════════
// ADVISORY SCORECARD (2026-06-11) — score our own morning claims.
// Idea adopted from a public vol dashboard that tracks its own predictions.
// Each evening: score GEX regime calls (PIN = day range < 100 pts;
// BREAKOUT = range >= 70 pts — thresholds from the 2026-06-10 validation:
// PIN median 57, BREAKOUT median 84, 95% of PIN days < 100) and the
// Day-type strategy claims (favored → P/L > 0; below-normal → P/L < its
// normal; flat → |P/L| < half its normal). Ledger: data/advisory_scorecard.json
// ════════════════════════════════════════════════════════════════════
const SCORE_DAYTYPE_CLAIMS = {
  'NEUTRAL/BEAR': [['diagPL', 'flat', 356], ['bobfPL', 'favored', 510]],
  'NEUTRAL/CHOP': [['diagPL', 'favored', 378]],
  'NEUTRAL/BULL': [['stradPL', 'below-normal', 1091]],
};

// Vol-flow claims (2026-06-11): scored cells = the ones that replicated both
// halves in the 3-leg study. Straddle/VOL_BID is suggestive-only — NOT scored.
// 'above-normal' → P/L > normal; 'below-normal' → P/L < normal.
const SCORE_VOLFLOW_CLAIMS = {
  VOL_BID: [['m8bfPL', 'below-normal', 434]],
  VOL_SUPPLY: [['diagPL', 'below-normal', 378]],
  MIXED: [['m8bfPL', 'above-normal', 434], ['diagPL', 'above-normal', 378]],
};

async function scoreAdvisories(env) {
  const gh = { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } };
  const [cycR, bunR, hist] = await Promise.all([
    fetch('https://raw.githubusercontent.com/rava8989/brave/main/cyclicality_data.json', gh),
    fetch('https://raw.githubusercontent.com/rava8989/brave/main/cor1m_contango_bundle.json', gh),
    env.SIGNAL_KV.get('history_data').then(r => r ? JSON.parse(r) : []),
  ]);
  if (!cycR.ok || !bunR.ok) throw new Error('source fetch failed');
  const cyc = await cycR.json();
  const daily = (await bunR.json()).daily || [];
  const regimeByDate = Object.fromEntries(daily.map(x => [x.date, x.regime]));
  const bundleDates = daily.map(x => x.date);
  const histBy = Object.fromEntries(hist.map(r => [r.date, r]));
  const cycBy = Object.fromEntries(cyc.days.map(x => [x.d, x]));

  // gex_daily monthly files
  const gexd = {};
  for (const ym of [...new Set(cyc.days.filter(x => x.d >= '2026-03-01').map(x => x.d.slice(0, 7)))]) {
    try {
      const r = await fetch(`https://raw.githubusercontent.com/rava8989/brave/main/data/gex_daily/${ym}.json`, gh);
      if (r.ok) Object.assign(gexd, await r.json());
    } catch (_) {}
  }

  // vix decomposition (vol-flow labels) — keyed by the day the label is FOR;
  // the advisory on day d used the latest label STRICTLY BEFORE d (≤5d gap).
  let decomp = {};
  try {
    const r = await fetch('https://raw.githubusercontent.com/rava8989/brave/main/data/vix_decomposition.json', gh);
    if (r.ok) decomp = await r.json();
  } catch (_) {}
  const decompDates = Object.keys(decomp).sort();
  const priorLabelFor = d => {
    let lo = 0, hi = decompDates.length - 1, best = null;
    while (lo <= hi) { const m = (lo + hi) >> 1;
      if (decompDates[m] < d) { best = decompDates[m]; lo = m + 1; } else hi = m - 1; }
    if (!best) return null;
    const gap = (new Date(d) - new Date(best)) / 86400000;
    return gap <= 5 ? decomp[best].label : null;
  };

  const dayRange = d => {
    const m = cycBy[d]?.m; if (!m) return null;
    let c = 0, hi = 0, lo = 0;
    for (const v of m) { c += (v || 0); if (c > hi) hi = c; if (c < lo) lo = c; }
    return hi - lo;
  };
  const mkDate = s => new Date(+s.slice(0, 4), +s.slice(5, 7) - 1, +s.slice(8, 10), 12);

  const scored = {};
  for (const d of Object.keys(gexd).sort()) {
    if (!cycBy[d]) continue;
    const entry = {};
    // 1. GEX regime call
    const am = gexd[d]?.am;
    const rng = dayRange(d);
    if (am?.regime && rng != null) {
      const hit = am.regime === 'BREAKOUT' ? rng >= 70 : rng < 100;
      entry.gex = { regime: am.regime, range: Math.round(rng), hit };
    }
    // 2. Day-type strategy claims (line used PRIOR day's regime + that day's shape)
    const bi = bundleDates.indexOf(d);
    const prevRegime = bi > 0 ? regimeByDate[bundleDates[bi - 1]] : null;
    const group = regimeGroup(prevRegime);
    const cycInfo = classifyCyclePrediction(cyc.days, mkDate(d));
    if (group && cycInfo) {
      const shape = { BULLISH: 'BULL', BEARISH: 'BEAR', CHOPPY: 'CHOP', MIXED: 'MIX' }[cycInfo.cls];
      const key = `${group}/${shape}`;
      entry.daytype = { key, cells: [] };
      for (const [fld, claim, normal] of (SCORE_DAYTYPE_CLAIMS[key] || [])) {
        const pnl = histBy[d]?.[fld];
        if (pnl == null || pnl === 0) continue;   // strategy didn't trade → unscorable
        const hit = claim === 'favored' ? pnl > 0
                  : claim === 'below-normal' ? pnl < normal
                  : Math.abs(pnl) < normal / 2;   // 'flat'
        entry.daytype.cells.push({ strat: fld.replace('PL', ''), claim, pnl, hit });
      }
    }
    // 3. Vol-flow claims (prior day's decomposition label)
    const vfLabel = priorLabelFor(d);
    if (vfLabel) {
      entry.volflow = { label: vfLabel, cells: [] };
      for (const [fld, claim, normal] of (SCORE_VOLFLOW_CLAIMS[vfLabel] || [])) {
        const pnl = histBy[d]?.[fld];
        if (pnl == null || pnl === 0) continue;   // strategy didn't trade → unscorable
        const hit = claim === 'above-normal' ? pnl > normal : pnl < normal;
        entry.volflow.cells.push({ strat: fld.replace('PL', ''), claim, pnl, hit });
      }
      if (!entry.volflow.cells.length) delete entry.volflow;
    }
    if (entry.gex || (entry.daytype && entry.daytype.cells.length) || entry.volflow) scored[d] = entry;
  }

  await githubUpsertResearchFile(env, 'data/advisory_scorecard.json',
    cur => { Object.assign(cur, scored); return cur; }, 'auto: advisory scorecard');
  return { days: Object.keys(scored).length };
}

// Month-to-date scorecard line for the evening preview.
async function scorecardLine(env, etNow) {
  try {
    const r = await fetch('https://raw.githubusercontent.com/rava8989/brave/main/data/advisory_scorecard.json',
      { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } });
    if (!r.ok) return null;
    const led = await r.json();
    const ym = isoDateET(etNow).slice(0, 7);
    let gh = 0, gt = 0, dh = 0, dt = 0, vh = 0, vt = 0;
    for (const [d, e] of Object.entries(led)) {
      if (!d.startsWith(ym)) continue;
      if (e.gex) { gt++; if (e.gex.hit) gh++; }
      for (const c of (e.daytype?.cells || [])) { dt++; if (c.hit) dh++; }
      for (const c of (e.volflow?.cells || [])) { vt++; if (c.hit) vh++; }
    }
    if (!gt && !dt && !vt) return null;
    const parts = [];
    if (gt) parts.push(`GEX ${gh}/${gt}`);
    if (dt) parts.push(`Day-type ${dh}/${dt}`);
    if (vt) parts.push(`Vol-flow ${vh}/${vt}`);
    return `Scorecard: ${parts.join(' · ')} (MTD)`;
  } catch (_) { return null; }
}

// Generic GitHub research-file upsert — same auth pattern as
// mirrorHistoryToGitHub but path-parameterized and merge-based.
// mutate(currentObj) → newObj. 404 (no file yet) starts from {}.
async function githubUpsertResearchFile(env, path, mutate, message) {
  if (!env.GITHUB_TOKEN) return { skipped: 'no GITHUB_TOKEN' };
  const apiUrl = `https://api.github.com/repos/rava8989/brave/contents/${path}`;
  const ghHeaders = {
    'Authorization': `Bearer ${env.GITHUB_TOKEN}`,
    'Accept': 'application/vnd.github+json',
    'User-Agent': 'schwab-proxy-worker/1.0',
    'X-GitHub-Api-Version': '2022-11-28',
  };
  const getResp = await fetch(apiUrl, { headers: ghHeaders });
  let sha = null, cur = {};
  if (getResp.ok) {
    const meta = await getResp.json();
    sha = meta.sha;
    try { cur = JSON.parse(atob((meta.content || '').replace(/\n/g, ''))); } catch (_) { cur = {}; }
  } else if (getResp.status !== 404) {
    throw new Error(`GH GET ${path} ${getResp.status}`);
  }
  const next = mutate(cur) || cur;
  const body = { message: message || `auto: update ${path}`, content: btoa(JSON.stringify(next, null, 0)) };
  if (sha) body.sha = sha;
  const putResp = await fetch(apiUrl, {
    method: 'PUT', headers: { ...ghHeaders, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!putResp.ok) throw new Error(`GH PUT ${path} ${putResp.status}: ${(await putResp.text()).slice(0, 150)}`);
  return { ok: true };
}

// EOD: fold today\'s KV research captures into monthly GitHub files so they
// survive the 90-day KV TTL. Best-effort — never blocks the EOD settle.
async function persistResearchArtifacts(env, etNow) {
  const todayISO = isoDateET(etNow);
  const ym = todayISO.slice(0, 7);
  const jobs = [
    [`fly_marks_${todayISO}`,      `data/fly_marks/${ym}.json`,   'fly marks'],
    [`tail_put_snap_${todayISO}`,  `data/tail_puts/${ym}.json`,   'tail put snap'],
    [`diag_chain_snap_${todayISO}`,`data/diag_chains/${ym}.json`, 'diag chain snap'],
    [`gxbf_chain_snap_${todayISO}`,`data/gxbf_chains/${ym}.json`, 'gxbf chain snap'],
    [`vix_surface_snap_${todayISO}`,`data/vix_surface/${ym}.json`,'vix surface snap'],
  ];
  const results = [];
  for (const [kvKey, ghPath, label] of jobs) {
    try {
      const raw = await env.SIGNAL_KV.get(kvKey);
      if (!raw) { results.push({ label, skipped: 'no KV data' }); continue; }
      const payload = JSON.parse(raw);
      const r = await githubUpsertResearchFile(env, ghPath,
        cur => { cur[todayISO] = payload; return cur; },
        `auto: ${label} ${todayISO}`);
      results.push({ label, ...r });
      try { await logEvent(env, 'info', 'research', `${label} persisted to ${ghPath}`, {}); } catch (_) {}
    } catch (e) {
      results.push({ label, error: e.message });
      console.warn(`[research-persist] ${label} failed:`, e.message);
      try { await logEvent(env, 'error', 'research', `${label} persist FAILED`, { msg: e.message }); } catch (_) {}
    }
  }
  // GEX daily summary: morning snapshot (gex_daily KV) + closing gex_current
  try {
    const amRaw = await env.SIGNAL_KV.get(`gex_daily_${todayISO}`);
    const pmRaw = await env.SIGNAL_KV.get('gex_current');
    const trim = g => g ? { t: g.timestamp, spot: g.spot, regime: g.regime, totalGex: g.totalGex,
                            flip: g.flipStrike ?? null, maxPos: g.maxPosStrike ?? null, maxNeg: g.maxNegStrike ?? null } : null;
    const am = amRaw ? JSON.parse(amRaw).am : null;
    const pm = pmRaw ? trim(JSON.parse(pmRaw)) : null;
    if (am || pm) {
      await githubUpsertResearchFile(env, `data/gex_daily/${ym}.json`,
        cur => { cur[todayISO] = { am, pm }; return cur; }, `auto: gex daily ${todayISO}`);
      results.push({ label: 'gex daily', ok: true });
    } else results.push({ label: 'gex daily', skipped: 'no snapshots' });
  } catch (e) {
    results.push({ label: 'gex daily', error: e.message });
    console.warn('[research-persist] gex daily failed:', e.message);
  }
  // VIXEQ daily self-feed (2026-06-11): open from the 9:30-10:00 capture
  // (cor1m_open KV), close quoted fresh now. Extends data/vixeq_daily.json
  // (ThetaData backfill 2024-10→2026-06) Schwab-only, same {date:{open,close}}.
  try {
    const openRec = JSON.parse(await env.SIGNAL_KV.get(`cor1m_open_${todayISO}`) || 'null');
    const open = openRec?.vixeq ?? null;
    let close = null;
    try {
      const tk = await getAccessToken(env);
      const q = await fetchSchwabJSON('https://api.schwabapi.com/marketdata/v1/quotes?symbols=%24VIXEQ&fields=quote', tk, env);
      const qq = q?.['$VIXEQ']?.quote;
      const tt = qq?.tradeTime ?? qq?.quoteTime;
      if (tt && isoDateET(toET(new Date(tt))) === todayISO && qq.lastPrice > 0) close = parseFloat(qq.lastPrice.toFixed(2));
    } catch (_) { /* close best-effort */ }
    if (open != null || close != null) {
      await githubUpsertResearchFile(env, 'data/vixeq_daily.json',
        cur => { cur[todayISO] = { open: open ?? cur[todayISO]?.open ?? null, close: close ?? cur[todayISO]?.close ?? null }; return cur; },
        `auto: vixeq ${todayISO}`);
      results.push({ label: 'vixeq daily', ok: true });
    } else results.push({ label: 'vixeq daily', skipped: 'no data' });
  } catch (e) {
    results.push({ label: 'vixeq daily', error: e.message });
    console.warn('[research-persist] vixeq failed:', e.message);
  }
  return results;
}

// ── COT weekly self-feed (2026-06-12) ───────────────────────────────────
// CFTC publishes Friday ~15:30 ET (data as of Tuesday). Appends any new
// weeks for the 9 currency contracts to data/cot_currencies.json on GitHub.
// Initial backfill: fetch_cot_data.py (2000→). Manual: GET /cot-refresh-now.
const COT_CODES = { EUR: '099741', JPY: '097741', GBP: '096742', CAD: '090741',
                    CHF: '092741', AUD: '232741', NZD: '112741', MXN: '095741', DXY: '098662' };

async function cotWeeklyRefresh(env) {
  const cur = await (await fetch('https://raw.githubusercontent.com/rava8989/brave/main/data/cot_currencies.json',
    { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } })).json();
  const added = {};
  for (const [key, code] of Object.entries(COT_CODES)) {
    const rows = cur.series[key] || [];
    const last = rows.length ? rows[rows.length - 1][0] : '2000-01-01';
    const q = new URLSearchParams({
      '$select': 'report_date_as_yyyy_mm_dd,open_interest_all,'
        + 'noncomm_positions_long_all,noncomm_positions_short_all,'
        + 'comm_positions_long_all,comm_positions_short_all,'
        + 'nonrept_positions_long_all,nonrept_positions_short_all',
      '$where': `cftc_contract_market_code='${code}' AND report_date_as_yyyy_mm_dd>'${last}'`,
      '$order': 'report_date_as_yyyy_mm_dd ASC', '$limit': '100',
    });
    try {
      const r = await fetch(`https://publicreporting.cftc.gov/resource/6dca-aqww.json?${q}`,
        { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } });
      if (!r.ok) continue;
      const fresh = await r.json();
      const newRows = fresh.map(x => [
        x.report_date_as_yyyy_mm_dd.slice(0, 10), +x.open_interest_all,
        +x.noncomm_positions_long_all, +x.noncomm_positions_short_all,
        +x.comm_positions_long_all, +x.comm_positions_short_all,
        +x.nonrept_positions_long_all, +x.nonrept_positions_short_all,
      ]).filter(r2 => r2.slice(1).every(Number.isFinite));
      if (newRows.length) added[key] = newRows;
    } catch (e) { console.warn('[cot]', key, e.message); }
  }
  if (!Object.keys(added).length) return { ok: true, added: 0 };
  await githubUpsertResearchFile(env, 'data/cot_currencies.json', curF => {
    for (const [key, rows] of Object.entries(added)) {
      const have = new Set((curF.series[key] || []).map(r => r[0]));
      curF.series[key] = (curF.series[key] || []).concat(rows.filter(r => !have.has(r[0])));
    }
    return curF;
  }, `auto: COT ${Object.values(added)[0][Object.values(added)[0].length - 1][0]}`);
  try { await logEvent(env, 'info', 'research', `COT refreshed: +${Object.values(added).reduce((a, b) => a + b.length, 0)} rows`, {}); } catch (_) {}
  return { ok: true, added: Object.fromEntries(Object.entries(added).map(([k, v]) => [k, v.length])) };
}

// ── Auto-Tilt advisory (2026-06-10) — ADVISORY ONLY, no sizing automation ──
// Ports multi-strategy-tester.html _tiltWindowMAR/_tiltWeightsForDay with the
// tester defaults (win 60, floor 0.25x, cap 2.0x, minTrades 5, marCap 99).
// Reads KV history_data (rows strictly BEFORE today — no look-ahead) and
// renders one line for the Discord messages: "Tilt 60d: M8BF 1.2x ...".
const TILT_P = { win: 60, floorM: 0.25, capM: 2, minTrades: 5, marCap: 99 };
const TILT_FIELDS = [['M8BF', 'm8bfPL'], ['Strad', 'stradPL'], ['BOBF', 'bobfPL'], ['GXBF', 'gxbfPL'], ['Diag', 'diagPL']];

function tiltWindowMAR(rows, i, fld, p) {
  const lo = Math.max(0, i - p.win);
  let n = 0, sum = 0, cum = 0, peak = 0, mdd = 0;
  for (let j = lo; j < i; j++) {
    const v = rows[j][fld];
    if (v == null || v === 0) continue;
    n++; sum += v; cum += v;
    if (cum > peak) peak = cum;
    const dd = peak - cum;
    if (dd > mdd) mdd = dd;
  }
  if (n < p.minTrades) return { neutral: true, mar: 0 };
  return { neutral: false, mar: mdd > 0 ? sum / mdd : (sum > 0 ? p.marCap : 0) };
}

async function computeTiltLine(env, todayISO) {
  const raw = await env.SIGNAL_KV.get('history_data');
  if (!raw) return null;
  let rows;
  try { rows = JSON.parse(raw); } catch (_) { return null; }
  if (!Array.isArray(rows) || rows.length < 20) return null;
  rows = rows.filter(r => r.date && r.date < todayISO).sort((a, b) => a.date.localeCompare(b.date));
  const i = rows.length, p = TILT_P, nAct = TILT_FIELDS.length, eq = 1 / nAct;
  const raw_ = {}, isNeutral = {};
  let rawSum = 0, nNeutral = 0;
  for (const [, fld] of TILT_FIELDS) {
    const mres = tiltWindowMAR(rows, i, fld, p);
    if (mres.neutral) { isNeutral[fld] = true; nNeutral++; }
    else { raw_[fld] = Math.min(Math.max(mres.mar, 0), p.marCap); rawSum += raw_[fld]; }
  }
  const nNon = nAct - nNeutral;
  const nonNeutralMass = 1 - nNeutral * eq;
  const parts = [];
  for (const [name, fld] of TILT_FIELDS) {
    let wi;
    if (isNeutral[fld])  wi = eq;
    else if (rawSum > 0) wi = nonNeutralMass * (raw_[fld] / rawSum);
    else                 wi = nonNeutralMass / nNon;
    wi = Math.min(Math.max(wi, p.floorM * eq), p.capM * eq);
    parts.push(`${name} ${(wi * nAct).toFixed(1)}x`);
  }
  return `Tilt 60d   │ ${parts.join(' · ')}`;
}

// Morning GEX line (2026-06-10, git-mined validation): the PIN/BREAKOUT
// regime is real — BREAKOUT mornings realized 103-pt avg ranges vs 60 on
// PIN, and the pin-style book bled on BREAKOUT mornings. One line, colored
// by regime, in both morning messages. ADVISORY only.
async function computeGexLine(env) {
  const raw = await env.SIGNAL_KV.get('gex_current');
  if (!raw) return null;
  const g = JSON.parse(raw);
  if (!g || !g.regime || g.totalGex == null) return null;
  const bn = g.totalGex / 1e9;
  const flipTxt = g.flipStrike != null ? `flip ${g.flipStrike}` : 'no flip in range (one-sided)';
  return `GEX        │ ${g.regime} ${bn >= 0 ? '+' : '-'}$${Math.abs(bn).toFixed(1)}B · ${flipTxt}`;
}

// CycleLab shape advisory for the morning message (2026-06-10).
// INFORMATIONAL ONLY. Fetches cyclicality_data.json (the worker itself keeps
// it current via the EOD append) and classifies today\'s 4-week prediction.
async function computeCycleLine(env, etNow) {
  const todayISO = isoDateET(etNow);
  const ck = `cycle_line_${todayISO}`;
  const cached = await env.SIGNAL_KV.get(ck);
  if (cached) return cached === 'none' ? null : cached;
  let line = null;
  try {
    const [r, rb] = await Promise.all([
      fetch('https://raw.githubusercontent.com/rava8989/brave/main/cyclicality_data.json',
        { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } }),
      fetch('https://raw.githubusercontent.com/rava8989/brave/main/cor1m_contango_bundle.json',
        { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } }),
    ]);
    if (r.ok) {
      const cyc = await r.json();
      const info = classifyCyclePrediction(cyc.days, etNow);
      // Regime: last bundle day (= yesterday) — today's COR1M may not be
      // captured yet at send time; regimes persist day-to-day. Honest label.
      let group = null;
      if (rb.ok) {
        try {
          const daily = (await rb.json()).daily || [];
          if (daily.length) group = regimeGroup(daily[daily.length - 1].regime);
        } catch (_) {}
      }
      line = dayTypeAdvisoryLine(group, info);
    }
  } catch (e) { console.warn('[cycle-line]', e.message); }
  await env.SIGNAL_KV.put(ck, line || 'none', { expirationTtl: 86400 });
  return line;
}

// ── Vol-flow advisory (2026-06-11) — INFORMATIONAL ONLY ────────────────
// JS port of compute_vix_decomposition.py: yesterday's ~30DTE smile vs the
// day before's splits ΔATM-IV into sticky-strike slide / parallel (real
// repricing) / twist → label. Validated cells live in signal-engine
// VOLFLOW_STATS (M8BF↓ after VOL_BID, Diag↓ after VOL_SUPPLY, both ↑ on
// MIXED — all replicate both halves). Smile source: vix_surface_snap KV
// (15:45 capture), GitHub data/vix_surface/<ym>.json fallback (incl. the
// ThetaData-backfill seed).

function _smileFn(rec) {
  const byK = {};
  for (const r of rec.rows || []) {
    const k = r[0], iv = r[4];                 // [k, right, bid, ask, iv, (delta)]
    if (iv != null && iv > 0) (byK[k] = byK[k] || []).push(iv);
  }
  const pts = Object.entries(byK)
    .map(([k, v]) => [parseFloat(k), v.reduce((a, b) => a + b, 0) / v.length])
    .sort((a, b) => a[0] - b[0]);
  if (pts.length < 4) return null;
  const ks = pts.map(p => p[0]), ivs = pts.map(p => p[1]);
  const fn = x => {
    if (x <= ks[0]) return ivs[0];
    if (x >= ks[ks.length - 1]) return ivs[ivs.length - 1];
    for (let i = 1; i < ks.length; i++) {
      if (x <= ks[i]) {
        const w = (x - ks[i - 1]) / (ks[i] - ks[i - 1]);
        return ivs[i - 1] * (1 - w) + ivs[i] * w;
      }
    }
    return ivs[ivs.length - 1];
  };
  return { fn, lo: ks[0], hi: ks[ks.length - 1] };
}

// Same math/keys as compute_vix_decomposition.py — the page chart reads both.
function computeDecompPair(prevRec, curRec) {
  const f0 = _smileFn(prevRec), f1 = _smileFn(curRec);
  if (!f0 || !f1 || !prevRec.spot || !curRec.spot) return null;
  const s0 = prevRec.spot, s1 = curRec.spot;
  const atm0 = f0.fn(s0), atm1 = f1.fn(s1);
  const dATM = atm1 - atm0;
  const slide = f0.fn(Math.min(Math.max(s1, f0.lo), f0.hi)) - f0.fn(s0);
  const klo = Math.max(f0.lo, f1.lo, s0 * 0.88), khi = Math.min(f0.hi, f1.hi, s0 * 1.06);
  if (khi <= klo) return null;
  let parallel = 0;
  for (let j = 0; j < 9; j++) parallel += f1.fn(klo + (khi - klo) * j / 8) - f0.fn(klo + (khi - klo) * j / 8);
  parallel /= 9;
  const residual = dATM - slide - parallel;
  const putSkew0 = f0.fn(s0 * 0.92) - atm0, putSkew1 = f1.fn(s1 * 0.92) - atm1;
  const callSkew0 = f0.fn(s0 * 1.04) - atm0, callSkew1 = f1.fn(s1 * 1.04) - atm1;
  let label;
  if (Math.abs(slide) >= Math.abs(dATM) * 2 / 3 && Math.abs(parallel) < Math.max(Math.abs(dATM) / 3, 0.15)) label = 'MECHANICAL';
  else if (parallel >= 0.5) label = 'VOL_BID';
  else if (parallel <= -0.5) label = 'VOL_SUPPLY';
  else label = 'MIXED';
  const r2 = x => Math.round(x * 100) / 100;
  return { atm: r2(atm1), spot: s1, dATM: r2(dATM), slide: r2(slide), parallel: r2(parallel),
           residual: r2(residual), put_skew: r2(putSkew1), d_put_skew: r2(putSkew1 - putSkew0),
           call_skew: r2(callSkew1), d_call_skew: r2(callSkew1 - callSkew0), label };
}

// EOD: compute today's decomposition record (today's 15:45 smile vs the
// prior session's) → KV vix_decomp_<date> + upsert data/vix_decomposition.json
// so the page chart self-feeds. Idempotent; best-effort.
async function computeVixDecompDaily(env, etNow) {
  const todayISO = isoDateET(etNow);
  if (etNow.getDay() === 0 || etNow.getDay() === 6 || isHol(etNow)) return { skipped: 'non-trading' };
  if (await env.SIGNAL_KV.get(`vix_decomp_${todayISO}`)) return { skipped: 'done' };
  const curRaw = await env.SIGNAL_KV.get(`vix_surface_snap_${todayISO}`);
  if (!curRaw) return { skipped: 'no surface snap today' };
  const cur = JSON.parse(curRaw);
  // prior session smile: KV walk-back ≤5d, then GitHub monthly file(s)
  let prev = null;
  for (let back = 1; back <= 5 && !prev; back++) {
    const d = new Date(etNow); d.setDate(d.getDate() - back);
    const raw = await env.SIGNAL_KV.get(`vix_surface_snap_${isoDateET(d)}`);
    if (raw) { try { prev = JSON.parse(raw); } catch (_) {} }
  }
  if (!prev) {
    const lo = new Date(etNow); lo.setDate(lo.getDate() - 5);
    const months = [...new Set([isoDateET(lo).slice(0, 7), todayISO.slice(0, 7)])];
    const found = {};
    for (const ym of months) {
      try {
        const r = await fetch(`https://raw.githubusercontent.com/rava8989/brave/main/data/vix_surface/${ym}.json`,
          { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } });
        if (r.ok) Object.assign(found, await r.json());
      } catch (_) {}
    }
    const loISO = isoDateET(lo);
    const cand = Object.keys(found).filter(d => d < todayISO && d >= loISO).sort();
    if (cand.length) prev = found[cand[cand.length - 1]];
  }
  if (!prev) return { skipped: 'no prior smile within 5d' };
  const rec = computeDecompPair(prev, cur);
  if (!rec) return { skipped: 'smiles too sparse' };
  await env.SIGNAL_KV.put(`vix_decomp_${todayISO}`, JSON.stringify(rec), { expirationTtl: 90 * 86400 });
  await githubUpsertResearchFile(env, 'data/vix_decomposition.json',
    curF => { curF[todayISO] = rec; return curF; }, `auto: vix decomp ${todayISO} ${rec.label}`);
  return { ok: true, label: rec.label };
}

// Morning-message line: YESTERDAY's label (KV walk-back ≤5d, GitHub
// data/vix_decomposition.json fallback). Cached per day like computeCycleLine.
async function computeVolFlowLine(env, etNow) {
  const todayISO = isoDateET(etNow);
  const ck = `volflow_line_${todayISO}`;
  const cached = await env.SIGNAL_KV.get(ck);
  if (cached) return cached === 'none' ? null : cached;
  let label = null;
  for (let back = 1; back <= 5 && !label; back++) {
    const d = new Date(etNow); d.setDate(d.getDate() - back);
    const raw = await env.SIGNAL_KV.get(`vix_decomp_${isoDateET(d)}`);
    if (raw) { try { label = JSON.parse(raw).label; } catch (_) {} }
  }
  if (!label) {
    try {
      const r = await fetch('https://raw.githubusercontent.com/rava8989/brave/main/data/vix_decomposition.json',
        { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } });
      if (r.ok) {
        const dd = await r.json();
        const lo = new Date(etNow); lo.setDate(lo.getDate() - 5);
        const loISO = isoDateET(lo);
        const cand = Object.keys(dd).filter(d => d < todayISO && d >= loISO).sort();
        if (cand.length) label = dd[cand[cand.length - 1]].label;
      }
    } catch (e) { console.warn('[volflow-line]', e.message); }
  }
  const line = volFlowAdvisoryLine(label);
  await env.SIGNAL_KV.put(ck, line || 'none', { expirationTtl: 86400 });
  return line;
}

// ── M8BF service-WR line (2026-06-12) — INFORMATIONAL ONLY ─────────────
// Yesterday's whole-day service win rate (history m8bfWR) + cold-streak
// context. Trailing AVERAGES tested flat — only yesterday's value + the
// trailing-5 extreme flag are shown. Cached per day.
async function computeM8bfWrLine(env, etNow) {
  const todayISO = isoDateET(etNow);
  const ck = `m8bfwr_line_${todayISO}`;
  const cached = await env.SIGNAL_KV.get(ck);
  if (cached) return cached === 'none' ? null : cached;
  let line = null;
  try {
    const raw = await env.SIGNAL_KV.get('history_data');
    if (raw) {
      const rows = JSON.parse(raw).filter(r => r.date && r.date < todayISO && r.m8bfWR != null)
        .sort((a, b) => a.date.localeCompare(b.date));
      if (rows.length >= 6) {
        const yWR = rows[rows.length - 1].m8bfWR;
        const t5 = rows.slice(-5).reduce((s, r) => s + r.m8bfWR, 0) / 5;
        // percentile of current trailing-5 vs all history
        const all5 = [];
        for (let i = 5; i <= rows.length; i++)
          all5.push(rows.slice(i - 5, i).reduce((s, r) => s + r.m8bfWR, 0) / 5);
        const pct = 100 * all5.filter(v => v <= t5).length / all5.length;
        line = m8bfWrAdvisoryLine(yWR, t5, pct);
      }
    }
  } catch (e) { console.warn('[m8bfwr-line]', e.message); }
  await env.SIGNAL_KV.put(ck, line || 'none', { expirationTtl: 86400 });
  return line;
}

// ── Nightly data-completeness watchdog (2026-06-12, user-approved) ─────
// Verifies TODAY landed in every self-feeding dataset; auto-heals via the
// idempotent jobs; Discord only when something needed fixing. Own tick
// (18:35-18:50) so it can never be starved by other chains (lessons P17).
async function dataCompletenessCheck(env, etNow) {
  const todayISO = isoDateET(etNow);
  // Before ~16:30 ET today's data legitimately doesn't exist yet — checks
  // would all false-alarm. (The scheduled run is 18:35.)
  if (etNow.getHours() < 16 || (etNow.getHours() === 16 && etNow.getMinutes() < 30))
    return { date: todayISO, skipped: 'before EOD — nothing to verify yet' };
  const ym = todayISO.slice(0, 7);
  const gh = { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } };
  const J = async (u) => { const r = await fetch(u, gh); return r.ok ? r.json() : null; };
  const healed = [], failed = [], ok = [];

  const checks = [
    ['cyclicality SPX', async () => {
      const j = await J('https://raw.githubusercontent.com/rava8989/brave/main/cyclicality_data.json');
      return !!(j && j.days.some(d => d.d === todayISO));
    }, () => appendCyclicalityDays(env)],
    ['cyclicality NDX', async () => {
      const j = await J('https://raw.githubusercontent.com/rava8989/brave/main/cyclicality_ndx.json');
      return !!(j && j.days.some(d => d.d === todayISO));
    }, () => appendCyclicalityDays(env, { symbol: '%24NDX', file: 'cyclicality_ndx.json' })],
    ['vix decomposition', async () => {
      const j = await J('https://raw.githubusercontent.com/rava8989/brave/main/data/vix_decomposition.json');
      return !!(j && j[todayISO]);
    }, () => computeVixDecompDaily(env, etNow)],
    ['research persists', async () => {
      // representative pair: fly marks (captured every RTH day) + vixeq
      const fm = await J(`https://raw.githubusercontent.com/rava8989/brave/main/data/fly_marks/${ym}.json`);
      const ve = await J('https://raw.githubusercontent.com/rava8989/brave/main/data/vixeq_daily.json');
      const kvFm = await env.SIGNAL_KV.get(`fly_marks_${todayISO}`);
      const fmOk = !kvFm || !!(fm && fm[todayISO]);   // no capture → nothing to persist
      return fmOk && !!(ve && ve[todayISO]);
    }, () => persistResearchArtifacts(env, etNow)],
    ['EOD history fields', async () => {
      const raw = await env.SIGNAL_KV.get('history_data');
      if (!raw) return false;
      const row = JSON.parse(raw).find(r => r.date === todayISO);
      return !!(row && row.vixClose != null);
    }, null],   // settle has its own retry path — report only
  ];
  if (etNow.getDay() === 5) checks.push(['COT weekly', async () => {
    const j = await J('https://raw.githubusercontent.com/rava8989/brave/main/data/cot_currencies.json');
    if (!j) return false;
    const last = j.series.EUR[j.series.EUR.length - 1][0];
    return (new Date(todayISO) - new Date(last)) / 86400000 <= 5;
  }, () => cotWeeklyRefresh(env)]);

  for (const [name, check, heal] of checks) {
    try {
      if (await check()) { ok.push(name); continue; }
      if (!heal) { failed.push(name + ' (no auto-heal)'); continue; }
      await heal();
      if (await check()) healed.push(name);
      else failed.push(name);
    } catch (e) { failed.push(`${name} (${e.message.slice(0, 60)})`); }
  }
  const result = { date: todayISO, ok: ok.length, healed, failed };
  if (healed.length || failed.length) {
    try {
      const dcRaw = await env.SIGNAL_KV.get('discord_config');
      if (dcRaw) {
        const dc = JSON.parse(dcRaw);
        if (dc.channelId) await sendDiscordDM(env, dc.channelId,
          `🩺 **Data watchdog** (${todayISO})` +
          (healed.length ? `\n✅ auto-healed: ${healed.join(', ')}` : '') +
          (failed.length ? `\n❌ NEEDS ATTENTION: ${failed.join(', ')}` : ''),
          dc.proxyUrl);
      }
    } catch (_) {}
  }
  try { await logEvent(env, failed.length ? 'error' : 'info', 'watchdog', JSON.stringify(result).slice(0, 200), {}); } catch (_) {}
  return result;
}

// ── Weekly digest (2026-06-12, user-approved) — Sundays 18:00 ET ───────
async function weeklyDigest(env) {
  const etNow = toET(new Date());
  const todayISO = isoDateET(etNow);
  const weekAgo = new Date(etNow); weekAgo.setDate(weekAgo.getDate() - 7);
  const fromISO = isoDateET(weekAgo);
  const lines = [`📒 **Weekly digest — week ending ${todayISO}**`];
  try {
    const rows = JSON.parse(await env.SIGNAL_KV.get('history_data') || '[]')
      .filter(r => r.date && r.date > fromISO && r.date <= todayISO);
    const F = [['M8BF', 'm8bfPL'], ['Strad', 'stradPL'], ['BOBF', 'bobfPL'], ['GXBF', 'gxbfPL'], ['Diag', 'diagPL'], ['Tail', 'tailPL']];
    let tot = 0;
    const parts = [];
    for (const [nm, f] of F) {
      const v = rows.map(r => r[f]).filter(x => x != null && x !== 0);
      if (!v.length) continue;
      const s = v.reduce((a, b) => a + b, 0); tot += s;
      parts.push(`${nm} ${s >= 0 ? '+' : ''}$${Math.round(s).toLocaleString()} (${v.length})`);
    }
    lines.push(`P/L: ${parts.join(' · ') || 'no trades'} → **${tot >= 0 ? '+' : ''}$${Math.round(tot).toLocaleString()}**`);
  } catch (_) {}
  try {
    const led = await (await fetch('https://raw.githubusercontent.com/rava8989/brave/main/data/advisory_scorecard.json',
      { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } })).json();
    let g = [0, 0], d = [0, 0], v = [0, 0];
    for (const [dt, e] of Object.entries(led)) {
      if (dt <= fromISO || dt > todayISO) continue;
      if (e.gex) { g[1]++; if (e.gex.hit) g[0]++; }
      for (const c of (e.daytype?.cells || [])) { d[1]++; if (c.hit) d[0]++; }
      for (const c of (e.volflow?.cells || [])) { v[1]++; if (c.hit) v[0]++; }
    }
    lines.push(`Scorecard wk: GEX ${g[0]}/${g[1]} · Day-type ${d[0]}/${d[1]} · Vol-flow ${v[0]}/${v[1]}`);
  } catch (_) {}
  try {
    const nxt = [];
    const horizon = new Date(etNow); horizon.setDate(horizon.getDate() + 7);
    for (const [label, arr] of [['FED', fedSch], ['CPI', cpiSch], ['OPEX', opexSch]]) {
      for (const s of arr) {
        const d2 = parseLong(s);
        if (d2 && d2 > etNow && d2 <= horizon) nxt.push(`${label} ${isoDateET(d2).slice(5)}`);
      }
    }
    if (nxt.length) lines.push(`Next week: ${nxt.join(' · ')}`);
  } catch (_) {}
  const dcRaw = await env.SIGNAL_KV.get('discord_config');
  if (dcRaw) {
    const dc = JSON.parse(dcRaw);
    if (dc.channelId) await sendDiscordDM(env, dc.channelId, lines.join('\n'), dc.proxyUrl);
  }
  return { ok: true, lines: lines.length };
}

// ── Daily total-risk cap (2026-06-09) ──────────────────────────────────
// On multi-strategy days, Straddle + BOBF + GXBF + Diagonal can all be live
// at once and nothing checked COMBINED max-loss against the account. Each
// automated open now calls enforceRiskCap() with its own max theoretical
// loss; if existing open exposure + the new trade would exceed the cap,
// the open is refused (fail-closed: a blocked good trade costs opportunity,
// an unblocked stack can cost the account).
//
// Config: KV key 'risk_config' = { "enabled": true, "maxOpenRiskUsd": 8000 }
//   — change the cap WITHOUT redeploying:
//   npx wrangler kv key put --namespace-id=<NS> risk_config '{"enabled":true,"maxOpenRiskUsd":10000}' --remote
// Default: enabled, $8,000 (≈25% of a $31k account).
// M8BF is signal-only (user trades manually) — not gated here.
// mode: 'warn' = trade still fires, Discord warning only (user choice
// 2026-06-10); 'block' = refuse the trade. Both KV-tunable, no redeploy.
const RISK_CAP_DEFAULTS = { enabled: true, maxOpenRiskUsd: 8000, mode: 'warn' };

// Max theoretical loss per open position, in dollars:
//   Straddle / BOBF / GXBF (debit verticals/flies): debit × 100 × contracts
//   Diagonal: (debit + width) × 100 × contracts — spread can go inverted in
//   a crash through both strikes (see index.html sizing notes).
async function computeOpenRiskExposureUsd(env, todayISO) {
  const [stradRaw, bobfRaw, gxbfRaw, diagRaw] = await Promise.all([
    env.SIGNAL_KV.get('straddle_open_trade'),
    env.SIGNAL_KV.get('bobf_open_trade'),
    env.SIGNAL_KV.get('gxbf_open_trade'),
    env.SIGNAL_KV.get('diagonal_open_trade'),
  ]);
  const parts = {};
  const safe = raw => { try { return raw ? JSON.parse(raw) : null; } catch { return null; } };

  const strad = safe(stradRaw);
  if (strad && strad.openDate === todayISO && strad.status !== 'closed') {
    const debit = strad.fillDebit ?? strad.entryDebit;
    if (debit > 0) parts.straddle = Math.round(debit * 100 * (strad.contracts || 1));
  }
  const bobf = safe(bobfRaw);
  if (bobf && bobf.openDate === todayISO && bobf.status !== 'closed') {
    const debit = bobf.fillDebit ?? bobf.entryDebit;
    if (debit > 0) parts.bobf = Math.round(debit * 100 * (bobf.contracts || 1));
  }
  const gxbf = safe(gxbfRaw);
  if (gxbf && gxbf.openDate === todayISO && gxbf.status !== 'closed') {
    if (gxbf.netDebit > 0) parts.gxbf = Math.round(gxbf.netDebit * 100 * (gxbf.contracts || 1));
  }
  const diag = safe(diagRaw);
  if (diag && !diag.closeDate && diag.status !== 'closed' && diag.entryDebit != null) {
    const width = (diag.shortStrike && diag.longStrike) ? (diag.shortStrike - diag.longStrike) : 0;
    parts.diagonal = Math.round((diag.entryDebit + width) * 100 * (diag.contracts || 1));
  }
  const totalUsd = Object.values(parts).reduce((s, v) => s + v, 0);
  return { totalUsd, parts };
}

// Returns { ok: true } or { ok: false, reason } — and on a block, fires a
// once-per-strategy-per-day Discord note so refused trades are never silent.
async function enforceRiskCap(env, etNow, strategy, newTradeMaxLossUsd) {
  try {
    const cfgRaw = await env.SIGNAL_KV.get('risk_config');
    const cfg = { ...RISK_CAP_DEFAULTS, ...(cfgRaw ? JSON.parse(cfgRaw) : {}) };
    if (!cfg.enabled) return { ok: true };
    const todayISO = isoDateET(etNow);
    const { totalUsd, parts } = await computeOpenRiskExposureUsd(env, todayISO);
    const projected = totalUsd + Math.max(0, Math.round(newTradeMaxLossUsd || 0));
    if (projected <= cfg.maxOpenRiskUsd) return { ok: true };

    const warnOnly = cfg.mode !== 'block';   // default 'warn' — trade proceeds
    const reason = `open risk $${totalUsd.toLocaleString()} (${JSON.stringify(parts)}) + new ${strategy} $${Math.round(newTradeMaxLossUsd).toLocaleString()} = $${projected.toLocaleString()} > cap $${cfg.maxOpenRiskUsd.toLocaleString()}`;
    await logEvent(env, 'warn', 'risk-cap', `${strategy} ${warnOnly ? 'WARNING (trade proceeds)' : 'open BLOCKED'}`, { strategy, totalUsd, parts, newTradeMaxLossUsd, cap: cfg.maxOpenRiskUsd, mode: cfg.mode });
    // Discord note, once per strategy per day
    const alertKey = `risk_cap_alert_${strategy}_${todayISO}`;
    if (!(await env.SIGNAL_KV.get(alertKey))) {
      try {
        const dcRaw = await env.SIGNAL_KV.get('discord_config');
        if (dcRaw) {
          const dc = JSON.parse(dcRaw);
          if (dc.channelId) {
            await sendDiscordDM(env, dc.channelId,
              warnOnly
                ? `⚠️ **Risk warning — ${strategy.toUpperCase()} still traded** — ${reason}.\nCombined open max-loss is past your comfort line. Consider trimming. (Switch to hard blocking: KV \`risk_config\` → \`{"mode":"block"}\`.)`
                : `🛑 **Risk cap blocked ${strategy.toUpperCase()}** — ${reason}.\nRaise the cap or set \`{"mode":"warn"}\` via KV \`risk_config\` if intentional.`,
              dc.proxyUrl);
            await env.SIGNAL_KV.put(alertKey, 'sent', { expirationTtl: 86400 });
          }
        }
      } catch (_) { /* notify is best-effort */ }
    }
    if (warnOnly) return { ok: true, warned: true, reason };
    return { ok: false, reason };
  } catch (e) {
    // Fail-OPEN on infrastructure errors: a broken risk check must not
    // silently halt all trading. The error is logged for follow-up.
    console.warn('[risk-cap] check failed (allowing trade):', e.message || e);
    try { await logEvent(env, 'error', 'risk-cap', 'check failed — trade allowed', { msg: e.message }); } catch (_) {}
    return { ok: true, degraded: true };
  }
}

// ── Discord DM send (consolidates the standalone discord-proxy worker) ──
// Priority order, first available wins:
//   1. env.DISCORD_TOKEN (direct in-worker send — preferred, one less worker)
//   2. env.DISCORD_PROXY service binding (legacy; kept until standalone retired)
//   3. proxyUrl arg (legacy public URL)
// Returns {ok, status?, data?, error?}. Never throws — caller decides on retry.
async function sendDiscordDM(env, userId, message, proxyUrl = null) {
  if (!userId || !message) return { ok: false, error: 'missing userId/message' };

  // Detect whether message is an embed object (Option E format) or a string.
  const isEmbed = typeof message === 'object' && message !== null && !Array.isArray(message);

  // Path 1: native — DM via DISCORD_TOKEN directly (supports embeds)
  if (env.DISCORD_TOKEN) {
    try {
      const dmResp = await fetch('https://discord.com/api/v10/users/@me/channels', {
        method: 'POST',
        headers: { 'Authorization': `Bot ${env.DISCORD_TOKEN}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ recipient_id: userId }),
      });
      if (!dmResp.ok) {
        const txt = await dmResp.text();
        return { ok: false, status: dmResp.status, error: `DM channel ${dmResp.status}: ${txt.slice(0, 200)}` };
      }
      const dm = await dmResp.json();
      const payload = isEmbed
        ? { embeds: [message] }
        : { content: String(message).slice(0, 2000) };
      const msgResp = await fetch(`https://discord.com/api/v10/channels/${dm.id}/messages`, {
        method: 'POST',
        headers: { 'Authorization': `Bot ${env.DISCORD_TOKEN}`, 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      let data;
      try { data = await msgResp.json(); } catch { data = { raw: 'non-json' }; }
      if (!msgResp.ok) return { ok: false, status: msgResp.status, data, error: `send ${msgResp.status}` };
      return { ok: true, status: 200, data, source: 'native' };
    } catch (e) {
      return { ok: false, error: 'native: ' + e.message };
    }
  }

  // Path 2/3 (legacy proxies): downgrade embed → plain text since they don't pass embeds through.
  const textMessage = isEmbed ? embedToText(message) : String(message);

  // Path 2: service binding (legacy discord-proxy worker)
  if (env.DISCORD_PROXY) {
    try {
      const hdrs = { 'Content-Type': 'application/json' };
      if (env.PROXY_SECRET) hdrs['Authorization'] = `Bearer ${env.PROXY_SECRET}`;
      const r = await env.DISCORD_PROXY.fetch(new Request('https://dummy/', {
        method: 'POST', headers: hdrs,
        body: JSON.stringify({ userId, message: textMessage.slice(0, 2000) }),
      }));
      let data; try { data = await r.json(); } catch { data = { raw: 'non-json' }; }
      return { ok: r.ok, status: r.status, data, source: 'service-binding',
               ...(r.ok ? {} : { error: `service ${r.status}` }) };
    } catch (e) {
      return { ok: false, error: 'service: ' + e.message };
    }
  }

  // Path 3: HTTP proxyUrl
  if (proxyUrl && proxyUrl.startsWith('https://')) {
    try {
      const hdrs = { 'Content-Type': 'application/json' };
      if (env.PROXY_SECRET) hdrs['Authorization'] = `Bearer ${env.PROXY_SECRET}`;
      const r = await fetch(proxyUrl, {
        method: 'POST', headers: hdrs,
        body: JSON.stringify({ userId, message: textMessage.slice(0, 2000) }),
      });
      let data; try { data = await r.json(); } catch { data = { raw: 'non-json' }; }
      return { ok: r.ok, status: r.status, data, source: 'http-url',
               ...(r.ok ? {} : { error: `http ${r.status}` }) };
    } catch (e) {
      return { ok: false, error: 'http: ' + e.message };
    }
  }

  return { ok: false, error: 'no Discord transport available (no DISCORD_TOKEN/DISCORD_PROXY/proxyUrl)' };
}

// ── Signal subscribers (2026-06-15) — extra Discord user IDs that get a DM
// copy of each morning signal. KV `signal_subscribers` = [{id,label,paused}].
// Discord rule: the bot can only DM a user who shares a server with it AND
// allows server-member DMs; otherwise the API 403s (surfaced to the UI).
async function getSubscribers(env) {
  try { const raw = await env.SIGNAL_KV.get('signal_subscribers'); return raw ? JSON.parse(raw) : []; }
  catch { return []; }
}
async function fanoutSubscribers(env, message) {
  const subs = (await getSubscribers(env)).filter(s => s && s.id && !s.paused);
  const out = [];
  for (const s of subs) {
    try {
      const r = await sendDiscordDM(env, s.id, message);
      out.push({ id: s.id, ok: !!r.ok, status: r.status, error: r.error });
    } catch (e) { out.push({ id: s.id, ok: false, error: e.message }); }
  }
  if (out.length) { try { await logEvent(env, 'info', 'fanout', `signal → ${out.filter(x=>x.ok).length}/${out.length} subscribers`, {}); } catch {} }
  return out;
}

// ── Centralized event logger ──
// Appends to daily_log_<isoDateET> KV. Cap 200 newest entries per day, 7-day
// TTL. FIRE-AND-FORGET: failures are swallowed so logging never breaks the
// calling code path. Read back via GET /logs?date=YYYY-MM-DD (auth-required).
// Use sparingly — log only signal-grade events (signal sent/failed, trade
// open/close, phantom cleanup, auto-recovery, alerts), not every tick.
async function logEvent(env, level, tag, msg, data = null) {
  try {
    const now = new Date();
    const etNow = (typeof toET === 'function') ? toET(now) : now;
    const dateISO = (typeof isoDateET === 'function')
      ? isoDateET(etNow)
      : `${etNow.getFullYear()}-${String(etNow.getMonth()+1).padStart(2,'0')}-${String(etNow.getDate()).padStart(2,'0')}`;
    const key = `daily_log_${dateISO}`;
    const entry = {
      ts: now.toISOString(),
      etTime: `${String(etNow.getHours()).padStart(2,'0')}:${String(etNow.getMinutes()).padStart(2,'0')}:${String(etNow.getSeconds()).padStart(2,'0')}`,
      level, tag, msg,
      ...(data ? { data } : {}),
    };
    let arr = [];
    try {
      const raw = await env.SIGNAL_KV.get(key);
      arr = raw ? JSON.parse(raw) : [];
      if (!Array.isArray(arr)) arr = [];
    } catch { arr = []; }
    arr.unshift(entry);
    if (arr.length > 200) arr = arr.slice(0, 200);
    await env.SIGNAL_KV.put(key, JSON.stringify(arr), { expirationTtl: 7 * 86400 });
  } catch (e) {
    // Never let logging break the main flow
    console.warn('[logEvent] swallowed:', e.message);
  }
}

function checkRateLimit(request) {
  const ip = request.headers.get('CF-Connecting-IP') || request.headers.get('X-Forwarded-For') || 'unknown';
  const now = Date.now();

  // Clean up expired entries
  for (const [key, val] of rateLimitMap) {
    if (val.reset < now) rateLimitMap.delete(key);
  }

  const entry = rateLimitMap.get(ip);
  if (!entry || entry.reset < now) {
    rateLimitMap.set(ip, { count: 1, reset: now + 60_000 });
    return false; // not rate limited
  }

  entry.count += 1;
  if (entry.count > 60) return true; // rate limited
  return false;
}

// ════════════════════════════════════════════════════════════════════
// SIGNAL ENGINE — import from shared module (single source of truth).
// NEVER inline signal logic here. ALL signal rules live in signal-engine.js.
// Edit rules THERE so browser + worker + history page stay in sync.
// ════════════════════════════════════════════════════════════════════

import {
  cpiSch, fedSch, opexSch, holidays, vixSch, earningsSchedule, T,
  toET, dateLong, todayLong,
  isWkend, isHol, isTrade, nextTrade, prevTrade,
  isTodayAfter, isTodayBefore, parseLong, schedInMonth,
  isVixAfterOpexDay, isPostOpexMon, isLastTradeMo, isEomN, isFirstTradeMo, isFirstTradeMon,
  m8Sched, m8Msg, ordinal, wdName, tradeWdLabel,
  isEarningsDay, isNonAmznTslaEarningsDay, isDayAfterAnyEarnings,
  calculateSignal, computeDiagonalSignal, computeVixPct20d,
  classifyCyclePrediction, cycleAdvisoryLine, regimeGroup, dayTypeAdvisoryLine,
  volFlowAdvisoryLine, m8bfWrAdvisoryLine, computeSkewReading,
} from './signal-engine.js';
import { Resvg, initWasm } from '@resvg/resvg-wasm';
import resvgWasm from '@resvg/resvg-wasm/index_bg.wasm';


// ════════════════════════════════════════════════════════════════════
// TAIL HEDGE — fetch today's signal from the default (Balanced) preset in the bundle.
// Bundle is auto-refreshed daily by scripts/refresh_tail_hedge.sh.
// Returns a short status string for the Discord message.
//
// 3 possible states:
//   TRADE today @ 9:45 — buy 0DTE SPXW put delta -0.10, hold to 4 PM
//   SKIP today (VVIX X.XX ≥ 110, puts too expensive). Stay TRIGGERED.
//   No Tail Hedge today (COR1M X.XX, need cross below 7.75)
let _tailHedgeCache = { value: null, fetchedAt: 0 };
async function getTailHedgeStatusLine(env = null) {
  // 5-minute in-worker cache — values change slowly so this is plenty fresh.
  const now = Date.now();
  if (_tailHedgeCache.value && (now - _tailHedgeCache.fetchedAt) < 5*60*1000) {
    return _tailHedgeCache.value;
  }
  try {
    // ET date (2026-06-09 fix: was UTC toISOString — wrong date in evenings).
    const etNow = toET(new Date());
    const todayISO = isoDateET(etNow);

    // CLOUD-FIRST today's values (worker's own Schwab capture); bundle is
    // the fallback + the authority for trigger RESOLUTION (profit exits are
    // only known to the backtest, rebuilt whenever the user's Mac is on).
    let cor1m = null, vvix = null;
    if (env) {
      try {
        const kvOpen = await env.SIGNAL_KV.get(`cor1m_open_${todayISO}`);
        if (kvOpen) { const o = JSON.parse(kvOpen); cor1m = o.cor1m ?? null; vvix = o.vvix ?? null; }
      } catch (_) {}
    }

    const r = await fetch('https://raw.githubusercontent.com/rava8989/brave/main/cor1m_contango_bundle.json',
      { cf: { cacheTtl: 300, cacheEverything: true } });
    let bundleTriggered = false, bundleLastDay = null, bundleLastTriggerDate = null;
    if (r.ok) {
      const b = await r.json();
      const dailyToday = (b.daily || []).find(d => d.date === todayISO);
      if (cor1m == null) cor1m = dailyToday?.cor1m ?? null;
      if (vvix == null)  vvix  = dailyToday?.vvix ?? null;
      const daily = b.daily || [];
      bundleLastDay = daily.length ? daily[daily.length - 1].date : null;
      const defId = b.default_preset || 'balanced';
      const triggers = b.preset_results?.[defId]?.triggers || [];
      const last = triggers[triggers.length - 1];
      bundleTriggered = !!(last && last.exit_reason !== 'profitable');
      bundleLastTriggerDate = last?.trigger_date ?? null;   // START of the bundle's latest episode
    }

    // Cloud-detected cross SINCE the bundle's last day also counts as
    // triggered (covers PC-off stretches where the bundle goes stale).
    // A cloud RESOLVED (first profitable day, set by settleTailEOD) newer than
    // the bundle STOPS the campaign even if a stale bundle still shows active.
    let cloudTriggered = false, cloudResolved = false;
    if (env) {
      try {
        const stRaw = await env.SIGNAL_KV.get('tail_trigger_state');
        if (stRaw) {
          const st = JSON.parse(stRaw);
          cloudTriggered = st.state === 'TRIGGERED'
            && (!bundleLastDay || st.since > bundleLastDay);
          // The worker's RESOLVED is authoritative for the CURRENT trigger episode.
          // Compare resolvedOn to the bundle's last TRIGGER-START (NOT bundleLastDay,
          // the data clock): if the worker resolved on/after the bundle's latest
          // episode started, it's the SAME episode → stay stopped, even when a freshly
          // rebuilt bundle (bundleLastDay advanced past resolvedOn) still shows it
          // active and disagrees on the marginal profit. Only a bundle trigger that
          // STARTED after resolvedOn is a genuinely new episode — and a real cloud
          // cross re-arms anyway via state→TRIGGERED (which carries no resolvedOn).
          cloudResolved = st.state === 'RESOLVED' && st.resolvedOn
            && (!bundleLastTriggerDate || st.resolvedOn >= bundleLastTriggerDate);
        }
      } catch (_) {}
    }

    const isTriggered = cloudResolved ? false : (bundleTriggered || cloudTriggered);
    let line;
    if (isTriggered) {
      if (vvix != null && vvix >= 110) {
        line = `Tail Hedge │ SKIP today (VVIX ${vvix.toFixed(2)} ≥ 110)`;
      } else {
        line = `Tail Hedge │ ▶ TRADE @ 9:45 — buy 0DTE SPXW put Δ-0.10  (VVIX ${vvix?.toFixed(2) ?? '—'})`;
      }
    } else {
      const c = cor1m != null ? cor1m.toFixed(2) : '—';
      line = `Tail Hedge │ No trade today (COR1M ${c}, need < 7.75)`;
    }
    _tailHedgeCache = { value: line, fetchedAt: now };
    return line;
  } catch (e) {
    return `Tail Hedge │ status unavailable (${e.message})`;
  }
}

// SPX options-skew advisory line for the morning message (informational).
// Fetches the daily VIX-smile decomposition (put_skew/call_skew) from GitHub,
// builds the net-skew series, and returns computeSkewReading().line — or null.
// Context only, never gates. See signal-engine.js computeSkewReading().
async function computeSkewLine() {
  try {
    const r = await fetch('https://raw.githubusercontent.com/rava8989/brave/main/data/vix_decomposition.json',
      { cf: { cacheTtl: 600, cacheEverything: true }, headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } });
    if (!r.ok) return null;
    const all = await r.json();
    const series = Object.keys(all).sort().map(dt => {
      const o = all[dt];
      return (o && o.spot && o.put_skew != null && o.call_skew != null)
        ? { date: dt, net: o.put_skew - o.call_skew, spot: o.spot } : null;
    }).filter(Boolean);
    const reading = computeSkewReading(series);
    return reading ? reading.line : null;
  } catch (_) { return null; }
}

// ════════════════════════════════════════════════════════════════════
// DISCORD MESSAGE BUILDER (ported from index.html discordBuildMessage)
// ════════════════════════════════════════════════════════════════════

// Shared footer — used by both the text message and the image-card `content`
// so the live.html link is clickable text instead of being baked into the PNG.
// <URL> suppresses Discord's embed preview; *…* italicizes the disclaimer.
const DISCORD_FOOTER = '📈 Trades are posted live here: <https://rava8989.github.io/brave/live.html>\n*Not financial advice. For informational purposes only.*';

function buildDiscordMessage(signal, vixValues, tailLine) {
  const GRN = '\x1b[32m', RED = '\x1b[31m', DIM = '\x1b[2m', RST = '\x1b[0m';

  const isActive  = text => text && !text.startsWith('No ');
  const isBlocked = text => text && text.startsWith('No ');
  const sigColor  = text => isActive(text) ? GRN : isBlocked(text) ? RED : DIM;

  const m8bfDisplay = signal.m8bfText.replace(/^M8BF\s*[—-]\s*/, '').replace(/^M8BF$/, '—');
  // Use m8bfStrikeInfo (independent of main signal) so strikes render on M8BF
  // line even when main signal was overridden by OPEX+1 GXBF / blocked by gap up.
  // Show only when M8BF's OWN status is active (m8bfText starts with "M8BF").
  const si = signal.m8bfStrikeInfo;
  const m8bfActiveOwn = signal.m8bfText && signal.m8bfText.startsWith('M8BF');
  const strikes = (m8bfActiveOwn && si && si.blocked) ? `Banned center strikes — skip these:  ${si.blocked.join('  ')}  Combo bans (wing-width mod 100 → banned center end):  ${Object.entries(si.comboBans || {}).map(([k,v])=>`${k}→${v}`).join('  ')}` : '';
  const m8bfReason = signal.blockT === 'hard' && signal.rec.includes('M8BF') ? signal.blockD : '';

  let inner = `${DIM}📅 ${signal.dateStr} — ${signal.dayLabel}${RST}\n`;
  inner += `${DIM}${'─'.repeat(34)}${RST}\n`;

  const mc = sigColor(signal.m8bfText);
  inner += `${mc}M8BF     │ ${m8bfDisplay}${RST}\n`;
  if (strikes) inner += `${mc}         │ ${strikes}${RST}\n`;
  if (m8bfReason) inner += `${mc}         │ ${m8bfReason}${RST}\n`;
  inner += `${sigColor(signal.stradText)}Straddle │ ${signal.stradText}${RST}\n`;
  inner += `${sigColor(signal.gxbfText)}GXBF     │ ${signal.gxbfText}${RST}\n`;
  inner += `${sigColor(signal.bobfRec)}BOBF     │ ${signal.bobfRec}${RST}\n`;
  // Diagonal (companion — 10 ITM / 20 wide, 6-filter stack: OPEX-1+EOM+EOM-1+NM+VIX_MID 50-80+COR1M_LOW, 12:30–15:00 ET window; 2026-06-09 safer-tail retune)
  if (signal.diagText) {
    inner += `${sigColor(signal.diagText)}Diagonal │ ${signal.diagText}${RST}\n`;
  }

  // Tail Hedge (companion — recommended preset thr 7.75 / delta -0.10 / VVIX<110; 2026-06-16 sweep: 2× return, half drawdown of -0.20)
  if (tailLine) {
    const tc = tailLine.includes('TRADE') ? GRN : tailLine.includes('SKIP') ? RED : DIM;
    inner += `${tc}${tailLine}${RST}\n`;
  }

  // Auto-Tilt advisory (60d MAR weights — informational, user sizes manually)
  if (signal._tiltLine) inner += `${DIM}${signal._tiltLine}${RST}\n`;

  // GEX regime (validated 2026-06-10: BREAKOUT mornings ≈ 1.7× wider days)
  if (signal._gexLine) {
    const gc = signal._gexLine.includes('PIN') ? GRN : signal._gexLine.includes('BREAKOUT') ? RED : DIM;
    inner += `${gc}${signal._gexLine}${RST}\n`;
  }

  // CycleLab week-pattern (informational — which strategies historically
  // deviate from their own normal on days like today)
  if (signal._cycleLine) {
    const cc = signal._cycleLine.includes('/BULL') ? GRN : signal._cycleLine.includes('/BEAR') ? RED : DIM;
    inner += `${cc}${signal._cycleLine}${RST}\n`;
  }
  if (signal._volFlowLine) {
    const vc = signal._volFlowLine.includes('VOL_BID') ? RED
             : signal._volFlowLine.includes('MIXED') ? GRN : DIM;
    inner += `${vc}${signal._volFlowLine}${RST}\n`;
  }
  if (signal._skewLine) {
    const kc = signal._skewLine.includes('Distribution') ? RED
             : (signal._skewLine.includes('Healthy') || signal._skewLine.includes('Capitulation')) ? GRN : DIM;
    inner += `${kc}${signal._skewLine}${RST}\n`;
  }
  if (signal._m8bfWrLine) {
    const wc = signal._m8bfWrLine.includes('✓both halves') ? GRN
             : signal._m8bfWrLine.includes('COLDEST') ? RED : DIM;
    inner += `${wc}${signal._m8bfWrLine}${RST}\n`;
  }

  // VIX values
  inner += `${DIM}${'─'.repeat(34)}${RST}\n`;
  inner += `${DIM}VIX Prev Close  │ ${vixValues.yClose ?? '—'}${RST}\n`;
  inner += `${DIM}VIX Prev Open   │ ${vixValues.yOpen ?? '—'}${RST}\n`;
  inner += `${DIM}VIX Today Open  │ ${vixValues.todayOpen ?? '—'}${RST}\n`;
  inner += `${DIM}Overnight Drop  │ ${signal.oNight.toFixed(2)}${RST}\n`;
  inner += `${DIM}Open-to-Open    │ ${isNaN(signal.o2o) ? '—' : signal.o2o.toFixed(2)}${RST}\n`;

  // SPX gap
  if (signal.spxGapPct !== null && signal.spxGapPct !== undefined) {
    const dir = signal.spxGapPct > 0 ? '▲' : '▼';
    inner += `${DIM}${'─'.repeat(34)}${RST}\n`;
    inner += `${DIM}SPX Gap         │ ${dir}${Math.abs(signal.spxGapPct).toFixed(2)}%${RST}\n`;
  }

  return `\`\`\`ansi\n${inner}\`\`\`\n${DISCORD_FOOTER}`;
}

// ════════════════════════════════════════════════════════════════════
// EMBED BUILDER — Option E (Discord rich embed)
// Returns a single embed object suitable for { embeds: [obj] } payload.
// ════════════════════════════════════════════════════════════════════
function buildSigma3Embed(signal, vixValues, vixSource) {
  const { todayOpen, yClose, yOpen } = vixValues;
  const oNight = (yClose != null && todayOpen != null) ? (yClose - todayOpen) : null;
  const o2o    = (yOpen != null && todayOpen != null)  ? (yOpen - todayOpen) : null;

  const stripPrefix = (s, p) => s ? s.replace(new RegExp(`^${p}\\s*[—-]?\\s*`), '').replace(/^No\s+\w+\s*\(?/, '').replace(/\)\s*$/, '') : '';

  const fires = [], blocked = [];

  // M8BF
  const m8bfActive = signal.m8bfText && signal.m8bfText.startsWith('M8BF');
  if (m8bfActive) {
    const sc = signal.m8bfStrikeInfo;
    fires.push(`• **M8BF** — window ${sc?.window || signal.entryT || ''}`);
  } else if (signal.m8bfText) {
    blocked.push(`• **M8BF** — ${stripPrefix(signal.m8bfText, 'No M8BF')}`);
  }

  // Straddle
  const stradActive = signal.theme === 'strad';
  if (stradActive) {
    fires.push(`• **Straddle** — ${signal.entryT || '9:32 AM'}${signal.rec?.includes('NM') ? ' (NM)' : signal.rec?.includes('EOM') ? ' (EOM)' : ''}`);
  } else if (signal.stradText) {
    blocked.push(`• **Straddle** — ${stripPrefix(signal.stradText, 'No Straddle')}`);
  }

  // GXBF
  const gxbfActive = signal.theme === 'gxbf';
  if (gxbfActive) {
    const srcTag = signal.centerSource === 'oi' ? ' · OI center' : ' · Vol center';
    fires.push(`• **GXBF** — ${signal.entryT || '9:36 AM'}${srcTag}`);
  } else if (signal.gxbfText) {
    blocked.push(`• **GXBF** — ${stripPrefix(signal.gxbfText, 'No GXBF')}`);
  }

  // BOBF
  if (signal.bobfBadge !== 'BLOCKED') {
    fires.push(`• **BOBF** — ${signal.bobfRec?.replace(/^BOBF\s*[—-]?\s*/, '') || 'fires'}`);
  } else if (signal.bobfRec) {
    blocked.push(`• **BOBF** — ${stripPrefix(signal.bobfRec, 'No BOBF')}`);
  }

  // Diagonal
  if (signal.diagGo) {
    const pctStr = signal.vixPct20d != null ? ` (VIX 20d ${signal.vixPct20d}%)` : '';
    fires.push(`• **Diagonal** — 12:30–15:00 ET${pctStr}`);
  } else if (signal.diagText) {
    blocked.push(`• **Diagonal** — ${stripPrefix(signal.diagText, 'No Diagonal')}`);
  }

  // Tail Hedge (Balanced) — passed in via tailLine (already a formatted string).
  // Detect which bucket to put it in based on the keyword.
  if (signal._tailLine) {
    const tl = signal._tailLine;
    if (tl.includes('TRADE')) {
      fires.push(`• **Tail Hedge** — buy 0DTE SPXW put Δ-0.10 @ 9:45 ET`);
    } else if (tl.includes('SKIP')) {
      blocked.push(`• **Tail Hedge** — ${tl.split('│')[1]?.trim() || 'skip today'}`);
    } else {
      blocked.push(`• **Tail Hedge** — no trigger active`);
    }
  }

  // Color: green if ANY fires, red if all blocked
  const color = fires.length > 0 ? 0x22c55e : 0xef4444;

  // SPX gap
  const gapStr = (signal.spxGapPct != null)
    ? `${signal.spxGapPct > 0 ? '▲' : '▼'}${Math.abs(signal.spxGapPct).toFixed(2)}%`
    : '—';

  // VIX data block (monospace via code fence)
  const fmt = (v) => v != null ? v.toFixed(2) : '—';
  const marketData = '```\n' +
    `VIX  ${fmt(yClose)} → ${fmt(todayOpen)}   o/n ${fmt(oNight)}   o2o ${fmt(o2o)}\n` +
    `SPX  gap ${gapStr}\n` +
    '```';

  const fields = [];
  if (fires.length)   fields.push({ name: '✅ Today\'s plan', value: fires.join('\n'),   inline: false });
  if (blocked.length) fields.push({ name: '❌ Skipping',      value: blocked.join('\n'), inline: false });
  fields.push({ name: '📊 Market data', value: marketData, inline: false });

  // M8BF avoid centers (only when M8BF actually fires)
  if (m8bfActive && signal.m8bfStrikeInfo?.blocked?.length) {
    const sc = signal.m8bfStrikeInfo;
    const combos = Object.entries(sc.comboBans || {}).map(([k,v]) => `${k}→${v}`).join(', ');
    let val = `Skip centers: \`${sc.blocked.join(' · ')}\``;
    if (combos) val += `\nCombo bans: \`${combos}\``;
    fields.push({ name: '🚫 M8BF strike map', value: val, inline: false });
  }

  return {
    title: `Sigma 3 — ${signal.dateStr}`,
    description: `_${signal.dayLabel}_`,
    color,
    fields,
    footer: { text: `${vixSource === 'schwab' ? '📡 Schwab' : '📡 Tastytrade'} · Not financial advice` },
    timestamp: new Date().toISOString(),
  };
}

// Render embed → plain text (fallback for proxy paths that don't support embeds)
function embedToText(embed) {
  let out = `**${embed.title}**\n${embed.description || ''}\n\n`;
  for (const f of (embed.fields || [])) {
    out += `**${f.name}**\n${f.value}\n\n`;
  }
  if (embed.footer?.text) out += `_${embed.footer.text}_`;
  return out.trim();
}

// ════════════════════════════════════════════════════════════════════
// SCHWAB TOKEN HELPERS
// ════════════════════════════════════════════════════════════════════

async function getAccessToken(env, forceRefresh = false) {
  const tokensRaw = await env.SIGNAL_KV.get('schwab_tokens');
  if (!tokensRaw) throw new Error('No Schwab tokens in KV — sync from browser first');
  const tokens = JSON.parse(tokensRaw);

  // Check refresh token not expired
  if (Date.now() > tokens.refreshExpiry) {
    throw new Error('Schwab refresh token expired — re-authenticate in browser');
  }

  // Refresh access token if within 2 minutes of expiry or forced (401 retry)
  if (forceRefresh || Date.now() > tokens.expiry - 120000) {
    // Mutex: if a refresh is already in-flight, all concurrent callers share the same promise
    // so only ONE actual Schwab refresh call is made (Schwab refresh tokens are single-use)
    if (!_tokenRefreshPromise) {
      let timedOut = false;
      _tokenRefreshPromise = Promise.race([
        (async () => {
          try {
            const credsRaw = await env.SIGNAL_KV.get('schwab_creds');
            if (!credsRaw) throw new Error('No Schwab creds in KV');
            const creds = JSON.parse(credsRaw);

            const body = new URLSearchParams({
              grant_type: 'refresh_token',
              refresh_token: tokens.refresh,
            });

            const resp = await fetch('https://api.schwabapi.com/v1/oauth/token', {
              method: 'POST',
              headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': 'Basic ' + btoa(`${creds.appKey}:${env.SCHWAB_APP_SECRET}`),
              },
              body,
            });

            // Retry-on-stale: Schwab 400s when the refresh_token has already
            // been rotated by another Worker isolate (or an external client
            // that still talks to Schwab directly). In that case, re-read KV
            // — whichever client won will have written the new tokens — and
            // return that access token. This is the cross-isolate counterpart
            // to the in-isolate _tokenRefreshPromise mutex.
            if (resp.status === 400) {
              const freshRaw = await env.SIGNAL_KV.get('schwab_tokens');
              if (freshRaw) {
                const fresh = JSON.parse(freshRaw);
                if (fresh.refresh !== tokens.refresh && Date.now() < fresh.expiry - 60000) {
                  console.warn('[proxy] Token refresh lost race; using winner from KV');
                  await recordRefreshHealth(env, true);
                  return fresh.access;
                }
              }
            }

            if (!resp.ok) {
              // Capture Schwab's actual error response so we can debug 400s.
              // Tokens are sensitive — log only first/last 4 chars + length to
              // confirm round-trip integrity without leaking the secret.
              let errBody = '';
              try { errBody = (await resp.text()).slice(0, 500); } catch {}
              const r = tokens.refresh || '';
              const fingerprint = r ? `${r.slice(0,4)}…${r.slice(-4)}(len=${r.length})` : '(none)';
              console.error('[proxy] Token refresh HTTP', resp.status, '— body:', errBody, '— refresh fp:', fingerprint, '— appKey:', creds.appKey?.slice(0, 8) + '…');
              throw new Error('Token refresh HTTP ' + resp.status + ': ' + errBody.slice(0, 120));
            }
            const data = await resp.json();
            if (!data.access_token) throw new Error('Token refresh failed: ' + (data.error_description || JSON.stringify(data)));

            // ALWAYS write to KV on a successful Schwab response, even if our
            // 30s timeout already fired. Rationale: Schwab's refresh_token is
            // single-use — if Schwab rotated, we hold the ONLY valid copy.
            // The old code's `if (!timedOut)` gate dropped the new tokens on
            // the floor when the timeout fired, guaranteeing a 24h stuck
            // state until manual re-auth (observed 2026-04-23). No other
            // isolate can possibly have fresher tokens because this specific
            // refresh_token only validates with Schwab once.
            const newTokens = {
              access: data.access_token,
              refresh: data.refresh_token || tokens.refresh,
              expiry: Date.now() + (data.expires_in * 1000),
              refreshExpiry: data.refresh_token
                ? Date.now() + (7 * 24 * 60 * 60 * 1000)
                : tokens.refreshExpiry,
            };
            await env.SIGNAL_KV.put('schwab_tokens', JSON.stringify(newTokens));
            if (timedOut) {
              console.warn('[proxy] Token refresh returned after timeout — wrote fresh tokens anyway');
            }
            await recordRefreshHealth(env, true);
            return newTokens.access;
          } catch (err) {
            // Record the failure so the browser UI can surface a red banner
            // instead of silently serving stale data. Re-throw to preserve
            // caller error handling.
            await recordRefreshHealth(env, false, err?.message || String(err));
            throw err;
          } finally {
            _tokenRefreshPromise = null;
          }
        })(),
        new Promise((_, reject) => setTimeout(async () => {
          timedOut = true;
          // Record timeout as health failure so browser UI catches it even if
          // the inner refresh never completes. If the inner fn eventually
          // resolves successfully, it will overwrite this with an ok=true
          // record — that's the desired convergence.
          await recordRefreshHealth(env, false, 'Token refresh timeout (30s)');
          // CRITICAL: null _tokenRefreshPromise here too. If the inner refresh
          // fetch was killed by the Workers runtime after our Response returned
          // (which happens for cron handlers when the cron tick ends), its
          // finally block never runs, leaving _tokenRefreshPromise pinned to a
          // rejected value forever. Subsequent calls in the same isolate would
          // skip the `if (!_tokenRefreshPromise)` branch and re-throw the same
          // timeout error indefinitely (observed: 77 consecutive errors over
          // 3+ hours, only fixed by a new deploy that killed the isolate).
          // Concurrent refreshes are safe: the retry-on-stale path at line ~195
          // catches the loser and returns the winner's access from KV.
          _tokenRefreshPromise = null;
          reject(new Error('Token refresh timeout (30s)'));
        }, 30000))
      ]);
    }
    return await _tokenRefreshPromise;
  }

  return tokens.access;
}

async function fetchSchwabJSON(url, token, env) {
  let resp = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  // Retry once with refreshed token on 401
  if (resp.status === 401 && env) {
    console.warn('[proxy] Schwab 401 — retrying with fresh token');
    const freshToken = await getAccessToken(env, true);
    resp = await fetch(url, { headers: { Authorization: `Bearer ${freshToken}` } });
  }
  if (!resp.ok) throw new Error(`Schwab API ${resp.status}: ${url.split('?')[0]}`);
  return resp.json();
}

// ════════════════════════════════════════════════════════════════════
// SCHEDULED HANDLER (Cron Trigger)
// ════════════════════════════════════════════════════════════════════
// TASTYTRADE BACKUP CLIENT (OAuth2 — refresh token never expires)
// ────────────────────────────────────────────────────────────────────
// Free with funded Tastytrade account. Used as a redundant data source
// when Schwab fails (e.g., token expired at 9:30 AM).
//
// OAuth2 flow:
//   1. User registers an OAuth app at my.tastytrade.com (one-time).
//   2. User visits /tasty-oauth-start (browser) → redirected to Tastytrade
//      authorize page → approves → Tastytrade redirects back to
//      /tasty-oauth-callback with ?code=...
//   3. Callback exchanges code for refresh_token, stores it in KV.
//   4. Going forward, getTastyAccessToken() uses refresh_token to mint
//      short-lived (15 min) access tokens. Refresh token never expires.
// ════════════════════════════════════════════════════════════════════

const TASTY_BASE = 'https://api.tastyworks.com';
const TASTY_AUTH_BASE = 'https://my.tastytrade.com';
const TASTY_REDIRECT_URI = 'https://schwab-proxy.ravamt4.workers.dev/tasty-oauth-callback';

// Common headers required by current Tastytrade API
function tastyHeaders(extra = {}) {
  return {
    'Accept': 'application/json',
    'Accept-Version': '20251101',
    'User-Agent': 'schwab-proxy-worker/1.0',
    ...extra,
  };
}

// Build the authorize URL the user visits to grant access
function tastyAuthorizeUrl(env) {
  const params = new URLSearchParams({
    response_type: 'code',
    client_id: env.TASTYTRADE_CLIENT_ID,
    redirect_uri: TASTY_REDIRECT_URI,
    scope: 'read',
  });
  return `${TASTY_AUTH_BASE}/auth.html?${params}`;
}

// Exchange the OAuth `code` from the callback for access_token + refresh_token
async function tastyExchangeCode(env, code) {
  const body = new URLSearchParams({
    grant_type: 'authorization_code',
    code,
    client_id: env.TASTYTRADE_CLIENT_ID,
    client_secret: env.TASTYTRADE_CLIENT_SECRET,
    redirect_uri: TASTY_REDIRECT_URI,
  });
  const resp = await fetch(`${TASTY_BASE}/oauth/token`, {
    method: 'POST',
    headers: tastyHeaders({ 'Content-Type': 'application/x-www-form-urlencoded' }),
    body,
  });
  if (!resp.ok) {
    const txt = await resp.text();
    throw new Error(`Tastytrade code-exchange HTTP ${resp.status}: ${txt.slice(0, 300)}`);
  }
  return resp.json();  // { access_token, refresh_token, expires_in, ... }
}

// Get a short-lived access token. Uses cached one if still fresh, else
// refreshes via the long-lived refresh_token (stored in KV).
async function getTastyAccessToken(env) {
  // Try cache (access tokens live ~15 min)
  const cached = await env.SIGNAL_KV.get('tasty_access_token');
  if (cached) {
    const obj = JSON.parse(cached);
    if (obj.expires_at > Date.now() + 60_000) return obj.access_token;  // 60s buffer
  }
  // Refresh
  const refresh = await env.SIGNAL_KV.get('tasty_refresh_token');
  if (!refresh) throw new Error('Tastytrade refresh_token missing — visit /tasty-oauth-start to authorize');
  if (!env.TASTYTRADE_CLIENT_SECRET) throw new Error('TASTYTRADE_CLIENT_SECRET not configured');

  const body = new URLSearchParams({
    grant_type: 'refresh_token',
    client_secret: env.TASTYTRADE_CLIENT_SECRET,
    refresh_token: refresh,
  });
  const resp = await fetch(`${TASTY_BASE}/oauth/token`, {
    method: 'POST',
    headers: tastyHeaders({ 'Content-Type': 'application/x-www-form-urlencoded' }),
    body,
  });
  if (!resp.ok) {
    const txt = await resp.text();
    throw new Error(`Tastytrade token refresh HTTP ${resp.status}: ${txt.slice(0, 300)}`);
  }
  const data = await resp.json();
  const accessToken = data.access_token;
  const expiresIn = data.expires_in || 900;  // default 15 min
  await env.SIGNAL_KV.put('tasty_access_token', JSON.stringify({
    access_token: accessToken,
    expires_at: Date.now() + (expiresIn * 1000),
  }), { expirationTtl: expiresIn });
  // Refresh token may rotate on refresh — if a new one came back, save it
  if (data.refresh_token && data.refresh_token !== refresh) {
    await env.SIGNAL_KV.put('tasty_refresh_token', data.refresh_token);
  }
  return accessToken;
}

// Get latest VIX quote via Tastytrade. Returns { price, asOf, source }.
// Endpoint per tastyware/tastytrade SDK: /market-data/{instrumentType}/{symbol}
// VIX is InstrumentType.INDEX → "Index", path /market-data/Index/VIX
// Generic Tasty index quote — same /market-data/Index endpoint as VIX but
// symbol-parameterized. Returns { price, open, asOf } or throws. Used as the
// BACKUP source when Schwab quotes are stale/down (2026-06-10 user request:
// "tasty jumps in and picks up the slack").
async function tastyGetIndexQuote(env, symbol) {
  const token = await getTastyAccessToken(env);
  const resp = await fetch(`${TASTY_BASE}/market-data/Index/${encodeURIComponent(symbol)}`,
    { headers: tastyHeaders({ 'Authorization': `Bearer ${token}` }) });
  if (!resp.ok) throw new Error(`Tasty ${symbol} HTTP ${resp.status}: ${(await resp.text()).slice(0, 150)}`);
  const d = (await resp.json())?.data || {};
  const price = d.last ?? d['last-price'] ?? d.mark ?? d.mid ?? null;
  const open = d.open ?? d['open-price'] ?? null;
  const asOf = d['updated-at'] || d['quote-time'] || d.timestamp || null;
  if (price == null) throw new Error(`Tasty ${symbol}: no price field`);
  return { price: parseFloat(price), open: open != null ? parseFloat(open) : null,
           prevClose: d['prev-close'] != null ? parseFloat(d['prev-close']) : (d['close-price'] != null ? parseFloat(d['close-price']) : null),
           asOf, raw: d };
}

async function tastyGetVix(env) {
  const token = await getTastyAccessToken(env);
  const url = `${TASTY_BASE}/market-data/Index/VIX`;
  const resp = await fetch(url, { headers: tastyHeaders({ 'Authorization': `Bearer ${token}` }) });
  if (!resp.ok) {
    const txt = await resp.text();
    throw new Error(`Tastytrade VIX HTTP ${resp.status}: ${txt.slice(0, 200)}`);
  }
  const body = await resp.json();
  const d = body?.data || body;  // payload may be wrapped
  // Tasty exposes today's open as a stable field (mirrors how /Index/SPX does
  // it). Use this for the morning signal — matches Schwab pricehistory 9:30
  // candle.open. The `last`/`mid` fields are CURRENT tick (drifts after open).
  const openRaw = d?.open ?? d?.['open-price'] ?? null;
  const openNum = openRaw != null ? parseFloat(openRaw) : null;
  // VALUE-STALENESS GUARD (2026-06-16): Tasty's `open` carries the prior CLOSE
  // as a pre-open placeholder while `updated-at` keeps ticking fresh on every
  // mark — so a timestamp gate alone (lesson 2026-05-21) lets the stale value
  // through. A real session open ~never equals the prior close to the penny;
  // equality is the signature of "open not published yet". Null it so no caller
  // mistakes it for today's open — they fall back to Schwab (the trusted source)
  // or keep polling. Bug: 2026-06-16 posted VIX open 16.2 == prevClose 16.2.
  const prevCloseRaw = d?.['prev-close'] ?? d?.['close-price'] ?? null;
  const prevCloseNum = prevCloseRaw != null ? parseFloat(prevCloseRaw) : null;
  const openStale = openNum != null && prevCloseNum != null &&
                    Math.abs(openNum - prevCloseNum) < 0.005;
  const open = openStale ? null : openNum;
  // `last`/`mid` kept for callers that want current price (e.g. vixClose path).
  // Fall back to the RAW open (pre-guard) as last resort so `.price` is always
  // a number for backward-compat callers.
  const priceRaw = d?.last ?? d?.['last-price'] ?? d?.mid ?? d?.mark ?? d?.bid ?? openNum;
  const asOf  = d?.['updated-at'] || d?.['quote-time'] || d?.timestamp;
  if (priceRaw == null) {
    throw new Error(`Tastytrade VIX: no usable price field in payload: ${JSON.stringify(d).slice(0, 250)}`);
  }
  return {
    price: parseFloat(priceRaw),
    open,                 // validated today's open — null if stale pre-open snapshot
    openStale,
    prevClose: prevCloseNum,
    asOf, source: 'tastytrade', endpoint: '/market-data/Index/VIX', raw: d,
  };
}

// Get SPX index quote via Tastytrade (same Index market-data endpoint as VIX).
// Returns { price, open, last, asOf, source, raw }. `price` = today's open if
// present (what the morning signal needs), else last/mark. Used as the
// PRIMARY for the signal's SPX-open, with Schwab as fallback — Tasty's
// refresh_token never expires, so a dead Schwab token can't kill the signal.
async function tastyGetSpx(env) {
  const token = await getTastyAccessToken(env);
  const url = `${TASTY_BASE}/market-data/Index/SPX`;
  const resp = await fetch(url, { headers: tastyHeaders({ 'Authorization': `Bearer ${token}` }) });
  if (!resp.ok) {
    const txt = await resp.text();
    throw new Error(`Tastytrade SPX HTTP ${resp.status}: ${txt.slice(0, 200)}`);
  }
  const body = await resp.json();
  const d = body?.data || body;
  const open = d?.open ?? d?.['open-price'] ?? null;
  const last = d?.last ?? d?.['last-price'] ?? d?.mid ?? d?.mark ?? d?.bid ?? null;
  const price = open ?? last;
  const asOf = d?.['updated-at'] || d?.['quote-time'] || d?.timestamp;
  if (price == null) {
    throw new Error(`Tastytrade SPX: no usable price field: ${JSON.stringify(d).slice(0, 250)}`);
  }
  return {
    price: parseFloat(price),
    open: open != null ? parseFloat(open) : null,
    last: last != null ? parseFloat(last) : null,
    asOf, source: 'tastytrade', endpoint: '/market-data/Index/SPX', raw: d,
  };
}

// ─── Tastytrade SPX option-chain fetcher ─────────────────────────────────
// Returns a Schwab-shape { spot, callExpDateMap, putExpDateMap } so it
// can drop into any existing consumer as a fallback when Schwab is dead
// (token expired) or fails. opts:
//   root         — 'SPXW' (default; PM-settled 0DTE/weekly), 'SPX' (AM)
//   expirations  — array of 'YYYY-MM-DD' to limit fetch volume (recommended)
//   strikeCount  — N strikes nearest spot, per expiration (default 80)
//   contractType — 'CALL'|'PUT'|'BOTH' (default 'BOTH')
// Tasty doesn't expose open interest via REST — `openInterest:0` for now.
// Live trading paths use bid/ask/mark only, so OI=0 is acceptable.
async function tastyFetchSpxChain(env, opts = {}) {
  const root = opts.root || 'SPXW';
  const wantExp = opts.expirations ? new Set(opts.expirations) : null;
  const strikeCount = Math.max(1, opts.strikeCount || 80);
  const ct = (opts.contractType || 'BOTH').toUpperCase();
  const wantCall = ct === 'BOTH' || ct === 'CALL';
  const wantPut  = ct === 'BOTH' || ct === 'PUT';

  const token = await getTastyAccessToken(env);
  const hdr = tastyHeaders({ Authorization: `Bearer ${token}` });

  // 1) Nested chain (structure) + index spot in parallel
  const [nestedResp, idxResp] = await Promise.all([
    fetch(`${TASTY_BASE}/option-chains/${encodeURIComponent(root)}/nested`, { headers: hdr }),
    fetch(`${TASTY_BASE}/market-data/Index/SPX`, { headers: hdr }),
  ]);
  if (!nestedResp.ok) {
    const txt = await nestedResp.text();
    throw new Error(`Tasty nested chain HTTP ${nestedResp.status}: ${txt.slice(0, 200)}`);
  }
  const nestedJson = await nestedResp.json();
  const item0 = nestedJson?.data?.items?.[0];
  if (!item0) throw new Error('Tasty nested chain: empty items[]');

  let spot = null;
  if (idxResp.ok) {
    try {
      const idxJson = await idxResp.json();
      const d = idxJson?.data || {};
      const cand = d.last ?? d['last-price'] ?? d.mark ?? d.mid ?? d['close-price'];
      if (cand != null) spot = parseFloat(cand);
    } catch (_) {}
  }

  // 2) Filter expirations (optional) + nearest-N strikes (around spot if known)
  const expirations = (item0.expirations || []).filter(e => !wantExp || wantExp.has(e['expiration-date']));
  const slots = [];          // [{expKey, strikes: [{strike, callSym, putSym}]}]
  const allSyms = [];
  for (const e of expirations) {
    const expDate = e['expiration-date'];
    const dte = e['days-to-expiration'];
    const expKey = `${expDate}:${dte}`;
    let strikes = e.strikes || [];
    if (Number.isFinite(spot) && strikes.length > strikeCount) {
      strikes = strikes.slice().sort((a, b) =>
        Math.abs(parseFloat(a['strike-price']) - spot) -
        Math.abs(parseFloat(b['strike-price']) - spot)
      ).slice(0, strikeCount);
    }
    const items = [];
    for (const s of strikes) {
      const strike = parseFloat(s['strike-price']);
      const it = { strike, callSym: wantCall ? s.call : null, putSym: wantPut ? s.put : null };
      items.push(it);
      if (it.callSym) allSyms.push(it.callSym);
      if (it.putSym)  allSyms.push(it.putSym);
    }
    slots.push({ expKey, items });
  }

  // 3) Batch-fetch quotes via /market-data?symbols=sym1,sym2,... (comma-batch).
  // Chunks of 100 symbols, capped concurrency to avoid Tasty rate limits.
  const quoteMap = {};
  const CHUNK = 100;
  const MAX_PAR = 5;
  const batches = [];
  for (let i = 0; i < allSyms.length; i += CHUNK) batches.push(allSyms.slice(i, i + CHUNK));
  for (let i = 0; i < batches.length; i += MAX_PAR) {
    const wave = batches.slice(i, i + MAX_PAR);
    const results = await Promise.all(wave.map(async b => {
      const url = `${TASTY_BASE}/market-data?symbols=${encodeURIComponent(b.join(','))}`;
      const r = await fetch(url, { headers: hdr });
      if (!r.ok) {
        const t = await r.text();
        throw new Error(`Tasty batch-quote HTTP ${r.status}: ${t.slice(0, 160)}`);
      }
      return r.json();
    }));
    for (const r of results) for (const it of (r?.data?.items || [])) if (it.symbol) quoteMap[it.symbol] = it;
  }

  // 4) Assemble Schwab-compatible callExpDateMap / putExpDateMap
  const callExpDateMap = {};
  const putExpDateMap  = {};
  const num = v => (v == null || v === '') ? 0 : parseFloat(v);
  const mkContract = (q, strike, putCall) => ({
    putCall,
    symbol: q.symbol,
    bid: num(q.bid),
    ask: num(q.ask),
    last: num(q.last),
    mark: num(q.mark ?? q.mid),
    bidSize: num(q['bid-size']),
    askSize: num(q['ask-size']),
    strikePrice: strike,
    volatility: num(q.volatility) * 100,   // Schwab uses percent; Tasty decimal
    delta: num(q.delta),
    gamma: num(q.gamma),
    theta: num(q.theta),
    vega: num(q.vega),
    rho: num(q.rho),
    openInterest: 0,   // not exposed via Tasty REST
    totalVolume: 0,    // not exposed via Tasty REST
    _tastySource: true,
  });
  let mapped = 0, missing = 0;
  for (const slot of slots) {
    for (const it of slot.items) {
      const sk = it.strike.toFixed(1);   // Schwab key format "7400.0"
      if (it.callSym) {
        const q = quoteMap[it.callSym];
        if (q) {
          if (!callExpDateMap[slot.expKey]) callExpDateMap[slot.expKey] = {};
          (callExpDateMap[slot.expKey][sk] = callExpDateMap[slot.expKey][sk] || []).push(mkContract(q, it.strike, 'CALL'));
          mapped++;
        } else missing++;
      }
      if (it.putSym) {
        const q = quoteMap[it.putSym];
        if (q) {
          if (!putExpDateMap[slot.expKey]) putExpDateMap[slot.expKey] = {};
          (putExpDateMap[slot.expKey][sk] = putExpDateMap[slot.expKey][sk] || []).push(mkContract(q, it.strike, 'PUT'));
          mapped++;
        } else missing++;
      }
    }
  }

  return {
    spot,
    underlyingPrice: spot,     // Schwab also exposes this top-level alias
    callExpDateMap,
    putExpDateMap,
    fetchedAt: Date.now(),
    _source: 'tastytrade',
    _stats: { expirations: slots.length, symsRequested: allSyms.length, mapped, missing },
  };
}

// ════════════════════════════════════════════════════════════════════

async function handleEOD(env, etNow) {
  const todayISO = `${etNow.getFullYear()}-${String(etNow.getMonth()+1).padStart(2,'0')}-${String(etNow.getDate()).padStart(2,'0')}`;
  let token = null;
  try { token = await getAccessToken(env); } catch(e) { console.warn('[proxy]', e.message || e); }

  const end = Date.now();
  const start = end - 3 * 24 * 60 * 60 * 1000;
  const todayStr = etNow.toDateString();

  // Fetch VIX close — Tastytrade PRIMARY (gives true 4:15 PM official close),
  // Schwab fallback (1-min data stops at 4:00 PM so it's only ever 4:00 value).
  // Tastytrade's prev-close field reflects the previous trading day's official
  // close. Call NEXT morning to capture today's close — or use today's `last`
  // if calling at EOD before next-day refresh.
  let vixClose = null;
  try {
    const tastyVix = await tastyGetVix(env);
    const prevClose = parseFloat(tastyVix?.raw?.['prev-close']);
    const prevDate  = tastyVix?.raw?.['prev-close-date'];
    // If Tasty's prev-close-date == today's ISO, that IS today's official close.
    // (Tasty rolls prev-close at next-day open, so this branch fires when worker
    // runs the day-after-EOD reconciliation cron.)
    if (prevDate === todayISO && Number.isFinite(prevClose)) {
      vixClose = parseFloat(prevClose.toFixed(2));
    } else if (Number.isFinite(parseFloat(tastyVix?.raw?.last))) {
      // Same-day EOD path: use Tasty's current `last` near 4:15. May be 4:14
      // tick instead of the true 4:15 settle, but closer than Schwab 4:00.
      vixClose = parseFloat(parseFloat(tastyVix.raw.last).toFixed(2));
    }
  } catch (e) { console.warn('[proxy] tasty vixClose err:', e.message || e); }
  // Schwab fallback if Tasty failed (1-min data cuts off at 4:00 PM so this is
  // a "better than null" value, not the official 4:15).
  if (vixClose === null) {
    try {
      const vixHist = await fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24VIX&periodType=day&period=3&frequencyType=minute&frequency=1&startDate=${start}&endDate=${end}&needExtendedHoursData=true`, token);
      if (vixHist.candles) {
        const todayCandles = vixHist.candles.filter(c => toET(new Date(c.datetime)).toDateString() === todayStr);
        todayCandles.sort((a, b) => a.datetime - b.datetime);
        const closeCandle = todayCandles.slice().reverse().find(c => {
          const d = toET(new Date(c.datetime));
          return d.getHours() * 60 + d.getMinutes() <= 16 * 60 + 15;
        });
        if (closeCandle) vixClose = parseFloat(closeCandle.close.toFixed(2));
      }
      if (vixClose === null) {
        const q = await fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/quotes?symbols=%24VIX&fields=quote`, token);
        const cp = q?.['$VIX']?.quote?.closePrice;
        if (cp) vixClose = parseFloat(cp.toFixed(2));
      }
    } catch (e) { console.warn('[proxy] schwab vixClose err:', e.message || e); }
  }

  // Fetch SPX close
  let spxClose = null;
  try {
    const spxHist = await fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24SPX&periodType=day&period=3&frequencyType=minute&frequency=1&startDate=${start}&endDate=${end}&needExtendedHoursData=true`, token);
    if (spxHist.candles) {
      const todayCandles = spxHist.candles.filter(c => toET(new Date(c.datetime)).toDateString() === todayStr);
      todayCandles.sort((a, b) => a.datetime - b.datetime);
      const closeCandle = todayCandles.slice().reverse().find(c => {
        const d = toET(new Date(c.datetime));
        return d.getHours() * 60 + d.getMinutes() <= 16 * 60 + 15;
      });
      if (closeCandle) spxClose = parseFloat(closeCandle.close.toFixed(2));
    }
    if (spxClose === null) {
      const q = await fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/quotes?symbols=%24SPX&fields=quote`, token);
      const cp = q?.['$SPX']?.quote?.closePrice;
      if (cp) spxClose = parseFloat(cp.toFixed(2));
    }
  } catch (e) { console.warn('[proxy]', e.message || e); }

  // Stooq fallback for SPX close when Schwab tokens are expired
  if (spxClose === null) {
    try {
      const todayISO2 = `${etNow.getFullYear()}-${String(etNow.getMonth()+1).padStart(2,'0')}-${String(etNow.getDate()).padStart(2,'0')}`;
      spxClose = await getSpxCloseForDate(todayISO2);
    } catch (e) { console.warn('[proxy]', e.message || e); }
  }

  // Compute m8bfPL directly from today's signals + spxClose (no live_updater dependency)
  let m8bfPL = null;
  let m8bfWR = null;

  // Re-scrape all Discord signals for today (full pagination) to ensure completeness
  // The live KV polling may have missed signals if cron was down
  let fullSigs = [];
  if (env.DISCORD_USER_TOKEN) {
    try {
      fullSigs = await fetchAllDiscordSignalsForDate(env.DISCORD_USER_TOKEN, '1048242197029458040', todayISO);
      if (fullSigs.length > 0) {
        const signals = fullSigs.map(s => ({
          time: s.time, center: s.center, lower: s.lower, upper: s.upper,
          t1: s.t1, premium: s.premium, cp: s.cp ?? 0, banned: isBanned(s.center, s.lower, s.t1),
        }));
        await env.SIGNAL_KV.put('signals_today', JSON.stringify({ date: todayISO, signals }));
        console.log(`[eod] Re-scraped ${signals.length} signals for ${todayISO}`);
      }
    } catch (e) { console.warn('[eod] signal re-scrape:', e.message); }
  }

  // Compute m8bfWR = win rate across ALL signals posted today (from KV)
  if (spxClose != null) {
    try {
      const kvRaw = await env.SIGNAL_KV.get('signals_today');
      if (kvRaw) {
        const kv = JSON.parse(kvRaw);
        if (kv.date === todayISO && Array.isArray(kv.signals) && kv.signals.length > 0) {
          m8bfWR = computeWinRateFromSignals(kv.signals, spxClose);
        }
      }
    } catch (e) { console.warn('[proxy]', e.message || e); }
  }

  // Determine if today is a SKIP day per the live signal logic.
  // m8bfBlockedByLive == true means: live system would NOT trade M8BF today (calendar
  // blocks, gap, o2o, 0% rule, etc.). In that case the backtester must also skip,
  // so we set m8bfPL = 0 and add the date to M8BF_SKIP after appending trades.
  let m8bfBlockedByLive = false;

  // FIX (2026-06-09 audit P0 #3): both STEP 1 and STEP 2 used to ignore the
  // 90%-WR override in signal-engine.js. On a day where prevWR≥90 falls on
  // EOM/EOM-1/OPEX-1/NM-non-Mon, the live bot DID fire M8BF — but EOD silently
  // wrote m8bfPL=0 and added the date to M8BF_SKIP, corrupting the ledger.
  //
  // STEP 1 now DEFERS to STEP 2 when the 90% override is possible. STEP 2
  // calls calculateSignal with prevWR and trusts sig.theme === "m8bf"
  // (which incorporates the 90% override AND the GXBF exclusion).
  let calendarWouldBlock = false;
  let ninetyOverridePossible = false;
  let calendarBlockReason = '';
  {
    const eomDay = isEomN(0, etNow);
    const eom1 = isEomN(1, etNow);
    const opex1 = opexSch.some(ds => isTodayBefore(ds, etNow));
    const vixExpAfterOpex = isVixAfterOpexDay(etNow);
    const nonAmznTslaEarn = isNonAmznTslaEarningsDay(etNow);
    const cpiDay = cpiSch.includes(todayLong(etNow));
    const nmDay = isFirstTradeMo(etNow);
    const nmMon = isFirstTradeMon(etNow);
    const nmNonMon = nmDay && !nmMon;
    calendarWouldBlock = eomDay || eom1 || opex1 || vixExpAfterOpex || nonAmznTslaEarn || cpiDay || nmNonMon;
    calendarBlockReason = `eom=${eomDay}, eom-1=${eom1}, opex-1=${opex1}, vixAfterOpex=${vixExpAfterOpex}, earn=${nonAmznTslaEarn}, cpi=${cpiDay}, nm-non-mon=${nmNonMon}`;

    if (calendarWouldBlock) {
      // Look up prior m8bfWR. If ≥ 90 and not CPI day, the 90% override may
      // force M8BF (GXBF firing would suppress this — STEP 2 has VIX to check).
      try {
        const hist_q = await getHistory(env);
        const prevWREntry = (Array.isArray(hist_q) ? hist_q : [])
          .filter(e => e.date < todayISO && e.m8bfWR != null)
          .sort((a, b) => b.date.localeCompare(a.date))[0];
        const prevWR_q = prevWREntry ? parseFloat(prevWREntry.m8bfWR) : null;
        if (prevWR_q != null && prevWR_q >= 90 && !cpiDay) {
          ninetyOverridePossible = true;
        }
      } catch (e) { console.warn('[eod] prevWR lookup for 90% override:', e.message); }

      if (!ninetyOverridePossible) {
        m8bfBlockedByLive = true;
        console.log(`[eod] M8BF blocked by calendar (${calendarBlockReason})`);
      } else {
        console.log(`[eod] Calendar would block M8BF (${calendarBlockReason}) — 90% override possible, deferring to STEP 2`);
      }
    }
  }

  // STEP 2 — gap/o2o/signal-based bans (need vixOpen/spxOpen, only run if morning wrote them).
  // Also resolves the 90% override deferred from STEP 1.
  if (!m8bfBlockedByLive) {
    try {
      const hist0 = await getHistory(env);
      if (Array.isArray(hist0) && hist0.length) {
        const todayE = hist0.find(e => e.date === todayISO);
        const prior = hist0
          .filter(e => e.date < todayISO && e.vixClose != null && e.vixOpen != null)
          .sort((a, b) => b.date.localeCompare(a.date))[0];
        const prevWREntry = hist0
          .filter(e => e.date < todayISO && e.m8bfWR != null)
          .sort((a, b) => b.date.localeCompare(a.date))[0];
        if (todayE && prior && todayE.vixOpen != null) {
          const spxGapPct = (todayE.spxOpen != null && prior.spxClose != null)
            ? ((todayE.spxOpen - prior.spxClose) / prior.spxClose) * 100
            : null;
          const prevWR_s2 = prevWREntry ? parseFloat(prevWREntry.m8bfWR) : null;
          const sig = calculateSignal({
            vixToday: todayE.vixOpen,
            vixYOpen: prior.vixOpen,
            vixYClose: prior.vixClose,
            spxGapPct,
            etDate: etNow,
            prevWR: prevWR_s2,
          });
          // FIX (2026-06-09 audit P0 #3): m8bfBanned alone misses the 90%-WR
          // override. The override applies when prevWR >= 90, not CPI, and
          // GXBF didn't take precedence (sig.theme !== 'gxbf' — equivalent to
          // signal-engine.js's !rec.includes("GXBF") check). When override
          // applies, M8BF fires even on a calendar-banned day.
          const gxbfTookPrecedence = (sig.theme === 'gxbf');
          const ninetyOverrideFires = (prevWR_s2 != null && prevWR_s2 >= 90 && !sig.cpiDay && !gxbfTookPrecedence);
          const m8bfWouldNotFire = (sig.m8bfBanned || sig.cpiDay) && !ninetyOverrideFires;
          if (m8bfWouldNotFire) {
            m8bfBlockedByLive = true;
            console.log(`[eod] M8BF blocked: m8bfBanned=${sig.m8bfBanned}, cpiDay=${sig.cpiDay}, 90%override=${ninetyOverrideFires}, gxbfTook=${gxbfTookPrecedence}`);
          } else if (ninetyOverrideFires && calendarWouldBlock) {
            console.log(`[eod] 90% override CONFIRMED — calendar block (${calendarBlockReason}) overridden (prevWR=${prevWR_s2})`);
          }
        } else if (ninetyOverridePossible && calendarWouldBlock) {
          // No VIX data yet. 90% override was POSSIBLE but unverifiable.
          // Be conservative: leave m8bfBlockedByLive=false (the qualifying-
          // signal-in-window check below only fires if a real signal was
          // scraped, so we won't fabricate an M8BF P/L on a quiet day).
          console.log(`[eod] No VIX data, 90% override possible — leaving m8bfBlockedByLive=false`);
        }
      }
    } catch (e) { console.warn('[eod] live signal check:', e.message); }
  }

  // Compute m8bfPL from first qualifying signal in window (same logic as backfillMissingPL)
  if (m8bfBlockedByLive) {
    m8bfPL = 0;
  } else if (spxClose != null && fullSigs.length > 0) {
    try {
      const dow = etNow.getDay();
      const win = getM8BFWindow(dow, todayISO);
      if (win) {
        const [winLo, winHi] = win;
        // Honor the manual-cancellation skip list (same KV used by
        // selectM8bfQualifying). Without this, EOD would compute P&L for a
        // signal the user explicitly cancelled — observed 2026-05-22 where
        // EOD recorded m8bfPL from the cancelled 13:02 trade (-$363) instead
        // of the 13:06 trade actually held (-$1,311).
        let skipTimes = new Set();
        try {
          const skipRaw = await env.SIGNAL_KV.get(`m8bf_skip_signals_${todayISO}`);
          if (skipRaw) skipTimes = new Set(JSON.parse(skipRaw) || []);
        } catch (_) { /* no-op */ }

        let qualifying = null;
        for (const sig of fullSigs) {
          if (!sig.time) continue;
          if (skipTimes.has(sig.time)) continue;   // ← manual cancellation
          const [h, m] = sig.time.split(':').map(Number);
          const mins = h * 60 + m;
          if (mins >= winLo && mins < winHi && !isBanned(sig.center, sig.lower, sig.t1)) {
            qualifying = sig;
            break;
          }
        }
        if (qualifying) {
          const lo = qualifying.lower, hi = qualifying.upper;
          const wing = (hi - lo) / 2;
          const intrinsic = Math.max(0, Math.min(spxClose - lo, hi - spxClose));
          const clipped = Math.min(intrinsic, wing);
          m8bfPL = Math.round((clipped - qualifying.premium) * 100);
          console.log(`[eod] m8bfPL computed: $${m8bfPL} (center=${qualifying.center}, premium=${qualifying.premium}, spxClose=${spxClose})`);
        } else {
          // No qualifying signal in window — also a skip day for the backtester
          m8bfPL = 0;
          m8bfBlockedByLive = true;
          console.log('[eod] No qualifying signal in window — marking as skip day');
        }
      }
    } catch (e) { console.warn('[eod] m8bfPL compute:', e.message); }
  }

  // Backfill vixOpen/spxOpen if morning signal missed them
  let vixOpen = null, spxOpen = null;
  try {
    const hist = await getHistory(env);
    if (Array.isArray(hist) && hist.length) {
      const todayEntry = hist.find(e => e.date === todayISO);
      if (todayEntry && todayEntry.vixOpen == null && token) {
        // Fetch VIX open from candles
        try {
          const vixHist2 = await fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24VIX&periodType=day&period=3&frequencyType=minute&frequency=1&startDate=${start}&endDate=${end}&needExtendedHoursData=true`, token);
          if (vixHist2.candles) {
            const openCandles = vixHist2.candles.filter(c => {
              const d = toET(new Date(c.datetime));
              return d.toDateString() === todayStr && d.getHours() === 9 && d.getMinutes() >= 30 && d.getMinutes() <= 35;
            }).sort((a, b) => a.datetime - b.datetime);
            if (openCandles.length) vixOpen = parseFloat(openCandles[0].open.toFixed(2));
          }
        } catch (e) { console.warn('[eod] vixOpen backfill:', e.message); }
      }
      if (todayEntry && todayEntry.spxOpen == null && token) {
        try {
          const spxQ = await fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/quotes?symbols=%24SPX&fields=quote`, token);
          const op = spxQ?.['$SPX']?.quote?.openPrice;
          if (op) spxOpen = parseFloat(op.toFixed(2));
        } catch (e) { console.warn('[eod] spxOpen backfill:', e.message); }
      }
    }
  } catch (e) { console.warn('[eod] open backfill check:', e.message); }

  const fields = {};
  if (vixClose != null) fields.vixClose = vixClose;
  if (spxClose != null) fields.spxClose = spxClose;
  if (m8bfPL != null) fields.m8bfPL = m8bfPL;
  if (m8bfWR != null) fields.m8bfWR = m8bfWR;
  if (vixOpen != null) fields.vixOpen = vixOpen;
  if (spxOpen != null) fields.spxOpen = spxOpen;

  // cor1m from the worker's own cloud capture (2026-06-09) — fills the
  // history column with zero dependence on the user's Mac. Upsert merge
  // only fills when null, so a LaunchAgent-written value is never clobbered.
  try {
    const kvCor = await env.SIGNAL_KV.get(`cor1m_open_${todayISO}`);
    if (kvCor) {
      const o = JSON.parse(kvCor);
      if (o.cor1m != null) fields.cor1m = o.cor1m;
    }
  } catch (_) { /* non-critical */ }

  // wroteFields tells callers whether history_data.json was actually updated.
  // Callers use this to decide if they should set `eod_done_<date>` — we only
  // want to set it after a real write, otherwise a failed EOD (expired Schwab
  // token + Stooq hiccup) locks out every later retry for the day.
  let wroteFields = false;
  if (Object.keys(fields).length > 0) {
    await upsertHistoryGitHub(env, todayISO, fields);
    wroteFields = true;
  }

  // Settle the live-tracked straddle (if any) at SPX close → writes stradPL.
  // upsertHistoryGitHub conditional-overwrite means it won't clobber a manually-
  // set stradPL — only fills if currently null.
  if (spxClose != null) {
    try {
      const stradResult = await settleStraddleEOD(env, etNow, spxClose);
      console.log('[strad] EOD settle:', JSON.stringify(stradResult));
    } catch (e) { console.warn('[strad] EOD settle failed:', e.message); }
    try {
      const bobfResult = await settleBobfEOD(env, etNow, spxClose);
      console.log('[bobf] EOD settle:', JSON.stringify(bobfResult));
    } catch (e) { console.warn('[bobf] EOD settle failed:', e.message); }
    try {
      const gxbfResult = await settleGxbfEOD(env, etNow, spxClose);
      console.log('[gxbf] EOD settle:', JSON.stringify(gxbfResult));
    } catch (e) { console.warn('[gxbf] EOD settle failed:', e.message); }
    try {
      const tailResult = await settleTailEOD(env, etNow, spxClose);
      console.log('[tail] EOD settle:', JSON.stringify(tailResult));
    } catch (e) { console.warn('[tail] EOD settle failed:', e.message); }
  }

  // Append today's signals to TRADES database in backtester.html (reuse cached fullSigs)
  let appendResult = { appended: 0 };
  if (spxClose != null && fullSigs.length > 0) {
    try {
      appendResult = await appendTradesToBacktester(env, todayISO, etNow, fullSigs, spxClose, m8bfBlockedByLive);
    } catch (e) {
      appendResult = { appended: 0, error: e.message };
    }
  }

  return { status: 'eod', date: todayISO, vixClose, spxClose, m8bfPL, wroteFields, trades: appendResult };
}

// ════════════════════════════════════════════════════════════════════
// DISCORD SIGNAL POLLING
// ════════════════════════════════════════════════════════════════════

function parseDiscordSignal(content) {
  // Format: BUY +1 Butterfly SPX 100 ... 6455/6405/6355 CALL @14.25 LMT
  // Strikes are typically posted high→low for CALLs and low→high for PUTs.
  // FIX (2026-06-09 audit P0 #8): NORMALIZE strike ordering so that
  // lower < center < upper regardless of CALL/PUT post format. Without this,
  // PUT butterflies posted low→high (e.g. 6355/6405/6455 PUT) produced
  // upper=6355 < lower=6455, and the P/L formula
  //   max(0, min(spxClose - lower, upper - spxClose))
  // returned 0 intrinsic for ANY spxClose between strikes → silent
  // m8bfPL = -premium*100 regardless of actual outcome.
  const strikeMatch = content.match(/BUY \+1 Butterfly SPX[^/]*(\d{4,5})\/(\d{4,5})\/(\d{4,5})\s*(CALL|PUT)\s*@([\d.]+)/i);
  if (!strikeMatch) return null;
  const s1 = parseInt(strikeMatch[1]);
  const s2 = parseInt(strikeMatch[2]);
  const s3 = parseInt(strikeMatch[3]);
  const cpStr = (strikeMatch[4] || 'CALL').toUpperCase();
  const cp = cpStr === 'PUT' ? 1 : 0; // 0=CALL, 1=PUT
  const premium = parseFloat(strikeMatch[5]);
  if (isNaN(s1) || isNaN(s2) || isNaN(s3) || isNaN(premium)) return null;
  // Sort to canonical order: lower < center < upper. Wing structure is
  // symmetric around center for a standard butterfly, but defensive sort
  // handles edge cases (asymmetric flies, mis-typed posts) too.
  const [lower, center, upper] = [s1, s2, s3].sort((a, b) => a - b);
  // T1 from "Target 1: XXXX"
  const t1Match = content.match(/Target\s*1[:\s]+(\d{4,5})/i);
  const t1 = t1Match ? parseInt(t1Match[1]) : center + 5;
  return { center, upper, lower, t1, premium, cp };
}

// M8BF banned-strike check.
//
// FULL ban: center % 100 ∈ {10, 25, 35, 40, 65, 80}.
// COMBO ban: M8BF_COMBO_BANS[t1 % 100] === center % 100  → BANNED.
//   COMBO_BANS = { 0:95, 20:15, 55:50, 65:60, 85:90 }
//   t1 is the Discord "Target 1" field (from `Target 1: XXXX` in the post),
//   distinct from `lower` (= center − wing). The rule is keyed off t1's last
//   two digits, NOT lower's and NOT (center − lower).
//
// Example (today's 09:36 signal): center=7495 (cmod=95), t1=7500 (t1mod=0).
//   COMBO_BANS[0] === 95 → BANNED. With the previous (center−lower) keying
//   this got `spread%100 = 50`, missed the ban, and let the trade through.
//
// Reference unit-test cases:
//   isBanned(7495, t1=7500) === true   (t1%100=0, COMBO_BANS[0]=95, center%100=95)
//   isBanned(7395, t1=7400) === true   (same combo)
//   isBanned(7395, t1=7355) === false  (t1%100=55, COMBO_BANS[55]=50, center%100=95 ≠ 50)
function isBanned(center, lower, t1) {
  const FULL_BANS = new Set([10, 25, 35, 40, 65, 80]);
  const COMBO_BANS = { 0: 95, 20: 15, 55: 50, 65: 60, 85: 90 };
  if (FULL_BANS.has(center % 100)) return true;
  if (t1 != null) {
    const t1Mod = ((t1 % 100) + 100) % 100;
    if (COMBO_BANS[t1Mod] !== undefined && center % 100 === COMBO_BANS[t1Mod]) return true;
  }
  return false;
}

async function pollDiscordSignals(env) {
  const token = env.DISCORD_USER_TOKEN;
  const channelId = '1048242197029458040';
  if (!token) return { polled: false, reason: 'no token' };

  const etNow = toET(new Date());
  const todayISO = `${etNow.getFullYear()}-${String(etNow.getMonth()+1).padStart(2,'0')}-${String(etNow.getDate()).padStart(2,'0')}`;

  // Load existing signals from KV
  const existingRaw = await env.SIGNAL_KV.get('signals_today');
  const existing = existingRaw ? JSON.parse(existingRaw) : { date: '', signals: [] };

  let signals = [];
  let afterId = null;

  if (existing.date === todayISO) {
    signals = existing.signals || [];
    afterId = await env.SIGNAL_KV.get('discord_last_msg_id');
  }

  // First poll of the day — start from midnight UTC today
  if (!afterId) {
    const midnight = new Date(); midnight.setUTCHours(0, 0, 0, 0);
    const discordEpoch = 1420070400000n;
    afterId = ((BigInt(midnight.getTime()) - discordEpoch) << 22n).toString();
  }

  const apiUrl = `https://discord.com/api/v9/channels/${channelId}/messages?limit=100&after=${afterId}`;
  let messages;
  try {
    const resp = await fetch(apiUrl, {
      headers: {
        'Authorization': token,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      },
    });
    if (!resp.ok) return { polled: false, status: resp.status };
    messages = await resp.json();
    if (!Array.isArray(messages)) return { polled: false, reason: 'bad response' };
  } catch (e) {
    return { polled: false, error: e.message };
  }

  if (!messages.length) return { polled: true, newSignals: 0, total: signals.length };

  // Sort oldest → newest
  messages.sort((a, b) => a.id.localeCompare(b.id));

  // Save latest message ID
  await env.SIGNAL_KV.put('discord_last_msg_id', messages[messages.length - 1].id);

  const seenMsgIds = new Set(signals.map(s => s.msgId).filter(Boolean));
  let newCount = 0;

  for (const msg of messages) {
    if (seenMsgIds.has(msg.id)) continue;

    const msgET = toET(new Date(msg.timestamp));
    const msgISO = `${msgET.getFullYear()}-${String(msgET.getMonth()+1).padStart(2,'0')}-${String(msgET.getDate()).padStart(2,'0')}`;
    if (msgISO !== todayISO) continue;

    const sig = parseDiscordSignal(msg.content || '');
    if (!sig) continue;

    signals.push({
      time: `${String(msgET.getHours()).padStart(2,'0')}:${String(msgET.getMinutes()).padStart(2,'0')}`,
      center: sig.center,
      lower: sig.lower,
      upper: sig.upper,
      t1: sig.t1,
      premium: sig.premium,
      cp: sig.cp ?? 0,
      banned: isBanned(sig.center, sig.lower, sig.t1),
      msgId: msg.id,
    });
    seenMsgIds.add(msg.id);
    newCount++;
  }

  await env.SIGNAL_KV.put('signals_today', JSON.stringify({ date: todayISO, signals }));
  return { polled: true, newSignals: newCount, total: signals.length };
}

// ════════════════════════════════════════════════════════════════════
// DIAGONAL TRADE HANDLER (live)
// Fires at 12:30 ET each weekday: closes prior open trade, then opens
// a new one if signal-engine.js says so. State lives in KV at:
//   diagonal_open_trade  — the currently-active trade (one at a time)
//   diagonal_closed_log  — last 30 closed trades (for live page tape)
// Live page polls /diagonal-today which reads KV.
// Strikes: short = round5(spot+30), long = K_short - 40 (canonical 30/40).
// Expiries: short = next trading day, long = ~25 trading days out.
// ════════════════════════════════════════════════════════════════════

const DIAG_SHORT_OFFSET = 10;     // pts ITM (10 ITM — safer-tail config, 2026-06-09)
const DIAG_LONG_OFFSET  = 20;     // pts BELOW short (so 10 OTM relative to spot; width=20)
const DIAG_LONG_DTE     = 25;     // CALENDAR days target (matches Python long_dte=25)
const DIAG_LONG_DTE_TOL = 5;      // ±5 calendar days → 20-30 DTE acceptable range

function snap5(x) { return Math.round(x / 5) * 5; }

function isoDateET(d) {
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
}

function nextTradeDayET(etDate) {
  const d = new Date(etDate);
  d.setHours(12, 0, 0, 0);
  do { d.setDate(d.getDate() + 1); } while (d.getDay() === 0 || d.getDay() === 6 || isHol(d));
  return d;
}

function addTradeDaysET(etDate, n) {
  const d = new Date(etDate);
  d.setHours(12, 0, 0, 0);
  let added = 0;
  while (added < n) {
    d.setDate(d.getDate() + 1);
    if (d.getDay() === 0 || d.getDay() === 6) continue;
    if (isHol(d)) continue;
    added++;
  }
  return d;
}

// Look up a put quote at the requested strike. Schwab chain map keys are
// "YYYY-MM-DD:DTE"; strikes are dotted strings ("7290.0"). We tolerate
// ±5 strike fuzz so a missing exact strike falls back to the closest neighbor.
function pickPutFromChain(putExpDateMap, expISO, strike) {
  const targetKey = Object.keys(putExpDateMap).find(k => k.startsWith(expISO + ':'));
  if (!targetKey) return null;
  const strikes = putExpDateMap[targetKey];
  // Try exact, then ±5, ±10
  for (const offset of [0, -5, 5, -10, 10]) {
    const k = String(strike + offset).includes('.') ? String(strike + offset) : String(strike + offset) + '.0';
    if (strikes[k] && strikes[k][0]) {
      const q = strikes[k][0];
      return {
        strike: parseFloat(q.strikePrice ?? (strike + offset)),
        bid: q.bid,
        ask: q.ask,
        mid: (q.bid != null && q.ask != null) ? (q.bid + q.ask) / 2 : null,
        symbol: q.symbol,
        expirationDate: q.expirationDate,
        daysToExpiration: q.daysToExpiration,
      };
    }
  }
  return null;
}

// Fetch SPX put chains for a date-range covering both legs in one call.
// fromDate / toDate are YYYY-MM-DD. Returns {spot, putExpDateMap}.
// Tries Schwab first; falls back to Tasty if Schwab token is dead or call fails.
async function fetchSpxPutChain(token, fromDate, toDate, env) {
  if (token) {
    try {
      const params = new URLSearchParams({
        symbol: '$SPX',
        contractType: 'PUT',
        fromDate, toDate,
        strikeCount: '60',
        includeUnderlyingQuote: 'true',
        strategy: 'SINGLE',
      });
      const data = await fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/chains?${params}`, token, env);
      const spot = data.underlyingPrice || data.underlying?.last || data.underlying?.mark;
      return { spot, putExpDateMap: data.putExpDateMap || {}, _source: 'schwab' };
    } catch (e) {
      console.warn(`[fetchSpxPutChain] Schwab failed (${fromDate}..${toDate}) → Tasty fallback:`, e.message);
    }
  } else {
    console.warn(`[fetchSpxPutChain] no Schwab token (${fromDate}..${toDate}) → direct Tasty`);
  }
  // Tasty fallback: pull PUT chain across the date range.
  const exps = [];
  try {
    const start = new Date(fromDate + 'T12:00:00Z');
    const end = new Date(toDate + 'T12:00:00Z');
    for (let d = new Date(start); d <= end; d.setUTCDate(d.getUTCDate() + 1)) {
      exps.push(d.toISOString().slice(0, 10));
    }
  } catch (_) { exps.push(fromDate, toDate); }
  return tastyFetchSpxChain(env, { root: 'SPXW', strikeCount: 60, contractType: 'PUT', expirations: exps });
}

// Open a diagonal at 12:30 ET. Returns the trade record (or throws).
async function openDiagonalTrade(env, token, etNow, vixPct20d, preChain = null) {
  const todayISO = isoDateET(etNow);
  const shortExp = isoDateET(nextTradeDayET(etNow));
  // Calendar days, NOT trading days — matches Python long_dte=25.
  // Trading-day version pushed expiry ~10 calendar days too far (36 DTE
  // instead of 25-26 — observed 2026-05-07 picked Jun 12 instead of Jun 1).
  const _longTarget = new Date(etNow);
  _longTarget.setDate(_longTarget.getDate() + DIAG_LONG_DTE);
  const longExpTarget = isoDateET(_longTarget);

  // Fetch full chain spanning both expiries — reuse master if it covers them.
  const chain = await chainOrFetch(preChain, token, env, [shortExp, longExpTarget], 'PUT');
  const spot = chain.spot;
  const putExpDateMap = chain.putExpDateMap;
  if (!spot) throw new Error('Diagonal open: no spot price in chain response');

  const reqShort = snap5(spot + DIAG_SHORT_OFFSET);
  const shortLeg = pickPutFromChain(putExpDateMap, shortExp, reqShort);
  if (!shortLeg) throw new Error(`Diagonal open: no SPX ${reqShort}P @ ${shortExp} in chain`);
  const kShort = shortLeg.strike;            // actual filled short strike (post-fuzz)
  const kLong  = kShort - DIAG_LONG_OFFSET;  // re-anchor long on actual short

  // Long leg — find the actual expiry closest to target within tolerance
  let longLeg = null, longExpUsed = null;
  const candidateExps = Object.keys(putExpDateMap)
    .map(k => k.split(':')[0])
    .filter(d => d > shortExp);  // strictly later than short
  candidateExps.sort((a, b) => Math.abs(daysBetween(longExpTarget, a)) - Math.abs(daysBetween(longExpTarget, b)));
  for (const expISO of candidateExps) {
    const dteDiff = Math.abs(daysBetween(longExpTarget, expISO));
    if (dteDiff > DIAG_LONG_DTE_TOL) break;  // sorted, so done
    const candidate = pickPutFromChain(putExpDateMap, expISO, kLong);
    if (candidate && candidate.mid != null) { longLeg = candidate; longExpUsed = expISO; break; }
  }
  if (!longLeg) throw new Error(`Diagonal open: no long leg ~${kLong}P near ${longExpTarget}`);

  if (shortLeg.mid == null || longLeg.mid == null) throw new Error('Diagonal open: missing bid/ask on a leg');

  // Three pricings for the same diagonal:
  //   debit       = longMid  - shortMid  → theoretical fair value (mid-to-mid)
  //   askFill     = longAsk  - shortBid  → worst-case fill cost (BUY long at ask, SELL short at bid)
  //   bidExit     = longBid  - shortAsk  → worst-case close credit (SELL long at bid, BUY short at ask)
  // Real-world fills on SPX put diagonals typically land between debit and
  // askFill. The historical entryDebit field (mid-mid) is preserved as-is so
  // P&L math and prior records don't shift; askFill/bidExit are additive.
  const debit   = longLeg.mid - shortLeg.mid;
  const askFill = longLeg.ask - shortLeg.bid;
  const bidExit = longLeg.bid - shortLeg.ask;

  // Daily total-risk cap (2026-06-09): diagonal max loss = debit + width
  // (crash through both strikes inverts the spread — see index.html sizing).
  const diagWidth = kShort - longLeg.strike;
  const diagGate = await enforceRiskCap(env, etNow, 'diagonal', (debit + diagWidth) * 100);
  if (!diagGate.ok) throw new Error(`risk-cap blocked diagonal open: ${diagGate.reason}`);

  // FIX (2026-06-09 audit P0 #7): record ACTUAL filled long strike, not the
  // pre-fuzz target. pickPutFromChain() tolerates ±5-10 strike fuzz; if the
  // chain didn't have the exact target 7250P but had 7245P, longLeg.strike is
  // 7245 while kLong is still 7250. Storing kLong made the closer at line
  // ~1620 look up the wrong leg if the target strike came back into the chain
  // by close time. shortStrike already uses kShort = shortLeg.strike (line 1490)
  // — apply the same pattern to longStrike.
  const trade = {
    openDate: todayISO,
    openTimeET: `${String(etNow.getHours()).padStart(2,'0')}:${String(etNow.getMinutes()).padStart(2,'0')}`,
    spotEntry: parseFloat(spot.toFixed(2)),
    vixPct20d,
    shortStrike: kShort,
    longStrike: longLeg.strike,
    shortExp,
    longExp: longExpUsed,
    // shortDte was hard-coded to 1 — wrong on long weekends (Fri Memorial Day
    // open → Tue expiry is 4 calendar days, not 1). Use calendar-day diff so
    // shortDte mirrors longDte's semantics.
    shortDte: Math.abs(daysBetween(todayISO, shortExp)),
    longDte:  Math.abs(daysBetween(todayISO, longExpUsed)),
    shortSymbol: shortLeg.symbol,
    longSymbol: longLeg.symbol,
    entryShortMid: parseFloat(shortLeg.mid.toFixed(2)),
    entryLongMid: parseFloat(longLeg.mid.toFixed(2)),
    entryShortBid: shortLeg.bid,
    entryShortAsk: shortLeg.ask,
    entryLongBid: longLeg.bid,
    entryLongAsk: longLeg.ask,
    entryDebit: parseFloat(debit.toFixed(2)),
    entryAskFill: parseFloat(askFill.toFixed(2)),   // realistic worst-case fill
    entryBidExit: parseFloat(bidExit.toFixed(2)),   // realistic worst-case close
    contracts: 1,
    // Live fields — refreshed by every market-hours cron tick
    currentSpot: parseFloat(spot.toFixed(2)),
    currentShortMid: parseFloat(shortLeg.mid.toFixed(2)),
    currentLongMid: parseFloat(longLeg.mid.toFixed(2)),
    currentValue: parseFloat(debit.toFixed(2)),
    currentAskFill: parseFloat(askFill.toFixed(2)),
    currentBidExit: parseFloat(bidExit.toFixed(2)),
    currentPnl: 0,
    lastQuoteAt: new Date().toISOString(),
    status: 'open',
  };
  return trade;
}

// Refresh just the live-quote fields on the open trade. Cheap call.
async function refreshDiagonalLiveQuotes(env, token, preChain = null) {
  const raw = await env.SIGNAL_KV.get('diagonal_open_trade');
  if (!raw) return null;
  const trade = JSON.parse(raw);
  if (trade.status !== 'open') return null;

  // DEFENSIVE: phantom-trade cleanup. If today's close+open lifecycle already
  // ran (diag_done_<today> set) AND the open trade is from a prior day, the
  // delete in handleDiagonalTrade silently failed (CF KV occasionally drops
  // a write). Clear it now and bail — no quotes to refresh on a closed trade.
  const todayISO = isoDateET(toET());
  if (trade.openDate && trade.openDate < todayISO) {
    const diagDone = await env.SIGNAL_KV.get(`diag_done_${todayISO}`);
    if (diagDone) {
      console.warn(`[diag] phantom open trade detected (openDate=${trade.openDate}, diag_done set) — clearing`);
      await logEvent(env, 'warn', 'diag-phantom', 'phantom open trade cleared at refresh path',
                     { openDate: trade.openDate });
      await env.SIGNAL_KV.delete('diagonal_open_trade');
      return null;
    }
  }

  // Re-fetch the chain spanning both leg expiries — but reuse master chain
  // if it covers what we need (it almost always will).
  try {
    const chain = await chainOrFetch(preChain, token, env, [trade.shortExp, trade.longExp], 'PUT');
    const spot = chain.spot;
    const putExpDateMap = chain.putExpDateMap;
    const sNow = pickPutFromChain(putExpDateMap, trade.shortExp, trade.shortStrike);
    const lNow = pickPutFromChain(putExpDateMap, trade.longExp, trade.longStrike);
    if (!sNow || !lNow || sNow.mid == null || lNow.mid == null) return trade;
    trade.currentSpot = spot ? parseFloat(spot.toFixed(2)) : trade.currentSpot;
    trade.currentShortMid = parseFloat(sNow.mid.toFixed(2));
    trade.currentLongMid = parseFloat(lNow.mid.toFixed(2));
    trade.currentValue = parseFloat((lNow.mid - sNow.mid).toFixed(2));
    // Realistic fill estimates — what you'd actually pay to open / get to close
    // RIGHT NOW. Pulled from the same chain refresh as the mid values.
    if (sNow.ask != null && sNow.bid != null && lNow.ask != null && lNow.bid != null) {
      trade.currentAskFill = parseFloat((lNow.ask - sNow.bid).toFixed(2));
      trade.currentBidExit = parseFloat((lNow.bid - sNow.ask).toFixed(2));
    }
    trade.currentPnl = Math.round((trade.currentValue - trade.entryDebit) * 100 * trade.contracts);
    trade.lastQuoteAt = new Date().toISOString();
    await env.SIGNAL_KV.put('diagonal_open_trade', JSON.stringify(trade));
  } catch (e) {
    console.warn('[diag] refresh quotes failed:', e.message);
  }
  return trade;
}

// Close a trade at the current chain. Mutates input trade with close fields,
// returns the realized PnL.
async function closeDiagonalTrade(env, token, openTrade, etNow, preChain = null) {
  const closeISO = isoDateET(etNow);
  // For an expired short (closeISO >= shortExp), price intrinsic = max(K - SPX, 0)
  const chain = await chainOrFetch(preChain, token, env, [openTrade.shortExp, openTrade.longExp], 'PUT');
  const spot = chain.spot;
  const putExpDateMap = chain.putExpDateMap;

  let closeShortMid, closeLongMid;
  const sNow = pickPutFromChain(putExpDateMap, openTrade.shortExp, openTrade.shortStrike);
  const lNow = pickPutFromChain(putExpDateMap, openTrade.longExp, openTrade.longStrike);

  if (sNow && sNow.mid != null) {
    closeShortMid = sNow.mid;
  } else if (closeISO >= openTrade.shortExp && spot != null) {
    closeShortMid = Math.max(openTrade.shortStrike - spot, 0);  // expired intrinsic
  } else {
    throw new Error('Diagonal close: missing short leg quote');
  }
  if (lNow && lNow.mid != null) {
    closeLongMid = lNow.mid;
  } else {
    throw new Error('Diagonal close: missing long leg quote');
  }

  const closeValue = closeLongMid - closeShortMid;
  const pnl = (closeValue - openTrade.entryDebit) * 100 * openTrade.contracts;

  const closed = {
    ...openTrade,
    status: 'closed',
    closeDate: closeISO,
    closeTimeET: `${String(etNow.getHours()).padStart(2,'0')}:${String(etNow.getMinutes()).padStart(2,'0')}`,
    spotExit: spot ? parseFloat(spot.toFixed(2)) : null,
    closeShortMid: parseFloat(closeShortMid.toFixed(2)),
    closeLongMid: parseFloat(closeLongMid.toFixed(2)),
    closeValue: parseFloat(closeValue.toFixed(2)),
    pnl: Math.round(pnl),
  };
  return closed;
}

// Helper: integer days between two YYYY-MM-DD strings (calendar days).
function daysBetween(a, b) {
  const da = new Date(a + 'T12:00:00Z');
  const db = new Date(b + 'T12:00:00Z');
  return Math.round((db - da) / 86400000);
}

// Orchestrate close-then-open at 12:30 ET. Idempotent via diag_done_<date>.
async function handleDiagonalTrade(env, etNow, preChain = null) {
  const todayISO = isoDateET(etNow);
  const out = { date: todayISO, closed: null, opened: null, skipped: null };

  let token;
  try { token = await getAccessToken(env); }
  catch (e) { return { ...out, error: 'token: ' + e.message }; }

  // 1. Close prior open trade (if any & opened on a different day)
  const openRaw = await env.SIGNAL_KV.get('diagonal_open_trade');
  let openTrade = openRaw ? JSON.parse(openRaw) : null;

  if (openTrade && openTrade.openDate < todayISO && openTrade.status === 'open') {
    try {
      const closed = await closeDiagonalTrade(env, token, openTrade, etNow, preChain);
      // Commit diagPL for the OPEN date (matches history convention)
      await upsertHistoryGitHub(env, openTrade.openDate, { diagPL: closed.pnl });
      // Append to closed log
      const logRaw = await env.SIGNAL_KV.get('diagonal_closed_log');
      const log = logRaw ? JSON.parse(logRaw) : [];
      log.unshift(closed);
      await env.SIGNAL_KV.put('diagonal_closed_log', JSON.stringify(log.slice(0, 30)));
      // Clear the slot
      await env.SIGNAL_KV.delete('diagonal_open_trade');
      out.closed = { openDate: closed.openDate, closeDate: closed.closeDate, pnl: closed.pnl };
    } catch (e) {
      out.closeError = e.message;
      console.warn('[diag] close failed:', e.message);
      // Don't open a new trade if close failed — manual recovery needed
      return out;
    }
  }

  // 2. Compute today's vixPct20d for signal check (mirror handleScheduled logic)
  let vixPct20d = null;
  let vixToday = null;
  try {
    const histData = await getHistory(env);
    if (Array.isArray(histData) && histData.length) {
      const todayRow = histData.find(r => r.date === todayISO);
      vixToday = todayRow?.vixOpen != null ? parseFloat(todayRow.vixOpen) : null;
      // Same fallback as /diagonal-today: prior vixClose if today's vixOpen missing.
      if (vixToday == null) {
        const prior = histData
          .filter(r => r.date < todayISO && r.vixClose != null && r.vixClose > 0)
          .sort((a, b) => a.date.localeCompare(b.date));
        if (prior.length) vixToday = parseFloat(prior[prior.length - 1].vixClose);
      }
      const vix20 = histData
        .filter(r => r.date < todayISO && r.vixClose != null && r.vixClose > 0)
        .sort((a, b) => a.date.localeCompare(b.date))
        .slice(-20)
        .map(r => parseFloat(r.vixClose));
      // CANONICAL — see signal-engine.js computeVixPct20d (single source of
      // truth used by both this worker and the backtester).
      vixPct20d = computeVixPct20d(vixToday, vix20).pct;
    }
  } catch (e) { /* signal will be 'pending' if no vixPct20d */ }

  // 3a. Today's COR1M for the Diagonal gate (COR1M < 10 → no trade).
  //     CLOUD-FIRST (2026-06-09): the worker's own Schwab capture
  //     (cor1m_open_<date>, written ~9:30 ET) — machine-independent.
  //     Bundle fallback only if the capture is missing.
  let cor1mToday = null;
  try {
    const kvOpen = await env.SIGNAL_KV.get(`cor1m_open_${todayISO}`);
    if (kvOpen) {
      const o = JSON.parse(kvOpen);
      if (o.cor1m != null) cor1mToday = parseFloat(o.cor1m);
    }
  } catch (_) {}
  if (cor1mToday == null) {
    try {
      const r = await fetch('https://raw.githubusercontent.com/rava8989/brave/main/cor1m_contango_bundle.json',
        { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } });
      if (r.ok) {
        const bundle = await r.json();
        const todayRow = (bundle?.daily || []).find(d => d?.date === todayISO);
        if (todayRow?.cor1m != null) cor1mToday = parseFloat(todayRow.cor1m);
      }
    } catch (_) { /* leave null — signal-engine will defer with "pending COR1M data" */ }
  }

  // 3b. Compute diagonal signal (now COR1M-gated).
  const sig = computeDiagonalSignal(etNow, vixPct20d, cor1mToday);
  if (!sig.diagGo) {
    out.skipped = sig.diagSkipCode || 'no-data';
    out.signalText = sig.diagText;
    return out;
  }

  // 4. Open new trade
  try {
    const newTrade = await openDiagonalTrade(env, token, etNow, vixPct20d, preChain);
    await env.SIGNAL_KV.put('diagonal_open_trade', JSON.stringify(newTrade));
    out.opened = {
      openDate: newTrade.openDate,
      shortStrike: newTrade.shortStrike,
      longStrike: newTrade.longStrike,
      shortExp: newTrade.shortExp,
      longExp: newTrade.longExp,
      entryDebit: newTrade.entryDebit,
    };
  } catch (e) {
    out.openError = e.message;
    console.warn('[diag] open failed:', e.message);
  }

  return out;
}

// ════════════════════════════════════════════════════════════════════
// STRADDLE TRADE HANDLER (live)
// Lifecycle:
//   9:30-9:32 ET → if calculateSignal says theme==='strad', open trade
//      Fetch ATM 0DTE call+put, compute mid debit
//      If mid ≤ max_debit → fill at mid
//      Else → place working limit at max_debit, status='working'
//   Every 2-min market tick before 13:30 ET → refresh working orders.
//      If current mid ≤ max_debit → fill, status='filled'
//   13:30 ET → expire any still-working orders, status='expired'
//   16:15 ET (EOD) → if filled, compute pnl using SPX close, write stradPL
// Max debit: NM + plain Straddle = $32, EOM Straddle = $35.
// (Plain regular-day cap lowered from $35 → $32 on 2026-05-22 per user.)
// KV keys:
//   straddle_open_trade  — current trade or null
//   straddle_done_<date> — idempotency for EOD record
// ════════════════════════════════════════════════════════════════════

const STRADDLE_MAX_DEBIT_NM    = 32.00;  // NM Straddle: $3,200 max risk
const STRADDLE_MAX_DEBIT_EOM   = 35.00;  // EOM Straddle: $3,500 max risk
const STRADDLE_MAX_DEBIT_OTHER = 32.00;  // Plain regular-day Straddle: $3,200 max risk
const STRADDLE_WORK_CUTOFF_HR  = 13;     // 13:30 ET cutoff
const STRADDLE_WORK_CUTOFF_MIN = 30;

// Pick a call OR put quote at the requested strike from chain map.
function pickContractFromChain(expDateMap, expISO, strike) {
  const targetKey = Object.keys(expDateMap).find(k => k.startsWith(expISO + ':'));
  if (!targetKey) return null;
  const strikes = expDateMap[targetKey];
  for (const offset of [0, -5, 5, -10, 10]) {
    const k = String(strike + offset).includes('.') ? String(strike + offset) : String(strike + offset) + '.0';
    if (strikes[k] && strikes[k][0]) {
      const q = strikes[k][0];
      return {
        strike: parseFloat(q.strikePrice ?? (strike + offset)),
        bid: q.bid, ask: q.ask,
        mid: (q.bid != null && q.ask != null) ? (q.bid + q.ask) / 2 : null,
        symbol: q.symbol,
      };
    }
  }
  return null;
}

// Fetch full SPX option chain (call+put) for a single expiry. Used for
// straddle entry + monitoring. Mirrors GEX fetch pattern.
// Tries Schwab first; falls back to Tasty if Schwab token is dead or call fails.
async function fetchSpxFullChain(token, expDate, env) {
  if (token) {
    try {
      const baseParams = `symbol=%24SPX&strikeCount=20&fromDate=${expDate}&toDate=${expDate}&includeUnderlyingQuote=true&strategy=SINGLE`;
      const [callData, putData] = await Promise.all([
        fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/chains?${baseParams}&contractType=CALL`, token, env),
        fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/chains?${baseParams}&contractType=PUT`, token, env),
      ]);
      const spot = callData.underlyingPrice || callData.underlying?.last
                || putData.underlyingPrice  || putData.underlying?.last;
      return {
        spot,
        callExpDateMap: callData.callExpDateMap || {},
        putExpDateMap:  putData.putExpDateMap  || {},
        _source: 'schwab',
      };
    } catch (e) {
      console.warn(`[fetchSpxFullChain] Schwab failed (${expDate}) → Tasty fallback:`, e.message);
    }
  } else {
    console.warn(`[fetchSpxFullChain] no Schwab token (${expDate}) → direct Tasty`);
  }
  return tastyFetchSpxChain(env, { root: 'SPXW', strikeCount: 20, expirations: [expDate] });
}

// Master SPX chain — fetched ONCE per cron tick and passed to every handler
// (GEX, straddle, BOBF, diagonal). strikeCount=80 — UNCHANGED shared trading
// chain. Every strategy picks specific near-money strikes well within this band,
// so this is the live trade-execution feed and must not move for a GEX display
// tweak. (GEX's wider ±8% window/curve operate on whatever strikes this chain
// provides; a dedicated wide GEX fetch can be added later if needed.)
// No date range = Schwab returns all available expiries, 0DTE through ~30+ DTE.
async function fetchMasterSpxChain(token, env) {
  if (token) {
    try {
      const baseParams = 'symbol=%24SPX&strikeCount=80&includeUnderlyingQuote=true&strategy=SINGLE';
      const [callData, putData] = await Promise.all([
        fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/chains?${baseParams}&contractType=CALL`, token, env),
        fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/chains?${baseParams}&contractType=PUT`, token, env),
      ]);
      const spot = callData.underlyingPrice || callData.underlying?.last
                || putData.underlyingPrice  || putData.underlying?.last;
      return {
        spot,
        callExpDateMap: callData.callExpDateMap || {},
        putExpDateMap:  putData.putExpDateMap  || {},
        fetchedAt: Date.now(),
        _source: 'schwab',
      };
    } catch (e) {
      console.warn('[fetchMasterSpxChain] Schwab failed → Tasty fallback:', e.message);
    }
  } else {
    console.warn('[fetchMasterSpxChain] no Schwab token → direct Tasty');
  }
  return tastyFetchSpxChain(env, { root: 'SPXW', strikeCount: 80 });
}

// Returns a chain compatible with what each handler needs. If `preChain` has
// the requested expiries, reuse it (zero Schwab calls). Else falls through
// to a targeted fetch. Used by the diagonal/straddle/bobf live refreshers.
async function chainOrFetch(preChain, token, env, expectedExpiries, contractFilter = 'BOTH') {
  if (preChain) {
    const haveAll = expectedExpiries.every(exp => {
      const map = contractFilter === 'PUT' ? preChain.putExpDateMap : preChain.callExpDateMap;
      return Object.keys(map).some(k => k.startsWith(exp + ':'));
    });
    if (haveAll) return preChain;
  }
  // Fallback: fetch covering all needed expiries
  const min = expectedExpiries.reduce((a, b) => a < b ? a : b);
  const max = expectedExpiries.reduce((a, b) => a > b ? a : b);
  if (contractFilter === 'PUT') {
    return fetchSpxPutChain(token, min, max, env);
  }
  // For CALL/BOTH, do a master fetch (covers everything)
  return fetchMasterSpxChain(token, env);
}

function straddleMaxDebit(badge) {
  // badge = 'NM STRADDLE' | 'EOM STRADDLE' | 'STRADDLE'
  if (badge === 'NM STRADDLE')  return STRADDLE_MAX_DEBIT_NM;
  if (badge === 'EOM STRADDLE') return STRADDLE_MAX_DEBIT_EOM;
  return STRADDLE_MAX_DEBIT_OTHER;   // plain regular day
}

// Open or work a straddle. Called from the morning signal block once per day.
// `signal` is the calculateSignal result; we expect signal.theme === 'strad'.
async function openStraddleTrade(env, token, etNow, signal, preChain = null) {
  const todayISO = isoDateET(etNow);
  const expISO = todayISO;  // 0DTE — same-day expiry
  const chain = preChain || await fetchSpxFullChain(token, expISO, env);
  const { spot, callExpDateMap, putExpDateMap } = chain;
  if (!spot) throw new Error('Straddle open: no spot price in chain response');

  // Straddle CENTER = SPX OPEN (rounded to nearest 5), NOT spot-at-entry.
  // The morning signal block writes spxOpen to morning_signal_data_<today>
  // KV right before this runs (same handleScheduled invocation). Fall back
  // to history_data.json if KV is missing (e.g. /straddle-recovery path).
  // Final fallback to spot-at-entry preserves the old behavior if both
  // sources fail, but logs loudly so the bug is visible.
  let spxOpen = null, anchorSource = null;
  try {
    const msdRaw = await env.SIGNAL_KV.get(`morning_signal_data_${todayISO}`);
    if (msdRaw) {
      const msd = JSON.parse(msdRaw);
      if (msd.spxOpen != null && !isNaN(parseFloat(msd.spxOpen))) {
        spxOpen = parseFloat(msd.spxOpen);
        anchorSource = 'morning_signal_data KV';
      }
    }
  } catch (_) { /* fall through */ }
  if (spxOpen == null) {
    try {
      const hist = await getHistory(env);
      if (Array.isArray(hist) && hist.length) {
        const todayRow = hist.find(r => r.date === todayISO);
        if (todayRow?.spxOpen != null) {
          spxOpen = parseFloat(todayRow.spxOpen);
          anchorSource = 'history_data.json';
        }
      }
    } catch (_) { /* fall through */ }
  }
  if (spxOpen == null) {
    spxOpen = spot;
    anchorSource = 'spot-at-entry (FALLBACK — spxOpen missing)';
    console.warn(`[strad] spxOpen unavailable, using spot ${spot} as center anchor`);
  }
  const requestedK = snap5(spxOpen);
  console.log(`[strad] center anchor: spxOpen=${spxOpen} → strike=${requestedK} (source: ${anchorSource}, spot-now=${spot})`);

  // Find closest strike to spot that BOTH the call and put maps have, with
  // valid bid/ask on both. Walk outward from the requested strike in $5 steps.
  // Avoids the "fuzz lands call on 7355, put on 7350" mismatch that previously
  // threw an error (observed today 2026-05-08 — straddle never auto-opened).
  let strike = null, callLeg = null, putLeg = null;
  for (const offset of [0, -5, 5, -10, 10, -15, 15, -20, 20]) {
    const k = requestedK + offset;
    const c = pickContractFromChain(callExpDateMap, expISO, k);
    const p = pickContractFromChain(putExpDateMap,  expISO, k);
    if (c && p && c.strike === k && p.strike === k && c.mid != null && p.mid != null) {
      strike = k; callLeg = c; putLeg = p;
      break;
    }
  }
  if (!callLeg || !putLeg) {
    throw new Error(`Straddle open: no common strike with both legs near ${requestedK} for ${expISO}`);
  }

  // Long straddle: BUY call at ASK + BUY put at ASK (worst-case real fill).
  // mid-mid (debit) is the theoretical fair value — what the page shows.
  // Real cost to open is callAsk + putAsk; close credit is callBid + putBid.
  const debit   = callLeg.mid + putLeg.mid;
  const askFill = callLeg.ask + putLeg.ask;
  const bidExit = callLeg.bid + putLeg.bid;
  const maxDebit = straddleMaxDebit(signal.badge || 'STRADDLE');

  // Daily total-risk cap (2026-06-09): debit × 100 = this trade's max loss.
  const riskGate = await enforceRiskCap(env, etNow, 'straddle', debit * 100);
  if (!riskGate.ok) throw new Error(`risk-cap blocked straddle open: ${riskGate.reason}`);

  const trade = {
    openDate: todayISO,
    openTimeET: `${String(etNow.getHours()).padStart(2,'0')}:${String(etNow.getMinutes()).padStart(2,'0')}`,
    badge: signal.badge || 'STRADDLE',
    spotEntry: parseFloat(spot.toFixed(2)),
    strike,
    expDate: expISO,
    callSymbol: callLeg.symbol,
    putSymbol:  putLeg.symbol,
    entryCallMid: parseFloat(callLeg.mid.toFixed(2)),
    entryPutMid:  parseFloat(putLeg.mid.toFixed(2)),
    entryCallBid: callLeg.bid, entryCallAsk: callLeg.ask,
    entryPutBid:  putLeg.bid,  entryPutAsk:  putLeg.ask,
    entryDebit: parseFloat(debit.toFixed(2)),
    entryAskFill: parseFloat(askFill.toFixed(2)),
    entryBidExit: parseFloat(bidExit.toFixed(2)),
    maxDebit,
    contracts: 1,
    // Live fields
    currentSpot: parseFloat(spot.toFixed(2)),
    currentCallMid: parseFloat(callLeg.mid.toFixed(2)),
    currentPutMid:  parseFloat(putLeg.mid.toFixed(2)),
    currentValue:   parseFloat(debit.toFixed(2)),
    currentAskFill: parseFloat(askFill.toFixed(2)),
    currentBidExit: parseFloat(bidExit.toFixed(2)),
    currentPnl: 0,
    lastQuoteAt: new Date().toISOString(),
    // Status
    status: debit <= maxDebit ? 'filled' : 'working',
    fillDebit: debit <= maxDebit ? parseFloat(debit.toFixed(2)) : null,
    fillTimeET: debit <= maxDebit ? `${String(etNow.getHours()).padStart(2,'0')}:${String(etNow.getMinutes()).padStart(2,'0')}` : null,
    workingExpiry: `${todayISO} 13:30 ET`,
  };
  return trade;
}

// Refresh live mids on the open straddle. Also handles working→filled and
// working→expired transitions.
async function refreshStraddleLiveQuotes(env, token, etNow, preChain = null) {
  const raw = await env.SIGNAL_KV.get('straddle_open_trade');
  if (!raw) return null;
  const trade = JSON.parse(raw);
  if (trade.status === 'expired' || trade.status === 'closed') return trade;

  // Past cutoff and still working → expire
  const pastCutoff = etNow.getHours() > STRADDLE_WORK_CUTOFF_HR ||
                     (etNow.getHours() === STRADDLE_WORK_CUTOFF_HR && etNow.getMinutes() >= STRADDLE_WORK_CUTOFF_MIN);
  if (trade.status === 'working' && pastCutoff) {
    trade.status = 'expired';
    trade.expiredAt = new Date().toISOString();
    await env.SIGNAL_KV.put('straddle_open_trade', JSON.stringify(trade));
    return trade;
  }

  try {
    const chain = preChain || await fetchSpxFullChain(token, trade.expDate, env);
    const { spot, callExpDateMap, putExpDateMap } = chain;
    const c = pickContractFromChain(callExpDateMap, trade.expDate, trade.strike);
    const p = pickContractFromChain(putExpDateMap,  trade.expDate, trade.strike);
    if (!c || !p || c.mid == null || p.mid == null) return trade;
    const newDebit = c.mid + p.mid;
    trade.currentSpot   = spot ? parseFloat(spot.toFixed(2)) : trade.currentSpot;
    trade.currentCallMid = parseFloat(c.mid.toFixed(2));
    trade.currentPutMid  = parseFloat(p.mid.toFixed(2));
    trade.currentValue   = parseFloat(newDebit.toFixed(2));
    // Realistic live fill estimates — what you'd actually pay to open / get to
    // close RIGHT NOW. callAsk + putAsk for entry, callBid + putBid for exit.
    if (c.ask != null && c.bid != null && p.ask != null && p.bid != null) {
      trade.currentAskFill = parseFloat((c.ask + p.ask).toFixed(2));
      trade.currentBidExit = parseFloat((c.bid + p.bid).toFixed(2));
    }

    // Working → filled if price drops to the limit
    if (trade.status === 'working' && newDebit <= trade.maxDebit) {
      trade.status = 'filled';
      trade.fillDebit = parseFloat(newDebit.toFixed(2));
      trade.fillTimeET = `${String(etNow.getHours()).padStart(2,'0')}:${String(etNow.getMinutes()).padStart(2,'0')}`;
      // Re-snapshot leg fills at fill time
      trade.entryCallMid = parseFloat(c.mid.toFixed(2));
      trade.entryPutMid  = parseFloat(p.mid.toFixed(2));
      trade.entryDebit   = parseFloat(newDebit.toFixed(2));
      // Re-snapshot ask-fill / bid-exit at fill time too (matches diagonal)
      if (c.ask != null && c.bid != null && p.ask != null && p.bid != null) {
        trade.entryAskFill = parseFloat((c.ask + p.ask).toFixed(2));
        trade.entryBidExit = parseFloat((c.bid + p.bid).toFixed(2));
      }
    }

    // Live P&L only meaningful when filled
    if (trade.status === 'filled') {
      trade.currentPnl = Math.round((trade.currentValue - trade.entryDebit) * 100 * trade.contracts);
    }
    trade.lastQuoteAt = new Date().toISOString();
    await env.SIGNAL_KV.put('straddle_open_trade', JSON.stringify(trade));
  } catch (e) {
    console.warn('[strad] refresh failed:', e.message);
  }
  return trade;
}

// EOD: settle the straddle at SPX close. Records stradPL.
// `spxClose` provided by handleEOD (from Schwab quote).
async function settleStraddleEOD(env, etNow, spxClose) {
  const raw = await env.SIGNAL_KV.get('straddle_open_trade');
  if (!raw) return { status: 'no-trade' };
  const trade = JSON.parse(raw);
  if (trade.openDate !== isoDateET(etNow)) return { status: 'wrong-date', openDate: trade.openDate };
  if (trade.status === 'closed' || trade.status === 'expired') {
    // Working order that never filled → no trade, no P&L
    if (trade.status === 'expired') {
      // Still mark in history so the day shows "no trade" cleanly?
      // Per existing convention stradPL=null for skip, 0 means "traded, broke even"
      // Leave null — the EOD cron handles m8bf etc. similarly.
    }
    return { status: trade.status };
  }
  if (trade.status !== 'filled') return { status: trade.status };

  // Intrinsic value at expiry: |spxClose - strike|
  const closeIntrinsic = Math.abs(spxClose - trade.strike);
  const pnl = Math.round((closeIntrinsic - trade.entryDebit) * 100 * trade.contracts);

  trade.status = 'closed';
  trade.closeDate = isoDateET(etNow);
  trade.spxClose = parseFloat(spxClose.toFixed(2));
  trade.closeValue = parseFloat(closeIntrinsic.toFixed(2));
  trade.pnl = pnl;
  await env.SIGNAL_KV.put('straddle_open_trade', JSON.stringify(trade));

  // Append to closed log
  const logRaw = await env.SIGNAL_KV.get('straddle_closed_log');
  const log = logRaw ? JSON.parse(logRaw) : [];
  log.unshift(trade);
  await env.SIGNAL_KV.put('straddle_closed_log', JSON.stringify(log.slice(0, 30)));

  // Commit stradPL to history_data.json
  await upsertHistoryGitHub(env, trade.openDate, { stradPL: pnl });

  return { status: 'settled', pnl, strike: trade.strike, debit: trade.entryDebit, closeValue: closeIntrinsic };
}

// ════════════════════════════════════════════════════════════════════
// BOBF TRADE HANDLER (live)
// 3 types of broken-wing call butterfly, all 0DTE on SPX:
//   FRIDAY (Fri only)     — body offset +15, wings ±30 from body
//   VIX_UP  (Mon–Thu)     — body offset +25, wings ±30
//   VIX_DOWN (Mon–Thu)    — body offset +25, wings ±30
// All three: SHORT 2 body calls, LONG 1 lower-wing call, LONG 1 upper-wing.
//
// Window: 10:29 ET – 12:15 ET. First qualifying minute opens the trade.
// Mutual exclusion via bobf_done_<date> KV key — max 1 BOBF per day.
//
// Entry filters (all must pass at signal time):
//   - Calendar blackout (mirrors signal-engine bobfBlocks): CPI / NM-Mon /
//     VIX-exp / OPEX / OPEX-1 / EOM-1 / EOM-2 / earnings / VIX > 23
//   - SPX > 5-day SMA (last 5 daily closes)
//   - SPX move from open ≥ type-specific threshold
//   - RSI(14) on daily close ∈ type-specific band
//   - For VIX_UP / VIX_DOWN: overnight VIX moved in the right direction
//     by ≥ 0.01 points
//
// Friday max premium $12 → leave working limit if mid > 12, cancel at 12:15.
// VIX_UP / VIX_DOWN have no premium cap — fill at first qualifying minute.
// Held to expiration (4:00 PM cash close). PnL settled from intrinsic at
// SPX close: lower_intrinsic − 2*body_intrinsic + upper_intrinsic − debit.
// ════════════════════════════════════════════════════════════════════

const BOBF_BODY_OFFSET_FRIDAY = 15;
const BOBF_BODY_OFFSET_VIX    = 25;
const BOBF_WING_OFFSET        = 30;
const BOBF_FRIDAY_MAX_PREMIUM = 12.00;
const BOBF_VIX_O_N_THRESHOLD  = 0.01;
const BOBF_FRIDAY_MOVE_MIN    = 0.001;   // 0.1%
const BOBF_VIX_UP_MOVE_MIN    = 0.002;   // 0.2%
const BOBF_VIX_DOWN_MOVE_MIN  = 0.002;   // 0.2%
const BOBF_VIX_DOWN_MOVE_MAX  = 0.007;   // 0.7%
const BOBF_FRIDAY_RSI_MIN     = 40;
const BOBF_FRIDAY_RSI_MAX     = 65;
const BOBF_VIX_DOWN_RSI_MAX   = 70;
const BOBF_VIX_MAX            = 23;

function bobfInWindow(etNow) {
  const h = etNow.getHours(), m = etNow.getMinutes();
  if (h === 10 && m >= 29) return true;
  if (h === 11) return true;
  if (h === 12 && m < 15) return true;
  return false;
}

function bobfPastWindow(etNow) {
  const h = etNow.getHours(), m = etNow.getMinutes();
  return (h === 12 && m >= 15) || h > 12;
}

// Standard Wilder RSI(14) on daily closes. Returns null if insufficient data.
function computeRSI14(closes) {
  if (closes.length < 15) return null;
  let gain = 0, loss = 0;
  for (let i = 1; i <= 14; i++) {
    const diff = closes[i] - closes[i-1];
    if (diff > 0) gain += diff; else loss -= diff;
  }
  let avgGain = gain / 14, avgLoss = loss / 14;
  for (let i = 15; i < closes.length; i++) {
    const diff = closes[i] - closes[i-1];
    avgGain = (avgGain * 13 + Math.max(diff, 0)) / 14;
    avgLoss = (avgLoss * 13 + Math.max(-diff, 0)) / 14;
  }
  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return 100 - 100 / (1 + rs);
}

function computeSMA5(closes) {
  if (closes.length < 5) return null;
  const last5 = closes.slice(-5);
  return last5.reduce((a,b) => a + b, 0) / 5;
}

// Pick the BOBF type for today, or null if none qualifies.
function determineBobfType(etNow, vixToday, vixYClose) {
  const dow = etNow.getDay();
  if (dow === 5) return { type: 'friday', label: 'Friday RSI BOBF', bodyOffset: BOBF_BODY_OFFSET_FRIDAY };
  if (dow >= 1 && dow <= 4) {
    if (vixToday == null || vixYClose == null) return { type: null, reason: 'VIX data missing' };
    const diff = vixToday - vixYClose;
    if (diff >=  BOBF_VIX_O_N_THRESHOLD) return { type: 'vix_up',   label: 'BOBF VIX up',   bodyOffset: BOBF_BODY_OFFSET_VIX };
    if (diff <= -BOBF_VIX_O_N_THRESHOLD) return { type: 'vix_down', label: 'BOBF VIX down', bodyOffset: BOBF_BODY_OFFSET_VIX };
    return { type: null, reason: 'flat overnight VIX (Δ<0.01)' };
  }
  return { type: null, reason: 'weekend' };
}

// Live entry-filter evaluation. Returns { ready: bool, reason: string }.
function bobfEntryReady(typeInfo, spotNow, spxOpen, sma5, rsi14) {
  if (sma5 == null || rsi14 == null) return { ready: false, reason: 'history insufficient (need 15+ days)' };
  if (spotNow <= sma5) return { ready: false, reason: `SPX ${spotNow.toFixed(2)} ≤ 5d SMA ${sma5.toFixed(2)}` };
  const moveUp = (spotNow - spxOpen) / spxOpen;
  if (typeInfo.type === 'friday') {
    if (moveUp < BOBF_FRIDAY_MOVE_MIN)  return { ready: false, reason: `move-up ${(moveUp*100).toFixed(2)}% < 0.10%` };
    if (rsi14 < BOBF_FRIDAY_RSI_MIN || rsi14 > BOBF_FRIDAY_RSI_MAX) return { ready: false, reason: `RSI ${rsi14.toFixed(1)} outside ${BOBF_FRIDAY_RSI_MIN}-${BOBF_FRIDAY_RSI_MAX} band` };
  } else if (typeInfo.type === 'vix_up') {
    if (moveUp < BOBF_VIX_UP_MOVE_MIN)  return { ready: false, reason: `move-up ${(moveUp*100).toFixed(2)}% < 0.20%` };
  } else if (typeInfo.type === 'vix_down') {
    if (moveUp < BOBF_VIX_DOWN_MOVE_MIN) return { ready: false, reason: `move-up ${(moveUp*100).toFixed(2)}% < 0.20%` };
    if (moveUp > BOBF_VIX_DOWN_MOVE_MAX) return { ready: false, reason: `move-up ${(moveUp*100).toFixed(2)}% > 0.70%` };
    if (rsi14 > BOBF_VIX_DOWN_RSI_MAX)   return { ready: false, reason: `RSI ${rsi14.toFixed(1)} > 70` };
  }
  return { ready: true, moveUp };
}

// Main entry flow — called from every cron tick during the window.
// Idempotent via bobf_done_<date>: exits early once trade fires or window expires.
// Static-filter pre-flight: runs ONCE per day at the morning signal block.
// Catches RSI / type / calendar / VIX>23 disqualifications that won't change
// intraday so we skip 60+ futile entry attempts in the 10:29-12:15 window.
// Sets bobf_done_<date> with the rejection reason; handleBobfEntry then
// short-circuits on every market tick.
async function prefilterBobf(env, etNow, vixToday, vixYClose) {
  const todayISO = isoDateET(etNow);
  const doneKey = `bobf_done_${todayISO}`;
  const existing = await env.SIGNAL_KV.get(doneKey);
  if (existing) return { skipped: 'already-done', why: existing };

  // 1. Type qualification (Fri vs Mon-Thu vix_up/down vs flat-overnight skip)
  const typeInfo = determineBobfType(etNow, vixToday, vixYClose);
  if (!typeInfo.type) {
    await env.SIGNAL_KV.put(doneKey, `no-type:${typeInfo.reason}`, { expirationTtl: 86400 });
    return { skipped: 'no-type', reason: typeInfo.reason };
  }

  // 2. Calendar blackouts + VIX>23 (mirrors signal-engine bobfBlocks)
  const cpiDay   = cpiSch.includes(todayLong(etNow));
  const nmDay    = isFirstTradeMo(etNow);
  const nmMon    = isFirstTradeMon(etNow);
  const vixExpDay = vixSch.includes(todayLong(etNow));
  const opexDay  = opexSch.includes(todayLong(etNow));
  const opex1    = opexSch.some(ds => isTodayBefore(ds, etNow));
  const eom1     = isEomN(1, etNow);
  const eom2     = isEomN(2, etNow);
  const earnDay  = isEarningsDay(etNow);
  const blackouts = [];
  if (cpiDay) blackouts.push('CPI');
  if (nmMon) blackouts.push('NM Mon');
  if (vixExpDay) blackouts.push('VIX exp');
  if (opexDay) blackouts.push('OPEX');
  if (opex1) blackouts.push('OPEX-1');
  if (eom2) blackouts.push('EOM-2');
  if (eom1) blackouts.push('EOM-1');
  if (earnDay) blackouts.push('earnings');
  if (vixToday != null && vixToday > BOBF_VIX_MAX) blackouts.push(`VIX ${vixToday}>${BOBF_VIX_MAX}`);
  if (blackouts.length) {
    await env.SIGNAL_KV.put(doneKey, `blackout:${blackouts.join(',')}`, { expirationTtl: 86400 });
    return { skipped: 'blackout', reasons: blackouts };
  }

  // 3. RSI(14) + SMA5 + spxOpen — all daily-close-based, fixed for the entire
  //    trading day. Cache them in KV so handleBobfEntry doesn't re-fetch
  //    history_data.json from GitHub on every tick (~106 saved fetches/day).
  let rsi14 = null, sma5 = null, spxOpen = null;
  try {
    const histData = await getHistory(env);
    if (Array.isArray(histData) && histData.length) {
      const todayRow = histData.find(r => r.date === todayISO);
      spxOpen = todayRow?.spxOpen != null ? parseFloat(todayRow.spxOpen) : null;
      const sortedPrior = histData
        .filter(r => r.date < todayISO && r.spxClose != null)
        .sort((a, b) => a.date.localeCompare(b.date));
      const closes30 = sortedPrior.slice(-30).map(r => parseFloat(r.spxClose));
      rsi14 = computeRSI14(closes30);
      sma5 = computeSMA5(closes30);
    }
  } catch (_) { /* leave handleBobfEntry to recheck */ }

  if (rsi14 != null) {
    if (typeInfo.type === 'friday' && (rsi14 < BOBF_FRIDAY_RSI_MIN || rsi14 > BOBF_FRIDAY_RSI_MAX)) {
      await env.SIGNAL_KV.put(doneKey, `rsi:${rsi14.toFixed(1)} outside ${BOBF_FRIDAY_RSI_MIN}-${BOBF_FRIDAY_RSI_MAX} band`, { expirationTtl: 86400 });
      return { skipped: 'rsi-out', rsi14, type: 'friday' };
    }
    if (typeInfo.type === 'vix_down' && rsi14 > BOBF_VIX_DOWN_RSI_MAX) {
      await env.SIGNAL_KV.put(doneKey, `rsi:${rsi14.toFixed(1)} > ${BOBF_VIX_DOWN_RSI_MAX}`, { expirationTtl: 86400 });
      return { skipped: 'rsi-high', rsi14, type: 'vix_down' };
    }
  }

  // SAFETY + AUTO-RECOVERY: if today's spxOpen is missing from history
  // (morning signal block failed — Schwab outage etc.), the entry handler
  // can't compute move-up% and will silently never fire. Don't give up
  // immediately: try to recover from Schwab NOW (the 9:30 candle exists
  // by 10:29 when prefilter runs). Only mark data-stale if recovery fails.
  if (spxOpen == null) {
    console.warn(`[bobf] spxOpen missing for ${todayISO}, attempting auto-recovery from Schwab 9:30 candle...`);
    try {
      const recovered = await recoverOpenPricesFromSchwab(env, etNow);
      if (recovered.spxOpen != null) {
        spxOpen = recovered.spxOpen;
        const fields = { spxOpen };
        if (recovered.vixOpen != null) fields.vixOpen = recovered.vixOpen;
        try {
          await upsertHistoryGitHub(env, todayISO, fields);
          console.log(`[bobf] auto-recovery wrote spxOpen=${spxOpen}${recovered.vixOpen != null ? ` vixOpen=${recovered.vixOpen}` : ''} for ${todayISO}`);
          await logEvent(env, 'warn', 'bobf-recover', 'auto-recovered missing spxOpen from Schwab 9:30 candle', fields);
        } catch (writeErr) {
          console.warn('[bobf] auto-recovery history write failed:', writeErr.message);
        }
      }
    } catch (e) {
      console.warn('[bobf] auto-recovery failed:', e.message);
    }
  }
  if (spxOpen == null) {
    await env.SIGNAL_KV.put(doneKey, 'data-stale: spxOpen missing — morning signal block failed and auto-recovery also failed', { expirationTtl: 86400 });
    console.warn(`[bobf] data-stale for ${todayISO}: spxOpen null even after recovery, prefilter cannot evaluate entry conditions`);
    return { skipped: 'data-stale', reason: 'spxOpen missing in history_data.json (recovery failed)' };
  }

  // Cache the day's static inputs for handleBobfEntry to reuse every tick.
  await env.SIGNAL_KV.put(`bobf_static_${todayISO}`, JSON.stringify({
    rsi14, sma5, spxOpen, vixToday, vixYClose,
    type: typeInfo.type, label: typeInfo.label, bodyOffset: typeInfo.bodyOffset,
  }), { expirationTtl: 86400 });

  // All static filters pass — entry checks during 10:29-12:15 will evaluate
  // the dynamic conditions (move-up %, SMA5).
  return { passed: true, type: typeInfo.type, rsi14 };
}

async function handleBobfEntry(env, etNow, preChain = null) {
  const todayISO = isoDateET(etNow);
  const out = { date: todayISO, status: null };

  const doneKey = `bobf_done_${todayISO}`;
  const done = await env.SIGNAL_KV.get(doneKey);
  if (done) return { ...out, status: 'already-done', why: done };

  // Guard: if today's open BOBF trade already exists, don't re-evaluate —
  // refreshBobfLiveQuotes handles working→filled transitions. Without this
  // guard, every tick re-runs the full entry filter and could overwrite the
  // working-order's entry strikes/debit if spot moved between ticks (which
  // would corrupt the trade record the user is actually holding).
  try {
    const existingRaw = await env.SIGNAL_KV.get('bobf_open_trade');
    if (existingRaw) {
      const existing = JSON.parse(existingRaw);
      if (existing.openDate === todayISO && existing.status !== 'closed' && existing.status !== 'expired') {
        return { ...out, status: 'already-open', existingStatus: existing.status };
      }
    }
  } catch (_) { /* fall through — KV blip will recover next tick */ }

  // Past window → mark done permanently (no fire, no trade)
  if (bobfPastWindow(etNow)) {
    await env.SIGNAL_KV.put(doneKey, 'window-passed', { expirationTtl: 86400 });
    return { ...out, status: 'window-passed' };
  }
  if (!bobfInWindow(etNow)) return { ...out, status: 'pre-window' };

  // Try BOBF static cache first (written by prefilterBobf at 9:30). If hit,
  // we skip the GitHub history fetch entirely on every market tick.
  let vixToday, spxOpen, vixYClose, sma5, rsi14, typeInfo;
  const staticRaw = await env.SIGNAL_KV.get(`bobf_static_${todayISO}`);
  if (staticRaw) {
    const s = JSON.parse(staticRaw);
    vixToday = s.vixToday; spxOpen = s.spxOpen; vixYClose = s.vixYClose;
    sma5 = s.sma5; rsi14 = s.rsi14;
    typeInfo = { type: s.type, label: s.label, bodyOffset: s.bodyOffset };
  } else {
    // Cache miss (e.g. cold start, prefilter never ran) — fall back to KV history
    let histData;
    try {
      histData = await getHistory(env);
      if (!Array.isArray(histData) || !histData.length) {
        return { ...out, status: 'error', error: 'history empty (KV not seeded)' };
      }
    } catch (e) { return { ...out, status: 'error', error: 'history fetch: ' + e.message }; }

    const todayRow = histData.find(r => r.date === todayISO);
    vixToday = todayRow?.vixOpen != null ? parseFloat(todayRow.vixOpen) : null;
    spxOpen  = todayRow?.spxOpen != null ? parseFloat(todayRow.spxOpen) : null;

    const sortedPrior = histData
      .filter(r => r.date < todayISO && r.spxClose != null)
      .sort((a, b) => a.date.localeCompare(b.date));
    if (sortedPrior.length === 0) return { ...out, status: 'error', error: 'no prior history' };

    const yClose = sortedPrior[sortedPrior.length - 1];
    vixYClose = yClose.vixClose != null ? parseFloat(yClose.vixClose) : null;
    const closes30 = sortedPrior.slice(-30).map(r => parseFloat(r.spxClose));
    sma5  = computeSMA5(closes30);
    rsi14 = computeRSI14(closes30);

    typeInfo = determineBobfType(etNow, vixToday, vixYClose);
  }
  if (!typeInfo.type) {
    await env.SIGNAL_KV.put(doneKey, 'no-type:' + (typeInfo.reason || 'unknown'), { expirationTtl: 86400 });
    return { ...out, status: 'no-type', reason: typeInfo.reason };
  }

  // Calendar blackouts (mirrors signal-engine bobfBlocks)
  const cpiDay   = cpiSch.includes(todayLong(etNow));
  const nmDay    = isFirstTradeMo(etNow);
  const nmMon    = isFirstTradeMon(etNow);
  const vixExpDay = vixSch.includes(todayLong(etNow));
  const opexDay  = opexSch.includes(todayLong(etNow));
  const opex1    = opexSch.some(ds => isTodayBefore(ds, etNow));
  const eom1     = isEomN(1, etNow);
  const eom2     = isEomN(2, etNow);
  const earnDay  = isEarningsDay(etNow);
  const blackouts = [];
  if (cpiDay) blackouts.push('CPI');
  if (nmMon) blackouts.push('NM Mon');
  if (vixExpDay) blackouts.push('VIX exp');
  if (opexDay) blackouts.push('OPEX');
  if (opex1) blackouts.push('OPEX-1');
  if (eom2) blackouts.push('EOM-2');
  if (eom1) blackouts.push('EOM-1');
  if (earnDay) blackouts.push('earnings');
  if (vixToday != null && vixToday > BOBF_VIX_MAX) blackouts.push(`VIX ${vixToday}>${BOBF_VIX_MAX}`);
  if (blackouts.length) {
    await env.SIGNAL_KV.put(doneKey, 'blackout:' + blackouts.join(','), { expirationTtl: 86400 });
    return { ...out, status: 'blackout', reason: blackouts.join(', '), type: typeInfo.type };
  }

  // Use master chain if available (saves 2 Schwab calls), else fetch our own.
  let token, spot, callExpDateMap;
  try {
    token = await getAccessToken(env);
    const chain = preChain || await fetchSpxFullChain(token, todayISO, env);
    spot = chain.spot; callExpDateMap = chain.callExpDateMap;
  } catch (e) { return { ...out, status: 'error', error: 'chain fetch: ' + e.message }; }
  if (!spot) return { ...out, status: 'error', error: 'no SPX spot' };

  if (spxOpen == null) return { ...out, status: 'error', error: 'no spxOpen — wait for morning EOD write' };

  // Entry-filter check
  const ready = bobfEntryReady(typeInfo, spot, spxOpen, sma5, rsi14);
  if (!ready.ready) return { ...out, status: 'waiting', reason: ready.reason, type: typeInfo.type, sma5, rsi14, spotNow: spot, spxOpen };

  // Compute strikes — pick body FIRST, then re-anchor wings on the actual body
  // strike picked (in case fuzz fallback in pickContractFromChain lands on a
  // neighbor). Otherwise wings could mismatch the body and butterfly geometry
  // breaks, which mis-prices settlement intrinsic.
  const reqBody  = snap5(spot + typeInfo.bodyOffset);
  const bodyLeg  = pickContractFromChain(callExpDateMap, todayISO, reqBody);
  if (!bodyLeg) return { ...out, status: 'error', error: `missing body leg in chain @ ${reqBody}` };
  const kBody  = bodyLeg.strike;            // actual filled body strike
  const kLower = kBody - BOBF_WING_OFFSET;
  const kUpper = kBody + BOBF_WING_OFFSET;

  const lowerLeg = pickContractFromChain(callExpDateMap, todayISO, kLower);
  const upperLeg = pickContractFromChain(callExpDateMap, todayISO, kUpper);
  if (!lowerLeg || !upperLeg) return { ...out, status: 'error', error: `missing wing leg in chain (${kLower}/${kUpper}, anchored on body ${kBody})` };
  if (lowerLeg.mid == null || bodyLeg.mid == null || upperLeg.mid == null) return { ...out, status: 'error', error: 'missing bid/ask on a leg' };
  // Verify wings landed on EXACT requested strikes (no fuzz on wings — that
  // would break the symmetric debit math).
  if (lowerLeg.strike !== kLower || upperLeg.strike !== kUpper) {
    return { ...out, status: 'error', error: `wing fuzz mismatch — lower req ${kLower}/got ${lowerLeg.strike}, upper req ${kUpper}/got ${upperLeg.strike}` };
  }

  const debit = lowerLeg.mid - 2 * bodyLeg.mid + upperLeg.mid;

  // Daily total-risk cap (2026-06-09): butterfly max loss = debit × 100.
  // Status-return (not throw): next 2-min tick re-checks; if exposure clears
  // (e.g. straddle settled), BOBF can still fire inside its window.
  {
    const bobfGate = await enforceRiskCap(env, etNow, 'bobf', debit * 100);
    if (!bobfGate.ok) {
      return { ...out, status: 'risk-cap-blocked', detail: bobfGate.reason };
    }
  }

  // ── Item 9: live VIX validator (defense-in-depth) ──
  // vixToday above is the cached morning 9:30 print. By trade-fire time
  // (10:29-12:15 ET) VIX may have spiked above the BOBF gate. Re-pull live
  // VIX and re-check the gate. If violated, abort THIS tick without marking
  // done — next 2-min tick re-checks; VIX may revert.
  try {
    const liveVixQ = await fetchSchwabJSON(
      'https://api.schwabapi.com/marketdata/v1/quotes?symbols=%24VIX&fields=quote',
      token, env);
    const liveVix = liveVixQ?.['$VIX']?.quote?.lastPrice;
    if (liveVix != null && liveVix > BOBF_VIX_MAX) {
      console.warn(`[bobf-validate] live VIX ${liveVix.toFixed(2)} > ${BOBF_VIX_MAX} — aborting fire`);
      await logEvent(env, 'warn', 'bobf-validate',
        `live VIX spiked above ${BOBF_VIX_MAX} at fire time, aborting`,
        { liveVix: parseFloat(liveVix.toFixed(2)), vixOpenCached: vixToday, gate: BOBF_VIX_MAX });
      return { ...out, status: 'vix-spiked',
               liveVix: parseFloat(liveVix.toFixed(2)),
               gate: BOBF_VIX_MAX, type: typeInfo.type };
    }
  } catch (vErr) {
    // Don't block trade on validator failure — falls through to existing logic
    console.warn('[bobf-validate] live VIX fetch failed:', vErr.message);
  }

  // Friday max-premium logic (working order pattern). VIX_UP / VIX_DOWN: no cap.
  let status, fillDebit = null, fillTimeET = null, maxDebit = null;
  if (typeInfo.type === 'friday') {
    maxDebit = BOBF_FRIDAY_MAX_PREMIUM;
    if (debit <= BOBF_FRIDAY_MAX_PREMIUM) {
      status = 'filled';
      fillDebit = debit;
      fillTimeET = `${String(etNow.getHours()).padStart(2,'0')}:${String(etNow.getMinutes()).padStart(2,'0')}`;
    } else {
      status = 'working';
    }
  } else {
    status = 'filled';
    fillDebit = debit;
    fillTimeET = `${String(etNow.getHours()).padStart(2,'0')}:${String(etNow.getMinutes()).padStart(2,'0')}`;
  }

  const trade = {
    openDate: todayISO,
    openTimeET: `${String(etNow.getHours()).padStart(2,'0')}:${String(etNow.getMinutes()).padStart(2,'0')}`,
    type: typeInfo.type,
    label: typeInfo.label,
    spotEntry: parseFloat(spot.toFixed(2)),
    spxOpen: parseFloat(spxOpen.toFixed(2)),
    moveUpPct: parseFloat((ready.moveUp * 100).toFixed(3)),
    sma5: parseFloat(sma5.toFixed(2)),
    rsi14: parseFloat(rsi14.toFixed(2)),
    vixToday, vixYClose,
    bodyStrike: kBody, lowerStrike: kLower, upperStrike: kUpper,
    expDate: todayISO,
    bodySymbol: bodyLeg.symbol, lowerSymbol: lowerLeg.symbol, upperSymbol: upperLeg.symbol,
    entryLowerMid: parseFloat(lowerLeg.mid.toFixed(2)),
    entryBodyMid:  parseFloat(bodyLeg.mid.toFixed(2)),
    entryUpperMid: parseFloat(upperLeg.mid.toFixed(2)),
    entryDebit:    parseFloat(debit.toFixed(2)),
    maxDebit,
    contracts: 1,
    // Live fields
    currentSpot: parseFloat(spot.toFixed(2)),
    currentLowerMid: parseFloat(lowerLeg.mid.toFixed(2)),
    currentBodyMid:  parseFloat(bodyLeg.mid.toFixed(2)),
    currentUpperMid: parseFloat(upperLeg.mid.toFixed(2)),
    currentValue:    parseFloat(debit.toFixed(2)),
    currentPnl: 0,
    lastQuoteAt: new Date().toISOString(),
    status, fillDebit, fillTimeET,
    workingExpiry: `${todayISO} 12:15 ET`,
  };

  await env.SIGNAL_KV.put('bobf_open_trade', JSON.stringify(trade));
  if (status === 'filled') {
    await env.SIGNAL_KV.put(doneKey, 'filled', { expirationTtl: 86400 });
  }
  // working orders: leave doneKey unset so subsequent ticks can flip working→filled

  console.log(`[bobf] opened ${status} type=${typeInfo.type} body=${kBody} debit=${debit.toFixed(2)} maxDebit=${maxDebit ?? '-'}`);
  await logEvent(env, 'info', 'bobf-open', `opened ${status} type=${typeInfo.type}`, {
    body: kBody, debit: parseFloat(debit.toFixed(2)), maxDebit, type: typeInfo.type,
  });
  return { ...out, status: 'opened', type: typeInfo.type, trade: { strikes: [kLower, kBody, kUpper], debit, status } };
}

// Refresh live mids on the open BOBF trade. Handles working→filled transition
// + working→expired at 12:15 ET.
async function refreshBobfLiveQuotes(env, token, etNow, preChain = null) {
  const raw = await env.SIGNAL_KV.get('bobf_open_trade');
  if (!raw) return null;
  const trade = JSON.parse(raw);
  if (trade.status === 'closed' || trade.status === 'expired') return trade;

  // Working order past 12:15 ET → expire
  if (trade.status === 'working' && bobfPastWindow(etNow)) {
    trade.status = 'expired';
    trade.expiredAt = new Date().toISOString();
    await env.SIGNAL_KV.put('bobf_open_trade', JSON.stringify(trade));
    await env.SIGNAL_KV.put(`bobf_done_${isoDateET(etNow)}`, 'expired', { expirationTtl: 86400 });
    return trade;
  }

  try {
    const chain = preChain || await fetchSpxFullChain(token, trade.expDate, env);
    const { spot, callExpDateMap } = chain;
    const lower = pickContractFromChain(callExpDateMap, trade.expDate, trade.lowerStrike);
    const body  = pickContractFromChain(callExpDateMap, trade.expDate, trade.bodyStrike);
    const upper = pickContractFromChain(callExpDateMap, trade.expDate, trade.upperStrike);
    if (!lower || !body || !upper || lower.mid == null || body.mid == null || upper.mid == null) return trade;

    const newDebit = lower.mid - 2 * body.mid + upper.mid;
    trade.currentSpot     = spot ? parseFloat(spot.toFixed(2)) : trade.currentSpot;
    trade.currentLowerMid = parseFloat(lower.mid.toFixed(2));
    trade.currentBodyMid  = parseFloat(body.mid.toFixed(2));
    trade.currentUpperMid = parseFloat(upper.mid.toFixed(2));
    trade.currentValue    = parseFloat(newDebit.toFixed(2));

    // Working → filled if mid drops to ≤ max debit
    let justFilled = false;
    if (trade.status === 'working' && newDebit <= trade.maxDebit) {
      trade.status = 'filled';
      trade.fillDebit = parseFloat(newDebit.toFixed(2));
      trade.fillTimeET = `${String(etNow.getHours()).padStart(2,'0')}:${String(etNow.getMinutes()).padStart(2,'0')}`;
      // Re-snapshot leg fills at the actual fill time
      trade.entryLowerMid = parseFloat(lower.mid.toFixed(2));
      trade.entryBodyMid  = parseFloat(body.mid.toFixed(2));
      trade.entryUpperMid = parseFloat(upper.mid.toFixed(2));
      trade.entryDebit    = parseFloat(newDebit.toFixed(2));
      justFilled = true;
    }

    if (trade.status === 'filled') {
      trade.currentPnl = Math.round((trade.currentValue - trade.entryDebit) * 100 * trade.contracts);
    }
    trade.lastQuoteAt = new Date().toISOString();
    // Write trade record FIRST (source of truth), then the done-key marker.
    // If worker is evicted between the two writes, next tick still finds the
    // filled trade in `bobf_open_trade` and treats it correctly. Reverse order
    // would mark "done" but lose the fill snapshot.
    await env.SIGNAL_KV.put('bobf_open_trade', JSON.stringify(trade));
    if (justFilled) {
      await env.SIGNAL_KV.put(`bobf_done_${isoDateET(etNow)}`, 'filled', { expirationTtl: 86400 });
    }
  } catch (e) { console.warn('[bobf] refresh failed:', e.message); }
  return trade;
}

// Settle BOBF at SPX close. PnL from intrinsic of the call butterfly:
//   pnl = (lower_intrinsic − 2*body_intrinsic + upper_intrinsic − debit) × 100
async function settleBobfEOD(env, etNow, spxClose) {
  const raw = await env.SIGNAL_KV.get('bobf_open_trade');
  if (!raw) return { status: 'no-trade' };
  const trade = JSON.parse(raw);
  if (trade.openDate !== isoDateET(etNow)) return { status: 'wrong-date', openDate: trade.openDate };
  if (trade.status === 'closed') return { status: 'already-closed' };
  if (trade.status === 'expired') return { status: 'expired-no-trade' };
  if (trade.status !== 'filled') return { status: trade.status };

  const lowerI = Math.max(spxClose - trade.lowerStrike, 0);
  const bodyI  = Math.max(spxClose - trade.bodyStrike,  0);
  const upperI = Math.max(spxClose - trade.upperStrike, 0);
  const intrinsic = lowerI - 2 * bodyI + upperI;
  const pnl = Math.round((intrinsic - trade.entryDebit) * 100 * trade.contracts);

  trade.status = 'closed';
  trade.closeDate = isoDateET(etNow);
  trade.spxClose = parseFloat(spxClose.toFixed(2));
  trade.closeIntrinsic = parseFloat(intrinsic.toFixed(2));
  trade.pnl = pnl;
  await env.SIGNAL_KV.put('bobf_open_trade', JSON.stringify(trade));

  const logRaw = await env.SIGNAL_KV.get('bobf_closed_log');
  const log = logRaw ? JSON.parse(logRaw) : [];
  log.unshift(trade);
  await env.SIGNAL_KV.put('bobf_closed_log', JSON.stringify(log.slice(0, 30)));

  await upsertHistoryGitHub(env, trade.openDate, { bobfPL: pnl });
  return { status: 'settled', pnl, type: trade.type, strikes: [trade.lowerStrike, trade.bodyStrike, trade.upperStrike], debit: trade.entryDebit, intrinsic };
}

// ════════════════════════════════════════════════════════════════════
// GXBF TRADE HANDLER (live)
// ────────────────────────────────────────────────────────────────────
// GXBF = compute volume-weighted dealer-gamma peak live from the Schwab
// 0DTE call chain → use that strike as the butterfly center → widen the
// wing until risk ≈ reward (closest to exactly 50/50) → 0DTE SPXW long
// CALL fly → hold to 4 PM cash close.
//
// Pipeline mirrors BOBF/Straddle verbatim:
//   - Gate: only act when the morning signal theme is 'gxbf' (analogous to
//     the Straddle `signal.theme === 'strad'` branch). STRATEGY
//     INDEPENDENCE: GXBF never blocks / is blocked by M8BF/Straddle/BOBF;
//     its card reflects only GXBF's own state.
//   - Idempotent via gxbf_done_<date>.
//   - Center computed in-house via computeGxbfCenterLive (Black-Scholes
//     gamma × totalVolume × spot² × 100 × 0.01, per-strike, ±5% range).
//     Was Discord-scraped historically; now uses the live chain only.
//   - Long-call butterfly: BUY 1 @ K−W, SELL 2 @ K, BUY 1 @ K+W.
//   - Wing W ∈ [5,150] step 5. netDebit = mid(K−W) − 2·mid(K) + mid(K+W).
//     risk = netDebit, reward = W − netDebit. Keep candidates where
//     netDebit > 0 && risk ≤ reward (netDebit ≤ W/2). Pick W minimizing
//     |netDebit − W/2|.
//   - Held to expiration. PnL from intrinsic at SPX close S:
//     clamp(max(0,S−(K−W)) − 2·max(0,S−K) + max(0,S−(K+W)), 0, W).
//     pnl = (intrinsic − netDebit) × 100 × contracts  (same convention as
//     settleBobfEOD / settleStraddleEOD).
// ════════════════════════════════════════════════════════════════════

const GXBF_WING_MIN  = 5;
const GXBF_WING_MAX  = 100;   // cap → max risk = W/2 = 50pt = $5,000/contract
const GXBF_WING_STEP = 5;

// GXBF entry window — opens at 09:35 ET (NOT earlier).
//
// 2026-06-09 FIX: window used to start at 09:33 ET, which dated back to the
// pre-2026-05 Discord-scrape era where the worker watched for the "Major
// Positive by Volume" Discord post that landed ~09:34:59 ET. With the
// in-house live-gamma replacement (computeGxbfCenterLive), there is NO
// reason to attempt entry before 09:35 — the SPX 0DTE call chain quotes
// settle in the first ~5 minutes of regular session, and firing at 09:33
// reads stale/transient quotes for the gamma center.
//
// User feedback (2026-06-09): "GXBF should not trade before 9:35 — at 9:35
// you check the KX (gamma) levels, then fire ~9:36." Window aligned with
// signal-engine.js `rec = "GXBF @ 9:36 AM"`.
function gxbfInWindow(etNow) {
  const h = etNow.getHours(), m = etNow.getMinutes();
  return h === 9 && m >= 35 && m <= 45;
}
function gxbfPastWindow(etNow) {
  const h = etNow.getHours(), m = etNow.getMinutes();
  return (h === 9 && m > 45) || h > 9;
}

// Compute the GXBF gamma center LIVE from the Schwab/Tasty option chain.
// Replaces the old Discord "Major Positive by Volume" scraper — same idea
// (volume-weighted dealer gamma peak) but computed in-house from the live
// 0DTE call chain. Uses the same Black-Scholes gamma formula as calculateGEX
// (R=0.043, Q=0.013) and the same ±5% strike range filter. Live entry runs
// at 09:35 ET so we hard-code today's 16:00 ET close for T (half-day case
// is rare and the morning T is dominated by the day-fraction anyway).
//
// Returns { center, centerOI, spot, _source: 'live-chain' } where:
//   - center   = strike with max volume-weighted gamma (snapped to 5)
//   - centerOI = strike with max open-interest-weighted gamma (snapped to 5)
//
// ─────────────────────────────────────────────────────────────────────────
// FULL METHODOLOGY: see tasks/GXBF_METHODOLOGY.md  (the durable reference).
// Read it before touching this function, fetch_thetadata_gxbf.py::build_day,
// or any GXBF gating logic in signal-engine.js. It covers:
//   • why per-strike IV (NOT a uniform VIX-as-σ),
//   • the S²·100·0.01 dealer-exposure factors (mirrored from calculateGEX),
//   • the hybrid live rule (OI on OPEX-1 / VIX-exp / FED, else volume),
//   • known bugs we've hit and the fixes that stuck,
//   • the deleted Discord scraper (commit 8280d55) — do not chase it.
// ─────────────────────────────────────────────────────────────────────────
function computeGxbfCenterLive(callExpDateMap, expDate, spot) {
  if (!callExpDateMap || !spot || spot <= 0) return null;
  const R = 0.043, Q = 0.013, MULT = 100;
  const expKey = Object.keys(callExpDateMap).find(k => k.startsWith(expDate + ':'));
  if (!expKey) return null;

  // T = hours-to-16:00-ET / (365·24). Floor of 15 min so T never collapses
  // to zero if called late in the window. (Matches calculateGEX::zeroDteT.)
  const etNow = toET(new Date());
  const hrsLeft = (16 - etNow.getHours()) - (etNow.getMinutes() / 60) - (etNow.getSeconds() / 3600);
  const safeHrs = Math.max(hrsLeft, 0.25);
  const T = safeHrs / (365 * 24);

  const normPdf = (x) => Math.exp(-0.5 * x * x) / Math.sqrt(2 * Math.PI);
  const bsGamma = (S, K, sigma) => {
    if (T <= 0 || sigma <= 0 || S <= 0) return 0;
    const d1 = (Math.log(S / K) + (R - Q + 0.5 * sigma * sigma) * T) / (sigma * Math.sqrt(T));
    return normPdf(d1) * Math.exp(-Q * T) / (S * sigma * Math.sqrt(T));
  };

  // ±5% range matches calculateGEX's chart-window filter (line 3840-3841).
  const rangePct = 0.05;
  const lo = spot * (1 - rangePct), hi = spot * (1 + rangePct);

  const strikes = callExpDateMap[expKey] || {};
  let maxVol = 0, centerByVolume = null;
  let maxOI  = 0, centerByOI     = null;

  for (const strikeStr of Object.keys(strikes)) {
    const contracts = strikes[strikeStr] || [];
    for (const c of contracts) {
      const K = parseFloat(c.strikePrice != null ? c.strikePrice : strikeStr);
      if (!isFinite(K) || K <= 0) continue;
      if (K < lo || K > hi) continue;
      const vol = Math.max(c.totalVolume || 0, 0);
      const oi  = Math.max(c.openInterest || 0, 0);
      if (vol === 0 && oi === 0) continue;
      const iv = (c.volatility || 0) / 100;
      const gamma = bsGamma(spot, K, iv > 0 ? iv : 0.2);
      const gex_vol = gamma * vol * spot * spot * MULT * 0.01;
      const gex_oi  = gamma * oi  * spot * spot * MULT * 0.01;
      if (gex_vol > maxVol) { maxVol = gex_vol; centerByVolume = K; }
      if (gex_oi  > maxOI)  { maxOI  = gex_oi;  centerByOI     = K; }
    }
  }

  if (centerByVolume == null) return null;
  return {
    center:   snap5(Math.round(centerByVolume)),
    centerOI: centerByOI != null ? snap5(Math.round(centerByOI)) : null,
    spot,
    _source: 'live-chain',
  };
}

// Wing selection: KEEP WIDENING, take the WIDEST wing where risk ≤ reward.
// Iterate W from GXBF_WING_MIN to GXBF_WING_MAX step GXBF_WING_STEP.
// `midFn(strike)` returns the option mid or null. For each W require all 3
// strikes (K−W, K, K+W) to exist with a valid mid. netDebit = mid(K−W) −
// 2·mid(K) + mid(K+W). risk = netDebit, reward = W − netDebit. A wing is
// valid iff netDebit > 0 && risk ≤ reward (i.e. netDebit ≤ W/2). It does
// NOT need to be exactly 50/50 — only that risk never exceeds reward. Since
// W ascends, overwriting on every valid W leaves the WIDEST valid wing.
// Returns that candidate, or null if none qualify (→ no trade).
function selectGxbfWing(K, midFn) {
  let bestC = null;
  for (let W = GXBF_WING_MIN; W <= GXBF_WING_MAX; W += GXBF_WING_STEP) {
    const lowerMid = midFn(K - W);
    const centerMid = midFn(K);
    const upperMid = midFn(K + W);
    if (lowerMid == null || centerMid == null || upperMid == null) continue;
    const netDebit = lowerMid - 2 * centerMid + upperMid;
    const risk = netDebit;
    const reward = W - netDebit;
    if (!(netDebit > 0 && risk <= reward)) continue;  // risk ≤ reward (netDebit ≤ W/2)
    bestC = { W, netDebit, risk, reward,
              lowerMid, centerMid, upperMid };          // widest valid so far
  }
  return bestC;
}

// Main GXBF entry flow — called from the morning post-Discord deferred block
// (gated on signal.theme === 'gxbf'). Idempotent via gxbf_done_<date>.
async function handleGxbfEntry(env, etNow, signal, preChain = null) {
  const todayISO = isoDateET(etNow);
  const out = { date: todayISO, status: null };
  const doneKey = `gxbf_done_${todayISO}`;

  const done = await env.SIGNAL_KV.get(doneKey);
  if (done) return { ...out, status: 'already-done', why: done };

  // 2026-06-09 belt-and-suspenders: even if the caller's window gate is
  // wrong or removed, refuse to fire before 09:35 ET. This guarantees the
  // SPX 0DTE chain quotes have had a few minutes to settle so the live
  // gamma center is real, not a transient post-open value. User reported
  // an early 09:33 fire today — gxbfInWindow was the root cause, but if
  // any other entry-point ever calls this directly, it stays protected.
  {
    const h = etNow.getHours(), m = etNow.getMinutes();
    const beforeFireTime = (h < 9) || (h === 9 && m < 35);
    if (beforeFireTime) {
      console.warn(`[gxbf] entry refused — too early (${h}:${String(m).padStart(2,'0')} ET, need ≥ 09:35)`);
      return { ...out, status: 'too-early', etTime: `${h}:${String(m).padStart(2,'0')}` };
    }
  }

  if (gxbfPastWindow(etNow)) {
    await env.SIGNAL_KV.put(doneKey, 'window-passed', { expirationTtl: 86400 });
    return { ...out, status: 'window-passed' };
  }

  // 1. Build the chain FIRST (reuse master chain — zero extra Schwab calls).
  //    We need the chain to compute the gamma center live.
  let token, spot, callExpDateMap;
  try {
    token = await getAccessToken(env);
    const chain = preChain || await fetchSpxFullChain(token, todayISO, env);
    spot = chain.spot; callExpDateMap = chain.callExpDateMap;
  } catch (e) { return { ...out, status: 'error', error: 'chain fetch: ' + e.message }; }
  if (!spot) return { ...out, status: 'error', error: 'no SPX spot' };

  // 2. Compute the gamma center LIVE from the chain (volume- AND OI-weighted).
  //    Replaces the old Discord scraper. snap5/round already applied inside.
  const computed = computeGxbfCenterLive(callExpDateMap, todayISO, spot);
  if (!computed) {
    // No qualifying strikes (e.g. empty 0DTE chain). Don't mark done —
    // subsequent cron ticks re-attempt within the entry window.
    return { ...out, status: 'no-center', reason: 'live-gamma compute returned null' };
  }

  // 2a. Pick the per-day center via signal.centerSource (hybrid routing).
  //     'oi'  → OPEX-1 / VIX-expiry / FED days (per signal-engine.js)
  //     'vol' → all other GXBF days (default)
  //     Fallback to volume center with a warning if OI was requested but null.
  const requestedSource = (signal && signal.centerSource) || 'vol';
  let centerSource = requestedSource;
  let K;
  if (requestedSource === 'oi') {
    if (computed.centerOI != null) {
      K = computed.centerOI;
    } else {
      K = computed.center;
      centerSource = 'vol-fallback';
      console.warn(`[gxbf] centerSource=oi requested but computed.centerOI is null; falling back to volume center ${K}`);
      await logEvent(env, 'warn', 'gxbf-center-fallback',
        'centerSource=oi requested but centerOI null; using volume center',
        { volCenter: computed.center, centerOI: computed.centerOI });
    }
  } else {
    K = computed.center;
  }

  // 4. Wing 50/50 selection. midFn pulls the call mid for an exact strike
  //    (no fuzz — symmetric debit math requires exact strikes).
  const legCache = new Map();
  const legFor = (strike) => {
    if (legCache.has(strike)) return legCache.get(strike);
    const leg = pickContractFromChain(callExpDateMap, todayISO, strike);
    const exact = (leg && leg.strike === strike && leg.mid != null) ? leg : null;
    legCache.set(strike, exact);
    return exact;
  };
  const midFn = (strike) => { const l = legFor(strike); return l ? l.mid : null; };

  const pick = selectGxbfWing(K, midFn);
  if (!pick) {
    await env.SIGNAL_KV.put(doneKey, 'no-wing: no W with netDebit>0 & risk≤reward', { expirationTtl: 86400 });
    await logEvent(env, 'warn', 'gxbf-skip', 'no qualifying wing (risk≤reward) found', { center: K, centerSource, centerOI: computed.centerOI, centerVol: computed.center });
    try {
      const dcRaw = await env.SIGNAL_KV.get('discord_config');
      if (dcRaw) {
        const dc = JSON.parse(dcRaw);
        if (dc.channelId) await sendDiscordDM(env, dc.channelId,
          `⚠️ **GXBF** — no trade. Center ${K} (${centerSource} grid); no wing 5–100 had netDebit>0 with risk ≤ reward.`,
          dc.proxyUrl);
      }
    } catch (_) { /* non-critical */ }
    return { ...out, status: 'no-wing', center: K, centerSource };
  }

  const W = pick.W;
  const kLower = K - W, kUpper = K + W;
  const lowerLeg = legFor(kLower);
  const centerLeg = legFor(K);
  const upperLeg = legFor(kUpper);
  if (!lowerLeg || !centerLeg || !upperLeg) {
    return { ...out, status: 'error', error: `selected wing ${W} leg vanished on re-fetch (K=${K})` };
  }

  const netDebit = pick.netDebit;
  const maxRisk = netDebit;
  const maxReward = W - netDebit;

  // Daily total-risk cap (2026-06-09): fly max loss = netDebit × 100.
  // Status-return: stays un-done so later ticks in the 9:35-9:45 window
  // retry if exposure clears.
  {
    const gxbfGate = await enforceRiskCap(env, etNow, 'gxbf', netDebit * 100);
    if (!gxbfGate.ok) {
      return { ...out, status: 'risk-cap-blocked', detail: gxbfGate.reason };
    }
  }

  const trade = {
    openDate: todayISO,
    openTimeET: `${String(etNow.getHours()).padStart(2,'0')}:${String(etNow.getMinutes()).padStart(2,'0')}`,
    label: 'GXBF',
    center: K,
    wing: W,
    centerSource,                  // 'oi' | 'vol' | 'vol-fallback' (hybrid per-day)
    centerSourceRequested: requestedSource,
    centerVol: computed.center,
    centerOI: computed.centerOI,
    spotEntry: parseFloat(spot.toFixed(2)),
    lowerStrike: kLower, centerStrike: K, upperStrike: kUpper,
    expDate: todayISO,
    lowerSymbol: lowerLeg.symbol, centerSymbol: centerLeg.symbol, upperSymbol: upperLeg.symbol,
    entryLowerMid:  parseFloat(lowerLeg.mid.toFixed(2)),
    entryCenterMid: parseFloat(centerLeg.mid.toFixed(2)),
    entryUpperMid:  parseFloat(upperLeg.mid.toFixed(2)),
    netDebit:  parseFloat(netDebit.toFixed(2)),
    maxRisk:   parseFloat(maxRisk.toFixed(2)),
    maxReward: parseFloat(maxReward.toFixed(2)),
    contracts: 1,
    // Live fields
    currentSpot:      parseFloat(spot.toFixed(2)),
    currentLowerMid:  parseFloat(lowerLeg.mid.toFixed(2)),
    currentCenterMid: parseFloat(centerLeg.mid.toFixed(2)),
    currentUpperMid:  parseFloat(upperLeg.mid.toFixed(2)),
    currentValue:     parseFloat(netDebit.toFixed(2)),
    currentPnl: 0,
    lastQuoteAt: new Date().toISOString(),
    status: 'filled',
  };

  // Write trade record FIRST (source of truth), then the done-key marker.
  await env.SIGNAL_KV.put('gxbf_open_trade', JSON.stringify(trade));
  await env.SIGNAL_KV.put(doneKey, 'filled', { expirationTtl: 86400 });

  console.log(`[gxbf] opened filled center=${K} (source=${centerSource}) wing=${W} netDebit=${netDebit.toFixed(2)} (risk ${maxRisk.toFixed(2)} ≤ reward ${maxReward.toFixed(2)})`);
  await logEvent(env, 'info', 'gxbf-open', 'opened filled', {
    center: K, wing: W, netDebit: parseFloat(netDebit.toFixed(2)),
    maxRisk: parseFloat(maxRisk.toFixed(2)), maxReward: parseFloat(maxReward.toFixed(2)),
    centerSource, centerSourceRequested: requestedSource,
    centerVol: computed.center, centerOI: computed.centerOI, spot: parseFloat(spot.toFixed(2)),
  });

  // Independent Discord notification (separate from the morning signal post).
  try {
    const dcRaw = await env.SIGNAL_KV.get('discord_config');
    if (dcRaw) {
      const dc = JSON.parse(dcRaw);
      const sourceLabel = centerSource === 'oi' ? 'OI-weighted' : centerSource === 'vol-fallback' ? 'Volume (OI fallback)' : 'Volume-weighted';
      const altLabel = centerSource === 'oi' ? `Volume ${computed.center}` : (computed.centerOI != null ? `OI ${computed.centerOI}` : null);
      if (dc.channelId) await sendDiscordDM(env, dc.channelId,
        `🦋 **GXBF opened** — SPX ${kLower}/${K}/${kUpper} CALL fly (wing ${W})\n` +
        `Net debit $${netDebit.toFixed(2)} · max risk $${(maxRisk*100).toFixed(0)} · max reward $${(maxReward*100).toFixed(0)} · 0DTE\n` +
        `Center ${K} (${sourceLabel} · live gamma calc)${altLabel ? ` · alt ${altLabel}` : ''} · spot ${spot.toFixed(2)}`,
        dc.proxyUrl);
    }
  } catch (_) { /* non-critical */ }

  return { ...out, status: 'opened', center: K, wing: W, netDebit, centerSource };
}

// Refresh live mids on the open GXBF trade. Reuses the master chain (zero
// extra Schwab calls). Mirrors refreshBobfLiveQuotes.
async function refreshGxbfLiveQuotes(env, token, etNow, preChain = null) {
  const raw = await env.SIGNAL_KV.get('gxbf_open_trade');
  if (!raw) return null;
  const trade = JSON.parse(raw);
  if (trade.status === 'closed' || trade.status === 'expired') return trade;

  try {
    const chain = preChain || await fetchSpxFullChain(token, trade.expDate, env);
    const { spot, callExpDateMap } = chain;
    const lower  = pickContractFromChain(callExpDateMap, trade.expDate, trade.lowerStrike);
    const center = pickContractFromChain(callExpDateMap, trade.expDate, trade.centerStrike);
    const upper  = pickContractFromChain(callExpDateMap, trade.expDate, trade.upperStrike);
    if (!lower || !center || !upper || lower.mid == null || center.mid == null || upper.mid == null) return trade;

    const newDebit = lower.mid - 2 * center.mid + upper.mid;
    trade.currentSpot      = spot ? parseFloat(spot.toFixed(2)) : trade.currentSpot;
    trade.currentLowerMid  = parseFloat(lower.mid.toFixed(2));
    trade.currentCenterMid = parseFloat(center.mid.toFixed(2));
    trade.currentUpperMid  = parseFloat(upper.mid.toFixed(2));
    trade.currentValue     = parseFloat(newDebit.toFixed(2));
    if (trade.status === 'filled') {
      trade.currentPnl = Math.round((trade.currentValue - trade.netDebit) * 100 * trade.contracts);
    }
    trade.lastQuoteAt = new Date().toISOString();
    await env.SIGNAL_KV.put('gxbf_open_trade', JSON.stringify(trade));
  } catch (e) { console.warn('[gxbf] refresh failed:', e.message); }
  return trade;
}

// Mark-to-market the open M8BF butterfly every market tick from the shared
// master chain (zero extra Schwab calls) and stash it in m8bf_live_<date>.
// Without this, GET /trade has no live mid and live.html falls back to the
// AT-EXPIRATION intrinsic, which overstates intraday profit while the short
// body still carries extrinsic (observed: card showed +$1,320 vs ~+$400 real
// at 11:05). Mirrors refreshBobf/GxbfLiveQuotes. M8BF is stateless (no
// m8bf_open_trade KV), so the trade is re-derived via the SHARED
// selectM8bfQualifying — guaranteeing the quoted legs == the /trade legs.
async function refreshM8bfLiveQuotes(env, token, etNow, preChain = null) {
  try {
    if (await m8bfBannedReason(env, etNow)) return;
    const sel = await selectM8bfQualifying(env, etNow);
    if (sel.status !== 'open' || !sel.qualifying) return;
    const q = sel.qualifying;
    const expDate = sel.todayT;  // M8BF is 0DTE
    const chain = preChain || await fetchSpxFullChain(token, expDate, env);
    const spot = chain.spot;
    // cp 0 = CALL fly, 1 = PUT fly. Long-fly net debit is the same convex
    // combination of the option mids either way: low − 2·center + high.
    const map = (q.cp === 1) ? chain.putExpDateMap : chain.callExpDateMap;
    if (!map) return;
    const lower  = pickContractFromChain(map, expDate, q.lower);
    const center = pickContractFromChain(map, expDate, q.center);
    const upper  = pickContractFromChain(map, expDate, q.upper);
    if (!lower || !center || !upper ||
        lower.mid == null || center.mid == null || upper.mid == null) return;
    const curVal = lower.mid - 2 * center.mid + upper.mid;
    const rec = {
      currentValue:     parseFloat(curVal.toFixed(2)),
      currentLowerMid:  parseFloat(lower.mid.toFixed(2)),
      currentCenterMid: parseFloat(center.mid.toFixed(2)),
      currentUpperMid:  parseFloat(upper.mid.toFixed(2)),
      currentSpot:      spot ? parseFloat(spot.toFixed(2)) : null,
      currentPnl:       Math.round((curVal - q.premium) * 100),  // 1 contract (M8BF convention everywhere)
      signal_time:      q.time,
      lastQuoteAt:      new Date().toISOString(),
    };
    await env.SIGNAL_KV.put(`m8bf_live_${sel.todayT}`, JSON.stringify(rec), { expirationTtl: 86400 });
  } catch (e) { console.warn('[m8bf] live refresh failed:', e.message); }
}

// Settle GXBF at SPX close. Long-call-fly intrinsic at SPX close S:
//   clamp( max(0,S−(K−W)) − 2·max(0,S−K) + max(0,S−(K+W)), 0, W )
//   pnl = (intrinsic − netDebit) × 100 × contracts
// (same per-point-per-contract convention as settleBobfEOD/settleStraddleEOD)
async function settleGxbfEOD(env, etNow, spxClose) {
  const raw = await env.SIGNAL_KV.get('gxbf_open_trade');
  if (!raw) return { status: 'no-trade' };
  const trade = JSON.parse(raw);
  if (trade.openDate !== isoDateET(etNow)) return { status: 'wrong-date', openDate: trade.openDate };
  if (trade.status === 'closed') return { status: 'already-closed' };
  if (trade.status === 'expired') return { status: 'expired-no-trade' };
  if (trade.status !== 'filled') return { status: trade.status };

  const W = trade.wing;
  const lowerI  = Math.max(spxClose - trade.lowerStrike,  0);
  const centerI = Math.max(spxClose - trade.centerStrike, 0);
  const upperI  = Math.max(spxClose - trade.upperStrike,  0);
  const rawIntrinsic = lowerI - 2 * centerI + upperI;
  const intrinsic = Math.min(Math.max(rawIntrinsic, 0), W);
  const pnl = Math.round((intrinsic - trade.netDebit) * 100 * trade.contracts);

  trade.status = 'closed';
  trade.closeDate = isoDateET(etNow);
  trade.spxClose = parseFloat(spxClose.toFixed(2));
  trade.closeIntrinsic = parseFloat(intrinsic.toFixed(2));
  trade.pnl = pnl;
  await env.SIGNAL_KV.put('gxbf_open_trade', JSON.stringify(trade));

  const logRaw = await env.SIGNAL_KV.get('gxbf_closed_log');
  const log = logRaw ? JSON.parse(logRaw) : [];
  log.unshift(trade);
  await env.SIGNAL_KV.put('gxbf_closed_log', JSON.stringify(log.slice(0, 30)));

  await upsertHistoryGitHub(env, trade.openDate, { gxbfPL: pnl });
  return { status: 'settled', pnl, center: trade.centerStrike, wing: W,
           strikes: [trade.lowerStrike, trade.centerStrike, trade.upperStrike],
           debit: trade.netDebit, intrinsic };
}

// ════════════════════════════════════════════════════════════════════
// TAIL HEDGE — live-trade parity (2026-06-22). Brings Tail Hedge onto the
// SAME conveyor as strad/bobf/gxbf: cron freezes the open → intraday P&L →
// EOD settle writes tailPL → first profitable day STOPS the campaign.
// Entry cost basis = candidate MID (matches the backtest that produced the
// historical tailPL column; skipper tracks its own real fill separately).
// ────────────────────────────────────────────────────────────────────

// Freeze today's tail open from the 9:45 put snapshot once we're past 9:45 ET
// on a TRADE day. Idempotent (first call wins). Called by BOTH the cron (robust,
// no page-poll needed) and GET /tail-today.
async function freezeTailOpenIfDue(env, etNow, line = null) {
  const todayISO = isoDateET(etNow);
  const existing = await env.SIGNAL_KV.get(`tail_open_trade_${todayISO}`);
  if (existing) return JSON.parse(existing);
  const statusLine = line || await getTailHedgeStatusLine(env);
  const h = etNow.getHours(), m = etNow.getMinutes();
  const pastEntry = h > 9 || (h === 9 && m >= 45);
  if (!(statusLine.includes('▶ TRADE') && pastEntry && h < 16)) return null;
  const snapRaw = await env.SIGNAL_KV.get(`tail_put_snap_${todayISO}`);
  const snap = snapRaw ? JSON.parse(snapRaw) : null;
  if (!snap || !Array.isArray(snap.puts) || !snap.puts.length) return null;
  const e0 = snap.puts[0].e;
  const candidate = snap.puts.filter(p => p.e === e0)
    .sort((a, b) => Math.abs(a.d + 0.10) - Math.abs(b.d + 0.10))[0] || null;
  if (!candidate) return null;
  const entryMid = (candidate.b != null && candidate.a != null)
    ? parseFloat(((candidate.b + candidate.a) / 2).toFixed(2))
    : (candidate.a ?? candidate.b ?? null);
  const tailOpen = {
    openDate: todayISO, openTimeET: '09:45', strike: candidate.k, expDate: candidate.e,
    entryBid: candidate.b, entryAsk: candidate.a, entryMid, contracts: 1, status: 'filled',
    label: 'Tail Hedge', currentSpot: snap.spot ?? null,
  };
  await env.SIGNAL_KV.put(`tail_open_trade_${todayISO}`, JSON.stringify(tailOpen), { expirationTtl: 7 * 86400 });
  return tailOpen;
}

// Refresh intraday mid + live P&L on today's open tail put (mirrors
// refreshBobfLiveQuotes). Single long put: currentPnl = (mid − entryMid)×100×qty.
async function refreshTailLiveQuotes(env, etNow, preChain = null) {
  const todayISO = isoDateET(etNow);
  const raw = await env.SIGNAL_KV.get(`tail_open_trade_${todayISO}`);
  if (!raw) return null;
  const trade = JSON.parse(raw);
  if (trade.status === 'closed') return trade;
  try {
    if (!preChain || !preChain.putExpDateMap) return trade;
    const put = pickContractFromChain(preChain.putExpDateMap, trade.expDate, trade.strike);
    if (!put || put.mid == null) return trade;
    const entryCost = trade.entryMid != null ? trade.entryMid
      : (trade.entryBid != null && trade.entryAsk != null) ? (trade.entryBid + trade.entryAsk) / 2
      : (trade.entryAsk ?? trade.entryBid);
    trade.currentSpot  = preChain.spot ? parseFloat(preChain.spot.toFixed(2)) : trade.currentSpot;
    trade.currentMid   = parseFloat(put.mid.toFixed(2));
    trade.currentValue = parseFloat(put.mid.toFixed(2));
    trade.currentPnl   = Math.round((put.mid - entryCost) * 100 * (trade.contracts || 1));
    trade.lastQuoteAt  = new Date().toISOString();
    await env.SIGNAL_KV.put(`tail_open_trade_${todayISO}`, JSON.stringify(trade), { expirationTtl: 7 * 86400 });
  } catch (e) { console.warn('[tail] refresh failed:', e.message); }
  return trade;
}

// Settle Tail Hedge at SPX close. Single long 0DTE SPXW put:
//   intrinsic = max(0, strike − spxClose);  pnl = (intrinsic − entryMid) × 100 × contracts
// (mirrors research_tail_hedge.py; same ×100×contracts + Math.round as the other settles)
// On the first PROFITABLE day, flip tail_trigger_state → RESOLVED so the campaign
// stops signalling TRADE until COR1M crosses below 7.75 again.
async function settleTailEOD(env, etNow, spxClose) {
  const todayISO = isoDateET(etNow);
  const raw = await env.SIGNAL_KV.get(`tail_open_trade_${todayISO}`);
  if (!raw) return { status: 'no-trade' };
  const trade = JSON.parse(raw);
  if (trade.openDate !== todayISO) return { status: 'wrong-date', openDate: trade.openDate };
  if (trade.status === 'closed') return { status: 'already-closed', pnl: trade.pnl };

  let S = spxClose;
  if (S == null) { try { S = await getSpxCloseForDate(todayISO); } catch (_) {} }
  if (S == null) return { status: 'no-close' };

  const contracts = trade.contracts || 1;
  const entryCost = trade.entryMid != null ? trade.entryMid
    : (trade.entryBid != null && trade.entryAsk != null) ? (trade.entryBid + trade.entryAsk) / 2
    : (trade.entryAsk ?? trade.entryBid);
  const intrinsic = Math.max(0, trade.strike - S);
  const pnl = Math.round((intrinsic - entryCost) * 100 * contracts);

  trade.status = 'closed';
  trade.closeDate = todayISO;
  trade.spxClose = parseFloat(S.toFixed(2));
  trade.closeIntrinsic = parseFloat(intrinsic.toFixed(2));
  trade.entryMid = parseFloat(Number(entryCost).toFixed(2));
  trade.pnl = pnl;
  await env.SIGNAL_KV.put(`tail_open_trade_${todayISO}`, JSON.stringify(trade), { expirationTtl: 7 * 86400 });

  const logRaw = await env.SIGNAL_KV.get('tail_closed_log');
  const log = logRaw ? JSON.parse(logRaw) : [];
  log.unshift(trade);
  await env.SIGNAL_KV.put('tail_closed_log', JSON.stringify(log.slice(0, 30)));

  await upsertHistoryGitHub(env, todayISO, { tailPL: pnl });

  // First profitable day STOPS the campaign (previously only the offline bundle
  // did this via exit_reason='profitable'; now the live worker ends it too).
  if (pnl > 0) {
    try {
      const stRaw = await env.SIGNAL_KV.get('tail_trigger_state');
      const st = stRaw ? JSON.parse(stRaw) : null;
      if (st && st.state === 'TRIGGERED') {
        st.state = 'RESOLVED'; st.resolvedOn = todayISO; st.exitReason = 'profitable'; st.exitPnl = pnl;
        await env.SIGNAL_KV.put('tail_trigger_state', JSON.stringify(st));
        _tailHedgeCache = { value: null, fetchedAt: 0 };  // bust the 5-min status cache
      }
    } catch (_) { /* campaign-stop is best-effort */ }
  }
  return { status: 'settled', pnl, strike: trade.strike, entryMid: entryCost, intrinsic, spxClose: S };
}

async function handleScheduled(env) {
  const etNow = toET();
  const dow = etNow.getDay();

  // Not a weekday → skip
  if (dow === 0 || dow === 6) return { status: 'skipped', reason: 'weekend' };

  // Not a trading day (holiday) → skip
  if (!isTrade(etNow)) return { status: 'skipped', reason: 'holiday' };

  const etHour = etNow.getHours();
  const etMin = etNow.getMinutes();
  const todayISO = `${etNow.getFullYear()}-${String(etNow.getMonth()+1).padStart(2,'0')}-${String(etNow.getDate()).padStart(2,'0')}`;

  // EOD self-ping: fire on ANY cron tick after 16:16 ET when eod_done_<date> is
  // unset. Previously only 16:16–16:25 ET triggered EOD — if Cloudflare dropped
  // every tick in that 9-min window (as happened 2026-04-17, 4+ hours of cron
  // silence), EOD stayed missing until something external hit /gex. With this
  // widening, the dedicated 17:17 ET cron (and any `*/2` afternoon tick that
  // makes it through) rescues the EOD write without needing a browser hit.
  const afterEOD = (etHour === 16 && etMin >= 16) || etHour >= 17;
  const eodKey = `eod_done_${todayISO}`;
  const eodAlreadyDone = afterEOD ? await env.SIGNAL_KV.get(eodKey) : null;
  const isEOD = afterEOD && !eodAlreadyDone;

  const isMarket  = (etHour > 9 || (etHour === 9 && etMin >= 30)) && etHour < 16;

  // Always poll Discord during market hours
  let discordResult = {};
  if (isMarket && env.DISCORD_USER_TOKEN) {
    discordResult = await pollDiscordSignals(env);
  }

  // EOD cron: capture vixClose + spxClose + m8bfPL + backfill any missing m8bfWR
  if (isEOD) {
    const eodResult = await handleEOD(env, etNow);
    // Only mark eod_done after real write. If Schwab token expired AND Stooq
    // failed, fields object is empty — leaving the flag unset lets the next
    // cron tick (or /gex hit) retry instead of silently giving up.
    if (eodResult.wroteFields) {
      await env.SIGNAL_KV.put(eodKey, 'done', { expirationTtl: 86400 });
    }
    let backfillWR = {}, backfillPL = {};
    try { backfillWR = await backfillMissingWR(env); } catch(e) { backfillWR = { error: e.message }; }
    try { backfillPL = await backfillMissingPL(env); } catch(e) { backfillPL = { error: e.message }; }
    // Append today's raw signals to scraped_signals.csv on GitHub
    let scrapeAppend = {};
    try { scrapeAppend = await appendScrapedSignals(env, etNow); } catch(e) { scrapeAppend = { error: e.message }; }
    // Heavy GitHub-write jobs MOVED to their own 16:25 tick (2026-06-12,
    // lessons P17): bundled here they pushed the EOD invocation past the
    // subrequest budget — settle succeeded, every later persist silently
    // died (06-11: fly marks/surface/decomp/cyclicality all missing while
    // captures sat healthy in KV). See eodAuxJobs().
    return { ...eodResult, discord: discordResult, backfill_wr: backfillWR, backfill_pl: backfillPL, scrape_append: scrapeAppend };
  }

  // ── 16:25–16:40 ET aux tick (2026-06-12, lessons P17): the GitHub-heavy
  // research jobs get their own invocation so the settle's subrequest
  // budget can't starve them. Once per day; evening 18:00 retries remain.
  if (etHour === 16 && etMin >= 25 && etMin < 40) {
    const auxKey = `eod_aux_${todayISO}`;
    if (!(await env.SIGNAL_KV.get(auxKey))) {
      await env.SIGNAL_KV.put(auxKey, 'running', { expirationTtl: 86400 });
      let ok = true;
      try { await persistResearchArtifacts(env, etNow); } catch (e) { ok = false; console.warn('[research-persist]', e.message); }
      try { await computeVixDecompDaily(env, etNow); } catch (e) { ok = false; console.warn('[vix-decomp]', e.message); }
      try { await appendCyclicalityDays(env); } catch (e) { ok = false; console.warn('[cyclelab]', e.message); }
      try { await appendCyclicalityDays(env, { symbol: '%24NDX', file: 'cyclicality_ndx.json' }); } catch (e) { console.warn('[cyclelab-ndx]', e.message); }
      if (etNow.getDay() === 5) {
        try { await cotWeeklyRefresh(env); } catch (e) { console.warn('[cot]', e.message); }
      }
      if (ok) await env.SIGNAL_KV.put(auxKey, 'done', { expirationTtl: 86400 });
      else await env.SIGNAL_KV.delete(auxKey);   // retry next tick within the window
      return { eod_aux: ok ? 'done' : 'partial-retry' };
    }
  }

  // ── Master SPX chain fetch — ONE call per market tick, shared across all
  //    chain-consuming handlers (GEX, diagonal, straddle, BOBF). Cuts Schwab
  //    API usage from ~7 chain calls per tick to 2 (one CALL + one PUT).
  let masterChain = null;
  let schwabToken = null;
  if (isMarket) {
    // Try Schwab token first — but a failure here MUST NOT block the master
    // chain fetch, which has a Tasty fallback. Schwab-token-dependent ops
    // (order placement, GEX update) still gate on `schwabToken` below.
    try { schwabToken = await getAccessToken(env); }
    catch (e) { console.warn('[chain] Schwab token unavailable, chain will use Tasty:', e.message); }
    try {
      masterChain = await fetchMasterSpxChain(schwabToken, env);
    } catch (e) {
      console.warn('[chain] master chain fetch failed (both Schwab and Tasty):', e.message);
      // Handlers will fall through to their own targeted fetches.
    }
  }

  // ── GEX update during market hours (every cron tick) ──
  let gexResult = {};
  if (isMarket && schwabToken) {
    try {
      gexResult = await handleGEXUpdate(env, schwabToken, masterChain);
    } catch (e) {
      gexResult = { gex: 'error', error: e.message };
      console.warn('[proxy] GEX update failed:', e.message || e);
    }
  }

  // ── COR1M + VVIX cloud capture (2026-06-09 — machine-independence) ──
  // Schwab quotes $COR1M and $VVIX live (validated against ThetaData:
  // exact match). The worker samples them itself so the Tail Hedge status,
  // Diagonal COR1M gate, and the history cor1m column no longer depend on
  // the user's Mac being on (ThetaData/LaunchAgent = research-only now).
  //  • 9:30-9:36: every tick until the day's open is captured
  //  • after: every 5th minute → intraday series for cross detection
  if (isMarket && schwabToken) {
    try { await captureCor1mVvix(env, etNow, schwabToken); }
    catch (e) { console.warn('[cor1m] capture failed:', e.message || e); }
    // Research capture: morning (~10:00) GEX snapshot → gex_daily_<date>.am
    // (EOD persist pairs it with the close snapshot into data/gex_daily/)
    try {
      const hG = etNow.getHours(), mG = etNow.getMinutes();
      if (hG === 9 && mG >= 55 || hG === 10 && mG <= 10) {
        const kG = `gex_daily_${isoDateET(etNow)}`;
        if (!(await env.SIGNAL_KV.get(kG))) {
          const cur = await env.SIGNAL_KV.get('gex_current');
          if (cur) {
            const g = JSON.parse(cur);
            await env.SIGNAL_KV.put(kG, JSON.stringify({ am: {
              t: g.timestamp, spot: g.spot, regime: g.regime, totalGex: g.totalGex,
              flip: g.flipStrike ?? null, maxPos: g.maxPosStrike ?? null, maxNeg: g.maxNegStrike ?? null,
            } }), { expirationTtl: 90 * 86400 });
          }
        }
      }
    } catch (e) { console.warn('[gex-daily]', e.message); }
    // Research capture: 9:45-ish SPX put snapshot (Tail Hedge dataset, ThetaData-free)
    try { await captureTailPutSnap(env, etNow, masterChain); } catch (e) { console.warn('[tail-snap]', e.message); }
    // Tail Hedge live-trade parity: freeze today's open at/after 9:45 (robust —
    // no page-poll needed) and refresh its intraday P&L every tick.
    try { await freezeTailOpenIfDue(env, etNow); } catch (e) { console.warn('[tail-freeze]', e.message); }
    try { await refreshTailLiveQuotes(env, etNow, masterChain); } catch (e) { console.warn('[tail-refresh]', e.message); }
    // Research capture: Diagonal 12:30 put chain + GXBF 9:35 call chain
    // (with these all three bt datasets grow Schwab-only — ThetaData optional)
    try { await captureDiagChainSnap(env, etNow, masterChain); } catch (e) { console.warn('[diag-snap]', e.message); }
    try { await captureGxbfChainSnap(env, etNow, masterChain); } catch (e) { console.warn('[gxbf-snap]', e.message); }
    // Research capture: ~15:45 30DTE smile (VIX decomposition dataset)
    try { await captureVixSurfaceSnap(env, etNow, schwabToken); } catch (e) { console.warn('[surface-snap]', e.message); }
    // CycleLab live actual — today's session-so-far into KV (every 5 min)
    try { await captureCycTodaySlots(env, etNow, schwabToken); } catch (e) { console.warn('[cyc-live]', e.message); }
  }

  // ── Diagonal trade: open/close at 12:30 ET. Idempotent via diag_done_<date>.
  // Window 12:30–12:40 ET gives up to 5 retry attempts (cron is */2). We only
  // mark `diag_done` after a clean run — if the chain fetch / GitHub commit
  // throws, the next tick within the window retries automatically. After 12:40
  // we stop trying for the day and the live page shows the error state.
  let diagResult = {};
  const diagDoneKey = `diag_done_${todayISO}`;
  const isDiagonalEntry = (etHour === 12 && etMin >= 30 && etMin < 40);
  if (isDiagonalEntry) {
    const diagDone = await env.SIGNAL_KV.get(diagDoneKey);
    if (!diagDone) {
      try {
        diagResult = await handleDiagonalTrade(env, etNow, masterChain);
        // Only mark done when we either (a) opened a new trade, (b) cleanly
        // skipped per signal, OR (c) closed a prior trade without error.
        // Transient errors (token, chain, GitHub) leave the slot unmarked so
        // the next 2-min tick retries within the 12:30–12:40 window.
        const hadError = !!(diagResult.error || diagResult.openError || diagResult.closeError);
        if (!hadError) {
          await env.SIGNAL_KV.put(diagDoneKey, 'done', { expirationTtl: 86400 });
        } else {
          console.warn('[diag] not marking done — will retry next tick:', JSON.stringify(diagResult));
        }
      } catch (e) {
        diagResult = { diagonal: 'error', error: e.message };
        console.warn('[diag] handler threw:', e.message);
      }
    }
  }

  // ── Refresh live quotes on the open diagonal, straddle, AND BOBF every market tick ──
  //    All three reuse the master chain fetched above (zero extra Schwab calls).
  if (isMarket && schwabToken) {
    // ── Straddle RETRY-OPEN ──────────────────────────────────────────
    // If today's morning signal said theme=strad but no straddle_open_trade
    // record exists, the 9:32 open attempt failed (Schwab glitch / chain
    // no-quote / a thrown exception we didn't see). Retry every minute
    // until cutoff so a transient blip doesn't lose us the whole day.
    // Once it succeeds, refreshStraddleLiveQuotes (below) will watch the
    // debit and flip to 'filled' the moment price hits the limit.
    try {
      const todayISORet = isoDateET(etNow);
      const stradExisting = await env.SIGNAL_KV.get('straddle_open_trade');
      const stradExistingObj = stradExisting ? JSON.parse(stradExisting) : null;
      const haveTodayStrad = stradExistingObj && stradExistingObj.openDate === todayISORet;
      const beforeCutoff = etNow.getHours() < STRADDLE_WORK_CUTOFF_HR ||
        (etNow.getHours() === STRADDLE_WORK_CUTOFF_HR && etNow.getMinutes() < STRADDLE_WORK_CUTOFF_MIN);
      if (!haveTodayStrad && beforeCutoff) {
        const msdRaw = await env.SIGNAL_KV.get(`morning_signal_data_${todayISORet}`);
        if (msdRaw) {
          const msd = JSON.parse(msdRaw);
          if (msd.theme === 'strad') {
            try {
              const retrySig = { theme: 'strad', badge: msd.badge || 'STRADDLE', rec: msd.rec };
              const trade = await openStraddleTrade(env, schwabToken, etNow, retrySig, masterChain);
              await env.SIGNAL_KV.put('straddle_open_trade', JSON.stringify(trade));
              await logEvent(env, 'info', 'strad-retry', `retry-opened ${trade.status}`, {
                strike: trade.strike, entryDebit: trade.entryDebit, maxDebit: trade.maxDebit,
                attemptedAt: `${etNow.getHours()}:${String(etNow.getMinutes()).padStart(2,'0')} ET`,
              });
              // Clear the cosmetic skip so the live page flips off "strad-missed"
              await env.SIGNAL_KV.delete(`straddle_skip_${todayISORet}`);
              console.log(`[strad-retry] opened ${trade.status} K=${trade.strike} debit=${trade.entryDebit}`);
            } catch (rErr) {
              // Don't spam the log on every minute — but keep console visibility.
              console.warn('[strad-retry] still failing:', rErr.message);
            }
          }
        }
      }
    } catch (e) { console.warn('[strad-retry] outer:', e.message); }

    try {
      await refreshDiagonalLiveQuotes(env, schwabToken, masterChain);
      await refreshStraddleLiveQuotes(env, schwabToken, etNow, masterChain);
      await refreshBobfLiveQuotes(env, schwabToken, etNow, masterChain);
      await refreshGxbfLiveQuotes(env, schwabToken, etNow, masterChain);
      await refreshM8bfLiveQuotes(env, schwabToken, etNow, masterChain);
    } catch (e) {
      console.warn('[live] refresh failed:', e.message);
    }
    // Research capture: archive each open fly\'s mid every 5 min (TP/stop research)
    try { await archiveFlyMarks(env, etNow); } catch (e) { console.warn('[fly-marks]', e.message); }
  }

  // ── BOBF entry: check every market tick during 10:29-12:15 ET window ──
  //    Also reuses the master chain. handleBobfEntry is self-retrying (it
  //    re-evaluates each tick when no open trade exists); refreshBobfLiveQuotes
  //    below handles working→filled transitions once a trade is recorded.
  let bobfResult = {};
  if (isMarket) {
    try { bobfResult = await handleBobfEntry(env, etNow, masterChain); }
    catch (e) {
      bobfResult = { bobf: 'error', error: e.message };
      console.warn('[bobf] entry failed:', e.message);
      // Upgrade silent failure → KV log so we can SEE why entry threw.
      try { await logEvent(env, 'error', 'bobf-open', `entry attempt threw`, { msg: e.message, stack: (e.stack || '').slice(0, 300) }); } catch (_) {}
    }
  }

  // ── GXBF entry: retry every market tick during the 9:35-9:45 ET window ──
  // 2026-06-09 FIX: window updated from 9:33 → 9:35 ET (see gxbfInWindow doc).
  // The chain quotes settle in the first ~5 minutes of regular session, so
  // 09:33 fires were reading transient post-open quotes for the gamma center.
  // Retry every tick (mirrors how BOBF retries every tick) until the entry
  // completes or the window passes. STRATEGY INDEPENDENCE:
  // gated solely on GXBF's OWN theme, read from the persisted morning signal
  // (morning_signal_data_<date>). Never consults M8BF/Straddle/BOBF state.
  let gxbfResult = {};
  if (isMarket && gxbfInWindow(etNow)) {
    const gxbfDone = await env.SIGNAL_KV.get(`gxbf_done_${todayISO}`);
    if (!gxbfDone) {
      try {
        const msdRaw = await env.SIGNAL_KV.get(`morning_signal_data_${todayISO}`);
        const msd = msdRaw ? JSON.parse(msdRaw) : null;
        if (msd && msd.theme === 'gxbf') {
          gxbfResult = await handleGxbfEntry(env, etNow, msd, masterChain);
        } else {
          gxbfResult = { status: 'not-gxbf-theme', theme: msd ? msd.theme : 'pending' };
        }
      } catch (e) { gxbfResult = { gxbf: 'error', error: e.message }; console.warn('[gxbf] entry failed:', e.message); }
    }
  }

  // ── 9:35 ET morning-signal self-check + Discord alert ──
  // If by 9:35 ET we still don't have a 'sent' marker for today's morning
  // signal, fire ONE Discord alert so silent failures don't rot for hours.
  // Most failures self-heal within 1-2 cron ticks; this only catches the
  // "stuck for 5+ min" case (Schwab outage, stuck claim, cron stall, etc.).
  // Rate-limited via `morning_alert_<date>` flag (24-hr TTL).
  const morningAlertKey = `morning_alert_${todayISO}`;
  try {
    const past935 = etHour > 9 || (etHour === 9 && etMin >= 35);
    if (past935 && isMarket) {
      const alreadyAlerted = await env.SIGNAL_KV.get(morningAlertKey);
      const currentStatus = await env.SIGNAL_KV.get(`morning_signal_${todayISO}`);
      // An in-flight 'claim:' means a tick is actively sending RIGHT NOW
      // (claims are ≤40s and self-release). That is NOT "missing" — it's
      // in-progress. Only alarm on GENUINE absence (null/expired). This
      // kills the false "Morning signal MISSING" DM that fired during the
      // self-heal recovery window on 2026-05-15.
      const inFlight = !!(currentStatus && currentStatus.startsWith('claim:'));
      if (!alreadyAlerted && currentStatus !== 'sent' && !inFlight) {
        const minsLate = (etHour - 9) * 60 + etMin - 30;
        const stRaw = await env.SIGNAL_KV.get('schwab_refresh_state');
        const refreshState = stRaw ? JSON.parse(stRaw) : null;
        const schwabHealthy = !refreshState || refreshState.ok !== false;
        const claimStuck = !!(currentStatus && currentStatus.startsWith('claim:'));
        const reasons = [];
        if (!schwabHealthy) {
          const errs = refreshState?.consecutiveErrors || 0;
          reasons.push(`Schwab refresh degraded (${errs} errors)`);
        }
        if (claimStuck) {
          const parts = currentStatus.split(':');
          const claimAgeMs = parts[2] ? Date.now() - parseInt(parts[2], 10) : 0;
          reasons.push(`stuck claim (age ${Math.round(claimAgeMs/1000)}s)`);
        }
        if (!currentStatus) reasons.push('no claim yet — cron may have stalled');
        const dcRaw = await env.SIGNAL_KV.get('discord_config');
        if (dcRaw) {
          const dc = JSON.parse(dcRaw);
          if (dc.channelId) {
            const result = await sendDiscordDM(env, dc.channelId,
              `🚨 **Morning signal MISSING** — ${minsLate} min past 9:30 ET\n` +
              `State: \`${currentStatus || 'none'}\`\n` +
              (reasons.length ? `Suspected: ${reasons.join('; ')}\n` : '') +
              `→ Check worker logs, hit \`/trigger\` to retry, or re-auth Schwab via dashboard.`,
              dc.proxyUrl);
            if (result.ok) {
              await env.SIGNAL_KV.put(morningAlertKey, 'sent', { expirationTtl: 86400 });
              console.warn(`[morning-check] FIRED ALERT: ${minsLate} min late, status=${currentStatus || 'none'} via ${result.source}`);
              await logEvent(env, 'error', 'morning-check', `morning signal ${minsLate} min late — Discord alert sent`,
                             { status: currentStatus || null, reasons, source: result.source });
            } else {
              console.warn('[morning-check] post failed:', result.error);
            }
          }
        }
      }
    }
  } catch (selfCheckErr) {
    console.error('[morning-check] failed:', selfCheckErr.message);
  }

  // ── Morning signal: retries every cron tick until sent ──
  const morningDoneKey = `morning_signal_${todayISO}`;
  const morningDone = await env.SIGNAL_KV.get(morningDoneKey);
  const preMarket = etHour < 9 || (etHour === 9 && etMin < 30);

  // Self-heal: if morning signal already sent today but no straddle_skip
  // recorded AND no live straddle trade for today, derive + write a skip
  // reason so the live page shows the correct status. Covers days when the
  // morning cron ran BEFORE the skip-write code was deployed (and any future
  // case where the skip write was lost mid-flight).
  if (morningDone === 'sent' && isMarket) {
    const stradSkipKey = `straddle_skip_${todayISO}`;
    const haveSkip = await env.SIGNAL_KV.get(stradSkipKey);
    if (!haveSkip) {
      const stradOpenRaw = await env.SIGNAL_KV.get('straddle_open_trade');
      const stradOpen = stradOpenRaw ? JSON.parse(stradOpenRaw) : null;
      const haveStrad = stradOpen && stradOpen.openDate === todayISO;
      if (!haveStrad) {
        // Recompute signal cheaply just to get rec/theme (mirrors morning block).
        try {
          const recoveryToken = await getAccessToken(env);
          // Pull just the prices we need; we don't need full vix history here.
          const [vixHist, spxHist] = await Promise.all([
            fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24VIX&periodType=day&period=3&frequencyType=minute&frequency=1&needExtendedHoursData=true`, recoveryToken, env),
            fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24SPX&periodType=day&period=3&frequencyType=minute&frequency=1&needExtendedHoursData=true`, recoveryToken, env),
          ]);
          // Derive vixToday/vixYClose/vixYOpen/spxGapPct from candles
          const todayStr = etNow.toDateString();
          const vCandles = (vixHist.candles || []).slice().sort((a,b) => a.datetime - b.datetime);
          const vToday = vCandles.find(c => toET(new Date(c.datetime)).toDateString() === todayStr && toET(new Date(c.datetime)).getHours() === 9 && toET(new Date(c.datetime)).getMinutes() >= 30);
          const vYesterday = vCandles.filter(c => toET(new Date(c.datetime)).toDateString() !== todayStr);
          const vY = vYesterday[vYesterday.length - 1];
          const vYOpen = vYesterday.find(c => toET(new Date(c.datetime)).getHours() === 9 && toET(new Date(c.datetime)).getMinutes() >= 30);
          const sCandles = (spxHist.candles || []).slice().sort((a,b) => a.datetime - b.datetime);
          const sToday = sCandles.find(c => toET(new Date(c.datetime)).toDateString() === todayStr && toET(new Date(c.datetime)).getHours() === 9 && toET(new Date(c.datetime)).getMinutes() >= 30);
          const sYesterday = sCandles.filter(c => toET(new Date(c.datetime)).toDateString() !== todayStr);
          const sY = sYesterday[sYesterday.length - 1];
          const vixT = vToday?.open, vixYC = vY?.close, vixYO = vYOpen?.open;
          const spxGap = (sToday?.open != null && sY?.close != null) ? ((sToday.open - sY.close) / sY.close) * 100 : 0;
          if (vixT != null && vixYC != null && vixYO != null) {
            const recoverySig = calculateSignal({
              vixToday: vixT, vixYOpen: vixYO, vixYClose: vixYC,
              spxGapPct: spxGap, etDate: etNow,
            });
            if (recoverySig.theme !== 'strad') {
              await env.SIGNAL_KV.put(stradSkipKey, JSON.stringify({
                theme: recoverySig.theme,
                rec: recoverySig.rec,
                recordedAt: new Date().toISOString(),
                source: 'self-heal',
              }), { expirationTtl: 86400 });
              console.log(`[strad] self-heal wrote skip: theme=${recoverySig.theme} rec=${recoverySig.rec}`);
            }
          }
        } catch (e) { console.warn('[strad] self-heal failed:', e.message); }
      }
    }
  }

  if (morningDone === 'sent' || globalThis.__morningSentDay === todayISO || preMarket) {
    return { status: 'discord_poll', discord: discordResult, gex: gexResult, diagonal: diagResult, time: `${etHour}:${String(etMin).padStart(2,'0')} ET` };
  }

  // ── Stuck-claim self-heal + notify ──
  // Claims carry a timestamp suffix (claim:<uuid>:<ms>). If we find one >40s
  // old here (post-9:30 ET) it means the previous tick crashed between claim
  // and 'sent'. The send path is now ≤~35s, so a claim older than 40s is
  // genuinely dead — clear it immediately and let THIS tick retry. Combined
  // with the 90s claim TTL, an orphaned claim can never block more than ~40s.
  if (morningDone && morningDone.startsWith('claim:')) {
    const parts = morningDone.split(':');
    const claimTsMs = parseInt(parts[2] || '0', 10);
    const ageMs = claimTsMs ? Date.now() - claimTsMs : 0;
    if (claimTsMs && ageMs > 40_000) {
      const ageS = Math.round(ageMs / 1000);
      console.warn(`[proxy] Stuck claim detected (age ${ageS}s) — clearing and notifying`);
      await env.SIGNAL_KV.delete(morningDoneKey);
      // Fire-and-forget Discord notification
      try {
        const dcRaw = await env.SIGNAL_KV.get('discord_config');
        if (dcRaw) {
          const dc = JSON.parse(dcRaw);
          if (dc.channelId) {
            await sendDiscordDM(env, dc.channelId,
              `⚠️ **Stuck morning claim** (${ageS}s old) — cleared, retrying this tick.`,
              dc.proxyUrl);
          }
        }
      } catch (notifyErr) {
        console.warn('[proxy] stuck-claim notify failed:', notifyErr.message || notifyErr);
      }
      // Fall through: acquire a fresh claim below
    } else if (claimTsMs && ageMs <= 40_000) {
      // Another tick owns a fresh claim (<40s, still actively sending) —
      // don't stomp it. It will either finish ('sent') or self-release.
      return {
        status: 'claim_in_flight',
        claim: morningDone,
        age_ms: ageMs,
        time: `${etHour}:${String(etMin).padStart(2,'0')} ET`,
      };
    }
    // If claimTsMs === 0 (legacy value without timestamp) we also fall through;
    // short TTL will age it out on its own.
  }

  // ── Claim the slot BEFORE doing slow API work ──
  // Concurrent cron ticks can pass the gate above before any of them writes,
  // because the slot claim used to live ~30s later (after VIX/SPX fetches).
  // We write a unique token, wait for KV to propagate, then verify our token won.
  //
  // TTL is 90s (not 300s, not 86400s): the send path is now fast (≤~35s:
  // 20s VIX race + 12s fallback + quick compute/post), so a healthy claim
  // never lives long. If a tick is hard-killed mid-send, the claim
  // self-expires within 90s and the next minute's cron re-fires — no
  // 3-min stuck-claim window. 'sent' marker still uses 86400s.
  const claimToken = crypto.randomUUID();
  const claimValue = `claim:${claimToken}:${Date.now()}`;
  await env.SIGNAL_KV.put(morningDoneKey, claimValue, { expirationTtl: 90 });
  await new Promise(r => setTimeout(r, 1500)); // let concurrent ticks also write
  const claimCheck = await env.SIGNAL_KV.get(morningDoneKey, { cacheTtl: 30 });
  if (claimCheck !== claimValue) {
    console.log(`[proxy] Lost claim race (saw ${claimCheck}, mine was ${claimValue}) — skipping`);
    return { status: 'duplicate_skipped', claimWinner: claimCheck, time: `${etHour}:${String(etMin).padStart(2,'0')} ET` };
  }

  console.log('[proxy] Morning window — sending signal');

  // 1. Get access token
  const token = await getAccessToken(env);

  // 2. Fetch VIX 5-day history → yesterday open + close
  const end = Date.now();
  const start = end - 5 * 24 * 60 * 60 * 1000;
  const vixHistUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24VIX&periodType=day&period=5&frequencyType=minute&frequency=1&startDate=${start}&endDate=${end}&needExtendedHoursData=true`;
  const vixHist = await fetchSchwabJSON(vixHistUrl, token, env);
  if (!vixHist.candles || !vixHist.candles.length) throw new Error('No VIX history data');

  const candles = vixHist.candles;
  const todayStr = etNow.toDateString();

  // Find yesterday's trading day
  let yDate = null;
  for (let i = candles.length - 1; i >= 0; i--) {
    const d = new Date(candles[i].datetime);
    const dET = toET(d);
    if (dET.toDateString() !== todayStr) { yDate = dET.toDateString(); break; }
  }

  let vixYOpen = null, vixYClose = null;
  if (yDate) {
    const yCandles = candles.filter(c => toET(new Date(c.datetime)).toDateString() === yDate);
    yCandles.sort((a, b) => a.datetime - b.datetime);
    const openCandle = yCandles.find(c => {
      const d = toET(new Date(c.datetime));
      return d.getHours() === 9 && d.getMinutes() >= 30 && d.getMinutes() <= 35;
    });
    const closeCandle = yCandles.slice().reverse().find(c => {
      const d = toET(new Date(c.datetime));
      return d.getHours() === 16 && d.getMinutes() >= 10 && d.getMinutes() <= 15;
    }) || yCandles[yCandles.length - 1];

    if (openCandle) vixYOpen = parseFloat(openCandle.open.toFixed(2));
    if (closeCandle) vixYClose = parseFloat(closeCandle.close.toFixed(2));
  }

  // FALLBACK ONLY (2026-06-22 fix). quote.closePrice is NOT holiday-aware:
  // after a market holiday it returns the HOLIDAY's phantom close instead of the
  // real prior-session close (Juneteenth 2026-06-19 → 16.78 vs the true 6-18
  // close 16.40), which flips the overnight-VIX sign and mis-fires the Straddle
  // /GXBF gate. The minute-candle path above already skips holidays (no intraday
  // bars on a closed day), so trust it; only use quote.closePrice when the
  // candle yielded nothing.
  if (vixYClose === null) {
    try {
      const qData = await fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/quotes?symbols=%24VIX&fields=quote`, token, env);
      const qClose = qData?.['$VIX']?.quote?.closePrice;
      if (qClose) vixYClose = parseFloat(qClose.toFixed(2));
    } catch (e) { console.warn('[proxy]', e.message || e); }
  }

  if (vixYClose === null) throw new Error('Could not determine yesterday VIX close');

  // 3. Get today's VIX open = FIRST CHANGE in VIX after 9:30:00 ET.
  //
  // DUAL-SOURCE RACE: poll Schwab quote.lastPrice AND Tastytrade
  // market-data.last in parallel (500ms each). First source to detect a
  // value/timestamp change from its baseline WINS. The other source keeps
  // running briefly until it notices the winner has captured, then exits.
  //
  // Why dual-source: either feed can lag or stay stale-cached at 9:30. By
  // racing both, we get the FIRST genuine new VIX publication regardless
  // of which vendor's CDN updates first.
  //
  // PER-TICK budget: 20s, NOT 5 min. The cron fires every minute during
  // market hours, so the cron itself is the retry loop. Holding the claim
  // across a 5-min in-tick poll was the root cause of stuck claims —
  // Cloudflare kills the cron tick long before 5 min, orphaning the claim
  // for ~3 min until self-heal. 20s catches the first genuine Cboe
  // publication (they republish ~every 15s); if this tick juuust misses,
  // the next minute's tick catches it. Signal lands 9:30:xx, worst 9:31:xx.
  let vixToday = null;
  let vixSource = null;
  {
    const maxWaitMs = 20_000;
    const pollIntervalMs = 500;
    const deadlineMs = Date.now() + maxWaitMs;
    const startedAt = Date.now();
    const state = { schwab: null, tasty: null };  // separate slots — Schwab is always preferred

    async function pollSchwab() {
      // CANONICAL VIX OPEN — matches dashboard's schwabFetchHistorical() exactly:
      // fetch pricehistory 1-min VIX bars, find first candle with ET hour=9 min>=30,
      // use its `.open` field (the actual first 9:30 tick value).
      //
      // Previously this used /quotes lastPrice which drifts from the 9:30
      // candle.open by 0.05-0.10 because quote.lastPrice is the CURRENT tick,
      // not the first 9:30 tick. Caused the 2026-06-08 Discord vs Dashboard
      // signal divergence (Discord said 90% dead-zone, Dashboard said 95% edge).
      // See user feedback 2026-06-08.
      let attempt = 0;
      while (Date.now() < deadlineMs && state.schwab === null) {
        attempt++;
        try {
          // 5-day window matches dashboard for cache parity
          const end = Date.now();
          const start = end - 5 * 24 * 60 * 60 * 1000;
          const histUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24VIX&periodType=day&period=5&frequencyType=minute&frequency=1&startDate=${start}&endDate=${end}&needExtendedHoursData=true`;
          const hist = await fetchSchwabJSON(histUrl, token, env);
          if (hist?.candles?.length) {
            // Find first candle in today's session at-or-after 9:30 ET (matches
            // dashboard filter: hour > 9 OR (hour === 9 && min >= 30)).
            const todayStr = etNow.toDateString();
            const open930 = hist.candles
              .filter(c => {
                const d = toET(new Date(c.datetime));
                return d.toDateString() === todayStr &&
                       (d.getHours() > 9 || (d.getHours() === 9 && d.getMinutes() >= 30));
              })
              .sort((a, b) => a.datetime - b.datetime)[0];
            if (open930 && open930.open != null) {
              const price = parseFloat(open930.open.toFixed(2));
              const tET = toET(new Date(open930.datetime));
              state.schwab = price;
              console.log(`[proxy] SCHWAB 9:30 candle.open captured: ${price} @ ${tET.toTimeString().slice(0,8)} ET (attempt ${attempt}, ${Math.round((Date.now()-startedAt)/1000)}s)`);
              return;
            }
          }
        } catch (e) { /* keep trying — candle may not exist yet at 9:30:00 */ }
        if (state.schwab !== null) return;
        await new Promise(r => setTimeout(r, pollIntervalMs));
      }
    }

    async function pollTasty() {
      // CANONICAL TASTY OPEN — uses /market-data/Index/VIX `open` field
      // (today's session open). Matches dashboard's Schwab pricehistory 9:30
      // candle.open methodology in spirit. Tasty's `open` field appears
      // once Cboe publishes the day's first regular-session print (~9:30:01).
      //
      // FIX (2026-06-09): freshness check must require timestamp >= 9:30 ET,
      // not just today's date. Without the time check, a cron tick firing at
      // exactly 9:30:00 ET would accept Tasty's pre-market cached `open`
      // value (today's date, but pre-9:30 timestamp) and post a message with
      // STALE VIX BEFORE Cboe's first regular-session publication.
      // Matches Schwab's candle filter at line ~3568 and _isFreshTick below.
      let attempt = 0;
      while (Date.now() < deadlineMs && state.tasty === null && state.schwab === null) {
        attempt++;
        try {
          const result = await tastyGetVix(env);  // { open, price, asOf, raw }
          if (result.open != null && state.tasty === null) {
            // Sanity: only accept if updated-at timestamp is from today's
            // REGULAR-SESSION (date matches AND time >= 9:30 ET) — defends
            // against stale prior-session cache AND pre-market cache.
            const ts = result.raw?.['updated-at'] || result.asOf;
            const dt = ts ? new Date(String(ts)) : null;
            let fresh = false;
            if (dt && isFinite(dt.getTime())) {
              const tET = toET(dt);
              const sameDay = tET.toDateString() === etNow.toDateString();
              const postOpen = (tET.getHours() * 60 + tET.getMinutes()) >= 570; // 9:30 ET
              fresh = sameDay && postOpen;
            }
            if (fresh) {
              state.tasty = parseFloat(result.open.toFixed(2));
              console.log(`[proxy] TASTY captured 9:30 open from /Index/VIX.open: ${state.tasty} @ ${ts} (attempt ${attempt}, ${Math.round((Date.now()-startedAt)/1000)}s)`);
              return;
            }
            // open present but stale (pre-market or yesterday) — keep polling.
          }
        } catch (e) { /* keep trying — Tasty may not have today's open yet */ }
        if (state.tasty !== null || state.schwab !== null) return;
        await new Promise(r => setTimeout(r, pollIntervalMs));
      }
    }

    // DUAL-SOURCE RACE — both vendors now read TODAY'S OPEN (not current tick):
    //   Schwab: pricehistory 1-min, first candle @ hour=9 min>=30, use `.open`
    //   Tasty:  /market-data/Index/VIX, use `open` field
    // Whichever vendor publishes the 9:30 open first wins. The dual-source
    // 2nd Discord message still posts the OTHER vendor's data for cross-check.
    // Both methodologies match the dashboard's schwabFetchHistorical() now.
    // CLAUDE.md rule 11 satisfied: worker + dashboard agree on vix_today_open.
    // VIX OPEN = the first print at/after 9:30:00 ET = the open of the first
    // 9:30 1-min candle (pollSchwab). SCHWAB ONLY. Tasty's /Index/VIX `.open`
    // is the spiky auction open, NOT the first print, so it must never set the
    // displayed open. If Schwab's candle is slow, the quote-advancing-tick
    // fallback below (also first-print) covers it — we never fall to Tasty.open.
    // (user, 2026-06-18, settled for good: "first print after 9:30 ... today 16.67")
    await pollSchwab();
    vixToday = state.schwab;
    vixSource = state.schwab != null ? 'schwab' : null;
    if (vixToday !== null) {
      console.log(`[proxy] VIX OPEN ${vixToday} captured via ${vixSource} after ${Math.round((Date.now()-startedAt)/1000)}s of polling`);
    } else {
      console.warn('[proxy] Neither source caught a VIX change in 5min — both feeds may be stale');
    }
  }

  if (vixToday === null) {
    // Last-resort fallback to old quote-polling logic.
  //    VIX is a calculated index — Cboe republishes a new value every ~15s.
  //    The first poll we issue may return a cached/stale tick (Schwab's edge
  //    caches the last published value), which can show a pre-open snapshot
  //    timestamped at 9:30:00 but reflecting pre-market data. Bug observed
  //    2026-05-13: first tick showed 18.25 (yesterday's vintage) while the
  //    9:31 minute bar opened at 17.98.
  //
  //    Fix: poll FAST (200ms), record the first observed tradeTime as
  //    "baseline", and only accept a tick whose tradeTime has advanced past
  //    that baseline AND is >= 9:30:00 ET. That guarantees we see a
  //    genuinely-new Cboe publication, not Schwab's cached pre-open snapshot.
  //
  //    Why not candles/openPrice:
  //    - Schwab's pricehistory for $VIX can lag 60-90 min behind real time
  //    - Schwab's quote.openPrice is unreliable for calculated indices (VIX)
  //      e.g. 2026-04-14: openPrice=18.73 but first print was 18.25
  const quoteUrl = `https://api.schwabapi.com/marketdata/v1/quotes?symbols=%24VIX&fields=quote`;
  const maxWaitMs = 12_000;  // per-tick — cron retries next minute, never hold claim long
  const pollIntervalMs = 200;  // tight loop — Cboe pubs ~every 15s, catch ASAP
  const deadlineMs = Date.now() + maxWaitMs;
  let attempt = 0;
  let baselineTradeTime = null;  // first tradeTime we see — proves "tick advanced"
  while (Date.now() < deadlineMs) {
    attempt++;
    const vixQuote = await fetchSchwabJSON(quoteUrl, token, env);
    const vixQ = vixQuote?.['$VIX']?.quote;
    if (vixQ?.lastPrice && vixQ?.tradeTime) {
      const tt = vixQ.tradeTime;
      const tradeET = toET(new Date(tt));
      const tradeMin = tradeET.getHours() * 60 + tradeET.getMinutes();
      const isToday = tradeET.toDateString() === todayStr;
      const postOpen = tradeMin >= 570;

      // First observed tradeTime becomes the baseline.
      if (baselineTradeTime === null) {
        baselineTradeTime = tt;
        console.log(`[proxy] VIX baseline tick: lastPrice=${vixQ.lastPrice}, tradeTime=${tradeET.toTimeString().slice(0,8)} ET (waiting for next publication)`);
      } else if (tt > baselineTradeTime && isToday && postOpen) {
        // Accept: tradeTime has advanced AND is post-9:30 ET = genuinely new
        // Cboe publication.
        vixToday = parseFloat(vixQ.lastPrice.toFixed(2));
        vixSource = 'schwab';  // fallback path uses Schwab quotes
        console.log(`[proxy] VIX open ${vixToday} (tradeTime ${tradeET.toTimeString().slice(0,8)} ET, advanced from baseline, attempt ${attempt})`);
        break;
      }
    }
    await new Promise(r => setTimeout(r, pollIntervalMs));
  }

  if (vixToday === null) {
    // VIX not published yet THIS tick. Do NOT throw (that would noise an
    // error DM) and do NOT leave the claim sitting (that orphans it →
    // stuck-claim → ~3 min silence). RELEASE the claim and bail cleanly;
    // the next cron tick (≤60s) re-claims fresh and tries again. By then
    // Cboe has definitely published. Net: at most 1 extra minute, no spam,
    // no stuck claim — vs the old 5-min-poll-then-orphan failure mode.
    await env.SIGNAL_KV.delete(morningDoneKey);
    console.warn(`[proxy] VIX not ready this tick (${attempt} attempts) — claim released, cron retries next minute`);
    return { status: 'vix_pending_retry', time: `${etHour}:${String(etMin).padStart(2,'0')} ET` };
  }
  }  // end Schwab-fallback else block

  // 4. Fetch SPX quote → gap % + today's SPX open
  let spxGapPct = null;
  let spxTodayOpen = null;
  let spxYClose = null;   // hoisted to function scope — also read by dual-source 2nd-post block below
  try {
    // Get SPX yesterday close from history
    const spxHistUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24SPX&periodType=day&period=5&frequencyType=minute&frequency=1&startDate=${start}&endDate=${end}&needExtendedHoursData=true`;
    const spxHist = await fetchSchwabJSON(spxHistUrl, token, env);
    if (spxHist.candles && yDate) {
      const spxYCandles = spxHist.candles.filter(c => toET(new Date(c.datetime)).toDateString() === yDate);
      spxYCandles.sort((a, b) => a.datetime - b.datetime);
      const spxCloseCandle = spxYCandles.slice().reverse().find(c => {
        const d = toET(new Date(c.datetime));
        return d.getHours() === 16 && d.getMinutes() >= 10 && d.getMinutes() <= 15;
      }) || (spxYCandles.length ? spxYCandles[spxYCandles.length - 1] : null);
      if (spxCloseCandle) spxYClose = spxCloseCandle.close;
    }

    // Get SPX today open — Tasty PRIMARY (refresh_token never expires, so a
    // dead Schwab token can't null this and break the signal), Schwab FALLBACK.
    // FIX (2026-06-09): Tasty's `open` field must pass freshness check (today's
    // date AND timestamp >= 9:30 ET). Previously a cron tick firing at 9:30:00
    // accepted Tasty's pre-market cached `open` (yesterday's value with a
    // today-dated timestamp), producing a stale SPX open in the morning signal.
    // Also no longer falls back to `last`/`price` — those are CURRENT ticks
    // and drift after open; `open` is the only reliable session-open marker.
    try {
      const ts = await tastyGetSpx(env);
      const dt = ts.asOf ? new Date(String(ts.asOf)) : null;
      let freshSpx = false;
      if (dt && isFinite(dt.getTime())) {
        const tET = toET(dt);
        const sameDay = tET.toDateString() === etNow.toDateString();
        const postOpen = (tET.getHours() * 60 + tET.getMinutes()) >= 570;
        freshSpx = sameDay && postOpen;
      }
      if (ts.open != null && isFinite(ts.open) && ts.open > 0 && freshSpx) {
        spxTodayOpen = parseFloat(ts.open.toFixed(2));
        console.log(`[proxy] SPX open ${spxTodayOpen} via tastytrade (primary, fresh @ ${ts.asOf})`);
      } else if (ts.open != null) {
        console.log(`[proxy] SPX Tasty open ${ts.open} REJECTED — stale (asOf=${ts.asOf}). Falling back to Schwab.`);
      }
    } catch (e) { console.warn('[proxy] Tasty SPX failed, trying Schwab:', e.message || e); }
    if (spxTodayOpen == null) {
      // Schwab fallback — prefer pricehistory 9:30 candle (matches dashboard);
      // quote.openPrice is unreliable for indices pre/at open.
      try {
        const spxHistUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24SPX&periodType=day&period=2&frequencyType=minute&frequency=1&needExtendedHoursData=false`;
        const spxHist = await fetchSchwabJSON(spxHistUrl, token, env);
        const todayStr = etNow.toDateString();
        const open930 = (spxHist.candles || [])
          .filter(c => {
            const d = toET(new Date(c.datetime));
            return d.toDateString() === todayStr &&
                   (d.getHours() > 9 || (d.getHours() === 9 && d.getMinutes() >= 30));
          })
          .sort((a, b) => a.datetime - b.datetime)[0];
        if (open930 && open930.open != null) {
          spxTodayOpen = parseFloat(open930.open.toFixed(2));
          console.log(`[proxy] SPX open ${spxTodayOpen} via Schwab pricehistory 9:30 candle (fallback)`);
        }
      } catch (e) { console.warn('[proxy] Schwab SPX pricehistory failed:', e.message); }
      // Last resort: quote endpoint (kept for emergency, but openPrice
      // can be 0/null at exactly 9:30 — only use if pricehistory failed).
      if (spxTodayOpen == null) {
        try {
          const spxQuoteUrl = `https://api.schwabapi.com/marketdata/v1/quotes?symbols=%24SPX&fields=quote`;
          const spxQuote = await fetchSchwabJSON(spxQuoteUrl, token, env);
          const spxQ = spxQuote?.['$SPX']?.quote;
          if (spxQ?.openPrice != null && spxQ.openPrice > 0) {
            spxTodayOpen = parseFloat(spxQ.openPrice.toFixed(2));
            console.log(`[proxy] SPX open ${spxTodayOpen} via Schwab quote.openPrice (last resort)`);
          }
        } catch (e) { console.warn('[proxy] Schwab quote.openPrice failed:', e.message); }
      }
    }

    if (spxYClose && spxTodayOpen) {
      spxGapPct = ((spxTodayOpen - spxYClose) / spxYClose) * 100;
    }
  } catch (e) { console.warn('[proxy]', e.message || e); }

  // 4b. GitHub PUT for vixOpen + spxOpen is DEFERRED until after Discord post
  // (it takes ~1-2s and would block the Discord message — runs after section 9).

  // 4c. Fetch previous day's m8bfWR + last-20 vixClose from history_data.json.
  //     vixPct20d is required for the diagonal filter (VIX_MID 50–80% dead zone).
  //     rsi14 is required for the BOBF type-aware gate (Friday 40-65 / vix-down ≤70).
  let prevWR = null;
  let vixPct20d = null;
  let rsi14 = null;
  try {
    const histData = await getHistory(env);
    if (Array.isArray(histData) && histData.length) {
      // Find the most recent entry before today that has m8bfWR
      const sorted = histData
        .filter(r => r.date < todayISO && r.m8bfWR != null)
        .sort((a, b) => b.date.localeCompare(a.date));
      if (sorted.length > 0) {
        prevWR = parseFloat(sorted[0].m8bfWR);
        console.log(`[proxy] prevWR = ${prevWR}% (from ${sorted[0].date})`);
      }

      // Pull the last 20 prior vixClose values (newest last) for the diagonal
      // regime filter. CANONICAL via signal-engine.js computeVixPct20d — DO
      // NOT inline the percentile math here. See lessons.md P5.
      const vix20 = histData
        .filter(r => r.date < todayISO && r.vixClose != null && r.vixClose > 0)
        .sort((a, b) => a.date.localeCompare(b.date))
        .slice(-20)
        .map(r => parseFloat(r.vixClose));
      const { pct: pctComputed } = computeVixPct20d(vixToday, vix20);
      if (pctComputed != null) {
        vixPct20d = pctComputed;
        console.log(`[proxy] vixPct20d = ${vixPct20d}% (vixToday=${vixToday} vs last ${vix20.length} closes)`);
      }

      // Compute RSI(14) on prior daily closes for the BOBF type-aware gate.
      // Same data source the dashboard uses; ensures the Discord message and
      // /trade endpoint show "No BOBF (RSI X.X outside 40-65)" instead of
      // wrongly claiming BOBF is in play.
      const closes30 = histData
        .filter(r => r.date < todayISO && r.spxClose != null && r.spxClose > 0)
        .sort((a, b) => a.date.localeCompare(b.date))
        .slice(-30)
        .map(r => parseFloat(r.spxClose));
      if (closes30.length >= 15) {
        rsi14 = computeRSI14(closes30);
        console.log(`[proxy] rsi14 = ${rsi14?.toFixed(2)} (from ${closes30.length} prior closes)`);
      }
    }
  } catch (e) { console.warn('[proxy] history fetch failed:', e.message || e); }

  // 5. Calculate signal
  // COR1M open from the cloud capture — may still be null at 9:30–9:37
  // (calculated index prints late); the Diagonal line then shows "pending"
  // and the /trade endpoint + dashboard pick up the real status minutes later.
  const cor1mOpenToday = await getCor1mOpenToday(env, isoDateET(etNow));
  const signal = calculateSignal({
    vixToday,
    vixYOpen,
    vixYClose,
    spxGapPct,
    etDate: etNow,
    prevWR,
    vixPct20d,
    rsi14,
    cor1m: cor1mOpenToday,
  });

  // 5a. Persist the morning signal so /straddle-today and other endpoints
  // can read EXACTLY what the morning cron computed (including the live
  // quote-polled vixToday and the official quote-overridden vixYClose
  // — those differ from the 9:30 candle data and matter for theme).
  try {
    await env.SIGNAL_KV.put(`morning_signal_data_${isoDateET(etNow)}`, JSON.stringify({
      ...signal,
      vixToday, vixYOpen, vixYClose, spxGapPct, prevWR, vixPct20d, rsi14,
      spxOpen: spxTodayOpen,  // ← straddle center anchor (snap5 of this)
      computedAt: new Date().toISOString(),
    }), { expirationTtl: 86400 });
  } catch (_) { /* non-critical */ }

  // 5b/5c: Straddle entry + BOBF prefilter are DEFERRED until after the Discord
  // post (they take ~500ms+ each and would block the message). Run after section 9.

  // 6. Build Discord message — include Tail Hedge today's signal
  const vixValues = { yOpen: vixYOpen, yClose: vixYClose, todayOpen: vixToday };
  const canonBanner = vixSource === 'schwab' ? '📡 **SCHWAB DATA**\n\n' : '📡 **TASTYTRADE DATA**\n\n';
  const tailLineCanon = await getTailHedgeStatusLine(env);
  signal._tailLine = tailLineCanon;  // for embed builder
  try { signal._tiltLine = await computeTiltLine(env, isoDateET(etNow)); } catch (_) { /* advisory only */ }
  try { signal._gexLine = await computeGexLine(env); } catch (_) { /* advisory only */ }
  try { signal._cycleLine = await computeCycleLine(env, etNow); } catch (_) { /* advisory only */ }
  try { signal._volFlowLine = await computeVolFlowLine(env, etNow); } catch (_) { /* advisory only */ }
  try { signal._m8bfWrLine = await computeM8bfWrLine(env, etNow); } catch (_) { /* advisory only */ }
  try { signal._skewLine = await computeSkewLine(); } catch (_) { /* advisory only */ }
  const message = canonBanner + buildDiscordMessage(signal, vixValues, tailLineCanon);

  // 7. Slot already claimed at the top of the morning block. Reuse the same key.
  const msDoneKey = morningDoneKey;

  // 8. Post to Discord (via consolidated helper — tries DISCORD_TOKEN first)
  const dcRaw = await env.SIGNAL_KV.get('discord_config');
  if (!dcRaw) { await env.SIGNAL_KV.delete(msDoneKey); throw new Error('No Discord config in KV — sync from browser'); }
  const dc = JSON.parse(dcRaw);

  // 8-pre. LAST-CALL dupe gate (2026-06-10: TRIPLE send at 9:33:18/9:33:33/
  // 9:34:09). The claim verify at the top runs ~20-35s before this point;
  // KV is eventually consistent, so a parallel invocation (overlapping cron
  // tick or /gex fallback) can pass its own claim verify without either side
  // seeing the other. By NOW the rival's claim/'sent' write has had those
  // ~30s to propagate — one fresh read here kills the duplicate pre-post.
  const lastCall = await env.SIGNAL_KV.get(msDoneKey);
  if (lastCall === 'sent' || (lastCall && lastCall.startsWith('claim:') && lastCall !== claimValue)) {
    console.log(`[proxy] Pre-send dupe gate: slot='${(lastCall || '').slice(0, 24)}…' not mine — aborting duplicate`);
    return { status: 'duplicate_avoided_presend', time: `${etHour}:${String(etMin).padStart(2, '0')} ET` };
  }

  // MORNING CARD IMAGE (2026-06-18) — render the forecast card to a PNG and post
  // it; the M8BF skip/combo "strikes" go as a small (-#) subtext BELOW the image.
  // ANY failure (render/font/upload) falls back to the original text message so
  // the morning signal can never go silent.
  let result = null;
  try {
    const cardData = buildMorningCardData(signal, vixValues, tailLineCanon);
    const png = await renderMorningCardPng(cardData);
    // Footer rides as the message content (renders ABOVE the image in Discord)
    // so the live link + disclaimer sit on top of the card, link clickable.
    result = await sendDiscordImage(env, dc.channelId, png, dc.proxyUrl, 'morning.png', DISCORD_FOOTER);
  } catch (e) {
    await logEvent(env, 'warn', 'morning', 'card image failed — text fallback', { msg: e && (e.message || String(e)) });
    result = null;
  }
  if (!result || !result.ok) {
    result = await sendDiscordDM(env, dc.channelId, message.slice(0, 2000), dc.proxyUrl);
  }
  let dcData = result.data || {};
  if (!result.ok) {
    // Retry once on 429 (rate limit) after Retry-After delay
    if (result.status === 429 && dcData.retry_after) {
      await new Promise(r => setTimeout(r, (dcData.retry_after + 0.5) * 1000));
      const retry = await sendDiscordDM(env, dc.channelId, message.slice(0, 2000), dc.proxyUrl);
      if (!retry.ok) {
        await env.SIGNAL_KV.delete(msDoneKey);
        throw new Error('Discord post failed after retry: ' + JSON.stringify(retry));
      }
      await env.SIGNAL_KV.put(msDoneKey, 'sent', { expirationTtl: 86400 });
      globalThis.__morningSentDay = todayISO;
      return retry.data || { ok: true };
    }
    await env.SIGNAL_KV.delete(msDoneKey);
    throw new Error('Discord post failed: ' + (result.error || JSON.stringify(dcData)));
  }

  // Mark morning signal as fully sent
  await env.SIGNAL_KV.put(msDoneKey, 'sent', { expirationTtl: 86400 });
  globalThis.__morningSentDay = todayISO;   // isolate-local guard (KV-lag immune)
  // NOTE: subscriber fan-out was MOVED to live trade EXECUTIONS (2026-06-16,
  // user: "send the actual trade each time it fills, not the morning signal").
  // See the /link-notify route + skipper's fill handler (fanoutText). The
  // morning signal is intentionally NOT fanned out to subscribers anymore.
  await logEvent(env, 'info', 'morning', 'canonical signal posted', {
    source: vixSource,   // 'schwab' or 'tastytrade' — which won the VIX race
    rec: signal.rec, badge: signal.badge, theme: signal.theme,
    vix: { todayOpen: vixToday, yOpen: vixYOpen, yClose: vixYClose },
    spxOpen: spxTodayOpen, spxGapPct,
  });

  // ── 8b. (DISABLED 2026-06-18) Dual-source "2nd message from the OTHER vendor".
  //    Removed because the open is Schwab first-print ONLY. Tasty's `.open` is
  //    the wrong field (spiky auction open, not the first 9:30 print), so
  //    posting it as a peer message only ever showed a conflicting number and
  //    caused endless confusion. ONE Schwab morning message now, period.
  //    The block below is gated off (safe to delete in a future cleanup).
  //    (user req 2026-06-18 — "first print after 9:30 ... today was 16.67")
  if (false) try {
    const otherSource = vixSource === 'schwab' ? 'tastytrade' : 'schwab';
    let vixOther = null, spxOther = null;
    let vixOtherTs = null, spxOtherTs = null;
    // Freshness check: timestamp is from today's regular session (>= 9:30 ET).
    // Accepts epoch ms (Schwab tradeTime) or ISO string (Tasty updated-at).
    function _isFreshTick(ts) {
      if (ts == null) return false;
      const d = (typeof ts === 'number') ? new Date(ts) : new Date(String(ts));
      if (!isFinite(d.getTime())) return false;
      const et = toET(d);
      if (et.toDateString() !== etNow.toDateString()) return false;
      return (et.getHours() * 60 + et.getMinutes()) >= 570;  // 9*60+30
    }
    // Budget for the 2nd source — reduced from 60s to 22s (2026-06-09 fix).
    // Cloudflare scheduled handlers have a ~30s wall-clock limit. The canonical
    // post takes ~5-8s; 60s here was being killed mid-poll, dropping the 2nd
    // message entirely. 22s + 8s canonical leaves headroom.
    //
    // 2026-06-09 FIX: 2nd-source Schwab now uses pricehistory 9:30 candle
    // (same methodology as pollSchwab in the main race) instead of the quote
    // endpoint. The quote endpoint's tradeTime can lag 10-30s at market open,
    // making _isFreshTick fail repeatedly within the budget. pricehistory
    // publishes the 9:30 candle within 1-2 seconds of Cboe's first print.
    // SPX 2nd-source Tasty branch: use s.open ONLY (no fallback to last/
    // price — those are CURRENT ticks, not 9:30 OPEN, so the cross-check
    // would compare apples to oranges).
    const dualDeadline = Date.now() + 22_000;
    let dualAttempts = 0;
    let lastVixTs = null, lastSpxTs = null, lastVixVal = null, lastSpxVal = null;
    while (Date.now() < dualDeadline && (vixOther == null || spxOther == null)) {
      dualAttempts++;
      let lastFetchErr = null;
      if (otherSource === 'tastytrade') {
        if (vixOther == null) {
          try {
            const r = await tastyGetVix(env);
            // Use the `open` field ONLY (today's session open, matches Schwab
            // pricehistory 9:30 candle.open) — never `price` (current tick),
            // same as the SPX 2nd-source branch. tastyGetVix null-guards a
            // stale pre-open `open`, so v stays null until a real open prints.
            const v = r.open;
            const ts = r.raw?.['updated-at'] || r.asOf;
            if (v != null) { lastVixVal = parseFloat(v.toFixed(2)); lastVixTs = ts; }
            if (v != null && _isFreshTick(ts)) { vixOther = lastVixVal; vixOtherTs = ts; }
          } catch (e) { lastFetchErr = `tasty-vix:${e.message}`; }
        }
        if (spxOther == null) {
          try {
            const s = await tastyGetSpx(env);
            const ts = s.asOf || s.raw?.['updated-at'];
            // Only use s.open (today's session open). s.last is the current
            // tick which drifts from open; including it would make the 2nd
            // message compare current price vs canonical open — misleading.
            if (s.open != null) { lastSpxVal = parseFloat(s.open.toFixed(2)); lastSpxTs = ts; }
            if (s.open != null && _isFreshTick(ts)) { spxOther = lastSpxVal; spxOtherTs = ts; }
          } catch (e) { lastFetchErr = `tasty-spx:${e.message}`; }
        }
      } else if (token) {
        // SCHWAB 2nd-source via pricehistory — NOT quote endpoint. pricehistory
        // publishes the 9:30 candle within 1-2 seconds of Cboe's first print;
        // quote.tradeTime can lag 10-30 seconds at market open.
        if (vixOther == null) {
          try {
            const end = Date.now(); const start = end - 5 * 24 * 60 * 60 * 1000;
            const histUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24VIX&periodType=day&period=5&frequencyType=minute&frequency=1&startDate=${start}&endDate=${end}&needExtendedHoursData=true`;
            const hist = await fetchSchwabJSON(histUrl, token, env);
            if (hist?.candles?.length) {
              const todayStr2 = etNow.toDateString();
              const open930 = hist.candles
                .filter(c => {
                  const d = toET(new Date(c.datetime));
                  return d.toDateString() === todayStr2 &&
                         (d.getHours() > 9 || (d.getHours() === 9 && d.getMinutes() >= 30));
                })
                .sort((a, b) => a.datetime - b.datetime)[0];
              if (open930 && open930.open != null) {
                lastVixVal = parseFloat(open930.open.toFixed(2));
                lastVixTs = open930.datetime;
                if (_isFreshTick(open930.datetime)) { vixOther = lastVixVal; vixOtherTs = open930.datetime; }
              }
            }
          } catch (e) { lastFetchErr = `schwab-vix:${e.message}`; }
        }
        if (spxOther == null) {
          try {
            const end = Date.now(); const start = end - 2 * 24 * 60 * 60 * 1000;
            const histUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24SPX&periodType=day&period=2&frequencyType=minute&frequency=1&startDate=${start}&endDate=${end}&needExtendedHoursData=false`;
            const hist = await fetchSchwabJSON(histUrl, token, env);
            if (hist?.candles?.length) {
              const todayStr2 = etNow.toDateString();
              const open930 = hist.candles
                .filter(c => {
                  const d = toET(new Date(c.datetime));
                  return d.toDateString() === todayStr2 &&
                         (d.getHours() > 9 || (d.getHours() === 9 && d.getMinutes() >= 30));
                })
                .sort((a, b) => a.datetime - b.datetime)[0];
              if (open930 && open930.open != null) {
                lastSpxVal = parseFloat(open930.open.toFixed(2));
                lastSpxTs = open930.datetime;
                if (_isFreshTick(open930.datetime)) { spxOther = lastSpxVal; spxOtherTs = open930.datetime; }
              }
            }
          } catch (e) { lastFetchErr = `schwab-spx:${e.message}`; }
        }
      } else {
        await logEvent(env, 'warn', 'morning', `2nd-source dropped — Schwab token unavailable (otherSource=${otherSource})`, { vixSource });
        break;
      }
      if (lastFetchErr) console.warn('[proxy] dual-msg fetch err:', lastFetchErr);
      if (vixOther == null || spxOther == null) {
        await new Promise(r => setTimeout(r, 800));
      }
    }
    if (vixOther == null) {
      // Log to KV so /debug-morning-log shows WHY the 2nd message didn't post.
      await logEvent(env, 'warn', 'morning', `2nd-source dropped — no fresh VIX after ${Math.round((Date.now()-(dualDeadline-22000))/1000)}s`, {
        otherSource, canonicalSource: vixSource, attempts: dualAttempts,
        lastVixVal, lastVixTs, lastSpxVal, lastSpxTs,
      });
    }
    if (vixOther != null) {
      const spxGapOther = (spxYClose && spxOther) ? ((spxOther - spxYClose) / spxYClose) * 100 : spxGapPct;
      const signalOther = calculateSignal({
        vixToday: vixOther, vixYOpen, vixYClose,
        spxGapPct: spxGapOther,
        etDate: etNow, prevWR, vixPct20d, rsi14,
        cor1m: cor1mOpenToday,
      });
      signalOther._tiltLine = signal._tiltLine;   // same advisory on the 2nd copy
      signalOther._gexLine = signal._gexLine;
      signalOther._cycleLine = signal._cycleLine;
      signalOther._volFlowLine = signal._volFlowLine;
      signalOther._m8bfWrLine = signal._m8bfWrLine;
      const otherBanner = otherSource === 'schwab' ? '📡 **SCHWAB DATA**\n\n' : '📡 **TASTYTRADE DATA**\n\n';
      const msgOther = otherBanner + buildDiscordMessage(signalOther, { yOpen: vixYOpen, yClose: vixYClose, todayOpen: vixOther }, tailLineCanon);
      await new Promise(r => setTimeout(r, 1500));   // Discord rate-limit safety
      const r2 = await sendDiscordDM(env, dc.channelId, msgOther.slice(0, 2000), dc.proxyUrl);
      if (r2.ok) {
        await logEvent(env, 'info', 'morning', `2nd-source signal posted (${otherSource})`, {
          rec: signalOther.rec, badge: signalOther.badge, theme: signalOther.theme,
          vix: vixOther, vixTs: vixOtherTs, spxOpen: spxOther, spxTs: spxOtherTs,
          attempts: dualAttempts,
        });
      } else {
        // 2nd-source send failed — log it so /debug-morning-log shows the error.
        await logEvent(env, 'warn', 'morning', `2nd-source Discord post FAILED (${otherSource})`, {
          status: r2.status, error: r2.error, data: JSON.stringify(r2.data || {}).slice(0, 240),
          rec: signalOther.rec, vix: vixOther,
        });
      }
    }
  } catch (e) {
    await logEvent(env, 'warn', 'morning', `dual-source 2nd post EXCEPTION (canonical already sent)`, { msg: e.message, stack: (e.stack || '').slice(0, 240) });
  }

  // ════════════════════════════════════════════════════════════════════
  // POST-DISCORD WORK (deferred until after the message went out)
  // ────────────────────────────────────────────────────────────────────
  // Everything here was previously running BEFORE Discord, delaying the
  // message by 2-5 seconds. Now Discord fires first, then these run.
  // Each block is wrapped in try/catch so a failure here doesn't break
  // the morning_signal_<today>='sent' state (already marked).
  // ════════════════════════════════════════════════════════════════════

  // a) GitHub PUT — write vixOpen + spxOpen to history_data.json
  try {
    await upsertHistoryGitHub(env, todayISO, {
      vixOpen: vixToday,
      ...(spxTodayOpen != null ? { spxOpen: spxTodayOpen } : {}),
    });
  } catch (e) { console.warn('[proxy/post] GitHub PUT:', e.message || e); }

  // b) Open straddle if signal says so, OR record skip reason
  if (signal.theme === 'strad') {
    try {
      const existingRaw = await env.SIGNAL_KV.get('straddle_open_trade');
      const existing = existingRaw ? JSON.parse(existingRaw) : null;
      if (!existing || existing.openDate !== isoDateET(etNow)) {
        // ── Item 9: pre-trade signal validator ──
        // Re-pull VIX from a fresh source (Tasty) and recompute theme.
        // If theme flips (rare, but caught May-14-like off-strategy fires),
        // abort with skip reason instead of opening a phantom trade.
        let validatorAborted = false;
        try {
          // tastyGetVix returns an OBJECT — extract a NUMERIC cross-check VIX:
          // prefer the validated session open, fall back to the current tick.
          // (Bug fix 2026-06-16: this used the object directly → drift was NaN
          //  and freshVix.toFixed() threw, so the validator silently aborted /
          //  no-op'd every live straddle. Now it compares real numbers.)
          const fresh = await tastyGetVix(env);
          const freshVix = fresh.open ?? fresh.price;
          if (freshVix != null && Number.isFinite(freshVix)) {
            const drift = Math.abs(freshVix - vixToday);
            // Recompute signal with the fresh VIX
            const freshSig = calculateSignal({
              vixToday: freshVix, vixYOpen, vixYClose, spxGapPct,
              etDate: etNow, prevWR, vixPct20d, rsi14,
            });
            if (freshSig.theme !== 'strad') {
              validatorAborted = true;
              console.warn(`[strad-validate] ABORT: theme flipped (morning vix ${vixToday} → fresh ${freshVix.toFixed(2)}, theme=${freshSig.theme})`);
              await logEvent(env, 'error', 'strad-validate',
                `aborted open: theme flipped at fire time`,
                { morningVix: vixToday, freshVix: parseFloat(freshVix.toFixed(2)),
                  drift: parseFloat(drift.toFixed(2)),
                  morningTheme: 'strad', freshTheme: freshSig.theme });
              // Record skip reason so live page shows correct status
              await env.SIGNAL_KV.put(`straddle_skip_${isoDateET(etNow)}`, JSON.stringify({
                theme: freshSig.theme,
                rec: `Pre-trade validator aborted (vix drift ${drift.toFixed(2)}): ${freshSig.rec}`,
                recordedAt: new Date().toISOString(),
                source: 'pre-trade-validator',
              }), { expirationTtl: 86400 });
            } else if (drift > 0.5) {
              console.log(`[strad-validate] drift ${drift.toFixed(2)} but theme=strad — proceeding`);
            }
          } else {
            // No usable fresh VIX (Tasty `open` still the stale pre-open snapshot
            // and no tick) — do NOT abort on a missing cross-check. A degraded
            // open beats a phantom skip; the morning signal already gated entry.
            console.log('[strad-validate] no usable fresh VIX — skipping validation, proceeding with open');
            try { await logEvent(env, 'info', 'strad-validate', 'validation skipped (no usable fresh VIX)', { morningVix: vixToday }); } catch (_) {}
          }
        } catch (vErr) {
          console.warn('[strad-validate] fresh VIX fetch failed (proceeding):', vErr.message);
        }
        if (!validatorAborted) {
          const stradToken = await getAccessToken(env);
          const trade = await openStraddleTrade(env, stradToken, etNow, signal, masterChain);
          await env.SIGNAL_KV.put('straddle_open_trade', JSON.stringify(trade));
          console.log(`[strad] opened ${trade.status} K=${trade.strike} debit=${trade.entryDebit} maxDebit=${trade.maxDebit}`);
          await logEvent(env, 'info', 'strad-open', `opened ${trade.status}`, {
            strike: trade.strike, entryDebit: trade.entryDebit, maxDebit: trade.maxDebit,
          });
        }
      }
    } catch (e) {
      // Upgrade from console.warn to logEvent so we can SEE these failures.
      // Today (2026-05-22): the 9:32 cron threw silently and the bot never
      // retried — straddle_open_trade stayed null all morning, live page
      // misleadingly showed "Working order" via the cosmetic skip-state.
      console.warn('[strad] open failed:', e.message);
      try { await logEvent(env, 'error', 'strad-open', `open attempt failed`, { msg: e.message, stack: (e.stack || '').slice(0, 300) }); } catch (_) {}
    }
  } else {
    try {
      await env.SIGNAL_KV.put(`straddle_skip_${isoDateET(etNow)}`, JSON.stringify({
        theme: signal.theme,
        rec: signal.rec,
        recordedAt: new Date().toISOString(),
      }), { expirationTtl: 86400 });
    } catch (_) { /* non-critical */ }
  }

  // b2) Open GXBF if signal says so, OR record skip reason.
  // STRATEGY INDEPENDENCE: this branch reads ONLY signal.theme === 'gxbf'
  // (GXBF's own gate — exactly analogous to the Straddle theme === 'strad'
  // branch above). It never blocks / is blocked by M8BF/Straddle/BOBF.
  if (signal.theme === 'gxbf') {
    try {
      const existingRaw = await env.SIGNAL_KV.get('gxbf_open_trade');
      const existing = existingRaw ? JSON.parse(existingRaw) : null;
      if (!existing || existing.openDate !== isoDateET(etNow)) {
        const gxbfResult = await handleGxbfEntry(env, etNow, signal, masterChain);
        console.log(`[gxbf] entry: ${JSON.stringify(gxbfResult)}`);
      }
    } catch (e) { console.warn('[gxbf] open failed:', e.message); }
  } else {
    try {
      await env.SIGNAL_KV.put(`gxbf_skip_${isoDateET(etNow)}`, JSON.stringify({
        theme: signal.theme,
        rec: signal.gxbfText || signal.rec,
        recordedAt: new Date().toISOString(),
      }), { expirationTtl: 86400 });
    } catch (_) { /* non-critical */ }
  }

  // c) BOBF static-filter pre-flight
  try {
    const pf = await prefilterBobf(env, etNow, vixToday, vixYClose);
    if (pf?.skipped) console.log(`[bobf] prefilter skipped: ${pf.skipped} ${pf.reason || (pf.reasons||[]).join(',')||''}`);
  } catch (e) { console.warn('[bobf] prefilter failed:', e.message); }

  return {
    status: 'success',
    signal: signal.rec,
    badge: signal.badge,
    vix: { todayOpen: vixToday, yOpen: vixYOpen, yClose: vixYClose },
    spxGapPct,
    spxOpen: spxTodayOpen,
    githubDate: todayISO,
    postedAt: new Date().toISOString(),
  };
}

// ════════════════════════════════════════════════════════════════════
// GEX (GAMMA EXPOSURE) CALCULATION
// ════════════════════════════════════════════════════════════════════

function calculateGEX(chainData, spot, onlyNearest = false) {
  const R = 0.043, Q = 0.013, MULT = 100;

  function normPdf(x) { return Math.exp(-0.5 * x * x) / Math.sqrt(2 * Math.PI); }

  function bsGamma(S, K, T, sigma) {
    if (T <= 0 || sigma <= 0 || S <= 0) return 0;
    const d1 = (Math.log(S / K) + (R - Q + 0.5 * sigma * sigma) * T) / (sigma * Math.sqrt(T));
    return normPdf(d1) * Math.exp(-Q * T) / (S * sigma * Math.sqrt(T));
  }

  // ── VANNA / CHARM greeks (information-only, same R/Q/MULT/normPdf/d1 convention as bsGamma) ──
  // Vanna = dDelta/dSigma = dVega/dSpot. Per 1.00 (=100 vol-pt) sigma move.
  function bsVanna(S, K, T, sigma) {
    if (T <= 0 || sigma <= 0 || S <= 0) return 0;
    const sqrtT = Math.sqrt(T);
    const d1 = (Math.log(S / K) + (R - Q + 0.5 * sigma * sigma) * T) / (sigma * sqrtT);
    const d2 = d1 - sigma * sqrtT;
    return -Math.exp(-Q * T) * normPdf(d1) * d2 / sigma;
  }

  // normCdf — Abramowitz-Stegun 7.1.26, needed for the charm carry-drift term.
  function normCdf(x) {
    const t = 1 / (1 + 0.2316419 * Math.abs(x));
    const d = 0.3989422804014327 * Math.exp(-0.5 * x * x); // = N'(x)
    const p = d * t * (0.319381530 + t * (-0.356563782 + t * (1.781477937 +
            t * (-1.821255978 + t * 1.330274429))));
    return x >= 0 ? 1 - p : p;
  }

  // helper: carry-drift term = (2(R-Q)T - d2*sigma*sqrtT) / (2 T sigma sqrtT)
  function N_carryDriftHelper(d1, d2, sigma, sqrtT, T) {
    return (2 * (R - Q) * T - d2 * sigma * sqrtT) / (2 * T * sigma * sqrtT);
  }

  // Charm = -dDelta/dT (delta change as time PASSES). isCall picks the carry term.
  // Per 1 YEAR of T; the CEX scaler converts to 1 calendar day.
  function bsCharm(S, K, T, sigma, isCall) {
    if (T <= 0 || sigma <= 0 || S <= 0) return 0;
    const sqrtT = Math.sqrt(T);
    const d1 = (Math.log(S / K) + (R - Q + 0.5 * sigma * sigma) * T) / (sigma * sqrtT);
    const d2 = d1 - sigma * sqrtT;
    const Nd1 = isCall ? normCdf(d1) : (normCdf(d1) - 1);
    const inner = N_carryDriftHelper(d1, d2, sigma, sqrtT, T);
    const dDelta_dT = -Q * Math.exp(-Q * T) * Nd1
                    + Math.exp(-Q * T) * normPdf(d1) * inner;
    return -dDelta_dT;
  }

  // Greek-exposure unit scalers (mirror the gex `* S*S*MULT*0.01` base).
  const VOL_PT   = 0.01;      // 1 implied-vol POINT = 0.01 in sigma
  const DAY_FRAC = 1 / 365;   // 1 calendar day as fraction of a year

  const callMap = chainData.callExpDateMap || {};
  const putMap = chainData.putExpDateMap || {};
  const S = spot;

  // Aggregate expirations: all (default) or only nearest (0DTE mode)
  const allExpiries = [...new Set([...Object.keys(callMap), ...Object.keys(putMap)])].sort();
  if (allExpiries.length === 0) return null;
  const expiriesToUse = onlyNearest ? [allExpiries[0]] : allExpiries;

  // Compute real hours remaining until today's 16:00 ET close (for 0DTE T calc)
  // Falls back to a safe minimum of 15 minutes (1/(365*96)) near / after close.
  function zeroDteT() {
    const etNow = toET(new Date());
    const hrsLeft = (16 - etNow.getHours()) - (etNow.getMinutes() / 60) - (etNow.getSeconds() / 3600);
    const safeHrs = Math.max(hrsLeft, 0.25); // 15 min floor
    return safeHrs / (365 * 24);
  }

  // Accumulate per-strike across selected expirations
  const strikeAccum = {}; // strike → { callGex, putGex, callOI, putOI, callVex, putVex, callCex, putCex }
  let totalCallGex = 0, totalPutGex = 0;
  // INFO-ONLY flow: total traded VOLUME (not OI) across all selected expirations.
  // Volume is the day's flow → summed unconditionally (even when oi===0), not gated on OI.
  let totalCallVol = 0, totalPutVol = 0;
  let nearestDte = Infinity;
  // Per-contract gamma inputs (OI>0 rows that fed gex) — reused for the spot-shifted curve.
  const gammaInputs = [];

  for (const expKey of expiriesToUse) {
    const dteParts = expKey.split(':');
    const dte = parseInt(dteParts[1]) || 0;
    if (dte < nearestDte) nearestDte = dte;
    // 0DTE uses real hours-to-close; longer-dated uses calendar dte/365
    const T_years = dte === 0 ? zeroDteT() : Math.max(dte / 365, 1 / (365 * 24));

    const calls = callMap[expKey] || {};
    const puts = putMap[expKey] || {};
    const strikeSet = new Set([...Object.keys(calls), ...Object.keys(puts)]);

    for (const strikeStr of strikeSet) {
      const K = parseFloat(strikeStr);
      if (isNaN(K)) continue;

      if (!strikeAccum[strikeStr]) strikeAccum[strikeStr] = { strike: K, callGex: 0, putGex: 0, callOI: 0, putOI: 0, callVex: 0, putVex: 0, callCex: 0, putCex: 0 };
      const acc = strikeAccum[strikeStr];

      const callContracts = calls[strikeStr] || [];
      const putContracts = puts[strikeStr] || [];

      for (const c of callContracts) {
        const oi = Math.max(c.openInterest || 0, 0);
        const iv = (c.volatility || 0) / 100;
        // INFO-ONLY flow: sum traded volume for ALL contracts (count it even when oi===0).
        totalCallVol += Math.max(c.totalVolume || 0, 0);
        // Standard GEX: use open interest only (volume is turnover, not dealer inventory)
        if (oi === 0) continue;
        acc.callOI += oi;
        const sig = iv > 0 ? iv : 0.2;
        const gamma = bsGamma(S, K, T_years, sig);
        const gex = gamma * oi * S * S * MULT * 0.01;
        acc.callGex += gex;
        totalCallGex += gex;
        // VEX (per +1 vol-pt) / CEX (delta drift over REMAINING life to expiry) — same base & dealer sign as gex.
        // CEX uses × T_years (not per-day): bounded + interpretable, avoids the 0DTE per-day 1/T charm blow-up.
        const vex = bsVanna(S, K, T_years, sig) * oi * S * S * MULT * 0.01 * VOL_PT;
        acc.callVex += vex;
        const cex = bsCharm(S, K, T_years, sig, true) * oi * S * S * MULT * 0.01 * T_years;
        acc.callCex += cex;
        gammaInputs.push({ K, T: T_years, iv: sig, oi, isCall: true });
      }

      for (const p of putContracts) {
        const oi = Math.max(p.openInterest || 0, 0);
        const iv = (p.volatility || 0) / 100;
        // INFO-ONLY flow: sum traded volume for ALL contracts (count it even when oi===0).
        totalPutVol += Math.max(p.totalVolume || 0, 0);
        if (oi === 0) continue;
        acc.putOI += oi;
        const sig = iv > 0 ? iv : 0.2;
        const gamma = bsGamma(S, K, T_years, sig);
        const gex = gamma * oi * S * S * MULT * 0.01;
        acc.putGex -= gex; // puts negative
        totalPutGex -= gex;
        // Dealer short puts → subtract
        const vexP = bsVanna(S, K, T_years, sig) * oi * S * S * MULT * 0.01 * VOL_PT;
        acc.putVex -= vexP;
        const cexP = bsCharm(S, K, T_years, sig, false) * oi * S * S * MULT * 0.01 * T_years;
        acc.putCex -= cexP;
        gammaInputs.push({ K, T: T_years, iv: sig, oi, isCall: false });
      }
    }
  }

  // Filter strikes to ±8% from spot (widened 2026-06-21 from ±5% to include far walls)
  const rangePct = 0.08;
  const lo = S * (1 - rangePct), hi = S * (1 + rangePct);

  const strikeResults = Object.values(strikeAccum)
    .map(s => ({ ...s, netGex: s.callGex + s.putGex, netVex: s.callVex + s.putVex, netCex: s.callCex + s.putCex }))
    .filter(s => (s.callOI > 0 || s.putOI > 0) && s.strike >= lo && s.strike <= hi)
    .sort((a, b) => a.strike - b.strike);

  const dte = nearestDte;

  // Compute totalGex from filtered strikes only (matches what the chart displays)
  let totalCallGexFiltered = 0, totalPutGexFiltered = 0;
  let totalVex = 0, totalCex = 0; // net VEX / CEX over the same filtered strikes as gex
  for (const r of strikeResults) {
    totalCallGexFiltered += r.callGex;
    totalPutGexFiltered += r.putGex;
    totalVex += r.netVex;
    totalCex += r.netCex;
  }
  const totalGex = totalCallGexFiltered + totalPutGexFiltered;

  // Max positive gamma strike
  let maxPosStrike = null, maxPosGex = 0;
  let maxNegStrike = null, maxNegGex = 0;
  for (const r of strikeResults) {
    if (r.netGex > maxPosGex) { maxPosStrike = r.strike; maxPosGex = r.netGex; }
    if (r.netGex < maxNegGex) { maxNegStrike = r.strike; maxNegGex = r.netGex; }
  }

  // GEX flip: cumulative net_gex zero crossing nearest to spot.
  // Significance gate (2026-06-11, user caught flip 6972 with spot 7293):
  // on a one-sided day the cumulative can graze zero at the band edge off a
  // noise pocket (+62M vs −48B that day) — a technically-true crossing with
  // no meaning. A real flip requires the cumulative to hold REAL mass on
  // BOTH sides (≥2% of |total|); otherwise flip = null ("no flip in range").
  let flipStrike = null;
  {
    const crossings = [];
    let cumGex = 0, maxCum = -Infinity, minCum = Infinity;
    let minAbsCum = Infinity, minAbsCumStrike = null; // fallback: closest to zero
    for (let i = 0; i < strikeResults.length; i++) {
      const prevCum = cumGex;
      cumGex += strikeResults[i].netGex;
      if (cumGex > maxCum) maxCum = cumGex;
      if (cumGex < minCum) minCum = cumGex;
      if (i > 0 && ((prevCum < 0 && cumGex >= 0) || (prevCum > 0 && cumGex <= 0))) {
        const s0 = strikeResults[i - 1].strike;
        const s1 = strikeResults[i].strike;
        const ratio = Math.abs(prevCum) / (Math.abs(prevCum) + Math.abs(cumGex));
        crossings.push(Math.round(s0 + ratio * (s1 - s0)));
      }
      if (Math.abs(cumGex) < minAbsCum) {
        minAbsCum = Math.abs(cumGex);
        minAbsCumStrike = strikeResults[i].strike;
      }
    }
    const sig = Math.abs(totalGex) * 0.02;
    const twoSided = maxCum > sig && minCum < -sig;
    if (twoSided && crossings.length > 0) {
      // Pick the crossing nearest to spot price
      crossings.sort((a, b) => Math.abs(a - S) - Math.abs(b - S));
      flipStrike = crossings[0];
    } else if (twoSided && minAbsCumStrike !== null) {
      // No true zero crossing — use the strike where cumulative is closest to zero
      flipStrike = minAbsCumStrike;
    }
    // else: book is one-sided across the whole band → no meaningful flip
  }

  // Top 10 walls by absolute net GEX
  const walls = [...strikeResults]
    .sort((a, b) => Math.abs(b.netGex) - Math.abs(a.netGex))
    .slice(0, 10)
    .map(w => ({
      strike: w.strike,
      callGex: Math.round(w.callGex),
      putGex: Math.round(w.putGex),
      netGex: Math.round(w.netGex),
      callOI: w.callOI,
      putOI: w.putOI,
      direction: w.netGex >= 0 ? 'stabilizing' : 'amplifying',
    }));

  // ── Spot-shifted gamma curve: net dealer GEX if spot were S', book (IV/OI/T) fixed.
  //    ±rangePct in 25 steps. Re-evaluates bsGamma at each simulated spot.
  //    Emitted as [{shift, gamma}] where shift = S'−spot, gamma = netGex at S'
  //    (UI plots x = data.spot + shift, y = gamma).
  const gammaCurve = [];
  {
    const steps = 25;
    for (let i = 0; i < steps; i++) {
      const frac = -rangePct + (2 * rangePct) * (i / (steps - 1));
      const Sp = S * (1 + frac);
      let netGex = 0;
      for (const g of gammaInputs) {
        let gex = bsGamma(Sp, g.K, g.T, g.iv) * g.oi * Sp * Sp * MULT * 0.01;
        if (!g.isCall) gex = -gex; // dealer short puts → negative
        netGex += gex;
      }
      gammaCurve.push({ shift: parseFloat((Sp - S).toFixed(2)), gamma: Math.round(netGex) });
    }
  }

  const regime = totalGex > 0 ? 'PIN' : 'BREAKOUT';

  return {
    timestamp: Math.floor(Date.now() / 1000),
    spot: parseFloat(S.toFixed(2)),
    regime,
    totalGex: Math.round(totalGex),
    totalCallGex: Math.round(totalCallGexFiltered),
    totalPutGex: Math.round(totalPutGexFiltered),
    flipStrike,
    maxPosStrike,
    maxPosGex: Math.round(maxPosGex),
    maxNegStrike,
    maxNegGex: Math.round(maxNegGex),
    // Information-only greek scalars (net dealer exposure over the filtered band)
    vanna: Math.round(totalVex),
    charm: Math.round(totalCex),
    // Information-only traded-volume flow (all expirations, all contracts w/ volume)
    callVol: totalCallVol,
    putVol: totalPutVol,
    pcRatio: totalCallVol > 0 ? +(totalPutVol / totalCallVol).toFixed(2) : null,
    gammaCurve,
    windowPct: parseFloat((rangePct * 100).toFixed(2)),
    windowWidened: true,
    pctChange1m: null, // filled in by history comparison
    pctChange5m: null,
    walls,
    strikes: strikeResults.map(s => ({
      strike: s.strike,
      netGex: Math.round(s.netGex),
      callGex: Math.round(s.callGex),
      putGex: Math.round(s.putGex),
    })),
    events: [],
    commentary: null,
    updatedAt: new Date().toISOString(),
    expiry: expiriesToUse[0],
    expiryCount: expiriesToUse.length,
    dte,
  };
}

async function handleGEXUpdate(env, token, preChain = null) {
  // 1. Use pre-fetched master chain if available (saves 2 Schwab calls per tick),
  //    else fetch our own. preChain is the same shape we'd build below.
  let chainData, spot;
  if (preChain) {
    chainData = { callExpDateMap: preChain.callExpDateMap, putExpDateMap: preChain.putExpDateMap };
    spot = preChain.spot;
  } else {
    // GEX wants a wide strike window (±8% band + far walls) → strikeCount=150.
    const baseParams = 'symbol=%24SPX&strikeCount=150&includeUnderlyingQuote=true&strategy=SINGLE';
    const [callData, putData] = await Promise.all([
      fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/chains?${baseParams}&contractType=CALL`, token),
      fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/chains?${baseParams}&contractType=PUT`, token),
    ]);
    chainData = {
      callExpDateMap: callData.callExpDateMap || {},
      putExpDateMap: putData.putExpDateMap || {},
    };
    spot = callData.underlyingPrice || callData.underlying?.last || callData.underlying?.mark
        || putData.underlyingPrice || putData.underlying?.last || putData.underlying?.mark;
  }
  if (!spot) throw new Error('No SPX spot price in chain response');

  // 4. Calculate GEX — both all-expiry and 0DTE-only
  const gexData = calculateGEX(chainData, spot, false);     // all expirations
  const gex0dte = calculateGEX(chainData, spot, true);      // 0DTE only
  if (!gexData) throw new Error('GEX calculation returned null (no expirations)');

  // Store 0DTE snapshot separately
  if (gex0dte) {
    await env.SIGNAL_KV.put('gex_current_0dte', JSON.stringify(gex0dte));
  }

  // Store SPX price tick for live.html chart (replaces dead live_updater.py → spx_history.json)
  try {
    const etNowGex = toET(new Date());
    const todayGex = `${etNowGex.getFullYear()}-${String(etNowGex.getMonth()+1).padStart(2,'0')}-${String(etNowGex.getDate()).padStart(2,'0')}`;
    const hh = String(etNowGex.getHours()).padStart(2, '0');
    const mm = String(etNowGex.getMinutes()).padStart(2, '0');
    // Snap to 5-min intervals for consistency
    const mm5 = String(Math.floor(parseInt(mm) / 5) * 5).padStart(2, '0');
    const timeKey = `${hh}:${mm5}`;

    const spxHistKey = `spx_history_${todayGex}`;
    const spxHistRaw = await env.SIGNAL_KV.get(spxHistKey);
    const spxHist = spxHistRaw ? JSON.parse(spxHistRaw) : [];

    // Only add if this 5-min slot doesn't exist yet
    if (!spxHist.some(p => p.time === timeKey)) {
      spxHist.push({ time: timeKey, price: parseFloat(spot.toFixed(2)) });
      spxHist.sort((a, b) => a.time.localeCompare(b.time));
      await env.SIGNAL_KV.put(spxHistKey, JSON.stringify(spxHist), { expirationTtl: 86400 });
    }
  } catch (e) { console.warn('[gex] spx history tick:', e.message); }

  // 5. Load history from KV for % change tracking
  const historyRaw = await env.SIGNAL_KV.get('gex_history');
  let history = historyRaw ? JSON.parse(historyRaw) : [];

  // Calculate % changes
  const now = Date.now();
  if (history.length > 0) {
    // 1-min change: find snapshot closest to 1 min ago
    const target1m = now - 60_000;
    const snap1m = history.reduce((best, s) => {
      return Math.abs(s.ts - target1m) < Math.abs(best.ts - target1m) ? s : best;
    }, history[0]);
    if (snap1m.totalGex !== 0 && Math.abs(snap1m.ts - target1m) < 180_000) {
      gexData.pctChange1m = parseFloat(((gexData.totalGex - snap1m.totalGex) / Math.abs(snap1m.totalGex) * 100).toFixed(1));
    }

    // 5-min change
    const target5m = now - 300_000;
    const snap5m = history.reduce((best, s) => {
      return Math.abs(s.ts - target5m) < Math.abs(best.ts - target5m) ? s : best;
    }, history[0]);
    if (snap5m.totalGex !== 0 && Math.abs(snap5m.ts - target5m) < 600_000) {
      gexData.pctChange5m = parseFloat(((gexData.totalGex - snap5m.totalGex) / Math.abs(snap5m.totalGex) * 100).toFixed(1));
    }
  }

  // 5. Detect events by comparing with previous snapshot
  const events = [];
  const prevRaw = await env.SIGNAL_KV.get('gex_current');
  if (prevRaw) {
    const prev = JSON.parse(prevRaw);
    // Regime flip
    if (prev.regime && prev.regime !== gexData.regime) {
      events.push('regime_flip');
    }
    // Wall break: spot crossed a top-5 wall
    if (prev.walls && prev.spot) {
      const top5 = prev.walls.slice(0, 5);
      for (const wall of top5) {
        const crossed = (prev.spot < wall.strike && gexData.spot >= wall.strike) ||
                        (prev.spot > wall.strike && gexData.spot <= wall.strike);
        if (crossed) {
          events.push('wall_break');
          break;
        }
      }
    }
    // GEX surge: >20% change in total_gex
    if (prev.totalGex && prev.totalGex !== 0) {
      const pctChange = Math.abs((gexData.totalGex - prev.totalGex) / prev.totalGex * 100);
      if (pctChange > 20) events.push('gex_surge');
    }
  }
  gexData.events = events;

  // 5a. Append new events to persistent daily event log in KV
  if (events.length > 0) {
    try {
      const etNow = toET();
      const todayISO = `${etNow.getFullYear()}-${String(etNow.getMonth()+1).padStart(2,'0')}-${String(etNow.getDate()).padStart(2,'0')}`;
      const logKey = `gex_events_${todayISO}`;
      const logRaw = await env.SIGNAL_KV.get(logKey);
      const log = logRaw ? JSON.parse(logRaw) : [];
      const ts = gexData.updatedAt;
      for (const evt of events) {
        log.push({ type: evt, ts, spot: gexData.spot, regime: gexData.regime });
      }
      await env.SIGNAL_KV.put(logKey, JSON.stringify(log), { expirationTtl: 86400 });
    } catch (e) { console.warn('[gex] event log save failed:', e.message); }
  }

  // 5b. Generate AI commentary (every 15 min + on big events, 200/day hard limit)
  // Reuse prevRaw from step 5 to carry forward existing commentary
  const prevParsed = prevRaw ? JSON.parse(prevRaw) : null;
  try {
    const commentary = await generateGEXCommentary(env, gexData, events);
    if (commentary) {
      gexData.commentary = commentary;
      gexData.commentaryAt = new Date().toISOString();
      // Append to daily commentary log
      try {
        const etNow2 = toET();
        const todayISO2 = `${etNow2.getFullYear()}-${String(etNow2.getMonth()+1).padStart(2,'0')}-${String(etNow2.getDate()).padStart(2,'0')}`;
        const cLogKey = `gex_commentary_${todayISO2}`;
        const cLogRaw = await env.SIGNAL_KV.get(cLogKey);
        const cLog = cLogRaw ? JSON.parse(cLogRaw) : [];
        cLog.push({ text: commentary, ts: gexData.commentaryAt, spot: gexData.spot, regime: gexData.regime });
        await env.SIGNAL_KV.put(cLogKey, JSON.stringify(cLog), { expirationTtl: 86400 });
      } catch (e2) { console.warn('[gex] commentary log save failed:', e2.message); }
    } else if (prevParsed?.commentary) {
      gexData.commentary = prevParsed.commentary;
      gexData.commentaryAt = prevParsed.commentaryAt || null;
    }
  } catch (e) {
    console.warn('[proxy] Commentary generation failed:', e.message || e);
    if (prevParsed?.commentary) {
      gexData.commentary = prevParsed.commentary;
      gexData.commentaryAt = prevParsed.commentaryAt || null;
    }
  }

  // 5c. Append today's traded-volume snapshot to a daily intraday flow series.
  //     INFO-ONLY (powers the "Call vs Put Flow — Today" chart). Wrapped so a
  //     flow-capture failure can NEVER break the GEX update.
  if (typeof gexData.callVol === 'number' && gexData.callVol >= 0 &&
      typeof gexData.putVol === 'number' && gexData.putVol >= 0) {
    try {
      const etNowFlow = toET(new Date());
      const flowKey = `gex_flow_${isoDateET(etNowFlow)}`;
      const flowRaw = await env.SIGNAL_KV.get(flowKey);
      let flow = flowRaw ? JSON.parse(flowRaw) : [];
      flow.push({ ts: Math.floor(Date.now() / 1000), cv: gexData.callVol, pv: gexData.putVol });
      if (flow.length > 250) flow = flow.slice(-250);
      await env.SIGNAL_KV.put(flowKey, JSON.stringify(flow), { expirationTtl: 172800 }); // ~2 days
    } catch (e) { console.warn('[gex] flow series capture failed:', e.message); }
  }

  // 6. Store current snapshot in KV
  await env.SIGNAL_KV.put('gex_current', JSON.stringify(gexData));

  // 7. Append to history (keep last 60 snapshots for % change tracking)
  history.push({ ts: now, totalGex: gexData.totalGex, regime: gexData.regime });
  if (history.length > 60) history = history.slice(-60);
  await env.SIGNAL_KV.put('gex_history', JSON.stringify(history));

  // 8. Commit to GitHub every 10 min OR on big events
  const lastCommitRaw = await env.SIGNAL_KV.get('gex_last_github_commit');
  const lastCommitTs = lastCommitRaw ? parseInt(lastCommitRaw) : 0;
  const timeSinceCommit = now - lastCommitTs;
  const hasBigEvent = events.includes('regime_flip') || events.includes('gex_surge');

  if (timeSinceCommit >= 600_000 || hasBigEvent) {
    try {
      await commitGexToGitHub(env, gexData);
      await env.SIGNAL_KV.put('gex_last_github_commit', String(now));
    } catch (e) {
      console.warn('[proxy] GEX GitHub commit failed:', e.message || e);
    }
  }

  return { gex: 'updated', regime: gexData.regime, totalGex: gexData.totalGex, events };
}

// ════════════════════════════════════════════════════════════════════
// CLAUDE API COMMENTARY — 200 calls/day hard limit
// Generates dealer hedging commentary from GEX data.
// Runs every 15 min + immediately on big events (regime_flip, gex_surge, wall_break).
// ════════════════════════════════════════════════════════════════════

const ANTHROPIC_DAILY_LIMIT = 200;

async function getAnthropicCallCount(env) {
  const etNow = toET(new Date());
  const dateKey = `anthropic_calls_${etNow.getFullYear()}-${String(etNow.getMonth()+1).padStart(2,'0')}-${String(etNow.getDate()).padStart(2,'0')}`;
  const raw = await env.SIGNAL_KV.get(dateKey);
  return { count: raw ? parseInt(raw) : 0, dateKey };
}

async function incrementAnthropicCallCount(env, dateKey, currentCount) {
  await env.SIGNAL_KV.put(dateKey, String(currentCount + 1), { expirationTtl: 172800 }); // auto-expire after 48h
}

async function generateGEXCommentary(env, gexData, events) {
  const apiKey = env.ANTHROPIC_API_KEY;
  if (!apiKey) return null;

  // ── Hard daily limit check ──
  const { count, dateKey } = await getAnthropicCallCount(env);
  if (count >= ANTHROPIC_DAILY_LIMIT) {
    console.warn(`[proxy] Anthropic daily limit reached (${count}/${ANTHROPIC_DAILY_LIMIT})`);
    return null;
  }

  // ── Should we generate? Every 15 min OR on big events ──
  const hasBigEvent = events.includes('regime_flip') || events.includes('wall_break') || events.includes('gex_surge');
  const lastCommentaryRaw = await env.SIGNAL_KV.get('gex_last_commentary_ts');
  const lastCommentaryTs = lastCommentaryRaw ? parseInt(lastCommentaryRaw) : 0;
  const timeSinceCommentary = Date.now() - lastCommentaryTs;

  if (!hasBigEvent && timeSinceCommentary < 300_000) { // 5 min = 300,000 ms
    return null; // not time yet, no big event
  }

  // ── Build prompt ──
  const top5Walls = (gexData.walls || []).slice(0, 5).map(w =>
    `  ${w.strike}: net ${w.netGex > 0 ? '+' : ''}${(w.netGex/1e6).toFixed(1)}M (${w.direction})`
  ).join('\n');

  const eventDesc = events.length > 0
    ? `EVENTS JUST DETECTED: ${events.join(', ')}`
    : 'No new events';

  const prompt = `You are a professional options market analyst. Analyze this real-time SPX Gamma Exposure (GEX) data and provide a concise dealer hedging commentary.

CURRENT GEX SNAPSHOT:
- SPX Spot: ${gexData.spot}
- Regime: ${gexData.regime} (${gexData.regime === 'PIN' ? 'dealers are long gamma → they sell rallies/buy dips → stabilizing, expect mean reversion' : 'dealers are short gamma → they buy rallies/sell dips → amplifying, expect trending/volatile moves'})
- Total GEX: ${(gexData.totalGex/1e6).toFixed(1)}M
- GEX Flip Strike: ${gexData.flipStrike || 'N/A'}
- Max Positive Gamma: ${gexData.maxPosStrike} (${(gexData.maxPosGex/1e6).toFixed(1)}M) — dealers sell here
- Max Negative Gamma: ${gexData.maxNegStrike} (${(gexData.maxNegGex/1e6).toFixed(1)}M) — dealers buy here
- 1-min % change: ${gexData.pctChange1m != null ? gexData.pctChange1m + '%' : 'N/A'}
- 5-min % change: ${gexData.pctChange5m != null ? gexData.pctChange5m + '%' : 'N/A'}

TOP 5 GEX WALLS:
${top5Walls}

${eventDesc}

Provide exactly 2-3 sentences of actionable dealer hedging commentary. Focus on:
1. What dealers are likely doing RIGHT NOW based on spot vs key levels
2. Expected price behavior given the regime and GEX profile
3. Key strikes to watch

Then add a final line starting with "Bottom line:" — a single short plain-English sentence (no jargon) summarizing what this means for price direction. Example: "Bottom line: expect choppy sideways action around 5500."

Be direct and technical in the main commentary. No disclaimers. Use trader language.`;

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);
    let resp;
    try {
      resp = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': apiKey,
          'anthropic-version': '2023-06-01',
        },
        body: JSON.stringify({
          model: 'claude-sonnet-4-20250514',
          max_tokens: 300,
          messages: [{ role: 'user', content: prompt }],
        }),
        signal: controller.signal,
      });
      clearTimeout(timeout);
    } catch (e) { clearTimeout(timeout); throw e; }

    if (!resp.ok) {
      const errText = await resp.text();
      console.warn(`[proxy] Anthropic API error ${resp.status}: ${errText.slice(0, 200)}`);
      return null;
    }

    const result = await resp.json();
    const text = result.content?.[0]?.text || null;

    // ── Increment call count AFTER successful call ──
    await incrementAnthropicCallCount(env, dateKey, count);
    await env.SIGNAL_KV.put('gex_last_commentary_ts', String(Date.now()));

    console.log(`[proxy] Anthropic call ${count + 1}/${ANTHROPIC_DAILY_LIMIT} — commentary generated`);
    return text;
  } catch (e) {
    console.warn('[proxy] Anthropic API call failed:', e.message || e);
    return null;
  }
}

async function commitGexToGitHub(env, gexData) {
  const ghToken = env.GITHUB_TOKEN;
  if (!ghToken) return;

  const apiUrl = 'https://api.github.com/repos/rava8989/brave/contents/gex_data.json';
  const headers = {
    'Authorization': `Bearer ${ghToken}`,
    'Accept': 'application/vnd.github+json',
    'User-Agent': 'schwab-proxy-worker/1.0',
    'X-GitHub-Api-Version': '2022-11-28',
  };

  // Get current file SHA (may not exist yet)
  let sha = null;
  try {
    const getResp = await fetch(apiUrl, { headers });
    if (getResp.ok) {
      const meta = await getResp.json();
      sha = meta.sha;
    }
  } catch (e) { /* file may not exist yet */ }

  const body = {
    message: `auto: GEX update ${gexData.regime} ${new Date().toISOString().slice(0, 16)}`,
    content: btoa(unescape(encodeURIComponent(JSON.stringify(gexData, null, 2)))),
  };
  if (sha) body.sha = sha;

  const putResp = await fetch(apiUrl, {
    method: 'PUT',
    headers: { ...headers, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!putResp.ok) {
    const err = await putResp.text();
    throw new Error(`GitHub PUT gex_data.json failed: ${putResp.status} — ${err.slice(0, 200)}`);
  }
}

// ════════════════════════════════════════════════════════════════════
// KV-BACKED HISTORY STORE (Item 5)
// ────────────────────────────────────────────────────────────────────
// Single source of truth: KV key `history_data` (JSON array).
// Worker writes are fast (~10ms) and atomic per key. GitHub mirror
// happens asynchronously after each KV write so git history is preserved
// but no caller waits for the 1-2s PUT.
//
// First read on a fresh deploy falls back to GitHub raw (one-time seed),
// then everything stays in KV. POST /history-migrate forces a re-seed.
// ════════════════════════════════════════════════════════════════════

const HISTORY_KV_KEY = 'history_data';
const HISTORY_GH_RAW = 'https://raw.githubusercontent.com/rava8989/brave/main/history_data.json';

async function getHistory(env) {
  // Primary read: KV (fast)
  try {
    const raw = await env.SIGNAL_KV.get(HISTORY_KV_KEY);
    if (raw) {
      const data = JSON.parse(raw);
      if (Array.isArray(data) && data.length > 0) return data;
    }
  } catch (e) {
    console.warn('[history] KV read failed, falling back to GitHub:', e.message);
  }
  // Fallback: GitHub raw (cold start, or KV was wiped). Also seeds KV.
  try {
    const ghResp = await fetch(`${HISTORY_GH_RAW}?t=${Date.now()}`,
      { headers: { 'User-Agent': 'schwab-proxy', 'Cache-Control': 'no-cache' } });
    if (!ghResp.ok) throw new Error(`GitHub raw ${ghResp.status}`);
    const data = await ghResp.json();
    if (Array.isArray(data)) {
      // Seed KV so subsequent reads are fast (do NOT await — fire-and-forget)
      try { await env.SIGNAL_KV.put(HISTORY_KV_KEY, JSON.stringify(data)); } catch (_) {}
      return data;
    }
    throw new Error('GitHub raw returned non-array');
  } catch (e) {
    console.error('[history] GitHub fallback failed:', e.message);
    return [];
  }
}

async function setHistory(env, contentArray, opts = {}) {
  // Snapshot pre-write state to KV backups index (re-uses Item 4 helper).
  if (!opts.skipBackup) {
    try {
      const prev = await getHistory(env);
      await backupHistorySnapshot(env, prev, opts.dateStr || 'kv-write', opts.fields || {});
    } catch (e) {
      console.warn('[history] backup before setHistory failed:', e.message);
    }
  }
  // Primary write: KV (atomic per key, ~10ms)
  await env.SIGNAL_KV.put(HISTORY_KV_KEY, JSON.stringify(contentArray));
}

async function mirrorHistoryToGitHub(env, contentArray, message) {
  // Async GitHub mirror — preserves git history but doesn't block writes.
  // Errors logged but never thrown (mirroring failure must not break trades).
  if (!env.GITHUB_TOKEN) return { skipped: 'no GITHUB_TOKEN' };
  const apiUrl = 'https://api.github.com/repos/rava8989/brave/contents/history_data.json';
  const ghHeaders = {
    'Authorization': `Bearer ${env.GITHUB_TOKEN}`,
    'Accept': 'application/vnd.github+json',
    'User-Agent': 'schwab-proxy-worker/1.0',
    'X-GitHub-Api-Version': '2022-11-28',
  };
  // Retry on sha conflict (409/422) + transient errors. Two near-simultaneous
  // settles (e.g. m8bf + tail at EOD) each do GET-sha→PUT; GitHub's sha lags
  // briefly so the second PUT 409s. The old code swallowed that with NO retry
  // → KV had tailPL but GitHub never did (2026-06-23 tail-PL drift). On each
  // retry we re-GET a fresh sha AND re-read the latest KV, so the GitHub copy
  // converges to current KV regardless of which mirror lands last.
  const maxAttempts = 4;
  let lastErr = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      let body = contentArray;
      if (attempt > 1) {
        await new Promise(r => setTimeout(r, 250 * (attempt - 1)));
        try { const fresh = await getHistory(env); if (Array.isArray(fresh) && fresh.length) body = fresh; } catch (_) {}
      }
      const getResp = await fetch(apiUrl, { headers: ghHeaders });
      if (!getResp.ok) throw new Error(`GH GET ${getResp.status}`);
      const meta = await getResp.json();
      const putResp = await fetch(apiUrl, {
        method: 'PUT',
        headers: { ...ghHeaders, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message: message || 'auto: KV mirror',
          content: btoa(JSON.stringify(body, null, 0)),
          sha: meta.sha,
        }),
      });
      if (putResp.status === 409 || putResp.status === 422) {
        lastErr = `GH PUT ${putResp.status} (sha conflict)`;
        continue; // sha raced — re-GET + retry
      }
      if (!putResp.ok) {
        const err = await putResp.text();
        throw new Error(`GH PUT ${putResp.status}: ${err.slice(0, 200)}`);
      }
      await recordMirrorHealth(env, true);
      return { ok: true, attempts: attempt };
    } catch (e) {
      lastErr = e.message;
      if (attempt === maxAttempts) break;
    }
  }
  console.warn('[history-mirror] failed after retries (KV state still good):', lastErr);
  await recordMirrorHealth(env, false, lastErr);
  return { ok: false, error: lastErr };
}

// ════════════════════════════════════════════════════════════════════
// GITHUB HISTORY UPSERT (legacy name — now writes KV first, mirrors to GH)
// Same merge semantics: alwaysOverwrite=['vixClose','spxClose','m8bfWR'].
// ════════════════════════════════════════════════════════════════════

// ── Recover today's spxOpen + vixOpen from Schwab 9:30 candle ──
// Used as auto-recovery when the morning signal block failed to write them
// to history (Schwab outage at 9:30, claim stuck, etc.). The first 1-min
// candle at 9:30 ET has the OPEN of the regular session — this matches TOS.
// Returns { spxOpen, vixOpen } with either field possibly null if its
// candle didn't materialize.
async function recoverOpenPricesFromSchwab(env, etNow) {
  const token = await getAccessToken(env);
  const todayDateStr = etNow.toDateString();
  const baseUrl = (sym) =>
    `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=${sym}&periodType=day&period=2&frequencyType=minute&frequency=1&needExtendedHoursData=false`;

  async function fetchOpen(symbol) {
    try {
      const hist = await fetchSchwabJSON(baseUrl(symbol), token, env);
      const cs = hist.candles || [];
      // Prefer the exact 9:30 minute bar; fall back to the first bar in
      // 9:30-9:35 if 9:30 itself is missing.
      const todayCandles = cs.filter(c => toET(new Date(c.datetime)).toDateString() === todayDateStr);
      todayCandles.sort((a, b) => a.datetime - b.datetime);
      const exact930 = todayCandles.find(c => {
        const d = toET(new Date(c.datetime));
        return d.getHours() === 9 && d.getMinutes() === 30;
      });
      const fallback = todayCandles.find(c => {
        const d = toET(new Date(c.datetime));
        return d.getHours() === 9 && d.getMinutes() >= 30 && d.getMinutes() <= 35;
      });
      const pick = exact930 || fallback;
      return pick?.open != null ? parseFloat(pick.open.toFixed(2)) : null;
    } catch (e) {
      console.warn(`[recover-open] ${symbol} fetch failed:`, e.message);
      return null;
    }
  }

  const [spxOpen, vixOpen] = await Promise.all([fetchOpen('%24SPX'), fetchOpen('%24VIX')]);
  return { spxOpen, vixOpen };
}

// ── Snapshot the PRE-WRITE history state into KV (last 10 only) ──
// Defense against bad writes: any field overwrite, deleted row, or schema
// regression can be inspected/restored from these. Each backup is its own
// KV key (cheap reads); an index key tracks them for rotation.
async function backupHistorySnapshot(env, contentJson, dateStr, fields) {
  const ts = new Date().toISOString();
  const backupKey = `history_backup_${ts}`;
  try {
    await env.SIGNAL_KV.put(backupKey, JSON.stringify(contentJson), { expirationTtl: 14 * 86400 });
    let idx = [];
    try {
      const idxRaw = await env.SIGNAL_KV.get('history_backups_index');
      idx = idxRaw ? JSON.parse(idxRaw) : [];
      if (!Array.isArray(idx)) idx = [];
    } catch { idx = []; }
    idx.unshift({ key: backupKey, ts, dateStr, fields: Object.keys(fields || {}) });
    while (idx.length > 10) {
      const drop = idx.pop();
      try { await env.SIGNAL_KV.delete(drop.key); } catch (delErr) {
        console.warn('[history-backup] rotate delete failed:', delErr.message);
      }
    }
    await env.SIGNAL_KV.put('history_backups_index', JSON.stringify(idx));
  } catch (e) {
    // Non-fatal: write proceeds even if backup fails. Don't block trading.
    console.warn('[history-backup] snapshot failed:', e.message);
  }
}

async function upsertHistoryGitHub(env, dateStr, fields, _retries = 3) {
  // 1. Read current state from KV (Item 5 — primary store)
  const content = await getHistory(env);
  if (!Array.isArray(content) || content.length === 0) {
    throw new Error('history KV empty — run POST /history-migrate to seed from GitHub first');
  }

  // 1a. Backup PRE-WRITE state to KV (Item 4)
  await backupHistorySnapshot(env, content, dateStr, fields);

  // 2. Upsert today's row (same merge semantics as before)
  const idx = content.findIndex(r => r.date === dateStr);
  if (idx >= 0) {
    for (const [k, v] of Object.entries(fields)) {
      const alwaysOverwrite = ['vixClose', 'spxClose', 'm8bfWR'].includes(k);
      if (alwaysOverwrite || content[idx][k] == null) content[idx][k] = v;
    }
  } else {
    content.push({ date: dateStr, ...fields });
    content.sort((a, b) => a.date.localeCompare(b.date));
  }

  // 3. Ensure 10 future trading day placeholders always exist (skip weekends + holidays)
  const today = dateStr;
  const lastDate = content[content.length - 1]?.date || today;
  const futureRows = content.filter(r => r.date > today).length;
  if (futureRows < 10) {
    const needed = 10 - futureRows;
    let d = new Date(lastDate + 'T12:00:00Z');
    let added = 0;
    const existingDates = new Set(content.map(r => r.date));
    while (added < needed) {
      d.setUTCDate(d.getUTCDate() + 1);
      const dow = d.getUTCDay();
      if (dow === 0 || dow === 6) continue;
      if (isHol(d)) continue;
      const iso = d.toISOString().slice(0, 10);
      if (!existingDates.has(iso)) {
        content.push({ date: iso });
        existingDates.add(iso);
        added++;
      }
    }
    content.sort((a, b) => a.date.localeCompare(b.date));
  }

  // 4. Primary write to KV — atomic, ~10ms (no merge conflicts ever)
  await setHistory(env, content, { dateStr, fields, skipBackup: true });

  // 5. Mirror to GitHub asynchronously (git history + backup; never blocks).
  //    We swallow errors here so a flaky GitHub doesn't break trading.
  if (env.GITHUB_TOKEN) {
    try {
      await mirrorHistoryToGitHub(env, content,
        `auto: history update for ${dateStr} (${Object.keys(fields).join(', ')})`);
    } catch (e) {
      console.warn('[history-mirror] non-fatal:', e.message);
    }
  }
}

// ════════════════════════════════════════════════════════════════════
// BACKFILL MISSING m8bfWR
// ════════════════════════════════════════════════════════════════════

async function fetchAllDiscordSignalsForDate(token, channelId, dateISO) {
  // Fetch all butterfly signals posted on dateISO ET, paginated
  const [y, m, d] = dateISO.split('-').map(Number);
  // 12:00-22:00 UTC covers both EDT (9:30-4 ET = 13:30-20 UTC) and EST (9:30-4 ET = 14:30-21 UTC)
  const startMs = Date.UTC(y, m - 1, d, 12, 0, 0);
  const endMs   = Date.UTC(y, m - 1, d, 22, 0, 0);
  const discordEpoch = 1420070400000n;
  let afterSnowflake = ((BigInt(startMs) - discordEpoch) << 22n).toString();
  const beforeSnowflake = ((BigInt(endMs) - discordEpoch) << 22n).toString();

  const allSignals = [];

  for (let page = 0; page < 5; page++) {
    const resp = await fetch(
      `https://discord.com/api/v9/channels/${channelId}/messages?limit=100&after=${afterSnowflake}&before=${beforeSnowflake}`,
      { headers: { 'Authorization': token, 'User-Agent': 'Mozilla/5.0' } }
    );
    if (!resp.ok) throw new Error(`Discord API ${resp.status} for ${dateISO}`);
    const batch = await resp.json();
    if (!Array.isArray(batch) || batch.length === 0) break;

    batch.sort((a, b) => a.id.localeCompare(b.id));
    for (const msg of batch) {
      // Verify message is from the correct ET date
      const msgET = toET(new Date(msg.timestamp));
      const msgDate = `${msgET.getFullYear()}-${String(msgET.getMonth()+1).padStart(2,'0')}-${String(msgET.getDate()).padStart(2,'0')}`;
      if (msgDate !== dateISO) continue;

      const sig = parseDiscordSignal(msg.content || '');
      if (!sig) continue;
      // Attach posting time (ET) for TRADES TIME column
      sig.time = `${String(msgET.getHours()).padStart(2,'0')}:${String(msgET.getMinutes()).padStart(2,'0')}`;
      allSignals.push(sig); // no dedup — each post counts
    }
    if (batch.length < 100) break;
    afterSnowflake = batch[batch.length - 1].id;
  }
  return allSignals;
}

// ── Append today's full Discord signals to scraped_signals.csv on GitHub ──
async function appendScrapedSignals(env, etNow) {
  const todayISO = `${etNow.getFullYear()}-${String(etNow.getMonth()+1).padStart(2,'0')}-${String(etNow.getDate()).padStart(2,'0')}`;
  const token = env.DISCORD_USER_TOKEN;
  const channelId = '1048242197029458040';

  // Check if already appended today
  const doneKey = `scrape_appended_${todayISO}`;
  if (await env.SIGNAL_KV.get(doneKey)) return { skipped: true, date: todayISO };

  // Fetch raw Discord messages for today
  const [y, m, d] = todayISO.split('-').map(Number);
  const startMs = Date.UTC(y, m - 1, d, 12, 0, 0);
  const endMs = Date.UTC(y, m - 1, d, 22, 0, 0);
  const discordEpoch = 1420070400000n;
  let afterSnowflake = ((BigInt(startMs) - discordEpoch) << 22n).toString();
  const beforeSnowflake = ((BigInt(endMs) - discordEpoch) << 22n).toString();

  const rows = [];
  for (let page = 0; page < 10; page++) {
    const resp = await fetch(
      `https://discord.com/api/v9/channels/${channelId}/messages?limit=100&after=${afterSnowflake}&before=${beforeSnowflake}`,
      { headers: { 'Authorization': token, 'User-Agent': 'Mozilla/5.0' } }
    );
    if (!resp.ok) break;
    const batch = await resp.json();
    if (!Array.isArray(batch) || batch.length === 0) break;
    batch.sort((a, b) => a.id.localeCompare(b.id));

    for (const msg of batch) {
      const content = msg.content || '';
      const msgET = toET(new Date(msg.timestamp));
      const msgDate = `${msgET.getFullYear()}-${String(msgET.getMonth()+1).padStart(2,'0')}-${String(msgET.getDate()).padStart(2,'0')}`;
      if (msgDate !== todayISO) continue;
      const msgTime = `${String(msgET.getHours()).padStart(2,'0')}:${String(msgET.getMinutes()).padStart(2,'0')}`;

      const priceM = content.match(/Price:\s*([\d.]+)/i);
      if (!priceM) continue; // not a signal message

      const field = (pat) => { const m = content.match(pat); return m ? m[1] : ''; };
      const bfStrikesM = content.match(/Butterfly[^/]*(\d{4,5})\/(\d{4,5})\/(\d{4,5})\s*(CALL|PUT)/i);
      const bfPriceM = content.match(/Butterfly[^@]*@([\d.]+)/i);
      const icStrikesM = content.match(/Iron Condor[^/]*(\d{4,5})\/(\d{4,5})\/(\d{4,5})\/(\d{4,5})/i);
      const icPriceM = content.match(/Iron Condor[^@]*@([\d.]+)/i);
      const vtStrikesM = content.match(/Vertical[^/]*(\d{4,5})\/(\d{4,5})\s*(CALL|PUT)/i);
      const vtPriceM = content.match(/Vertical[^@]*@([\d.]+)/i);

      // Sonar IC (appears after "Sonar:" label in the message)
      const sonarBlock = content.split(/Sonar:/i)[1] || '';
      const sonarStrikesM = sonarBlock.match(/Iron Condor[^/]*(\d{4,5})\/(\d{4,5})\/(\d{4,5})\/(\d{4,5})/i);
      const sonarPriceM = sonarBlock.match(/Iron Condor[^@]*@([\d.]+)/i);

      const csvLine = [
        msg.timestamp, msgDate, msgTime,
        priceM[1],
        field(/Trend:\s*(\w+)/i),
        field(/Predicted Close:\s*([\d.]+)/i),
        field(/Strength:\s*([\d.]+)/i),
        field(/Short term:\s*([\d.]+)/i),
        field(/Long term:\s*([\d.]+)/i),
        field(/Short term bias:\s*(\w+)/i),
        field(/Long term bias:\s*(\w+)/i),
        field(/Calls:\s*([\d.]+)/i),
        field(/Puts:\s*([\d.]+)/i),
        field(/Center:\s*([\d.]+)/i),
        field(/Range:\s*([\d.]+)/i),
        field(/Target 1:\s*([\d.]+)/i),
        field(/Target 2:\s*([\d.]+)/i),
        field(/Delta:\s*([\d.]+)/i),
        field(/Gamma:\s*([\d.]+)/i),
        field(/Interest:\s*([\d.]+)/i),
        field(/Sonar:\s*([\d.]+)/i),
        field(/Volume:\s*([\d.]+)/i),
        bfStrikesM ? 'BUY' : '',
        bfStrikesM ? `${bfStrikesM[1]}/${bfStrikesM[2]}/${bfStrikesM[3]}` : '',
        bfStrikesM ? bfStrikesM[2] : '',
        bfStrikesM ? bfStrikesM[1] : '',
        bfStrikesM ? bfStrikesM[3] : '',
        bfPriceM ? bfPriceM[1] : '',
        icStrikesM ? 'SELL' : '',
        icStrikesM ? `${icStrikesM[1]}/${icStrikesM[2]}/${icStrikesM[3]}/${icStrikesM[4]}` : '',
        icPriceM ? icPriceM[1] : '',
        sonarStrikesM ? 'SELL' : '',
        sonarStrikesM ? `${sonarStrikesM[1]}/${sonarStrikesM[2]}/${sonarStrikesM[3]}/${sonarStrikesM[4]}` : '',
        sonarPriceM ? sonarPriceM[1] : '',
        vtStrikesM ? 'SELL' : '',
        vtStrikesM ? `${vtStrikesM[1]}/${vtStrikesM[2]}` : '',
        vtStrikesM ? vtStrikesM[3] : '',
        vtPriceM ? vtPriceM[1] : '',
      ].map(v => String(v).includes(',') ? `"${v}"` : v).join(',');
      rows.push(csvLine);
    }
    if (batch.length < 100) break;
    afterSnowflake = batch[batch.length - 1].id;
  }

  if (rows.length === 0) return { date: todayISO, appended: 0 };

  // Fetch current scraped_signals.csv from GitHub, append rows, push
  const apiUrl = 'https://api.github.com/repos/rava8989/brave/contents/scraped_signals.csv';
  const ghHeaders = {
    'Authorization': `Bearer ${env.GITHUB_TOKEN}`,
    'Accept': 'application/vnd.github+json',
    'User-Agent': 'schwab-proxy-worker/1.0',
    'X-GitHub-Api-Version': '2022-11-28',
  };

  const getResp = await fetch(apiUrl, { headers: ghHeaders });
  let existingContent = '', sha = '';
  if (getResp.ok) {
    const meta = await getResp.json();
    sha = meta.sha;
    existingContent = atob(meta.content.replace(/\n/g, ''));
  }

  const newContent = existingContent.trimEnd() + '\n' + rows.join('\n') + '\n';
  const putResp = await fetch(apiUrl, {
    method: 'PUT',
    headers: ghHeaders,
    body: JSON.stringify({
      message: `auto: append ${rows.length} scraped signals for ${todayISO}`,
      content: btoa(newContent),
      sha,
    }),
  });

  if (!putResp.ok) throw new Error(`GitHub PUT failed: ${putResp.status}`);
  await env.SIGNAL_KV.put(doneKey, 'done', { expirationTtl: 86400 * 3 });
  return { date: todayISO, appended: rows.length };
}

function computeWinRateFromSignals(signals, spxClose) {
  if (!signals || signals.length === 0) return null;
  let wins = 0;
  for (const sig of signals) {
    const intrinsic = Math.max(0, Math.min(spxClose - sig.lower, sig.upper - spxClose));
    if (intrinsic > sig.premium) wins++;
  }
  return Math.round(wins / signals.length * 100);
}

async function getSpxCloseForDate(dateISO) {
  // Use Stooq CSV API (works from Cloudflare Workers)
  const [y, m, d] = dateISO.split('-');
  const dateCompact = `${y}${m}${d}`;
  const url = `https://stooq.com/q/d/l/?s=^spx&d1=${dateCompact}&d2=${dateCompact}&i=d`;
  const resp = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
  if (!resp.ok) throw new Error(`Stooq fetch failed: ${resp.status}`);
  const text = await resp.text();
  // CSV: Date,Open,High,Low,Close,Volume
  const lines = text.trim().split('\n');
  if (lines.length < 2) throw new Error('No data from Stooq');
  const parts = lines[1].split(',');
  const close = parseFloat(parts[4]);
  if (isNaN(close)) throw new Error('Invalid close from Stooq');
  return parseFloat(close.toFixed(2));
}

async function backfillMissingWR(env, force = false, targetDates = null) {
  const token = env.DISCORD_USER_TOKEN;
  const channelId = '1048242197029458040';
  if (!token) throw new Error('DISCORD_USER_TOKEN not set');

  // 1. Read history from KV (source of truth). The old path read+PUT GitHub directly,
  //    which the next KV→GitHub mirror reverted → perpetual backfill/null churn. Now
  //    KV-first: read KV, mutate, setHistory + mirror once (audit 2026-06-22).
  const content = await getHistory(env);

  // 2. Find entries to process
  const today = new Date().toISOString().slice(0, 10);
  const missing = content.filter(r => {
    if (r.date > today) return false;
    if (targetDates) return targetDates.includes(r.date); // specific dates
    if (force) return true; // all past dates
    return r.m8bfWR == null; // default: only missing
  });

  const filled = [];
  const failed = [];

  for (const entry of missing) {
    try {
      // Get SPX close
      const spxClose = await getSpxCloseForDate(entry.date);
      if (spxClose == null) { failed.push({ date: entry.date, reason: 'no SPX close' }); continue; }

      // Fetch ALL butterfly signals for that day from Discord
      const signals = await fetchAllDiscordSignalsForDate(token, channelId, entry.date);
      if (signals.length === 0) {
        failed.push({ date: entry.date, reason: 'no signals found in Discord' }); continue;
      }

      // Win rate = % of signals where intrinsic > premium
      const m8bfWR = computeWinRateFromSignals(signals, spxClose);

      // Update in-memory content (always overwrite m8bfWR)
      const idx = content.findIndex(r => r.date === entry.date);
      if (idx >= 0) {
        content[idx].m8bfWR = m8bfWR;
        if (content[idx].spxClose == null) content[idx].spxClose = spxClose;
      } else {
        content.push({ date: entry.date, spxClose, m8bfWR });
        content.sort((a, b) => a.date.localeCompare(b.date));
      }
      filled.push({ date: entry.date, spxClose, signals: signals.length, m8bfWR });
    } catch (e) {
      failed.push({ date: entry.date, reason: e.message });
    }
  }

  // 3. Persist KV-first (source of truth), then mirror ONCE to GitHub.
  if (filled.length > 0) {
    await setHistory(env, content, { dateStr: 'backfill-wr' });
    await mirrorHistoryToGitHub(env, content, `auto: backfill m8bfWR for ${filled.map(f => f.date).join(', ')}`);
  }

  return { filled, failed, total_missing: missing.length };
}

// ════════════════════════════════════════════════════════════════════
// BACKFILL MISSING m8bfPL
// ════════════════════════════════════════════════════════════════════

// Trade entry windows per JS getDay() (0=Sun,1=Mon...6=Sat) — from discord_scraper.py
const M8BF_WINDOWS = {
  1: [11*60,     11*60+30],  // Mon 11:00–11:30
  2: [13*60+30,  14*60],     // Tue 13:30–14:00
  3: [12*60,     12*60+30],  // Wed 12:00–12:30
  4: [11*60,     11*60+30],  // Thu 11:00–11:30
  5: [13*60,     13*60+30],  // Fri 13:00–13:30
};
const M8BF_WINDOWS_2ND_THU = [13*60+30, 14*60]; // 2nd trading Thu: 13:30–14:00

// Is dateISO the 2nd trading Thursday of its month?
function isSecondTradingThursday(dateISO) {
  const d = new Date(dateISO + 'T12:00:00Z');
  if (d.getUTCDay() !== 4) return false; // not Thursday
  const year = d.getUTCFullYear(), month = d.getUTCMonth();
  let thuCount = 0;
  for (let day = 1; day <= d.getUTCDate(); day++) {
    const check = new Date(Date.UTC(year, month, day));
    if (check.getUTCDay() === 4) thuCount++;
  }
  return thuCount === 2;
}

function getM8BFWindow(dow, dateISO) {
  if (dow === 4 && isSecondTradingThursday(dateISO)) return M8BF_WINDOWS_2ND_THU;
  return M8BF_WINDOWS[dow];
}

// M8BF banned-day reason — pure date math, no side effects. Mirrors the
// inline check that GET /trade used (signal-engine.js m8bfBanned + NM-non-Mon
// + CPI). Returns the human reason string, or null when M8BF is tradeable.
// Returns the M8BF ban reason for `etNow`, OR null if M8BF is allowed.
// HONORS the 90% override: if prevWR ≥ 90% and it's not a CPI day and GXBF
// isn't firing today, the override forces M8BF through any calendar ban.
// Mirrors signal-engine.js calculateSignal lines ~489-495 — keeps /trade
// (and refreshM8bfLiveQuotes) in lock-step with the Discord auto-message.
// 2026-05-28: bug bit when EOM-1 + prevWR=99% — /trade said banned, Discord
// said fire. Both now agree via the same override path.
async function m8bfBannedReason(env, etNow) {
  const eomDay = isLastTradeMo(etNow);
  const eom1   = isEomN(1, etNow);
  const opex1  = opexSch.some(ds => isTodayBefore(ds, etNow));
  const vixExpAfterOpex = isVixAfterOpexDay(etNow);
  const nonAmznTslaEarn = isNonAmznTslaEarningsDay(etNow);
  const cpiDay = cpiSch.includes(todayLong(etNow));
  const nmDay = isFirstTradeMo(etNow);
  const nmMon = isFirstTradeMon(etNow);
  const nmNonMon = nmDay && !nmMon;
  const m8bfBanned = eomDay || eom1 || opex1 || vixExpAfterOpex || nonAmznTslaEarn || nmNonMon;
  if (!(m8bfBanned || cpiDay)) return null;
  if (cpiDay) return 'CPI day';   // CPI is never overridden

  // 90% override check — prevWR from most recent prior history row.
  try {
    const todayISO = isoDateET(etNow);
    const hist = await getHistory(env);
    if (Array.isArray(hist) && hist.length) {
      const sorted = hist
        .filter(r => r.date && r.date < todayISO && r.m8bfWR != null)
        .sort((a, b) => b.date.localeCompare(a.date));
      const prevWR = sorted.length ? parseFloat(sorted[0].m8bfWR) : null;
      if (prevWR != null && prevWR >= 90) {
        // Override fires unless GXBF would also fire today (90% rule cannot
        // cancel GXBF — strategy independence).
        const todayRow = hist.find(r => r.date === todayISO);
        const vToday  = todayRow?.vixOpen != null ? parseFloat(todayRow.vixOpen) : null;
        const vYClose = sorted[0]?.vixClose != null ? parseFloat(sorted[0].vixClose) : null;
        const oNight  = (vToday != null && vYClose != null) ? (vYClose - vToday) : null;
        const gxbfFires = oNight != null && oNight > 0.65 && vToday < 25;
        if (!gxbfFires) return null;   // M8BF fires via 90% override
      }
    }
  } catch { /* if history fetch fails, fall through to ban-reason return */ }

  return eomDay ? 'EOM'
       : eom1   ? 'EOM-1'
       : opex1  ? 'day before OPEX'
       : vixExpAfterOpex ? 'VIX exp day'
       : nonAmznTslaEarn ? 'earnings'
       : nmNonMon ? 'NM (Straddle day)'
       : 'banned';
}

// Pure M8BF qualifying-signal selection (no side effects, no Discord poll).
// SINGLE SOURCE OF TRUTH shared by GET /trade and refreshM8bfLiveQuotes so
// the live-quoted legs are ALWAYS the exact trade /trade reports (real money:
// a divergence would mark-to-market the wrong strikes). Caller handles the
// banned-day gate first via m8bfBannedReason(). Byte-faithful to the previous
// inline /trade selection.
async function selectM8bfQualifying(env, etNow) {
  const todayT = isoDateET(etNow);
  const dow = etNow.getDay();
  const win = getM8BFWindow(dow, todayT);
  const sigRaw = await env.SIGNAL_KV.get('signals_today');
  const sigData = sigRaw ? JSON.parse(sigRaw) : { date: '', signals: [] };
  if (!win || sigData.date !== todayT) {
    return { status: 'waiting', reason: 'No window today or no signals', todayT };
  }
  // Manual-cancellation skip list — write this KV to ignore specific signal
  // times for the rest of today (bot keeps monitoring for any other signal
  // in the window). Cleared automatically by EOD via TTL.
  //   key:   m8bf_skip_signals_<YYYY-MM-DD>
  //   value: JSON array of "HH:MM" times, e.g. ["13:02"]
  let skipTimes = new Set();
  try {
    const skipRaw = await env.SIGNAL_KV.get(`m8bf_skip_signals_${todayT}`);
    if (skipRaw) skipTimes = new Set(JSON.parse(skipRaw) || []);
  } catch (_) { /* no-op */ }

  const [winLo, winHi] = win;
  let qualifying = null;
  for (const sig of (sigData.signals || [])) {
    if (!sig.time) continue;
    if (skipTimes.has(sig.time)) continue;   // ← manual cancellation
    const [h, m] = sig.time.split(':').map(Number);
    const mins = h * 60 + m;
    if (mins >= winLo && mins < winHi && !sig.banned) { qualifying = sig; break; }
  }
  if (!qualifying) {
    const nowMins = etNow.getHours() * 60 + etNow.getMinutes();
    const winStr = `${Math.floor(winLo/60)}:${String(winLo%60).padStart(2,'0')}-${Math.floor(winHi/60)}:${String(winHi%60).padStart(2,'0')}`;
    if (nowMins >= winHi) {
      const reason = skipTimes.size
        ? `Window passed — ${skipTimes.size} signal(s) manually cancelled (${[...skipTimes].join(', ')})`
        : 'Window passed, no qualifying signal';
      return { status: 'no_signal', reason, todayT };
    }
    return {
      status: 'waiting',
      window: winStr,
      reason: skipTimes.size
        ? `Cancelled ${[...skipTimes].join(', ')} — watching for new signal in ${winStr} ET`
        : undefined,
      todayT,
    };
  }
  return { status: 'open', qualifying, todayT };
}

async function backfillMissingPL(env, targetDates = null) {
  const token = env.DISCORD_USER_TOKEN;
  const channelId = '1048242197029458040';
  if (!token) throw new Error('DISCORD_USER_TOKEN not set');

  // KV-first read (source of truth) — the old GitHub read+PUT path was reverted by
  // the next KV→GitHub mirror, causing perpetual backfill/null churn (audit 2026-06-22).
  const content = await getHistory(env);

  const etNow = toET(new Date());
  const todayISO = `${etNow.getFullYear()}-${String(etNow.getMonth()+1).padStart(2,'0')}-${String(etNow.getDate()).padStart(2,'0')}`;

  let missing;
  if (targetDates) {
    missing = content.filter(r => targetDates.includes(r.date));
  } else {
    missing = content.filter(r => r.date <= todayISO && r.m8bfPL == null && r.spxClose != null);
  }

  const filled = [], failed = [];

  for (const row of missing) {
    try {
      // Get day of week in ET
      const etDate = toET(new Date(row.date + 'T20:00:00Z'));
      const dow = etDate.getDay(); // 0=Sun,1=Mon...6=Sat

      // Calendar-only M8BF bans (earnings, EOM, OPEX-1, VIX-after-OPEX, CPI).
      // FIX (2026-06-09 audit P0 #3): now respects the 90%-WR override. On a
      // calendar-banned day where the prior trading day's m8bfWR ≥ 90 AND
      // it's not a CPI day, the override forces M8BF to fire (matches
      // signal-engine.js:502). Previously this branch wrote m8bfPL=0 even
      // when the live bot actually executed an M8BF trade — silent loss.
      const eomDay = isEomN(0, etDate);
      const eom1 = isEomN(1, etDate);
      const opex1 = opexSch.some(ds => isTodayBefore(ds, etDate));
      const vixExpAfterOpex = isVixAfterOpexDay(etDate);
      const nonAmznTslaEarn = isNonAmznTslaEarningsDay(etDate);
      const cpiDay = cpiSch.includes(todayLong(etDate));
      const calendarBlocked = eomDay || eom1 || opex1 || vixExpAfterOpex || nonAmznTslaEarn || cpiDay;
      if (calendarBlocked) {
        // Check 90% override: look for the most recent prior m8bfWR in the
        // content array we already loaded.
        const priorWREntry = content
          .filter(r => r.date < row.date && r.m8bfWR != null)
          .sort((a, b) => b.date.localeCompare(a.date))[0];
        const priorWR = priorWREntry ? parseFloat(priorWREntry.m8bfWR) : null;
        const ninetyOverride = (priorWR != null && priorWR >= 90 && !cpiDay);
        // GXBF check: if GXBF fired today (gxbfPL non-null and non-zero),
        // the 90% override doesn't apply per signal-engine.js:502.
        const gxbfFired = (row.gxbfPL != null && row.gxbfPL !== 0);
        if (!ninetyOverride || gxbfFired) {
          row.m8bfPL = 0;
          filled.push({ date: row.date, pl: 0, blocked: { eom: eomDay, 'eom-1': eom1, 'opex-1': opex1, vixAfterOpex: vixExpAfterOpex, earn: nonAmznTslaEarn, cpi: cpiDay }, ninetyOverride: false });
          continue;
        }
        // Otherwise fall through — calendar block overridden by 90% rule.
        console.log(`[backfill] 90% override at ${row.date}: priorWR=${priorWR}, allowing M8BF P/L computation`);
      }

      const win = getM8BFWindow(dow, row.date);
      if (!win) {
        failed.push({ date: row.date, reason: 'no window for dow=' + dow });
        continue;
      }
      const [winLo, winHi] = win;

      // Fetch all signals for this date in chronological order
      const signals = await fetchAllDiscordSignalsForDate(token, channelId, row.date);

      // First qualifying signal: in window + not banned
      let qualifying = null;
      for (const sig of signals) {
        if (!sig.time) continue;
        const [h, m] = sig.time.split(':').map(Number);
        const mins = h * 60 + m;
        if (mins >= winLo && mins < winHi && !isBanned(sig.center, sig.lower, sig.t1)) {
          qualifying = sig;
          break;
        }
      }

      if (!qualifying) {
        failed.push({ date: row.date, reason: 'no qualifying signal in window' });
        continue;
      }

      // PL = round((min(intrinsic, wing) - premium) * 100)
      const lo = qualifying.lower;
      const hi = qualifying.upper;
      const wing = (hi - lo) / 2;
      const intrinsic = Math.max(0, Math.min(row.spxClose - lo, hi - row.spxClose));
      const clipped = Math.min(intrinsic, wing);
      const pl = Math.round((clipped - qualifying.premium) * 100);

      row.m8bfPL = pl;
      filled.push({ date: row.date, pl, center: qualifying.center, lower: lo, upper: hi, premium: qualifying.premium, spxClose: row.spxClose });
    } catch (e) {
      failed.push({ date: row.date, error: e.message });
    }
  }

  if (filled.length > 0) {
    // KV-first (source of truth), then mirror ONCE to GitHub.
    await setHistory(env, content, { dateStr: 'backfill-pl' });
    await mirrorHistoryToGitHub(env, content, `auto: backfill m8bfPL for ${filled.map(f => f.date).join(', ')}`);
  }

  return { filled, failed, total_missing: missing.length };
}

// ════════════════════════════════════════════════════════════════════
// APPEND DAILY SIGNALS TO TRADES DATABASE (backtester.html)
// ════════════════════════════════════════════════════════════════════

async function appendTradesToBacktester(env, todayISO, etNow, signals, spxClose, addToSkip = false) {
  if (!signals || signals.length === 0) return { appended: 0, reason: 'no signals' };
  if (spxClose == null) return { appended: 0, reason: 'no spxClose' };

  const token = env.GITHUB_TOKEN;
  if (!token) return { appended: 0, reason: 'no GITHUB_TOKEN' };

  const owner = 'rava8989';
  const repo = 'brave';
  const path = 'backtester.html';
  const ghHeaders = {
    'Authorization': `Bearer ${token}`,
    'Accept': 'application/vnd.github+json',
    'User-Agent': 'schwab-proxy-worker/1.0',
    'X-GitHub-Api-Version': '2022-11-28',
  };

  // 1. Fetch raw file content
  const rawResp = await fetch(`https://raw.githubusercontent.com/${owner}/${repo}/main/${path}?t=${Date.now()}`, {
    headers: { 'Cache-Control': 'no-cache' },
  });
  if (!rawResp.ok) throw new Error(`Raw fetch failed: ${rawResp.status}`);
  let html = await rawResp.text();

  // 2. Dedup check — skip if today's date already in TRADES
  if (html.includes(`["${todayISO}",`)) {
    return { appended: 0, reason: `already exists: ${todayISO}` };
  }

  // 3. Build new TRADES rows — 9-field schema must match backtester.html and rebake_trades.py
  // [D, DAY, TIME, PREM, SPR, PROF, MAXP, CTR, BANNED]
  // SPR is half-wing width (center→wing), not full span.
  // BANNED encodes both full bans AND combo bans (computed from real T1).
  // DAY: 0=Mon..4=Fri  (JS getDay: 0=Sun..6=Sat)
  const dayIdx = etNow.getDay() - 1;

  // Bucket signals into 5-min buckets, keeping the FIRST signal per bucket
  // (matches rebake_trades.py behavior so daily append matches a fresh rebake).
  const buckets = new Map();
  for (const sig of signals) {
    const t = sig.time || '10:00';
    const [hh, mm] = t.split(':').map(Number);
    const bucket = `${String(hh).padStart(2,'0')}:${String(Math.floor(mm/5)*5).padStart(2,'0')}`;
    if (!buckets.has(bucket)) buckets.set(bucket, { ...sig, bucket });
  }

  const newRows = [];
  for (const bucket of [...buckets.keys()].sort()) {
    const sig = buckets.get(bucket);
    const spr = Math.floor((sig.upper - sig.lower) / 2);
    if (spr <= 0) continue;
    const maxp = Math.round((spr - sig.premium) * 100);
    if (maxp <= 0) continue;
    const intrinsic = Math.max(0, Math.min(spxClose - sig.lower, sig.upper - spxClose));
    const prof = Math.round((intrinsic - sig.premium) * 100);
    const banned = isBanned(sig.center, sig.lower, sig.t1);
    newRows.push([todayISO, dayIdx, bucket, sig.premium, spr, prof, maxp, sig.center, banned]);
  }

  if (newRows.length === 0) return { appended: 0, reason: 'all rows filtered out' };

  // 4. Find injection point — right before ];\nconst META
  const injectionMarker = '];\nconst META';
  const injIdx = html.indexOf(injectionMarker);
  if (injIdx === -1) throw new Error('Cannot find TRADES injection point in backtester.html');

  const rowsStr = newRows.map(r => JSON.stringify(r)).join(',');
  html = html.slice(0, injIdx) + ',' + rowsStr + html.slice(injIdx);

  // 5. Update META: maxDate and count
  html = html.replace(/("maxDate"\s*:\s*)"[^"]*"/, `$1"${todayISO}"`);
  const countMatch = html.match(/"count"\s*:\s*(\d+)/);
  if (countMatch) {
    const newCount = parseInt(countMatch[1]) + newRows.length;
    html = html.replace(/"count"\s*:\s*\d+/, `"count": ${newCount}`);
  }

  // 5b. If today is a SKIP date (live system blocked M8BF), inject into M8BF_SKIP set
  if (addToSkip && !html.includes(`"${todayISO}"`)) {
    const skipMatch = html.match(/(const M8BF_SKIP = new Set\(\[)([\s\S]*?)(\]\);)/);
    if (skipMatch) {
      const inner = skipMatch[2].trimEnd();
      const sep = inner.endsWith(',') ? '' : ',';
      const injected = `${skipMatch[1]}${inner}${sep}\n  "${todayISO}"\n${skipMatch[3]}`;
      html = html.replace(skipMatch[0], injected);
    }
  }

  // 6. Push via Git Data API (handles files >1MB — GitHub Contents API has 1MB limit)

  // 6a. Create blob
  const blobResp = await fetch(`https://api.github.com/repos/${owner}/${repo}/git/blobs`, {
    method: 'POST',
    headers: { ...ghHeaders, 'Content-Type': 'application/json' },
    body: JSON.stringify({ content: html, encoding: 'utf-8' }),
  });
  if (!blobResp.ok) {
    const err = await blobResp.text();
    throw new Error(`Blob create failed: ${blobResp.status} — ${err.slice(0, 200)}`);
  }
  const { sha: blobSha } = await blobResp.json();

  // 6b–6f. Get HEAD, create tree+commit, update ref (retry on 422 race condition)
  for (let attempt = 1; attempt <= 3; attempt++) {
    // 6b. Get current HEAD commit SHA
    const refResp = await fetch(`https://api.github.com/repos/${owner}/${repo}/git/ref/heads/main`, { headers: ghHeaders });
    if (!refResp.ok) throw new Error(`Ref fetch failed: ${refResp.status}`);
    const { object: { sha: headSha } } = await refResp.json();

    // 6c. Get tree SHA from HEAD commit
    const commitResp = await fetch(`https://api.github.com/repos/${owner}/${repo}/git/commits/${headSha}`, { headers: ghHeaders });
    if (!commitResp.ok) throw new Error(`Commit fetch failed: ${commitResp.status}`);
    const { tree: { sha: treeSha } } = await commitResp.json();

    // 6d. Create new tree
    const treeResp = await fetch(`https://api.github.com/repos/${owner}/${repo}/git/trees`, {
      method: 'POST',
      headers: { ...ghHeaders, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        base_tree: treeSha,
        tree: [{ path, mode: '100644', type: 'blob', sha: blobSha }],
      }),
    });
    if (!treeResp.ok) {
      const err = await treeResp.text();
      throw new Error(`Tree create failed: ${treeResp.status} — ${err.slice(0, 200)}`);
    }
    const { sha: newTreeSha } = await treeResp.json();

    // 6e. Create new commit
    const newCommitResp = await fetch(`https://api.github.com/repos/${owner}/${repo}/git/commits`, {
      method: 'POST',
      headers: { ...ghHeaders, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        message: `auto: append ${newRows.length} trades for ${todayISO}`,
        tree: newTreeSha,
        parents: [headSha],
      }),
    });
    if (!newCommitResp.ok) {
      const err = await newCommitResp.text();
      throw new Error(`Commit create failed: ${newCommitResp.status} — ${err.slice(0, 200)}`);
    }
    const { sha: newCommitSha } = await newCommitResp.json();

    // 6f. Update ref to point at new commit
    const updateRefResp = await fetch(`https://api.github.com/repos/${owner}/${repo}/git/refs/heads/main`, {
      method: 'PATCH',
      headers: { ...ghHeaders, 'Content-Type': 'application/json' },
      body: JSON.stringify({ sha: newCommitSha }),
    });
    if (updateRefResp.ok) break; // success
    if (updateRefResp.status === 422 && attempt < 3) {
      console.warn(`[proxy] Ref update 422 race condition, retry ${attempt}/3`);
      continue;
    }
    const err = await updateRefResp.text();
    throw new Error(`Ref update failed: ${updateRefResp.status} — ${err.slice(0, 200)}`);
  }

  return { appended: newRows.length, date: todayISO, signals: signals.length };
}

// ════════════════════════════════════════════════════════════════════
// WORKER EXPORT
// ════════════════════════════════════════════════════════════════════

// ════════════════════════════════════════════════════════════════════
// MORNING CARD IMAGE — forecast-only card rendered SVG → PNG (resvg-wasm).
// wasm is bundled; fonts are fetched from CDN once and cached per isolate.
// Any failure here is caught by the caller, which falls back to text.
// ════════════════════════════════════════════════════════════════════
let _resvgReady = null;
async function ensureResvg() {
  if (!_resvgReady) _resvgReady = initWasm(resvgWasm);
  await _resvgReady;
}
let _cardFonts = null;
async function getCardFonts() {
  if (_cardFonts) return _cardFonts;
  const SETS = [
    ['https://cdn.jsdelivr.net/npm/@expo-google-fonts/inter/Inter_400Regular.ttf',
     'https://cdn.jsdelivr.net/npm/@expo-google-fonts/inter/Inter_600SemiBold.ttf'],
    ['https://cdn.jsdelivr.net/npm/dejavu-fonts-ttf@2.37.3/ttf/DejaVuSans.ttf',
     'https://cdn.jsdelivr.net/npm/dejavu-fonts-ttf@2.37.3/ttf/DejaVuSans-Bold.ttf'],
  ];
  let lastErr = null;
  for (const set of SETS) {
    try {
      const bufs = await Promise.all(set.map(async u => {
        const r = await fetch(u, { cf: { cacheTtl: 604800, cacheEverything: true } });
        if (!r.ok) throw new Error(`font ${r.status} ${u}`);
        return new Uint8Array(await r.arrayBuffer());
      }));
      _cardFonts = bufs;
      return _cardFonts;
    } catch (e) { lastErr = e; }
  }
  throw lastErr || new Error('no fonts');
}
function _cardEsc(s) {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}
// data: { title, date, vix, vixSub, rows:[{n,det,yes}], tiles:[[label,val,color]], stats:[[label,val]] }
function buildMorningCardSvg(d) {
  const W = 460, P = 18;
  const C = { card: '#1e1f22', yesBg: '#2a2c30', noBg: '#26282c', text: '#f2f3f5',
    sub: '#b5bac1', mute: '#80848e', nameNo: '#dadde1', green: '#4ade80', red: '#f87171' };
  const F = 'Inter, DejaVu Sans, sans-serif';
  let s = '';
  s += `<text x="${P}" y="36" font-family="${F}" font-size="17" font-weight="600" fill="${C.text}">${_cardEsc(d.title)}</text>`;
  s += `<text x="${P}" y="53" font-family="${F}" font-size="12" fill="${C.mute}">${_cardEsc(d.date)}</text>`;
  s += `<text x="${W - P}" y="36" text-anchor="end" font-family="${F}" font-size="21" font-weight="600" fill="${C.text}">${_cardEsc(d.vix)}</text>`;
  s += `<text x="${W - P}" y="53" text-anchor="end" font-family="${F}" font-size="11" fill="${d.vixSubUp ? C.red : C.mute}">${_cardEsc(d.vixSub)}</text>`;
  if (d.vixPrior) s += `<text x="${W - P}" y="65" text-anchor="end" font-family="${F}" font-size="9.5" fill="#6b7078">${_cardEsc(d.vixPrior)}</text>`;
  const innerW = W - 2 * P, y0 = 76, rowH = 38, step = 44;
  d.rows.forEach((r, i) => {
    const top = y0 + i * step, bg = r.yes ? C.yesBg : C.noBg, bar = r.yes ? C.green : C.red;
    const tag = r.yes ? 'YES' : 'NO', pillW = r.yes ? 42 : 34, pillX = P + innerW - 10 - pillW;
    s += `<rect x="${P}" y="${top}" width="${innerW}" height="${rowH}" rx="8" fill="${bg}"/>`;
    s += `<rect x="${P}" y="${top + 1}" width="4" height="${rowH - 2}" rx="2" fill="${bar}"/>`;
    s += `<clipPath id="rc${i}"><rect x="${P + 10}" y="${top}" width="${pillX - 8 - (P + 10)}" height="${rowH}"/></clipPath>`;
    s += `<text x="${P + 14}" y="${top + 24}" clip-path="url(#rc${i})" font-family="${F}" font-size="14"><tspan font-weight="600" fill="${r.yes ? C.text : C.nameNo}">${_cardEsc(r.n)}</tspan><tspan font-size="13" fill="${C.sub}">  ${_cardEsc(r.det)}</tspan></text>`;
    s += `<rect x="${pillX}" y="${top + 10}" width="${pillW}" height="18" rx="6" fill="${r.yes ? 'rgba(74,222,128,0.14)' : 'rgba(248,113,113,0.13)'}"/>`;
    s += `<text x="${pillX + pillW / 2}" y="${top + 23}" text-anchor="middle" font-family="${F}" font-size="11" font-weight="600" fill="${bar}">${tag}</text>`;
  });
  const tileY = y0 + d.rows.length * step + 4, tileH = 38, gap = 7, tileW = (innerW - (d.tiles.length - 1) * gap) / d.tiles.length;
  d.tiles.forEach((t, i) => {
    const x = P + i * (tileW + gap);
    s += `<rect x="${x}" y="${tileY}" width="${tileW}" height="${tileH}" rx="8" fill="${C.yesBg}"/>`;
    s += `<text x="${x + tileW / 2}" y="${tileY + 15}" text-anchor="middle" font-family="${F}" font-size="10" fill="${C.mute}">${_cardEsc(t[0])}</text>`;
    s += `<text x="${x + tileW / 2}" y="${tileY + 31}" text-anchor="middle" font-family="${F}" font-size="13" font-weight="600" fill="${t[2]}">${_cardEsc(t[1])}</text>`;
  });
  const statLabelY = tileY + tileH + 22;
  s += `<text x="${P}" y="${statLabelY}" font-family="${F}" font-size="11" letter-spacing="0.5" fill="${C.mute}">STATS</text>`;
  const sy0 = statLabelY + 18;
  d.stats.forEach((st, i) => {
    const y = sy0 + i * 20;
    s += `<text x="${P}" y="${y}" font-family="${F}" font-size="12"><tspan fill="${C.mute}">${_cardEsc(st[0])}</tspan><tspan fill="${C.sub}">   ${_cardEsc(st[1])}</tspan></text>`;
  });
  let H = sy0 + d.stats.length * 20 + 6;
  // Optional M8BF strikes block — INSIDE the card, small + muted, only when armed.
  if (d.m8bfStrikes) {
    const sepY = H + 2;
    s += `<line x1="${P}" y1="${sepY}" x2="${W - P}" y2="${sepY}" stroke="#34363c" stroke-width="1"/>`;
    const l1 = sepY + 19;
    s += `<text x="${P}" y="${l1}" font-family="${F}" font-size="11"><tspan fill="${C.mute}">M8BF skip-ends</tspan><tspan fill="${C.sub}">   ${_cardEsc(d.m8bfStrikes.skip)}</tspan></text>`;
    const l2 = l1 + 18;
    s += `<text x="${P}" y="${l2}" font-family="${F}" font-size="11"><tspan fill="${C.mute}">combo bans</tspan><tspan fill="${C.sub}">   ${_cardEsc(d.m8bfStrikes.combos)}</tspan></text>`;
    H = l2 + 8;
  }
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}"><rect x="0" y="0" width="${W}" height="${H}" rx="16" fill="${C.card}"/>${s}</svg>`;
}
async function renderMorningCardPng(d) {
  await ensureResvg();
  const fonts = await getCardFonts();
  const r = new Resvg(buildMorningCardSvg(d), {
    fitTo: { mode: 'width', value: 920 },
    font: { fontBuffers: fonts, defaultFontFamily: 'Inter', loadSystemFonts: false },
  });
  return r.render().asPng();
}
// Map the live `signal` (same object the text builder uses) → card data.
// Faithful: reuses the exact status strings + the active/blocked rule (a status
// starting with "No " = blocked = red/NO). M8BF is shown as CONDITIONAL
// ("watching … on flow signal") because it only fires if a flow signal lands
// in its window — it is NOT a scheduled trade.
function buildMorningCardData(signal, vixValues, tailLine) {
  const isNo = t => !t || /^No\s/i.test(String(t).trim());
  const strip = (t, name) => {
    let d = String(t || '').trim();
    d = d.replace(new RegExp('^(No\\s+)?' + name + '\\b\\s*', 'i'), '');
    d = d.replace(/^[—\-:│|@]\s*/, '').trim();
    const pm = d.match(/^\((.*)\)$/); if (pm) d = pm[1];
    return d.trim();
  };
  const rows = [];
  rows.push({ n: 'GXBF', det: strip(signal.gxbfText, 'GXBF') || '—', yes: !isNo(signal.gxbfText) });
  {
    const m8Active = !isNo(signal.m8bfText) && /^M8BF/i.test(String(signal.m8bfText || '').trim());
    let det;
    if (m8Active) {
      const win = (String(signal.m8bfText).match(/(\d{1,2}:\d{2}\s*[–-]\s*\d{1,2}:\d{2})/) || [])[1];
      det = win ? `watching ${win} · on flow signal` : 'watching · on flow signal';
    } else { det = strip(signal.m8bfText, 'M8BF') || '—'; }
    rows.push({ n: 'M8BF', det, yes: m8Active });
  }
  rows.push({ n: 'Straddle', det: strip(signal.stradText, 'Straddle') || '—', yes: !isNo(signal.stradText) });
  rows.push({ n: 'BOBF', det: strip(signal.bobfRec, 'BOBF') || '—', yes: !isNo(signal.bobfRec) });
  if (signal.diagText) rows.push({ n: 'Diagonal', det: strip(signal.diagText, 'Diagonal') || '—', yes: !isNo(signal.diagText) });
  if (tailLine) {
    const tl = String(tailLine);
    const tYes = /\bTRADE\b/.test(tl) && !/No trade|\bSKIP\b/i.test(tl);
    let tdet;
    if (tYes) {                                  // concise — the full line overflowed the row
      const dm = tl.match(/Δ\s*-?[\d.]+/);
      tdet = `9:45 · 0DTE put ${dm ? dm[0].replace(/\s+/g, '') : 'Δ-0.10'}`;
    } else if (/\bSKIP\b/i.test(tl)) {
      tdet = 'SKIP · VVIX ≥ 110';
    } else {
      tdet = strip(tl.replace(/^Tail\s*Hedge\s*[│|]?\s*/i, ''), 'Tail Hedge') || 'no trade';
    }
    rows.push({ n: 'Tail Hedge', det: tdet, yes: tYes });
  }
  const vix = (vixValues.todayOpen != null) ? String(vixValues.todayOpen) : '—';
  // Overnight VIX direction in plain words. oNight = priorClose − todayOpen:
  // positive = VIX FELL overnight ("down"); negative = VIX ROSE ("up").
  const on = (typeof signal.oNight === 'number' && isFinite(signal.oNight)) ? signal.oNight : null;
  let vixSub = 'VIX', vixSubUp = false;
  if (on != null) {
    const mag = Math.abs(on).toFixed(2);
    if (on > 0.005) vixSub = `VIX down ${mag}`;
    else if (on < -0.005) { vixSub = `VIX up ${mag}`; vixSubUp = true; }
    else vixSub = 'VIX flat';
  }
  // Audit line (small/subtle): prior-day VIX close + open so the overnight-gate
  // inputs are verifiable on the card itself (prior close − today open = drop above).
  const _yc = vixValues.yClose != null ? Number(vixValues.yClose).toFixed(2) : null;
  const _yo = vixValues.yOpen  != null ? Number(vixValues.yOpen).toFixed(2)  : null;
  const vixPrior = (_yc || _yo)
    ? `prev${_yc ? ' ' + _yc + ' cls' : ''}${_yc && _yo ? ' ·' : ''}${_yo ? ' ' + _yo + ' opn' : ''}`
    : null;
  const gapStr = (signal.spxGapPct != null) ? `${signal.spxGapPct > 0 ? '+' : ''}${signal.spxGapPct.toFixed(2)}%` : '—';
  const tiles = [
    ['SPX GAP', gapStr, (signal.spxGapPct != null && signal.spxGapPct < 0) ? '#f87171' : '#4ade80'],
  ];
  const statLine = (line) => {
    if (!line) return null;
    let s = String(line).replace(/\x1b\[[0-9;]*m/g, '').trim();
    let m = s.match(/^([^│|—]+?)\s*[│|—]\s*(.+)$/);
    if (m) return [m[1].trim(), m[2].trim()];
    m = s.match(/^(\S+(?:\s\S+)?)\s{2,}(.+)$/);
    if (m) return [m[1].trim(), m[2].trim()];
    return [s, ''];
  };
  const stats = [signal._cycleLine, signal._volFlowLine, signal._m8bfWrLine]
    .map(statLine).filter(Boolean).slice(0, 6);
  let m8bfStrikes = null;
  {
    const si = signal.m8bfStrikeInfo;
    const m8on = signal.m8bfText && /^M8BF/i.test(String(signal.m8bfText).trim());
    if (m8on && si && Array.isArray(si.blocked) && si.blocked.length) {
      m8bfStrikes = {
        skip: si.blocked.join(' · '),
        combos: Object.entries(si.comboBans || {}).map(([k, v]) => `${k}→${v}`).join(' · '),
      };
    }
  }
  return {
    title: 'Σ3 — Today’s Plan',
    date: `${signal.dateStr || ''}${signal.dayLabel ? ' · ' + signal.dayLabel : ''}`.trim(),
    vix, vixSub, vixSubUp, vixPrior, rows, tiles, stats, m8bfStrikes,
  };
}
// The M8BF skip-list / combo-bans → Discord small (-#) subtext, posted BELOW
// the image. Only when M8BF is actually armed today.
function buildM8bfSubtext(signal) {
  const si = signal.m8bfStrikeInfo;
  const active = signal.m8bfText && /^M8BF/i.test(String(signal.m8bfText).trim());
  if (!active || !si || !si.blocked) return null;
  const skip = (si.blocked || []).join(' · ');
  const combos = Object.entries(si.comboBans || {}).map(([k, v]) => `${k}→${v}`).join(' · ');
  let line = `-# M8BF · skip center-ends: ${skip}`;
  if (combos) line += `  |  combo bans: ${combos}`;
  return line.slice(0, 1900);
}
// Post a PNG as a Discord attachment (multipart). Image upload requires the
// bot token path; the legacy proxies can't pass files, so callers fall back to
// text when this returns !ok.
function _b64FromBytes(bytes) {
  let bin = ''; const chunk = 0x8000;
  for (let i = 0; i < bytes.length; i += chunk) bin += String.fromCharCode.apply(null, bytes.subarray(i, i + chunk));
  return btoa(bin);
}
async function sendDiscordImage(env, userId, pngBytes, proxyUrl = null, filename = 'morning.png', content = '') {
  // Path 1: direct bot token (multipart) if this worker has it.
  if (env.DISCORD_TOKEN) {
    try {
      const dmResp = await fetch('https://discord.com/api/v10/users/@me/channels', {
        method: 'POST', headers: { Authorization: `Bot ${env.DISCORD_TOKEN}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ recipient_id: userId }),
      });
      if (!dmResp.ok) return { ok: false, status: dmResp.status, error: `dm-chan ${dmResp.status}` };
      const dm = await dmResp.json();
      const fd = new FormData();
      fd.append('payload_json', JSON.stringify({ content, attachments: [{ id: 0, filename }] }));
      fd.append('files[0]', new Blob([pngBytes], { type: 'image/png' }), filename);
      const r = await fetch(`https://discord.com/api/v10/channels/${dm.id}/messages`, {
        method: 'POST', headers: { Authorization: `Bot ${env.DISCORD_TOKEN}` }, body: fd,
      });
      let data; try { data = await r.json(); } catch { data = {}; }
      return { ok: r.ok, status: r.status, data, source: 'image-direct', ...(r.ok ? {} : { error: `img ${r.status}` }) };
    } catch (e) { return { ok: false, error: 'image-direct: ' + e.message }; }
  }
  // Paths 2/3: base64 → discord-proxy (which holds the bot token) decodes + uploads.
  const payload = JSON.stringify({ userId, imageB64: _b64FromBytes(pngBytes), filename, content });
  const hdrs = { 'Content-Type': 'application/json' };
  if (env.PROXY_SECRET) hdrs['Authorization'] = `Bearer ${env.PROXY_SECRET}`;
  if (env.DISCORD_PROXY) {
    try {
      const r = await env.DISCORD_PROXY.fetch(new Request('https://dummy/', { method: 'POST', headers: hdrs, body: payload }));
      let data; try { data = await r.json(); } catch { data = {}; }
      const ok = r.ok && data.ok !== false;
      return { ok, status: r.status, data, source: 'image-binding', ...(ok ? {} : { error: `proxy-img ${r.status} ${data.error || ''}` }) };
    } catch (e) { return { ok: false, error: 'image-binding: ' + e.message }; }
  }
  if (proxyUrl && proxyUrl.startsWith('https://')) {
    try {
      const r = await fetch(proxyUrl, { method: 'POST', headers: hdrs, body: payload });
      let data; try { data = await r.json(); } catch { data = {}; }
      const ok = r.ok && data.ok !== false;
      return { ok, status: r.status, data, source: 'image-http', ...(ok ? {} : { error: `proxy-img ${r.status} ${data.error || ''}` }) };
    } catch (e) { return { ok: false, error: 'image-http: ' + e.message }; }
  }
  return { ok: false, error: 'no image transport (no DISCORD_TOKEN/DISCORD_PROXY/proxyUrl)' };
}
// M8BF conditional context notes — worker port of index.html's dashboard block
// (kept byte-identical so the card matches the dashboard). history rows:
// { date, m8bfWR, vixOpen, vixClose, spxOpen, spxClose }. etNow must be ET.
function computeM8bfContextNotes(history, etNow, todayVixOpen) {
  const rows = (history || [])
    .filter(r => r.m8bfWR != null)
    .sort((a, b) => a.date.localeCompare(b.date));
  if (rows.length < 2) return [];
  const notes = [];
  const last = rows[rows.length - 1];
  const daysDiff = (etNow.getTime() - new Date(last.date + 'T12:00:00').getTime()) / 86400000;
  function longToISO(s) { const d = new Date(s + ' 12:00:00'); if (isNaN(d)) return null; return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`; }
  function dateToISO(d) { return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`; }
  const twISO = new Set();
  for (let y = 2024; y <= 2027; y++) for (const m of [2, 5, 8, 11]) { let count = 0; for (let d = 1; d <= 31; d++) { const dt = new Date(y, m, d); if (dt.getMonth() !== m) break; if (dt.getDay() === 5 && ++count === 3) { twISO.add(dateToISO(dt)); break; } } }
  const todayLongStr = dateLong(etNow);
  const isOpex = () => opexSch.includes(todayLongStr);
  const isFed = () => fedSch.includes(todayLongStr);
  const isVixExp = () => vixSch.includes(todayLongStr);
  const isLastTradeMoToday = () => isLastTradeMo(etNow);
  const isTodayBeforeTW = () => twISO.has(dateToISO(nextTrade(etNow)));
  const isTodayAfterTW = () => twISO.has(dateToISO(prevTrade(etNow)));
  const isNMplus1 = () => isFirstTradeMo(prevTrade(etNow));
  const byDateISO = Object.fromEntries(rows.map(r => [r.date, r]));
  const isoDates = rows.map(r => r.date);
  const isoIdx = Object.fromEntries(isoDates.map((d, i) => [d, i]));
  const prevTradeISO = d => { const i = isoIdx[d]; return (i != null && i > 0) ? isoDates[i - 1] : null; };
  const nextTradeISO = d => { const i = isoIdx[d]; return (i != null && i < isoDates.length - 1) ? isoDates[i + 1] : null; };
  const isFirstTradeMoISO = d => { const p = prevTradeISO(d); return !p || p.slice(0, 7) !== d.slice(0, 7); };
  const isLastTradeMoISO = d => { const n = nextTradeISO(d); return !n || n.slice(0, 7) !== d.slice(0, 7); };
  const isNM1ISO = d => { const p = prevTradeISO(d); return p && isFirstTradeMoISO(p); };
  const opexISO = new Set(opexSch.map(longToISO).filter(Boolean));
  const fedISO = new Set(fedSch.map(longToISO).filter(Boolean));
  const vixExpISO = new Set(vixSch.map(longToISO).filter(Boolean));
  const earnISO = {};
  earningsSchedule.forEach(e => { const iso = longToISO(e.date); if (!iso) return; if (!earnISO[e.ticker]) earnISO[e.ticker] = new Set(); earnISO[e.ticker].add(iso); });
  const allWR = rows.map(r => parseFloat(r.m8bfWR)).filter(w => !isNaN(w));
  const baselineWR = allWR.length ? allWR.reduce((a, b) => a + b, 0) / allWR.length : 55;
  const NOTE_DELTA_MIN_PP = 10;
  function wrOn(predicate) {
    const wrs = [];
    for (const r of rows) { if (!predicate(r)) continue; const w = parseFloat(r.m8bfWR); if (!isNaN(w)) wrs.push(w); }
    if (wrs.length < 3) return null;
    const avg = wrs.reduce((a, b) => a + b, 0) / wrs.length;
    const delta = avg - baselineWR;
    if (Math.abs(delta) < NOTE_DELTA_MIN_PP) return null;
    return { wr: avg.toFixed(1), delta: (delta >= 0 ? '+' : '') + delta.toFixed(1), n: wrs.length };
  }
  const tag = s => s ? `${s.wr}% (${s.delta}pp vs avg, n=${s.n})` : null;
  const pOpex = r => opexISO.has(r.date);
  const pFed = r => fedISO.has(r.date);
  const pVixExp = r => vixExpISO.has(r.date);
  const pEom = r => isLastTradeMoISO(r.date);
  const pNM1 = r => isNM1ISO(r.date);
  const pEarn = t => r => earnISO[t] && earnISO[t].has(r.date);
  const pDayAfter = pred => r => { const p = prevTradeISO(r.date); return p && pred(byDateISO[p]); };
  const pDayBefore = pred => r => { const n = nextTradeISO(r.date); return n && pred(byDateISO[n]); };
  const todayVix = parseFloat(todayVixOpen);
  if (isOpex()) {
    if (!isNaN(todayVix) && todayVix < 18) { const s = wrOn(r => pOpex(r) && r.vixOpen != null && parseFloat(r.vixOpen) < 18); if (s) notes.push(`Today is OPEX with VIX below 18 — M8BF historically averages ${tag(s)}.`); }
    else { const s = wrOn(pOpex); if (s) notes.push(`Today is OPEX — M8BF historically averages ${tag(s)}.`); }
  }
  { const s = wrOn(pFed); if (isFed() && s) notes.push(`Today is a FED day — M8BF historically averages ${tag(s)}.`); }
  { const s = wrOn(pEom); if (isLastTradeMoToday() && s) notes.push(`Today is EOM (last trading day of month) — M8BF historically averages ${tag(s)}.`); }
  { const s = wrOn(pVixExp); if (isVixExp() && s) notes.push(`Today is VIX expiry — M8BF historically averages ${tag(s)}.`); }
  { const s = wrOn(pNM1); if (isNMplus1() && s) notes.push(`Today is the 2nd trading day of the month — M8BF historically averages ${tag(s)}.`); }
  earningsSchedule.filter(e => e.date === todayLongStr).forEach(e => { const s = wrOn(pEarn(e.ticker)); if (s) notes.push(`Today is ${e.company} earnings — M8BF historically averages ${tag(s)} on ${e.ticker} earnings days.`); });
  if (!isNaN(todayVix)) {
    if (todayVix >= 30) { const s = wrOn(r => r.vixOpen != null && parseFloat(r.vixOpen) >= 30); if (s) notes.push(`VIX opened at ${todayVix.toFixed(1)} today — M8BF averages ${tag(s)} when VIX opens ≥30.`); }
    else if (todayVix >= 25) { const s = wrOn(r => r.vixOpen != null && parseFloat(r.vixOpen) >= 25 && parseFloat(r.vixOpen) < 30); if (s) notes.push(`VIX opened at ${todayVix.toFixed(1)} today — M8BF averages ${tag(s)} when VIX opens 25-30.`); }
  }
  { const s = wrOn(pDayBefore(pFed)); if (fedSch.some(ds => isTodayBefore(ds, etNow)) && s) notes.push(`Tomorrow is a FED day — M8BF averages ${tag(s)} the day before Fed decisions.`); }
  { const s = wrOn(pDayBefore(pOpex)); if (opexSch.some(ds => isTodayBefore(ds, etNow)) && s) notes.push(`Tomorrow is OPEX — M8BF averages ${tag(s)} the day before OPEX.`); }
  { const s = wrOn(pDayBefore(r => twISO.has(r.date))); if (isTodayBeforeTW() && s) notes.push(`Tomorrow is Triple Witching — M8BF averages ${tag(s)} the day before TW.`); }
  earningsSchedule.filter(e => isTodayBefore(e.date, etNow)).forEach(e => { const s = wrOn(pDayBefore(pEarn(e.ticker))); if (s) notes.push(`Tomorrow is ${e.company} earnings — M8BF averages ${tag(s)} the day before ${e.ticker} earnings.`); });
  if (daysDiff <= 3) {
    { const s = wrOn(pDayAfter(r => twISO.has(r.date))); if (isTodayAfterTW() && s) notes.push(`Yesterday was Triple Witching — M8BF averages ${tag(s)} the day after TW.`); }
    { const s = wrOn(pDayAfter(pOpex)); if (opexSch.some(ds => isTodayAfter(ds, etNow)) && s) notes.push(`Yesterday was OPEX — M8BF averages ${tag(s)} the day after OPEX.`); }
    earningsSchedule.filter(e => longToISO(e.date) === last.date).forEach(e => { const s = wrOn(pDayAfter(pEarn(e.ticker))); if (s) notes.push(`Yesterday was ${e.company} earnings — M8BF averages ${tag(s)} the day after ${e.ticker} earnings.`); });
    if (parseFloat(last.m8bfWR) === 0) { const s = wrOn(pDayAfter(r => parseFloat(r.m8bfWR) === 0)); if (s) notes.push(`Last session (${last.date}) was 0% win rate — next-day average is ${tag(s)}.`); }
    if (last.vixClose) {
      const vc = parseFloat(last.vixClose);
      if (vc >= 30) { const s = wrOn(pDayAfter(r => r.vixClose != null && parseFloat(r.vixClose) >= 30)); if (s) notes.push(`VIX closed at ${vc.toFixed(1)} last session — next-day M8BF averages ${tag(s)} when prior VIX close ≥30.`); }
      else if (vc >= 25) { const s = wrOn(pDayAfter(r => r.vixClose != null && parseFloat(r.vixClose) >= 25 && parseFloat(r.vixClose) < 30)); if (s) notes.push(`VIX closed at ${vc.toFixed(1)} last session — next-day M8BF averages ${tag(s)} when prior VIX close 25-30.`); }
    }
    const vixIntra = r => { if (!r || r.vixOpen == null || r.vixClose == null) return null; const vo = parseFloat(r.vixOpen), vc = parseFloat(r.vixClose); return (vc - vo) / vo * 100; };
    if (last.vixOpen && last.vixClose) {
      const vi = vixIntra(last);
      if (vi >= 10) { const s = wrOn(pDayAfter(r => { const v = vixIntra(r); return v != null && v >= 10; })); if (s) notes.push(`VIX spiked ${vi.toFixed(1)}% intraday last session — next-day M8BF averages ${tag(s)} when VIX rises 10%+.`); }
      else if (vi >= 5) { const s = wrOn(pDayAfter(r => { const v = vixIntra(r); return v != null && v >= 5 && v < 10; })); if (s) notes.push(`VIX rose ${vi.toFixed(1)}% intraday last session — next-day M8BF averages ${tag(s)} when VIX rises 5-10%.`); }
      else if (vi <= -10) { const s = wrOn(pDayAfter(r => { const v = vixIntra(r); return v != null && v <= -10; })); if (s) notes.push(`VIX dropped ${Math.abs(vi).toFixed(1)}% intraday last session — next-day M8BF averages ${tag(s)} when VIX drops 10%+.`); }
    }
    const spxRet = r => { if (!r || r.spxOpen == null || r.spxClose == null) return null; const so = parseFloat(r.spxOpen), sc = parseFloat(r.spxClose); return (sc - so) / so * 100; };
    if (last.spxOpen && last.spxClose) {
      const sr = spxRet(last);
      if (sr <= -2) { const s = wrOn(pDayAfter(r => { const x = spxRet(r); return x != null && x <= -2; })); if (s) notes.push(`SPX fell ${Math.abs(sr).toFixed(1)}% last session — next-day M8BF averages ${tag(s)} after a 2%+ SPX down day.`); }
      else if (sr <= -1) { const s = wrOn(pDayAfter(r => { const x = spxRet(r); return x != null && x > -2 && x <= -1; })); if (s) notes.push(`SPX fell ${Math.abs(sr).toFixed(1)}% last session — next-day M8BF averages ${tag(s)} after a 1-2% SPX down day.`); }
      else if (sr >= 1 && sr < 2) { const s = wrOn(pDayAfter(r => { const x = spxRet(r); return x != null && x >= 1 && x < 2; })); if (s) notes.push(`SPX rose ${sr.toFixed(1)}% last session — next-day M8BF averages ${tag(s)} after a 1-2% SPX up day.`); }
    }
  }
  return notes;
}
const SAMPLE_MORNING_CARD = {
  title: 'Σ3 — Today’s Plan', date: 'Mon · Jun 22 2026 · OPEX+1', vix: '16.67', vixSub: 'VIX up 0.27', vixSubUp: true, vixPrior: 'prev 16.40 cls · 16.32 opn',
  rows: [
    { n: 'GXBF', det: 'fires 9:36 AM', yes: true }, { n: 'M8BF', det: 'window 11:00–11:30', yes: true },
    { n: 'Straddle', det: 'overnight VIX drop > 0.65', yes: false }, { n: 'BOBF', det: 'OPEX', yes: false },
    { n: 'Diagonal', det: 'COR1M 6.79 < 10', yes: false }, { n: 'Tail Hedge', det: '9:45 · 0DTE put Δ-0.10', yes: true },
  ],
  tiles: [['SPX GAP', '+0.91%', '#4ade80']],
  stats: [
    ['Day-type', 'NEUTRAL/BULL · Strad below norm ($695 vs $1091)'],
    ['Vol-flow', 'VOL_BID · M8BF $149 vs $434 · Strad $1902 vs $1091'],
    ['M8BF WR', '6% yday · soft ($310–350 vs $427)'],
  ],
  m8bfStrikes: { skip: '10 · 25 · 35 · 40 · 65 · 80', combos: '0→95 · 20→15 · 55→50 · 65→60 · 85→90' },
};

export default {
  async fetch(request, env, ctx) {
    const origin = request.headers.get('Origin') || '';
    if (!env.ALLOWED_ORIGIN) {
      console.error('ALLOWED_ORIGIN env var is not set — all cross-origin requests will be blocked');
    }
    const allowed = env.ALLOWED_ORIGIN || 'null';
    const corsOk = origin !== '' && (origin === allowed || origin.startsWith('http://localhost'));

    const corsHeaders = {
      'Access-Control-Allow-Origin': corsOk ? origin || '*' : '',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Sync-Secret',
      'Access-Control-Max-Age': '86400',
    };

    const url = new URL(request.url);

    // ── Rate limiting ──
    if (checkRateLimit(request)) {
      return jsonResp({ error: 'Rate limit exceeded' }, 429, corsHeaders);
    }

    // ── GET /status ── Secured debug endpoint
    if (url.pathname === '/status' && request.method === 'GET') {
      if (request.headers.get('X-Sync-Secret') !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, corsHeaders);
      }
      try {
        const lastRun = await env.SIGNAL_KV.get('last_run');
        return jsonResp(lastRun ? JSON.parse(lastRun) : { status: 'never_run' }, 200, {});
      } catch (e) {
        return jsonResp({ error: e.message }, 500, {});
      }
    }

    // ── GET /raw-discord?date=YYYY-MM-DD ── Show raw Discord messages for debugging
    if (url.pathname === '/test-card' && request.method === 'GET') {
      try {
        const png = await renderMorningCardPng(SAMPLE_MORNING_CARD);
        return new Response(png, { headers: { 'content-type': 'image/png', 'cache-control': 'no-store' } });
      } catch (e) {
        return new Response('card render failed: ' + (e && (e.stack || e.message) || e), { status: 500 });
      }
    }

    if (url.pathname === '/test-card-discord' && request.method === 'GET') {
      try {
        const dcRaw = await env.SIGNAL_KV.get('discord_config');
        if (!dcRaw) return new Response('no discord_config', { status: 500 });
        const dc = JSON.parse(dcRaw);
        const png = await renderMorningCardPng(SAMPLE_MORNING_CARD);
        const r = await sendDiscordImage(env, dc.channelId, png, dc.proxyUrl, 'morning.png', DISCORD_FOOTER);
        return new Response(JSON.stringify({ image: r.ok, status: r.status, error: r.error || null }), { headers: { 'content-type': 'application/json' } });
      } catch (e) {
        return new Response('test-card-discord failed: ' + (e && (e.stack || e.message) || e), { status: 500 });
      }
    }

    // POST /send-card — manual "Send Card" from the dashboard. Renders the card
    // from the CALCULATOR'S OWN signal (sent in the body), so it works any time
    // (weekend, or when the live data feed is down — exactly when you'd reach for
    // the manual fallback). No live re-pull. Image-first, text fallback. Server
    // enriches with advisory lines + tail (KV-backed, work offline). Gated to the
    // dashboard Origin. Body is parsed regardless of content-type (no CORS preflight).
    if (url.pathname === '/send-card' && request.method === 'POST') {
      const cors = { 'Access-Control-Allow-Origin': 'https://rava8989.github.io' };
      if ((request.headers.get('Origin') || '') !== 'https://rava8989.github.io') {
        return jsonResp({ ok: false, error: 'forbidden — dashboard only' }, 403, cors);
      }
      try {
        const body = await request.json();
        const signal = body && body.signal;
        const vixValues = (body && body.vixValues) || {};
        if (!signal) return jsonResp({ ok: false, error: 'no signal in body' }, 400, cors);
        const dcRaw = await env.SIGNAL_KV.get('discord_config');
        const dc = dcRaw ? JSON.parse(dcRaw) : null;
        if (!dc || !dc.channelId) return jsonResp({ ok: false, error: 'no discord_config in KV' }, 200, cors);
        const etNow = toET(new Date());
        // Enrich with server-side stats (KV-backed → available even with no live feed).
        try { signal._cycleLine   = await computeCycleLine(env, etNow); } catch (_) {}
        try { signal._volFlowLine = await computeVolFlowLine(env, etNow); } catch (_) {}
        try { signal._m8bfWrLine  = await computeM8bfWrLine(env, etNow); } catch (_) {}
        let tailLine = null; try { tailLine = await getTailHedgeStatusLine(env); } catch (_) {}
        let r = null;
        try {
          const png = await renderMorningCardPng(buildMorningCardData(signal, vixValues, tailLine));
          r = await sendDiscordImage(env, dc.channelId, png, dc.proxyUrl, 'morning.png', DISCORD_FOOTER);
        } catch (e) { r = { ok: false, error: 'render: ' + e.message }; }
        if (!r || !r.ok) {
          const txt = buildDiscordMessage(signal, vixValues, tailLine);
          const rt = await sendDiscordDM(env, dc.channelId, txt.slice(0, 2000), dc.proxyUrl);
          return jsonResp({ ok: !!(rt && rt.ok), kind: 'text-fallback', imgErr: r && r.error }, 200, cors);
        }
        return jsonResp({ ok: true, kind: 'image' }, 200, cors);
      } catch (e) { return jsonResp({ ok: false, error: e.message }, 200, cors); }
    }

    if (url.pathname === '/raw-discord' && request.method === 'GET') {
      if (request.headers.get('X-Sync-Secret') !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, {});
      }
      try {
        const dateISO = url.searchParams.get('date') || new Date().toISOString().slice(0,10);
        const [y, m, d] = dateISO.split('-').map(Number);
        const startMs = Date.UTC(y, m-1, d, 12, 0, 0);
        const endMs   = Date.UTC(y, m-1, d, 22, 0, 0);
        const discordEpoch = 1420070400000n;
        const afterSnowflake = ((BigInt(startMs) - discordEpoch) << 22n).toString();
        const beforeSnowflake = ((BigInt(endMs) - discordEpoch) << 22n).toString();
        const resp = await fetch(
          `https://discord.com/api/v9/channels/1048242197029458040/messages?limit=10&after=${afterSnowflake}&before=${beforeSnowflake}`,
          { headers: { 'Authorization': env.DISCORD_USER_TOKEN, 'User-Agent': 'Mozilla/5.0' } }
        );
        if (!resp.ok) throw new Error(`Discord API ${resp.status}`);
        const msgs = await resp.json();
        const sample = (Array.isArray(msgs) ? msgs : []).map(m => ({
          id: m.id, ts: m.timestamp, content: (m.content||'').slice(0,200),
          embeds: (m.embeds||[]).map(e => ({ title: e.title, desc: (e.description||'').slice(0,200) }))
        }));
        return jsonResp({ date: dateISO, count: Array.isArray(msgs) ? msgs.length : msgs, sample }, 200, {});
      } catch(e) {
        return jsonResp({ error: e.message }, 500, {});
      }
    }

    // ── GET /check-wr?from=YYYY-MM-DD&to=YYYY-MM-DD ── Compare stored vs recalculated m8bfWR
    if (url.pathname === '/check-wr' && request.method === 'GET') {
      if (request.headers.get('X-Sync-Secret') !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, {});
      }
      try {
        const from = url.searchParams.get('from');
        const to   = url.searchParams.get('to');
        if (!from || !to) return jsonResp({ error: 'missing from/to params' }, 400, {});

        const ghResp = await fetch('https://api.github.com/repos/rava8989/brave/contents/history_data.json', {
          headers: { 'Authorization': `Bearer ${env.GITHUB_TOKEN}`, 'Accept': 'application/vnd.github+json', 'User-Agent': 'schwab-proxy-worker/1.0', 'X-GitHub-Api-Version': '2022-11-28' }
        });
        if (!ghResp.ok) throw new Error(`GitHub GET failed: ${ghResp.status}`);
        const meta = await ghResp.json();
        const content = JSON.parse(atob(meta.content.replace(/\n/g, '')));
        const rows = content.filter(r => r.m8bfWR != null && r.date >= from && r.date <= to);

        const results = [];
        for (const row of rows) {
          try {
            const spxClose = row.spxClose ?? await getSpxCloseForDate(row.date);
            const signals = await fetchAllDiscordSignalsForDate(env.DISCORD_USER_TOKEN, '1048242197029458040', row.date);
            const calc = signals.length > 0 ? computeWinRateFromSignals(signals, spxClose) : null;
            const diff = calc != null ? Math.abs(calc - row.m8bfWR) : null;
            results.push({ date: row.date, stored: row.m8bfWR, calc, signals: signals.length, diff, match: diff != null && diff <= 2 });
          } catch(e) {
            results.push({ date: row.date, stored: row.m8bfWR, calc: null, error: e.message });
          }
        }
        const matched = results.filter(r => r.match).length;
        const mismatched = results.filter(r => r.calc != null && !r.match);
        return jsonResp({ from, to, total: results.length, matched, mismatched_count: mismatched.length, mismatched, all: results }, 200, {});
      } catch(e) {
        return jsonResp({ error: e.message }, 500, {});
      }
    }

    // ── GET /backfill-wr ── Fill missing m8bfWR from Discord history + Stooq SPX
    // ?force=true recalculates last 60 days regardless of existing values
    if (url.pathname === '/backfill-wr' && request.method === 'GET') {
      if (request.headers.get('X-Sync-Secret') !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, {});
      }
      try {
        const force = url.searchParams.get('force') === 'true';
        const datesParam = url.searchParams.get('dates'); // e.g. 2026-03-24,2026-03-25
        const results = await backfillMissingWR(env, force, datesParam ? datesParam.split(',') : null);
        return jsonResp(results, 200, {});
      } catch (e) {
        return jsonResp({ error: e.message }, 500, {});
      }
    }

    // ── GET /backfill-pl ── Fill missing m8bfPL from Discord history
    if (url.pathname === '/backfill-pl' && request.method === 'GET') {
      if (request.headers.get('X-Sync-Secret') !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, {});
      }
      try {
        const datesParam = url.searchParams.get('dates');
        const results = await backfillMissingPL(env, datesParam ? datesParam.split(',') : null);
        return jsonResp(results, 200, {});
      } catch (e) {
        return jsonResp({ error: e.message }, 500, {});
      }
    }

    // ── GET /rescrape?date=YYYY-MM-DD ── Re-scrape all Discord signals for a date into KV
    if (url.pathname === '/rescrape' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || secret !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      try {
        const dateISO = url.searchParams.get('date') || (() => { const et = toET(); return `${et.getFullYear()}-${String(et.getMonth()+1).padStart(2,'0')}-${String(et.getDate()).padStart(2,'0')}`; })();
        const allSigs = await fetchAllDiscordSignalsForDate(env.DISCORD_USER_TOKEN, '1048242197029458040', dateISO);
        // Build signals array with banned flag
        const signals = allSigs.map(s => ({
          time: s.time,
          center: s.center,
          lower: s.lower,
          upper: s.upper,
          t1: s.t1,
          premium: s.premium,
          cp: s.cp ?? 0,
          banned: isBanned(s.center, s.lower, s.t1),
        }));
        await env.SIGNAL_KV.put('signals_today', JSON.stringify({ date: dateISO, signals }));
        return jsonResp({ date: dateISO, total: signals.length, banned: signals.filter(s => s.banned).length }, 200, { 'Access-Control-Allow-Origin': '*' });
      } catch (e) {
        return jsonResp({ error: e.message }, 500, { 'Access-Control-Allow-Origin': '*' });
      }
    }

    // ── GET /scrape-raw?from=YYYY-MM-DD&to=YYYY-MM-DD ── Fetch raw Discord signals for CSV
    if (url.pathname === '/scrape-raw' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || secret !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      try {
        const from = url.searchParams.get('from');
        const to = url.searchParams.get('to');
        if (!from || !to) return jsonResp({ error: 'missing from/to' }, 400, {});

        const token = env.DISCORD_USER_TOKEN;
        const channelId = '1048242197029458040';
        const results = [];

        // Iterate trading days
        const startD = new Date(from + 'T12:00:00Z');
        const endD = new Date(to + 'T12:00:00Z');
        for (let d = new Date(startD); d <= endD; d.setUTCDate(d.getUTCDate() + 1)) {
          const dow = d.getUTCDay();
          if (dow === 0 || dow === 6) continue;
          const dateISO = d.toISOString().slice(0, 10);

          // Fetch raw messages for this date
          const [y, m, dd] = dateISO.split('-').map(Number);
          const startMs = Date.UTC(y, m - 1, dd, 12, 0, 0);
          const endMs = Date.UTC(y, m - 1, dd, 22, 0, 0);
          const discordEpoch = 1420070400000n;
          let afterSnowflake = ((BigInt(startMs) - discordEpoch) << 22n).toString();
          const beforeSnowflake = ((BigInt(endMs) - discordEpoch) << 22n).toString();

          for (let page = 0; page < 10; page++) {
            const resp = await fetch(
              `https://discord.com/api/v9/channels/${channelId}/messages?limit=100&after=${afterSnowflake}&before=${beforeSnowflake}`,
              { headers: { 'Authorization': token, 'User-Agent': 'Mozilla/5.0' } }
            );
            if (!resp.ok) break;
            const batch = await resp.json();
            if (!Array.isArray(batch) || batch.length === 0) break;
            batch.sort((a, b) => a.id.localeCompare(b.id));

            for (const msg of batch) {
              const content = msg.content || '';
              const msgET = toET(new Date(msg.timestamp));
              const msgDate = `${msgET.getFullYear()}-${String(msgET.getMonth()+1).padStart(2,'0')}-${String(msgET.getDate()).padStart(2,'0')}`;
              if (msgDate !== dateISO) continue;
              const msgTime = `${String(msgET.getHours()).padStart(2,'0')}:${String(msgET.getMinutes()).padStart(2,'0')}`;

              // Parse full signal fields
              const priceM = content.match(/Price:\s*([\d.]+)/i);
              const trendM = content.match(/Trend:\s*(\w+)/i);
              const predM = content.match(/Predicted Close:\s*([\d.]+)/i);
              const strM = content.match(/Strength:\s*([\d.]+)/i);
              const stM = content.match(/Short term:\s*([\d.]+)/i);
              const ltM = content.match(/Long term:\s*([\d.]+)/i);
              const sbM = content.match(/Short term bias:\s*(\w+)/i);
              const lbM = content.match(/Long term bias:\s*(\w+)/i);
              const callsM = content.match(/Calls:\s*([\d.]+)/i);
              const putsM = content.match(/Puts:\s*([\d.]+)/i);
              const centerM = content.match(/Center:\s*([\d.]+)/i);
              const rangeM = content.match(/Range:\s*([\d.]+)/i);
              const t1M = content.match(/Target 1:\s*([\d.]+)/i);
              const t2M = content.match(/Target 2:\s*([\d.]+)/i);
              const deltaM = content.match(/Delta:\s*([\d.]+)/i);
              const gammaM = content.match(/Gamma:\s*([\d.]+)/i);
              const interestM = content.match(/Interest:\s*([\d.]+)/i);
              const sonarM = content.match(/Sonar:\s*([\d.]+)/i);
              const volumeM = content.match(/Volume:\s*([\d.]+)/i);

              // Butterfly
              const bfM = content.match(/(BUY \+1 Butterfly SPX[^@]*@([\d.]+)\s*LMT)/i);
              const bfStrikesM = content.match(/Butterfly[^/]*(\d{4,5})\/(\d{4,5})\/(\d{4,5})\s*(CALL|PUT)/i);

              // IC
              const icM = content.match(/(SELL -1 Iron Condor SPX[^@]*@([\d.]+)\s*LMT)/i);
              const icStrikesM = content.match(/Iron Condor[^/]*(\d{4,5})\/(\d{4,5})\/(\d{4,5})\/(\d{4,5})/i);

              // Sonar IC
              const sonarICM = content.match(/Sonar:[^S]*SELL -1 Iron Condor SPX[^@]*@([\d.]+)/i);
              const sonarStrikesM = content.match(/Sonar:[^S]*Iron Condor[^/]*(\d{4,5})\/(\d{4,5})\/(\d{4,5})\/(\d{4,5})/i);

              // Vertical
              const vtM = content.match(/(SELL -1 Vertical SPX[^@]*@([\d.]+)\s*LMT)/i);
              const vtStrikesM = content.match(/Vertical[^/]*(\d{4,5})\/(\d{4,5})\s*(CALL|PUT)/i);

              if (!priceM) continue; // not a signal message

              results.push({
                datetime: msg.timestamp,
                date: msgDate,
                time: msgTime,
                price: priceM ? priceM[1] : '',
                trend: trendM ? trendM[1] : '',
                predicted_close: predM ? predM[1] : '',
                strength: strM ? strM[1] : '',
                short_term: stM ? stM[1] : '',
                long_term: ltM ? ltM[1] : '',
                short_bias: sbM ? sbM[1] : '',
                long_bias: lbM ? lbM[1] : '',
                calls: callsM ? callsM[1] : '',
                puts: putsM ? putsM[1] : '',
                center: centerM ? centerM[1] : '',
                range: rangeM ? rangeM[1] : '',
                target1: t1M ? t1M[1] : '',
                target2: t2M ? t2M[1] : '',
                delta: deltaM ? deltaM[1] : '',
                gamma: gammaM ? gammaM[1] : '',
                interest: interestM ? interestM[1] : '',
                sonar: sonarM ? sonarM[1] : '',
                volume: volumeM ? volumeM[1] : '',
                bf_action: bfM ? 'BUY' : '',
                bf_strikes: bfStrikesM ? `${bfStrikesM[1]}/${bfStrikesM[2]}/${bfStrikesM[3]}` : '',
                bf_center: bfStrikesM ? bfStrikesM[2] : '',
                bf_upper: bfStrikesM ? bfStrikesM[1] : '',
                bf_lower: bfStrikesM ? bfStrikesM[3] : '',
                bf_price: bfM ? bfM[2] : '',
                ic_action: icM ? 'SELL' : '',
                ic_strikes: icStrikesM ? `${icStrikesM[1]}/${icStrikesM[2]}/${icStrikesM[3]}/${icStrikesM[4]}` : '',
                ic_price: icM ? icM[2] : '',
                sonar_action: sonarICM ? 'SELL' : '',
                sonar_strikes: sonarStrikesM ? `${sonarStrikesM[1]}/${sonarStrikesM[2]}/${sonarStrikesM[3]}/${sonarStrikesM[4]}` : '',
                sonar_price: sonarICM ? sonarICM[1] : '',
                vt_action: vtM ? 'SELL' : '',
                vt_strikes: vtStrikesM ? `${vtStrikesM[1]}/${vtStrikesM[2]}` : '',
                vt_side: vtStrikesM ? vtStrikesM[3] : '',
                vt_price: vtM ? vtM[2] : '',
              });
            }
            if (batch.length < 100) break;
            afterSnowflake = batch[batch.length - 1].id;
          }
          // Small delay between days to avoid rate limits
          await new Promise(r => setTimeout(r, 500));
        }
        return jsonResp({ total: results.length, signals: results }, 200, { 'Access-Control-Allow-Origin': '*' });
      } catch (e) {
        return jsonResp({ error: e.message }, 500, { 'Access-Control-Allow-Origin': '*' });
      }
    }

    // ── GET /vix-for-date ── No CORS restriction (script access), secured by secret
    if (url.pathname === '/vix-for-date' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret');
      if (!secret || secret !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, {});
      }
      try {
        const dateStr = url.searchParams.get('date');
        if (!dateStr || !/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
          return jsonResp({ error: 'Invalid date param, expected YYYY-MM-DD' }, 400, {});
        }
        const token = await getAccessToken(env);
        const [y, m, d] = dateStr.split('-').map(Number);
        // Use noon–22:00 UTC to stay within the ET trading day (avoids prev-day bleed)
        // 12:00 UTC = ~7-8 AM ET (before open), 22:00 UTC = ~5-6 PM ET (after close)
        const dayStart = Date.UTC(y, m - 1, d, 12, 0, 0);
        const dayEnd   = Date.UTC(y, m - 1, d, 22, 0, 0);
        const vixUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24VIX&periodType=day&period=1&frequencyType=minute&frequency=1&startDate=${dayStart}&endDate=${dayEnd}&needExtendedHoursData=true`;
        const spxUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24SPX&periodType=day&period=1&frequencyType=minute&frequency=1&startDate=${dayStart}&endDate=${dayEnd}&needExtendedHoursData=true`;
        const [vixData, spxData] = await Promise.all([fetchSchwabJSON(vixUrl, token), fetchSchwabJSON(spxUrl, token)]);
        function extractOHLC(candles) {
          if (!candles || !candles.length) return { open: null, close: null };
          candles.sort((a, b) => a.datetime - b.datetime);
          const openCandle  = candles.find(c => { const et = toET(new Date(c.datetime)); return et.getHours() * 60 + et.getMinutes() >= 570; });
          const closeCandle = candles.slice().reverse().find(c => { const et = toET(new Date(c.datetime)); return et.getHours() * 60 + et.getMinutes() <= 975; });
          return { open: openCandle ? parseFloat(openCandle.open.toFixed(2)) : null, close: closeCandle ? parseFloat(closeCandle.close.toFixed(2)) : null };
        }
        const vixOHLC = extractOHLC(vixData.candles);
        const spxOHLC = extractOHLC(spxData.candles);
        return jsonResp({ date: dateStr, vixOpen: vixOHLC.open, vixClose: vixOHLC.close, spxOpen: spxOHLC.open, spxClose: spxOHLC.close }, 200, {});
      } catch (e) {
        return jsonResp({ error: e.message }, 500, {});
      }
    }

    // ── GET /vix-bulk ── Fetch VIX+SPX open/close for a date range (requires sync secret)
    // Usage: /vix-bulk?start=2024-08-01&end=2024-09-01&secret=xxx
    if (url.pathname === '/vix-bulk' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || secret !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      try {
        const startDate = url.searchParams.get('start');
        const endDate = url.searchParams.get('end');
        if (!startDate || !endDate) return jsonResp({ error: 'Need start and end params (YYYY-MM-DD)' }, 400, { 'Access-Control-Allow-Origin': '*' });
        const token = await getAccessToken(env);
        const [sy, sm, sd] = startDate.split('-').map(Number);
        const [ey, em, ed] = endDate.split('-').map(Number);
        const startMs = Date.UTC(sy, sm - 1, sd, 0, 0, 0);
        const endMs = Date.UTC(ey, em - 1, ed, 23, 59, 59);
        const vixUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24VIX&periodType=day&period=1&frequencyType=minute&frequency=1&startDate=${startMs}&endDate=${endMs}&needExtendedHoursData=true`;
        const spxUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24SPX&periodType=day&period=1&frequencyType=minute&frequency=1&startDate=${startMs}&endDate=${endMs}&needExtendedHoursData=true`;
        const [vixData, spxData] = await Promise.all([fetchSchwabJSON(vixUrl, token), fetchSchwabJSON(spxUrl, token)]);

        // Group candles by date, extract 9:31 open and last candle <= 4:15 close
        function groupByDate(candles) {
          if (!candles || !candles.length) return {};
          const byDate = {};
          for (const c of candles) {
            const et = toET(new Date(c.datetime));
            const key = `${et.getFullYear()}-${String(et.getMonth()+1).padStart(2,'0')}-${String(et.getDate()).padStart(2,'0')}`;
            if (!byDate[key]) byDate[key] = [];
            byDate[key].push({ ...c, etMins: et.getHours() * 60 + et.getMinutes() });
          }
          const result = {};
          for (const [date, dayCandles] of Object.entries(byDate)) {
            dayCandles.sort((a, b) => a.datetime - b.datetime);
            // Open: first candle at or after 9:30 (570 mins)
            const openCandle = dayCandles.find(c => c.etMins >= 570);
            // Close: last candle at or before 4:15 (975 mins)
            const closeCandle = dayCandles.slice().reverse().find(c => c.etMins <= 975);
            result[date] = {
              open: openCandle ? parseFloat(openCandle.open.toFixed(2)) : null,
              close: closeCandle ? parseFloat(closeCandle.close.toFixed(2)) : null,
            };
          }
          return result;
        }
        const vixByDate = groupByDate(vixData.candles);
        const spxByDate = groupByDate(spxData.candles);
        const dates = [...new Set([...Object.keys(vixByDate), ...Object.keys(spxByDate)])].sort();
        const rows = dates.map(d => ({
          date: d,
          vixOpen: vixByDate[d]?.open ?? null,
          vixClose: vixByDate[d]?.close ?? null,
          spxOpen: spxByDate[d]?.open ?? null,
          spxClose: spxByDate[d]?.close ?? null,
        }));
        return jsonResp({ count: rows.length, rows }, 200, { 'Access-Control-Allow-Origin': '*' });
      } catch (e) {
        return jsonResp({ error: e.message }, 500, { 'Access-Control-Allow-Origin': '*' });
      }
    }

    // ── GET /vix-bulk-daily ── Fetch VIX+SPX daily open/close for a date range (years of history)
    // Usage: /vix-bulk-daily?start=2024-08-01&end=2026-04-01&secret=xxx
    if (url.pathname === '/vix-bulk-daily' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || secret !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      try {
        const startDate = url.searchParams.get('start');
        const endDate = url.searchParams.get('end');
        if (!startDate || !endDate) return jsonResp({ error: 'Need start and end params (YYYY-MM-DD)' }, 400, { 'Access-Control-Allow-Origin': '*' });
        const token = await getAccessToken(env);
        const [sy, sm, sd] = startDate.split('-').map(Number);
        const [ey, em, ed] = endDate.split('-').map(Number);
        const startMs = Date.UTC(sy, sm - 1, sd, 0, 0, 0);
        const endMs = Date.UTC(ey, em - 1, ed, 23, 59, 59);
        const vixUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24VIX&periodType=year&period=2&frequencyType=daily&frequency=1&startDate=${startMs}&endDate=${endMs}`;
        const spxUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24SPX&periodType=year&period=2&frequencyType=daily&frequency=1&startDate=${startMs}&endDate=${endMs}`;
        const [vixData, spxData] = await Promise.all([fetchSchwabJSON(vixUrl, token), fetchSchwabJSON(spxUrl, token)]);
        function toDateMap(candles) {
          if (!candles || !candles.length) return {};
          const m = {};
          for (const c of candles) {
            const et = toET(new Date(c.datetime));
            const key = `${et.getFullYear()}-${String(et.getMonth()+1).padStart(2,'0')}-${String(et.getDate()).padStart(2,'0')}`;
            m[key] = { open: parseFloat(c.open.toFixed(2)), close: parseFloat(c.close.toFixed(2)) };
          }
          return m;
        }
        const vixByDate = toDateMap(vixData.candles);
        const spxByDate = toDateMap(spxData.candles);
        const dates = [...new Set([...Object.keys(vixByDate), ...Object.keys(spxByDate)])].sort();
        const rows = dates.map(d => ({
          date: d,
          vixOpen: vixByDate[d]?.open ?? null,
          vixClose: vixByDate[d]?.close ?? null,
          spxOpen: spxByDate[d]?.open ?? null,
          spxClose: spxByDate[d]?.close ?? null,
        }));
        return jsonResp({ count: rows.length, rows }, 200, { 'Access-Control-Allow-Origin': '*' });
      } catch (e) {
        return jsonResp({ error: e.message }, 500, { 'Access-Control-Allow-Origin': '*' });
      }
    }

    // ── GET /kv-debug ── List KV keys (requires sync secret)
    if (url.pathname === '/kv-debug' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || secret !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      const list = await env.SIGNAL_KV.list();
      return jsonResp({ keys: list.keys.map(k => k.name) }, 200, { 'Access-Control-Allow-Origin': '*' });
    }

    // ── GET /trigger ── Manually trigger the scheduled handler (requires sync secret)
    if (url.pathname === '/trigger' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || secret !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      try {
        const result = await handleScheduled(env);
        result.date = result.date || new Date().toISOString();
        await env.SIGNAL_KV.put('last_run', JSON.stringify(result));
        return jsonResp(result, 200, { 'Access-Control-Allow-Origin': '*' });
      } catch (e) {
        return jsonResp({ error: e.message }, 500, { 'Access-Control-Allow-Origin': '*' });
      }
    }

    // ── POST /discord-send ── Browser/external endpoint that lets us retire
    // the standalone discord-proxy worker. Same body shape as the legacy
    // worker: { userId, message }. Optionally accepts Bearer PROXY_SECRET.
    if (url.pathname === '/link-notify' && request.method === 'POST') {
      // Worker-to-worker Discord note (skipper). LINK_SECRET-gated.
      if (!env.LINK_SECRET || request.headers.get('X-Link-Secret') !== env.LINK_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, corsHeaders);
      }
      try {
        const { text, fanoutText } = await request.json();
        const dcRaw = await env.SIGNAL_KV.get('discord_config');
        if (!dcRaw) return jsonResp({ ok: false, error: 'no discord_config' }, 200, corsHeaders);
        const dc = JSON.parse(dcRaw);
        // Channel post is OPTIONAL — skip it when no text (fanout-only mode), so
        // paper trades can relay to subscribers WITHOUT cluttering the channel.
        let r = { ok: true };
        if (text && String(text).trim()) {
          r = await sendDiscordDM(env, dc.channelId, String(text).slice(0, 1800), dc.proxyUrl);
        }
        // fanoutText (optional) = subscriber-facing trade message, relayed to the
        // signal_subscribers DM list. Set for EVERY skipper trade entry (paper or
        // live fill) so subscribers get each trade regardless of our exec mode.
        let fanned = 0;
        if (fanoutText) {
          try {
            const o = await fanoutSubscribers(env, String(fanoutText).slice(0, 1800));
            fanned = o.filter(x => x.ok).length;
          } catch (e) { console.warn('[link-notify/fanout]', e.message); }
        }
        return jsonResp({ ok: !!r.ok, fanned }, 200, corsHeaders);
      } catch (e) { return jsonResp({ ok: false, error: e.message }, 400, corsHeaders); }
    }

    if (url.pathname === '/discord-send' && request.method === 'POST') {
      const cors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
      };
      // Optional PROXY_SECRET check (mirrors discord-proxy's lenient behavior)
      if (env.PROXY_SECRET) {
        const auth = request.headers.get('Authorization') || '';
        // Allow browser CORS (no Authorization header from same-origin XHR);
        // for direct API calls, require the secret.
        const origin = request.headers.get('Origin') || '';
        const fromBrowser = origin.length > 0;
        const validAuth = auth === `Bearer ${env.PROXY_SECRET}`;
        if (!fromBrowser && !validAuth) {
          return jsonResp({ ok: false, error: 'Unauthorized' }, 401, cors);
        }
      }
      try {
        const body = await request.json();
        // Accept either body.embed (Option E rich card) or body.message (legacy text)
        const payload = body.embed || body.message;
        const result = await sendDiscordDM(env, body.userId, payload);
        return jsonResp(result, result.ok ? 200 : 500, cors);
      } catch (e) {
        return jsonResp({ ok: false, error: e.message }, 400, cors);
      }
    }

    // ── GET /history ── Public read of the KV-backed history store.
    //   GET /history                  → full array
    //   GET /history?date=YYYY-MM-DD  → single date row
    //   GET /history?since=YYYY-MM-DD → all rows >= date
    // Public CORS (no auth) — same audience as raw.githubusercontent.com was.
    if (url.pathname === '/history' && request.method === 'GET') {
      const cors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        // Short cache so writes propagate quickly (KV is fast anyway)
        'Cache-Control': 'public, max-age=15',
      };
      try {
        const data = await getHistory(env);
        const dateQ = url.searchParams.get('date');
        const sinceQ = url.searchParams.get('since');
        if (dateQ) {
          const row = (data || []).find(r => r.date === dateQ) || null;
          return jsonResp({ date: dateQ, row }, 200, cors);
        }
        if (sinceQ) {
          const rows = (data || []).filter(r => r.date >= sinceQ);
          return jsonResp({ since: sinceQ, count: rows.length, rows }, 200, cors);
        }
        return jsonResp(data || [], 200, cors);
      } catch (e) {
        return jsonResp({ error: e.message }, 500, cors);
      }
    }

    // ── POST /history-migrate ── Force-seed KV from GitHub raw.
    // One-time call after deploy (or after KV wipe). Returns row count + diff.
    if (url.pathname === '/history-migrate' && request.method === 'POST') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || secret !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      const cors = { 'Access-Control-Allow-Origin': '*' };
      try {
        const ghResp = await fetch(`${HISTORY_GH_RAW}?t=${Date.now()}`,
          { headers: { 'User-Agent': 'schwab-proxy', 'Cache-Control': 'no-cache' } });
        if (!ghResp.ok) return jsonResp({ error: `GitHub raw ${ghResp.status}` }, 500, cors);
        const ghData = await ghResp.json();
        if (!Array.isArray(ghData)) return jsonResp({ error: 'GitHub raw returned non-array' }, 500, cors);
        // Snapshot current KV state before overwriting
        let prevCount = 0;
        try {
          const prev = await env.SIGNAL_KV.get(HISTORY_KV_KEY);
          if (prev) {
            const p = JSON.parse(prev);
            prevCount = Array.isArray(p) ? p.length : 0;
            await backupHistorySnapshot(env, p, 'pre-migrate', { source: 'history-migrate' });
          }
        } catch (_) {}
        await env.SIGNAL_KV.put(HISTORY_KV_KEY, JSON.stringify(ghData));
        await logEvent(env, 'info', 'history-migrate', 'KV seeded from GitHub raw',
                       { prevCount, newCount: ghData.length });
        return jsonResp({
          migrated: true, prevCount, newCount: ghData.length,
          delta: ghData.length - prevCount,
        }, 200, cors);
      } catch (e) {
        return jsonResp({ error: e.message }, 500, cors);
      }
    }

    // ── GET /logs ── Read daily event log from KV. Auth required.
    //   GET /logs                            → today's events
    //   GET /logs?date=YYYY-MM-DD            → that day's events
    //   GET /logs?date=...&level=error|warn  → filter by level
    //   GET /logs?date=...&tag=morning       → filter by tag
    // GET /debug-morning-log?date=YYYY-MM-DD
    // Read-only public endpoint that returns ONLY morning-tagged log entries
    // for a single date. No auth — but it only exposes signal-related logs
    // (tag='morning'), not credentials/tokens/PII. Temporary diagnostic.
    if (url.pathname === '/debug-morning-log' && request.method === 'GET') {
      const cors = { 'Access-Control-Allow-Origin': '*' };
      try {
        const dateParam = url.searchParams.get('date') || isoDateET(toET(new Date()));
        if (!/^\d{4}-\d{2}-\d{2}$/.test(dateParam)) {
          return jsonResp({ error: 'invalid date format' }, 400, cors);
        }
        const raw = await env.SIGNAL_KV.get(`daily_log_${dateParam}`);
        let entries = raw ? JSON.parse(raw) : [];
        if (!Array.isArray(entries)) entries = [];
        // Filter to morning-tagged only — that's the only thing safe to expose
        const morningOnly = entries.filter(e => e.tag === 'morning');
        return jsonResp({ date: dateParam, count: morningOnly.length, entries: morningOnly }, 200, cors);
      } catch (e) {
        return jsonResp({ error: e.message }, 500, cors);
      }
    }

    if (url.pathname === '/logs' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || secret !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      const cors = { 'Access-Control-Allow-Origin': '*' };
      try {
        const etNowLg = toET(new Date());
        const dateParam = url.searchParams.get('date') || isoDateET(etNowLg);
        if (!/^\d{4}-\d{2}-\d{2}$/.test(dateParam)) {
          return jsonResp({ error: 'invalid date format (need YYYY-MM-DD)' }, 400, cors);
        }
        const levelFilter = url.searchParams.get('level');
        const tagFilter = url.searchParams.get('tag');
        const raw = await env.SIGNAL_KV.get(`daily_log_${dateParam}`);
        let entries = raw ? JSON.parse(raw) : [];
        if (!Array.isArray(entries)) entries = [];
        if (levelFilter) entries = entries.filter(e => e.level === levelFilter);
        if (tagFilter) entries = entries.filter(e => e.tag === tagFilter);
        return jsonResp({ date: dateParam, count: entries.length, entries }, 200, cors);
      } catch (e) {
        return jsonResp({ error: e.message }, 500, cors);
      }
    }

    // ── GET /history-backups ── List/inspect/restore pre-write history snapshots.
    //   GET  /history-backups                    → list last 10 backup metadata
    //   GET  /history-backups?key=<backupKey>    → return full JSON content of one backup
    //   POST /history-backups?restore=<key>      → push that backup back to GitHub (DESTRUCTIVE)
    // All operations require X-Sync-Secret. Restores REPLACE current history.
    if (url.pathname === '/history-backups') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || secret !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      const cors = { 'Access-Control-Allow-Origin': '*' };
      try {
        if (request.method === 'GET') {
          const key = url.searchParams.get('key');
          if (key) {
            if (!key.startsWith('history_backup_')) {
              return jsonResp({ error: 'invalid key prefix' }, 400, cors);
            }
            const raw = await env.SIGNAL_KV.get(key);
            if (!raw) return jsonResp({ error: 'backup not found (may have expired)' }, 404, cors);
            return jsonResp({ key, content: JSON.parse(raw) }, 200, cors);
          }
          const idxRaw = await env.SIGNAL_KV.get('history_backups_index');
          const idx = idxRaw ? JSON.parse(idxRaw) : [];
          return jsonResp({ backups: idx, count: idx.length }, 200, cors);
        }
        if (request.method === 'POST') {
          const restoreKey = url.searchParams.get('restore');
          if (!restoreKey) return jsonResp({ error: 'missing ?restore=<key>' }, 400, cors);
          if (!restoreKey.startsWith('history_backup_')) {
            return jsonResp({ error: 'invalid key prefix' }, 400, cors);
          }
          const backupRaw = await env.SIGNAL_KV.get(restoreKey);
          if (!backupRaw) return jsonResp({ error: 'backup not found' }, 404, cors);
          const restoredContent = JSON.parse(backupRaw);
          // Push directly to GitHub, bypassing the upsert merge logic
          const token = env.GITHUB_TOKEN;
          if (!token) return jsonResp({ error: 'GITHUB_TOKEN not set' }, 500, cors);
          const apiUrl = 'https://api.github.com/repos/rava8989/brave/contents/history_data.json';
          const ghHeaders = {
            'Authorization': `Bearer ${token}`,
            'Accept': 'application/vnd.github+json',
            'User-Agent': 'schwab-proxy-worker/1.0',
            'X-GitHub-Api-Version': '2022-11-28',
          };
          const getResp = await fetch(apiUrl, { headers: ghHeaders });
          if (!getResp.ok) return jsonResp({ error: `GitHub GET ${getResp.status}` }, 500, cors);
          const meta = await getResp.json();
          // Snapshot the CURRENT state before overwriting (safety net for the restore itself)
          const currentContent = JSON.parse(atob(meta.content.replace(/\n/g, '')));
          await backupHistorySnapshot(env, currentContent, 'pre-restore', { restoredFrom: restoreKey });
          const putResp = await fetch(apiUrl, {
            method: 'PUT',
            headers: { ...ghHeaders, 'Content-Type': 'application/json' },
            body: JSON.stringify({
              message: `restore: history rewound to backup ${restoreKey}`,
              content: btoa(JSON.stringify(restoredContent, null, 0)),
              sha: meta.sha,
            }),
          });
          if (!putResp.ok) {
            const err = await putResp.text();
            return jsonResp({ error: `GitHub PUT ${putResp.status}: ${err}` }, 500, cors);
          }
          return jsonResp({ restored: restoreKey, rows: restoredContent.length }, 200, cors);
        }
        return jsonResp({ error: 'method not allowed' }, 405, cors);
      } catch (e) {
        return jsonResp({ error: e.message }, 500, cors);
      }
    }

    // ── GET /schwab-health ── Public circuit-breaker endpoint
    // Returns current Schwab refresh-token health so the dashboard can
    // display a red "re-authenticate now" banner when KV says the token
    // is valid but Schwab has been rejecting it. Populated by
    // recordRefreshHealth() in every token-refresh path.
    if (url.pathname === '/schwab-health' && request.method === 'GET') {
      const publicCors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '86400',
      };
      try {
        const raw = await env.SIGNAL_KV.get('schwab_refresh_state');
        const state = raw ? JSON.parse(raw) : { ok: null, lastSuccess: null, msg: 'never recorded' };
        // Add a computed "stale" flag so the browser doesn't need to do clock math
        const now = Date.now();
        const lastSuccess = state.lastSuccess || 0;
        const lastError = state.lastError || 0;
        const minutesSinceSuccess = lastSuccess ? Math.round((now - lastSuccess) / 60000) : null;
        const minutesSinceError = lastError ? Math.round((now - lastError) / 60000) : null;
        // Show red banner when: explicit failure + 3+ consecutive errors + last success >30min ago
        const alarming = state.ok === false
          && (state.consecutiveErrors || 0) >= 3
          && (!minutesSinceSuccess || minutesSinceSuccess > 30);
        return jsonResp({
          ...state,
          now,
          minutesSinceSuccess,
          minutesSinceError,
          alarming,
        }, 200, publicCors);
      } catch (e) {
        return jsonResp({ error: e.message }, 500, publicCors);
      }
    }

    // ── GET /health ── Unified public health check for uptime monitoring.
    // Returns a structured view of every critical subsystem (morning signal,
    // EOD, Schwab refresh, cron liveness, GEX freshness). The `alarming`
    // boolean flips true whenever any subsystem is degraded, with specific
    // reasons in `alerts[]`. Designed to be polled every 5-10 min by an
    // external uptime monitor so silent failures surface before the user
    // notices Discord stayed quiet.
    // ── GET /diagonal-trigger ── Manually run handleDiagonalTrade NOW
    // Bypasses the normal 12:30 ET window so we can verify the open path
    // end-to-end without waiting for the cron. Auth-required.
    // ── GET /straddle-trigger ── Manually open a straddle NOW.
    // Useful when the 9:32 cron's openStraddleTrade threw and we need to
    // surface the actual error or do a late open. Auth-required.
    if (url.pathname === '/straddle-trigger' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || secret !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      try {
        const etNowDT = toET(new Date());
        const todayDT = isoDateET(etNowDT);
        const tokenDT = await getAccessToken(env);

        // Recompute signal so we know the badge (NM/EOM/plain) for max debit
        const _phEnd = Date.now();
        const _phStart = _phEnd - 4 * 24 * 60 * 60 * 1000;
        const [vH, sH] = await Promise.all([
          fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24VIX&periodType=day&period=3&frequencyType=minute&frequency=1&startDate=${_phStart}&endDate=${_phEnd}&needExtendedHoursData=true`, tokenDT, env),
          fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24SPX&periodType=day&period=3&frequencyType=minute&frequency=1&startDate=${_phStart}&endDate=${_phEnd}&needExtendedHoursData=true`, tokenDT, env),
        ]);
        const todayStr = etNowDT.toDateString();
        const findFirstAt930 = (cs) => (cs || []).slice().sort((a,b) => a.datetime - b.datetime).find(c => {
          const d = toET(new Date(c.datetime));
          return d.toDateString() === todayStr && (d.getHours() > 9 || (d.getHours() === 9 && d.getMinutes() >= 30));
        });
        const findYesterdayClose = (cs) => {
          const sorted = (cs || []).slice().sort((a,b) => a.datetime - b.datetime);
          const yesterday = sorted.filter(c => toET(new Date(c.datetime)).toDateString() !== todayStr);
          return yesterday[yesterday.length - 1];
        };
        const findYesterdayOpenAt930 = (cs) => {
          const sorted = (cs || []).slice().sort((a,b) => a.datetime - b.datetime);
          const yesterday = sorted.filter(c => toET(new Date(c.datetime)).toDateString() !== todayStr);
          if (!yesterday.length) return null;
          const yStr = toET(new Date(yesterday[yesterday.length - 1].datetime)).toDateString();
          return yesterday.find(c => {
            const d = toET(new Date(c.datetime));
            return d.toDateString() === yStr && (d.getHours() > 9 || (d.getHours() === 9 && d.getMinutes() >= 30));
          });
        };
        const vT = findFirstAt930(vH.candles)?.open;
        const vYC = findYesterdayClose(vH.candles)?.close;
        const vYO = findYesterdayOpenAt930(vH.candles)?.open;
        const sT = findFirstAt930(sH.candles)?.open;
        const sYC = findYesterdayClose(sH.candles)?.close;
        const gap = (sT != null && sYC != null) ? ((sT - sYC) / sYC) * 100 : 0;
        const sig = calculateSignal({
          vixToday: vT, vixYOpen: vYO, vixYClose: vYC,
          spxGapPct: gap, etDate: etNowDT,
        });
        if (sig.theme !== 'strad') {
          return jsonResp({ ok: false, reason: 'signal-not-strad', theme: sig.theme, rec: sig.rec }, 200, { 'Access-Control-Allow-Origin': '*' });
        }
        // Try to open. Surfaces the actual error from openStraddleTrade.
        const trade = await openStraddleTrade(env, tokenDT, etNowDT, sig);
        await env.SIGNAL_KV.put('straddle_open_trade', JSON.stringify(trade));
        // Clear the skip — we have a real trade now
        await env.SIGNAL_KV.delete(`straddle_skip_${todayDT}`);
        return jsonResp({ ok: true, trade, signal: sig }, 200, { 'Access-Control-Allow-Origin': '*' });
      } catch (e) {
        return jsonResp({ ok: false, error: e.message, stack: e.stack }, 500, { 'Access-Control-Allow-Origin': '*' });
      }
    }

    // ── GET /m8bf-backfill?date=YYYY-MM-DD ── Force-recompute m8bfPL + WR
    // for a specific historical date by re-scraping Discord. Bypasses the
    // "only if m8bfPL is null" guard in backfillMissingPL so we can fix
    // days where the cron stalled, set m8bfPL=0 by default, but real
    // Discord signals exist. Auth-required.
    if (url.pathname === '/m8bf-backfill' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || secret !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      const targetDate = url.searchParams.get('date');
      const verbose = url.searchParams.get('verbose') === '1';
      if (verbose && targetDate) {
        // Just dump raw scraped signals for the date — no writing
        try {
          const dcToken = env.DISCORD_USER_TOKEN;
          const channelId = '1048242197029458040';
          const sigs = await fetchAllDiscordSignalsForDate(dcToken, channelId, targetDate);
          const etD = toET(new Date(targetDate + 'T20:00:00Z'));
          const dow = etD.getDay();
          const win = getM8BFWindow(dow, targetDate);
          return jsonResp({
            date: targetDate,
            dow,
            windowMinutes: win,
            totalSignalsScraped: sigs.length,
            signals: sigs.slice(0, 100).map(s => ({
              time: s.time, center: s.center, t1: s.t1,
              lower: s.lower, upper: s.upper, premium: s.premium,
              banned: isBanned(s.center, s.lower, s.t1),
            })),
          }, 200, { 'Access-Control-Allow-Origin': '*' });
        } catch (e) {
          return jsonResp({ error: e.message }, 500, { 'Access-Control-Allow-Origin': '*' });
        }
      }
      if (!targetDate || !/^\d{4}-\d{2}-\d{2}$/.test(targetDate)) {
        return jsonResp({ error: 'date param required (YYYY-MM-DD)' }, 400, { 'Access-Control-Allow-Origin': '*' });
      }
      try {
        // First null out the existing m8bfPL so backfill picks it up
        // (the function's null-check is bypassed by targetDates anyway,
        // but we want a clean re-write).
        const ghHeaders = {
          'Authorization': `Bearer ${env.GITHUB_TOKEN}`,
          'Accept': 'application/vnd.github+json',
          'User-Agent': 'schwab-proxy-worker/1.0',
          'X-GitHub-Api-Version': '2022-11-28',
        };
        const apiUrl = 'https://api.github.com/repos/rava8989/brave/contents/history_data.json';
        const getR = await fetch(apiUrl, { headers: ghHeaders });
        const meta = await getR.json();
        const content = JSON.parse(atob(meta.content.replace(/\n/g, '')));
        const idx = content.findIndex(r => r.date === targetDate);
        if (idx < 0) return jsonResp({ error: `Date ${targetDate} not found in history` }, 404, { 'Access-Control-Allow-Origin': '*' });
        const before = { m8bfPL: content[idx].m8bfPL, m8bfWR: content[idx].m8bfWR };
        delete content[idx].m8bfPL;
        delete content[idx].m8bfWR;
        // Write back temporarily so backfill sees null
        const putR = await fetch(apiUrl, {
          method: 'PUT',
          headers: { ...ghHeaders, 'Content-Type': 'application/json' },
          body: JSON.stringify({
            message: `auto: clear m8bfPL/WR for ${targetDate} before recompute`,
            content: btoa(JSON.stringify(content, null, 0)),
            sha: meta.sha,
          }),
        });
        if (!putR.ok) return jsonResp({ error: `pre-clear failed: ${putR.status}` }, 500, { 'Access-Control-Allow-Origin': '*' });

        // Now run backfill on this specific date — it'll re-scrape Discord
        // history for the date and recompute m8bfPL + m8bfWR.
        const result = await backfillMissingPL(env, [targetDate]);

        // Re-read to show what got written
        const verifyR = await fetch(apiUrl, { headers: ghHeaders });
        const verifyMeta = await verifyR.json();
        const verifyContent = JSON.parse(atob(verifyMeta.content.replace(/\n/g, '')));
        const after = verifyContent.find(r => r.date === targetDate) || {};

        return jsonResp({
          ok: true,
          date: targetDate,
          before,
          after: { m8bfPL: after.m8bfPL, m8bfWR: after.m8bfWR },
          backfillResult: result,
        }, 200, { 'Access-Control-Allow-Origin': '*' });
      } catch (e) {
        return jsonResp({ error: e.message, stack: e.stack }, 500, { 'Access-Control-Allow-Origin': '*' });
      }
    }

    if (url.pathname === '/diagonal-trigger' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || secret !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      try {
        const etNowDT = toET(new Date());
        const result = await handleDiagonalTrade(env, etNowDT);
        return jsonResp(result, 200, { 'Access-Control-Allow-Origin': '*' });
      } catch (e) {
        return jsonResp({ error: e.message, stack: e.stack }, 500, { 'Access-Control-Allow-Origin': '*' });
      }
    }

    // ── GET /diagonal-today ── Public live diagonal state (no auth)
    // Returns the currently-open diagonal trade with live mid-quote refresh,
    // plus the most recent closed trade for the live page's tape view.
    // The cron refreshes mids every 2 min so this endpoint just reads KV
    // (zero Schwab API cost). On stale data the live page can show an age
    // indicator using `lastQuoteAt`.
    // ── GET /bobf-today ── Public live BOBF state (no auth)
    // Returns the currently-open/working/expired/closed BOBF trade.
    if (url.pathname === '/bobf-today' && request.method === 'GET') {
      const publicCors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '86400',
      };
      try {
        const etNowB = toET(new Date());
        const todayB = isoDateET(etNowB);
        const isWeekendB = etNowB.getDay() === 0 || etNowB.getDay() === 6;
        const isHolidayB = isHol(etNowB);

        let openRaw = await env.SIGNAL_KV.get('bobf_open_trade');
        let open = openRaw ? JSON.parse(openRaw) : null;

        // Stale-trade filter: settleBobfEOD overwrites bobf_open_trade with the
        // settled (status='closed') trade instead of deleting it, so on the next
        // trading day the slot still holds yesterday's closed trade. The render
        // logic treats anything in `open` as today's active position. Drop it
        // here — the settled trade is already preserved in bobf_closed_log and
        // surfaces as `lastClosed`, so no data is lost.
        if (open && (open.openDate !== todayB || open.status === 'closed' || open.status === 'expired')) {
          open = null;
        }

        // Staleness-triggered self-refresh (defends against cron stalls)
        if (open && (open.status === 'filled' || open.status === 'working') && !isWeekendB && !isHolidayB) {
          const isMktHr = (etNowB.getHours() > 9 || (etNowB.getHours() === 9 && etNowB.getMinutes() >= 30)) && etNowB.getHours() < 16;
          const ageMs = open.lastQuoteAt ? (Date.now() - new Date(open.lastQuoteAt).getTime()) : Infinity;
          if (isMktHr && ageMs > 90_000) {
            try {
              const tk = await getAccessToken(env);
              await refreshBobfLiveQuotes(env, tk, etNowB);
              openRaw = await env.SIGNAL_KV.get('bobf_open_trade');
              open = openRaw ? JSON.parse(openRaw) : null;
            } catch (e) { console.warn('[bobf-today] on-demand refresh failed:', e.message); }
          }
        }

        const logRaw = await env.SIGNAL_KV.get('bobf_closed_log');
        const closedLog = logRaw ? JSON.parse(logRaw) : [];
        const lastClosed = closedLog[0] || null;

        const doneKey = `bobf_done_${todayB}`;
        const doneState = await env.SIGNAL_KV.get(doneKey);

        // Surface the prefilter cache (RSI, SMA5, spxOpen, type, etc.)
        // so the live page can show today's RSI/SMA without re-fetching.
        const staticRaw = await env.SIGNAL_KV.get(`bobf_static_${todayB}`);
        const staticData = staticRaw ? JSON.parse(staticRaw) : null;

        // ── Calendar-blackout PREVIEW ──
        // The prefilter only runs at 9:30 ET, so before then `doneState` is
        // null and the live page wrongly shows "trade fires". Compute BOBF's
        // OWN calendar blackouts here (VIX-independent ones only) so the card
        // can show "No BOBF — OPEX" pre-market. This is a READ-ONLY mirror of
        // the prefilterBobf calendar block — it changes NO strategy rule, it
        // just surfaces the existing one earlier. Per strategy-independence:
        // this reflects BOBF's own rules, not any other strategy's.
        let blackoutPreview = null;
        if (!isWeekendB && !isHolidayB) {
          const _bo = [];
          if (cpiSch.includes(todayLong(etNowB)))        _bo.push('CPI');
          if (isFirstTradeMon(etNowB))                   _bo.push('NM Mon');
          if (vixSch.includes(todayLong(etNowB)))        _bo.push('VIX exp');
          if (opexSch.includes(todayLong(etNowB)))       _bo.push('OPEX');
          if (opexSch.some(ds => isTodayBefore(ds, etNowB))) _bo.push('OPEX-1');
          if (isEomN(2, etNowB))                         _bo.push('EOM-2');
          if (isEomN(1, etNowB))                         _bo.push('EOM-1');
          if (isEarningsDay(etNowB))                     _bo.push('earnings');
          if (_bo.length) blackoutPreview = _bo.join(',');
        }

        return jsonResp({
          date: todayB,
          isWeekend: isWeekendB,
          isHoliday: isHolidayB,
          open,
          lastClosed,
          doneState,
          blackoutPreview,    // calendar-only blackout string, or null (VIX>23 not included — needs live VIX)
          static: staticData,  // {rsi14, sma5, spxOpen, vixToday, vixYClose, type, label, bodyOffset}
          serverTimeET: `${String(etNowB.getHours()).padStart(2,'0')}:${String(etNowB.getMinutes()).padStart(2,'0')}`,
          windowET: '10:29 - 12:15',
          maxPremiumFriday: BOBF_FRIDAY_MAX_PREMIUM,
        }, 200, publicCors);
      } catch (e) {
        return jsonResp({ error: e.message }, 500, publicCors);
      }
    }

    // ── GET /gxbf-today ── Public live GXBF state (no auth)
    // Mirrors /bobf-today: returns the open/closed GXBF trade, doneState,
    // and skip reason. Center is computed live from the chain at entry
    // (no Discord scrape). Phantom prior-day/closed-slot cleanup +
    // staleness self-refresh during market hours. STRATEGY INDEPENDENCE:
    // surfaces only GXBF's own state.
    if (url.pathname === '/gxbf-today' && request.method === 'GET') {
      const publicCors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '86400',
      };
      try {
        const etNowG = toET(new Date());
        const todayG = isoDateET(etNowG);
        const isWeekendG = etNowG.getDay() === 0 || etNowG.getDay() === 6;
        const isHolidayG = isHol(etNowG);

        let openRaw = await env.SIGNAL_KV.get('gxbf_open_trade');
        let open = openRaw ? JSON.parse(openRaw) : null;

        // Stale-trade filter: settleGxbfEOD overwrites gxbf_open_trade with
        // the settled (status='closed') trade instead of deleting it, so on
        // the next trading day the slot still holds yesterday's closed trade.
        // Drop it here — the settled trade is preserved in gxbf_closed_log
        // and surfaces as `lastClosed`, so no data is lost. (Mirrors the
        // /bobf-today stale-trade filter.)
        if (open && (open.openDate !== todayG || open.status === 'closed' || open.status === 'expired')) {
          open = null;
        }

        // Staleness-triggered self-refresh (defends against cron stalls)
        if (open && open.status === 'filled' && !isWeekendG && !isHolidayG) {
          const isMktHr = (etNowG.getHours() > 9 || (etNowG.getHours() === 9 && etNowG.getMinutes() >= 30)) && etNowG.getHours() < 16;
          const ageMs = open.lastQuoteAt ? (Date.now() - new Date(open.lastQuoteAt).getTime()) : Infinity;
          if (isMktHr && ageMs > 90_000) {
            try {
              const tk = await getAccessToken(env);
              await refreshGxbfLiveQuotes(env, tk, etNowG);
              openRaw = await env.SIGNAL_KV.get('gxbf_open_trade');
              open = openRaw ? JSON.parse(openRaw) : null;
              if (open && (open.openDate !== todayG || open.status === 'closed' || open.status === 'expired')) open = null;
            } catch (e) { console.warn('[gxbf-today] on-demand refresh failed:', e.message); }
          }
        }

        const logRaw = await env.SIGNAL_KV.get('gxbf_closed_log');
        const closedLog = logRaw ? JSON.parse(logRaw) : [];
        const lastClosed = closedLog[0] || null;

        const doneKey = `gxbf_done_${todayG}`;
        const doneState = await env.SIGNAL_KV.get(doneKey);

        // Skip reason recorded at 9:30 if signal.theme !== 'gxbf' that day.
        let skip = null;
        const skipRaw = await env.SIGNAL_KV.get(`gxbf_skip_${todayG}`);
        if (skipRaw) skip = JSON.parse(skipRaw);

        // On-demand recovery from the persisted morning signal (mirrors the
        // /straddle-today morning-data fallback). Only GXBF's own theme.
        const haveOpenTodayG = open && open.openDate === todayG;
        if (!haveOpenTodayG && !skip) {
          const morningDataRaw = await env.SIGNAL_KV.get(`morning_signal_data_${todayG}`);
          if (morningDataRaw) {
            const morningData = JSON.parse(morningDataRaw);
            if (morningData.theme === 'gxbf') {
              skip = { theme: 'gxbf-missed', rec: `${morningData.gxbfText || morningData.rec} — open failed`,
                       recordedAt: new Date().toISOString(), source: 'morning-data' };
            } else {
              skip = { theme: morningData.theme, rec: morningData.gxbfText || morningData.rec,
                       recordedAt: new Date().toISOString(), source: 'morning-data' };
            }
            await env.SIGNAL_KV.put(`gxbf_skip_${todayG}`, JSON.stringify(skip), { expirationTtl: 86400 });
          }
        }

        return jsonResp({
          date: todayG,
          isWeekend: isWeekendG,
          isHoliday: isHolidayG,
          open,
          lastClosed,
          doneState,
          skip,                 // {theme, rec} when signal said no-GXBF today
          serverTimeET: `${String(etNowG.getHours()).padStart(2,'0')}:${String(etNowG.getMinutes()).padStart(2,'0')}`,
          windowET: '09:35 - 09:45',
        }, 200, publicCors);
      } catch (e) {
        return jsonResp({ error: e.message }, 500, publicCors);
      }
    }

    // ── GET /straddle-today ── Public live straddle state (no auth)
    // Returns the currently-open/working/expired/closed straddle trade plus
    // the most recent closed straddle. Live mids refreshed by the cron, so
    // this is a cheap KV read (no Schwab API call here).
    if (url.pathname === '/straddle-today' && request.method === 'GET') {
      const publicCors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '86400',
      };
      try {
        const etNowSt = toET(new Date());
        const todaySt = isoDateET(etNowSt);
        const isWeekendSt = etNowSt.getDay() === 0 || etNowSt.getDay() === 6;
        const isHolidaySt = isHol(etNowSt);

        let openRaw = await env.SIGNAL_KV.get('straddle_open_trade');
        let open = openRaw ? JSON.parse(openRaw) : null;

        // ── Self-heal #0: phantom-trade cleanup ─────────────────────────────
        // Mirrors the Diagonal pattern (line 4879). Three cases:
        //  (a) Prior-day trade left in open slot — close+open lifecycle failed
        //      to KV.delete after settle. Refresh loop keeps mutating ghost.
        //  (b) Today's slot holds an already-closed/expired trade — settle
        //      wrote the closed_log entry but didn't drop the open key.
        //  (c) Off-strategy phantom (observed 2026-05-14): trade exists for
        //      today, but morning signal said theme!='strad'. Means it fired
        //      on stale/wrong data and slipped through the gate. Delete it
        //      so live page reflects the morning signal's actual outcome.
        if (open && open.openDate && open.openDate < todaySt) {
          console.warn(`[strad-today] phantom #a: prior-day open (openDate=${open.openDate}) — clearing`);
          await env.SIGNAL_KV.delete('straddle_open_trade');
          await logEvent(env, 'warn', 'strad-phantom', `phantom #a cleared: prior-day open`, { openDate: open.openDate, todayDate: todaySt });
          open = null;
          openRaw = null;
        } else if (open && (open.status === 'closed' || open.status === 'expired')) {
          console.warn(`[strad-today] phantom #b: open slot held ${open.status} trade — clearing`);
          await env.SIGNAL_KV.delete('straddle_open_trade');
          await logEvent(env, 'warn', 'strad-phantom', `phantom #b cleared: open slot held ${open.status} trade`, { status: open.status });
          open = null;
          openRaw = null;
        } else if (open && open.openDate === todaySt) {
          try {
            const morningDataRaw = await env.SIGNAL_KV.get(`morning_signal_data_${todaySt}`);
            if (morningDataRaw) {
              const morningData = JSON.parse(morningDataRaw);
              if (morningData.theme && morningData.theme !== 'strad') {
                console.warn(`[strad-today] phantom #c: off-strategy open (morning theme=${morningData.theme}) — clearing`);
                await env.SIGNAL_KV.delete('straddle_open_trade');
                await logEvent(env, 'error', 'strad-phantom', `phantom #c cleared: off-strategy open`, { openTheme: 'strad', morningTheme: morningData.theme });
                open = null;
                openRaw = null;
              }
            }
          } catch (phantomErr) {
            console.warn('[strad-today] phantom-c check failed:', phantomErr.message);
          }
        }

        // Staleness-triggered self-refresh (defends against cron stalls)
        if (open && (open.status === 'filled' || open.status === 'working') && !isWeekendSt && !isHolidaySt) {
          const isMktHr = (etNowSt.getHours() > 9 || (etNowSt.getHours() === 9 && etNowSt.getMinutes() >= 30)) && etNowSt.getHours() < 16;
          const ageMs = open.lastQuoteAt ? (Date.now() - new Date(open.lastQuoteAt).getTime()) : Infinity;
          if (isMktHr && ageMs > 90_000) {
            try {
              const tk = await getAccessToken(env);
              await refreshStraddleLiveQuotes(env, tk, etNowSt);
              openRaw = await env.SIGNAL_KV.get('straddle_open_trade');
              open = openRaw ? JSON.parse(openRaw) : null;
            } catch (e) { console.warn('[strad-today] on-demand refresh failed:', e.message); }
          }
        }

        const logRaw = await env.SIGNAL_KV.get('straddle_closed_log');
        const closedLog = logRaw ? JSON.parse(logRaw) : [];
        const lastClosed = closedLog[0] || null;

        // Skip reason recorded at 9:30 if signal.theme !== 'strad' that day.
        let skip = null;
        const skipRaw = await env.SIGNAL_KV.get(`straddle_skip_${todaySt}`);
        if (skipRaw) skip = JSON.parse(skipRaw);

        // ── On-demand recovery (uses ACTUAL morning signal data) ───────────
        // The morning cron writes morning_signal_data_<date> with the signal
        // it computed (including live-quote-polled vixToday and official
        // vixYClose from quotes endpoint, which differ from candle data).
        // Reading that gives us the EXACT signal the worker saw, not a
        // recomputation that might disagree.
        const haveOpenToday = open && open.openDate === todaySt;
        if (!haveOpenToday) {
          // Clear stale "strad-missed" skip from old recovery code that
          // recomputed signal from candle data and got it wrong.
          if (skip && skip.source === 'on-demand-recovery-missed') {
            await env.SIGNAL_KV.delete(`straddle_skip_${todaySt}`);
            skip = null;
          }
          // Backfill: existing strad-missed skip lacks plannedStrike / plannedMaxDebit
          // (added 2026-05-22). Force recompute so live page can show working-order info.
          if (skip && skip.theme === 'strad-missed' && skip.plannedStrike === undefined) {
            await env.SIGNAL_KV.delete(`straddle_skip_${todaySt}`);
            skip = null;
          }
          // Backfill: if cached plannedMaxDebit doesn't match the current
          // straddleMaxDebit() for this badge, the maxDebit constants changed
          // — force recompute so live page shows the right limit.
          if (skip && skip.theme === 'strad-missed' && skip.plannedMaxDebit != null
              && skip.badge && straddleMaxDebit(skip.badge) !== skip.plannedMaxDebit) {
            await env.SIGNAL_KV.delete(`straddle_skip_${todaySt}`);
            skip = null;
          }
          if (!skip) {
            const morningDataRaw = await env.SIGNAL_KV.get(`morning_signal_data_${todaySt}`);
            if (morningDataRaw) {
              const morningData = JSON.parse(morningDataRaw);
              if (morningData.theme === 'strad') {
                // Signal genuinely said straddle but no live trade exists.
                // Compute planned strike + maxDebit so the live page can show
                // "Working order at strike X · limit $Y" instead of a generic
                // "open failed" — until workCutoffET (13:30) hits, this is the
                // working state. After cutoff the live page flips to "price
                // target not hit".
                const spxOpen = parseFloat(morningData.spxOpen);
                const plannedStrike = isFinite(spxOpen) && spxOpen > 0
                  ? Math.round(spxOpen / 5) * 5
                  : null;
                const plannedMaxDebit = straddleMaxDebit(morningData.badge || 'STRADDLE');
                skip = {
                  theme: 'strad-missed',
                  rec: `${morningData.rec} — open failed`,
                  badge: morningData.badge || 'STRADDLE',
                  plannedStrike,
                  plannedMaxDebit,
                  recordedAt: new Date().toISOString(),
                  source: 'morning-data',
                };
              } else {
                skip = { theme: morningData.theme, rec: morningData.rec,
                         recordedAt: new Date().toISOString(), source: 'morning-data' };
              }
              await env.SIGNAL_KV.put(`straddle_skip_${todaySt}`, JSON.stringify(skip), { expirationTtl: 86400 });
            }
            // If morningData isn't present yet (today, since this code is
            // newly deployed), leave skip as null. Live page will show
            // generic 'WAITING SIGNAL' which is honest — we don't know
            // what signal said.
          }
        }

        return jsonResp({
          date: todaySt,
          isWeekend: isWeekendSt,
          isHoliday: isHolidaySt,
          open,                                  // current trade or null
          lastClosed,                            // most recent settled trade
          skip,                                  // {theme, rec} when signal said no-straddle today
          serverTimeET: `${String(etNowSt.getHours()).padStart(2,'0')}:${String(etNowSt.getMinutes()).padStart(2,'0')}`,
          maxDebits: { NM: STRADDLE_MAX_DEBIT_NM, EOM: STRADDLE_MAX_DEBIT_EOM, plain: STRADDLE_MAX_DEBIT_OTHER },
          workCutoffET: '13:30',
        }, 200, publicCors);
      } catch (e) {
        return jsonResp({ error: e.message }, 500, publicCors);
      }
    }

    if (url.pathname === '/diagonal-today' && request.method === 'GET') {
      const publicCors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '86400',
      };
      try {
        const etNowDT = toET(new Date());
        const todayDT = isoDateET(etNowDT);
        const isWeekendDT = etNowDT.getDay() === 0 || etNowDT.getDay() === 6;
        const isHolidayDT = isHol(etNowDT);

        let openRaw = await env.SIGNAL_KV.get('diagonal_open_trade');
        let open = openRaw ? JSON.parse(openRaw) : null;

        // ── Self-heal #0: phantom-trade cleanup ─────────────────────────────
        // If today's close+open lifecycle already ran but the KV.delete after
        // close silently failed, the prior-day trade sticks around as a ghost
        // and keeps getting refreshed. Detect on first endpoint hit and clear.
        if (open && open.openDate && open.openDate < todayDT && open.status === 'open') {
          const diagDone = await env.SIGNAL_KV.get(`diag_done_${todayDT}`);
          if (diagDone) {
            console.warn(`[diag-today] phantom open trade (openDate=${open.openDate}) — clearing`);
            await env.SIGNAL_KV.delete('diagonal_open_trade');
            await logEvent(env, 'warn', 'diag-phantom', 'phantom open trade cleared at endpoint',
                           { openDate: open.openDate, todayDate: todayDT });
            open = null;
            openRaw = null;
          }
        }

        // ── Self-heal #1: close+open lifecycle (cron-stall defense) ─────────
        // The 12:30 ET close+open cycle is supposed to be triggered by the
        // cron tick. If the cron stalls (recurring Cloudflare issue), the
        // prior-day trade stays open and no new one opens. Self-heal here:
        // if it's past 12:30 ET on a market day AND we have an open trade
        // whose openDate is before today AND no diag_done_<today> key,
        // run handleDiagonalTrade (which is idempotent — sets diag_done
        // after success, so concurrent endpoint hits won't double-trigger).
        if (open && open.openDate && open.openDate < todayDT && !isWeekendDT && !isHolidayDT) {
          const past1230 = etNowDT.getHours() > 12 || (etNowDT.getHours() === 12 && etNowDT.getMinutes() >= 30);
          const beforeMarketClose = etNowDT.getHours() < 16;
          if (past1230 && beforeMarketClose) {
            const diagDone = await env.SIGNAL_KV.get(`diag_done_${todayDT}`);
            if (!diagDone) {
              try {
                console.log('[diag-today] cron-stall recovery: triggering close+open lifecycle');
                await handleDiagonalTrade(env, etNowDT);
                // handleDiagonalTrade writes both the close result AND the new
                // trade. Re-read to pick up the new state.
                openRaw = await env.SIGNAL_KV.get('diagonal_open_trade');
                open = openRaw ? JSON.parse(openRaw) : null;
                // Mark done so subsequent endpoint calls don't re-trigger
                await env.SIGNAL_KV.put(`diag_done_${todayDT}`, 'self-heal', { expirationTtl: 86400 });
              } catch (e) { console.warn('[diag-today] cron-stall recovery failed:', e.message); }
            }
          }
        }

        // ── Self-heal #2: stale live mids (cron-stall mid-trade) ────────────
        // If we have an open trade and its lastQuoteAt is older than 90s,
        // fetch fresh chain mids on this endpoint hit. Browser polls every
        // 30s so first stale poll triggers refresh, subsequent ones cache.
        if (open && open.status === 'open' && !isWeekendDT && !isHolidayDT) {
          const isMarketHoursDT = (etNowDT.getHours() > 9 ||
                                   (etNowDT.getHours() === 9 && etNowDT.getMinutes() >= 30)) &&
                                  etNowDT.getHours() < 16;
          const ageMs = open.lastQuoteAt ? (Date.now() - new Date(open.lastQuoteAt).getTime()) : Infinity;
          if (isMarketHoursDT && ageMs > 90_000) {
            try {
              const refreshToken = await getAccessToken(env);
              await refreshDiagonalLiveQuotes(env, refreshToken);
              // Re-read to get the freshly written values
              openRaw = await env.SIGNAL_KV.get('diagonal_open_trade');
              open = openRaw ? JSON.parse(openRaw) : null;
            } catch (e) { console.warn('[diag-today] on-demand refresh failed:', e.message); }
          }
        }

        const logRaw = await env.SIGNAL_KV.get('diagonal_closed_log');
        const closedLog = logRaw ? JSON.parse(logRaw) : [];
        const lastClosed = closedLog[0] || null;

        // Today's signal preview — even if 12:30 hasn't fired yet, the live
        // page can show "Diagonal pending" / "Skipped (NM)" / "GO at 12:30".
        // Fetch vixPct20d via the same path the cron uses.
        let vixPct20d = null, vixToday = null, sigPreview = null;
        try {
          const histData = await getHistory(env);
          if (Array.isArray(histData) && histData.length) {
            const todayRow = histData.find(r => r.date === todayDT);
            vixToday = todayRow?.vixOpen != null ? parseFloat(todayRow.vixOpen) : null;
            // FALLBACK: if morning signal block didn't write today's vixOpen
            // (Schwab outage at 9:30 AM), use the most recent vixClose from
            // history. Slightly off (close vs open) but typically within a few
            // basis points and beats "no signal at all". Keeps VIX 20d
            // percentile working without any live Schwab dependency.
            if (vixToday == null) {
              const prior = histData
                .filter(r => r.date < todayDT && r.vixClose != null && r.vixClose > 0)
                .sort((a, b) => a.date.localeCompare(b.date));
              if (prior.length) {
                vixToday = parseFloat(prior[prior.length - 1].vixClose);
                console.warn(`[diag-today] vixOpen missing for ${todayDT}; falling back to prior vixClose=${vixToday}`);
              }
            }
            const vix20 = histData
              .filter(r => r.date < todayDT && r.vixClose != null && r.vixClose > 0)
              .sort((a, b) => a.date.localeCompare(b.date))
              .slice(-20)
              .map(r => parseFloat(r.vixClose));
            // CANONICAL — single source: signal-engine.js computeVixPct20d.
            vixPct20d = computeVixPct20d(vixToday, vix20).pct;
          }
        } catch (_) { /* fall through to null */ }
        try { sigPreview = computeDiagonalSignal(etNowDT, vixPct20d); } catch (_) {}

        return jsonResp({
          date: todayDT,
          isWeekend: isWeekendDT,
          isHoliday: isHolidayDT,
          open,                              // currently active trade or null
          lastClosed,                        // most recently closed trade (for context)
          signal: sigPreview,                // {diagText, diagBadge, diagGo, diagSkipCode, vixPct20d}
          vixPct20d,
          serverTimeET: `${String(etNowDT.getHours()).padStart(2,'0')}:${String(etNowDT.getMinutes()).padStart(2,'0')}`,
        }, 200, publicCors);
      } catch (e) {
        return jsonResp({ error: e.message }, 500, publicCors);
      }
    }

    // ── GET /heartbeat?token=... ── External 1-min cron replacement.
    // Cloudflare Workers' built-in cron scheduler drops ticks silently
    // (recurring issue — see live page self-heal endpoints elsewhere).
    // External monitors (cron-job.org, UptimeRobot Pro, GH Actions, etc.)
    // hit this endpoint every minute during market hours and force
    // handleScheduled() to run — bypassing CF's mood.
    //
    // Dedicated token (HEARTBEAT_TOKEN secret, separate from SYNC_SECRET)
    // so the URL is safe to paste into 3rd-party monitor config without
    // exposing the master admin secret.
    //
    // Logs last_heartbeat_at in KV so the user can verify the external
    // monitor is actually hitting us.
    if (url.pathname === '/heartbeat' && request.method === 'GET') {
      const tokenH = url.searchParams.get('token');
      if (!tokenH || tokenH !== env.HEARTBEAT_TOKEN) {
        return jsonResp({ error: 'Unauthorized — bad/missing token param' }, 401,
          { 'Access-Control-Allow-Origin': '*' });
      }
      // Respond instantly with 200. Run handleScheduled() in the background
      // via ctx.waitUntil() so cron-job.org (and other 30s-timeout monitors)
      // don't log timeouts on the busy 9:30 ET tick. Cloudflare keeps the
      // worker alive up to its wall-clock budget to complete the background
      // task — even if the client connection closed.
      const startMs = Date.now();
      const runScheduled = async () => {
        let scheduledResult, error = null;
        try {
          scheduledResult = await handleScheduled(env);
        } catch (e) {
          error = e.message;
          scheduledResult = { status: 'error', error: e.message };
        }
        const ranMs = Date.now() - startMs;
        try {
          await env.SIGNAL_KV.put('last_heartbeat', JSON.stringify({
            at: new Date().toISOString(),
            ranMs,
            status: scheduledResult?.status || 'unknown',
            error,
            ua: request.headers.get('User-Agent') || '',
            ip: request.headers.get('cf-connecting-ip') || '',
          }));
        } catch (_) {}
        try {
          await env.SIGNAL_KV.put('last_run', JSON.stringify({
            ...(scheduledResult || {}),
            date: new Date().toISOString(),
            source: 'heartbeat',
          }));
        } catch (_) {}
      };
      if (ctx && typeof ctx.waitUntil === 'function') {
        ctx.waitUntil(runScheduled());
        return jsonResp({ ok: true, queued: true }, 200, { 'Access-Control-Allow-Origin': '*' });
      }
      // Fallback: synchronous (e.g., in `wrangler dev` without ctx)
      await runScheduled();
      return jsonResp({ ok: true, ranMs: Date.now() - startMs }, 200, { 'Access-Control-Allow-Origin': '*' });
    }

    // GET /tasty-oauth-start — kick off Tastytrade OAuth.
    // Visit this in a browser → redirects to Tastytrade authorize page →
    // user approves → Tastytrade redirects back to /tasty-oauth-callback.
    if (url.pathname === '/tasty-oauth-start' && request.method === 'GET') {
      if (!env.TASTYTRADE_CLIENT_ID) {
        return jsonResp({ error: 'TASTYTRADE_CLIENT_ID not configured' }, 500, { 'Access-Control-Allow-Origin': '*' });
      }
      return Response.redirect(tastyAuthorizeUrl(env), 302);
    }

    // GET /tasty-oauth-callback?code=... — Tastytrade redirects here after
    // the user approves. Exchanges code for refresh_token, stores it in KV,
    // and shows a confirmation page.
    if (url.pathname === '/tasty-oauth-callback' && request.method === 'GET') {
      const code = url.searchParams.get('code');
      const err  = url.searchParams.get('error');
      if (err) {
        return new Response(`<h1>OAuth error</h1><p>${err}: ${url.searchParams.get('error_description') || ''}</p>`,
          { status: 400, headers: { 'Content-Type': 'text/html' } });
      }
      if (!code) {
        return new Response(`<h1>Missing code parameter</h1>`,
          { status: 400, headers: { 'Content-Type': 'text/html' } });
      }
      try {
        const tokens = await tastyExchangeCode(env, code);
        if (!tokens.refresh_token) throw new Error('no refresh_token in response: ' + JSON.stringify(tokens).slice(0, 300));
        // Persist refresh token (long-lived) and cache access token
        await env.SIGNAL_KV.put('tasty_refresh_token', tokens.refresh_token);
        if (tokens.access_token) {
          await env.SIGNAL_KV.put('tasty_access_token', JSON.stringify({
            access_token: tokens.access_token,
            expires_at: Date.now() + ((tokens.expires_in || 900) * 1000),
          }), { expirationTtl: tokens.expires_in || 900 });
        }
        return new Response(
          `<html><body style="font-family:system-ui;padding:40px;background:#0f1117;color:#e5e7eb">
            <h1 style="color:#22c55e">✓ Tastytrade connected</h1>
            <p>Refresh token saved. The worker can now mint access tokens automatically.</p>
            <p>Test it: <a style="color:#818cf8" href="/tasty-vix-test">/tasty-vix-test</a></p>
          </body></html>`,
          { status: 200, headers: { 'Content-Type': 'text/html' } }
        );
      } catch (e) {
        return new Response(
          `<html><body style="font-family:system-ui;padding:40px;background:#0f1117;color:#e5e7eb">
            <h1 style="color:#ef4444">OAuth callback failed</h1>
            <pre>${e.message}</pre>
          </body></html>`,
          { status: 500, headers: { 'Content-Type': 'text/html' } }
        );
      }
    }

    // GET /tasty-vix-test — debug endpoint to verify Tastytrade VIX path
    // Returns the current VIX price as Tastytrade sees it, plus session
    // status and which endpoint shape worked. No auth required — read-only
    // debug. Behind public CORS like /health.
    if (url.pathname === '/tasty-vix-test' && request.method === 'GET') {
      const publicCors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '86400',
      };
      try {
        const cached = await env.SIGNAL_KV.get('tasty_session_token');
        const result = await tastyGetVix(env);
        return jsonResp({
          ok: true,
          sessionCached: !!cached,
          vix: result,
        }, 200, publicCors);
      } catch (e) {
        return jsonResp({ ok: false, error: e.message }, 500, publicCors);
      }
    }

    if (url.pathname === '/tasty-spx-test' && request.method === 'GET') {
      const publicCors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '86400',
      };
      try {
        const cached = await env.SIGNAL_KV.get('tasty_session_token');
        const result = await tastyGetSpx(env);
        return jsonResp({
          ok: true,
          sessionCached: !!cached,
          spx: result,
        }, 200, publicCors);
      } catch (e) {
        return jsonResp({ ok: false, error: e.message }, 500, publicCors);
      }
    }

    // GET /tasty-chain-test?exp=YYYY-MM-DD[&strikes=20&type=BOTH&compare=1]
    // Test the tastyFetchSpxChain wrapper end-to-end. With ?compare=1 it also
    // fetches the same chain via Schwab so you can verify shape parity and
    // quote agreement. Read-only. Doesn't affect any live path.
    if (url.pathname === '/tasty-chain-test' && request.method === 'GET') {
      const cors = { 'Access-Control-Allow-Origin': '*' };
      try {
        const root = url.searchParams.get('root') || 'SPXW';
        const exp = url.searchParams.get('exp');
        const strikes = parseInt(url.searchParams.get('strikes') || '20', 10);
        const ctype = (url.searchParams.get('type') || 'BOTH').toUpperCase();
        const compare = url.searchParams.get('compare') === '1';
        const opts = { root, strikeCount: strikes, contractType: ctype };
        if (exp) opts.expirations = [exp];

        const tStart = Date.now();
        const tasty = await tastyFetchSpxChain(env, opts);
        const tastyMs = Date.now() - tStart;

        const sample = {};
        const firstCallExp = Object.keys(tasty.callExpDateMap)[0];
        if (firstCallExp) {
          const strikeKeys = Object.keys(tasty.callExpDateMap[firstCallExp]).sort((a,b) =>
            Math.abs(parseFloat(a) - (tasty.spot||0)) - Math.abs(parseFloat(b) - (tasty.spot||0)));
          const atm = strikeKeys.slice(0, 3);
          sample.tasty = atm.map(k => {
            const c = tasty.callExpDateMap[firstCallExp]?.[k]?.[0];
            const p = tasty.putExpDateMap[firstCallExp]?.[k]?.[0];
            return { strike: k, call: c && { bid: c.bid, ask: c.ask, mark: c.mark, delta: c.delta, gamma: c.gamma, symbol: c.symbol }, put: p && { bid: p.bid, ask: p.ask, mark: p.mark, delta: p.delta, gamma: p.gamma, symbol: p.symbol } };
          });
        }

        let schwab = null, schwabMs = null, schwabErr = null, schwabSample = null;
        if (compare) {
          const sStart = Date.now();
          try {
            const token = await getAccessToken(env);
            const params = new URLSearchParams({
              symbol: '$SPX', strikeCount: String(strikes), includeUnderlyingQuote: 'true', strategy: 'SINGLE',
            });
            if (exp) { params.set('fromDate', exp); params.set('toDate', exp); }
            if (ctype !== 'BOTH') params.set('contractType', ctype);
            const data = await fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/chains?${params}`, token, env);
            schwabMs = Date.now() - sStart;
            schwab = {
              spot: data.underlyingPrice || data.underlying?.last,
              callExps: Object.keys(data.callExpDateMap || {}),
              putExps: Object.keys(data.putExpDateMap || {}),
            };
            // Sample ATM strikes via Schwab
            const sExp = Object.keys(data.callExpDateMap || {})[0];
            if (sExp) {
              const sStrikes = Object.keys(data.callExpDateMap[sExp]).sort((a,b) =>
                Math.abs(parseFloat(a) - (schwab.spot||0)) - Math.abs(parseFloat(b) - (schwab.spot||0)));
              schwabSample = sStrikes.slice(0, 3).map(k => {
                const c = data.callExpDateMap[sExp]?.[k]?.[0];
                const p = (data.putExpDateMap || {})[sExp]?.[k]?.[0];
                return { strike: k, call: c && { bid: c.bid, ask: c.ask, mark: c.mark, delta: c.delta, gamma: c.gamma, symbol: c.symbol }, put: p && { bid: p.bid, ask: p.ask, mark: p.mark, delta: p.delta, gamma: p.gamma, symbol: p.symbol } };
              });
            }
          } catch (e) { schwabErr = e.message; }
        }

        return jsonResp({
          ok: true,
          opts,
          tasty: {
            spot: tasty.spot, fetchMs: tastyMs,
            callExpKeys: Object.keys(tasty.callExpDateMap),
            putExpKeys: Object.keys(tasty.putExpDateMap),
            stats: tasty._stats,
            sample: sample.tasty,
          },
          schwab: compare ? { spot: schwab?.spot, fetchMs: schwabMs, error: schwabErr, callExps: schwab?.callExps, putExps: schwab?.putExps, sample: schwabSample } : 'skip (pass ?compare=1)',
        }, 200, cors);
      } catch (e) {
        return jsonResp({ ok: false, error: e.message, stack: e.stack?.slice(0, 400) }, 500, cors);
      }
    }

    // GET /debug-morning-dual
    // Exercises the SAME dual-source signal logic the morning cron uses,
    // but with single quick fetches (no polling for new ticks). Returns
    // JSON with both signals + both rendered Discord messages + any per-
    // source errors. Does NOT post to Discord. Use to verify the code path
    // works without waiting for 9:30 ET.
    //   ?force_no_schwab=1  — simulate Schwab failure (skip Schwab fetch)
    //   ?force_no_tasty=1   — simulate Tasty failure (skip Tasty fetch)
    //   ?force_no_token=1   — simulate Schwab token unavailable
    if (url.pathname === '/tasty-index-test' && request.method === 'GET') {
      const sym = url.searchParams.get('sym') || 'VIX';
      let result;
      try { result = await tastyGetIndexQuote(env, sym); delete result.raw; }
      catch (e) { result = { error: e.message }; }
      return new Response(JSON.stringify({ sym, ...result }, null, 2),
        { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
    }

    // Manual "*-now" trigger routes hit Schwab / GitHub / Discord. Gate them with
    // SYNC_SECRET (header or ?secret=), like the other trigger routes, so they can't
    // be invoked anonymously (audit 2026-06-22). Cron calls the underlying functions
    // directly — not these HTTP routes — so scheduled runs are unaffected.
    if (request.method !== 'OPTIONS' &&
        ['/cyclicality-append-now', '/score-advisories-now', '/research-persist-now',
         '/cot-refresh-now', '/watchdog-now', '/weekly-digest-now', '/vix-decomp-now',
         '/remirror-history'].includes(url.pathname)) {
      const s = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!s || s !== env.SYNC_SECRET) return jsonResp({ error: 'Unauthorized' }, 401, corsHeaders);
    }

    if (url.pathname === '/cyclicality-append-now' && request.method === 'GET') {
      let result;
      try { result = await appendCyclicalityDays(env, { symbol: url.searchParams.get('symbol') === 'ndx' ? '%24NDX' : '%24SPX', file: url.searchParams.get('symbol') === 'ndx' ? 'cyclicality_ndx.json' : 'cyclicality_data.json', backDays: parseInt(url.searchParams.get('backDays') || '12', 10) }); } catch (e) { result = { error: e.message }; }
      return new Response(JSON.stringify(result, null, 2),
        { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
    }

    if (url.pathname === '/score-advisories-now' && request.method === 'GET') {
      let result;
      try { result = await scoreAdvisories(env); } catch (e) { result = { error: e.message }; }
      return new Response(JSON.stringify(result, null, 2),
        { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
    }

    if (url.pathname === '/advisory-lines' && request.method === 'GET') {
      // The three informational lines the REAL morning message carries —
      // served to the dashboard so its test/manual sends match production.
      const out = { tilt: null, gex: null, daytype: null, volflow: null };
      const etNowA = toET(new Date());
      try { out.tilt = await computeTiltLine(env, isoDateET(etNowA)); } catch (_) {}
      try { out.gex = await computeGexLine(env); } catch (_) {}
      try { out.daytype = await computeCycleLine(env, etNowA); } catch (_) {}
      try { out.volflow = await computeVolFlowLine(env, etNowA); } catch (_) {}
      try { out.m8bfwr = await computeM8bfWrLine(env, etNowA); } catch (_) {}
      return new Response(JSON.stringify(out),
        { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
    }

    if (url.pathname === '/research-persist-now' && request.method === 'GET') {
      // Manual trigger for the EOD research persist (idempotent upserts).
      // ?date=YYYY-MM-DD back-heals a missed day (KV captures live 90d).
      let etNowR = toET(new Date());
      const ovr = url.searchParams.get('date');
      if (ovr && /^\d{4}-\d{2}-\d{2}$/.test(ovr)) etNowR = toET(new Date(`${ovr}T16:30:00-04:00`));
      let result;
      try { result = await persistResearchArtifacts(env, etNowR); }
      catch (e) { result = { error: e.message }; }
      return new Response(JSON.stringify({ date: isoDateET(etNowR), result }, null, 2),
        { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
    }

    if (url.pathname === '/tail-today' && request.method === 'GET') {
      // Tail Hedge live status for the Live page (user 2026-06-11: the only
      // manual strategy must appear among today's trades when triggered).
      // Truth source = getTailHedgeStatusLine (same line as the morning msg).
      const etNowT = toET(new Date());
      const todayT = isoDateET(etNowT);
      const line = await getTailHedgeStatusLine(env);
      let st = null, snap = null, openRec = null;
      try { st = JSON.parse(await env.SIGNAL_KV.get('tail_trigger_state') || 'null'); } catch (_) {}
      try { snap = JSON.parse(await env.SIGNAL_KV.get(`tail_put_snap_${todayT}`) || 'null'); } catch (_) {}
      try { openRec = JSON.parse(await env.SIGNAL_KV.get(`cor1m_open_${todayT}`) || 'null'); } catch (_) {}
      // candidate = nearest-expiry put closest to Δ-0.10 from the 9:45 snapshot
      let candidate = null;
      if (snap && Array.isArray(snap.puts) && snap.puts.length) {
        const e0 = snap.puts[0].e;
        candidate = snap.puts.filter(p => p.e === e0)
          .sort((a, b) => Math.abs(a.d + 0.10) - Math.abs(b.d + 0.10))[0] || null;
      }
      // Frozen bot-tradeable open: cron freezes it at/after 9:45 (this is a
      // fallback that freezes if a client polls first). After settleTailEOD it
      // carries status='closed' + pnl and is surfaced as `lastClosed`.
      let tailOpen = null;
      try { tailOpen = await freezeTailOpenIfDue(env, etNowT, line); } catch (_) {}
      // Settled days → Final P&L card on the live page (mirrors bobf/gxbf lastClosed).
      let lastClosed = null;
      try {
        const lcRaw = await env.SIGNAL_KV.get('tail_closed_log');
        const lc = lcRaw ? JSON.parse(lcRaw) : [];
        lastClosed = lc[0] || null;
      } catch (_) {}
      // A settled (closed) open is shown as lastClosed, not as an active open.
      if (tailOpen && tailOpen.status === 'closed') tailOpen = null;
      return new Response(JSON.stringify({
        date: todayT, line,
        active: line.includes('▶ TRADE'), skip: line.includes('SKIP today'),
        state: st, cor1m: openRec?.cor1m ?? null, vvix: openRec?.vvix ?? null,
        spot: snap?.spot ?? null, snapAt: snap?.at ?? null, candidate,
        open: tailOpen, lastClosed,
      }), { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
    }

    if (url.pathname === '/settle-tail' && request.method === 'POST') {
      // Manual/backfill trigger for the Tail Hedge EOD settle + safety fallback
      // if the EOD cron missed it. Origin-gated (mirrors /send-card). Idempotent:
      // settleTailEOD flips status→'closed' and upsertHistoryGitHub won't
      // overwrite an existing tailPL. Optional ?spxClose=NNNN overrides the close.
      const originS = request.headers.get('Origin') || '';
      if (originS !== 'https://rava8989.github.io') {
        return new Response(JSON.stringify({ error: 'forbidden' }),
          { status: 403, headers: { 'Content-Type': 'application/json' } });
      }
      const etNowS = toET(new Date());
      let spxCloseS = null;
      const qpS = url.searchParams.get('spxClose');
      if (qpS && !isNaN(parseFloat(qpS))) spxCloseS = parseFloat(qpS);
      if (spxCloseS == null) {
        // Use the CANONICAL EOD close already in history (1-min candle ≤16:15,
        // the exact value the other strategies settled against) so tail P&L is
        // consistent — NOT quote.closePrice (that's a prior/quote field).
        try {
          const histS = await getHistory(env);
          const rowS = (histS || []).find(r => r.date === isoDateET(etNowS));
          if (rowS && rowS.spxClose != null) spxCloseS = parseFloat(rowS.spxClose);
        } catch (_) {}
      }
      let resultS;
      try { resultS = await settleTailEOD(env, etNowS, spxCloseS); }
      catch (e) { resultS = { error: e.message }; }
      return new Response(JSON.stringify({ date: isoDateET(etNowS), spxClose: spxCloseS, result: resultS }, null, 2),
        { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': 'https://rava8989.github.io' } });
    }

    if (url.pathname === '/eod-history' && request.method === 'GET') {
      // Read-only Schwab daily-candle proxy (2026-06-12, COT edge study +
      // general research). Public like the other market-data debug routes —
      // serves OHLC only, no account access. ?symbol=FXE&years=12
      const sym = (url.searchParams.get('symbol') || '').toUpperCase().replace(/[^A-Z0-9$./]/g, '');
      const years = Math.min(20, Math.max(1, parseInt(url.searchParams.get('years') || '10', 10)));
      if (!sym) return new Response('{"error":"symbol required"}',
        { status: 400, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
      try {
        const tk = await getAccessToken(env);
        // ?minuteDay=YYYY-MM-DD probes 1-min depth: returns that day's 1-min count
        const md = url.searchParams.get('minuteDay');
        if (md && /^\d{4}-\d{2}-\d{2}$/.test(md)) {
          const s = Date.parse(`${md}T08:00:00Z`), e = Date.parse(`${md}T23:00:00Z`);
          const dd = await fetchSchwabJSON(
            `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=${encodeURIComponent(sym)}&periodType=day&frequencyType=minute&frequency=1&startDate=${s}&endDate=${e}&needExtendedHoursData=false`,
            tk, env);
          const cc = (dd.candles || []).filter(c => c.close > 0);
          const bars = cc.map(c => { const d = toET(new Date(c.datetime)); return { t: `${String(d.getHours()).padStart(2,'0')}:${String(d.getMinutes()).padStart(2,'0')}`, o: c.open, c: c.close }; });
          const at = hhmm => bars.find(b => b.t === hhmm) || null;
          return new Response(JSON.stringify({ symbol: sym, minuteDay: md, n: cc.length,
            open0930: at('09:30'), close1559: at('15:59'), close1600: at('16:00'), close1615: at('16:15'), last: bars[bars.length - 1] ?? null }),
            { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
        }
        const end = Date.now(), start = end - years * 365.25 * 86400000;
        const data = await fetchSchwabJSON(
          `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=${encodeURIComponent(sym)}&periodType=year&frequencyType=daily&startDate=${Math.round(start)}&endDate=${end}&needExtendedHoursData=false`,
          tk, env);
        const ohlc = url.searchParams.get('ohlc') === '1';
        const candles = (data.candles || []).filter(c => c.close > 0)
          .map(c => ohlc
            ? [isoDateET(toET(new Date(c.datetime))), c.open, c.high, c.low, c.close]
            : [isoDateET(toET(new Date(c.datetime))), c.close]);
        return new Response(JSON.stringify({ symbol: sym, n: candles.length, candles }),
          { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message }),
          { status: 500, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
      }
    }

    if (url.pathname === '/cot-refresh-now' && request.method === 'GET') {
      // Manual trigger for the weekly COT self-feed (idempotent).
      let result;
      try { result = await cotWeeklyRefresh(env); }
      catch (e) { result = { error: e.message }; }
      return new Response(JSON.stringify(result, null, 2),
        { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
    }

    if (url.pathname === '/subscribers') {
      // Manage signal-DM recipients. Gated by the sync secret. CORS-open for
      // the dashboard (which sends the secret in the body / query).
      const sCors = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET, POST, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type, X-Sync-Secret' };
      if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: sCors });
      const body = request.method === 'POST' ? await request.json().catch(() => ({})) : {};
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret') || body.secret;
      if (!secret || secret !== env.SYNC_SECRET) return jsonResp({ error: 'Unauthorized' }, 401, sCors);

      let subs = await getSubscribers(env);
      if (request.method === 'GET') return jsonResp({ subscribers: subs }, 200, sCors);

      const act = body.action;
      if (act === 'add') {
        const id = String(body.id || '').trim();
        if (!/^\d{15,21}$/.test(id)) return jsonResp({ error: 'id must be a 15-21 digit Discord user ID' }, 400, sCors);
        if (!subs.find(s => s.id === id)) subs.push({ id, label: (body.label || '').slice(0, 40), paused: false });
        await env.SIGNAL_KV.put('signal_subscribers', JSON.stringify(subs));
        return jsonResp({ ok: true, subscribers: subs }, 200, sCors);
      }
      if (act === 'remove') {
        subs = subs.filter(s => s.id !== String(body.id));
        await env.SIGNAL_KV.put('signal_subscribers', JSON.stringify(subs));
        return jsonResp({ ok: true, subscribers: subs }, 200, sCors);
      }
      if (act === 'pause') {
        subs = subs.map(s => s.id === String(body.id) ? { ...s, paused: !s.paused } : s);
        await env.SIGNAL_KV.put('signal_subscribers', JSON.stringify(subs));
        return jsonResp({ ok: true, subscribers: subs }, 200, sCors);
      }
      if (act === 'test') {
        // DM one recipient a test, surfacing Discord's exact error (e.g. 403
        // = not in a mutual server / DMs closed).
        const r = await sendDiscordDM(env, String(body.id),
          '🔔 Test from Σ3 Signals — you are set to receive trade signals here. (If you got this, delivery works.)');
        return jsonResp({ ok: !!r.ok, status: r.status, error: r.error,
          hint: !r.ok && r.status === 403 ? 'This user must share a server with the bot AND allow DMs from server members.' : undefined }, 200, sCors);
      }
      return jsonResp({ error: 'unknown action' }, 400, sCors);
    }

    if (url.pathname === '/bot-info' && request.method === 'GET') {
      // Diagnostic: which servers (guilds) the bot is in + its identity.
      // The bot can DM anyone who shares one of these guilds with it.
      if (!env.DISCORD_TOKEN) return jsonResp({ error: 'no DISCORD_TOKEN (bot uses a proxy path)' }, 200, corsHeaders);
      try {
        const meR = await fetch('https://discord.com/api/v10/users/@me', { headers: { Authorization: `Bot ${env.DISCORD_TOKEN}` } });
        const me = await meR.json();
        const gR = await fetch('https://discord.com/api/v10/users/@me/guilds', { headers: { Authorization: `Bot ${env.DISCORD_TOKEN}` } });
        const guilds = await gR.json();
        return jsonResp({ bot: `${me.username}#${me.discriminator} (${me.id})`,
          guilds: Array.isArray(guilds) ? guilds.map(g => ({ name: g.name, id: g.id })) : guilds }, 200, corsHeaders);
      } catch (e) { return jsonResp({ error: e.message }, 500, corsHeaders); }
    }

    if (url.pathname === '/cyclicality-today' && request.method === 'GET') {
      // CycleLab live actual — today's session-so-far (KV, written by the
      // cron every 5 min during RTH). Pure KV read: zero Schwab calls.
      const raw = await env.SIGNAL_KV.get(url.searchParams.get('symbol') === 'ndx' ? 'cyc_today_ndx' : 'cyc_today');
      return new Response(raw || 'null',
        { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
    }

    if (url.pathname === '/watchdog-now' && request.method === 'GET') {
      let result;
      try { result = await dataCompletenessCheck(env, toET(new Date())); }
      catch (e) { result = { error: e.message }; }
      return new Response(JSON.stringify(result, null, 2),
        { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
    }

    // GET /remirror-history — Repair: force-push current KV history → GitHub.
    // KV is the source of truth; this re-mirrors it to the GitHub copy the
    // dashboard reads, healing any silent mirror drift (e.g. a settle whose
    // KV→GitHub PUT lost a sha race, as 2026-06-23 tailPL did). Read-only on
    // KV — never the reverse. Auth: SYNC_SECRET via the guard above.
    if (url.pathname === '/remirror-history' && request.method === 'GET') {
      let result;
      try {
        const content = await getHistory(env);
        const r = await mirrorHistoryToGitHub(env, content, 'manual: re-mirror KV→GitHub (repair drift)');
        result = { ok: r.ok, entries: Array.isArray(content) ? content.length : null, mirror: r };
      } catch (e) { result = { ok: false, error: e.message }; }
      return new Response(JSON.stringify(result, null, 2),
        { status: result.ok ? 200 : 502, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
    }

    if (url.pathname === '/weekly-digest-now' && request.method === 'GET') {
      let result;
      try { result = await weeklyDigest(env); }
      catch (e) { result = { error: e.message }; }
      return new Response(JSON.stringify(result, null, 2),
        { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
    }

    if (url.pathname === '/vix-decomp-now' && request.method === 'GET') {
      // Manual trigger for the daily vol-flow decomposition (idempotent).
      // ?date=YYYY-MM-DD back-heals a missed day from the KV surface snap.
      let etNowV = toET(new Date());
      const ovrV = url.searchParams.get('date');
      if (ovrV && /^\d{4}-\d{2}-\d{2}$/.test(ovrV)) etNowV = toET(new Date(`${ovrV}T16:30:00-04:00`));
      let result;
      try { result = await computeVixDecompDaily(env, etNowV); }
      catch (e) { result = { error: e.message }; }
      let line = null;
      try { line = await computeVolFlowLine(env, etNowV); } catch (_) {}
      return new Response(JSON.stringify({ date: isoDateET(etNowV), result, advisoryLine: line }, null, 2),
        { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
    }

    if (url.pathname === '/debug-morning-dual' && request.method === 'GET') {
      const cors = { 'Access-Control-Allow-Origin': '*' };
      const forceNoSchwab = url.searchParams.get('force_no_schwab') === '1';
      const forceNoTasty  = url.searchParams.get('force_no_tasty')  === '1';
      const forceNoToken  = url.searchParams.get('force_no_token')  === '1';
      const out = { ok: true, simulated: { forceNoSchwab, forceNoTasty, forceNoToken }, errors: {} };
      try {
        // etNow needed by the freshness check below — declare BEFORE Schwab fetch.
        const etNow = toET(new Date());
        const todayISO = isoDateET(etNow);
        // 1. Pull VIX + SPX from BOTH sources independently (single-fetch).
        let token = null;
        if (!forceNoToken) {
          try { token = await getAccessToken(env); }
          catch (e) { out.errors.schwabToken = e.message; }
        } else {
          out.errors.schwabToken = '(forced)';
        }

        let vixSchwab = null, spxSchwab = null, vixTasty = null, spxTasty = null;
        let vixSchwabTs = null, spxSchwabTs = null, vixTastyTs = null, spxTastyTs = null;
        let vixSchwabFresh = null, spxSchwabFresh = null, vixTastyFresh = null, spxTastyFresh = null;

        // Same freshness gate the morning cron uses: today's date + >= 9:30 ET.
        function _isFresh(ts) {
          if (ts == null) return false;
          const d = (typeof ts === 'number') ? new Date(ts) : new Date(String(ts));
          if (!isFinite(d.getTime())) return false;
          const et = toET(d);
          if (et.toDateString() !== etNow.toDateString()) return false;
          return (et.getHours() * 60 + et.getMinutes()) >= 570;
        }

        // Schwab VIX + SPX (parallel, single fetch each) — captures ts + freshness
        if (token && !forceNoSchwab) {
          const [vR, sR] = await Promise.allSettled([
            fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/quotes?symbols=%24VIX&fields=quote`, token, env),
            fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/quotes?symbols=%24SPX&fields=quote`, token, env),
          ]);
          if (vR.status === 'fulfilled') {
            const q = vR.value?.['$VIX']?.quote;
            if (q?.lastPrice != null) {
              vixSchwab = parseFloat(parseFloat(q.lastPrice).toFixed(2));
              vixSchwabTs = q.tradeTime;
              vixSchwabFresh = _isFresh(q.tradeTime);
            }
          } else { out.errors.schwabVix = vR.reason?.message || String(vR.reason); }
          if (sR.status === 'fulfilled') {
            const q = sR.value?.['$SPX']?.quote;
            // morning cron uses openPrice only (rejects lastPrice — that's yesterday's close pre-market)
            if (q?.openPrice != null && q.openPrice > 0) {
              spxSchwab = parseFloat(parseFloat(q.openPrice).toFixed(2));
              spxSchwabTs = q.tradeTime;
              spxSchwabFresh = _isFresh(q.tradeTime);
            }
          } else { out.errors.schwabSpx = sR.reason?.message || String(sR.reason); }
        } else if (forceNoSchwab) {
          out.errors.schwabVix = '(forced)'; out.errors.schwabSpx = '(forced)';
        }

        // Tasty VIX + SPX (parallel)
        if (!forceNoTasty) {
          const [vR, sR] = await Promise.allSettled([tastyGetVix(env), tastyGetSpx(env)]);
          if (vR.status === 'fulfilled') {
            vixTasty = parseFloat(vR.value.price.toFixed(2));
            vixTastyTs = vR.value.raw?.['updated-at'] || vR.value.asOf;
            vixTastyFresh = _isFresh(vixTastyTs);
          } else out.errors.tastyVix = vR.reason?.message || String(vR.reason);
          if (sR.status === 'fulfilled') {
            const v = sR.value.open ?? sR.value.last ?? sR.value.price;
            if (v != null) {
              spxTasty = parseFloat(v.toFixed(2));
              spxTastyTs = sR.value.asOf || sR.value.raw?.['updated-at'];
              spxTastyFresh = _isFresh(spxTastyTs);
            }
          } else { out.errors.tastySpx = sR.reason?.message || String(sR.reason); }
        } else {
          out.errors.tastyVix = '(forced)'; out.errors.tastySpx = '(forced)';
        }

        out.values = { vixSchwab, spxSchwab, vixTasty, spxTasty };
        out.timestamps = { vixSchwabTs, spxSchwabTs, vixTastyTs, spxTastyTs };
        out.freshness = { vixSchwabFresh, spxSchwabFresh, vixTastyFresh, spxTastyFresh };
        // Apply the freshness gate to determine which messages WOULD post in production.
        // If VIX isn't fresh, the morning cron skips that source's message.
        if (vixSchwabFresh === false) vixSchwab = null;
        if (vixTastyFresh === false) vixTasty = null;
        if (spxSchwabFresh === false) spxSchwab = null;
        if (spxTastyFresh === false) spxTasty = null;

        // 2. Read history for prevWR / vixPct20d / rsi14 (mirrors morning cron)
        // (etNow + todayISO already declared above for the freshness check)
        let prevWR = null, vixPct20d = null, rsi14 = null, vixYOpen = null, vixYClose = null, spxYClose = null;
        try {
          const histData = await getHistory(env);
          if (Array.isArray(histData) && histData.length) {
            const sorted = histData.filter(r => r.date < todayISO && r.m8bfWR != null)
              .sort((a, b) => b.date.localeCompare(a.date));
            if (sorted.length) prevWR = parseFloat(sorted[0].m8bfWR);

            const vix20 = histData.filter(r => r.date < todayISO && r.vixClose != null && r.vixClose > 0)
              .sort((a, b) => a.date.localeCompare(b.date)).slice(-20).map(r => parseFloat(r.vixClose));
            const refVix = vixTasty ?? vixSchwab;
            // CANONICAL — single source: signal-engine.js computeVixPct20d.
            vixPct20d = computeVixPct20d(refVix, vix20).pct;

            const closes30 = histData.filter(r => r.date < todayISO && r.spxClose != null && r.spxClose > 0)
              .sort((a, b) => a.date.localeCompare(b.date)).slice(-30).map(r => parseFloat(r.spxClose));
            if (closes30.length >= 15) rsi14 = computeRSI14(closes30);

            // Prior trading day's vix open/close + spx close — used by calculateSignal
            const prior = histData.filter(r => r.date < todayISO && r.vixClose != null)
              .sort((a, b) => b.date.localeCompare(a.date))[0];
            if (prior) {
              vixYOpen = prior.vixOpen != null ? parseFloat(prior.vixOpen) : null;
              vixYClose = parseFloat(prior.vixClose);
              spxYClose = prior.spxClose != null ? parseFloat(prior.spxClose) : null;
            }
          }
        } catch (e) { out.errors.history = e.message; }
        out.context = { prevWR, vixPct20d, rsi14, vixYOpen, vixYClose, spxYClose };

        // 3. Compute signal twice (once per source) — same calculateSignal call
        const cor1mOpenTrade = await getCor1mOpenToday(env, isoDateET(etNow));
        function makeSignal(vixVal, spxOpen) {
          if (vixVal == null) return null;
          const gap = (spxYClose && spxOpen) ? ((spxOpen - spxYClose) / spxYClose) * 100 : null;
          return calculateSignal({
            vixToday: vixVal, vixYOpen, vixYClose,
            spxGapPct: gap,
            etDate: etNow, prevWR, vixPct20d, rsi14,
            cor1m: cor1mOpenTrade,
          });
        }
        const sigSchwab = makeSignal(vixSchwab, spxSchwab);
        const sigTasty  = makeSignal(vixTasty,  spxTasty);
        // Tail Hedge today (fetched once, cached). Shared between both renders.
        const tailLine = await getTailHedgeStatusLine(env);
        // Advisory lines — keep the preview identical to the real morning message
        let tiltL = null, gexL = null, cycL = null, volL = null, wrL = null;
        try { tiltL = await computeTiltLine(env, isoDateET(etNow)); } catch (_) {}
        try { gexL = await computeGexLine(env); } catch (_) {}
        try { cycL = await computeCycleLine(env, etNow); } catch (_) {}
        try { volL = await computeVolFlowLine(env, etNow); } catch (_) {}
        try { wrL = await computeM8bfWrLine(env, etNow); } catch (_) {}
        for (const s of [sigSchwab, sigTasty]) if (s) { s._tiltLine = tiltL; s._gexLine = gexL; s._cycleLine = cycL; s._volFlowLine = volL; s._m8bfWrLine = wrL; }

        function renderMsg(sig, vixVal, source) {
          if (!sig) return null;
          const banner = source === 'schwab' ? '📡 **SCHWAB DATA**\n\n' : '📡 **TASTYTRADE DATA**\n\n';
          return (banner + buildDiscordMessage(sig, { yOpen: vixYOpen, yClose: vixYClose, todayOpen: vixVal }, tailLine)).slice(0, 2000);
        }
        out.schwab = sigSchwab ? {
          rec: sigSchwab.rec, theme: sigSchwab.theme, badge: sigSchwab.badge,
          message: renderMsg(sigSchwab, vixSchwab, 'schwab'),
        } : null;
        out.tasty = sigTasty ? {
          rec: sigTasty.rec, theme: sigTasty.theme, badge: sigTasty.badge,
          message: renderMsg(sigTasty, vixTasty, 'tastytrade'),
        } : null;

        // Summary signals
        out.summary = {
          schwabMessageBuilt: !!out.schwab,
          tastyMessageBuilt: !!out.tasty,
          recsAgree: sigSchwab && sigTasty ? sigSchwab.rec === sigTasty.rec : null,
          vixDelta: (vixSchwab != null && vixTasty != null) ? +(vixSchwab - vixTasty).toFixed(2) : null,
          spxDelta: (spxSchwab != null && spxTasty != null) ? +(spxSchwab - spxTasty).toFixed(2) : null,
        };

        return jsonResp(out, 200, cors);
      } catch (e) {
        return jsonResp({ ok: false, error: e.message, stack: e.stack?.slice(0, 400) }, 500, cors);
      }
    }

    // GET /tasty-chain-probe?root=SPXW[&exp=YYYY-MM-DD]
    // Diagnostic ONLY. Hits Tasty's option-chains endpoints and a sample
    // market-data quote so we can see the response shape before building a
    // wrapper. Read-only, additive — no impact on live signal/trading.
    if (url.pathname === '/tasty-chain-probe' && request.method === 'GET') {
      const cors = { 'Access-Control-Allow-Origin': '*' };
      try {
        const root = url.searchParams.get('root') || 'SPXW';
        const expFilter = url.searchParams.get('exp');  // YYYY-MM-DD optional
        const token = await getTastyAccessToken(env);
        const hdr = tastyHeaders({ 'Authorization': `Bearer ${token}` });
        const out = { root, expFilter, ts: new Date().toISOString() };

        // 1. NESTED chain (structured by expiration → strike → call/put)
        const r1 = await fetch(`${TASTY_BASE}/option-chains/${encodeURIComponent(root)}/nested`, { headers: hdr });
        const t1 = await r1.text();
        let j1 = null;
        try { j1 = JSON.parse(t1); } catch (_) {}
        out.nested = { status: r1.status, ok: r1.ok };
        if (j1) {
          const d = j1?.data || j1;
          const items = d?.items || [];
          out.nested.topKeys = Object.keys(d || {});
          out.nested.itemCount = items.length;
          // Show the structure of the first item + first expiration deep
          if (items[0]) {
            out.nested.firstItemKeys = Object.keys(items[0]);
            const exps = items[0]?.expirations || items[0]?.['expirations'] || [];
            out.nested.firstItemExpCount = exps.length;
            if (exps[0]) {
              out.nested.firstExpKeys = Object.keys(exps[0]);
              const strikes = exps[0]?.strikes || [];
              out.nested.firstExpStrikeCount = strikes.length;
              if (strikes[0]) {
                out.nested.firstStrikeKeys = Object.keys(strikes[0]);
                out.nested.firstStrikeSample = strikes[0];
              }
            }
          }
        } else {
          out.nested.bodyHead = t1.slice(0, 400);
        }

        // 2. COMPACT chain (flat list of option symbols)
        const r2 = await fetch(`${TASTY_BASE}/option-chains/${encodeURIComponent(root)}/compact`, { headers: hdr });
        const t2 = await r2.text();
        let j2 = null;
        try { j2 = JSON.parse(t2); } catch (_) {}
        out.compact = { status: r2.status, ok: r2.ok };
        if (j2) {
          const d = j2?.data || j2;
          const items = d?.items || [];
          out.compact.topKeys = Object.keys(d || {});
          out.compact.itemCount = items.length;
          if (items[0]) {
            out.compact.firstItemKeys = Object.keys(items[0]);
            const syms = items[0]?.['option-chains'] || items[0]?.symbols || items[0]?.['streamer-symbols'] || [];
            out.compact.symbolFieldFound = Object.keys(items[0]).filter(k => Array.isArray(items[0][k])).slice(0, 5);
            out.compact.firstFewSymbols = (Array.isArray(syms) ? syms : []).slice(0, 6);
            out.compact.firstItemSample = items[0];  // tiny; just the top wrapper
          }
        } else {
          out.compact.bodyHead = t2.slice(0, 400);
        }

        // 3. Pick a sample option symbol and probe market-data endpoints
        let sampleSym = null;
        try {
          const compactItems = j2?.data?.items || j2?.items || [];
          for (const it of compactItems) {
            for (const k of Object.keys(it)) {
              if (Array.isArray(it[k]) && it[k].length) {
                const candidate = it[k].find(s => typeof s === 'string') || it[k][0];
                if (typeof candidate === 'string') { sampleSym = candidate; break; }
              }
            }
            if (sampleSym) break;
          }
        } catch (_) {}
        out.sampleSym = sampleSym;

        if (sampleSym) {
          // Try /market-data/{Index|Equity Option|Future Option}/{symbol} variants
          for (const instr of ['Equity Option', 'EquityOption', 'Option']) {
            const path = `${TASTY_BASE}/market-data/${encodeURIComponent(instr)}/${encodeURIComponent(sampleSym)}`;
            const r = await fetch(path, { headers: hdr });
            const t = await r.text();
            let j = null; try { j = JSON.parse(t); } catch (_) {}
            out[`mdSingle_${instr.replace(/\s/g,'_')}`] = {
              path, status: r.status, ok: r.ok,
              topKeys: j ? Object.keys(j) : null,
              data: j?.data || null,
              bodyHead: !j ? t.slice(0, 300) : undefined,
            };
            if (r.ok) break;  // first one that works is enough
          }
          // Try batch quote endpoints with several URL/symbol-format variants.
          // We need ONE batch call that returns quotes for many option symbols.
          // Collect 4 sample syms so we can validate batching, not just single.
          const syms = [];
          try {
            const items = j2?.data?.items || [];
            for (const it of items) {
              for (const arr of [it['symbols'], it['streamer-symbols']]) {
                if (Array.isArray(arr)) syms.push(...arr.slice(0, 2));
              }
              if (syms.length >= 4) break;
            }
          } catch (_) {}
          const sym1 = syms[0] || sampleSym;
          const sym2 = syms[1] || sampleSym;
          const sym1NoDouble = sym1?.replace(/\s+/g, ' ');
          const sym1Strip = sym1?.replace(/\s+/g, '');
          // Streamer-symbol form (.SPXW260520C2800) from nested first strike
          const streamerSym = j1?.data?.items?.[0]?.expirations?.[0]?.strikes?.[0]?.['call-streamer-symbol'];
          const variants = [
            { tag: 'by-type_single_double-space',  url: `${TASTY_BASE}/market-data/by-type?option=${encodeURIComponent(sym1)}` },
            { tag: 'by-type_single_single-space',  url: `${TASTY_BASE}/market-data/by-type?option=${encodeURIComponent(sym1NoDouble)}` },
            { tag: 'by-type_single_no-space',      url: `${TASTY_BASE}/market-data/by-type?option=${encodeURIComponent(sym1Strip)}` },
            { tag: 'by-type_two_comma',            url: `${TASTY_BASE}/market-data/by-type?option=${encodeURIComponent(sym1 + ',' + sym2)}` },
            { tag: 'by-type_two_repeated',         url: `${TASTY_BASE}/market-data/by-type?option=${encodeURIComponent(sym1)}&option=${encodeURIComponent(sym2)}` },
            { tag: 'by-type_streamer',             url: streamerSym ? `${TASTY_BASE}/market-data/by-type?option=${encodeURIComponent(streamerSym)}` : null },
            { tag: 'md_root_symbols',              url: `${TASTY_BASE}/market-data?symbols=${encodeURIComponent(sym1)}` },
            { tag: 'md_root_symbols_comma2',       url: `${TASTY_BASE}/market-data?symbols=${encodeURIComponent(sym1 + ',' + sym2)}` },
            { tag: 'md_root_symbols_comma4',       url: `${TASTY_BASE}/market-data?symbols=${encodeURIComponent(syms.slice(0,4).join(','))}` },
            { tag: 'md_root_symbols_repeated',     url: `${TASTY_BASE}/market-data?symbols=${encodeURIComponent(sym1)}&symbols=${encodeURIComponent(sym2)}` },
            { tag: 'md_root_options',              url: `${TASTY_BASE}/market-data?options[]=${encodeURIComponent(sym1)}&options[]=${encodeURIComponent(sym2)}` },
            { tag: 'chain_quotes',                 url: `${TASTY_BASE}/option-chains/${encodeURIComponent(root)}/quotes` },
            { tag: 'chain_quotes_with_filter',     url: expFilter ? `${TASTY_BASE}/option-chains/${encodeURIComponent(root)}/quotes?expiration-date=${expFilter}` : null },
          ];
          out.batchProbe = {};
          for (const v of variants) {
            if (!v.url) { out.batchProbe[v.tag] = { skipped: true }; continue; }
            const r = await fetch(v.url, { headers: hdr });
            const t = await r.text();
            let j = null; try { j = JSON.parse(t); } catch (_) {}
            const items = j?.data?.items;
            out.batchProbe[v.tag] = {
              url: v.url,
              status: r.status, ok: r.ok,
              hasItems: Array.isArray(items),
              itemCount: Array.isArray(items) ? items.length : null,
              firstItemKeys: Array.isArray(items) && items[0] ? Object.keys(items[0]).slice(0, 12) : null,
              bodyHead: !j ? t.slice(0, 240) : (Array.isArray(items) && items.length === 0 ? t.slice(0, 240) : undefined),
            };
          }
          out.batchProbe._sampleSyms = { sym1, sym2, sym1NoDouble, sym1Strip, streamerSym };
        }

        return jsonResp({ ok: true, probe: out }, 200, cors);
      } catch (e) {
        return jsonResp({ ok: false, error: e.message, stack: e.stack?.slice(0, 400) }, 500, cors);
      }
    }

    if (url.pathname === '/health' && request.method === 'GET') {
      const publicCors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '86400',
      };
      // ?refresh=now triggers a force refresh against Schwab — useful for
      // recovering after a re-auth without waiting for natural token expiry.
      if (url.searchParams.get('refresh') === 'now') {
        try {
          const token = await getAccessToken(env, true);
          return jsonResp({ ok: true, refreshed: true, accessLen: token.length, accessTail: token.slice(-8) }, 200, publicCors);
        } catch (e) {
          return jsonResp({ ok: false, refreshed: false, error: e.message }, 500, publicCors);
        }
      }
      // ?run=now manually invokes the scheduled handler — recovery path when
      // Cloudflare crons stall (observed 2026-05-01: last cron 16h ago).
      // Public to avoid lock-out when the SYNC_SECRET is in the dashboard
      // but the dashboard's own crons are also down.
      if (url.searchParams.get('run') === 'now') {
        try {
          const result = await handleScheduled(env);
          result.date = result.date || new Date().toISOString();
          await env.SIGNAL_KV.put('last_run', JSON.stringify(result));
          return jsonResp({ ok: true, ran: true, result }, 200, publicCors);
        } catch (e) {
          return jsonResp({ ok: false, ran: false, error: e.message }, 500, publicCors);
        }
      }
      try {
        const now = Date.now();
        const etNow = toET(new Date(now));
        const etH = etNow.getHours();
        const etM = etNow.getMinutes();
        const dow = etNow.getDay();
        const todayISO = `${etNow.getFullYear()}-${String(etNow.getMonth()+1).padStart(2,'0')}-${String(etNow.getDate()).padStart(2,'0')}`;
        const isWeekday = dow >= 1 && dow <= 5;
        const postOpen = etH > 9 || (etH === 9 && etM >= 32);  // +2 min grace
        const postClose = etH > 16 || (etH === 16 && etM >= 16);
        const inMarketHours = isWeekday && ((etH === 9 && etM >= 30) || (etH >= 10 && etH < 16));

        // Fetch all KV keys in parallel
        const [morningRaw, eodRaw, schwabRaw, lastRunRaw, gexRaw, mirrorRaw] = await Promise.all([
          env.SIGNAL_KV.get(`morning_signal_${todayISO}`),
          env.SIGNAL_KV.get(`eod_done_${todayISO}`),
          env.SIGNAL_KV.get('schwab_refresh_state'),
          env.SIGNAL_KV.get('last_run'),
          env.SIGNAL_KV.get('gex_current'),
          env.SIGNAL_KV.get('history_mirror_state'),
        ]);

        const alerts = [];

        // ── Morning signal state ──
        let morningState = null;
        let morningClaimAgeS = null;
        let morningStuck = false;
        if (morningRaw === 'sent') {
          morningState = 'sent';
        } else if (morningRaw && morningRaw.startsWith('claim:')) {
          morningState = 'claim';
          const parts = morningRaw.split(':');
          const ts = parseInt(parts[2] || '0', 10);
          if (ts) {
            morningClaimAgeS = Math.round((now - ts) / 1000);
            if (morningClaimAgeS > 120) morningStuck = true;
          }
        }
        if (isWeekday && postOpen && !postClose && morningState !== 'sent') {
          alerts.push(morningStuck ? 'morning_signal_stuck' : 'morning_signal_not_sent');
        }

        // ── EOD state ──
        if (isWeekday && postClose && eodRaw !== 'done') {
          alerts.push('eod_not_done');
        }

        // ── Schwab refresh ──
        const schwab = schwabRaw ? JSON.parse(schwabRaw) : null;
        const schwabSuccessMs = schwab?.lastSuccess || 0;
        const schwabMinutesSinceSuccess = schwabSuccessMs ? Math.round((now - schwabSuccessMs) / 60000) : null;
        // Refresh-token countdown (7-day Schwab policy) — dashboard banner fuel
        let refreshDaysLeft = null;
        try {
          const tkRaw2 = await env.SIGNAL_KV.get('schwab_tokens');
          const exp2 = tkRaw2 ? JSON.parse(tkRaw2).refreshExpiry : null;
          if (exp2) refreshDaysLeft = Math.round((exp2 - now) / 8640000) / 10;   // 0.1d precision
        } catch (_) {}
        if (refreshDaysLeft != null && refreshDaysLeft <= 1.5) alerts.push('schwab_token_expiring');
        if (schwab && schwab.ok === false && (schwab.consecutiveErrors || 0) >= 3) {
          alerts.push('schwab_refresh_degraded');
        }

        // ── GitHub mirror (2026-06-09 — expired-PAT class of failure) ──
        // Mirror writes happen only a few times/day, so even 2 consecutive
        // failures means KV has been drifting ahead of GitHub for hours.
        const mirror = mirrorRaw ? JSON.parse(mirrorRaw) : null;
        const mirrorSuccessMs = mirror?.lastSuccess || 0;
        const mirrorMinutesSinceSuccess = mirrorSuccessMs ? Math.round((now - mirrorSuccessMs) / 60000) : null;
        if (mirror && mirror.ok === false && (mirror.consecutiveErrors || 0) >= 2) {
          alerts.push('history_mirror_degraded');
        }

        // ── Open risk exposure (2026-06-09 — daily total-risk cap) ──
        let openRisk = null;
        try {
          const cfgRaw2 = await env.SIGNAL_KV.get('risk_config');
          const cfg2 = { ...RISK_CAP_DEFAULTS, ...(cfgRaw2 ? JSON.parse(cfgRaw2) : {}) };
          const exp = await computeOpenRiskExposureUsd(env, todayISO);
          openRisk = { ...exp, capUsd: cfg2.maxOpenRiskUsd, enabled: cfg2.enabled, mode: cfg2.mode || 'warn' };
          if (cfg2.enabled && exp.totalUsd > cfg2.maxOpenRiskUsd) {
            alerts.push('open_risk_over_cap');
          }
        } catch (_) { /* informational only */ }

        // ── COR1M cloud capture (2026-06-09 — machine-independence) ──
        let cor1mCloud = null;
        try {
          const c = await env.SIGNAL_KV.get(`cor1m_open_${todayISO}`);
          if (c) cor1mCloud = JSON.parse(c);
          // Capture missing after 10:05 ET on a weekday = capture path broken.
          // (Start was 9:40, but $COR1M's first print can arrive 9:35–9:40 and
          // the freshness-gated capture window now runs to 10:00.)
          if (!cor1mCloud && isWeekday && (etH > 10 || (etH === 10 && etM >= 5)) && etH < 16) {
            alerts.push('cor1m_capture_missing');
          }
        } catch (_) {}

        // ── Cron liveness via last_run ──
        const lastRun = lastRunRaw ? JSON.parse(lastRunRaw) : null;
        const lastRunMs = lastRun?.date ? Date.parse(lastRun.date) : 0;
        const lastRunAgeS = lastRunMs ? Math.round((now - lastRunMs) / 1000) : null;
        // Cron fires every 2 min during market hours — 10 min without a run = stall
        if (inMarketHours && (lastRunAgeS === null || lastRunAgeS > 600)) {
          alerts.push('cron_stalled');
        }

        // ── GEX freshness ──
        // gexData.updatedAt is written as an ISO string by handleGEXUpdate,
        // so parse it to ms before doing clock math.
        let gexUpdatedAtIso = null;
        let gexAgeS = null;
        try {
          if (gexRaw) {
            const g = JSON.parse(gexRaw);
            gexUpdatedAtIso = g.updatedAt || null;
            const ms = gexUpdatedAtIso ? (
              typeof gexUpdatedAtIso === 'number' ? gexUpdatedAtIso : Date.parse(gexUpdatedAtIso)
            ) : 0;
            if (ms && !Number.isNaN(ms)) gexAgeS = Math.round((now - ms) / 1000);
          }
        } catch { /* ignore malformed JSON */ }
        if (inMarketHours && (gexAgeS === null || gexAgeS > 600)) {
          alerts.push('gex_stale');
        }

        const alarming = alerts.length > 0;
        return jsonResp({
          now,
          todayISO,
          etTime: `${etH}:${String(etM).padStart(2,'0')} ET`,
          isWeekday,
          inMarketHours,
          morning_signal: {
            state: morningState,
            raw: morningRaw,
            claim_age_s: morningClaimAgeS,
            stuck: morningStuck,
          },
          eod_done: eodRaw === 'done',
          schwab_refresh: schwab ? {
            ok: schwab.ok,
            consecutiveErrors: schwab.consecutiveErrors || 0,
            minutesSinceSuccess: schwabMinutesSinceSuccess,
            refreshDaysLeft,
            msg: schwab.msg,
          } : { state: 'never_recorded', refreshDaysLeft },
          history_mirror: mirror ? {
            ok: mirror.ok,
            consecutiveErrors: mirror.consecutiveErrors || 0,
            minutesSinceSuccess: mirrorMinutesSinceSuccess,
            msg: mirror.msg,
          } : { state: 'never_recorded' },
          open_risk: openRisk,
          cor1m_cloud: cor1mCloud || { state: 'not_captured_today' },
          last_run: lastRun ? {
            status: lastRun.status,
            date: lastRun.date,
            age_s: lastRunAgeS,
          } : null,
          gex: {
            available: !!gexRaw,
            updatedAt: gexUpdatedAtIso,
            age_s: gexAgeS,
          },
          heartbeat: await (async () => {
            const hb = await env.SIGNAL_KV.get('last_heartbeat');
            if (!hb) return { configured: false };
            const p = JSON.parse(hb);
            const ageS = Math.round((Date.now() - new Date(p.at).getTime()) / 1000);
            return { configured: true, lastAt: p.at, age_s: ageS, status: p.status, ranMs: p.ranMs };
          })(),
          alarming,
          alerts,
        }, 200, publicCors);
      } catch (e) {
        return jsonResp({ error: e.message, alarming: true, alerts: ['health_endpoint_threw'] }, 500, publicCors);
      }
    }

    // ── GET /gex ── Public endpoint, returns current GEX data from KV
    // Auto-refreshes if data is stale (>3 min) during market hours (cron is unreliable on free tier)
    if (url.pathname === '/gex' && request.method === 'GET') {
      const publicCors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '86400',
      };
      try {
        // ── One-shot rescrape trigger (set via KV key 'rescrape_trigger') ──
        const rescrapeDate = await env.SIGNAL_KV.get('rescrape_trigger');
        if (rescrapeDate) {
          try {
            await env.SIGNAL_KV.delete('rescrape_trigger');
            const allSigs = await fetchAllDiscordSignalsForDate(env.DISCORD_USER_TOKEN, '1048242197029458040', rescrapeDate);
            const signals = allSigs.map(s => ({
              time: s.time, center: s.center, lower: s.lower, upper: s.upper,
              t1: s.t1, premium: s.premium, cp: s.cp ?? 0, banned: isBanned(s.center, s.lower, s.t1),
            }));
            await env.SIGNAL_KV.put('signals_today', JSON.stringify({ date: rescrapeDate, signals }));
            console.log(`[gex] Rescrape complete: ${signals.length} signals for ${rescrapeDate}`);
          } catch (e) { console.warn('[gex] rescrape failed:', e.message); }
        }

        let data = await env.SIGNAL_KV.get('gex_current');

        // Auto-refresh: if no data or stale during market hours, trigger inline update
        const etNow = toET();
        const etH = etNow.getHours(), etM = etNow.getMinutes(), dow = etNow.getDay();
        const marketOpen = dow >= 1 && dow <= 5 && (etH > 9 || (etH === 9 && etM >= 30)) && etH < 16;
        if (marketOpen) {
          let needsRefresh = !data;
          if (data && !needsRefresh) {
            const parsed = JSON.parse(data);
            const age = Date.now() / 1000 - (parsed.timestamp || 0);
            if (age > 180) needsRefresh = true; // stale if >3 min old
          }
          if (needsRefresh) {
            // Cooldown: only one auto-refresh per 2 min to stay within KV write limits
            const lastRefresh = await env.SIGNAL_KV.get('gex_last_refresh_ts');
            const sinceRefresh = lastRefresh ? Date.now() - parseInt(lastRefresh) : Infinity;
            if (sinceRefresh > 120_000) {
              try {
                await env.SIGNAL_KV.put('gex_last_refresh_ts', String(Date.now()));
                const token = await getAccessToken(env);
                await handleGEXUpdate(env, token);
                data = await env.SIGNAL_KV.get('gex_current');
              } catch (e) {
                console.warn('[gex] inline refresh failed:', e.message || e);
              }
            }
          }
        }

        // ── Morning signal fallback: fire from /gex if cron missed it ──
        // ≥9:40 ET only (2026-06-10): cron normally completes by ~9:34; at
        // 9:33 this fallback fired DURING the cron's in-flight send (the
        // 'sent' marker hadn't propagated) and produced a duplicate message.
        // A fallback that jumps in 3 min after open isn't a fallback.
        if (marketOpen && (etH > 9 || (etH === 9 && etM >= 40))) {
          const todayCheck = `${etNow.getFullYear()}-${String(etNow.getMonth()+1).padStart(2,'0')}-${String(etNow.getDate()).padStart(2,'0')}`;
          const mKey = `morning_signal_${todayCheck}`;
          const mFailKey = `morning_signal_fail_${todayCheck}`;
          const mDone = await env.SIGNAL_KV.get(mKey);
          if (!mDone) {
            // Failure cooldown: don't retry more than once per 60s to avoid hammering APIs
            const lastFail = await env.SIGNAL_KV.get(mFailKey);
            const sinceFail = lastFail ? Date.now() - parseInt(lastFail) : Infinity;
            if (sinceFail > 60_000) {
              try {
                console.log('[gex] Morning signal not sent yet — triggering from /gex');
                await handleScheduled(env);
              } catch (e) {
                console.warn('[gex] morning signal fallback failed:', e.message || e);
                await env.SIGNAL_KV.put(mFailKey, String(Date.now()), { expirationTtl: 3600 });
              }
            }
          }
        }

        // ── EOD fallback: fire from /gex any time after 4:16 PM ET on a weekday
        // if EOD hasn't already completed for today. Backs up the self-ping in
        // handleScheduled for cases where every afternoon cron missed (rare but
        // observed 2026-04-17 — 4+ hours of Cloudflare cron silence). Any browser
        // hit on /gex resurrects the EOD write.
        const afterEOD = (etH === 16 && etM >= 16) || (etH >= 17 && etH < 24);
        const isEODWindow = dow >= 1 && dow <= 5 && afterEOD;
        if (isEODWindow) {
          const todayCheck = `${etNow.getFullYear()}-${String(etNow.getMonth()+1).padStart(2,'0')}-${String(etNow.getDate()).padStart(2,'0')}`;
          const eodKey = `eod_done_${todayCheck}`;
          const eodDone = await env.SIGNAL_KV.get(eodKey);
          if (!eodDone) {
            try {
              console.log('[gex] EOD not run yet — triggering from /gex');
              const r = await handleEOD(env, etNow);
              // Only mark done after a real write. If Schwab expired + Stooq
              // down, fields are empty — leaving the flag unset lets the next
              // tick/hit retry.
              if (r.wroteFields) {
                await env.SIGNAL_KV.put(eodKey, 'done', { expirationTtl: 86400 });
              } else {
                console.warn('[gex] EOD ran but wrote no fields — leaving eod_done unset for retry');
              }
            } catch (e) {
              console.warn('[gex] EOD fallback failed:', e.message || e);
            }
          }
        }

        if (!data) return jsonResp({ error: 'No GEX data available yet' }, 404, publicCors);

        // Mode switch: ?mode=0dte returns 0DTE-only GEX, default is all-expiry
        const gexMode = url.searchParams.get('mode');
        let actualMode = 'all';
        if (gexMode === '0dte') {
          const dte0 = await env.SIGNAL_KV.get('gex_current_0dte');
          if (dte0) { data = dte0; actualMode = '0dte'; }
          // else: falls back to all-expiry data, actualMode stays 'all'
        }

        // Inject full daily event + commentary logs so all devices see complete history
        try {
          const etNow2 = toET();
          const todayISO2 = `${etNow2.getFullYear()}-${String(etNow2.getMonth()+1).padStart(2,'0')}-${String(etNow2.getDate()).padStart(2,'0')}`;
          const parsed = JSON.parse(data);
          parsed.gexMode = actualMode; // tells frontend which mode is actually served
          const logRaw = await env.SIGNAL_KV.get(`gex_events_${todayISO2}`);
          if (logRaw) parsed.eventLog = JSON.parse(logRaw);
          const cLogRaw = await env.SIGNAL_KV.get(`gex_commentary_${todayISO2}`);
          if (cLogRaw) parsed.commentaryLog = JSON.parse(cLogRaw);
          // Intraday call-vs-put flow series for the "Call vs Put Flow — Today" chart
          const flowRaw = await env.SIGNAL_KV.get(`gex_flow_${todayISO2}`);
          parsed.flowSeries = flowRaw ? JSON.parse(flowRaw) : [];
          data = JSON.stringify(parsed);
        } catch (e) { /* serve without full logs if parse fails */ }

        return new Response(data, {
          status: 200,
          headers: { 'Content-Type': 'application/json', ...publicCors },
        });
      } catch (e) {
        return jsonResp({ error: e.message }, 500, publicCors);
      }
    }

    // Preflight
    if (request.method === 'OPTIONS') {
      // Allow CORS preflight for /gex from any origin
      if (url.pathname === '/gex') {
        return new Response(null, {
          status: 204,
          headers: {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '86400',
          },
        });
      }
      return new Response(null, { status: 204, headers: corsHeaders });
    }

    if (!corsOk) {
      return new Response('Forbidden', { status: 403 });
    }

    try {
      // ── POST /sync ── Browser pushes tokens/creds/discord to KV
      if (url.pathname === '/sync' && request.method === 'POST') {
        const secret = request.headers.get('X-Sync-Secret');
        if (!secret || secret !== env.SYNC_SECRET) {
          return jsonResp({ error: 'Unauthorized' }, 401, corsHeaders);
        }

        const json = await request.json();

        // Validate optional history record fields if present
        if ('date' in json && (typeof json.date !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(json.date))) {
          return jsonResp({ error: 'Invalid payload: date' }, 400, corsHeaders);
        }
        const numOrNull = ['m8bfPL', 'stradPL', 'gxbfPL', 'bobfPL', 'm8bfWR', 'vixOpen', 'vixClose', 'spxOpen', 'spxClose'];
        for (const field of numOrNull) {
          if (field in json && json[field] !== null && typeof json[field] !== 'number') {
            return jsonResp({ error: `Invalid payload: ${field}` }, 400, corsHeaders);
          }
        }

        const { schwab_tokens, schwab_creds, discord_config } = json;

        if (schwab_tokens) await env.SIGNAL_KV.put('schwab_tokens', JSON.stringify(schwab_tokens));
        if (schwab_creds)  await env.SIGNAL_KV.put('schwab_creds', JSON.stringify(schwab_creds));
        if (discord_config) await env.SIGNAL_KV.put('discord_config', JSON.stringify(discord_config));

        return jsonResp({ ok: true, synced: Object.keys(json) }, 200, corsHeaders);
      }

      // ── GET /access-token ──
      // Central token source for browser + Python scraper. The Worker is the
      // only party that rotates Schwab's refresh_token; other clients call
      // here and get a valid access_token without ever touching refresh_token
      // themselves. This eliminates the 3-way rotation race that was kicking
      // every client into 401 hell.
      if (url.pathname === '/access-token' && request.method === 'GET') {
        const accTokenSecret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
        const linkSecret = request.headers.get('X-Link-Secret');
        const linkOk = env.LINK_SECRET && linkSecret === env.LINK_SECRET;   // skipper (2026-06-12)
        if (!linkOk && (!accTokenSecret || accTokenSecret !== env.SYNC_SECRET)) {
          return jsonResp({ error: 'Unauthorized' }, 401, corsHeaders);
        }
        try {
          // ?force=true triggers an immediate refresh against Schwab, useful
          // for diagnosing token issues without waiting for natural expiry.
          const force = url.searchParams.get('force') === 'true';
          const token = await getAccessToken(env, force);
          const tokensRaw = await env.SIGNAL_KV.get('schwab_tokens');
          const expiry = tokensRaw ? JSON.parse(tokensRaw).expiry : null;
          return jsonResp({ access_token: token, expiry, forced: force }, 200, corsHeaders);
        } catch (e) {
          return jsonResp({ error: e.message }, 500, corsHeaders);
        }
      }

      // ── POST /token ──
      if (url.pathname === '/token' && request.method === 'POST') {
        const tokenSecret = request.headers.get('X-Sync-Secret');
        if (!tokenSecret || tokenSecret !== env.SYNC_SECRET) {
          return jsonResp({ error: 'Unauthorized' }, 401, corsHeaders);
        }

        const json = await request.json();
        const { app_key, grant_type, code, redirect_uri, refresh_token } = json;
        if (!app_key || !grant_type) {
          return jsonResp({ error: 'Missing app_key or grant_type' }, 400, corsHeaders);
        }

        const body = new URLSearchParams({ grant_type });
        if (grant_type === 'authorization_code') {
          body.set('code', code);
          body.set('redirect_uri', redirect_uri);
        } else if (grant_type === 'refresh_token') {
          body.set('refresh_token', refresh_token);
        }

        const resp = await fetch('https://api.schwabapi.com/v1/oauth/token', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Basic ' + btoa(`${app_key}:${env.SCHWAB_APP_SECRET}`),
          },
          body,
        });

        const data = await resp.json();
        return jsonResp(data, resp.status, corsHeaders);
      }

      // ── GET /market/* ──
      // Pass-through to Schwab market data API. If caller omits Authorization,
      // worker auto-fetches a fresh token from KV (refreshes if needed).
      if (url.pathname.startsWith('/market/') && request.method === 'GET') {
        const subpath = url.pathname.slice('/market'.length);
        const ALLOWED = ['/pricehistory', '/quotes', '/chains', '/markets', '/instruments'];
        if (!ALLOWED.some(p => subpath.startsWith(p))) {
          return jsonResp({ error: 'Path not allowed' }, 403, corsHeaders);
        }
        const upstream = `https://api.schwabapi.com/marketdata/v1${subpath}${url.search}`;

        // Use provided Authorization header, else auto-fetch from KV + refresh if expired
        let authHeader = request.headers.get('Authorization');
        if (!authHeader) {
          const token = await getAccessToken(env);
          authHeader = `Bearer ${token}`;
        }

        const resp = await fetch(upstream, { headers: { 'Authorization': authHeader } });
        const data = await resp.json();
        return jsonResp(data, resp.status, corsHeaders);
      }

      // ── GET /history ── Historical signals table
      if (url.pathname === '/history' && request.method === 'GET') {
        const months = Math.min(Math.max(parseInt(url.searchParams.get('months')) || 6, 1), 12);
        const token = await getAccessToken(env);

        const vixUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24VIX&periodType=month&period=${months}&frequencyType=daily&frequency=1`;
        const spxUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24SPX&periodType=month&period=${months}&frequencyType=daily&frequency=1`;

        const [vixData, spxData] = await Promise.all([
          fetchSchwabJSON(vixUrl, token),
          fetchSchwabJSON(spxUrl, token),
        ]);

        if (!vixData.candles?.length) return jsonResp({ error: 'No VIX data' }, 502, corsHeaders);
        if (!spxData.candles?.length) return jsonResp({ error: 'No SPX data' }, 502, corsHeaders);

        // Sort ascending
        vixData.candles.sort((a, b) => a.datetime - b.datetime);
        spxData.candles.sort((a, b) => a.datetime - b.datetime);

        // Build SPX lookup by date string
        const spxByDate = new Map();
        for (const c of spxData.candles) {
          const d = toET(new Date(c.datetime));
          const key = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
          spxByDate.set(key, c);
        }

        const rows = [];
        const vix = vixData.candles;
        for (let i = 1; i < vix.length; i++) {
          const d = toET(new Date(vix[i].datetime));
          const etDate = new Date(d.getFullYear(), d.getMonth(), d.getDate(), 12);
          if (!isTrade(etDate)) continue;

          const dateKey = `${etDate.getFullYear()}-${String(etDate.getMonth()+1).padStart(2,'0')}-${String(etDate.getDate()).padStart(2,'0')}`;
          const prevD = toET(new Date(vix[i-1].datetime));
          const prevKey = `${prevD.getFullYear()}-${String(prevD.getMonth()+1).padStart(2,'0')}-${String(prevD.getDate()).padStart(2,'0')}`;

          const vixTodayOpen = parseFloat(vix[i].open.toFixed(2));
          const vixPrevClose = parseFloat(vix[i-1].close.toFixed(2));
          const vixPrevOpen = parseFloat(vix[i-1].open.toFixed(2));

          let spxGapPct = null;
          const spxToday = spxByDate.get(dateKey);
          const spxPrev = spxByDate.get(prevKey);
          if (spxToday && spxPrev) {
            spxGapPct = ((spxToday.open - spxPrev.close) / spxPrev.close) * 100;
          }

          const signal = calculateSignal({
            vixToday: vixTodayOpen,
            vixYOpen: vixPrevOpen,
            vixYClose: vixPrevClose,
            spxGapPct,
            etDate,
          });

          rows.push({
            date: dateKey,
            dateLong: dateLong(etDate),
            dayLabel: signal.dayLabel,
            vixOpen: vixTodayOpen,
            vixPrevClose,
            vixPrevOpen,
            overnightDrop: parseFloat((vixPrevClose - vixTodayOpen).toFixed(2)),
            o2o: parseFloat((vixPrevOpen - vixTodayOpen).toFixed(2)),
            spxGapPct: spxGapPct !== null ? parseFloat(spxGapPct.toFixed(2)) : null,
            signal,
          });
        }

        rows.reverse(); // most recent first
        return jsonResp(rows, 200, corsHeaders);
      }

      // ── GET /signals ── Today's Discord signals from KV
      if (url.pathname === '/signals' && request.method === 'GET') {
        const data = await env.SIGNAL_KV.get('signals_today');
        return jsonResp(data ? JSON.parse(data) : { date: '', signals: [] }, 200, corsHeaders);
      }

      // ── GET /spx-history ── Today's SPX price ticks (replaces dead spx_history.json)
      if (url.pathname === '/spx-history' && request.method === 'GET') {
        const etNowH = toET(new Date());
        const todayH = `${etNowH.getFullYear()}-${String(etNowH.getMonth()+1).padStart(2,'0')}-${String(etNowH.getDate()).padStart(2,'0')}`;
        const spxHistRaw = await env.SIGNAL_KV.get(`spx_history_${todayH}`);
        return jsonResp({ date: todayH, data: spxHistRaw ? JSON.parse(spxHistRaw) : [] }, 200, corsHeaders);
      }

      // ── GET /trade ── Today's M8BF trade status (replaces dead today_trade.json)
      if (url.pathname === '/trade' && request.method === 'GET') {
        const etNowT = toET(new Date());
        const todayT = isoDateET(etNowT);  // byte-identical to the old manual y-m-d

        // ── lastClosed (mirrors /bobf-today, /gxbf-today, etc.). bobf-bot's
        // stats card relies on this for "TRADES" / "CLOSED P/L" counting on
        // M8BF — without it, every M8BF paper trade stays in 'logged' status
        // forever (the bot's upLookup only ever sees today's open). Most
        // recent date < today with m8bfPL non-null. Same-day open + close
        // because M8BF is 0DTE. See lessons.md P6. ──
        let lastClosedM8bf = null, todaySettled = null;
        try {
          const _hist = await getHistory(env);
          if (Array.isArray(_hist)) {
            const _prior = _hist
              .filter(r => r.date && r.date < todayT && r.m8bfPL != null)
              .sort((a, b) => b.date.localeCompare(a.date));
            if (_prior.length) {
              lastClosedM8bf = {
                date: _prior[0].date,
                openDate: _prior[0].date,
                closeDate: _prior[0].date,
                pnl: parseFloat(_prior[0].m8bfPL),
                status: 'settled',
              };
            }
            // Today's settled row (2026-06-15): once EOD writes m8bfPL +
            // spxClose, the live trade is DONE — serve the settled value so
            // the page stops marking 0DTE to a possibly-stale intraday spot
            // (live page showed +$1,774 @ a frozen 7549 vs +$1,181 @ the real
            // 7555 close).
            const _td = _hist.find(r => r.date === todayT);
            if (_td && _td.m8bfPL != null && _td.spxClose != null) {
              todaySettled = { pnl: parseFloat(_td.m8bfPL), spxClose: parseFloat(_td.spxClose) };
            }
          }
        } catch { /* if history fetch fails, lastClosed stays null */ }

        // ── M8BF banned-day gate (early return — no Discord poll on banned
        //    days, exactly as before). Logic moved verbatim into
        //    m8bfBannedReason() so refreshM8bfLiveQuotes shares it. ──
        const bannedReason = await m8bfBannedReason(env, etNowT);
        if (bannedReason) {
          return jsonResp({ date: todayT, triggered: false, status: 'banned', reason: `No M8BF (${bannedReason})`, lastClosed: lastClosedM8bf }, 200, corsHeaders);
        }

        // ── Cron-stall self-heal: if the last cron tick is stale AND we're
        // in market hours, force a Discord poll on demand so today's
        // signals_today gets refreshed. Without this, when cron stalls during
        // the M8BF window the signals never get captured and live page shows
        // 'no signal' forever even if Discord did post one.
        const isMktHrT = (etNowT.getHours() > 9 || (etNowT.getHours() === 9 && etNowT.getMinutes() >= 30)) && etNowT.getHours() < 16;
        if (isMktHrT) {
          const lastRunRaw = await env.SIGNAL_KV.get('last_run');
          const lastRun = lastRunRaw ? JSON.parse(lastRunRaw) : null;
          const lastRunMs = lastRun?.date ? new Date(lastRun.date).getTime() : 0;
          const ageMs = Date.now() - lastRunMs;
          if (ageMs > 5 * 60 * 1000) {  // >5 min stale → poll now
            try {
              if (env.DISCORD_USER_TOKEN) {
                const pollResult = await pollDiscordSignals(env);
                console.log('[/trade] cron-stall self-heal poll:', JSON.stringify(pollResult));
              }
            } catch (e) { console.warn('[/trade] cron-stall poll failed:', e.message); }
          }
        }

        // Shared selection (identical logic to the previous inline block).
        const sel = await selectM8bfQualifying(env, etNowT);
        if (sel.status === 'waiting' && sel.reason) {
          return jsonResp({ date: todayT, triggered: false, status: 'waiting', reason: sel.reason, lastClosed: lastClosedM8bf }, 200, corsHeaders);
        }
        if (sel.status === 'no_signal') {
          return jsonResp({ date: todayT, triggered: false, status: 'no_signal', reason: sel.reason, lastClosed: lastClosedM8bf }, 200, corsHeaders);
        }
        if (sel.status === 'waiting') {
          return jsonResp({ date: todayT, triggered: false, status: 'waiting', window: sel.window, lastClosed: lastClosedM8bf }, 200, corsHeaders);
        }

        // status === 'open'
        const qualifying = sel.qualifying;
        const resp = {
          date: todayT,
          triggered: true,
          status: 'open',
          signal_time: qualifying.time,
          center: qualifying.center,
          bf_lower: qualifying.lower,
          bf_upper: qualifying.upper,
          t1: qualifying.t1,
          premium: qualifying.premium,
          cp: qualifying.cp,
          lastClosed: lastClosedM8bf,
        };
        // Merge the REAL mark-to-market spread mid written every tick by
        // refreshM8bfLiveQuotes. live.html prefers this over the
        // at-expiration intrinsic (which overstates intraday profit). Absent
        // (pre-quote / chain miss / cron stall) → client falls back to the
        // intrinsic formula, i.e. no worse than the old behavior.
        try {
          const liveRaw = await env.SIGNAL_KV.get(`m8bf_live_${todayT}`);
          if (liveRaw) {
            const lv = JSON.parse(liveRaw);
            if (lv && lv.currentValue != null) {
              resp.currentValue     = lv.currentValue;
              resp.currentPnl       = lv.currentPnl;
              resp.currentSpot      = lv.currentSpot;
              resp.currentLowerMid  = lv.currentLowerMid;
              resp.currentCenterMid = lv.currentCenterMid;
              resp.currentUpperMid  = lv.currentUpperMid;
              resp.lastQuoteAt      = lv.lastQuoteAt;
            }
          }
        } catch { /* fall back to intrinsic on the client */ }
        // Settled override: EOD has the day's real m8bfPL @ the actual close.
        // Set currentValue so the client's (currentValue − premium) yields the
        // settled P/L exactly, and flag it so the page labels it "Final".
        if (todaySettled) {
          resp.status = 'settled';
          resp.settled = true;
          resp.spxClose = todaySettled.spxClose;
          resp.currentSpot = todaySettled.spxClose;
          resp.currentPnl = todaySettled.pnl;
          resp.currentValue = parseFloat((resp.premium + todaySettled.pnl / 100).toFixed(2));
        }
        return jsonResp(resp, 200, corsHeaders);
      }

      return jsonResp({ error: 'Not found' }, 404, corsHeaders);
    } catch (e) {
      return jsonResp({ error: e.message }, 500, corsHeaders);
    }
  },

  async scheduled(event, env, ctx) {
    console.log('[cron] Triggered at', new Date().toISOString());

    // Sunday digest cron runs ONLY the digest — the main tick's guards
    // assume weekdays and must not run on Sundays.
    if (event.cron === '0 22 * * 0') {
      try { await weeklyDigest(env); } catch (e) { console.warn('[digest]', e.message); }
      return;
    }

    // ── Slow-degradation watchdog: alert Discord if Schwab refresh has been
    //    failing for too long. Without this, a broken refresh chain rots
    //    silently for hours (observed 2026-04-30: 376 errors / 17 hrs before
    //    user noticed missing morning signal).
    //    Rate-limited to one alert per hour to avoid spam during outages.
    try {
      const stRaw = await env.SIGNAL_KV.get('schwab_refresh_state');
      if (stRaw) {
        const st = JSON.parse(stRaw);
        const errs = st.consecutiveErrors || 0;
        const lastAlert = parseInt(await env.SIGNAL_KV.get('schwab_alert_last_ms') || '0');
        const ALERT_COOLDOWN_MS = 60 * 60 * 1000;  // 1 hour
        const ERR_THRESHOLD = 10;
        if (!st.ok && errs >= ERR_THRESHOLD && (Date.now() - lastAlert) > ALERT_COOLDOWN_MS) {
          const dcRaw = await env.SIGNAL_KV.get('discord_config');
          if (dcRaw) {
            const dc = JSON.parse(dcRaw);
            if (dc.channelId) {
              const minsSinceOK = st.lastSuccess ? Math.round((Date.now() - st.lastSuccess) / 60000) : null;
              const alertResult = await sendDiscordDM(env, dc.channelId,
                `🚨 **Schwab refresh degraded** — ${errs} consecutive errors${minsSinceOK ? ` (${minsSinceOK} min since last success)` : ''}.\nMessage: \`${(st.msg || '').slice(0, 150)}\`\n→ Re-authenticate Schwab in dashboard, then hit \`/health?refresh=now\` to recover.`,
                dc.proxyUrl);
              if (alertResult.ok) {
                await env.SIGNAL_KV.put('schwab_alert_last_ms', String(Date.now()), { expirationTtl: 86400 });
                console.log('[cron] Posted Schwab degraded alert via', alertResult.source);
                await logEvent(env, 'error', 'schwab-degraded',
                  `${errs} consecutive Schwab refresh errors — Discord alert sent`,
                  { errs, minsSinceOK, msg: (st.msg || '').slice(0, 150) });
              } else {
                console.warn('[cron] Schwab degraded alert post failed:', alertResult.error);
              }
            }
          }
        }
      }
    } catch (watchdogErr) {
      console.error('[cron] Watchdog failed:', watchdogErr.message);
    }

    // ── GitHub-mirror watchdog (2026-06-09) ──
    // Mirror writes fire only a few times a day (morning open, EOD settle,
    // strategy settles), so 2 consecutive failures ≈ half a day of KV→GitHub
    // drift. Alert once per 6h. The expired-PAT incident sat silent for 2
    // days because nothing surfaced these failures.
    try {
      const mirrorRaw = await env.SIGNAL_KV.get('history_mirror_state');
      if (mirrorRaw) {
        const mst = JSON.parse(mirrorRaw);
        const merrs = mst.consecutiveErrors || 0;
        const lastMirrorAlert = parseInt(await env.SIGNAL_KV.get('mirror_alert_last_ms') || '0');
        const MIRROR_COOLDOWN_MS = 6 * 60 * 60 * 1000;  // 6 hours
        if (!mst.ok && merrs >= 2 && (Date.now() - lastMirrorAlert) > MIRROR_COOLDOWN_MS) {
          const dcRaw = await env.SIGNAL_KV.get('discord_config');
          if (dcRaw) {
            const dc = JSON.parse(dcRaw);
            if (dc.channelId) {
              const minsSinceOK = mst.lastSuccess ? Math.round((Date.now() - mst.lastSuccess) / 60000) : null;
              const alertResult = await sendDiscordDM(env, dc.channelId,
                `🚨 **GitHub history mirror failing** — ${merrs} consecutive errors${minsSinceOK ? ` (${Math.round(minsSinceOK/60)}h since last success)` : ''}.\nError: \`${(mst.msg || '').slice(0, 150)}\`\n→ KV is fine but the dashboard's history_data.json is going stale. Likely an expired GITHUB_TOKEN — rotate it:\n\`echo NEW_PAT | npx wrangler secret put GITHUB_TOKEN --name schwab-proxy\``,
                dc.proxyUrl);
              if (alertResult.ok) {
                await env.SIGNAL_KV.put('mirror_alert_last_ms', String(Date.now()), { expirationTtl: 86400 });
                await logEvent(env, 'error', 'mirror-degraded',
                  `${merrs} consecutive GitHub mirror failures — Discord alert sent`,
                  { merrs, minsSinceOK, msg: (mst.msg || '').slice(0, 150) });
              }
            }
          }
        }
      }
    } catch (mirrorWatchdogErr) {
      console.error('[cron] Mirror watchdog failed:', mirrorWatchdogErr.message);
    }

    // ── Evening preview (2026-06-10): tomorrow\'s special days + health ──
    // Once per weekday 18:00-18:20 ET. Tells the user TONIGHT what tomorrow
    // is (CPI/FED/OPEX+-1/VIX-exp/EOM/NM/earnings), which strategies that
    // gates, plus a one-line system health check and the tilt advisory.
    try {
      const etP = toET(new Date());
      const inWinP = etP.getDay() >= 1 && etP.getDay() <= 5 && etP.getHours() === 18 && etP.getMinutes() < 20;
      if (inWinP) {
        const todayP = isoDateET(etP);
        const prevKey = `evening_preview_${todayP}`;
        if (!(await env.SIGNAL_KV.get(prevKey))) {
          await env.SIGNAL_KV.put(prevKey, 'sent', { expirationTtl: 86400 });
          const tm = nextTrade(etP);
          const tags = [];
          if (cpiSch.includes(todayLong(tm)))  tags.push('CPI → M8BF/Straddle/BOBF blocked (GXBF + Diagonal exempt)');
          if (fedSch.includes(todayLong(tm)))  tags.push('FED day');
          if (opexSch.includes(todayLong(tm))) tags.push('OPEX');
          if (opexSch.some(ds => isTodayBefore(ds, tm))) tags.push('OPEX-1 → Diagonal blocked');
          if (opexSch.some(ds => isTodayAfter(ds, tm)))  tags.push('OPEX+1 → GXBF auto-trigger (unless VIX gaps ≥2% up)');
          if (vixSch.includes(todayLong(tm)))  tags.push('VIX expiry');
          if (isLastTradeMo(tm))               tags.push('EOM → GXBF + Diagonal blocked, EOM Straddle day');
          if (isEomN(1, tm))                   tags.push('EOM-1 → Diagonal blocked');
          if (isFirstTradeMo(tm))              tags.push(`NM → ${tm.getDay() === 1 ? 'Monday (M8BF stands)' : 'non-Monday → NM Straddle'}; Diagonal blocked`);
          if (isEarningsDay(tm))               tags.push('big-tech earnings day');
          let health = [];
          try {
            const ms = await env.SIGNAL_KV.get('history_mirror_state');
            health.push(ms && JSON.parse(ms).ok === false ? 'mirror ⚠️' : 'mirror ✓');
          } catch (_) {}
          try {
            const co = await env.SIGNAL_KV.get(`cor1m_open_${todayP}`);
            health.push(co ? `COR1M ✓ (${JSON.parse(co).cor1m})` : 'COR1M ⚠️ not captured');
          } catch (_) {}
          // Schwab refresh-token age warning (2026-06-11, user-approved):
          // tokens die 7 days after re-auth; warn while there's still time
          // to reconnect instead of discovering a dead dashboard at 8 AM.
          let tokenWarn = null, tokenDaysLeft = null;
          try {
            const tkRaw = await env.SIGNAL_KV.get('schwab_tokens');
            if (tkRaw) {
              const tk = JSON.parse(tkRaw);
              if (tk.refreshExpiry) {
                tokenDaysLeft = (tk.refreshExpiry - Date.now()) / 86400000;
                // 3.5d threshold: a Friday-evening warning still covers
                // weekend expiries (preview only sends on weekdays).
                if (tokenDaysLeft <= 3.5) {
                  const when = tokenDaysLeft <= 1 ? 'within 24h' : `in ~${Math.ceil(tokenDaysLeft)} days`;
                  tokenWarn = `⚠️ Schwab token expires ${when} — open the dashboard and press Connect Schwab (takes 30s)`;
                }
              }
            }
          } catch (_) {}
          let tiltP = null;
          try { tiltP = await computeTiltLine(env, isoDateET(nextTrade(etP))); } catch (_) {}
          // Retry research persistence (idempotent) — catches EOD-time failures
          try { await persistResearchArtifacts(env, etP); } catch (_) {}
          // Vol-flow decomposition retry (idempotent) — then tomorrow's context
          // line: today's label IS what the morning message will report.
          let volP = null;
          try {
            await computeVixDecompDaily(env, etP);
            const vdRaw = await env.SIGNAL_KV.get(`vix_decomp_${todayP}`);
            if (vdRaw) {
              const vd = JSON.parse(vdRaw);
              volP = `Vol flow today: ${vd.label} (slide ${vd.slide >= 0 ? '+' : ''}${vd.slide} · real ${vd.parallel >= 0 ? '+' : ''}${vd.parallel})`;
            }
          } catch (_) {}
          // Score our own advisory claims (GEX regime + Day-type + Vol-flow cells)
          try { await scoreAdvisories(env); } catch (e) { console.warn('[scorecard]', e.message); }
          const dcRaw = await env.SIGNAL_KV.get('discord_config');
          if (dcRaw) {
            const dc = JSON.parse(dcRaw);
            if (dc.channelId) {
              let scoreLine = null;
              try { scoreLine = await scorecardLine(env, etP); } catch (_) {}
              const msg = `🌙 **Tomorrow — ${todayLong(tm)} (${tradeWdLabel(tm)})**\n` +
                (tags.length ? tags.map(t => `• ${t}`).join('\n') : '• No special days — all strategies on their own merits') +
                `\n${health.join(' · ')}` +
                (tokenWarn ? `\n${tokenWarn}` : '') +
                (tiltP ? `\n${tiltP.replace('   │', ':')}` : '') +
                (volP ? `\n${volP}` : '') +
                (scoreLine ? `\n${scoreLine}` : '');
              await sendDiscordDM(env, dc.channelId, msg, dc.proxyUrl);
              // ≤1 day left → a second, standalone ping so it can't be missed
              // inside the preview wall of text.
              if (tokenDaysLeft != null && tokenDaysLeft <= 1) {
                await sendDiscordDM(env, dc.channelId,
                  `🚨 **SCHWAB TOKEN DIES WITHIN 24H** — without re-auth the bot cannot trade tomorrow.\nDashboard → Connect Schwab.`,
                  dc.proxyUrl);
              }
            }
          }
        }
      }
    } catch (e) { console.warn('[evening-preview]', e.message); }

    // ── Nightly data watchdog: own 18:35-18:50 window (lessons P17) ──
    if (etP.getDay() >= 1 && etP.getDay() <= 5 && etP.getHours() === 18
        && etP.getMinutes() >= 35 && etP.getMinutes() < 50 && !isHol(etP)) {
      const wdKey = `watchdog_${todayP}`;
      if (!(await env.SIGNAL_KV.get(wdKey))) {
        await env.SIGNAL_KV.put(wdKey, 'running', { expirationTtl: 86400 });
        try {
          const wd = await dataCompletenessCheck(env, etP);
          if (!wd.failed.length) await env.SIGNAL_KV.put(wdKey, 'done', { expirationTtl: 86400 });
          else await env.SIGNAL_KV.delete(wdKey);   // retry within window
        } catch (e) { await env.SIGNAL_KV.delete(wdKey); console.warn('[watchdog]', e.message); }
      }
    }

    // ── Tail-bundle staleness watchdog (2026-06-09) ──
    // The Tail Hedge LaunchAgent (5 PM ET) can fail silently — observed today:
    // macOS TCC blocks launchd from reading ~/Desktop ("Operation not
    // permitted", exit 126), so the bundle (and the cor1m history column)
    // silently stops refreshing. Once per weekday evening (18:00-18:20 ET),
    // fetch the bundle's last daily date; if it isn't today, alert Discord
    // with the manual-run command. One fetch/day (~1.8MB) — negligible.
    try {
      const etW = toET(new Date());
      const isWkdayW = etW.getDay() >= 1 && etW.getDay() <= 5;
      const inWindow = etW.getHours() === 18 && etW.getMinutes() < 20;
      if (isWkdayW && inWindow) {
        const todayW = isoDateET(etW);
        const checkedKey = `tail_bundle_check_${todayW}`;
        if (!(await env.SIGNAL_KV.get(checkedKey))) {
          await env.SIGNAL_KV.put(checkedKey, 'checked', { expirationTtl: 86400 });
          const r = await fetch('https://raw.githubusercontent.com/rava8989/brave/main/cor1m_contango_bundle.json',
            { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } });
          if (r.ok) {
            const bundle = await r.json();
            const daily = bundle?.daily || [];
            const lastDate = daily.length ? daily[daily.length - 1].date : null;
            if (lastDate && lastDate < todayW) {
              const dcRaw = await env.SIGNAL_KV.get('discord_config');
              if (dcRaw) {
                const dc = JSON.parse(dcRaw);
                if (dc.channelId) {
                  await sendDiscordDM(env, dc.channelId,
                    `ℹ️ **Backtester bundle is stale** — last day ${lastDate}, expected ${todayW}.\nLIVE is unaffected (COR1M/VVIX are cloud-captured from Schwab). Only the Tail Hedge backtester page lags.\n→ Refresh when the Mac is on: \`bash scripts/refresh_tail_hedge.sh\``,
                    dc.proxyUrl);
                  await logEvent(env, 'warn', 'tail-bundle-stale',
                    `bundle last=${lastDate}, expected ${todayW} — Discord alert sent`, { lastDate });
                }
              }
            }
          }
        }
      }
    } catch (tailWatchdogErr) {
      console.warn('[cron] Tail-bundle watchdog failed:', tailWatchdogErr.message);
    }

    let result;
    try {
      result = await handleScheduled(env);
      console.log('[cron] Result:', JSON.stringify(result).slice(0, 500));
    } catch (e) {
      console.error('[cron] Error:', e.message || e);
      result = { status: 'error', error: e.message, date: new Date().toISOString() };

      // Send error notification to Discord so failures aren't silent
      try {
        const dcRaw = await env.SIGNAL_KV.get('discord_config');
        if (dcRaw) {
          const dc = JSON.parse(dcRaw);
          if (dc.channelId) {
            await sendDiscordDM(env, dc.channelId,
              `⚠️ **Signal Error**\n\`${e.message}\`\nCheck schwab-proxy logs.`,
              dc.proxyUrl);
            await logEvent(env, 'error', 'cron-error', e.message);
          }
        }
      } catch (notifyErr) {
        console.error('[cron] Failed to send error notification:', notifyErr.message);
      }
    }
    result.date = result.date || new Date().toISOString();
    await env.SIGNAL_KV.put('last_run', JSON.stringify(result));
  },
};

function jsonResp(data, status, headers) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json', ...headers },
  });
}

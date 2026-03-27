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
// CALENDAR DATA (ported from index.html)
// ════════════════════════════════════════════════════════════════════

const cpiSch   = ["January 13, 2026","February 13, 2026","March 11, 2026","April 10, 2026","May 12, 2026","June 10, 2026","July 14, 2026","August 12, 2026","September 11, 2026","October 14, 2026","November 10, 2026","December 10, 2026"];
const fedSch   = ["January 28, 2026","March 18, 2026","April 29, 2026","June 17, 2026","July 29, 2026","September 16, 2026","October 28, 2026","December 9, 2026"];
const opexSch  = ["January 16, 2026","February 20, 2026","March 20, 2026","April 17, 2026","May 15, 2026","June 18, 2026","July 17, 2026","August 21, 2026","September 18, 2026","October 16, 2026","November 20, 2026","December 18, 2026"];
const holidays = ["January 1, 2026","January 19, 2026","February 16, 2026","April 3, 2026","May 25, 2026","June 19, 2026","July 3, 2026","September 7, 2026","November 26, 2026","December 25, 2026"];
const vixSch   = ["January 21, 2026","February 18, 2026","March 18, 2026","April 15, 2026","May 20, 2026","June 17, 2026","July 15, 2026","August 19, 2026","September 16, 2026","October 21, 2026","November 18, 2026","December 16, 2026"];

const earningsSchedule = [
  { date:"January 28, 2026",  company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:true },
  { date:"January 28, 2026",  company:"Meta",      ticker:"META", timing:"AH", confirmed:true },
  { date:"January 29, 2026",  company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:true },
  { date:"February 4, 2026",  company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:true },
  { date:"February 5, 2026",  company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:true },
  { date:"February 26, 2026", company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:true },
  { date:"April 23, 2026",    company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:false },
  { date:"April 28, 2026",    company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:false },
  { date:"April 29, 2026",    company:"Meta",      ticker:"META", timing:"AH", confirmed:true  },
  { date:"April 30, 2026",    company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:false },
  { date:"April 30, 2026",    company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:false },
  { date:"May 20, 2026",      company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:true  },
  { date:"July 23, 2026",     company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:false },
  { date:"July 28, 2026",     company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:false },
  { date:"July 29, 2026",     company:"Meta",      ticker:"META", timing:"AH", confirmed:false },
  { date:"July 30, 2026",     company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:false },
  { date:"July 30, 2026",     company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:false },
  { date:"August 19, 2026",   company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:false },
  { date:"October 22, 2026",  company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:false },
  { date:"October 27, 2026",  company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:false },
  { date:"October 28, 2026",  company:"Meta",      ticker:"META", timing:"AH", confirmed:false },
  { date:"October 29, 2026",  company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:false },
  { date:"October 29, 2026",  company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:false },
  { date:"November 18, 2026", company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:false },
];

const T = {
  DROP_GXBF: 0.65, DROP_STRAD_MIN: 0,
  O2O_M8BF: 1.4, VIX_MAX_GXBF: 25, VIX_MAX_BOBF: 23,
  SPX_GAP_THRESHOLD: 0.9,
};

// ════════════════════════════════════════════════════════════════════
// DATE HELPERS (ported from index.html — uses ET dates)
// ════════════════════════════════════════════════════════════════════

function toET(d = new Date()) {
  return new Date(d.toLocaleString('en-US', { timeZone: 'America/New_York' }));
}

function dateLong(d) {
  return d.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' });
}

function todayLong(etDate) { return dateLong(etDate); }

const isWkend = d => { const w = d.getDay(); return w === 0 || w === 6; };
const isHol   = d => holidays.includes(dateLong(d));
const isTrade = d => !isWkend(d) && !isHol(d);

function nextTrade(d) { const n = new Date(d); do { n.setDate(n.getDate() + 1); } while (isWkend(n) || isHol(n)); return n; }
function prevTrade(d) { const p = new Date(d); do { p.setDate(p.getDate() - 1); } while (isWkend(p) || isHol(p)); return p; }

function isTodayAfter(ds, etDate) {
  const d = new Date(ds); if (isNaN(d)) return false;
  const a = nextTrade(d);
  return a.getFullYear() === etDate.getFullYear() && a.getMonth() === etDate.getMonth() && a.getDate() === etDate.getDate();
}
function isTodayBefore(ds, etDate) {
  const d = new Date(ds); if (isNaN(d)) return false;
  const b = prevTrade(d);
  return b.getFullYear() === etDate.getFullYear() && b.getMonth() === etDate.getMonth() && b.getDate() === etDate.getDate();
}

function parseLong(s) { const d = new Date(s); return isNaN(d) ? null : new Date(d.getFullYear(), d.getMonth(), d.getDate(), 12); }
function schedInMonth(list, ref) {
  const y = ref.getFullYear(), m = ref.getMonth();
  for (const s of list) { const d = parseLong(s); if (d && d.getFullYear() === y && d.getMonth() === m) return d; }
  return null;
}

function isPostVixBeforeOpex(ref) {
  const vx = schedInMonth(vixSch, ref), op = schedInMonth(opexSch, ref);
  if (!vx || !op || vx >= op) return false;
  const pv = nextTrade(vx);
  return pv.getFullYear() === ref.getFullYear() && pv.getMonth() === ref.getMonth() && pv.getDate() === ref.getDate();
}

const isPostOpexMon = (etDate) => opexSch.some(ds => isTodayAfter(ds, etDate)) && etDate.getDay() === 1;
const isLastTradeMo = (d) => nextTrade(d).getMonth() !== d.getMonth();
function isEomN(n, d) { const f = new Date(d.getFullYear(), d.getMonth() + 1, 1); let t = prevTrade(f); for (let i = 0; i < n; i++) t = prevTrade(t); return t.getFullYear() === d.getFullYear() && t.getMonth() === d.getMonth() && t.getDate() === d.getDate(); }
const isFirstTradeMo = (d) => prevTrade(d).getMonth() !== d.getMonth();
function isFirstTradeMon(d) {
  const y = d.getFullYear(), m = d.getMonth();
  let x = new Date(y, m, 1); while (isWkend(x) || isHol(x)) x.setDate(x.getDate() + 1);
  while (x.getDay() !== 1) x = nextTrade(x);
  return x.getFullYear() === y && x.getMonth() === m && x.getDate() === d.getDate();
}

function m8Sched(dow) {
  switch (dow) {
    case 1: return { t: "10:00", window: "10:00–10:29", s: ["05", "45", "70", "95"], everyWeek: false };
    case 2: return { t: "12:00", window: "12:00–12:29", s: ["00", "05", "35", "45", "70", "75", "85", "90"], everyWeek: false };
    case 3: return { t: "11:30", window: "11:30–11:59", s: ["30", "45", "50", "70", "80", "95"], everyWeek: false };
    case 4: return { t: "11:30", window: "11:30–11:59", s: ["05", "15", "55", "75", "85"], everyWeek: true };
    case 5: return { t: "13:00", window: "13:00–13:29", s: ["45", "55", "65", "70", "75", "85", "95"], everyWeek: true };
    default: return null;
  }
}
function m8Msg(d) { const sc = m8Sched(d.getDay()); return sc ? `M8BF — Window ${sc.window}` : "M8BF"; }

function ordinal(n) { const s = ["th", "st", "nd", "rd"], v = n % 100; return n + (s[(v - 20) % 10] || s[v] || s[0]); }
function wdName(d) { return ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"][d] || ""; }
function tradeWdLabel(d) {
  const t = new Date(d.getFullYear(), d.getMonth(), d.getDate());
  const dow = t.getDay(), ms = new Date(t.getFullYear(), t.getMonth(), 1);
  let c = 0;
  for (let x = new Date(ms); x <= t; x.setDate(x.getDate() + 1)) { if (!isTrade(x)) continue; if (x.getDay() === dow) c++; }
  if (!isTrade(t)) return `${wdName(dow)} (market closed)`;
  return `${ordinal(c)} ${wdName(dow)}`;
}

function isEarningsDay(etDate) { return earningsSchedule.some(e => e.date === todayLong(etDate)); }

// ════════════════════════════════════════════════════════════════════
// SIGNAL CALCULATION (ported from index.html calculateStrategy)
// Win rate overrides default to unchecked (wr0=false, wr90=false)
// ════════════════════════════════════════════════════════════════════

function calculateSignal({ vixToday, vixYOpen, vixYClose, spxGapPct, etDate }) {
  const cpiDay = cpiSch.includes(todayLong(etDate));
  const dow = etDate.getDay();
  const isMon = dow === 1, isFri = dow === 5, isWed = dow === 3;
  const nmDay = isFirstTradeMo(etDate), nmMon = isFirstTradeMon(etDate);
  const fedDay = fedSch.includes(todayLong(etDate));
  const opexDay = opexSch.includes(todayLong(etDate));
  const postOpMon = isPostOpexMon(etDate);
  const postOpDay = opexSch.some(ds => isTodayAfter(ds, etDate));
  const eomDay = isLastTradeMo(etDate), eom1 = isEomN(1, etDate), eom2 = isEomN(2, etDate);
  const vixExpDay = vixSch.includes(todayLong(etDate));
  const opex1 = opexSch.some(ds => isTodayBefore(ds, etDate));
  const earningsDay = isEarningsDay(etDate);
  const msftMetaEarnings = earningsSchedule.some(e => e.date === todayLong(etDate) && (e.ticker === 'MSFT' || e.ticker === 'META'));

  const o2o = (vixYOpen != null) ? vixYOpen - vixToday : NaN;
  const oNight = vixYClose - vixToday;

  let spxGapForcesM8BF = false;
  if (spxGapPct !== null && spxGapPct !== undefined) {
    spxGapForcesM8BF = Math.abs(spxGapPct) >= T.SPX_GAP_THRESHOLD;
  }

  let rec = "", theme = "neutral", crossed = false, pmNote = false;
  let blockT = "", blockD = "", entryT = "", badge = "";
  let strikeInfo = null;

  if (cpiDay) {
    if (oNight > 0) { rec = "Long ATM Call @ 9:32 AM (CPI) — Max $13"; theme = "strad"; entryT = "9:32 AM"; badge = "CPI CALL"; }
    else { rec = "No Trade (CPI, VIX up)"; theme = "block"; crossed = true; blockT = "hard"; blockD = "CPI day, VIX up"; badge = "BLOCKED"; }
  } else if (spxGapForcesM8BF) {
    const dir = spxGapPct > 0 ? '▲' : '▼';
    rec = m8Msg(etDate); theme = "m8bf"; badge = "M8BF"; strikeInfo = m8Sched(dow); entryT = strikeInfo?.window || "";
    blockD = `SPX gap ${dir}${Math.abs(spxGapPct).toFixed(2)}% ≥ ${T.SPX_GAP_THRESHOLD}% → M8BF forced`;
  } else {
    if (oNight > T.DROP_GXBF) {
      if (vixToday >= T.VIX_MAX_GXBF) { rec = `No GXBF (VIX ${vixToday} ≥ ${T.VIX_MAX_GXBF})`; theme = "block"; crossed = true; blockT = "vix"; blockD = `VIX ${vixToday} ≥ ${T.VIX_MAX_GXBF}`; badge = "BLOCKED"; }
      else { rec = `GXBF @ 9:36 AM`; theme = "gxbf"; entryT = "9:36 AM"; badge = "GXBF"; }
    } else if (o2o > T.O2O_M8BF) {
      rec = m8Msg(etDate); theme = "m8bf"; badge = "M8BF"; strikeInfo = m8Sched(dow); entryT = strikeInfo?.window || "";
    } else if (oNight > T.DROP_STRAD_MIN && oNight < T.DROP_GXBF) {
      rec = "Straddle @ 9:32 AM"; theme = "strad"; entryT = "9:32 AM"; badge = "STRADDLE";
    } else {
      rec = m8Msg(etDate); theme = "m8bf"; badge = "M8BF"; strikeInfo = m8Sched(dow); entryT = strikeInfo?.window || "";
    }

    if (rec.startsWith("M8BF")) {
      if (fedDay) { rec = "No M8BF (Fed day)"; theme = "block"; crossed = true; blockT = "hard"; blockD = "M8BF not traded on Fed days"; badge = "BLOCKED"; strikeInfo = null; }
      else if (msftMetaEarnings) {
        const tickers = earningsSchedule.filter(e => e.date === todayLong(etDate) && (e.ticker === 'MSFT' || e.ticker === 'META')).map(e => e.ticker).join(', ');
        rec = `No M8BF (${tickers} earnings)`; theme = "block"; crossed = true; blockT = "hard"; blockD = `M8BF not traded on MSFT/META earnings days`; badge = "BLOCKED"; strikeInfo = null;
      }
      else if (eom1) { rec = "No M8BF (EOM-1)"; theme = "block"; crossed = true; blockT = "hard"; blockD = "M8BF not traded on EOM-1 (no edge)"; badge = "BLOCKED"; strikeInfo = null; }
    }
    if (isPostVixBeforeOpex(etDate) && rec.startsWith("M8BF")) { rec = "No M8BF (post-VIX exp)"; theme = "block"; crossed = true; blockT = "hard"; blockD = "VIX exp before OPEX this month"; badge = "BLOCKED"; strikeInfo = null; }

    if (nmDay && !isMon && (rec.startsWith("M8BF") || rec.startsWith("No M8BF"))) { rec = "NM Straddle @ 9:32 AM"; theme = "strad"; crossed = false; blockT = ""; entryT = "9:32 AM"; badge = "NM STRADDLE"; strikeInfo = null; }
    if (eomDay) { rec = "Straddle @ 9:32 AM (EOM)"; theme = "strad"; crossed = false; blockT = ""; entryT = "9:32 AM"; badge = "EOM STRADDLE"; strikeInfo = null; }
    if (isWed && !fedDay && !eomDay && !nmDay && rec.startsWith("Straddle")) { rec = m8Msg(etDate); theme = "m8bf"; badge = "M8BF"; strikeInfo = m8Sched(dow); entryT = strikeInfo?.window || ""; }
    if (opexDay && rec.startsWith("Straddle")) { rec = "No Straddle (OPEX day)"; theme = "block"; crossed = true; blockT = "hard"; blockD = "Straddle not on OPEX"; badge = "BLOCKED"; }
    if (postOpMon && rec.startsWith("M8BF")) pmNote = true;

    // Win rate overrides: default unchecked → skip

    if (postOpDay) {
      const isM8 = rec.startsWith("M8BF"), isStr = rec.startsWith("Straddle") || rec.startsWith("NM Straddle");
      if (isM8 || isStr) {
        if (vixToday >= T.VIX_MAX_GXBF) { rec = `No GXBF (VIX ${vixToday} ≥ ${T.VIX_MAX_GXBF})`; theme = "block"; crossed = true; pmNote = false; blockT = "vix"; blockD = `OPEX+1 GXBF blocked, VIX ${vixToday}`; badge = "BLOCKED"; strikeInfo = null; }
        else { rec = "GXBF @ 9:36 AM (OPEX+1)"; theme = "gxbf"; crossed = false; pmNote = false; blockT = ""; entryT = "9:36 AM"; badge = "GXBF"; strikeInfo = null; }
      }
    }
  }

  if (pmNote) rec += " (afternoon times preferred)";

  // ── BOBF card logic ──
  const bobfBlocks = [];
  if (nmMon) bobfBlocks.push("NM Monday");
  if (vixExpDay) bobfBlocks.push("VIX expiration");
  if (opexDay) bobfBlocks.push("OPEX");
  if (opex1) bobfBlocks.push("OPEX-1");
  if (eom2) bobfBlocks.push("EOM-2");
  if (eom1) bobfBlocks.push("EOM-1");
  if (earningsDay) { const tickers = earningsSchedule.filter(e => e.date === todayLong(etDate)).map(e => e.ticker).join(','); bobfBlocks.push(`earnings (${tickers})`); }
  if (vixToday > T.VIX_MAX_BOBF) bobfBlocks.push("high VIX");

  let bobfRec, bobfBadge;
  if (bobfBlocks.length) {
    bobfRec = `No BOBF (${bobfBlocks.join(", ")})`;
    bobfBadge = "BLOCKED";
  } else {
    bobfRec = isFri ? "BOBF (Friday version)" : "BOBF in play";
    bobfBadge = isFri ? "FRIDAY VERSION" : "IN PLAY";
  }

  // ── Build dimmed card texts for inactive strategies ──
  let m8bfText = rec, stradText = rec, gxbfText = rec;

  if (theme === 'm8bf') {
    stradText = `No Straddle (${oNight <= 0 ? 'overnight VIX down' : 'overnight VIX drop > ' + T.DROP_GXBF})`;
    gxbfText = `No GXBF (overnight VIX drop ≤ ${T.DROP_GXBF})`;
  } else if (theme === 'strad') {
    m8bfText = `No M8BF (${oNight > 0 ? 'overnight VIX up' : 'open-to-open ≤ ' + T.O2O_M8BF})`;
    gxbfText = `No GXBF (overnight VIX drop ≤ ${T.DROP_GXBF})`;
  } else if (theme === 'gxbf') {
    m8bfText = `No M8BF (overnight VIX up)`;
    stradText = `No Straddle (overnight VIX drop > ${T.DROP_GXBF})`;
  } else if (theme === 'block') {
    // keep rec as-is for the blocked card
    if (rec.includes('M8BF')) {
      stradText = `No Straddle (${oNight <= 0 ? 'overnight VIX down' : 'overnight VIX drop > ' + T.DROP_GXBF})`;
      gxbfText = `No GXBF (overnight VIX drop ≤ ${T.DROP_GXBF})`;
    } else if (rec.includes('Straddle') || rec.includes('CPI') || rec.includes('Trade')) {
      m8bfText = `No M8BF`;
      gxbfText = `No GXBF`;
    } else if (rec.includes('GXBF')) {
      m8bfText = `No M8BF`;
      stradText = `No Straddle`;
    }
  }

  return {
    rec, theme, crossed, badge, entryT, blockT, blockD, pmNote,
    strikeInfo,
    m8bfText, stradText, gxbfText,
    bobfRec, bobfBadge, bobfBlocks,
    oNight, o2o,
    spxGapPct, spxGapForcesM8BF,
    dayLabel: tradeWdLabel(etDate),
    dateStr: todayLong(etDate),
    cpiDay, fedDay, opexDay, postOpDay: opexSch.some(ds => isTodayAfter(ds, etDate)), eomDay,
  };
}

// ════════════════════════════════════════════════════════════════════
// DISCORD MESSAGE BUILDER (ported from index.html discordBuildMessage)
// ════════════════════════════════════════════════════════════════════

function buildDiscordMessage(signal, vixValues) {
  const GRN = '\x1b[32m', RED = '\x1b[31m', DIM = '\x1b[2m', RST = '\x1b[0m';

  const isActive  = text => text && !text.startsWith('No ');
  const isBlocked = text => text && text.startsWith('No ');
  const sigColor  = text => isActive(text) ? GRN : isBlocked(text) ? RED : DIM;

  const m8bfDisplay = signal.m8bfText.replace(/^M8BF\s*[—-]\s*/, '').replace(/^M8BF$/, '—');
  const strikes = (signal.strikeInfo && signal.theme === 'm8bf') ? signal.strikeInfo.s.join('  ') : '';
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
    const forced = signal.spxGapForcesM8BF;
    inner += `${DIM}${'─'.repeat(34)}${RST}\n`;
    inner += `${forced ? RED : DIM}SPX Gap         │ ${dir}${Math.abs(signal.spxGapPct).toFixed(2)}%${forced ? ' ⚠️ M8BF forced' : ''}${RST}\n`;
  }

  return `\`\`\`ansi\n${inner}\`\`\`\n*Not financial advice. For informational purposes only.*`;
}

// ════════════════════════════════════════════════════════════════════
// SCHWAB TOKEN HELPERS
// ════════════════════════════════════════════════════════════════════

async function getAccessToken(env) {
  const tokensRaw = await env.SIGNAL_KV.get('schwab_tokens');
  if (!tokensRaw) throw new Error('No Schwab tokens in KV — sync from browser first');
  const tokens = JSON.parse(tokensRaw);

  // Check refresh token not expired
  if (Date.now() > tokens.refreshExpiry) {
    throw new Error('Schwab refresh token expired — re-authenticate in browser');
  }

  // Refresh access token if within 2 minutes of expiry
  if (Date.now() > tokens.expiry - 120000) {
    // Mutex: if a refresh is already in-flight, all concurrent callers share the same promise
    // so only ONE actual Schwab refresh call is made (Schwab refresh tokens are single-use)
    if (!_tokenRefreshPromise) {
      _tokenRefreshPromise = (async () => {
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

          const data = await resp.json();
          if (!data.access_token) throw new Error('Token refresh failed: ' + (data.error_description || JSON.stringify(data)));

          const newTokens = {
            access: data.access_token,
            refresh: data.refresh_token || tokens.refresh,
            expiry: Date.now() + (data.expires_in * 1000),
            // Update refreshExpiry whenever Schwab issues a new refresh token (7-day window resets)
            refreshExpiry: data.refresh_token
              ? Date.now() + (7 * 24 * 60 * 60 * 1000)
              : tokens.refreshExpiry,
          };
          await env.SIGNAL_KV.put('schwab_tokens', JSON.stringify(newTokens));
          return newTokens.access;
        } finally {
          _tokenRefreshPromise = null;
        }
      })();
    }
    return await _tokenRefreshPromise;
  }

  return tokens.access;
}

async function fetchSchwabJSON(url, token) {
  const resp = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  return resp.json();
}

// ════════════════════════════════════════════════════════════════════
// SCHEDULED HANDLER (Cron Trigger)
// ════════════════════════════════════════════════════════════════════

async function handleEOD(env, etNow) {
  const todayISO = `${etNow.getFullYear()}-${String(etNow.getMonth()+1).padStart(2,'0')}-${String(etNow.getDate()).padStart(2,'0')}`;
  const token = await getAccessToken(env);

  const end = Date.now();
  const start = end - 3 * 24 * 60 * 60 * 1000;
  const todayStr = etNow.toDateString();

  // Fetch VIX close
  let vixClose = null;
  try {
    const vixHist = await fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24VIX&periodType=day&period=3&frequencyType=minute&frequency=1&startDate=${start}&endDate=${end}&needExtendedHoursData=true`, token);
    if (vixHist.candles) {
      const todayCandles = vixHist.candles.filter(c => toET(new Date(c.datetime)).toDateString() === todayStr);
      todayCandles.sort((a, b) => a.datetime - b.datetime);
      // Last candle at or before 16:15
      const closeCandle = todayCandles.slice().reverse().find(c => {
        const d = toET(new Date(c.datetime));
        return d.getHours() * 60 + d.getMinutes() <= 16 * 60 + 15;
      });
      if (closeCandle) vixClose = parseFloat(closeCandle.close.toFixed(2));
    }
    // Fallback: quote closePrice
    if (vixClose === null) {
      const q = await fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/quotes?symbols=%24VIX&fields=quote`, token);
      const cp = q?.['$VIX']?.quote?.closePrice;
      if (cp) vixClose = parseFloat(cp.toFixed(2));
    }
  } catch (e) { /* non-fatal */ }

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
  } catch (e) { /* non-fatal */ }

  // Read today_trade.json from GitHub — written by live_updater.py at EOD
  let m8bfPL = null;
  try {
    const tradeResp = await fetch(
      `https://raw.githubusercontent.com/rava8989/brave/main/today_trade.json?t=${Date.now()}`,
      { headers: { 'Cache-Control': 'no-cache' } }
    );
    if (tradeResp.ok) {
      const trade = await tradeResp.json();
      if (
        trade.date === todayISO &&
        trade.status === 'closed' &&
        typeof trade.final_pl === 'number'
      ) {
        m8bfPL = trade.final_pl;
      }
    }
  } catch (e) { /* non-fatal */ }

  const fields = {};
  if (vixClose != null) fields.vixClose = vixClose;
  if (spxClose != null) fields.spxClose = spxClose;
  if (m8bfPL != null) fields.m8bfPL = m8bfPL;

  if (Object.keys(fields).length > 0) {
    await upsertHistoryGitHub(env, todayISO, fields, m8bfPL != null);
  }

  return { status: 'eod', date: todayISO, vixClose, spxClose, m8bfPL };
}

// ════════════════════════════════════════════════════════════════════
// DISCORD SIGNAL POLLING
// ════════════════════════════════════════════════════════════════════

function parseDiscordSignal(content) {
  // Format: BUY +1 Butterfly SPX 100 ... 6455/6405/6355 PUT @14.25 LMT
  const strikeMatch = content.match(/BUY \+1 Butterfly SPX[^/]*(\d{4,5})\/(\d{4,5})\/(\d{4,5})[^@]*@([\d.]+)/i);
  if (!strikeMatch) return null;
  const upper = parseInt(strikeMatch[1]);
  const center = parseInt(strikeMatch[2]);
  const lower = parseInt(strikeMatch[3]);
  const premium = parseFloat(strikeMatch[4]);
  if (isNaN(center) || isNaN(premium)) return null;
  // T1 from "Target 1: XXXX"
  const t1Match = content.match(/Target\s*1[:\s]+(\d{4,5})/i);
  const t1 = t1Match ? parseInt(t1Match[1]) : center + 5;
  return { center, upper, lower, t1, premium };
}

function isBanned(center, t1) {
  const FULL_BANS = new Set([10, 25, 35, 40, 65, 80]);
  const COMBO_BANS = { 0: 95, 20: 15, 55: 50, 65: 60, 85: 90 };
  if (FULL_BANS.has(center % 100)) return true;
  const t1Mod = t1 % 100;
  if (COMBO_BANS[t1Mod] !== undefined && center % 100 === COMBO_BANS[t1Mod]) return true;
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

  const seenCenters = new Set(signals.map(s => s.center));
  let newCount = 0;

  for (const msg of messages) {
    const msgET = toET(new Date(msg.timestamp));
    const msgISO = `${msgET.getFullYear()}-${String(msgET.getMonth()+1).padStart(2,'0')}-${String(msgET.getDate()).padStart(2,'0')}`;
    if (msgISO !== todayISO) continue;

    const sig = parseDiscordSignal(msg.content || '');
    if (!sig || seenCenters.has(sig.center)) continue;

    seenCenters.add(sig.center);
    signals.push({
      time: `${String(msgET.getHours()).padStart(2,'0')}:${String(msgET.getMinutes()).padStart(2,'0')}`,
      center: sig.center,
      t1: sig.t1,
      premium: sig.premium,
      banned: isBanned(sig.center, sig.t1),
    });
    newCount++;
  }

  await env.SIGNAL_KV.put('signals_today', JSON.stringify({ date: todayISO, signals }));
  return { polled: true, newSignals: newCount, total: signals.length };
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
  const isMorning = etHour === 9  && etMin >= 30 && etMin <= 40;
  const isEOD     = etHour === 16 && etMin >= 0  && etMin <= 15;
  const isMarket  = (etHour > 9 || (etHour === 9 && etMin >= 30)) && etHour < 16;

  // Always poll Discord during market hours
  let discordResult = {};
  if (isMarket && env.DISCORD_USER_TOKEN) {
    discordResult = await pollDiscordSignals(env);
  }

  // EOD cron: capture vixClose + spxClose + m8bfPL
  if (isEOD) {
    const eodResult = await handleEOD(env, etNow);
    return { ...eodResult, discord: discordResult };
  }

  if (!isMorning) {
    return { status: 'discord_poll', discord: discordResult, time: `${etHour}:${String(etMin).padStart(2,'0')} ET` };
  }

  // 1. Get access token
  const token = await getAccessToken(env);

  // 2. Fetch VIX 5-day history → yesterday open + close
  const end = Date.now();
  const start = end - 5 * 24 * 60 * 60 * 1000;
  const vixHistUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24VIX&periodType=day&period=5&frequencyType=minute&frequency=1&startDate=${start}&endDate=${end}&needExtendedHoursData=true`;
  const vixHist = await fetchSchwabJSON(vixHistUrl, token);
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

  // Override yClose with quote's closePrice (official previous close)
  try {
    const qData = await fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/quotes?symbols=%24VIX&fields=quote`, token);
    const qClose = qData?.['$VIX']?.quote?.closePrice;
    if (qClose) vixYClose = parseFloat(qClose.toFixed(2));
  } catch (e) { /* fall back to candle close */ }

  if (vixYClose === null) throw new Error('Could not determine yesterday VIX close');

  // 3. Get today's VIX open from candles — first 1-min candle at or after 9:30 ET
  let vixToday = null;
  const todayCandles = candles.filter(c => {
    const d = toET(new Date(c.datetime));
    if (d.toDateString() !== todayStr) return false;
    const mins = d.getHours() * 60 + d.getMinutes();
    return mins >= 570; // 9:30 ET = 570 minutes
  });
  todayCandles.sort((a, b) => a.datetime - b.datetime);
  if (todayCandles.length > 0) {
    // Use the open of the very first candle at or after 9:30 ET
    vixToday = parseFloat(todayCandles[0].open.toFixed(2));
  }

  // Fallback: re-fetch with fresh history if candle not yet in first response
  if (vixToday === null) {
    await new Promise(r => setTimeout(r, 15000)); // wait 15s for candle to appear
    const retry = await fetchSchwabJSON(vixHistUrl, token);
    if (retry.candles) {
      const rc = retry.candles.filter(c => {
        const d = toET(new Date(c.datetime));
        return d.toDateString() === todayStr && d.getHours() * 60 + d.getMinutes() >= 570;
      }).sort((a, b) => a.datetime - b.datetime);
      if (rc.length > 0) vixToday = parseFloat(rc[0].open.toFixed(2));
    }
  }

  // Last resort: quote API (less accurate but better than failing)
  if (vixToday === null) {
    const vixQuote = await fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/quotes?symbols=%24VIX&fields=quote`, token);
    const quote = vixQuote?.['$VIX']?.quote;
    if (quote?.openPrice) vixToday = parseFloat(quote.openPrice.toFixed(2));
    else if (quote?.lastPrice) vixToday = parseFloat(quote.lastPrice.toFixed(2));
  }

  if (vixToday === null) throw new Error('Could not get VIX today open');

  // 4. Fetch SPX quote → gap % + today's SPX open
  let spxGapPct = null;
  let spxTodayOpen = null;
  try {
    // Get SPX yesterday close from history
    const spxHistUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24SPX&periodType=day&period=5&frequencyType=minute&frequency=1&startDate=${start}&endDate=${end}&needExtendedHoursData=true`;
    const spxHist = await fetchSchwabJSON(spxHistUrl, token);
    let spxYClose = null;
    if (spxHist.candles && yDate) {
      const spxYCandles = spxHist.candles.filter(c => toET(new Date(c.datetime)).toDateString() === yDate);
      spxYCandles.sort((a, b) => a.datetime - b.datetime);
      const spxCloseCandle = spxYCandles.slice().reverse().find(c => {
        const d = toET(new Date(c.datetime));
        return d.getHours() === 16 && d.getMinutes() >= 10 && d.getMinutes() <= 15;
      }) || (spxYCandles.length ? spxYCandles[spxYCandles.length - 1] : null);
      if (spxCloseCandle) spxYClose = spxCloseCandle.close;
    }

    // Get SPX today open from quote
    const spxQuoteUrl = `https://api.schwabapi.com/marketdata/v1/quotes?symbols=%24SPX&fields=quote`;
    const spxQuote = await fetchSchwabJSON(spxQuoteUrl, token);
    const spxQ = spxQuote?.['$SPX']?.quote;
    spxTodayOpen = spxQ ? parseFloat((spxQ.openPrice || spxQ.lastPrice).toFixed(2)) : null;

    if (spxYClose && spxTodayOpen) {
      spxGapPct = ((spxTodayOpen - spxYClose) / spxYClose) * 100;
    }
  } catch (e) { /* SPX gap is optional */ }

  // 4b. Write vixOpen + spxOpen to history_data.json via GitHub API
  const todayISO = `${etNow.getFullYear()}-${String(etNow.getMonth()+1).padStart(2,'0')}-${String(etNow.getDate()).padStart(2,'0')}`;
  try {
    await upsertHistoryGitHub(env, todayISO, {
      vixOpen: vixToday,
      ...(spxTodayOpen != null ? { spxOpen: spxTodayOpen } : {}),
    });
  } catch (e) { /* non-fatal — Discord still fires */ }

  // 5. Calculate signal
  const signal = calculateSignal({
    vixToday,
    vixYOpen,
    vixYClose,
    spxGapPct,
    etDate: etNow,
  });

  // 6. Build Discord message
  const vixValues = { yOpen: vixYOpen, yClose: vixYClose, todayOpen: vixToday };
  const message = buildDiscordMessage(signal, vixValues);

  // 7. Post to Discord
  const dcRaw = await env.SIGNAL_KV.get('discord_config');
  if (!dcRaw) throw new Error('No Discord config in KV — sync from browser');
  const dc = JSON.parse(dcRaw);

  // Validate proxyUrl against allowlist — only Discord webhook URLs are permitted
  const discordWebhookPattern = /^https:\/\/discord\.com\/api\/webhooks\/[^/?#]+\/[^/?#]+$/;
  if (!dc.proxyUrl || !discordWebhookPattern.test(dc.proxyUrl)) {
    throw new Error('Invalid proxyUrl: must be a Discord webhook URL (https://discord.com/api/webhooks/*)');
  }

  const dcResp = await fetch(dc.proxyUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ userId: dc.channelId, message }),
  });
  const dcData = await dcResp.json();
  if (!dcResp.ok) throw new Error('Discord post failed: ' + JSON.stringify(dcData));

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
// GITHUB HISTORY UPSERT
// Reads history_data.json from GitHub, merges fields for dateStr,
// commits back. Requires GITHUB_TOKEN env var (repo: rava8989/brave).
// ════════════════════════════════════════════════════════════════════

async function upsertHistoryGitHub(env, dateStr, fields, computeWR = false) {
  const token = env.GITHUB_TOKEN;
  if (!token) throw new Error('GITHUB_TOKEN not set');

  const apiUrl = 'https://api.github.com/repos/rava8989/brave/contents/history_data.json';
  const headers = {
    'Authorization': `Bearer ${token}`,
    'Accept': 'application/vnd.github+json',
    'User-Agent': 'schwab-proxy-worker/1.0',
    'X-GitHub-Api-Version': '2022-11-28',
  };

  // 1. Fetch current file
  const getResp = await fetch(apiUrl, { headers });
  if (!getResp.ok) throw new Error(`GitHub GET failed: ${getResp.status}`);
  const meta = await getResp.json();
  const sha = meta.sha;
  const content = JSON.parse(atob(meta.content.replace(/\n/g, '')));

  // 2. Upsert today's row
  const idx = content.findIndex(r => r.date === dateStr);
  if (idx >= 0) {
    // Only overwrite fields that are still null/missing (don't clobber m8bfPL etc.)
    for (const [k, v] of Object.entries(fields)) {
      if (content[idx][k] == null) content[idx][k] = v;
    }
  } else {
    content.push({ date: dateStr, ...fields });
    content.sort((a, b) => a.date.localeCompare(b.date));
  }

  // 2b. Compute rolling 20-day M8BF win rate if P&L was just written
  if (computeWR) {
    const finalIdx = content.findIndex(r => r.date === dateStr);
    if (finalIdx >= 0 && content[finalIdx].m8bfPL != null) {
      const WINDOW = 20;
      const signalDays = content
        .filter(r => r.m8bfPL != null && r.date <= dateStr)
        .sort((a, b) => a.date.localeCompare(b.date))
        .slice(-WINDOW);
      if (signalDays.length > 0) {
        const wins = signalDays.filter(r => r.m8bfPL >= 0).length;
        content[finalIdx].m8bfWR = Math.round(wins / signalDays.length * 100);
      }
    }
  }

  // 3. Push back
  const putResp = await fetch(apiUrl, {
    method: 'PUT',
    headers: { ...headers, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      message: `auto: history update for ${dateStr} (${Object.keys(fields).join(', ')})`,
      content: btoa(JSON.stringify(content, null, 0)),
      sha,
    }),
  });
  if (!putResp.ok) {
    const err = await putResp.text();
    throw new Error(`GitHub PUT failed: ${putResp.status} — ${err}`);
  }
}

// ════════════════════════════════════════════════════════════════════
// WORKER EXPORT
// ════════════════════════════════════════════════════════════════════

export default {
  async fetch(request, env) {
    const origin = request.headers.get('Origin') || '';
    if (!env.ALLOWED_ORIGIN) {
      console.error('ALLOWED_ORIGIN env var is not set — all cross-origin requests will be blocked');
    }
    const allowed = env.ALLOWED_ORIGIN || 'null';
    const corsOk = origin !== '' && origin === allowed;

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
        const vixUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24VIX&periodType=day&period=1&frequencyType=minute&frequency=1&startDate=${dayStart}&endDate=${dayEnd}&needExtendedHoursData=false`;
        const spxUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24SPX&periodType=day&period=1&frequencyType=minute&frequency=1&startDate=${dayStart}&endDate=${dayEnd}&needExtendedHoursData=false`;
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

    // Preflight
    if (request.method === 'OPTIONS') {
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
      if (url.pathname.startsWith('/market/') && request.method === 'GET') {
        const subpath = url.pathname.slice('/market'.length);
        // Allowlist: only permit safe read-only market data endpoints
        const ALLOWED = ['/pricehistory', '/quotes', '/chains', '/markets', '/instruments'];
        if (!ALLOWED.some(p => subpath.startsWith(p))) {
          return jsonResp({ error: 'Path not allowed' }, 403, corsHeaders);
        }
        const upstream = `https://api.schwabapi.com/marketdata/v1${subpath}${url.search}`;

        const resp = await fetch(upstream, {
          headers: {
            'Authorization': request.headers.get('Authorization') || '',
          },
        });

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

      return jsonResp({ error: 'Not found' }, 404, corsHeaders);
    } catch (e) {
      return jsonResp({ error: e.message }, 500, corsHeaders);
    }
  },

  async scheduled(event, env, ctx) {
    let result;
    try {
      result = await handleScheduled(env);
    } catch (e) {
      result = { status: 'error', error: e.message, date: new Date().toISOString() };
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

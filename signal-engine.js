/**
 * SIGNAL ENGINE — single source of truth for M8BF/Straddle/GXBF/BOBF logic.
 *
 * Used by BOTH:
 *   - schwab-proxy.js (Cloudflare worker → Discord message)
 *   - index.html      (browser calculator)
 *   - history.html    (history page)
 *
 * Loaded in browser via: <script type="module">
 * Loaded in worker via:  import { ... } from './signal-engine.js'
 *
 * NEVER duplicate this logic elsewhere. If signal rules change, they change HERE.
 */

// ════════════════════════════════════════════════════════════════════
// SCHEDULES (calendar data)
// ════════════════════════════════════════════════════════════════════

export const cpiSch   = ["January 11, 2024","February 13, 2024","March 12, 2024","April 10, 2024","May 15, 2024","June 12, 2024","July 11, 2024","August 14, 2024","September 11, 2024","October 10, 2024","November 13, 2024","December 11, 2024","January 15, 2025","February 12, 2025","March 12, 2025","April 10, 2025","May 13, 2025","June 11, 2025","July 15, 2025","August 12, 2025","September 11, 2025","October 24, 2025","December 18, 2025","January 13, 2026","February 13, 2026","March 11, 2026","April 10, 2026","May 12, 2026","June 10, 2026","July 14, 2026","August 12, 2026","September 11, 2026","October 14, 2026","November 10, 2026","December 10, 2026"];
export const fedSch   = ["January 31, 2024","March 20, 2024","May 1, 2024","June 12, 2024","July 31, 2024","September 18, 2024","November 7, 2024","December 18, 2024","January 29, 2025","March 19, 2025","May 7, 2025","June 18, 2025","July 30, 2025","September 17, 2025","October 29, 2025","December 10, 2025","January 28, 2026","March 18, 2026","April 29, 2026","June 17, 2026","July 29, 2026","September 16, 2026","October 28, 2026","December 9, 2026"];
export const opexSch  = ["January 19, 2024","February 16, 2024","March 15, 2024","April 19, 2024","May 17, 2024","June 21, 2024","July 19, 2024","August 16, 2024","September 20, 2024","October 18, 2024","November 15, 2024","December 20, 2024","January 17, 2025","February 21, 2025","March 21, 2025","April 17, 2025","May 16, 2025","June 20, 2025","July 18, 2025","August 15, 2025","September 19, 2025","October 17, 2025","November 21, 2025","December 19, 2025","January 16, 2026","February 20, 2026","March 20, 2026","April 17, 2026","May 15, 2026","June 18, 2026","July 17, 2026","August 21, 2026","September 18, 2026","October 16, 2026","November 20, 2026","December 18, 2026"];
export const holidays = ["January 1, 2024","January 15, 2024","February 19, 2024","March 29, 2024","May 27, 2024","June 19, 2024","July 4, 2024","September 2, 2024","November 28, 2024","December 25, 2024","January 1, 2025","January 9, 2025","January 20, 2025","February 17, 2025","April 18, 2025","May 26, 2025","June 19, 2025","July 4, 2025","September 1, 2025","November 27, 2025","December 25, 2025","January 1, 2026","January 19, 2026","February 16, 2026","April 3, 2026","May 25, 2026","June 19, 2026","July 3, 2026","September 7, 2026","November 26, 2026","December 25, 2026"];
export const vixSch   = ["January 17, 2024","February 14, 2024","March 20, 2024","April 17, 2024","May 22, 2024","June 18, 2024","July 17, 2024","August 21, 2024","September 18, 2024","October 16, 2024","November 20, 2024","December 18, 2024","January 22, 2025","February 19, 2025","March 18, 2025","April 16, 2025","May 21, 2025","June 18, 2025","July 16, 2025","August 20, 2025","September 17, 2025","October 22, 2025","November 19, 2025","December 17, 2025","January 21, 2026","February 18, 2026","March 18, 2026","April 15, 2026","May 19, 2026","June 17, 2026","July 22, 2026","August 19, 2026","September 16, 2026","October 21, 2026","November 18, 2026","December 16, 2026"];

export const earningsSchedule = [
  // ── 2024 (historical, confirmed) ──
  { date:"January 24, 2024",  company:"Tesla",     ticker:"TSLA", timing:"AH", confirmed:true },
  { date:"January 30, 2024",  company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:true },
  { date:"January 30, 2024",  company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:true },
  { date:"February 1, 2024",  company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:true },
  { date:"February 1, 2024",  company:"Meta",      ticker:"META", timing:"AH", confirmed:true },
  { date:"February 1, 2024",  company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:true },
  { date:"February 21, 2024", company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:true },
  { date:"April 23, 2024",    company:"Tesla",     ticker:"TSLA", timing:"AH", confirmed:true },
  { date:"April 24, 2024",    company:"Meta",      ticker:"META", timing:"AH", confirmed:true },
  { date:"April 25, 2024",    company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:true },
  { date:"April 25, 2024",    company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:true },
  { date:"April 30, 2024",    company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:true },
  { date:"May 2, 2024",       company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:true },
  { date:"May 22, 2024",      company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:true },
  { date:"July 23, 2024",     company:"Tesla",     ticker:"TSLA", timing:"AH", confirmed:true },
  { date:"July 23, 2024",     company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:true },
  { date:"July 30, 2024",     company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:true },
  { date:"July 31, 2024",     company:"Meta",      ticker:"META", timing:"AH", confirmed:true },
  { date:"August 1, 2024",    company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:true },
  { date:"August 1, 2024",    company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:true },
  { date:"August 28, 2024",   company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:true },
  { date:"October 23, 2024",  company:"Tesla",     ticker:"TSLA", timing:"AH", confirmed:true },
  { date:"October 29, 2024",  company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:true },
  { date:"October 30, 2024",  company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:true },
  { date:"October 30, 2024",  company:"Meta",      ticker:"META", timing:"AH", confirmed:true },
  { date:"October 31, 2024",  company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:true },
  { date:"October 31, 2024",  company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:true },
  { date:"November 20, 2024", company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:true },
  // ── 2025 (historical, confirmed) ──
  { date:"January 29, 2025",  company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:true },
  { date:"January 29, 2025",  company:"Meta",      ticker:"META", timing:"AH", confirmed:true },
  { date:"January 29, 2025",  company:"Tesla",     ticker:"TSLA", timing:"AH", confirmed:true },
  { date:"January 30, 2025",  company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:true },
  { date:"February 4, 2025",  company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:true },
  { date:"February 6, 2025",  company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:true },
  { date:"February 26, 2025", company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:true },
  { date:"April 22, 2025",    company:"Tesla",     ticker:"TSLA", timing:"AH", confirmed:true },
  { date:"April 24, 2025",    company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:true },
  { date:"April 30, 2025",    company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:true },
  { date:"April 30, 2025",    company:"Meta",      ticker:"META", timing:"AH", confirmed:true },
  { date:"May 1, 2025",       company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:true },
  { date:"May 1, 2025",       company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:true },
  { date:"May 28, 2025",      company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:true },
  { date:"July 23, 2025",     company:"Tesla",     ticker:"TSLA", timing:"AH", confirmed:true },
  { date:"July 23, 2025",     company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:true },
  { date:"July 30, 2025",     company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:true },
  { date:"July 30, 2025",     company:"Meta",      ticker:"META", timing:"AH", confirmed:true },
  { date:"July 31, 2025",     company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:true },
  { date:"July 31, 2025",     company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:true },
  { date:"August 27, 2025",   company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:true },
  { date:"October 22, 2025",  company:"Tesla",     ticker:"TSLA", timing:"AH", confirmed:true },
  { date:"October 29, 2025",  company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:true },
  { date:"October 29, 2025",  company:"Meta",      ticker:"META", timing:"AH", confirmed:true },
  { date:"October 29, 2025",  company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:true },
  { date:"October 30, 2025",  company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:true },
  { date:"October 30, 2025",  company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:true },
  { date:"November 19, 2025", company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:true },
  // ── Q1 2026 (Jan/Feb reports) ──
  { date:"January 28, 2026",  company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:true },
  { date:"January 28, 2026",  company:"Meta",      ticker:"META", timing:"AH", confirmed:true },
  { date:"January 29, 2026",  company:"Tesla",     ticker:"TSLA", timing:"AH", confirmed:true },
  { date:"January 29, 2026",  company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:true },
  { date:"February 4, 2026",  company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:true },
  { date:"February 5, 2026",  company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:true },
  { date:"February 26, 2026", company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:true },
  // ── Q2 2026 (Apr/May reports) ──
  { date:"April 22, 2026",    company:"Tesla",     ticker:"TSLA", timing:"AH", confirmed:true  },
  { date:"April 29, 2026",    company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:true  },
  { date:"April 29, 2026",    company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:true  },
  { date:"April 29, 2026",    company:"Meta",      ticker:"META", timing:"AH", confirmed:true  },
  { date:"April 29, 2026",    company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:true  },
  { date:"April 30, 2026",    company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:true  },
  { date:"May 20, 2026",      company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:true  },
  // ── Q3 2026 (Jul/Aug reports — estimates) ──
  { date:"July 22, 2026",     company:"Tesla",     ticker:"TSLA", timing:"AH", confirmed:false },
  { date:"July 23, 2026",     company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:false },
  { date:"July 28, 2026",     company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:false },
  { date:"July 29, 2026",     company:"Meta",      ticker:"META", timing:"AH", confirmed:false },
  { date:"July 30, 2026",     company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:false },
  { date:"July 30, 2026",     company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:false },
  { date:"August 19, 2026",   company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:false },
  // ── Q4 2026 (Oct/Nov reports — estimates) ──
  { date:"October 21, 2026",  company:"Tesla",     ticker:"TSLA", timing:"AH", confirmed:false },
  { date:"October 22, 2026",  company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:false },
  { date:"October 27, 2026",  company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:false },
  { date:"October 28, 2026",  company:"Meta",      ticker:"META", timing:"AH", confirmed:false },
  { date:"October 29, 2026",  company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:false },
  { date:"October 29, 2026",  company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:false },
  { date:"November 18, 2026", company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:false },
];

// ════════════════════════════════════════════════════════════════════
// THRESHOLDS
// ════════════════════════════════════════════════════════════════════

export const T = {
  DROP_GXBF: 0.65,
  O2O_M8BF: 1.4, VIX_MAX_GXBF: 25, VIX_MAX_BOBF: 23,
  SPX_GAP_THRESHOLD: 0.9,
};

// ════════════════════════════════════════════════════════════════════
// DATE HELPERS (ET-based)
// ════════════════════════════════════════════════════════════════════

export function toET(d = new Date()) {
  return new Date(d.toLocaleString('en-US', { timeZone: 'America/New_York' }));
}

export function dateLong(d) {
  return d.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' });
}

export function todayLong(etDate) { return dateLong(etDate); }

export const isWkend = d => { const w = d.getDay(); return w === 0 || w === 6; };
export const isHol   = d => holidays.includes(dateLong(d));
export const isTrade = d => !isWkend(d) && !isHol(d);

export function nextTrade(d) { const n = new Date(d); do { n.setDate(n.getDate() + 1); } while (isWkend(n) || isHol(n)); return n; }
export function prevTrade(d) { const p = new Date(d); do { p.setDate(p.getDate() - 1); } while (isWkend(p) || isHol(p)); return p; }

export function isTodayAfter(ds, etDate) {
  const d = new Date(ds); if (isNaN(d)) return false;
  const a = nextTrade(d);
  return a.getFullYear() === etDate.getFullYear() && a.getMonth() === etDate.getMonth() && a.getDate() === etDate.getDate();
}
export function isTodayBefore(ds, etDate) {
  const d = new Date(ds); if (isNaN(d)) return false;
  const b = prevTrade(d);
  return b.getFullYear() === etDate.getFullYear() && b.getMonth() === etDate.getMonth() && b.getDate() === etDate.getDate();
}

export function parseLong(s) { const d = new Date(s); return isNaN(d) ? null : new Date(d.getFullYear(), d.getMonth(), d.getDate(), 12); }
export function schedInMonth(list, ref) {
  const y = ref.getFullYear(), m = ref.getMonth();
  for (const s of list) { const d = parseLong(s); if (d && d.getFullYear() === y && d.getMonth() === m) return d; }
  return null;
}

export function isVixAfterOpexDay(ref) {
  if (!vixSch.includes(todayLong(ref))) return false;
  const vx = schedInMonth(vixSch, ref), op = schedInMonth(opexSch, ref);
  if (!vx || !op) return false;
  return vx > op;
}

export const isPostOpexMon = (etDate) => opexSch.some(ds => isTodayAfter(ds, etDate)) && etDate.getDay() === 1;
export const isLastTradeMo = (d) => nextTrade(d).getMonth() !== d.getMonth();
export function isEomN(n, d) { const f = new Date(d.getFullYear(), d.getMonth() + 1, 1); let t = prevTrade(f); for (let i = 0; i < n; i++) t = prevTrade(t); return t.getFullYear() === d.getFullYear() && t.getMonth() === d.getMonth() && t.getDate() === d.getDate(); }
export const isFirstTradeMo = (d) => prevTrade(d).getMonth() !== d.getMonth();
export function isFirstTradeMon(d) { return isFirstTradeMo(d) && d.getDay() === 1; }

// ════════════════════════════════════════════════════════════════════
// M8BF SCHEDULE + HELPERS
// ════════════════════════════════════════════════════════════════════

export function m8Sched(dow) {
  const blocked = ["10","25","35","40","65","80"];
  const comboBans = {0:95,20:15,55:50,65:60,85:90};
  switch (dow) {
    case 1: return { t: "11:00", window: "11:00–11:30", blocked, comboBans };
    case 2: return { t: "13:30", window: "13:30–14:00", blocked, comboBans };
    case 3: return { t: "12:00", window: "12:00–12:30", blocked, comboBans };
    case 4: return { t: "11:00", window: "11:00–11:30", blocked, comboBans };
    case 5: return { t: "13:00", window: "13:00–13:30", blocked, comboBans };
    default: return null;
  }
}
export function m8Msg(d) { const sc = m8Sched(d.getDay()); return sc ? `M8BF — Window ${sc.window}` : "M8BF"; }

// ════════════════════════════════════════════════════════════════════
// DAY-OF-WEEK LABELING
// ════════════════════════════════════════════════════════════════════

export function ordinal(n) { const s = ["th", "st", "nd", "rd"], v = n % 100; return n + (s[(v - 20) % 10] || s[v] || s[0]); }
export function wdName(d) { return ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"][d] || ""; }
export function tradeWdLabel(d) {
  const t = new Date(d.getFullYear(), d.getMonth(), d.getDate());
  const dow = t.getDay(), ms = new Date(t.getFullYear(), t.getMonth(), 1);
  let c = 0;
  for (let x = new Date(ms); x <= t; x.setDate(x.getDate() + 1)) { if (!isTrade(x)) continue; if (x.getDay() === dow) c++; }
  if (!isTrade(t)) return `${wdName(dow)} (market closed)`;
  return `${ordinal(c)} ${wdName(dow)}`;
}

// ════════════════════════════════════════════════════════════════════
// EARNINGS HELPERS
// ════════════════════════════════════════════════════════════════════

export function isEarningsDay(etDate) { return earningsSchedule.some(e => e.date === todayLong(etDate)); }
export function isNonAmznTslaEarningsDay(etDate) { return earningsSchedule.some(e => e.date === todayLong(etDate) && e.ticker !== 'AMZN' && e.ticker !== 'TSLA'); }
export function isDayAfterAnyEarnings(etDate) { return earningsSchedule.some(e => isTodayAfter(e.date, etDate)); }

// ════════════════════════════════════════════════════════════════════
// DIAGONAL SIGNAL (companion strategy, single source of truth)
// Consumed by: schwab-proxy.js (via calculateSignal), index.html (direct)
// ────────────────────────────────────────────────────────────────────
// Canonical 7-filter stack — priority:
//   OPEX-1 > NM > EARN_MEGA(AAPL/MSFT/TSLA/META) > VIX_MID (50–80%)
// Mirrors compute_diagonal_pnl.py DEFAULT_PARAMS.special_active exactly.
// CPI / FED / EOM / ALL_EARNINGS intentionally NOT in the stack.
// Entry 12:30–15:00 ET (window; pick any clock time, must equal Exit Time).
// Exit 12:30–15:00 ET next trading day · 1/25 DTE · short +10 ITM · long −20 below · ±5 pt tol.
// Entry and exit must use the SAME wall-clock time so only one diagonal is live at a time.
// ════════════════════════════════════════════════════════════════════
const DIAG_EARN_TICKERS = new Set(['AAPL', 'MSFT', 'TSLA', 'META']);
export function computeDiagonalSignal(etDate, vixPct20d = null) {
  const opex1 = opexSch.some(ds => isTodayBefore(ds, etDate));
  const nmDay = isFirstTradeMo(etDate);
  const earnMegaTickers = earningsSchedule
    .filter(e => e.date === todayLong(etDate) && DIAG_EARN_TICKERS.has(e.ticker))
    .map(e => e.ticker);
  const earnMega = earnMegaTickers.length > 0;

  let diagText, diagBadge = '…', diagGo = false, diagSkipCode = null;
  if (opex1) {
    diagSkipCode = 'OPEX-1';
    diagText = 'No Diagonal (OPEX-1)';
    diagBadge = 'SKIP';
  } else if (nmDay) {
    diagSkipCode = 'NM';
    diagText = 'No Diagonal (NM)';
    diagBadge = 'SKIP';
  } else if (earnMega) {
    diagSkipCode = 'EARN';
    diagText = `No Diagonal (earnings: ${earnMegaTickers.join(',')})`;
    diagBadge = 'SKIP';
  } else if (vixPct20d !== null && vixPct20d !== undefined && vixPct20d > 50 && vixPct20d <= 80) {
    diagSkipCode = 'VIX_MID';
    diagText = `No Diagonal (VIX 20d ${vixPct20d}% — dead zone)`;
    diagBadge = 'SKIP';
  } else if (vixPct20d === null || vixPct20d === undefined) {
    // All calendar filters cleared but no VIX percentile yet — waiting state.
    diagText = 'Diagonal pending VIX 20d data';
    diagBadge = '…';
  } else {
    // All filters cleared → GO.
    diagGo = true;
    const band = vixPct20d <= 50 ? 'calm' : 'panic';
    diagText = `Diagonal 12:30–15:00 ET window (VIX 20d ${vixPct20d}% — ${band} edge)`;
    diagBadge = '⏰ 12:30–15:00 ET';
  }

  return { diagText, diagBadge, diagGo, diagSkipCode, vixPct20d };
}

// ════════════════════════════════════════════════════════════════════
// SIGNAL CALCULATION (single source of truth)
// Consumed by: schwab-proxy.js, index.html, history.html
// ════════════════════════════════════════════════════════════════════

export function calculateSignal({ vixToday, vixYOpen, vixYClose, spxGapPct, etDate, prevWR = null, vixPct20d = null }) {
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

  const o2o = (vixYOpen != null) ? vixYOpen - vixToday : NaN;
  const oNight = vixYClose - vixToday;

  const spxGapCancelsStrad = (spxGapPct !== null && spxGapPct !== undefined && Math.abs(spxGapPct) >= T.SPX_GAP_THRESHOLD);
  const vixExpAfterOpex = isVixAfterOpexDay(etDate);
  const nonAmznTslaEarn = isNonAmznTslaEarningsDay(etDate);
  const m8bfBanned = eomDay || eom1 || opex1 || vixExpAfterOpex || nonAmznTslaEarn;

  let rec = "", theme = "neutral", crossed = false, pmNote = false;
  let blockT = "", blockD = "", entryT = "", badge = "";
  let strikeInfo = null;
  let cpiLongCall = false;

  if (cpiDay) {
    rec = "No trades (CPI day)";
    theme = "block";
    crossed = true;
    blockT = "cpi-day";
    blockD = "CPI day — all strategies blocked";
    badge = "BLOCKED";
    strikeInfo = null;
  } else {
    if (oNight > T.DROP_GXBF) {
      if (vixToday >= T.VIX_MAX_GXBF) { rec = `No GXBF (VIX ${vixToday} ≥ ${T.VIX_MAX_GXBF})`; theme = "block"; crossed = true; blockT = "vix"; blockD = `VIX ${vixToday} ≥ ${T.VIX_MAX_GXBF}`; badge = "BLOCKED"; }
      else { rec = `GXBF @ 9:36 AM`; theme = "gxbf"; entryT = "9:36 AM"; badge = "GXBF"; }
    } else if (oNight > 0) {
      rec = "Straddle @ 9:32 AM"; theme = "strad"; entryT = "9:32 AM"; badge = "STRADDLE";
    } else {
      rec = m8Msg(etDate); theme = "m8bf"; badge = "M8BF"; strikeInfo = m8Sched(dow); entryT = strikeInfo?.window || "";
    }

    if (rec.startsWith("M8BF")) {
      if (eomDay) { rec = "No M8BF (EOM)"; theme = "block"; crossed = true; blockT = "hard"; blockD = "M8BF not traded on EOM"; badge = "BLOCKED"; strikeInfo = null; }
      else if (eom1) { rec = "No M8BF (EOM-1)"; theme = "block"; crossed = true; blockT = "hard"; blockD = "M8BF not traded on EOM-1"; badge = "BLOCKED"; strikeInfo = null; }
      else if (opex1) { rec = "No M8BF (day before OPEX)"; theme = "block"; crossed = true; blockT = "hard"; blockD = "Skipped day before OPEX"; badge = "BLOCKED"; strikeInfo = null; }
      else if (nonAmznTslaEarn) { rec = "No M8BF (earnings)"; theme = "block"; crossed = true; blockT = "hard"; blockD = "Not traded on earnings days (except AMZN/TSLA)"; badge = "BLOCKED"; strikeInfo = null; }
      else if (vixExpAfterOpex) { rec = "No M8BF (VIX exp day)"; theme = "block"; crossed = true; blockT = "hard"; blockD = "VIX exp day when VIX exp falls after OPEX"; badge = "BLOCKED"; strikeInfo = null; }
    }

    if (nmDay && !isMon && (rec.startsWith("M8BF") || rec.startsWith("No M8BF") || rec.startsWith("Straddle") || rec.startsWith("GXBF") || rec.startsWith("No GXBF"))) { rec = "NM Straddle @ 9:32 AM"; theme = "strad"; crossed = false; blockT = ""; entryT = "9:32 AM"; badge = "NM STRADDLE"; strikeInfo = null; }
    if (eomDay) { rec = "Straddle @ 9:32 AM (EOM)"; theme = "strad"; crossed = false; blockT = ""; entryT = "9:32 AM"; badge = "EOM STRADDLE"; strikeInfo = null; }
    if (isWed && !fedDay && !m8bfBanned && !nmDay && rec.startsWith("Straddle")) { rec = m8Msg(etDate); theme = "m8bf"; badge = "M8BF"; strikeInfo = m8Sched(dow); entryT = strikeInfo?.window || ""; }
    if (opexDay && rec.startsWith("Straddle")) { rec = "No Straddle (OPEX day)"; theme = "block"; crossed = true; blockT = "hard"; blockD = "Straddle not on OPEX"; badge = "BLOCKED"; }
    if (postOpMon && rec.startsWith("M8BF")) pmNote = true;
  }

  if (pmNote) rec += " (afternoon times preferred)";

  // SPX gap cancels straddle
  if (spxGapCancelsStrad && blockT !== '0%rule' && (rec === "Straddle @ 9:32 AM" || rec === "Straddle @ 9:32 AM (EOM)" || rec.startsWith("NM Straddle"))) {
    const dir = spxGapPct > 0 ? '▲' : '▼';
    rec = `No Straddle (SPX gap ${dir}${Math.abs(spxGapPct).toFixed(2)}%)`; theme = "block"; crossed = true; blockT = "gap"; blockD = `SPX gap ≥ ${T.SPX_GAP_THRESHOLD}%`; badge = "BLOCKED"; strikeInfo = null;
  }

  // o2o cancels straddle
  if (o2o > T.O2O_M8BF && blockT !== '0%rule' && (rec === "Straddle @ 9:32 AM" || rec.startsWith("NM Straddle"))) {
    rec = `No Straddle (o2o ${o2o.toFixed(1)} > ${T.O2O_M8BF})`; theme = "block"; crossed = true; blockT = "o2o"; blockD = `Open-to-open ${o2o.toFixed(1)} > ${T.O2O_M8BF}`; badge = "BLOCKED"; strikeInfo = null;
  }

  // WR=0% and WR>=90% overrides
  if (prevWR != null) {
    if (prevWR === 0 && !cpiDay && !fedDay) {
      rec = "Straddle @ 9:32 AM"; theme = "strad"; crossed = false;
      blockT = "0%rule"; entryT = "9:32 AM"; badge = "STRADDLE"; strikeInfo = null;
    } else if (prevWR >= 90 && !cpiDay) {
      const sc = m8Sched(dow);
      rec = m8Msg(etDate); theme = "m8bf"; badge = "M8BF";
      strikeInfo = sc; entryT = sc?.window || ""; blockT = "90%rule";
    }
  }

  // OPEX+1 GXBF override
  if (postOpDay && !cpiDay && blockT !== '0%rule' && blockT !== '90%rule') {
    const isM8 = rec.startsWith("M8BF"), isStr = rec.startsWith("Straddle") || rec.startsWith("NM Straddle");
    if (isM8 || isStr) {
      const vixOvernightPct = (vixToday - vixYClose) / vixYClose * 100;
      if (vixToday >= T.VIX_MAX_GXBF) { rec = `No GXBF (VIX ${vixToday} ≥ ${T.VIX_MAX_GXBF})`; theme = "block"; crossed = true; pmNote = false; blockT = "vix"; blockD = `OPEX+1 GXBF blocked, VIX ${vixToday}`; badge = "BLOCKED"; strikeInfo = null; }
      else if (vixOvernightPct >= 2) { rec = `No GXBF (VIX gapped up ${vixOvernightPct.toFixed(1)}% overnight)`; theme = "block"; crossed = true; pmNote = false; blockT = "vix"; blockD = `OPEX+1 GXBF blocked — VIX gap up ${vixOvernightPct.toFixed(1)}%`; badge = "BLOCKED"; strikeInfo = null; }
      else { rec = "GXBF @ 9:36 AM (OPEX+1)"; theme = "gxbf"; crossed = false; pmNote = false; blockT = ""; entryT = "9:36 AM"; badge = "GXBF"; strikeInfo = null; }
    }
  }

  // ── BOBF card logic ──
  const bobfBlocks = [];
  if (cpiDay) bobfBlocks.push("CPI day");
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
  // Each strategy card MUST evaluate its own rules (STRATEGY INDEPENDENCE).
  // Helper keeps the reasoning in ONE place so drift cannot happen.
  const m8bfOwnText = () => m8bfBanned
    ? (eomDay?`No M8BF (EOM)`:eom1?`No M8BF (EOM-1)`:opex1?`No M8BF (day before OPEX)`:nonAmznTslaEarn?`No M8BF (earnings)`:vixExpAfterOpex?`No M8BF (VIX exp day)`:`No M8BF`)
    : m8Msg(etDate);
  const gxbfOwnText = () => (oNight > T.DROP_GXBF)
    ? (vixToday >= T.VIX_MAX_GXBF ? `No GXBF (VIX ${vixToday} ≥ ${T.VIX_MAX_GXBF})` : `GXBF @ 9:36 AM`)
    : `No GXBF (overnight VIX drop ≤ ${T.DROP_GXBF})`;
  const stradOwnText = () => `No Straddle (${oNight <= 0 ? 'overnight VIX up' : oNight > T.DROP_GXBF ? 'overnight VIX drop > ' + T.DROP_GXBF : blockT === '90%rule' ? 'WR ≥ 90%' : 'non-CPI/Fed Wednesday'})`;

  let m8bfText = rec, stradText = rec, gxbfText = rec;

  if (cpiDay) {
    m8bfText = `No M8BF (CPI day)`;
    stradText = `No Straddle (CPI day)`;
    gxbfText = `No GXBF (CPI day)`;
  } else if (theme === 'm8bf') {
    stradText = stradOwnText();
    gxbfText = gxbfOwnText();
  } else if (theme === 'strad') {
    m8bfText = m8bfOwnText();
    gxbfText = gxbfOwnText();
  } else if (theme === 'gxbf') {
    m8bfText = m8bfOwnText();
    stradText = stradOwnText();
  } else if (theme === 'block') {
    if (rec.includes('M8BF')) {
      stradText = stradOwnText();
      gxbfText = gxbfOwnText();
    } else if (rec.includes('Straddle') || rec.includes('Trade')) {
      m8bfText = m8bfOwnText();
      gxbfText = gxbfOwnText();
    } else if (rec.includes('GXBF')) {
      m8bfText = m8bfOwnText();
      stradText = stradOwnText();
    }
  }

  // m8bfStrikeInfo — independent of main strikeInfo, so consumers can show
  // the M8BF banned-strike list even when the main signal was overridden
  // (e.g. OPEX+1 GXBF auto-fire, which nulls the main strikeInfo).
  // null when the M8BF's OWN status is blocked (CPI/m8bfBanned).
  const m8bfStrikeInfo = (cpiDay || m8bfBanned) ? null : m8Sched(dow);

  // ── DIAGONAL (companion strategy) — delegated to single source of truth ──
  const { diagText, diagBadge, diagGo, diagSkipCode } = computeDiagonalSignal(etDate, vixPct20d);

  return {
    rec, theme, crossed, badge, entryT, blockT, blockD, pmNote,
    strikeInfo, m8bfStrikeInfo, cpiLongCall,
    m8bfText, stradText, gxbfText,
    bobfRec, bobfBadge, bobfBlocks,
    // Diagonal (companion — independent of Sigma 3)
    diagText, diagBadge, diagGo, diagSkipCode, vixPct20d,
    oNight, o2o,
    spxGapPct, spxGapCancelsStrad, m8bfBanned,
    dayLabel: tradeWdLabel(etDate),
    dateStr: todayLong(etDate),
    cpiDay, fedDay, opexDay, postOpDay, eomDay,
    // flags each consumer may need for its own rendering
    eom1, eom2, opex1, nmDay, nmMon, isMon, isFri, isWed,
    vixExpDay, vixExpAfterOpex, nonAmznTslaEarn, earningsDay, postOpMon,
  };
}

// Convenience: attach to globalThis for any non-module loader that needs it.
// Modules prefer `import { calculateSignal } from './signal-engine.js'`.
if (typeof globalThis !== 'undefined') {
  globalThis.SignalEngine = {
    T, cpiSch, fedSch, opexSch, holidays, vixSch, earningsSchedule,
    toET, dateLong, todayLong, isWkend, isHol, isTrade,
    nextTrade, prevTrade, isTodayAfter, isTodayBefore, parseLong, schedInMonth,
    isVixAfterOpexDay, isPostOpexMon, isLastTradeMo, isEomN, isFirstTradeMo, isFirstTradeMon,
    m8Sched, m8Msg, ordinal, wdName, tradeWdLabel,
    isEarningsDay, isNonAmznTslaEarningsDay, isDayAfterAnyEarnings,
    computeDiagonalSignal,
    calculateSignal,
  };
}

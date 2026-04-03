#!/usr/bin/env node
// Signal logic regression tests — run with: node test_signal.js
// Mirrors the calculateStrategy() branching in index.html exactly.
// Add a test case any time a bug is found or a rule changes.

const T = {
  DROP_GXBF: 0.65,
  O2O_M8BF: 1.4,
  VIX_MAX_GXBF: 25,
  SPX_GAP_THRESHOLD: 0.9,
};

// Simplified stubs — return fixed strings so tests don't depend on schedules
const m8Msg = () => "M8BF Window";

// ── Pure signal function (mirrors index.html calculateStrategy logic) ──
function calcSignal({
  vToday, vYClose, vYOpen,
  cpiDay = false,
  eomDay = false, eom1 = false,
  nmDay = false, isMon = false, isWed = false,
  fedDay = false,
  opexDay = false,
  opex1 = false,
  vixExpAfterOpex = false,
  earningsBlock = false,
  dayAfterEarnings = false,
  postOpDay = false,
  spxGapPct = null,
  wr0 = false, wr90 = false,
  o2oOverride = null, // if provided, overrides computed o2o
}) {
  const oNight = vYClose - vToday;
  const o2o = o2oOverride !== null ? o2oOverride : (vYOpen - vToday);
  const spxGapCancelsStrad = spxGapPct !== null && Math.abs(spxGapPct) >= T.SPX_GAP_THRESHOLD;

  let rec = "", theme = "neutral";
  let blockT = "";

  if (cpiDay) {
    if (oNight > 0) { rec = "CPI CALL"; theme = "strad"; }
    else            { rec = "No Trade (CPI)"; theme = "block"; }
  } else {
    // ── Core VIX overnight branch ──
    if (oNight > T.DROP_GXBF) {
      if (vToday >= T.VIX_MAX_GXBF) { rec = `No GXBF (VIX ${vToday} ≥ ${T.VIX_MAX_GXBF})`; theme = "block"; }
      else                           { rec = "GXBF @ 9:36 AM"; theme = "gxbf"; }
    } else if (oNight > 0) {
      rec = "Straddle @ 9:32 AM"; theme = "strad";
    } else {
      rec = m8Msg(); theme = "m8bf";
    }

    // EOM / EOM-1 / OPEX-1 / VIX exp block on M8BF
    if (rec.startsWith("M8BF")) {
      if (eomDay) { rec = "No M8BF (EOM)"; theme = "block"; }
      else if (eom1) { rec = "No M8BF (EOM-1)"; theme = "block"; }
      else if (opex1) { rec = "No M8BF (day before OPEX)"; theme = "block"; }
      else if (earningsBlock) { rec = "No M8BF (earnings)"; theme = "block"; }
      else if (dayAfterEarnings) { rec = "No M8BF (day after earnings)"; theme = "block"; }
      else if (vixExpAfterOpex) { rec = "No M8BF (VIX exp day)"; theme = "block"; }
    }

    // NM Straddle (first trading day of month, non-Monday) — overrides M8BF, Straddle, and GXBF
    if (nmDay && !isMon && (rec.startsWith("M8BF") || rec.startsWith("No M8BF") || rec.startsWith("Straddle") || rec.startsWith("GXBF") || rec.startsWith("No GXBF"))) {
      rec = "NM Straddle @ 9:32 AM"; theme = "strad";
    }

    // EOM always straddle
    if (eomDay) { rec = "Straddle @ 9:32 AM (EOM)"; theme = "strad"; }

    // Wednesday (non-Fed, non-blocked, non-NM): Straddle → M8BF
    const m8bfBanned = eomDay || eom1 || opex1 || vixExpAfterOpex || earningsBlock || dayAfterEarnings;
    if (isWed && !fedDay && !m8bfBanned && !nmDay && rec.startsWith("Straddle")) {
      rec = m8Msg(); theme = "m8bf";
    }

    // OPEX day blocks straddle
    if (opexDay && rec.startsWith("Straddle")) {
      rec = "No Straddle (OPEX day)"; theme = "block";
    }

    // SPX gap cancels straddle only — no effect on butterfly
    if (spxGapCancelsStrad && (rec === "Straddle @ 9:32 AM" || rec === "Straddle @ 9:32 AM (EOM)" || rec.startsWith("NM Straddle"))) {
      rec = "No Straddle (SPX gap)"; theme = "block"; blockT = "gap";
    }

    // o2o cancels straddle only — no effect on butterfly
    if (o2o > T.O2O_M8BF && (rec === "Straddle @ 9:32 AM" || rec.startsWith("NM Straddle"))) {
      rec = "No Straddle (o2o)"; theme = "block"; blockT = "o2o";
    }

    // WR=0% and WR>=90% are the STRONGEST overrides — trump gap, o2o, everything (except CPI/Fed)
    if (wr0) {
      if (!cpiDay && !fedDay) { rec = "Straddle @ 9:32 AM"; theme = "strad"; blockT = "0%rule"; }
    } else if (wr90) {
      if (!cpiDay) { rec = m8Msg(); theme = "m8bf"; blockT = "90%rule"; }
    }

    // OPEX+1: override to GXBF (or block) — but NOT when WR 0% or 90% forced the signal
    if (postOpDay && blockT !== '0%rule' && blockT !== '90%rule') {
      const isM8  = rec.startsWith("M8BF");
      const isStr = rec.startsWith("Straddle") || rec.startsWith("NM Straddle");
      if (isM8 || isStr) {
        const vixOvernightPct = (vToday - vYClose) / vYClose * 100;
        if (vToday >= T.VIX_MAX_GXBF)    { rec = `No GXBF (VIX ${vToday} ≥ ${T.VIX_MAX_GXBF})`; theme = "block"; }
        else if (vixOvernightPct >= 2)    { rec = `No GXBF (VIX gapped up overnight)`; theme = "block"; }
        else                              { rec = "GXBF @ 9:36 AM (OPEX+1)"; theme = "gxbf"; }
      }
    }
  }

  return { rec, theme, blockT };
}

// ── Test runner ──
let passed = 0, failed = 0;

function test(name, inputs, expectRec, expectTheme) {
  const result = calcSignal(inputs);
  const recOk   = result.rec.startsWith(expectRec);
  const themeOk = result.theme === expectTheme;
  if (recOk && themeOk) {
    console.log(`  ✅  ${name}`);
    passed++;
  } else {
    console.error(`  ❌  ${name}`);
    if (!recOk)   console.error(`       rec:   got "${result.rec}" — expected starts with "${expectRec}"`);
    if (!themeOk) console.error(`       theme: got "${result.theme}" — expected "${expectTheme}"`);
    failed++;
  }
}

const BASE = { vToday: 20, vYClose: 20, vYOpen: 20 }; // VIX flat, no special day

console.log("\n── Core VIX overnight branch ──");
test("VIX drop > 0.65 → GXBF",
  { ...BASE, vYClose: 21, vToday: 20 },          // drop = 1.0
  "GXBF @ 9:36 AM", "gxbf");

test("VIX drop exactly 0.65 → Straddle (not GXBF)",
  { ...BASE, vYClose: 20.65, vToday: 20 },        // drop = 0.65 — boundary, NOT > 0.65
  "Straddle @ 9:32 AM", "strad");

test("VIX drop 0.08 → Straddle (today's bug)",
  { ...BASE, vYClose: 26.78, vToday: 26.7 },      // drop = 0.08
  "Straddle @ 9:32 AM", "strad");

test("VIX drop 0.01 → Straddle (minimum drop)",
  { ...BASE, vYClose: 20.01, vToday: 20 },
  "Straddle @ 9:32 AM", "strad");

test("VIX flat (oNight = 0) → M8BF",
  { ...BASE, vYClose: 20, vToday: 20 },
  "M8BF", "m8bf");

test("VIX up overnight → M8BF",
  { ...BASE, vYClose: 19, vToday: 20 },           // oNight = -1
  "M8BF", "m8bf");

test("GXBF blocked — VIX drop > 0.65 but VIX ≥ 25",
  { ...BASE, vYClose: 26, vToday: 25, vYOpen: 20 },
  "No GXBF (VIX 25", "block");

console.log("\n── CPI day ──");
test("CPI + VIX drop → CPI Call",
  { ...BASE, vYClose: 21, vToday: 20, cpiDay: true },
  "CPI CALL", "strad");

test("CPI + VIX up → No Trade",
  { ...BASE, vYClose: 19, vToday: 20, cpiDay: true },
  "No Trade", "block");

console.log("\n── EOM / EOM-1 ──");
test("EOM → EOM Straddle (overrides everything)",
  { ...BASE, vYClose: 21, vToday: 20, eomDay: true },
  "Straddle @ 9:32 AM (EOM)", "strad");

test("EOM (VIX up, would be M8BF) → EOM Straddle",
  { ...BASE, vYClose: 19, vToday: 20, eomDay: true },
  "Straddle @ 9:32 AM (EOM)", "strad");

test("EOM-1 + M8BF → blocked",
  { ...BASE, vYClose: 19, vToday: 20, eom1: true },
  "No M8BF (EOM-1)", "block");

test("EOM-1 + Straddle day → Straddle still fires (EOM-1 only blocks M8BF)",
  { ...BASE, vYClose: 20.3, vToday: 20, eom1: true },
  "Straddle @ 9:32 AM", "strad");

console.log("\n── Earnings ──");
test("Earnings (non-AMZN/TSLA) + M8BF → blocked",
  { ...BASE, vYClose: 19, vToday: 20, earningsBlock: true },
  "No M8BF (earnings)", "block");

test("Day after earnings + M8BF → blocked",
  { ...BASE, vYClose: 19, vToday: 20, dayAfterEarnings: true },
  "No M8BF (day after earnings)", "block");

test("Earnings + Straddle day → Straddle still fires (earnings only blocks M8BF)",
  { ...BASE, vYClose: 20.3, vToday: 20, earningsBlock: true },
  "Straddle @ 9:32 AM", "strad");

test("Wed + earnings → Straddle (not converted to M8BF because m8bfBanned)",
  { ...BASE, vYClose: 20.3, vToday: 20, isWed: true, earningsBlock: true },
  "Straddle @ 9:32 AM", "strad");

console.log("\n── NM day ──");
test("NM non-Monday + M8BF → NM Straddle",
  { ...BASE, vYClose: 19, vToday: 20, nmDay: true, isMon: false },
  "NM Straddle @ 9:32 AM", "strad");

test("NM non-Monday + Straddle (VIX drop 0.01-0.65) → NM Straddle",
  { ...BASE, vYClose: 20.3, vToday: 20, nmDay: true, isMon: false },
  "NM Straddle @ 9:32 AM", "strad");

test("NM Monday → M8BF (NM Straddle does not apply on Monday)",
  { ...BASE, vYClose: 19, vToday: 20, nmDay: true, isMon: true },
  "M8BF", "m8bf");

console.log("\n── Wednesday ──");
test("Wed (normal) + Straddle → M8BF",
  { ...BASE, vYClose: 20.3, vToday: 20, isWed: true },
  "M8BF", "m8bf");

test("Wed + Fed day → Straddle (not converted)",
  { ...BASE, vYClose: 20.3, vToday: 20, isWed: true, fedDay: true },
  "Straddle @ 9:32 AM", "strad");

test("Wed + EOM → EOM Straddle (not converted to M8BF)",
  { ...BASE, vYClose: 20.3, vToday: 20, isWed: true, eomDay: true },
  "Straddle @ 9:32 AM (EOM)", "strad");

test("Wed + VIX up → M8BF (no straddle to convert, already M8BF)",
  { ...BASE, vYClose: 19, vToday: 20, isWed: true },
  "M8BF", "m8bf");

console.log("\n── OPEX day ──");
test("OPEX day + Straddle → blocked",
  { ...BASE, vYClose: 20.3, vToday: 20, opexDay: true },
  "No Straddle (OPEX day)", "block");

test("OPEX day + GXBF (drop > 0.65) → GXBF still fires",
  { ...BASE, vYClose: 21, vToday: 20, opexDay: true },
  "GXBF @ 9:36 AM", "gxbf");

console.log("\n── OPEX+1 ──");
test("OPEX+1 + Straddle (VIX drop 0.3) + VIX < 25 → GXBF OPEX+1",
  { ...BASE, vYClose: 20.3, vToday: 20, postOpDay: true },   // oNight=0.3 → Straddle → OPEX+1 override
  "GXBF @ 9:36 AM (OPEX+1)", "gxbf");

test("OPEX+1 + M8BF (VIX flat) + VIX < 25 → GXBF OPEX+1",
  { ...BASE, vYClose: 20, vToday: 20, postOpDay: true },      // oNight=0 → M8BF → OPEX+1 override
  "GXBF @ 9:36 AM (OPEX+1)", "gxbf");

test("OPEX+1 + VIX ≥ 25 → blocked",
  { ...BASE, vYClose: 26, vToday: 25.5, postOpDay: true },
  "No GXBF (VIX", "block");

test("OPEX+1 + VIX gap up ≥ 2% overnight → blocked",
  { ...BASE, vYClose: 20, vToday: 20.5, postOpDay: true },  // (20.5-20)/20*100 = 2.5%
  "No GXBF (VIX gapped up", "block");

console.log("\n── WR overrides ──");
test("WR=0 + M8BF → Straddle",
  { ...BASE, vYClose: 19, vToday: 20, wr0: true },
  "Straddle @ 9:32 AM", "strad");

test("WR=0 + M8BF + Fed day → stays M8BF (Fed takes priority)",
  { ...BASE, vYClose: 19, vToday: 20, wr0: true, fedDay: true },
  "M8BF", "m8bf");

test("WR=90 + Straddle → M8BF",
  { ...BASE, vYClose: 20.3, vToday: 20, wr90: true },
  "M8BF", "m8bf");

test("WR=90 + EOM Straddle → M8BF",
  { ...BASE, vYClose: 20.3, vToday: 20, wr90: true, eomDay: true },
  "M8BF", "m8bf");

console.log("\n── o2o cancels straddle ──");
test("o2o > 1.4 + Straddle → No Straddle (not M8BF)",
  { ...BASE, vYClose: 20.3, vToday: 20, o2oOverride: 1.5 },
  "No Straddle (o2o)", "block");

test("o2o > 1.4 + EOM Straddle → stays EOM Straddle",
  { ...BASE, vYClose: 20.3, vToday: 20, eomDay: true, o2oOverride: 1.5 },
  "Straddle @ 9:32 AM (EOM)", "strad");

test("o2o ≤ 1.4 + Straddle → stays Straddle",
  { ...BASE, vYClose: 20.3, vToday: 20, o2oOverride: 1.4 },
  "Straddle @ 9:32 AM", "strad");

test("o2o > 1.4 + M8BF day → stays M8BF (o2o only affects straddle)",
  { ...BASE, vYClose: 19, vToday: 20, o2oOverride: 1.5 },
  "M8BF", "m8bf");

console.log("\n── SPX gap cancels straddle ──");
test("SPX gap ≥ 0.9% + Straddle → No Straddle (not M8BF)",
  { ...BASE, vYClose: 20.3, vToday: 20, spxGapPct: 1.2 },
  "No Straddle (SPX gap)", "block");

test("SPX gap ≥ 0.9% + M8BF day → stays M8BF (gap only affects straddle)",
  { ...BASE, vYClose: 19, vToday: 20, spxGapPct: 1.2 },
  "M8BF", "m8bf");

test("SPX gap ≥ 0.9% + EOM → cancels EOM Straddle",
  { ...BASE, vYClose: 20.3, vToday: 20, spxGapPct: 1.2, eomDay: true },
  "No Straddle (SPX gap)", "block");

test("SPX gap < 0.9% → no override",
  { ...BASE, vYClose: 20.3, vToday: 20, spxGapPct: 0.5 },
  "Straddle @ 9:32 AM", "strad");

console.log("\n── WR trumps gap/o2o ──");
test("WR=0 trumps SPX gap → Straddle (gap canceled straddle, 0% overrides back)",
  { ...BASE, vYClose: 20.3, vToday: 20, spxGapPct: 1.2, wr0: true },
  "Straddle @ 9:32 AM", "strad");

test("WR=0 trumps o2o → Straddle (o2o canceled straddle, 0% overrides back)",
  { ...BASE, vYClose: 20.3, vToday: 20, o2oOverride: 1.5, wr0: true },
  "Straddle @ 9:32 AM", "strad");

test("WR=0 trumps GXBF → Straddle",
  { ...BASE, vYClose: 22, vToday: 20, wr0: true },
  "Straddle @ 9:32 AM", "strad");

test("WR=0 + CPI day → stays CPI (CPI trumps 0%)",
  { ...BASE, vYClose: 20.3, vToday: 20, cpiDay: true, wr0: true },
  "CPI CALL", "strad");

test("WR=90 trumps SPX gap-blocked → M8BF",
  { ...BASE, vYClose: 20.3, vToday: 20, spxGapPct: 1.2, wr90: true },
  "M8BF", "m8bf");

test("WR=90 trumps o2o-blocked → M8BF",
  { ...BASE, vYClose: 20.3, vToday: 20, o2oOverride: 1.5, wr90: true },
  "M8BF", "m8bf");

test("WR=90 + CPI day → stays CPI (CPI trumps 90%)",
  { ...BASE, vYClose: 20.3, vToday: 20, cpiDay: true, wr90: true },
  "CPI CALL", "strad");

console.log("\n── NM/EOM override GXBF ──");
test("NM Straddle overrides GXBF (non-Monday)",
  { ...BASE, vYClose: 21, vToday: 20, nmDay: true, isMon: false },  // oNight=1 → GXBF, but NM overrides
  "NM Straddle @ 9:32 AM", "strad");

test("NM Straddle overrides No GXBF (VIX high, non-Monday)",
  { ...BASE, vYClose: 26, vToday: 25, nmDay: true, isMon: false },  // No GXBF → NM overrides
  "NM Straddle @ 9:32 AM", "strad");

test("EOM Straddle overrides GXBF",
  { ...BASE, vYClose: 21, vToday: 20, eomDay: true },  // oNight=1 → GXBF, but EOM overrides
  "Straddle @ 9:32 AM (EOM)", "strad");

console.log("\n── WR on banned/special days ──");
test("WR=90 on m8bfBanned day (EOM-1) → M8BF (90% trumps ban)",
  { ...BASE, vYClose: 20.3, vToday: 20, wr90: true, eom1: true },
  "M8BF", "m8bf");

test("OPEX+1 + WR=0% → stays Straddle (WR trumps OPEX+1)",
  { ...BASE, vYClose: 20.3, vToday: 20, postOpDay: true, wr0: true },
  "Straddle @ 9:32 AM", "strad");

test("OPEX+1 + WR=90% → stays M8BF (WR trumps OPEX+1)",
  { ...BASE, vYClose: 20.3, vToday: 20, postOpDay: true, wr90: true },
  "M8BF", "m8bf");

console.log("\n── CPI edge cases ──");
test("CPI + VIX flat (oNight=0) → No Trade",
  { ...BASE, vYClose: 20, vToday: 20, cpiDay: true },
  "No Trade", "block");

console.log("\n── NM + Wednesday interaction ──");
test("Wednesday + NM (non-Monday) → NM Straddle (NM overrides Wed conversion)",
  { ...BASE, vYClose: 20.3, vToday: 20, isWed: true, nmDay: true, isMon: false },
  "NM Straddle @ 9:32 AM", "strad");

console.log("\n── o2o / SPX gap cancel NM Straddle ──");
test("o2o > 1.4 cancels NM Straddle",
  { ...BASE, vYClose: 20.3, vToday: 20, nmDay: true, isMon: false, o2oOverride: 1.5 },
  "No Straddle (o2o)", "block");

test("SPX gap ≥ 0.9% cancels NM Straddle",
  { ...BASE, vYClose: 20.3, vToday: 20, nmDay: true, isMon: false, spxGapPct: 1.2 },
  "No Straddle (SPX gap)", "block");

// ── Summary ──
console.log(`\n${"─".repeat(44)}`);
console.log(`  ${passed} passed  |  ${failed} failed  |  ${passed + failed} total`);
console.log(`${"─".repeat(44)}\n`);
process.exit(failed > 0 ? 1 : 0);

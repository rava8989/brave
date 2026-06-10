#!/usr/bin/env node
/**
 * Python ↔ JS parity test for the GXBF backtester.
 *
 * The GXBF backtest logic exists twice:
 *   • JS:     gxbf-backtester.html  (runBacktestBS, browser backtester)
 *   • Python: rebuild_gxbfPL.py     (gates + computes gxbfPL history)
 *
 * The 2026-06-10 audit found they had silently diverged (wrong FED dates,
 * stale VIX-expiry approximation, missing zero-guards → phantom trades).
 * This test makes that class of bug impossible to miss:
 *
 *   1. Serve the repo over local HTTP (module scripts need it).
 *   2. Open gxbf-backtester.html in headless system Chrome (puppeteer-core),
 *      wait for data + SignalEngine, verify the page DEFAULTS still match the
 *      contract rebuild_gxbfPL.py hardcodes (entry 09:35, volume center,
 *      real GXBF gating ON, wing 5–100, drop>0.65, VIX<25, gap≥2%, 1 contract).
 *   3. Click RUN BACKTEST, read `lastTrades` (per-date P/L) + the date
 *      universe the page ran over.
 *   4. Run `python3 rebuild_gxbfPL.py --dry-run-json …` — compute-only mode,
 *      NEVER touches history_data.json — and read its {date: pnl|null} map.
 *   5. Compare on the INTERSECTION of dates both systems cover:
 *        • fired-vs-gated must agree per date (one fired, other gated = FAIL)
 *        • fired P/L within ±$1 per date, totals within ±$1
 *      Coverage gaps (date only in one universe) are PRINTED but not failed.
 *
 * Exit 0 on parity, 1 with a readable diff on mismatch, 2 on harness crash.
 *
 * Usage:  node scripts/test-gxbf-parity.js
 */
import puppeteer from 'puppeteer-core';
import { execFile } from 'child_process';
import http from 'http';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const REPO_ROOT = path.resolve(__dirname, '..');

// macOS-installed Chrome (same pattern as test-diagonal-parity.js).
const CHROME_PATH =
  process.env.PUPPETEER_EXECUTABLE_PATH ||
  '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';

const PY_OUT = '/tmp/gxbf_parity_py.json';   // dry-run output (never history)
const PNL_TOL = 1.0;          // ±$1 float tolerance, total and per-date
const MAX_DIFF_LINES = 30;    // cap each mismatch list in the diff output

// The fixed contract rebuild_gxbfPL.py implements. If a page DEFAULT drifts
// from this, the two engines no longer compute the same thing — that IS the
// drift this test exists to catch, so it's a failure (exit 1), not a crash.
const PY_CONTRACT = {
  entryTime: '09:35',     // python reads by_time['09:35'] only
  centerSrc: 'vol',       // python: use_oi = False (hybrid retired 2026-06-10)
  gxbfGating: true,       // python always applies real GXBF gating
  ovDropThr: 0.65,        // OV_DROP_THR
  gVixMax: 25,            // VIX_MAX
  gGapPct: 2,             // OPEX1_GAP_PCT
  wingMin: 5,             // WING_MIN
  wingMax: 100,           // WING_MAX
  maxContracts: 1,        // python computes per-1-contract P/L
};

// ── Minimal HTTP server so module scripts + fetches work ──────────────────
function startServer(rootDir) {
  return new Promise(resolve => {
    const types = {
      '.html': 'text/html', '.js': 'application/javascript',
      '.css': 'text/css', '.json': 'application/json',
      '.svg': 'image/svg+xml', '.png': 'image/png',
      '.gz': 'application/gzip',
    };
    const server = http.createServer((req, res) => {
      let urlPath = decodeURIComponent(req.url.split('?')[0]);
      if (urlPath === '/') urlPath = '/index.html';
      const filePath = path.join(rootDir, urlPath);
      if (!filePath.startsWith(rootDir)) { res.writeHead(403); return res.end(); }
      fs.readFile(filePath, (err, data) => {
        if (err) { res.writeHead(404); return res.end(); }
        const ext = path.extname(filePath);
        res.writeHead(200, { 'Content-Type': types[ext] || 'application/octet-stream' });
        res.end(data);
      });
    });
    server.listen(0, '127.0.0.1', () => resolve({ server, port: server.address().port }));
  });
}

// ── JS side: drive gxbf-backtester.html in headless Chrome ────────────────
async function runJsBacktest(baseUrl) {
  const browser = await puppeteer.launch({
    executablePath: CHROME_PATH,
    headless: 'new',
    args: ['--no-sandbox', '--disable-dev-shm-usage'],
  });
  try {
    const page = await browser.newPage();
    page.on('dialog', d => d.dismiss().catch(() => {}));   // never hang on alert()
    page.on('pageerror', err => console.error(`  [page error] ${err.message}`));
    page.on('console', msg => {
      if (msg.type() === 'error') console.error(`  [page console.error] ${msg.text()}`);
    });

    await page.goto(`${baseUrl}/gxbf-backtester.html`, { waitUntil: 'domcontentloaded', timeout: 30000 });

    // Data ready when: RUN enabled (set after loadData resolves) AND the
    // SignalEngine module attached — buildSpecialDateSets sources OPEX/VIX/FED
    // from SignalEngine schedules; running before it lands would leave the
    // special-day sets empty and guarantee a false mismatch.
    await page.waitForFunction(
      `!document.getElementById('runBtn').disabled
         && !!globalThis.SignalEngine
         && !!globalThis.SignalEngine.opexSch
         && typeof globalThis.SignalEngine.computeVixPct20d === 'function'`,
      { timeout: 60000, polling: 250 },
    );

    // The page's own defaults — exactly what a user gets on load.
    const setup = await page.evaluate(() => {
      const p = getParams();
      return {
        from: p.from, to: p.to,
        entryTime: p.entryTime, centerSrc: p.centerSrc, gxbfGating: p.gxbfGating,
        ovDropOn: p.ovDropOn, ovDropThr: p.ovDropThr,
        gVixMax: p.gVixMax, gGapPct: p.gGapPct,
        wingMin: p.wingMin, wingMax: p.wingMax,
        contracts: p.contracts, maxContracts: p.maxContracts,
        ignoreMargin: p.ignoreMargin,
        specialMode: p.specialMode,
        specialActive: Array.from(p.specialActive),
        dataDays: GXBF_DATA?.length ?? 0,
      };
    });

    // Defaults must match the contract Python hardcodes — drift here means
    // the rebuilder no longer reproduces what the page shows.
    const drifted = [];
    for (const [k, want] of Object.entries(PY_CONTRACT)) {
      if (setup[k] !== want) drifted.push(`${k}: page default=${JSON.stringify(setup[k])} python expects=${JSON.stringify(want)}`);
    }
    if (setup.specialMode && setup.specialActive.length) {
      drifted.push(`special-day filter active by default (${setup.specialMode}: ${setup.specialActive.join(',')}) — python has no such filter`);
    }
    if (drifted.length) {
      const err = new Error('page defaults no longer match rebuild_gxbfPL.py contract:\n    ' + drifted.join('\n    '));
      err.isParityFailure = true;
      throw err;
    }

    // Drive the real button (same path a user takes: getParams → runBacktest).
    await page.evaluate(() => document.getElementById('runBtn').click());
    await page.waitForFunction(
      `typeof lastTrades !== 'undefined' && lastTrades !== null`,
      { timeout: 45000, polling: 250 },
    );

    const result = await page.evaluate(() => {
      const p = getParams();
      const byDate = {};
      for (const t of lastTrades) byDate[t.openDate] = t.pnl;
      return {
        count: lastTrades.length,
        total: lastTrades.reduce((s, t) => s + t.pnl, 0),
        byDate,
        // The full date universe the JS run considered (default from/to spans
        // the whole file). A date here that is NOT in byDate = JS gated it.
        universe: GXBF_DATA.map(r => r.date).filter(d => d >= p.from && d <= p.to),
      };
    });

    return { setup, result };
  } finally {
    await browser.close();
  }
}

// ── Python side: rebuild_gxbfPL.py --dry-run-json (no history mutation) ───
function runPyBacktest() {
  return new Promise((resolve, reject) => {
    execFile(
      'python3', ['rebuild_gxbfPL.py', '--dry-run-json', PY_OUT],
      { cwd: REPO_ROOT, maxBuffer: 64 * 1024 * 1024, timeout: 120000 },
      (err, stdout, stderr) => {
        if (stderr.trim()) console.error(`  [python] ${stderr.trim()}`);
        if (err) return reject(new Error(`rebuild_gxbfPL.py --dry-run-json failed: ${err.message}`));
        stdout.trim().split('\n').forEach(l => console.log(`     ${l}`));
        try {
          const byDate = JSON.parse(fs.readFileSync(PY_OUT, 'utf8'));
          resolve(byDate);   // { date: pnl|null } for every gxbf_bt_data date
        } catch (e) {
          reject(new Error(`could not read/parse ${PY_OUT}: ${e.message}`));
        }
      },
    );
  });
}

// ── Comparison ─────────────────────────────────────────────────────────────
const fmt$ = n => (n < 0 ? '-$' : '$') + Math.abs(n).toFixed(2);

function capped(list) {
  return list.length > MAX_DIFF_LINES
    ? [...list.slice(0, MAX_DIFF_LINES), `    … +${list.length - MAX_DIFF_LINES} more`]
    : list;
}

function compare(js, py) {
  const problems = [];

  // Coverage: align to the intersection. The html runs over gxbf_bt_data
  // dates in its from/to range; the dry-run emits every gxbf_bt_data date.
  // A date present on only one side is a COVERAGE gap (e.g. history/data
  // window differences) — printed for visibility, never a failure.
  const pyDates = Object.keys(py);
  const jsSet = new Set(js.universe);
  const pySet = new Set(pyDates);
  const common = js.universe.filter(d => pySet.has(d)).sort();
  const onlyJs = js.universe.filter(d => !pySet.has(d)).sort();
  const onlyPy = pyDates.filter(d => !jsSet.has(d)).sort();
  console.log(`Coverage    : ${common.length} common, ${onlyJs.length} only-JS, ${onlyPy.length} only-PY (coverage gaps are not failures)`);
  if (onlyJs.length) {
    const fired = onlyJs.filter(d => d in js.byDate);
    console.log(`  only-JS dates (${fired.length} of them fired):`);
    capped(onlyJs.map(d => `    ${d}${d in js.byDate ? `  pnl=${fmt$(js.byDate[d])}` : ''}`)).forEach(l => console.log(l));
  }
  if (onlyPy.length) {
    const fired = onlyPy.filter(d => py[d] !== null);
    console.log(`  only-PY dates (${fired.length} of them fired):`);
    capped(onlyPy.map(d => `    ${d}${py[d] !== null ? `  pnl=${fmt$(py[d])}` : ''}`)).forEach(l => console.log(l));
  }

  // Within common coverage: fired/gated must agree, fired values within ±$1.
  const firedOnlyJs = [];   // JS traded, python gated  → drift
  const firedOnlyPy = [];   // python traded, JS gated  → drift
  const valueDiffs = [];
  let jsTotal = 0, pyTotal = 0, bothFired = 0;
  for (const d of common) {
    const jsFired = d in js.byDate;
    const pyFired = py[d] !== null && py[d] !== undefined;
    if (jsFired) jsTotal += js.byDate[d];
    if (pyFired) pyTotal += py[d];
    if (jsFired && !pyFired) { firedOnlyJs.push(d); continue; }
    if (!jsFired && pyFired) { firedOnlyPy.push(d); continue; }
    if (!jsFired) continue;   // both gated — agreement
    bothFired++;
    if (Math.abs(js.byDate[d] - py[d]) > PNL_TOL) {
      valueDiffs.push({ d, js: js.byDate[d], py: py[d] });
    }
  }

  console.log(`\nFired dates : JS=${Object.keys(js.byDate).length}  PY=${pyDates.filter(d => py[d] !== null).length}  (both fired on ${bothFired} common dates)`);
  const dTotal = jsTotal - pyTotal;
  const totalOk = Math.abs(dTotal) <= PNL_TOL;
  console.log(`Total P/L   : JS=${fmt$(jsTotal)}  PY=${fmt$(pyTotal)}  Δ=${fmt$(dTotal)}  → ${totalOk ? 'MATCH (±$1)' : 'MISMATCH'}  [common coverage]`);
  if (!totalOk) problems.push(`total P/L differs by ${fmt$(dTotal)} over common coverage`);
  console.log(`Per-date    : ${valueDiffs.length} value mismatches (>±$1), ${firedOnlyJs.length} fired-only-JS, ${firedOnlyPy.length} fired-only-PY`);

  if (firedOnlyJs.length) {
    problems.push(`${firedOnlyJs.length} date(s) where JS FIRED but python GATED`);
    console.log('\n  JS fired / python gated (gxbf-backtester.html vs rebuild_gxbfPL.py):');
    capped(firedOnlyJs.map(d => `    ${d}  JS pnl=${fmt$(js.byDate[d])}  PY=null`)).forEach(l => console.log(l));
  }
  if (firedOnlyPy.length) {
    problems.push(`${firedOnlyPy.length} date(s) where python FIRED but JS GATED`);
    console.log('\n  python fired / JS gated:');
    capped(firedOnlyPy.map(d => `    ${d}  PY pnl=${fmt$(py[d])}  JS=no trade`)).forEach(l => console.log(l));
  }
  if (valueDiffs.length) {
    problems.push(`${valueDiffs.length} date(s) with per-date P/L diff > $${PNL_TOL}`);
    console.log('\n  Per-date P/L mismatches:');
    capped(valueDiffs.map(({ d, js: a, py: b }) =>
      `    ${d}  JS=${fmt$(a)}  PY=${fmt$(b)}  Δ=${fmt$(a - b)}`,
    )).forEach(l => console.log(l));
  }

  return problems;
}

// ── Main ───────────────────────────────────────────────────────────────────
async function main() {
  if (!fs.existsSync(CHROME_PATH)) {
    console.error(`✗ Chrome not found at ${CHROME_PATH}`);
    console.error('  Set PUPPETEER_EXECUTABLE_PATH or install Chrome.');
    process.exit(2);
  }

  const t0 = Date.now();
  const { server, port } = await startServer(REPO_ROOT);
  try {
    console.log('GXBF parity test: gxbf-backtester.html (JS) vs rebuild_gxbfPL.py (Python)');
    console.log(`Serving ${REPO_ROOT} on http://127.0.0.1:${port}\n`);

    console.log('1/3  Running JS backtest in headless Chrome (page defaults)…');
    let setup, js;
    try {
      ({ setup, result: js } = await runJsBacktest(`http://127.0.0.1:${port}`));
    } catch (err) {
      if (err.isParityFailure) {
        console.error(`\n✗ PARITY FAILED — ${err.message}`);
        console.error('\n  Either restore the page default or update rebuild_gxbfPL.py +');
        console.error('  PY_CONTRACT in this test so both engines compute the same config.');
        process.exit(1);
      }
      throw err;
    }
    console.log(`     page params: ${setup.from} → ${setup.to}, entry ${setup.entryTime}, center=${setup.centerSrc}, ` +
      `real gating=${setup.gxbfGating ? 'ON' : 'OFF'} (drop>${setup.ovDropThr}, VIX<${setup.gVixMax}, OPEX+1 gap≥${setup.gGapPct}%)`);
    console.log(`     wings ${setup.wingMin}–${setup.wingMax}, maxContracts=${setup.maxContracts}, data: ${setup.dataDays} days ` +
      `→ ${js.count} trades, total ${fmt$(js.total)}`);

    console.log('\n2/3  Running python3 rebuild_gxbfPL.py --dry-run-json (no history write)…');
    const py = await runPyBacktest();

    console.log('\n3/3  Comparing…');
    const problems = compare(js, py);

    const secs = ((Date.now() - t0) / 1000).toFixed(1);
    if (problems.length) {
      console.error(`\n✗ PARITY FAILED in ${secs}s — gxbf-backtester.html and rebuild_gxbfPL.py disagree:`);
      problems.forEach(p => console.error(`    • ${p}`));
      console.error('\n  The two implementations have drifted (the 2026-06-10 audit caught wrong');
      console.error('  FED dates + phantom zero-bar trades this way). Do NOT rebuild gxbfPL');
      console.error('  history until resolved.');
      process.exit(1);
    }
    console.log(`\n✓ PARITY OK in ${secs}s — ${js.count} fired dates agree, total ${fmt$(js.total)}, every per-date P/L within ±$${PNL_TOL}.`);
    process.exit(0);
  } finally {
    server.close();
  }
}

main().catch(err => {
  console.error('parity test crashed:', err.message);
  process.exit(2);
});

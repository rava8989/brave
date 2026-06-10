#!/usr/bin/env node
/**
 * Python ↔ JS parity test for the Diagonal backtester.
 *
 * The Diagonal backtest logic exists twice:
 *   • JS:     diagonal.html  (runBacktestBS, browser backtester)
 *   • Python: compute_diagonal_pnl.py  (run_backtest — writes diagPL history)
 *
 * The 2026-05-15 "traded in backtest but live blocked" bug (lessons.md P5)
 * came from these two drifting apart. The VIX-percentile pre-commit hook
 * blocks re-implementations, but nothing verifies the two backtests still
 * AGREE on results. This test does:
 *
 *   1. Serve the repo over local HTTP (module scripts need it).
 *   2. Open diagonal.html in headless system Chrome (puppeteer-core).
 *   3. Wait for data + SignalEngine + COR1M map, read the page's own default
 *      params via getParams(), click RUN BACKTEST, read `lastTrades`.
 *   4. Run compute_diagonal_pnl.run_backtest via a python3 -c wrapper with
 *      the SAME params (date range, times, strikes, filters, VIX band,
 *      COR1M floor). The wrapper calls run_backtest directly and prints
 *      JSON — it NEVER touches history_data.json.
 *   5. Compare trade count, total P/L (±$1) and per-date P/L (±$1/date).
 *
 * Exit 0 on parity, 1 with a readable diff on mismatch, 2 on harness crash.
 *
 * Usage:  node scripts/test-diagonal-parity.js
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

// macOS-installed Chrome (same pattern as smoke-test-dashboard.js).
const CHROME_PATH =
  process.env.PUPPETEER_EXECUTABLE_PATH ||
  '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';

const PNL_TOL = 1.0;          // ±$1 float tolerance, total and per-date
const MAX_DIFF_LINES = 30;    // cap each mismatch list in the diff output

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

// ── JS side: drive diagonal.html in headless Chrome ───────────────────────
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

    await page.goto(`${baseUrl}/diagonal.html`, { waitUntil: 'domcontentloaded', timeout: 30000 });

    // Data ready when: RUN enabled (set after loadData resolves), the
    // SignalEngine module attached (VIX_MID needs computeVixPct20d), and the
    // COR1M map fetched (COR1M_LOW filter needs it — Python always loads it,
    // so running before it lands would be a guaranteed false mismatch).
    await page.waitForFunction(
      `!document.getElementById('runBtn').disabled
         && !!globalThis.SignalEngine
         && typeof globalThis.SignalEngine.computeVixPct20d === 'function'
         && !!window.COR1M_BY_DATE`,
      { timeout: 60000, polling: 250 },
    );

    // The page's own defaults — exactly what a user gets on load.
    const setup = await page.evaluate(() => {
      const p = getParams();
      return {
        from: p.from, to: p.to,
        entryTime: p.entryTime, exitTime: p.exitTime,
        shortDte: p.shortDte, shortOffset: p.shortOffset,
        longDte: p.longDte, longOffset: p.longOffset, longDteTol: p.longDteTol,
        dteFallback: p.dteFallback, contracts: p.contracts,
        tpEnabled: p.tpEnabled,
        specialMode: p.specialMode,
        specialActive: Array.from(p.specialActive),
        vixLo: (typeof window.VIX_BAND_LO === 'number') ? window.VIX_BAND_LO : 50,
        vixHi: (typeof window.VIX_BAND_HI === 'number') ? window.VIX_BAND_HI : 80,
        cor1mMin: (typeof window.COR1M_MIN === 'number') ? window.COR1M_MIN : 10,
        cor1mDays: Object.keys(window.COR1M_BY_DATE || {}).length,
        dataDays: DIAGONAL_DATA?.dates?.length ?? 0,
        realDays: REAL_DATA?.dates?.length ?? 0,
      };
    });

    if (setup.tpEnabled) {
      throw new Error('page default has Take-Profit ENABLED — Python has no TP walker; parity undefined');
    }

    // Drive the real button (same path a user takes: getParams → runBacktest).
    await page.evaluate(() => document.getElementById('runBtn').click());
    await page.waitForFunction(
      `typeof lastTrades !== 'undefined' && lastTrades !== null`,
      { timeout: 45000, polling: 250 },
    );

    const result = await page.evaluate(() => {
      const byDate = {};
      const srcByDate = {};
      for (const t of lastTrades) {
        byDate[t.openDate] = t.pnl;
        srcByDate[t.openDate] = t.priceSource;
      }
      return {
        count: lastTrades.length,
        total: lastTrades.reduce((s, t) => s + t.pnl, 0),
        byDate, srcByDate,
      };
    });

    return { setup, result };
  } finally {
    await browser.close();
  }
}

// ── Python side: run_backtest via -c wrapper (no history mutation) ────────
const PY_WRAPPER = `
import sys, json, io
from contextlib import redirect_stdout
sys.path.insert(0, ${JSON.stringify(REPO_ROOT)})
import compute_diagonal_pnl as m

cfg = json.loads(sys.argv[1])
params = dict(m.DEFAULT_PARAMS)
params.update({
    "date_from":   cfg["from"],
    "date_to":     cfg["to"],
    "entry_time":  cfg["entryTime"],
    "exit_time":   cfg["exitTime"],
    "short_dte":   int(cfg["shortDte"]),
    "short_offset": int(cfg["shortOffset"]),
    "long_dte":    int(cfg["longDte"]),
    "long_offset": int(cfg["longOffset"]),
    "long_dte_tol": int(cfg["longDteTol"]),
    "dte_fallback": cfg["dteFallback"],
    "contracts":   int(cfg["contracts"]),
    "ignore_margin": True,
    "special_mode": cfg["specialMode"],
    "special_active": set(cfg["specialActive"]),
    "vix_band":    (float(cfg["vixLo"]), float(cfg["vixHi"])),
    "cor1m_min":   float(cfg["cor1mMin"]),
})

# run_backtest prints a COR1M info line — keep stdout clean for our JSON.
buf = io.StringIO()
with redirect_stdout(buf):
    data    = m.load_bundles([params["entry_time"], params["exit_time"]])
    halfday = m.load_halfday_close()
    bs      = m.load_bs_data()
    res     = m.run_backtest(data, params, halfday, bs_data=bs)
sys.stderr.write(buf.getvalue())

trades = res["trades"]
print(json.dumps({
    "count":  len(trades),
    "total":  sum(t["pnl"] for t in trades),
    "byDate": {t["openDate"]: t["pnl"] for t in trades},
    "srcByDate": {t["openDate"]: t["priceSource"] for t in trades},
    "skipped": res["skipped"],
    "dataDays": len(data["dates"]),
}))
`;

function runPyBacktest(cfg) {
  return new Promise((resolve, reject) => {
    execFile(
      'python3', ['-c', PY_WRAPPER, JSON.stringify(cfg)],
      { cwd: REPO_ROOT, maxBuffer: 64 * 1024 * 1024, timeout: 120000 },
      (err, stdout, stderr) => {
        if (stderr.trim()) console.error(`  [python] ${stderr.trim()}`);
        if (err) return reject(new Error(`python wrapper failed: ${err.message}`));
        try { resolve(JSON.parse(stdout)); }
        catch (e) { reject(new Error(`python wrapper printed non-JSON: ${stdout.slice(0, 400)}`)); }
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

  // 1. Trade count
  const countOk = js.count === py.count;
  console.log(`\nTrade count : JS=${js.count}  PY=${py.count}  → ${countOk ? 'MATCH' : 'MISMATCH'}`);
  if (!countOk) problems.push(`trade count differs: JS=${js.count} PY=${py.count}`);

  // 2. Total P/L
  const dTotal = js.total - py.total;
  const totalOk = Math.abs(dTotal) <= PNL_TOL;
  console.log(`Total P/L   : JS=${fmt$(js.total)}  PY=${fmt$(py.total)}  Δ=${fmt$(dTotal)}  → ${totalOk ? 'MATCH (±$1)' : 'MISMATCH'}`);
  if (!totalOk) problems.push(`total P/L differs by ${fmt$(dTotal)}`);

  // 3. Per-date P/L
  const jsDates = Object.keys(js.byDate);
  const pyDates = Object.keys(py.byDate);
  const onlyJs = jsDates.filter(d => !(d in py.byDate)).sort();
  const onlyPy = pyDates.filter(d => !(d in js.byDate)).sort();
  const common = jsDates.filter(d => d in py.byDate).sort();
  const valueDiffs = [];
  for (const d of common) {
    const a = js.byDate[d], b = py.byDate[d];
    if (Math.abs(a - b) > PNL_TOL) {
      valueDiffs.push({ d, js: a, py: b });
    }
  }
  console.log(`Per-date    : ${common.length} common, ${onlyJs.length} only-JS, ${onlyPy.length} only-PY, ${valueDiffs.length} value mismatches (>±$1)`);

  if (onlyJs.length) {
    problems.push(`${onlyJs.length} date(s) traded ONLY by JS`);
    console.log('\n  Dates traded ONLY in JS (diagonal.html):');
    capped(onlyJs.map(d => `    ${d}  pnl=${fmt$(js.byDate[d])}  src=${js.srcByDate[d]}`)).forEach(l => console.log(l));
  }
  if (onlyPy.length) {
    problems.push(`${onlyPy.length} date(s) traded ONLY by Python`);
    console.log('\n  Dates traded ONLY in Python (compute_diagonal_pnl.py):');
    capped(onlyPy.map(d => `    ${d}  pnl=${fmt$(py.byDate[d])}  src=${py.srcByDate[d]}`)).forEach(l => console.log(l));
  }
  if (valueDiffs.length) {
    problems.push(`${valueDiffs.length} date(s) with per-date P/L diff > $${PNL_TOL}`);
    console.log('\n  Per-date P/L mismatches:');
    capped(valueDiffs.map(({ d, js: a, py: b }) =>
      `    ${d}  JS=${fmt$(a)}  PY=${fmt$(b)}  Δ=${fmt$(a - b)}  (src JS=${js.srcByDate[d]} PY=${py.srcByDate[d]})`,
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
    console.log('Diagonal parity test: diagonal.html (JS) vs compute_diagonal_pnl.py (Python)');
    console.log(`Serving ${REPO_ROOT} on http://127.0.0.1:${port}\n`);

    console.log('1/3  Running JS backtest in headless Chrome (page defaults)…');
    const { setup, result: js } = await runJsBacktest(`http://127.0.0.1:${port}`);
    console.log(`     page params: ${setup.from} → ${setup.to}, entry/exit ${setup.entryTime}/${setup.exitTime}, ` +
      `strikes +${setup.shortOffset}/-${setup.longOffset}, DTE ${setup.shortDte}/${setup.longDte}±${setup.longDteTol} (${setup.dteFallback})`);
    console.log(`     filters: ${setup.specialMode} [${setup.specialActive.join(', ')}], ` +
      `VIX band ${setup.vixLo}-${setup.vixHi}, COR1M≥${setup.cor1mMin} (${setup.cor1mDays} days loaded)`);
    console.log(`     data: ${setup.dataDays} B-S days, ${setup.realDays} REAL days → ${js.count} trades, total ${fmt$(js.total)}`);

    console.log('\n2/3  Running Python run_backtest with the same params (no history write)…');
    const py = await runPyBacktest(setup);
    console.log(`     data: ${py.dataDays} bundle days → ${py.count} trades, total ${fmt$(py.total)}  skipped=${JSON.stringify(py.skipped)}`);

    console.log('\n3/3  Comparing…');
    const problems = compare(js, py);

    const secs = ((Date.now() - t0) / 1000).toFixed(1);
    if (problems.length) {
      console.error(`\n✗ PARITY FAILED in ${secs}s — diagonal.html and compute_diagonal_pnl.py disagree:`);
      problems.forEach(p => console.error(`    • ${p}`));
      console.error('\n  The two implementations have drifted (see lessons.md P5 for the last');
      console.error('  time this happened). Do NOT regenerate diagPL history until resolved.');
      process.exit(1);
    }
    console.log(`\n✓ PARITY OK in ${secs}s — ${js.count} trades, total ${fmt$(js.total)}, every per-date P/L within ±$${PNL_TOL}.`);
    process.exit(0);
  } finally {
    server.close();
  }
}

main().catch(err => {
  console.error('parity test crashed:', err.message);
  process.exit(2);
});

#!/usr/bin/env node
/**
 * Headless-Chrome smoke test for dashboard pages.
 *
 * Loads each page in headless Chrome, waits for DOMContentLoaded + module
 * scripts to settle, captures console.error messages. Exits non-zero if any
 * page produced a JS error.
 *
 * Wired into the pre-commit hook via scripts/check-html-smoke.sh.
 *
 * Usage:
 *   node scripts/smoke-test-dashboard.js [file1.html file2.html ...]
 *   (no args = run all default pages)
 */
import puppeteer from 'puppeteer-core';
import http from 'http';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const REPO_ROOT = path.resolve(__dirname, '..');

// macOS-installed Chrome. CI runners use Linux paths via env override.
const CHROME_PATH =
  process.env.PUPPETEER_EXECUTABLE_PATH ||
  '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';

// Default pages to smoke-test. Override via CLI args.
const DEFAULT_PAGES = [
  'index.html',
  'history.html',
  'diagonal.html',
  'cor1m_contango.html',
  'multi-strategy-tester.html',
  'gxbf-backtester.html',
];

// Console messages matching these patterns are NOT failures.
// Add new patterns conservatively — false negatives let real bugs through.
const IGNORE_PATTERNS = [
  /favicon\.ico/i,                              // 404 on favicon is fine
  /Failed to load resource.*404/i,              // missing optional assets
  /history_data\.json.*404/,                    // local fetches may fail
  /cor1m_contango_bundle\.json.*404/,
  /Refused to load.*Content Security Policy/i,  // CSP warnings are HTTP-fetch only
  /net::ERR_FAILED.*\.json/,                    // local data fetches
];

function shouldIgnore(message) {
  return IGNORE_PATTERNS.some(re => re.test(message));
}

// ── Minimal HTTP server so module scripts load (file:// blocks CORS) ───────
function startServer(rootDir) {
  return new Promise(resolve => {
    const types = {
      '.html': 'text/html', '.js': 'application/javascript',
      '.css': 'text/css', '.json': 'application/json',
      '.svg': 'image/svg+xml', '.png': 'image/png',
    };
    const server = http.createServer((req, res) => {
      let urlPath = req.url.split('?')[0];
      if (urlPath === '/') urlPath = '/index.html';
      const filePath = path.join(rootDir, urlPath);
      // Path traversal guard
      if (!filePath.startsWith(rootDir)) {
        res.writeHead(403); return res.end();
      }
      fs.readFile(filePath, (err, data) => {
        if (err) { res.writeHead(404); return res.end(); }
        const ext = path.extname(filePath);
        res.writeHead(200, { 'Content-Type': types[ext] || 'application/octet-stream' });
        res.end(data);
      });
    });
    server.listen(0, '127.0.0.1', () => {
      resolve({ server, port: server.address().port });
    });
  });
}

async function smokeTest(pages) {
  if (!fs.existsSync(CHROME_PATH)) {
    console.error(`✗ Chrome not found at ${CHROME_PATH}`);
    console.error('  Set PUPPETEER_EXECUTABLE_PATH or install Chrome.');
    process.exit(1);
  }

  const { server, port } = await startServer(REPO_ROOT);
  const baseUrl = `http://127.0.0.1:${port}`;

  let browser;
  try {
    browser = await puppeteer.launch({
      executablePath: CHROME_PATH,
      headless: 'new',
      args: ['--no-sandbox', '--disable-dev-shm-usage'],
    });

    const failures = [];

    for (const pageFile of pages) {
      const filePath = path.join(REPO_ROOT, pageFile);
      if (!fs.existsSync(filePath)) {
        console.error(`✗ ${pageFile}: file not found`);
        failures.push({ page: pageFile, errors: ['file not found'] });
        continue;
      }

      const page = await browser.newPage();
      const errors = [];
      page.on('pageerror', err => errors.push(`pageerror: ${err.message}`));
      page.on('console', msg => {
        if (msg.type() === 'error') {
          const text = msg.text();
          if (!shouldIgnore(text)) errors.push(`console.error: ${text}`);
        }
      });

      try {
        await page.goto(`${baseUrl}/${pageFile}`, {
          waitUntil: 'networkidle2',
          timeout: 15000,
        });
        // Give module scripts + DOMContentLoaded handlers a moment to settle
        await new Promise(r => setTimeout(r, 1500));
      } catch (e) {
        errors.push(`navigation failed: ${e.message}`);
      }

      await page.close();

      if (errors.length) {
        console.error(`✗ ${pageFile}`);
        errors.forEach(e => console.error(`    ${e}`));
        failures.push({ page: pageFile, errors });
      } else {
        console.log(`✓ ${pageFile}`);
      }
    }

    return failures;
  } finally {
    if (browser) await browser.close();
    server.close();
  }
}

const pages = process.argv.slice(2).length > 0 ? process.argv.slice(2) : DEFAULT_PAGES;

console.log(`Smoke testing ${pages.length} page(s) in headless Chrome…`);
smokeTest(pages).then(failures => {
  if (failures.length) {
    console.error(`\n✗ ${failures.length} page(s) failed smoke test. Fix before committing.`);
    process.exit(1);
  }
  console.log(`\n✓ All ${pages.length} page(s) loaded without JS errors.`);
  process.exit(0);
}).catch(err => {
  console.error('smoke test crashed:', err.message);
  process.exit(2);
});

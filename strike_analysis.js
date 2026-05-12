#!/usr/bin/env node
/**
 * Strike Ending Analysis — Full Week
 * Reads TRADES from backtester.html, applies M8BF model logic,
 * then groups by last 2 digits of center strike × day of week.
 */

const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'backtester.html'), 'utf8');

// Extract TRADES array
const tradesMatch = html.match(/const TRADES\s*=\s*(\[[\s\S]*?\]);\s*\n/);
if (!tradesMatch) { console.error('Could not find TRADES'); process.exit(1); }
const TRADES = eval(tradesMatch[1]);

// Extract M8BF_SKIP
const skipMatch = html.match(/const M8BF_SKIP\s*=\s*new Set\((\[[\s\S]*?\])\)/);
const M8BF_SKIP = new Set(eval(skipMatch[1]));

// Constants (from backtester.html)
const D=0,DAY=1,CP=2,PREM=3,SPR=4,PROF=5,MAXP=6,CTR=7,TIME=8,PEAK=9,TROUGH=10,PKFIRST=11;
const M8BF_BLOCKED = new Set([10,25,35,40,65,80]);
const M8BF_COMBO_BANS = {0:95,20:15,55:50,65:60,85:90};
const M8BF_WIN = {0:['11:00','11:30'],1:['13:30','14:00'],2:['12:00','12:30'],3:['11:00','11:30'],4:['13:00','13:30']};
const DOW_NAMES = ['Mon','Tue','Wed','Thu','Fri'];

// Build M8BF trades (same logic as backtester.html)
function buildM8BFTrades() {
  const byDate = {};
  for (const t of TRADES) { (byDate[t[D]] = byDate[t[D]] || []).push(t); }
  const result = [];
  for (const date of Object.keys(byDate).sort()) {
    if (M8BF_SKIP.has(date)) continue;
    const dayTrades = byDate[date];
    const dow = dayTrades[0][DAY];
    const win = M8BF_WIN[dow];
    if (!win) continue;
    const [ws, we] = win;
    const q = dayTrades
      .filter(t => {
        if (t[TIME] < ws || t[TIME] > we) return false;
        const c = t[CTR] % 100;
        if (M8BF_BLOCKED.has(c)) return false;
        if (M8BF_COMBO_BANS[(t[CTR] - t[SPR]) % 100] === c) return false;
        return true;
      })
      .sort((a, b) => a[TIME] < b[TIME] ? -1 : 1);
    if (q.length) result.push(q[0]);
  }
  return result;
}

// Also build ALL trades in window (not just first) to get full picture per strike ending
function buildAllM8BFWindowTrades() {
  const byDate = {};
  for (const t of TRADES) { (byDate[t[D]] = byDate[t[D]] || []).push(t); }
  const result = [];
  for (const date of Object.keys(byDate).sort()) {
    if (M8BF_SKIP.has(date)) continue;
    const dayTrades = byDate[date];
    const dow = dayTrades[0][DAY];
    const win = M8BF_WIN[dow];
    if (!win) continue;
    const [ws, we] = win;
    // Get first qualifying trade (the one M8BF would pick)
    const q = dayTrades
      .filter(t => {
        if (t[TIME] < ws || t[TIME] > we) return false;
        const c = t[CTR] % 100;
        if (M8BF_BLOCKED.has(c)) return false;
        if (M8BF_COMBO_BANS[(t[CTR] - t[SPR]) % 100] === c) return false;
        return true;
      })
      .sort((a, b) => a[TIME] < b[TIME] ? -1 : 1);
    if (q.length) result.push(q[0]);
  }
  return result;
}

const m8bfTrades = buildM8BFTrades();

console.log(`Total M8BF trades: ${m8bfTrades.length}\n`);

// Determine AM/PM split — if entry time < 12:00 it's AM, else PM
function isAM(t) { return t[TIME] < '12:00'; }

// Group by (last2, dow)
// For each combo: collect profits
const stats = {}; // key: `${last2}_${dow}`

for (const t of m8bfTrades) {
  const last2 = t[CTR] % 100;
  const dow = t[DAY];
  const key = `${last2}_${dow}`;
  if (!stats[key]) stats[key] = { wins: 0, losses: 0, amPL: 0, pmPL: 0, totalPL: 0, trades: 0, amTrades: 0, pmTrades: 0 };
  const s = stats[key];
  s.trades++;
  s.totalPL += t[PROF];
  if (t[PROF] > 0) s.wins++;
  else s.losses++;
  if (isAM(t)) { s.amPL += t[PROF]; s.amTrades++; }
  else { s.pmPL += t[PROF]; s.pmTrades++; }
}

// Also compute overall stats per last2 (all days combined)
const overallStats = {};
for (const t of m8bfTrades) {
  const last2 = t[CTR] % 100;
  if (!overallStats[last2]) overallStats[last2] = { wins: 0, losses: 0, amPL: 0, pmPL: 0, totalPL: 0, trades: 0, amTrades: 0, pmTrades: 0 };
  const s = overallStats[last2];
  s.trades++;
  s.totalPL += t[PROF];
  if (t[PROF] > 0) s.wins++;
  else s.losses++;
  if (isAM(t)) { s.amPL += t[PROF]; s.amTrades++; }
  else { s.pmPL += t[PROF]; s.pmTrades++; }
}

// Get all unique last2 values, sorted
const allLast2 = [...new Set(m8bfTrades.map(t => t[CTR] % 100))].sort((a, b) => a - b);

// Print per-day tables
for (let dow = 0; dow < 5; dow++) {
  const dayName = DOW_NAMES[dow];
  const [ws, we] = M8BF_WIN[dow];
  console.log(`\n${'='.repeat(70)}`);
  console.log(`  ${dayName.toUpperCase()} (Window: ${ws}–${we})`);
  console.log(`${'='.repeat(70)}`);
  console.log(`${'Last2'.padStart(6)}  ${'Win%'.padStart(6)}  ${'AM P/L'.padStart(8)}  ${'PM P/L'.padStart(8)}  ${'Total'.padStart(8)}  ${'Trades'.padStart(6)}`);
  console.log(`${'-'.repeat(6)}  ${'-'.repeat(6)}  ${'-'.repeat(8)}  ${'-'.repeat(8)}  ${'-'.repeat(8)}  ${'-'.repeat(6)}`);

  const dayLast2 = allLast2.filter(l2 => stats[`${l2}_${dow}`]);
  let dayTotalPL = 0, dayTotalTrades = 0, dayTotalWins = 0;

  for (const l2 of dayLast2) {
    const s = stats[`${l2}_${dow}`];
    if (!s) continue;
    const winPct = s.trades > 0 ? (s.wins / s.trades * 100).toFixed(0) : '—';
    const amAvg = s.amTrades > 0 ? Math.round(s.amPL / s.amTrades) : 0;
    const pmAvg = s.pmTrades > 0 ? Math.round(s.pmPL / s.pmTrades) : 0;
    const blocked = M8BF_BLOCKED.has(l2) ? ' ✗' : '';
    console.log(`${String(l2).padStart(5)}${blocked}  ${String(winPct + '%').padStart(6)}  ${('$' + amAvg).padStart(8)}  ${('$' + pmAvg).padStart(8)}  ${('$' + s.totalPL).padStart(8)}  ${String(s.trades).padStart(6)}`);
    dayTotalPL += s.totalPL;
    dayTotalTrades += s.trades;
    dayTotalWins += s.wins;
  }

  const dayWinPct = dayTotalTrades > 0 ? (dayTotalWins / dayTotalTrades * 100).toFixed(0) : '—';
  console.log(`${'-'.repeat(6)}  ${'-'.repeat(6)}  ${'-'.repeat(8)}  ${'-'.repeat(8)}  ${'-'.repeat(8)}  ${'-'.repeat(6)}`);
  console.log(`${'TOTAL'.padStart(6)}  ${String(dayWinPct + '%').padStart(6)}  ${''.padStart(8)}  ${''.padStart(8)}  ${('$' + dayTotalPL).padStart(8)}  ${String(dayTotalTrades).padStart(6)}`);
}

// Overall summary
console.log(`\n${'='.repeat(70)}`);
console.log(`  ALL DAYS COMBINED`);
console.log(`${'='.repeat(70)}`);
console.log(`${'Last2'.padStart(6)}  ${'Win%'.padStart(6)}  ${'AM P/L'.padStart(8)}  ${'PM P/L'.padStart(8)}  ${'Total'.padStart(8)}  ${'Trades'.padStart(6)}`);
console.log(`${'-'.repeat(6)}  ${'-'.repeat(6)}  ${'-'.repeat(8)}  ${'-'.repeat(8)}  ${'-'.repeat(8)}  ${'-'.repeat(6)}`);

let grandTotal = 0, grandTrades = 0, grandWins = 0;
for (const l2 of allLast2) {
  const s = overallStats[l2];
  if (!s) continue;
  const winPct = s.trades > 0 ? (s.wins / s.trades * 100).toFixed(0) : '—';
  const amAvg = s.amTrades > 0 ? Math.round(s.amPL / s.amTrades) : 0;
  const pmAvg = s.pmTrades > 0 ? Math.round(s.pmPL / s.pmTrades) : 0;
  const blocked = M8BF_BLOCKED.has(l2) ? ' ✗' : '';
  console.log(`${String(l2).padStart(5)}${blocked}  ${String(winPct + '%').padStart(6)}  ${('$' + amAvg).padStart(8)}  ${('$' + pmAvg).padStart(8)}  ${('$' + s.totalPL).padStart(8)}  ${String(s.trades).padStart(6)}`);
  grandTotal += s.totalPL;
  grandTrades += s.trades;
  grandWins += s.wins;
}

const grandWinPct = grandTrades > 0 ? (grandWins / grandTrades * 100).toFixed(0) : '—';
console.log(`${'-'.repeat(6)}  ${'-'.repeat(6)}  ${'-'.repeat(8)}  ${'-'.repeat(8)}  ${'-'.repeat(8)}  ${'-'.repeat(6)}`);
console.log(`${'TOTAL'.padStart(6)}  ${String(grandWinPct + '%').padStart(6)}  ${''.padStart(8)}  ${''.padStart(8)}  ${('$' + grandTotal).padStart(8)}  ${String(grandTrades).padStart(6)}`);

#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'backtester.html'), 'utf8');

// Extract TRADES array — the FULL database
const tradesMatch = html.match(/const TRADES\s*=\s*(\[[\s\S]*?\]);\s*\n/);
if (!tradesMatch) { console.error('Could not find TRADES'); process.exit(1); }
const TRADES = eval(tradesMatch[1]);

const D=0,DAY=1,CP=2,PREM=3,SPR=4,PROF=5,MAXP=6,CTR=7,TIME=8;

console.log(`Total trades in database: ${TRADES.length}`);

// Build data structure per day: { 0: { last2: {wins, totalPL, trades} }, ... }
const DATA = {};
for (let dow = 0; dow < 5; dow++) DATA[dow] = {};

for (const t of TRADES) {
  const last2 = t[CTR] % 100;
  const dow = t[DAY];
  if (!DATA[dow][last2]) DATA[dow][last2] = { last2, wins:0, totalPL:0, trades:0 };
  const s = DATA[dow][last2];
  s.trades++;
  s.totalPL += t[PROF];
  if (t[PROF] > 0) s.wins++;
}

// Convert to sorted arrays with winPct
const result = {};
for (let dow = 0; dow < 5; dow++) {
  result[dow] = Object.values(DATA[dow])
    .map(s => ({ ...s, winPct: s.trades > 0 ? Math.round(s.wins / s.trades * 100) : 0 }))
    .sort((a, b) => a.last2 - b.last2);
}

// Read template and inject data
let template = fs.readFileSync(path.join(__dirname, 'strike_table_template.html'), 'utf8');
template = template.replace('/*DATA_PLACEHOLDER*/null', JSON.stringify(result));
fs.writeFileSync(path.join(__dirname, 'strike_table.html'), template);

let total = 0;
for (let dow = 0; dow < 5; dow++) for (const r of result[dow]) total += r.trades;
console.log(`Output trades: ${total}`);
console.log('Done.');

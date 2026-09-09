// Σ3 Discord card — skin A "Ticket" (owner pick 2026-09-09). Pure SVG string builder,
// no runtime deps, so the worker and the local preview render the exact same bytes.
// A card is one data object:
//   { c: 'amber'|'green'|'gray'|'blue'|'red', strat, verdict, sub, lines:[..], k:[[label,value]..],
//     title, draw:'none'|'fly'|'straddle'|'condor'|'diag'|'track'|'gauge', strikes:[..], track:{}, gauge:n }
// Colors are fixed by type: amber heads-up, green trade, gray no-trade/skip, blue execution, red loss/alert.

export const W = 520;                       // rendered at 2× by resvg for crisp Discord display
const RAIL = 8, PL = 24, PR = 18, INNER = W - PL - PR;
const C = { card: '#1e1f22', tile: '#2a2c30', line: '#3a3d44', text: '#f2f3f5', sub: '#b5bac1', mute: '#80848e',
  amber: '#f5b942', green: '#4ade80', gray: '#9aa0a8', blue: '#7ab8ff', red: '#f87171' };
const F = 'Inter', M = 'JetBrains Mono';

export const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
// crude but stable width estimates (px) for wrapping — Inter avg 0.53em, JetBrains Mono 0.6em
const wI = (s, px) => String(s).length * px * 0.53;
const wM = (s, px) => String(s).length * px * 0.6;

export function wrap(text, px, maxW, maxLines = 3) {
  const words = String(text ?? '').split(/\s+/).filter(Boolean);
  const lines = []; let cur = '';
  for (const w of words) {
    const t = cur ? cur + ' ' + w : w;
    if (wI(t, px) <= maxW) cur = t;
    else { if (cur) lines.push(cur); cur = w; }
  }
  if (cur) lines.push(cur);
  if (lines.length > maxLines) { lines.length = maxLines; lines[maxLines - 1] = lines[maxLines - 1].replace(/\s*\S*$/, '') + '…'; }
  return lines;
}

// ── drawings (each returns { svg, h }) drawn inside x∈[PL, PL+INNER] ──
function xmap(lo, hi) { return (v) => PL + 8 + (v - lo) / (hi - lo) * (INNER - 16); }
const lbl = (x, y, t, anchor = 'middle', fill = C.sub) => `<text x="${x}" y="${y}" font-family="${F}" font-size="11" fill="${fill}" text-anchor="${anchor}">${esc(t)}</text>`;
const DR = {
  fly(y, col, [a, b, c]) {
    const x = xmap(a, c);
    return { h: 64, svg: `<line x1="${PL}" y1="${y + 44}" x2="${PL + INNER}" y2="${y + 44}" stroke="${C.line}"/>` +
      `<polyline points="${x(a)},${y + 44} ${x(b)},${y + 8} ${x(c)},${y + 44}" fill="none" stroke="${col}" stroke-width="2.5" stroke-linejoin="round"/>` +
      lbl(x(a), y + 60, a) + lbl(x(b), y + 60, b) + lbl(x(c), y + 60, c) };
  },
  straddle(y, col, [k]) {
    const x = xmap(k - 40, k + 40);
    return { h: 64, svg: `<line x1="${PL}" y1="${y + 24}" x2="${PL + INNER}" y2="${y + 24}" stroke="${C.line}"/>` +
      `<polyline points="${x(k - 35)},${y + 6} ${x(k)},${y + 44} ${x(k + 35)},${y + 6}" fill="none" stroke="${col}" stroke-width="2.5" stroke-linejoin="round"/>` +
      lbl(x(k), y + 60, k) + lbl(x(k - 35), y + 60, k - 35) + lbl(x(k + 35), y + 60, k + 35) };
  },
  condor(y, col, [a, b, c, d]) {
    const x = xmap(a - 10, d + 10);
    return { h: 64, svg: `<line x1="${PL}" y1="${y + 44}" x2="${PL + INNER}" y2="${y + 44}" stroke="${C.line}"/>` +
      `<polyline points="${PL + 8},${y + 44} ${x(a)},${y + 44} ${x(b)},${y + 10} ${x(c)},${y + 10} ${x(d)},${y + 44} ${PL + INNER - 8},${y + 44}" fill="none" stroke="${col}" stroke-width="2.5" stroke-linejoin="round"/>` +
      [a, b, c, d].map((v) => lbl(x(v), y + 60, v)).join('') };
  },
  diag(y, col, [l, s]) {
    return { h: 64, svg: `<line x1="${PL}" y1="${y + 44}" x2="${PL + INNER}" y2="${y + 44}" stroke="${C.line}"/>` +
      `<path d="M${PL + 8} ${y + 14} C ${PL + 180} ${y + 14}, ${PL + 300} ${y + 38}, ${PL + INNER - 8} ${y + 44}" fill="none" stroke="${col}" stroke-width="2.5"/>` +
      lbl(PL + 130, y + 60, `long ${l}P · 25d`) + lbl(PL + INNER - 130, y + 60, `short ${s}P · 1d`) };
  },
  track(y, col, tr) {
    const x = xmap(tr.lo, tr.hi);
    const dot = (v, fill, t, ty) => v == null ? '' : `<circle cx="${x(v)}" cy="${y + 30}" r="6" fill="${fill}"/>` + lbl(x(v), ty, t);
    return { h: 66, svg: `<line x1="${PL + 8}" y1="${y + 30}" x2="${PL + INNER - 8}" y2="${y + 30}" stroke="${C.line}" stroke-width="2"/>` +
      dot(tr.t1, C.mute, `T1 ${tr.t1}`, y + 14) + dot(tr.magnet, col, `magnet ${tr.magnet}`, y + 14) + dot(tr.center, C.text, `center ${tr.center}`, y + 56) };
  },
  gauge(y, col, v) {
    const x = PL + 8 + ((Math.max(-20, Math.min(20, v)) + 20) / 40) * (INNER - 16);
    return { h: 46, svg: `<defs><linearGradient id="gg"><stop offset="0" stop-color="${C.red}"/><stop offset=".5" stop-color="${C.line}"/><stop offset="1" stop-color="${C.green}"/></linearGradient></defs>` +
      `<rect x="${PL + 8}" y="${y + 10}" width="${INNER - 16}" height="8" rx="4" fill="url(#gg)"/><rect x="${x - 1.5}" y="${y + 4}" width="3" height="20" rx="1.5" fill="#ffffff"/>` +
      lbl(PL + 8, y + 40, '−20B · call spread zone', 'start', C.mute) + lbl(PL + INNER / 2, y + 40, '0', 'middle', C.mute) + lbl(PL + INNER - 8, y + 40, '+20B · condor zone', 'end', C.mute) };
  },
};

export function ticketSvg(m) {
  const col = C[m.c] || C.gray;
  let s = '', y = 0;
  // header: strategy name + verdict stamp
  const stampTxt = String(m.verdict || '').toUpperCase();
  const stampW = Math.round(wI(stampTxt, 12) * 1.12 + 22);
  s += `<text x="${PL}" y="38" font-family="${F}" font-size="15" font-weight="600" fill="${C.text}">${esc(m.strat)}</text>`;
  s += `<rect x="${W - PR - stampW}" y="21" width="${stampW}" height="24" rx="6" fill="none" stroke="${col}" stroke-width="2"/>`;
  s += `<text x="${W - PR - stampW / 2}" y="38" text-anchor="middle" font-family="${F}" font-size="12" font-weight="600" letter-spacing="1" fill="${col}">${esc(stampTxt)}</text>`;
  y = 62;
  // big line: monospaced strikes/verdict + muted sub
  const big = String(m.big ?? m.sub ?? ''), small = m.big != null ? String(m.sub ?? '') : '';
  const bigPx = wM(big, 24) <= INNER ? 24 : wM(big, 19) <= INNER ? 19 : 15;   // long structures shrink instead of clipping
  const bigW = wM(big, bigPx);
  s += `<text x="${PL}" y="${y + 18}" font-family="${M}" font-size="${bigPx}" font-weight="600" fill="${m.c === 'gray' ? C.sub : C.text}">${esc(big)}</text>`;
  if (small) {
    if (bigW + 10 + wI(small, 14) <= INNER) s += `<text x="${PL + bigW + 10}" y="${y + 18}" font-family="${F}" font-size="14" fill="${C.mute}">${esc(small)}</text>`;
    else { y += 20; s += `<text x="${PL}" y="${y + 16}" font-family="${F}" font-size="13" fill="${C.mute}">${esc(small)}</text>`; }
  }
  y += 34;
  // description lines (wrapped)
  const desc = wrap((m.lines || []).join(' '), 13, INNER, 3);
  for (const ln of desc) { s += `<text x="${PL}" y="${y + 12}" font-family="${F}" font-size="13" fill="${C.sub}">${esc(ln)}</text>`; y += 18; }
  y += 6;
  // drawing
  if (m.draw && m.draw !== 'none' && DR[m.draw]) {
    const d = DR[m.draw](y + 2, col, m.draw === 'track' ? m.track : m.draw === 'gauge' ? m.gauge : m.strikes);
    s += d.svg; y += d.h + 8;
  }
  // KPI tiles (≤4)
  const k = (m.k || []).slice(0, 4);
  if (k.length) {
    const gap = 8, tw = (INNER - gap * (k.length - 1)) / k.length, th = 44;
    k.forEach(([l, v], i) => {
      const x = PL + i * (tw + gap);
      const vs = String(v); const vpx = wI(vs, 14) > tw - 20 ? 12 : 14;
      s += `<rect x="${x}" y="${y}" width="${tw}" height="${th}" rx="8" fill="${C.tile}"/>`;
      s += `<text x="${x + 10}" y="${y + 16}" font-family="${F}" font-size="10" font-weight="600" letter-spacing="0.8" fill="${C.mute}">${esc(String(l).toUpperCase())}</text>`;
      s += `<text x="${x + 10}" y="${y + 34}" font-family="${F}" font-size="${vpx}" font-weight="600" fill="${C.text}">${esc(vs)}</text>`;
    });
    y += th + 12;
  }
  // footer
  y += 2;
  s += `<line x1="${PL}" y1="${y}" x2="${W - PR}" y2="${y}" stroke="${C.line}"/>`;
  s += `<text x="${PL}" y="${y + 20}" font-family="${F}" font-size="11" fill="${C.mute}">${esc(m.title || '')}</text>`;
  // disclaimer as a bold warning pill (owner 2026-09-09: "bolder, yellow, a little aggressive")
  const nfa = 'NOT FINANCIAL ADVICE', nfaW = Math.round(wI(nfa, 10.5) * 1.15 + 20), nfaH = 20;
  s += `<rect x="${W - PR - nfaW}" y="${y + 7}" width="${nfaW}" height="${nfaH}" rx="5" fill="${C.amber}"/>`;
  s += `<text x="${W - PR - nfaW / 2}" y="${y + 21}" text-anchor="middle" font-family="${F}" font-size="10.5" font-weight="600" letter-spacing="0.8" fill="#412402">${nfa}</text>`;
  const H = y + 34;
  return { height: H, svg: `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}">` +
    `<defs><clipPath id="card"><rect x="0" y="0" width="${W}" height="${H}" rx="16"/></clipPath></defs>` +
    `<rect x="0" y="0" width="${W}" height="${H}" rx="16" fill="${C.card}"/>` +
    `<rect x="0" y="0" width="${RAIL}" height="${H}" fill="${col}" clip-path="url(#card)"/>` + s + `</svg>` };
}

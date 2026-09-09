// Local preview: render every catalog card through the SAME resvg pipeline the worker uses
// (Inter 400/600 + JetBrains Mono 600), save PNGs, and write an approval page.
import fs from 'node:fs';
import path from 'node:path';
import { Resvg, initWasm } from '@resvg/resvg-wasm';
import { ticketSvg, esc } from './ticket.mjs';
import { CATALOG } from './catalog.mjs';

const SP = '/private/tmp/claude-501/-Users-ravshanrakhmanov/6231789a-973f-4240-9e34-6d2b27bd3ee0/scratchpad';
const OUT = path.join(SP, 'cards'); fs.mkdirSync(OUT, { recursive: true });
await initWasm(fs.readFileSync(new URL('../node_modules/@resvg/resvg-wasm/index_bg.wasm', import.meta.url)));
const fonts = ['Inter_400Regular.ttf', 'Inter_600SemiBold.ttf', 'JetBrainsMono_600SemiBold.ttf', 'JetBrainsMono_500Medium.ttf']
  .map((f) => fs.readFileSync(path.join(SP, 'fonts', f)));

let n = 0, bytes = 0; const sections = [];
for (const g of CATALOG) {
  const msgs = [];
  for (const m of g.m) {
    let body;
    if (m.text) body = `<div class="txt"><span class="mini" style="background:var(--${m.c})"></span>${esc(m.lines[0])}</div>`;
    else {
      const { svg, height } = ticketSvg(m);
      const png = new Resvg(svg, { fitTo: { mode: 'width', value: 1040 }, font: { fontBuffers: fonts, defaultFontFamily: 'Inter', loadSystemFonts: false } }).render().asPng();
      n++; bytes += png.length;
      fs.writeFileSync(path.join(OUT, `${String(n).padStart(2, '0')}_${m.strat.replace(/[^a-z0-9]+/gi, '_')}.png`), png);
      body = `<img src="data:image/png;base64,${Buffer.from(png).toString('base64')}" width="520" height="${height}" alt="${esc(m.strat)} card" style="width:100%;max-width:520px;height:auto;border-radius:14px;display:block">`;
    }
    msgs.push(`<div class="msg"><div class="av">Σ3</div><div><div class="hdr">Σ3 Signals<span class="app">APP</span><span class="t">${esc(m.t)}</span></div>${body}${m.order ? `<div class="code">${esc(m.order)}<span class="cp">Copy</span></div>` : ''}</div></div>`);
  }
  sections.push(`<section class="grp"><h2>${esc(g.name)}</h2><p class="why">${esc(g.why)}</p><div class="chat">${msgs.join('')}</div></section>`);
}
const html = `<title>Σ3 Discord Cards</title>
<style>
:root{--chat:#313338;--side:#2b2d31;--text:#f2f3f5;--sub:#b5bac1;--mute:#80848e;--line:#3a3d44;--green:#4ade80;--red:#f87171;--amber:#f5b942;--blue:#7ab8ff;--gray:#9aa0a8;--code:#2b2d31}
*{box-sizing:border-box}body{margin:0;background:#1a1b1e;color:var(--text);font-family:Inter,"Segoe UI",system-ui,-apple-system,sans-serif;font-size:14px;line-height:1.5}
.wrap{max-width:1180px;margin:0 auto;padding:24px 20px 80px}h1{font-size:22px;font-weight:600;margin:0 0 4px}.lead{color:var(--sub);max-width:78ch;margin:0 0 22px}
.grp{margin:0 0 34px}.grp h2{font-size:16px;font-weight:600;margin:0 0 2px}.grp .why{color:var(--sub);margin:0 0 10px;font-size:13px}
.chat{background:var(--chat);border-radius:12px;padding:10px 22px 16px;max-width:760px}.msg{display:grid;grid-template-columns:40px 1fr;gap:14px;padding:10px 0}.msg + .msg{border-top:1px solid rgba(255,255,255,.04)}
.av{width:40px;height:40px;border-radius:50%;background:#f0b232;display:flex;align-items:center;justify-content:center;font-weight:700;color:#1e1f22;font-size:13px}
.hdr{font-size:14px;font-weight:500;margin-bottom:6px}.hdr .app{background:#5865f2;color:#fff;font-size:10px;font-weight:600;border-radius:4px;padding:1px 5px;margin-left:6px;vertical-align:1px}.hdr .t{color:var(--mute);font-weight:400;font-size:12px;margin-left:8px}
.code{margin-top:8px;max-width:520px;display:flex;align-items:center;gap:10px;background:var(--code);border:1px solid #1e1f22;border-radius:6px;padding:8px 10px;font-family:"JetBrains Mono",ui-monospace,Menlo,monospace;font-size:12.5px;color:#dbdee1}.code .cp{margin-left:auto;font-family:Inter,sans-serif;font-size:11px;color:var(--sub);border:1px solid var(--line);border-radius:4px;padding:2px 7px;white-space:nowrap}
.txt{max-width:520px;color:#dbdee1;font-size:14px}.txt .mini{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:8px;vertical-align:1px}
.note{margin-top:30px;padding:14px 16px;background:var(--side);border-radius:10px;color:var(--sub);font-size:13px;max-width:900px}.note b{color:var(--text)}
</style>
<div class="wrap"><h1>Σ3 Discord cards · round three · real renders, ticket skin</h1>
<p class="lead">These are actual PNGs from the worker's own SVG-to-PNG pipeline with the embedded fonts, at the size Discord shows them. What you see here is byte-for-byte what the channel will get. The order string stays a code block under each trade card.</p>
${sections.join('')}
<div class="note"><b>Nothing pushed.</b> ${n} cards rendered. Tell me what to cut, merge or reword; when you say push, I wire each sender to its card and deploy.</div></div>`;
fs.writeFileSync(path.join(SP, 'discord-cards-a.html'), html);
console.log(`rendered ${n} cards · ${(bytes / 1024).toFixed(0)} KB PNG total · page ${(html.length / 1024).toFixed(0)} KB → ${path.join(SP, 'discord-cards-a.html')}`);

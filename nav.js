// Shared site nav (2026-06-10) — single source for cross-page navigation.
// Every page includes:  <script src="nav.js?v=..." defer></script>
// Edit the PAGES list HERE only; never inline per-page copies (CLAUDE.md rule 2 spirit).
(function () {
  const PAGES = [
    ['index.html',                 '⌂ Dashboard'],
    ['live.html',                  '● Live'],
    ['history.html',               '☰ History'],
    ['backtester.html',            'M8BF BT'],
    ['gxbf-backtester.html',       'GXBF BT'],
    ['diagonal.html',              '◢ Diagonal'],
    ['multi-strategy-tester.html', '⊞ Multi'],
    ['cor1m_contango.html',        '▼ Tail Hedge'],
    ['cyclicality.html',           '◐ CycleLab'],
    ['gex.html',                   'Γ GEX'],
  ];
  function build() {
    const here = (location.pathname.split('/').pop() || 'index.html');
    const bar = document.createElement('nav');
    bar.id = 'siteNav';
    bar.style.cssText =
      'position:sticky;top:0;z-index:9999;display:flex;gap:4px;align-items:center;' +
      'overflow-x:auto;-webkit-overflow-scrolling:touch;padding:7px 10px;' +
      'background:#0b1220;border-bottom:1px solid #2d3f55;font:600 12.5px Inter,system-ui,sans-serif;';
    for (const [href, label] of PAGES) {
      const a = document.createElement('a');
      a.href = href;
      a.textContent = label;
      const on = here === href;
      a.style.cssText =
        'white-space:nowrap;text-decoration:none;padding:5px 11px;border-radius:7px;' +
        (on ? 'background:#6366f1;color:#fff;'
            : 'color:#94a3b8;background:#162032;border:1px solid #22324a;');
      if (!on) {
        a.onmouseenter = () => { a.style.color = '#e2e8f0'; a.style.background = '#1e293b'; };
        a.onmouseleave = () => { a.style.color = '#94a3b8'; a.style.background = '#162032'; };
      }
      bar.appendChild(a);
    }
    document.body.prepend(bar);
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', build);
  else build();
})();

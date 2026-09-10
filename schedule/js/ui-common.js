/* =====================================================================
 *  ui-common.js — small DOM/rendering helpers shared by every view
 * ===================================================================== */
const Views = {};            // each ui-*.js file registers itself here

const UI = (function () {
  'use strict';

  function esc(s) {
    return String(s === null || s === undefined ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }
  function el(html) {
    const t = document.createElement('template');
    t.innerHTML = html.trim();
    return t.content.firstElementChild;
  }
  function qs(root, sel) { return root.querySelector(sel); }
  function qsa(root, sel) { return Array.prototype.slice.call(root.querySelectorAll(sel)); }

  /* ---------- code chips ---------- */
  const TOUR_VAR = { A: 'a', B: 'b', C: 'c', R: 'r' };
  function codeClass(code) {
    if (!code) return 'code code-none';
    const meta = Engine.codeMeta(code);
    if (meta.double) return 'code code-double';
    if (['A', 'B', 'C', 'R', 'RDO', 'MDO', 'HOL'].indexOf(code) !== -1) return 'code code-' + code;
    if (meta.kind === 'leave') return 'code code-leave';
    if (meta.kind === 'work' && meta.tours && meta.tours.length === 1 && TOUR_VAR[meta.tours[0]]) return 'code code-' + meta.tours[0];
    return 'code code-other';
  }
  function codeStyle(code) {
    if (!code) return '';
    const meta = Engine.codeMeta(code);
    if (meta.double && meta.tours && meta.tours.length === 2) {
      const a = TOUR_VAR[meta.tours[0]] || 'r', b = TOUR_VAR[meta.tours[1]] || 'r';
      return 'background:linear-gradient(135deg,var(--c-' + a + '-bg) 0 50%,var(--c-' + b + '-bg) 50% 100%);';
    }
    return '';
  }
  function codeLabel(code) { return Engine.codeMeta(code).label; }
  function codeChip(code, size, extraClass) {
    const len = code ? String(code).length : 0;
    const cls = codeClass(code) + (size ? ' ' + size : '') + (extraClass ? ' ' + extraClass : '') + (len >= 5 ? ' len4 len5' : len === 4 ? ' len4' : '');
    const style = codeStyle(code);
    return '<span class="' + cls + '"' + (style ? ' style="' + style + '"' : '') + ' title="' + esc(codeLabel(code)) + '">' + esc(code || '—') + '</span>';
  }
  function codeOptions(selected) {
    const cfg = Engine.getConfig();
    const primary = cfg.primaryCodes.filter(function (c) { return cfg.codes[c]; });
    const rest = Object.keys(cfg.codes).filter(function (c) { return primary.indexOf(c) === -1; });
    function opt(c) { return '<option value="' + esc(c) + '"' + (c === selected ? ' selected' : '') + '>' + esc(c) + ' — ' + esc(cfg.codes[c].label) + '</option>'; }
    return '<optgroup label="Common">' + primary.map(opt).join('') + '</optgroup><optgroup label="Other codes">' + rest.map(opt).join('') + '</optgroup>';
  }
  function empOptions(selected, opts) {
    opts = opts || {};
    return Engine.activeRoster().filter(function (e) {
      if (opts.excludeId && e.id === opts.excludeId) return false;
      if (!opts.includeVacant && e.vacant) return false;
      return true;
    }).map(function (e) {
      return '<option value="' + esc(e.id) + '"' + (e.id === selected ? ' selected' : '') + '>' + esc(e.name) + ' (slot ' + e.slot + ')</option>';
    }).join('');
  }
  function empName(id) { const e = Engine.getEmployee(id); return e ? e.name : (id || '—'); }
  function empShort(id) { return Swaps.shortName(id); }
  function firstName(name) {
    if (!name) return '';
    const parts = name.split(',');
    return parts.length > 1 ? parts[1].trim().split(' ')[0] : name.split(' ')[0];
  }

  /* ---------- status pills / alerts ---------- */
  function statusPill(status) {
    const label = { pending: 'Pending', approved: 'Approved', rejected: 'Rejected', cancelled: 'Cancelled' }[status] || status;
    return '<span class="pill pill-' + esc(status) + '">' + esc(label) + '</span>';
  }
  function validationHTML(v, opts) {
    opts = opts || {};
    if (!v) return '';
    let h = '<div class="alerts">';
    (v.errors || []).forEach(function (e) { h += '<div class="alert alert-err">✖ ' + esc(e) + '</div>'; });
    (v.warnings || []).forEach(function (w) { h += '<div class="alert alert-warn">⚠ ' + esc(w) + '</div>'; });
    (v.notes || []).forEach(function (n) { h += '<div class="alert alert-info">ℹ ' + esc(n) + '</div>'; });
    if (!(v.errors || []).length && !(v.warnings || []).length) h += '<div class="alert alert-ok">✔ ' + esc(opts.okText || 'No conflicts found — coverage stays within limits.') + '</div>';
    h += '</div>';
    return h;
  }
  function diffTable(req) {
    const rows = (req.changes || []).slice().sort(function (a, b) { return a.date < b.date ? -1 : a.date > b.date ? 1 : a.employeeId < b.employeeId ? -1 : 1; });
    let h = '<table class="diff"><thead><tr><th>Date</th><th>Who</th><th>Now</th><th></th><th>Requested</th></tr></thead><tbody>';
    rows.forEach(function (c) {
      const cur = Engine.getScheduleForDate(c.date, { employeeId: c.employeeId }).code;
      const stale = c.from && cur !== c.from && req.status === 'pending';
      h += '<tr><td>' + esc(Engine.formatShort(c.date)) + '</td><td>' + esc(empShort(c.employeeId)) + '</td>' +
        '<td>' + codeChip(cur, 'sm') + (stale ? ' <small class="muted">(was ' + esc(c.from) + ')</small>' : '') + '</td>' +
        '<td class="arrow">→</td><td>' + codeChip(c.to, 'sm') + '</td></tr>';
    });
    h += '</tbody></table>';
    return h;
  }
  function fmtDateTime(iso) {
    if (!iso) return '';
    const d = new Date(iso);
    if (isNaN(d.getTime())) return iso;
    return Engine.formatShort(Engine.toISO(d)) + ' ' + d.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' });
  }

  /* ---------- sheet (bottom sheet on phones, dialog on desktop) ---------- */
  const sheetRoot = document.getElementById('sheetRoot');
  let sheetOnClose = null;
  function openSheet(o) {
    closeSheet();
    const wrap = el('<div class="sheet-backdrop" role="dialog" aria-modal="true"><div class="sheet">' +
      '<div class="handle"></div>' +
      '<div class="sheet-head"><h3>' + esc(o.title || '') + '</h3><button type="button" class="btn btn-sm btn-ghost" data-close aria-label="Close">✕</button></div>' +
      '<div class="sheet-body"></div></div></div>');
    const body = qs(wrap, '.sheet-body');
    if (typeof o.body === 'string') body.innerHTML = o.body; else if (o.body) body.appendChild(o.body);
    wrap.addEventListener('click', function (ev) {
      if (ev.target === wrap || ev.target.closest('[data-close]')) closeSheet();
    });
    sheetOnClose = o.onClose || null;
    sheetRoot.appendChild(wrap);
    document.body.style.overflow = 'hidden';
    if (o.onOpen) o.onOpen(body, wrap);
    return body;
  }
  function closeSheet() {
    if (!sheetRoot.firstChild) return;
    sheetRoot.innerHTML = '';
    document.body.style.overflow = '';
    const fn = sheetOnClose; sheetOnClose = null;
    if (fn) fn();
  }
  function isSheetOpen() { return !!sheetRoot.firstChild; }

  function confirmDialog(o) {
    return new Promise(function (resolve) {
      let done = false;
      openSheet({
        title: o.title || 'Are you sure?',
        body: '<p>' + (o.html ? o.html : esc(o.message || '')) + '</p>' +
          '<div class="row" style="margin-top:12px;justify-content:flex-end">' +
          '<button type="button" class="btn" data-close>' + esc(o.cancelLabel || 'Cancel') + '</button>' +
          '<button type="button" class="btn ' + (o.danger ? 'btn-danger' : 'btn-primary') + '" data-ok>' + esc(o.okLabel || 'OK') + '</button></div>',
        onOpen: function (body) {
          qs(body, '[data-ok]').addEventListener('click', function () { done = true; closeSheet(); resolve(true); });
        },
        onClose: function () { if (!done) resolve(false); },
      });
    });
  }
  function promptDialog(o) {
    return new Promise(function (resolve) {
      let done = false;
      openSheet({
        title: o.title || '',
        body: (o.message ? '<p>' + esc(o.message) + '</p>' : '') +
          '<label class="field">' + esc(o.label || '') +
          (o.multiline ? '<textarea class="textarea" data-in>' + esc(o.value || '') + '</textarea>'
            : '<input class="input" data-in type="' + esc(o.type || 'text') + '" value="' + esc(o.value || '') + '" ' + (o.attrs || '') + '>') +
          '</label>' +
          '<div class="row" style="margin-top:12px;justify-content:flex-end">' +
          '<button type="button" class="btn" data-close>Cancel</button>' +
          '<button type="button" class="btn btn-primary" data-ok>' + esc(o.okLabel || 'Save') + '</button></div>',
        onOpen: function (body) {
          const input = qs(body, '[data-in]');
          setTimeout(function () { input.focus(); }, 50);
          function ok() { done = true; const v = input.value; closeSheet(); resolve(v); }
          qs(body, '[data-ok]').addEventListener('click', ok);
          if (!o.multiline) input.addEventListener('keydown', function (ev) { if (ev.key === 'Enter') ok(); });
        },
        onClose: function () { if (!done) resolve(null); },
      });
    });
  }

  /* ---------- toast ---------- */
  let toastTimer = null;
  function toast(msg, type) {
    const root = document.getElementById('toastRoot');
    root.innerHTML = '<div class="toast' + (type === 'error' ? ' err' : '') + '">' + esc(msg) + '</div>';
    clearTimeout(toastTimer);
    toastTimer = setTimeout(function () { root.innerHTML = ''; }, type === 'error' ? 5000 : 2600);
  }

  /* ---------- images ---------- */
  function downscaleImage(file) {
    return new Promise(function (resolve, reject) {
      const reader = new FileReader();
      reader.onerror = function () { reject(new Error('Could not read the photo')); };
      reader.onload = function () {
        const img = new Image();
        img.onload = function () {
          const max = APP_CONFIG.photoMaxDimension || 1400;
          let w = img.naturalWidth, h = img.naturalHeight;
          const scale = Math.min(1, max / Math.max(w, h));
          w = Math.round(w * scale); h = Math.round(h * scale);
          const canvas = document.createElement('canvas');
          canvas.width = w; canvas.height = h;
          canvas.getContext('2d').drawImage(img, 0, 0, w, h);
          resolve(canvas.toDataURL('image/jpeg', APP_CONFIG.photoJpegQuality || 0.74));
        };
        img.onerror = function () { reject(new Error('That file is not an image')); };
        img.src = reader.result;
      };
      reader.readAsDataURL(file);
    });
  }

  function download(filename, text, mime) {
    const blob = new Blob([text], { type: mime || 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = filename; document.body.appendChild(a); a.click();
    setTimeout(function () { URL.revokeObjectURL(url); a.remove(); }, 500);
  }

  return {
    esc: esc, el: el, qs: qs, qsa: qsa,
    codeChip: codeChip, codeClass: codeClass, codeStyle: codeStyle, codeLabel: codeLabel, codeOptions: codeOptions,
    empOptions: empOptions, empName: empName, empShort: empShort, firstName: firstName,
    statusPill: statusPill, validationHTML: validationHTML, diffTable: diffTable, fmtDateTime: fmtDateTime,
    openSheet: openSheet, closeSheet: closeSheet, isSheetOpen: isSheetOpen, confirmDialog: confirmDialog, promptDialog: promptDialog,
    toast: toast, downscaleImage: downscaleImage, download: download,
  };
})();

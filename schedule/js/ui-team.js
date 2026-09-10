/* =====================================================================
 *  ui-team.js — "Swap schedule": who is working / off on a day, and the
 *  full monthly labor grid (same layout as the posted sheet).
 *  Supervisors can tap any grid cell to override it.
 * ===================================================================== */
Views.team = (function () {
  'use strict';
  const esc = UI.esc, chip = UI.codeChip;
  const TOUR_LABEL = { A: 'A · Night', B: 'B · Day', C: 'C · Evening', R: 'R · Relief' };

  function modeSeg() {
    const mode = App.state.teamMode;
    const official = App.state.teamLayout === 'official';
    return '<div class="seg grow no-print" style="margin-bottom:10px">' +
      '<button type="button" class="' + (mode === 'month' && official ? 'active' : '') + '" data-action="mode" data-mode="month" data-layout="official">Posted sheet</button>' +
      '<button type="button" class="' + (mode === 'month' && !official ? 'active' : '') + '" data-action="mode" data-mode="month" data-layout="colors">Colors</button>' +
      '<button type="button" class="' + (mode === 'day' ? 'active' : '') + '" data-action="mode" data-mode="day">Day</button></div>';
  }

  /* ---------------- day view ---------------- */
  function dayView() {
    const iso = App.state.teamDate;
    const today = Engine.todayISO();
    const sum = Engine.getDaySummary(iso);
    const me = Store.getSession() && Store.getSession().employeeId;
    let h = '<div class="card"><div class="cal-nav no-print">' +
      '<button type="button" class="btn btn-sm" data-action="tday">Today</button>' +
      '<button type="button" class="btn btn-icon" data-action="tprev" aria-label="Previous day">‹</button>' +
      '<button type="button" class="title" data-action="tjump">' + esc(Engine.formatLong(iso)) + '</button>' +
      '<button type="button" class="btn btn-icon" data-action="tnext" aria-label="Next day">›</button>' +
      '<input type="date" data-change="tjump" style="display:none"></div>';
    if (sum.holiday) h += '<div class="alert alert-err">★ ' + esc(sum.holiday.name) + '</div>';
    if (iso === today) h += '<p class="muted" style="font-size:.8rem;margin:0 0 6px">Today</p>';

    ['A', 'B', 'C', 'R'].forEach(function (t) {
      const people = sum.byTour[t];
      const req = sum.coverage.required[t];
      const real = sum.coverage.counts[t];
      h += '<div class="tour-block"><div class="th"><span>' + chip(t, 'sm') + '&nbsp; ' + esc(TOUR_LABEL[t]) + '</span>' +
        '<span class="count' + (req && real < req ? ' short' : '') + '">' + real + (req ? ' / ' + req + ' needed' : '') + '</span></div><div class="body">';
      if (!people.length) h += '<div class="person muted">nobody scheduled</div>';
      people.forEach(function (p) {
        h += '<div class="person"><span class="' + (p.vacant ? 'muted' : '') + '">' + esc(p.name) + (p.vacant ? ' <span class="pill pill-muted">vacant</span>' : '') + (p.employeeId === me ? ' <span class="pill pill-accent">me</span>' : '') + ' <span class="sub">' + esc(Engine.shiftCode(Engine.getEmployee(p.employeeId))) + '</span></span>' + chip(p.code, 'sm') + '</div>';
      });
      h += '</div></div>';
    });
    const offs = sum.byTour.off.concat(sum.byTour.other);
    h += '<div class="tour-block"><div class="th"><span>Off / leave</span><span class="count">' + offs.length + '</span></div><div class="body">';
    if (!offs.length) h += '<div class="person muted">nobody off</div>';
    offs.forEach(function (p) {
      h += '<div class="person"><span>' + esc(p.name) + (p.employeeId === me ? ' <span class="pill pill-accent">me</span>' : '') + ' <span class="sub">' + esc(Engine.shiftCode(Engine.getEmployee(p.employeeId))) + '</span></span>' + chip(p.code, 'sm') + '</div>';
    });
    h += '</div></div>';
    if (sum.coverage.issues.length) h += '<div class="alerts">' + sum.coverage.issues.map(function (i) { return '<div class="alert alert-warn">⚠ ' + esc(i) + '</div>'; }).join('') + '</div>';
    h += '</div>';

    /* Swap helper for the signed-in worker */
    if (me && Engine.getEmployee(me) && iso >= today) {
      const mine = sum.entries.filter(function (e) { return e.employeeId === me; })[0];
      const iWork = mine && Engine.isWork(mine.code);
      const candidates = sum.entries.filter(function (e) {
        if (e.employeeId === me || e.vacant) return false;
        return iWork ? Engine.isOff(e.code) && e.code !== 'HOL' : Engine.isWork(e.code);
      });
      h += '<div class="card"><h3>Swap ideas for ' + esc(Engine.formatShort(iso)) + '</h3>';
      h += '<p class="muted" style="font-size:.85rem">You are ' + chip(mine ? mine.code : null, 'sm') + ' ' + (iWork ? '— these coworkers are off and could cover:' : '— these coworkers are working and could hand you a tour:') + '</p>';
      if (!candidates.length) h += '<p class="muted">Nobody available by the rotation on this day.</p>';
      h += '<div class="list">';
      candidates.forEach(function (c) {
        h += '<div class="item"><div class="grow"><div class="name">' + esc(c.name) + '</div><div class="sub">' + esc(Engine.shiftCode(Engine.getEmployee(c.employeeId))) + ' · ' + esc(c.meta.label) + '</div></div>' + chip(c.code, 'sm') +
          '<button type="button" class="btn btn-sm btn-primary" data-action="swap-with" data-id="' + esc(c.employeeId) + '">Swap</button></div>';
      });
      h += '</div></div>';
    }
    return h;
  }

  /* ---------------- month grid ---------------- */
  function monthGrid() {
    const cal = App.state.cal;
    const y = cal.y, m = cal.m;
    const dim = Engine.daysInMonth(y, m);
    const today = Engine.todayISO();
    const sup = Store.isSupervisor();
    const roster = Engine.rosterSorted();
    const official = App.state.teamLayout === 'official';
    let h = '<div class="card"><div class="cal-nav no-print">' +
      '<button type="button" class="btn btn-sm" data-action="today">Today</button>' +
      '<button type="button" class="btn btn-icon" data-action="prev" aria-label="Previous month">‹</button>' +
      '<span class="title">' + esc(Engine.monthLabel(y, m)) + '</span>' +
      '<button type="button" class="btn btn-icon" data-action="next" aria-label="Next month">›</button></div>';
    if (official) {
      const fit = App.state.sheetFit !== false;
      h += '<div class="row between no-print" style="margin:0 0 8px;gap:6px">' +
        '<div class="seg"><button type="button" class="' + (fit ? 'active' : '') + '" data-action="fit" data-fit="1">Whole sheet</button><button type="button" class="' + (fit ? '' : 'active') + '" data-action="fit" data-fit="0">Actual size</button></div>' +
        '<button type="button" class="btn btn-sm btn-primary" data-action="print">🖨 Print</button></div>' +
        '<p class="muted no-print" style="font-size:.76rem;margin:0 0 6px">' + (fit ? 'Pinch to zoom in · ' : 'Scroll sideways · ') + (sup ? 'to change a cell use Colors · ' : '') + 'red = not the regular rotation</p>' +
        '<div class="' + (fit ? 'olabor-fit' : 'olabor-scroll') + '" data-sheet>' + Forms.laborScheduleHTML(y, m) + '</div></div>';
      return h;
    }
    h += '<h2 class="print-only">Monthly labor schedule — ' + esc(Engine.monthLabel(y, m)) + '</h2>' +
      '<p class="muted no-print" style="font-size:.78rem;margin:0 0 8px">' + (sup ? 'Tap a cell to change it. ' : '') + 'Scroll sideways · blue outline = changed from rotation</p>' +
      '<div class="lgrid-wrap"><table class="lgrid"><thead><tr><th class="name">Name<small>slot</small></th>';
    const days = [];
    for (let d = 1; d <= dim; d++) {
      const iso = y + '-' + (m < 10 ? '0' : '') + m + '-' + (d < 10 ? '0' : '') + d;
      days.push(iso);
      const hol = Engine.getHoliday(iso);
      h += '<th class="' + (Engine.isWeekend(iso) ? 'we ' : '') + (hol ? 'hol' : '') + '" title="' + esc(hol ? hol.name : '') + '">' + d + '<small>' + Engine.WEEKDAYS[Engine.weekday(iso)] + '</small></th>';
    }
    h += '</tr></thead><tbody>';
    let lastKey = null;
    roster.forEach(function (e) {
      const k = Engine.shiftCode(e);
      h += '<tr class="' + (lastKey !== null && k !== lastKey ? 'slot-start' : '') + '"><td class="name" title="' + esc(e.name) + '">' + esc(e.name) + '<small>' + esc(k) + '</small></td>';
      lastKey = k;
      days.forEach(function (iso) {
        const s = Engine.getScheduleForDate(iso, { employeeId: e.id });
        const cls = [];
        if (Engine.isWeekend(iso)) cls.push('we');
        if (s.source === 'exception') cls.push('exc');
        if (iso === today) cls.push('today');
        if (sup) cls.push('editable');
        h += '<td class="' + cls.join(' ') + '"' + (sup ? ' data-action="cell" data-date="' + iso + '" data-id="' + esc(e.id) + '"' : '') + '>' + chip(s.code) + '</td>';
      });
      h += '</tr>';
    });
    h += '</tbody></table></div>';
    h += '<div class="row no-print" style="margin-top:8px"><button type="button" class="btn btn-sm" data-action="print">Print this grid</button></div></div>';
    return h;
  }

  /* Scale the sheet down so the whole page fits the screen width. */
  function fitSheet() {
    const wrap = document.querySelector('.olabor-fit[data-sheet]');
    if (!wrap) return;
    const inner = wrap.querySelector('.olabor');
    if (!inner) return;
    inner.style.transform = 'none';
    wrap.style.height = '';
    const natural = inner.scrollWidth || inner.offsetWidth;
    const avail = wrap.clientWidth;
    const s = natural > 0 ? Math.min(1, avail / natural) : 1;
    inner.style.transform = 'scale(' + s + ')';
    wrap.style.height = Math.ceil(inner.offsetHeight * s) + 'px';
  }
  let resizeBound = false;
  function render(root) {
    root.innerHTML = modeSeg() + (App.state.teamMode === 'month' ? monthGrid() : dayView());
    fitSheet();
    if (!resizeBound) { resizeBound = true; window.addEventListener('resize', function () { fitSheet(); }); }
  }

  /* ---------------- supervisor: edit one cell ---------------- */
  function editCell(iso, employeeId) {
    const s = Engine.getScheduleForDate(iso, { employeeId: employeeId });
    const body = '<div class="row between"><div><div style="font-weight:700">' + esc(UI.empName(employeeId)) + '</div>' +
      '<div class="muted" style="font-size:.85rem">' + esc(Engine.formatLong(iso)) + '</div>' +
      '<div class="muted" style="font-size:.78rem">Rotation: ' + esc(s.rotationCode || '—') + (s.holiday ? ' (holiday)' : '') + '</div></div>' + chip(s.code, 'lg') + '</div>' +
      '<label class="field" style="margin-top:12px">New code<select class="select" data-code>' + UI.codeOptions(s.code) + '</select></label>' +
      '<label class="field">Note (optional)<input class="input" data-note value="' + esc(s.exception && s.exception.source === 'manual' ? s.exception.note || '' : '') + '" placeholder="e.g. approved verbally, comp day"></label>' +
      '<div class="row" style="margin-top:12px;justify-content:flex-end">' +
      (s.exception ? '<button type="button" class="btn" data-reset>Back to rotation</button>' : '') +
      '<button type="button" class="btn" data-close>Cancel</button><button type="button" class="btn btn-primary" data-save>Save</button></div>';
    UI.openSheet({
      title: 'Edit cell',
      body: body,
      onOpen: function (root) {
        UI.qs(root, '[data-save]').addEventListener('click', function () {
          const code = UI.qs(root, '[data-code]').value;
          const note = UI.qs(root, '[data-note]').value.trim();
          Store.mutate(function (d) { Swaps.setOverride(d, iso, employeeId, code, { note: note, by: Store.getSession().name }); }, { reason: 'override' })
            .then(function () { UI.closeSheet(); UI.toast('Saved'); })
            .catch(function (e) { UI.toast(e.message, 'error'); });
        });
        const r = UI.qs(root, '[data-reset]');
        if (r) r.addEventListener('click', function () {
          Store.mutate(function (d) { Swaps.setOverride(d, iso, employeeId, null); }, { reason: 'override' })
            .then(function () { UI.closeSheet(); UI.toast('Back to rotation'); })
            .catch(function (e) { UI.toast(e.message, 'error'); });
        });
      },
    });
  }

  function shiftMonth(delta) {
    let m = App.state.cal.m + delta, y = App.state.cal.y;
    while (m < 1) { m += 12; y -= 1; }
    while (m > 12) { m -= 12; y += 1; }
    App.setMonth(y, m);
  }

  return {
    render: render,
    editCell: editCell,
    actions: {
      mode: function (b) {
        App.state.teamMode = b.dataset.mode; Store.setPref('teamMode', b.dataset.mode);
        if (b.dataset.layout) { App.state.teamLayout = b.dataset.layout; Store.setPref('teamLayout', b.dataset.layout); }
        App.render();
      },
      layout: function (b) { App.state.teamLayout = b.dataset.layout; Store.setPref('teamLayout', b.dataset.layout); App.render(); },
      fit: function (b) { App.state.sheetFit = b.dataset.fit === '1'; Store.setPref('sheetFit', App.state.sheetFit); App.render(); },
      tday: function () { App.state.teamDate = Engine.todayISO(); App.render(); },
      tprev: function () { App.state.teamDate = Engine.addDays(App.state.teamDate, -1); App.render(); },
      tnext: function () { App.state.teamDate = Engine.addDays(App.state.teamDate, 1); App.render(); },
      tjump: function (btn) {
        const input = btn.parentElement.querySelector('input[type=date]');
        if (input && typeof input.showPicker === 'function') { try { input.showPicker(); return; } catch (e) { /* fall through */ } }
        UI.promptDialog({ title: 'Go to date', label: 'Date', type: 'date', value: App.state.teamDate, okLabel: 'Go' }).then(function (v) {
          if (v && Engine.isValidISO(v)) { App.state.teamDate = v; App.render(); }
        });
      },
      today: function () { const t = Engine.todayISO(); App.setMonth(Number(t.slice(0, 4)), Number(t.slice(5, 7))); },
      prev: function () { shiftMonth(-1); },
      next: function () { shiftMonth(1); },
      cell: function (td) { editCell(td.dataset.date, td.dataset.id); },
      print: function () { window.print(); },
      'swap-with': function (b) {
        App.go('requests', { newRequest: { partner: b.dataset.id, date: App.state.teamDate, employeeId: Store.getSession().employeeId } });
      },
    },
    changes: {
      tjump: function (input) { if (input.value && Engine.isValidISO(input.value)) { App.state.teamDate = input.value; App.render(); } },
    },
  };
})();

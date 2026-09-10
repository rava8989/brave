/* =====================================================================
 *  ui-calendar.js — Today card + month calendar for one person
 * ===================================================================== */
Views.calendar = (function () {
  'use strict';
  const esc = UI.esc, chip = UI.codeChip;

  function sourceText(s) {
    if (!s.slot) return 'No slot assigned';
    if (s.source === 'exception') {
      const ex = s.exception || {};
      return 'Changed from rotation (' + (ex.note || ex.source || 'manual override') + ')' + (ex.by ? ' · by ' + ex.by : '');
    }
    if (s.source === 'holiday') return (s.holiday ? s.holiday.name : 'Holiday') + ' — rotation day was ' + s.base;
    return 'Rotation · slot ' + s.slot + ' · cycle day ' + (s.cycleIndex + 1) + ' of ' + Engine.getConfig().rotation.cycleLength;
  }

  function viewingHTML(who) {
    const sess = Store.getSession();
    const mine = sess && sess.employeeId;
    let h = '<div class="row viewing-as no-print">';
    h += '<label class="field" style="flex:1;margin:0"><span class="sr-only">Viewing schedule for</span>' +
      '<select class="select" data-change="viewing" aria-label="Whose schedule to show">' +
      '<option value="">— choose whose schedule to show —</option>' + UI.empOptions(who, { includeVacant: true }) + '</select></label>';
    if (mine && who !== mine) h += '<button type="button" class="btn btn-sm" data-action="view-me">Me</button>';
    h += '</div>';
    return h;
  }

  function todayCard(who, today) {
    if (!who) return '<div class="card"><p class="muted">Pick a name above to see the schedule.</p></div>';
    const t = Engine.getScheduleForDate(today, { employeeId: who });
    const tomorrow = Engine.getScheduleForDate(Engine.addDays(today, 1), { employeeId: who });
    const nextWork = Engine.findNext(today, { employeeId: who }, function (s) { return s.isWork; });
    const nextRdo = Engine.findNext(today, { employeeId: who }, function (s) { return s.code === 'RDO' || s.code === 'MDO'; });
    const emp = Engine.getEmployee(who);
    function mini(k, s) {
      return '<div class="mini"><div class="k">' + esc(k) + '</div><div class="d">' + (s ? esc(Engine.formatShort(s.date)) : '—') + '</div>' + (s ? chip(s.code) : '') + '</div>';
    }
    return '<div class="card today-card">' +
      '<div class="today-head"><div>' +
      '<div class="label">TODAY · ' + esc(UI.firstName(emp ? emp.name : '').toUpperCase() || 'SLOT ' + (emp ? emp.slot : '')) + '</div>' +
      '<div class="date">' + esc(Engine.formatLong(today)) + '</div>' +
      '<div class="meaning">' + esc(t.meta.label) + (t.holiday ? ' · ' + esc(t.holiday.name) : '') + '</div>' +
      '</div><div>' + chip(t.code, 'xl') + '</div></div>' +
      '<div class="today-rows">' + mini('Tomorrow', tomorrow) + mini('Next work', nextWork) + mini('Next RDO', nextRdo) + '</div>' +
      '</div>';
  }

  function navHTML(y, m) {
    return '<div class="cal-nav no-print">' +
      '<button type="button" class="btn btn-sm" data-action="today">Today</button>' +
      '<button type="button" class="btn btn-icon" data-action="prev" aria-label="Previous month">‹</button>' +
      '<button type="button" class="title" data-action="jump" aria-label="Jump to a date">' + esc(Engine.monthLabel(y, m)) + '</button>' +
      '<button type="button" class="btn btn-icon" data-action="next" aria-label="Next month">›</button>' +
      '<input type="date" class="input" data-change="jump" aria-label="Go to date" style="display:none">' +
      '</div>' +
      '<h2 class="print-only">' + esc(Engine.monthLabel(y, m)) + '</h2>';
  }

  function gridHTML(y, m, who, today) {
    const weeks = Engine.getMonthMatrix(y, m);
    let h = '<div class="cal-head">' + Engine.WEEKDAYS.map(function (d) { return '<div>' + d.toUpperCase() + '</div>'; }).join('') + '</div><div class="cal-grid">';
    weeks.forEach(function (row) {
      row.forEach(function (c) {
        const s = Engine.getScheduleForDate(c.iso, who ? { employeeId: who } : null);
        const cls = ['cal-cell'];
        if (!c.inMonth) cls.push('out');
        if (c.iso === today) cls.push('today');
        if (Engine.isWeekend(c.iso)) cls.push('weekend');
        if (s.holiday) cls.push('hol');
        if (c.iso === App.state.selectedDate) cls.push('selected');
        h += '<button type="button" class="' + cls.join(' ') + '" data-action="day" data-date="' + c.iso + '" aria-label="' + esc(Engine.formatLong(c.iso) + (s.code ? ', ' + s.meta.label : '')) + '">' +
          '<span class="num">' + Number(c.iso.slice(8, 10)) + '</span>' +
          (who ? chip(s.code) : '<span class="code code-none">·</span>') +
          (s.holiday ? '<span class="mark hol" title="' + esc(s.holiday.name) + '">★</span>' : (s.source === 'exception' ? '<span class="mark exc" title="Changed from rotation">●</span>' : '')) +
          '</button>';
      });
    });
    h += '</div>';
    return h;
  }

  function render(root) {
    const who = App.viewingEmployeeId();
    const today = Engine.todayISO();
    const cal = App.state.cal;
    root.innerHTML = viewingHTML(who) + todayCard(who, today) + '<div class="card">' + navHTML(cal.y, cal.m) + gridHTML(cal.y, cal.m, who, today) +
      '<p class="muted" style="font-size:.76rem;margin-top:8px">Tap a day for details · ★ holiday · ● changed from the rotation</p></div>';
  }

  function openDay(iso) {
    const who = App.viewingEmployeeId();
    const s = Engine.getScheduleForDate(iso, who ? { employeeId: who } : null);
    const sum = Engine.getDaySummary(iso);
    const sess = Store.getSession();
    App.state.selectedDate = iso;
    let h = '';
    if (who) {
      h += '<div class="row between"><div><div style="font-weight:700">' + esc(UI.empName(who)) + '</div><div class="muted" style="font-size:.85rem">' + esc(s.meta.label) + '</div>' +
        '<div class="muted" style="font-size:.78rem">' + esc(sourceText(s)) + '</div></div>' + chip(s.code, 'lg') + '</div>';
    }
    if (s.holiday) h += '<div class="alert alert-err" style="margin-top:8px">★ ' + esc(s.holiday.name) + '</div>';
    h += '<h3 style="margin-top:12px">Team this day</h3>';
    ['A', 'B', 'C', 'R'].forEach(function (t) {
      const people = sum.byTour[t];
      const req = sum.coverage.required[t];
      h += '<div class="row" style="margin:4px 0"><span style="width:42px">' + chip(t, 'sm') + '</span>' +
        '<span class="' + (req && people.length < req ? 'muted' : '') + '" style="flex:1;font-size:.88rem">' + (people.length ? esc(people.map(function (p) { return UI.empShort(p.employeeId) + (p.meta.double ? ' (' + p.code + ')' : ''); }).join(', ')) : '<span class="muted">nobody</span>') + '</span>' +
        (req ? '<span class="muted" style="font-size:.75rem">' + sum.coverage.counts[t] + '/' + req + '</span>' : '') + '</div>';
    });
    if (sum.byTour.off.length) {
      h += '<div class="row" style="margin:4px 0"><span style="width:42px" class="muted">Off</span><span style="flex:1;font-size:.88rem" class="muted">' +
        esc(sum.byTour.off.map(function (p) { return UI.empShort(p.employeeId) + ' (' + p.code + ')'; }).join(', ')) + '</span></div>';
    }
    if (sum.coverage.issues.length) h += '<div class="alerts">' + sum.coverage.issues.map(function (i) { return '<div class="alert alert-warn">⚠ ' + esc(i) + '</div>'; }).join('') + '</div>';
    h += '<div class="row" style="margin-top:14px">';
    if (sess && (sess.employeeId || Store.isSupervisor())) {
      h += '<button type="button" class="btn btn-primary" data-x="request">Request a change on this day</button>';
    }
    if (Store.isSupervisor() && who) h += '<button type="button" class="btn" data-x="edit">Edit this cell</button>';
    h += '<button type="button" class="btn" data-x="team">Team view</button></div>';
    UI.openSheet({
      title: Engine.formatLong(iso),
      body: h,
      onOpen: function (body) {
        body.addEventListener('click', function (ev) {
          const b = ev.target.closest('[data-x]');
          if (!b) return;
          UI.closeSheet();
          if (b.dataset.x === 'request') App.go('requests', { newRequest: { date: iso, employeeId: who } });
          else if (b.dataset.x === 'edit') Views.team.editCell(iso, who);
          else if (b.dataset.x === 'team') { App.state.teamDate = iso; App.state.teamMode = 'day'; App.go('team'); }
        });
      },
      onClose: function () { App.state.selectedDate = null; },
    });
  }

  function shiftMonth(delta) {
    const cal = App.state.cal;
    let m = cal.m + delta, y = cal.y;
    while (m < 1) { m += 12; y -= 1; }
    while (m > 12) { m -= 12; y += 1; }
    App.setMonth(y, m);
  }

  return {
    render: render,
    actions: {
      today: function () { const t = Engine.todayISO(); App.setMonth(Number(t.slice(0, 4)), Number(t.slice(5, 7))); },
      prev: function () { shiftMonth(-1); },
      next: function () { shiftMonth(1); },
      jump: function (btn) {
        const input = btn.parentElement.querySelector('input[type=date]');
        if (input && typeof input.showPicker === 'function') { try { input.showPicker(); return; } catch (e) { /* fall through */ } }
        UI.promptDialog({ title: 'Jump to date', label: 'Date', type: 'date', value: Engine.todayISO(), okLabel: 'Go' }).then(function (v) {
          if (v && Engine.isValidISO(v)) { App.setMonth(Number(v.slice(0, 4)), Number(v.slice(5, 7))); openDay(v); }
        });
      },
      day: function (btn) { openDay(btn.dataset.date); },
      'view-me': function () { App.setViewing(Store.getSession().employeeId); },
    },
    changes: {
      viewing: function (sel) { App.setViewing(sel.value || null); },
      jump: function (input) { const v = input.value; if (v && Engine.isValidISO(v)) { App.setMonth(Number(v.slice(0, 4)), Number(v.slice(5, 7))); openDay(v); } },
    },
    openDay: openDay,
  };
})();

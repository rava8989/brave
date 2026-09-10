/* =====================================================================
 *  ui-requests.js — swap / time-off requests
 *    workers    : new-request wizard (photo of the signed sheet first),
 *                 their requests, cancel
 *    supervisor : queue with live conflict check, approve / reject
 *  Every request can be shown / printed as the official paper form.
 * ===================================================================== */
Views.requests = (function () {
  'use strict';
  const esc = UI.esc, chip = UI.codeChip;

  /* ------------------------------------------------------------------
   * list
   * ---------------------------------------------------------------- */
  function visibleRequests() {
    const all = (Store.get().requests || []).slice();
    const sess = Store.getSession();
    const me = sess && sess.employeeId;
    const f = App.state.reqFilter;
    let list = all;
    if (f === 'pending') list = all.filter(function (r) { return r.status === 'pending'; });
    else if (f === 'approved') list = all.filter(function (r) { return r.status === 'approved'; });
    else if (f === 'closed') list = all.filter(function (r) { return r.status === 'rejected' || r.status === 'cancelled'; });
    else if (f === 'mine') list = all.filter(function (r) { return me && ((r.employees || []).indexOf(me) !== -1 || r.createdBy === me); });
    const order = { pending: 0, approved: 1, rejected: 2, cancelled: 3 };
    list.sort(function (a, b) { return (order[a.status] - order[b.status]) || (b.createdAt > a.createdAt ? 1 : -1); });
    return list;
  }

  function filterSeg() {
    const sup = Store.isSupervisor();
    const f = App.state.reqFilter;
    const pending = (Store.get().requests || []).filter(function (r) { return r.status === 'pending'; }).length;
    const items = sup
      ? [['pending', 'Pending' + (pending ? ' (' + pending + ')' : '')], ['approved', 'Approved'], ['closed', 'Closed'], ['all', 'All']]
      : [['mine', 'Mine'], ['pending', 'Pending' + (pending ? ' (' + pending + ')' : '')], ['all', 'All']];
    return '<div class="seg grow no-print" style="margin-bottom:10px">' + items.map(function (it) {
      return '<button type="button" class="' + (f === it[0] ? 'active' : '') + '" data-action="filter" data-f="' + it[0] + '">' + esc(it[1]) + '</button>';
    }).join('') + '</div>';
  }

  function decisionButtons(r) {
    const sup = Store.isSupervisor();
    const sess = Store.getSession();
    const me = sess && sess.employeeId;
    let h = '';
    if (r.status === 'pending' && sup) {
      h += '<button type="button" class="btn btn-success" data-action="approve" data-id="' + esc(r.id) + '">✔ Approve</button>' +
        '<button type="button" class="btn btn-danger" data-action="reject" data-id="' + esc(r.id) + '">✖ Reject</button>';
    }
    if (r.status === 'pending' && (sup || r.createdBy === me || (r.employees || []).indexOf(me) !== -1)) {
      h += '<button type="button" class="btn btn-ghost" data-action="cancel" data-id="' + esc(r.id) + '">Cancel request</button>';
    }
    return h;
  }

  function requestCard(r) {
    const sup = Store.isSupervisor();
    const live = r.status === 'pending' ? Swaps.validate(r, { state: Store.get(), role: 'supervisor' }) : null;
    let h = '<div class="req-card" id="req-' + esc(r.id) + '">';
    h += '<div class="head"><div><div class="title">' + esc(Swaps.describe(r)) + '</div>' +
      '<div class="meta">Submitted ' + esc(UI.fmtDateTime(r.createdAt)) + (r.createdByName ? ' by ' + esc(r.createdByName) : '') + (r.fromPhoto ? ' · read from photo' : '') + '</div>' +
      (r.decidedAt ? '<div class="meta">' + esc(r.status === 'approved' ? 'Approved' : r.status === 'rejected' ? 'Rejected' : 'Cancelled') + ' ' + esc(UI.fmtDateTime(r.decidedAt)) + (r.decidedBy ? ' by ' + esc(r.decidedBy) : '') + (r.decisionNote ? ' — “' + esc(r.decisionNote) + '”' : '') + '</div>' : '') +
      '</div>' + UI.statusPill(r.status) + '</div>';
    if (r.reason) h += '<p style="font-size:.9rem;margin:6px 0 0"><span class="muted">Reason:</span> ' + esc(r.reason) + '</p>';
    h += UI.diffTable(r);
    if (live) h += UI.validationHTML(live, { okText: 'No conflicts — coverage stays within limits. Approving applies it to the schedule instantly.' });
    else if (r.validation && (r.validation.warnings || []).length && r.status !== 'pending') {
      h += '<p class="muted" style="font-size:.78rem;margin-top:6px">Had ' + r.validation.warnings.length + ' warning(s) when submitted.</p>';
    }
    if (r.photoId) h += '<img class="photo-thumb" data-photo="' + esc(r.photoId) + '" alt="Photo of the signed request form" data-action="zoom">';
    h += '<div class="row no-print" style="margin-top:10px">' +
      '<button type="button" class="btn" data-action="form" data-id="' + esc(r.id) + '">📄 Official form</button>' + decisionButtons(r);
    if (r.status === 'approved' && sup) {
      h += '<label class="check" style="margin-left:auto"><input type="checkbox" data-change="entered" data-id="' + esc(r.id) + '"' + (r.enteredInComputer ? ' checked' : '') + '> Entered into computer</label>';
    } else if (r.status === 'approved' && r.enteredInComputer) {
      h += '<span class="pill pill-muted">Entered into computer</span>';
    }
    h += '</div></div>';
    return h;
  }

  function listView() {
    const sess = Store.getSession();
    const list = visibleRequests();
    let h = '';
    if (sess && (sess.employeeId || Store.isSupervisor())) {
      h += '<button type="button" class="btn btn-primary btn-block no-print" data-action="new" style="margin-bottom:10px">＋ New swap / time-off request</button>';
    }
    h += filterSeg();
    if (!list.length) h += '<div class="card"><p class="muted">No requests here yet.</p></div>';
    list.forEach(function (r) { h += requestCard(r); });
    return h;
  }

  function loadPhotos(root) {
    UI.qsa(root, 'img[data-photo]').forEach(function (img) {
      Store.getPhoto(img.dataset.photo).then(function (url) { if (url) img.src = url; else img.alt = 'Photo not available on this device'; }).catch(function () { img.remove(); });
    });
  }

  /* ------------------------------------------------------------------
   * official form page (view / print one request)
   * ---------------------------------------------------------------- */
  function formPage() {
    const fv = App.state.formView;
    const draft = fv === '__draft__';
    const req = draft ? buildFromState(App.state.newReq) : Swaps.findRequest(Store.get(), fv);
    if (!req) { App.state.formView = null; return listView(); }
    let h = '<div class="form-page"><div class="row toolbar no-print">' +
      '<button type="button" class="btn" data-action="form-back">‹ Back</button>' +
      '<button type="button" class="btn btn-primary" data-action="print">🖨 Print</button>' +
      (draft ? '' : decisionButtons(req)) + '</div>';
    if (draft) h += '<p class="muted no-print" style="font-size:.85rem">Print this, get the signatures, then photograph it and attach it before submitting.</p>';
    h += '<div class="osheet-scroll">' + Forms.swapSheetHTML(req) + '</div></div>';
    return h;
  }

  /* ------------------------------------------------------------------
   * wizard
   * ---------------------------------------------------------------- */
  function startNew(params) {
    params = params || {};
    const sess = Store.getSession();
    const today = Engine.todayISO();
    const employeeId = params.employeeId || sess.employeeId || (Engine.activeRoster().filter(function (e) { return !e.vacant; })[0] || {}).id;
    const date = params.date && params.date >= today ? params.date : null;
    App.state.newReq = {
      step: params.type ? (params.type === 'swap' && !params.partner ? 2 : 3) : 1,
      type: params.type || null,
      employeeId: employeeId,
      partner: params.partner || null,
      dates: date ? [date] : [],
      pick: date ? { y: Number(date.slice(0, 4)), m: Number(date.slice(5, 7)) } : { y: Number(today.slice(0, 4)), m: Number(today.slice(5, 7)) },
      to: {},
      reason: '',
      photo: null,
      fromPhoto: false,
      parseNotes: [],
      approveNow: false,
    };
  }

  function key(date, id) { return date + '|' + id; }

  function currentChanges(n) {
    const out = [];
    n.dates.slice().sort().forEach(function (d) {
      const mine = Engine.getScheduleForDate(d, { employeeId: n.employeeId }).code;
      if (n.type === 'swap' && n.partner) {
        const theirs = Engine.getScheduleForDate(d, { employeeId: n.partner }).code;
        out.push({ employeeId: n.employeeId, date: d, from: mine, to: n.to[key(d, n.employeeId)] || theirs });
        out.push({ employeeId: n.partner, date: d, from: theirs, to: n.to[key(d, n.partner)] || mine });
      } else {
        out.push({ employeeId: n.employeeId, date: d, from: mine, to: n.to[key(d, n.employeeId)] || (Engine.isWork(mine) ? 'VAC' : mine) });
      }
    });
    return out;
  }

  function stepsBar(step) {
    let h = '<div class="wizard-steps">';
    for (let i = 1; i <= 5; i++) h += '<span class="' + (i <= step ? 'on' : '') + '"></span>';
    return h + '</div>';
  }
  function navButtons(n, nextLabel, nextEnabled) {
    return '<div class="row" style="margin-top:14px;justify-content:space-between">' +
      '<button type="button" class="btn" data-action="back">' + (n.step === 1 ? 'Cancel' : '‹ Back') + '</button>' +
      (nextLabel ? '<button type="button" class="btn btn-primary" data-action="next"' + (nextEnabled === false ? ' disabled' : '') + '>' + esc(nextLabel) + '</button>' : '') + '</div>';
  }
  function photoInput(changeName) {
    return '<input type="file" accept="image/*" capture="environment" data-change="' + changeName + '" class="sr-only">';
  }

  function wizardView() {
    const n = App.state.newReq;
    const sup = Store.isSupervisor();
    let h = '<div class="card">' + stepsBar(n.step);

    if (n.step === 1) {
      h += '<h2>New request</h2>';
      if (sup) h += '<label class="field">Request for<select class="select" data-change="emp">' + UI.empOptions(n.employeeId) + '</select></label>';
      h += '<div class="wizard-photo-first">';
      if (n.aiBusy) {
        h += '<p style="text-align:center;font-weight:700;margin:6px 0">Reading the sheet…</p><p class="muted" style="text-align:center;font-size:.85rem">This takes about 10–20 seconds.</p>';
      } else if (n.photo) {
        h += '<img class="photo-thumb" src="' + n.photo.dataUrl + '" alt="Attached photo" style="max-height:160px"><p style="text-align:center;margin:6px 0 0;font-size:.85rem">✔ Photo attached — now pick the type below and tap the days.</p>' +
          '<div class="row" style="justify-content:center;margin-top:6px"><button type="button" class="btn btn-sm" data-action="photo-remove">Remove photo</button></div>';
      } else {
        h += '<label class="btn btn-block btn-primary" style="min-height:60px;cursor:pointer">📷 Upload the signed swap sheet<br><small style="font-weight:500">' + (Store.aiAvailable() ? 'the app fills everything in from the photo' : 'photo goes with the request; then tap the days') + '</small>' + photoInput('photo-first') + '</label>';
      }
      h += '</div><div class="or-divider">— or fill it in by hand —</div>';
      h += '<div class="stack">' +
        '<button type="button" class="btn btn-block' + (n.type === 'swap' ? ' btn-primary' : '') + '" data-action="type" data-type="swap" style="min-height:56px">🔁 Swap shifts with a coworker</button>' +
        '<button type="button" class="btn btn-block' + (n.type === 'timeoff' ? ' btn-primary' : '') + '" data-action="type" data-type="timeoff" style="min-height:56px">🏖 Time off / change my own shift</button></div>';
      h += navButtons(n, null);
    }

    if (n.step === 2) {
      h += '<h2>Swap with whom?</h2><p class="muted" style="font-size:.85rem">Showing what each coworker works on ' + esc(n.dates[0] ? Engine.formatShort(n.dates[0]) : 'today') + '.</p><div class="list">';
      const ref = n.dates[0] || Engine.todayISO();
      Engine.rosterSorted().filter(function (e) { return e.id !== n.employeeId && !e.vacant; }).forEach(function (e) {
        const c = Engine.getScheduleForDate(ref, { employeeId: e.id }).code;
        h += '<div class="item clickable' + (n.partner === e.id ? ' selected' : '') + '" data-action="partner" data-id="' + esc(e.id) + '"><div class="grow"><div class="name">' + esc(e.name) + '</div><div class="sub">' + esc(Engine.shiftCode(e)) + '</div></div>' + chip(c, 'sm') + '</div>';
      });
      h += '</div>' + navButtons(n, 'Next ›', !!n.partner);
    }

    if (n.step === 3) {
      const p = n.pick;
      const weeks = Engine.getMonthMatrix(p.y, p.m);
      const today = Engine.todayISO();
      h += '<h2>Which days?</h2><p class="muted" style="font-size:.85rem">Tap the days involved. Top chip = ' + esc(UI.firstName(UI.empName(n.employeeId))) +
        (n.partner ? ', bottom chip = ' + esc(UI.firstName(UI.empName(n.partner))) : '') + '.</p>';
      h += '<div class="cal-nav" style="grid-template-columns:44px 1fr 44px"><button type="button" class="btn btn-icon" data-action="pprev">‹</button><span class="title">' + esc(Engine.monthLabel(p.y, p.m)) + '</span><button type="button" class="btn btn-icon" data-action="pnext">›</button></div>';
      h += '<div class="pick-grid">' + Engine.WEEKDAYS.map(function (d) { return '<div class="hd">' + d.toUpperCase().slice(0, 2) + '</div>'; }).join('');
      weeks.forEach(function (row) {
        row.forEach(function (c) {
          const mine = Engine.getScheduleForDate(c.iso, { employeeId: n.employeeId }).code;
          const past = c.iso < today && !sup;
          const cls = ['pick-cell'];
          if (!c.inMonth) cls.push('out');
          if (past) cls.push('past');
          if (n.dates.indexOf(c.iso) !== -1) cls.push('on');
          h += '<button type="button" class="' + cls.join(' ') + '" data-action="pick" data-date="' + c.iso + '"' + (past ? ' disabled' : '') + '>' +
            '<span class="num">' + Number(c.iso.slice(8, 10)) + '</span>' + chip(mine, 'sm') +
            (n.partner ? chip(Engine.getScheduleForDate(c.iso, { employeeId: n.partner }).code, 'sm') : '') + '</button>';
        });
      });
      h += '</div>';
      h += '<p style="font-size:.85rem;margin-top:8px">' + (n.dates.length ? '<b>' + n.dates.length + '</b> day(s): ' + esc(n.dates.slice().sort().map(Engine.formatDayMonth).join(', ')) : '<span class="muted">No days selected yet.</span>') + '</p>';
      h += navButtons(n, 'Next ›', n.dates.length > 0);
    }

    if (n.step === 4) {
      h += '<h2>Requested shifts</h2>';
      const changes = currentChanges(n);
      let last = null;
      changes.forEach(function (c) {
        if (c.date !== last) { h += '<div style="font-weight:700;font-size:.85rem;margin-top:10px">' + esc(Engine.formatLong(c.date)) + '</div>'; last = c.date; }
        h += '<div class="change-row"><div class="who">' + esc(UI.empShort(c.employeeId)) + '<small>now ' + esc(c.from || '—') + '</small></div>' +
          '<span class="muted">→</span><select class="select" data-change="to" data-key="' + esc(key(c.date, c.employeeId)) + '">' + UI.codeOptions(c.to) + '</select></div>';
      });
      h += '<label class="field" style="margin-top:14px">Reason (optional)<textarea class="textarea" data-input="reason" placeholder="e.g. doctor appointment, family event">' + esc(n.reason) + '</textarea></label>';
      h += '<div class="field">Photo of the signed request form</div>';
      if (n.photo) {
        h += '<img class="photo-thumb" src="' + n.photo.dataUrl + '" alt="Attached photo">' +
          '<div class="row" style="margin-top:6px"><button type="button" class="btn btn-sm" data-action="photo-remove">Remove photo</button>' +
          (Store.aiAvailable() ? '<button type="button" class="btn btn-sm btn-primary" data-action="photo-read"' + (n.aiBusy ? ' disabled' : '') + '>' + (n.aiBusy ? 'Reading…' : '✨ Fill in from photo') + '</button>' : '') + '</div>';
      } else {
        h += '<label class="btn btn-block" style="cursor:pointer">📷 Take / choose photo' + photoInput('photo') + '</label>' +
          '<p class="muted" style="font-size:.78rem">No signed sheet yet? Continue to Review and print the pre-filled form from there.</p>';
      }
      h += navButtons(n, 'Review ›', true);
    }

    if (n.step === 5) {
      const req = buildFromState(n);
      const v = Swaps.validate(req, { state: Store.get(), role: sup ? 'supervisor' : 'worker' });
      const notes = (n.parseNotes || []).slice();
      h += '<h2>Review</h2><p style="font-weight:700">' + esc(Swaps.describe(req)) + '</p>';
      if (n.fromPhoto) h += '<p class="muted" style="font-size:.85rem">Read from your photo — check every cell against the sheet before submitting.</p>';
      h += '<p class="muted" style="font-size:.78rem;margin:6px 0 4px">Official form (scroll sideways) — before on top, requested below:</p>';
      h += '<div class="osheet-scroll">' + Forms.swapSheetHTML(req) + '</div>';
      h += '<div class="row" style="margin-top:8px"><button type="button" class="btn btn-sm" data-action="form-draft">🖨 Print this form to sign</button>' +
        '<button type="button" class="btn btn-sm" data-action="edit-cells">✎ Edit shifts</button></div>';
      h += UI.diffTable(req);
      if (notes.length) h += '<div class="alerts">' + notes.map(function (x) { return '<div class="alert alert-warn">⚠ ' + esc(x) + '</div>'; }).join('') + '</div>';
      h += UI.validationHTML(v);
      if (n.photo) h += '<img class="photo-thumb" src="' + n.photo.dataUrl + '" alt="Attached photo"><div class="row" style="margin-top:6px"><button type="button" class="btn btn-sm" data-action="photo-remove">Remove photo</button></div>';
      else h += '<label class="btn btn-block" style="cursor:pointer;margin-top:8px">📷 Attach photo of the signed sheet' + photoInput('photo') + '</label>';
      if (sup) h += '<label class="check" style="margin-top:8px"><input type="checkbox" data-change="approve-now"' + (n.approveNow ? ' checked' : '') + '> Approve and apply to the schedule now</label>';
      else if (v.warnings.length) h += '<p class="muted" style="font-size:.8rem">The warnings above will be shown to the supervisor; you can still submit.</p>';
      h += '<div class="row" style="margin-top:14px;justify-content:space-between"><button type="button" class="btn" data-action="back">‹ Back</button>' +
        '<button type="button" class="btn btn-primary" data-action="submit"' + (v.ok ? '' : ' disabled') + '>' + (sup && n.approveNow ? 'Approve & apply' : 'Submit request') + '</button></div>';
    }
    h += '</div>';
    return h;
  }

  function buildFromState(n) {
    const sess = Store.getSession();
    const r = Swaps.buildRequest({
      type: n.type || 'change',
      createdBy: sess.employeeId || 'supervisor',
      createdByName: sess.name || '',
      changes: currentChanges(n).filter(function (c) { return c.to !== c.from; }),   // drop no-change rows
      reason: n.reason,
      photoId: n.photo ? 'p-' + (n.photoIdSeed || 'draft') : null,
    });
    r.fromPhoto = !!n.fromPhoto;
    return r;
  }

  async function submit() {
    const n = App.state.newReq;
    const sup = Store.isSupervisor();
    n.photoIdSeed = n.photoIdSeed || Swaps.uid();
    const req = buildFromState(n);
    const v = Swaps.validate(req, { state: Store.get(), role: sup ? 'supervisor' : 'worker' });
    if (!v.ok) { UI.toast(v.errors[0], 'error'); return; }
    req.validation = { warnings: v.warnings, notes: v.notes.concat(n.parseNotes || []), checkedAt: new Date().toISOString() };
    try {
      if (n.photo) await Store.savePhoto(req.photoId, n.photo.dataUrl);
      const approveNow = sup && n.approveNow;
      await Store.mutate(function (d) {
        d.requests.push(req);
        if (approveNow) Swaps.applyDecision(d, req.id, { status: 'approved', by: Store.getSession().name || 'Supervisor', note: 'Applied on entry' });
      }, { reason: 'request' });
      App.state.newReq = null;
      App.state.reqFilter = sup ? (approveNow ? 'approved' : 'pending') : 'mine';
      UI.toast(approveNow ? 'Applied to the schedule' : 'Request submitted — waiting for the supervisor');
      App.render();
    } catch (e) { UI.toast(e.message, 'error'); }
  }

  async function decide(id, status) {
    const r = Swaps.findRequest(Store.get(), id);
    if (!r) return;
    let note = '';
    if (status === 'approved') {
      const v = Swaps.validate(r, { state: Store.get(), role: 'supervisor' });
      if (v.errors.length) { UI.toast(v.errors[0], 'error'); return; }
      if (v.warnings.length) {
        const ok = await UI.confirmDialog({
          title: 'Approve with conflicts?',
          html: 'This request has warnings:<ul>' + v.warnings.map(function (w) { return '<li>' + esc(w) + '</li>'; }).join('') + '</ul>Approve anyway?',
          okLabel: 'Approve anyway', danger: true,
        });
        if (!ok) return;
      }
    } else {
      note = await UI.promptDialog({ title: 'Reject request', label: 'Reason (shown to the requester)', multiline: true, okLabel: 'Reject' });
      if (note === null) return;
    }
    try {
      await Store.mutate(function (d) { Swaps.applyDecision(d, id, { status: status, by: Store.getSession().name || 'Supervisor', note: note }); }, { reason: 'decision' });
      UI.toast(status === 'approved' ? 'Approved — schedule updated' : 'Rejected');
      if (App.state.formView === id) App.state.formView = null;
    } catch (e) { UI.toast(e.message, 'error'); }
  }

  /* ------------------------------------------------------------------
   * photo → request (AI on the sync server)
   * ---------------------------------------------------------------- */
  function presentScheduleHint(y, m) {
    const dim = Engine.daysInMonth(y, m);
    const out = {};
    Engine.activeRoster().forEach(function (e) {
      const row = {};
      for (let d = 1; d <= dim; d++) {
        const iso = y + '-' + (m < 10 ? '0' : '') + m + '-' + (d < 10 ? '0' : '') + d;
        row[iso] = Engine.getScheduleForDate(iso, { employeeId: e.id }).code;
      }
      out[e.id] = row;
    });
    return out;
  }

  /* Handwritten names are usually "R. Rakhmanov" or just "Bouaziz". Match on
   * the surname, use an initial / first name to tell namesakes apart, and
   * when a bare surname is still ambiguous prefer the person whose sheet
   * this is (preferId = the signed-in worker).                          */
  function matchEmployee(name, preferId) {
    if (!name) return null;
    const clean = String(name).toLowerCase().replace(/[^a-z\s,.]/g, ' ');
    const tokens = clean.split(/[\s,.]+/).filter(Boolean);
    if (!tokens.length) return null;
    const roster = Engine.activeRoster().filter(function (e) { return !e.vacant; });
    function parts(e) {
      const p = e.name.toLowerCase().split(',');
      return { last: p[0].replace(/[^a-z]/g, ''), first: (p[1] || '').trim().replace(/[^a-z\s]/g, '').split(/\s+/).filter(Boolean) };
    }
    const joined = tokens.join('');
    const exact = roster.filter(function (e) { return e.name.toLowerCase().replace(/[^a-z]/g, '') === joined; });
    if (exact.length === 1) return exact[0].id;
    let hits = roster.filter(function (e) {
      const last = parts(e).last;
      return last && tokens.some(function (t) { return t.length >= 3 && (t === last || last.indexOf(t) === 0 || t.indexOf(last) === 0); });
    });
    if (hits.length > 1) {
      const rest = tokens.filter(function (t) { return !hits.some(function (e) { const l = parts(e).last; return t === l || l.indexOf(t) === 0 || t.indexOf(l) === 0; }); });
      if (rest.length) {
        const narrowed = hits.filter(function (e) {
          return parts(e).first.some(function (f) { return rest.some(function (t) { return f.indexOf(t) === 0 || t.indexOf(f) === 0; }); });
        });
        if (narrowed.length) hits = narrowed;
      }
      if (hits.length > 1 && preferId && hits.some(function (e) { return e.id === preferId; })) hits = hits.filter(function (e) { return e.id === preferId; });
    }
    return hits.length === 1 ? hits[0].id : null;
  }

  function applyParsed(parsed) {
    const n = App.state.newReq;
    const cfg = Engine.getConfig();
    const notes = [];
    const ids = [];
    const items = [];
    (parsed.changes || []).forEach(function (c) {
      const id = c.employeeId && Engine.getEmployee(c.employeeId) ? c.employeeId : matchEmployee(c.name, n.employeeId);
      if (!id) { notes.push('Could not match the name “' + (c.name || '?') + '” to the roster (' + (c.date || '?') + ').'); return; }
      if (!Engine.isValidISO(c.date)) { notes.push('Unreadable date for ' + UI.empShort(id) + ': “' + (c.date || '?') + '”.'); return; }
      const to = String(c.to || '').toUpperCase().replace(/\s+/g, '');
      if (!cfg.codes[to]) { notes.push('Unknown code “' + (c.to || '?') + '” for ' + UI.empShort(id) + ' on ' + Engine.formatShort(c.date) + '.'); return; }
      if (ids.indexOf(id) === -1) ids.push(id);
      items.push({ id: id, date: c.date, to: to, present: c.present ? String(c.present).toUpperCase().replace(/\s+/g, '') : null });
    });
    if (!items.length) return 0;
    /* who is this request for? */
    if (ids.indexOf(n.employeeId) === -1) n.employeeId = ids[0];
    const others = ids.filter(function (x) { return x !== n.employeeId; });
    if (others.length > 1) notes.push('The sheet names more than two people; only ' + UI.empShort(n.employeeId) + ' and ' + UI.empShort(others[0]) + ' were used.');
    n.partner = others[0] || null;
    n.type = n.partner ? 'swap' : 'timeoff';
    n.dates = [];
    n.to = {};
    items.forEach(function (it) {
      if (it.id !== n.employeeId && it.id !== n.partner) return;
      if (n.dates.indexOf(it.date) === -1) n.dates.push(it.date);
      n.to[key(it.date, it.id)] = it.to;
      const cur = Engine.getScheduleForDate(it.date, { employeeId: it.id }).code;
      if (it.present && it.present !== cur) notes.push('Sheet shows ' + UI.empShort(it.id) + ' as ' + it.present + ' on ' + Engine.formatShort(it.date) + ' but the schedule says ' + cur + ' — check the day columns.');
    });
    /* A day the sheet lists for only one of the two people is not a swap on
     * that day: the other person explicitly keeps their current shift.  */
    n.dates.forEach(function (d) {
      [n.employeeId, n.partner].forEach(function (id) {
        if (!id) return;
        const k = key(d, id);
        if (!Object.prototype.hasOwnProperty.call(n.to, k)) n.to[k] = Engine.getScheduleForDate(d, { employeeId: id }).code;
      });
    });
    if (parsed.reason && !n.reason) n.reason = String(parsed.reason);
    if (parsed.notes) notes.push('Reader notes: ' + String(parsed.notes));
    n.parseNotes = notes;
    n.fromPhoto = true;
    const first = n.dates.slice().sort()[0];
    if (first) n.pick = { y: Number(first.slice(0, 4)), m: Number(first.slice(5, 7)) };
    return n.dates.length;
  }

  function readPhoto(dataUrl) {
    const n = App.state.newReq;
    n.aiBusy = true; App.render();
    const p = n.pick;
    Store.parsePhoto(dataUrl, {
      year: p.y, month: p.m,
      employees: Engine.activeRoster().map(function (e) { return { id: e.id, name: e.name, slot: e.slot }; }),
      requester: n.employeeId,
      present: presentScheduleHint(p.y, p.m),
    }).then(function (res) {
      n.aiBusy = false;
      if (res && res.error) throw new Error(res.error);
      const k = applyParsed(res || {});
      if (k) { n.step = 5; UI.toast('Filled in ' + k + ' day(s) from the photo — check them'); }
      else UI.toast('Could not find any changes in the photo — fill it in by hand', 'error');
      App.render();
    }).catch(function (e) { n.aiBusy = false; UI.toast(e.message, 'error'); App.render(); });
  }

  function attachPhoto(input, thenRead) {
    const file = input.files && input.files[0];
    if (!file) return;
    UI.toast('Processing photo…');
    UI.downscaleImage(file).then(function (dataUrl) {
      App.state.newReq.photo = { dataUrl: dataUrl };
      if (thenRead) readPhoto(dataUrl); else { UI.toast('Photo attached'); App.render(); }
    }).catch(function (e) { UI.toast(e.message, 'error'); });
  }

  function render(root) {
    root.innerHTML = App.state.formView ? formPage() : (App.state.newReq ? wizardView() : listView());
    loadPhotos(root);
  }

  return {
    render: render,
    startNew: startNew,
    actions: {
      filter: function (b) { App.state.reqFilter = b.dataset.f; App.render(); },
      new: function () { startNew({}); App.render(); },
      zoom: function (img) {
        if (!img.src) return;
        UI.openSheet({ title: 'Signed form', body: '<img class="photo-full" src="' + img.src + '" alt="Photo of the signed request form">' });
      },
      form: function (b) { App.state.formView = b.dataset.id; App.render(); window.scrollTo(0, 0); },
      'form-draft': function () { App.state.formView = '__draft__'; App.render(); window.scrollTo(0, 0); },
      'form-back': function () { App.state.formView = null; App.render(); },
      print: function () { window.print(); },
      approve: function (b) { decide(b.dataset.id, 'approved'); },
      reject: function (b) { decide(b.dataset.id, 'rejected'); },
      cancel: function (b) {
        UI.confirmDialog({ title: 'Cancel this request?', message: 'It will be marked cancelled.', okLabel: 'Cancel request', danger: true }).then(function (ok) {
          if (!ok) return;
          Store.mutate(function (d) { Swaps.applyDecision(d, b.dataset.id, { status: 'cancelled', by: Store.getSession().name || '' }); }, { reason: 'decision' })
            .then(function () { App.state.formView = null; UI.toast('Request cancelled'); }).catch(function (e) { UI.toast(e.message, 'error'); });
        });
      },
      'ai-info': function () {
        UI.openSheet({
          title: 'Reading the sheet automatically',
          body: '<p>Reading handwriting needs the shared sync server with an AI key. Until it is set up (see README, “Sharing between phones”), fill the request in by hand — it takes about five taps — and attach the photo of the signed sheet at the end.</p>' +
            '<div class="row" style="justify-content:flex-end;margin-top:10px"><button type="button" class="btn btn-primary" data-close>OK</button></div>',
        });
      },
      /* wizard */
      type: function (b) { const n = App.state.newReq; n.type = b.dataset.type; n.step = n.type === 'swap' ? 2 : 3; App.render(); },
      partner: function (el) { const n = App.state.newReq; n.partner = el.dataset.id; n.to = {}; App.render(); },
      'edit-cells': function () { App.state.newReq.step = 4; App.render(); },
      back: function () {
        const n = App.state.newReq;
        if (n.step === 1) { App.state.newReq = null; }
        else if (n.step === 5 && n.fromPhoto) n.step = 4;
        else if (n.step === 3 && n.type !== 'swap') n.step = 1;
        else n.step -= 1;
        App.render();
      },
      next: function () {
        const n = App.state.newReq;
        if (n.step === 2 && !n.partner) return UI.toast('Pick a coworker first');
        if (n.step === 3 && !n.dates.length) return UI.toast('Pick at least one day');
        n.step += 1; App.render();
      },
      pprev: function () { const p = App.state.newReq.pick; p.m -= 1; if (p.m < 1) { p.m = 12; p.y -= 1; } App.render(); },
      pnext: function () { const p = App.state.newReq.pick; p.m += 1; if (p.m > 12) { p.m = 1; p.y += 1; } App.render(); },
      pick: function (b) {
        const n = App.state.newReq, d = b.dataset.date;
        const i = n.dates.indexOf(d);
        if (i === -1) n.dates.push(d); else n.dates.splice(i, 1);
        App.render();
      },
      'photo-remove': function () { App.state.newReq.photo = null; App.render(); },
      'photo-read': function () { readPhoto(App.state.newReq.photo.dataUrl); },
      submit: function () { submit(); },
    },
    changes: {
      entered: function (cb) {
        const id = cb.dataset.id, val = cb.checked;
        Store.mutate(function (d) { const r = Swaps.findRequest(d, id); if (r) r.enteredInComputer = val; }, { reason: 'entered' }).catch(function (e) { UI.toast(e.message, 'error'); });
      },
      emp: function (sel) { const n = App.state.newReq; n.employeeId = sel.value; n.partner = null; n.to = {}; },
      to: function (sel) { App.state.newReq.to[sel.dataset.key] = sel.value; },
      'approve-now': function (cb) { App.state.newReq.approveNow = cb.checked; App.render(); },
      photo: function (input) { attachPhoto(input, false); },
      'photo-first': function (input) { attachPhoto(input, Store.aiAvailable()); },
    },
    inputs: {
      reason: function (ta) { App.state.newReq.reason = ta.value; },
    },
  };
})();

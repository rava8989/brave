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
   * wizard — an editable copy of the paper form
   *   people : everyone on the form (first = the requester)
   *   to     : { 'YYYY-MM-DD|employeeId': requestedCode } for tapped cells
   * Any person, any day, any code: same-day trades, diagonal trades, and
   * a person's own days moved around can all sit on one request.
   * ---------------------------------------------------------------- */
  function startNew(params) {
    params = params || {};
    const sess = Store.getSession();
    const today = Engine.todayISO();
    const employeeId = params.employeeId || sess.employeeId || (Engine.rosterSorted().filter(function (e) { return !e.vacant; })[0] || {}).id;
    const date = params.date && params.date >= today ? params.date : null;
    const people = [employeeId];
    if (params.partner && params.partner !== employeeId) people.push(params.partner);
    App.state.newReq = {
      step: params.partner || date ? 3 : 1,
      people: people,
      employeeId: employeeId,
      pick: date ? { y: Number(date.slice(0, 4)), m: Number(date.slice(5, 7)) } : { y: Number(today.slice(0, 4)), m: Number(today.slice(5, 7)) },
      to: {},
      reason: '',
      photo: null,
      fromPhoto: false,
      parseNotes: [],
      approveNow: false,
      openDay: date || null,
    };
  }

  function key(date, id) { return date + '|' + id; }
  function present(date, id) { return Engine.getScheduleForDate(date, { employeeId: id }).code; }

  /* Only cells that were actually changed become part of the request. */
  function currentChanges(n) {
    const out = [];
    Object.keys(n.to).forEach(function (k) {
      const i = k.indexOf('|');
      const date = k.slice(0, i), id = k.slice(i + 1);
      if (!Engine.isValidISO(date) || !Engine.getEmployee(id)) return;
      const from = present(date, id);
      if (n.to[k] && n.to[k] !== from) out.push({ employeeId: id, date: date, from: from, to: n.to[k] });
    });
    out.sort(function (x, y) { return x.date < y.date ? -1 : x.date > y.date ? 1 : n.people.indexOf(x.employeeId) - n.people.indexOf(y.employeeId); });
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
  function changesListHTML(n) {
    const ch = currentChanges(n);
    if (!ch.length) return '<p class="muted" style="font-size:.85rem;margin-top:8px">No changes yet — tap a day.</p>';
    let h = '<div class="list" style="margin-top:8px">';
    let last = null;
    ch.forEach(function (c) {
      h += '<div class="item clickable" data-action="day" data-date="' + c.date + '" style="min-height:40px;padding:4px 10px">' +
        '<div class="grow"><span class="name" style="font-size:.85rem">' + (c.date !== last ? esc(Engine.formatShort(c.date)) + ' · ' : '') + esc(UI.empShort(c.employeeId)) + '</span></div>' +
        chip(c.from, 'sm') + '<span class="muted">→</span>' + chip(c.to, 'sm') + '</div>';
      last = c.date;
    });
    return h + '</div>';
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
        h += '<img class="photo-thumb" src="' + n.photo.dataUrl + '" alt="Attached photo" style="max-height:160px"><p style="text-align:center;margin:6px 0 0;font-size:.85rem">✔ Photo attached.</p>' +
          '<div class="row" style="justify-content:center;margin-top:6px"><button type="button" class="btn btn-sm" data-action="photo-remove">Remove photo</button></div>';
      } else {
        h += '<label class="btn btn-block btn-primary" style="min-height:60px;cursor:pointer">📷 Upload the signed swap sheet<br><small style="font-weight:500">' + (Store.aiAvailable() ? 'the app fills everything in from the photo' : 'photo goes with the request; then fill in the days') + '</small>' + photoInput('photo-first') + '</label>';
      }
      h += '</div>';
      h += '<button type="button" class="btn btn-block" style="min-height:56px" data-action="by-hand">✎ Fill in the form' + (n.photo ? '' : ' (no photo yet)') + '</button>';
      h += navButtons(n, null);
    }

    if (n.step === 2) {
      h += '<h2>Who is on this form?</h2><p class="muted" style="font-size:.85rem">Just you for time off or moving your own days. Add whoever you are trading with.</p><div class="list">';
      Engine.rosterSorted().filter(function (e) { return !e.vacant; }).forEach(function (e) {
        const on = n.people.indexOf(e.id) !== -1;
        const me = e.id === n.employeeId;
        h += '<div class="item' + (on ? ' selected' : '') + (me ? '' : ' clickable') + '"' + (me ? '' : ' data-action="person" data-id="' + esc(e.id) + '"') + '><div class="grow"><div class="name">' + esc(e.name) + (me ? ' <span class="pill pill-accent">me</span>' : '') + '</div><div class="sub">' + esc(Engine.shiftCode(e)) + '</div></div>' + (on ? '<span class="pill pill-accent">on form</span>' : '') + '</div>';
      });
      h += '</div>' + navButtons(n, 'Next ›', true);
    }

    if (n.step === 3) {
      const p = n.pick;
      const weeks = Engine.getMonthMatrix(p.y, p.m);
      const today = Engine.todayISO();
      h += '<h2>Requested changes</h2><p class="muted" style="font-size:.85rem">Tap a day and set what each person should work. Rows: ' + esc(n.people.map(function (id) { return UI.firstName(UI.empName(id)) || UI.empShort(id); }).join(', ')) + '.</p>';
      h += '<div class="cal-nav" style="grid-template-columns:44px 1fr 44px"><button type="button" class="btn btn-icon" data-action="pprev">‹</button><span class="title">' + esc(Engine.monthLabel(p.y, p.m)) + '</span><button type="button" class="btn btn-icon" data-action="pnext">›</button></div>';
      h += '<div class="pick-grid">' + Engine.WEEKDAYS.map(function (d) { return '<div class="hd">' + d.toUpperCase().slice(0, 2) + '</div>'; }).join('');
      weeks.forEach(function (row) {
        row.forEach(function (c) {
          const past = c.iso < today && !sup;
          const cls = ['pick-cell'];
          if (!c.inMonth) cls.push('out');
          if (past) cls.push('past');
          const changed = n.people.some(function (id) { const v = n.to[key(c.iso, id)]; return v && v !== present(c.iso, id); });
          if (changed) cls.push('on');
          h += '<button type="button" class="' + cls.join(' ') + '" data-action="day" data-date="' + c.iso + '"' + (past ? ' disabled' : '') + '><span class="num">' + Number(c.iso.slice(8, 10)) + '</span>';
          n.people.forEach(function (id) {
            const cur = present(c.iso, id);
            const req = n.to[key(c.iso, id)];
            h += (req && req !== cur) ? chip(req, 'sm', 'req') : chip(cur, 'sm');
          });
          h += '</button>';
        });
      });
      h += '</div>' + changesListHTML(n);
      h += navButtons(n, 'Next ›', currentChanges(n).length > 0);
    }

    if (n.step === 4) {
      h += '<h2>Reason and photo</h2>';
      h += '<label class="field">Reason (optional)<textarea class="textarea" data-input="reason" placeholder="e.g. doctor appointment, family event">' + esc(n.reason) + '</textarea></label>';
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
      h += '<p class="muted" style="font-size:.78rem;margin:6px 0 4px">Official form (scroll sideways) — present on top, requested below:</p>';
      h += '<div class="osheet-scroll">' + Forms.swapSheetHTML(req) + '</div>';
      h += '<div class="row" style="margin-top:8px"><button type="button" class="btn btn-sm" data-action="form-draft">🖨 Print this form to sign</button>' +
        '<button type="button" class="btn btn-sm" data-action="edit-cells">✎ Edit days</button></div>';
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

  /* Tap a day → set each person's requested code for that day. */
  function openDayEditor(iso) {
    const n = App.state.newReq;
    let body = '<p class="muted" style="font-size:.82rem;margin:0 0 6px">Leave a row as it is if that person does not change that day.</p>';
    n.people.forEach(function (id) {
      const cur = present(iso, id);
      body += '<div class="change-row"><div class="who">' + esc(UI.empShort(id)) + '<small>now ' + esc(cur || '—') + '</small></div><span class="muted">→</span>' +
        '<select class="select" data-p="' + esc(id) + '">' + UI.codeOptions(n.to[key(iso, id)] || cur) + '</select></div>';
    });
    body += '<div class="row" style="margin-top:12px;justify-content:space-between"><div class="row">' +
      (n.people.length === 2 ? '<button type="button" class="btn btn-sm" data-x="swap">⇄ Trade this day</button>' : '') +
      '<button type="button" class="btn btn-sm btn-ghost" data-x="clear">Clear</button></div>' +
      '<button type="button" class="btn btn-primary" data-x="done">Done</button></div>';
    UI.openSheet({
      title: Engine.formatLong(iso),
      body: body,
      onOpen: function (root) {
        root.addEventListener('click', function (ev) {
          const x = ev.target.closest('[data-x]');
          if (!x) return;
          const sels = UI.qsa(root, 'select[data-p]');
          if (x.dataset.x === 'swap') {
            const a = present(iso, n.people[0]), b = present(iso, n.people[1]);
            sels[0].value = b; sels[1].value = a;
            return;
          }
          if (x.dataset.x === 'clear') { sels.forEach(function (sel) { sel.value = present(iso, sel.dataset.p); }); }
          sels.forEach(function (sel) {
            const k = key(iso, sel.dataset.p);
            if (sel.value && sel.value !== present(iso, sel.dataset.p)) n.to[k] = sel.value; else delete n.to[k];
          });
          UI.closeSheet(); App.render();
        });
      },
    });
  }

  function buildFromState(n) {
    const sess = Store.getSession();
    const changes = currentChanges(n);
    const r = Swaps.buildRequest({
      type: n.people.length > 1 ? 'swap' : 'change',
      createdBy: sess.employeeId || 'supervisor',
      createdByName: sess.name || '',
      changes: changes,
      reason: n.reason,
      photoId: n.photo ? 'p-' + (n.photoIdSeed || 'draft') : null,
    });
    /* keep everyone who was on the form, in form order, even with no change yet */
    r.employees = n.people.filter(function (id) { return Engine.getEmployee(id); });
    r.fromPhoto = !!n.fromPhoto;
    return r;
  }

  async function submit() {
    const n = App.state.newReq;
    const sup = Store.isSupervisor();
    n.photoIdSeed = n.photoIdSeed || Swaps.uid();
    const req = buildFromState(n);
    req.employees = req.employees.filter(function (id) { return req.changes.some(function (c) { return c.employeeId === id; }); });
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
    const items = [];
    (parsed.changes || []).forEach(function (c) {
      const id = c.employeeId && Engine.getEmployee(c.employeeId) ? c.employeeId : matchEmployee(c.name, n.employeeId);
      if (!id) { notes.push('Could not match the name “' + (c.name || '?') + '” to the roster (' + (c.date || '?') + ').'); return; }
      if (!Engine.isValidISO(c.date)) { notes.push('Unreadable date for ' + UI.empShort(id) + ': “' + (c.date || '?') + '”.'); return; }
      const to = String(c.to || '').toUpperCase().replace(/\s+/g, '');
      if (!cfg.codes[to]) { notes.push('Unknown code “' + (c.to || '?') + '” for ' + UI.empShort(id) + ' on ' + Engine.formatShort(c.date) + '.'); return; }
      items.push({ id: id, date: c.date, to: to, present: c.present ? String(c.present).toUpperCase().replace(/\s+/g, '') : null });
    });
    if (!items.length) return 0;
    const ids = [];
    items.forEach(function (it) { if (ids.indexOf(it.id) === -1) ids.push(it.id); });
    if (ids.indexOf(n.employeeId) !== -1) { ids.splice(ids.indexOf(n.employeeId), 1); ids.unshift(n.employeeId); }
    else n.employeeId = ids[0];
    n.people = ids.slice(0, 4);
    if (ids.length > 4) notes.push('The sheet names more than four people; only the first four were used.');
    n.to = {};
    let count = 0;
    items.forEach(function (it) {
      if (n.people.indexOf(it.id) === -1) return;
      const cur = present(it.date, it.id);
      n.to[key(it.date, it.id)] = it.to;
      count++;
      if (it.present && it.present !== cur) notes.push('Sheet shows ' + UI.empShort(it.id) + ' as ' + it.present + ' on ' + Engine.formatShort(it.date) + ' but the schedule says ' + cur + ' — check the day columns.');
    });
    if (parsed.reason && !n.reason) n.reason = String(parsed.reason);
    if (parsed.notes) notes.push('Reader notes: ' + String(parsed.notes));
    n.parseNotes = notes;
    n.fromPhoto = true;
    const first = Object.keys(n.to).sort()[0];
    if (first) n.pick = { y: Number(first.slice(0, 4)), m: Number(first.slice(5, 7)) };
    return count;
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
      if (thenRead) readPhoto(dataUrl); else { UI.toast('Photo attached — now fill in the form'); if (App.state.newReq.step === 1) App.state.newReq.step = 2; App.render(); }
    }).catch(function (e) { UI.toast(e.message, 'error'); });
  }

  function render(root) {
    root.innerHTML = App.state.formView ? formPage() : (App.state.newReq ? wizardView() : listView());
    loadPhotos(root);
    const n = App.state.newReq;
    if (n && n.openDay && n.step === 3) { const d = n.openDay; n.openDay = null; setTimeout(function () { openDayEditor(d); }, 50); }
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
      'by-hand': function () { App.state.newReq.step = 2; App.render(); },
      person: function (el) {
        const n = App.state.newReq, id = el.dataset.id;
        const i = n.people.indexOf(id);
        if (i === -1) { if (n.people.length >= 4) return UI.toast('Up to four people on one form'); n.people.push(id); }
        else { n.people.splice(i, 1); Object.keys(n.to).forEach(function (k) { if (k.slice(k.indexOf('|') + 1) === id) delete n.to[k]; }); }
        App.render();
      },
      'edit-cells': function () { App.state.newReq.step = 3; App.render(); },
      back: function () {
        const n = App.state.newReq;
        if (n.step === 1) { App.state.newReq = null; }
        else if (n.step === 5 && n.fromPhoto) n.step = 3;
        else n.step -= 1;
        App.render();
      },
      next: function () {
        const n = App.state.newReq;
        if (n.step === 3 && !currentChanges(n).length) return UI.toast('Tap a day and set a change first');
        n.step += 1; App.render();
      },
      pprev: function () { const p = App.state.newReq.pick; p.m -= 1; if (p.m < 1) { p.m = 12; p.y -= 1; } App.render(); },
      pnext: function () { const p = App.state.newReq.pick; p.m += 1; if (p.m > 12) { p.m = 1; p.y += 1; } App.render(); },
      day: function (b) { openDayEditor(b.dataset.date); },
      'photo-remove': function () { App.state.newReq.photo = null; App.render(); },
      'photo-read': function () { readPhoto(App.state.newReq.photo.dataUrl); },
      submit: function () { submit(); },
    },
    changes: {
      entered: function (cb) {
        const id = cb.dataset.id, val = cb.checked;
        Store.mutate(function (d) { const r = Swaps.findRequest(d, id); if (r) r.enteredInComputer = val; }, { reason: 'entered' }).catch(function (e) { UI.toast(e.message, 'error'); });
      },
      emp: function (sel) { const n = App.state.newReq; n.employeeId = sel.value; n.people = [sel.value]; n.to = {}; },
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

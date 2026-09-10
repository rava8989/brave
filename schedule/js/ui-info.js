/* =====================================================================
 *  ui-info.js — legend, how it works, account, data / sync, printing,
 *  and the supervisor tools (roster, holidays, coverage, PIN, backup).
 * ===================================================================== */
Views.info = (function () {
  'use strict';
  const esc = UI.esc, chip = UI.codeChip;

  function legendHTML() {
    const cfg = Engine.getConfig();
    const primary = cfg.primaryCodes.filter(function (c) { return cfg.codes[c]; });
    const rest = Object.keys(cfg.codes).filter(function (c) { return primary.indexOf(c) === -1; });
    const open = window.innerWidth >= 720 || Store.getPref('legendOpen', false);
    function li(c) { return '<div class="li">' + chip(c, 'sm') + '<span>' + esc(cfg.codes[c].label) + '</span></div>'; }
    return '<details class="card legend-box" ' + (open ? 'open' : '') + ' data-legend><summary>Legend</summary>' +
      '<div class="legend" style="margin-top:8px">' + primary.map(li).join('') + '</div>' +
      '<h3 style="margin-top:12px;font-size:.85rem" class="muted">Other codes</h3><div class="legend">' + rest.map(li).join('') + '</div></details>';
  }

  function accountHTML() {
    const s = Store.getSession();
    const sync = Store.sync;
    let h = '<div class="card"><h2>Account</h2>';
    h += '<p><b>' + esc(s.name || 'Supervisor') + '</b> · ' + (s.role === 'supervisor' ? 'Supervisor (full access)' : 'Worker (can view and submit requests)') + '</p>';
    h += '<p class="muted" style="font-size:.85rem">' + (Store.isRemote()
      ? 'Shared schedule via sync server' + (sync.lastPull ? ' · last refresh ' + esc(UI.fmtDateTime(sync.lastPull)) : '') + (sync.error ? ' · <span style="color:var(--danger)">' + esc(sync.error) + '</span>' : '')
      : 'Local mode — data is stored only in this browser. Set APP_CONFIG.syncUrl in js/config.js to share between phones.') + '</p>';
    h += '<div class="row">' + (Store.isRemote() ? '<button type="button" class="btn btn-sm" data-action="refresh">Refresh now</button>' : '') +
      '<button type="button" class="btn btn-sm" data-action="signout">Sign out</button></div></div>';
    return h;
  }

  function printHTML() {
    return '<div class="card no-print"><h2>Print</h2><div class="row">' +
      '<button type="button" class="btn btn-sm" data-action="print-cal">My month calendar</button>' +
      '<button type="button" class="btn btn-sm" data-action="print-team">Monthly labor schedule (official layout)</button>' +
      '<button type="button" class="btn btn-sm" data-action="print-rot">Rotation</button></div></div>';
  }

  function aboutHTML() {
    const rot = Engine.getConfig().rotation;
    return '<div class="card"><h2>How the rotation works</h2>' +
      '<p style="font-size:.9rem">Six slots, two people per slot, one shared ' + rot.cycleLength + '-day cycle starting ' + esc(Engine.formatLong(rot.referenceDate)) + '. ' +
      'Each slot runs the same pattern one week ahead of the slot before it, which keeps 2 people on the night (A) and evening (C) tours every day and 2 on the day (B) tour at weekends. ' +
      'The A/C and C/A doubles cover the mutual day off (MDO) of the slot three weeks away.</p>' +
      '<p style="font-size:.9rem">Holidays turn B and R days into HOL; A and C tours still work. Approved swap requests and supervisor edits are stored as per-person exceptions on top of the rotation, so nothing else has to be re-entered.</p>' +
      '<p class="muted" style="font-size:.8rem">Data: js/config.js (rotation, holidays, roster) · Full notes: README.md in the schedule folder.</p></div>';
  }

  /* ---------------- supervisor tools ---------------- */
  function rosterHTML() {
    const st = Store.get();
    const showInactive = App.state.showInactive;
    let h = '<div class="card"><h2>Roster <button type="button" class="btn btn-sm" data-action="add-emp">＋ Add</button></h2><div class="list">';
    st.roster.forEach(function (e) {
      if (e.active === false && !showInactive) return;
      h += '<div class="item' + (e.active === false ? ' muted' : '') + '"><div class="grow"><div class="name">' + esc(e.name) + (e.vacant ? ' <span class="pill pill-muted">vacant</span>' : '') + (e.active === false ? ' <span class="pill pill-muted">inactive</span>' : '') + '</div><div class="sub">' + esc(e.group || '') + '</div></div>' +
        '<select class="select" style="width:auto;min-height:38px" data-change="emp-group" data-id="' + esc(e.id) + '" aria-label="Group">' + Engine.groupOrder().map(function (g) { return '<option value="' + esc(g) + '"' + (Engine.groupCodeOf(e) === g ? ' selected' : '') + '>' + esc(g) + '</option>'; }).join('') + '</select>' +
        (Engine.isRotating(e) ? '<select class="select" style="width:auto;min-height:38px" data-change="emp-slot" data-id="' + esc(e.id) + '" aria-label="Slot">' + Engine.slotNumbers().map(function (s) { return '<option value="' + s + '"' + (Number(e.slot) === s ? ' selected' : '') + '>Slot ' + s + '</option>'; }).join('') + '</select>' : '') +
        '<button type="button" class="btn btn-sm btn-icon" data-action="emp-menu" data-id="' + esc(e.id) + '" aria-label="More">⋯</button></div>';
    });
    h += '</div><label class="check" style="margin-top:6px"><input type="checkbox" data-change="show-inactive"' + (showInactive ? ' checked' : '') + '> Show inactive</label></div>';
    return h;
  }

  function holidaysHTML() {
    const list = Engine.getConfig().holidays.slice().sort(function (a, b) { return a.date < b.date ? -1 : 1; });
    const today = Engine.todayISO();
    let h = '<div class="card"><h2>Holidays <button type="button" class="btn btn-sm" data-action="add-hol">＋ Add</button></h2>';
    h += '<p class="muted" style="font-size:.8rem">✔ = seen on a posted schedule. Others are the standard observed dates — confirm against the agency calendar.</p><div class="list">';
    list.forEach(function (hd) {
      if (hd.date < Engine.addDays(today, -60)) return;
      h += '<div class="item"><div class="grow"><div class="name">' + esc(hd.name) + (hd.verified ? ' <span class="pill pill-approved">✔</span>' : '') + '</div><div class="sub">' + esc(Engine.formatLong(hd.date)) + '</div></div>' +
        '<button type="button" class="btn btn-sm btn-ghost" data-action="del-hol" data-date="' + esc(hd.date) + '">Remove</button></div>';
    });
    h += '</div>' + (Store.get().holidays ? '<button type="button" class="btn btn-sm" style="margin-top:8px" data-action="reset-hol">Reset to built-in list</button>' : '') + '</div>';
    return h;
  }

  function settingsHTML() {
    const cfg = Engine.getConfig();
    const cov = cfg.coverage;
    const repl = cfg.holidayRule.replaces;
    let h = '<div class="card"><h2>Rules</h2>';
    h += '<div class="grid-3">' +
      '<label class="field">A tour min<input class="input" type="number" min="0" max="9" data-change="cov" data-t="A" value="' + esc(cov.A ? cov.A.min : 0) + '"></label>' +
      '<label class="field">C tour min<input class="input" type="number" min="0" max="9" data-change="cov" data-t="C" value="' + esc(cov.C ? cov.C.min : 0) + '"></label>' +
      '<label class="field">B tour min<input class="input" type="number" min="0" max="9" data-change="cov" data-t="B" value="' + esc(cov.B ? cov.B.min : 0) + '"></label></div>';
    h += '<label class="field" style="margin-top:8px">B minimum applies<select class="select" data-change="cov-when">' +
      [['weekend', 'Weekends only'], ['weekend-or-holiday', 'Weekends and holidays'], ['always', 'Every day'], ['never', 'Never']].map(function (o) {
        return '<option value="' + o[0] + '"' + ((cov.B && cov.B.when) === o[0] ? ' selected' : '') + '>' + o[1] + '</option>';
      }).join('') + '</select></label>';
    h += '<label class="field" style="margin-top:8px">Warn above this many work days in a row<input class="input" type="number" min="3" max="14" data-change="max-streak" value="' + esc(cov.maxConsecutiveWorkDays || 7) + '"></label>';
    h += '<div class="field" style="margin-top:8px">On a holiday, these codes become HOL</div><div class="row">' + ['A', 'B', 'C', 'R'].map(function (c) {
      return '<label class="check"><input type="checkbox" data-change="hol-rule" data-c="' + c + '"' + (repl.indexOf(c) !== -1 ? ' checked' : '') + '> ' + chip(c, 'sm') + '</label>';
    }).join('') + '</div>';
    if (!Store.isRemote()) h += '<div class="row" style="margin-top:10px"><button type="button" class="btn btn-sm" data-action="pin">Change supervisor PIN</button></div>';
    h += '</div>';
    return h;
  }

  function dataHTML() {
    return '<div class="card"><h2>Backup</h2><div class="row">' +
      '<button type="button" class="btn btn-sm" data-action="export">Download backup (JSON)</button>' +
      '<label class="btn btn-sm" style="cursor:pointer">Restore from backup<input type="file" accept="application/json,.json" class="sr-only" data-change="import"></label>' +
      '<button type="button" class="btn btn-sm btn-danger" data-action="reset">Reset everything</button></div>' +
      '<p class="muted" style="font-size:.78rem;margin-top:6px">A backup contains the roster, rotation edits, holidays, all overrides and requests (photos are not included).</p></div>';
  }

  function render(root) {
    let h = legendHTML() + accountHTML() + printHTML();
    if (Store.isSupervisor()) h += '<h2 style="margin:16px 0 8px">Supervisor tools</h2>' + rosterHTML() + holidaysHTML() + settingsHTML() + dataHTML();
    h += aboutHTML();
    root.innerHTML = h;
    const det = UI.qs(root, '[data-legend]');
    if (det) det.addEventListener('toggle', function () { Store.setPref('legendOpen', det.open); });
  }

  function printTab(tab) {
    App.setTab(tab);
    setTimeout(function () { window.print(); }, 150);
  }

  function mutateCoverage(fn) {
    return Store.mutate(function (d) {
      const cov = JSON.parse(JSON.stringify(Object.assign({}, SCHEDULE_CONFIG.coverage, d.settings.coverage || {})));
      fn(cov);
      d.settings.coverage = cov;
    }, { reason: 'settings' }).catch(function (e) { UI.toast(e.message, 'error'); });
  }
  function mutateHolidays(fn) {
    return Store.mutate(function (d) {
      const list = JSON.parse(JSON.stringify(d.holidays || SCHEDULE_CONFIG.holidays));
      d.holidays = fn(list) || list;
    }, { reason: 'holidays' }).catch(function (e) { UI.toast(e.message, 'error'); });
  }

  return {
    render: render,
    actions: {
      refresh: function () { Store.pull({ force: true }).then(function (ok) { UI.toast(ok ? 'Up to date' : 'Could not reach the sync server', ok ? '' : 'error'); App.render(); }); },
      signout: function () { App.logout(); },
      'print-cal': function () { printTab('calendar'); },
      'print-team': function () { App.state.teamMode = 'month'; App.state.teamLayout = 'official'; printTab('team'); },
      'print-rot': function () { printTab('rotation'); },
      'add-emp': function () {
        UI.promptDialog({ title: 'Add employee', label: 'Name (Last, First)', okLabel: 'Add' }).then(function (name) {
          if (!name || !name.trim()) return;
          const id = name.trim().toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '') + '-' + Math.random().toString(36).slice(2, 6);
          Store.mutate(function (d) { d.roster.push({ id: id, name: name.trim(), group: Engine.groupOrder()[0], slot: 1, active: true }); }, { reason: 'roster' }).then(function () { UI.toast('Added — set the group and slot'); });
        });
      },
      'emp-menu': function (b) {
        const id = b.dataset.id;
        const e = Engine.getEmployee(id) || Store.get().roster.filter(function (x) { return x.id === id; })[0];
        if (!e) return;
        UI.openSheet({
          title: e.name,
          body: '<div class="stack">' +
            '<button type="button" class="btn btn-block" data-x="rename">Rename</button>' +
            '<button type="button" class="btn btn-block" data-x="vacant">' + (e.vacant ? 'Mark as filled (not vacant)' : 'Mark as vacant position') + '</button>' +
            '<button type="button" class="btn btn-block ' + (e.active === false ? '' : 'btn-danger') + '" data-x="toggle">' + (e.active === false ? 'Reactivate' : 'Deactivate (hide from schedule)') + '</button></div>',
          onOpen: function (root) {
            root.addEventListener('click', function (ev) {
              const x = ev.target.closest('[data-x]'); if (!x) return;
              const what = x.dataset.x; UI.closeSheet();
              if (what === 'rename') {
                UI.promptDialog({ title: 'Rename', label: 'Name', value: e.name }).then(function (v) {
                  if (!v || !v.trim()) return;
                  Store.mutate(function (d) { d.roster.forEach(function (r) { if (r.id === id) r.name = v.trim(); }); }, { reason: 'roster' });
                });
              } else if (what === 'vacant') {
                Store.mutate(function (d) { d.roster.forEach(function (r) { if (r.id === id) r.vacant = !r.vacant; }); }, { reason: 'roster' });
              } else if (what === 'toggle') {
                Store.mutate(function (d) { d.roster.forEach(function (r) { if (r.id === id) r.active = r.active === false; }); }, { reason: 'roster' });
              }
            });
          },
        });
      },
      'add-hol': function () {
        UI.promptDialog({ title: 'Add holiday', label: 'Date', type: 'date', okLabel: 'Next' }).then(function (date) {
          if (!date || !Engine.isValidISO(date)) return;
          UI.promptDialog({ title: 'Add holiday', label: 'Name', okLabel: 'Add' }).then(function (name) {
            if (!name) return;
            mutateHolidays(function (list) { return list.filter(function (x) { return x.date !== date; }).concat([{ date: date, name: name.trim(), verified: true }]); });
          });
        });
      },
      'del-hol': function (b) {
        const date = b.dataset.date;
        mutateHolidays(function (list) { return list.filter(function (x) { return x.date !== date; }); });
      },
      'reset-hol': function () { Store.mutate(function (d) { d.holidays = null; }, { reason: 'holidays' }); },
      pin: function () {
        UI.promptDialog({ title: 'Supervisor PIN', label: 'New PIN (4–8 digits)', type: 'text', attrs: 'inputmode="numeric" pattern="[0-9]*"' }).then(function (v) {
          if (v === null) return;
          if (!/^\d{4,8}$/.test(v)) return UI.toast('PIN must be 4–8 digits', 'error');
          Store.mutate(function (d) { d.settings.supervisorPin = v; }, { reason: 'settings' }).then(function () { UI.toast('PIN changed'); });
        });
      },
      export: function () { UI.download('schedule-backup-' + Engine.todayISO() + '.json', Store.exportJSON()); },
      reset: function () {
        UI.confirmDialog({ title: 'Reset everything?', message: 'Roster, rotation edits, holidays, overrides and requests will go back to the built-in defaults.', okLabel: 'Reset', danger: true }).then(function (ok) {
          if (ok) Store.resetAll().then(function () { UI.toast('Reset done'); });
        });
      },
    },
    changes: {
      'emp-slot': function (sel) {
        const id = sel.dataset.id, slot = Number(sel.value);
        Store.mutate(function (d) { d.roster.forEach(function (r) { if (r.id === id) r.slot = slot; }); }, { reason: 'roster' }).catch(function (e) { UI.toast(e.message, 'error'); });
      },
      'emp-group': function (sel) {
        const id = sel.dataset.id, g = sel.value;
        Store.mutate(function (d) { d.roster.forEach(function (r) { if (r.id === id) { r.group = g; if (!Engine.groupInfo(g) || Engine.groupInfo(g).kind === 'static') r.slot = 1; } }); }, { reason: 'roster' }).catch(function (e) { UI.toast(e.message, 'error'); });
      },
      'show-inactive': function (cb) { App.state.showInactive = cb.checked; App.render(); },
      cov: function (input) { const t = input.dataset.t, v = Math.max(0, Number(input.value) || 0); mutateCoverage(function (c) { c[t] = Object.assign({ when: 'always' }, c[t] || {}, { min: v }); }); },
      'cov-when': function (sel) { const v = sel.value; mutateCoverage(function (c) { c.B = Object.assign({ min: 2 }, c.B || {}, { when: v }); }); },
      'max-streak': function (input) { const v = Math.max(3, Number(input.value) || 7); mutateCoverage(function (c) { c.maxConsecutiveWorkDays = v; }); },
      'hol-rule': function (cb) {
        const c = cb.dataset.c, on = cb.checked;
        Store.mutate(function (d) {
          const cur = (d.settings.holidayReplaces || SCHEDULE_CONFIG.holidayRule.replaces).slice();
          const i = cur.indexOf(c);
          if (on && i === -1) cur.push(c); if (!on && i !== -1) cur.splice(i, 1);
          d.settings.holidayReplaces = cur;
        }, { reason: 'settings' }).catch(function (e) { UI.toast(e.message, 'error'); });
      },
      import: function (input) {
        const file = input.files && input.files[0]; if (!file) return;
        const reader = new FileReader();
        reader.onload = function () {
          UI.confirmDialog({ title: 'Restore backup?', message: 'This replaces the current roster, rotation, holidays, overrides and requests.', okLabel: 'Restore', danger: true }).then(function (ok) {
            if (!ok) return;
            Store.importJSON(String(reader.result)).then(function () { UI.toast('Backup restored'); }).catch(function (e) { UI.toast('Could not read that file: ' + e.message, 'error'); });
          });
        };
        reader.readAsText(file);
      },
    },
  };
})();

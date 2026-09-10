/* =====================================================================
 *  ui-rotation.js — the six-slot rotation: this week's lineup and the
 *  full 42-day pattern per slot (editable by a supervisor).
 * ===================================================================== */
Views.rotation = (function () {
  'use strict';
  const esc = UI.esc, chip = UI.codeChip;

  function mondayOf(iso) { const w = Engine.weekday(iso); return Engine.addDays(iso, w === 0 ? -6 : 1 - w); }

  /* Cycle index of the Monday that starts the pattern grid (week 1, Mon). */
  function gridIndex0() {
    const rot = Engine.getConfig().rotation;
    const w = Engine.weekday(rot.referenceDate);
    const delta = w === 0 ? -6 : 1 - w;                 // back to Monday
    const n = rot.cycleLength;
    return ((delta % n) + n) % n;
  }

  function lineupHTML() {
    const today = Engine.todayISO();
    const start = App.state.rotWeekStart || mondayOf(today);
    const slots = Engine.slotNumbers();
    let h = '<div class="card"><h2>Week lineup <span class="row no-print" style="gap:4px">' +
      '<button type="button" class="btn btn-icon btn-sm" data-action="wprev" aria-label="Previous week">‹</button>' +
      '<button type="button" class="btn btn-sm" data-action="wnow">This week</button>' +
      '<button type="button" class="btn btn-icon btn-sm" data-action="wnext" aria-label="Next week">›</button></span></h2>' +
      '<p class="muted" style="font-size:.8rem;margin:0 0 8px">' + esc(Engine.formatShort(start)) + ' – ' + esc(Engine.formatShort(Engine.addDays(start, 6))) + '</p>' +
      '<div class="lineup"><div class="hd">Slot</div>';
    for (let d = 0; d < 7; d++) {
      const iso = Engine.addDays(start, d);
      h += '<div class="hd">' + Engine.WEEKDAYS[Engine.weekday(iso)] + '<small>' + Number(iso.slice(8, 10)) + '</small></div>';
    }
    slots.forEach(function (s) {
      const names = Engine.employeesInSlot(s).map(function (e) { return UI.empShort(e.id); }).join(' / ');
      h += '<div class="slot">' + s + '<small>' + esc(names) + '</small></div>';
      for (let d = 0; d < 7; d++) {
        const iso = Engine.addDays(start, d);
        const code = Engine.getRotationCode(iso, s);
        h += '<div class="cell' + (iso === today ? ' today' : '') + '">' + chip(code) + '</div>';
      }
    });
    h += '</div></div>';
    return h;
  }

  function patternHTML() {
    const rot = Engine.getConfig().rotation;
    const slot = App.state.rotSlot;
    const editing = App.state.rotEdit;
    const pattern = editing ? editing.pattern : rot.slots[slot];
    const n = rot.cycleLength;
    const today = Engine.todayISO();
    const todayIdx = Engine.getCycleIndex(today);
    const i0 = gridIndex0();
    const names = Engine.employeesInSlot(slot).map(function (e) { return e.name; }).join(' · ');
    let h = '<div class="card"><h2>Six-week pattern</h2>' +
      '<div class="seg grow no-print" style="margin-bottom:8px">' + Engine.slotNumbers().map(function (s) {
        return '<button type="button" class="' + (s === slot ? 'active' : '') + '" data-action="slot" data-slot="' + s + '">Slot ' + s + '</button>';
      }).join('') + '</div>' +
      '<p class="muted" style="font-size:.82rem;margin:0 0 8px">' + esc(names || 'no one assigned') + ' · today is cycle day ' + (todayIdx + 1) + ' of ' + n + '</p>' +
      '<div class="rot-grid"><div class="hd"></div>' + ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'].map(function (d) { return '<div class="hd">' + d + '</div>'; }).join('');
    const weeks = Math.ceil(n / 7);
    for (let w = 0; w < weeks; w++) {
      h += '<div class="wk">Wk ' + (w + 1) + '</div>';
      for (let d = 0; d < 7; d++) {
        const idx = (i0 + 7 * w + d) % n;
        const code = pattern[idx];
        h += '<div class="cell' + (idx === todayIdx ? ' now' : '') + (editing ? ' editing' : '') + '"' + (editing ? ' data-action="pcell" data-idx="' + idx + '"' : '') + ' title="cycle day ' + (idx + 1) + '">' + chip(code) + '</div>';
      }
    }
    h += '</div>';
    if (Store.isSupervisor()) {
      h += '<div class="row no-print" style="margin-top:10px">';
      if (!editing) {
        h += '<button type="button" class="btn btn-sm" data-action="edit">Edit pattern</button>';
        if (Store.get().rotation) h += '<button type="button" class="btn btn-sm" data-action="reset-rot">Reset to built-in</button>';
      } else {
        h += '<button type="button" class="btn btn-sm btn-primary" data-action="save">Save slot ' + slot + '</button>' +
          '<button type="button" class="btn btn-sm" data-action="save-all">Save &amp; apply to all slots</button>' +
          '<button type="button" class="btn btn-sm" data-action="cancel">Cancel</button>';
      }
      h += '</div>';
      if (editing) h += '<p class="muted" style="font-size:.78rem">Tap a cell to change it. “Apply to all slots” copies this pattern to every slot, shifted one week per slot.</p>';
    }
    h += '<p class="muted" style="font-size:.78rem;margin-top:8px">The cycle restarts every ' + n + ' days from ' + esc(Engine.formatLong(rot.referenceDate)) + '. Each slot runs one week ahead of the slot before it: slot ' + (slot < Engine.slotNumbers().length ? slot + 1 : 1) + ' works today what slot ' + slot + ' works next week.</p>';
    h += '</div>';
    return h;
  }

  function render(root) {
    if (!App.state.rotWeekStart) App.state.rotWeekStart = mondayOf(Engine.todayISO());
    root.innerHTML = lineupHTML() + patternHTML();
  }

  function saveRotation(patterns) {
    return Store.mutate(function (d) {
      const base = JSON.parse(JSON.stringify(d.rotation || SCHEDULE_CONFIG.rotation));
      Object.keys(patterns).forEach(function (s) { base.slots[s] = patterns[s]; });
      d.rotation = base;
    }, { reason: 'rotation' });
  }

  return {
    render: render,
    actions: {
      wprev: function () { App.state.rotWeekStart = Engine.addDays(App.state.rotWeekStart, -7); App.render(); },
      wnext: function () { App.state.rotWeekStart = Engine.addDays(App.state.rotWeekStart, 7); App.render(); },
      wnow: function () { App.state.rotWeekStart = mondayOf(Engine.todayISO()); App.render(); },
      slot: function (b) {
        if (App.state.rotEdit) { UI.toast('Save or cancel the edit first'); return; }
        App.state.rotSlot = Number(b.dataset.slot); Store.setPref('rotSlot', App.state.rotSlot); App.render();
      },
      edit: function () {
        App.state.rotEdit = { pattern: Engine.getConfig().rotation.slots[App.state.rotSlot].slice() };
        App.render();
      },
      cancel: function () { App.state.rotEdit = null; App.render(); },
      pcell: function (cell) {
        const idx = Number(cell.dataset.idx);
        const cur = App.state.rotEdit.pattern[idx];
        UI.openSheet({
          title: 'Cycle day ' + (idx + 1),
          body: '<label class="field">Code<select class="select" data-code>' + UI.codeOptions(cur) + '</select></label>' +
            '<div class="row" style="margin-top:12px;justify-content:flex-end"><button type="button" class="btn" data-close>Cancel</button><button type="button" class="btn btn-primary" data-ok>Set</button></div>',
          onOpen: function (root) {
            UI.qs(root, '[data-ok]').addEventListener('click', function () {
              App.state.rotEdit.pattern[idx] = UI.qs(root, '[data-code]').value; UI.closeSheet(); App.render();
            });
          },
        });
      },
      save: function () {
        const p = {}; p[App.state.rotSlot] = App.state.rotEdit.pattern;
        saveRotation(p).then(function () { App.state.rotEdit = null; UI.toast('Slot ' + App.state.rotSlot + ' saved'); App.render(); }).catch(function (e) { UI.toast(e.message, 'error'); });
      },
      'save-all': function () {
        const rot = Engine.getConfig().rotation;
        const n = rot.cycleLength, off = rot.slotOffsetDays || 7;
        const base = App.state.rotEdit.pattern, cur = App.state.rotSlot;
        const p = {};
        Engine.slotNumbers().forEach(function (s) {
          const shift = ((s - cur) * off % n + n) % n;
          p[s] = base.map(function (_, i) { return base[(i + shift) % n]; });
        });
        UI.confirmDialog({ title: 'Apply to all slots?', message: 'Every slot will be replaced by this pattern shifted ' + off + ' days per slot.', okLabel: 'Apply' }).then(function (ok) {
          if (!ok) return;
          saveRotation(p).then(function () { App.state.rotEdit = null; UI.toast('All slots updated'); App.render(); }).catch(function (e) { UI.toast(e.message, 'error'); });
        });
      },
      'reset-rot': function () {
        UI.confirmDialog({ title: 'Reset rotation?', message: 'Go back to the pattern built into config.js.', okLabel: 'Reset', danger: true }).then(function (ok) {
          if (!ok) return;
          Store.mutate(function (d) { d.rotation = null; }, { reason: 'rotation' }).then(function () { UI.toast('Rotation reset'); });
        });
      },
    },
    changes: {},
  };
})();

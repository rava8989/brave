/* =====================================================================
 *  swaps.js — swap / time-off requests: model, conflict checks, approval
 *
 *  A request is a list of per-person, per-date changes:
 *      { employeeId, date: 'YYYY-MM-DD', from: 'A', to: 'RDO' }
 *  "from" is the code the requester saw when writing the request; it is
 *  used to detect a schedule that changed underneath a pending request.
 *
 *  validate(request, ctx) never mutates anything and returns
 *      { ok, errors[], warnings[], notes[] }
 *  errors  → the request cannot be submitted
 *  warnings→ conflicts the supervisor must knowingly accept
 *  notes   → informational (e.g. an uneven swap)
 *
 *  applyDecision(draftState, id, decision) is the ONLY code path that
 *  turns an approved request into schedule exceptions.
 * ===================================================================== */
const Swaps = (function () {
  'use strict';
  const E = (typeof Engine !== 'undefined') ? Engine : require('./engine.js');

  function uid() {
    return Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 8);
  }
  function nowISO() { return new Date().toISOString(); }
  function uniq(list) { return list.filter(function (x, i) { return list.indexOf(x) === i; }); }
  function empName(id) { const e = E.getEmployee(id); return e ? e.name : id; }
  function shortName(id) {
    const n = empName(id);
    return n.indexOf(',') !== -1 ? n.split(',')[0] : n;
  }

  function normalizeChange(c) {
    return {
      employeeId: c.employeeId,
      date: c.date,
      from: c.from || null,
      to: String(c.to || '').trim(),
    };
  }

  function buildRequest(o) {
    const changes = (o.changes || []).map(normalizeChange);
    return {
      id: o.id || uid(),
      type: o.type || 'swap',                 // swap | timeoff | change
      status: o.status || 'pending',          // pending | approved | rejected | cancelled
      createdAt: o.createdAt || nowISO(),
      createdBy: o.createdBy || null,         // employee id, or 'supervisor'
      createdByName: o.createdByName || '',
      employees: uniq(changes.map(function (c) { return c.employeeId; })),
      changes: changes,
      reason: o.reason || '',
      photoId: o.photoId || null,
      validation: o.validation || null,       // snapshot taken at submit time
      decidedAt: null,
      decidedBy: null,
      decisionNote: '',
      enteredInComputer: false,
    };
  }

  /* Two people trade whatever they have on each of the given dates. */
  function swapChanges(empA, empB, dates) {
    const out = [];
    dates.forEach(function (d) {
      const a = E.getScheduleForDate(d, { employeeId: empA }).code;
      const b = E.getScheduleForDate(d, { employeeId: empB }).code;
      out.push({ employeeId: empA, date: d, from: a, to: b });
      out.push({ employeeId: empB, date: d, from: b, to: a });
    });
    return out;
  }

  function affectedDates(req) {
    return uniq((req.changes || []).map(function (c) { return c.date; })).sort();
  }

  function describe(req) {
    const who = (req.employees || []).map(shortName).join(' ⇄ ');
    const dates = affectedDates(req);
    const span = dates.length === 1 ? E.formatDayMonth(dates[0])
      : dates.length ? E.formatDayMonth(dates[0]) + ' – ' + E.formatDayMonth(dates[dates.length - 1]) + ' (' + dates.length + ' days)' : 'no dates';
    const kind = req.type === 'swap' ? 'Swap' : req.type === 'timeoff' ? 'Time off' : 'Change';
    return kind + ': ' + who + ' · ' + span;
  }

  /* ------------------------------------------------------------------ */
  function validate(req, ctx) {
    ctx = ctx || {};
    const state = ctx.state || { requests: [] };
    const today = ctx.today || E.todayISO();
    const role = ctx.role || 'worker';
    const cfg = E.getConfig();
    const errors = [], warnings = [], notes = [];
    const changes = req.changes || [];

    if (!changes.length) errors.push('Add at least one change.');
    const seen = {};
    changes.forEach(function (c) {
      const emp = E.getEmployee(c.employeeId);
      const dateOk = E.isValidISO(c.date);
      if (!emp) errors.push('Unknown employee "' + c.employeeId + '".');
      if (!dateOk) errors.push('Invalid date "' + c.date + '".');
      if (!c.to || !cfg.codes[c.to]) errors.push('Unknown code "' + c.to + '".');
      const key = c.employeeId + '|' + c.date;
      if (seen[key]) errors.push('Two changes for ' + shortName(c.employeeId) + ' on the same day (' + c.date + ').');
      seen[key] = true;
      if (role !== 'supervisor' && dateOk && c.date < today) {
        errors.push(E.formatShort(c.date) + ' is in the past — only a supervisor can change it.');
      }
    });
    if (errors.length) return { ok: false, errors: uniq(errors), warnings: [], notes: [] };

    /* Stale / no-op */
    changes.forEach(function (c) {
      const cur = E.getScheduleForDate(c.date, { employeeId: c.employeeId }).code;
      if (c.from && cur !== c.from) {
        warnings.push(shortName(c.employeeId) + ' on ' + E.formatShort(c.date) + ' is now ' + cur +
          ' (the request was written against ' + c.from + ').');
      }
      if (cur === c.to) notes.push(shortName(c.employeeId) + ' is already ' + c.to + ' on ' + E.formatShort(c.date) + ' — no change needed.');
    });

    /* Overlap with other pending requests */
    (state.requests || []).forEach(function (r) {
      if (!r || r.id === req.id || r.status !== 'pending') return;
      (r.changes || []).forEach(function (rc) {
        if (changes.some(function (c) { return c.employeeId === rc.employeeId && c.date === rc.date; })) {
          warnings.push('Another pending request (' + describe(r) + ') also changes ' + shortName(rc.employeeId) + ' on ' + E.formatShort(rc.date) + '.');
        }
      });
    });

    /* Coverage on every affected date: warn only where THIS request pushes a
     * tour below its minimum; a shortage that already exists is a note.   */
    const byDate = {};
    changes.forEach(function (c) { (byDate[c.date] = byDate[c.date] || {})[c.employeeId] = c.to; });
    Object.keys(byDate).sort().forEach(function (date) {
      const before = E.getDaySummary(date).coverage;
      const after = E.getDaySummary(date, byDate[date]).coverage;
      Object.keys(after.required).forEach(function (t) {
        const need = after.required[t];
        if (after.counts[t] >= need) return;
        const msg = E.formatShort(date) + ': ' + t + ' tour would have ' + after.counts[t] + ' (needs ' + need + ')';
        if (after.counts[t] < before.counts[t]) warnings.push(msg + ' — this request removes cover.');
        else notes.push(E.formatShort(date) + ': ' + t + ' tour is already short (' + after.counts[t] + ' of ' + need + ') before this request.');
      });
    });

    /* Doubles on adjacent days, long streaks */
    const byEmp = {};
    changes.forEach(function (c) { (byEmp[c.employeeId] = byEmp[c.employeeId] || {})[c.date] = c.to; });
    const maxStreak = (cfg.coverage && cfg.coverage.maxConsecutiveWorkDays) || 7;
    Object.keys(byEmp).forEach(function (id) {
      const ov = byEmp[id];
      function codeOn(d) {
        return Object.prototype.hasOwnProperty.call(ov, d) ? ov[d] : E.getScheduleForDate(d, { employeeId: id }).code;
      }
      Object.keys(ov).forEach(function (date) {
        if (cfg.coverage && cfg.coverage.warnConsecutiveDoubles && E.isDouble(codeOn(date)) &&
            (E.isDouble(codeOn(E.addDays(date, -1))) || E.isDouble(codeOn(E.addDays(date, 1))))) {
          warnings.push(shortName(id) + ' would work doubles on back-to-back days around ' + E.formatShort(date) + '.');
        }
        const streak = E.workStreakAround(date, id, ov);
        if (streak > maxStreak) {
          warnings.push(shortName(id) + ' would work ' + streak + ' days in a row around ' + E.formatShort(date) + ' (limit ' + maxStreak + ').');
        }
      });
    });

    /* Uneven swap (informational) */
    if (req.type === 'swap' && req.employees && req.employees.length === 2) {
      const gives = {};
      req.employees.forEach(function (id) { gives[id] = 0; });
      changes.forEach(function (c) {
        const before = E.tours(c.from || E.getScheduleForDate(c.date, { employeeId: c.employeeId }).code).length;
        const after = E.tours(c.to).length;
        gives[c.employeeId] += before - after;     // tours given away
      });
      const a = req.employees[0], b = req.employees[1];
      if (gives[a] !== -gives[b] || gives[a] !== 0 && gives[b] !== 0 && gives[a] !== -gives[b]) {
        notes.push('Tours are not traded one-for-one: ' + shortName(a) + ' gives ' + gives[a] + ', ' + shortName(b) + ' gives ' + gives[b] + '.');
      }
    }

    return { ok: true, errors: [], warnings: uniq(warnings), notes: uniq(notes) };
  }

  /* ------------------------------------------------------------------ */
  function findRequest(state, id) {
    return (state.requests || []).filter(function (r) { return r.id === id; })[0] || null;
  }

  function applyDecision(draft, requestId, decision) {
    const req = findRequest(draft, requestId);
    if (!req) throw new Error('Request not found: ' + requestId);
    if (decision.status === 'approved') {
      draft.exceptions = draft.exceptions || {};
      req.changes.forEach(function (c) {
        const rotation = E.getRegularCode(c.date, c.employeeId);
        const day = draft.exceptions[c.date] || (draft.exceptions[c.date] = {});
        if (c.to === rotation) {
          delete day[c.employeeId];                     // back to the rotation → no override needed
          if (!Object.keys(day).length) delete draft.exceptions[c.date];
        } else {
          day[c.employeeId] = { code: c.to, note: describe(req), source: req.id, by: decision.by || '', at: decision.at || nowISO() };
        }
      });
    }
    req.status = decision.status;
    req.decidedAt = decision.at || nowISO();
    req.decidedBy = decision.by || '';
    req.decisionNote = decision.note || '';
    return req;
  }

  /* Supervisor manual override of one cell (not tied to a request). */
  function setOverride(draft, date, employeeId, code, meta) {
    draft.exceptions = draft.exceptions || {};
    const rotation = E.getRegularCode(date, employeeId);
    const day = draft.exceptions[date] || (draft.exceptions[date] = {});
    if (!code || code === rotation) {
      delete day[employeeId];
      if (!Object.keys(day).length) delete draft.exceptions[date];
    } else {
      day[employeeId] = Object.assign({ code: code, note: '', source: 'manual', at: nowISO() }, meta || {});
    }
  }

  return {
    uid: uid, buildRequest: buildRequest, swapChanges: swapChanges, affectedDates: affectedDates,
    describe: describe, validate: validate, applyDecision: applyDecision, findRequest: findRequest,
    setOverride: setOverride, shortName: shortName, empName: empName,
  };
})();

if (typeof module !== 'undefined' && module.exports) { module.exports = Swaps; }

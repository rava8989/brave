/* =====================================================================
 *  engine.js — the schedule engine (pure logic, no DOM)
 *
 *  Public API (window.Engine):
 *    configure(cfg)                 load merged config (rotation, holidays, ...)
 *    getConfig()
 *    -- dates --
 *    todayISO(), toISO(date), parseISO(iso), isValidISO(iso)
 *    addDays(iso, n), daysBetween(a, b), weekday(iso)      (0 = Sunday)
 *    isWeekend(iso), formatLong(iso), formatShort(iso), monthLabel(y, m)
 *    daysInMonth(y, m), isLeapYear(y), getMonthMatrix(y, m)
 *    -- rotation --
 *    getCycleIndex(iso), getSlotForDate(iso, employeeId), getBaseCode(iso, slot)
 *    getHoliday(iso), getExceptionForDate(iso, employeeId)
 *    getShiftForDate(iso, who), getScheduleForDate(iso, who)
 *    getRange(startISO, days, who), findNext(iso, who, predicate)
 *    -- codes / coverage --
 *    codeMeta(code), tours(code), isWork(code), isOff(code), isDouble(code)
 *    getDaySummary(iso, overrides), checkCoverage(iso, entries)
 *    -- employees --
 *    getEmployee(id), activeRoster(), employeesInSlot(slot)
 *
 *  `who` is { employeeId } (preferred; exceptions apply) or { slot }.
 * ===================================================================== */
const Engine = (function () {
  'use strict';

  const DAY_MS = 86400000;
  const WEEKDAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  const WEEKDAYS_LONG = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
  const MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July',
    'August', 'September', 'October', 'November', 'December'];
  const MONTHS_SHORT = MONTHS.map(function (m) { return m.slice(0, 3); });

  /* Runtime config; filled by configure(). */
  let cfg = null;

  /* ------------------------------------------------------------------
   * Date helpers — dates travel as 'YYYY-MM-DD' strings. Day arithmetic
   * is done in UTC so DST changes can never shift a date.
   * ---------------------------------------------------------------- */
  function pad2(n) { return (n < 10 ? '0' : '') + n; }

  function toISO(d) {
    return d.getFullYear() + '-' + pad2(d.getMonth() + 1) + '-' + pad2(d.getDate());
  }
  function parts(iso) {
    const p = String(iso).split('-');
    return [Number(p[0]), Number(p[1]), Number(p[2])];
  }
  function parseISO(iso) {
    const p = parts(iso);
    return new Date(p[0], p[1] - 1, p[2]);
  }
  function isValidISO(iso) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(String(iso))) return false;
    const p = parts(iso);
    const d = new Date(Date.UTC(p[0], p[1] - 1, p[2]));
    return d.getUTCFullYear() === p[0] && d.getUTCMonth() === p[1] - 1 && d.getUTCDate() === p[2];
  }
  function utcMs(iso) {
    const p = parts(iso);
    return Date.UTC(p[0], p[1] - 1, p[2]);
  }
  function fromUtcMs(ms) {
    const d = new Date(ms);
    return d.getUTCFullYear() + '-' + pad2(d.getUTCMonth() + 1) + '-' + pad2(d.getUTCDate());
  }
  function todayISO() { return toISO(new Date()); }
  function addDays(iso, n) { return fromUtcMs(utcMs(iso) + n * DAY_MS); }
  function daysBetween(a, b) { return Math.round((utcMs(b) - utcMs(a)) / DAY_MS); }
  function weekday(iso) { return new Date(utcMs(iso)).getUTCDay(); }
  function isWeekend(iso) { const w = weekday(iso); return w === 0 || w === 6; }
  function isLeapYear(y) { return (y % 4 === 0 && y % 100 !== 0) || y % 400 === 0; }
  function daysInMonth(y, m) { return new Date(Date.UTC(y, m, 0)).getUTCDate(); } // m = 1..12
  function monthLabel(y, m) { return MONTHS[m - 1] + ' ' + y; }
  function formatLong(iso) {
    const p = parts(iso);
    return WEEKDAYS[weekday(iso)] + ', ' + MONTHS_SHORT[p[1] - 1] + ' ' + p[2] + ', ' + p[0];
  }
  function formatShort(iso) {
    const p = parts(iso);
    return WEEKDAYS[weekday(iso)] + ' ' + MONTHS_SHORT[p[1] - 1] + ' ' + p[2];
  }
  function formatDayMonth(iso) {
    const p = parts(iso);
    return MONTHS_SHORT[p[1] - 1] + ' ' + p[2];
  }
  function mod(n, m) { return ((n % m) + m) % m; }

  /* Sunday-to-Saturday matrix for a month; cells outside the month are
   * flagged inMonth:false so the grid can render them muted.            */
  function getMonthMatrix(y, m) {
    const first = y + '-' + pad2(m) + '-01';
    const lead = weekday(first);
    const start = addDays(first, -lead);
    const weeks = [];
    let iso = start;
    do {
      const row = [];
      for (let i = 0; i < 7; i++) {
        row.push({ iso: iso, inMonth: Number(iso.slice(5, 7)) === m && Number(iso.slice(0, 4)) === y });
        iso = addDays(iso, 1);
      }
      weeks.push(row);
    } while (Number(iso.slice(5, 7)) === m && Number(iso.slice(0, 4)) === y);
    return weeks;
  }

  /* ------------------------------------------------------------------
   * Configuration
   * ---------------------------------------------------------------- */
  function configure(input) {
    const holidayIndex = {};
    (input.holidays || []).forEach(function (h) { if (h && h.date) holidayIndex[h.date] = h; });
    cfg = {
      rotation: input.rotation,
      holidays: input.holidays || [],
      holidayIndex: holidayIndex,
      holidayRule: input.holidayRule || { replaces: ['B', 'R'] },
      coverage: input.coverage || {},
      codes: input.codes || {},
      primaryCodes: input.primaryCodes || Object.keys(input.codes || {}),
      exceptions: input.exceptions || {},
      roster: input.roster || [],
    };
    cfg.rosterIndex = {};
    cfg.roster.forEach(function (e) { cfg.rosterIndex[e.id] = e; });
    return cfg;
  }
  function getConfig() { return cfg; }
  function assertConfigured() { if (!cfg) throw new Error('Engine.configure() has not been called'); }

  /* ------------------------------------------------------------------
   * Codes
   * ---------------------------------------------------------------- */
  function codeMeta(code) {
    assertConfigured();
    if (!code) return { label: 'Not scheduled', kind: 'other', tours: [] };
    const m = cfg.codes[code];
    if (m) return Object.assign({ code: code, tours: [] }, m);
    return { code: code, label: code, kind: 'other', tours: [] };
  }
  function tours(code) { return codeMeta(code).tours || []; }
  function isWork(code) { return codeMeta(code).kind === 'work'; }
  function isOff(code) { const k = codeMeta(code).kind; return k === 'off' || k === 'leave'; }
  function isDouble(code) { return !!codeMeta(code).double; }
  function allCodes() { assertConfigured(); return Object.keys(cfg.codes); }

  /* ------------------------------------------------------------------
   * Employees
   * ---------------------------------------------------------------- */
  function getEmployee(id) { assertConfigured(); return cfg.rosterIndex[id] || null; }
  function activeRoster() {
    assertConfigured();
    return cfg.roster.filter(function (e) { return e.active !== false; });
  }
  function employeesInSlot(slot) {
    return activeRoster().filter(function (e) { return Number(e.slot) === Number(slot); });
  }
  function slotNumbers() {
    assertConfigured();
    return Object.keys(cfg.rotation.slots).map(Number).sort(function (a, b) { return a - b; });
  }

  /* ------------------------------------------------------------------
   * Rotation
   * ---------------------------------------------------------------- */
  function getCycleIndex(iso) {
    assertConfigured();
    const rot = cfg.rotation;
    return mod(daysBetween(rot.referenceDate, iso), rot.cycleLength);
  }
  function getSlotForDate(iso, employeeId) {
    const e = getEmployee(employeeId);
    return e ? Number(e.slot) : null;
  }
  function getBaseCode(iso, slot) {
    assertConfigured();
    const pattern = cfg.rotation.slots[slot];
    if (!pattern || !pattern.length) return null;
    return pattern[getCycleIndex(iso) % pattern.length] || null;
  }
  function getHoliday(iso) { assertConfigured(); return cfg.holidayIndex[iso] || null; }
  function getExceptionForDate(iso, employeeId) {
    assertConfigured();
    if (!employeeId) return null;
    const day = cfg.exceptions[iso];
    return day && day[employeeId] ? day[employeeId] : null;
  }

  /* Rotation + holiday rule, before any per-person exception. */
  function getRotationCode(iso, slot) {
    const base = getBaseCode(iso, slot);
    const holiday = getHoliday(iso);
    if (holiday && base && cfg.holidayRule.replaces.indexOf(base) !== -1) return 'HOL';
    return base;
  }

  function resolveWho(who) {
    if (!who) return { employeeId: null, slot: null, employee: null };
    if (who.employeeId) {
      const e = getEmployee(who.employeeId);
      return { employeeId: who.employeeId, slot: e ? Number(e.slot) : (who.slot || null), employee: e };
    }
    return { employeeId: null, slot: who.slot ? Number(who.slot) : null, employee: null };
  }

  /* The one function every view relies on. */
  function getScheduleForDate(iso, who) {
    assertConfigured();
    const w = resolveWho(who);
    const base = w.slot ? getBaseCode(iso, w.slot) : null;
    const holiday = getHoliday(iso);
    const rotationCode = w.slot ? getRotationCode(iso, w.slot) : null;
    const exception = getExceptionForDate(iso, w.employeeId);
    let code = rotationCode;
    let source = 'rotation';
    if (holiday && rotationCode === 'HOL' && base !== 'HOL') source = 'holiday';
    if (exception && exception.code) { code = exception.code; source = 'exception'; }
    const meta = codeMeta(code);
    return {
      date: iso,
      weekday: weekday(iso),
      weekdayName: WEEKDAYS[weekday(iso)],
      employeeId: w.employeeId,
      employee: w.employee,
      slot: w.slot,
      cycleIndex: getCycleIndex(iso),
      base: base,
      rotationCode: rotationCode,
      holiday: holiday,
      exception: exception,
      code: code,
      source: source,
      meta: meta,
      tours: meta.tours || [],
      isWork: meta.kind === 'work',
      isOff: meta.kind === 'off' || meta.kind === 'leave',
      isDouble: !!meta.double,
    };
  }
  function getShiftForDate(iso, who) { return getScheduleForDate(iso, who).code; }

  function getRange(startISO, days, who) {
    const out = [];
    for (let i = 0; i < days; i++) out.push(getScheduleForDate(addDays(startISO, i), who));
    return out;
  }

  /* Next date (strictly after `iso` unless includeStart) matching predicate. */
  function findNext(iso, who, predicate, opts) {
    const includeStart = opts && opts.includeStart;
    const limit = (opts && opts.limit) || 120;
    for (let i = includeStart ? 0 : 1; i <= limit; i++) {
      const s = getScheduleForDate(addDays(iso, i), who);
      if (predicate(s)) return s;
    }
    return null;
  }

  /* ------------------------------------------------------------------
   * Day summary + coverage
   * ---------------------------------------------------------------- */

  /* entries: [{ employeeId, code }] — counts people per tour. Vacant
   * positions on the roster are listed but never counted.
   * Returns { counts: {A,B,C,R}, issues: [string], required: {...} }   */
  function checkCoverage(iso, entries) {
    assertConfigured();
    const counts = { A: 0, B: 0, C: 0, R: 0 };
    entries.forEach(function (en) {
      const emp = en.employeeId ? cfg.rosterIndex[en.employeeId] : null;
      if (emp && emp.vacant) return;
      tours(en.code).forEach(function (t) { if (counts[t] !== undefined) counts[t] += 1; });
    });
    const issues = [];
    const required = {};
    const weekendOrHoliday = isWeekend(iso) || !!getHoliday(iso);
    ['A', 'B', 'C', 'R'].forEach(function (t) {
      const rule = cfg.coverage[t];
      if (!rule || !rule.min) return;
      let applies = false;
      if (rule.when === 'always') applies = true;
      else if (rule.when === 'weekend-or-holiday') applies = weekendOrHoliday;
      else if (rule.when === 'weekend') applies = isWeekend(iso);
      else if (rule.when === 'holiday') applies = !!getHoliday(iso);
      else if (rule.when === 'weekday') applies = !weekendOrHoliday;
      if (!applies) return;
      required[t] = rule.min;
      if (counts[t] < rule.min) {
        issues.push(formatShort(iso) + ': only ' + counts[t] + ' on ' + t + ' tour (needs ' + rule.min + ')');
      }
    });
    return { counts: counts, issues: issues, required: required, weekendOrHoliday: weekendOrHoliday };
  }

  /* Everybody's schedule for one day, optionally with overrides applied
   * ({ employeeId: code }) — used to preview a swap before approval.    */
  function getDaySummary(iso, overrides) {
    const roster = activeRoster();
    const entries = roster.map(function (e) {
      const s = getScheduleForDate(iso, { employeeId: e.id });
      const over = overrides && Object.prototype.hasOwnProperty.call(overrides, e.id);
      const code = over ? overrides[e.id] : s.code;
      return {
        employeeId: e.id,
        name: e.name,
        slot: Number(e.slot),
        vacant: !!e.vacant,
        scheduled: s,
        code: code,
        overridden: over && overrides[e.id] !== s.code,
        meta: codeMeta(code),
      };
    });
    const byTour = { A: [], B: [], C: [], R: [], off: [], other: [] };
    entries.forEach(function (en) {
      const t = tours(en.code);
      if (t.length) t.forEach(function (x) { if (byTour[x]) byTour[x].push(en); });
      else if (isOff(en.code)) byTour.off.push(en);
      else byTour.other.push(en);
    });
    const coverage = checkCoverage(iso, entries.map(function (en) { return { employeeId: en.employeeId, code: en.code }; }));
    return { date: iso, holiday: getHoliday(iso), entries: entries, byTour: byTour, coverage: coverage };
  }

  /* Consecutive work-day streak that includes `iso` for an employee,
   * with optional overrides { 'YYYY-MM-DD': code } for that person.     */
  function workStreakAround(iso, employeeId, dateOverrides) {
    function codeOn(d) {
      if (dateOverrides && Object.prototype.hasOwnProperty.call(dateOverrides, d)) return dateOverrides[d];
      return getScheduleForDate(d, { employeeId: employeeId }).code;
    }
    if (!isWork(codeOn(iso))) return 0;
    let n = 1;
    let d = addDays(iso, -1);
    while (isWork(codeOn(d)) && n < 60) { n++; d = addDays(d, -1); }
    d = addDays(iso, 1);
    while (isWork(codeOn(d)) && n < 60) { n++; d = addDays(d, 1); }
    return n;
  }

  return {
    WEEKDAYS: WEEKDAYS, WEEKDAYS_LONG: WEEKDAYS_LONG, MONTHS: MONTHS, MONTHS_SHORT: MONTHS_SHORT,
    configure: configure, getConfig: getConfig,
    todayISO: todayISO, toISO: toISO, parseISO: parseISO, isValidISO: isValidISO,
    addDays: addDays, daysBetween: daysBetween, weekday: weekday, isWeekend: isWeekend,
    isLeapYear: isLeapYear, daysInMonth: daysInMonth, monthLabel: monthLabel,
    formatLong: formatLong, formatShort: formatShort, formatDayMonth: formatDayMonth,
    getMonthMatrix: getMonthMatrix,
    codeMeta: codeMeta, tours: tours, isWork: isWork, isOff: isOff, isDouble: isDouble, allCodes: allCodes,
    getEmployee: getEmployee, activeRoster: activeRoster, employeesInSlot: employeesInSlot, slotNumbers: slotNumbers,
    getCycleIndex: getCycleIndex, getSlotForDate: getSlotForDate, getBaseCode: getBaseCode,
    getRotationCode: getRotationCode, getHoliday: getHoliday, getExceptionForDate: getExceptionForDate,
    getScheduleForDate: getScheduleForDate, getShiftForDate: getShiftForDate,
    getRange: getRange, findNext: findNext,
    checkCoverage: checkCoverage, getDaySummary: getDaySummary, workStreakAround: workStreakAround,
  };
})();

if (typeof module !== 'undefined' && module.exports) { module.exports = Engine; }

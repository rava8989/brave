/* Run:  node schedule/tests/engine.test.js
 * Plain-node tests for the schedule engine and swap validation.
 * No test framework needed. */
'use strict';
const path = require('path');
const { SCHEDULE_CONFIG, ROSTER } = require(path.join(__dirname, '..', 'js', 'config.js'));
const Engine = require(path.join(__dirname, '..', 'js', 'engine.js'));
const Swaps = require(path.join(__dirname, '..', 'js', 'swaps.js'));

let passed = 0, failed = 0;
function eq(actual, expected, label) {
  if (JSON.stringify(actual) === JSON.stringify(expected)) { passed++; return; }
  failed++;
  console.error('FAIL ' + label + '\n   expected ' + JSON.stringify(expected) + '\n   actual   ' + JSON.stringify(actual));
}
function ok(cond, label) { eq(!!cond, true, label); }

function freshConfig(overrides) {
  return Object.assign({
    rotation: SCHEDULE_CONFIG.rotation,
    holidays: SCHEDULE_CONFIG.holidays,
    holidayRule: SCHEDULE_CONFIG.holidayRule,
    coverage: SCHEDULE_CONFIG.coverage,
    codes: SCHEDULE_CONFIG.codes,
    primaryCodes: SCHEDULE_CONFIG.primaryCodes,
    exceptions: {},
    roster: ROSTER,
    groups: SCHEDULE_CONFIG.groups,
  }, overrides || {});
}
Engine.configure(freshConfig());

/* ---------- date math ---------- */
eq(Engine.isLeapYear(2028), true, 'leap 2028');
eq(Engine.isLeapYear(2026), false, 'not leap 2026');
eq(Engine.isLeapYear(2100), false, 'not leap 2100');
eq(Engine.isLeapYear(2000), true, 'leap 2000');
eq(Engine.daysInMonth(2028, 2), 29, 'Feb 2028 has 29 days');
eq(Engine.daysInMonth(2026, 2), 28, 'Feb 2026 has 28 days');
eq(Engine.addDays('2028-02-28', 1), '2028-02-29', 'leap-day add');
eq(Engine.addDays('2026-12-31', 1), '2027-01-01', 'year rollover');
eq(Engine.addDays('2026-01-01', -1), '2025-12-31', 'year rollback');
eq(Engine.daysBetween('2026-01-01', '2026-10-01'), 273, 'days Jan1→Oct1 2026');
eq(Engine.weekday('2026-01-01'), 4, '2026-01-01 is Thursday');
eq(Engine.weekday('2026-09-06'), 0, '2026-09-06 is Sunday');
eq(Engine.weekday('2026-09-05'), 6, '2026-09-05 is Saturday');
eq(Engine.isValidISO('2026-02-29'), false, 'invalid Feb 29 2026');
Engine.configure(freshConfig());
eq(!!Engine.getHoliday('2026-11-03'), false, 'Election Day is not a holiday');
eq(!!Engine.getHoliday('2026-11-27'), true, 'day after Thanksgiving is a holiday');
eq(Engine.isValidISO('2028-02-29'), true, 'valid Feb 29 2028');
eq(Engine.isValidISO('2026-13-01'), false, 'invalid month');
eq(Engine.formatLong('2026-09-10'), 'Thu, Sep 10, 2026', 'formatLong');
const m = Engine.getMonthMatrix(2026, 9);
eq(m[0][0].iso, '2026-08-30', 'Sep 2026 grid starts Sun Aug 30');
eq(m[0][2].iso, '2026-09-01', 'Sep 1 2026 in Tue column');
eq(m[m.length - 1][6].iso, '2026-10-03', 'Sep 2026 grid ends Sat Oct 3');
eq(m.length, 5, 'Sep 2026 has 5 rows');
eq(Engine.getMonthMatrix(2026, 8).length, 6, 'Aug 2026 has 6 rows');
eq(Engine.getMonthMatrix(2026, 2).length, 4, 'Feb 2026 has 4 rows (starts Sunday)');

/* ---------- rotation structure ---------- */
const slots = SCHEDULE_CONFIG.rotation.slots;
for (let s = 1; s <= 6; s++) eq(slots[s].length, 42, 'slot ' + s + ' has 42 entries');
for (let s = 2; s <= 6; s++) {
  for (let i = 0; i < 42; i++) {
    if (slots[s][i] !== slots[1][(i + 7 * (s - 1)) % 42]) {
      failed++; console.error('FAIL slot ' + s + ' index ' + i + ' is not slot 1 shifted by ' + (7 * (s - 1)) + ' days'); break;
    }
  }
}
passed++;
eq(Engine.getCycleIndex('2026-01-01'), 0, 'cycle index 0 on reference date');
eq(Engine.getCycleIndex('2026-02-12'), 0, 'cycle repeats after 42 days');
eq(Engine.getCycleIndex('2025-12-31'), 41, 'day before reference wraps to 41');
eq(Engine.getBaseCode('2026-01-01', 1), 'R', 'slot 1 on 2026-01-01 = R');

/* ---------- cells read from the posted schedules / swap sheets ---------- */
function code(iso, id) { return Engine.getScheduleForDate(iso, { employeeId: id }).code; }
// Rakhmanov (slot 5) — September swap sheet "present schedule"
eq(code('2026-09-04', 'rakhmanov-r'), 'A',   'Rakhmanov Sep 4 = A');
eq(code('2026-09-05', 'rakhmanov-r'), 'A',   'Rakhmanov Sep 5 = A');
eq(code('2026-09-06', 'rakhmanov-r'), 'A',   'Rakhmanov Sep 6 = A');
eq(code('2026-09-07', 'rakhmanov-r'), 'A',   'Rakhmanov Sep 7 (Labor Day) stays A');
eq(code('2026-09-08', 'rakhmanov-r'), 'A/C', 'Rakhmanov Sep 8 = A/C');
eq(code('2026-09-09', 'rakhmanov-r'), 'A',   'Rakhmanov Sep 9 = A');
eq(code('2026-09-10', 'rakhmanov-r'), 'MDO', 'Rakhmanov Sep 10 = MDO');
eq(code('2026-09-25', 'rakhmanov-r'), 'R',   'Rakhmanov Sep 25 = R (R block is Mon–Fri)');
eq(code('2026-09-28', 'rakhmanov-r'), 'RDO', 'Rakhmanov Sep 28 = RDO');
eq(code('2026-09-29', 'rakhmanov-r'), 'MDO', 'Rakhmanov Sep 29 = MDO');
eq(code('2026-09-30', 'rakhmanov-r'), 'C',   'Rakhmanov Sep 30 = C');
eq(code('2026-10-01', 'rakhmanov-r'), 'C/A', 'Rakhmanov Oct 1 = C/A');
eq(code('2026-10-05', 'rakhmanov-r'), 'C',   'Rakhmanov Oct 5 = C');
eq(code('2026-10-06', 'rakhmanov-r'), 'RDO', 'Rakhmanov Oct 6 = RDO');
// Bouaziz (slot 4) — September swap sheet
eq(code('2026-09-02', 'bouaziz-a'), 'RDO', 'Bouaziz Sep 2 = RDO');
eq(code('2026-09-04', 'bouaziz-a'), 'B',   'Bouaziz Sep 4 = B');
eq(code('2026-09-06', 'bouaziz-a'), 'B',   'Bouaziz Sep 6 = B');
eq(code('2026-09-09', 'bouaziz-a'), 'RDO', 'Bouaziz Sep 9 = RDO');
eq(code('2026-09-10', 'bouaziz-a'), 'RDO', 'Bouaziz Sep 10 = RDO');
// Kingston (slot 1) — October posted schedule
eq(code('2026-10-01', 'kingston-j'), 'RDO', 'Kingston Oct 1 = RDO');
eq(code('2026-10-02', 'kingston-j'), 'A',   'Kingston Oct 2 = A');
eq(code('2026-10-06', 'kingston-j'), 'A/C', 'Kingston Oct 6 = A/C');
eq(code('2026-10-08', 'kingston-j'), 'MDO', 'Kingston Oct 8 = MDO');
eq(code('2026-10-12', 'kingston-j'), 'HOL', 'Kingston Oct 12 = HOL');
eq(code('2026-10-19', 'kingston-j'), 'R',   'Kingston Oct 19 = R');
eq(code('2026-10-27', 'kingston-j'), 'MDO', 'Kingston Oct 27 = MDO');
eq(code('2026-10-29', 'kingston-j'), 'C/A', 'Kingston Oct 29 = C/A');
eq(code('2026-09-07', 'kingston-j'), 'HOL', 'Kingston Labor Day = HOL (R day)');
// Holiday on a B tour: the crew whose B block runs through the weekend works Mon/Tue/Thu/Fri holidays
eq(code('2026-09-07', 'bouaziz-a'), 'B',    'Bouaziz (Fri–Tue B block) works Labor Day');
eq(code('2026-10-12', 'rakhmanov-r'), 'B',  'Rakhmanov (Fri–Tue B block) works Columbus Day');
eq(code('2026-10-12', 'kingston-j'), 'HOL', 'Kingston (Mon–Fri B block) gets Columbus Day off');
// Wednesday holiday: the Mon–Fri crew works it (slot 1 is on its Mon–Fri B block on Wed Feb 4 2026)
Engine.configure(freshConfig({ holidays: [{ date: '2026-02-04', name: 'Test Wednesday' }] }));
eq(code('2026-02-04', 'kingston-j'), 'B',   'Mon–Fri B crew works a Wednesday holiday');
eq(code('2026-02-04', 'ahmed-n'), 'HOL',    'static B crew still gets a Wednesday holiday off');
Engine.configure(freshConfig());
// Joseph (slot 2) — October posted schedule starts MDO RDO RDO RDO B ...
eq(code('2026-10-01', 'joseph-g'), 'MDO', 'Joseph Oct 1 = MDO');
eq(code('2026-10-05', 'joseph-g'), 'B',   'Joseph Oct 5 = B');
eq(code('2026-10-22', 'joseph-g'), 'C/A', 'Joseph Oct 22 = C/A');
// Same-slot partners are identical by rotation
eq(code('2026-10-01', 'handel-j'), code('2026-10-01', 'rakhmanov-r'), 'Handel = Rakhmanov (slot 5)');
// Static groups (posted sheet): B shift Mon–Fri, A shift Mon–Fri, RDO weekends, HOL on Labor Day
eq(code('2026-09-01', 'ahmed-n'), 'B',   'Ahmed (B shift) Tue Sep 1 = B');
eq(code('2026-09-05', 'ahmed-n'), 'RDO', 'Ahmed Sat Sep 5 = RDO');
eq(code('2026-09-07', 'ahmed-n'), 'HOL', 'Ahmed Labor Day = HOL');
eq(code('2026-09-01', 'adjepong-n'), 'A',   'Adjepong (A shift) Tue Sep 1 = A');
eq(code('2026-09-06', 'adjepong-n'), 'RDO', 'Adjepong Sun Sep 6 = RDO');
eq(code('2026-09-07', 'adjepong-n'), 'HOL', 'Adjepong Labor Day = HOL');
eq(Engine.shiftCode(Engine.getEmployee('rakhmanov-r')), 'LGA_6RR9.5', 'shift code for a rotating slot');
eq(Engine.shiftCode(Engine.getEmployee('ahmed-n')), 'LGA_BSS1.1', 'shift code for a static group');
eq(Engine.rosterSorted().map(function (e) { return e.id; }).slice(0, 2), ['ahmed-n', 'atolagbe-a'], 'sheet order starts with the B shift group');
eq(Engine.rosterSorted().map(function (e) { return e.id; }).slice(-1), ['segovia-e'], 'sheet order ends with the A shift group');
eq(Engine.employeesInSlot(1).map(function (e) { return e.id; }), ['kingston-j', 'rodriguez-r'], 'employeesInSlot ignores static groups');

/* ---------- 24/7 coverage holds on every day of the cycle (all 12 positions filled) ---------- */
Engine.configure(freshConfig({ roster: ROSTER.map(function (e) { return Object.assign({}, e, { vacant: false }); }) }));
let covOk = true;
for (let i = 0; i < 42; i++) {
  const iso = Engine.addDays('2026-01-05', i);       // a Monday, 6 weeks
  const sum = Engine.getDaySummary(iso);
  if (sum.coverage.issues.length) { covOk = false; console.error('coverage issue', iso, sum.coverage.issues, sum.coverage.counts); }
}
ok(covOk, 'rotation keeps 2 on A, 2 on C every day and 2 on B every weekend');
Engine.configure(freshConfig());
// The vacant slot-2 position must not count: on 2026-09-10 slot 2 is C/A, so A and C are really 1 of 2
const vac = Engine.getDaySummary('2026-09-10');
eq([vac.coverage.counts.A, vac.coverage.counts.C, vac.coverage.issues.length], [9, 1, 1], 'vacant position is not counted toward coverage (A = 8 static + Joseph)');

/* ---------- holidays / exceptions / source ---------- */
let s = Engine.getScheduleForDate('2026-10-12', { employeeId: 'kingston-j' });
eq([s.base, s.code, s.source, !!s.holiday], ['B', 'HOL', 'holiday', true], 'holiday replaces B and is flagged');
s = Engine.getScheduleForDate('2026-10-12', { employeeId: 'rakhmanov-r' });
eq([s.base, s.code, s.source], ['B', 'B', 'rotation'], 'weekend B crew keeps working on the holiday');
s = Engine.getScheduleForDate('2026-10-12', { employeeId: 'huang-d' });
eq([s.base, s.code, s.source], ['A', 'A', 'rotation'], 'A tour works on the holiday');
Engine.configure(freshConfig({ exceptions: { '2026-09-25': { 'rakhmanov-r': { code: 'HOL', note: 'swap' } } } }));
s = Engine.getScheduleForDate('2026-09-25', { employeeId: 'rakhmanov-r' });
eq([s.base, s.code, s.source], ['R', 'HOL', 'exception'], 'exception overrides rotation');
eq(code('2026-09-25', 'handel-j'), 'R', 'exception is per employee, not per slot');
Engine.configure(freshConfig());

/* ---------- findNext ---------- */
const nextRdo = Engine.findNext('2026-09-10', { employeeId: 'rakhmanov-r' }, function (x) { return x.code === 'RDO'; });
eq(nextRdo.date, '2026-09-11', 'next RDO after Sep 10 is Sep 11');
const nextWork = Engine.findNext('2026-09-10', { employeeId: 'rakhmanov-r' }, function (x) { return x.isWork; });
eq(nextWork.date, '2026-09-14', 'next work day after Sep 10 MDO is Mon Sep 14 (B)');

/* ---------- slot view (no employee) ---------- */
eq(Engine.getScheduleForDate('2026-10-01', { slot: 5 }).code, 'C/A', 'slot view works without employee');

/* ---------- swap validation ---------- */
const state = { roster: ROSTER, exceptions: {}, requests: [] };
const swap = Swaps.buildRequest({
  type: 'swap', createdBy: 'rakhmanov-r', createdByName: 'Rakhmanov, Ravshan',
  changes: Swaps.swapChanges('rakhmanov-r', 'bouaziz-a', ['2026-09-18']),   // Fri: Rakh B, Bouaziz RDO
  reason: 'test',
});
let v = Swaps.validate(swap, { state: state, today: '2026-09-10', role: 'worker' });
eq(v.errors, [], 'plain swap has no errors');
eq(v.warnings.length, 0, 'plain swap between B and RDO on a weekday has no warnings');

// Taking a night-tour person off a Saturday without cover → coverage warning
const off = Swaps.buildRequest({
  type: 'timeoff', createdBy: 'rakhmanov-r', createdByName: 'Rakhmanov, Ravshan',
  changes: [{ employeeId: 'rakhmanov-r', date: '2026-09-05', from: 'A', to: 'VAC' }],
  reason: 'test',
});
v = Swaps.validate(off, { state: state, today: '2026-09-01', role: 'worker' });
ok(v.warnings.some(function (w) { return /A tour would have 1/.test(w); }), 'coverage warning when A drops to 1');
// A pre-existing shortage (the vacancy) is reported as a note, not blamed on the request
const unrelated = Swaps.buildRequest({
  type: 'timeoff', createdBy: 'rakhmanov-r', createdByName: 'R',
  changes: [{ employeeId: 'huang-d', date: '2026-09-10', from: 'B', to: 'VAC' }], reason: '',
});
v = Swaps.validate(unrelated, { state: state, today: '2026-09-01', role: 'worker' });
ok(!v.warnings.some(function (w) { return /A tour/.test(w); }) && v.notes.some(function (n) { return /already short/.test(n); }), 'existing shortage is a note, not a warning');

// Past date is an error for workers, allowed for supervisor
v = Swaps.validate(off, { state: state, today: '2026-09-20', role: 'worker' });
ok(v.errors.some(function (e) { return /past/.test(e); }), 'worker cannot change past dates');
v = Swaps.validate(off, { state: state, today: '2026-09-20', role: 'supervisor' });
ok(!v.errors.some(function (e) { return /past/.test(e); }), 'supervisor may change past dates');

// Stale "from" code is flagged
const stale = Swaps.buildRequest({
  type: 'timeoff', createdBy: 'rakhmanov-r', createdByName: 'R',
  changes: [{ employeeId: 'rakhmanov-r', date: '2026-09-18', from: 'A', to: 'VAC' }], reason: '',
});
v = Swaps.validate(stale, { state: state, today: '2026-09-01', role: 'worker' });
ok(v.warnings.some(function (w) { return /now B/.test(w); }), 'stale from-code produces a warning');

// Overlap with a pending request
const state2 = { roster: ROSTER, exceptions: {}, requests: [Object.assign({}, swap, { status: 'pending' })] };
v = Swaps.validate(stale, { state: state2, today: '2026-09-01', role: 'worker' });
ok(v.warnings.some(function (w) { return /pending request/.test(w); }), 'overlap with pending request is flagged');

// Approval writes exceptions and restores rotation when swapped back
const draft = { roster: ROSTER, exceptions: {}, requests: [Object.assign({}, swap, { status: 'pending' })] };
Swaps.applyDecision(draft, swap.id, { status: 'approved', by: 'Supervisor', at: 'now' });
eq(draft.exceptions['2026-09-18']['rakhmanov-r'].code, 'RDO', 'approval sets Rakhmanov Sep 18 → RDO');
eq(draft.exceptions['2026-09-18']['bouaziz-a'].code, 'B', 'approval sets Bouaziz Sep 18 → B');
eq(draft.requests[0].status, 'approved', 'request marked approved');
Engine.configure(freshConfig({ exceptions: draft.exceptions }));
eq(code('2026-09-18', 'rakhmanov-r'), 'RDO', 'engine reflects approved swap');
const back = Swaps.buildRequest({
  type: 'swap', createdBy: 'rakhmanov-r', createdByName: 'R',
  changes: Swaps.swapChanges('rakhmanov-r', 'bouaziz-a', ['2026-09-18']), reason: 'undo',
});
draft.requests.push(Object.assign({}, back, { status: 'pending' }));
Swaps.applyDecision(draft, back.id, { status: 'approved', by: 'Supervisor', at: 'now' });
eq(draft.exceptions['2026-09-18'], undefined, 'swapping back removes the exception entirely');
// Static-group override: back to the weekly pattern removes the exception too
Swaps.setOverride(draft, '2026-09-15', 'ahmed-n', 'VAC');
eq(draft.exceptions['2026-09-15']['ahmed-n'].code, 'VAC', 'static-group override stored');
Swaps.setOverride(draft, '2026-09-15', 'ahmed-n', 'B');
eq(draft.exceptions['2026-09-15'], undefined, 'static-group override back to regular B is removed');
Engine.configure(freshConfig());

console.log((failed ? 'FAILED ' + failed + ' / ' : 'PASSED ') + (passed + failed) + ' checks');
process.exit(failed ? 1 : 0);

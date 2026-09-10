/* =====================================================================
 *  ui-forms.js — paper-accurate renderers
 *    Forms.swapSheetHTML(request)   the "TIME OFF OR SHIFT CHANGE REQUEST"
 *                                   form, filled in from a request
 *    Forms.laborScheduleHTML(y, m)  the posted "MONTHLY LABOR SCHEDULE" grid
 *  Both are plain black-on-white HTML so they print like the originals.
 * ===================================================================== */
const Forms = (function () {
  'use strict';
  const esc = UI.esc;

  function ordinal(n) {
    const r = n % 100;
    if (r >= 11 && r <= 13) return n + 'th';
    return n + (['th', 'st', 'nd', 'rd'][n % 10] || 'th');
  }
  function usDate(iso) {
    if (!iso) return '';
    const d = iso.length > 10 ? Engine.toISO(new Date(iso)) : iso;
    return Number(d.slice(5, 7)) + '/' + Number(d.slice(8, 10)) + '/' + d.slice(0, 4);
  }
  function org() { return SCHEDULE_CONFIG.org || {}; }

  function monthForRequest(req) {
    const dates = Swaps.affectedDates(req);
    const d = dates[0] || Engine.todayISO();
    return { y: Number(d.slice(0, 4)), m: Number(d.slice(5, 7)) };
  }

  /* One of the two 1–31 grids. Days after the month get extra columns
   * labelled 1st, 2nd… under a "→ Month" header, as people do by hand.  */
  function gridHTML(req, mode) {
    const ym = monthForRequest(req);
    const dim = Engine.daysInMonth(ym.y, ym.m);
    const prefix = ym.y + '-' + (ym.m < 10 ? '0' : '') + ym.m + '-';
    const lastISO = prefix + dim;
    const dates = Swaps.affectedDates(req);
    const cols = [];
    for (let d = 1; d <= dim; d++) cols.push({ iso: prefix + (d < 10 ? '0' : '') + d, label: String(d) });
    const extra = dates.filter(function (d) { return d > lastISO; }).map(function (d) {
      return { iso: d, label: ordinal(Number(d.slice(8, 10))), month: Engine.MONTHS[Number(d.slice(5, 7)) - 1], nxt: true };
    });
    const all = cols.concat(extra);
    let h = '<table class="osheet-grid"><thead>';
    if (extra.length) {
      h += '<tr class="nxt-row"><th class="nm"></th><th colspan="' + dim + '"></th><th colspan="' + extra.length + '" class="nxt">→ ' + esc(extra[0].month) + '</th></tr>';
    }
    h += '<tr><th class="nm">NAME</th>' + all.map(function (c) { return '<th' + (c.nxt ? ' class="nxt"' : '') + '>' + esc(c.label) + '</th>'; }).join('') + '</tr></thead><tbody>';
    const emps = (req.employees || []).slice();
    emps.forEach(function (id) {
      h += '<tr><td class="nm">' + esc(UI.empName(id)) + '</td>';
      all.forEach(function (c) {
        const ch = (req.changes || []).filter(function (x) { return x.employeeId === id && x.date === c.iso; })[0];
        let v = '';
        if (ch) v = mode === 'present' ? (ch.from || Engine.getScheduleForDate(c.iso, { employeeId: id }).code || '') : ch.to;
        h += '<td' + (c.nxt ? ' class="nxt"' : '') + '>' + esc(v) + '</td>';
      });
      h += '</tr>';
    });
    for (let i = emps.length; i < 2; i++) h += '<tr><td class="nm">&nbsp;</td>' + all.map(function () { return '<td></td>'; }).join('') + '</tr>';
    h += '</tbody></table>';
    return h;
  }

  function swapSheetHTML(req) {
    const ym = monthForRequest(req);
    const submitted = usDate(req.createdAt || Engine.todayISO());
    const decided = req.decidedAt ? usDate(req.decidedAt) : '';
    const from = req.createdByName || UI.empName((req.employees || [])[0]);
    const approved = req.status === 'approved', rejected = req.status === 'rejected';
    const o = org();
    return '<div class="osheet">' +
      '<div class="os-title">TIME OFF OR SHIFT CHANGE REQUEST</div>' +
      '<div class="os-sub">THIS REQUEST IS NOT APPROVED UNTIL YOU RECEIVE A SIGNED COPY OF THIS FORM</div>' +
      '<div class="os-row"><span>TO: SUPERVISOR</span><span>DATE: <u class="os-fill os-short">' + esc(submitted) + '</u></span></div>' +
      '<div class="os-row"><span>FROM: <u class="os-fill os-wide">' + esc(from) + '</u></span></div>' +
      '<div class="os-row"><span>MONTH: <u class="os-fill os-wide">' + esc(Engine.MONTHS[ym.m - 1]) + '</u></span></div>' +
      '<div class="os-gtitle">MY PRESENT SCHEDULE IS</div>' + gridHTML(req, 'present') +
      '<div class="os-gtitle">TIME OFF OR CHANGE PERIOD REQUESTED</div>' + gridHTML(req, 'requested') +
      '<div class="os-line">ALL EMPLOYEES MUST SIGN <u class="os-fill os-sig">&nbsp;</u></div>' +
      '<div class="os-line">REASON FOR REQUEST <u class="os-fill os-sig">' + esc(req.reason || '') + '&nbsp;</u></div>' +
      '<div class="os-line">LIST PRESENT TIME BALANCE <span class="os-gap">COMP. HOURS <u class="os-fill os-short">&nbsp;</u></span> <span class="os-gap">VAC. HOURS <u class="os-fill os-short">&nbsp;</u></span></div>' +
      '<div class="os-line">YOUR REQUEST HAS BEEN <span class="os-gap">APPROVED <u class="os-fill os-short">' + (approved ? '✔ ' + esc(decided) : '&nbsp;') + '</u></span> <span class="os-gap">REJECTED <u class="os-fill os-short">' + (rejected ? '✔ ' + esc(decided) : '&nbsp;') + '</u></span></div>' +
      '<div class="os-line">REASON FOR REJECTION <u class="os-fill os-sig">' + (rejected ? esc(req.decisionNote || '') : '') + '&nbsp;</u></div>' +
      '<div class="os-sigs"><div><u class="os-fill os-wide">' + ((approved || rejected) ? esc(req.decidedBy || '') : '') + '&nbsp;</u><span>SUPERVISOR</span></div>' +
      '<div><u class="os-fill os-short">' + esc(decided) + '&nbsp;</u><span>DATE</span></div></div>' +
      '<div class="os-line">ENTERED INTO COMPUTER <u class="os-fill os-short">' + (req.enteredInComputer ? '✔' : '&nbsp;') + '</u></div>' +
      '<div class="os-foot">' + (o.formRevision ? '<span class="os-rev">' + esc(o.formRevision) + '</span>' : '') +
      '** ALL REQUESTS WILL ONLY BE APPROVED AND SCHEDULED ON <u>TUESDAY</u> &amp; <u>THURSDAY</u> **<br>IT IS YOUR RESPONSIBILITY TO FOLLOW UP ON THIS REQUEST</div>' +
      '</div>';
  }

  /* The posted monthly grid: one boxed block per group (B shift, rotating,
   * A shift), NAME | SHIFT | SLOT | 1..N, dark RDO cells, black for the
   * regular code and red for anything else, the printed legend, page no. */
  function laborScheduleHTML(y, m) {
    const dim = Engine.daysInMonth(y, m);
    const prefix = y + '-' + (m < 10 ? '0' : '') + m + '-';
    const o = org();
    const days = [];
    for (let d = 1; d <= dim; d++) days.push(prefix + (d < 10 ? '0' : '') + d);
    let h = '<div class="olabor"><div class="ol-head"><div class="ol-brand"><b>' + esc(o.nameBold || o.name || '') + '</b> ' + esc(o.nameRest || '') + '</div>' +
      '<div class="ol-title">MONTHLY LABOR SCHEDULE<span>Month of ' + esc(Engine.monthLabel(y, m)) + '</span></div>' +
      '<div></div></div>';
    h += '<table class="ol-grid"><thead><tr><th class="nm">NAME</th><th class="sh">SHIFT</th><th class="sl">SLOT</th>';
    days.forEach(function (iso, i) {
      h += '<th class="dy' + (Engine.isWeekend(iso) ? ' we' : '') + '">' + (i + 1) + '<small>' + Engine.WEEKDAYS[Engine.weekday(iso)] + '</small></th>';
    });
    h += '</tr></thead>';
    const roster = Engine.rosterSorted();
    Engine.groupOrder().forEach(function (g, gi) {
      const people = roster.filter(function (e) { return Engine.groupCodeOf(e) === g; });
      if (!people.length) return;
      if (gi > 0) h += '<tbody class="gap"><tr><td colspan="' + (days.length + 3) + '"></td></tr></tbody>';
      h += '<tbody class="grp">';
      people.forEach(function (e) {
        h += '<tr><td class="nm">' + esc(e.name) + '</td><td class="sh">' + esc(Engine.shiftCode(e)) + '</td><td class="sl">' + esc(e.slot) + '</td>';
        days.forEach(function (iso) {
          const s = Engine.getScheduleForDate(iso, { employeeId: e.id });
          const cls = [];
          if (s.code === 'RDO') cls.push('off');
          else if (s.source === 'exception' || ['A', 'B', 'C', 'R'].indexOf(s.code) === -1) cls.push('chg');
          h += '<td class="' + cls.join(' ') + '">' + esc(s.code || '') + '</td>';
        });
        h += '</tr>';
      });
      h += '</tbody>';
    });
    h += '</table>';
    const legend = SCHEDULE_CONFIG.printLegend;
    if (legend && legend.items) {
      const cols = legend.columns || 7;
      const rows = Math.ceil(legend.items.length / cols);
      h += '<div class="ol-legend"><b>LEGEND:</b><table class="ol-legend-t"><tbody>';
      for (let r = 0; r < rows; r++) {
        h += '<tr>';
        for (let c = 0; c < cols; c++) {
          const it = legend.items[c * rows + r];
          h += '<td>' + (it ? esc(it[0]) + ' - ' + esc(it[1]) : '') + '</td>';
        }
        h += '</tr>';
      }
      h += '</tbody></table></div>';
    }
    h += '<div class="ol-foot">1 / 1</div></div>';
    return h;
  }

  return { swapSheetHTML: swapSheetHTML, laborScheduleHTML: laborScheduleHTML, monthForRequest: monthForRequest, usDate: usDate };
})();

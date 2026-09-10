/* =====================================================================
 *  config.js — ALL EDITABLE SCHEDULE DATA LIVES IN THIS FILE
 * =====================================================================
 *
 *  Nothing in here touches the UI. Change values, save, reload.
 *
 *  Sections
 *    1. APP_CONFIG        — app name, optional sync server, photo limits
 *    2. SCHEDULE_CONFIG   — rotation patterns, holidays, codes, coverage
 *    3. ROSTER            — employees and the slot each one belongs to
 *
 *  HOW THE ROTATION IS MODELLED
 *  ----------------------------
 *  The rotating group (LGA_6RR9) is a six-week, six-slot rotation.
 *  Every slot works the same 42-day cycle; each slot is simply one week
 *  (7 days) further along than the slot before it.
 *
 *      code for slot S on date D  =  slots[S][ daysSince(referenceDate, D) mod 42 ]
 *
 *  The reference date is 2026-01-01 (a Thursday). Index 0 of every slot
 *  array is what that slot works on 2026-01-01. Because the reference
 *  date is a Thursday, each 7-entry row below reads
 *
 *      Thu  Fri  Sat  Sun  Mon  Tue  Wed
 *
 *  The pattern was transcribed from the posted Sept/Oct 2026 labor
 *  schedules and the Sept swap sheet, then corrected by the crew (the R
 *  block is Mon–Fri). tests/engine.test.js pins the cells that were
 *  legible in the photos. Anything else that looks off can be fixed here
 *  or in the app (Supervisor → Rotation → Edit).
 *
 *  Structure of one cycle (slot 1, starting Thu 2026-01-01):
 *    R  block ..... Mon–Fri relief                    (5 days)
 *    off ........... Sat, Sun, Mon RDO, Tue MDO
 *    C  block ..... Wed–Mon evening, Thu = C/A double (6 days)
 *    off ........... Tue, Wed, Thu RDO
 *    B  block ..... Fri–Tue day tour                 (5 days)
 *    off ........... Wed, Thu RDO
 *    A  block ..... Fri–Wed night, Tue = A/C double  (6 days)
 *    off ........... Thu MDO, Fri–Sun RDO
 *    B  block ..... Mon–Fri day tour                 (5 days)
 *    off ........... Sat, Sun RDO   → back to the R block
 *
 *  The A/C double on the A block and the C/A double on the C block cover
 *  the MDO of the slot three weeks away, which is how 24/7 coverage of
 *  2 on A (night) and 2 on C (evening) is kept with two people per slot.
 * ===================================================================== */

/* ---------------------------------------------------------------------
 * 1. APP CONFIG
 * ------------------------------------------------------------------- */
const APP_CONFIG = {
  appName: 'LGA Rotating Schedule',

  /* Optional shared backend (see worker/README section in README.md).
   * Leave empty to run fully on this device with localStorage.
   * Example: 'https://lga-schedule.YOUR-SUBDOMAIN.workers.dev'            */
  syncUrl: '',

  /* Supervisor PIN used in local (no-sync) mode. Change it in the app
   * under Info → Supervisor tools, or here.                              */
  defaultSupervisorPin: '1234',

  /* How often (seconds) to refresh from the sync server when it is on.   */
  pollIntervalSec: 45,

  /* Photos of signed swap sheets are downscaled before saving.           */
  photoMaxDimension: 1400,
  photoJpegQuality: 0.74,

  /* How many months ahead the calendar / labor grid can navigate.        */
  monthsAhead: 24,
  monthsBack: 12,
};

/* ---------------------------------------------------------------------
 * 2. SCHEDULE CONFIG
 * ------------------------------------------------------------------- */
const SCHEDULE_CONFIG = {
  /* ---- 2a. Rotation --------------------------------------------- */
  rotation: {
    referenceDate: '2026-01-01',   // Thursday. Index 0 of every slot = this date.
    cycleLength: 42,               // six weeks
    slotOffsetDays: 7,             // slot S = slot 1 shifted 7*(S-1) days (informational)

    /* Rows read:  Thu   Fri   Sat   Sun   Mon   Tue   Wed              */
    slots: {
      1: [
        'R',   'R',   'RDO', 'RDO', 'RDO', 'MDO', 'C',
        'C/A', 'C',   'C',   'C',   'C',   'RDO', 'RDO',
        'RDO', 'B',   'B',   'B',   'B',   'B',   'RDO',
        'RDO', 'A',   'A',   'A',   'A',   'A/C', 'A',
        'MDO', 'RDO', 'RDO', 'RDO', 'B',   'B',   'B',
        'B',   'B',   'RDO', 'RDO', 'R',   'R',   'R',
      ],
      2: [
        'C/A', 'C',   'C',   'C',   'C',   'RDO', 'RDO',
        'RDO', 'B',   'B',   'B',   'B',   'B',   'RDO',
        'RDO', 'A',   'A',   'A',   'A',   'A/C', 'A',
        'MDO', 'RDO', 'RDO', 'RDO', 'B',   'B',   'B',
        'B',   'B',   'RDO', 'RDO', 'R',   'R',   'R',
        'R',   'R',   'RDO', 'RDO', 'RDO', 'MDO', 'C',
      ],
      3: [
        'RDO', 'B',   'B',   'B',   'B',   'B',   'RDO',
        'RDO', 'A',   'A',   'A',   'A',   'A/C', 'A',
        'MDO', 'RDO', 'RDO', 'RDO', 'B',   'B',   'B',
        'B',   'B',   'RDO', 'RDO', 'R',   'R',   'R',
        'R',   'R',   'RDO', 'RDO', 'RDO', 'MDO', 'C',
        'C/A', 'C',   'C',   'C',   'C',   'RDO', 'RDO',
      ],
      4: [
        'RDO', 'A',   'A',   'A',   'A',   'A/C', 'A',
        'MDO', 'RDO', 'RDO', 'RDO', 'B',   'B',   'B',
        'B',   'B',   'RDO', 'RDO', 'R',   'R',   'R',
        'R',   'R',   'RDO', 'RDO', 'RDO', 'MDO', 'C',
        'C/A', 'C',   'C',   'C',   'C',   'RDO', 'RDO',
        'RDO', 'B',   'B',   'B',   'B',   'B',   'RDO',
      ],
      5: [
        'MDO', 'RDO', 'RDO', 'RDO', 'B',   'B',   'B',
        'B',   'B',   'RDO', 'RDO', 'R',   'R',   'R',
        'R',   'R',   'RDO', 'RDO', 'RDO', 'MDO', 'C',
        'C/A', 'C',   'C',   'C',   'C',   'RDO', 'RDO',
        'RDO', 'B',   'B',   'B',   'B',   'B',   'RDO',
        'RDO', 'A',   'A',   'A',   'A',   'A/C', 'A',
      ],
      6: [
        'B',   'B',   'RDO', 'RDO', 'R',   'R',   'R',
        'R',   'R',   'RDO', 'RDO', 'RDO', 'MDO', 'C',
        'C/A', 'C',   'C',   'C',   'C',   'RDO', 'RDO',
        'RDO', 'B',   'B',   'B',   'B',   'B',   'RDO',
        'RDO', 'A',   'A',   'A',   'A',   'A/C', 'A',
        'MDO', 'RDO', 'RDO', 'RDO', 'B',   'B',   'B',
      ],
    },
  },

  /* ---- 2b. Holidays ----------------------------------------------
   * `verified: true` = seen on a posted schedule. The rest are the
   * standard observed holidays — confirm against the agency calendar.
   * On a holiday, codes listed in holidayRule.replaces become HOL.
   * A and C tours (and doubles) keep working, exactly as the posted
   * September schedule shows (e.g. Rakhmanov = A on Labor Day).       */
  holidays: [
    { date: '2026-01-01', name: "New Year's Day" },
    { date: '2026-01-19', name: 'Martin Luther King Jr. Day' },
    { date: '2026-02-16', name: "Presidents' Day" },
    { date: '2026-05-25', name: 'Memorial Day' },
    { date: '2026-06-19', name: 'Juneteenth' },
    { date: '2026-07-03', name: 'Independence Day (observed)' },
    { date: '2026-09-07', name: 'Labor Day', verified: true },
    { date: '2026-10-12', name: 'Columbus Day', verified: true },
    { date: '2026-11-11', name: 'Veterans Day' },
    { date: '2026-11-26', name: 'Thanksgiving Day' },
    { date: '2026-11-27', name: 'Day after Thanksgiving', verified: true },
    { date: '2026-12-25', name: 'Christmas Day' },
    { date: '2027-01-01', name: "New Year's Day" },
    { date: '2027-01-18', name: 'Martin Luther King Jr. Day' },
    { date: '2027-02-15', name: "Presidents' Day" },
    { date: '2027-05-31', name: 'Memorial Day' },
    { date: '2027-06-18', name: 'Juneteenth (observed)' },
    { date: '2027-07-05', name: 'Independence Day (observed)' },
    { date: '2027-09-06', name: 'Labor Day' },
    { date: '2027-10-11', name: 'Columbus Day' },
    { date: '2027-11-11', name: 'Veterans Day' },
    { date: '2027-11-25', name: 'Thanksgiving Day' },
    { date: '2027-11-26', name: 'Day after Thanksgiving', verified: true },
    { date: '2027-12-24', name: 'Christmas Day (observed)' },
  ],

  holidayRule: {
    replaces: ['B', 'R'],          // these rotation codes turn into HOL on a holiday
  },

  /* ---- 2c. Coverage requirements (used by swap conflict checks) --- */
  coverage: {
    A: { min: 2, when: 'always' },
    C: { min: 2, when: 'always' },
    B: { min: 2, when: 'weekend' },   // weekdays: static B group covers. Holidays: the posted schedule shows no B from this group; use 'weekend-or-holiday' to require it
    R: { min: 0, when: 'never' },
    maxConsecutiveWorkDays: 7,     // warn above this
    warnConsecutiveDoubles: true,  // warn if two doubles fall on adjacent days
  },

  /* ---- 2d. Codes -------------------------------------------------
   * kind: 'work' | 'off' | 'leave' | 'other'
   * tours: which tours the code covers (used for coverage counting)
   * Labels follow the legend printed on the Port Authority schedule.  */
  codes: {
    'A':    { label: 'Night tour (A)',              kind: 'work',  tours: ['A'] },
    'B':    { label: 'Day tour (B)',                kind: 'work',  tours: ['B'] },
    'C':    { label: 'Evening tour (C)',            kind: 'work',  tours: ['C'] },
    'R':    { label: 'Relief / regular B tour',     kind: 'work',  tours: ['R'] },
    'RDO':  { label: 'Regular Day Off',             kind: 'off' },
    'MDO':  { label: 'Mutual Day Off',              kind: 'off' },
    'HOL':  { label: 'Holiday',                     kind: 'off' },
    'COMP': { label: 'Comp time',                   kind: 'leave' },
    'PE':   { label: 'Personal Excuse day',         kind: 'leave' },
    'SICK': { label: 'Sick time',                   kind: 'leave' },
    'IOD':  { label: 'Injury on Duty',              kind: 'leave' },
    'DIF':  { label: 'Death in Family',             kind: 'leave' },
    'VAC':  { label: 'Vacation',                    kind: 'leave' },
    '.5VAC':{ label: 'Half-day vacation',           kind: 'leave' },
    'FMLA': { label: 'Family Medical Leave Act',    kind: 'leave' },
    'JD':   { label: 'Jury Duty',                   kind: 'leave' },
    'UB':   { label: 'Union Business',              kind: 'leave' },
    'MED':  { label: 'Medical appointment',         kind: 'leave' },
    'MLT':  { label: 'Military Leave',              kind: 'leave' },
    'A/C':  { label: 'Double: A then C (MDW)',      kind: 'work',  tours: ['A', 'C'], double: true },
    'A/B':  { label: 'Double: A then B (MDW)',      kind: 'work',  tours: ['A', 'B'], double: true },
    'B/A':  { label: 'Double: B then A (MDW)',      kind: 'work',  tours: ['B', 'A'], double: true },
    'B/C':  { label: 'Double: B then C (MDW)',      kind: 'work',  tours: ['B', 'C'], double: true },
    'C/A':  { label: 'Double: C then A (MDW)',      kind: 'work',  tours: ['C', 'A'], double: true },
    'C/B':  { label: 'Double: C then B (MDW)',      kind: 'work',  tours: ['C', 'B'], double: true },
    'E':    { label: 'Excess (B shift)',            kind: 'work',  tours: ['B'] },
    'T':    { label: 'Training',                    kind: 'work',  tours: [] },
    'EX':   { label: 'Extra time',                  kind: 'other', tours: [] },
    'XRF':  { label: 'Transferred',                 kind: 'other', tours: [] },
    'NA':   { label: 'Not available',               kind: 'other', tours: [] },
    '1.5CT':{ label: '1.5 hrs comp time',           kind: 'other', tours: [] },
    '2.5CT':{ label: '2.5 hrs comp time',           kind: 'other', tours: [] },
  },

  /* ---- 2f. Groups on the posted sheet, in print order ----------------
   * kind 'rotating' uses the six slot patterns above (SHIFT = code.slot).
   * kind 'static'   works the same week every week: weekly[0] = Sunday.
   * holidayReplaces: which regular codes become HOL for that group.      */
  groups: {
    'LGA_BSS1.1': { label: 'B shift', kind: 'static', weekly: ['RDO', 'B', 'B', 'B', 'B', 'B', 'RDO'], holidayReplaces: ['B'] },
    /* holidayB: which B crew works a holiday. 'weekend-block' = on a Mon/Tue/
     * Thu/Fri holiday the crew whose B block runs through the weekend works
     * (stays B) and the Mon–Fri B crew gets HOL; on a Wednesday holiday the
     * Mon–Fri crew works. R on a holiday is HOL.                          */
    'LGA_6RR9':   { label: 'Rotating relief', kind: 'rotating', holidayReplaces: ['B', 'R'], holidayB: 'weekend-block' },
    'LGA_ASS1.1': { label: 'A shift', kind: 'static', weekly: ['RDO', 'A', 'A', 'A', 'A', 'A', 'RDO'], holidayReplaces: ['A'] },
  },

  /* Printed on the paper-format views (swap sheet, monthly labor grid). */
  org: {
    nameBold: 'THE PORT AUTHORITY',
    nameRest: 'OF NY & NJ',
    formRevision: '8/12/2019',      // revision date printed on the request form
  },

  /* Legend exactly as printed at the foot of the posted sheet (7 columns,
   * read down each column).                                              */
  printLegend: {
    columns: 7,
    items: [
      ['A', 'A Shift Hours'], ['JD', 'Jury Duty'], ['UB', 'Union Business'], ['A/B', 'Shift A/B MDW'], ['COMP', 'Comp Time'],
      ['B', 'B Shift Hours'], ['MDO', 'Mutual Day Off'], ['VAC', 'Vacation'], ['A/C', 'Shift A/C MDW'], ['1.5CT', '1.5 Hrs Comp Time'],
      ['C', 'C Shift Hours'], ['PE', 'Personal Excuse Day'], ['T', 'Training'], ['B/A', 'Shift B/A MDW'], ['2.5CT', '2.5 Hrs Comp Time'],
      ['.5VAC', '1/2 Day Vacation'], ['DIF', 'Death In Family'], ['XRF', 'Transferred'], ['B/C', 'Shift B/C MDW'], null,
      ['E', 'Excess (B Shift)'], ['R', 'Relief'], ['EX', 'Extra Time'], ['C/A', 'Shift C/A MDW'], null,
      ['FMLA', 'Family Medical Leave Act'], ['RDO', 'Regular Day Off'], ['MED', 'Medical Appt. PA'], ['C/B', 'Shift C/B MDW'], null,
      ['HOL', 'Holiday'], ['SICK', 'Sick Time'], ['MLT', 'Military Leave'], ['NA', 'Not Available'], null,
    ],
  },

  /* Codes shown first in pickers and in the compact legend.            */
  primaryCodes: ['A', 'B', 'C', 'R', 'RDO', 'MDO', 'HOL', 'COMP', 'PE', 'SICK', 'IOD', 'DIF', 'VAC'],

  /* ---- 2e. Built-in exceptions ------------------------------------
   * Per-date overrides shipped with the site. Normally leave this empty
   * and enter changes in the app (they are stored in the browser / sync
   * server). Format:
   *   '2026-09-25': { 'rakhmanov-r': { code: 'HOL', note: 'approved swap' } }   */
  exceptions: {},
};

/* ---------------------------------------------------------------------
 * 3. ROSTER — everyone on the posted sheet, in sheet order
 *    id must be stable (it is used as the key for overrides/requests).
 *    Static-group people all show slot 1, as printed.
 * ------------------------------------------------------------------- */
const ROSTER = [
  /* LGA_BSS1.1 — B shift, Mon–Fri */
  { id: 'ahmed-n',        name: 'Ahmed, Naib',               group: 'LGA_BSS1.1', slot: 1 },
  { id: 'atolagbe-a',     name: 'Atolagbe, Abiodun O.',      group: 'LGA_BSS1.1', slot: 1 },
  { id: 'koronkiewicz-p', name: 'Koronkiewicz, Przemyslaw',  group: 'LGA_BSS1.1', slot: 1 },
  { id: 'ling-r',         name: 'Ling, Raymond K',           group: 'LGA_BSS1.1', slot: 1 },
  { id: 'marszalek-c',    name: 'Marszalek, Casimir D',      group: 'LGA_BSS1.1', slot: 1 },
  { id: 'min-d',          name: 'Min, Dae K',                group: 'LGA_BSS1.1', slot: 1 },
  { id: 'mitchell-d',     name: 'Mitchell II, Dale H.',      group: 'LGA_BSS1.1', slot: 1 },
  { id: 'rickman-d',      name: "Rickman, Deu'Wayne E",      group: 'LGA_BSS1.1', slot: 1 },
  { id: 'son-s',          name: 'Son, Seung Hyon',           group: 'LGA_BSS1.1', slot: 1 },
  { id: 'thompson-t',     name: 'Thompson, Timothy J',       group: 'LGA_BSS1.1', slot: 1 },
  { id: 'thorpe-o',       name: 'Thorpe, Oniel C.',          group: 'LGA_BSS1.1', slot: 1 },
  /* LGA_6RR9 — six-slot rotation */
  { id: 'kingston-j',     name: 'Kingston, Joseph C.',       group: 'LGA_6RR9', slot: 1 },
  { id: 'rodriguez-r',    name: 'Rodriguez, Roger',          group: 'LGA_6RR9', slot: 1 },
  { id: 'joseph-g',       name: 'Joseph, George R',          group: 'LGA_6RR9', slot: 2 },
  { id: 'vacant-2',       name: 'VACANT - Ashram',           group: 'LGA_6RR9', slot: 2, vacant: true },
  { id: 'rivers-n',       name: 'Rivers, Nigel',             group: 'LGA_6RR9', slot: 3 },
  { id: 'victoria-r',     name: 'Victoria, Robert',          group: 'LGA_6RR9', slot: 3 },
  { id: 'bouaziz-a',      name: 'Bouaziz, Abdelghani',       group: 'LGA_6RR9', slot: 4 },
  { id: 'gragossian-a',   name: 'Gragossian, Alan',          group: 'LGA_6RR9', slot: 4 },
  { id: 'handel-j',       name: 'Handel, Joseph',            group: 'LGA_6RR9', slot: 5 },
  { id: 'rakhmanov-r',    name: 'Rakhmanov, Ravshan',        group: 'LGA_6RR9', slot: 5 },
  { id: 'huang-d',        name: 'Huang, Denny',              group: 'LGA_6RR9', slot: 6 },
  { id: 'svenjak-m',      name: 'Svenjak, Mark',             group: 'LGA_6RR9', slot: 6 },
  /* LGA_ASS1.1 — A shift, Mon–Fri */
  { id: 'adjepong-n',     name: 'Adjepong, Nicholas D',      group: 'LGA_ASS1.1', slot: 1 },
  { id: 'duran-h',        name: 'Duran, Hugo A',             group: 'LGA_ASS1.1', slot: 1 },
  { id: 'gapa-m',         name: 'Gapa, Marcel',              group: 'LGA_ASS1.1', slot: 1 },
  { id: 'grassi-g',       name: 'Grassi Jr., Guiseppi',      group: 'LGA_ASS1.1', slot: 1 },
  { id: 'rakhmanov-a',    name: 'Rakhmanov, Alisher',        group: 'LGA_ASS1.1', slot: 1 },
  { id: 'ramnarine-a',    name: 'Ramnarine, Ashram',         group: 'LGA_ASS1.1', slot: 1 },
  { id: 'rodriguez-a',    name: 'Rodriguez, Alexander',      group: 'LGA_ASS1.1', slot: 1 },
  { id: 'segovia-e',      name: 'Segovia, Eduardo M',        group: 'LGA_ASS1.1', slot: 1 },
];

/* Node test harness support (ignored by the browser). */
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { APP_CONFIG, SCHEDULE_CONFIG, ROSTER };
}

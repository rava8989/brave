# LGA Rotating Schedule

A phone-first work-schedule site for the LGA six-slot rotating group, with a
built-in swap / time-off request workflow:

* **Calendar** – today's shift, tomorrow, next work day, next RDO, and a
  month calendar for any person, 12+ months forward and back.
* **Schedule** – opens on the MONTHLY LABOR SCHEDULE exactly as posted: all
  three groups (B shift, rotating, A shift) boxed in sheet order, dark RDO
  cells, red for anything irregular, the printed legend, one landscape page
  when printed. A colour view (supervisors tap any cell to change it) and a
  day view with "who could cover for me" are one tap away.
* **Requests** – a worker photographs the signed paper "TIME OFF OR SHIFT
  CHANGE REQUEST" sheet, the app reads it and fills the request in (or the
  worker taps it in by hand), shows it back as the official form with the
  PRESENT and REQUESTED grids, checks it for conflicts, and the supervisor
  taps **Approve**. The schedule updates itself. Every request can be
  viewed and printed as the official form at any time.
* **Rotation** – this week's lineup for all six slots and the 42-day pattern
  for each slot (editable by the supervisor).
* **Info** – legend, printing, backup, and supervisor tools (roster,
  holidays, coverage rules, PIN).

No build step, no framework: `index.html` + `css/` + `js/`. Open
`schedule/index.html` in a browser and it works.

---

## 1. Running it

### On GitHub Pages (this repo)

The site is served from the repository root, so the schedule lives at

```
https://rava8989.github.io/brave/schedule/
```

as soon as the branch is merged to `main` and Pages is enabled for the repo
(Settings → Pages → Deploy from a branch → `main` / root). Nothing else to
configure. On an iPhone open that URL in Safari → Share → **Add to Home
Screen** to get an app icon.

### Locally

Double-click `schedule/index.html`, or from the repo root:

```
cd schedule && npm run serve      # http://localhost:8790
```

---

## 2. Two modes: this device only, or shared

| | Local mode (default) | Shared mode |
|---|---|---|
| Set-up | none | deploy `worker/worker.js` to Cloudflare, put its URL in `js/config.js` |
| Where data lives | the browser's localStorage / IndexedDB | one JSON document in Cloudflare KV, cached on each phone |
| Roles | pick a name (worker) or enter the supervisor PIN | two access codes issued by the supervisor: a worker code and a supervisor code; the server enforces what each may write |
| Requests reach the supervisor | only on the same phone | from any phone, within ~45 s |

Local mode is fine for one person's calendar and for trying the workflow.
For the real thing (workers on their own phones, supervisor on theirs) use
shared mode – see §5.

---

## 3. Verify the rotation data (please read)

The rotation in `js/config.js` was transcribed from the photos of the posted
September and October 2026 labor schedules and the September swap sheet.
`npm test` proves the transcription reproduces every cell that was legible:

* Rakhmanov Sep 4–10 = A A A A A/C A MDO, Sep 25 = B, Sep 28–Oct 1 = RDO MDO C C/A, Oct 5 = C, Oct 6 = RDO
* Bouaziz Sep 2 = RDO, Sep 4–6 = B, Sep 9–10 = RDO
* Kingston Oct 1 = RDO, Oct 2 = A, Oct 6 = A/C, Oct 8 = MDO, Oct 12 = HOL, Oct 19 = R, Oct 27 = MDO, Oct 29 = C/A, Sep 7 = HOL
* Joseph Oct 1 = MDO, Oct 5 = B, Oct 22 = C/A

It also proves the pattern keeps 2 people on A and 2 on C every day and 2 on
B every weekend, exactly as the posted sheets do.

Two cells that could not be read from the photos were settled by the crew:
the relief block runs **Monday to Friday** (R on the Friday), and the
Fri–Tue B block is followed by RDO on Wednesday and Thursday.

To check the rest: open **Schedule** for October 2026 and compare with the
sheet on the wall. Fix a wrong cell either in `js/config.js` (the six
`slots` arrays; each row is Thu Fri Sat Sun Mon Tue Wed) or in the app as
supervisor: **Rotation → Edit pattern → Save & apply to all slots**.

Other things to confirm:

* **Holidays** (`SCHEDULE_CONFIG.holidays`): only Labor Day and Columbus
  Day 2026 were visible on the sheets (`verified: true`). The rest are the
  standard observed dates – check Independence Day, Election Day and the
  day after Thanksgiving against the agency calendar. Edit under
  **Info → Holidays** or in the file.
* **Holiday rule**: A and C keep working. R becomes HOL. For B it depends
  on which block you are in: on a Mon/Tue/Thu/Fri holiday the crew whose B
  block runs through the weekend works it and the Mon–Fri B crew gets HOL;
  on a Wednesday holiday the Mon–Fri crew works it. The static B and A
  crews get HOL. (Matches the sheets: Bouaziz worked Labor Day, Kingston
  had HOL on Columbus Day.)
* **Coverage rule**: 2 on A and 2 on C every day, 2 on B at weekends. The
  posted schedules show no B coverage from this group on holidays, so the
  holiday B requirement is off by default – switch it on under **Info →
  Rules** if that is wrong.
* **MDO** is labelled "Mutual Day Off" as on the printed legend (the prompt
  called it Mandatory Day Off); **DIF** is "Death in Family" as printed.

---

## 4. How the swap workflow runs

The paper sheet stays the legal record: it is filled in and signed exactly
as today. The app takes it from there.

1. Worker signs in (picks their name), opens **Requests → New**.
2. **Upload the signed swap sheet** (camera opens on the phone). With the
   sync server and an AI key set up, the app reads the handwritten grids,
   matches the names to the roster, fills in the days and codes, and jumps
   straight to the review screen. Anything it could not read, or any cell
   where the sheet's "present" code disagrees with the schedule, is listed
   in yellow so it can be checked. Without the AI key the same screen
   offers the by-hand path: *Swap with a coworker* or *Time off / change my
   shift*, pick the coworker, tap the days, adjust codes, attach the photo.
3. **Review** shows the request as the official form – "MY PRESENT SCHEDULE
   IS" on top, "TIME OFF OR CHANGE PERIOD REQUESTED" below, 1–31 columns,
   one row per person, next-month days as extra "1st, 2nd…" columns – plus
   the before/after table. **Print this form to sign** prints it pre-filled
   if the paper has not been written yet.
4. The app checks the request:
   * errors (blocked): past dates, unknown codes, duplicate days;
   * warnings (shown to the supervisor): coverage drops below the minimum on
     any affected day, back-to-back doubles, more than 7 work days in a row,
     the schedule changed since the form was written, another pending
     request touches the same day;
   * notes: uneven trades, no-op changes.
5. The supervisor's **Requests** tab shows the queue with the before/after
   table, the photo and the live conflict check; **Official form** opens the
   same request as the paper form (with APPROVED / REJECTED, supervisor and
   date filled in once decided) and prints it. **Approve** writes the
   changes into the schedule immediately (as per-person exceptions on top of
   the rotation); **Reject** asks for a reason the worker sees. A checkbox
   records when it was entered into the official system.
6. Approved changes show a ● on the calendar and a blue outline on the
   month grid. **Team → Month grid → Official layout** shows the month in
   the same layout as the posted MONTHLY LABOR SCHEDULE (NAME / SHIFT / SLOT
   columns, gray RDO cells, red for holidays, leave and changes) and prints
   that way. The supervisor can also tap any grid cell (colors layout) to
   change it directly.

Reading the photo uses Claude (`claude-opus-5`) through the sync worker; the
key never reaches the phones. It costs a few cents per sheet. The worker
also sends Claude the schedule the app already has for that month, which is
what lets it line up the handwritten cells with the right day columns.

---

## 5. Sharing between phones (Cloudflare Worker)

The backend is a single dependency-free file, `worker/worker.js`, plus one
KV namespace. About ten minutes:

**Dashboard route (no CLI)**

1. Cloudflare dashboard → Workers & Pages → Create → Worker → name it
   `lga-schedule` → Deploy, then *Edit code*, paste `worker/worker.js`,
   Deploy.
2. Storage & Databases → KV → Create namespace `lga-schedule`.
3. Worker → Settings → Bindings → Add → KV namespace, variable name
   `SCHEDULE_KV`, pick the namespace.
4. Worker → Settings → Variables and Secrets → add secrets
   `SUPERVISOR_CODE` and `WORKER_CODE` (any phrases you choose), optionally
   `ANTHROPIC_API_KEY` (photo reading) and a variable `ALLOWED_ORIGIN` =
   `https://rava8989.github.io`.
5. Copy the worker URL (`https://lga-schedule.<you>.workers.dev`) into
   `APP_CONFIG.syncUrl` in `js/config.js`, commit, push.

**One-command route** (needs Node.js; wrangler opens a browser to log in)

```
bash schedule/worker/deploy.sh
```

It creates the KV namespace, writes its id into `wrangler.toml`, asks for
the two access codes and the optional Anthropic API key, and deploys. The
same steps by hand:

```
cd schedule/worker
npx wrangler kv namespace create SCHEDULE_KV     # paste the id into wrangler.toml
npx wrangler secret put SUPERVISOR_CODE
npx wrangler secret put WORKER_CODE
npx wrangler secret put ANTHROPIC_API_KEY        # optional: photo reading
npx wrangler deploy
```

First sign-in must be the supervisor (that seeds the shared document); then
hand the worker code to the crew. The server only lets the worker code add
pending requests or cancel them – roster, rotation, holidays, overrides and
approvals need the supervisor code. Codes are shared per role, so the app
records who submitted a request by the name chosen at sign-in; per-person
codes are a straightforward extension if ever needed.

---

## 6. Editing the schedule data

Everything editable is in `js/config.js`:

| Section | What |
|---|---|
| `APP_CONFIG` | app name, `syncUrl`, default PIN, photo size, poll interval |
| `SCHEDULE_CONFIG.rotation` | `referenceDate` (2026-01-01), `cycleLength` (42), the six `slots` arrays |
| `SCHEDULE_CONFIG.holidays` | `{ date, name, verified }` |
| `SCHEDULE_CONFIG.holidayRule` | which codes become HOL |
| `SCHEDULE_CONFIG.coverage` | minimums used by the conflict check |
| `SCHEDULE_CONFIG.codes` | every code, its meaning, which tours it covers |
| `SCHEDULE_CONFIG.org` | agency name, group code (SHIFT column) and form revision printed on the paper-format views |
| `SCHEDULE_CONFIG.exceptions` | per-date overrides shipped with the site (normally empty) |
| `SCHEDULE_CONFIG.groups` | the three blocks on the sheet: LGA_BSS1.1 (B shift Mon–Fri), LGA_6RR9 (rotating slots), LGA_ASS1.1 (A shift Mon–Fri), each with its own holiday rule |
| `SCHEDULE_CONFIG.printLegend` | the legend exactly as printed on the sheet |
| `ROSTER` | everyone on the sheet: id, name, group, slot |

Formula used by the engine (`js/engine.js`):

```
code(slot, date) = slots[slot][ daysSince('2026-01-01', date) mod 42 ]
then: holiday? and code in holidayRule.replaces → 'HOL'
then: per-person exception for that date, if any
```

Data entered in the app (roster edits, rotation edits, holidays, overrides,
requests) is stored separately from the file and takes precedence, so you
can redeploy the site without losing anything. **Info → Backup** downloads
it as JSON.

---

## 7. Tests

```
cd schedule
npm test        # 89 engine + swap checks (dates, leap years, year change,
                #  weekend boundaries, holidays, RDO/MDO, photo cells, coverage)
                #  + 27 worker checks (access codes, worker write policy,
                #  version conflicts, photos, /api/parse with a mocked model)
npm run lint    # ESLint reference-error check for all scripts
```

A Playwright smoke run (login → calendar → team → submit swap → official
form → approve → official month layout → grid edit → rotation → info →
reload → print → photo-first flow with a mocked reader) was used during
development at 390×844 and 1280×900.

---

## 8. Files

```
schedule/
  index.html          page shell (no inline scripts)
  css/styles.css      mobile-first styles, dark mode, print
  js/config.js        ← all editable data
  js/engine.js        date math + rotation engine
  js/store.js         localStorage / IndexedDB / sync client, roles
  js/swaps.js         request model, conflict checks, approval
  js/ui-common.js     helpers (chips, sheets, toasts, photo downscale)
  js/ui-forms.js      paper-accurate swap sheet + monthly labor schedule
  js/ui-calendar.js   today card + month calendar
  js/ui-team.js       day view + monthly labor grid
  js/ui-requests.js   request wizard, queue, decisions
  js/ui-rotation.js   week lineup + pattern editor
  js/ui-info.js       legend, account, print, supervisor tools
  js/app.js           bootstrap, login, tabs
  worker/worker.js    optional Cloudflare sync backend (+ photo reading)
  worker/wrangler.toml
  worker/deploy.sh    one-command deploy
  tests/engine.test.js
  tests/worker.test.mjs
```

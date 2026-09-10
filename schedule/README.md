# LGA Rotating Schedule

A phone-first work-schedule site for the LGA six-slot rotating group, with a
built-in swap / time-off request workflow:

* **Calendar** – today's shift, tomorrow, next work day, next RDO, and a
  month calendar for any person, 12+ months forward and back.
* **Team** – who is on A / B / C / R and who is off on any day, "who could
  cover for me" suggestions, and the full monthly labor grid in the same
  layout as the sheet on the wall.
* **Requests** – workers build a swap or time-off request in the app (with a
  photo of the signed paper form attached), the app checks it for conflicts,
  the supervisor taps **Approve**, and the schedule updates itself.
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

**Verify these rotation cells** – they could not be read with certainty and
were filled in from the structure of the pattern:

| Slot 1 cycle day | Date example (slot 1) | Chosen | Alternative |
|---|---|---|---|
| Friday after the Mon–Thu R block | Fri Oct 23 2026 | `B` (the Sep swap sheet shows Rakhmanov = B on the matching Fri Sep 25) | `R` or `RDO` |
| Friday of the Mon–Fri B block | Fri Oct 16 2026 | `B` | `RDO` |
| Wednesday after the Fri–Tue B block | Wed Oct 7 2026 for slot 6 | `RDO` (Bouaziz's sheet shows RDO on the matching Wed Sep 9) | `B` |

To check: open **Team → Month grid** for October 2026 and compare with the
sheet on the wall. Fix a wrong cell either in `js/config.js` (the six
`slots` arrays; each row is Thu Fri Sat Sun Mon Tue Wed) or in the app as
supervisor: **Rotation → Edit pattern → Save & apply to all slots**.

Other things to confirm:

* **Holidays** (`SCHEDULE_CONFIG.holidays`): only Labor Day and Columbus
  Day 2026 were visible on the sheets (`verified: true`). The rest are the
  standard observed dates – check Independence Day, Election Day and the
  day after Thanksgiving against the agency calendar. Edit under
  **Info → Holidays** or in the file.
* **Holiday rule**: on a holiday, B and R become HOL; A and C keep working
  (matches the sheets). Change under **Info → Rules**.
* **Coverage rule**: 2 on A and 2 on C every day, 2 on B at weekends. The
  posted schedules show no B coverage from this group on holidays, so the
  holiday B requirement is off by default – switch it on under **Info →
  Rules** if that is wrong.
* **MDO** is labelled "Mutual Day Off" as on the printed legend (the prompt
  called it Mandatory Day Off); **DIF** is "Death in Family" as printed.

---

## 4. How the swap workflow runs

1. Worker signs in (picks their name), opens **Requests → New**.
2. Chooses *Swap with a coworker* or *Time off / change my shift*, picks the
   coworker, taps the days (the picker shows both people's shifts), adjusts
   the requested codes if needed, adds a reason and a **photo of the signed
   paper form** (camera opens directly on the phone).
3. The app checks the request:
   * errors (blocked): past dates, unknown codes, duplicate days;
   * warnings (shown to the supervisor): coverage drops below the minimum on
     any affected day, back-to-back doubles, more than 7 work days in a row,
     the schedule changed since the form was written, another pending
     request touches the same day;
   * notes: uneven trades, no-op changes.
4. The supervisor's **Requests** tab shows the queue with the before/after
   table, the photo and the live conflict check. **Approve** writes the
   changes into the schedule immediately (as per-person exceptions on top of
   the rotation); **Reject** asks for a reason the worker sees. A checkbox
   records when it was entered into the official system.
5. Approved changes show a ● on the calendar and a blue outline on the
   month grid. The supervisor can also tap any grid cell to change it
   directly.

Optional: with `ANTHROPIC_API_KEY` set on the sync worker, the wizard shows
**Fill in from photo** – Claude reads the handwritten form and pre-fills the
days and codes, which the worker then confirms before submitting.

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

**wrangler route**

```
cd schedule/worker
npx wrangler kv namespace create SCHEDULE_KV     # paste the id into wrangler.toml
npx wrangler secret put SUPERVISOR_CODE
npx wrangler secret put WORKER_CODE
npx wrangler secret put ANTHROPIC_API_KEY        # optional
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
| `SCHEDULE_CONFIG.exceptions` | per-date overrides shipped with the site (normally empty) |
| `ROSTER` | employees and slots |

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
npm run lint    # ESLint reference-error check for all scripts
```

A Playwright smoke run (login → calendar → team → submit swap → approve →
month grid edit → rotation → info → reload → print) was used during
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
  js/ui-calendar.js   today card + month calendar
  js/ui-team.js       day view + monthly labor grid
  js/ui-requests.js   request wizard, queue, decisions
  js/ui-rotation.js   week lineup + pattern editor
  js/ui-info.js       legend, account, print, supervisor tools
  js/app.js           bootstrap, login, tabs
  worker/worker.js    optional Cloudflare sync backend
  worker/wrangler.toml
  tests/engine.test.js
```

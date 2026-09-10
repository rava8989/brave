/* =====================================================================
 *  worker.js — optional sync backend for the LGA Rotating Schedule app
 *  (Cloudflare Worker + one KV namespace). No dependencies, so it can be
 *  pasted straight into the Cloudflare dashboard editor or deployed with
 *  wrangler (see README.md → "Sharing between phones").
 *
 *  Secrets / vars (Settings → Variables):
 *    SUPERVISOR_CODE   access code that unlocks full write access
 *    WORKER_CODE       access code for workers (submit / cancel requests)
 *    ANTHROPIC_API_KEY optional — enables POST /api/parse (read a photo of
 *                      the paper swap sheet with Claude and pre-fill the form)
 *    ALLOWED_ORIGIN    optional — restrict CORS to your GitHub Pages origin,
 *                      e.g. https://rava8989.github.io   (default: *)
 *  Bindings:
 *    SCHEDULE_KV       KV namespace
 *
 *  Endpoints (all need  Authorization: Bearer <code>)
 *    GET  /api/health                 → { ok, ai }
 *    GET  /api/state                  → { role, ai, state }
 *    PUT  /api/state  { baseVersion, state }
 *                                     → { state }  (409 + latest state on version conflict)
 *    GET  /api/photos/:id             → { dataUrl }
 *    PUT  /api/photos/:id { dataUrl } → { ok }
 *    POST /api/parse  { dataUrl, hints } → { changes:[{name,date,to,present}], reason, notes }
 *
 *  Worker-role PUTs are validated: they may only add pending requests or
 *  cancel pending ones — never touch roster, rotation, holidays, overrides
 *  or decisions. Supervisor-role PUTs may change anything.
 * ===================================================================== */

const STATE_KEY = 'state';
const MAX_PHOTO_BYTES = 6 * 1024 * 1024;     // base64 data URL
const MAX_STATE_BYTES = 20 * 1024 * 1024;     // KV value limit is 25 MB

export default {
  async fetch(request, env) {
    const origin = env.ALLOWED_ORIGIN || '*';
    if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: cors(origin) });
    try {
      const res = await route(request, env);
      const h = new Headers(res.headers);
      Object.entries(cors(origin)).forEach(([k, v]) => h.set(k, v));
      return new Response(res.body, { status: res.status, headers: h });
    } catch (e) {
      return json({ error: 'Server error: ' + (e && e.message) }, 500, origin);
    }
  },
};

function cors(origin) {
  return {
    'Access-Control-Allow-Origin': origin,
    'Access-Control-Allow-Methods': 'GET,PUT,POST,DELETE,OPTIONS',
    'Access-Control-Allow-Headers': 'Authorization,Content-Type',
    'Access-Control-Max-Age': '86400',
    'Cache-Control': 'no-store',
  };
}
function json(obj, status, origin) {
  return new Response(JSON.stringify(obj), { status: status || 200, headers: Object.assign({ 'Content-Type': 'application/json' }, origin ? cors(origin) : {}) });
}

function roleFor(request, env) {
  const auth = request.headers.get('Authorization') || '';
  const code = auth.replace(/^Bearer\s+/i, '').trim();
  if (!code) return null;
  if (env.SUPERVISOR_CODE && safeEqual(code, env.SUPERVISOR_CODE)) return 'supervisor';
  if (env.WORKER_CODE && safeEqual(code, env.WORKER_CODE)) return 'worker';
  return null;
}
function safeEqual(a, b) {
  if (a.length !== b.length) return false;
  let r = 0;
  for (let i = 0; i < a.length; i++) r |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return r === 0;
}

async function loadState(env) {
  const raw = await env.SCHEDULE_KV.get(STATE_KEY);
  return raw ? JSON.parse(raw) : null;
}

async function route(request, env) {
  const url = new URL(request.url);
  const path = url.pathname.replace(/\/+$/, '');
  if (!env.SCHEDULE_KV) return json({ error: 'KV binding SCHEDULE_KV is missing' }, 500);
  if (!env.SUPERVISOR_CODE || !env.WORKER_CODE) return json({ error: 'SUPERVISOR_CODE / WORKER_CODE are not set' }, 500);

  const role = roleFor(request, env);
  if (path === '/api/health' && request.method === 'GET') return json({ ok: true, ai: !!env.ANTHROPIC_API_KEY, role: role || null });
  if (!role) return json({ error: 'Access code required' }, 401);

  if (path === '/api/state' && request.method === 'GET') {
    const state = await loadState(env);
    return json({ role, ai: !!env.ANTHROPIC_API_KEY, state: state || null });
  }

  if (path === '/api/state' && request.method === 'PUT') {
    const body = await request.json().catch(() => null);
    if (!body || typeof body !== 'object' || !body.state || typeof body.state !== 'object') return json({ error: 'Bad request' }, 400);
    const current = await loadState(env);
    const currentVersion = current ? Number(current.version) || 0 : 0;
    if (current && Number(body.baseVersion) !== currentVersion) return json({ error: 'Version conflict', state: current }, 409);
    const next = body.state;
    if (role === 'worker') {
      const why = workerWriteViolation(current, next);
      if (why) return json({ error: 'Workers cannot change that (' + why + ')' }, 403);
    }
    next.version = currentVersion + 1;
    next.updatedAt = new Date().toISOString();
    const raw = JSON.stringify(next);
    if (raw.length > MAX_STATE_BYTES) return json({ error: 'State too large' }, 413);
    await env.SCHEDULE_KV.put(STATE_KEY, raw);
    return json({ state: next });
  }

  const photo = path.match(/^\/api\/photos\/([A-Za-z0-9_.-]{1,80})$/);
  if (photo && request.method === 'GET') {
    const dataUrl = await env.SCHEDULE_KV.get('photo:' + photo[1]);
    if (!dataUrl) return json({ error: 'Not found' }, 404);
    return json({ dataUrl });
  }
  if (photo && request.method === 'PUT') {
    const body = await request.json().catch(() => null);
    if (!body || typeof body.dataUrl !== 'string' || !/^data:image\/(jpeg|png|webp);base64,/.test(body.dataUrl)) return json({ error: 'Expected a JPEG/PNG data URL' }, 400);
    if (body.dataUrl.length > MAX_PHOTO_BYTES) return json({ error: 'Photo too large' }, 413);
    await env.SCHEDULE_KV.put('photo:' + photo[1], body.dataUrl);
    return json({ ok: true });
  }
  if (photo && request.method === 'DELETE') {
    if (role !== 'supervisor') return json({ error: 'Supervisor only' }, 403);
    await env.SCHEDULE_KV.delete('photo:' + photo[1]);
    return json({ ok: true });
  }

  if (path === '/api/parse' && request.method === 'POST') {
    if (!env.ANTHROPIC_API_KEY) return json({ error: 'Photo reading is not enabled on this server' }, 501);
    const body = await request.json().catch(() => null);
    if (!body || typeof body.dataUrl !== 'string') return json({ error: 'Bad request' }, 400);
    return json(await parseSwapSheet(body.dataUrl, body.hints || {}, env));
  }

  return json({ error: 'Not found' }, 404);
}

/* ------------------------------------------------------------------
 * Worker-role write policy: everything identical except `requests`,
 * where the only allowed edits are appending pending requests and
 * turning a pending request into a cancelled one.
 * ---------------------------------------------------------------- */
function workerWriteViolation(current, next) {
  if (!current) return 'no schedule has been set up yet — a supervisor must sign in first';
  const keys = new Set([...Object.keys(current), ...Object.keys(next)]);
  for (const k of keys) {
    if (k === 'requests' || k === 'version' || k === 'updatedAt') continue;
    if (JSON.stringify(current[k] ?? null) !== JSON.stringify(next[k] ?? null)) return k;
  }
  const oldReqs = Array.isArray(current.requests) ? current.requests : [];
  const newReqs = Array.isArray(next.requests) ? next.requests : [];
  const byId = new Map(newReqs.map((r) => [r && r.id, r]));
  if (byId.size !== newReqs.length) return 'duplicate request ids';
  for (const r of oldReqs) {
    const n = byId.get(r.id);
    if (!n) return 'a request was removed';
    const a = Object.assign({}, r), b = Object.assign({}, n);
    if (a.status === 'pending' && b.status === 'cancelled') {
      delete a.status; delete b.status; delete a.decidedAt; delete b.decidedAt; delete a.decidedBy; delete b.decidedBy; delete a.decisionNote; delete b.decisionNote;
    }
    if (JSON.stringify(a) !== JSON.stringify(b)) return 'an existing request was modified';
    byId.delete(r.id);
  }
  for (const n of byId.values()) {
    if (!n || n.status !== 'pending') return 'new requests must be pending';
    if (!Array.isArray(n.changes) || !n.changes.length) return 'request has no changes';
  }
  return null;
}

/* ------------------------------------------------------------------
 * Optional: read a photo of the paper "Time off or shift change request"
 * form and return structured changes. Raw HTTP is used (no SDK) so the
 * worker stays dependency-free and dashboard-pasteable.
 * ---------------------------------------------------------------- */
async function parseSwapSheet(dataUrl, hints, env) {
  const m = dataUrl.match(/^data:(image\/(?:jpeg|png|webp));base64,(.+)$/);
  if (!m) return { error: 'Expected a JPEG/PNG data URL' };
  const employees = Array.isArray(hints.employees) ? hints.employees.slice(0, 40) : [];
  const codes = ['A', 'B', 'C', 'R', 'RDO', 'MDO', 'HOL', 'COMP', 'PE', 'SICK', 'IOD', 'DIF', 'VAC', '.5VAC', 'FMLA', 'JD', 'UB', 'MED', 'MLT',
    'A/C', 'A/B', 'B/A', 'B/C', 'C/A', 'C/B', 'E', 'T', 'EX', 'XRF', 'NA', '1.5CT', '2.5CT'];
  const system =
    'You read photos of a handwritten Port Authority "TIME OFF OR SHIFT CHANGE REQUEST" form. ' +
    'The form has two grids with day-of-month columns 1-31: "MY PRESENT SCHEDULE IS" (what is scheduled now) and ' +
    '"TIME OFF OR CHANGE PERIOD REQUESTED" (what the employees want instead). Each grid has one row per employee name. ' +
    'Report every cell of the REQUESTED grid as a change, and the matching PRESENT cell if it is filled in. ' +
    'Use only these codes: ' + codes.join(', ') + '. Write "V" as VAC. If the month is written on the form use it; days annotated as belonging to the next month (e.g. "1st", "2nd" with an arrow) belong to the following month. ' +
    'Match names to this roster when possible: ' + employees.map((e) => e.name + ' (id ' + e.id + ')').join('; ') + '. ' +
    'Dates must be YYYY-MM-DD. If you cannot read a cell confidently, leave it out and mention it in notes.';
  const year = Number(hints.year) || new Date().getFullYear();
  const month = Number(hints.month) || new Date().getMonth() + 1;
  /* The app's own computed schedule for the month, so the model can line up
   * handwritten cells with the right day columns and names. */
  let presentText = '';
  if (hints.present && typeof hints.present === 'object') {
    const byId = new Map(employees.map((e) => [e.id, e.name]));
    presentText = Object.keys(hints.present).slice(0, 40).map((id) => {
      const row = hints.present[id] || {};
      const cells = Object.keys(row).sort().map((iso) => Number(iso.slice(8, 10)) + '=' + row[iso]).join(' ');
      return (byId.get(id) || id) + ' (id ' + id + '): ' + cells;
    }).join('\n');
  }
  const schema = {
    type: 'object',
    properties: {
      month: { type: 'integer' },
      year: { type: 'integer' },
      changes: {
        type: 'array',
        items: {
          type: 'object',
          properties: {
            name: { type: 'string' },
            employeeId: { type: ['string', 'null'] },
            date: { type: 'string' },
            present: { type: ['string', 'null'] },
            to: { type: 'string' },
          },
          required: ['name', 'employeeId', 'date', 'present', 'to'],
          additionalProperties: false,
        },
      },
      reason: { type: 'string' },
      notes: { type: 'string' },
    },
    required: ['month', 'year', 'changes', 'reason', 'notes'],
    additionalProperties: false,
  };
  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-api-key': env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
    body: JSON.stringify({
      model: env.CLAUDE_MODEL || 'claude-opus-5',
      max_tokens: 4000,
      system,
      output_config: { format: { type: 'json_schema', schema } },
      messages: [{
        role: 'user',
        content: [
          { type: 'image', source: { type: 'base64', media_type: m[1], data: m[2] } },
          { type: 'text', text: 'The app is currently showing ' + year + '-' + String(month).padStart(2, '0') + '. Extract the requested changes from this form.' +
            (presentText ? '\n\nFor reference, the schedule the app currently has for that month (day=code), which should agree with the PRESENT grid where it is filled in:\n' + presentText : '') },
        ],
      }],
    }),
  });
  const data = await res.json().catch(() => null);
  if (!res.ok) return { error: 'Claude API error ' + res.status + ': ' + (data && data.error ? data.error.message : '') };
  if (data.stop_reason === 'refusal') return { error: 'The photo could not be processed' };
  const text = (data.content || []).filter((b) => b.type === 'text').map((b) => b.text).join('');
  try { return JSON.parse(text); } catch (e) { return { error: 'Could not understand the photo' }; }
}

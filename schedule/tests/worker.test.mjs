/* Run:  node schedule/tests/worker.test.mjs
 * Exercises the Cloudflare worker with an in-memory KV and a mocked Claude API. */
import worker from '../worker/worker.js';

let passed = 0, failed = 0;
function check(cond, label) { if (cond) passed++; else { failed++; console.error('FAIL ' + label); } }

const kv = new Map();
const env = {
  SCHEDULE_KV: { get: async (k) => kv.get(k) ?? null, put: async (k, v) => { kv.set(k, v); }, delete: async (k) => { kv.delete(k); } },
  SUPERVISOR_CODE: 'boss-code', WORKER_CODE: 'crew-code', ANTHROPIC_API_KEY: 'test-key', ALLOWED_ORIGIN: 'https://rava8989.github.io',
};
async function call(method, path, body, code) {
  const req = new Request('https://x.workers.dev' + path, {
    method, headers: Object.assign({ 'Content-Type': 'application/json' }, code ? { Authorization: 'Bearer ' + code } : {}),
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const res = await worker.fetch(req, env);
  let data = null; try { data = await res.json(); } catch (e) { /* none */ }
  return { status: res.status, data, headers: res.headers };
}

/* auth + CORS */
let r = await call('GET', '/api/state');
check(r.status === 401, 'no code → 401');
r = await call('GET', '/api/state', undefined, 'wrong');
check(r.status === 401, 'wrong code → 401');
r = await call('GET', '/api/health');
check(r.status === 200 && r.data.ai === true, 'health reports ai=true when key set');
check(r.headers.get('Access-Control-Allow-Origin') === 'https://rava8989.github.io', 'CORS origin restricted to ALLOWED_ORIGIN');
r = await call('OPTIONS', '/api/state');
check(r.status === 204, 'preflight → 204');

/* empty store */
r = await call('GET', '/api/state', undefined, 'crew-code');
check(r.status === 200 && r.data.role === 'worker' && r.data.state === null, 'worker sees role + empty state');

/* worker cannot seed; supervisor can */
const base = { version: 0, roster: [{ id: 'a', name: 'A', slot: 1 }], rotation: null, holidays: null, exceptions: {}, requests: [], settings: { supervisorPin: '1' } };
r = await call('PUT', '/api/state', { baseVersion: 0, state: base }, 'crew-code');
check(r.status === 403, 'worker cannot create the initial state');
r = await call('PUT', '/api/state', { baseVersion: 0, state: base }, 'boss-code');
check(r.status === 200 && r.data.state.version === 1, 'supervisor seeds state → version 1');

/* version conflict */
r = await call('PUT', '/api/state', { baseVersion: 0, state: base }, 'boss-code');
check(r.status === 409 && r.data.state.version === 1, 'stale baseVersion → 409 with latest state');

/* worker may append a pending request */
const withReq = JSON.parse(JSON.stringify(r.data.state));
withReq.requests.push({ id: 'r1', status: 'pending', changes: [{ employeeId: 'a', date: '2026-09-18', from: 'B', to: 'RDO' }], employees: ['a'] });
r = await call('PUT', '/api/state', { baseVersion: 1, state: withReq }, 'crew-code');
check(r.status === 200 && r.data.state.version === 2 && r.data.state.requests.length === 1, 'worker appends a pending request');

/* worker may not approve, edit exceptions, or touch the roster */
const bad1 = JSON.parse(JSON.stringify(r.data.state)); bad1.requests[0].status = 'approved';
check((await call('PUT', '/api/state', { baseVersion: 2, state: bad1 }, 'crew-code')).status === 403, 'worker cannot approve');
const bad2 = JSON.parse(JSON.stringify(r.data.state)); bad2.exceptions['2026-09-18'] = { a: { code: 'RDO' } };
check((await call('PUT', '/api/state', { baseVersion: 2, state: bad2 }, 'crew-code')).status === 403, 'worker cannot write exceptions');
const bad3 = JSON.parse(JSON.stringify(r.data.state)); bad3.roster[0].slot = 2;
check((await call('PUT', '/api/state', { baseVersion: 2, state: bad3 }, 'crew-code')).status === 403, 'worker cannot change the roster');
const bad4 = JSON.parse(JSON.stringify(r.data.state)); bad4.requests = [];
check((await call('PUT', '/api/state', { baseVersion: 2, state: bad4 }, 'crew-code')).status === 403, 'worker cannot delete a request');

/* worker may cancel a pending request */
const cancel = JSON.parse(JSON.stringify(r.data.state)); cancel.requests[0].status = 'cancelled'; cancel.requests[0].decidedAt = 'now'; cancel.requests[0].decidedBy = 'A';
r = await call('PUT', '/api/state', { baseVersion: 2, state: cancel }, 'crew-code');
check(r.status === 200 && r.data.state.requests[0].status === 'cancelled', 'worker cancels own pending request');

/* supervisor approves */
const appr = JSON.parse(JSON.stringify(r.data.state)); appr.requests[0].status = 'approved'; appr.exceptions['2026-09-18'] = { a: { code: 'RDO' } };
r = await call('PUT', '/api/state', { baseVersion: 3, state: appr }, 'boss-code');
check(r.status === 200 && r.data.state.version === 4, 'supervisor approval accepted');

/* photos */
r = await call('PUT', '/api/photos/p-1', { dataUrl: 'data:image/jpeg;base64,/9j/4AAQ' }, 'crew-code');
check(r.status === 200, 'photo upload');
r = await call('GET', '/api/photos/p-1', undefined, 'boss-code');
check(r.status === 200 && r.data.dataUrl.startsWith('data:image/jpeg'), 'photo download');
r = await call('PUT', '/api/photos/p-2', { dataUrl: 'data:text/html;base64,PGI+' }, 'crew-code');
check(r.status === 400, 'non-image photo rejected');
check((await call('DELETE', '/api/photos/p-1', undefined, 'crew-code')).status === 403, 'worker cannot delete photos');

/* /api/parse with a mocked Claude API */
let captured = null;
const realFetch = globalThis.fetch;
globalThis.fetch = async (url, opts) => {
  captured = { url, body: JSON.parse(opts.body), headers: opts.headers };
  const payload = { month: 9, year: 2026, changes: [{ name: 'R. Rakhmanov', employeeId: 'rakhmanov-r', date: '2026-09-25', present: 'B', to: 'HOL' }], reason: '', notes: '' };
  return new Response(JSON.stringify({ stop_reason: 'end_turn', content: [{ type: 'text', text: JSON.stringify(payload) }] }), { status: 200, headers: { 'Content-Type': 'application/json' } });
};
r = await call('POST', '/api/parse', { dataUrl: 'data:image/jpeg;base64,/9j/4AAQ', hints: { year: 2026, month: 9, employees: [{ id: 'rakhmanov-r', name: 'Rakhmanov, Ravshan' }], present: { 'rakhmanov-r': { '2026-09-25': 'B' } } } }, 'crew-code');
check(r.status === 200 && r.data.changes && r.data.changes[0].to === 'HOL', 'parse returns the model JSON');
check(captured.url === 'https://api.anthropic.com/v1/messages' && captured.headers['x-api-key'] === 'test-key' && captured.headers['anthropic-version'] === '2023-06-01', 'calls the Messages API with key + version headers');
check(captured.body.model === 'claude-opus-5' && captured.body.output_config.format.type === 'json_schema', 'uses claude-opus-5 with a JSON schema output');
check(captured.body.messages[0].content[0].type === 'image' && captured.body.messages[0].content[0].source.media_type === 'image/jpeg', 'sends the image as base64');
check(captured.body.messages[0].content[1].text.includes('25=B'), 'present-schedule hint is included in the prompt');
check(captured.body.system.includes('rakhmanov-r'), 'roster is in the system prompt');
globalThis.fetch = realFetch;
delete env.ANTHROPIC_API_KEY;
check((await call('POST', '/api/parse', { dataUrl: 'data:image/jpeg;base64,x' }, 'crew-code')).status === 501, 'parse → 501 without an API key');

console.log((failed ? 'FAILED ' + failed + ' / ' : 'PASSED ') + (passed + failed) + ' worker checks');
process.exit(failed ? 1 : 0);

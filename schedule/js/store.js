/* =====================================================================
 *  store.js — persistence, session (role), optional sync server
 *
 *  Shared state (everything supervisors/workers see together):
 *    {
 *      version, updatedAt,
 *      roster:     [{ id, name, slot, active, vacant }],
 *      rotation:   null | override of SCHEDULE_CONFIG.rotation,
 *      holidays:   null | override of SCHEDULE_CONFIG.holidays,
 *      exceptions: { 'YYYY-MM-DD': { employeeId: { code, note, source, by, at } } },
 *      requests:   [ ...Swaps.buildRequest() objects ],
 *      settings:   { supervisorPin, coverage, holidayReplaces }
 *    }
 *
 *  Modes
 *    local  — everything in this browser (localStorage + IndexedDB photos)
 *    remote — APP_CONFIG.syncUrl set: state lives on the sync worker; a
 *             copy is cached locally so the app opens offline.
 *
 *  Every write goes through Store.mutate(fn). In remote mode the write is
 *  optimistic-concurrency checked (version) and retried on conflict.
 * ===================================================================== */
const Store = (function () {
  'use strict';

  const KEYS = {
    state: 'lga-schedule.state.v1',
    session: 'lga-schedule.session.v1',
    prefs: 'lga-schedule.prefs.v1',
    photoPrefix: 'lga-schedule.photo.',
  };

  let state = null;
  let session = null;
  let prefs = {};
  const listeners = [];
  const sync = { mode: 'local', url: '', lastPull: null, error: null, busy: 0, ai: false, online: true };
  let pollTimer = null;

  /* ---------------- utils ---------------- */
  function clone(x) { return JSON.parse(JSON.stringify(x)); }
  function nowISO() { return new Date().toISOString(); }
  function lsGet(key, fallback) {
    try { const raw = localStorage.getItem(key); return raw ? JSON.parse(raw) : fallback; }
    catch (e) { return fallback; }
  }
  function lsSet(key, value) {
    try { localStorage.setItem(key, JSON.stringify(value)); return true; }
    catch (e) { console.warn('localStorage write failed', e); return false; }
  }
  function lsDel(key) { try { localStorage.removeItem(key); } catch (e) { /* ignore */ } }

  function defaultState() {
    return {
      version: 0,
      updatedAt: null,
      roster: clone(ROSTER),
      rotation: null,
      holidays: null,
      exceptions: clone(SCHEDULE_CONFIG.exceptions || {}),
      requests: [],
      settings: { supervisorPin: APP_CONFIG.defaultSupervisorPin, coverage: null, holidayReplaces: null },
    };
  }
  /* Fill any missing keys (older saved states, server states). */
  function normalize(s) {
    const d = defaultState();
    s = s && typeof s === 'object' ? s : {};
    Object.keys(d).forEach(function (k) { if (s[k] === undefined || s[k] === null && k !== 'rotation' && k !== 'holidays' && k !== 'updatedAt') s[k] = d[k]; });
    if (!Array.isArray(s.roster) || !s.roster.length) s.roster = d.roster;
    if (!Array.isArray(s.requests)) s.requests = [];
    if (typeof s.exceptions !== 'object') s.exceptions = {};
    s.settings = Object.assign(d.settings, s.settings || {});
    s.version = Number(s.version) || 0;
    return s;
  }

  /* The config the Engine should run with = built-in config + overrides. */
  function effectiveConfig() {
    const s = state || defaultState();
    const coverage = Object.assign({}, SCHEDULE_CONFIG.coverage, s.settings.coverage || {});
    const holidayRule = s.settings.holidayReplaces ? { replaces: s.settings.holidayReplaces } : SCHEDULE_CONFIG.holidayRule;
    return {
      rotation: s.rotation || SCHEDULE_CONFIG.rotation,
      holidays: s.holidays || SCHEDULE_CONFIG.holidays,
      holidayRule: holidayRule,
      coverage: coverage,
      codes: SCHEDULE_CONFIG.codes,
      primaryCodes: SCHEDULE_CONFIG.primaryCodes,
      exceptions: s.exceptions || {},
      roster: s.roster,
    };
  }

  /* ---------------- listeners ---------------- */
  function subscribe(fn) { listeners.push(fn); return function () { const i = listeners.indexOf(fn); if (i >= 0) listeners.splice(i, 1); }; }
  function emit(reason) {
    Engine.configure(effectiveConfig());
    listeners.forEach(function (fn) { try { fn(reason); } catch (e) { console.error(e); } });
  }

  /* ---------------- remote API ---------------- */
  function isRemote() { return sync.mode === 'remote'; }
  function authHeaders() {
    const h = { 'Content-Type': 'application/json' };
    if (session && session.code) h.Authorization = 'Bearer ' + session.code;
    return h;
  }
  async function api(path, opts) {
    opts = opts || {};
    const res = await fetch(sync.url.replace(/\/$/, '') + path, {
      method: opts.method || 'GET',
      headers: Object.assign(authHeaders(), opts.headers || {}),
      body: opts.body !== undefined ? JSON.stringify(opts.body) : undefined,
    });
    let data = null;
    try { data = await res.json(); } catch (e) { data = null; }
    return { ok: res.ok, status: res.status, data: data };
  }

  /* Ask the server who we are. Resolves { role } or throws. */
  async function remoteLogin(code) {
    const res = await fetch(sync.url.replace(/\/$/, '') + '/api/state', { headers: { Authorization: 'Bearer ' + code } });
    if (res.status === 401 || res.status === 403) throw new Error('That access code was not accepted.');
    if (!res.ok) throw new Error('Sync server error (' + res.status + ').');
    const data = await res.json();
    if (data.state) { state = normalize(data.state); lsSet(KEYS.state, state); }
    sync.lastPull = nowISO(); sync.error = null; sync.ai = !!data.ai;
    return { role: data.role };
  }

  async function pull(opts) {
    if (!isRemote() || !session || !session.code) return false;
    sync.busy++;
    try {
      const r = await api('/api/state');
      if (r.status === 401 || r.status === 403) { sync.error = 'Access code rejected'; return false; }
      if (!r.ok || !r.data || !r.data.state) { sync.error = 'Server error ' + r.status; return false; }
      sync.ai = !!r.data.ai;
      const incoming = normalize(r.data.state);
      sync.lastPull = nowISO(); sync.error = null; sync.online = true;
      if (!state || incoming.version !== state.version || (opts && opts.force)) {
        state = incoming; lsSet(KEYS.state, state); emit('pull');
      }
      return true;
    } catch (e) {
      sync.error = 'Offline — showing the last copy'; sync.online = false;
      return false;
    } finally { sync.busy--; }
  }

  /* ---------------- mutate ---------------- */
  async function mutate(fn, opts) {
    opts = opts || {};
    if (!isRemote()) {
      const draft = clone(state);
      fn(draft);
      draft.version = (state.version || 0) + 1;
      draft.updatedAt = nowISO();
      state = draft;
      lsSet(KEYS.state, state);
      emit(opts.reason || 'mutate');
      return state;
    }
    sync.busy++;
    try {
      let lastErr = null;
      for (let attempt = 0; attempt < 4; attempt++) {
        const draft = clone(state);
        fn(draft);
        draft.version = (state.version || 0) + 1;
        draft.updatedAt = nowISO();
        const r = await api('/api/state', { method: 'PUT', body: { baseVersion: state.version || 0, state: draft } });
        if (r.status === 409 && r.data && r.data.state) { state = normalize(r.data.state); lsSet(KEYS.state, state); continue; }
        if (r.status === 401 || r.status === 403) throw new Error((r.data && r.data.error) || 'Not allowed');
        if (!r.ok) { lastErr = new Error((r.data && r.data.error) || ('Server error ' + r.status)); break; }
        state = normalize(r.data.state || draft);
        lsSet(KEYS.state, state);
        sync.error = null; sync.online = true;
        emit(opts.reason || 'mutate');
        return state;
      }
      throw lastErr || new Error('Could not save — the schedule changed while you were editing. Try again.');
    } finally { sync.busy--; }
  }

  /* ---------------- photos ---------------- */
  function openDb() {
    return new Promise(function (resolve, reject) {
      if (typeof indexedDB === 'undefined') return reject(new Error('no indexedDB'));
      const req = indexedDB.open('lga-schedule', 1);
      req.onupgradeneeded = function () { req.result.createObjectStore('photos'); };
      req.onsuccess = function () { resolve(req.result); };
      req.onerror = function () { reject(req.error); };
    });
  }
  async function idbPut(id, dataUrl) {
    const db = await openDb();
    return new Promise(function (resolve, reject) {
      const tx = db.transaction('photos', 'readwrite');
      tx.objectStore('photos').put(dataUrl, id);
      tx.oncomplete = function () { resolve(true); };
      tx.onerror = function () { reject(tx.error); };
    });
  }
  async function idbGet(id) {
    const db = await openDb();
    return new Promise(function (resolve, reject) {
      const tx = db.transaction('photos', 'readonly');
      const req = tx.objectStore('photos').get(id);
      req.onsuccess = function () { resolve(req.result || null); };
      req.onerror = function () { reject(req.error); };
    });
  }
  async function savePhoto(id, dataUrl) {
    if (isRemote()) {
      const r = await api('/api/photos/' + encodeURIComponent(id), { method: 'PUT', body: { dataUrl: dataUrl } });
      if (!r.ok) throw new Error('Photo upload failed (' + r.status + ')');
      try { await idbPut(id, dataUrl); } catch (e) { /* cache only */ }
      return true;
    }
    try { await idbPut(id, dataUrl); return true; }
    catch (e) { return lsSet(KEYS.photoPrefix + id, dataUrl); }
  }
  async function getPhoto(id) {
    if (!id) return null;
    let local = null;
    try { local = await idbGet(id); } catch (e) { local = lsGet(KEYS.photoPrefix + id, null); }
    if (local) return local;
    if (isRemote()) {
      const r = await api('/api/photos/' + encodeURIComponent(id));
      if (r.ok && r.data && r.data.dataUrl) { try { await idbPut(id, r.data.dataUrl); } catch (e) { /* ignore */ } return r.data.dataUrl; }
    }
    return null;
  }

  /* ---------------- AI photo reading (optional, remote only) ---------------- */
  function aiAvailable() { return isRemote() && sync.ai; }
  async function parsePhoto(dataUrl, hints) {
    const r = await api('/api/parse', { method: 'POST', body: { dataUrl: dataUrl, hints: hints || {} } });
    if (!r.ok) throw new Error((r.data && r.data.error) || ('Could not read the photo (' + r.status + ')'));
    return r.data;
  }

  /* ---------------- session ---------------- */
  function getSession() { return session; }
  function isSupervisor() { return !!(session && session.role === 'supervisor'); }
  function currentEmployeeId() { return session && session.employeeId ? session.employeeId : null; }
  function setSession(s) { session = s; lsSet(KEYS.session, s); }
  function clearSession() { session = null; lsDel(KEYS.session); }

  /* Local-mode login. */
  function loginLocal(o) {
    if (o.role === 'supervisor') {
      const pin = String((state.settings && state.settings.supervisorPin) || APP_CONFIG.defaultSupervisorPin);
      if (String(o.pin || '') !== pin) throw new Error('Wrong PIN.');
      setSession({ role: 'supervisor', employeeId: o.employeeId || null, name: o.name || 'Supervisor', at: nowISO() });
    } else {
      setSession({ role: 'worker', employeeId: o.employeeId, name: o.name, at: nowISO() });
    }
    return session;
  }
  /* Remote-mode login: the server decides the role from the access code. */
  async function loginRemote(o) {
    const who = await remoteLogin(o.code);
    if (o.role === 'supervisor' && who.role !== 'supervisor') throw new Error('That code is a worker code, not a supervisor code.');
    const role = who.role === 'supervisor' && o.role === 'supervisor' ? 'supervisor' : 'worker';
    setSession({ role: role, employeeId: o.employeeId || null, name: o.name || (role === 'supervisor' ? 'Supervisor' : ''), code: o.code, at: nowISO() });
    emit('login');
    return session;
  }

  /* ---------------- prefs ---------------- */
  function getPref(k, d) { return prefs[k] === undefined ? d : prefs[k]; }
  function setPref(k, v) { prefs[k] = v; lsSet(KEYS.prefs, prefs); }

  /* ---------------- backup ---------------- */
  function exportJSON() { return JSON.stringify({ exportedAt: nowISO(), app: APP_CONFIG.appName, state: state }, null, 2); }
  async function importJSON(text) {
    const parsed = JSON.parse(text);
    const incoming = normalize(parsed.state || parsed);
    await mutate(function (d) {
      ['roster', 'rotation', 'holidays', 'exceptions', 'requests', 'settings'].forEach(function (k) { d[k] = incoming[k]; });
    }, { reason: 'import' });
  }
  async function resetAll() {
    await mutate(function (d) { Object.assign(d, defaultState()); }, { reason: 'reset' });
  }

  /* ---------------- init ---------------- */
  function startPolling() {
    if (pollTimer) clearInterval(pollTimer);
    if (!isRemote()) return;
    pollTimer = setInterval(function () { if (document.visibilityState === 'visible') pull(); }, Math.max(15, APP_CONFIG.pollIntervalSec || 45) * 1000);
    document.addEventListener('visibilitychange', function () { if (document.visibilityState === 'visible') pull(); });
    window.addEventListener('online', function () { pull(); });
  }

  async function init() {
    prefs = lsGet(KEYS.prefs, {}) || {};
    state = normalize(lsGet(KEYS.state, null) || defaultState());
    session = lsGet(KEYS.session, null);
    sync.url = (APP_CONFIG.syncUrl || '').trim();
    sync.mode = sync.url ? 'remote' : 'local';
    if (isRemote() && session && !session.code) session = null;   // stale local session
    if (!isRemote() && session && session.code) session = null;   // stale remote session
    Engine.configure(effectiveConfig());
    if (isRemote() && session) { await pull(); startPolling(); }
    return state;
  }

  return {
    init: init, get: function () { return state; }, subscribe: subscribe, mutate: mutate, effectiveConfig: effectiveConfig,
    isRemote: isRemote, sync: sync, pull: pull, startPolling: startPolling,
    savePhoto: savePhoto, getPhoto: getPhoto, aiAvailable: aiAvailable, parsePhoto: parsePhoto,
    getSession: getSession, setSession: setSession, clearSession: clearSession, isSupervisor: isSupervisor, currentEmployeeId: currentEmployeeId,
    loginLocal: loginLocal, loginRemote: loginRemote,
    getPref: getPref, setPref: setPref,
    exportJSON: exportJSON, importJSON: importJSON, resetAll: resetAll, defaultState: defaultState,
  };
})();

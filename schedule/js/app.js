/* =====================================================================
 *  app.js — bootstrap, login screen, tab routing, event delegation
 * ===================================================================== */
const App = (function () {
  'use strict';
  const esc = UI.esc;
  const TABS = ['calendar', 'team', 'requests', 'rotation', 'info'];

  const UI_VERSION = 5;   // bump to reset saved tab/view once after a big change
  const state = {
    tab: 'team',
    viewingId: null,
    cal: { y: 2026, m: 1 },
    selectedDate: null,
    teamDate: null,
    teamMode: 'month',
    teamLayout: 'official',
    formView: null,
    reqFilter: 'pending',
    rotSlot: 1,
    rotWeekStart: null,
    rotEdit: null,
    newReq: null,
    showInactive: false,
    login: { role: 'worker', employeeId: null },
  };

  const viewEl = document.getElementById('view');
  const tabbar = document.getElementById('tabbar');
  const chipEl = document.getElementById('userChip');
  const badgeEl = document.getElementById('reqBadge');
  const brandSub = document.getElementById('brandSub');

  /* ---------------- helpers used by views ---------------- */
  function viewingEmployeeId() {
    const s = Store.getSession();
    if (state.viewingId && Engine.getEmployee(state.viewingId)) return state.viewingId;
    if (s && s.employeeId && Engine.getEmployee(s.employeeId)) return s.employeeId;
    return null;
  }
  function setViewing(id) { state.viewingId = id; Store.setPref('viewingId', id); render(); }
  function setMonth(y, m) { state.cal = { y: y, m: m }; Store.setPref('calMonth', y + '-' + (m < 10 ? '0' : '') + m); render(); }
  function setTab(tab) {
    if (TABS.indexOf(tab) === -1) tab = 'calendar';
    state.tab = tab; Store.setPref('tab', tab);
    state.formView = null;
    UI.closeSheet();
    render();
    window.scrollTo(0, 0);
  }
  function go(tab, params) {
    params = params || {};
    if (params.newRequest) Views.requests.startNew(params.newRequest);
    setTab(tab);
  }

  /* ---------------- rendering ---------------- */
  function render() {
    const sess = Store.getSession();
    if (!sess) { renderLogin(); return; }
    const v = Views[state.tab] || Views.calendar;
    viewEl.innerHTML = '';
    try { v.render(viewEl); }
    catch (e) { console.error(e); viewEl.innerHTML = '<div class="card"><p>Something went wrong drawing this screen.</p><pre style="white-space:pre-wrap;font-size:.75rem">' + esc(e.stack || e.message) + '</pre></div>'; }
    UI.qsa(tabbar, 'button').forEach(function (b) { b.classList.toggle('active', b.dataset.tab === state.tab); });
    updateChrome();
  }
  function updateChrome() {
    const sess = Store.getSession();
    tabbar.hidden = !sess;
    chipEl.hidden = !sess;
    if (!sess) { brandSub.textContent = 'Sign in'; return; }
    const pending = (Store.get().requests || []).filter(function (r) {
      if (r.status !== 'pending') return false;
      if (Store.isSupervisor()) return true;
      return sess.employeeId && ((r.employees || []).indexOf(sess.employeeId) !== -1 || r.createdBy === sess.employeeId);
    }).length;
    badgeEl.hidden = !pending;
    badgeEl.textContent = pending;
    const sync = Store.sync;
    const dot = Store.isRemote() ? (sync.error ? 'err' : '') : 'off';
    chipEl.innerHTML = '<span class="sync-dot ' + dot + '" title="' + esc(Store.isRemote() ? (sync.error || 'Synced') : 'Local mode') + '"></span>' +
      '<span class="who">' + esc(sess.name || 'Supervisor') + '<span class="role">' + esc(sess.role) + '</span></span>' +
      '<button type="button" data-action="signout" aria-label="Sign out" title="Sign out">⎋</button>';
    brandSub.textContent = Store.isRemote() ? (sync.error ? 'Offline copy' : 'Shared schedule') : 'This device only';
  }

  /* ---------------- login ---------------- */
  function renderLogin() {
    tabbar.hidden = true; chipEl.hidden = true; brandSub.textContent = 'Sign in';
    const L = state.login;
    const remote = Store.isRemote();
    const people = Engine.rosterSorted().filter(function (e) { return !e.vacant; });
    let h = '<div class="login"><div class="logo">📅</div><h1>' + esc(APP_CONFIG.appName) + '</h1><p class="sub">' + (remote ? 'Shared schedule — enter your access code' : 'Who is using this phone?') + '</p>';
    h += '<div class="seg grow" style="margin-bottom:12px"><button type="button" class="' + (L.role === 'worker' ? 'active' : '') + '" data-action="role" data-role="worker">I’m a worker</button>' +
      '<button type="button" class="' + (L.role === 'supervisor' ? 'active' : '') + '" data-action="role" data-role="supervisor">Supervisor</button></div>';
    if (L.role === 'worker') {
      h += '<div class="who-list">';
      let lastGroup = null;
      people.forEach(function (e) {
        const g = Engine.groupCodeOf(e);
        if (g !== lastGroup) { const gi = Engine.groupInfo(g); h += '<div class="who-group">' + esc(g) + (gi && gi.label ? ' · ' + esc(gi.label) : '') + '</div>'; lastGroup = g; }
        h += '<button type="button" class="' + (L.employeeId === e.id ? 'on' : '') + '" data-action="pick-me" data-id="' + esc(e.id) + '">' + esc(e.name) + '<small>' + esc(Engine.shiftCode(e)) + '</small></button>';
      });
      h += '</div>';
      if (remote) h += '<label class="field" style="margin-top:12px">Worker access code<input class="input" type="password" data-input="code" autocomplete="off" value="' + esc(L.code || '') + '"></label>';
      h += '<button type="button" class="btn btn-primary btn-block" style="margin-top:12px" data-action="login"' + (L.employeeId ? '' : ' disabled') + '>Continue</button>';
    } else {
      h += '<label class="field">' + (remote ? 'Supervisor access code' : 'Supervisor PIN') + '<input class="input" type="password" inputmode="' + (remote ? 'text' : 'numeric') + '" data-input="code" autocomplete="off" value="' + esc(L.code || '') + '"></label>';
      h += '<label class="field" style="margin-top:8px">Your name (shown on approvals)<input class="input" data-input="supname" value="' + esc(L.supname || 'Supervisor') + '"></label>';
      h += '<button type="button" class="btn btn-primary btn-block" style="margin-top:12px" data-action="login">Continue</button>';
      if (!remote) h += '<p class="muted" style="font-size:.78rem;text-align:center;margin-top:8px">Default PIN is ' + esc(APP_CONFIG.defaultSupervisorPin) + ' until you change it under Info.</p>';
    }
    h += '<p class="muted" style="font-size:.78rem;text-align:center;margin-top:16px">' + (remote ? 'Codes are given out by the supervisor.' : 'No account needed — this only sets which schedule you see first.') + '</p></div>';
    viewEl.innerHTML = h;
  }

  const loginActions = {
    role: function (b) { state.login.role = b.dataset.role; render(); },
    'pick-me': function (b) { state.login.employeeId = b.dataset.id; render(); },
    login: function () { doLogin(); },
  };
  async function doLogin() {
    const L = state.login;
    try {
      if (L.role === 'worker') {
        const e = Engine.getEmployee(L.employeeId);
        if (!e) return UI.toast('Pick your name first', 'error');
        if (Store.isRemote()) await Store.loginRemote({ role: 'worker', employeeId: e.id, name: e.name, code: (L.code || '').trim() });
        else Store.loginLocal({ role: 'worker', employeeId: e.id, name: e.name });
        state.viewingId = e.id;
      } else {
        if (Store.isRemote()) await Store.loginRemote({ role: 'supervisor', name: (L.supname || 'Supervisor').trim(), code: (L.code || '').trim() });
        else Store.loginLocal({ role: 'supervisor', pin: (L.code || '').trim(), name: (L.supname || 'Supervisor').trim() });
        state.reqFilter = 'pending';
      }
      L.code = '';
      if (Store.isRemote()) Store.startPolling();
      state.tab = Store.isSupervisor() && (Store.get().requests || []).some(function (r) { return r.status === 'pending'; }) ? 'requests' : 'team';
      render();
    } catch (e) { UI.toast(e.message, 'error'); }
  }
  function logout() {
    UI.closeSheet();
    Store.clearSession();
    state.viewingId = null; state.newReq = null; state.rotEdit = null; state.formView = null; state.login = { role: 'worker', employeeId: null };
    render();
  }

  /* ---------------- events ---------------- */
  function dispatch(kind, target, ev) {
    const name = target.dataset[kind];
    if (!Store.getSession()) {
      if (kind === 'action' && loginActions[name]) { ev.preventDefault(); loginActions[name](target, ev); }
      else if (kind === 'input') state.login[name] = target.value;
      return;
    }
    if (kind === 'action' && name === 'signout') { logout(); return; }
    const v = Views[state.tab];
    const table = v && (kind === 'action' ? v.actions : kind === 'change' ? v.changes : v.inputs);
    const fn = table && table[name];
    if (fn) { if (kind === 'action') ev.preventDefault(); fn(target, ev); }
  }
  function wireEvents() {
    viewEl.addEventListener('click', function (ev) {
      const t = ev.target.closest('[data-action]');
      if (t && viewEl.contains(t)) dispatch('action', t, ev);
    });
    viewEl.addEventListener('change', function (ev) {
      const t = ev.target.closest('[data-change]');
      if (t) dispatch('change', t, ev);
    });
    viewEl.addEventListener('input', function (ev) {
      const t = ev.target.closest('[data-input]');
      if (t) dispatch('input', t, ev);
    });
    viewEl.addEventListener('keydown', function (ev) {
      if (ev.key === 'Enter' && !Store.getSession() && ev.target.matches('input')) { ev.preventDefault(); doLogin(); }
    });
    chipEl.addEventListener('click', function (ev) { if (ev.target.closest('[data-action="signout"]')) logout(); });
    tabbar.addEventListener('click', function (ev) {
      const b = ev.target.closest('button[data-tab]');
      if (b) setTab(b.dataset.tab);
    });
    document.addEventListener('keydown', function (ev) { if (ev.key === 'Escape') UI.closeSheet(); });
  }

  /* ---------------- init ---------------- */
  async function init() {
    try {
      await Store.init();
    } catch (e) { console.error(e); }
    const today = Engine.todayISO();
    const savedMonth = Store.getPref('calMonth', null);
    if (savedMonth && /^\d{4}-\d{2}$/.test(savedMonth)) state.cal = { y: Number(savedMonth.slice(0, 4)), m: Number(savedMonth.slice(5, 7)) };
    else state.cal = { y: Number(today.slice(0, 4)), m: Number(today.slice(5, 7)) };
    state.teamDate = today;
    state.teamMode = Store.getPref('teamMode', 'month');
    state.teamLayout = Store.getPref('teamLayout', 'official');
    state.rotSlot = Store.getPref('rotSlot', 1);
    state.viewingId = Store.getPref('viewingId', null);
    state.sheetFit = Store.getPref('sheetFit', true);
    if (Store.getPref('uiVersion', 0) !== UI_VERSION) {
      Store.setPref('uiVersion', UI_VERSION); Store.setPref('tab', 'team'); Store.setPref('teamMode', 'month'); Store.setPref('teamLayout', 'official');
      state.teamMode = 'month'; state.teamLayout = 'official';
    }
    const savedTab = Store.getPref('tab', 'team');
    state.tab = TABS.indexOf(savedTab) !== -1 ? savedTab : 'team';
    const sess = Store.getSession();
    if (sess && !Store.isSupervisor() && sess.employeeId) { state.reqFilter = 'mine'; if (!state.viewingId) state.viewingId = sess.employeeId; }
    if (sess && sess.employeeId && !Engine.getEmployee(sess.employeeId)) Store.clearSession();
    if (!Engine.getEmployee(state.rotSlot === undefined ? null : null)) { /* no-op */ }
    const emp = sess && Engine.getEmployee(sess.employeeId);
    if (emp) state.rotSlot = Number(emp.slot);
    wireEvents();
    Store.subscribe(function () { render(); });
    render();
    /* Keep "today" fresh if the app stays open past midnight. */
    setInterval(function () { if (Engine.todayISO() !== today && document.visibilityState === 'visible') location.reload(); }, 60000);
  }

  return {
    state: state, init: init, render: render, setTab: setTab, go: go,
    viewingEmployeeId: viewingEmployeeId, setViewing: setViewing, setMonth: setMonth, logout: logout,
  };
})();

document.addEventListener('DOMContentLoaded', function () { App.init(); });

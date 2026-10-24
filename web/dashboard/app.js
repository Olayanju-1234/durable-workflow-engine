/* workflowd dashboard — vanilla JS, no framework, no build step.
   Talks to the JSON API in internal/server/httpapi.go on the same origin. */

'use strict';

const REFRESH_MS = 2000;

const state = {
  filter: '',
  q: '',
  selected: null,   // workflow id shown in the drawer
  autoRefresh: true,
  timer: null,
};

/* ---------- tiny helpers ---------- */

const $ = (id) => document.getElementById(id);

// All workflow ids, types, errors and outputs are untrusted input: never
// interpolate them into innerHTML without escaping.
function esc(v) {
  return String(v ?? '').replace(/[&<>"']/g, (c) => (
    { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
  ));
}

function fmtTime(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  if (isNaN(d)) return '—';
  const now = Date.now();
  const diff = (now - d.getTime()) / 1000;
  // A timer's deadline is usually ahead of us.
  const ahead = diff < 0;
  const abs = Math.abs(diff);
  const say = (n, unit) => (ahead ? `in ${n}${unit}` : `${n}${unit} ago`);
  if (abs < 60) return say(Math.floor(abs), 's');
  if (abs < 3600) return say(Math.floor(abs / 60), 'm');
  if (abs < 86400) return say(Math.floor(abs / 3600), 'h');
  return d.toLocaleString();
}

// An engine started with --auth-token-file wants a bearer token. Ask once per
// page load; keep it in sessionStorage so it goes away with the tab.
const TOKEN_KEY = 'workflowd-token';
let apiToken = '';
try { apiToken = sessionStorage.getItem(TOKEN_KEY) || ''; } catch { /* storage blocked */ }
let tokenPrompted = false;

function setToken(token) {
  apiToken = token;
  try {
    if (token) sessionStorage.setItem(TOKEN_KEY, token);
    else sessionStorage.removeItem(TOKEN_KEY);
  } catch { /* storage blocked: keep it in memory only */ }
}

async function api(path, options) {
  const send = (token) => fetch(path, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
  });
  const sent = apiToken;
  let res = await send(sent);
  if (res.status === 401) {
    if (sent && sent === apiToken) setToken(''); // wrong or rotated: ask again on the next load
    if (apiToken && apiToken !== sent) {
      res = await send(apiToken); // a concurrent request was just given one
    } else if (!tokenPrompted) {
      tokenPrompted = true;
      const token = (window.prompt('This workflowd requires an API token:') || '').trim();
      if (token) {
        setToken(token);
        res = await send(token);
      }
    }
  }
  const text = await res.text();
  const body = text ? JSON.parse(text) : {};
  if (!res.ok) throw new Error(body.error || `HTTP ${res.status}`);
  return body;
}

let toastTimer = null;
function toast(msg, kind) {
  const t = $('toast');
  t.textContent = msg;
  t.dataset.kind = kind || 'info';
  t.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { t.hidden = true; }, 3200);
}

function setConn(stateName, text) {
  const c = $('conn');
  c.dataset.state = stateName;
  $('conn-text').textContent = text;
}

/* ---------- inline SVG assets ---------- */

const MARK_SVG = `
<svg class="mark" viewBox="0 0 32 32" aria-hidden="true">
  <rect x="1.5" y="1.5" width="29" height="7" rx="2.5" class="mk-weft"/>
  <rect x="1.5" y="12.5" width="29" height="7" rx="2.5" class="mk-weft"/>
  <rect x="1.5" y="23.5" width="29" height="7" rx="2.5" class="mk-weft"/>
  <rect x="1.5" y="1.5" width="7" height="29" rx="2.5" class="mk-warp"/>
  <rect x="12.5" y="1.5" width="7" height="29" rx="2.5" class="mk-warp"/>
  <rect x="23.5" y="1.5" width="7" height="29" rx="2.5" class="mk-warp"/>
  <rect x="1.5" y="1.5" width="7" height="7" class="mk-weft"/>
  <rect x="23.5" y="1.5" width="7" height="7" class="mk-weft"/>
  <rect x="12.5" y="12.5" width="7" height="7" class="mk-weft"/>
  <rect x="1.5" y="23.5" width="7" height="7" class="mk-weft"/>
  <rect x="23.5" y="23.5" width="7" height="7" class="mk-weft"/>
</svg>`;

// Step-kind glyphs, drawn as thread-work.
const GLYPHS = {
  ACTIVITY: `<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round"><path d="M1.5 8 Q8 2.5 14.5 8 Q8 13.5 1.5 8 Z"/><circle cx="8" cy="8" r="1.4"/></svg>`,
  TIMER: `<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"><circle cx="8" cy="8" r="6"/><path d="M8 4.5 V8 L10.5 10"/></svg>`,
  SIGNAL: `<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M3.5 14.5 V1.5"/><path d="M3.5 2.5 H12.5 L10 5.25 L12.5 8 H3.5"/></svg>`,
  MARKER: `<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round"><path d="M4 1.5 H12 V14.5 L8 11 L4 14.5 Z"/></svg>`,
};

const EMPTY_LOOM = `
<svg viewBox="0 0 220 90" width="220" height="90" aria-hidden="true" style="display:block;margin:0 auto 14px;opacity:.55">
  <g fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round">
    <path d="M30 12 V78 M190 12 V78"/>
    <path d="M22 12 H198 M22 66 H198" />
    <path d="M52 20 V64 M76 20 V64 M100 20 V64 M124 20 V64 M148 20 V64 M172 20 V64" stroke-dasharray="1 6" opacity=".9"/>
    <path d="M40 84 H180" stroke-dasharray="2 7" opacity=".55"/>
  </g>
</svg>`;

/* ---------- list + stats ---------- */

async function refresh() {
  try {
    const qs = state.filter ? `?status=${encodeURIComponent(state.filter)}` : '';
    // Stats always describe the whole set, so fetch unfiltered too when filtering.
    const [filtered, all] = await Promise.all([
      api(`/api/workflows${qs}`),
      state.filter ? api('/api/workflows') : null,
    ]);
    const everything = (all || filtered).workflows || [];
    renderStats(everything);
    renderChipCounts(everything);
    renderRows(filtered.workflows || []);
    setConn('ok', 'connected');
    if (state.selected) await loadDetail(state.selected, true);
  } catch (err) {
    setConn('error', 'disconnected');
    renderEmpty(`Cannot reach the engine — ${esc(err.message)}`);
  }
}

function renderStats(list) {
  const count = (...s) => list.filter((w) => s.includes(w.status)).length;
  $('stat-total').textContent = list.length;
  $('stat-active').textContent = count('RUNNING', 'WAITING');
  $('stat-completed').textContent = count('COMPLETED');
  $('stat-failed').textContent = count('FAILED', 'TERMINATED', 'TIMED_OUT', 'UNKNOWN');
}

function renderChipCounts(list) {
  const counts = {
    '': list.length,
    RUNNING: list.filter((w) => w.status === 'RUNNING').length,
    WAITING: list.filter((w) => w.status === 'WAITING').length,
    COMPLETED: list.filter((w) => w.status === 'COMPLETED').length,
    FAILED: list.filter((w) => w.status === 'FAILED').length,
    TERMINATED: list.filter((w) => w.status === 'TERMINATED').length,
    CANCELLED: list.filter((w) => w.status === 'CANCELLED').length,
  };
  for (const el of document.querySelectorAll('.chip-count')) {
    el.textContent = counts[el.dataset.count] ?? '';
  }
}

function matchesSearch(w) {
  if (!state.q) return true;
  const hay = `${w.workflow_id} ${w.workflow_type || ''}`.toLowerCase();
  return hay.includes(state.q);
}

function renderEmpty(msg) {
  $('rows').innerHTML = `<tr class="empty"><td colspan="6">${msg}</td></tr>`;
}

function renderRows(list) {
  const visible = list.filter(matchesSearch);
  if (!visible.length) {
    renderEmpty(state.q
      ? `Nothing on the floor matches “${esc(state.q)}”.`
      : state.filter
        ? `No workflows with status ${esc(state.filter)}.`
        : `${EMPTY_LOOM}Nothing on the loom yet. Mount a warp to begin.`);
    return;
  }
  $('rows').innerHTML = visible.map((w) => `
    <tr data-id="${esc(w.workflow_id)}" data-s="${esc(w.status)}" tabindex="0" role="button"
        aria-label="Inspect ${esc(w.workflow_id)}">
      <td class="wf-id">${esc(w.workflow_id)}</td>
      <td class="wf-type">${esc(w.workflow_type) || '<span class="muted">—</span>'}</td>
      <td><span class="badge" data-s="${esc(w.status)}">${esc(w.status)}</span></td>
      <td class="num">${esc(w.history_length)}</td>
      <td class="muted">${esc(fmtTime(w.started_at))}</td>
      <td class="muted">${esc(fmtTime(w.updated_at))}</td>
    </tr>`).join('');

  for (const tr of $('rows').querySelectorAll('tr[data-id]')) {
    tr.addEventListener('click', () => openDrawer(tr.dataset.id));
    tr.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); openDrawer(tr.dataset.id); }
    });
  }
}

/* ---------- detail drawer ---------- */

async function openDrawer(id) {
  state.selected = id;
  $('drawer').hidden = false;
  $('scrim').hidden = false;
  await loadDetail(id, false);
}

function closeDrawer() {
  state.selected = null;
  $('drawer').hidden = true;
  $('scrim').hidden = true;
}

async function loadDetail(id, quiet) {
  try {
    const w = await api(`/api/workflows/${encodeURIComponent(id)}`);
    renderDetail(w);
  } catch (err) {
    if (!quiet) toast(err.message, 'error');
  }
}

function renderDetail(w) {
  $('d-id').textContent = w.workflow_id;
  $('d-type').textContent = w.workflow_type || 'unknown type';
  $('d-status').innerHTML = `<span class="badge" data-s="${esc(w.status)}">${esc(w.status)}</span>`;
  $('d-started').textContent = fmtTime(w.started_at);
  $('d-updated').textContent = fmtTime(w.updated_at);
  $('d-events').textContent = w.history_length;
  // A workflow that continues as new keeps its id; each run gets a new run id.
  const short = (id) => (id || '').slice(0, 8);
  $('d-run').textContent = w.run_id
    ? `${short(w.run_id)}${w.continued_from_run_id ? ` · continued from ${short(w.continued_from_run_id)}` : ''}`
    : '—';
  $('d-run').title = w.run_id || '';

  $('d-error-wrap').hidden = !w.error;
  if (w.error) $('d-error').textContent = w.error;

  $('d-input').textContent = w.input || '—';
  $('d-output-wrap').hidden = !w.output;
  if (w.output) $('d-output').textContent = w.output;

  const steps = w.steps || [];
  $('d-steps').innerHTML = steps.length
    ? steps.map(renderStep).join('')
    : `<li class="pick"><div class="pick-rail"></div><div class="pick-body muted" style="font-style:italic;font-family:var(--serif)">The loom is threaded but no picks have been made yet.</div></li>`;

  // Only a live workflow can be signalled, cancelled or terminated.
  const live = w.status === 'RUNNING' || w.status === 'WAITING';
  $('d-actions').hidden = !live;
}

function renderStep(s) {
  const retried = s.attempts > 1
    ? `<span class="tag retry">${esc(s.attempts)} attempts</span>` : '';
  const errType = s.error_type ? `<span class="tag" title="error type">${esc(s.error_type)}</span>` : '';
  const waitingFor = s.signal_name
    ? `<span class="step-meta">parked until “${esc(s.signal_name)}” arrives</span>` : '';
  const fireAt = s.fire_at && s.status !== 'CANCELLED'
    ? `<span class="step-meta">pick fires ${esc(fmtTime(s.fire_at))}</span>` : '';
  const err = s.error ? `<div class="step-body"><pre class="err">${esc(s.error)}</pre></div>` : '';
  const out = (!s.error && s.output)
    ? `<div class="step-body"><pre class="io">${esc(s.output)}</pre></div>` : '';
  const glyph = GLYPHS[s.kind] || '';

  return `
    <li class="pick" data-s="${esc(s.status)}">
      <div class="pick-rail">
        <span class="pick-no">${esc(s.sequence)}</span>
        <span class="pick-glyph" title="${esc(s.kind)}">${glyph}</span>
      </div>
      <div class="pick-body">
        <div class="pick-thread" aria-hidden="true"></div>
        <div class="pick-head">
          <span class="pick-name">${esc(s.step_name)}</span>
          <span class="tag">${esc(s.kind)}</span>
          <span class="badge" data-s="${esc(s.status)}">${esc(s.status)}</span>
          ${retried}${errType}
        </div>
        ${waitingFor}${fireAt}${err}${out}
      </div>
    </li>`;
}

/* ---------- actions ---------- */

async function terminate() {
  const id = state.selected;
  if (!id) return;
  if (!confirm(`Cut the warp on ${id}? This durably fails the workflow and cannot be undone.`)) return;
  try {
    await api(`/api/workflows/${encodeURIComponent(id)}/terminate`, {
      method: 'POST',
      body: JSON.stringify({ reason: $('term-reason').value }),
    });
    toast('Warp cut — workflow terminated');
    $('term-reason').value = '';
    await refresh();
  } catch (err) {
    toast(err.message, 'error');
  }
}

async function cancelWorkflow(graceful) {
  const id = state.selected;
  if (!id) return;
  const ask = graceful
    ? `Ask ${id} to cancel? Its code is told and may compensate before it ends.`
    : `Cancel ${id}? It ends as CANCELLED, and workers running its activities are told to stop.`;
  if (!confirm(ask)) return;
  try {
    await api(`/api/workflows/${encodeURIComponent(id)}/cancel`, {
      method: 'POST',
      body: JSON.stringify({ reason: $('term-reason').value, graceful }),
    });
    toast(graceful ? 'Cancel requested — the workflow is winding down' : 'Workflow cancelled');
    $('term-reason').value = '';
    await refresh();
  } catch (err) {
    toast(err.message, 'error');
  }
}

async function sendSignal() {
  const id = state.selected;
  const name = $('sig-name').value.trim();
  if (!id || !name) { toast('Signal name is required', 'error'); return; }
  try {
    await api(`/api/workflows/${encodeURIComponent(id)}/signal`, {
      method: 'POST',
      body: JSON.stringify({ name, payload: $('sig-payload').value }),
    });
    toast(`Shuttle passed — “${name}” delivered`);
    $('sig-name').value = ''; $('sig-payload').value = '';
    await refresh();
  } catch (err) {
    toast(err.message, 'error');
  }
}

async function startWorkflow(e) {
  e.preventDefault();
  const type = $('s-type').value.trim();
  if (!type) { toast('Workflow type is required', 'error'); return; }
  const payload = { type };
  const id = $('s-id').value.trim();
  if (id) payload.workflow_id = id;
  const input = $('s-input').value;
  if (input !== '') payload.input = input;
  try {
    const res = await api('/api/workflows', {
      method: 'POST',
      body: JSON.stringify(payload),
    });
    toast(`Warp mounted — ${res.workflow_id}`, 'success');
    closeStartModal();
    await refresh();
  } catch (err) {
    toast(err.message, 'error');
  }
}

// On a node running the demo worker, point visitors at a workflow that will
// actually move, and at the input that shows the saga compensating.
async function loadNodeInfo() {
  try {
    const health = await api('/api/health');
    if (!health.demo_worker) return;
    if (!$('s-type').value) $('s-type').value = 'OrderSaga';
    $('s-input').placeholder = 'order-42 (or order-42-decline)';
    $('s-hint').textContent = 'A demo worker runs reserve, charge and ship on this node. '
      + 'Start an OrderSaga and watch it weave. Put "decline" in the input to see the saga release the reservation.';
  } catch {
    // Health is best-effort here; the dialog works without it.
  }
}

function openStartModal() {
  $('start-modal').hidden = false;
  $('modal-scrim').hidden = false;
  $('s-type').focus();
}

function closeStartModal() {
  $('start-modal').hidden = true;
  $('modal-scrim').hidden = true;
}

/* ---------- wiring ---------- */

function scheduleRefresh() {
  clearInterval(state.timer);
  if (state.autoRefresh) state.timer = setInterval(refresh, REFRESH_MS);
}

const THEME_ORDER = ['auto', 'light', 'dark'];
const THEME_LABEL = { auto: '◐ auto', light: '○ light', dark: '● dark' };

function initTheme() {
  const saved = localStorage.getItem('workflowd-theme') || 'auto';
  document.documentElement.dataset.theme = saved;
  $('theme').textContent = THEME_LABEL[saved] || THEME_LABEL.auto;
  $('theme').addEventListener('click', () => {
    const cur = document.documentElement.dataset.theme;
    const next = THEME_ORDER[(THEME_ORDER.indexOf(cur) + 1) % THEME_ORDER.length];
    document.documentElement.dataset.theme = next;
    localStorage.setItem('workflowd-theme', next);
    $('theme').textContent = THEME_LABEL[next];
    toast(`Theme: ${next}`);
  });
}

function init() {
  $('brand-mark').innerHTML = MARK_SVG;
  $('drawer-mark').innerHTML = MARK_SVG;
  initTheme();

  for (const chip of $('filters').querySelectorAll('.chip')) {
    chip.querySelector('.chip-count').dataset.count = chip.dataset.status;
    chip.addEventListener('click', () => {
      $('filters').querySelector('.is-active')?.classList.remove('is-active');
      chip.classList.add('is-active');
      state.filter = chip.dataset.status;
      refresh();
    });
  }

  let searchTimer = null;
  $('q').addEventListener('input', (e) => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => {
      state.q = e.target.value.trim().toLowerCase();
      refresh();
    }, 160);
  });

  $('refresh').addEventListener('click', refresh);
  $('new-wf').addEventListener('click', openStartModal);
  $('s-cancel').addEventListener('click', closeStartModal);
  $('modal-scrim').addEventListener('click', closeStartModal);
  $('start-form').addEventListener('submit', startWorkflow);

  $('d-close').addEventListener('click', closeDrawer);
  $('scrim').addEventListener('click', closeDrawer);
  $('signal-form').addEventListener('submit', (e) => { e.preventDefault(); sendSignal(); });
  $('terminate-form').addEventListener('submit', (e) => { e.preventDefault(); terminate(); });
  $('d-cancel').addEventListener('click', () => cancelWorkflow(false));
  $('d-request-cancel').addEventListener('click', () => cancelWorkflow(true));

  document.addEventListener('keydown', (e) => {
    if (e.key !== 'Escape') return;
    if (!$('start-modal').hidden) { closeStartModal(); return; }
    closeDrawer();
  });

  $('autorefresh').addEventListener('change', (e) => {
    state.autoRefresh = e.target.checked;
    scheduleRefresh();
  });

  refresh();
  scheduleRefresh();
  loadNodeInfo();
}

document.addEventListener('DOMContentLoaded', init);

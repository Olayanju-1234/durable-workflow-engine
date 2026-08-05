/* workflowd dashboard — vanilla JS, no framework, no build step.
   Talks to the JSON API in internal/server/httpapi.go on the same origin. */

'use strict';

const REFRESH_MS = 2000;

const state = {
  filter: '',
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
  if (diff < 60) return `${Math.max(0, Math.floor(diff))}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return d.toLocaleString();
}

async function api(path, options) {
  const res = await fetch(path, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
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

function renderEmpty(msg) {
  $('rows').innerHTML = `<tr class="empty"><td colspan="6">${msg}</td></tr>`;
}

function renderRows(list) {
  if (!list.length) {
    renderEmpty(state.filter
      ? `No workflows with status ${esc(state.filter)}.`
      : 'No workflows yet. Start one to see it here.');
    return;
  }
  $('rows').innerHTML = list.map((w) => `
    <tr data-id="${esc(w.workflow_id)}">
      <td class="wf-id">${esc(w.workflow_id)}</td>
      <td>${esc(w.workflow_type) || '<span class="muted">—</span>'}</td>
      <td><span class="badge" data-s="${esc(w.status)}">${esc(w.status)}</span></td>
      <td class="num">${esc(w.history_length)}</td>
      <td class="muted">${esc(fmtTime(w.started_at))}</td>
      <td class="muted">${esc(fmtTime(w.updated_at))}</td>
    </tr>`).join('');

  for (const tr of $('rows').querySelectorAll('tr[data-id]')) {
    tr.addEventListener('click', () => openDrawer(tr.dataset.id));
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

  $('d-error-wrap').hidden = !w.error;
  if (w.error) $('d-error').textContent = w.error;

  $('d-input').textContent = w.input || '—';
  $('d-output-wrap').hidden = !w.output;
  if (w.output) $('d-output').textContent = w.output;

  const steps = w.steps || [];
  $('d-steps').innerHTML = steps.length
    ? steps.map(renderStep).join('')
    : '<li class="muted" style="border-left-color:transparent">No steps scheduled yet.</li>';

  // Only a live workflow can be signalled or terminated.
  const live = w.status === 'RUNNING' || w.status === 'WAITING';
  $('d-actions').hidden = !live;
}

function renderStep(s) {
  const retried = s.attempts > 1
    ? `<span class="tag retry">${esc(s.attempts)} attempts</span>` : '';
  const waitingFor = s.signal_name
    ? `<span class="step-meta">waiting for “${esc(s.signal_name)}”</span>` : '';
  const fireAt = s.fire_at
    ? `<span class="step-meta">fires ${esc(fmtTime(s.fire_at))}</span>` : '';
  const err = s.error ? `<div class="step-body"><pre class="err">${esc(s.error)}</pre></div>` : '';
  const out = (!s.error && s.output)
    ? `<div class="step-body"><pre class="io">${esc(s.output)}</pre></div>` : '';

  return `
    <li data-s="${esc(s.status)}">
      <div class="step-head">
        <span class="step-name">${esc(s.sequence)}. ${esc(s.step_name)}</span>
        <span class="tag">${esc(s.kind)}</span>
        <span class="badge" data-s="${esc(s.status)}">${esc(s.status)}</span>
        ${retried}
      </div>
      ${waitingFor}${fireAt}${err}${out}
    </li>`;
}

/* ---------- actions ---------- */

async function terminate() {
  const id = state.selected;
  if (!id) return;
  if (!confirm(`Terminate ${id}? This durably fails the workflow and cannot be undone.`)) return;
  try {
    await api(`/api/workflows/${encodeURIComponent(id)}/terminate`, {
      method: 'POST',
      body: JSON.stringify({ reason: $('term-reason').value }),
    });
    toast('Workflow terminated');
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
    toast(`Signal “${name}” delivered`);
    $('sig-name').value = ''; $('sig-payload').value = '';
    await refresh();
  } catch (err) {
    toast(err.message, 'error');
  }
}

/* ---------- wiring ---------- */

function scheduleRefresh() {
  clearInterval(state.timer);
  if (state.autoRefresh) state.timer = setInterval(refresh, REFRESH_MS);
}

function initTheme() {
  const saved = localStorage.getItem('workflowd-theme') || 'auto';
  document.documentElement.dataset.theme = saved;
  $('theme').addEventListener('click', () => {
    const order = ['auto', 'light', 'dark'];
    const next = order[(order.indexOf(document.documentElement.dataset.theme) + 1) % 3];
    document.documentElement.dataset.theme = next;
    localStorage.setItem('workflowd-theme', next);
    toast(`Theme: ${next}`);
  });
}

function init() {
  initTheme();

  for (const chip of $('filters').querySelectorAll('.chip')) {
    chip.addEventListener('click', () => {
      $('filters').querySelector('.is-active')?.classList.remove('is-active');
      chip.classList.add('is-active');
      state.filter = chip.dataset.status;
      refresh();
    });
  }

  $('refresh').addEventListener('click', refresh);
  $('d-close').addEventListener('click', closeDrawer);
  $('scrim').addEventListener('click', closeDrawer);
  $('d-terminate').addEventListener('click', terminate);
  $('d-signal').addEventListener('click', sendSignal);
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closeDrawer(); });

  $('autorefresh').addEventListener('change', (e) => {
    state.autoRefresh = e.target.checked;
    scheduleRefresh();
  });

  refresh();
  scheduleRefresh();
}

document.addEventListener('DOMContentLoaded', init);

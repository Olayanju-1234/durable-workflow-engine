/* workflowd dashboard — vanilla JS, no framework, no build step.
   Talks to the JSON API in internal/server/httpapi.go on the same origin, and
   follows the engine live over GET /api/events (server-sent events). */

'use strict';

const LIST_LIMIT = 200;        // rows per page of the floor
const POLL_MS = 5000;          // when the engine (or a proxy) cannot stream
const TIME_TICK_MS = 15000;    // how often "12s ago" cells are redrawn

const state = {
  filters: { status: '', type: '', since: '', q: '' },
  rows: new Map(),   // workflow id -> its row, for the rows on show
  limit: LIST_LIMIT,
  total: 0,
  counts: {},
  types: [],
  live: true,
  selected: null,    // workflow id open in the drawer
  run: '',           // the run on show ('' = the current one)
  tab: 'weave',
  detail: null,      // the last describe of the selected run
  chain: [],         // its events, as shown in the card chain
  chainFor: '',      // `${id}|${run}` the chain belongs to
  picked: new Set(), // workflow ids selected for a bulk action
};

const LIVE = ['RUNNING', 'WAITING'];
const FINISHED = ['COMPLETED', 'FAILED', 'TERMINATED', 'CANCELLED', 'TIMED_OUT'];

/* ---------- tiny helpers ---------- */

const $ = (id) => document.getElementById(id);

// All workflow ids, types, errors and outputs are untrusted input: never
// interpolate them into innerHTML without escaping.
function esc(v) {
  return String(v ?? '').replace(/[&<>"']/g, (c) => (
    { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
  ));
}

const enc = encodeURIComponent;

function debounce(fn, ms) {
  let t = null;
  return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
}

function fmtTime(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  if (isNaN(d)) return '—';
  const diff = (Date.now() - d.getTime()) / 1000;
  // A timer's deadline is usually ahead of us.
  const ahead = diff < 0;
  const abs = Math.abs(diff);
  const say = (n, unit) => (ahead ? `in ${n}${unit}` : `${n}${unit} ago`);
  if (abs < 60) return say(Math.floor(abs), 's');
  if (abs < 3600) return say(Math.floor(abs / 60), 'm');
  if (abs < 86400) return say(Math.floor(abs / 3600), 'h');
  return d.toLocaleString();
}

function fmtAbs(iso) {
  const d = new Date(iso);
  return isNaN(d) ? '' : d.toLocaleString();
}

// A duration in milliseconds, as a person reads it.
function fmtDur(ms) {
  if (ms < 1000) return `${Math.max(0, Math.round(ms))}ms`;
  const s = ms / 1000;
  if (s < 60) return `${s.toFixed(s < 10 ? 2 : 1)}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${String(Math.floor(s % 60)).padStart(2, '0')}s`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ${String(m % 60).padStart(2, '0')}m`;
  return `${Math.floor(h / 24)}d ${h % 24}h`;
}

// Protobuf JSON carries 64-bit nanoseconds as strings.
function nanosToMs(v) {
  return v ? Number(v) / 1e6 : 0;
}

function sinceTime(since) {
  const m = /^(\d+)([mhd])$/.exec(since || '');
  if (!m) return '';
  const unit = { m: 60e3, h: 3600e3, d: 86400e3 }[m[2]];
  return new Date(Date.now() - Number(m[1]) * unit).toISOString();
}

/* ---------- payloads: shown readably ---------- */

function b64ToBytes(b64) {
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

// Colours a pretty-printed JSON text; every piece is escaped on the way.
function highlightJSON(src) {
  const re = /("(?:\\.|[^"\\])*")(\s*:)?|\b(true|false|null)\b|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)/g;
  let out = '';
  let last = 0;
  let m;
  while ((m = re.exec(src))) {
    out += esc(src.slice(last, m.index));
    if (m[1]) out += m[2] ? `<span class="j-key">${esc(m[1])}</span>${esc(m[2])}` : `<span class="j-str">${esc(m[1])}</span>`;
    else if (m[3]) out += `<span class="j-lit">${m[3]}</span>`;
    else out += `<span class="j-num">${m[4]}</span>`;
    last = re.lastIndex;
  }
  return out + esc(src.slice(last));
}

// payloadHTML renders a payload given as text, or as base64 bytes (b64): JSON
// pretty-printed, other text as is, and bytes that are not UTF-8 as hex.
function payloadHTML(text, b64) {
  if (b64 != null && b64 !== '') {
    const bytes = b64ToBytes(b64);
    try {
      text = new TextDecoder('utf-8', { fatal: true }).decode(bytes);
    } catch {
      const hex = Array.from(bytes.slice(0, 256), (b) => b.toString(16).padStart(2, '0')).join(' ');
      return `<pre class="io bin">${esc(hex)}${bytes.length > 256 ? ' …' : ''}</pre><p class="payload-note">${bytes.length} bytes, not text</p>`;
    }
  }
  if (text == null || text === '') return '<p class="payload-note">empty</p>';
  const t = text.trim();
  if (t.startsWith('{') || t.startsWith('[')) {
    try {
      return `<pre class="io json">${highlightJSON(JSON.stringify(JSON.parse(t), null, 2))}</pre>`;
    } catch { /* not JSON after all */ }
  }
  return `<pre class="io">${esc(text)}</pre>`;
}

/* ---------- the API ---------- */

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

// The namespace every call works in ('' = the one the token grants). A
// ?namespace= link picks it; otherwise the tab remembers the last choice.
const NS_KEY = 'workflowd-namespace';
let namespace = new URLSearchParams(location.search).get('namespace') || '';
if (!namespace) {
  try { namespace = sessionStorage.getItem(NS_KEY) || ''; } catch { /* storage blocked */ }
}

function headers(path, token) {
  return {
    'Content-Type': 'application/json',
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
    ...(namespace && path !== '/api/health' ? { 'X-Workflowd-Namespace': namespace } : {}),
  };
}

async function api(path, options) {
  const send = (token) => fetch(path, { ...options, headers: headers(path, token) });
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
  let body = {};
  try { body = text ? JSON.parse(text) : {}; } catch { body = { error: text.slice(0, 200) }; }
  if (!res.ok) {
    const err = new Error(body.error || `HTTP ${res.status}`);
    err.status = res.status;
    throw err;
  }
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

/* ---------- routing: a URL per view ---------- */

// The floor is "/", a workflow "/workflows/<id>"; filters, the run and the
// tab ride in the query, so any view can be linked to or reloaded.
function routeFromLocation() {
  const m = /^\/workflows\/(.+)$/.exec(location.pathname);
  const p = new URLSearchParams(location.search);
  let id = null;
  if (m) {
    try { id = decodeURIComponent(m[1]); } catch { id = m[1]; }
  }
  return {
    id,
    run: p.get('run') || '',
    tab: p.get('tab') || 'weave',
    filters: { status: p.get('status') || '', type: p.get('type') || '', since: p.get('since') || '', q: p.get('q') || '' },
  };
}

function urlFor(id, run, tab) {
  const p = new URLSearchParams();
  if (namespace) p.set('namespace', namespace);
  for (const [k, v] of Object.entries(state.filters)) if (v) p.set(k, v);
  if (id && run) p.set('run', run);
  if (id && tab && tab !== 'weave') p.set('tab', tab);
  const qs = p.toString();
  return (id ? `/workflows/${enc(id)}` : '/') + (qs ? `?${qs}` : '');
}

function go(replace) {
  const url = urlFor(state.selected, state.run, state.tab);
  if (url === location.pathname + location.search) return;
  if (replace) history.replaceState(null, '', url);
  else history.pushState(null, '', url);
}

function applyRoute(r, initial) {
  const changed = JSON.stringify(r.filters) !== JSON.stringify(state.filters);
  state.filters = r.filters;
  syncFilterControls();
  if (changed || initial) loadFloor();
  if (r.id) {
    if (r.id !== state.selected || r.run !== state.run) openWorkflow(r.id, { run: r.run, tab: r.tab, push: false });
    else if (r.tab !== state.tab) selectTab(r.tab, false);
  } else if (state.selected) {
    closeWorkflow(false);
  }
}

/* ---------- the floor: list, counts, filters ---------- */

let floorSeq = 0;

function listQuery(limit) {
  const p = new URLSearchParams({ sort: '-started', limit: String(limit) });
  const f = state.filters;
  if (f.status) p.set('status', f.status);
  if (f.type) p.set('type', f.type);
  if (f.q) p.set('q', f.q);
  const after = sinceTime(f.since);
  if (after) p.set('started_after', after);
  return p.toString();
}

async function loadFloor() {
  const seq = ++floorSeq;
  try {
    const res = await api(`/api/workflows?${listQuery(state.limit)}`);
    if (seq !== floorSeq) return; // a newer load has the floor
    state.rows = new Map((res.workflows || []).map((w) => [w.workflow_id, w]));
    for (const id of state.picked) if (!state.rows.has(id)) state.picked.delete(id);
    takeCounts(res);
    renderFloor();
    if (stream.mode === 'polling') setConn('ok', 'polling');
  } catch (err) {
    if (seq !== floorSeq) return;
    setConn('error', 'disconnected');
    renderEmpty(`Cannot reach the engine — ${esc(err.message)}`);
  }
}

function takeCounts(res) {
  state.total = res.total ?? state.rows.size;
  state.counts = res.counts || {};
  state.types = res.types || [];
  renderCounts();
  renderTypes();
  renderFloorNote();
}

// Counts move with every change, but cheaply: one small request, at most
// once a second.
const refreshCounts = debounce(async () => {
  try {
    takeCounts(await api(`/api/workflows?${listQuery(1)}`));
  } catch { /* the stream or the next load will tell */ }
}, 1000);

function renderCounts() {
  const c = state.counts;
  const n = (...s) => s.reduce((sum, k) => sum + (c[k] || 0), 0);
  const all = Object.values(c).reduce((a, b) => a + b, 0);
  $('stat-total').textContent = all;
  $('stat-active').textContent = n('RUNNING', 'WAITING');
  $('stat-completed').textContent = n('COMPLETED');
  $('stat-failed').textContent = n('FAILED', 'TERMINATED', 'TIMED_OUT', 'UNKNOWN');
  for (const el of document.querySelectorAll('.chip-count')) {
    el.textContent = el.dataset.count === '' ? all : (c[el.dataset.count] || '');
  }
}

function renderTypes() {
  const sel = $('f-type');
  const want = state.filters.type;
  const types = state.types.includes(want) || !want ? state.types : [...state.types, want];
  const html = `<option value="">every type</option>${types.map((t) => `<option value="${esc(t)}">${esc(t)}</option>`).join('')}`;
  if (sel.dataset.html !== html) {
    sel.innerHTML = html;
    sel.dataset.html = html;
  }
  sel.value = want;
}

function renderFloorNote() {
  const shown = state.rows.size;
  $('floor-note').textContent = state.total > shown ? `showing ${shown} of ${state.total}` : '';
  $('floor-foot').hidden = !(state.total > shown);
}

function syncFilterControls() {
  const f = state.filters;
  for (const chip of $('filters').querySelectorAll('.chip')) {
    chip.classList.toggle('is-active', chip.dataset.status === f.status);
  }
  $('f-type').value = f.type;
  $('f-since').value = f.since;
  if ($('q').value.trim().toLowerCase() !== f.q) $('q').value = f.q;
  $('f-clear').hidden = !(f.status || f.type || f.since || f.q);
}

function setFilter(patch) {
  Object.assign(state.filters, patch);
  state.limit = LIST_LIMIT;
  syncFilterControls();
  go(true);
  loadFloor();
}

// matches says whether a row belongs on the floor under the current filters.
function matches(w) {
  const f = state.filters;
  if (f.status && w.status !== f.status) return false;
  if (f.type && w.workflow_type !== f.type) return false;
  if (f.q && !`${w.workflow_id} ${w.workflow_type || ''}`.toLowerCase().includes(f.q)) return false;
  const after = sinceTime(f.since);
  if (after && (!w.started_at || w.started_at < after)) return false;
  return true;
}

function renderEmpty(msg) {
  $('rows').innerHTML = `<tr class="empty"><td colspan="7">${msg}</td></tr>`;
  renderBulkBar();
}

function emptyMessage() {
  const f = state.filters;
  if (f.q) return `Nothing on the floor matches “${esc(f.q)}”.`;
  if (f.status || f.type || f.since) return 'No workflows match these filters.';
  return `${EMPTY_LOOM}Nothing on the loom yet. Mount a warp to begin.`;
}

function rowCells(w) {
  const href = urlFor(w.workflow_id, '', 'weave');
  const picked = state.picked.has(w.workflow_id) ? ' checked' : '';
  return `
      <td class="sel"><input type="checkbox" class="row-sel"${picked} aria-label="Select ${esc(w.workflow_id)}"></td>
      <td class="wf-id"><a href="${esc(href)}" tabindex="-1">${esc(w.workflow_id)}</a></td>
      <td class="wf-type">${esc(w.workflow_type) || '<span class="muted">—</span>'}</td>
      <td><span class="badge" data-s="${esc(w.status)}">${esc(w.status)}</span></td>
      <td class="num">${esc(w.history_length)}</td>
      <td class="muted" data-t="${esc(w.started_at || '')}" title="${esc(fmtAbs(w.started_at))}">${esc(fmtTime(w.started_at))}</td>
      <td class="muted" data-t="${esc(w.updated_at || '')}" title="${esc(fmtAbs(w.updated_at))}">${esc(fmtTime(w.updated_at))}</td>`;
}

function buildRow(w) {
  const tr = document.createElement('tr');
  tr.dataset.id = w.workflow_id;
  tr.dataset.s = w.status;
  tr.tabIndex = 0;
  tr.setAttribute('aria-label', `Inspect ${w.workflow_id}`);
  tr.innerHTML = rowCells(w);
  tr.classList.toggle('is-selected', w.workflow_id === state.selected);
  return tr;
}

function rowEl(id) {
  return $('rows').querySelector(`tr[data-id="${CSS.escape(id)}"]`);
}

function renderFloor() {
  const list = [...state.rows.values()];
  if (!list.length) {
    renderEmpty(emptyMessage());
    renderFloorNote();
    return;
  }
  const body = $('rows');
  body.replaceChildren(...list.map(buildRow));
  renderFloorNote();
  renderBulkBar();
}

// keepAnchor runs fn without moving what the reader is looking at: rows
// inserted or removed above the viewport shift the page back by as much.
function keepAnchor(fn) {
  if (window.scrollY <= 0) { fn(); return; }
  const rows = [...$('rows').querySelectorAll('tr[data-id]')];
  const ref = rows.find((tr) => tr.getBoundingClientRect().bottom > 80);
  const before = ref?.getBoundingClientRect().top;
  fn();
  if (ref && ref.isConnected) window.scrollBy(0, ref.getBoundingClientRect().top - before);
}

function flash(tr, cls) {
  tr.classList.remove(cls);
  void tr.offsetWidth; // restart the animation
  tr.classList.add(cls);
}

// applyChange folds one live change into the floor: the row is updated in
// place, added where it sorts, or removed, and nothing else is redrawn.
function applyChange(w) {
  const id = w.workflow_id;
  if (id === state.selected) refreshDetailSoon();
  refreshCounts();
  const had = state.rows.has(id);
  const tr = rowEl(id);
  if (w.deleted || !matches(w)) {
    if (had) {
      state.rows.delete(id);
      state.picked.delete(id);
      renderBulkBar();
      keepAnchor(() => tr?.remove());
      if (!state.rows.size) renderEmpty(emptyMessage());
      renderFloorNote();
    }
    return;
  }
  if (had) {
    const old = state.rows.get(id);
    state.rows.set(id, w);
    if (tr) {
      tr.dataset.s = w.status;
      tr.innerHTML = rowCells(w);
      if (old.status !== w.status) flash(tr, 'is-changed');
    }
    return;
  }
  // A row this view has not shown: place it by start time, newest first.
  const rows = [...state.rows.values()];
  const at = rows.findIndex((r) => (r.started_at || '') < (w.started_at || ''));
  if (at < 0 && rows.length >= state.limit) return; // beyond this page
  const next = new Map();
  rows.forEach((r, i) => {
    if (i === at) next.set(id, w);
    next.set(r.workflow_id, r);
  });
  if (at < 0) next.set(id, w);
  state.rows = next;
  const row = buildRow(w);
  keepAnchor(() => {
    const body = $('rows');
    body.querySelector('tr.empty')?.remove();
    const before = at < 0 ? null : rowEl(rows[at].workflow_id);
    body.insertBefore(row, before);
  });
  flash(row, 'is-new');
  renderFloorNote();
}

// "12s ago" goes stale; redraw just those cells now and then.
function tickTimes() {
  for (const td of document.querySelectorAll('td[data-t]')) {
    if (td.dataset.t) td.textContent = fmtTime(td.dataset.t);
  }
}

/* ---------- the live stream ---------- */

// EventSource cannot send an Authorization header, so the stream is read with
// fetch. If it cannot be had (an engine before 0.5.0, or a proxy that buffers
// it), the floor falls back to polling.
const stream = { ctrl: null, retry: 0, timer: null, poll: null, mode: 'off', resync: false };

function stopStream() {
  stream.ctrl?.abort();
  stream.ctrl = null;
  clearTimeout(stream.timer);
  clearInterval(stream.poll);
  stream.poll = null;
}

async function startStream() {
  stopStream();
  if (!state.live) {
    stream.mode = 'paused';
    setConn('paused', 'paused');
    return;
  }
  const ctrl = new AbortController();
  stream.ctrl = ctrl;
  let res;
  try {
    res = await fetch('/api/events', { headers: headers('/api/events', apiToken), signal: ctrl.signal });
  } catch {
    if (!ctrl.signal.aborted) retryStream(ctrl);
    return;
  }
  if (res.status === 404) { startPolling(false); return; } // no stream on this engine
  if (res.status === 401 && !tokenPrompted) {
    try { await api('/api/namespaces'); } catch { /* the prompt was declined */ }
    if (stream.ctrl === ctrl) startStream();
    return;
  }
  if (!res.ok || !res.body) { retryStream(ctrl); return; }
  stream.retry = 0;
  stream.mode = 'live';
  setConn('ok', 'live');
  const reader = res.body.pipeThrough(new TextDecoderStream()).getReader();
  let buf = '';
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      buf += value.replace(/\r\n?/g, '\n');
      let i;
      while ((i = buf.indexOf('\n\n')) >= 0) {
        handleEvent(buf.slice(0, i));
        buf = buf.slice(i + 2);
      }
    }
  } catch { /* the connection dropped */ }
  if (!ctrl.signal.aborted) retryStream(ctrl);
}

function retryStream(ctrl) {
  if (stream.ctrl !== ctrl) return;
  stream.retry++;
  stream.resync = true; // whatever happened meanwhile was missed
  if (stream.retry > 4) { startPolling(true); return; }
  setConn('connecting', 'reconnecting…');
  stream.timer = setTimeout(startStream, Math.min(1000 * 2 ** (stream.retry - 1), 10000));
}

function startPolling(retryLater) {
  stopStream();
  stream.mode = 'polling';
  setConn('ok', 'polling');
  stream.poll = setInterval(() => {
    loadFloor();
    loadSchedules();
    if (state.selected) refreshDetail();
  }, POLL_MS);
  if (retryLater) stream.timer = setTimeout(() => { stream.retry = 0; startStream(); }, 60000);
}

const reloadSchedulesSoon = debounce(() => loadSchedules(), 300);

function handleEvent(block) {
  let name = 'message';
  let data = '';
  for (const line of block.split('\n')) {
    if (!line || line.startsWith(':')) continue;
    const c = line.indexOf(':');
    const field = c < 0 ? line : line.slice(0, c);
    let val = c < 0 ? '' : line.slice(c + 1);
    if (val.startsWith(' ')) val = val.slice(1);
    if (field === 'event') name = val;
    else if (field === 'data') data += (data ? '\n' : '') + val;
  }
  let payload = {};
  try { payload = data ? JSON.parse(data) : {}; } catch { return; }
  switch (name) {
    case 'hello':
      if (stream.resync) { stream.resync = false; resyncAll(); }
      break;
    case 'workflow':
      applyChange(payload);
      break;
    case 'schedules':
      reloadSchedulesSoon();
      break;
    case 'resync':
      resyncAll();
      break;
    default:
  }
}

function resyncAll() {
  loadFloor();
  loadSchedules();
  if (state.selected) refreshDetail();
}

/* ---------- detail drawer ---------- */

function markSelectedRow() {
  for (const tr of $('rows').querySelectorAll('tr.is-selected')) tr.classList.remove('is-selected');
  if (state.selected) rowEl(state.selected)?.classList.add('is-selected');
}

async function openWorkflow(id, { run = '', tab = 'weave', push = true } = {}) {
  const same = id === state.selected && run === state.run;
  state.selected = id;
  state.run = run;
  if (!same) {
    state.detail = null;
    state.chain = [];
    state.chainFor = '';
    $('d-chain').innerHTML = '';
    $('d-runs').innerHTML = '';
    $('d-id').textContent = id;
    $('d-type').textContent = '…';
    $('drawer-body').scrollTop = 0;
  }
  selectTab(tab, false);
  if (push) go(false);
  $('drawer').hidden = false;
  $('scrim').hidden = false;
  markSelectedRow();
  await refreshDetail(true);
}

function closeWorkflow(push) {
  state.selected = null;
  state.run = '';
  state.tab = 'weave';
  $('drawer').hidden = true;
  $('scrim').hidden = true;
  markSelectedRow();
  if (push) go(false);
}

function selectTab(tab, push) {
  if (!['weave', 'chain', 'runs'].includes(tab)) tab = 'weave';
  state.tab = tab;
  for (const b of document.querySelectorAll('.tab')) {
    const on = b.dataset.tab === tab;
    b.classList.toggle('is-active', on);
    b.setAttribute('aria-selected', on ? 'true' : 'false');
  }
  for (const p of document.querySelectorAll('.tab-panel')) p.hidden = p.dataset.panel !== tab;
  if (push) go(true);
  if (state.selected && tab === 'chain') loadChain();
  if (state.selected && tab === 'runs') loadRuns();
}

const viewingCurrent = () => !state.run || state.run === state.detail?.run_id;

async function refreshDetail(loud) {
  const id = state.selected;
  const run = state.run;
  if (!id) return;
  try {
    const w = await api(`/api/workflows/${enc(id)}${run ? `?run_id=${enc(run)}` : ''}`);
    if (state.selected !== id || state.run !== run) return;
    state.detail = w;
    renderDetail(w);
    if (state.tab === 'chain') loadChain();
    if (state.tab === 'runs') loadRuns();
  } catch (err) {
    if (loud) {
      toast(err.message, 'error');
      if (err.status === 404) {
        $('d-type').textContent = 'not found in this namespace';
        $('d-status').innerHTML = '';
        $('d-steps').innerHTML = '';
        $('d-actions').hidden = true;
      }
    }
  }
}

// Live changes to the open workflow come in bursts; describe it once per burst.
const refreshDetailSoon = debounce(() => { if (viewingCurrent()) refreshDetail(false); }, 250);

function renderDetail(w) {
  $('d-id').textContent = w.workflow_id;
  $('d-type').textContent = w.workflow_type || 'unknown type';
  $('d-run-note').textContent = state.run && state.run !== '' ? ' · an earlier run' : '';
  $('d-status').innerHTML = `<span class="badge" data-s="${esc(w.status)}">${esc(w.status)}</span>`;
  $('d-started').textContent = fmtTime(w.started_at);
  $('d-started').title = fmtAbs(w.started_at);
  $('d-updated').textContent = fmtTime(w.updated_at);
  $('d-updated').title = fmtAbs(w.updated_at);
  $('d-events').textContent = w.history_length;
  // A workflow that continues as new keeps its id; each run gets a new run id.
  const short = (id) => (id || '').slice(0, 8);
  $('d-run').textContent = w.run_id
    ? `${short(w.run_id)}${w.continued_from_run_id ? ` · continued from ${short(w.continued_from_run_id)}` : ''}`
    : '—';
  $('d-run').title = w.run_id || '';
  $('d-queue').textContent = w.task_queue || 'default';

  $('d-error-wrap').hidden = !w.error;
  if (w.error) $('d-error').textContent = w.error;

  setPayload($('d-input'), w.input, w.input_base64);
  $('d-output-wrap').hidden = !(w.output || w.output_base64);
  if (w.output || w.output_base64) setPayload($('d-output'), w.output, w.output_base64);

  const steps = w.steps || [];
  const html = steps.length
    ? steps.map(renderStep).join('')
    : `<li class="pick"><div class="pick-rail"></div><div class="pick-body muted" style="font-style:italic;font-family:var(--serif)">The loom is threaded but no picks have been made yet.</div></li>`;
  if ($('d-steps').dataset.html !== html) {
    $('d-steps').innerHTML = html;
    $('d-steps').dataset.html = html;
  }

  // Only the current run of a live workflow can be signalled, cancelled or terminated;
  // only the current run of a finished one can be reset.
  const live = LIVE.includes(w.status) && viewingCurrent();
  $('d-actions').hidden = !live;
  const resettable = FINISHED.includes(w.status) && viewingCurrent() && !!w.reset_point;
  $('d-reset').hidden = !resettable;
  $('d-chain').classList.toggle('can-reset', resettable);
  if (resettable) {
    const p = w.reset_point;
    $('reset-note').textContent = `Start a new run from ${resetSummary(w)}.`;
    $('d-reset-go').textContent = p.step_name ? `Retry from #${p.sequence} ${p.step_name}` : 'Run its ending again';
  }
}

// setPayload redraws a payload only when it changed, so a live refresh keeps
// the reader's selection and scroll inside it.
function setPayload(el, text, b64) {
  const html = (text || b64) ? payloadHTML(b64 ? null : text, b64 || null) : '<p class="payload-note">none</p>';
  if (el.dataset.html === html) return;
  el.innerHTML = html;
  el.dataset.html = html;
}

function renderStep(s) {
  const retried = s.attempts > 1
    ? `<span class="tag retry">${esc(s.attempts)} attempts</span>` : '';
  const errType = s.error_type ? `<span class="tag" title="error type">${esc(s.error_type)}</span>` : '';
  const waitingFor = s.signal_name
    ? `<span class="step-meta">parked until “${esc(s.signal_name)}” arrives</span>` : '';
  const queue = s.task_queue && s.task_queue !== 'default'
    ? `<span class="step-meta">on task queue “${esc(s.task_queue)}”</span>` : '';
  const fireAt = s.fire_at && s.status !== 'CANCELLED'
    ? `<span class="step-meta">pick fires ${esc(fmtTime(s.fire_at))}</span>` : '';
  const err = s.error ? `<div class="step-body"><pre class="err">${esc(s.error)}</pre></div>` : '';
  const out = (!s.error && (s.output || s.output_base64))
    ? `<div class="step-body">${payloadHTML(s.output_base64 ? null : s.output, s.output_base64 || null)}</div>` : '';
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
        ${waitingFor}${queue}${fireAt}${err}${out}
      </div>
    </li>`;
}

/* ---------- the card chain: raw history, readably ---------- */

const EVENT_KEYS = ['event_id', 'timestamp', 'raft_index', 'min_reader_format'];

// eventKind finds an event's one attributes field (protobuf JSON names it).
function eventKind(ev) {
  const k = Object.keys(ev).find((key) => !EVENT_KEYS.includes(key));
  return [k || 'unknown', (k && ev[k]) || {}];
}

function fieldRow(label, value) {
  if (value === undefined || value === null || value === '' || value === false) return '';
  return `<div><dt>${esc(label)}</dt><dd>${value === true ? 'yes' : esc(value)}</dd></div>`;
}

function stepLabel(a) {
  return a.step_name ? `#${a.sequence || '?'} ${a.step_name}` : '';
}

function retryText(p) {
  if (!p) return '';
  const parts = [`${p.max_attempts} attempts`];
  if (p.initial_backoff_ms) parts.push(`backoff ${fmtDur(Number(p.initial_backoff_ms))}`);
  if (p.max_backoff_ms) parts.push(`max ${fmtDur(Number(p.max_backoff_ms))}`);
  if (p.non_retryable_error_types?.length) parts.push(`never retry ${p.non_retryable_error_types.join(', ')}`);
  return parts.join(' · ');
}

// describeEvent turns one event into what the card shows: a label, a tone
// (its dye), its fields, its payloads and an error, if any.
function describeEvent(kind, a, seen) {
  const ms = (v) => (v ? fmtDur(Number(v)) : '');
  switch (kind) {
    case 'workflow_started':
      return {
        label: a.continued_from_run_id ? 'Warp re-mounted (continued as new)' : 'Warp mounted', tone: 'indigo',
        fields: [['type', a.workflow_type], ['run', a.run_id], ['task queue', a.task_queue],
          ['execution timeout', ms(a.execution_timeout_ms)], ['continued from', a.continued_from_run_id]],
        payloads: [['input', a.input]],
      };
    case 'step_scheduled': {
      const again = seen.has(a.sequence);
      seen.add(a.sequence);
      if (a.awaits_signal) {
        return { label: 'Waiting for a signal', tone: 'ochre', fields: [['step', stepLabel(a)], ['signal', a.signal_name]], payloads: [['input', a.input]] };
      }
      return {
        label: again ? 'Step scheduled again (retry)' : 'Step scheduled', tone: 'ochre',
        fields: [['step', stepLabel(a)], ['task queue', a.task_queue], ['retry policy', retryText(a.retry_policy)],
          ['start-to-close', ms(a.start_to_close_timeout_ms)], ['schedule-to-close', ms(a.schedule_to_close_timeout_ms)]],
        payloads: [['input', a.input]],
      };
    }
    case 'step_completed':
      return { label: 'Step completed', tone: 'leaf', fields: [['step', stepLabel(a)]], payloads: [['output', a.output]] };
    case 'step_failed': {
      let label = 'Step failed';
      if (a.cancelled) label = 'Step cancelled';
      else if (a.will_retry) label = 'Attempt failed, will retry';
      return {
        label, tone: a.cancelled ? 'slate' : a.will_retry ? 'ochre' : 'madder',
        fields: [['step', stepLabel(a)], ['error type', a.error_type], ['attempt', a.retry_count],
          ['not retryable', a.non_retryable]],
        error: a.error,
      };
    }
    case 'timer_started':
      return {
        label: 'Timer started', tone: 'walnut',
        fields: [['step', stepLabel(a)], ['fires', a.fire_at ? new Date(nanosToMs(a.fire_at)).toLocaleString() : '']],
      };
    case 'timer_fired':
      return { label: 'Timer fired', tone: 'walnut', fields: [['step', stepLabel(a)]] };
    case 'signal_received':
      if (a.cancel_request) return { label: 'Cancel requested', tone: 'slate', fields: [], payloads: [['reason', a.payload]] };
      return {
        label: 'Signal received', tone: 'indigo', fields: [['signal', a.signal_name], ['request id', a.request_id]],
        payloads: [['payload', a.payload]],
      };
    case 'marker_recorded':
      return {
        label: { version: 'Version marker', side_effect: 'Side effect recorded', cancel: 'Cancel delivered to the code' }[a.marker] || 'Marker recorded',
        tone: 'slate', fields: [['step', stepLabel(a)], ['marker', a.marker]], payloads: [['data', a.data]],
      };
    case 'workflow_completed':
      if (a.continued_as_new) {
        const n = a.continued_as_new;
        return {
          label: 'Continued as new', tone: 'leaf', fields: [['next run', n.run_id], ['type', n.workflow_type], ['task queue', n.task_queue]],
          payloads: [['next input', n.input]],
        };
      }
      return { label: 'Workflow completed', tone: 'leaf', fields: [], payloads: [['output', a.output]] };
    case 'workflow_failed': {
      let label = 'Workflow failed';
      let tone = 'madder';
      if (a.terminated) { label = 'Warp cut (terminated)'; tone = 'walnut'; }
      if (a.timed_out) { label = 'Timed out'; tone = 'slate'; }
      if (a.cancelled) { label = 'Cancelled'; tone = 'slate'; }
      return { label, tone, fields: [], error: a.error };
    }
    default:
      return { label: kind, tone: 'slate', fields: [] };
  }
}

function eventHTML(ev, t0, seen) {
  const [kind, attrs] = eventKind(ev);
  const d = describeEvent(kind, attrs, seen);
  const at = nanosToMs(ev.timestamp);
  const iso = at ? new Date(at).toISOString() : '';
  const fields = (d.fields || []).map(([k, v]) => fieldRow(k, v)).join('');
  const payloads = (d.payloads || [])
    .filter(([, v]) => v !== undefined && v !== '')
    .map(([k, v]) => `<div class="ev-payload"><span class="ev-plabel">${esc(k)}</span>${payloadHTML(null, v)}</div>`)
    .join('');
  const error = d.error ? `<pre class="err">${esc(d.error)}</pre>` : '';
  const format = Number(ev.min_reader_format || 0) > 1
    ? `<span class="tag" title="the oldest history format that reads this event">format ${esc(ev.min_reader_format)}</span>` : '';
  return `
    <li class="card-ev" data-tone="${esc(d.tone)}">
      <div class="ev-rail"><span class="ev-no">${esc(ev.event_id)}</span></div>
      <div class="ev-body">
        <div class="ev-head">
          <span class="ev-name">${esc(d.label)}</span>
          <code class="ev-kind">${esc(kind)}</code>
          ${format}
          <span class="ev-time" title="${esc(fmtAbs(iso))}">${at && t0 ? `+${esc(fmtDur(at - t0))}` : ''}</span>
          ${Number(ev.event_id) >= 2 ? `<button class="linkish ev-reset" type="button" data-event="${esc(ev.event_id)}" title="Start a new run that keeps the cards before this one">reset to here</button>` : ''}
        </div>
        ${fields ? `<dl class="ev-fields">${fields}</dl>` : ''}
        ${error}${payloads}
        <details class="ev-raw"><summary>as recorded</summary><pre class="io">${esc(JSON.stringify(ev, null, 2))}</pre></details>
      </div>
    </li>`;
}

// loadChain appends the events the chain has not shown yet, so a live
// workflow's history grows in place without losing the reader's place.
async function loadChain() {
  const id = state.selected;
  const run = state.run;
  if (!id) return;
  let res;
  try {
    res = await api(`/api/workflows/${enc(id)}/history${run ? `?run_id=${enc(run)}` : ''}`);
  } catch (err) {
    $('chain-note').textContent = err.message;
    return;
  }
  if (state.selected !== id || state.run !== run) return;
  const events = res.events || [];
  const key = `${id}|${run}`;
  const list = $('d-chain');
  const same = state.chainFor === key && state.chain.length <= events.length &&
    (!state.chain.length || state.chain[0].timestamp === events[0]?.timestamp);
  if (!same) {
    // Another workflow, run, or a new run under the same id: start over.
    state.chainFor = key;
    state.chain = [];
    list.innerHTML = '';
  }
  const t0 = nanosToMs(events[0]?.timestamp);
  const seen = new Set();
  state.chain.forEach((ev) => {
    const [kind, a] = eventKind(ev);
    if (kind === 'step_scheduled') seen.add(a.sequence);
  });
  const fresh = events.slice(state.chain.length);
  if (fresh.length) list.insertAdjacentHTML('beforeend', fresh.map((ev) => eventHTML(ev, t0, seen)).join(''));
  state.chain = events;
  $('chain-note').textContent = events.length
    ? `${events.length} card${events.length === 1 ? '' : 's'}, oldest first: every event exactly as the engine recorded it.`
    : 'No cards.';
}

async function loadRuns() {
  const id = state.selected;
  if (!id) return;
  let runs = [];
  try {
    runs = (await api(`/api/workflows/${enc(id)}/runs`)).runs || [];
  } catch (err) {
    $('d-runs').innerHTML = `<li class="muted">${esc(err.message)}</li>`;
    return;
  }
  if (state.selected !== id) return;
  const showing = state.run || state.detail?.run_id;
  $('d-runs').innerHTML = runs.map((r) => `
    <li class="run${r.run_id === showing ? ' is-on' : ''}" data-run="${esc(r.run_id)}" data-current="${r.current ? '1' : ''}" tabindex="0">
      <code>${esc(r.run_id)}</code>
      <span class="badge" data-s="${esc(r.status)}">${esc(r.status)}</span>
      ${r.current ? '<span class="tag">current</span>' : ''}
      <span class="muted">mounted ${esc(fmtTime(r.started_at))}${r.closed_at ? ` · closed ${esc(fmtTime(r.closed_at))}` : ''}</span>
    </li>`).join('') || '<li class="muted">No runs.</li>';
  for (const li of $('d-runs').querySelectorAll('li[data-run]')) {
    const pick = () => openWorkflow(id, { run: li.dataset.current ? '' : li.dataset.run, tab: 'weave' });
    li.addEventListener('click', pick);
    li.addEventListener('keydown', (e) => { if (e.key === 'Enter') pick(); });
  }
}

/* ---------- actions ---------- */

async function terminate() {
  const id = state.selected;
  if (!id) return;
  if (!confirm(`Cut the warp on ${id}? This durably fails the workflow and cannot be undone.`)) return;
  try {
    await api(`/api/workflows/${enc(id)}/terminate`, {
      method: 'POST',
      body: JSON.stringify({ reason: $('term-reason').value }),
    });
    toast('Warp cut — workflow terminated');
    $('term-reason').value = '';
    refreshDetail(false);
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
    await api(`/api/workflows/${enc(id)}/cancel`, {
      method: 'POST',
      body: JSON.stringify({ reason: $('term-reason').value, graceful }),
    });
    toast(graceful ? 'Cancel requested — the workflow is winding down' : 'Workflow cancelled');
    $('term-reason').value = '';
    refreshDetail(false);
  } catch (err) {
    toast(err.message, 'error');
  }
}

// checkSignal validates the payload as the chosen format and says so; a
// JSON payload that does not parse cannot be sent.
function checkSignal() {
  const text = $('sig-payload').value;
  const json = $('sig-mode').value === 'json';
  const out = $('sig-check');
  let ok = true;
  if (!json) {
    out.textContent = text ? `sent as plain text (${new TextEncoder().encode(text).length} bytes)` : 'empty payload';
  } else if (!text.trim()) {
    out.textContent = 'empty payload';
  } else {
    try {
      JSON.parse(text);
      out.textContent = 'valid JSON';
    } catch (err) {
      ok = false;
      out.textContent = `not valid JSON: ${err.message}`;
    }
  }
  out.dataset.ok = ok ? '1' : '';
  $('sig-payload').setAttribute('aria-invalid', ok ? 'false' : 'true');
  $('d-signal').disabled = !ok;
  return ok;
}

async function sendSignal() {
  const id = state.selected;
  const name = $('sig-name').value.trim();
  if (!id || !name) { toast('Signal name is required', 'error'); return; }
  if (!checkSignal()) { toast('Fix the payload: it is not valid JSON (or switch to text)', 'error'); return; }
  const json = $('sig-mode').value === 'json';
  const raw = $('sig-payload').value;
  const btn = $('d-signal');
  btn.disabled = true;
  try {
    await api(`/api/workflows/${enc(id)}/signal`, {
      method: 'POST',
      body: JSON.stringify({ name, payload: json ? raw.trim() : raw }),
    });
    toast(`Shuttle passed — “${name}” delivered`);
    $('sig-name').value = ''; $('sig-payload').value = '';
    checkSignal();
    refreshDetail(false);
  } catch (err) {
    toast(err.message, 'error');
  } finally {
    btn.disabled = !checkSignal();
  }
}

// resetTo starts the open, finished run again from an event (0: the default
// point, the step whose failure ended it).
async function resetTo(eventId) {
  const id = state.selected;
  const w = state.detail;
  if (!id || !w) return;
  const point = eventId || w.reset_point?.event_id;
  const what = eventId
    ? `card ${eventId}: the new run keeps cards 1 to ${eventId - 1} and carries on from there`
    : resetSummary(w);
  if (!confirm(`Weave ${id} again from ${what}?\n\nThe ${w.status.toLowerCase()} run is kept as an earlier run.`)) return;
  try {
    const res = await api(`/api/workflows/${enc(id)}/reset`, {
      method: 'POST',
      body: JSON.stringify({ event_id: point, reason: $('reset-reason').value.trim() }),
    });
    toast(`New run ${res.run_id.slice(0, 8)} from card ${res.reset_event_id}${res.reapplied_signals ? `, ${res.reapplied_signals} signal(s) carried over` : ''}`, 'success');
    $('reset-reason').value = '';
    state.run = '';
    state.chainFor = '';
    go(true);
    refreshDetail(true);
  } catch (err) {
    toast(err.message, 'error');
  }
}

function resetSummary(w) {
  const p = w.reset_point;
  if (!p) return '';
  return p.step_name
    ? `step #${p.sequence} ${p.step_name} (card ${p.event_id}): the steps before it are not run again`
    : `card ${p.event_id}: its code decides again how to end`;
}

/* ---------- bulk actions ---------- */

const BULK = {
  request_cancel: { title: 'Request cancel', verb: 'ask to cancel', done: 'asked to cancel', needs: LIVE,
    path: (id) => `/api/workflows/${enc(id)}/cancel`, body: (reason) => ({ reason, graceful: true }) },
  cancel: { title: 'Cancel', verb: 'cancel', done: 'cancelled', needs: LIVE,
    path: (id) => `/api/workflows/${enc(id)}/cancel`, body: (reason) => ({ reason }) },
  terminate: { title: 'Cut the warps', verb: 'terminate', done: 'terminated', needs: LIVE, danger: true,
    path: (id) => `/api/workflows/${enc(id)}/terminate`, body: (reason) => ({ reason }) },
  reset: { title: 'Weave again', verb: 'reset', done: 'reset', needs: FINISHED,
    path: (id) => `/api/workflows/${enc(id)}/reset`, body: (reason) => ({ reason }) },
};

const confirmState = { action: null, ids: [], running: false };

function renderBulkBar() {
  const n = state.picked.size;
  $('bulk-bar').hidden = n === 0;
  $('bulk-count').textContent = `${n} selected`;
  const shown = [...state.rows.keys()];
  const all = $('sel-all');
  all.checked = shown.length > 0 && shown.every((id) => state.picked.has(id));
  all.indeterminate = !all.checked && shown.some((id) => state.picked.has(id));
}

function pick(id, on) {
  if (on) state.picked.add(id);
  else state.picked.delete(id);
  renderBulkBar();
}

function openConfirm(action) {
  const a = BULK[action];
  const chosen = [...state.picked];
  const ids = chosen.filter((id) => a.needs.includes(state.rows.get(id)?.status));
  const skipped = chosen.length - ids.length;
  confirmState.action = action;
  confirmState.ids = ids;
  $('confirm-title').textContent = `${a.title}: ${ids.length} workflow${ids.length === 1 ? '' : 's'}`;
  const which = a.needs === LIVE ? 'running or waiting' : 'finished';
  $('confirm-text').textContent = (ids.length
    ? `This will ${a.verb} ${ids.length} workflow${ids.length === 1 ? '' : 's'}.`
    : `None of the selected workflows can be: only ${which} ones.`)
    + (skipped ? ` ${skipped} selected ${skipped === 1 ? 'is' : 'are'} not ${which} and ${skipped === 1 ? 'is' : 'are'} left alone.` : '')
    + (action === 'terminate' ? ' Terminating cannot be undone (a reset starts a new run).' : '')
    + (action === 'reset' ? ' Each starts a new run from the step whose failure ended it; the finished runs are kept.' : '');
  const shown = ids.slice(0, 8);
  $('confirm-list').innerHTML = shown.map((id) => `<li><code>${esc(id)}</code></li>`).join('')
    + (ids.length > shown.length ? `<li class="muted">and ${ids.length - shown.length} more</li>` : '');
  $('confirm-go').textContent = ids.length ? `${a.title} (${ids.length})` : a.title;
  $('confirm-go').disabled = !ids.length;
  $('confirm-go').className = `btn ${a.danger ? 'btn-danger' : 'btn-primary'}`;
  $('confirm-progress').textContent = '';
  $('confirm-reason').value = '';
  $('confirm-modal').hidden = false;
  $('modal-scrim').hidden = false;
  $('confirm-reason').focus();
}

function closeConfirm() {
  if (confirmState.running) return;
  $('confirm-modal').hidden = true;
  $('modal-scrim').hidden = true;
}

// runBulk acts on each workflow, four at a time; every one is its own API
// call, so the engine's rate limits and read-only modes apply to each.
async function runBulk(e) {
  e.preventDefault();
  const a = BULK[confirmState.action];
  const ids = confirmState.ids;
  if (!a || !ids.length || confirmState.running) return;
  confirmState.running = true;
  $('confirm-go').disabled = true;
  const reason = $('confirm-reason').value.trim();
  const failures = [];
  let done = 0;
  let next = 0;
  const worker = async () => {
    while (next < ids.length) {
      const id = ids[next++];
      try {
        await api(a.path(id), { method: 'POST', body: JSON.stringify(a.body(reason)) });
      } catch (err) {
        failures.push(`${id}: ${err.message}`);
      }
      done++;
      $('confirm-progress').textContent = `${done} of ${ids.length} done${failures.length ? `, ${failures.length} failed` : ''}`;
    }
  };
  await Promise.all(Array.from({ length: Math.min(4, ids.length) }, worker));
  confirmState.running = false;
  const ok = ids.length - failures.length;
  if (failures.length) {
    $('confirm-progress').textContent = `${ok} ${a.done}, ${failures.length} failed — ${failures.slice(0, 3).join('; ')}${failures.length > 3 ? '…' : ''}`;
    $('confirm-go').disabled = false;
    confirmState.ids = ids.filter((id) => failures.some((f) => f.startsWith(`${id}: `)));
    $('confirm-go').textContent = `Retry the ${failures.length} that failed`;
  } else {
    closeConfirm();
    toast(`${ok} ${a.done}`, 'success');
  }
  for (const id of ids) if (!confirmState.ids.includes(id) || !failures.length) state.picked.delete(id);
  renderBulkBar();
  for (const tr of $('rows').querySelectorAll('input.row-sel')) {
    tr.checked = state.picked.has(tr.closest('tr').dataset.id);
  }
  if (stream.mode !== 'live') loadFloor();
  if (state.selected && ids.includes(state.selected)) refreshDetail(false);
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
  const queue = $('s-queue').value.trim();
  if (queue && queue !== 'default') payload.task_queue = queue;
  try {
    const res = await api('/api/workflows', {
      method: 'POST',
      body: JSON.stringify(payload),
    });
    toast(`Warp mounted — ${res.workflow_id}`, 'success');
    closeStartModal();
    if (stream.mode !== 'live') loadFloor();
    openWorkflow(res.workflow_id);
  } catch (err) {
    toast(err.message, 'error');
  }
}

/* ---------- schedules (pattern cards) ---------- */

const sched = {
  open: null,       // the schedule id whose detail row is open
  detail: null,     // that row's element, kept across refreshes so typing survives them
  readOnly: false,  // the namespace refuses schedule changes
  last: '',         // the list as last rendered
};

async function loadSchedules() {
  let list;
  try {
    list = (await api('/api/schedules')).schedules || [];
  } catch {
    return; // the floor already says when the engine is unreachable
  }
  const key = JSON.stringify(list);
  if (key === sched.last) return;
  sched.last = key;
  renderSchedules(list);
}

function renderSchedules(list) {
  const body = $('sched-rows');
  if (!list.length) {
    body.innerHTML = `<tr class="empty"><td colspan="6">No pattern cards yet. A schedule starts a workflow on a cron
      pattern: punch one above, or <code>workflowd ctl schedule create</code>.</td></tr>`;
    return;
  }
  body.innerHTML = list.map((s) => {
    const state = s.paused ? 'PAUSED' : 'ACTIVE';
    const pattern = `${esc(s.cron)}${s.timezone ? ` <span class="muted">${esc(s.timezone)}</span>` : ''}`
      + (s.jitter_ms ? ` <span class="tag" title="jitter">±${esc(fmtMs(s.jitter_ms))}</span>` : '');
    const held = s.buffered_fire_at ? ` · holding ${esc(fmtTime(s.buffered_fire_at))}` : '';
    const note = s.paused && s.note ? ` <span class="muted">— ${esc(s.note)}</span>` : '';
    return `<tr data-s="${state}" data-id="${esc(s.schedule_id)}" tabindex="0">
      <td><strong>${esc(s.schedule_id)}</strong></td>
      <td>${pattern}</td>
      <td>${esc(s.workflow_type)}${s.task_queue ? ` <span class="muted">on ${esc(s.task_queue)}</span>` : ''}</td>
      <td>${s.paused ? '<span class="muted">paused</span>' : esc(fmtTime(s.next_fire_at))}</td>
      <td>${esc((s.overlap_policy || 'ALLOW_ALL').replace('_', ' ').toLowerCase())}</td>
      <td><span class="badge" data-s="${state}">${state}</span>${held}${note}</td>
    </tr>`;
  }).join('');
  for (const tr of body.querySelectorAll('tr[data-id]')) {
    tr.addEventListener('click', () => toggleSchedule(tr.dataset.id, list.find((s) => s.schedule_id === tr.dataset.id)));
    tr.addEventListener('keydown', (e) => { if (e.key === 'Enter') tr.click(); });
  }
  const row = body.querySelector(`tr[data-id="${CSS.escape(sched.open || '')}"]`);
  if (row && sched.detail) {
    row.after(sched.detail);
    updateDetailState(list.find((s) => s.schedule_id === sched.open));
  } else {
    sched.open = null;
    sched.detail = null;
  }
}

function fmtMs(ms) {
  return ms % 60000 === 0 ? `${ms / 60000}m` : ms % 1000 === 0 ? `${ms / 1000}s` : `${ms}ms`;
}

// A datetime-local value for d (local time, to the minute).
function localInput(d) {
  const pad = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

function toggleSchedule(id, s) {
  if (sched.open === id) {
    sched.detail?.remove();
    sched.open = null;
    sched.detail = null;
    return;
  }
  sched.detail?.remove();
  sched.open = id;
  const tr = document.createElement('tr');
  tr.className = 'sched-detail-row';
  const now = new Date();
  tr.innerHTML = `<td colspan="6"><div class="sched-detail">
    <div>
      <h4>Next fires</h4>
      <ol class="sched-preview" data-role="preview"><li>…</li></ol>
    </div>
    <div data-role="controls">
      <h4>Controls</h4>
      <div class="action-row">
        <input data-role="note" placeholder="why it is paused (optional)" aria-label="Pause note">
        <button class="btn btn-secondary" data-role="toggle" type="button"></button>
      </div>
      <h4>Backfill a past range</h4>
      <div class="action-row">
        <input type="datetime-local" data-role="from" aria-label="Backfill from" value="${localInput(new Date(now - 864e5))}">
        <input type="datetime-local" data-role="to" aria-label="Backfill to" value="${localInput(now)}">
        <select data-role="overlap" aria-label="Backfill overlap">
          <option value="ALLOW_ALL">start every fire</option>
          <option value="SKIP">only while none is running</option>
        </select>
        <button class="btn" data-role="backfill" type="button">Backfill</button>
      </div>
    </div>
    <p class="muted" data-role="readonly" hidden>Schedules are read-only in this namespace.</p>
  </div></td>`;
  tr.querySelector('[data-role="toggle"]').addEventListener('click', () => toggleSchedulePause(id));
  tr.querySelector('[data-role="backfill"]').addEventListener('click', () => backfillSchedule(id));
  sched.detail = tr;
  document.querySelector(`#sched-rows tr[data-id="${CSS.escape(id)}"]`).after(tr);
  updateDetailState(s);
  loadPreview(id);
}

function updateDetailState(s) {
  if (!sched.detail || !s) return;
  const btn = sched.detail.querySelector('[data-role="toggle"]');
  btn.textContent = s.paused ? 'Resume' : 'Pause';
  btn.dataset.paused = s.paused ? '1' : '';
  sched.detail.querySelector('[data-role="note"]').hidden = !!s.paused;
  sched.detail.querySelector('[data-role="controls"]').hidden = sched.readOnly;
  sched.detail.querySelector('[data-role="readonly"]').hidden = !sched.readOnly;
}

function renderPreview(el, res) {
  const fires = res.fires || [];
  el.innerHTML = fires.length
    ? fires.map((f) => `<li>${esc(new Date(f.fire_at).toLocaleString())}${f.start_at !== f.fire_at
      ? ` <span class="muted">→ starts ${esc(new Date(f.start_at).toLocaleTimeString())}</span>` : ''}</li>`).join('')
    : '<li>never fires again</li>';
  if (res.paused) el.insertAdjacentHTML('afterbegin', '<li class="note">Paused: these happen once it is resumed.</li>');
}

async function loadPreview(id) {
  const el = sched.detail?.querySelector('[data-role="preview"]');
  if (!el) return;
  try {
    renderPreview(el, await api(`/api/schedules/${encodeURIComponent(id)}/preview?count=5`));
  } catch (err) {
    el.innerHTML = `<li>${esc(err.message)}</li>`;
  }
}

async function toggleSchedulePause(id) {
  const btn = sched.detail.querySelector('[data-role="toggle"]');
  const resume = btn.dataset.paused === '1';
  try {
    await api(`/api/schedules/${encodeURIComponent(id)}/${resume ? 'resume' : 'pause'}`, {
      method: 'POST',
      body: JSON.stringify(resume ? {} : { note: sched.detail.querySelector('[data-role="note"]').value.trim() }),
    });
    toast(resume ? `Resumed ${id}` : `Paused ${id}`, 'success');
    sched.last = '';
    await loadSchedules();
    loadPreview(id);
  } catch (err) {
    toast(err.message, 'error');
  }
}

async function backfillSchedule(id) {
  const q = (r) => sched.detail.querySelector(`[data-role="${r}"]`).value;
  const from = new Date(q('from'));
  const to = new Date(q('to'));
  if (isNaN(from) || isNaN(to)) { toast('Pick a start and an end', 'error'); return; }
  try {
    const res = await api(`/api/schedules/${encodeURIComponent(id)}/backfill`, {
      method: 'POST',
      body: JSON.stringify({ start_time: from.toISOString(), end_time: to.toISOString(), overlap_policy: q('overlap') }),
    });
    toast(`Backfill: ${res.started} started, ${res.existing} already there, ${res.skipped} skipped`, 'success');
    loadFloor();
  } catch (err) {
    toast(err.message, 'error');
  }
}

function scheduleForm() {
  const v = (id) => $(id).value.trim();
  const body = {
    schedule_id: v('sc-id'), cron: v('sc-cron'), timezone: v('sc-tz'), workflow_type: v('sc-type'),
    input: $('sc-input').value, task_queue: v('sc-queue'), overlap_policy: $('sc-overlap').value,
    jitter_ms: Math.max(0, Math.round(Number($('sc-jitter').value || 0) * 1000)),
  };
  if (body.task_queue === 'default') body.task_queue = '';
  return body;
}

async function previewScheduleForm() {
  const el = $('sc-preview');
  el.hidden = false;
  try {
    renderPreview(el, await api('/api/schedules/preview', { method: 'POST', body: JSON.stringify({ ...scheduleForm(), count: 5 }) }));
  } catch (err) {
    el.innerHTML = `<li>${esc(err.message)}</li>`;
  }
}

async function createSchedule(e) {
  e.preventDefault();
  try {
    await api('/api/schedules', { method: 'POST', body: JSON.stringify(scheduleForm()) });
    toast(`Card punched: ${$('sc-id').value.trim()}`, 'success');
    $('sched-form').hidden = true;
    $('sched-form').reset();
    $('sc-preview').hidden = true;
    sched.last = '';
    loadSchedules();
  } catch (err) {
    toast(err.message, 'error');
  }
}


// The namespace picker shows only when the caller can use more than one.
async function loadNamespaces() {
  let info;
  try {
    info = await api('/api/namespaces');
  } catch (err) {
    if (namespace && /HTTP 40[13]|forbidden|token/i.test(err.message)) {
      // A remembered namespace this token cannot use: fall back to its own.
      namespace = '';
      try { sessionStorage.removeItem(NS_KEY); } catch { /* storage blocked */ }
      return loadNamespaces();
    }
    return;
  }
  const names = info.namespaces || [];
  const sel = $('ns');
  sel.innerHTML = names.map((n) => `<option value="${esc(n)}">${esc(n)}</option>`).join('');
  sel.value = info.current;
  sched.readOnly = !!info.read_only_schedules;
  $('sched-new').hidden = sched.readOnly;
  // Shown whenever there is a choice, or the view is not the default one.
  $('ns-pick').hidden = names.length < 2 && info.current === 'default';
}

function switchNamespace(next) {
  namespace = next;
  state.picked.clear();
  try { sessionStorage.setItem(NS_KEY, next); } catch { /* storage blocked */ }
  closeWorkflow(false);
  state.filters.type = ''; // types differ between namespaces
  state.limit = LIST_LIMIT;
  syncFilterControls();
  go(true);
  sched.open = null;
  sched.detail = null;
  sched.last = '';
  loadNamespaces();
  loadFloor();
  loadSchedules();
  startStream();
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

const THEME_ORDER = ['auto', 'light', 'dark'];
const THEME_LABEL = { auto: '◐ auto', light: '○ light', dark: '● dark' };

function initTheme() {
  let saved = 'auto';
  try { saved = localStorage.getItem('workflowd-theme') || 'auto'; } catch { /* storage blocked */ }
  document.documentElement.dataset.theme = saved;
  $('theme').textContent = THEME_LABEL[saved] || THEME_LABEL.auto;
  $('theme').addEventListener('click', () => {
    const cur = document.documentElement.dataset.theme;
    const next = THEME_ORDER[(THEME_ORDER.indexOf(cur) + 1) % THEME_ORDER.length];
    document.documentElement.dataset.theme = next;
    try { localStorage.setItem('workflowd-theme', next); } catch { /* storage blocked */ }
    $('theme').textContent = THEME_LABEL[next];
    toast(`Theme: ${next}`);
  });
}

function onRowActivate(e) {
  const tr = e.target.closest('tr[data-id]');
  if (!tr || e.target.closest('td.sel')) return; // the checkbox picks, it does not open
  // Let the id's link open a new tab as links do.
  if (e.type === 'click' && (e.metaKey || e.ctrlKey || e.shiftKey || e.button === 1)) return;
  if (e.type === 'keydown' && e.key !== 'Enter' && e.key !== ' ') return;
  e.preventDefault();
  openWorkflow(tr.dataset.id);
}

function init() {
  $('brand-mark').innerHTML = MARK_SVG;
  $('drawer-mark').innerHTML = MARK_SVG;
  initTheme();

  for (const chip of $('filters').querySelectorAll('.chip')) {
    chip.querySelector('.chip-count').dataset.count = chip.dataset.status;
    chip.addEventListener('click', () => setFilter({ status: chip.dataset.status }));
  }
  $('f-type').addEventListener('change', (e) => setFilter({ type: e.target.value }));
  $('f-since').addEventListener('change', (e) => setFilter({ since: e.target.value }));
  $('f-clear').addEventListener('click', () => setFilter({ status: '', type: '', since: '', q: '' }));
  const search = debounce((v) => setFilter({ q: v }), 250);
  $('q').addEventListener('input', (e) => search(e.target.value.trim().toLowerCase()));
  $('more').addEventListener('click', () => { state.limit += LIST_LIMIT; loadFloor(); });

  $('rows').addEventListener('click', onRowActivate);
  $('rows').addEventListener('keydown', onRowActivate);
  $('rows').addEventListener('change', (e) => {
    if (e.target.matches('input.row-sel')) pick(e.target.closest('tr').dataset.id, e.target.checked);
  });
  $('sel-all').addEventListener('change', (e) => {
    for (const id of state.rows.keys()) pick(id, e.target.checked);
    for (const box of $('rows').querySelectorAll('input.row-sel')) box.checked = e.target.checked;
  });
  for (const b of $('bulk-bar').querySelectorAll('[data-bulk]')) b.addEventListener('click', () => openConfirm(b.dataset.bulk));
  $('bulk-clear').addEventListener('click', () => {
    state.picked.clear();
    for (const box of $('rows').querySelectorAll('input.row-sel')) box.checked = false;
    renderBulkBar();
  });
  $('confirm-form').addEventListener('submit', runBulk);
  $('confirm-back').addEventListener('click', closeConfirm);
  $('confirm-close').addEventListener('click', closeConfirm);
  $('reset-form').addEventListener('submit', (e) => { e.preventDefault(); resetTo(0); });
  $('d-chain').addEventListener('click', (e) => {
    const b = e.target.closest('.ev-reset');
    if (b) resetTo(Number(b.dataset.event));
  });
  $('sig-payload').addEventListener('input', checkSignal);
  $('sig-mode').addEventListener('change', checkSignal);
  checkSignal();

  $('refresh').addEventListener('click', resyncAll);
  $('new-wf').addEventListener('click', openStartModal);
  $('s-cancel').addEventListener('click', closeStartModal);
  $('modal-scrim').addEventListener('click', () => { closeStartModal(); closeConfirm(); });
  $('start-form').addEventListener('submit', startWorkflow);

  $('d-close').addEventListener('click', () => closeWorkflow(true));
  $('scrim').addEventListener('click', () => closeWorkflow(true));
  $('d-link').addEventListener('click', async () => {
    try {
      await navigator.clipboard.writeText(location.href);
      toast('Link copied');
    } catch {
      toast(location.href);
    }
  });
  for (const b of document.querySelectorAll('.tab')) b.addEventListener('click', () => selectTab(b.dataset.tab, true));
  $('signal-form').addEventListener('submit', (e) => { e.preventDefault(); sendSignal(); });
  $('terminate-form').addEventListener('submit', (e) => { e.preventDefault(); terminate(); });
  $('d-cancel').addEventListener('click', () => cancelWorkflow(false));
  $('d-request-cancel').addEventListener('click', () => cancelWorkflow(true));

  document.addEventListener('keydown', (e) => {
    if (e.key !== 'Escape') return;
    if (!$('start-modal').hidden) { closeStartModal(); return; }
    if (!$('confirm-modal').hidden) { closeConfirm(); return; }
    if (state.selected) closeWorkflow(true);
  });
  window.addEventListener('popstate', () => applyRoute(routeFromLocation(), false));

  $('live').addEventListener('change', (e) => {
    state.live = e.target.checked;
    if (state.live) {
      stream.retry = 0;
      resyncAll(); // what happened while paused
    }
    startStream();
  });
  $('ns').addEventListener('change', (e) => switchNamespace(e.target.value));
  $('sched-new').addEventListener('click', () => { $('sched-form').hidden = !$('sched-form').hidden; });
  $('sc-close').addEventListener('click', () => { $('sched-form').hidden = true; });
  $('sc-try').addEventListener('click', previewScheduleForm);
  $('sched-form').addEventListener('submit', createSchedule);

  applyRoute(routeFromLocation(), true);
  loadSchedules();
  loadNodeInfo();
  loadNamespaces();
  startStream();
  setInterval(tickTimes, TIME_TICK_MS);
}

document.addEventListener('DOMContentLoaded', init);

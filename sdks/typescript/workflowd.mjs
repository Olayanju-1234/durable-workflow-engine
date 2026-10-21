/**
 * workflowd — TypeScript/JavaScript SDK
 *
 * Write durable workflows in Node. Zero dependencies: this file uses only
 * built-in `fetch` and `Buffer`, so there is no npm install, no bundler, no
 * protobuf toolchain.
 *
 * The model in one paragraph: your workflow function is re-run from the top
 * every time the engine needs a decision. Each durable call (`ctx.activity`,
 * `ctx.sleep`, `ctx.waitForSignal`, `ctx.getVersion`, `ctx.sideEffect`) is
 * matched against what already happened. Completed steps return their recorded
 * result instantly; the first step with no result yet becomes the command we
 * send back. So the function runs many times but each side effect happens
 * exactly once.
 *
 * Usage:
 *
 *   import { Worker, WorkflowFailure } from './workflowd.mjs';
 *
 *   const worker = new Worker({ address: 'http://localhost:8233' });
 *
 *   worker.workflow('OrderWorkflow', async (ctx) => {
 *     const order = ctx.inputAs('json');
 *     const reserved = await ctx.activity('reserve', order, { as: 'json' });
 *     await ctx.sleep('settle', 2000);
 *     try {
 *       await ctx.activity('charge', reserved, { retry: { maxAttempts: 3, initialBackoffMs: 500 } });
 *     } catch (err) {
 *       await ctx.activity('release', reserved);   // compensate
 *       throw new WorkflowFailure(`charge failed: ${err.message}`);
 *     }
 *     return await ctx.activity('ship', reserved);
 *   });
 *
 *   worker.activity('reserve', async (order) => ({ id: order.id, held: true }), { as: 'json' });
 *
 *   await worker.run();
 *
 * Payloads: a string travels as UTF-8 text, a Uint8Array/Buffer as raw bytes,
 * and anything else as JSON. Results come back as strings unless you ask for
 * `{ as: 'json' }` or `{ as: 'bytes' }`.
 *
 * Clusters: pass every node in `addresses`. A follower answers 503 naming the
 * leader; the worker moves there and registers its workflow types again.
 */

/** Thrown by the workflow function to fail the workflow deliberately. */
export class WorkflowFailure extends Error {
  constructor(message) {
    super(message);
    this.name = 'WorkflowFailure';
  }
}

/** Thrown when an activity recorded a failure — catch it to compensate. */
export class ActivityFailure extends Error {
  constructor(stepName, message) {
    super(message);
    this.name = 'ActivityFailure';
    this.stepName = stepName;
  }
}

/** The version `getVersion` returns to executions that predate the change. */
export const DEFAULT_VERSION = -1;

// ---- payloads ----

/** Encode a value onto `obj[field]` (text) or `obj[field + '_base64']` (bytes). */
function putPayload(obj, field, value) {
  if (value === undefined || value === null) {
    obj[field] = '';
  } else if (typeof value === 'string') {
    obj[field] = value;
  } else if (value instanceof Uint8Array) {
    obj[`${field}_base64`] = Buffer.from(value).toString('base64');
  } else if (value instanceof ArrayBuffer) {
    obj[`${field}_base64`] = Buffer.from(value).toString('base64');
  } else {
    obj[field] = JSON.stringify(value);
  }
  return obj;
}

/**
 * Read a payload from `obj`. The engine adds `<field>_base64` only when the
 * bytes are not valid UTF-8, so the text field is exact otherwise.
 */
export function readPayload(obj, field, as = 'text') {
  const b64 = obj?.[`${field}_base64`];
  const bytes = () => (b64 ? Buffer.from(b64, 'base64') : Buffer.from(obj?.[field] ?? '', 'utf8'));
  switch (as) {
    case 'bytes':
      return bytes();
    case 'json': {
      const text = b64 ? bytes().toString('utf8') : (obj?.[field] ?? '');
      return text === '' ? null : JSON.parse(text);
    }
    case 'text':
    case undefined:
      return b64 ? bytes().toString('utf8') : (obj?.[field] ?? '');
    default:
      throw new TypeError(`unknown payload format ${JSON.stringify(as)}; use 'text', 'json' or 'bytes'`);
  }
}

function retryPolicyDTO(retry) {
  if (!retry) return undefined;
  return {
    max_attempts: retry.maxAttempts ?? 1,
    initial_backoff_ms: Math.max(0, Math.floor(retry.initialBackoffMs ?? 0)),
    max_backoff_ms: Math.max(0, Math.floor(retry.maxBackoffMs ?? 0)),
  };
}

/**
 * How replay suspends, and why it is NOT an exception.
 *
 * The obvious implementation is to throw a sentinel when replay reaches a step
 * with no recorded result. That is what the Python SDK does (a BaseException,
 * which `except Exception` won't catch).
 *
 * JavaScript gives us no such escape hatch: `catch (err)` catches *everything*.
 * A workflow doing the most natural thing in the world —
 *
 *     try { await ctx.activity('charge', x); } catch (err) { compensate(); }
 *
 * — would swallow the sentinel and run its compensation on every single replay
 * pass. (This is not hypothetical: it is exactly the bug the first version of
 * this SDK shipped with.)
 *
 * So instead of throwing, a step with no result returns a promise that NEVER
 * settles. The workflow function simply stops at that `await` and can never
 * observe it, however it handles errors. A step history has no record of yet
 * is also added to the commands to send. Because recorded results are already
 * settled promises, the function runs as far as it can within one turn of the
 * event loop; `decide` waits for that turn to end, and everything the code
 * started by then (several activities, with Promise.all) is the decision.
 */
const FOREVER = () => new Promise(() => {});

/** A promise rejected with err that Node does not report as unhandled. */
function rejected(err) {
  const p = Promise.reject(err);
  p.catch(() => {});
  return p;
}

/**
 * Context is what a workflow function is given. Every method is durable: its
 * result is recorded in history and replayed on the next decision.
 */
class Context {
  constructor(task) {
    this.task = task;
    this.input = readPayload(task, 'input');
    this.workflowId = task.workflow_id;
    this._steps = task.steps ?? [];
    this._signals = task.signals ?? [];
    this._next = 0; // how many durable calls this replay has issued
    this._versions = new Map();
    this._pending = []; // new commands, in sequence order
    this._replayError = null;
  }

  /** The start input decoded as 'text' (same as ctx.input), 'json' or 'bytes'. */
  inputAs(as = 'text') {
    return readPayload(this.task, 'input', as);
  }

  /**
   * Run a named activity on a worker and return its output.
   * opts.retry = { maxAttempts, initialBackoffMs, maxBackoffMs } retries a
   * failed attempt first (backoff doubles each time); opts.as decodes the result.
   * Not awaiting it at once runs activities in parallel:
   *
   *   const [a, b] = await Promise.all([ctx.activity('a'), ctx.activity('b')]);
   */
  activity(name, input = '', opts = {}) {
    const command = putPayload({ type: 'schedule_activity', step_name: name }, 'input', input);
    const retry = retryPolicyDTO(opts.retry);
    if (retry) command.retry_policy = retry;
    return this._command('ACTIVITY', name, command, opts.as);
  }

  /** Pause durably for `ms` milliseconds. Survives engine restarts. */
  sleep(name, ms) {
    return this._command('TIMER', name, {
      type: 'start_timer',
      step_name: name,
      delay_ms: Math.max(0, Math.floor(ms)),
    });
  }

  /** Block until a matching external signal arrives; returns its payload. */
  waitForSignal(name, signalName, opts = {}) {
    return this._command(
      'SIGNAL',
      name,
      { type: 'await_signal', step_name: name, signal_name: signalName },
      opts.as,
    );
  }

  /**
   * Wait for the first of several ctx promises (activities, sleeps, signal
   * waits) to settle, and resolve to its index. When several have results,
   * the one history recorded first wins, so every replay agrees. Use this,
   * not Promise.race: race picks by call order, which can change on replay.
   *
   *   const reply = ctx.waitForSignal('reply', 'approved');
   *   const deadline = ctx.sleep('deadline', 48 * 3600 * 1000);
   *   if ((await ctx.waitAny([reply, deadline])) === 1) { ... timed out ... }
   */
  waitAny(promises) {
    let best = -1;
    let bestId = 0;
    promises.forEach((p, i) => {
      if (typeof p?._seq !== 'number') throw new TypeError('waitAny takes promises returned by ctx calls');
      const rec = this._steps[p._seq - 1];
      const id = rec?.resolved_event_id ?? 0;
      if (rec && (rec.status === 'COMPLETED' || rec.status === 'FAILED') && (best < 0 || id < bestId)) {
        best = i;
        bestId = id;
      }
    });
    return best < 0 ? FOREVER() : Promise.resolve(best);
  }

  /**
   * Change workflow code while executions are in flight:
   *
   *   if ((await ctx.getVersion('add-fraud-check', DEFAULT_VERSION, 1)) === 1) { ... }
   *
   * An execution reaching this point for the first time records maxSupported
   * and keeps it on every replay. One that ran past this point under the old
   * code has no record of it and gets DEFAULT_VERSION. A recorded version
   * outside [minSupported, maxSupported] fails the execution.
   */
  async getVersion(changeId, minSupported, maxSupported) {
    let v = this._versions.get(changeId);
    if (v === undefined) {
      const seq = this._next + 1;
      const rec = this._steps[seq - 1];
      if (!rec) {
        this._next = seq;
        this._pending.push({
          type: 'record_marker',
          step_name: changeId,
          sequence: seq,
          marker: 'version',
          data: String(maxSupported),
        });
        v = maxSupported;
      } else if (rec.kind !== 'MARKER' || rec.marker !== 'version' || rec.step_name !== changeId) {
        v = DEFAULT_VERSION; // this execution ran past here before the change
      } else {
        this._next = seq;
        v = Number.parseInt(readPayload(rec, 'output'), 10);
      }
      this._versions.set(changeId, v);
    }
    if (v < minSupported || v > maxSupported) {
      return this._nondeterminism(
        `non-determinism: execution recorded version ${v} of "${changeId}", ` +
          `but the code supports ${minSupported}..${maxSupported}`,
      );
    }
    return v;
  }

  /**
   * Run `fn` once, record its result, and return the recorded result on every
   * replay. For small non-deterministic values such as a random id; it must
   * not fail or do I/O.
   */
  async sideEffect(name, fn, opts = {}) {
    const seq = this._next + 1;
    if (!this._steps[seq - 1]) {
      this._next = seq;
      const command = putPayload(
        { type: 'record_marker', step_name: name, sequence: seq, marker: 'side_effect' },
        'data',
        await fn(),
      );
      this._pending.push(command);
      return readPayload(command, 'data', opts.as); // what every replay will return
    }
    return this._command('MARKER', name, null, opts.as);
  }

  _nondeterminism(message) {
    this._replayError ??= message;
    return FOREVER();
  }

  /**
   * The heart of replay. For the next step in call order: return the recorded
   * result, stay blocked, or start the step. The promise carries its sequence
   * for waitAny.
   */
  _command(kind, name, command, as) {
    const seq = ++this._next;
    const p = this._result(seq, kind, name, command, as);
    p._seq = seq;
    return p;
  }

  _result(seq, kind, name, command, as) {
    const recorded = this._steps[seq - 1];
    if (!recorded) {
      // No history for this step: start it, and wait for its result.
      this._pending.push({ ...command, sequence: seq });
      return FOREVER();
    }
    // Determinism check: the call the code is making now must be the step
    // history recorded at this position. A mismatch means the code changed in
    // a way that diverges from the durable run; it is reported in a way the
    // workflow code cannot catch and hide.
    const markerMismatch = kind === 'MARKER' && recorded.marker !== 'side_effect';
    if (recorded.kind !== kind || recorded.step_name !== name || markerMismatch) {
      return this._nondeterminism(
        `non-determinism at step ${seq}: code issued ${kind} "${name}" ` +
          `but history has ${recorded.kind} "${recorded.step_name}"`,
      );
    }
    if (recorded.status === 'COMPLETED') {
      try {
        return Promise.resolve(readPayload(recorded, 'output', as));
      } catch (err) {
        return rejected(err); // e.g. { as: 'json' } on a non-JSON result
      }
    }
    if (recorded.status === 'FAILED') {
      // A genuine, catchable error: this is how Saga compensation works.
      return rejected(new ActivityFailure(name, recorded.error || 'activity failed'));
    }
    return FOREVER(); // SCHEDULED: in flight (or being retried)
  }
}

const nextTurn = () => new Promise((resolve) => setImmediate(resolve));

/**
 * Replay a workflow function against a task and produce what happens next:
 * `{ commands: [...] }` (none means it is blocked on steps in flight) or
 * `{ replayError }`. Pure: it performs no I/O and mutates nothing outside its
 * own scope. (`command` is set too when there is exactly one.)
 */
export async function decide(fn, task) {
  const ctx = new Context(task);
  let settled = null;
  fn(ctx).then(
    (output) => {
      settled = { type: 'complete_workflow', output };
    },
    (err) => {
      // WorkflowFailure, an uncaught ActivityFailure, or a plain bug: all fail
      // the workflow, exactly like returning an error from a Go workflow.
      settled = { type: 'fail_workflow', error: err?.message ?? String(err) };
    },
  );
  // Recorded results are settled promises, so the code runs as far as it can
  // in microtasks; by the next turn of the event loop it has finished or is
  // waiting on steps with no result.
  await nextTurn();
  if (ctx._replayError) return { replayError: ctx._replayError };
  const commands = [...ctx._pending];
  if (settled?.type === 'complete_workflow') {
    commands.push(putPayload({ type: 'complete_workflow' }, 'output', settled.output));
  } else if (settled) {
    commands.push(settled);
  }
  return { commands, command: commands.length === 1 ? commands[0] : null };
}

class HTTPError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

/**
 * Connection talks to whichever node currently serves requests. A 503 from a
 * cluster follower names the leader (`leader_http`); without a name (an
 * election is running) or when a node is unreachable, it moves to the next
 * known address. `epoch` counts those moves so the worker knows to register
 * again: every new engine (restart, new leader) starts with no registrations.
 */
class Connection {
  constructor(addresses, token) {
    this.token = token;
    this.addrs = addresses.map((a) => a.replace(/\/$/, ''));
    if (this.addrs.length === 0) throw new Error('workflowd: at least one address is required');
    this.current = 0;
    this.epoch = 0;
  }

  get base() {
    return this.addrs[this.current];
  }

  _moveOn(from, leaderHttp) {
    this.epoch++;
    if (this.base !== from) return; // another call already moved on
    if (leaderHttp) {
      const url = `${new URL(from).protocol}//${leaderHttp}`;
      let i = this.addrs.indexOf(url);
      if (i < 0) i = this.addrs.push(url) - 1;
      if (i !== this.current) {
        this.current = i;
        return;
      }
    }
    this.current = (this.current + 1) % this.addrs.length;
  }

  /** One request, retried on another node while the cluster turns it away. */
  async request(path, { method = 'GET', body, attempts = 12 } = {}) {
    let backoff = 50;
    for (let attempt = 1; ; attempt++) {
      const from = this.base;
      let res;
      try {
        res = await fetch(from + path, {
          method,
          headers: {
            'Content-Type': 'application/json',
            ...(this.token ? { Authorization: `Bearer ${this.token}` } : {}),
          },
          body: body === undefined ? undefined : JSON.stringify(body),
        });
      } catch (err) {
        this._moveOn(from, '');
        if (attempt >= attempts) throw err;
        await sleep(backoff);
        backoff = Math.min(backoff * 2, 1000);
        continue;
      }
      if (res.status === 204) return null; // long-poll expired
      const text = await res.text();
      let parsed = {};
      try {
        parsed = text ? JSON.parse(text) : {};
      } catch {
        parsed = { error: text };
      }
      if (res.status === 503) {
        this._moveOn(from, parsed.leader_http ?? '');
        if (attempt >= attempts) throw new HTTPError(503, parsed.error ?? 'HTTP 503');
        if (!parsed.leader_http) {
          await sleep(backoff); // no leader yet: give the election a moment
          backoff = Math.min(backoff * 2, 1000);
        }
        continue;
      }
      if (!res.ok) throw new HTTPError(res.status, parsed.error ?? `HTTP ${res.status}`);
      return parsed;
    }
  }
}

/**
 * Worker connects to an engine, registers the workflow types it can run, and
 * serves both workflow decisions and activity executions.
 */
export class Worker {
  /**
   * `token` is sent as `Authorization: Bearer <token>` for an engine started
   * with --auth-token-file. An https address uses Node's trust store; for a
   * private CA, start Node with NODE_EXTRA_CA_CERTS=/path/to/ca.pem.
   */
  constructor({
    address = 'http://localhost:8233',
    addresses,
    workerId,
    sdk = 'typescript/0.1.0',
    token,
    activityConcurrency = 4,
    workflowConcurrency = 2,
  } = {}) {
    this.conn = new Connection(addresses ?? [address], token);
    this.workerId = workerId ?? `ts-worker-${Math.random().toString(16).slice(2, 10)}`;
    this.sdk = sdk;
    this.workflows = new Map();
    this.activities = new Map();
    this.activityConcurrency = activityConcurrency;
    this.workflowConcurrency = workflowConcurrency;
    this._registeredEpoch = -1;
    this._stop = false;
  }

  /** The base URL of the node currently in use. */
  get address() {
    return this.conn.base;
  }

  /** Register a workflow function under a type name. */
  workflow(type, fn) {
    this.workflows.set(type, fn);
    return this;
  }

  /**
   * Register an activity implementation under a step name. fn(input, task)
   * gets the input decoded per opts.as ('text' by default, 'json', 'bytes');
   * task.signal aborts if the lease is lost. Return any payload.
   */
  activity(name, fn, opts = {}) {
    this.activities.set(name, { fn, as: opts.as });
    return this;
  }

  stop() {
    this._stop = true;
  }

  /** Tell the engine which workflow types this worker can replay. */
  async register() {
    const types = [...this.workflows.keys()];
    if (types.length === 0) return;
    const epoch = this.conn.epoch;
    await this.conn.request('/api/workflow-types', {
      method: 'POST',
      body: { workflow_types: types, worker_id: this.workerId, sdk: this.sdk },
    });
    // The epoch from before the call: if the call itself moved nodes, the next
    // poll registers again, which is harmless.
    this._registeredEpoch = epoch;
  }

  /** Start a workflow. Returns its id. */
  async startWorkflow(type, input = '', workflowId = '') {
    // Chosen here so a retry after a lost reply cannot start a second execution.
    const id = workflowId || `${type}-${Math.random().toString(16).slice(2)}${Date.now().toString(16)}`;
    try {
      await this.conn.request('/api/workflows', {
        method: 'POST',
        body: putPayload({ type, workflow_id: id }, 'input', input),
      });
    } catch (err) {
      // Only an earlier attempt of this call can hold a fresh generated id.
      if (!(err.status === 409 && !workflowId)) throw err;
    }
    return id;
  }

  /**
   * Send an external signal to a running workflow. It is recorded once even if
   * the request is retried (after a lost reply or a leader change): every
   * attempt carries the same idempotency key, opts.requestId or a random one.
   */
  async signal(workflowId, name, payload = '', opts = {}) {
    const requestId = opts.requestId ?? `${Date.now().toString(16)}${Math.random().toString(16).slice(2)}`;
    await this.conn.request(`/api/workflows/${encodeURIComponent(workflowId)}/signal`, {
      method: 'POST',
      body: putPayload({ name, request_id: requestId }, 'payload', payload),
    });
  }

  /** Fetch a workflow's current state. */
  async describe(workflowId) {
    return this.conn.request(`/api/workflows/${encodeURIComponent(workflowId)}`);
  }

  async _workflowLoop() {
    const types = [...this.workflows.keys()].join(',');
    while (!this._stop) {
      let task;
      try {
        if (this._registeredEpoch !== this.conn.epoch) await this.register();
        task = await this.conn.request(
          `/api/workflow-tasks?types=${encodeURIComponent(types)}&worker_id=${encodeURIComponent(this.workerId)}`,
          { attempts: 1 },
        );
      } catch {
        await sleep(200); // engine unreachable or turned us away; retry
        continue;
      }
      if (!task) continue; // long-poll expired

      const fn = this.workflows.get(task.workflow_type);
      const result = fn
        ? await decide(fn, task)
        : { replayError: `no workflow registered for type ${task.workflow_type}` };
      const body = { workflow_id: task.workflow_id, task_token: task.task_token, replay_error: result.replayError ?? '' };
      // One command travels in the field every engine version understands.
      if ((result.commands?.length ?? 0) > 1) body.commands = result.commands;
      else body.command = result.commands?.[0] ?? null;

      try {
        await this.conn.request('/api/workflow-tasks/complete', { method: 'POST', body, attempts: 3 });
      } catch (err) {
        // 409 means the decision went stale (the history moved on). Polling
        // again is the correct response, so this is not worth reporting.
        if (err.status !== 409) console.error('[workflowd] complete failed:', err.message);
      }
    }
  }

  async _activityLoop() {
    // Name our activities so the engine never hands us another worker's step.
    const names = [...this.activities.keys()].join(',');
    while (!this._stop) {
      let task;
      try {
        task = await this.conn.request(
          `/api/activity-tasks?activities=${encodeURIComponent(names)}&worker_id=${encodeURIComponent(this.workerId)}`,
          { attempts: 1 },
        );
      } catch {
        await sleep(200);
        continue;
      }
      if (!task) continue;
      await this._execute(task);
    }
  }

  async _execute(task) {
    const entry = this.activities.get(task.step_name);
    const abort = new AbortController();
    const stopBeats = this._heartbeat(task, abort);
    const result = { workflow_id: task.workflow_id, step_name: task.step_name, sequence: task.sequence, lease_token: task.lease_token };
    try {
      if (!entry) throw new Error(`no activity registered for step "${task.step_name}"`);
      const input = readPayload(task, 'input', entry.as);
      putPayload(result, 'output', await entry.fn(input, { ...task, signal: abort.signal }));
    } catch (err) {
      result.error = err?.message || String(err) || 'activity failed';
    } finally {
      stopBeats();
    }
    try {
      await this.conn.request('/api/activity-tasks/complete', { method: 'POST', body: result, attempts: 3 });
    } catch (err) {
      // A lost lease means the task was reassigned; drop the result.
      if (err.status !== 409) console.error('[workflowd] activity complete failed:', err.message);
    }
  }

  // Extend the lease every third of its duration; a 409 means it is gone, so
  // the activity's signal aborts.
  _heartbeat(task, abort) {
    const every = Math.floor((task.lease_duration_ms ?? 0) / 3);
    if (every <= 0) return () => {};
    const timer = setInterval(async () => {
      try {
        await this.conn.request('/api/activity-tasks/heartbeat', {
          method: 'POST',
          body: { workflow_id: task.workflow_id, lease_token: task.lease_token, worker_id: this.workerId },
          attempts: 2,
        });
      } catch (err) {
        if (err.status === 409) {
          clearInterval(timer);
          abort.abort(new Error('lease lost'));
        }
      }
    }, every);
    return () => clearInterval(timer);
  }

  /** Register, then serve workflow decisions and activities until stop(). */
  async run() {
    this._stop = false;
    const loops = [];
    if (this.workflows.size > 0) {
      try {
        await this.register();
      } catch (err) {
        // A refused type is a bug to surface; an engine that is not up yet is
        // retried by the workflow loop.
        if (err.status && err.status !== 503) throw err;
      }
      for (let i = 0; i < this.workflowConcurrency; i++) loops.push(this._workflowLoop());
    }
    // An empty filter means "any activity", so a worker with none must not poll.
    if (this.activities.size > 0) {
      for (let i = 0; i < this.activityConcurrency; i++) loops.push(this._activityLoop());
    }
    await Promise.all(loops);
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

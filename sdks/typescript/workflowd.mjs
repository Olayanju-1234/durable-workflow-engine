/**
 * workflowd — TypeScript/JavaScript SDK
 *
 * Write durable workflows in Node. Zero dependencies: this file uses only
 * built-in `fetch`, so there is no npm install, no bundler, no protobuf
 * toolchain.
 *
 * The model in one paragraph: your workflow function is re-run from the top
 * every time the engine needs a decision. Each durable call (`ctx.activity`,
 * `ctx.sleep`, `ctx.waitForSignal`) is matched against what already happened.
 * Completed steps return their recorded result instantly; the first step with
 * no result yet becomes the command we send back. So the function runs many
 * times but each side effect happens exactly once.
 *
 * Usage:
 *
 *   import { Worker, WorkflowFailure } from './workflowd.mjs';
 *
 *   const worker = new Worker({ address: 'http://localhost:8233' });
 *
 *   worker.workflow('OrderWorkflow', async (ctx) => {
 *     const reserved = await ctx.activity('reserve', ctx.input);
 *     await ctx.sleep('settle', 2000);
 *     try {
 *       await ctx.activity('charge', reserved);
 *     } catch (err) {
 *       await ctx.activity('release', reserved);   // compensate
 *       throw new WorkflowFailure(`charge failed: ${err.message}`);
 *     }
 *     return await ctx.activity('ship', reserved);
 *   });
 *
 *   worker.activity('reserve', async (input) => `reserved:${input}`);
 *
 *   await worker.run();
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

/**
 * How replay suspends, and why it is NOT an exception.
 *
 * The obvious implementation is to throw a sentinel when replay reaches a step
 * with no recorded result. That is what the Go SDK does (panic/recover) and the
 * Python SDK does (a BaseException, which `except Exception` won't catch).
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
 * So instead of throwing, an unrecorded step records the command and returns a
 * promise that NEVER settles. The workflow function simply suspends at that
 * `await` and can never observe it, no matter how it handles errors. `decide`
 * races the function against a signal that fires when the command is recorded.
 */
const FOREVER = () => new Promise(() => {});

/**
 * Context is what a workflow function is given. Every method is durable: its
 * result is recorded in history and replayed on the next decision.
 */
class Context {
  constructor(task, onSuspend) {
    this.task = task;
    this.input = task.input ?? '';
    this.workflowId = task.workflow_id;
    this._steps = task.steps ?? [];
    this._signals = task.signals ?? [];
    this._next = 0; // how many durable calls this replay has issued
    this._onSuspend = onSuspend; // called once, with the outcome to report
  }

  /** Run a named activity on a worker and return its output. */
  async activity(name, input = '') {
    return this._command('ACTIVITY', name, {
      type: 'schedule_activity',
      step_name: name,
      input: String(input ?? ''),
    });
  }

  /** Pause durably for `ms` milliseconds. Survives engine restarts. */
  async sleep(name, ms) {
    return this._command('TIMER', name, {
      type: 'start_timer',
      step_name: name,
      delay_ms: Math.max(0, Math.floor(ms)),
    });
  }

  /** Block until a matching external signal arrives; returns its payload. */
  async waitForSignal(name, signalName) {
    return this._command('SIGNAL', name, {
      type: 'await_signal',
      step_name: name,
      signal_name: signalName,
    });
  }

  /**
   * The heart of replay. For the next step in call order: return the recorded
   * result, stay blocked, or yield the command that creates it.
   */
  _command(kind, name, command) {
    const seq = ++this._next;
    const recorded = this._steps[seq - 1];

    if (recorded) {
      // Determinism check: the call the code is making now must be the step
      // history recorded at this position. A mismatch means the code changed
      // in a way that diverges from the durable run. Reported through the
      // suspend channel so workflow code cannot catch and hide it.
      if (recorded.kind !== kind || recorded.step_name !== name) {
        this._onSuspend({
          replayError:
            `non-determinism at step ${seq}: code issued ${kind} "${name}" ` +
            `but history has ${recorded.kind} "${recorded.step_name}"`,
        });
        return FOREVER();
      }
      if (recorded.status === 'COMPLETED') return recorded.output ?? '';
      if (recorded.status === 'FAILED') {
        // A genuine, catchable error: this is how Saga compensation works.
        return Promise.reject(new ActivityFailure(name, recorded.error || 'activity failed'));
      }
      // SCHEDULED: still in flight. Suspend with nothing to report.
      this._onSuspend({ command: null });
      return FOREVER();
    }

    // No history for this step: this is what happens next.
    this._onSuspend({ command: { ...command, sequence: seq } });
    return FOREVER();
  }
}

/**
 * Replay a workflow function against a task and produce the next command.
 * Pure: it performs no I/O and mutates nothing outside its own scope.
 */
export async function decide(fn, task) {
  // Fires the first time replay suspends. Later calls are ignored: once we know
  // the next command, anything the (now-abandoned) function does is irrelevant.
  let settle;
  const suspended = new Promise((resolve) => {
    settle = resolve;
  });
  let done = false;
  const onSuspend = (outcome) => {
    if (!done) {
      done = true;
      settle(outcome);
    }
  };

  const ctx = new Context(task, onSuspend);

  // Race the workflow function against its own suspension. Whichever happens
  // first decides the outcome — and because suspension never settles the
  // function's promise, these are genuinely exclusive.
  const ran = fn(ctx).then(
    (output) => ({ command: { type: 'complete_workflow', output: String(output ?? '') } }),
    (err) => {
      // WorkflowFailure, an uncaught ActivityFailure, or a plain bug: all fail
      // the workflow, exactly like returning an error from a Go workflow.
      if (done) return suspended; // already suspended; ignore late rejection
      return { command: { type: 'fail_workflow', error: err?.message ?? String(err) } };
    },
  );

  return Promise.race([ran, suspended]);
}

/**
 * Worker connects to an engine, registers the workflow types it can run, and
 * serves both workflow decisions and activity executions.
 */
export class Worker {
  constructor({ address = 'http://localhost:8233', workerId, sdk = 'typescript/0.1.0' } = {}) {
    this.address = address.replace(/\/$/, '');
    this.workerId = workerId ?? `ts-worker-${Math.random().toString(16).slice(2, 10)}`;
    this.sdk = sdk;
    this.workflows = new Map();
    this.activities = new Map();
    this._stop = false;
  }

  /** Register a workflow function under a type name. */
  workflow(type, fn) {
    this.workflows.set(type, fn);
    return this;
  }

  /** Register an activity implementation under a step name. */
  activity(name, fn) {
    this.activities.set(name, fn);
    return this;
  }

  stop() {
    this._stop = true;
  }

  async _fetch(path, options = {}) {
    const res = await fetch(this.address + path, {
      ...options,
      headers: { 'Content-Type': 'application/json', ...(options.headers ?? {}) },
    });
    if (res.status === 204) return null; // long-poll expired
    const text = await res.text();
    const body = text ? JSON.parse(text) : {};
    if (!res.ok) {
      const err = new Error(body.error ?? `HTTP ${res.status}`);
      err.status = res.status;
      throw err;
    }
    return body;
  }

  /** Tell the engine which workflow types this worker can replay. */
  async register() {
    const types = [...this.workflows.keys()];
    if (types.length === 0) return;
    await this._fetch('/api/workflow-types', {
      method: 'POST',
      body: JSON.stringify({ workflow_types: types, worker_id: this.workerId, sdk: this.sdk }),
    });
  }

  /** Start a workflow. Returns its id. */
  async startWorkflow(type, input = '', workflowId = '') {
    const body = await this._fetch('/api/workflows', {
      method: 'POST',
      body: JSON.stringify({ type, input: String(input), workflow_id: workflowId }),
    });
    return body.workflow_id;
  }

  /** Send an external signal to a running workflow. */
  async signal(workflowId, name, payload = '') {
    await this._fetch(`/api/workflows/${encodeURIComponent(workflowId)}/signal`, {
      method: 'POST',
      body: JSON.stringify({ name, payload: String(payload) }),
    });
  }

  /** Fetch a workflow's current state. */
  async describe(workflowId) {
    return this._fetch(`/api/workflows/${encodeURIComponent(workflowId)}`);
  }

  async _workflowLoop() {
    // An empty filter means "any type", so a worker with no workflows must not poll.
    if (this.workflows.size === 0) return;
    const types = [...this.workflows.keys()].join(',');
    while (!this._stop) {
      let task;
      try {
        task = await this._fetch(`/api/workflow-tasks?types=${encodeURIComponent(types)}&worker_id=${this.workerId}`);
      } catch {
        await sleep(500); // engine unreachable; back off and retry
        continue;
      }
      if (!task) continue; // long-poll expired

      const fn = this.workflows.get(task.workflow_type);
      const result = fn
        ? await decide(fn, task)
        : { replayError: `no workflow registered for type ${task.workflow_type}` };

      try {
        await this._fetch('/api/workflow-tasks/complete', {
          method: 'POST',
          body: JSON.stringify({
            workflow_id: task.workflow_id,
            task_token: task.task_token,
            command: result.command ?? null,
            replay_error: result.replayError ?? '',
          }),
        });
      } catch (err) {
        // 409 means the decision went stale (the history moved on). Polling
        // again is the correct response, so this is not worth reporting.
        if (err.status !== 409) console.error('[workflowd] complete failed:', err.message);
      }
    }
  }

  async _activityLoop() {
    // Name our activities so the engine never hands us another worker's step.
    if (this.activities.size === 0) return;
    const names = [...this.activities.keys()].join(',');
    while (!this._stop) {
      let task;
      try {
        task = await this._fetch(
          `/api/activity-tasks?activities=${encodeURIComponent(names)}&worker_id=${this.workerId}`,
        );
      } catch {
        await sleep(500);
        continue;
      }
      if (!task) continue;

      const fn = this.activities.get(task.step_name);
      let output = '';
      let error = '';
      if (!fn) {
        error = `no activity registered for step "${task.step_name}"`;
      } else {
        try {
          output = String((await fn(task.input, task)) ?? '');
        } catch (err) {
          error = err?.message ?? String(err);
        }
      }

      try {
        await this._fetch('/api/activity-tasks/complete', {
          method: 'POST',
          body: JSON.stringify({
            workflow_id: task.workflow_id,
            step_name: task.step_name,
            sequence: task.sequence,
            lease_token: task.lease_token,
            output,
            error,
          }),
        });
      } catch (err) {
        // A lost lease means the task was reassigned; drop the result.
        if (err.status !== 409) console.error('[workflowd] activity complete failed:', err.message);
      }
    }
  }

  /** Register, then serve workflow decisions and activities until stop(). */
  async run() {
    await this.register();
    await Promise.all([this._workflowLoop(), this._activityLoop()]);
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

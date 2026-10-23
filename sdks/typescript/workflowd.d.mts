// Type definitions for workflowd.mjs. Keep in step with its exports.

/** Throw inside a workflow to fail it deliberately. */
export class WorkflowFailure extends Error {
  constructor(message: string);
}

/** Rejected by `ctx.activity` when the activity recorded a failure. Catch it to compensate. */
export class ActivityFailure extends Error {
  constructor(stepName: string, message: string);
  readonly stepName: string;
}

/** The version `getVersion` returns to executions that predate the change. */
export const DEFAULT_VERSION: -1;

/**
 * A payload to send: a string travels as UTF-8 text, a Uint8Array or ArrayBuffer as raw
 * bytes, and anything else as JSON.
 */
export type Payload = unknown;

/** How to decode a received payload. */
export type PayloadFormat = 'text' | 'json' | 'bytes';

/** Retry a failed activity attempt before the workflow sees the error. */
export interface RetryPolicy {
  /** Attempts in total, counting the first; 1 means no retries. */
  maxAttempts: number;
  /** The first wait, doubled after each further failure. */
  initialBackoffMs?: number;
  /** Cap on the wait; 0 or absent means no cap. */
  maxBackoffMs?: number;
}

export interface ActivityOptions {
  retry?: RetryPolicy;
  /** Fail an attempt this long after a worker picks it up; heartbeats do not extend it. */
  startToCloseTimeoutMs?: number;
  /** Fail the step this long after it is first scheduled, every attempt and backoff included. */
  scheduleToCloseTimeoutMs?: number;
}

export interface StartWorkflowOptions {
  /** End the workflow as TIMED_OUT if it has not finished this long after it starts. */
  executionTimeoutMs?: number;
}

/** What a workflow function receives. Every method is durable. */
export interface Context {
  /** The workflow's input as text, as given to startWorkflow. */
  readonly input: string;
  readonly workflowId: string;
  /** The input decoded as text, JSON or bytes. */
  inputAs(as?: 'text'): string;
  inputAs(as: 'bytes'): Uint8Array;
  inputAs<T = unknown>(as: 'json'): T;
  /** Run a named activity on a worker and return its output. */
  activity(name: string, input?: Payload, opts?: ActivityOptions & { as?: 'text' }): Promise<string>;
  activity(name: string, input: Payload, opts: ActivityOptions & { as: 'bytes' }): Promise<Uint8Array>;
  activity<T = unknown>(name: string, input: Payload, opts: ActivityOptions & { as: 'json' }): Promise<T>;
  /** Pause durably for `ms` milliseconds. Survives engine restarts. */
  sleep(name: string, ms: number): Promise<unknown>;
  /**
   * Resolve to the index of the first of several ctx promises to settle, by the order history
   * recorded their results, so every replay agrees. Use it instead of Promise.race.
   */
  waitAny(promises: Array<Promise<unknown>>): Promise<number>;
  /** Block until a matching external signal arrives; resolves to its payload. */
  waitForSignal(name: string, signalName: string, opts?: { as?: 'text' }): Promise<string>;
  waitForSignal(name: string, signalName: string, opts: { as: 'bytes' }): Promise<Uint8Array>;
  waitForSignal<T = unknown>(name: string, signalName: string, opts: { as: 'json' }): Promise<T>;
  /**
   * Change workflow code while executions are in flight. An execution reaching this call for
   * the first time records maxSupported; one that ran past it under older code gets
   * DEFAULT_VERSION.
   */
  getVersion(changeId: string, minSupported: number, maxSupported: number): Promise<number>;
  /** Run fn once, record its result, and return the recorded result on every replay. */
  sideEffect(name: string, fn: () => Payload | Promise<Payload>, opts?: { as?: 'text' }): Promise<string>;
  sideEffect<T = unknown>(name: string, fn: () => Payload | Promise<Payload>, opts: { as: 'json' }): Promise<T>;
  sideEffect(name: string, fn: () => Payload | Promise<Payload>, opts: { as: 'bytes' }): Promise<Uint8Array>;
}

/** A workflow: ordinary async code, replayed from the top on every decision. */
export type WorkflowFunction = (ctx: Context) => Promise<unknown>;

/** The activity task as given to an activity function. */
export interface ActivityTask {
  workflow_id: string;
  step_name: string;
  sequence: number;
  /** Aborts if the worker loses the task's lease (the work was reassigned) or the attempt times out. */
  signal: AbortSignal;
  /** The attempt's start-to-close timeout, if the workflow set one. */
  start_to_close_timeout_ms?: number;
  [field: string]: unknown;
}

/** An activity: where side effects happen. Throw to fail the step; return any payload. */
export type ActivityFunction = (input: any, task: ActivityTask) => unknown | Promise<unknown>;

/** A command for the engine, as sent on the wire. */
export interface Command {
  type:
    | 'schedule_activity'
    | 'start_timer'
    | 'await_signal'
    | 'record_marker'
    | 'complete_workflow'
    | 'fail_workflow'
    | 'noop';
  [field: string]: unknown;
}

/** The outcome of replaying a workflow function against one task. */
export interface Decision {
  /** Every new command, in order; empty when the workflow waits on steps in flight. */
  commands?: Command[];
  /** The single command, when there is exactly one. */
  command?: Command | null;
  replayError?: string;
}

/** A workflow task as the engine sends it: input, recorded steps and buffered signals. */
export interface WorkflowTask {
  workflow_id: string;
  workflow_type?: string;
  task_token?: string;
  input?: string;
  input_base64?: string;
  steps?: Array<Record<string, unknown>>;
  signals?: Array<Record<string, unknown>>;
  [field: string]: unknown;
}

/** Replay `fn` against `task` and produce the next command. Pure: no I/O. */
export function decide(fn: WorkflowFunction, task: WorkflowTask): Promise<Decision>;

/** Read a payload field (and its `_base64` twin) from a wire object. */
export function readPayload(obj: Record<string, unknown> | undefined, field: string, as?: PayloadFormat): unknown;

export interface WorkerOptions {
  /** The engine's HTTP address, e.g. "https://workflowd.internal:8233". Default "http://localhost:8233". */
  address?: string;
  /** Every cluster node's HTTP address; replaces `address`. Followers' leader hints are followed. */
  addresses?: string[];
  workerId?: string;
  sdk?: string;
  /**
   * Sent as `Authorization: Bearer <token>` for an engine started with --auth-token-file.
   * For a private CA over https, start Node with NODE_EXTRA_CA_CERTS=/path/ca.pem.
   */
  token?: string;
  /** Activities run at once. Default 4. */
  activityConcurrency?: number;
  /** Workflow tasks decided at once. Default 2. */
  workflowConcurrency?: number;
}

/** Connects to an engine and serves workflow decisions plus activity executions. */
export class Worker {
  constructor(options?: WorkerOptions);
  /** The node currently in use. */
  readonly address: string;
  readonly workerId: string;
  /** Register a workflow function under a type name. */
  workflow(type: string, fn: WorkflowFunction): this;
  /** Register an activity implementation under a step name; `as` decodes its input. */
  activity(name: string, fn: ActivityFunction, opts?: { as?: PayloadFormat }): this;
  /** Ask the polling loops to finish. */
  stop(): void;
  /** Tell the engine which workflow types this worker can replay. */
  register(): Promise<void>;
  /** Start a workflow; resolves to its id. */
  startWorkflow(type: string, input?: Payload, workflowId?: string, opts?: StartWorkflowOptions): Promise<string>;
  /** Send an external signal to a running workflow; recorded once, even when retried. */
  signal(workflowId: string, name: string, payload?: Payload, opts?: { requestId?: string }): Promise<void>;
  /** The engine's view of a workflow: status, steps, output, error. */
  describe(workflowId: string): Promise<Record<string, unknown>>;
  /** Register, then serve workflow decisions and activities until stop(). */
  run(): Promise<void>;
}

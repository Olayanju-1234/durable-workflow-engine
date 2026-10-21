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

/** What a workflow function receives. Every method is durable. */
export interface Context {
  /** The workflow's input, as given to startWorkflow. */
  readonly input: string;
  readonly workflowId: string;
  /** Run a named activity on a worker and return its output. */
  activity(name: string, input?: string): Promise<string>;
  /** Pause durably for `ms` milliseconds. Survives engine restarts. */
  sleep(name: string, ms: number): Promise<void>;
  /** Block until a matching external signal arrives; resolves to its payload. */
  waitForSignal(name: string, signalName: string): Promise<string>;
}

/** A workflow: ordinary async code, replayed from the top on every decision. */
export type WorkflowFunction = (ctx: Context) => Promise<unknown>;

/** An activity: where side effects happen. Throw to fail the step. */
export type ActivityFunction = (input: string) => unknown | Promise<unknown>;

/** A command for the engine, as sent on the wire. */
export interface Command {
  type: 'schedule_activity' | 'start_timer' | 'await_signal' | 'complete_workflow' | 'fail_workflow' | 'noop';
  [field: string]: unknown;
}

/** The outcome of replaying a workflow function against one task. */
export interface Decision {
  command?: Command | null;
  replayError?: string;
}

/** A workflow task as the engine sends it: input, recorded steps and buffered signals. */
export interface WorkflowTask {
  workflow_id: string;
  workflow_type?: string;
  task_token?: string;
  input?: string;
  steps?: Array<Record<string, unknown>>;
  signals?: Array<Record<string, unknown>>;
  [field: string]: unknown;
}

/** Replay `fn` against `task` and produce the next command. Pure: no I/O. */
export function decide(fn: WorkflowFunction, task: WorkflowTask): Promise<Decision>;

export interface WorkerOptions {
  /** The engine's HTTP address, e.g. "https://workflowd.internal:8233". Default "http://localhost:8233". */
  address?: string;
  workerId?: string;
  sdk?: string;
  /**
   * Sent as `Authorization: Bearer <token>` for an engine started with --auth-token-file.
   * For a private CA over https, start Node with NODE_EXTRA_CA_CERTS=/path/ca.pem.
   */
  token?: string;
}

/** Connects to an engine and serves workflow decisions plus activity executions. */
export class Worker {
  constructor(options?: WorkerOptions);
  readonly address: string;
  readonly workerId: string;
  /** Register a workflow function under a type name. */
  workflow(type: string, fn: WorkflowFunction): this;
  /** Register an activity implementation under a step name. */
  activity(name: string, fn: ActivityFunction): this;
  /** Ask the polling loops to finish. */
  stop(): void;
  /** Tell the engine which workflow types this worker can replay. */
  register(): Promise<void>;
  /** Start a workflow; resolves to its id. */
  startWorkflow(type: string, input?: string, workflowId?: string): Promise<string>;
  /** Send an external signal to a running workflow. */
  signal(workflowId: string, name: string, payload?: string): Promise<void>;
  /** The engine's view of a workflow: status, steps, output, error. */
  describe(workflowId: string): Promise<Record<string, unknown>>;
  /** Register, then serve workflow decisions and activities until stop(). */
  run(): Promise<void>;
}

# ADR-0008: Workflow and activity timeouts

- **Status:** Accepted
- **Date:** 2026-10-23

## Context

Nothing in workflowd could time out. A lease only proves a worker is alive: a worker that
heartbeats while stuck keeps its task forever, and a workflow waiting on a signal nobody
sends waits forever. `TIMED_OUT` existed as a status but no event produced it. liftoff, the
first production user, needed a bound on webhook deliveries and on whole runs.

Forces:

- **Deadlines must be a function of history.** Timers live in memory and die with the
  process; whatever times out has to come back after a restart or a leader change with the
  same deadline.
- **Old binaries must keep reading new histories** where we can manage it, so a rollback
  costs features, not readability.
- **Leases stay as they are.** They answer "is the worker alive?". A timeout answers "has
  this taken too long?". Mixing them would make a slow but healthy worker look dead.

## Decision

1. **Three limits, all optional** (zero means none), recorded as durations next to the event
   they are measured from:
   - `WorkflowStarted.execution_timeout_ms`: the workflow ends this long after it starts.
   - `StepScheduled.start_to_close_timeout_ms`: one attempt, from dispatch to result.
   - `StepScheduled.schedule_to_close_timeout_ms`: the whole step, from its first
     `StepScheduled`, every attempt and backoff included.

   Retries copy both activity limits onto their `StepScheduled`, like the retry policy.
2. **A workflow that times out records `WorkflowFailed{timed_out: true}`.** The engine
   reconstructs it as `TIMED_OUT`. This follows the `terminated` flag: one terminal event,
   a flag saying why, and an older binary reads a plain `FAILED`.
3. **Start-to-close rides on the lease.** The lease timer fires at the earlier of the lease
   deadline and the attempt's start-to-close deadline; heartbeats move only the first. A
   timed-out attempt fails exactly like a dead worker's (a `StepFailed` the retry policy
   judges), with a timeout message. Leases are in memory, so after an engine restart a
   re-dispatched attempt gets a fresh start-to-close window.
4. **Schedule-to-close is a durable deadline** on a new timer service (`deadlines`, which
   also holds execution timeouts). A retry is planned only if its backoff ends before the
   deadline; otherwise the failure is final at once. If the deadline passes while the step
   is queued, running or (after downtime) between attempts, the engine records a final
   `StepFailed`, drops the lease and the queued task, and advances.
5. **A final `StepFailed` may close a step that is between attempts.** Until now a step
   waiting for a retry could only be re-opened by its next `StepScheduled`. Recording a
   second, final failure for it is how a deadline (and, later, a cancel) ends it without
   inventing an attempt that never ran.
6. Workers learn the attempt's limit: the task carries `start_to_close_timeout_ms`. The Go
   worker gives the activity a ctx with that deadline, TypeScript aborts `task.signal`,
   Python sets `lease_lost`.

## Consequences

- `TIMED_OUT` is real for clients, the dashboard and `workflowd_workflows_finished_total`;
  `workflowd_activity_timeouts_total{timeout}` counts attempts and steps that ran out of time.
- Every SDK sets the limits per call (Go options, TypeScript `startToCloseTimeoutMs` /
  `scheduleToCloseTimeoutMs` / `executionTimeoutMs`, Python seconds, Java `ActivityOptions`).
- `testsuite` honours schedule-to-close when planning retries. Activities take no virtual
  time there, so a start-to-close timeout never fires in a unit test.
- **Rollback:** an older binary ignores every limit (nothing times out), reads a timed-out
  workflow as `FAILED`, and **refuses a history where a final `StepFailed` closed a step
  between attempts** (rule 5). Check the data directory before rolling back past this
  release. An older SDK keeps working against this engine. An SDK that sets a timeout against an
  older engine has that decision refused (unknown field), so the workflow waits until the
  engine is upgraded.

## Alternatives considered

- **A `WorkflowTimedOut` event.** Cleaner to read, but an older binary would refuse every
  timed-out history instead of showing it as failed.
- **Absolute deadlines in the events** (like `TimerStarted.fire_at`). Equivalent for
  recovery, since event timestamps are durable too; durations read better in history and
  copy onto retries unchanged.
- **Start-to-close as its own durable timer.** It would survive restarts, but the attempt it
  measures does not: recovery re-dispatches the step, and the new attempt deserves its own
  window.
- **Heartbeat timeouts** (a per-activity heartbeat interval shorter than the lease). The
  lease duration already plays that role; a per-activity value can come later if needed.

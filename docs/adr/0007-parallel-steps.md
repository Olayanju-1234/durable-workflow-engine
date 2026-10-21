# ADR-0007: Parallel steps and batched commands

- **Status:** Accepted
- **Date:** 2026-10-21

## Context

Until now a workflow had at most one step in flight. The state machine enforced it
(`StepScheduled` while a step was open was a corrupt history), the engine keyed leases
by workflow id, and the workflow-task protocol carried exactly one command. Real
workflows fan out (charge and email at once) and race (a reply against a deadline).

Forces:

- **Replay is from the full history.** SDKs re-run the workflow function against every
  event recorded so far, not decision by decision. Anything the code can observe must
  give the same answer on a later replay that sees more history.
- **Retries are invisible to workflow code.** With one step in flight the engine simply
  didn't ask for a decision during a backoff. With several, another step's result wakes
  the workflow while one is between attempts.
- **Old histories, old SDKs and old engines.** Every sequential history must stay valid,
  and an SDK sending one command must keep working, as must a new SDK against an older
  engine for sequential workflows.

## Decision

1. **History:** several steps may be open. A new step takes the next sequence; a retry
   re-opens a failed activity at its own sequence; a result names its step by sequence.
   `WorkflowCompleted` may leave steps open (they are abandoned).
2. **A failure that will be retried says so:** `StepFailed.will_retry`. Such a step stays
   open to workflow code (`Step.Retrying`); over HTTP it is presented as `SCHEDULED`.
3. **Decide after every result**, not only when nothing is in flight. Recovery also
   decides when the last event was a result, so a crash between a result and its
   decision can't strand a workflow.
4. **Batches:** a decision is every new command the code issued before it blocked
   (`commands`), applied in order and validated whole. A single command still travels in
   the old `command` field.
5. **Leases per step** (workflow id + sequence).
6. **Races are decided by history order.** `WaitAny` returns the future whose step's
   result has the lowest event id (`Step.ResolvedEventID`). That order is fixed once
   written, so a replay that sees both results agrees with the decision that saw one.
   There is no non-blocking "is it done?": its answer depends on when a replay happens.

## Consequences

- Go: `ExecuteActivityAsync`, `NewTimer`, `WaitForSignalAsync`, `Future.Get`, `WaitAny`.
  TypeScript: commands collected until the code is quiescent (one event-loop turn), so
  `Promise.all` works unchanged; `ctx.waitAny` replaces `Promise.race`. Python: `*_async`
  futures, `wait_all`, `wait_any`. Java stays one command per task.
- Markers (`GetVersion`, `SideEffect`) join the batch instead of costing a round trip.
- More decisions per workflow (one per result). For sequential code each extra decision
  replays to the open step and returns a noop; the per-workflow cache keeps it cheap.
- **Rollback:** an older binary refuses a history with two open steps, so it can't
  recover a data directory where any workflow ran parallel steps. Sequential histories
  stay readable both ways.

## Alternatives considered

- **Record decision boundaries (Temporal's WorkflowTaskCompleted events)** and replay
  decision by decision. Allows "is it ready?" queries but doubles the events and rewrites
  every SDK's replay loop. History-order races give the useful part for far less.
- **Workflow-level goroutines/channels.** Non-deterministic scheduling; the futures API
  gives fan-out and races without it.
- **Keep retries silent by never deciding during a backoff.** Breaks as soon as another
  step's result needs a decision.

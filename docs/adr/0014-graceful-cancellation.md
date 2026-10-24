# ADR-0014: Graceful cancellation

- **Status:** Accepted
- **Date:** 2026-10-24

## Context

ADR-0009 gave workflows a hard cancel: a `WorkflowFailed{cancelled}` event ends the run at
once, and the code never learns. That is right when nothing needs undoing, and wrong for a saga:
an order cancelled after its stock was reserved should release the reservation, and only the
workflow code knows what it holds. ADR-0009 named the alternative (deliver the cancel to the
code, let it compensate, then end) and left it for later.

Forces:

- **Replay.** Workflow code is re-run from the top on every decision. The cancellation must
  reach it at the same point every time, or a replay takes a different path and corrupts the
  run. But a cancel request arrives at an arbitrary moment, and history does not record where a
  decision's view of history ended (there are no "workflow task" events).
- **Old binaries and SDKs.** An older engine should keep reading the history; an older SDK must
  not crash on it.
- **The hard cancel stays.** Operators need a stop that does not depend on workflow code.

## Decision

1. **A request, not an ending.** `CancelWorkflow` with `graceful` (HTTP `"graceful": true`, Go
   `client.Graceful()`, TS `{ graceful: true }`, Python `graceful=True`, Java `cancel(id, reason,
   true)`) records `SignalReceived{cancel_request}` under the reserved name `__workflowd.cancel`,
   with the reason as payload, and asks the code to decide. A second request changes nothing.
   User signals may no longer use the reserved prefix. It needs history format 5: an older binary
   would read a signal nobody waits for and never tell the code.
2. **The code is told where it first waits.** At the first point the code would wait on a step
   with no result, every step it started and is still waiting on is *interrupted*: waiting on it
   fails with the cancellation (Go `workflow.ErrWorkflowCancelled`, which matches `ErrCancelled`;
   `CancelledFailure` elsewhere), and the ones still open are cancelled (ADR-0009: workers told,
   timers dropped). The clock moves to the request. Steps the code starts afterwards run
   normally: that is the compensation.
3. **The point is recorded.** The decision that tells the code also records a `cancel` marker
   (a step, kind `MARKER`) holding the history length that decision saw. On later replays the
   SDK shows the code no result recorded after that length until it reaches the marker's
   sequence, so it waits, and is told, at the same point; reaching it at another sequence is
   non-determinism. Results the decision had seen (even ones recorded after the request) stay
   visible; markers are always visible.
4. **How it ends is up to the code.** Failing with the cancellation (or an error wrapping it)
   ends the run `CANCELLED` (`fail_workflow` with `cancelled`); returning normally completes it.
   The engine records `CANCELLED` only when a cancel was requested: code that fails with its
   own step's cancellation, unasked, has failed.
5. **Everyone else:** a data-driven definition (no code) ends `CANCELLED` at once. Go's
   `Sleep` and `WaitForSignal` return no error, so `ctx.CancelRequested()` says the code was told
   (TS `ctx.cancelRequested`, Python `ctx.cancel_requested`, Java `ctx.cancelRequested()`), true
   from the same point on every replay. An SDK before 0.3.0 ignores the request and the workflow
   carries on; the hard cancel still ends it. Registering an SDK now resumes waiting workflows
   that owe a decision, so a request made while no worker ran the type is delivered when one
   connects.

## Consequences

- Sagas compensate on cancellation the way they do on failure; the demo's `OrderSaga` releases
  its reservation, and the dashboard has *Request cancel* beside *Cancel*.
- Every SDK carries the same visibility rule; a new SDK must implement it (the protocol doc
  describes it) or ignore the request.
- `workflowd_cancel_requests_total` counts requests; outcomes stay in
  `workflowd_workflows_finished_total`.
- **Rollback:** a directory with a cancel request or a cancel marker is format 5.

## Alternatives considered

- **Record workflow-task boundaries in history** (Temporal's model). It fixes where every
  decision's view ended, but adds events to every decision of every workflow, and an SDK change
  to read them, for the sake of one feature.
- **Tell the code at the request's position in history** (hide everything recorded after the
  request until it is delivered). Simple, but the decision that delivers it may already have
  seen results recorded after the request, so the first run and its replays disagree.
- **Cancellation scopes** (Temporal's `CancellationScope`). More expressive (shield some steps
  from the cancel), but a large API in four SDKs. Interrupting what the code waits on, and
  letting new steps run, covers compensation.
- **A new event type for the request.** Cleaner to read, but older SDKs would meet an event kind
  they cannot parse; a flagged signal is already in their vocabulary.

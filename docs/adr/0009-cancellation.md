# ADR-0009: Cancelling workflows and steps

- **Status:** Accepted
- **Date:** 2026-10-23

## Context

Running liftoff on workflowd exposed three gaps:

- **No way to say "this work is no longer wanted".** Terminate is an operator's emergency
  stop and reads as one (`TERMINATED`). A customer cancelling an order is a normal outcome.
- **Workers never learned.** A worker running an activity of a finished workflow found out
  only that its lease was gone, which also means "reassigned, someone else is doing it".
- **A race's loser stayed open.** liftoff's custom step waits for a callback or a deadline
  with `waitAny`. When the callback won, the completed workflow showed its timer as
  `SCHEDULED` forever. Worse, a losing signal wait kept its place in the queue of waits: a
  later wait for the same signal never got one, because signals go to waits in sequence order
  and the abandoned wait came first.

Forces: old binaries should keep reading new histories; replay must stay deterministic; and
a loser must not be cancelled behind the code's back, because loops re-race the same step
(remind every day until the approval arrives).

## Decision

1. **Cancel a workflow** with a new `CancelWorkflow` RPC (HTTP `POST
   /api/workflows/{id}/cancel`). It records `WorkflowFailed{cancelled: true}`, which the
   engine reconstructs as a new terminal status, `CANCELLED` (`WORKFLOW_STATUS_CANCELLED = 7`).
   As with `terminated` and `timed_out`, an older binary reads it as `FAILED`. It is a hard
   cancel: the workflow code gets no chance to run cleanup.
2. **Cancel a step** with a `cancel_step` command naming an existing step (by sequence and
   name, checked like every command). The engine records `StepFailed{cancelled: true}` for any
   step kind and reconstructs a new step status, `CANCELLED`. An older binary reads a failed
   step. A step that already has a result is left alone, so a cancel is idempotent. A step
   between attempts is closed by the same final-failure rule ADR-0008 introduced.
3. **Cancellation is explicit.** `WaitAny` does not cancel losers by itself; the SDKs offer
   `ctx.CancelStep(f)` (Go), `ctx.cancel(p)` and `waitAny(ps, { cancelRest: true })`
   (TypeScript), `future.cancel()` and `wait_any(..., cancel_rest=True)` (Python). A cancel
   takes effect in the replay that issues it: waiting on the step fails at once, and for
   `WaitAny` a step cancelled there counts as resolved after every recorded result, which is
   where its cancel event will land. A cancelled step does not move `ctx.Now()`: the code
   caused it, so it is not news.
4. **Presentation:** a step still open when its workflow finished is shown as `CANCELLED`
   ("not needed") in `GetWorkflow`. Nothing is recorded for it; history is unchanged.
5. **Workers are told.** When a step is cancelled or its workflow finishes, the engine
   replaces the step's lease with a tombstone (bounded, in memory) holding the reason. The
   worker's next heartbeat or completion fails with `ErrActivityCancelled`: HTTP 409 with
   `"cancelled": true`, gRPC `FAILED_PRECONDITION` with an `ActivityCancelled` detail. Older
   SDKs stop on the 409 / status code as before. After a restart (no tombstones) the engine
   still answers "cancelled" for a finished workflow by reading its history.

## Consequences

- liftoff's race closes cleanly with `{ cancelRest: true }`, and even without it a finished
  workflow no longer shows a phantom `SCHEDULED` step.
- A queued activity that is cancelled never reaches a worker; a retrying one never retries.
- Metrics: `workflowd_steps_cancelled_total`, `workflowd_workflows_finished_total{status="cancelled"}`.
- **Rollback:** an older binary reads cancelled workflows and steps as failed; it refuses a
  history where a step between attempts was cancelled (ADR-0008, rule 5); and on recovery it
  may retry an activity whose cancel was the last event before the crash (it takes the
  failure for one recorded before `will_retry` existed). An SDK sending `cancel_step` to an
  older engine has that decision refused.

## Alternatives considered

- **A `StepCancelled` / `WorkflowCancelled` event type.** Cleaner names, but an older binary
  would refuse every history containing one.
- **Cancel the losers of every `WaitAny`.** Matches `Promise.race` intuition and would have
  fixed liftoff with no code change, but breaks a loop that re-races a step it still wants.
- **Graceful cancellation** (Temporal-style: deliver the cancel to workflow code, let it run
  compensation, then end). More useful for sagas, and possible later as a new request event;
  it needs a cancellation scope in every SDK. The hard cancel covers liftoff's needs today.
- **Tell the worker through a successful heartbeat** (`cancel_requested: true`). Older
  workers would ignore the field and keep working; failing the call stops them too.

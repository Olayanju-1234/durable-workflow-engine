# ADR-0021: Resetting a finished run

- **Status:** Accepted
- **Date:** 2026-10-25

## Context

When a workflow fails because something outside it was broken (a downstream API down, a bad
deploy of an activity, a missing config value), the operator fixes the cause and wants the
workflow to carry on from where it broke. Before 0.5.0 the only way was to start it again from
scratch (with an id reuse policy that allows it), which re-runs every step that had already
succeeded: charges a card twice, sends a second email. Temporal calls the answer *reset*; liftoff
has the same need for webhooks whose receiving end was down.

Forces:

- **Replay must see exactly the history it saw before.** Workflow code reads the clock
  (`ctx.now()`), seeded random numbers (seeded by workflow id and start time), versions and side
  effects from history. Any change to the events it replays makes it decide differently.
- **The finished run is evidence.** It must stay readable after the reset.
- **Every existing history and binary keeps working**, and a rollback stays possible.
- **One run at a time per id** (ADR-0012): a reset cannot run beside a live run.

## Decision

1. **A reset starts a new run of the same workflow id from a point in a finished run.** The
   point is an event id *E*: the new run starts with events 1 to *E* − 1 of the finished run,
   copied unchanged (ids, timestamps, payloads, format stamps), so the code replays them exactly
   as before and decides afresh from there. The finished run is kept as an earlier run under the
   archive name ADR-0012 defined, as continue-as-new and id reuse leave runs.
2. **Only the WorkflowStarted event changes**: a new `run_id`, `reset_from_run_id`,
   `reset_event_id` and `reset_reason` (new fields older binaries ignore), and an execution
   timeout extended by the time since the start, so the deadline counts again from the reset
   while the recorded start, which the clock and the random seed depend on, stays put.
3. **The default point is the step that broke**: the opening event of the most recent step that
   failed for good or was still open when the run ended (so a timed-out sleep or a terminated
   activity runs again), or with no such step the run's last event (its code decides again how to
   end, which is how a code fix is retried). Never later than a graceful cancel request, which the
   new run would otherwise inherit. Describe reports it (`reset_point`) so a UI can say what will
   run again.
4. **Signals recorded after the point are recorded again** on the new run (name, payload and
   idempotency key), in order: they arrived and must not be lost. A graceful cancel request is
   not: resetting a cancelled workflow is how it is un-cancelled.
5. **The new run is written whole.** `HistoryLog.NewRun` (and `HistoryStore.NewRun`, and the
   cluster's `NewRun` command, with `more_events`) take several events: they are written to
   `<id>.history.next`, fsync'd and renamed over the current file, so a crash leaves the finished
   run current (the reset did not happen) or the whole new run, never part of one.
6. **Then the run resumes from its history alone**, through the same code recovery uses: steps
   open at the point (a parallel step whose result came after it) are queued or re-armed, buffered
   signals are delivered, and a decision is asked for.
7. **Only finished runs.** A live run is refused (`409`, gRPC `FAILED_PRECONDITION`): cancel or
   terminate it first. A run that continued as new is refused too: reset its latest run.
8. **Where:** `ResetWorkflow` (gRPC), `POST /api/workflows/{id}/reset` `{"event_id","reason"}`,
   the Go client's `ResetWorkflow`, `workflowd ctl reset` (one id, several, or every match of a
   filter, asked first), and the dashboard (retry from the failed step, or reset to any card in the
   history). A read-only namespace refuses it, and it counts against the rate limit like any write.

## Consequences

- **No new history format.** An older binary reads a reset run as an ordinary run (it ignores
  the reset fields) and the earlier run as an ordinary archived run: a reset does not block a
  rollback. In a cluster, every node must run 0.5.0 before anything is reset: an older node would
  apply the first event of a reset run and drop the rest (ADR-0011's "upgrade every node before
  using new features").
- Steps after the point run again, including other branches of parallel code whose results came
  after it. Activities must already be idempotent (at-least-once); the reset only re-runs work the
  workflow had not finished using.
- `ctx.runId` (`run_id` on a workflow task) differs in the new run. Code that branches on it is
  not replay-safe across a reset; nothing in the SDKs does.
- A reset run's start time in listings is the original start; its WorkflowStarted says when and
  why it was reset.
- `workflowd_workflows_reset_total` counts resets.

## Alternatives considered

- **Re-run the failed step in the same run** (append a fresh StepScheduled after the failure).
  Simpler on disk, but the code already saw the failure on replay and moved on (compensated,
  failed); undoing that would mean hiding events from replay, a second meaning for history.
- **A WorkflowReset event in the new run.** Clearer on disk, but every SDK, including the
  0.2.0 one liftoff runs, would have to skip an event kind it does not know, and an older binary
  would refuse the history: a reset would block rollbacks.
- **Reset live runs** (terminate, then reset, in one call). Convenient, but an operator should
  decide to stop a live run on purpose; two calls keep that explicit.
- **Not reapplying signals.** Matches the history exactly, but a signal that arrived during the
  failure (an approval, a callback) would be silently lost.

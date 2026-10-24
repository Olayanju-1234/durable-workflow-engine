# ADR-0012: Continue-as-new, runs, and history and payload limits

- **Status:** Accepted
- **Date:** 2026-10-24

## Context

A workflow's history only grows. A workflow that polls every minute, or an entity that lives
for months (a subscription, a tenant), records events forever: every workflow task carries the
whole history to the SDK, every replay walks it, recovery reads it, and the cached state holds
it. Nothing bounded any of this, so the failure would have come as a slow engine, an SDK that
cannot receive its task (gRPC's 4 MiB default), or memory running out, with no message saying
why. Payload sizes were bounded by accident: 1 MiB on HTTP bodies, 4 MiB on gRPC messages,
64 MiB per record, and nothing on what a worker could return.

Forces:

- **The fix belongs to workflow code**, which knows when its state is small enough to carry
  forward (Temporal's continue-as-new). The engine's job is to make that cheap, safe across
  crashes, and to say when it is due.
- **One file per workflow id** is the store (ADR-0006), and the file name is the id. A new run
  under the same id needs the old history out of the way, atomically.
- **Cluster mode replicates every history mutation** (ADR-0004); a new run must be one
  replicated command whose apply is a pure function of the directory and the entry.
- **Older binaries must not misread** what we write (ADR-0011).

## Decision

1. **Runs.** A workflow id has one current run, in `<id>.history`. Every run has a run id,
   recorded on its `WorkflowStarted` (`run_id`, `continued_from_run_id`); a history from
   before run ids is given one derived from its start time, the same on every read.
2. **Continue-as-new is a terminal flag:** `WorkflowCompleted.continued_as_new` carries the next
   run's id, type, input and execution timeout. Status `CONTINUED_AS_NEW`. It needs history
   format 5: an older binary would read it as a plain completion and never start the next run.
3. **A new run is one store operation, `NewRun(id, archiveID, first)`:** hard-link the current
   file to the archive name and fsync the directory; write the new run's first event to
   `<id>.history.next` and fsync it; rename it over `<id>.history` and fsync the directory. At
   every moment `<id>.history` is either the finished run or the new one. A crash before the
   rename leaves the finished run current, and recovery redoes the step from its last event
   (the link may already exist: the same file is fine). In a cluster it is a `NewRun` Raft
   command; a replay of the log skips it once the new run's first event carries its index.
4. **Archived runs are history files with a reserved name,**
   `__workflowd-run~<closed at, hex nanos>~<run id>~<workflow id>`. Recovery, the index and
   listing skip reserved names (as older binaries already do), snapshots carry them with no
   change, and the name alone gives retention the close time and `ListWorkflowRuns` the owner.
   New workflow ids are limited to 190 bytes so the name fits in 255.
5. **Timers name their run.** A timer, retry or deadline armed for a run that has since
   continued as new must not fire into the next, which reuses step numbers and names.
6. **Limits** (flags; defaults in brackets): events per run [51,200], bytes per run [50 MiB],
   bytes per payload [2 MiB]. At 80% of either history limit the engine logs, counts
   (`workflowd_history_limit_total{kind="warned"}`) and tells the code to continue as new. At
   the limit, the event that would cross it is not written: the workflow fails with a message
   that names the limit and the remedy, and the caller gets `ErrHistoryLimit`. The terminal
   event is always written, so a history ends at most one event over.
7. **The suggestion is deterministic.** The task carries the id of the event where the history
   crossed 80%; the SDKs answer `continueAsNewSuggested()` true once the code has observed a
   result at or after it. Replays reach the same answer at the same point of the code, however
   far the history has grown since. Continuing as new before the steps history recorded is
   reported as non-determinism, so stale state can never be carried forward.
8. **Payloads are checked by the engine**, the same for HTTP and gRPC: a start input or signal
   over the limit is refused (413 / `RESOURCE_EXHAUSTED`); an activity output over it fails the
   step as `PayloadTooLarge`, non-retryable (every retry would return the same); a workflow
   output, step input, marker or next-run input over it fails the workflow, since its code
   will decide the same on every replay. Transports accept a request of four payloads plus
   1 MiB (HTTP body and gRPC message), and the Go client receives up to 128 MiB, room for a
   full history.

## Consequences

- A workflow can run forever with a bounded history; earlier runs stay readable
  (`ListWorkflowRuns`, `GetWorkflow?run_id=`) until retention removes them.
- Signals the finished run had not consumed are not carried over, and signal idempotency keys
  are per run: drain signals before continuing.
- Retention now also purges archived runs, and cancelled workflows (which it used to skip).
- Upgrading applies the limits to existing workflows: one already past 51,200 events fails at
  its next event. Raise `--max-history-events` before upgrading if any are that long.
- **Rollback:** a directory where a workflow continued as new is format 5. Binaries before
  0.3.0 ignore archived runs and the new fields, and read an interrupted continue-as-new as
  completed.

## Alternatives considered

- **Copy the old history to the archive event by event.** Simple in a cluster (existing append
  commands) but costs a full history of writes for exactly the histories that are largest.
- **Truncate the history in place, keeping no earlier runs.** Cheapest, but the earlier runs
  are what an operator reads when a long-lived workflow misbehaves.
- **A directory per workflow for its runs.** Tidier, but snapshots, retention and the cluster's
  delete would all need to learn subdirectories; reserved names reuse what exists.
- **Suggest continue-as-new from the task's history length.** Not deterministic: a replay of
  an early point of the code would see today's length and take a different branch.

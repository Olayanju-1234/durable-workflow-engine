# ADR-0016: Named task queues

- **Status:** Accepted
- **Date:** 2026-10-24

## Context

Tasks have been routed by name alone: an activity task by its step name, a workflow task by its
workflow type. Two workers that implement the same activity name for different apps therefore
share one queue. The public demo hit this at once: its in-process worker runs `reserve`,
`charge`, `ship` and `release`, which are exactly the activity names the SDK examples use, so an
example pointed at the demo had its activities run by the demo worker (and the demo's by the
example). Namespaces (ADR-0015) separate tenants, but one tenant can have the same problem
between its own services, versions or deployment groups.

Forces: workers and clients that know nothing of queues must behave exactly as before; a
workflow's activities should go where its workers are without every call naming a queue; and
routing must survive retries, restarts and continue-as-new, like everything else in history.

## Decision

1. **Every task is on a task queue**, within its namespace. A poller names one queue (the
   `task_queue` field on `PollTask` / `PollWorkflowTask`, `?task_queue=` on HTTP) and receives
   only that queue's tasks; the step-name or type filter still applies inside the queue. A
   poller that names none polls the **default queue**, which is every engine's only queue
   before this ADR.
2. **A workflow runs on the queue it was started on** (`StartWorkflowRequest.task_queue`,
   `client.WithTaskQueue`, the SDKs' `taskQueue`), recorded on `WorkflowStarted.task_queue`.
   Its workflow tasks go there, and so do its activities unless the code names another queue
   for one (`ScheduleActivityCommand.task_queue`, `workflow.WithTaskQueue`, the SDKs' activity
   option). The queue an activity resolves to is recorded on its `StepScheduled`, so retries,
   lease reassignments and recovery send it to the same place without consulting the workflow.
   Continue-as-new keeps the queue unless the code names a new one; schedules name theirs.
3. **The default queue is recorded as nothing.** `"default"` and `""` both mean it, and history
   records it as an empty field, so a deployment that never names a queue writes exactly what it
   wrote before.
4. **Format 6.** An older binary would ignore the field and hand a named queue's tasks to
   default-queue pollers: the collision this fixes, on a rollback. Events that carry a named
   queue are stamped `min_reader_format = 6`, the same format as namespaces (both are 0.4.0).
5. **Names** are 1 to 128 letters, digits, dots, underscores and hyphens; anything else is
   refused (400 / `INVALID_ARGUMENT`, or an invalid command for an SDK).
6. **Registration is unchanged.** A worker still registers the workflow types it can replay in
   its namespace; the queue a run is on decides which of those workers receives its tasks.
   SDK workers start workflows on their own queue by default, since the worker that starts a
   workflow is usually the one that runs it.

## Consequences

- The demo worker stays on the default queue; the SDK examples run on `ts-orders`, `py-orders`
  and `java-orders`, so pointing one at the demo no longer steals or loses activities.
- A workflow started on a queue no worker polls waits, visibly (`task_queue` in describe and
  the dashboard), until one does. That is the price of explicit routing, as in Temporal.
- Queues are created by use: there is nothing to configure, and no per-queue limits yet.
- The test environment (`testsuite`) runs every activity in one process and ignores queues.

## Alternatives considered

- **Prefix activity names per app** (`liftoff.charge`). Works today, but every workflow and
  worker has to agree on the convention, and history and dashboards carry the noise.
- **Route by worker identity (sticky routing).** Solves a different problem (cache locality),
  which ADR-0006's numbers showed we do not have.
- **Resolve an activity's queue at dispatch from the workflow's.** Fewer bytes in history, but
  then the fold must resolve defaults, and a retry after a continue-as-new or a code change
  could route differently from the first attempt. Recording the resolved queue keeps routing a
  function of the step's own events.

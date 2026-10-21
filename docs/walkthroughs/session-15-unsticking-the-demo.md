# Session 15 — Unsticking the public demo

*Plain-English walkthrough for a Node/TS developer who is new to Go. This session
was driven by production, not the roadmap: we looked at the live server, found
four problems, and fixed them as three PRs plus some server changes.*

## The problem: a demo that never moved

The live dashboard showed one `OrderSaga` stuck on `WAITING` since 7 October.
Nothing was broken in the engine. It was doing exactly what it should.

Remember the split: the **engine** decides what happens next and records it; a
**worker** does the actual work (reserve stock, charge a card). The engine never
runs your activity code itself. On the VPS we deployed the engine and no workers.

The Node equivalent: a BullMQ queue with a job sitting in it and no consumer
process running. The job waits forever, durably, which is the correct behaviour.
The demo just needed a consumer.

## Why "just add a worker" was not safe

The obvious fix was a small worker built into the binary. But reading the queue
code turned up a catch: **there was one global activity queue**. Every worker got
every task, whatever its step name. The SDKs answer a task they don't recognise
by failing it ("no activity registered for step X").

In BullMQ terms, it was as if every job type went into one queue and every
consumer took whatever came next. Run two consumers that handle different job
types and each one fails the other's jobs.

So an in-process demo worker would have grabbed and failed tasks belonging to
anyone else's worker. The real fix had to come first: **route tasks by name.**

### The routing change

A worker now says which activities it implements when it polls:

```
GET /api/activity-tasks?activities=reserve,charge     (HTTP SDKs)
PollTaskRequest{ worker_id, step_names: [...] }       (gRPC)
```

In Go, the engine method grew a *variadic* parameter:

```go
func (e *Engine) PollTask(ctx context.Context, workerID string, stepNames ...string) (*Task, error)
```

`...string` is TypeScript's rest parameter, `(...stepNames: string[])`. Every
existing call like `PollTask(ctx, "w")` still compiles and means "any task", so
nothing else had to change.

### One queue type, two uses

The polyglot work (Session 13) already had a filtered queue for *workflow*
tasks. Rather than write a second copy for activity tasks, I made it generic:

```go
type routedQueue[T any] struct {
    route   func(T) string   // how to read an item's key: step name, or workflow type
    mu      sync.Mutex
    pending []T
    ready   chan struct{}
}
```

That's `class RoutedQueue<T> { constructor(private route: (t: T) => string) }`.
Both queues are now one line each:

```go
newRoutedQueue((*Task).GetStepName)            // activity tasks, keyed by step
newRoutedQueue((*WorkflowTask).GetWorkflowType) // workflow tasks, keyed by type
```

Two Go ideas in there you won't have met in Node:

- **The mutex.** JavaScript runs your code on one thread, so two callbacks can
  never touch an array at the same instant. Go's goroutines really do run in
  parallel, so two pollers could both grab the same task. `mu.Lock()` makes
  "find a match and remove it" one indivisible step. That guarantees no task is
  handed out twice.
- **Closing a channel as a broadcast.** Pollers wait on `ready`. Each new task
  closes that channel, which wakes *every* waiter at once, and then swaps in a
  fresh one. Picture a shared Promise that everyone awaits: you resolve it, then
  replace it with a new pending one for the next wait. Each woken poller rechecks
  whether the new task is one it can do.

The SDKs got a related fix. An empty filter means "anything", so a TS worker
with no activities registered used to poll anyway and fail everyone's tasks. It
now skips that loop entirely.

## The demo worker

`--demo-worker` starts four goroutines (think four `while (true)` async loops)
that poll only for `reserve`, `charge`, `ship` and `release`. To make the demo
worth watching:

- Each step takes 0.8–2 s, so you can see it move on the dashboard.
- A quarter of first `charge` attempts fail with "payment gateway timeout". The
  retry policy kicks in and the second attempt succeeds, so visitors see retries
  without having to ask for them.
- Put `decline` in the input and `charge` fails every time. After 3 attempts the
  saga compensates (`release`) and the workflow fails with a clear reason.

Using random numbers here is fine, but it would be **forbidden inside a workflow**.
The rule from Session 8 still applies: workflow code is replayed, so it must be
deterministic. Activity code runs once and its *result* is recorded, so it can do
anything: random numbers, clocks, HTTP calls.

### A bug the test caught

My first version assumed every step receives `reserved:<order>` as input. That's
true for `OrderSaga`, which passes reserve's output to every later step. But
`OrderWorkflow`, the older data-driven type, *chains* outputs: ship receives
charge's output. The test expected `shipped:order-7` and got
`shipped:charged:order-7`. That's a one-line fix, and the kind of thing that's only
obvious once a test runs the real workflow types.

### In production

After deploying, the engine's startup recovery replayed the stuck workflow's
history, saw `reserve` was in flight, and put it back on the queue. The demo worker
picked it up a millisecond later. **It finished in about 5 seconds, 14 days late.**
That's durable execution doing its job: nothing was lost, it just waited.

## `--retention`: a TTL for finished workflows

Every workflow is a file on disk, and files were never deleted. On an open demo
that means disk use only grows. `--retention 168h` deletes histories of
**finished** workflows whose last event is more than a week old.

The Mongo analogy is a TTL index (`expireAfterSeconds`) with a condition on top:
only completed, failed or terminated workflows are eligible. A workflow that has
been waiting for a signal for a year is live, so it stays.

Two details worth knowing:

- Deleting a file isn't durable until the *directory* is fsync'd, for the same
  reason a new file isn't (Session 5). `Delete` does both.
- The purge takes the engine's global lock **per workflow**, not for the whole pass,
  so cleaning up thousands of old files never freezes live traffic.

It's built and tested but **not switched on in production**, because it deletes
data. That's your call.

## Shutdown, done properly

Two small fixes. The dashboard's HTTP server was never told to shut down on
SIGTERM, and SDK long-polls (up to 25 s each) would have held it open anyway.
Now every request's context comes from the shutdown signal:

```go
BaseContext: func(net.Listener) context.Context { return ctx }
```

That's like handing every Express request an `AbortSignal` from one shared
`AbortController`. Calling `abort()` on shutdown makes every waiting long-poll
return immediately, with a clean 204.

The ordering of cleanup uses Go's `defer`, which runs in **reverse** order of
declaration (like nested `finally` blocks unwinding):

```go
defer engine.Close()   // runs 3rd
defer bg.Wait()        // runs 2nd: wait for the demo worker to stop
defer stop()           // runs 1st: cancel the context, which tells the worker to stop
```

## The server side

None of these are Go, but they matter just as much:

- **Rate limit (nginx).** Anyone could create workflows through the public API,
  without limit. nginx now allows 10 writes a minute per IP (bursts of 20) on the
  demo, and caps request bodies at 16 KB. Reads and the dashboard's polling are
  untouched. It's `express-rate-limit`, applied before requests reach the app.
- **Backups.** A daily systemd timer (cron's modern cousin) tars the data folder
  and keeps 14 days. It's on the same disk, so it covers mistakes, not losing the disk.
- **Rollback.** The previous binary is kept as `workflowd.prev`. Undoing a bad
  deploy is one copy and a restart.

One lesson from testing the rate limit: my first test sent 25 requests one after
another and all got through. From the laptop each request took ~0.8 s, and at 10 a
minute the limit refills one slot every 6 s, so the sequential test was too slow
to hit the limit. 30 parallel requests showed it working: 24 got `429`.

## What's next

- Decide whether to switch on `--retention` in production (suggested: a week).
- A small leak spotted along the way: the store keeps one open file handle per
  workflow and only closes it on delete or shutdown. Finished workflows should
  release theirs.
- Then back to the roadmap: Phase 4, Raft.

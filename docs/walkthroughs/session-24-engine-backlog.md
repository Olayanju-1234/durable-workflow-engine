# Session 24 walkthrough: clearing the engine backlog

This session finished every engine and SDK item left on the roadmap: six pull requests
(#19, #22–#26) plus this docs one. Here is what each piece does, why it was built the way
it was, and the Go ideas it uses, with Node/TypeScript comparisons.

## A reminder of the model

A workflow is a function the engine re-runs from the top every time something happens.
Every durable call (`ctx.activity`, `ctx.sleep`, ...) is written to an append-only
history file. On a re-run, calls that already have a result return it instantly; the first
call without one is "what happens next". If you know Redux: the history is the list of
actions, the workflow's state is what you get by running the reducer over all of them, and
crash recovery is "replay the actions".

## 1. SDK features (PR #19)

**Retry policies from code.** Before, only the engine operator could say "retry `charge`
three times". Now the workflow says it on the call:

```js
await ctx.activity('charge', order, { retry: { maxAttempts: 3, initialBackoffMs: 500 } });
```

Like Bull's `attempts` + `backoff` job options, with one difference: the policy is written
into the history event that schedules the step. If the engine restarts during a backoff,
it reads the policy back from disk and carries on. A test proves it: it deletes the policy
from the replay code and the "survives a restart" test fails.

**Versions in every language.** `getVersion('fraud-check', DEFAULT_VERSION, 1)` is a
feature flag frozen per execution: the first time an execution reaches it, the answer is
recorded; old executions that ran past that point before the change get "old code".

**Binary payloads.** JSON strings can't carry arbitrary bytes, so the engine adds a
`input_base64` twin when the bytes aren't valid UTF-8 (`Buffer.from(b64, 'base64')` on the
Node side). Normal text payloads look exactly as before.

**Following the leader.** In a cluster, a follower answers `503` with
`{"leader_http": "10.0.0.2:8233"}`, like a `307 Temporary Redirect` to the primary
database. The SDKs move there, and because a new leader starts with an empty registry, they
register their workflow types again. The e2e test gives the worker only a fake follower
and restarts the engine halfway through; it fails if either behaviour is removed.

## 2. Per-workflow locks and a state cache (PR #22)

**The problem.** The engine had one `sync.Mutex` for everything, like one global
`async-mutex` around every request handler. Worse, every operation re-read the whole
history file and replayed it, so a step on a 1000-step workflow did 1000 events of work.
That's the "O(n²) ceiling": total work grows with the square of the history.

**The fix, in two parts.**

1. One lock per workflow: a `map[workflowID]*wfState`, each with its own mutex, like a
   `Map<string, Mutex>` keyed by user id. Two workflows never wait for each other.
2. Keep each workflow's events and state in memory and fold each new event in
   (`state.Folder.Apply(event)`), like a Redux store applying one action instead of
   re-running the reducer over the whole action log.

```go
w := e.lockWorkflow(id)   // like: const release = await mutexFor(id).acquire()
defer e.unlockWorkflow(w) // like: try { ... } finally { release() }
```

`defer` runs when the function returns, however it returns: Go's `finally`.

**The subtle rule:** if writing an event fails, we don't know whether it reached the disk
(the write may have landed and only the fsync failed). So the cache for that workflow is
thrown away and the next operation reads the file again. A test fakes exactly that case.

**Numbers:** a step on a 1000-step workflow went from 2.25 ms to 49 µs, and stays flat as
the history grows.

Go ideas: `container/list` is a doubly linked list, used here as a simple LRU (least
recently used) cache of idle workflows, the same shape as the `lru-cache` npm package.

## 3. The visibility index and a storage decision (PR #23)

The dashboard's list used to open and replay every history file on each request: a full
table scan. Now the engine keeps one small summary per workflow in memory, built once at
startup (during recovery, which reads everything anyway) and updated on every write. It's
a database index. Listing 10,000 workflows went from 734 ms to 0.75 ms.

The roadmap had "move to an LSM storage engine (BadgerDB) if needed". With real numbers
(ADR-0006), it isn't: reads are now cached, file count is fine with retention, and the
remaining cost is the fsync per event, which can be batched later without a new
dependency. Deciding not to build something, with evidence, is a real outcome.

## 4. Cron schedules (PR #24)

`node-cron` inside your app fires in every replica. These schedules are durable and fire
once, on the cluster leader:

```go
c.CreateSchedule(ctx, client.Schedule{ID: "nightly", Cron: "0 2 * * *", WorkflowType: "Report"})
```

Two tricks:

- **Idempotent run ids.** Each run is started as `nightly-20261022T020000Z` (the fire
  time). If a restart or failover makes the engine try the same fire again, it gets
  "already exists" and moves on. It's the same idea as Stripe's idempotency keys.
- **Stored as a pretend workflow.** The schedules live in a reserved history made of event
  types that already existed, so older engine versions can still start on the same data
  directory, and Raft replicates schedules without any new code.

The cron parser is hand-written (`internal/cron`) to keep zero dependencies, and the tests
cover daylight-saving changes in both directions.

## 5. Parallel steps (PR #25, ADR-0007)

The biggest change. Before, a workflow did one thing at a time. Now:

```js
const [stock, price] = await Promise.all([ctx.activity('stock', sku), ctx.activity('price', sku)]);
```

How the TypeScript SDK makes that work: a step with no result returns a promise that never
settles (the old trick: `catch` can't intercept it). Steps with results return already
resolved promises, so the function runs as far as it can in microtasks. After one turn of
the event loop (`setImmediate`), everything the code started is collected and sent as a
batch: three activities at once.

**Why `Promise.race` is banned in workflows.** Imagine racing a reply against a deadline.
The first time, only the deadline has fired, so race picks the deadline. Later, both have
results, and `Promise.race` picks by call order: the reply. The same code took a different
branch on replay. `ctx.waitAny` picks by the order the history recorded the results, which
never changes. A test builds exactly that history and fails if `WaitAny` picks by call
order.

**Retries must stay invisible.** If `a` fails and will be retried while `b` finishes, the
workflow wakes up for `b`. It must not see `a`'s failure, so the failure event now says
`will_retry`, and the step counts as still running.

Go has the same API with futures: `ExecuteActivityAsync` returns a `Future` whose `Get()`
waits, like a promise you `await`.

## 6. Exactly-once signals (PR #26)

A signal sent to a cluster leader that dies before replying: did it land? The client
retries, and before this change the retry could record it twice. Now every signal carries
an idempotency key (`request_id`), stored in the history next to the signal, so the next
leader sees it and doesn't record it again. The test uses a server that records the signal
and then pretends the connection dropped, which is exactly the failure being fixed.

## 7. Sticky queues: measured, not built

Temporal sends a workflow's tasks back to the worker that has it in memory, to skip the
replay. Measuring our replay: 74 µs for a 100-step workflow, 0.7 ms for 1000. That's at or
below the network round trip the task itself costs, so a worker-side cache would add a lot
of moving parts to save very little. The roadmap item is closed with that reasoning.

## How the claims are tested

Every claim that matters has a test, and each of those tests was checked by breaking the
code on purpose (a "mutation") and watching the test fail: removing the per-workflow lock,
keeping the cache after a failed write, keying leases per workflow instead of per step,
making `WaitAny` pick by call order, sending only the first command of a batch, and so on.
A test that still passes with the feature broken isn't testing the feature.

## What's next

Publishing the SDK packages (an owner step), a "continue-as-new" for very long histories,
batching fsyncs if write rates need it, and parallel steps in the Java SDK.

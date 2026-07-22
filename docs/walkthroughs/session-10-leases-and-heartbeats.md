# Session 10 Walkthrough — Leases & Heartbeats (Phase 3, Week 8)

*Plain-English tour of what was built and why, with Node/TS analogies. Written for
someone who knows Express/NestJS/Mongo but is new to Go and durable execution.*

## The problem we finally fixed

Since Week 4 the engine has survived **its own** crashes: kill the engine, restart
it, and every workflow picks up where it left off. But there was a hole we've been
carrying on purpose (logged in every session since Session 5): if a **worker**
crashed while holding a task, nobody ever noticed. The engine had handed the step
out and was politely waiting forever for a result that would never come. The
workflow sat in WAITING until you restarted the whole engine.

Think of it like a BullMQ job that a consumer picked up and then the consumer pod
got OOM-killed. If your queue has no *visibility timeout*, that job is just...
gone. Stuck "active" forever. Redis-backed queues solve this with a lock + a
stalled-job checker. That's exactly what we built — except ours produces a durable
audit trail in the history log.

## The idea: a lease is a claim with an expiry date

Three rules, and the whole feature falls out of them:

1. **When a worker takes a task, it doesn't own it — it leases it.** The engine
   hands over the task with a random **lease token** (a 128-bit unguessable id,
   like a session token) and a deadline (default 30 seconds).
2. **The worker must finish, or check in, before the deadline.** Finishing is
   `CompleteTask` (now requires the token). Checking in is the new **`Heartbeat`**
   RPC — "still alive, still working" — which pushes the deadline back out. Like
   `SETEX` refreshing a Redis lock's TTL.
3. **Miss the deadline and the engine assumes you're dead.** The task is taken
   away and given to the next worker who polls. If the "dead" worker was actually
   just slow and comes back later (a **zombie**), its stale token is rejected —
   it can't record anything over the new attempt's work.

## The clever part: a dead worker is just a failed attempt

Here's the design decision that made this week small instead of big. When a lease
expires, what should the engine *do*? We could have invented a new "reassignment"
mechanism. Instead, the engine records a normal step failure in history —
`StepFailed("lease expired: worker \"zombie-1\" missed its deadline")` — and hands
it to the **retry machinery we already built in Week 7**.

So reassignment is literally a retry:

- Attempts remaining on the step's `RetryPolicy`? Back off, then re-dispatch to
  whoever polls next. That's the reassignment.
- Attempts exhausted (or no retry policy)? The workflow sees the error, exactly
  like a worker that *reported* a failure — and a Saga workflow can catch it and
  compensate.

Why this is right and not just lazy:

- **It's durable.** The expiry is an event in the log, so it survives engine
  restarts and shows up when you read the history. (The lease bookkeeping itself
  is in-memory and disposable — same philosophy as the task queue and timers:
  durable facts in the log, throwaway scheduling in RAM.)
- **It's bounded.** A poisonous task that crashes every worker that touches it
  burns through `MaxAttempts` and then fails the workflow, instead of crash-looping
  workers forever.
- **It's honest.** With no retry policy, one dead worker = the workflow fails with
  an error naming the worker. Loud and debuggable beats silently stuck. If a step's
  workers might die mid-run, give it `MaxAttempts > 1` — that's now part of what a
  retry policy *means*.

## What the code looks like now

New file, [internal/server/lease.go](../../internal/server/lease.go). The `lease`
struct is tiny — who holds what until when:

```go
type lease struct {
    workflowID string
    stepName   string
    sequence   int64
    workerID   string    // who — named in the expiry error
    token      string    // proof of ownership
    deadline   time.Time // extended by Heartbeat
}
```

The engine keeps `map[string]*lease` keyed by workflow id (one step in flight per
workflow ⇒ one lease per workflow). In TS you'd write `Map<string, Lease>` — same
thing.

**The expiry checker reuses our timer service.** We already had a min-heap timer
goroutine for durable `Sleep` steps, and a second one for retry backoffs. Leases
add a third. When a lease is granted, we arm a timer at its deadline. When it
fires, the callback (`onLeaseExpired`) looks at the *current* state under the
engine's lock and decides:

- Lease gone? The worker finished in time — do nothing.
- Deadline moved? A heartbeat came in — re-arm the timer at the new deadline and
  check again later. (Heartbeats don't create timers; the pending fire just
  notices and reschedules itself. Lazy, simple, correct.)
- Deadline actually passed? Record the failure, run the retry logic.

This "fire, then re-check reality under the lock" pattern is the same one the
Sleep timers and retry timers use — a stale fire is always a harmless no-op. In
Node terms: the `setTimeout` callback never *trusts* its own scheduling; it
re-reads the source of truth first. That discipline is what makes the concurrency
safe.

**Functional options — a Go idiom worth knowing.** Tests need 100ms leases;
production wants 30s. Go doesn't have optional/named parameters, so the idiomatic
substitute is *functional options*:

```go
// TS: new Engine(store, logger, { leaseDuration: 100 })
// Go:
e := NewEngine(store, logger, WithLeaseDuration(100*time.Millisecond))
```

`WithLeaseDuration(d)` returns a function that mutates the engine during
construction, and `NewEngine(..., opts ...Option)` applies whatever it's given.
Zero options = old behavior, so no existing caller changed. You'll see this
pattern all over real Go libraries (gRPC itself uses it: `grpc.WithTransportCredentials(...)`).

## The zombie test (the week's deliverable)

`TestLeaseExpiryReassignsTaskAndRejectsZombie` tells the whole story:

1. Start a one-step workflow with a 100ms lease and `MaxAttempts: 2`.
2. Worker "zombie" polls, gets the task + token... and goes dark.
3. ~110ms later the reaper records the expiry; the retry re-dispatches.
4. Worker "healthy" polls, gets **the same step** with a **different token**,
   completes it. Workflow COMPLETED, `Attempts == 2`.
5. Now "zombie" wakes up and tries to `CompleteTask` with its old token —
   **rejected**. The log shows exactly one completion, plus a `StepFailed`
   naming the zombie. Full audit trail.

And we **teeth-verified** the suite: temporarily disabling the reaper makes the
expiry tests fail exactly the way the old bug behaved — the healthy worker waits
forever because the task is never reassigned. So the tests genuinely guard the
feature, they don't just pass by coincidence.

There's also a heartbeat test with a nice property: the workflow has *no* retry
policy, so if the lease ever expired the workflow would visibly FAIL. The worker
"works" for 1.5s on a 500ms lease, heartbeating every 100ms, and finishes with
`Attempts == 1` — proof the heartbeats really extended the lease three times over.

## What we deliberately did NOT build

The roadmap line mentioned **sticky queues / worker affinity**. Deferred, with
reasoning: sticky queues exist (in Temporal) to keep a *workflow's replay cache*
warm on one worker — the worker remembers the workflow state so it doesn't have to
re-replay history every decision. But in our architecture **the engine does all
the replaying**; workers are stateless functions that take input bytes and return
output bytes, like a pool of identical Lambda handlers. There is no worker-side
state to be sticky *about*. If the Phase-5 SDK ever moves replay onto workers,
we'll revisit.

## Where we are

- **Engine crash?** Survived since Week 4 (the Kill Test).
- **Worker crash?** Survived as of today — reassigned, bounded by retry policy,
  audited in history. The debt ledger entry from Session 5 is closed.
- **Next (Week 9):** observability — Prometheus `/metrics` and a visibility API
  (list workflows, get one, terminate one). Making the engine *inspectable* the
  way `temporal workflow list` is.

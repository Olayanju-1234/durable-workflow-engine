# Session 27 walkthrough: 0.3.0, workflows that run forever

A product review after the 0.2.0 release ranked what still stopped workflowd from being trusted
with long-lived, real work: histories that grow forever, no rule for what is safe to roll back,
no way to say "start this, or give me the one already running", a cancel that couldn't let the
code clean up, and Python and Java SDKs that lagged behind TypeScript. This session shipped all
five as separate pull requests (#42–#46), released the SDKs as 0.3.0 (#47) and redeployed both
engines. Here is each piece in plain terms, with Node/TypeScript comparisons.

## A reminder of the model

A workflow is a function the engine re-runs from the top every time something happens. Each
durable call (`ctx.activity`, `ctx.sleep`, ...) is an event in an append-only history file. On a
re-run, calls that already have a result return it at once; the first one without a result is
"what happens next". Think Redux: the history is the action log, the workflow's position is the
reducer's output, and replay is running the reducer again.

Two rules shaped everything below:

1. **Anything the engine must remember across a crash has to be in the history.**
2. **A replay must take exactly the same path every time.** If re-running the code makes a
   different call at step 7 than the one recorded, the run is corrupt. That is called
   non-determinism, and most of this session's hard thinking was about avoiding it.

## 1. An upgrade/rollback contract (PR #42, ADR-0011)

**The problem.** Upgrading workflowd is always safe: new binaries read every old history. Rolling
back is the risky direction, because an old binary meets events it doesn't understand. Three
times last week a release note had to say "older binaries can't read X". That is a warning in
prose that nothing enforces.

**The fix: numbered history formats.** Like a database schema version, but raised only when a
feature that needs it is actually used:

| Format | What it adds | First binary that reads it |
|---:|---|---|
| 1 | the basics | every binary |
| 2 | version / side-effect markers | 0.1.0 |
| 3 | two steps in flight at once | 0.1.0 |
| 4 | cancelled steps, steps closed between retries | 0.2.0 |
| 5 | continue-as-new, graceful cancellation | 0.3.0 |

- Each event that an older binary would misread carries `min_reader_format` (a stamp).
- The data directory has a `FORMAT` file with the highest format any history needs. The store
  raises it **before** writing the first event that needs it, so a crash can only leave it too
  high (a rollback looks less safe than it is), never too low.
- A binary from 0.3.0 on **refuses to start** on a directory in a newer format, before reading a
  single history, with a message that says which release it needs. Before, it would start and then
  fail on individual workflows with something like `event 5 has unknown or empty attributes`.
- `workflowd --check-format --data-dir /opt/durable/data` tells an operator what a directory needs
  and whether this binary can serve it. `docs/UPGRADING.md` is the procedure.

Node analogy: it's like the `engines` field in `package.json` plus a migration table, except the
"migration" never runs: the version only records which readers are allowed in.

## 2. Python and Java catch up (PR #43)

TypeScript got `ctx.now()`, `ctx.random()` and signal-wait timeouts in 0.2.0. Now Python and
Java have them too. The interesting one is the clock: `Date.now()` in workflow code is forbidden
because a replay next week would read a different time and could take a different branch.
`ctx.now()` instead returns "when the latest step result the code has seen was recorded", which
is in the history, so every replay reads the same value. `ctx.random()` is a pseudo-random
generator seeded from the workflow id, so it produces the same sequence on every replay.

Java was also rebuilt to start several steps at once (`activityAsync` returns a `Future`, like a
`Promise` you haven't awaited yet), to cancel a step, and to let an activity check
`actx.isCancelled()`. The laptop has no JDK, so a new Java end-to-end test runs in CI only; it is
set to **fail** rather than quietly skip there, so a missing JDK can't hide a broken SDK.

## 3. Continue-as-new and limits (PR #44, ADR-0012)

**The problem.** A workflow that loops (poll every minute, bill every month) adds events forever.
Every replay walks the whole history, so it gets slower until something breaks.

**The fix.** The workflow code says "I'm done with this run; start a fresh one with this state":

```ts
while (!ctx.continueAsNewSuggested()) {
  await ctx.activity('bill', state);
  await ctx.sleep('month', 30 * 86_400_000);
}
return ctx.continueAsNew(state); // same workflow id, empty history, new run id
```

It is like a tail call, or like rotating a log file: the workflow id stays the same (clients keep
using it), but the history starts empty and the run gets a new **run id**.

**How it's stored, crash-safely.** The old run's history file is hard-linked to an archive name
(`__workflowd-run~<closed at>~<run id>~<workflow id>`), the new run's first event is written to a
temporary file, and that file is renamed over the old one. A rename is atomic, so at every moment
the workflow's file is either the whole old run or the new one. If the machine dies before the
rename, the old run is still current, and its last event says what the next run should be, so
recovery simply does the swap again. The archived runs stay readable (`GET /api/workflows/{id}/runs`)
until retention deletes them.

A bug this prevented: timers are keyed by workflow id and step number. A new run reuses step
numbers, so a timer left over from the old run at step 3 would have fired into the new run's step
3. Every timer now carries its run id, and a timer for an old run is ignored.

**Limits.** Each run's history is capped (51,200 events, 50 MiB by default) and so is each payload
(2 MiB, the same on HTTP and gRPC). At 80% the engine warns and `continueAsNewSuggested()` turns
true. At the limit the workflow fails with a message that says why and what to do, instead of
slowly grinding.

**Why the suggestion had to be designed carefully.** The naive version, "is the history big right
now?", is not deterministic: replaying an early part of the code today would see today's big
history and take the continue branch at the wrong place, throwing away later steps. Instead the
engine tells the SDK *which event* crossed 80%, and the SDK answers true only once the code has
seen a result at or after that event. An early point in the code always answers false, on every
replay.

## 4. Workflow id reuse (PR #45, ADR-0013)

Starting a workflow under your own id (`order-42`) makes starts idempotent: a second start gets
a 409. Two options now change that:

- **id reuse policy**: may a *finished* id start again? `REJECT_DUPLICATE` (default),
  `ALLOW_DUPLICATE`, or `ALLOW_DUPLICATE_FAILED_ONLY`. The finished run is kept as an earlier run.
- **id conflict policy**: `USE_EXISTING` returns the run that holds the id instead of a 409:
  "start it, or give me the one already there", which is what an idempotent API endpoint wants.

The important property is the race: if 32 requests try to start `order-42` at the same instant,
exactly one begins a run. The check and the write happen under that workflow's lock (think of a
mutex per key, like a Redis `SET NX` per order id). The test fires 32 goroutines (Go's
lightweight threads, like 32 concurrent `Promise`s) and checks there is exactly one new run.

## 5. Graceful cancellation (PR #46, ADR-0014)

0.2.0's cancel ends a workflow on the spot. That's wrong for a saga: if you reserved stock and
the customer cancels, you want to release it, and only the workflow code knows what it holds.

**What it does now.** `worker.cancel(id, reason, { graceful: true })` records a cancel request and
tells the workflow code at its next `await`: that await rejects with `CancelledFailure`. Steps the
code starts *after* that run normally, so it can compensate:

```ts
try {
  await ctx.sleep('ship-window', 48 * 3_600_000);
} catch (err) {
  if (err instanceof CancelledFailure) await ctx.activity('release', reserved); // compensate
  throw err; // ends the workflow as CANCELLED
}
```

**The hard part: replaying it the same way.** The cancel arrives at a random moment. The first
time, the code is told at whichever `await` it had reached, given everything in the history at
that moment. A replay next week sees *more* history (for example, the cancelled step's
cancellation event), so it could reach a different `await` first and take a different path.

The fix: when the SDK first tells the code, it records a small `cancel` marker that says "I told
it after having seen N events". On every later replay the SDK hides results past event N until
the code reaches that marker, which recreates exactly what the first run saw. It's the same trick
as replaying a video game from a recorded input log: you have to feed the inputs at the same
frames, not all at once.

Go's `ctx.Sleep` returns nothing, so Go code checks `ctx.CancelRequested()` after it. The demo's
`OrderSaga` now does that, so the dashboard's new **Request cancel** button shows compensation
happening.

## How we knew it worked

Beyond the normal tests, each guarantee got a **mutation test**: deliberately break the code
(remove the lock, skip the stamp, ignore the marker) and check that a test fails. If a test still
passes with the code broken, the test wasn't really checking the guarantee. About 60 mutations
across the five PRs; a handful survived at first, which led to stronger tests (one also showed the
retention guard was duplicated, so the check that really mattered had no test).

`-race` (Go's data-race detector) also caught a real bug before it shipped: describing a workflow
read its cached state after releasing its lock, while another goroutine could be appending to it.

## What shipped

- SDKs 0.3.0 on npm and PyPI (`npm install workflowd`, `pip install workflowd`).
- Both engines (the public demo and liftoff's private one) on the new binary. liftoff's engine
  held 23 finished workflows and nothing in flight, so there was nothing the new binary had to
  continue.
- Two older test flakes (timers of 100–150 ms racing the test's own calls under heavy load) were
  widened along the way.

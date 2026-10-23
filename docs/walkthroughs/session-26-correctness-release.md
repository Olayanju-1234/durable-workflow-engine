# Session 26 walkthrough: the correctness release (0.2.0)

liftoff started running its company webhooks and custom steps on workflowd on 22 October.
Using it for real showed five gaps: nothing could time out, nothing could be cancelled, every
failure was retried the same way, a finished workflow showed a race's losing timer as
`SCHEDULED` forever, and the TypeScript SDK was awkward from NestJS (ESM-only, no
deterministic clock). This session fixed all of them in four pull requests (#35–#38), then
released SDK 0.2.0 and redeployed both engines. Here is what each piece does and why, with
Node/TypeScript comparisons.

## A reminder of the model

A workflow is a function the engine re-runs from the top every time something happens. Each
durable call (`ctx.activity`, `ctx.sleep`, ...) is an event in an append-only history file.
On a re-run, calls that already have a result return it at once; the first call without one
is "what happens next". Think of the history as a Redux action log and the workflow's state
as the reducer run over it.

The rule that shaped every design below: **anything the engine must remember across a crash
has to be in the history**. Timers in memory die with the process; a deadline written into
an event can be re-armed by the next process.

## 1. Timeouts (PR #35, ADR-0008)

Three optional limits, like the three `timeout`s you'd put around an axios call, a Bull job
and a whole saga:

| Limit | Measures | When it passes |
|---|---|---|
| execution timeout | the whole workflow, from its start | the workflow ends as `TIMED_OUT` |
| start-to-close | one activity attempt, from when a worker picks it up | that attempt fails; the retry policy decides |
| schedule-to-close | the whole step, every attempt and backoff | the step fails for good |

```ts
await worker.startWorkflow('Order', order, `order-${order.id}`, { executionTimeoutMs: 86_400_000 });
await ctx.activity('render', doc, { startToCloseTimeoutMs: 30_000, scheduleToCloseTimeoutMs: 300_000 });
```

**How they survive a restart.** Each limit is stored as a duration next to the event it is
measured from: `WorkflowStarted.execution_timeout_ms`, and the two activity limits on
`StepScheduled`. Every event has a timestamp, so the deadline is "that timestamp plus the
duration": a pure function of history. On restart, `Recover()` re-arms it; a deadline that
passed while the engine was down fires at once.

**Why start-to-close rides on the lease.** A lease answers "is the worker alive?" and a
heartbeat extends it (like a Redis lock you keep refreshing with `PEXPIRE`). Start-to-close
answers "has this taken too long?", and a heartbeat must *not* extend it, or a stuck worker
that still heartbeats would run forever. Both are about one worker's claim on one attempt,
so the lease timer simply fires at whichever comes first. In Go terms the lease struct got a
second time field and a small method:

```go
func (le *lease) due() time.Time { // like Math.min(deadline, stopBy) for dates
	if !le.stopBy.IsZero() && le.stopBy.Before(le.deadline) {
		return le.stopBy
	}
	return le.deadline
}
```

**Why "TIMED_OUT" is a flag, not a new event.** The engine records the timeout as the
existing `WorkflowFailed` event with `timed_out: true`. An older engine binary, if we ever
roll back, still reads that history (as `FAILED`) instead of refusing it. It is the same trick
already used for `terminated`.

## 2. Cancellation (PR #36, ADR-0009)

**The liftoff bug.** The custom step races a callback against a deadline:

```ts
const callback = ctx.waitForSignal('await-callback', 'callback');
const deadline = ctx.sleep('deadline', ms);
if ((await ctx.waitAny([callback, deadline])) === 0) return 'called back';
```

When the callback won, the workflow completed, but the dashboard showed the deadline timer as
`SCHEDULED` forever. Nothing had told the engine the timer no longer mattered. There was a
nastier version: if the *deadline* won and the code later waited for another `callback`
signal, the abandoned first wait would swallow it, because signals go to waits in the order
the waits were created, like a FIFO queue of resolvers that still held a dead one.

**The fix, three parts.**

1. Workflow code can cancel a step: `ctx.cancel(promise)`, or `waitAny([...], { cancelRest: true })`.
   It sends a `cancel_step` command and the engine records it. A cancelled timer stops
   mattering, a cancelled signal wait stops taking signals, a cancelled activity is dropped
   from the queue.
2. A finished workflow's still-open steps are *shown* as `CANCELLED` ("not needed"). That is
   presentation only; history doesn't change.
3. A whole workflow can be cancelled: `worker.cancel(id, reason)`, `POST
   /api/workflows/{id}/cancel`, `client.CancelWorkflow`, or the dashboard's new Cancel button.
   It ends as `CANCELLED`, the normal "no longer wanted" outcome; terminate stays the emergency stop.

**Why cancellation is explicit.** `Promise.race` intuition says "the others don't matter",
but a reminder loop re-races the same step: "remind me every day until approval arrives"
races the approval against a new timer each day. Auto-cancelling the approval on the first
reminder would break it. So `waitAny` only cancels the losers when you ask.

**Telling the worker.** When a step is cancelled, its worker is still running the activity.
The engine swaps the lease for a "tombstone" that remembers why, so the worker's next
heartbeat or completion is refused with "cancelled" rather than "lease lost" (HTTP 409 with
`"cancelled": true`). In TypeScript the activity's `task.signal` aborts with a
`CancelledFailure`, exactly like an `AbortController` you'd pass to `fetch`:

```ts
worker.activity('provision', async (input, task) => {
  await fetch(url, { signal: task.signal }); // stops if the workflow is cancelled
});
```

## 3. Non-retryable failures (PR #37, ADR-0010)

liftoff retries a webhook six times, which is right for a timeout or a 503 and pointless for
a 404. Unable to say "don't retry this", liftoff *returned* `{ delivered: false }` instead of
throwing, so the workflow's own status said `COMPLETED` for a failed delivery.

Now the activity says so:

```ts
if (res.status >= 400 && res.status < 500) throw new NonRetryableError(`webhook answered ${res.status}`);
```

or the workflow lists error types not worth retrying:

```ts
retry: { maxAttempts: 6, initialBackoffMs: 5000, nonRetryableErrorTypes: ['BadRequest'] }
```

An error's "type" is its class name (`err.name` in JS, the exception class in Python and
Java), so `class BadRequest extends Error` just works. The engine gives its own failures
types too (`LeaseExpired`, `StartToCloseTimeout`, `ScheduleToCloseTimeout`), so you can say
"never retry a timeout". The workflow sees the type on the error: `err.type`,
`err.nonRetryable`.

**One place decides.** All retry logic is one Go function, `retryAfter`: attempts left? not
flagged? type not listed? backoff ends before the schedule-to-close deadline? It reads
everything from history, so after a crash the engine makes the same decision (a test writes
a history by hand, "crashes", and checks recovery doesn't retry).

**Not breaking older engines.** New SDKs send `error_type` on most failures. An older engine
rejects unknown JSON fields (`DisallowUnknownFields`, like `zod.strict()`), which would lose
the failure and make the attempt look like a dead worker. So the SDKs retry the completion
without the new fields when the engine answers `400 unknown field`. The e2e test pretends to
be an old engine for one step to prove it.

## 4. TypeScript ergonomics (PR #38)

**CommonJS next to ESM.** NestJS compiles to CommonJS, so liftoff had to `require()` an
ES module (Node 22.12+ only). The SDK's code now lives in `workflowd.cjs`, and
`workflowd.mjs` is three lines that re-export it. `package.json` points `require` at one and
`import` at the other. Why not ship two copies? The "dual package hazard": with two copies,
an `ActivityFailure` thrown by the CJS copy is not `instanceof` the ESM copy's class. One
implementation, two doors, no hazard. CI checks both doors expose the same objects.

**A deterministic clock.** `Date.now()` in workflow code is a bug: every replay reads a
different time. liftoff worked around it with `ctx.sideEffect('now', () => Date.now())`.
Now there is `ctx.now()`: the time of the latest result the code has received. The engine
sends each step's `resolved_at`, and the clock moves when the code *consumes* a result
(`await`), not when it starts a step, so every replay that reaches the same line reads the
same time. The trick that makes "when it is awaited" observable in JS: step results are a
small `Promise` subclass whose `then` moves the clock first. `await` calls `then` on
anything that isn't a plain `Promise`.

Also: `ctx.random()` (a seeded generator, the same sequence on every replay),
`ctx.sleepUntil(name, date)`, `ctx.waitForSignal(name, signal, { timeoutMs })` (resolves to
`undefined` on timeout and cancels the wait), and activities now get `task.attempt` and
`task.activityId` (the same on every attempt: an idempotency key for the side effect).

**A Go bug found on the way.** The Go SDK's `ctx.Now()` moved to a marker's timestamp
(`GetVersion`, `SideEffect`) on replay but not on the first run, so code reading the clock
right after `GetVersion` saw two different times. Markers are recorded by the code itself,
so they no longer move the clock.

## How we know it works: teeth

Every guarantee has a test, and every test was checked by breaking the code on purpose (a
"mutation") and watching it fail: 9 mutations for timeouts, 10 for cancellation, 11 for
non-retryable failures, 5 for the TypeScript features, all caught. One survived at first:
removing the "already cancelled in this replay" mark only cost an extra round trip, which no
engine-level test could see, so a replay-level test now pins "the cancel and the next
command go out together".

## Rolling back

Older engines read most of the new history (timed-out and cancelled workflows as `FAILED`,
cancelled steps as failed steps). The exception: a history where a step *between retries*
was closed by a deadline or a cancel. An older binary refuses that one, so check before
rolling back past this release (ADR-0008, ADR-0009).

## What changes for liftoff

liftoff can upgrade to `workflowd@0.2.0` (the private engine already runs the new build):

- `require('workflowd')` works without Node 22's require(esm).
- `ctx.now()` replaces `sideEffect('now', () => Date.now())`.
- The custom step's race becomes `waitForSignal('await-callback', 'callback', { timeoutMs })`,
  or `waitAny([...], { cancelRest: true })`; either way the losing step is closed.
- A 4xx from a webhook can `throw new NonRetryableError(...)` instead of returning a settled
  "not delivered" result, so the workflow's status tells the truth.
- `startToCloseTimeoutMs` bounds one delivery attempt; `scheduleToCloseTimeoutMs` bounds all six.
- Cancelling a custom-step request can `worker.cancel(workflowId)`, and an in-flight delivery
  learns it through `task.signal`.

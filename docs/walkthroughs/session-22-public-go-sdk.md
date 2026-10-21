# Session 22: the public Go SDK (Phase 5)

*Plain-English walkthrough for a Node/TS developer who is new to Go. Phases 1–4
built an engine that can't lose a workflow. This phase made it something another
Go developer can actually `go get` and use: write a workflow in their own service,
test it without a server, and change it safely while it runs.*

## What you can do now

```bash
go run ./cmd/workflowd --serve     # terminal 1: the engine
go run ./examples/hello Ada        # terminal 2: a worker + client
```

```
started workflow Greeting-41832ea0c50f4069
result: Hello, Ada! 👋
```

From a fresh clone with an empty Go cache that took 42 seconds, most of it the
first compile. Before this session, the only way to run a Go workflow was to edit
`cmd/workflowd/main.go` and rebuild the engine. Now the workflow lives in *your*
program, the way the TypeScript and Python SDKs already worked.

## Why Go users were stuck: the `internal/` rule

Go has a rule with no Node equivalent: a package under a directory called
`internal/` can only be imported by code inside the parent of that directory.
Think of it as a `package.json` `"exports"` field enforced by the compiler.
The engine lives in `internal/server`, so code in someone else's module can't
import it.

The trick is that the rule is about *who writes the import*, not what ends up in
the final program. A new package `client/` sits inside this module, so it may
import `internal/server`. Someone else's code imports `client/`, and gets the
engine's gRPC stubs through it. It's like a library's `index.ts` re-exporting a
few things from `src/internal/`: users only see the public file.

That gave five public packages (ADR-0005 records why):

| Package | Node analogy | What it's for |
|---|---|---|
| `workflow` | the types you `import type` | `Context`, `Func`: what workflow code is written against |
| `worker` | a BullMQ `Worker` | runs your workflows and activities in your process |
| `client` | an SDK client like `new Stripe(key)` | start, signal, terminate, describe, wait |
| `testsuite` | Jest fake timers | run a workflow in a unit test, in virtual time |
| `embedded` | an in-memory MongoDB for tests, but durable | a real engine inside your program |

## How a worker works

The worker is a long-running loop, like a queue consumer:

1. **Register**: "I can run `Checkout` workflows."
2. **Poll for a workflow task**: the engine sends the whole history of one
   workflow and asks "what next?".
3. **Replay** the Go function against that history (the same replay engine the
   engine itself uses) and send back exactly one command, like "schedule
   `charge-card`".
4. **Poll for activity tasks** by name, run the function (call Stripe, send the
   email), heartbeat while it runs, report the result.

The worker keeps no state. If it dies, the engine just hands the task to
another one. That's why you can run three copies, or restart them during a deploy.

## The bug you can't see in Node: stopping a function from outside

Replay re-runs your function from the top and has to *stop* it at the first step
that hasn't happened yet. In Phase 2 that was done with `panic` (Go's `throw`)
and `recover` (Go's `catch`).

The problem: workflow code can call `recover()` too. A workflow that wraps a step
in `defer func() { recover() }()` (the Go version of `try { } catch {}`) would
swallow the "stop here" signal and keep running with a half-replayed history.
It's the same trap the TypeScript SDK fell into in Session 13, where a `try/catch`
around `await ctx.activity()` caught the "suspend" exception.

The fix uses something Node doesn't have. Each replay now runs your function on
its own goroutine (a lightweight thread), and stopping it calls
`runtime.Goexit()`. That ends the goroutine immediately. `recover()` can't catch
it, because it isn't a panic. We proved it by switching back to panic and
watching `TestSuspensionCannotBeRecovered` fail.

## Deterministic helpers

Workflow code is re-run many times, so it must make the same calls in the same
order every time. Three helpers cover the common needs:

- **`ctx.Now()`** instead of `Date.now()`: the timestamp of the last recorded
  event the code has replayed past. It's the same on every replay, and it's in
  UTC because a test caught that `time.Unix` returns *local* time, which would
  differ between two workers in different time zones.
- **`ctx.SideEffect(name, fn)`** for things like a random id: run once, record
  the result in history, return the recording forever after.
- **`ctx.Logger()`**: a logger that stays quiet while replaying, so a log line
  appears once instead of once per replay.

## Changing code while it runs: `GetVersion`

A workflow can sleep for a week. If you deploy new code in that week that adds a
step, the old execution's history no longer matches the code. That's like
changing a database migration that already ran in production.

```go
if ctx.GetVersion("fraud-check", workflow.DefaultVersion, 1) == 1 {
	ctx.ExecuteActivity("fraud-check", order)
}
```

The first time an execution reaches this line it writes a **marker** into its
history: "this one took version 1". On every replay it reads that marker back.
An execution that passed this point *before* the deploy has no marker there, so
it gets `DefaultVersion` and replays the old path. The clever bit: when there's
no marker, `GetVersion` doesn't use up a step number, so the rest of the old
history still lines up.

The marker is a new event type in the history file. Old binaries don't know
it, so rolling back to one after markers exist isn't safe. That's written down
in the PR and the docs.

## Testing without a server

```go
env := testsuite.NewEnv()
env.RegisterActivity("bill", mockBill)
env.SignalAfter(time.Hour, "renew", []byte("yes"))
out, err := env.ExecuteWorkflow(subscription, input) // a 7-day Sleep returns instantly
```

It's Jest fake timers for workflows. The test environment writes history the
same way the engine does and replays your function for every decision, so a
workflow that branches on something it shouldn't (a global counter, `time.Now()`)
fails the test with a non-determinism error, just as it would in production.

## Following the leader in a cluster

Phase 4 made followers answer "not me, ask n3 at 10.0.0.3:7233". The client now
reads that hint (a gRPC *trailer*, like a response header that arrives after the
body) and retries on the leader. With no hint, it tries the next address it
knows, with a short backoff.

The subtle part is the worker. A new leader starts a fresh engine that has never
heard of your workflow types. So every time a call gets turned away, the worker
registers again. The test for this starts a real 3-node Raft cluster, gives the
client *only one follower's address*, kills the leader mid-activity, and checks
the workflow still finishes with every step recorded exactly once.

## Two engine bugs the SDK found

Writing a real client made the engine behave the way users would see it, and two
long-standing bugs fell out. Both affected the TypeScript and Python SDKs too.

1. **Stranded after a restart.** After a restart, the engine recovered workflows
   *before* any SDK worker had reconnected, and it skipped anything whose type
   wasn't registered yet. A workflow waiting on an activity or a timer was never
   picked up again. Now the engine records what happened (activity results,
   fired timers, delivered signals) whether or not the type is registered, and
   only the "what next?" decision waits for a worker.
2. **A signal at the wrong moment.** If a signal arrived while a worker was
   still deciding, the decision was correctly thrown away as out of date, but
   nothing asked for a new one, so the workflow sat there forever. This only
   failed on CI, sometimes. With the fix removed, the end-to-end test hung in 9
   of 20 runs; with it, none. The lesson from Session 21 held: get the failure
   to happen on your machine, don't guess.

## The examples

`examples/hello`, `examples/checkout` (a Saga with a manager-approval signal and
a declined card) and `examples/onboarding` (an email drip over days, with
`GetVersion`). Each has tests that run it against an embedded engine and in the
test environment, so CI proves the examples in the README actually work. The
onboarding one was also used by hand: stop the engine during the "1 day" sleep,
start it again, and the next email still goes out on time and nothing is sent twice.

## What's still open

Retry policies can only be set on the engine, `GetVersion` is Go-only, a signal
retried during a leader change can be recorded twice, steps run one at a time,
and there's no TLS. All listed in the README's Limitations section, which is the
honest place for them.

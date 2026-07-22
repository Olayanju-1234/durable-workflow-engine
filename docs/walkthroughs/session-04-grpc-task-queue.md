# Walkthrough — Session 4: The gRPC Task Queue

> For someone who knows backend engineering (Node/TypeScript/NestJS/Mongo) but is new to Go.
> Everything here is explained in plain terms, with analogies to the stack you already know.

---

## 1. Where we are

So far we've built two of the four big pieces:

- **The history store** (`internal/history`) — the notebook. An append-only log on disk that
  records everything that happens to a workflow, fsync'd so it survives a crash.
- **The state machine** (`internal/state`) — the brain. Two pure functions: `Reconstruct`
  (where are we?) and `Decide` (what next?). They read the notebook but touch nothing else.

The brain was pure and self-contained, which made it easy to test — but it couldn't *do*
anything. In last session's `--demo`, a fake worker lived inside the loop and "executed" each
step instantly by echoing its name. There was no real outside world.

**This session we connected the brain to the outside world.** Real programs — workers — can now
connect over the network, ask for work, run it, and report back. The thing in the middle that
hands out work and records results is the **server** (`internal/server`).

Here's the mental model. Temporal (the system we're cloning) works exactly like this:

```
  ┌──────────┐  "start OrderWorkflow"   ┌──────────────┐
  │  client  │ ───────────────────────▶ │              │
  └──────────┘                          │    SERVER    │  ◀── owns the history (notebook)
                                        │  (the brain  │  ◀── owns the task queue (in-tray)
  ┌──────────┐  "got any work?"         │   + the      │
  │  worker  │ ◀──────────────────────▶ │   plumbing)  │
  │ (runs    │  "here: run step 'ship'" │              │
  │  steps)  │  "done, output=X"        │              │
  └──────────┘ ───────────────────────▶ └──────────────┘
```

The worker is **dumb and stateless** — it knows how to run a step and nothing else. The server
holds *all* the durable state. If a worker dies, you start another one; nothing is lost because
the worker never held anything worth losing. That separation is the whole design.

---

## 2. What is gRPC? (and why not just REST?)

You've built REST APIs: a client sends `POST /workflows` with JSON, the server replies with
JSON. **gRPC is the same idea — a client calls a function on a server over the network — but
with two differences that matter here:**

1. **You define the API in a schema file first** (`.proto`), and a code generator writes both
   the client and server "stubs" for you. You don't hand-write request parsing. In Node terms,
   imagine if writing one `.proto` gave you a fully-typed Axios client *and* a typed Express
   router, in sync, for free.
2. **Messages travel as protobuf** (compact binary), not JSON. Faster, smaller, strongly typed.
   We already use protobuf for our history events, so this is the same tool we know.

Here's the service we defined, in [proto/service.proto](../../proto/service.proto):

```proto
service WorkflowService {
  rpc StartWorkflow(StartWorkflowRequest) returns (StartWorkflowResponse);
  rpc PollTask(PollTaskRequest) returns (PollTaskResponse);
  rpc CompleteTask(CompleteTaskRequest) returns (CompleteTaskResponse);
}
```

Read `rpc StartWorkflow(...) returns (...)` as: "there's a remote function called
`StartWorkflow` that takes this request and gives back that response." Three functions, three
verbs in our story:

- **StartWorkflow** — a client kicks off a workflow.
- **PollTask** — a worker asks "got any work for me?"
- **CompleteTask** — a worker says "here's the result of the work you gave me."

Running `make proto` feeds this file to the code generator and produces two Go files in
`internal/server/`: `service.pb.go` (the message types) and `service_grpc.pb.go` (the client +
the server interface). **We never edit those — they're generated, like `node_modules` or Prisma
client output.** Our job is to *implement* the server interface they define.

> **Toolchain note:** gRPC code generation needs an extra plugin, `protoc-gen-go-grpc`, on top
> of the `protoc-gen-go` we already had. `make tools` installs both. We also added the gRPC
> runtime library (`google.golang.org/grpc`) as our first real third-party dependency — think
> `npm install @grpc/grpc-js`, recorded in `go.mod`/`go.sum` (≈ `package.json`/lockfile).

---

## 3. The `oneof` trick for "success or failure"

When a worker finishes a step, it either succeeded (and has an output) or failed (and has an
error message). It's never both. In TypeScript you'd reach for a discriminated union:

```ts
type Result =
  | { kind: "ok";    output: Uint8Array }
  | { kind: "error"; error: string };
```

protobuf has exactly this; it's called a `oneof`:

```proto
message CompleteTaskRequest {
  string workflow_id = 1;
  string step_name = 2;
  int64 sequence = 3;
  oneof result {           // exactly one of these is set
    bytes output = 4;      // → success
    string error = 5;      // → failure
  }
}
```

On the Go side we check which one was set with a `switch` on the type — Go's version of
checking the `kind` discriminator. You'll see that switch in [grpc.go](../../internal/server/grpc.go).

---

## 4. The three new files (and how they split the work)

The golden rule we followed: **keep all the workflow logic in one place, and make the network
layer a dumb translator.** That gives us three files with very different jobs:

| File | Role | TS analogy |
|------|------|-----------|
| [engine.go](../../internal/server/engine.go) | **The brain + hands.** Owns the history store and the task queue; runs the decision loop. | Your service / business-logic layer |
| [queue.go](../../internal/server/queue.go) | **The in-tray.** Holds pending tasks until a worker grabs one. | A job queue (BullMQ), but in-memory |
| [grpc.go](../../internal/server/grpc.go) | **The translator.** Turns network requests into engine calls and back. | Your Express/Nest controller |

Let's take them one at a time.

---

## 5. `engine.go` — the brain meets the hands

Last session's `Decide` was pure: it returned a *description* of what to do ("schedule step
`ship`") but did nothing. The **Engine** is the impure part that actually does it. It's the
same loop as `--demo`, but now the worker lives across a network instead of inside the loop.

The Engine holds three things:

```go
type Engine struct {
    store *history.HistoryStore           // the notebook (durable)
    queue *taskQueue                      // the in-tray (in-memory)
    log   *slog.Logger

    mu   sync.Mutex                       // the "one writer at a time" lock
    defs map[string]state.WorkflowDefinition  // type name → plan
}
```

`defs` is a registry of known workflow types (≈ a `Map<string, Plan>`). You `Register` a
workflow type before serving, the same way you register routes before `app.listen()`.

**The heartbeat is `advanceLocked`** — this is `--demo`'s loop body, extracted:

```
read the full history  →  Decide  →  record the resulting event  →  (maybe) enqueue a task
```

- If `Decide` says **schedule a step**: write `StepScheduled` to disk, *then* drop a task in the
  in-tray. (Order matters — see §7.)
- If it says **complete/fail the workflow**: write the terminal event. Done.
- If it says **noop**: a step is already in flight; wait for the worker. Do nothing.

Two public methods drive it:

- **`StartWorkflow`** records `WorkflowStarted`, then calls `advanceLocked` → schedules step 1.
- **`CompleteTask`** records the worker's `StepCompleted`/`StepFailed`, then calls
  `advanceLocked` → schedules the next step, or finishes the workflow.

That's the entire engine. Start kicks it off; each Complete nudges it one step further; the
history on disk is the only memory. **Crash mid-way and restart, and replaying the log puts you
right back where you were** — that's the durability promise, and it costs us nothing extra
because we never kept state anywhere but the log.

### The `mu` lock and why event IDs stay correct

`sync.Mutex` is a lock: `mu.Lock()` / `defer mu.Unlock()` means "only one goroutine runs this
section at a time" (≈ a mutex/critical section; Node mostly dodges this with its single thread,
but Go truly runs things in parallel). We hold it around the whole read→decide→write cycle.

Why? Each event gets a sequential ID (1, 2, 3, …) computed as "current history length + 1". If
two completions raced, both could read length 5 and both write event 6 — a corrupt log. The
lock makes the cycle atomic, so IDs stay contiguous. Simple and correct; we optimize for
concurrency later.

### Rejecting duplicates — the load-bearing safety check

Before recording a completion, `CompleteTask` checks: *does this result actually match the step
that's currently in flight?* (same name, same sequence, and the workflow is actually waiting).

This matters because networks retry. A worker might send `CompleteTask` twice (its first reply
got lost). The first one records the result and moves on; the second now refers to a step that's
already done — so we **reject it with an error instead of writing a second, contradictory event.**
Without this check, a retry would poison the history. This is the completion-side half of "no
duplicate dispatch."

---

## 6. `queue.go` — a channel *is* a thread-safe queue

The in-tray is astonishingly small — about ten lines of real code — because Go hands us the
perfect tool: a **channel**.

A channel is a typed, thread-safe pipe. One goroutine puts a value in (`ch <- task`); another
takes one out (`task := <-ch`). The magic property for us:

> **Each value sent into a channel is received by exactly one receiver.**

If ten workers are all blocked on `<-ch` and one task arrives, **exactly one** worker gets it.
The other nine keep waiting. We don't write any locking, any "is this task taken?" flag, any
SQL `SELECT … FOR UPDATE SKIP LOCKED`. The language guarantees single delivery. **That guarantee
is literally our "no duplicate dispatch" requirement, for free.**

```go
type taskQueue struct {
    ch chan *Task
}

func (q *taskQueue) enqueue(t *Task) { q.ch <- t }     // put work in

func (q *taskQueue) poll(ctx context.Context) (*Task, error) {
    select {
    case t := <-q.ch:        // a task arrived → hand it over
        return t, nil
    case <-ctx.Done():       // the worker hung up / shutdown → stop waiting
        return nil, ctx.Err()
    }
}
```

### "Long-polling" and `select`

Notice `poll` *blocks* — if the in-tray is empty, the worker just waits, sometimes for minutes,
until a task shows up. That's called **long-polling**, and it's how the worker idles cheaply: no
busy-loop hammering the server "anything yet? anything yet?", just one quiet call that returns
the instant work exists. (Temporal works the same way.)

`select` is Go's "wait on several things at once, take whichever fires first." Here it waits on
*two* events: a task arriving, **or** the context being cancelled. `context.Context` is Go's
standard cancellation signal (≈ an `AbortSignal` threaded through every call). When the worker
disconnects or the server shuts down, the context fires, `poll` returns an error instead of
hanging forever, and the worker loop exits cleanly.

### Why the in-tray is allowed to be forgetful

The queue is purely in-memory — restart the server and it's empty. **That's intentional and
safe.** The durable truth is the history on disk: a step that was scheduled but not completed is
written there as a `StepScheduled` with no matching `StepCompleted`. Next session (Week 4),
startup recovery will scan the logs, find those dangling steps, and re-drop them into a fresh
in-tray. The queue is a convenience cache, never a source of truth — so it's fine for it to
forget.

---

## 7. One subtle ordering rule: write first, enqueue second

In `advanceLocked`, when we schedule a step we **append `StepScheduled` to disk first, and only
then drop the task in the in-tray.** Never the other way around.

Why? Imagine we enqueued first and crashed before writing to disk. A worker could grab the task
and start running a step the history has *no record of*. Now the durable log and reality
disagree — the exact thing a durable engine must never allow. By writing first, the rule becomes:
**a worker can only ever see work that's already permanent.** Worst case after a crash is a step
recorded but never dispatched — and recovery simply re-dispatches it. Safe direction to fail.

---

## 8. `grpc.go` — the dumb translator

This file is deliberately boring, and that's the point. Each method does three things: unpack
the request, call the Engine, pack the response. Example:

```go
func (s *GRPCServer) StartWorkflow(ctx context.Context, req *StartWorkflowRequest) (*StartWorkflowResponse, error) {
    id, err := s.engine.StartWorkflow(req.GetWorkflowType(), req.GetInput(), req.GetWorkflowId())
    if err != nil {
        return nil, status.Errorf(codes.InvalidArgument, "%v", err)
    }
    return &StartWorkflowResponse{WorkflowId: id}, nil
}
```

No business logic — it forwards to the Engine and translates the result. When the Engine returns
an error, we wrap it in a **gRPC status code** (`InvalidArgument`, `FailedPrecondition`, …),
which is gRPC's equivalent of an HTTP status (400, 412). The client sees a typed error, not a
stack trace.

(That `UnimplementedWorkflowServiceServer` we embed is a forward-compatibility seatbelt: if we
add a new RPC to the proto but haven't written it yet, the code still compiles — the embedded
type provides a default "not implemented" version. Embedding ≈ composition/mixin.)

---

## 9. Running it for real

The daemon learned a new trick: [`workflowd --serve`](../../cmd/workflowd/main.go).

```sh
go run ./cmd/workflowd --serve --addr :7233
```

This opens a real TCP port (`:7233`, Temporal's default), starts the gRPC server, and waits. On
Ctrl-C (or `SIGTERM` from Kubernetes), it does a **graceful shutdown**: `GracefulStop()` lets
in-flight requests finish instead of cutting them off mid-write — important when "mid-write"
means a half-recorded history event. We smoke-tested exactly this: start, confirm it's
listening, send `SIGTERM`, watch it drain cleanly.

---

## 10. How we proved it works

Two layers of tests, both run under Go's race detector (`-race`, which flags concurrency bugs):

**The headline test** ([engine_test.go](../../internal/server/engine_test.go)) is the Week-3
deliverable — *a real two-step workflow running to completion over actual gRPC.* It spins up the
full server, connects a real gRPC client, and runs a real worker loop (poll → run → complete).
We start a workflow and wait until the **history on disk** shows it COMPLETED — we assert against
the durable truth, not in-memory state, so we know the effect actually persisted. We also test
the unhappy paths, because those are where durability lives or dies:

- a worker that **fails** a step → the workflow ends FAILED with the right error;
- starting an **unknown** workflow type → rejected with `InvalidArgument`;
- a **duplicate** `CompleteTask` → rejected with `FailedPrecondition`, log stays clean.

> We connect the test client over an **in-memory listener** (`bufconn`) instead of a real TCP
> port — same gRPC code path, but no ports to clash and nothing to clean up. Fast and
> deterministic.

**The queue test** ([queue_test.go](../../internal/server/queue_test.go)) pins down the core
claim directly: enqueue 1000 tasks, unleash 16 workers polling at once, and assert **every task
came out exactly once** — no task dropped, none handed to two workers. That's "no duplicate
dispatch," proven, not just asserted.

Everything is green: `make ci` (format check, vet, build, all tests under `-race`).

---

## 11. The one-paragraph summary

We connected the pure brain to the real world. A client can now `StartWorkflow` over gRPC; the
**Engine** records it and schedules the first step into an in-memory **task queue** (a Go
channel, which guarantees each task goes to exactly one worker); a real **worker** long-polls
for that task, runs it, and reports back with `CompleteTask`; the Engine records the result and
schedules the next step — repeating until the workflow is done, with every event fsync'd to the
history log along the way. The network layer is a thin translator; all the logic and all the
durable state live in the Engine and its log. **Next session (Week 4): crash recovery — kill the
server mid-workflow, restart it, and watch it pick up exactly where it left off.**

# ADR-0005: The public Go SDK: workflow, client, worker, embedded

- **Status:** Accepted
- **Date:** 2026-10-21

## Context

A Go developer outside this repository cannot run a workflow today. Go workflows
are `sdk.WorkflowFunc`s, but the only thing that can execute one is
`server.Engine.RegisterWorkflow`, and `internal/server` is off limits to other
modules. So every Go workflow has to be compiled into `cmd/workflowd`, which is
how the `OrderSaga` demo works. TypeScript and Python developers are better off:
since Session 13 their SDKs run workflow code in their own process and talk to
the engine over the workflow-task protocol (`PollWorkflowTask` /
`CompleteWorkflowTask`).

Forces:

- **Parity.** A Go developer should get what the TS/Python SDKs already give:
  workflows and activities in their own process, deployed independently of the
  engine.
- **Cluster mode exists (ADR-0004).** Followers turn calls away with gRPC
  `Unavailable` plus `workflowd-leader*` trailers. A client must follow them,
  and a worker must notice that a new leader's engine has none of its in-memory
  workflow-type registrations.
- **Go's suspend mechanism is catchable.** The replay engine suspends a workflow
  function by panicking and recovering. Workflow code that writes
  `defer func() { recover() }()` swallows the suspension and runs on with a
  half-replayed state.
- **ADR-0003 says the engine is embeddable as a Go library.** Nothing public
  makes that true yet.
- No new module dependencies; the module path and brand stay as they are.

## Decision

### 1. Four public packages; generated code stays internal

| Package | For | Contents |
|---|---|---|
| `workflow` | writing workflow code | `Context` (`ExecuteActivity`, `Sleep`, `WaitForSignal`, `Input`, `Now`, `Info`, `Logger`), `Func` |
| `client` | starting and inspecting workflows | `Dial`, `StartWorkflow`, `SignalWorkflow`, `TerminateWorkflow`, `DescribeWorkflow`, `WaitForResult` |
| `worker` | running Go workflows and activities | `New`, `RegisterWorkflow`, `RegisterActivity`, `Run`, `ActivityInfo` |
| `embedded` | one-process apps and integration tests | `Start` an engine on a local data dir, get a `*client.Client` |

Go's `internal` rule only restricts *who imports* an internal package, not what
ends up in the binary. `client` and `worker` live inside this module, so they
may import `internal/server` and use the generated gRPC stubs, while user code
imports only the public packages. The public packages expose their own types
(`client.WorkflowDescription`, `client.Status`, ...), never generated ones. The
`.proto` files remain the wire contract for other languages.

### 2. Worker over gRPC is the primary path

`worker.Worker` registers its workflow types with `RegisterWorkflowType`, long-polls
`PollWorkflowTask`, replays the Go function against the raw history in the task
and returns one command with `CompleteWorkflowTask`. Activities are polled with
`PollTask` filtered by the registered activity names (`step_names`), heartbeated
at a third of the lease, and completed with `CompleteTask`. This is exactly the
protocol the TS/Python SDKs use, so the engine treats a Go worker like any other
SDK, and Go workflow code never has to be compiled into `workflowd`.

The worker re-registers its workflow types whenever its connection sees
`Unavailable` (engine restart, leader change, or the same node winning a new
term with a fresh engine). Registrations are in-memory per engine, and a type
nobody has registered is never dispatched.

### 3. Suspend with `runtime.Goexit` in a dedicated goroutine

Each decision runs the workflow function on its own goroutine. When a durable
call reaches the first step history has no result for, the context records the
decision and calls `runtime.Goexit()`. `recover()` cannot stop `Goexit`, so
workflow code cannot swallow a suspension, and any durable call made later (for
example from a deferred function while the goroutine unwinds) exits again
without changing the recorded decision. A real panic in workflow code is
recovered on that goroutine and returned as an error. The worker reports it as a
replay error, which fails the workflow, matching what the TS SDK does with an
uncaught exception. In-process planners (the `cmd/workflowd` demo) get an error
from `Decide` instead of a crashed daemon.

### 4. Deterministic helpers that need no new events

- `Now()` is the timestamp of the latest history event the code has observed:
  the `WorkflowStarted` event, then whichever event resolved each step it has
  replayed past. It advances only when a step resolves, and it is identical on
  every replay.
- `Logger()` drops records while the code is replaying steps it has already run,
  so a log line appears once per decision instead of once per replay.
- `GetVersion` and `SideEffect` need a history event of their own and come in
  Week 16.

### 5. Clients follow leader hints, with honest retry semantics

`client.Dial` takes the address of every node it may talk to. A call that comes
back `Unavailable` with a `workflowd-leader-grpc` trailer is retried on that
address; one without a hint (an election is running, or the node is down) is
retried on the next address after a short backoff, within a budget of about
five seconds. A follower rejects a call before doing anything, so retrying it is
safe. A leader that loses leadership while a write is in flight cannot say
whether the write committed, so:

- `StartWorkflow` generates the workflow id on the client when the caller gives
  none, and treats `AlreadyExists` on a retry as success.
- `SignalWorkflow` is at-least-once across a leader change: a retried signal can
  be recorded twice. This is documented, not hidden.

### 6. `embedded`: a real engine in the caller's process

`embedded.Start` opens a history store, runs `Recover()`, serves the gRPC API on
an in-memory listener and returns a client connected to it. Workers attach to
that client exactly as they would to a remote engine. The engine, history store,
HTTP API and Raft stay unexported: embedding reuses the client/worker surface
instead of freezing engine internals. Cluster mode is not embeddable.

### 7. `sdk` is deprecated, not removed

`sdk.Context` and `sdk.WorkflowFunc` become aliases of `workflow.Context` and
`workflow.Func`, and `sdk.Workflow.Decide` delegates to the new replay engine, so
existing code compiles unchanged. New code uses `workflow` + `worker`.

## Consequences

- A Go developer can `go get` the module, write workflows and activities in
  their own service, and run them against a local or remote `workflowd`, or
  entirely in-process with `embedded`.
- User binaries link `internal/server` (most of it is dead code the linker
  drops). That is the price of not maintaining a second, public copy of the
  generated stubs.
- Payloads stay `[]byte`, matching the wire and the other SDKs. Typed or JSON
  helpers can be layered on later without changing the protocol.
- A retried `SignalWorkflow` during a leader change may duplicate the signal.
  Fixing that needs a request id recorded in history (follow-up).
- Activity retry policies are still configured on the engine; an SDK-side
  retry option is a follow-up.

## Alternatives considered

- **Move the generated code to public `api/` packages.** Lets users call the raw
  stubs, but freezes Go types we still want to change and touches every file
  that names a history event. The wrapper packages give the same reach.
- **Use the HTTP/JSON transport from Go, like TS/Python.** It ships a
  pre-reconstructed view, so Go would replay a different shape than the
  in-process engine does. gRPC carries the raw history the Go replay engine
  already understands.
- **Keep panic/recover for suspension.** Simpler, but a `recover()` in workflow
  code silently corrupts replay. `Goexit` costs one goroutine per decision.
- **Export `server.Engine` for embedding.** Gives the most control, but makes the
  engine's internals a compatibility promise in the same release that adds the
  SDK. `embedded` can grow options later without that promise.
- **Compile user workflows into `workflowd` (plugins or code generation).**
  Couples user deploys to engine deploys, the opposite of what an SDK is for.

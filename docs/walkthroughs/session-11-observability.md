# Session 11 Walkthrough — Observability: /metrics + Visibility API (Phase 3, Week 9)

*Plain-English tour of what was built and why, with Node/TS analogies. Written for
someone who knows Express/NestJS/Mongo but is new to Go and durable execution.*

## The problem this week solves

The engine has been getting steadily more reliable — it survives its own crashes,
it survives worker crashes — but it's been a **black box**. The only ways to know
what it was doing were reading log lines or opening `.history` files by hand.
Two questions every operator asks were unanswerable:

1. **"How is the system doing?"** — throughput, failures, queue backlog, how long
   steps take. That's *metrics*: numbers over time, graphed in a dashboard.
2. **"What is workflow X doing, and can I kill it?"** — that's *visibility*: a
   query API over individual workflows, plus the one write every ops team needs,
   the kill switch.

If this were a NestJS service you'd reach for `prom-client` for the first and
build a small admin REST module for the second. We built both — but the first one
we built *from scratch*, and that's the most interesting part of the session.

## Part 1: Prometheus metrics without the Prometheus library

### Thirty seconds on how Prometheus works

Prometheus is pull-based: your service exposes a plain-text HTTP page (usually
`/metrics`), and the Prometheus server **scrapes** it every 15 seconds and stores
the time series. The page is almost comically simple — it's just names and
numbers:

```
# HELP workflowd_workflows_started_total Workflow executions started.
# TYPE workflowd_workflows_started_total counter
workflowd_workflows_started_total 42
workflowd_workflows_finished_total{status="completed"} 40
workflowd_workflows_finished_total{status="failed"} 2
```

That `{status="..."}` part is a **label** — one metric family split into several
series, like a GROUP BY. Grafana then graphs `rate(...)` over these.

### Why we didn't just `go get` the official client

This project's whole identity is *single binary, zero dependencies* — the WAL is
hand-rolled, the timers are hand-rolled. The official `client_golang` library is
a big dependency tree for what is, at its core, "keep some numbers and print
them". So `internal/metrics` is our own ~250-line implementation of exactly the
four instrument types Prometheus defines, nothing more:

- **Counter** — a number that only goes up (`workflows_started_total`). In Go
  it's an `atomic.Uint64`: think of `atomic` as the CPU-level version of not
  needing a mutex — increments from many goroutines can't corrupt it. (In Node
  you never think about this because one thread runs your JS; in Go, dozens of
  goroutines touch these counters concurrently.)
- **Gauge** — a number that goes up *and down* (`workflows_active`,
  `task_queue_depth`). Like a live `SELECT COUNT(*) WHERE status='RUNNING'`,
  except maintained incrementally.
- **GaugeFunc** — a gauge computed *at scrape time* from a function. Queue depth
  is just `len(channel)` — Go channels know their own backlog — so instead of
  bookkeeping we hand the registry a closure it calls when Prometheus asks.
- **Histogram** — for durations. It doesn't store every observation (that would
  grow forever); it keeps **buckets**: "how many observations were ≤ 5ms, ≤ 10ms,
  ≤ 25ms, …" plus a running sum and count. Percentiles get computed later by the
  Prometheus server from those buckets. We use one for step duration, measured
  from task dispatch to the worker's result.

A `Registry` holds all of them and renders the text page; `registry.Handler()`
is a standard `http.Handler` (Go's version of an Express middleware — an
interface with one method), mounted at `/metrics`.

The only subtle code is **CounterVec** (the labelled counter). Each distinct
label value lazily creates a child counter, and rendering sorts and escapes them.
The rule that matters: label values must be *low-cardinality* — statuses, not
workflow ids. One series per workflow id would blow up Prometheus's memory, the
classic metrics foot-gun.

### Where the numbers come from

Each counter is incremented at the **exact line where the corresponding event is
durably appended to history** (inside the same mutex-protected section). That's a
deliberate invariant: the numbers on `/metrics` should agree with what replaying
the logs would tell you — one source of truth, one instrumentation point. Eleven
families in all: workflows started / finished-by-status / active, tasks
dispatched, task results, lease expiries, retries, signals, timer fires, queue
depth, and the step-duration histogram.

`workflowd --serve` now takes `--metrics-addr` (default `:9090`) and runs the
metrics page on its **own** HTTP listener — a Prometheus scrape should never
share a port (or a failure mode) with production gRPC traffic.

## Part 2: the visibility API

Three new RPCs on the same gRPC service. All read-only except the last:

- **`ListWorkflows`** — one summary row per workflow: id, type, status, started/
  last-updated timestamps, error, history length. Optional status filter. Like
  `GET /admin/workflows?status=RUNNING`.
- **`GetWorkflow`** — everything about one workflow: the summary plus every step
  as replay reconstructs it (name, kind, status, **attempts** — so you can see
  "charge failed twice, succeeded on the third try" — errors, outputs, which
  signal a waiting step is parked on). This is the endpoint a dashboard's detail
  page will call. Unknown id → gRPC `NotFound` (think 404).
- **`TerminateWorkflow`** — the kill switch, with a reason string.

Two design points worth internalizing:

**Listing is a full scan, on purpose.** `ListWorkflows` literally reads and
replays every history file in the data directory. O(n), honest, and fine at this
scale — the data dir *is* the index. It's logged as debt; a real deployment with
100k workflows gets a visibility index later. Also: a workflow whose log is
corrupt shows up in the list with an `UNSPECIFIED` status and the error text.
Hiding a broken workflow from the ops API would be the worst possible behavior.

**Termination is not a new concept — it's a failure with a flag.** On disk,
terminate appends the *existing* `WorkflowFailed` event with a new
`terminated: true` field and the reason in the error text. No new state-machine
states, no new replay logic (the state machine already allowed "workflow fails
while a step is in flight" — an asymmetry documented back in Session 5, waiting
for exactly this). The API layer presents `Failed + terminated` as status
`TERMINATED` so humans can tell "the code failed" from "an operator killed it".

The satisfying part is what termination *reuses*: when you kill a workflow whose
task a worker is actively running, the engine just drops the lease (Week 8's
machinery). When that worker finishes and calls `CompleteTask`, its token no
longer matches and it's rejected — the same zombie-fencing that handles dead
workers now guarantees a kill can't be overwritten by a late result. There's a
test that proves exactly that sequence: poll → terminate → complete → rejected.

## Go concepts you touched this week

- **`atomic.Uint64` / `atomic.Int64`** — lock-free counters safe across
  goroutines. The reason Node code never needs this (single-threaded event loop)
  is the reason Go code does.
- **`http.Handler`** — Go's one-method interface for "thing that serves HTTP".
  Our registry satisfies it, so mounting `/metrics` is one line. Structural
  typing, exactly like a TS interface.
- **`httptest`** — the standard library ships a fake HTTP server/recorder for
  tests (what you'd use `supertest` for in Express land).
- **`go vet` has opinions about names** — a method called `WriteTo(io.Writer)`
  must match the standard `io.WriterTo` signature or vet fails the build. Ours
  is called `WriteText` instead. Convention-as-law is very Go.
- **Sentinel errors** — `ErrNotFound` + `errors.Is(err, ErrNotFound)` is Go's
  version of `instanceof NotFoundException`: the engine returns wrapped sentinel
  errors, and the gRPC layer maps them to status codes (`NotFound`,
  `FailedPrecondition`) the way a NestJS exception filter maps exceptions to
  HTTP statuses.

## Where we are now

Weeks 1–9 done. The engine is durable, worker-crash-safe, retries with backoff,
runs real Go workflow code deterministically — and now you can *watch* it
(Prometheus) and *operate* it (list, inspect, kill). Next session is Week 10:
an embedded web dashboard (`go:embed` compiles the HTML/JS straight into the
binary — still one file to deploy) that consumes exactly the two things built
this week.

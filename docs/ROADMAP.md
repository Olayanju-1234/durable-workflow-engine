# Roadmap

17 weeks, 5 phases, distilled from the blueprint. Check items off as they land. The
**Status** line below is the single source of truth for "where are we."

> **Status:** Phase 3 COMPLETE (Weeks 1–10), plus an **unplanned polyglot milestone** (Session 13): workflows can now be written in TypeScript and Python as well as Go, via a `PollWorkflowTask`/`CompleteWorkflowTask` protocol built on the existing `Planner` seam. Week 10 delivered the `go:embed` dashboard + JSON HTTP API; Week 9 delivered observability. Session 14: **full UI revamp — "The Loom" design language** (dashboard + site + Pattern Book use cases) and a **backend hardening pass**: correct HTTP/gRPC error codes, transient-I/O retry for timer callbacks, optional `--auth-token` bearer auth, list pagination+search, enriched health endpoint. Session 15: **production hardening of the public demo**: activity-task routing by step name (`?activities=` / `step_names`), in-process `--demo-worker` (the demo had been frozen since 7 Oct), opt-in `--retention`, GET-only SPA fallback, clean SIGTERM drain; on the VPS, an nginx per-IP write limit and daily data backups. Session 16: finished workflows now release their cached history file handle (`HistoryStore.Release`, called when a terminal event is appended), closing a slow fd leak. Session 17: a failed write or fsync (ENOSPC/EIO) now drops the cached history handle, so the retry reopens the file and truncates the torn record instead of appending after it. Session 18: **Phase 4 / Week 11 — Raft leader election** — hand-rolled deterministic Raft core (`internal/raft`), durable term/vote, CheckQuorum + §6 vote suppression, gRPC transport, simulated-network + randomized safety tests, and a runnable 3-node localhost cluster (`make cluster`, Raft only). Next: **Week 12 — log replication**. Open backlog: no TLS, gRPC/metrics unauthenticated, Java SDK unverified, O(n) visibility full-scan, O(n²) global-lock ceiling, `CronSchedule` (deferred from Wk5).

Legend: ✅ done · 🚧 in progress · ⬜ not started

## Phase 0 — Onboarding ✅
- ✅ Go toolchain + protoc installed; module initialized; repo architected.

## Phase 1 — Single-node engine with persistence (Weeks 1–4)
**Week 1 — History store** ✅
- ✅ `.proto` event types (WorkflowStarted, StepScheduled, StepCompleted, StepFailed, WorkflowCompleted, WorkflowFailed)
- ✅ `HistoryStore.Append` / `ReadAll` with `[len][CRC32C][protobuf]` framing + fsync
- ✅ Restart-survival test (append 10 → reopen → verify) + corrupt-record + missing-workflow tests

**Week 2 — Workflow state machine** ✅
- ✅ States: RUNNING, WAITING, COMPLETED, FAILED, TIMED_OUT (+ NOT_STARTED; TIMED_OUT reserved for Phase 2 timers)
- ✅ Decision loop: `Reconstruct` (history → state) + `Decide` (state → schedule/complete/fail); structural validation of corrupt logs
- ✅ Pure-function test: hand-built history → assert decisions; plus drive-to-completion/failure loops and a `--demo` driver

**Week 3 — gRPC task queue** ✅
- ✅ Service: `StartWorkflow`, `PollTask`, `CompleteTask` (`internal/server`, gRPC; `workflowd --serve`)
- ✅ In-memory task queue (buffered channel → exactly-one delivery, no duplicate dispatch)
- ✅ End-to-end: 2-step workflow runs to completion via a test worker (bufconn) + failure/rejection paths

**Week 4 — Crash recovery & WAL** ✅
- ✅ Startup scan + replay of all history files; re-enqueue incomplete steps (`HistoryStore.ListWorkflows` + `Engine.Recover`, wired into `workflowd --serve`)
- ✅ fsync-on-commit semantics documented & tested — torn-tail truncation on open + parent-dir fsync (Session 5); startup-wide recovery scan exercises the full durability path (Session 6)
- ✅ **The Kill Test:** 5-step workflow, SIGKILL at step 3, restart, verify 4–5 run with no duplicates (`kill_test.go`, re-exec pattern, teeth-verified)

## Phase 2 — Timers, signals, replay (Weeks 5–7)
**Week 5 — Timers & signals** ✅
- ✅ `Sleep` (timer step) + `AwaitSignal` (`WaitForEvent`); typed `StepDef` model (Activity/Timer/Signal)
- ✅ Min-heap timer service (`internal/timer`); durable `fire_at`; timers re-armed on recovery (survive restart)
- ✅ `SignalWorkflow` gRPC endpoint + `SignalReceived` event; buffered delivery (k-th wait ↔ k-th signal)
- ⬜ `CronSchedule` — deferred (thin layer on the timer service; revisit after the replay engine)

**Week 6 — Deterministic replay engine** ✅
- ✅ Workflows as Go functions (`sdk`: `Context.ExecuteActivity/Sleep/WaitForSignal`, `WorkflowFunc`, `Workflow.Decide`)
- ✅ Replay via re-execution against history (yield = panic/recover); `Planner` seam so engine drives data- or code-based workflows identically
- ✅ Determinism enforced (step kind/name mismatch → error); step kind reconstructed from history (recovery/signals need no definition)
- ✅ Tests: code workflow runs to completion + survives restart; non-determinism detected; replay is pure

**Week 7 — Retries & Saga** ✅
- ✅ Configurable retry policies (`RetryPolicy`, per-activity `SetRetryPolicy`); exponential backoff on a second timer service; `retry_count` recorded; retries re-open the same sequence (multi-attempt step) and survive restart
- ✅ Saga/compensation (failed `ExecuteActivity` → workflow catches the error → runs compensating activities) — falls out of the replay engine, no new machinery
- ✅ Order-flow demo (`orderSaga` in `workflowd --serve`) + tests (retry-then-succeed, retry-exhausted→FAILED, compensation, retry-survives-restart)

## Phase 3 — Multi-worker, failure handling, observability (Weeks 8–10)
**Week 8 — Leases & heartbeats** ✅
- ✅ Lease tokens: every dispatched task is a time-boxed, token-proven claim (`PollTask` grants; `CompleteTask` must present; zombie results rejected)
- ✅ `Heartbeat` RPC: long-running activities extend their lease; a rejected heartbeat tells the worker to abandon the work
- ✅ Reassignment on lease expiry: recorded as `StepFailed("lease expired")` → Wk7 retry machinery (backoff + re-dispatch, or surface to workflow) — closes the worker-crash-stranding debt (teeth-verified)
- ✅ Worker identity: `worker_id` recorded on the lease and named in the expiry error/audit trail
- ⬜ Sticky queues / worker affinity — deferred: workers are stateless activity executors (all replay happens on the engine), so there is no worker-side cache to stick to; revisit with the Phase-5 SDK

**Week 9 — Observability** ✅
- ✅ slog structured logging (in place since Wk3; every workflow line carries `workflow_id`)
- ✅ Prometheus `/metrics` — zero-dep text-format registry (`internal/metrics`: Counter/CounterVec/Gauge/GaugeFunc/Histogram); engine instrumented (started/finished-by-status/active, dispatched, task results, lease expiries, retries, signals, timers, queue depth, step-duration histogram); `workflowd --serve --metrics-addr` (own HTTP listener)
- ✅ Visibility API — `ListWorkflows` (+status filter; broken logs surfaced, not hidden), `GetWorkflow` (per-step reconstruction), `TerminateWorkflow` (durable `WorkflowFailed{terminated:true}`, lease dropped so in-flight workers are fenced; presents as `TERMINATED`)
- ⬜ Visibility index — List is a full scan+replay of the data dir; fine now, revisit with the storage upgrade (ADR-0001)

**Week 10 — Embedded dashboard & public site** ✅
- ✅ JSON HTTP API (`internal/server/httpapi.go`) — hand-rolled on `net/http` + `encoding/json` (no grpc-gateway dep); list/get/start/signal/terminate/health; explicit DTOs so the wire contract is readable (`"RUNNING"`, RFC3339) and decoupled from proto naming
- ✅ Embedded dashboard (`web/` package, `go:embed dashboard`) — workflow list + status filters + auto-refresh, detail drawer with step timeline/attempts/errors, signal + terminate actions; vanilla HTML/CSS/JS, no build step; served by `workflowd --serve --http-addr` (default `:8233`), SPA fallback to index.html
- ✅ Public site (`web/site/`, not embedded) — landing page + comprehensive developer documentation (quickstart, concepts, determinism rules, writing workflows/workers, gRPC + HTTP + metrics reference, operations, limitations, FAQ)
- ✅ Tests: HTTP API lifecycle (start→inspect→terminate), error-code table, signal round-trip, embedded-assets-served + API-wins-over-catch-all; live smoke-tested end to end
- ⬜ Auth/TLS on the HTTP + gRPC ports — none today; documented as a deployment constraint (bind to localhost / front with a proxy)

## Polyglot workflows — unplanned, Session 13 ✅
Not in the original 17-week plan; prioritized over Raft because adoption matters more than HA
for a pre-alpha project, and the wire protocol is cheaper to settle *before* replicating it.
- ✅ Workflow-task protocol: `RegisterWorkflowType` / `PollWorkflowTask` / `CompleteWorkflowTask`, `WorkflowCommand` oneof; built on the existing `Planner` seam so history, recovery, timers, retries and leases were untouched
- ✅ Stale-decision fencing (history-length optimistic check) — makes re-dispatch of a dead SDK worker's task safe
- ✅ Command validation before anything reaches the log; a rejected command leaves the task pending so a fixed deployment self-heals (teeth-verified against a stranding bug)
- ✅ HTTP transport shipping the pre-reconstructed view → SDKs need no protobuf toolchain and no dependencies
- ✅ TypeScript + Python SDKs, verified end-to-end (activity → durable timer → signal → Saga → completion)
- ⚠️ Java SDK written but never compiled (no JDK available) — reference implementation
- ⬜ SDK polish: retries/backoff config from the SDK side, richer payload types (bytes/JSON), published packages (npm/PyPI/Maven)

## Phase 4 — Replication with Raft (Weeks 11–14)
**Week 11 — Leader election** ✅ (Session 18)
- ✅ Deterministic Raft core (`internal/raft/core.go`): terms, randomized election timeouts, RequestVote with the up-to-date log check, one durable vote per term, heartbeats, higher-term-wins
- ✅ CheckQuorum (leader steps down without a majority) + §6 vote suppression (followers in lease ignore vote requests)
- ✅ Durable hard state (term + vote) persisted before any message leaves the node; shared CRC framing extracted to `internal/record`
- ✅ `Node` driver + one-way `Send(Envelope)` gRPC transport; `workflowd --node-id --peers --raft-addr --raft-tick`; `make cluster` runs a 3-node localhost cluster (Raft only until Wk14)
- ✅ Tests: simulated in-memory network, 200 randomized chaos schedules checking election safety + liveness, teeth test for a forgotten vote, real 3-node gRPC failover
- ⬜ Wk12: log replication (AppendEntries), commit on majority, follower catch-up, conflicts
- ⬜ Wk13: five safety properties; snapshotting + log truncation
- ⬜ Wk14: wire Raft into the engine; chaos tests (kill leader, partition, 100 concurrent)

## Phase 5 — SDK polish & DX (Weeks 15–17)
- ⬜ Wk15: clean public SDK surface (`workflow.*`, `client.*`, `worker.New`)
- ⬜ Wk16: `workflow.Version` for safe deploys; testing framework (mock engine, time control)
- ⬜ Wk17: README, quickstart (<5 min), 3 example workflows, architecture docs, launch blog post

## Cross-cutting / backlog
- ⬜ Adopt `buf` for proto lint + breaking-change detection (ADR-0002)
- ⬜ Storage upgrade path to LSM/BadgerDB if needed (ADR-0001)
- ⬜ Brand name + final module path before public release
- ⬜ Managed-product (commercial) layer: dashboard, multi-tenancy, RBAC, audit

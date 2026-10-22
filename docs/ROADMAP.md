# Roadmap

17 weeks, 5 phases, distilled from the blueprint. Check items off as they land. The
**Status** line below is the single source of truth for "where are we."

> **Status:** **ALL 17 WEEKS COMPLETE.** Session 25: the TypeScript and Python SDKs are **published** (npm + PyPI `workflowd` 0.1.1, released from CI by trusted publishing, no tokens). Phase 5 COMPLETE (Weeks 15–17). Phase 4 COMPLETE (Weeks 1–14). Phase 3 COMPLETE (Weeks 1–10), plus an **unplanned polyglot milestone** (Session 13): workflows can now be written in TypeScript and Python as well as Go, via a `PollWorkflowTask`/`CompleteWorkflowTask` protocol built on the existing `Planner` seam. Week 10 delivered the `go:embed` dashboard + JSON HTTP API; Week 9 delivered observability. Session 14: **full UI revamp — "The Loom" design language** (dashboard + site + Pattern Book use cases) and a **backend hardening pass**: correct HTTP/gRPC error codes, transient-I/O retry for timer callbacks, optional `--auth-token` bearer auth, list pagination+search, enriched health endpoint. Session 15: **production hardening of the public demo**: activity-task routing by step name (`?activities=` / `step_names`), in-process `--demo-worker` (the demo had been frozen since 7 Oct), opt-in `--retention`, GET-only SPA fallback, clean SIGTERM drain; on the VPS, an nginx per-IP write limit and daily data backups. Session 16: finished workflows now release their cached history file handle (`HistoryStore.Release`, called when a terminal event is appended), closing a slow fd leak. Session 17: a failed write or fsync (ENOSPC/EIO) now drops the cached history handle, so the retry reopens the file and truncates the torn record instead of appending after it. Session 18: **Phase 4 / Week 11 — Raft leader election** — hand-rolled deterministic Raft core (`internal/raft`), durable term/vote, CheckQuorum + §6 vote suppression, gRPC transport, simulated-network + randomized safety tests, and a runnable 3-node localhost cluster (`make cluster`, Raft only). Session 19: **Week 12 — log replication** — AppendEntries with entries, majority commit (current-term rule + opening empty entry), conflict repair with term-skipping hints, durable CRC-framed Raft log, separate apply goroutine, term-fenced `Propose`. Session 20: **Week 13 — safety + snapshots** — all five Raft safety properties checked across 200 randomized schedules with teeth knobs proving each can fire, snapshots + log compaction + InstallSnapshot, and ADR-0004 (replicated history, leader-only engine). Session 21: **Week 14 — the engine on Raft** — every history write goes through the replicated log, a fresh leader-only engine per term (started after the term's opening entry is applied, then `Recover()`), followers answer with leader hints (gRPC + HTTP), and real-process chaos tests (kill leader mid-workflow, partitioned leader can't commit, 100 workflows across a leader change). **PHASE 4 COMPLETE.** Session 22: **Phase 5 — the public Go SDK** — `workflow` / `worker` / `client` / `embedded` (Wk15, ADR-0005; Go workflows run in the user's process over gRPC, follow leader hints, suspend with `runtime.Goexit`), `GetVersion` + `SideEffect` markers and a virtual-time `testsuite` (Wk16), README + 42-second quickstart + three tested examples + docs + launch blog draft (Wk17); two engine bugs fixed on the way (steps resumed without a registered planner; stale SDK decision after an early signal re-dispatched). **PHASE 5 COMPLETE.** Session 23 (security/CI/packaging, parallel to the engine-backlog session): **opt-in TLS + bearer auth on every listener** (gRPC interceptor, public-reads option, metrics token, Raft mutual TLS, secrets from files/env), **`buf` lint + breaking checks in CI**, **every SDK (Java included, first compile) run end to end in CI in plaintext and over TLS**, and **publish-ready npm/PyPI/Maven packages** with a manual release workflow (not published). Session 24 (engine backlog, parallel to Session 23): **SDK-chosen retry policies, version markers, binary/JSON payloads, heartbeats and leader-hint following in every SDK** (#19), **per-workflow locks + an in-memory state cache** (O(n²) ceiling gone: 46x per step at 1000 steps, #22), **a visibility index** (listing 10k workflows 734 ms → 0.75 ms) and **ADR-0006: no LSM engine** (#23), **durable cron schedules**, leader-only and idempotent per fire (#24), **parallel steps** with batched commands and history-ordered `WaitAny` in Go/TS/Python (ADR-0007, #25), **exactly-once signals** with replicated idempotency keys (#26); sticky queues closed with measurements (not needed). Open backlog: the Java SDK on Maven Central (owner: GPG key + Sonatype), per-user auth/RBAC, brand + module path (owner), continue-as-new for very long histories, group commit (fsync batching) if write rates need it, parallel steps in the Java SDK, cluster follow-ups (PreVote, follower reads, single-node → cluster import).

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
- ✅ `CronSchedule` (Session 24, PR #24): durable cron schedules over gRPC/HTTP/Go client, hand-written parser (`internal/cron`), leader-only, idempotent per fire, catch-up after downtime

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
- ✅ Sticky queues / worker affinity — **closed, not built** (Session 24): workers keep no state, every task carries the full history, and replaying a decision costs 74 µs at 100 steps and 0.7 ms at 1000 (`BenchmarkReplayDecision`), at or below the task's own round trip; the engine side is cached (#22). Rationale in ARCHITECTURE "Why no sticky queues"

**Week 9 — Observability** ✅
- ✅ slog structured logging (in place since Wk3; every workflow line carries `workflow_id`)
- ✅ Prometheus `/metrics` — zero-dep text-format registry (`internal/metrics`: Counter/CounterVec/Gauge/GaugeFunc/Histogram); engine instrumented (started/finished-by-status/active, dispatched, task results, lease expiries, retries, signals, timers, queue depth, step-duration histogram); `workflowd --serve --metrics-addr` (own HTTP listener)
- ✅ Visibility API — `ListWorkflows` (+status filter; broken logs surfaced, not hidden), `GetWorkflow` (per-step reconstruction), `TerminateWorkflow` (durable `WorkflowFailed{terminated:true}`, lease dropped so in-flight workers are fenced; presents as `TERMINATED`)
- ✅ Visibility index (Session 24, PR #23): in-memory, built by recovery, kept on append and purge; listing 10k workflows 734 ms → 0.75 ms

**Week 10 — Embedded dashboard & public site** ✅
- ✅ JSON HTTP API (`internal/server/httpapi.go`) — hand-rolled on `net/http` + `encoding/json` (no grpc-gateway dep); list/get/start/signal/terminate/health; explicit DTOs so the wire contract is readable (`"RUNNING"`, RFC3339) and decoupled from proto naming
- ✅ Embedded dashboard (`web/` package, `go:embed dashboard`) — workflow list + status filters + auto-refresh, detail drawer with step timeline/attempts/errors, signal + terminate actions; vanilla HTML/CSS/JS, no build step; served by `workflowd --serve --http-addr` (default `:8233`), SPA fallback to index.html
- ✅ Public site (`web/site/`, not embedded) — landing page + comprehensive developer documentation (quickstart, concepts, determinism rules, writing workflows/workers, gRPC + HTTP + metrics reference, operations, limitations, FAQ)
- ✅ Tests: HTTP API lifecycle (start→inspect→terminate), error-code table, signal round-trip, embedded-assets-served + API-wins-over-catch-all; live smoke-tested end to end
- ✅ Auth/TLS on every port (Session 23, PR #18): opt-in TLS on gRPC/HTTP/metrics, bearer token on the JSON API and gRPC, `--auth-public-reads`, a metrics token, Raft mutual TLS; secrets from files/env

## Polyglot workflows — unplanned, Session 13 ✅
Not in the original 17-week plan; prioritized over Raft because adoption matters more than HA
for a pre-alpha project, and the wire protocol is cheaper to settle *before* replicating it.
- ✅ Workflow-task protocol: `RegisterWorkflowType` / `PollWorkflowTask` / `CompleteWorkflowTask`, `WorkflowCommand` oneof; built on the existing `Planner` seam so history, recovery, timers, retries and leases were untouched
- ✅ Stale-decision fencing (history-length optimistic check) — makes re-dispatch of a dead SDK worker's task safe
- ✅ Command validation before anything reaches the log; a rejected command leaves the task pending so a fixed deployment self-heals (teeth-verified against a stranding bug)
- ✅ HTTP transport shipping the pre-reconstructed view → SDKs need no protobuf toolchain and no dependencies
- ✅ TypeScript + Python SDKs, verified end-to-end (activity → durable timer → signal → Saga → completion)
- ✅ Java SDK compiled (Java 11 target) and run end to end in CI, like TS and Python, in plaintext and over TLS + token (Session 23, PRs #16, #18)
- ✅ SDK polish (Session 24, PR #19): retry policies set from the SDK (recorded with the step), bytes/JSON payloads both ways, `getVersion`/`sideEffect` and leader-hint following in TS/Python/Java, HTTP heartbeats
- ✅ Parallel steps (Session 24, PR #25, ADR-0007): several activities/timers/signal waits at once, batched commands, history-ordered `WaitAny`; Go, TypeScript, Python
- ✅ Exactly-once signals across failover (Session 24, PR #26): idempotency key on `SignalWorkflow`, recorded and replicated with the history
- ✅ Publish-ready packages: npm `workflowd` (+ `.d.mts` types), PyPI `workflowd`, Maven `io.github.olayanju-1234:workflowd`, CHANGELOG, manual release workflow (Session 23, PR #20). Publishing itself is the owner's step (`sdks/README.md`)

## Phase 4 — Replication with Raft (Weeks 11–14) ✅
**Week 11 — Leader election** ✅ (Session 18)
- ✅ Deterministic Raft core (`internal/raft/core.go`): terms, randomized election timeouts, RequestVote with the up-to-date log check, one durable vote per term, heartbeats, higher-term-wins
- ✅ CheckQuorum (leader steps down without a majority) + §6 vote suppression (followers in lease ignore vote requests)
- ✅ Durable hard state (term + vote) persisted before any message leaves the node; shared CRC framing extracted to `internal/record`
- ✅ `Node` driver + one-way `Send(Envelope)` gRPC transport; `workflowd --node-id --peers --raft-addr --raft-tick`; `make cluster` runs a 3-node localhost cluster (Raft only until Wk14)
- ✅ Tests: simulated in-memory network, 200 randomized chaos schedules checking election safety + liveness, teeth test for a forgotten vote, real 3-node gRPC failover
**Week 12 — Log replication** ✅ (Session 19)
- ✅ AppendEntries with entries + the prevLogIndex/prevLogTerm consistency check; truncate only at a real conflict; follower commit capped at the last new entry
- ✅ Commit on a majority of durable copies, counting only current-term entries (Figure 8); every new leader opens its term with an empty entry
- ✅ Follower catch-up (batched, ≤256 entries / ~1 MB per message) and conflict repair with term-skipping hints (Figure 7 converges)
- ✅ Durable Raft log (`raft/log`, CRC-framed, one fsync per batch, torn tail cut, suffix truncation); apply on its own goroutine; term-fenced `Propose` with honest error semantics
- ✅ Tests: Figure 7/8 scenarios, State Machine Safety in 200 randomized schedules with proposals, real gRPC replication/failover/restart; each rule mutation-verified
**Week 13 — Safety properties + snapshots** ✅ (Session 20)
- ✅ Randomized checker of all five properties (Election Safety, Leader Append-Only, Log Matching, Leader Completeness, State Machine Safety) over 200 seeded failure schedules with proposals and snapshots
- ✅ Teeth: `testingKnobs` switch off one rule each; permanent tests prove every property fires on a broken core and stays silent on the real one
- ✅ Snapshots: local snapshot every N applied entries, durable snapshot file, atomic log compaction, InstallSnapshot for lagging followers (matching suffix kept, conflicting log replaced), restore on restart, crash windows covered
- ✅ ADR-0004 written before Week 14
**Week 14 — The engine on Raft** ✅ (Session 21)
- ✅ Every history write goes through the replicated log (`HistoryLog` seam; single-node unchanged); the history directory is the state machine (`raft_index` stamps, event-id fence, idempotent replay, snapshot = raw history files)
- ✅ Leader-only engine: a fresh engine per term after the opening entry is applied, then `Recover()`; term-fenced writes; engine stopped on leadership loss; demo worker + retention per term
- ✅ Followers reject client/worker calls with a leader hint (gRPC `Unavailable` + trailers, HTTP `503` + JSON/header); health on every node; Raft gauges on `/metrics`
- ✅ Chaos tests with real processes and TCP-proxy partitions: kill leader mid-workflow, partitioned leader cannot commit, 100 workflows across a leader change (no lost or duplicated completed steps, byte-identical histories)
- ✅ ADR-0004 implemented; production stays single-node

## Phase 5 — SDK polish & DX (Weeks 15–17) ✅
**Week 15 — Public Go SDK** ✅ (Session 22, PR #13)
- ✅ ADR-0005: `workflow`, `client`, `worker`, `embedded`; generated protobuf stays internal
- ✅ `worker.New`: Go workflows + activities in the user's process via `PollWorkflowTask` / `PollTask(step_names)`, heartbeats, re-registration on every reconnect
- ✅ `client.*`: start / signal / terminate / describe / wait over gRPC, following leader hints; idempotent start via client-generated ids
- ✅ Replay engine in `internal/replay`: `runtime.Goexit` suspension (can't be swallowed by `recover()`), deterministic `ctx.Now()`, replay-aware `ctx.Logger()`; `sdk` kept as deprecated aliases
- ✅ Engine fixes: step outcomes recorded without a registered planner; stale decision after an early signal re-dispatched
- ✅ Tests: e2e saga, heartbeats, restart re-registration, real 3-node failover with one seeded follower; each guarantee mutation-verified

**Week 16 — Versioning + testing toolkit** ✅ (Session 22, PR #14)
- ✅ `ctx.GetVersion` / `ctx.SideEffect` via `MarkerRecordedEvent` + `RecordMarkerCommand` (old executions replay `DefaultVersion` without consuming a sequence)
- ✅ `testsuite`: mocked activities, virtual time, scheduled signals, blocked detection, every decision replayed (catches non-determinism)
- ✅ Tests: versioned replay (teeth), versioned deploy e2e (v1 worker → v2 worker mid-flight), in-process marker chaining

**Week 17 — Docs + launch** ✅ (Session 22, PR #15)
- ✅ README rewrite; quickstart timed at 42 s from a fresh clone with empty Go caches
- ✅ Three examples (`hello`, `checkout`, `onboarding`), each run in CI against an embedded engine and in `testsuite`
- ✅ ARCHITECTURE.md, `web/site/docs.html` + landing page, `sdks/README.md`
- ✅ Launch blog post drafted at `docs/blog/2026-10-launch-draft.md` (draft only; not published)

## Cross-cutting / backlog
- ✅ Adopt `buf` for proto lint + breaking-change detection (ADR-0002; Session 23, PR #17)
- ✅ Storage upgrade path to LSM/BadgerDB — **decided: not needed** (Session 24, ADR-0006, with measurements); per-workflow locks + state cache (PR #22) removed the O(n²) ceiling
- ⬜ Brand name + final module path before public release
- ⬜ Managed-product (commercial) layer: dashboard, multi-tenancy, RBAC, audit

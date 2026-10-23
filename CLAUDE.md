# CLAUDE.md — Project Guide for AI Sessions

This file is auto-loaded by Claude Code at the start of every session in this repo.
It is the durable, version-controlled memory that travels with the project. Read it
first, then check `docs/SESSION_LOG.md` for the latest state.

## What this is

A **single-binary, zero-dependency, embeddable durable workflow execution engine** in
Go — a lightweight self-hostable alternative to Temporal. Workflows are sequences of
events in an append-only history; the engine replays that history to resume exactly
where it left off after a crash. Open-core: the engine is OSS (Apache-2.0); a managed
product (dashboard, multi-tenancy, RBAC, audit) is the future commercial layer.

Full design spec: the blueprint PDF (`durable_workflow_engine_blueprint.pdf`, re-attached
per session). Distilled into `docs/ROADMAP.md` (17 weeks, 5 phases).

## Audience (IMPORTANT — how to communicate)

The developer is an experienced **Node/TypeScript/NestJS/Mongo** backend engineer who is
**new to Go and to durable-execution/Temporal concepts**. They cannot yet read the Go code
unaided. So: teach, don't just ship. Explain Go via analogies to the stack they know
(`go.mod` ≈ `package.json`, struct+methods ≈ class, `err != nil` ≈ try/catch, `defer` ≈
finally, interfaces ≈ TS structural typing, `go test` ≈ Jest, `go run` ≈ ts-node). Explain
domain concepts (event sourcing, replay, fsync) in plain terms before the code. End every
session with a beginner walkthrough (see Session protocol step 4).

## Architecture (see docs/ARCHITECTURE.md for detail)

The whole engine is one loop: **read history → replay to find current position →
schedule next step → record result.** Four components, built across phases:

| Component        | Package            | Phase | Responsibility |
|------------------|--------------------|-------|----------------|
| History Store    | `internal/history` | 1 ✅  | Append-only, CRC32-framed, fsync'd event log. Source of truth. |
| State Machine    | `internal/state`   | 1 ✅  | Replay history → emit decisions (schedule/complete/fail). |
| gRPC Task Queue  | `internal/server`  | 1 ✅  | StartWorkflow / PollTask / CompleteTask; worker bridge. |
| Replay Engine    | `sdk` + engine     | 2 ✅  | Deterministic replay of workflow code (workflows as Go funcs). |
| Timers/Signals   | `internal/timer`   | 2 ✅  | Min-heap timers, external signals (cron deferred). |
| Leases/Heartbeats| `internal/server`  | 3 ✅  | Lease tokens on dispatch, Heartbeat RPC, dead-worker reassignment. |
| Observability    | `internal/metrics` | 3 ✅  | Zero-dep Prometheus `/metrics`; visibility API (List/Get/Terminate). |
| Dashboard + site | `web/`             | 3 ✅  | `go:embed` operational UI + JSON HTTP API; public landing page & docs. |
| Polyglot SDKs    | `sdks/`            | —  ✅  | Workflow-task protocol + TypeScript/Python/Java SDKs (dependency-free). |
| Consensus (Raft) | `internal/raft`    | 4 ✅  | Elections, log replication, snapshots, five-property safety checker. |
| Cluster          | `internal/cluster` | 4 ✅  | Replicated history state machine, leader-only per-term engine, leader hints (ADR-0004). |
| Public Go SDK    | `workflow`, `worker`, `client`, `testsuite`, `embedded` | 5 ✅ | What Go developers import (ADR-0005). Replay engine in `internal/replay`; `sdk` is deprecated aliases. |
| Locking + cache + index | `internal/server` (`wfstate.go`, `index.go`) | backlog ✅ | Per-workflow locks, in-memory history + `state.Folder` state, visibility index (Session 24). |
| Schedules        | `internal/cron`, `internal/server/schedules.go` | backlog ✅ | Durable cron schedules, leader-only, idempotent per fire (Session 24). |
| Auth + TLS       | `internal/tlsconfig`, `internal/server/auth.go`, `cmd/workflowd/security.go` | — ✅ | Opt-in TLS on every listener, bearer token on HTTP + gRPC, metrics token, Raft mTLS. |

## Build / test / regenerate

```sh
make build    # go build ./...
make test     # go test ./... -race
make vet      # go vet ./...
make fmt      # gofmt -w .
make proto    # regenerate protobuf (needs protoc + ~/go/bin on PATH)
make tools    # install protoc-gen-go
```

Toolchain: Go 1.26.x, protoc 35.x (both via Homebrew). `protoc-gen-go` lives in
`$(go env GOPATH)/bin`.

## Conventions

- **Formatting:** `gofmt` is law. CI fails on unformatted code.
- **Errors:** always wrap with `%w` and a `pkg:` prefix, e.g. `fmt.Errorf("history: open: %w", err)`. No naked returns of lower-level errors.
- **Logging:** standard library `log/slog`, structured. Never `fmt.Println` for engine events. Every workflow log line carries `workflow_id` (and `step`/`worker_id` where relevant).
- **Comments:** explain *why*, not *what*. 1–2 lines. Well-named identifiers carry the rest.
- **Tests:** table-driven where it helps; every durability/concurrency claim gets a test. Use `t.TempDir()`, `-race`.
- **Determinism (workflow code, Phase 2+):** no `time.Now()`, no `rand`, no bare goroutines inside workflows — use the SDK's `workflow.Now/Random/Go`. This is load-bearing for replay.
- **Commits:** Conventional Commits — `type(scope): description` (e.g. `feat(history): add CRC framing`). No `Co-Authored-By`. Never `--no-verify`. Lint+vet+test before committing.
- **Never commit:** `.claude/`, `/data/`, `.env*` (already in `.gitignore`).

## Session protocol (IMPORTANT — do this every session)

At the **end of every working session**, before wrapping up:
1. Append a dated entry to `docs/SESSION_LOG.md` (what changed, what's next, any gotchas).
2. Update the **Status** line in `docs/ROADMAP.md` (check off completed items).
3. Update the global memory at `~/.claude/projects/-Users-horla-Desktop-durable-workflow-engine/memory/` (build memory + MEMORY.md index).
4. Write a beginner walkthrough to `docs/walkthroughs/session-NN-*.md` (plain English, Node/TS
   analogies, what was built and why) and summarize it in chat. See the Audience section.

This is how context survives across sessions and machines. The in-repo docs are the
source of truth because they travel with `git clone`; the global memory is the local cache.

## Current status

**Session 26 (correctness release, SDKs 0.2.0):** from running liftoff in production:
timeouts (ADR-0008), cancellation of workflows and steps with workers told (ADR-0009),
non-retryable failures and error types (ADR-0010), a TypeScript CommonJS build plus
`ctx.now()`/`random()`/`sleepUntil()` and signal-wait timeouts (#35–#39). Demo and the private
liftoff engine run `81c98f8`; npm + PyPI `workflowd` 0.2.0.

**Session 25 (release):** the SDKs are on npm and PyPI as `workflowd` (0.1.1 latest),
published from `release-sdks.yml` by trusted publishing on both registries; no registry
tokens exist. Maven Central (Java) is still the owner's to set up.

**Session 24 (engine backlog; parallel to Session 23):** SDK-chosen retry policies,
version markers, binary/JSON payloads, heartbeats and leader hints in every SDK (#19);
per-workflow locks + an in-memory state cache (#22); a visibility index and ADR-0006
(no LSM) (#23); cron schedules (#24); parallel steps with batched commands (ADR-0007,
#25); exactly-once signals (#26). Sticky queues closed with measurements.

**Session 23 (security, CI, packaging; parallel to the engine-backlog session):**
opt-in TLS and bearer auth on every listener (no flags = unchanged), `buf` in CI, every
SDK (Java included) run end to end in CI in plaintext and over TLS, and publish-ready
npm/PyPI/Maven packages with a manual, dry-run-by-default release workflow. Nothing is
published; `sdks/README.md` has the owner's steps.

**Phase 5 complete (Weeks 15–17): the public Go SDK, all 17 roadmap weeks done.**
Session 22: `workflow` / `worker` / `client` / `embedded` (Wk15), `GetVersion` +
`SideEffect` markers and `testsuite` (Wk16), README, quickstart, three tested
`examples/`, docs and a launch blog draft (Wk17). Engine fixes found on the way:
steps resume without a registered planner, and a stale SDK decision after an early
signal is re-dispatched.

**Phase 4 complete (Weeks 1–14): Raft replication, opt-in with `--node-id`/`--peers`.**
**Phase 3 complete (Weeks 1–10), plus polyglot workflows (Session 13, unplanned).**
Week 9: observability (zero-dep Prometheus `/metrics`; visibility API). Week 10:
JSON HTTP API + `go:embed` dashboard + public site under `web/`. Session 13:
**workflows are no longer Go-only** — a `PollWorkflowTask`/`CompleteWorkflowTask`
protocol built on the existing `Planner` seam lets an SDK in any language replay
workflow code and return one command; TypeScript and Python SDKs are verified
end-to-end, Java is written but uncompiled. Next: **Phase 4 / Week 11 — Raft**
(started, parked for polyglot; `proto/raft.proto` is groundwork). See
`docs/SESSION_LOG.md` for the running log; `docs/ROADMAP.md` Status line is
authoritative.

Session 15 hardened the public demo: activity tasks are routed by step name, and an
in-process `--demo-worker` plus opt-in `--retention` were added (see SESSION_LOG).
Sessions 16–17 hardened the history store's cached append handles: released when a
workflow finishes, and dropped on a failed write or fsync so the reopen truncates the
torn record. Session 18 started Phase 4 for real: a hand-rolled, deterministic Raft
core with leader election (`internal/raft`), a gRPC transport, and opt-in cluster
flags (`--node-id`, `--peers`); cluster mode runs Raft only until Week 14. Session 19
added log replication: majority commit, conflict repair, a durable Raft log and a
term-fenced `Node.Propose`. Session 20 added the five-property safety checker (with
teeth knobs) and snapshots; ADR-0004 fixes the design for wiring the engine in.
Session 21 completed Phase 4: in cluster mode every history write is a Raft commit,
only the leader runs an engine (a fresh one per term, after `Recover()`), followers
answer with leader hints, and real-process chaos tests cover leader kill, partition
and 100 workflows across a leader change. Production stays single-node.

**Cluster invariants worth not breaking:** apply only committed entries; a new
leader serves only after `Status.LeaderReady()` (its term's opening entry applied);
engine writes are fenced by term; the history state machine's apply is a pure
function of (file, entry) via `raft_index` stamps and the event-id fence; history
files are fsync'd before a snapshot marker claims them durable.

**Polyglot invariants worth not breaking:** SDK decisions are fenced by history
length (stale → reject); a malformed command must leave the task *pending* or the
workflow strands; a stale decision whose workflow still needs one must be
re-dispatched; and each language needs a suspend mechanism workflow code can't
catch (Go `runtime.Goexit` on its own goroutine, Python `BaseException`, Java
`Error`, JS **non-settling promise**, since `catch` catches everything in JS).

**SDK invariants worth not breaking:** recording a step's outcome (recovery,
timer, signal, activity result) never requires the workflow type to be
registered; only deciding does. A worker re-registers whenever its connection is
turned away. `GetVersion` consumes a step sequence only when a version marker is
there (or is being recorded).

**Concurrency invariants worth not breaking:** everything that reads a workflow's
history and then appends holds that workflow's lock; lock order is a workflow lock,
then `Engine.mu`, never two workflow locks; `Engine.mu` is never held across I/O. A
failed append drops the cached history (the write may have landed). The engine is the
only writer of its histories; a closed engine refuses writes. Never wait on the index
build while holding a workflow lock (Recover builds it).

**Timeout/cancel invariants worth not breaking:** every deadline is a duration recorded next to
the event it starts from, and recovery re-arms it; heartbeats extend the lease, never
start-to-close; a retry is planned only if it starts before schedule-to-close; terminal reasons and
cancelled steps are flags on existing events (older binaries keep reading them); a cancelled step
never moves `ctx.Now()`; the retry decision lives only in `retryAfter`; the TS SDK is one module
(`workflowd.cjs`) behind both entry points.

**Parallel-step invariants worth not breaking:** a failure that will be retried is
recorded with `will_retry` and must stay invisible to workflow code; the engine decides
after every step result, and recovery decides when the last event was a result;
"first of" races are decided by `ResolvedEventID`, never by call order; a single command
travels in the old `command` field so older engines understand it. Schedule run ids
are `<schedule>-<fire time>` (idempotent firing); signal idempotency keys live in
history.

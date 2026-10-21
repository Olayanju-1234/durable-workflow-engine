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
| Consensus (Raft) | `internal/raft`    | 4     | Leader election, log replication, snapshots, HA. |
| Public SDK       | `sdk`              | 5     | Ergonomic Go API developers import. |

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
torn record.

**Polyglot invariants worth not breaking:** SDK decisions are fenced by history
length (stale → reject); a malformed command must leave the task *pending* or the
workflow strands; and each language needs a suspend mechanism workflow code can't
catch (Go panic, Python `BaseException`, Java `Error`, JS **non-settling promise** —
`catch` catches everything in JS).

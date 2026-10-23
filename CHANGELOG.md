# Changelog

Notable changes to workflowd: the engine binary and its SDKs (npm `workflowd`, PyPI
`workflowd`, Maven `io.github.olayanju-1234:workflowd`). The three SDKs share one version.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions
follow [SemVer](https://semver.org/): before 1.0, a minor version may change the API.

## [Unreleased]

## [0.2.0] - 2026-10-23

The first correctness release, from running liftoff (a NestJS app) on workflowd in
production: timeouts, cancellation, non-retryable failures and TypeScript ergonomics. The new
SDK features need an engine from this release; an older engine refuses the new fields, and an
older SDK keeps working against this engine.

### Added
- Timeouts (ADR-0008), all optional and recorded in history: a workflow **execution timeout**
  (ends it as `TIMED_OUT`), and per-activity **start-to-close** (one attempt, heartbeats
  can't extend it) and **schedule-to-close** (the whole step, retries included) timeouts.
  Go: `client.WithExecutionTimeout`, `workflow.WithStartToCloseTimeout`,
  `workflow.WithScheduleToCloseTimeout` (the activity's ctx gets the deadline). TypeScript:
  `startToCloseTimeoutMs` / `scheduleToCloseTimeoutMs` on `ctx.activity`,
  `startWorkflow(type, input, id, { executionTimeoutMs })`, `task.signal` aborts at the
  limit. Python: `start_to_close_timeout` / `schedule_to_close_timeout` (seconds),
  `start_workflow(..., execution_timeout=)`, `lease_lost` is set at the limit. Java:
  `Workflowd.ActivityOptions`, `startWorkflow(type, input, id, Duration)`. HTTP:
  `execution_timeout_ms`, `start_to_close_timeout_ms`, `schedule_to_close_timeout_ms`.
- Engine metric `workflowd_activity_timeouts_total{timeout}`;
  `workflowd_workflows_finished_total{status="timed_out"}`.
- Cancellation (ADR-0009). **Cancel a workflow**: it ends as `CANCELLED` (gRPC
  `CancelWorkflow`, `POST /api/workflows/{id}/cancel`, Go `client.CancelWorkflow`, TS/Python/Java
  `worker.cancel`, a dashboard button). **Cancel a step** the code no longer wants, such as a
  race's loser: Go `ctx.CancelStep(f)` / `workflow.ErrCancelled`, TS `ctx.cancel(p)` /
  `ctx.waitAny(ps, { cancelRest: true })` / `CancelledFailure`, Python `future.cancel()` /
  `wait_any(..., cancel_rest=True)` / `CancelledFailure`; the `cancel_step` command. A cancelled
  signal wait no longer swallows a later signal meant for the next wait.
- Workers learn that their activity was cancelled (its step or workflow): heartbeat and complete
  answer 409 `"cancelled": true` / gRPC `ActivityCancelled`; Go activity ctx cause
  `worker.ErrActivityCancelled`, TS `task.signal` aborts with `CancelledFailure`, Python sets
  `cancelled`. Metric `workflowd_steps_cancelled_total`.

- Non-retryable failures (ADR-0010). An activity can fail its step at once, whatever the retry
  policy: Go `worker.NonRetryable(err)`, TS `throw new NonRetryableError(msg)` (or any error with
  `nonRetryable: true`), Python `raise NonRetryableError(msg)`, Java
  `throw new Workflowd.NonRetryableException(msg)`. A retry policy can list error types it should
  not retry (`NonRetryableErrorTypes` / `nonRetryableErrorTypes` / `non_retryable_error_types` /
  `withNonRetryableErrorTypes`); an error's type is its class name in TS/Python/Java,
  `worker.Typed(type, err)` in Go, and `LeaseExpired` / `StartToCloseTimeout` /
  `ScheduleToCloseTimeout` for the engine's own failures. Workflow code sees the type and flag:
  Go `*workflow.ActivityError`, TS/Python/Java `ActivityFailure.type` / `nonRetryable`. HTTP
  `error_type` / `non_retryable` on activity completion; SDKs resend without them to an engine
  that refuses them.

- TypeScript: a **CommonJS build** next to the ESM one (`require('workflowd')` works on any Node
  18+; both load one module, so `instanceof` checks hold across them); `ctx.now()` (deterministic
  clock), `ctx.random()` (seeded per execution), `ctx.sleepUntil(name, date)`,
  `ctx.waitForSignal(name, signal, { timeoutMs })` (resolves to `undefined` on timeout and
  cancels the wait), and `task.attempt` / `task.activityId` in activities.
- Activity tasks carry `attempt` and a stable `activity_id` (Go `worker.ActivityInfo.Attempt` /
  `ActivityID`, Python `info["attempt"]` / `info["activity_id"]`); workflow tasks carry
  `started_at` and each step's `resolved_at`.

### Fixed
- Go: `ctx.Now()` after `GetVersion` or `SideEffect` read the marker's time on replay but not on
  the first run; markers no longer move the workflow clock.
- A finished workflow's steps that were still open (a race's losing timer, say) showed as
  `SCHEDULED` forever; they now show as `CANCELLED` ("not needed").

## [0.1.1] - 2026-10-22

No SDK behaviour changes (only the reported version string); the first release published
entirely from CI.

### Changed
- Releases: npm publishes through trusted publishing (OIDC) like PyPI; `NPM_TOKEN` is no
  longer used. The npm job runs on Node 24 with npm 11.5.1+.

## [0.1.0] - 2026-10-22

The first published SDK release: npm and PyPI on 2026-10-22; Maven Central to follow.

### Added
- SDKs for TypeScript/JavaScript, Python and Java: workflows as ordinary functions with
  durable activities, sleeps and signals, Saga compensation by catching an activity failure,
  and deterministic replay that workflow code cannot interrupt. Each is one dependency-free file.
- Publish-ready packages: `sdks/typescript/package.json` with TypeScript declarations
  (`workflowd.d.mts`), `sdks/python/pyproject.toml`, `sdks/java/pom.xml`, and a manual
  release workflow (`.github/workflows/release-sdks.yml`).
- Auth and TLS in the SDKs: a `token` option and `https://` addresses (a private CA via
  `NODE_EXTRA_CA_CERTS`, Python `ca_file`, Java `Workflowd.trustPem`).
- Engine: opt-in TLS on the gRPC, dashboard/API and metrics listeners (`--tls-cert`,
  `--tls-key`, reloaded on renewal); a bearer token on the JSON API and gRPC from a file or
  `$WORKFLOWD_AUTH_TOKEN` (`--auth-token-file`); `--auth-public-reads`; a separate metrics
  token (`--metrics-auth-token-file`); mutual TLS between Raft peers (`--raft-tls-*`).
- Go client: `client.Options.TLS`, `client.Options.AuthToken`, `client.ErrUnauthenticated`.
- CI: every SDK runs end to end against a real engine, in plaintext and over TLS with a
  required token; `buf lint` and `buf breaking` guard the protobuf wire format.
- SDKs: retry policies on an activity call (recorded with the step), `getVersion` /
  `sideEffect`, JSON and binary payloads, heartbeats for long activities, a list of
  addresses with cluster leader-hint following and re-registration after a restart,
  exactly-once signals (an idempotency key per signal). TypeScript and Python: parallel
  steps (`Promise.all` / `ctx.waitAny`; `activity_async`, `wait_all`, `wait_any`).
- Engine: per-workflow locks and an in-memory state cache, a visibility index, cron
  schedules (`/api/schedules`, `CreateSchedule`), parallel steps with batched commands,
  `request_id` on `SignalWorkflow`, `POST /api/activity-tasks/heartbeat`.
- Go: `workflow.WithRetryPolicy`, `ExecuteActivityAsync`, `NewTimer`, `WaitForSignalAsync`,
  `WaitAny`; `client.CreateSchedule` / `DeleteSchedule` / `ListSchedules`,
  `client.WithSignalRequestID`.

### Changed
- `--auth-token` now also requires the token on gRPC calls, not only the JSON API.
- Go: `client.SignalWorkflow` is exactly-once (it sends an idempotency key); a closed
  engine refuses writes.
- Java: `Workflowd` moved from the default package to `io.github.olayanju1234.workflowd`.
- `worker.Run` (Go) returns at once when the engine rejects its token.

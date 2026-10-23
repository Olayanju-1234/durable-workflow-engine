# Changelog

Notable changes to workflowd: the engine binary and its SDKs (npm `workflowd`, PyPI
`workflowd`, Maven `io.github.olayanju-1234:workflowd`). The three SDKs share one version.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions
follow [SemVer](https://semver.org/): before 1.0, a minor version may change the API.

## [Unreleased]

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

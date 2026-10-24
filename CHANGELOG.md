# Changelog

Notable changes to workflowd: the engine binary and its SDKs (npm `workflowd`, PyPI
`workflowd`, Maven `io.github.olayanju-1234:workflowd`). The three SDKs share one version.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions
follow [SemVer](https://semver.org/): before 1.0, a minor version may change the API.

## [Unreleased]

### Added
- **Namespaces** (ADR-0015): logical tenants inside one engine, each with its own workflows,
  schedules, workflow types, task routing and retention. `--namespaces FILE` (JSON) configures
  them with their own tokens (`token_file` or `token_sha256`) and retention. A namespace token
  reaches only its namespace (403 elsewhere); the engine token (`--auth-token`) reaches all; a
  call picks one with `X-Workflowd-Namespace`, `?namespace=` or gRPC metadata
  `workflowd-namespace`. `GET /api/namespaces`. Go `client.Options.Namespace`, `namespace` in the
  TypeScript and Python workers, `worker.namespace(...)` in Java, a namespace picker in the
  dashboard. The `default` namespace is everything that existed before: histories, URLs, clients
  and the open demo are unchanged. A namespace's histories live in `namespaces/<name>/` and need
  history format 6.
- **Named task queues** (ADR-0016): workflow and activity tasks are routed by queue, then by type
  or step name, so two workers can implement the same activity name for different apps. A
  workflow runs on the queue it was started on (`task_queue`; Go `client.WithTaskQueue`), and its
  activities do too unless the code names another (`workflow.WithTaskQueue`, TS/Python/Java
  activity options); retries, recovery and continue-as-new keep the queue. Workers poll one queue
  (Go `worker.Options.TaskQueue`, TS `taskQueue`, Python `task_queue`, Java `.taskQueue()`), and the
  SDK workers start workflows on their own queue by default. The default queue is what every
  existing worker and start uses. Schedules name theirs. Describe, the dashboard and tasks show
  `task_queue`. A named queue needs history format 6.

- **`workflowd ctl`** (ADR-0017): the operator's command line, a subcommand of the same binary over
  the JSON API. `health`, `namespaces`, `list` (status, type, text, paging), `describe`, `history`,
  `runs`, `start`, `signal`, `cancel` (`--graceful`), `terminate`, `schedule list|create|delete`,
  `backup` (from the engine with its token, or a data directory) and `restore` (offline, into a
  new directory only). Address, token file, namespace and CA from flags or `WORKFLOWD_*`; tables,
  or `--json`.
- `GET /api/workflows/{id}/history[?run_id=]`: a run's events as recorded (protobuf JSON).
  `GET /api/admin/backup`: a tar.gz of every history, engine token only. `GET /api/workflows`
  takes `type=` (exact workflow type).

- **Guardrails** (ADR-0018), engine-wide by flag and per namespace: quotas on starts
  (`--max-active-workflows`, `--max-starts-per-minute`; checked by the engine for every start,
  schedule fires included, race-free), per-client rate limits on writes (`--rate-limit`,
  `--rate-limit-burst`; by namespace token, else by address, with `X-Forwarded-For` believed only
  from `--trusted-proxies`; the engine token is exempt), and read-only modes (`--read-only`,
  `--read-only-schedules`). Refusals are 403 (`PERMISSION_DENIED`) or 429 with `Retry-After`
  (`RESOURCE_EXHAUSTED`). Metrics `workflowd_requests_refused_total{reason}` and
  `workflowd_starts_refused_total{reason}`.

### Changed
- SDK workers (TypeScript, Python, Java) retry a registration refused with 429 instead of
  stopping; the Go worker's `Run` stops on `PERMISSION_DENIED` like an invalid token.
- The SDK examples run on their own task queues (`ts-orders`, `py-orders`, `java-orders`), so
  they no longer share the demo worker's `reserve`/`charge`/`ship`/`release`.

## [0.3.0] - 2026-10-24

The second release from running liftoff in production: workflows that run forever, a contract
for upgrades and rollbacks, idempotent and reusable workflow ids, graceful cancellation, and the
Python and Java SDKs caught up with TypeScript. The new SDK features need an engine from this
release; older SDKs keep working against it. Every existing history stays readable. See
`docs/UPGRADING.md` before upgrading: the history limits apply to existing workflows.

### Added
- **An upgrade/rollback contract** (ADR-0011, `docs/UPGRADING.md`). History has numbered formats;
  each event that an older binary would refuse or misread carries `min_reader_format`, and the data
  directory records its format in `FORMAT`, raised the first time a feature that needs it is used.
  A binary refuses to start on a directory (or recover a history) in a newer format than it reads,
  with a message naming the release it needs, instead of failing workflow by workflow.
  `workflowd --check-format` reports what a directory needs and whether this binary serves it;
  `--rescan-format` records the current need (lowering it after purges); `--version` and
  `/api/health` (`history_format`, `reads_history_format`) report formats.
- **Python** SDK: `ctx.now()` (deterministic clock, UTC `datetime`), `ctx.random()`,
  `ctx.sleep_until(name, when)`, and `ctx.wait_for_signal(name, signal, timeout=seconds)` (returns
  `None` on timeout and cancels the wait).
- **Java** SDK: parallel steps (`ctx.activityAsync`, `sleepAsync`, `waitForSignalAsync` return a
  `Workflowd.Future`; `ctx.waitAll`, `ctx.waitAny(cancelRest, ...)`), step cancellation
  (`future.cancel()`, `CancelledFailure`), `ctx.now()`, `ctx.random()`, `ctx.sleepUntil`,
  `ctx.waitForSignal(name, signal, Duration)` (null on timeout), and
  `worker.activityWithContext` (an `ActivityContext` with the attempt, an idempotency key and
  `isLeaseLost()` / `isCancelled()`). `worker.start()` serves in the background; `worker.describe`.
  Several commands per task (`commands`), like the other SDKs.
- The Java SDK runs end to end from `go test ./sdks` when a JDK is installed (as in CI).
- **Continue-as-new** (ADR-0012): a workflow ends its run and starts a fresh one under the same
  workflow id with an empty history. Go `return nil, workflow.ContinueAsNew(state)`, TypeScript
  `return ctx.continueAsNew(state)`, Python `return ctx.continue_as_new(state)`, Java
  `return ctx.continueAsNew(state)`; the `continue_as_new` command. Every run has a run id
  (`run_id`, `ctx.Info().RunID`, `ctx.runId`); earlier runs are kept until retention removes them:
  `GET /api/workflows/{id}/runs`, `GET /api/workflows/{id}?run_id=`, gRPC `ListWorkflowRuns`, Go
  `client.ListWorkflowRuns` / `DescribeWorkflowRun`, `worker.runs` / `describe(id, runId)` in the
  other SDKs. Status `CONTINUED_AS_NEW` (earlier runs). Crash-safe: recovery finishes a
  continue-as-new that a crash interrupted. The test environment follows it (`env.Runs()`).
- **History and payload limits** (ADR-0012): `--max-history-events` (51,200), `--max-history-bytes`
  (50 MiB) per run, `--max-payload-bytes` (2 MiB) per payload on HTTP and gRPC alike. At 80% of a
  history limit the engine warns and `continueAsNewSuggested()` (all SDKs) turns true, at the same
  point on every replay; at the limit the workflow fails with a clear error. Oversized payloads are
  refused (413 / `RESOURCE_EXHAUSTED`) or, when produced inside, fail the step (`PayloadTooLarge`)
  or the workflow. Metric `workflowd_history_limit_total{kind}`; `history_size_bytes` on workflows.
  Workflow tasks carry `run_id`, `history_size_bytes` and `suggest_continue_as_new_at`.
- **Workflow id reuse and conflict policies** (ADR-0013): `id_reuse_policy` (`REJECT_DUPLICATE`
  default, `ALLOW_DUPLICATE`, `ALLOW_DUPLICATE_FAILED_ONLY`) lets a finished id start a new run,
  keeping the finished one as an earlier run; `id_conflict_policy` `USE_EXISTING` returns the run
  that holds a taken id instead of a 409 (HTTP 200, `started: false`). Concurrent starts for one id
  begin exactly one run. Go `client.Start` + `WithIDReusePolicy` / `WithIDConflictPolicy`, TS
  `worker.start(type, input, { workflowId, idReusePolicy, idConflictPolicy })`, Python
  `worker.start(..., id_reuse_policy=, id_conflict_policy=)`, Java `worker.start(type, input, id,
  StartOptions)`; all return the run id and whether a run started.
- **Graceful cancellation** (ADR-0014): cancel with `graceful` and the workflow code is told at
  its next wait: the steps it waits on fail with a cancellation (Go `workflow.ErrWorkflowCancelled`,
  `CancelledFailure` in TS/Python/Java), open ones are cancelled, and steps it starts afterwards
  run normally, so it can compensate; it ends `CANCELLED` if it lets the cancellation end it. The
  point it is told is recorded (a `cancellation` marker step) and replays agree. Go
  `client.CancelWorkflow(ctx, id, reason, client.Graceful())` and `ctx.CancelRequested()`, TS
  `worker.cancel(id, reason, { graceful: true })` / `ctx.cancelRequested`, Python
  `worker.cancel(id, reason, graceful=True)` / `ctx.cancel_requested`, Java `worker.cancel(id,
  reason, true)` / `ctx.cancelRequested()`, HTTP `{"graceful": true}`, the dashboard's *Request
  cancel*, `testsuite.Env.RequestCancelAfter`. Metric `workflowd_cancel_requests_total`. The
  hard cancel is unchanged. The demo's `OrderSaga` releases its reservation when cancelled.

### Changed
- Signal names starting with `__workflowd` are reserved (one is the graceful cancel request).
- Registering an SDK now resumes every workflow of its types that owes a decision, not only
  ones with nothing in flight, so a step result or cancel request recorded while no worker ran
  the type is acted on as soon as one connects.
- HTTP request bodies may be up to four payloads plus 1 MiB (9 MiB by default; was 1 MiB), the
  same as gRPC messages; the Go client accepts replies up to 128 MiB, so a large history reaches it.
- New workflow ids are limited to 190 bytes, so an earlier run's file name fits.
- Start returns the run id (`run_id` in the HTTP response, `StartWorkflowResponse.run_id`).

### Fixed
- Retention skipped cancelled workflows; it now purges them, and earlier runs of continued workflows.

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

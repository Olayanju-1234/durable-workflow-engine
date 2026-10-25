# Session Log

Append-only log of working sessions. Newest first. Each entry: date, what changed,
what's next, gotchas. Keep it terse — this is the breadcrumb trail across sessions/machines.

---

## 2026-10-25 — Session 29: 0.5.0, see it, fix it, trust it (SDKs 0.5.0)

0.4.0 made one engine safe for several apps. 0.5.0 is about operating it: a dashboard that is live
and navigable, a way to fix a failed run instead of starting over, numbers without Prometheus,
history encrypted at rest, tokens rotated without a restart, a record of who did what, and traces
that follow a request through a workflow into its activities. Eight PRs, each squash-merged on
green CI, then both deploys.

**Done:**
- #57 roadmap: ticked graceful cancellation (ADR-0014, #46) and Python/Java parity (#43), shipped in 0.3.0.
- #58 **Live dashboard** (`83290e4`): `GET /api/events` (server-sent events) replaces 2 s polling:
  an in-engine change feed (`feed.go`) published from the visibility index and schedules, 200 ms
  coalescing, 15 s pings, `X-Accel-Buffering: no`, at most 256 subscribers, a lagging client gets
  `resync`. The page streams with `fetch` (EventSource can't send a token) and falls back to
  polling on a 404 or after 4 failed reconnects. Rows update in place by key (selection and scroll
  kept); a URL per workflow (`/workflows/<id>?namespace=&run=&tab=`); an event-history timeline
  with pretty-printed payloads; filters for status, type, start-time range, namespace and search
  (`started_after`/`started_before`/`sort` on the list API, plus status counts and types).
- #59 **Reset + bulk actions** (ADR-0021, `94bc7f3`): `POST /api/workflows/{id}/reset`, gRPC
  `ResetWorkflow`, `ctl reset [--event N]`, client `ResetWorkflow`. A reset is a new run whose first
  events are the old run's up to the reset point, unchanged; `WorkflowStarted` gets a new run id and
  `reset_from_run_id`/`reset_event_id`/`reset_reason`; signals received after the point are
  reapplied, cancel requests are not; the execution timeout is extended. Default point: the opening
  event of the last step that failed for good (or was open at the end), else the last event. The
  multi-event `NewRun` is atomic (`.next` + rename; the cluster replicates it as one entry). No
  format bump. Bulk cancel/terminate/reset in `ctl` (`--status/--type/--query`, asks `yes`) and in
  the dashboard (row selection, bulk bar, confirmation naming what is skipped); the signal form
  validates JSON.
- #60 **The pulse** (`3f21c81`): an in-engine sampler (10 s, 1 h ring) over the existing metrics,
  per namespace: throughput, failure rate, step p50/p95 latency, activity and workflow queue depth,
  active workflows. `GET /api/metrics?window=1m..1h`; new labelled families
  `workflowd_namespace_*`; a dashboard panel with KPI tiles, three SVG charts with a crosshair
  tooltip and a table view (palette run through the dataviz validator, light and dark).
- #61 **Encryption at rest** (ADR-0022, `f830263`): `--encryption-keys FILE` (JSON keyring, one
  active key id, older ids kept for reading). AES-256-GCM per event, AAD binds the key id and event
  id, so a record can't be moved or relabelled. `HistoryEvent.sealed`/`key_id`; format 7 only once
  a sealed event exists; plaintext histories stay readable. `--encrypt-all` / `--decrypt-all`
  rewrite a stopped engine's data dir; `--check-format` reports which keys histories need. Losing
  every copy of a key loses the histories sealed with it: docs say so in bold.
- #62 **Reload + audit** (ADR-0023, `1508da0`): the auth token file, metrics token, namespaces file
  and keyring reload on SIGHUP and when their content changes (checked every `--reload-interval`,
  10 s), all or nothing: a bad file keeps the old config and logs why. An append-only audit log
  (`--audit-log`, default `<data-dir>/audit/audit.log`, `off` to disable; JSON lines, fsync per entry,
  16 MiB × 4 rotation) records start, signal, cancel (and graceful cancel requests), terminate,
  reset, schedule changes, backups and config reloads (which namespaces were added, removed or
  changed, which tokens and keys were replaced) with who (`engine:<hash>`, `token:<hash>`,
  `anonymous`, `operator`), when, outcome and the client address; never payloads or tokens. Recorded at the transport edge from the guard's own
  classification, with a test that every mutating route is classified. `GET /api/audit` (anonymous
  403), `ctl audit`.
- #63 **Tracing** (ADR-0024, `2979ae8`): W3C `traceparent`/`tracestate` on start (headers, request
  field or gRPC metadata) makes the run a child span, recorded on `WorkflowStarted.trace`; workflow
  tasks carry the run's span (stable across replays), activity tasks a new span per attempt; every
  SDK exposes both. Optional `--otlp-endpoint` export (OTLP/HTTP protobuf, hand-encoded with
  `protowire`, checked with `protoc --decode` against opentelemetry-proto v1.3.2), batched, never
  blocking. No format change.
- #64 release 0.5.0 (`7e1dc62`): npm dry run 40154714552, publish 40154759585; PyPI 40154816924;
  tag `sdks-v0.5.0` @ `7e1dc62`. Clean installs (npm `require` via `NODE_PATH`, PyPI venv) ran a
  traced workflow on the demo on their own queues (`verify-050-ts-mvn80vk1`,
  `verify-050-py-6add6c61`): COMPLETED, the caller's trace id reached the workflow (a new run span)
  and the activity (its own span), tracestate passed through.
- **Deployed `7e1dc62`** (fresh backups `data-20261025T022904Z` / `liftoff-data-20261025T022904Z`
  first; both data copies recovered and passed `--check-format` locally first; `.prev` = `4d9ac4c`
  on both):
  - demo, 02:34 UTC, unit unchanged (guardrails kept, nginx untouched): 33 workflows recovered,
    format 6. Through nginx: SSE streams unbuffered (hello, then 5 `workflow` frames for one run);
    `/api/metrics` 200; deep link `/workflows/<id>` 200; OrderSaga `verify-050-023846` COMPLETED; 30
    parallel writes → 20 through + 10 × 429; `POST /api/schedules` 403; `/api/audit` 403; the audit
    log `/opt/durable/data/audit/audit.log` records each start as `anonymous` with the client IP.
    Site redeployed (docs.html), byte-identical. Hookdash fine.
  - workflowd-liftoff, 02:39 UTC: 0 open, 26 workflows recovered, format 6; liftoff's worker
    re-registered both types 15 ms after the restart; api.liftoff `/health` 200; smoke
    `liftoff-webhook-smoke-20261025023956` COMPLETED "no webhook configured"; `ctl audit -n liftoff`
    shows it under the engine token's id; reloads cover the token and namespaces files; `/api/metrics`
    401 without a token.
  - **Encryption and OTLP are off on both** (the keys are the owner's call).

**Teeth:** 57 mutations across #59, #61, #62, #63: all caught (the key-id binding needed a twin-key
test to be observable).

**Fixed on the way:** `TestLosingTimerIsNotShownAsScheduled` raced a 200 ms timer under load (now
1 s); `TestRetentionPurgesArchivedRuns` could have its step taken by another test's worker (now on
its own task queue).

**Next (owner):** encryption keys (where the keyring lives, how it is backed up off the box; then
`--encryption-keys` + `--encrypt-all` per engine), an OTLP collector if traces are wanted, an R2
bucket + credentials for archiving, Maven Central.

**Gotchas:**
- Token rotation no longer needs an engine restart: edit the token or namespaces file and the engine
  picks it up within 10 s (or `systemctl kill -s HUP`). liftoff itself still restarts to read its `.env`.
- The audit log lives in the data dir, so the daily backup tar includes it.
- Rolling back to 0.4.0 is safe while encryption has never been on (dirs stay format 6). After
  `--encrypt-all`, a rollback needs `--decrypt-all` with the new binary first.
- `require('workflowd/package.json')` fails (not in the package's `exports`); read the file instead.
- My verify loop grepped for `"status":"COMPLETED"` in the describe JSON and matched a *step's*
  status: check the top-level field (`ctl describe` or `jq .status`).

---

## 2026-10-24 — Session 28: 0.4.0, one engine for several apps (SDKs 0.4.0)

A product review after 0.3.0 listed four pains: liftoff needed a whole second engine to stay
private, the SDK examples' activities collided with the demo worker's, every operation was
hand-written `curl`, and the demo's safety lived in nginx (shared with three other apps). Six
feature PRs and a release, each squash-merged on green CI, then both deploys.

**Done:**
- #49 **Namespaces** (ADR-0015): keys are the bare id in `default`, `<ns>/<id>` elsewhere
  (`data/namespaces/<ns>/<id>.history`, format 6, raised before the dir is made). `Engine.In(ns)`
  returns a `Scope`, the only door the APIs use: it rejects ids with `/` and maps every output back
  to local ids; polls, planners, retry policies, queues, schedules, index and retention are all
  per namespace. `--namespaces FILE` (JSON: tokens by file or SHA-256, retention). One `Authorizer`
  for HTTP and gRPC: engine token = all namespaces, namespace token = its own (403 elsewhere), no
  token = an open default only. `X-Workflowd-Namespace` / `?namespace=` / gRPC `workflowd-namespace`.
- #50 **Named task queues** (ADR-0016): routes are (namespace, queue, name); workers poll one
  queue; activities inherit the workflow's queue unless told otherwise; the resolved queue is
  recorded on `WorkflowStarted` / `StepScheduled` / `ContinuedAsNew` (only when not default, format
  6). Every SDK; examples on `ts-orders` / `py-orders` / `java-orders`, so they no longer collide
  with the demo worker.
- #51 **`workflowd ctl`** (ADR-0017): a subcommand over the HTTP API (health, namespaces, list,
  describe, history, runs, start, signal, cancel, terminate, schedule ..., backup, restore,
  archive ...), human tables or `--json`, exit codes 0/1/2, token from `WORKFLOWD_TOKEN_FILE` /
  env. New API: `GET /api/workflows/{id}/history` (protojson), `GET /api/admin/backup` (engine
  token only, tar.gz with `FORMAT` last). `ctl restore` writes only into a new, empty dir, checks
  every record, renames into place.
- #52 **Guardrails** (ADR-0018): per-namespace `max_active_workflows` / `max_starts_per_minute`
  (reserved under a lock before the start, refunded if it doesn't start); per-client token-bucket
  write limits (token, else IP; `X-Forwarded-For` read right to left only from
  `--trusted-proxies`); `--read-only` / `--read-only-schedules` (or per namespace). 429 with
  `Retry-After`, 403 with a reason; `workflowd_requests_refused_total{reason}`.
- #53 **Archiving** (ADR-0019): `--archive <dir> | file:///dir | s3://bucket/prefix?endpoint=&region=`
  (R2, S3) with hand-rolled SigV4 (botocore's aws4_testsuite vectors), Content-MD5 + ETag check;
  a run is deleted only after its archive copy is confirmed; `GET /api/archive/workflows[...]`,
  `ctl archive ...`.
- #54 **Schedule controls** (ADR-0020): pause/resume with a note, overlap `SKIP` / `BUFFER_ONE` /
  `ALLOW_ALL`, deterministic jitter (FNV of schedule + fire time), idempotent backfill (≤ 1000
  fires), preview of the next N (also for an unsaved spec); API, CLI and the dashboard's schedules
  panel. Paused/jitter/`BUFFER_ONE` specs are format 6.
- #55 release 0.4.0: npm dry run 40127281371, publish 40127369923; PyPI 40127527352; tag
  `sdks-v0.4.0` @ `4d9ac4c`. Clean installs (npm `require`, PyPI venv) ran a workflow whose
  `charge` activity sat on its own queue (`verify-040-ts` / `verify-040-py`) next to the demo
  worker's `charge` on the default queue; both COMPLETED with the SDK worker's output.
- **Deployed `4d9ac4c`** (fresh backups first; `.prev` = `83a1640` on both):
  - demo, 19:59 UTC, with `--rate-limit 10 --rate-limit-burst 20 --trusted-proxies 127.0.0.1,::1
    --read-only-schedules` (unit backup `/root/workflowd.service.20261024-1956`). From the box,
    30 parallel writes under one `X-Forwarded-For`: 20 through, 10 × 429 with `Retry-After`; another
    client still served; `POST /api/schedules` 403 from the engine; OrderSaga
    `verify-040-saga-200155` COMPLETED; Hookdash fine. Site redeployed (byte-identical).
  - workflowd-liftoff, 20:04 UTC: 0 open workflows, format 1. liftoff's worker re-registered both
    types 0.2 s after the restart; API `/health` 200; smoke `liftoff-webhook-smoke-20261024200500`
    COMPLETED "no webhook configured" (started with `workflowd ctl` on the box).
  - nginx untouched. Its demo write limit and read-only schedules are now redundant with the
    engine's own (same numbers); left in place. Its `client_max_body_size 16k` is stricter than the
    engine's 1 MiB request limit.

**Teeth:** 76 mutations across #49, #50, #52, #53, #54: 74 caught, 2 equivalent (the retry event's
queue, which the fold already carries; a paused check a pause already makes unreachable).

**Bugs found on the way:** continue-as-new wrote the namespaced key into the next run's
`WorkflowStarted` (Go code would have seen `acme/order-1` as its id); the rate limiter's sweep ran
after a new client's bucket was made and deleted it (every client's first write was free); a
`BUFFER_ONE` held fire could be starved by newer fires; `ArchivedHistory` passed a `/` prefix to a
validated lookup.

**Next:** liftoff could move from its own engine to a namespace (its call; the demo engine is
public, so a private namespace there trades isolation for one less unit). Archiving to R2 needs a
bucket and credentials (owner); it was verified with signing test vectors and a fake S3 only.
Maven Central (owner). Per-user auth/RBAC beyond namespace tokens.

**Gotchas:**
- The demo data dir is format 6 now (the verify runs used named queues). Rolling back below 0.4.0
  needs them purged (retention, from 2026-10-31 ~20:05 UTC), then `--rescan-format`. liftoff is 1.
- `--trusted-proxies` must list only the proxy's address: XFF from anyone else is ignored, and
  nginx's `$proxy_add_x_forwarded_for` appends the real peer, so each visitor gets their own bucket.
- A `BUFFER_ONE` held fire lives in memory; after a restart, catch-up takes the latest missed fire.
- liftoff's "token file is readable by other users" warning is expected (root:deploy 640, since #18).

---

## 2026-10-23 — Chaos tests keep their ports (CI flake on #40)

`TestChaosKillLeaderMidWorkflow` failed on CI run 39924292452 (docs-only #40): `reservePort`
closed each listener at once, the n1→n3 Raft proxy was then given n3's freed gRPC port (42839),
n3 exited on `bind: address already in use`, and killing the leader left one live node.

**Done:**
- The chaos harness binds every node's Raft, gRPC and HTTP ports itself and holds them for the
  whole test. Every run of a node, restarts included, inherits the sockets as fds 3–5
  (`cmd.ExtraFiles` → `net.FileListener`), so nothing binds between picking a port and serving it.
- Seams: `main.listen` (`net.Listen` in production; the HTTP servers now use it via
  `Serve`/`ServeTLS`) and `cluster.Config.Listen` for the Raft listener. The chaos node's
  `listen` rejects any address it was not handed, so a new listener can't quietly bind.
- A held socket queues connections to a dead node instead of refusing them, so the test's own
  gRPC/HTTP calls go through proxies too, and every proxy into a node is `down` (accept and
  close, like a refused connection) while it isn't running. `down` is separate from `cut`, so
  `heal` never revives a dead node.
- `go test ./cmd/workflowd -run Chaos -race -count=20`: all 60 runs pass (327 s, macOS).
- #41's own CI then hit a second flake, `TestScheduleToCloseStopsRetries` (#35): it wanted
  exactly 3 attempts inside 300 ms, about 150 ms of slack. Now 200 ms backoff and a 1400 ms
  deadline (the earliest a 4th attempt could start): 800 ms of slack, still fails if the
  deadline check in `retryAfter` is removed.

**Gotchas:**
- `exec` calls `File.Fd()` on `ExtraFiles`, which switches the shared socket to blocking mode.
  Harmless only because the test never accepts on the held listeners (the child switches it back
  in `net.FileListener`); don't add a test-side accept loop on them.
- The in-process cluster and raft tests still close reserved listeners just before binding (a
  microsecond window, never seen failing); `cluster.Config.Listen` is the hook if they ever do.

---

## 2026-10-24 — Session 27: 0.3.0, workflows that run forever (SDKs 0.3.0)

A product review after 0.2.0 ranked the remaining P0 gaps; all five shipped as separate PRs,
each squash-merged on green CI, then a release and both deploys.

**Done:**
- #42 **History formats + upgrade/rollback contract** (ADR-0011, `docs/UPGRADING.md`): formats
  1 base, 2 markers, 3 parallel steps, 4 closed steps (0.2.0), 5 runs/graceful cancel (0.3.0).
  `HistoryEvent.min_reader_format` stamps; data-dir `FORMAT` raised by the store before the first
  event that needs it (engine, cluster apply, snapshot install); `NewHistoryStore` refuses a newer
  directory; recovery refuses a newer history naming the workflow; unstamped histories classified
  by the Folder; `--check-format` / `--rescan-format`; `/api/health` `history_format`.
- #43 **SDK parity**: Python `ctx.now()`/`random()`/`sleep_until`/`wait_for_signal(timeout=)`;
  Java rebuilt on pending commands + `Workflowd.Future` (parallel steps, `waitAll`/`waitAny`,
  `future.cancel()`), `now()`/`random()`/`sleepUntil`/signal timeouts, `activityWithContext`
  (`isCancelled()`), `worker.start()`/`describe`. `TestJavaSDK` (sdks/) runs `E2E.java` when a JDK
  exists and **fails, not skips, when `CI` is set**.
- #44 **Continue-as-new + limits** (ADR-0012): `WorkflowCompleted.continued_as_new` (format 5);
  `HistoryLog.NewRun` = link old file to `__workflowd-run~<closed hex>~<run>~<id>`, write
  `<id>.history.next`, rename, fsync dir (Raft `NewRun` command, skipped on replay); recovery
  finishes an interrupted rotation; run ids on `WorkflowStarted` (legacy = `%016x` of start ts);
  timers carry `RunID`; `ListWorkflowRuns` / `?run_id=`; retention purges archives (and now
  CANCELLED). Limits 51,200 events / 50 MiB / 2 MiB payload (flags), warn at 80% with a
  deterministic `continueAsNewSuggested()` (task carries the event id that crossed); request limit
  4 payloads + 1 MiB on HTTP and gRPC; Go client receives 128 MiB; workflow ids ≤ 190 bytes.
- #45 **Id reuse/conflict policies** (ADR-0013): `REJECT_DUPLICATE` (default) /
  `ALLOW_DUPLICATE` / `ALLOW_DUPLICATE_FAILED_ONLY`; `USE_EXISTING` returns the holder (HTTP 200,
  `started:false`). Decided under the workflow lock; `NewRun` refuses an unfinished run.
- #46 **Graceful cancellation** (ADR-0014): `SignalReceived{cancel_request}` (reserved
  `__workflowd.cancel`, format 5); SDKs tell the code at its first wait, interrupt what it waits on
  (cancel_step for open ones), let new steps run; a `cancel` marker records the history length the
  delivering decision saw and replays hide later results until they reach it. `fail_workflow
  {cancelled}` → CANCELLED only if requested. `ctx.CancelRequested()`; dashboard *Request cancel*;
  demo `OrderSaga` releases on cancel; `resumeType` now resumes WAITING workflows that owe a decision.
- #47 release 0.3.0: npm dry run 40114017252, publish 40114071888; PyPI 40114139024; tag
  `sdks-v0.3.0` @ `83a1640`. Clean installs (npm `require`, PyPI venv) ran continue-as-new,
  `USE_EXISTING` and a graceful cancel with compensation on the public demo.
- **Deployed `83a1640`** to the demo (site too) and to `workflowd-liftoff` (23 COMPLETED, 0 open
  before the swap; fresh backups first; `.prev` = `81c98f8` on both). Prod data copies recovered
  locally first (demo format 4, liftoff format 1). liftoff's worker re-registered in 0.2 s; API
  `/health` 200; smoke webhook `liftoff-webhook-smoke-20261024165831` COMPLETED "no webhook
  configured".

**Teeth:** ~60 mutations across the five PRs, all caught in the end. Survivors fixed: weak clock /
timeout checks in the Python e2e (presentation masked an uncancelled wait), a duplicated retention
guard (made one authoritative), two graceful-cancel replay rules without a test.

**Bugs found on the way:** `GetWorkflowRun` read cached state after unlocking (race, caught by
`-race`); graceful delivery first computed the interrupted set after flipping `delivered`; two
pre-existing timing flakes (100–150 ms timers racing the test's own calls) widened.

**Next:** 0.4.0: namespaces, named task queues, `workflowd ctl`, quotas and a read-only mode,
archiving, schedule controls. liftoff could adopt 0.3.0 (`USE_EXISTING` instead of catching 409s,
graceful cancel for custom steps). Maven Central (owner).

**Gotchas:**
- The graceful-cancel marker is load-bearing for determinism; a new SDK must implement the
  cutoff rule (sdks/README.md) or ignore cancel requests.
- `isTerminalStatus` now includes CONTINUED_AS_NEW; the only guard against purging a run mid
  continue-as-new is in `purgeIfExpired` (tested).
- Rollback below 0.3.0: check `--check-format` first; a directory where anything continued as new
  or was gracefully cancelled is format 5. Today: liftoff 1; demo 5 (the verification runs used
  both features; retention purges them from 2026-10-31, then `--rescan-format`).
- `/usr/bin/javac` on the laptop is the macOS stub; Docker isn't running. Java only compiles in CI.

---

## 2026-10-23 — Session 26: the correctness release (SDKs 0.2.0), from running liftoff

liftoff (NestJS) began running webhooks and custom steps on a private workflowd on 2026-10-22;
a product review ranked the gaps it hit. Four PRs, each squash-merged on green CI, then a release.

**Done:**
- #35 **Timeouts** (ADR-0008): `execution_timeout_ms` on start (`WorkflowFailed{timed_out}` →
  `TIMED_OUT`), `start_to_close_timeout_ms` (per attempt, on the lease timer: `lease.due()`,
  heartbeats can't extend it) and `schedule_to_close_timeout_ms` (whole step; a retry is planned
  only if its backoff ends before the deadline) on `schedule_activity`. Durations recorded next
  to the event they start from; new `deadlines` timer service; recovery re-arms. New fold rule:
  a final `StepFailed` may close a step between retries. Every SDK, testsuite, docs.
- #36 **Cancellation** (ADR-0009): `CancelWorkflow` (gRPC, `POST /api/workflows/{id}/cancel`, Go
  client, TS/Python/Java `worker.cancel`, dashboard Cancel button) → `WorkflowFailed{cancelled}`,
  status `CANCELLED` (enum 7). `cancel_step` command → `StepFailed{cancelled}`, step status
  `CANCELLED` (Go `ctx.CancelStep`, TS `ctx.cancel` / `waitAny(..., { cancelRest })`, Python
  `future.cancel()` / `wait_any(cancel_rest=)`). Lease tombstones tell workers "cancelled"
  (409 `"cancelled": true`, gRPC `ActivityCancelled` detail). A finished workflow's open steps
  are presented as `CANCELLED`. Reproduced liftoff's race bug, including an abandoned signal wait
  swallowing a later signal.
- #37 **Non-retryable failures** (ADR-0010): `error_type` + `non_retryable` on activity
  completion and `StepFailed`; `RetryPolicy.non_retryable_error_types`; engine types its own
  failures (`LeaseExpired`, `StartToCloseTimeout`, `ScheduleToCloseTimeout`); one decision in
  `retryAfter`. SDK error type = class name; `NonRetryableError` (TS/Python),
  `NonRetryableException` (Java), `worker.NonRetryable` / `worker.Typed` (Go); `ActivityError` /
  `ActivityFailure.type`. SDKs resend without the new fields on `400 unknown field`.
- #38 **TypeScript ergonomics**: implementation in `workflowd.cjs`, `workflowd.mjs` re-exports it
  (one module for `import` and `require`; `check-package.sh` in CI and release); `ctx.now()`
  (results are a `Promise` subclass whose `then` moves the clock), `ctx.random()` (sfc32 seeded
  by workflow id + start), `ctx.sleepUntil`, `waitForSignal(..., { timeoutMs })`; activity tasks
  carry `attempt` + `activity_id`; workflow tasks `started_at` + `resolved_at`. Dashboard: error
  type tag, future times "in …".
- #39 release 0.2.0: npm run 39923818020 (dry run 39923737509 first), PyPI run 39923903841,
  tag `sdks-v0.2.0` @ `81c98f8`. Verified by clean installs (npm via `require`, PyPI in a venv)
  running workflows on the public demo.
- **Deployed `81c98f8`** to the demo (site too) and to the private `workflowd-liftoff` engine
  (its binary had to change: SDK 0.2.0 needs `started_at`, `cancel_step` and the new fields);
  fresh backups taken first; liftoff's worker re-registered in 200 ms.

**Bugs found on the way:** Go `ctx.Now()` moved to a marker's time on replay but not on the first
run (markers no longer move the clock); the dashboard showed future times as "0s ago".

**Teeth:** 35 mutations across the four PRs, all caught; one first survived (the replay's local
"already cancelled" mark only saves a round trip) and is now pinned by a replay-level test.

**Next:** liftoff adopts 0.2.0 (its own repo): `require`, `ctx.now()`, `NonRetryableError` for
4xx, `waitForSignal` with `timeoutMs`, `worker.cancel` on request cancel. Graceful cancellation
handlers; `ctx.now()`/`random` in Python and Java; Maven Central (owner).

**Gotchas:**
- No JDK on the laptop: Java compiles only in CI. #35 first failed on `activity(name, input,
  null)` becoming ambiguous between the `RetryPolicy` and `ActivityOptions` overloads.
- `scripts/sdk-version.py` reads the TS version from `workflowd.cjs` now (the `.mjs` is a wrapper).
- Rollback past this release: older binaries refuse a history where a step between retries was
  closed by a deadline or cancel; they read cancelled/timed-out workflows as FAILED. Check the
  data dir first. `workflowd.prev` on both instances is the `9f19ab8` build.
- `await` adopts a native Promise without calling an overridden `then` on the instance; only a
  subclass instance goes through `then`. That is why `Observed extends Promise`.
- The browser pane's `preview_start` reads the main checkout's `.claude/launch.json`, not a
  worktree's; the shell sandbox here can't reach a local preview port (seed it from the page).

---

## 2026-10-22 — Session 25: the SDKs are published (npm + PyPI, trusted publishing)

**Done:**
- **npm `workflowd`** 0.1.0 (hand upload: a brand-new npm package can't be created from
  CI) and 0.1.1 (from `release-sdks.yml`). **PyPI `workflowd`** 0.1.0 and 0.1.1, both from CI.
  Tags `sdks-v0.1.0`, `sdks-v0.1.1`. Every release verified by a clean install that ran an
  OrderSaga on the public demo.
- **Trusted publishing on both registries (no tokens anywhere).** PyPI: a pending GitHub
  publisher for this repo + `release-sdks.yml`, job uses `pypa/gh-action-pypi-publish` with
  `id-token: write` (#28). npm: a GitHub Actions trusted publisher with "Allow npm publish",
  job installs npm 11.5.1+ on Node 24 (#32). 0.1.1 (#33) was cut to validate npm's config
  before its 48-hour deadline; npm now shows it "Valid", and the publish carries signed
  provenance in Sigstore's transparency log.
- **Release-guard bug (#28):** `sdk-version.py --release` matched the substring
  `## [0.1.0]`, which the changelog's own instructions contained, so an undated release
  passed. A real publish attempt got through it (stopped later only by missing secrets). It
  now requires a heading at the start of a line.
- **Flaky test (#29):** `TestReplayWorkflowSurvivesRestart` (1 in 10 under `-race` load)
  polled for a pause on a 40 ms timer and could miss it. Test-only fix: a 750 ms cooldown.
- Docs: READMEs and CHANGELOG track what's published (#30, #31).

**Next:** Maven Central for the Java SDK (owner: GPG key + Sonatype account). Optional:
npm staged publishing (npm calls direct publish from CI "not recommended"), and npm's
"disallow bypass-2FA tokens" setting.

**Gotchas:**
- Account creation, sign-in, 2FA setup and handling tokens/keys stay with the owner even
  with permission. Claude filled the trusted-publisher forms in his signed-in Chrome.
- npm 11.6.2 crashed ("Exit handler never called!") when Enter was pressed at its "open in
  browser" prompt; cmd-click the printed link instead.
- Reloading a page while npm's security-key prompt is up cancels the change.
- A local `npm publish` must copy LICENSE + NOTICE into `sdks/typescript` first (CI does).
- Don't run the release with `registries=all` until Maven secrets exist: the maven job
  fails. Run `npm` and `pypi` separately.
- npm showed a placeholder `0.0.0-stage` for about a minute after the first publish.

---

## 2026-10-21 — Session 24 (engine backlog): SDK features, per-workflow locking, index, schedules, parallel steps, exactly-once signals

Run in parallel with Session 23 (security/CI/packaging). PRs #19, #22, #23, #24, #25,
#26 and this docs PR, each squash-merged after green CI; nothing deployed (the parent
session deploys).

**Done:**
- #19 SDK protocol: `ScheduleActivityCommand.retry_policy` recorded on `StepScheduled`
  (overrides the engine's by-name policy, survives restarts; Go `workflow.WithRetryPolicy`,
  honoured by `testsuite`); `record_marker` + marker steps over HTTP, so TS/Python/Java
  have `getVersion`/`sideEffect`; `<field>_base64` payload twins (only for non-UTF-8
  bytes); `POST /api/activity-tasks/heartbeat`; TS/Python/Java follow 503 leader hints
  over several addresses, re-register after any redirect/restart, heartbeat, JSON/bytes
  helpers. `sdks/sdk_e2e_test.go` drives TS and Python end to end from `go test`.
- #22 Per-workflow locks + in-memory state cache (`wfstate.go`, `state.Folder`); store
  lock not held across write/fsync/read; data-driven definitions decide from the cached
  state. Bench: 1000-step workflow 2253 → 49 µs/step; 64 concurrent workflows 243 → 59
  µs/step (no fsync), 47 → 25 ms/step (macOS full fsync).
- #23 Visibility index (`index.go`), ADR-0006 (stay on files; measured: recovery 0.83 s
  at 10k, 5.9 s at 50k; durable append 20 ms vs 10.6 µs buffered). List 734 → 0.75 ms.
- #24 Cron schedules: `internal/cron` (hand-written), `CreateSchedule`/`DeleteSchedule`/
  `ListSchedules` on gRPC, HTTP, Go client; stored in the reserved
  `__workflowd-schedules` history using existing event types; run id = schedule id +
  fire time; 24 h catch-up; leader-only; skip-if-running.
- #25 Parallel steps (ADR-0007): multi-open history, `StepFailed.will_retry`, decide
  after every result, batched `commands`, leases per (workflow, sequence), recovery of
  every open step; Go futures + `WaitAny` by `ResolvedEventID`; TS batching by
  quiescence + `ctx.waitAny`; Python `*_async` + `wait_all`/`wait_any`; `testsuite`
  rewritten as an event-driven simulator.
- #26 Exactly-once signals: `request_id` on `SignalWorkflow`, recorded on
  `SignalReceived`, checked before recording (also after the workflow finished).
- Sticky queues closed with measurements (`BenchmarkReplayDecision`: 74 µs at 100 steps).

**Teeth:** every load-bearing claim has a mutation that fails its test: recorded policy
dropped; no re-registration / ignored leader hint / no heartbeat (TS, Python); no
per-workflow lock, unfolded append, cache kept after a failed append, one global lock,
no eviction; index not updated on append/purge/failed append/recovery; non-deterministic
run ids, schedules not loaded, no catch-up, no skip; one lease per workflow, recovery of
only the latest step, retrying step treated as failed, `WaitAny` by code order, no
`will_retry`, recovery deciding only when RUNNING, SDK sending only the first command;
no signal dedup, a new key per client attempt.

**Bugs found on the way:** a closed engine still accepted writes (a "crashed" engine's
workers reported to it after a restart): `appendLocked` now refuses with
`ErrEngineClosed`. TS e2e raced its own registration
on CI (fixed in #23).

**Next:** publishing the SDKs (owner); continue-as-new; group commit if write rates
need it; parallel steps in Java; nginx: add `POST /api/activity-tasks/heartbeat` to the
demo's rate-limit exemption.

**Gotchas:**
- Rollback: binaries before #25 can't read a history with two steps in flight; before
  #19 they ignore SDK retry policies; before #26 they record retried signals twice.
  Schedules use only existing event types (an older binary lists the schedule history as
  one RUNNING workflow).
- The engine must never hold a workflow lock while waiting on the index build: Recover
  builds the index, so schedule catch-up runs after it (`recoverWorkflows` then
  `recoverSchedules`).
- Tests that watch the files (`waitForState`) can see an event a moment before the index
  does; wait through the engine (`GetWorkflow`) when comparing.

---

## 2026-10-21 — Session 23 (security, CI, packaging): TLS + auth everywhere, buf, SDKs in CI, publish-ready packages

Run in parallel with the engine-backlog session (PR #19 and later). PRs #16, #17, #18,
#20, all squash-merged; nothing deployed (the parent session deploys once both have merged).

**Done:**
- #16 `scripts/sdk-e2e.sh <sdk>` + CI job `sdk-e2e` (typescript/python/java): build the
  engine, run the SDK's example worker, drive an approved order (COMPLETED,
  `shipped(reserved(<id>))`) and a rejected one (FAILED after exactly one `release`).
  `sdks/java/Example.java`. **The Java SDK compiled for the first time with no code
  changes** (`javac --release 11 -Xlint:all,-serial -Werror` on JDK 21).
- #17 buf: `buf.yaml` (v2, module `proto/`), lint `STANDARD` minus
  DIRECTORY_SAME_PACKAGE / PACKAGE_DIRECTORY_MATCH, and RPC_REQUEST/RESPONSE_STANDARD_NAME
  ignored for `raft.proto` only; breaking `WIRE_JSON`. CI job `proto` (buf v1.73.0 from the
  release, sha256-checked) runs lint + `buf breaking` against the PR base sha.
  `make buf` / `make proto-check`. ADR-0002 follow-up closed.
- #18 auth + TLS, all opt-in (no flags = exactly as before): `--tls-cert/--tls-key` on
  gRPC, HTTP and metrics (`internal/tlsconfig`, cert reloaded on mtime change, last good
  cert kept); `--auth-token-file` / `$WORKFLOWD_AUTH_TOKEN` now guards gRPC too
  (`server.GRPCAuth` interceptors); `--auth-public-reads`; `--metrics-auth-token-file` /
  `$WORKFLOWD_METRICS_AUTH_TOKEN`; `--raft-tls-cert/key/ca` mutual TLS (`cluster.Config.PeerTLS`).
  Go `client.Options{TLS, AuthToken}`, `client.ErrUnauthenticated`, `worker.Run` stops on a
  rejected token. TS `token`, Python `token`/`ca_file`, Java 4-arg `Worker` + `trustPem`.
  Dashboard prompts for the token on 401 (sessionStorage). CI runs every SDK a second time
  over TLS with a required token (`E2E_SECURE=1`).
- #20 packages, not published: `sdks/typescript/package.json` + `workflowd.d.mts` (tsc-checked
  by `types-check.mts`), `sdks/python/pyproject.toml`, `sdks/java/pom.xml` (Java class now in
  `package io.github.olayanju1234.workflowd`; the file did not move, Maven stages it),
  `CHANGELOG.md`, `scripts/sdk-version.py` (six version strings agree), CI job
  `sdk-packages`, manual `.github/workflows/release-sdks.yml` (dry run by default). Owner's
  publish steps: `sdks/README.md` → "Publishing the SDKs".

**Tests:** gRPC auth matrix (5 methods × 4 bad headers + valid), public reads (HTTP + gRPC),
metrics guard, token sources (file/env/argv/conflicts), HTTPS listener refuses plaintext,
tlsconfig (trust, mTLS rejects no cert and a rogue CA, reload, bad files), a 3-node cluster
over mTLS whose Raft port refuses plaintext/cert-less/rogue callers, Go client over TLS+token,
worker with a bad token. `buf breaking` checked by hand to fail on a `bytes`→`string` change.

**Next:** owner publishes the SDKs (secrets + `gh workflow run release-sdks.yml`), after
renaming `## [Unreleased]` to `## [0.1.0] - <date>`. Per-user identities/RBAC stay with the
commercial layer. Raft peer certs aren't bound to node ids.

**Gotchas:**
- The old `--auth-token` already covered reads (the docs said "mutating routes"). So the
  "read auth" option is the inverse, `--auth-public-reads`. Polls are `GET`s but lease work,
  so they are never public.
- Behaviour change: `--auth-token` now also guards gRPC. Prod sets no token.
- `.mjs` needs `.d.mts` (TypeScript pairs them); a self-import (`from 'workflowd'`) in
  `types-check.mts` also tests the package.json `exports`.
- The Java file must not move while #19 rewrites it; `pom.xml` copies it into
  `target/sdk-src/io/github/olayanju1234/workflowd/` in `generate-sources`.
- macOS bash is 3.2: empty arrays under `set -u` need `${a[@]+"${a[@]}"}`.
- No JDK on the laptop: Java is verified in CI only.

---

## 2026-10-21 — Session 22: the public Go SDK — Phase 5 complete

Weeks 15–17, PRs #13 (Wk15), #14 (Wk16) and #15 (Wk17). Wk15+16 deployed together as
`14a1fa0` (single-node; recovery replayed 11 workflows, smoke OrderSaga completed).

**Done:**
- ADR-0005: public packages `workflow`, `client`, `worker`, `embedded` (+ `testsuite`).
  Generated protobuf stays internal: the public packages live in the module, so they
  may import `internal/server`; user code imports only them.
- `internal/replay`: the replay engine moved out of `sdk`. Each decision runs the
  workflow on its own goroutine and suspends with `runtime.Goexit`, which `recover()`
  can't stop; a panic becomes `*PanicError`. `ctx.Now()` = timestamp of the latest
  event replayed past (UTC); `ctx.Logger()` drops records while replaying.
- `client`: `Dial`, `StartWorkflow` (client-generated ids, so `AlreadyExists` on a
  retry counts as success), `SignalWorkflow` (at-least-once across a leader change),
  `TerminateWorkflow`, `DescribeWorkflow`, `WaitForResult` (waits through outages),
  sentinels. `internal/rpc`: per-node connections; `Unavailable` + leader trailer →
  hinted node, otherwise round-robin with backoff (~5 s budget); an epoch counter.
- `worker`: registers types, polls workflow tasks and activities (`step_names`),
  heartbeats at lease/3, re-registers whenever the connection epoch moves;
  `Start`/`Stop` (returns once registered) and `Run`; `GetActivityInfo`.
- `embedded`: real engine on a data dir over bufconn. `sdk`: deprecated aliases.
- Wk16: `MarkerRecordedEvent` (history oneof 19) + `RecordMarkerCommand` (oneof 6);
  `GetVersion` (consumes a sequence only when a version marker is there or being
  recorded; old executions get `DefaultVersion`) and `SideEffect`. `testsuite`:
  mocked activities, virtual time, signals in virtual time, `ErrBlocked`, replays
  every decision. Dashboard glyph for MARKER.
- Wk17: three examples (`hello`, `checkout`, `onboarding`), each tested against an
  embedded engine and in `testsuite`; README rewrite; quickstart timed at 42 s from a
  fresh clone with empty Go caches; ARCHITECTURE, docs.html, index.html, sdks/README;
  launch blog **draft** at `docs/blog/2026-10-launch-draft.md` (not published).

**Engine bugs found by the SDK work (both also hit TS/Python):**
- Recovery, timers, signals, `CompleteTask` and lease expiry bailed out when the
  workflow's type wasn't registered. SDK types register after recovery, so a remote
  workflow mid-activity or mid-timer at a restart/leader change was stranded. Now
  outcomes are recorded without a planner; only deciding waits (`advanceIfRegisteredLocked`).
- A signal landing while an SDK decision was outstanding made it stale (correct) but
  nothing re-dispatched, stranding the workflow. `CompleteWorkflowTask` now
  re-dispatches. Found on CI: the e2e test hung in 9/20 runs without the fix.

**Teeth:** panic-based suspension → `TestSuspensionCannotBeRecovered`; register-once or
signal gated on registration → restart test; ignoring leader hints →
`TestSDKFollowsLeaderAcrossFailover` (client seeded with one follower, real 3-node
Raft, leader killed mid-activity); `GetVersion` returning max on old histories →
`TestOldExecutionReplaysOldBranch`.

**Next (backlog):** SDK-side retry policies; `GetVersion`/`SideEffect` and leader
hints in TS/Python; signal request ids (exactly-once signals across failover);
parallel steps; replaying an exported history against new code; TLS/auth; per-workflow
locking; brand + module path decision.

**Gotchas:**
- Cluster test ports must be reserved with listeners held open; listen-and-close let
  n1's gRPC port come back as n2's Raft port on CI.
- `time.Unix` returns local time: workflow time must be `.UTC()` or workers in other
  zones replay different values (caught by a testsuite test).
- Rollback across this release: older binaries can't read `MarkerRecordedEvent`, and
  `Recover` fails on an unreadable history. Prod has none (demo workflows don't use
  markers; gRPC is not public).

---

## 2026-10-21 — Session 21: Raft, part 4 (the engine on Raft) — Phase 4 complete

Week 14 of Phase 4, PR #12. Week 13 (PR #11) merged as `3998bc7` and deployed
(single-node; recovery replayed 7 workflows, smoke OrderSaga completed).

**Done:**
- `internal/server`: `HistoryLog` seam (`WithHistoryLog`; default = the store, so
  single-node is unchanged), `EngineSource` routing for gRPC (`NewGRPCServerFrom`)
  and HTTP (`WithEngineSource`, `WithClusterStatus`), `ErrNotLeader` /
  `NotLeaderError{LeaderHint}`: gRPC `Unavailable` + `workflowd-leader[-grpc|-http]`
  trailers, HTTP `503` + JSON + `X-Workflowd-Leader` (or `Retry-After`). Health is
  served by every node. `Engine.Close` ends long polls (`ErrEngineClosed`).
- `proto/event.proto`: `HistoryEvent.raft_index` (optional; zero in single-node).
  `proto/cluster.proto`: `Command{AppendEvent|DeleteWorkflow}`, snapshot records.
- `internal/cluster`: the history state machine (stamps + event-id fence,
  idempotent replay, deletes vs reused ids, snapshot = raw history files, restore
  via `HistoryStore.ReplaceAll`, `raft/applied-snapshot` marker), the term-fenced
  `replicatedLog`, and `Cluster`: per-term engines started after `LeaderReady` +
  `Recover()`, stopped on leadership loss; leader hints learned from gRPC metadata
  on Raft traffic; Raft gauges on `/metrics`; refuses a single-node data dir.
- `internal/raft`: `StateMachine.Apply` returns a fatal error; transport takes
  extra dial options; the leader replicates while writing its own copy (§10.2.1,
  `Ready.LeaderAppend`); `UnsafeDisableFsyncForTests` (chaos tests only).
- `internal/history`: `ReadRaw`, `ReplaceAll`, `AppendBuffered` + `Sync`
  (cluster mode writes history without per-event fsync; `Sync` before a snapshot
  marker). `internal/record`: `WriteFile`/`ReadFile`/`ReplaceFile`/`SyncDir`,
  replacing raft's private copies.
- `workflowd --serve --node-id --peers` now serves the full API in cluster mode;
  demo worker and retention run per leadership term. `make cluster` adds
  `--demo-worker`.
- Engine fix found by the chaos test: a long poll whose deadline expired just as
  a task arrived leased the task to a departed worker (stranded until lease
  expiry, a failed attempt). `PollTask`/`PollWorkflowTask` now re-queue the task
  if the poller is gone (checked under the engine lock).
- Tests: routing (gRPC/HTTP hints, health, poll on closed engine, writes via
  `HistoryLog`, mid-flight leadership loss, departed poller); state machine
  (stamps, fence, idempotent replay, delete + reuse, snapshot round trip, restore
  skip, lost buffered writes restored by replay); in-process 3-node cluster
  (byte-identical histories, follower hints, failover mid-workflow, replicated
  retention, catch-up from history snapshot, deposed leader stops serving,
  rejection reaches the engine, bootstrap refusal); real-process chaos
  (`cmd/workflowd/chaos_test.go`: re-exec'd nodes, TCP proxies per Raft link):
  kill leader mid-workflow, partitioned leader cannot commit, 100 workflows
  across a leader kill. Full suite stable 6/6 runs in parallel under `-race`.
- Mutation-verified: no barrier, never stopping the engine, skipping Recover, no
  event-id fence, ignoring rejections, leasing to a departed poller.
- Raft fix found only on CI: commits on a new leader took ~1 s each. Each
  heartbeat re-sent all unacknowledged entries and each lagging ack triggered
  another send; on a loaded runner the duplicates snowballed until the 1024-slot
  peer queue was full (960 ms ≈ 1024 × 0.94 ms). Now pipelined: `next` advances
  past what was sent, a lost message is detected by the follower's rejection of
  the next one (per-peer delivery is FIFO), a 1024-entry in-flight window, and a
  256-slot peer queue. `TestHeartbeatsDoNotResendInflightEntries` (sent 11 times
  in 10 heartbeats before the fix) and `TestLostPipelinedEntryIsResent`.
- Diagnostics: Raft warns on slow commits (with per-follower progress), slow
  persist-and-send rounds and slow peer sends; failing chaos tests SIGQUIT the
  nodes and print workflowd goroutine stacks.

**Live check:** `make cluster`: follower `503` with `X-Workflowd-Leader`; leader
SIGKILLed during an OrderSaga's durable timer; the new leader served 1.2 s later
and completed the saga 5.8 s after the kill; survivors' history files identical.

**Measured:** macOS fsync (`F_FULLFSYNC`) ≈ 21 ms; three nodes on one disk made a
commit ≈ 117 ms (six fsyncs). Parallel leader write + buffered history: 60–80 ms;
Raft-log fsync off: ≈ 1 ms (100 workflows in 6.3 s).

**Next:** Phase 5 (public Go SDK, `workflow.Version`, quickstart). Cluster
follow-ups: PreVote, SDKs following leader hints, per-workflow locking for write
throughput, follower reads, TLS/auth on the Raft port, single-node → cluster
import, chunked snapshots, a cluster section in `web/site/docs.html`.

**Gotchas:**
- Engine throughput in a cluster = one commit per history write under the global
  lock. The 100-workflow test submits from 4 clients; 100 simultaneous submissions
  on a shared laptop disk queued completions past the 30 s lease.
- The chaos test's failures under parallel load were (1) the departed-poller bug,
  (2) 200 ms election timeouts in the in-process/gRPC tests (now 500 ms–1 s),
  (3) on CI only, the duplicate-message snowball above. Reproduce slow-machine
  bugs with data from the failing machine (logs, goroutine dumps), not guesses.
- Chaos node processes call `raft.UnsafeDisableFsyncForTests()`: they kill
  processes, not machines, so the page cache survives. Never call it elsewhere.
- `go test -run` in a re-exec'd child must target only the helper test, or the
  child re-runs the whole suite.

---

## 2026-10-21 — Session 20: Raft, part 3 (safety properties + snapshots)

Week 13 of Phase 4, PR #11. Week 12 (PR #10) merged as `19ad765` and deployed
(single-node; recovery replayed 6 workflows, smoke OrderSaga completed).

**Done:**
- Safety checker covers all five Figure 3 properties: Election Safety, Leader
  Append-Only (a leader's log copy must stay intact while it leads), Log Matching
  (pairwise, every 10 random steps), Leader Completeness (a new leader holds
  every entry ever committed), State Machine Safety (applies and snapshot
  restores).
- `testingKnobs` in the core (zero value = correct): `grantStaleVotes`,
  `skipPrevTermCheck`, `countOldTerms`. Permanent teeth tests: stale votes →
  Leader Completeness; skipped term check → Log Matching; Figure 8 with
  `countOldTerms` → Leader Completeness then State Machine Safety; a direct
  leader rewrite → Leader Append-Only; Session 18's forgotten vote → Election
  Safety.
- Snapshots: `InstallSnapshot` in the proto; core compaction (`compact`),
  leader sends its snapshot when a follower needs compacted entries (re-sent
  after 2 election timeouts, probes keep the follower's timer alive), follower
  keeps a matching suffix or replaces its whole log (Figure 13). `Ready.Snapshot`.
- `DiskStorage`: `snapshot` file (meta + data records, atomic), `compactLog`
  (atomic rewrite), `resetLog`; `load` skips entries a snapshot covers and resets
  a log that doesn't continue it (crash mid-install).
- `Node`: `StateMachine` gains `Snapshot`/`Restore`; restore at startup; leader
  snapshots installed in order on the apply goroutine; local snapshot every
  `SnapshotEvery` (default 8192) applied entries; run loop drains its inbox
  before each flush (one fsync per burst). gRPC message limit raised to 65 MB
  (`raft.ServerOptions()`).
- ADR-0004: replicated history + leader-only engine (what is replicated,
  apply-time stamps and fence, per-term engines, client routing, snapshot format,
  bootstrap rule, at-least-once in a cluster).
- Tests: snapshot unit tests (stale duplicate ignored, matching suffix kept,
  conflicting log replaced), lagging follower via snapshot, restart from own
  snapshot, random snapshots in the chaos runs (≈720 compactions and 237 whole-log
  installs across 200 seeds), snapshot storage crash windows, real gRPC catch-up
  from snapshot and a 5 MB snapshot.

**Next:** Week 14, wire the engine onto Raft per ADR-0004, plus real-process
chaos tests.

**Gotchas:**
- Mutation "reinstall a snapshot we're already past" first failed nothing. A
  delayed duplicate InstallSnapshot would wipe acknowledged entries;
  `TestStaleSnapshotIgnored` now pins it.
- A slow follower (fsync per message, several ms on macOS) made the leader fail
  its quorum check under a 100 ms test timeout. Draining the inbox before
  flushing fixed the cause; the gRPC tests also use 20-tick elections now.
- The 5 MB snapshot test took 30 s only under `-race` (1.5 s without): the race
  detector slows large copies ~20x. Measure before optimizing.
- `countOldTerms` never fires in random schedules (the opening empty entry
  makes Figure 8 rare); it is covered by the deterministic Figure 8 schedule.

---

## 2026-10-21 — Session 19: Raft, part 2 (log replication)

Week 12 of Phase 4, PR #10. Week 11 (PR #9) merged as `4eb0828` and is deployed
(single-node; recovery replayed 5 workflows, smoke OrderSaga completed in 8.3 s).

**Done:**
- Core: `propose(term, data...)` (term-fenced: refused unless this node leads
  exactly that term), `AppendEntries` carrying entries (≤256 or ~1 MB per
  message), the prevLogIndex/prevLogTerm consistency check, truncation only at a
  real conflict, follower commit capped at the last new entry, conflict hints that
  skip a whole term, commit on a majority counting only current-term entries (the
  leader counts itself only for entries already on its disk), and an empty entry
  appended by every new leader (`Status.LeaderReady()` once it is applied).
- `Ready` grows `Entries` (persist, replacing any suffix from `Entries[0].Index`)
  and `CommittedEntries` (apply).
- `DiskStorage` gains the log file (`raft/log`): CRC-framed `LogEntry` records,
  in-memory offsets for suffix truncation, torn tail cut on open, one write +
  fsync per batch, refuses gaps.
- `Node`: persist hard state, then entries, then send, then queue committed
  entries for a separate apply goroutine (slow applies never delay heartbeats).
  `Propose(ctx, term, data)` batches queued proposals into one append and waits
  for this node's apply. Errors: `ErrNotLeader` (refused), `ErrProposalDropped`
  (overwritten), `ErrLeadershipLost` (outcome unknown). `StateMachine.Apply`.
- `workflowd` cluster mode: a discard state machine; `/api/health` adds
  `commit_index` / `applied_index`.
- Tests: sim harness persists entries and applies committed ones; the checker
  adds State Machine Safety; replication scenarios (commit, catch-up past the batch
  limit, minority leader can't commit, paper Figure 7 convergence, Figure 8,
  stale append, commit cap, conflict hint, full restart replay); randomized chaos
  now proposes on every self-believed leader and demands identical applied
  sequences after settling; log-file storage tests; real gRPC: 200 concurrent
  proposals, follower catch-up, leader killed mid-stream, full restart.
- Teeth-verified by mutation: skipped consistency check, truncate on match,
  uncapped follower commit, counting old-term entries, vote without log check.

**Live check:** `make cluster`, kill the leader: commit/applied go from 1 to 2
as the new leader commits its opening entry (about 1.3 s).

**Next:** Week 13, the five safety properties as a randomized checker with
teeth knobs, then snapshots + log compaction + InstallSnapshot. ADR-0004 before
Week 14.

**Gotchas:**
- My first Figure 8 test ignored the leader's opening empty entry, so indexes
  were off by one and it passed with the bug reinstated. Mutation testing caught
  it. Every safety test now has a recorded mutation that turns it red.
- `deliverAll` caps deliveries at 100,000: a livelock bug used to hang the run
  for the 10-minute test timeout; it now fails in 0.1 s.
- macOS has no `timeout` command; bound test runs with `go test -timeout`.
- Messages must not alias the log's backing array (`slices.Clone`), or a later
  in-place conflict repair could rewrite a queued message.

---

## 2026-10-21 — Session 18: Raft, part 1 (leader election)

Phase 4 restarted in a side session that runs all four Raft weeks, one PR each.
Week 11 is PR #9.

**Done:**
- `internal/record`: the `[len][CRC32C][payload]` framing moved out of
  `internal/history` so the Raft files can share it. History keeps `headerSize`
  and `ErrCorruptRecord`; the on-disk format and every history test are unchanged.
- `proto/raft.proto` reshaped: one one-way RPC, `Send(Envelope)`, where every
  request and every response is its own message. `PersistentState` became
  `HardState` (term + vote); the log gets its own file in Week 12.
- `internal/raft` core (`core.go`): a deterministic state machine (tick / step ->
  `Ready{HardState, Messages}` -> advance). Randomized election timeouts in
  [electionTick, 2x), RequestVote with the up-to-date log check, one vote per
  term, heartbeats, the higher-term-wins rule, CheckQuorum (a leader that can't
  hear a majority steps down) and paper §6 vote suppression (a follower that
  heard a live leader within the minimum timeout ignores vote requests).
- `Node` (driver): one goroutine owns the core and persists each Ready's hard
  state (atomic temp + fsync + rename + dir fsync) before sending its messages. A
  failed persist stops the node (fail-stop). `GRPCTransport`: one goroutine and a
  1024-message drop-when-full queue per peer, reconnect backoff capped at 1 s.
- `workflowd --serve --node-id --peers [--raft-addr] [--raft-tick]` runs a cluster
  member. **Raft only for now**: no workflow API in cluster mode until Week 14;
  `/api/health` reports role/term/leader. `make cluster` (`scripts/cluster.sh`)
  starts n1–n3 on localhost.
- Tests: simulated in-memory cluster (`sim_test.go`, seeded, no clock/goroutines)
  with a safety checker; scenario tests (one leader, stability, re-election,
  partitioned leader steps down, split vote); core-rule tests (up-to-date log,
  one vote per term, lease suppression, term rules); 200 random chaos schedules
  (reorder/drop/duplicate/crash/partition) checking election safety plus
  liveness after healing; `TestForgottenVoteElectsTwoLeaders` (permanent teeth
  test); hard-state storage tests; a real 3-node gRPC cluster over loopback
  (elect, stop leader, fail over, restart from disk).
- Teeth-verified by hand: removing the up-to-date check, CheckQuorum, vote
  suppression, the one-vote rule, or the stale-term rule each fails a test.

**Live smoke test:** `make cluster` elected n1 in term 1; SIGKILL on n1 gave n2
leadership in term 2 about 1.06 s later; n1 restarted with term 1 / vote n1 from
disk and followed n2.

**Next:** Week 12, log replication (AppendEntries with entries, commit on
majority, conflict repair, durable log file).

**Gotchas:**
- gRPC's default reconnect backoff grows to 2 minutes; Raft needs peers heard
  within an election timeout, so the transport caps it at 1 s.
- Go randomizes map iteration. The core keeps peers in a sorted slice and never
  iterates a map where order is observable, or seeded runs would not replay.
- A partitioned node that rejoins with a higher term still costs one election
  (its reply to a heartbeat carries the higher term). PreVote would fix it;
  deferred.
- The randomized test with a 10-tick election timeout saw under 2 elections per
  run; 3 ticks gives about 9. Measure coverage, not just pass/fail.

---

## 2026-10-21 — Session 17: Drop the append handle when a write fails

Side session to close the torn-write bug found in Session 16. PR #7, merged and
deployed (`966b991`).

**Done:**
- `HistoryStore.Append` now releases the cached handle when the write or the fsync
  fails, through a new `releaseLocked` helper (the caller already holds `s.mu`, and
  Go mutexes aren't re-entrant). `Release` and `Delete` share the helper. The
  original error is still returned. The next `Append` reopens through `fileLocked`,
  which truncates the partial record before writing.
- Short writes need no extra check: `os.File.Write` documents and enforces a non-nil
  error whenever `n != len(b)` (it falls back to `io.ErrShortWrite`).
- Test seam: `HistoryStore.writeFile` (default `(*os.File).Write`).
  `TestAppendAfterFailedWriteRecovers` has two cases: a real half-record write
  followed by `syscall.ENOSPC`, and stray tail bytes plus the cached fd closed
  underneath the store (a real `os.ErrClosed`). Before the fix, the ENOSPC case
  read back `record at offset 57: corrupt record (CRC mismatch)` and the second
  case failed every retry with `file already closed`.

**Prod verification:** fresh backup `data-20261021T070259Z.tar.gz` before the restart;
`workflowd.prev` is now the `4c8b42a` build. `/api/health` reports `966b991`, recovery
replayed 4 workflows, the site and dashboard URLs all return 200, and the 3 listeners are
on 127.0.0.1. A smoke OrderSaga completed (10 events), holding 1 `.history` fd while
live and 0 afterwards.

**Next:** `--retention` in prod still awaits the owner's OK. Then Phase 4 / Week 11, Raft.

**Gotchas:**
- After a failed fsync the record is already complete in the file, so the reopen keeps
  it: `Append` returned an error but the event is visible to the next `ReadAll`. The
  engine re-reads history per op, so it treats the event as committed. This is unchanged
  from before.
- Before this fix, a restart could heal a torn tail only if it happened before the next
  append. Once a retry landed after the garbage, `recoverTornTail` hit a mid-log CRC
  error and refused to truncate, so the workflow was unreadable for good.

---

## 2026-10-21 — Session 16: Release history handles when workflows finish

Side session spun off from Session 15 to close the fd-leak backlog item. PR #5,
merged and deployed (`4c8b42a`).

**Done:**
- `HistoryStore.Release(id)` closes and forgets the cached append handle and leaves the
  file alone. A later `Append` reopens it, and torn-tail recovery runs again on that
  reopen. `OpenHandles()` reports the cache size (used by tests).
- `Engine.appendLocked` releases after a terminal event (`WorkflowCompleted` /
  `WorkflowFailed`) is durable. That one choke point covers decision complete/fail
  (Go and polyglot), `TerminateWorkflow`, and workflows that finish during `Recover`.
  A close error is logged, not returned: the event is already on disk.
- `Recover` needed no change: it reads finished workflows with `ReadAll` (a
  short-lived handle) and never caches. `TestRecoverHoldsNoHandlesForFinishedWorkflows`
  locks that in.
- Tests: `TestReleaseThenAppend` (history), `TestFinishedWorkflowReleasesHandle` and
  the recover test (server). Both server tests fail with the release call disabled.

**Prod verification:** before, 3 finished workflows and 3 `.history` fds open in the
process. After deploy, a fresh OrderSaga (with one charge retry) held 1 fd while live
and 0 once COMPLETED. Fresh backup `data-20261021T065202Z.tar.gz` taken before restart;
`workflowd.prev` is now the `9b1f777` build.

**Next:** fix torn writes on a live handle (below). `--retention` in prod still awaits
the owner's OK. Then Phase 4 / Week 11, Raft.

**Gotchas:**
- New bug found, not fixed: if `Append`'s write fails partway (ENOSPC, EIO), the
  partial record stays at the tail and the handle stays cached, so the next append
  lands after it and `ReadAll` then hits a CRC/length error (hard corruption). Tail
  recovery only runs on open. Likely fix: `Release` the handle on any write/fsync error
  so the next append reopens and truncates. A restart before the next append also heals it.
- The process's open-files soft limit on the VPS is 524287, so the old leak was slow;
  count fds with `ls -l /proc/$(systemctl show -p MainPID --value workflowd)/fd`.
- `GET /api/workflows/{id}` returns the summary fields flat (`status`, `history_length`),
  not under `summary` like the gRPC response.

---

## 2026-10-21 — Session 15: Unsticking the public demo + ops hardening

First session driven by a production review of the VPS (see the handoff in the local,
git-ignored `CLAUDE.local.md`). Also the first session to use branches + PRs (#1–#3)
instead of pushing to `main`. All three merged and deployed (`9b1f777`).

**Done (code):**
- **PR #1, housekeeping.** SPA fallback serves only GET/HEAD (405 otherwise; scanner
  `POST /graphql` used to get 200 + HTML). Unmatched `/api/*` → JSON 404. Dashboard
  `http.Server` now drained on SIGTERM; request contexts come from the signal context
  (`BaseContext`) so SDK long-polls end with 204 immediately. Fixed the
  `url=http://localhost127.0.0.1:8233` log line (`browseURL`).
- **PR #2, activity routing + `--demo-worker`.** The activity queue was global, and the SDKs
  fail tasks they don't implement, so any second worker with different activities
  would steal and fail tasks. `PollTask(ctx, workerID, stepNames...)`, proto
  `PollTaskRequest.step_names`, HTTP `?activities=`. Both queues now share a generic
  `routedQueue[T]`. SDKs pass their names and skip a loop when nothing is registered
  (empty filter = "any"). `--demo-worker` runs reserve/charge/ship/release in-process
  (0.8–2 s per step; 25 % of first charges fail transiently; "decline" in the input
  fails charge for good, so the saga compensates). `/api/health` has `demo_worker`; the
  dashboard pre-fills OrderSaga and explains "decline".
- **PR #3, `--retention`.** Deletes completed/failed/terminated histories whose last
  event is older than the window (startup pass, then every retention/10 clamped
  to [1m, 1h]). Never touches live workflows. `HistoryStore.Delete` closes the cached
  handle, removes the file, and fsyncs the dir. `workflowd_workflows_purged_total`. List
  skips histories that vanish mid-scan. **Built but NOT enabled in prod** (needs the
  owner's OK, because it deletes data).

**Done (prod, VPS):**
- Binary `9b1f777` deployed, version-stamped; `workflowd.prev` now exists for rollback.
- Unit ExecStart gains `--demo-worker` (old unit backed up in `/root/workflowd.service.*`).
- The workflow stuck since 2026-10-07 completed seconds after restart.
- nginx (`side-projects`, shared with Hookdash, backup in `/root/side-projects.*`):
  `map`+`limit_req_zone workflowd_demo_write` (10 r/m per IP, burst 20, 429) on the
  demo vhost only. It covers writes; GET/HEAD and SDK `*/complete` calls are exempt.
  `client_max_body_size 16k`. Verified: 24/30 parallel writes → 429, GETs untouched,
  20 KB body → 413, Hookdash still 200.
- `workflowd-backup.timer` (daily 03:30 UTC) → `/usr/local/sbin/workflowd-backup` tars
  `/opt/durable/data` into root-only `/var/backups/workflowd`, keeps 14 days. Same disk,
  so it covers mistakes and corruption, not losing the disk.

**Next:** decide on `--retention` for prod (suggest `168h`). Then Phase 4 / Week 11, Raft.

**Gotchas:**
- Workers registering the same activity name share its queue *by design*. With the TS/
  Python examples connected to a `--demo-worker` engine, their `charge` can take the
  demo's retry (so a "decline" order can complete). Don't rename the demo steps: live
  histories already record `reserve`, and replay would break.
- The history store caches one open append handle per workflow and only releases it on
  `Delete`/`Close`. Without retention that is a slow fd leak (finished workflows keep
  theirs). Follow-up: release on terminal.
- Foreground `sleep` is blocked in the agent shell; drive timing tests from Python.
- Sequential curl from the laptop takes ~0.8 s per request, which is slow enough to refill
  a 10 r/m bucket. Test rate limits with parallel requests.

---

## 2026-08-21 — Session 14: UI revamp ("The Loom") + backend hardening

**Done (UI):**
- Full redesign of `web/dashboard` + `web/site` around one concept: the Jacquard
  loom (first programmable machine; punch-card chain = append-only history,
  picks = steps, crash = weaver stepping away). Concept documented in `web/README.md`.
- Palette: natural dyes on linen (indigo/madder/ochre/leaf/walnut on `#F3ECDF`);
  dark mode = warm umber "dye vat", never blue-black. System font stacks only.
- Custom inline-SVG assets: 3×3 over-under weave mark (+ favicon), hero loom with
  animated shuttle and punch cards labelled with real event names, step-kind glyphs,
  line-art empty-state loom. No webfonts/images/icon fonts — zero-dep promise holds.
- Dashboard: stat tiles with thread rails, filter chips with live counts, client-side
  id/type search, start-workflow dialog ("mount a warp"), detail drawer with steps
  rendered as woven weft picks, signal/terminate as forms.
- Site: hero split layout + loom illustration, loop diagram, **Pattern Book use-case
  tabs** (saga/Go, onboarding drip/TS, webhook capture/Python, trial clock/Go) with
  per-language syntax highlighting in site.js.

**Done (backend audit + fixes):**
- Error classification: new sentinels `ErrAlreadyExists`, `ErrUnknownType`,
  `ErrInvalidArgument`. HTTP start → 409 duplicate / 400 unknown type / 500 I/O;
  signal → 404 unknown / 409 terminal / 400 empty name; gRPC mirrors
  (`AlreadyExists`, `NotFound`, `InvalidArgument`, `Internal`).
- Empty signal names rejected before they can be durably recorded.
- Callback resilience: timer/retry/lease/decision callbacks re-arm after transient
  I/O errors instead of stranding the workflow; new `advanceRetries` service retries
  a decision loop that failed mid-advance; lease-expiry append failure re-enqueues
  the task (history still shows the attempt scheduled, so this is consistent).
- `--auth-token` flag: constant-time bearer auth on mutating `/api` routes;
  health + static assets exempt; startup warning when disabled.
- List endpoint: `q` substring search + `limit`/`offset` pagination + `total`.
- Health endpoint enriched: version, uptime_seconds, active workflows, queue depths.
- Request logging middleware (Info for mutations, Debug for reads).
- New tests: `httpapi_auth_test.go` (auth matrix, duplicate 409, pagination/search,
  health shape); updated `TestHTTPAPIErrors` to the corrected codes.
- Verified live: 401/201 token flow, 409 dup, 400 empty signal, 404 unknown signal,
  enriched health JSON; full `-race` suite green.

**Next:** Phase 4 / Week 11 — Raft (unchanged).

**Gotchas:** dashboard CSS uses `color-mix()` — evergreen browsers only, by design.
Chip counts are computed client-side from the unfiltered list; TIMED_OUT/UNKNOWN have
no chip and count into the Failed stat only, but still appear under All with a slate rail.
Auth covers only the JSON API by design (gRPC/metrics unauthenticated) — documented.

---

## 2026-08-05 — Session 13: Polyglot workflows — TypeScript, Python & Java SDKs

**Roadmap change.** Started Week 11 (Raft), then the user redirected: workflows
should not be Go-only. Agreed that adoption beats HA for a pre-alpha project, and
that settling the wire protocol *before* replicating it is the cheaper order. Raft
is parked (its `proto/raft.proto` design is kept as groundwork; the half-written
`log.go` was removed rather than left broken).

**The insight that made this small:** the engine already drives workflows through
the `Planner` seam — `Decide(events) → Decision`. Polyglot support is just a
`Planner` implementation that answers over the network. No changes to history,
recovery, timers, retries or leases.

```
engine → enqueues a WorkflowTask carrying the workflow's history
SDK    → PollWorkflowTask, replays the workflow in ITS runtime
SDK    → CompleteWorkflowTask with ONE command
engine → applies it exactly like a local decision
```

**Done:**
- **Protocol** (`proto/service.proto`): `RegisterWorkflowType`, `PollWorkflowTask`,
  `CompleteWorkflowTask`; `WorkflowTask` (carries full history + `task_token` +
  `history_length`); `WorkflowCommand` oneof (schedule_activity / start_timer /
  await_signal / complete_workflow / fail_workflow). Cross-package proto import
  of `event.proto` for `HistoryEvent`.
- **Engine** (`internal/server/polyglot.go`): split `applyDecisionLocked` out of
  `advanceLocked` — the single place a decision becomes durable, now shared by
  in-process and remote planners. `remotePlanner` marks a type as SDK-owned;
  `advanceLocked` dispatches a task instead of deciding inline (deciding needs a
  network round-trip and `e.mu` is held). `workflowTaskQueue` filters by type
  (broadcast channel + slice, since pollers need type matching). Fourth timer
  service (`decisionTimers`) re-dispatches if an SDK worker dies.
- **Two safety properties**, both tested:
  - *Stale-decision fencing.* A task records the history length it was computed
    from; a command whose history has moved on is rejected (`Aborted` / HTTP 409).
    This is what makes re-dispatch safe — two workers may both decide, only one
    command can apply.
  - *Command validation.* Step sequencing is checked before anything reaches the
    log, so a buggy SDK can't write a history `Reconstruct` later refuses to read.
- **HTTP transport** (`httpapi_polyglot.go`): the gRPC path ships raw protobuf
  history; the HTTP path ships the **already-reconstructed** view (input, steps
  with results, buffered signals). That single decision is why an SDK is ~250
  lines with **zero dependencies** instead of requiring a protobuf toolchain.
  Also added polyglot activity endpoints, so a non-Go process can run both halves.
- **SDKs** (`sdks/`): TypeScript and Python — both verified end-to-end against a
  live engine (activity → durable timer → signal → Saga compensation → completion).
  Java written but **never compiled** (no JDK on this machine); shipped as a
  clearly-labelled reference implementation.
- **Docs**: polyglot guide on the site (protocol, per-language suspend table,
  safety properties), `sdks/README.md`, README, landing page. Corrected the
  now-false "Go only" claims written last session.

**State:** `make ci` ✅ (9 new Go polyglot tests). Live cross-language run ✅.

**Next:** Phase 4 / Week 11 — Raft (resumed), or more SDK polish.

**Gotchas:**
- **Rejecting a malformed command must NOT clear the pending decision.** The first
  version did, which left the workflow with no queued task *and* no pending
  decision — stranded forever. Now the decision stays pending so the timer
  re-offers it, and a fixed deployment heals the workflow. **Teeth-verified**: the
  test hangs on the old behaviour.
- **JavaScript cannot use exceptions to suspend replay.** `catch (err)` catches
  *everything*, so a workflow's `try { await ctx.activity(...) } catch {…}` swallowed
  the yield sentinel and ran compensation on every replay pass — the TS example
  silently stalled while Python (whose `_Yield` extends `BaseException`) worked
  first time. The TS SDK now suspends with a **promise that never settles**, raced
  against a suspend signal, which workflow code cannot observe. Java uses `Error`
  for the same reason.
- Activity workers were *already* language-agnostic (gRPC, since Week 3). The gap
  was only ever the workflow function. Worth saying plainly rather than implying
  the whole thing was Go-locked.
- Two workers registering the same activity *name* share one queue — a test that
  ran two examples concurrently had its failing `charge` silently satisfied by the
  other worker's succeeding one. Use distinct step names when testing failure paths.

---

## 2026-08-05 — Session 12: Dashboard, JSON API & public site (Phase 3, Week 10 — PHASE 3 COMPLETE)

The engine grew a face. Three things landed: a JSON HTTP API, the embedded
`go:embed` dashboard (the Week-10 deliverable), and a public website with
comprehensive developer documentation.

**Why a JSON API at all:** browsers cannot speak raw gRPC, so the dashboard
needed an HTTP surface. The options were grpc-gateway (a dependency tree and a
codegen step) or ~200 lines of `net/http` + `encoding/json`. Hand-rolled won, for
the same reason the WAL and the metrics registry are hand-rolled.

**Done:**
- **`internal/server/httpapi.go`** — `GET /api/health`, `GET /api/workflows`
  (+`?status=`), `POST /api/workflows`, `GET /api/workflows/{id}`,
  `POST /api/workflows/{id}/terminate`, `POST /api/workflows/{id}/signal`.
  Uses Go 1.22+ method+wildcard mux patterns (`GET /api/workflows/{id}` +
  `r.PathValue("id")`) so there is no hand-rolled path parsing. **Explicit DTOs,
  not the generated protobuf structs** — the HTTP contract stays readable
  (statuses are bare strings like `"RUNNING"`, timestamps RFC3339) and survives
  proto renames. `DisallowUnknownFields` + a 1 MB body cap. Sentinel errors map
  to codes: 400 / 404 / 409 (already terminal) / 500.
- **`web/` Go package** — `//go:embed dashboard` + `Handler()` with an SPA
  fallback (unknown path → index.html). `site/` sits beside it and is
  deliberately **not** embedded.
- **`web/dashboard/`** — stat tiles, filterable auto-refreshing (2s) workflow
  table, detail drawer with the full step timeline (kind/status/attempts/errors/
  outputs/awaited signal), signal + terminate actions, theme toggle persisted to
  localStorage. Vanilla HTML/CSS/JS, **no build step, no npm** — a dashboard
  needing a Node toolchain would undermine the single-binary promise. 32K total.
- **`web/site/`** — landing page (problem/solution, feature grid, honest
  comparison table vs queues and Temporal, phase status) and `docs.html`: a
  full developer manual — quickstart, mental model, history/durability, replay,
  a determinism do/don't table, writing workflows, registering, writing a worker,
  retries + Saga, CLI flags, gRPC API, HTTP API, metrics reference with example
  PromQL, dashboard, recovery, leases, limitations, FAQ.
- **`cmd/workflowd`**: new `--http-addr` (default `:8233`); extracted a
  `startHTTP` helper now shared by the dashboard and metrics servers (both
  drained on shutdown). Added `ReadHeaderTimeout` as a slowloris guard.
- **README refreshed** — it still claimed "Phase 1 in progress" from Session 2.
- **Tests** (`httpapi_test.go`): full lifecycle over HTTP (start→list→filter→
  detail→terminate→409), a table of error codes, signal round-trip, and
  `TestDashboardServed` proving the assets really are compiled in and that the
  API still wins over the dashboard's `/` catch-all.

**State:** `make ci` ✅. Live smoke test ✅ — booted the daemon, served the
dashboard + assets, started/listed/inspected/terminated a workflow over curl,
confirmed `/metrics` agreed (`started_total 1`, `finished_total{terminated} 1`,
`active 0`), verified 404/409 paths and the SPA fallback.

**Next:** Phase 4 / Week 11 — Raft: read the paper, leader election, 3-node
localhost cluster.

**Gotchas:**
- **`go:embed` cannot reference parent directories.** The embed directive must
  live in a package whose directory contains the assets — hence `web/embed.go`
  next to `web/dashboard/`, rather than embedding from `internal/`.
- **Go's `internal/` rule shapes the whole integration story.** External code
  can't import `internal/server`, so today workflows are registered by building
  the daemon from this repo, and third-party workers must generate their own
  stubs from `proto/service.proto`. Documented honestly on the docs page; fixing
  it properly *is* the Phase-5 SDK work.
- Route specificity: `mux.Handle("/", web.Handler())` is registered alongside the
  `/api/...` patterns and loses to them, so the dashboard catch-all never
  swallows API calls — asserted in a test rather than assumed.
- Everything rendered into the dashboard (ids, types, errors, outputs) is
  workflow-controlled and is escaped before touching `innerHTML`.

---

## 2026-07-12 — Session 11: Observability — /metrics + visibility API (Phase 3, Week 9)

The engine can now be *watched* and *operated*: Prometheus metrics on an HTTP
endpoint, and a visibility API (List / Get / Terminate) over gRPC. Both stay
true to the zero-dependency stance — the metrics registry and text-format
renderer are hand-rolled stdlib (`internal/metrics`), no client_golang.

**Done:**
- **`internal/metrics`** — dependency-free Prometheus text-format (v0.0.4)
  registry: `Counter`, `CounterVec` (labelled children, sorted + escaped
  output), `Gauge`, `GaugeFunc` (read at scrape, e.g. queue depth),
  `Histogram` (cumulative buckets, `DefBuckets` extended to 60s). `Handler()`
  serves it over HTTP. Exact-render + `-race` concurrency tests.
- **Engine instrumentation** (`internal/server/metrics.go` + call sites):
  `workflowd_workflows_started_total`, `_finished_total{status=completed|failed|terminated}`,
  `_workflows_active` (gauge; re-seeded by `Recover`), `_tasks_dispatched_total`,
  `_task_results_total{result}`, `_leases_expired_total`, `_retries_scheduled_total`,
  `_signals_received_total`, `_timers_fired_total`, `_task_queue_depth` (GaugeFunc
  over `len(chan)`), `_step_duration_seconds` histogram (lease grant → worker
  result; `lease.claimedAt` added). Counters increment at the exact point the
  matching event is durably appended, so /metrics agrees with a replay.
- **Visibility API** (`internal/server/visibility.go`, 3 new RPCs):
  - `ListWorkflows(status_filter)` → summaries (id, type, status, started/updated
    timestamps, error, history length), sorted by id. Full scan of the data dir —
    honest O(n) debt, noted below. A workflow whose log is unreadable/invalid is
    returned with `UNSPECIFIED` + the error, never silently hidden.
  - `GetWorkflow(id)` → summary + per-step detail (name/seq/kind/status/attempts/
    error/output/fire_at/signal) straight from `Reconstruct`. Unknown id → NotFound.
  - `TerminateWorkflow(id, reason)` → appends `WorkflowFailed{terminated:true,
    error:"terminated by operator: …"}` under `e.mu`, drops the lease (zombie
    fencing: an in-flight worker's late CompleteTask is rejected — teeth-verified
    in a test), leftover timers no-op via existing stale-fire guards. Terminal →
    FailedPrecondition; unknown → NotFound.
- **Proto:** `WorkflowFailedEvent` +`terminated` flag (state machine surfaces it
  as `State.Terminated`; API presents it as status `TERMINATED`, on-disk it is
  still a FAILED terminal event — no new state-machine states). New
  `WorkflowStatus` enum + `WorkflowSummary`/`StepInfo` messages. Regenerated.
- **`workflowd --serve`**: new `--metrics-addr` flag (default `:9090`, empty
  disables); /metrics on its own `net/http` server, shut down alongside gRPC.
  Smoke-tested live: booted the daemon, scraped all 11 families via curl.
- **Tests:** 4 new end-to-end (list+get+filter, terminate semantics, terminate
  fences an in-flight worker, metrics scrape after real traffic) + 7 metrics
  unit tests. Harness now exposes the Engine for registry access.

**State:** `make ci` ✅ (fmtcheck · vet · build · `go test ./... -race` all green).

**Next:** Week 10 — embedded web dashboard (`go:embed`), which consumes exactly
what this session built (visibility RPCs + metrics). Backlog unchanged:
O(n²) global-lock ceiling, `CronSchedule`, buf adoption. First git commit still
pending — repo has full history in docs only.

**Gotchas:**
- `vet` rejects any method named `WriteTo(io.Writer)` that doesn't match the
  `io.WriterTo` signature — hence `Registry.WriteText`.
- `ListWorkflows`/`GetWorkflow` deliberately do NOT take `e.mu` — the history
  store has its own lock and reads see only fsync'd records, so visibility can
  never stall the decision loop. `TerminateWorkflow` DOES take `e.mu` (it
  mutates history + leases).
- Metric increments live inside the same `e.mu` critical section as the append
  they describe, but a test polling the store can observe the event a hair
  before the counter — scrape assertions poll with a deadline (see
  `TestMetricsEndToEnd`), don't assert one instant snapshot.
- A terminated workflow leaves its in-flight step SCHEDULED in history forever
  (same shape as timeout-failure); `Reconstruct` allows `WorkflowFailed` with a
  step open — that asymmetry (vs `WorkflowCompleted`) is load-bearing here.

---

## 2026-07-12 — Session 10: Leases & heartbeats (Phase 3, Week 8)

Phase 3 begins. Every dispatched task now carries a **lease** — a time-boxed,
token-proven claim — and a dead worker's task is **reassigned** instead of
stranding the workflow. This closes the worker-crash-stranding debt that has been
logged since Session 5: the engine is now worker-crash-safe, not just
engine-crash-safe.

**The model:** `PollTask` grants a lease at dispatch — a random 128-bit token
(stamped on the `Task` the worker receives, with `lease_duration_ms`) and a
deadline (`now + leaseDuration`, default 30s, `WithLeaseDuration` to tune).
`CompleteTask` must present the current token; a new `Heartbeat` RPC extends the
deadline for long-running activities. If the lease expires unheartbeated, the
reaper records `StepFailed("lease expired: worker …")` and hands the failure to
the **existing Week-7 retry machinery** — attempts remaining ⇒ backoff +
re-dispatch to the next poller; exhausted ⇒ the workflow sees the error (and can
compensate). No new `Reconstruct`/state logic at all: a dead worker is just a
failed attempt.

**Done:**
- **Proto:** `Task` +`lease_token` +`lease_duration_ms`; `CompleteTaskRequest`
  +`lease_token`; new `Heartbeat` RPC (`HeartbeatRequest{workflow_id,
  lease_token, worker_id}`). Regenerated.
- **`internal/server/lease.go`** (new): `lease` struct (workflow/step/seq/worker/
  token/deadline), `claimLocked` (mint token, arm expiry timer), `Heartbeat`
  (extend deadline; reject unknown/stale token — the worker's cue to abandon the
  work), `checkLeaseLocked`, and `onLeaseExpired` — the reaper. Leases are
  **ephemeral** like the queue (map keyed by workflow id; one in-flight step ⇒
  one lease per workflow); the durable fact is the scheduled step, and an engine
  restart drops all leases and re-enqueues for fresh claims (Wk4 recovery,
  unchanged).
- **Reaper self-correction:** expiry fires act on *current* lease state under
  the engine mutex — lease gone ⇒ no-op; deadline moved (heartbeat, or a newer
  lease on the same step's retry) ⇒ re-arm at the new deadline; actually expired
  ⇒ reclaim. A heartbeat that lands after the deadline but before the reaper
  still rescues the lease. Third `timer.Service` (`leaseTimers`), stopped by
  `Close()`.
- **Zombie fencing:** `CompleteTask` validates the token before touching
  history, and releases the lease when a result is recorded. A worker whose
  lease expired (task reassigned) is rejected cleanly — two workers can never
  both record a result for the same attempt.
- **Engine options:** `NewEngine(store, logger, opts...)` — functional-options
  pattern; `WithLeaseDuration(d)`. All existing call sites unchanged.
- **Tests** (`lease_test.go` + gRPC `TestHeartbeatRPC`): expiry → reassignment
  to a healthy worker → COMPLETED with `Attempts==2` + zombie CompleteTask
  rejected + `StepFailed("lease expired")` audit trail in history; expiry with
  no retry policy → workflow FAILED naming the worker (explicit, not stranded);
  heartbeats keep a task alive across 3× the lease window (no-retry def gives it
  teeth); stale-token heartbeat rejected; Heartbeat RPC round-trip.
  **Teeth-verified:** disabling the reaper makes all three expiry tests fail —
  the healthy worker never gets the task (the old stranding behaviour).

**Scoped out (deliberately):** sticky queues / worker affinity from the Wk8
line. Sticky queues optimize *worker-side replay caches* (Temporal keeps
workflow state warm on one worker); our workers are stateless activity
executors — all replay happens on the engine — so there is nothing to stick to.
Revisit if workers ever cache workflow state (Phase 5 SDK).

**State:** `make ci` ✅ (fmtcheck · vet · build · test -race) · serve smoke ✅ ·
reaper teeth-verified.

**Next:** Phase 3 / Week 9 — observability: Prometheus `/metrics`, visibility
API (List/Get/Terminate). slog structured logging is already in place. Still
open: O(n²) global-lock/full-ReadAll ceiling (per-workflow locks + state
cache), `CronSchedule` (deferred from Wk5).

**Gotchas:**
- A lease-expiry failure **consumes an attempt** (records `StepFailed` with the
  current attempt count). Steps whose workers might die mid-run need
  `MaxAttempts > 1` — with the default no-retry policy, one dead worker fails
  the workflow (explicitly, with a "lease expired" error naming the worker).
  This is deliberate: durable, auditable, and a task that crashes its workers
  can't loop forever.
- The lease map is keyed by **workflow id** (valid while the engine is
  one-step-in-flight-per-workflow). Parallel steps within a workflow (Phase 5)
  will need `workflow_id/sequence` keys.
- Heartbeat does **not** arm a new timer — the pending expiry fire notices the
  extended deadline and re-arms (lazy re-arm). Stale fires from superseded
  leases are harmless no-ops.
- There are now **three** timer services (Sleep steps, retry backoffs, lease
  expiries); all stopped by `Engine.Close()`.

---

## 2026-07-02 — Session 9: Retries & Saga/compensation (Phase 2, Week 7 — PHASE 2 COMPLETE)

Failed activities now retry automatically with backoff, and workflows can compensate
(Saga) on failure. This completes Phase 2 — the engine has the full durable-execution
feature set: crash recovery, timers, signals, replay, retries, and compensation.

**Done:**
- **Retries (engine-level, transparent).** `RetryPolicy{MaxAttempts, Backoff,
  MaxBackoff}` (`internal/server/retry.go`), registered per activity via
  `SetRetryPolicy(stepName, policy)` (default: no retry). On an activity failure,
  `CompleteTask` records `StepFailed` (with `retry_count`) and, if attempts remain,
  arms a **backoff timer on a second `timer.Service`** (`retryTimers`) instead of
  advancing; the workflow never sees the transient error. `onRetryDue` re-schedules
  the **same sequence** (another attempt) and re-dispatches. Backoff is exponential,
  capped at `MaxBackoff`. Exhausted → advance (workflow sees the error / fails).
- **Multi-attempt steps in `Reconstruct`.** A `StepScheduled` at the *same* sequence
  as a failed activity re-opens that step (another attempt) rather than appending a
  new one; `state.Step` gained `Attempts`. So a retried activity stays one logical
  step — which keeps the replay engine's one-call-one-step mapping intact.
- **Retries survive restart.** Recovery's RUNNING branch: a failed activity with
  attempts left (engine died during the backoff window) re-arms the retry promptly
  (the in-memory backoff timer is gone; the failure + attempt count are in history).
- **Saga/compensation** — no new machinery: a failed `ExecuteActivity` returns a Go
  `error` the workflow catches and responds to by running compensating activities.
  `orderSaga` demo in `cmd/workflowd` (reserve → sleep → charge[retry×3] → ship; on
  charge failure, release + fail with a wrapped error), registered in `--serve`.
- **Tests:** retry-then-succeed (fails 2×, 3rd wins; `Attempts==3`); retry-exhausted
  → workflow FAILED after exactly `MaxAttempts` runs; Saga compensation (charge fails
  → release runs → FAILED with wrapped error, history = reserve✓/charge✗/release✓);
  retry-survives-restart; plus a `Reconstruct` retry unit test.

**State:** `make ci` ✅ (fmtcheck · vet · build · test -race) · `gofmt` clean · `--demo`
smoke ✅. **Phase 2 complete.**

**Next:** Phase 3 / Week 8 — multi-worker & failure handling: worker identity,
heartbeats, **lease tokens / visibility timeout** (this finally closes the
worker-crash-stranding debt — a dead worker's task gets reassigned), sticky queues.
Then Wk9 observability (metrics, visibility API) and Wk10 the embedded dashboard.
Also still open: the O(n²) global-lock/full-ReadAll ceiling (per-workflow locks +
state cache) and `CronSchedule` (deferred from Wk5).

**Gotchas:**
- Retries re-use the **same sequence** (multi-attempt), *not* a new one — that's what
  keeps the replay counter aligned (one `ExecuteActivity` call ↔ one step, regardless
  of attempts). A new-sequence retry would desync replay.
- There are now **two** timer services (Sleep steps vs. retry backoffs); both are
  stopped by `Engine.Close()`. Backoff timers are ephemeral — the durable fact is
  "failed activity with attempts remaining," which recovery re-derives.
- Retry policy is keyed by **step name** (Phase-2 simplification; ambiguous across
  workflows sharing a step name). Per-call activity options are Phase 5.

---

## 2026-07-02 — Session 8: Deterministic replay engine (Phase 2, Week 6)

The headline Phase-2 feature: a workflow can now be written as an **ordinary Go
function** whose execution is deterministically replayed against history to drive it
durably. "Write normal-looking Go, get durability."

**The replay model:** a workflow func calls `ctx.ExecuteActivity/Sleep/WaitForSignal`.
To decide the next action, the engine *re-runs the function from the top* against the
current history. Each call is a fast lookup: if the step's result is recorded, return
it (replay); if the step is in flight, block; if the step isn't in history yet, this
is the next action — emit the decision and unwind. Yield is a `panic(yield{decision})`
recovered by the driver; a normal function return means complete (or fail, if it
returns an error).

**Done:**
- **`sdk` package** (was a placeholder): `Context` (`ExecuteActivity`, `Sleep`,
  `WaitForSignal`, `Input`), `WorkflowFunc`, and `Workflow{Type, Fn}` whose
  `Decide(events) (state.Decision, error)` *is* the replay engine. A `replayContext`
  walks steps by a call counter; **determinism is enforced** — if the code issues a
  different step (kind or name) than history recorded at that position, Decide returns
  a `non-determinism` error instead of corrupting state. Data flow between steps is
  expressed in the Go code itself (no engine auto-chaining).
- **`Planner` seam** (`internal/server`): new interface `Decide(events) →
  state.Decision`. Both `state.WorkflowDefinition` (added a `Decide` method wrapping
  package `Decide`) and `sdk.Workflow` satisfy it. Engine now holds
  `map[string]Planner`; `Register(def)` keeps working, new `RegisterWorkflow(type,
  Planner)` accepts code-based workflows. `advanceLocked` takes a `Planner` and calls
  `planner.Decide` — **unchanged decision-application code** drives either model.
- **Step kind moved into history** so runtime needs no definition: `StepScheduledEvent`
  gained `awaits_signal`/`signal_name`; `state.Step` gained `Kind` and `Signal`,
  reconstructed from the opening event (`TimerStarted`→timer, `StepScheduled`+flag
  →signal, else activity). Recovery's WAITING branch and `deliverableSignal` now read
  `tail.Kind`/`tail.Signal` — **no `def` lookups** — so recovery, timers, and signal
  delivery work identically for code-based workflows.
- **Tests:** sdk unit (in-order decisions, in-flight NOOP, activity-error →
  FAIL, **non-determinism detected**, replay is pure). Engine e2e: a code workflow
  (activity → Sleep → WaitForSignal → activity) runs to completion, with the ship
  step's input *composed by the workflow code* and step kinds read from history; and
  the same workflow **survives a restart** (recovered mid-timer, finishes).

**State:** `make ci` ✅ (fmtcheck · vet · build · test -race) · `gofmt` clean.

**Next:** Phase 2 / Week 7 — retry policies (backoff) + Saga/compensation + an
order-flow demo. The replay model already supports compensation naturally: a failed
`ExecuteActivity` returns an error the workflow can catch and respond to by running
compensating activities. Retries need a policy + a `retry_count` on scheduling and a
backoff timer between attempts.

**Gotchas:**
- Replay re-runs the workflow function on **every** decision, so workflow code must be
  deterministic and side-effect-free except via `ctx` (no `time.Now`/`rand`/IO, and
  avoid `defer` with side effects — it runs each replay). This is the determinism
  contract; the enforcement catches step-level divergence, not arbitrary impurity.
- Yield uses `panic`/`recover` with a private `yield` type; a *real* panic in workflow
  code is re-raised, not swallowed.
- The `Planner` interface is satisfied structurally, so `sdk` does **not** import
  `server` (no cycle): `server` defines `Planner`, `sdk.Workflow` just has a matching
  `Decide` method. Callers wire them together.

---

## 2026-07-02 — Session 7: Durable timers & signals (Phase 2, Week 5)

Phase 2 begins. Workflows can now **sleep** and **wait for external events**, both
durably: a timer armed before a crash still fires after restart, and a signal that
arrives before its wait is buffered and delivered when the wait is reached.

**Model change (the enabler):** `WorkflowDefinition.Steps` went from `[]string` to
`[]state.StepDef`, a typed step that is an activity, a timer, or a signal wait.
Constructors: `state.Activity(name)`, `state.Sleep(name, dur)`,
`state.AwaitSignal(name, signal)`, and `state.Activities(names...)` for the common
all-activities pipeline. All existing call sites migrated to `Activities(...)`; the
zero value is an activity, so nothing else changed semantically.

**Done:**
- **Proto** (`event.proto`): three new events — `TimerStartedEvent` (step, sequence,
  `fire_at` unix-nanos, `input` carried through), `TimerFiredEvent`,
  `SignalReceivedEvent` (name, payload). `service.proto`: new `SignalWorkflow` RPC +
  messages. Regenerated (`make proto`, reproducible).
- **`internal/state`**: `StepKind` (Activity/Timer/Signal) + `StepDef` + constructors.
  `Reconstruct` folds the new events — timer steps open via `TimerStarted` (carrying
  `FireAt` for recovery) and close via `TimerFired` (output = input, a sleep makes no
  new data); signals accumulate into `State.Signals` (buffered, in arrival order).
  `Decide` gained `DecideStartTimer` (returns a `Delay`; the absolute deadline is
  computed by the impure caller, keeping Decide clock-free) and `DecideAwaitSignal`.
- **`internal/timer`** (new): min-heap (`container/heap`) `Service` with one
  goroutine that sleeps until the earliest deadline via `time.After`, then fires all
  due timers. Ephemeral like the queue; durability is the log. Tests: fires in
  deadline order, an already-expired timer (the recovery case) fires promptly, and
  adding an earlier deadline wakes the loop.
- **`internal/server`**: engine owns a `timer.Service` (started in `NewEngine`,
  stopped by new `Engine.Close()`). `advanceLocked` handles `DecideStartTimer`
  (record `TimerStarted` with `now+Delay`, arm the timer — no worker task) and
  `DecideAwaitSignal` (record a task-less `StepScheduled`, then try to deliver a
  buffered signal). `onTimerFired` records `TimerFired` + advances (with a
  still-waiting guard, so a stale fire is a no-op). `SignalWorkflow` records
  `SignalReceived` then delivers; `deliverableSignal` matches the k-th wait for a
  name to the k-th received signal (correct buffering). **Recovery re-arms timers**
  from the durable `FireAt` and re-tries buffered signal delivery, alongside the
  activity re-enqueue from Week 4. gRPC `SignalWorkflow` handler added.
- **Tests:** durable sleep runs to completion; **timer survives restart**
  (teeth-verified — remove the recovery re-arm and the workflow never resumes);
  signal delivered to a waiting workflow (gRPC); signal buffered before the wait;
  signalling a terminal workflow is `FailedPrecondition`. Plus state + timer units.

**State:** `make ci` ✅ (fmtcheck · vet · build · test -race) · `gofmt` clean ·
timer-restart teeth-verified.

**Scoped out (deliberately):** `CronSchedule` from the Week-5 line is deferred — it's
a thin layer on the timer service (fire → start a fresh run on a schedule) and is
cleaner to add after the Phase-2 replay engine lands. Logged, not forgotten.

**Next:** Phase 2 / Week 6 — the deterministic replay engine: replace the
`[]StepDef` data plan with a real Go workflow function whose execution *is* replayed
against history, and enforce determinism constraints (no wall clock / rand / bare
goroutines inside workflow code). The events, `Reconstruct`, and `Decide` built so
far stay; only the "where do the steps come from" layer changes.

**Gotchas:**
- `Decide` stays **pure / clock-free**: a timer decision carries a `Delay`, and the
  engine (not Decide) turns it into an absolute `fire_at` at record time. Recording
  the absolute deadline (not the duration) is what lets recovery re-arm correctly.
- Signal waits are steps with **no worker task**: they record a `StepScheduled` (so
  status is WAITING) but enqueue nothing; they complete via `SignalWorkflow`/buffer,
  recorded as a normal `StepCompleted`. So `Reconstruct` needs no signal-specific
  step logic — only the engine distinguishes them, via the definition's `StepKind`.
- Every `NewEngine` starts a timer goroutine — tests must `Engine.Close()` (added to
  the harness/`newTestEngine` cleanups) or leak it.

---

## 2026-07-01 — Session 6: Crash recovery & The Kill Test (Phase 1, Week 4)

The Phase-1 finale: the engine now rebuilds its runtime state from disk on startup
and survives an uncatchable `SIGKILL`. This completes Phase 1 — a single-node,
crash-safe durable engine.

**Done:**
- **`HistoryStore.ListWorkflows()`** (`internal/history/store.go`) — enumerates every
  `*.history` file in the data dir and returns the workflow ids. The data dir is the
  durable index recovery scans; non-`.history` files (sentinels, temp) are ignored.
  Extracted the `.history` suffix into a `historyExt` const so listing and pathing
  agree. Test `TestListWorkflows` (finds exactly the logged workflows, ignores a
  stray file, empty on a fresh dir).
- **`Engine.Recover()`** (`internal/server/engine.go`) — startup replay. Scans all
  workflows, `Reconstruct`s each, and puts non-terminal ones back:
  - **WAITING** (a step was in flight when we died): re-enqueue the in-flight step so
    a worker picks it up. At-least-once — if the worker had actually finished but the
    result never got recorded, the step re-runs. Phase 1 is engine-crash-safe, not
    exactly-once.
  - **RUNNING** (crashed mid-decision: result recorded, next step not yet scheduled,
    or first step never scheduled): `advanceLocked` makes the next decision.
  - Terminal / not-started / **unregistered type**: skipped (an unregistered type is
    logged and left on disk, not a startup failure). Completed steps never re-run —
    `Decide` derives the next action from history, which already records them done.
  Wired into `cmd/workflowd` `runServe`: `Recover()` runs after `Register`, before
  the listener accepts traffic.
- **The Kill Test** (`internal/server/kill_test.go`) — the Week-4 deliverable, using
  the Go **re-exec pattern**. A child process (`TestKillHelperVictim`, guarded by an
  env var, skipped in normal runs) starts a 5-step workflow, drives it to WAITING at
  step 3, writes a ready sentinel, and blocks. The parent `SIGKILL`s it (no defer, no
  graceful shutdown), reopens a fresh engine over the same data dir, asserts the kill
  left WAITING/step3 on disk, `Recover()`s, and drives steps 3–5 home. Asserts:
  final COMPLETED, output `done:step5`, exactly 12 events (started + 5×(sched+done) +
  completed — **no duplicate steps**), every step completed once in order.
  **Verified it has teeth:** disabling the WAITING re-enqueue makes the workflow stall
  at step 3 and the test fail (`did not terminate within 8s`).
- Recovery unit tests (`internal/server/recover_test.go`, no gRPC — engine directly):
  re-enqueues a WAITING step, advances a RUNNING one (and appends the next
  StepScheduled), skips a terminal workflow, skips an unregistered type without error.

**State:** `make ci` ✅ (fmtcheck · vet · build · test -race) · `gofmt` clean · Kill
Test teeth-verified.

**Next:** Phase 2 / Week 5 — timers & signals. Durable `Sleep`/`WaitForEvent`, a
min-heap timer service (fires → records `TimerFired` → re-drives), `SignalWorkflow`
RPC + `SignalReceived` event, and recovery re-arming pending timers from history
(extends this session's `Recover`).

**Gotchas:**
- The Kill Test re-execs `os.Args[0]` (the test binary) filtered to
  `-test.run=^TestKillHelperVictim$`; the victim is skipped in a normal `go test` run
  and only runs its body when `WORKFLOW_KILL_HELPER=1` is set. The ready sentinel
  lives **outside** the data dir so `ListWorkflows` never mistakes it for a log.
- `Recover` does exactly **one** decision per workflow at startup (Phase 1 is
  one-step-at-a-time); subsequent steps schedule as workers complete tasks, same as
  steady state. Don't loop it — that would double-schedule.
- `SIGKILL` durability rests entirely on the Session-5 fsync/torn-tail work: the child
  gets no chance to flush, so only already-`fsync`'d events survive — which is exactly
  the guarantee `Append` provides.

---

## 2026-06-29 — Session 5: Code review + durability hardening

Reviewed everything built so far (history store, state machine, gRPC server) for bugs and
"haunt us later" architectural mistakes, then fixed the ones worth fixing now. Two real
durability holes, plus several smaller guardrails. No new feature work.

**Fixed (durability — the important ones):**
- **Torn-tail truncation on open** (`internal/history/store.go`). The store tolerated a torn
  trailing record on *read* but never truncated it, and the append handle is `O_APPEND`
  (always writes at physical EOF). So the first append after a crash landed *after* the torn
  bytes and mis-framed every subsequent read — a 3-byte torn tail became a 3.7 GB "length".
  Added `recoverTornTail` (scan → `Truncate` to the last clean record boundary → fsync), run
  on the first open of an existing file, before any new append. A mid-log CRC error is still
  a hard error — only a torn *tail* is truncated. This is the property Week 4 crash recovery
  is built on, so it had to land first.
- **Parent-directory fsync on file creation** (`store.go`). `Append` fsync'd the file but
  never its directory, so a new workflow's first event (`WorkflowStarted`) could be lost on
  power loss despite a "successful" append. Added `syncDir`, called once when a new history
  file is created.
- New regression test `TestAppendAfterTornTailRecovers` — proven to fail without the fix
  (mis-frames to a 3.7 GB length) and pass with it.

**Fixed (smaller guardrails):**
- **Shutdown hang** (`cmd/workflowd/main.go`). `GracefulStop` waits for in-flight RPCs, but
  an idle worker parked on long-poll `PollTask` never returns → SIGTERM hangs forever. Now
  bounded by `shutdownGracePeriod` (10s) with a fallback to `gs.Stop()`.
- **Definition drift** (`internal/state/decision.go`). `Decide` now refuses to run when the
  recorded history's step names don't match the registered definition (a deploy that
  reorders/renames steps would otherwise silently execute a different workflow). Cheap
  stand-in until `workflow.Version` in Phase 5.
- **Workflow-ID collisions** (`internal/server/engine.go`). Auto-generated ids were
  `type-<unixnano>`, which can collide at coarse clock resolution; added an `%08x` random
  suffix (`math/rand/v2`).
- Documented the deliberate asymmetry in `Reconstruct` (a `WorkflowFailed` is allowed while
  a step is in flight; a `WorkflowCompleted` is not).

**Deferred on purpose (logged, not fixed):**
- **Global engine mutex + full `ReadAll` per operation → O(n²) I/O and a throughput
  ceiling.** Architectural; the fix is per-workflow locking + an in-memory state cache.
  Decide the locking model before implementing — don't bolt it on now.
- **Worker-crash stranding.** No lease/visibility-timeout: a worker that dies mid-task
  strands its workflow in WAITING until an engine restart re-enqueues it (Week 4). Leases
  are Phase 3. The thing to fix now is the *claim*: Phase-1 durability covers engine crashes,
  not worker crashes — not "exactly-once."

**State:** `go build` ✅ · `go vet` ✅ · `go test -race` ✅ · `gofmt` clean.

**Next:** Phase 1 / Week 4 — crash recovery & WAL: startup scan + replay of all history
files, re-enqueue incomplete steps, the Kill Test. The torn-tail recovery added this session
is the foundation for it.

**Gotchas:**
- `recoverTornTail` runs on the first append to an *existing* workflow file, so that append
  pays one extra full scan (the engine also `ReadAll`s it — two scans on first touch). Fine
  per workflow per process; revisit with the state cache.
- Directory fsync (`syncDir`) is Unix-correct (Linux/macOS); it would error on Windows. The
  project targets Unix and CI is Linux.

---

## 2026-06-29 — Session 4: gRPC task queue (Phase 1, Week 3)

**Done:**
- `proto/service.proto` — `WorkflowService` with `StartWorkflow` / `PollTask` /
  `CompleteTask`, plus a `Task` message and a `CompleteTaskRequest.result` oneof
  (output = success, error = failure). Generates into `internal/server` as package
  `server`. Makefile now installs `protoc-gen-go-grpc` (`make tools`) and runs
  `--go-grpc_out` (`make proto`). Added `google.golang.org/grpc v1.81.1`.
- `internal/server/engine.go` — `Engine`: owns the history store + task queue + a
  workflow-type registry. `StartWorkflow` records WorkflowStarted then schedules
  step 1; `CompleteTask` validates the result against the in-flight step, records
  StepCompleted/StepFailed, then advances. `advanceLocked` is the extracted
  `runDemo` loop: ReadAll → `state.Decide` → append event → enqueue task (or reach
  terminal). All mutation under one mutex so event ids stay contiguous.
- `internal/server/queue.go` — `taskQueue`: a buffered channel. Channel receive =
  exactly-one-delivery, which *is* the "no duplicate dispatch" guarantee.
  Context-aware `poll` for long-polling workers. Ephemeral by design.
- `internal/server/grpc.go` — `GRPCServer`: thin adapter from the generated
  `WorkflowServiceServer` to the Engine; maps engine errors to gRPC status codes.
- `cmd/workflowd --serve [--addr :7233]` — real TCP gRPC server with SIGINT/SIGTERM
  GracefulStop. `--demo` (inline loop) still works.
- Tests (all green under `-race`): `queue_test.go` — no-duplicate-dispatch across
  16 pollers / 1000 tasks + poll-blocks-until-cancel. `engine_test.go` (bufconn,
  real client + worker) — 2-step run to completion (the Week-3 deliverable),
  step-failure → FAILED, unknown-type rejection, duplicate-CompleteTask rejection.

**State:** `make ci` ✅ (fmtcheck · vet · build · test -race) · serve-mode smoke test
(listen + graceful shutdown) ✅ · `go mod tidy` clean.

**Next:** Phase 1 / Week 4 — crash recovery & WAL. Startup scan replays every
history file and re-enqueues scheduled-but-incomplete steps (the task queue is
ephemeral, so recovery re-derives it). Then **The Kill Test**: 5-step workflow,
`os.Kill` at step 3, restart, verify steps 4–5 run with no duplicates.

**Gotchas:**
- The proto-generated `Task` lives in package `server`, so the queue reuses
  `*server.Task` directly — don't define a second `Task` struct (name clash).
- `protoc-gen-go` was installed but not on `PATH`; the Makefile prepends
  `$(go env GOPATH)/bin`, so `make proto` works even when the shell `which` can't
  find it. New machines also need `make tools` for `protoc-gen-go-grpc`.
- gRPC status mapping is intentionally coarse (StartWorkflow→InvalidArgument,
  CompleteTask→FailedPrecondition); separating client errors from internal IO
  errors is a later polish item.
- bufconn client target must be `passthrough:///bufnet` with a `WithContextDialer`
  — the default `dns` resolver would try to resolve the fake name.

---

## 2026-06-22 — Session 3: Workflow state machine (Phase 1, Week 2)

**Done:**
- `internal/state/state.go` — `Reconstruct(events) (*State, error)`: pure fold of a
  workflow's history into status + per-step progress. Status enum (`NOT_STARTED`,
  `RUNNING`, `WAITING`, `COMPLETED`, `FAILED`, `TIMED_OUT`); `StepStatus`
  (`SCHEDULED`/`COMPLETED`/`FAILED`). Validates the log: WorkflowStarted must be first,
  no events after a terminal one, step results must match the in-flight step (seq+name),
  sequences contiguous from 1, no schedule while a step is open.
- `internal/state/decision.go` — `Decide(def, events) (Decision, error)`: pure next-action
  function. `WorkflowDefinition{Type, Steps}` is the Phase-1 stand-in for workflow code.
  Decisions: `NOOP` / `SCHEDULE_STEP` / `COMPLETE_WORKFLOW` / `FAIL_WORKFLOW`. Sequential,
  one step at a time; step input chains the previous step's output; a step failure fails
  the workflow (no retry policy until Wk7).
- Tests: `state_test.go` (status transitions, multi-step fold, 8 bad-history rejections) +
  `decision_test.go` (table of hand-built history → asserted decision — the Week-2
  deliverable — plus full drive-to-completion and drive-to-failure loops + an Example).
  8 top-level tests, all green under `-race`.
- `cmd/workflowd --demo` now drives a 3-step OrderWorkflow (reserve→charge→ship) end to end
  through the decision loop, persisting every event. Simulated inline worker (no gRPC yet).
- Updated `internal/state/doc.go` to "implemented".

**State:** `go build ./...` ✅ · `go vet` ✅ · `go test -race` ✅ · `gofmt` clean.

**Next:** Phase 1 / Week 3 — gRPC task queue in `internal/server` (`StartWorkflow`,
`PollTask`, `CompleteTask`). The server becomes the thing that records WorkflowStarted,
calls `state.Decide`, persists the event, and hands tasks to *real* external workers — the
inline `runDemo` loop is the blueprint for it.

**Gotchas:**
- The proto oneof wrapper interface (`isHistoryEvent_Attributes`) is **unexported**, so code
  outside package `history` can't name it. Build events as `&history.HistoryEvent{Attributes: ...}`
  and pass the whole event; don't try to accept the oneof interface as a parameter type.
- `Decide` assumes the caller already recorded `WorkflowStarted`; on empty history it returns
  `NOOP` by design (it does not invent the start event).
- A failed-then-retried step (new sequence) is *parseable* by `Reconstruct` but `Decide`
  never *produces* one in Phase 1 — retry lands in Week 7.

---

## 2026-06-21 — Session 2: Foundation & architecture

**Done:**
- Architected the full repo: Go project layout for all 5 phases, docs, ADRs, CI, license.
- `CLAUDE.md` (cross-session AI guide), `docs/ARCHITECTURE.md`, `docs/ROADMAP.md`,
  `docs/REFERENCES.md`, ADRs 0001–0003, `README.md`, `CONTRIBUTING.md`, Apache-2.0 `LICENSE`,
  `.editorconfig`, GitHub Actions CI.
- `cmd/workflowd` minimal daemon (opens the history store, `--demo` smoke test, `--version`).
- Package-doc placeholders for `internal/state`, `internal/server`, `sdk`.
- Decisions locked: module `github.com/olayanju-1234/durable-workflow-engine`,
  Apache-2.0 license, protoc now / buf later. Name to be branded at Phase 5.
- Added `docs/walkthroughs/session-02-foundation.md` — user is new to Go; every session now
  ends with a plain-English walkthrough (Node/TS analogies). Baked into CLAUDE.md.

**State:** `go build ./...` ✅ · `go vet` ✅ · `go test -race` ✅ · `gofmt` clean.

**Next:** Phase 1 / Week 2 — workflow state machine in `internal/state` (decision loop over
history → schedule/complete/fail; states RUNNING/WAITING/COMPLETED/FAILED/TIMED_OUT).

**Gotchas:** `make proto` needs `$(go env GOPATH)/bin` on PATH so `protoc` finds `protoc-gen-go`.
Generated `event.pb.go` is committed, so CI doesn't run protoc.

---

## 2026-06-21 — Session 1: Setup & history store

**Done:**
- Installed toolchain (Go 1.26.4, protoc 35.1 via Homebrew). git user.email set.
- `proto/event.proto` — 6 event types as a `HistoryEvent` oneof wrapper.
- `internal/history/store.go` — `HistoryStore.Append/ReadAll/Close`; disk framing
  `[4B len][4B CRC32C][protobuf]`, fsync per append, torn-tail tolerance, CRC detection.
- `internal/history/store_test.go` — restart-survival test (the Phase 1 Wk1 deliverable),
  plus missing-workflow and corrupt-record tests. All pass.
- `Makefile`, `.gitignore`.

**Next:** Architect the project properly (became Session 2).

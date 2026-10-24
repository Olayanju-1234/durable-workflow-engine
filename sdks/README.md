# SDKs — write workflows in any language

The engine owns durability (history, replay position, timers, retries, leases).
An SDK owns **your workflow code**. They talk over a tiny protocol, so workflow
logic can live in any language.

| SDK | Path | Status |
|-----|------|--------|
| Go | [`../workflow/`](../workflow/), [`../worker/`](../worker/), [`../client/`](../client/) | ✅ tested end to end, including leader failover (gRPC) |
| TypeScript / Node | [`typescript/`](typescript/) | ✅ run end to end in CI on every PR (Node 20), plus `sdks/sdk_e2e_test.go` |
| Python | [`python/`](python/) | ✅ run end to end in CI on every PR (Python 3.12), plus `sdks/sdk_e2e_test.go` |
| Java | [`java/`](java/) | ✅ compiled (`--release 11`, lint as errors) and run end to end in CI on every PR (JDK 21) |

CI runs each SDK's example worker against a freshly built engine with
[`scripts/sdk-e2e.sh`](../scripts/sdk-e2e.sh): an approved order must complete,
and a rejected one must run its compensation and fail. Run it locally with
`./scripts/sdk-e2e.sh typescript` (or `python`, `java`). `go test ./sdks` runs a
longer TypeScript and Python scenario (leader hints, an engine restart, retry
policies, versions, side effects, binary and JSON payloads, heartbeats) when
`node` / `python3` are installed.

Every SDK here is **dependency-free**: Node's `fetch`, Python's `urllib`, and
Java's `HttpClient` are all built in. No protobuf toolchain. The TypeScript and Python SDKs
are on npm and PyPI; Java is packaged and waiting on its first Maven Central release (see
[Publishing](#publishing-the-sdks)):

| SDK | Package | Install |
|-----|---------|-------------------------|
| TypeScript | npm `workflowd` ([`package.json`](typescript/package.json), types in [`workflowd.d.mts`](typescript/workflowd.d.mts)) | `npm install workflowd` (on npm) |
| Python | PyPI `workflowd` ([`pyproject.toml`](python/pyproject.toml)) | `pip install workflowd` (on PyPI) |
| Java | Maven `io.github.olayanju-1234:workflowd` ([`pom.xml`](java/pom.xml)), class `io.github.olayanju1234.workflowd.Workflowd` | see [`java/README.md`](java/README.md) |

Until Java is published, copy `Workflowd.java` into your project.

## Auth and TLS

For an engine started with `--auth-token-file` (or `$WORKFLOWD_AUTH_TOKEN`) and
`--tls-cert`/`--tls-key`, give the SDK an `https://` address and the token:

| SDK | Token | Private CA |
|---|---|---|
| Go | `client.Options{AuthToken: tok}` | `client.Options{TLS: &tls.Config{RootCAs: pool}}` |
| TypeScript | `new Worker({ address, token })` | start Node with `NODE_EXTRA_CA_CERTS=/path/ca.pem` |
| Python | `Worker(address=..., token=tok)` | `Worker(..., ca_file="/path/ca.pem")` |
| Java | `new Workflowd.Worker(address, id, tok, ssl)` | `ssl = Workflowd.trustPem(Path.of("/path/ca.pem"))` |

A certificate from a public CA needs nothing beyond the `https://` address. The
examples read `WORKFLOWD_AUTH_TOKEN` and `WORKFLOWD_CA_FILE`, and CI runs every SDK
over TLS with a required token (`E2E_SECURE=1 ./scripts/sdk-e2e.sh <sdk>`).

## How it works

The engine never runs your workflow code. Instead it asks a question:

```
engine  →  "here is workflow W's history. what happens next?"
SDK     →  replays your function against that history
SDK     →  "schedule activity 'charge'"  (a single command)
engine  →  records it durably, dispatches the work, repeats
```

Your workflow function is re-run from the top on every decision. Steps that
already completed return their recorded results instantly; the first step with
no result yet becomes the command. **The function runs many times, but each side
effect happens exactly once.**

That makes an SDK a pure function of history — it stores nothing, so a crashed
SDK worker costs nothing. The engine simply re-dispatches the task.

## Two transports

| | gRPC | HTTP/JSON |
|---|---|---|
| Task payload | raw protobuf history | pre-reconstructed steps |
| SDK must implement the history fold | yes | no |
| Dependencies | protobuf + gRPC | none |
| Used by | Go | TypeScript, Python, Java |

The HTTP transport ships the *already-reconstructed* view — the workflow's
input, its steps in order with their results, and any buffered signals — which
is precisely what replay needs. That is why a complete SDK fits in ~250 lines.

## Writing an SDK for a new language

You need four things:

1. **Register** — `POST /api/workflow-types` with the types you can run.
2. **Poll** — `GET /api/workflow-tasks?types=A,B` (long-poll; 204 means "nothing
   yet, poll again").
3. **Replay** — walk your workflow function, matching each durable call against
   `steps[]` in order. Return the recorded output, stay suspended if the step is
   still `SCHEDULED`, or emit a command for the first step not in history.
4. **Complete** — `POST /api/workflow-tasks/complete` with the commands: `"command"`
   for one, `"commands": [...]` for several (send a single command in `"command"`, so
   engines that predate batches still understand it).

Commands are a flat tagged union:

```json
{"type": "schedule_activity", "step_name": "charge", "sequence": 2, "input": "...",
 "retry_policy": {"max_attempts": 3, "initial_backoff_ms": 500, "max_backoff_ms": 10000,
                  "non_retryable_error_types": ["BadRequest"]},
 "start_to_close_timeout_ms": 10000, "schedule_to_close_timeout_ms": 120000}
{"type": "start_timer",       "step_name": "wait",   "sequence": 3, "delay_ms": 60000}
{"type": "await_signal",      "step_name": "gate",   "sequence": 4, "signal_name": "approved"}
{"type": "record_marker",     "step_name": "add-fraud-check", "sequence": 5, "marker": "version", "data": "1"}
{"type": "cancel_step",       "step_name": "gate",   "sequence": 4, "reason": "lost a race"}
{"type": "complete_workflow", "output": "..."}
{"type": "fail_workflow",     "error": "..."}
{"type": "noop"}
```

- **`retry_policy`** (optional) retries a failed attempt before the workflow sees
  the error. It is recorded with the step, so retries continue across restarts,
  and it overrides any policy the engine has for that step name. Failures whose
  `error_type` is in `non_retryable_error_types` are not retried (send the list only
  when it is non-empty: an engine before 0.2.0 refuses the field).
- **`start_to_close_timeout_ms`** / **`schedule_to_close_timeout_ms`** (optional) bound
  one attempt (from dispatch, heartbeats or not) and the whole step (every attempt and
  backoff). The activity task carries `start_to_close_timeout_ms` so the worker can stop
  the work too. Send them only when set: an engine that predates them refuses the field.
- **`cancel_step`** closes a step that is still open (a race's loser): it names an
  existing step and takes no sequence of its own. The step comes back with status
  `CANCELLED`; waiting on it is an error. A step that already has a result is left alone.
  Engines before 0.2.0 refuse the command.
- A worker turned away because its step was cancelled or its workflow finished gets a
  `409` with `"cancelled": true` from heartbeat and complete; stop the work.
- **`record_marker`** is how `getVersion` / `sideEffect` work: a step that
  completes as it is recorded. In `steps[]` it comes back with `"kind": "MARKER"`,
  `"marker": "version"` or `"side_effect"`, and the value in `output`. A version
  marker consumes a sequence only when it is there: an execution that predates
  the `getVersion` call has no marker at that position and gets the default
  version (-1) without moving the sequence, so its old history still lines up.

### Parallel steps

Workflow code can start several steps before waiting on any of them. Replay then
collects every new command the code issues until it blocks on a step with no result,
and returns them as one batch:

```json
{"commands": [
  {"type": "schedule_activity", "step_name": "charge", "sequence": 3},
  {"type": "schedule_activity", "step_name": "email",  "sequence": 4},
  {"type": "start_timer",       "step_name": "sla",    "sequence": 5, "delay_ms": 60000}]}
```

- Steps the batch opens take consecutive sequences after the last one in `steps[]`.
  A `complete_workflow` / `fail_workflow`, if any, comes last; `noop` can't be in a
  batch. A malformed batch is refused whole and the task stays pending.
- The engine asks for a decision after **every** step result, even while other steps
  are in flight, so code waiting on one result continues as soon as it lands.
- A step whose attempt failed and will be retried is sent as `SCHEDULED`: it has no
  result yet, and workflow code must keep waiting on it.
- Each resolved step carries `resolved_at` (RFC 3339) and the task carries `started_at`:
  an SDK's deterministic clock reads them (`ctx.now()` in TypeScript, Python and Java).
- Each resolved step carries `resolved_event_id`. "Wait for whichever finishes first"
  must pick the step with the lowest one, not the first in code order: the order of
  results is fixed in history, so every replay makes the same choice. (A plain
  `Promise.race` would pick by call order once both are resolved, which breaks on
  replay; the SDKs offer `ctx.waitAny` / `ctx.wait_any` instead.)
- Workflow code may finish while steps are still in flight (a lost race); the engine
  abandons them and turns their workers away.

| | TypeScript | Python | Java | Go |
|---|---|---|---|---|
| start without waiting | `ctx.activity(...)` (don't `await` yet), `ctx.sleep`, `ctx.waitForSignal` | `ctx.activity_async`, `sleep_async`, `wait_for_signal_async` | `ctx.activityAsync`, `sleepAsync`, `waitForSignalAsync` | `ExecuteActivityAsync`, `NewTimer`, `WaitForSignalAsync` |
| wait for all | `Promise.all([...])` | `ctx.wait_all(*futures)` | `ctx.waitAll(futures...)` | `f.Get()` on each |
| wait for the first | `ctx.waitAny([...])` | `ctx.wait_any(*futures)` | `ctx.waitAny(futures...)` | `ctx.WaitAny(futures...)` |
| cancel a step | `ctx.cancel(p)`, `{ cancelRest: true }` | `future.cancel()`, `cancel_rest=True` | `future.cancel()`, `waitAny(true, ...)` | `ctx.CancelStep(f)` |
| deterministic clock / random | `ctx.now()`, `ctx.random()` | `ctx.now()`, `ctx.random()` | `ctx.now()`, `ctx.random()` | `ctx.Now()`, `SideEffect` |
| signal wait with a timeout | `waitForSignal(n, s, { timeoutMs })` | `wait_for_signal(n, s, timeout=)` | `waitForSignal(n, s, Duration)` | `WaitAny` with `NewTimer` |

### Payloads

Payloads are bytes. Each travels as a JSON string (`input`, `output`,
`payload`, `data`), and the engine adds a `<name>_base64` twin when the bytes
are not valid UTF-8 (a JSON string cannot carry those exactly). Requests may
send either; the base64 form wins. The SDKs send strings as text, byte arrays as
base64 and anything else as JSON, and decode results as text unless asked for
`json` or `bytes`.

### Clusters and restarts

Give a worker every node's address. A follower answers `503` with
`{"leader_http": "host:port"}` (and an `X-Workflowd-Leader` header); move there.
A `503` with no leader (an election is running) or an unreachable node means
"try the next address after a short backoff". Every new engine (a restart, a new
leader, a new term) starts with no registrations, so after any of those, call
`POST /api/workflow-types` again before polling.

### Heartbeats

An activity running longer than its `lease_duration_ms` must call
`POST /api/activity-tasks/heartbeat {"workflow_id", "lease_token", "worker_id"}`
(the SDKs do it every third of the lease). `409` means the lease is gone: stop.

### The one hard part: how replay suspends

When replay reaches a step with no recorded result, the workflow function must
stop **without workflow code being able to intercept it**. This matters more
than it sounds: workflows legitimately write

```js
try { await ctx.activity('charge', x); } catch (e) { compensate(); }
```

If your suspend signal is an ordinary exception, that `catch` swallows it and
compensation runs on *every replay pass*. Each language needs its own escape
hatch:

| Language | Mechanism | Why it's safe |
|---|---|---|
| Go | `runtime.Goexit` on the workflow's own goroutine | `recover()` cannot stop it. (Until Phase 5 this was panic/recover, which a `recover()` in workflow code could swallow.) |
| Python | subclass `BaseException` | `except Exception` doesn't catch it |
| Java | subclass `Error` | `catch (Exception)` doesn't catch it |
| **TypeScript** | **a promise that never settles** | `catch` catches *everything* in JS, so suspension must not be a throw at all |

The TypeScript case is the interesting one — and it is not hypothetical. The
first version of that SDK used a thrown sentinel, and a workflow with a
`try/catch` around an activity silently stalled. It is now a non-settling
promise raced against a suspend signal, which workflow code cannot observe.

### Determinism

Replay only works if the same history produces the same sequence of calls. In
every language: no clocks, no randomness, no I/O, no concurrency inside workflow
code — only through the context. Anything else diverges. SDKs detect divergence
(the call at position *n* must match the recorded step at position *n*) and
report it as a replay error, which fails the workflow rather than corrupting it.

## Activities are separate, and simpler

Activities are where real side effects happen, and they need no replay at all —
just "run this, report the result". Poll `GET /api/activity-tasks`, do the work,
`POST /api/activity-tasks/complete`. Present the `lease_token` you were given;
if it is rejected, the task was reassigned and your result must be discarded. The task
carries `attempt` (from 1) and `activity_id` (`<workflow_id>/<sequence>`, the same on every
attempt: an idempotency key for the activity's side effect).

A failure is `{"error": "..."}`, optionally with `"error_type"` (the kind of failure;
the SDKs send the exception's class name, which a retry policy can list in
`non_retryable_error_types`) and `"non_retryable": true` (fail the step now, whatever
the policy: a 4xx answer that will never succeed). An engine before 0.2.0 refuses
those two fields with a `400 unknown field`; the SDKs then resend without them.

Activity workers have been language-agnostic since Week 3 — the SDKs here simply
bundle a loop for convenience.

## Publishing the SDKs

0.1.0 is out on npm (published by hand on 2026-10-22, since a brand-new npm package's first
upload can't come from CI) and on PyPI (published by this workflow through trusted
publishing, the same day). Maven Central has not been published yet. Everything is in place
for a manual release (0.1.1 went out this way on npm and PyPI, the same day):
[`.github/workflows/release-sdks.yml`](../.github/workflows/release-sdks.yml) builds the
three packages from `main` and publishes them: npm and PyPI through trusted publishing
(GitHub's OIDC token, no secrets), Maven Central with tokens stored as repository secrets.
CI's `sdk-packages` job builds and validates them on every PR (npm pack, `tsc` against
the declarations, `python -m build` + `twine check`, `mvn verify` with sources and
javadoc jars), and `scripts/sdk-version.py` fails if the six version strings disagree.

The names were free on 2026-10-21. Changing one before the first publish means editing
`typescript/package.json` (`name`), `python/pyproject.toml` (`name`), and for Java the
`pom.xml` `groupId`/`artifactId` plus the `package` line in `Workflowd.java` and the
import in `Example.java`.

**One-time setup (owner only).** For Maven, store each token with `gh secret set NAME -R
Olayanju-1234/durable-workflow-engine`, which prompts for the value so it never lands in
your shell history.

1. **npm** (no secret: trusted publishing). A brand-new package's first upload has to be
   done by hand (`npm login`, then `npm publish --access public` in `typescript/` with
   LICENSE and NOTICE copied in). Then, on the package's npm page under Settings → Trusted
   Publisher, add GitHub Actions: `Olayanju-1234` / `durable-workflow-engine`, workflow
   `release-sdks.yml`, no environment, with "Allow npm publish" ticked (without it, CI can
   only stage a release for manual approval). Set up for `workflowd` on 2026-10-22. The job
   installs npm 11.5.1+, which trusted publishing needs, and keeps `--provenance`.
2. **PyPI** (no secret: trusted publishing). Create a pypi.org account with 2FA. Under
   Account settings → Publishing, add a pending GitHub publisher: project `workflowd`,
   owner `Olayanju-1234`, repository `durable-workflow-engine`, workflow `release-sdks.yml`,
   environment left blank. The workflow then proves who it is with GitHub's OIDC token,
   and the first upload creates the project. No API token exists to leak or rotate.
3. **Maven Central** (`MAVEN_CENTRAL_USERNAME`, `MAVEN_CENTRAL_PASSWORD`,
   `MAVEN_GPG_PRIVATE_KEY`, `MAVEN_GPG_PASSPHRASE`).
   - Sign in to central.sonatype.com with GitHub; that verifies the
     `io.github.olayanju-1234` namespace. Under Account, generate a user token: its
     username and password are the first two secrets.
   - Make a signing key and publish its public half, which Central checks:
     ```sh
     gpg --quick-generate-key "Joseph Olayanju <your email>" rsa4096 sign 2y
     gpg --list-secret-keys --keyid-format long      # note the key id
     gpg --keyserver keyserver.ubuntu.com --send-keys <KEYID>
     gpg --armor --export-secret-keys <KEYID> | gh secret set MAVEN_GPG_PRIVATE_KEY -R Olayanju-1234/durable-workflow-engine
     gh secret set MAVEN_GPG_PASSPHRASE -R Olayanju-1234/durable-workflow-engine
     ```

**Each release.**

1. On `main`, bump the version in the six places `scripts/sdk-version.py` checks (the
   three manifests and the `SDK_VERSION` strings) and add a `## [<version>] - YYYY-MM-DD`
   heading in `CHANGELOG.md` above that release's notes. Merge that.
2. Dry run, which builds and validates everything and publishes nothing:
   `gh workflow run release-sdks.yml -R Olayanju-1234/durable-workflow-engine -f registries=all -f dry_run=true`
3. Publish: the same command with `-f dry_run=false`. Use `-f registries=npm` (or
   `pypi`, `maven`) to retry one registry. A version can be published only once per
   registry, so bump the version to fix a bad release.
4. Tag it: `git tag -a sdks-v<version> -m ... && git push origin sdks-v<version>`.

Maven Central validates the bundle and then publishes it automatically; it can take
30 minutes or more to show up in search.


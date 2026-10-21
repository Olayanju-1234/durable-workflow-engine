# SDKs — write workflows in any language

The engine owns durability (history, replay position, timers, retries, leases).
An SDK owns **your workflow code**. They talk over a tiny protocol, so workflow
logic can live in any language.

| SDK | Path | Status |
|-----|------|--------|
| Go | [`../workflow/`](../workflow/), [`../worker/`](../worker/), [`../client/`](../client/) | ✅ tested end to end, including leader failover (gRPC) |
| TypeScript / Node | [`typescript/`](typescript/) | ✅ run end to end in CI on every PR (Node 20) |
| Python | [`python/`](python/) | ✅ run end to end in CI on every PR (Python 3.12) |
| Java | [`java/`](java/) | ✅ compiled (`--release 11`, lint as errors) and run end to end in CI on every PR (JDK 21) |

CI runs each SDK's example worker against a freshly built engine with
[`scripts/sdk-e2e.sh`](../scripts/sdk-e2e.sh): an approved order must complete,
and a rejected one must run its compensation and fail. Run it locally with
`./scripts/sdk-e2e.sh typescript` (or `python`, `java`).

Every SDK here is **dependency-free**: Node's `fetch`, Python's `urllib`, and
Java's `HttpClient` are all built in. No protobuf toolchain. Each is also ready to
publish (not published yet; see [Publishing](#publishing-the-sdks)):

| SDK | Package | Install, once published |
|-----|---------|-------------------------|
| TypeScript | npm `workflowd` ([`package.json`](typescript/package.json), types in [`workflowd.d.mts`](typescript/workflowd.d.mts)) | `npm install workflowd` |
| Python | PyPI `workflowd` ([`pyproject.toml`](python/pyproject.toml)) | `pip install workflowd` |
| Java | Maven `io.github.olayanju-1234:workflowd` ([`pom.xml`](java/pom.xml)), class `io.github.olayanju1234.workflowd.Workflowd` | see [`java/README.md`](java/README.md) |

Until then, copy the one file into your project.

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
4. **Complete** — `POST /api/workflow-tasks/complete` with one command.

Commands are a flat tagged union:

```json
{"type": "schedule_activity", "step_name": "charge", "sequence": 2, "input": "..."}
{"type": "start_timer",       "step_name": "wait",   "sequence": 3, "delay_ms": 60000}
{"type": "await_signal",      "step_name": "gate",   "sequence": 4, "signal_name": "approved"}
{"type": "complete_workflow", "output": "..."}
{"type": "fail_workflow",     "error": "..."}
{"type": "noop"}
```

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
if it is rejected, the task was reassigned and your result must be discarded.

Activity workers have been language-agnostic since Week 3 — the SDKs here simply
bundle a loop for convenience.

## Publishing the SDKs

Nothing has been published. Everything is in place for a manual release:
[`.github/workflows/release-sdks.yml`](../.github/workflows/release-sdks.yml) builds the
three packages from `main` and publishes them with tokens stored as repository secrets.
CI's `sdk-packages` job builds and validates them on every PR (npm pack, `tsc` against
the declarations, `python -m build` + `twine check`, `mvn verify` with sources and
javadoc jars), and `scripts/sdk-version.py` fails if the six version strings disagree.

The names were free on 2026-10-21. Changing one before the first publish means editing
`typescript/package.json` (`name`), `python/pyproject.toml` (`name`), and for Java the
`pom.xml` `groupId`/`artifactId` plus the `package` line in `Workflowd.java` and the
import in `Example.java`.

**One-time setup (owner only).** Store each token with `gh secret set NAME -R
Olayanju-1234/durable-workflow-engine`, which prompts for the value so it never lands in
your shell history.

1. **npm** (`NPM_TOKEN`). Create an npmjs.com account with 2FA. Under Access Tokens,
   generate a granular token with read and write access to all packages (a new package
   name can't be selected before it exists) and "bypass 2FA" ticked, so CI can publish.
   npm caps the lifetime of write tokens, so set a reminder to rotate it. The workflow
   publishes with `--provenance`, which links the package to the workflow run.
2. **PyPI** (`PYPI_API_TOKEN`). Create a pypi.org account with 2FA, then an API token
   scoped to the entire account (a project-scoped token needs the project to exist).
   After the first upload, replace it with a token scoped to `workflowd`.
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
   three manifests and the `SDK_VERSION` strings; for 0.1.0 they are already set) and
   rename `## [Unreleased]` in `CHANGELOG.md` to `## [0.1.0] - YYYY-MM-DD`. Merge that.
2. Dry run, which builds and validates everything and publishes nothing:
   `gh workflow run release-sdks.yml -R Olayanju-1234/durable-workflow-engine -f registries=all -f dry_run=true`
3. Publish: the same command with `-f dry_run=false`. Use `-f registries=npm` (or
   `pypi`, `maven`) to retry one registry. A version can be published only once per
   registry, so bump the version to fix a bad release.
4. Tag it: `git tag sdks-v0.1.0 && git push origin sdks-v0.1.0`.

Maven Central validates the bundle and then publishes it automatically; it can take
30 minutes or more to show up in search.


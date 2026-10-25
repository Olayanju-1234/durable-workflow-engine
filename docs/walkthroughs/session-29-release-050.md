# Session 29 walkthrough: 0.5.0, see it, fix it, trust it

This session shipped workflowd 0.5.0 in eight PRs (#57 to #64) and deployed it to both engines on
the VPS. 0.4.0 made one engine safe to share between apps. 0.5.0 is about running it day to day:
seeing what is happening, fixing a failed workflow without starting over, and being able to trust
the engine with sensitive data and with your other tools. Here is each piece, in terms you know
from Node, NestJS and Mongo.

## 1. A live dashboard (#58)

**Before.** The dashboard asked the engine for the whole workflow list every 2 seconds and redrew
the page. Your selection and scroll position were lost on every redraw, and there was no link you
could paste to a colleague.

**Server-sent events (SSE).** SSE is a plain HTTP response that never ends: the server keeps
writing small `event: ... data: ...` messages down it. It is WebSockets' simpler sibling, one way
only (server to browser), which is all a dashboard needs. `GET /api/events` is that stream.

**Where the events come from.** Inside the engine there is now a small pub/sub, like an
`EventEmitter`: whenever a workflow's summary in the visibility index changes, the engine publishes
a "this workflow changed" message to everyone subscribed in that namespace. Two rules keep it safe:

- **Publishing never waits.** Each subscriber has a buffer. If a browser is slow and its buffer is
  full, the engine doesn't block (that would stall every workflow); it marks the subscriber as
  "lagged" and sends it a `resync` event, meaning "reload the list, you missed something".
- **Bursts are merged.** Changes are collected for 200 ms and sent together, so a workflow that
  runs ten steps in a second doesn't send ten messages.

A ping every 15 seconds keeps proxies from closing an idle connection, and a header tells nginx not
to buffer the stream (otherwise nginx would hold the messages back).

**In the browser.** The built-in `EventSource` can't send an `Authorization` header, so the page
reads the stream with `fetch` and a stream reader instead. Each message updates one row in place,
by key, the way React reconciles a list by `key`, so selection and scroll stay put. If the stream
isn't available (an older engine answers 404) the page falls back to polling.

**Navigable.** Each workflow has its own URL (`/workflows/<id>?namespace=...&tab=...`), the drawer
shows the full event history as a timeline with pretty-printed payloads, and the list has filters
for status, type, start time, namespace and search.

## 2. Reset and bulk actions (#59, ADR-0021)

**The problem.** A workflow failed at step 4 because of a bug in your activity. You fix the bug and
deploy. Before, the only option was to start a brand new workflow, which would run steps 1 to 3
again: charge the card again, send the email again.

**Reset.** Remember the engine's core idea: a workflow *is* its history, and the code is replayed
against it. So "go back to before step 4" is just "start a new run whose history is the old run's
history cut just before step 4". When the engine replays it, steps 1 to 3 are already recorded as
done, so they don't run again; step 4 is not, so it runs, with your fixed code.

Details that make it correct:

- The copied events are byte-for-byte the old ones (same ids, same timestamps), so replay sees
  exactly what the old run saw. Only the first event changes: a new run id and a note saying "reset
  from run X at event N".
- Signals that arrived *after* the cut are copied onto the new run, so nothing a caller sent is lost.
  Cancel requests are not, or the new run would be cancelled immediately.
- The old run is archived, not deleted (like continue-as-new in 0.3.0).
- Writing several events as the new run's start must be all-or-nothing. The store writes them to a
  `.next` file and renames it into place, a trick you may know from atomic file writes in Node:
  a rename either happened or it didn't.

By default the engine picks the cut point for you: just before the last step that failed. `ctl
reset <id> --event N` picks another.

**Bulk actions.** `workflowd ctl cancel|terminate|reset` now take a filter (`--status FAILED
--type Checkout`) and ask you to type `yes` first, like a careful `db.collection.deleteMany` with a
confirmation. The dashboard got checkboxes and a bulk bar with the same confirmation.

## 3. The pulse: numbers without Prometheus (#60)

The engine already counted things for Prometheus (`/metrics`). But that needs someone to run
Prometheus and Grafana. Now the engine samples its own counters every 10 seconds and keeps an hour
of history in memory, a ring buffer like a fixed-size array you overwrite in a loop. `GET
/api/metrics` returns it per namespace: workflows started and finished, failure rate, how long
steps take (p50 and p95, meaning "half are faster than" and "95% are faster than"), and how many
tasks are waiting in the queues. The dashboard draws it as three small charts with a table view.

## 4. Encryption at rest (#61, ADR-0022)

**The idea.** History files contain your payloads: emails, order details, maybe tokens. Anyone who
can read the disk or a backup can read them. With `--encryption-keys keys.json`, every event is
encrypted before it is written, using AES-GCM (the same algorithm Node's
`crypto.createCipheriv('aes-256-gcm', ...)` gives you).

**How it stays safe.**

- AES-GCM also proves the data wasn't changed. On top of that, each encrypted event is "bound" to
  its event id and key id (the AAD, extra data that must match on decrypt). So an attacker can't
  swap event 5 and event 6, or relabel one with another key's id.
- **Rotation by key id.** The key file lists several keys, each with an id, and says which one is
  active. New events use the active key; old events name the key they used, so they still decrypt.
  To rotate: add a new key, make it active, keep the old one.
- A missing key is an error, never "show the encrypted bytes as if they were text".
- Unencrypted histories stay readable, so turning encryption on doesn't touch old data unless you
  ask (`--encrypt-all` rewrites everything with the engine stopped; `--decrypt-all` undoes it).

**The catch.** If every copy of a key is lost, the histories encrypted with it are gone for good.
That is why it is off in production: where the key lives and how it is backed up is Joseph's call.

## 5. Reload without restart, and an audit log (#62, ADR-0023)

**Reload.** Before, changing a token meant restarting the engine. Now the engine re-reads its token
file, namespaces file and key file when they change (it checks every 10 seconds) or when it gets
`SIGHUP`, the Unix "please reload your config" signal (nginx uses it too). It is all or nothing: if
the new namespaces file has a typo, the engine keeps the old config and logs why, instead of
half-applying it.

**Audit log.** A JSON-lines file (one JSON object per line, like a structured log) that records
every operator action: start, signal, cancel, terminate, reset, schedule changes, backups and
config reloads. Each line says who (not the token itself, but a short hash of it, so you can tell
tokens apart without leaking them), when, what, whether it worked, and from which IP. Payloads are
never written there. It is recorded at the HTTP/gRPC layer, the way a NestJS interceptor sees every
request, and a test fails if someone adds a new write route and forgets to classify it.

## 6. Tracing (#63, ADR-0024)

**The problem.** In liftoff, a request comes in, starts a workflow, and the workflow's activity
calls a webhook. In a tracing tool (Jaeger, Honeycomb, Grafana Tempo) the request's trace stopped
at the engine: nothing connected the webhook call back to the request.

**W3C Trace Context.** The standard way to pass a trace between services is a header called
`traceparent`: `00-<trace id>-<span id>-01`. A *trace* is one end-to-end request; a *span* is one
piece of work in it, with a parent. If you've used OpenTelemetry in Node, this is what its HTTP
instrumentation adds to outgoing requests.

**What the engine does with it.**

- A start that carries a `traceparent` makes the workflow run a child span of the caller's. That
  context is saved in history, so it survives restarts.
- Workflow code sees the run's span (`ctx.traceparent`), and it is the same on every replay,
  because replay must see the same values every time.
- Each activity attempt gets its own span under the run's (`task.traceparent` in TypeScript), so an
  activity can put it on the HTTP calls it makes and they join the same trace.
- With `--otlp-endpoint`, the engine also sends its spans to a collector over OTLP, the
  OpenTelemetry wire format. We encode that protobuf by hand rather than pulling in the
  OpenTelemetry Go SDK (dozens of modules), and checked the encoding with `protoc`.

## 7. Release and deploy (#64)

- npm and PyPI `workflowd` 0.5.0, published from CI. Clean installs on the laptop ran a traced
  workflow on the public demo: the caller's trace id reached both the workflow and its activity.
- Both engines run `7e1dc62`. The public demo kept its guardrails; nginx was not touched. liftoff's
  worker reconnected 15 ms after the private engine restarted, its API answered 200, and a smoke
  webhook completed. The new audit logs are recording on both engines.
- Encryption and trace export are **off** on both. They wait for Joseph's decisions.

## What needs Joseph

1. **Encryption keys:** where the key file lives on the VPS, and an off-box backup of it. Then it
   is two flags and a one-off `--encrypt-all` per engine.
2. **A trace collector**, if he wants the spans (Grafana Cloud, Honeycomb or a self-hosted
   collector), and its API key in a file.
3. **R2 bucket and credentials** for archiving (from 0.4.0, still open).
4. **Maven Central** for the Java SDK.

## Words you met

- **SSE**: a long-lived HTTP response the server keeps writing events to.
- **Reset**: a new run made from the old run's history up to a chosen event.
- **AES-GCM / AAD**: encryption that also detects tampering; AAD is extra data that must match.
- **Key id / rotation**: naming keys so new data uses a new key while old data stays readable.
- **SIGHUP**: the conventional Unix signal for "reload your config".
- **Audit log**: an append-only record of who did what, and when.
- **traceparent / span**: the standard header that carries a trace between services; a span is one
  step of work inside a trace.
- **OTLP**: OpenTelemetry's protocol for sending spans to a collector.

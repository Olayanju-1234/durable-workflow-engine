# Session 12 Walkthrough — Dashboard, JSON API & Public Site (Phase 3, Week 10)

*Plain-English tour of what was built and why, with Node/TS analogies. Written for
someone who knows Express/NestJS/Mongo but is new to Go and durable execution.*

## What this session was about

For eleven sessions the engine had no face. You could tell what it was doing by
reading log lines, scraping Prometheus, or opening `.history` files in a hex
editor. This session gave it three faces:

1. **A JSON HTTP API** — so anything that speaks HTTP can drive the engine.
2. **A dashboard** — the operational UI, compiled *into* the binary.
3. **A public website** — a landing page plus real developer documentation.

## Part 1: why we needed an HTTP API at all

Here's a constraint that catches everyone: **browsers cannot speak gRPC.**

gRPC runs on HTTP/2 with binary protobuf framing and needs trailers — things the
browser `fetch()` API simply doesn't expose. This is why Temporal, Kubernetes,
and everything else in this space ship a REST layer next to their gRPC one.

The standard fix is `grpc-gateway`: a code generator that reads annotations in
your `.proto` and emits a REST↔gRPC translation server. It works, but it's a
codegen step plus a dependency tree — and this project's whole identity is
*single binary, zero dependencies*. We'd already hand-rolled the write-ahead log
and the Prometheus registry for exactly that reason.

So `internal/server/httpapi.go` is about 200 lines of standard library:

```go
mux.HandleFunc("GET  /api/workflows",            a.listWorkflows)
mux.HandleFunc("POST /api/workflows",            a.startWorkflow)
mux.HandleFunc("GET  /api/workflows/{id}",       a.getWorkflow)
mux.HandleFunc("POST /api/workflows/{id}/terminate", a.terminateWorkflow)
```

If that looks like Express (`app.get('/api/workflows/:id', ...)`), it should —
Go 1.22 taught the standard router method matching and `{id}` wildcards, so you
no longer need a third-party router like `gorilla/mux` for basic REST. You read
the parameter with `r.PathValue("id")`, the equivalent of `req.params.id`.

### One design decision worth calling out: hand-written DTOs

The engine already has structs for all this — the protobuf-generated ones. It
would have been *less* code to just serialize those. I didn't, for two reasons:

- **Readability.** The proto enum serializes as `4` or
  `"WORKFLOW_STATUS_TERMINATED"`. The API emits plain `"TERMINATED"`. Timestamps
  are RFC3339 strings, not raw nanosecond integers, so JavaScript can do
  `new Date(x)` directly.
- **Decoupling.** The HTTP contract is a public promise. Tying it to generated
  code means a proto field rename silently breaks every client.

This is the same instinct as a NestJS response DTO: never leak your database
entity straight out of a controller.

## Part 2: the dashboard, baked into the binary

The webapp lives in `web/dashboard/` — three files: `index.html`, `styles.css`,
`app.js`. **No React, no Vite, no npm, no build step.**

That's a deliberate constraint, not laziness. The entire pitch of this project is
"download one file and run it." If shipping the dashboard required a Node
toolchain and a `dist/` folder, that promise would quietly die.

### How `go:embed` works

This is the Go feature that makes it possible, and it has no real Node
equivalent:

```go
//go:embed dashboard
var assets embed.FS
```

That magic comment tells the *compiler* to read the `dashboard/` directory off
disk at build time and stitch the bytes into the executable. At runtime,
`assets` behaves like a read-only filesystem. There's no directory to deploy
alongside the binary — the HTML *is* the binary. Imagine if `require('./index.html')`
in Node produced a single self-contained executable; that's the idea.

One gotcha that shaped the folder layout: **`go:embed` cannot reach into parent
directories** (no `../`). The directive has to live in a package whose own folder
contains the assets. That's why there's a small `web/embed.go` sitting right next
to `web/dashboard/`, instead of embedding from somewhere in `internal/`.

### What the dashboard shows

- Four stat tiles: total / active / completed / failed.
- A workflow table with status filter chips, auto-refreshing every 2 seconds.
- Click a row and a drawer slides out with the **step timeline** — each step's
  kind (activity/timer/signal), status, how many attempts it took (so a retried
  activity visibly says "3 attempts"), its error or output, and which signal a
  parked step is waiting for.
- Buttons to send a signal to, or terminate, a live workflow.

### A security habit worth internalizing

Every string the dashboard renders — workflow ids, type names, error messages,
activity outputs — originated *outside* the dashboard. If someone starts a
workflow whose id contains `<img onerror=...>`, naive rendering executes it.

So there's an `esc()` helper applied to every interpolated value:

```js
function esc(v) {
  return String(v ?? '').replace(/[&<>"']/g, (c) => ({...}[c]));
}
```

React does this for you automatically, which is exactly why it's easy to forget
when you drop down to raw `innerHTML`.

## Part 3: the public site

`web/site/` holds a landing page and `docs.html`. It is **not** embedded in the
binary — it's a static site you'd deploy to GitHub Pages or Netlify. Both pages
are fully self-contained: no CDN, no Google Fonts, no analytics. Nothing loads
from a third party.

The documentation is the part I'd point a new user at. It covers the mental
model, a quickstart, how history and replay actually work, a determinism
do/don't table, writing workflows and workers, retries and Saga, every CLI flag,
all three API surfaces (gRPC, HTTP, metrics), crash recovery, leases, and an
explicit **Limitations** section.

That last section matters. It says plainly: single node, no auth, Go only,
at-least-once, no versioning, listing is a full scan. Documentation that hides
limitations wastes people's time and burns trust the first time they hit one.

### The honest bit about integration

While writing the docs I had to confront something about how the project is
actually structured. Go has a hard rule: a package under a directory named
`internal/` can only be imported from within the same module. It's enforced by
the compiler, not convention.

Almost all of this engine lives in `internal/`. So an outside developer *cannot*
`import` the engine into their app today. What they can do is clone the repo,
register their workflow in `cmd/workflowd/main.go`, and build. External workers
can be written in any language, but must generate their own gRPC stubs from
`proto/service.proto`.

I documented that plainly rather than implying a nicer story. It also clarifies
what Phase 5 ("clean public SDK surface") is really for: moving the public API
*out* of `internal/` so `go get` works.

## Go concepts you touched this session

- **`go:embed`** — compile files into the binary. No Node analogue.
- **`embed.FS` / `fs.Sub`** — the embedded files present as a filesystem
  interface, so `http.FileServer` serves them with no idea they aren't on disk.
- **Go 1.22 routing patterns** — `"GET /api/workflows/{id}"` in the standard
  `ServeMux`, with `r.PathValue("id")`. More specific patterns beat the `/`
  catch-all, which is what lets the dashboard and API share one port.
- **`json.Decoder.DisallowUnknownFields()`** — rejects unrecognised keys instead
  of silently ignoring them, the way a `class-validator` DTO with
  `forbidNonWhitelisted` would in NestJS.
- **`http.MaxBytesReader`** — caps request body size (a body-parser limit).
- **`httptest.NewServer`** — spins up a real HTTP server on a random port for
  tests; the `supertest` equivalent.

## Where we are now

**Phase 3 is complete** — weeks 1 through 10 of 17. The engine is durable,
worker-crash-safe, retries with backoff, runs real Go workflows deterministically,
exposes metrics, and now has a dashboard and a documentation site.

Next is the biggest chunk of the project: **Phase 4, Raft consensus.** That's
where the single-node limitation gets fixed — leader election, log replication,
and surviving the loss of a whole machine rather than just a process.

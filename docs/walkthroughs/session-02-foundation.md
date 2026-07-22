# Walkthrough — Session 2: The Foundation & the History Store

> For someone who knows backend engineering (Node/TypeScript/NestJS/Mongo) but is new to Go.
> Everything here is explained in plain terms, with analogies to the stack you already know.

---

## 1. What are we even building?

A **durable workflow engine**. Think of a multi-step backend process you've built before:

```
order created → charge card → reserve stock → ship → send receipt email
```

In a normal Node service, that logic lives in memory while it runs. **What happens if the
process crashes after "charge card" but before "ship"?** The in-memory state is gone. On
restart you don't know where you were. You either re-run from the top (double-charge the
card 😱) or you bolt on DB status flags + cron jobs + retry logic to fake a memory — which is
fragile and full of idempotency bugs. Every team reinvents this badly.

**Durable execution** fixes it: the engine writes down every step's result to disk *as it
happens*. If it crashes, on restart it reads that record back and continues from exactly
where it stopped — never re-charging the card. Temporal is the famous product that does this,
but it needs Cassandra/MySQL + multiple services + Kubernetes. **We're building the
lightweight version: one binary, no database.**

---

## 2. The one big idea: an append-only history

Here's the trick that makes everything work. Instead of storing the *current state* of a
workflow ("we're on step 3"), we store the **list of things that have happened**, in order,
and never edit it — only append:

```
1. WorkflowStarted   (order #42)
2. StepScheduled      "charge card"
3. StepCompleted      "charge card" → {chargeId: "ch_abc"}
4. StepScheduled      "ship"
5. StepCompleted      "ship" → {tracking: "1Z..."}
```

This is called **event sourcing**. The current state isn't stored anywhere — you *recompute*
it by replaying the events from the top. (If you've used a Redux/event log, or Postgres's
WAL, or Kafka, it's the same idea.)

**Why this is powerful:** crash recovery becomes trivial. After a crash, the engine reads the
events back from disk, sees the last thing that happened was "step 4 scheduled but never
completed," and just re-runs step 4. The append-only log *is* the memory. No special recovery
code — recovery is just "read the log and continue."

That log is the **History Store**, and it's what we built this session. It's the literal
foundation — everything else (the state machine, the gRPC API, replication) sits on top.

---

## 3. A 5-minute Go orientation (via Node/TS)

You don't need to learn Go to follow this. Here's the Rosetta Stone for what's in our code:

| Go thing | What it is | Node/TS analogy |
|----------|-----------|-----------------|
| `go.mod` | module name + dependencies | `package.json` |
| `go.sum` | dependency checksums | `package-lock.json` |
| a **package** | all `.go` files in one folder share a namespace | a barrel module — no `import` needed between files in the same folder |
| `internal/` folder | Go *enforces* it as private to this module | a non-exported module |
| `Capitalized` name | **public** (exported) | `export function Foo()` |
| `lowercase` name | **private** to the package | non-exported |
| `struct` + methods | data + functions on it; **no inheritance** | a `class` (composition only) |
| `func (s *HistoryStore) Append(...)` | a method `Append` on `HistoryStore` | `class HistoryStore { append() {} }` |
| `*HistoryStore` (the `*`) | a **pointer** = pass by reference | passing an object (objects are refs in JS) |
| `err != nil` checks | errors are **return values**, checked explicitly | `try/catch`, but inline and mandatory |
| `defer f.Close()` | run this when the function returns | `finally { f.close() }` |
| `[]byte` | a slice of bytes | `Buffer` / `Uint8Array` |
| `interface` | a set of methods a type implicitly satisfies | TS `interface` (structural typing — same vibe) |
| `func TestX(t *testing.T)` in `_test.go` | a test, run by `go test` | a Jest `test()` |
| `go run ./cmd/x` | compile + run | `ts-node` |
| `go build` | compile to a single binary | `tsc` (but output is one executable) |
| `make test` etc. | task runner | `npm run test` (the `Makefile` is the `scripts` block) |

Two Go-isms worth internalizing:
- **No exceptions for normal errors.** A function that can fail returns `(result, error)`. You
  check `if err != nil`. It's verbose but you always see exactly where failure is handled.
- **Capitalization = visibility.** `Append` is public API; `fileLocked` is internal. There's no
  `public`/`private` keyword — the case of the first letter *is* the keyword.

---

## 4. What we built, file by file

```
proto/event.proto              the "vocabulary": what events can exist
internal/history/event.pb.go   auto-generated Go from that .proto (don't hand-edit)
internal/history/store.go      the History Store itself — write & read events safely
internal/history/store_test.go the proof it survives a restart
cmd/workflowd/main.go          the actual program you run
```

### `proto/event.proto` — the vocabulary
This defines the *types* of events, like defining your DTOs or a DB schema. We use **Protocol
Buffers** (protobuf): you write a schema, and a tool generates typed code + an efficient binary
format. Analogy: like `class-validator` DTOs, but the wire format is compact binary instead of
JSON, and the codegen gives you the types for free. We chose it because the gRPC API later
*requires* protobuf anyway, so we reuse one schema for both disk and network.

Our events: `WorkflowStarted`, `StepScheduled`, `StepCompleted`, `StepFailed`,
`WorkflowCompleted`, `WorkflowFailed`. They're wrapped in one `HistoryEvent` envelope that
carries an `event_id` (1, 2, 3…) and a `timestamp`.

### `internal/history/store.go` — the heart
Two public methods: **`Append`** (write one event) and **`ReadAll`** (read them all back).

**How an event is laid out on disk.** We can't just dump bytes — when reading back we need to
know where each event ends, and detect if the disk corrupted it. So every event is written as a
**record** with a small header:

```
┌───────────┬──────────────┬─────────────────────────────┐
│ 4 bytes:  │ 4 bytes:     │ N bytes:                    │
│ length N  │ CRC32 of     │ the event, as protobuf bytes │
│           │ the payload  │                             │
└───────────┴──────────────┴─────────────────────────────┘
```

- **length** → so the reader knows exactly how many bytes this event is (where it ends and the
  next begins).
- **CRC32** → a checksum. When reading, we recompute it and compare. If a byte got flipped (bit
  rot, half-written during a crash), the check fails and we refuse to return garbage. Like an
  ETag/hash that proves the bytes are intact.
- **payload** → the actual event.

**`Append` step by step:**
1. Reject unsafe workflow IDs (e.g. one containing `/` that could write outside our folder).
2. `proto.Marshal(event)` — turn the event object into bytes (serialize).
3. Build the record: `[length][crc][payload]` in one buffer.
4. `s.mu.Lock()` — take a mutex so two concurrent writers can't interleave bytes (a classic
   race condition; same reason you'd lock a critical section).
5. Write the record, then **`f.Sync()`** — this is `fsync`, the important bit. Normally the OS
   *buffers* writes in memory and flushes later; if the power dies, you lose them. `fsync`
   forces the bytes onto the physical disk *before* we report success. So "Append returned
   nil(no error)" genuinely means "this survived." Correctness over speed (fsync is slow; we
   optimize later).

**`ReadAll` step by step:** open the file and loop — read the 8-byte header, get the length and
expected CRC, read exactly that many payload bytes, recompute the CRC and compare, decode the
protobuf back into an event, add it to the list. Stop at end-of-file. If the *last* record is
half-written (the engine crashed mid-append), we detect the short read and treat it as the end
of the log — the torn tail is harmless because we never trust a record until its CRC checks out.

### `internal/history/store_test.go` — the proof
This is the deliverable the blueprint calls the first proof of durability. In plain English the
main test does: **write 10 events → close everything (drop all in-memory state) → reopen the
store fresh from disk → read all 10 back → assert they're identical.** That's literally
"survives a restart." Two more tests check that a missing workflow returns empty (not an error)
and that a deliberately corrupted byte is caught by the CRC.

Run it: `make test`. It takes ~0.2s for 10 writes — that pause *is* the fsyncs hitting disk.
Proof the durability is real, not faked.

### `cmd/workflowd/main.go` — the program
`cmd/` is the Go convention for "things you actually run" (each subfolder = one binary). Right
now `workflowd` (the "d" = daemon, like `sshd`) just opens the history store and reports it's
ready. With `--demo` it writes one event and reads it back, to prove the whole stack works from
a real binary. The gRPC server that workers talk to comes in Week 3.

---

## 5. The rest of the foundation (the "company," not just the code)

Because this could become a real product, we set it up like one:
- **`CLAUDE.md`** — instructions auto-loaded by the AI each session (so context survives).
- **`docs/`** — `ARCHITECTURE.md` (how it all fits), `ROADMAP.md` (the 17-week plan with
  checkboxes), `adr/` (Architecture Decision Records — a short memo per big decision, so
  "why did we do it this way?" is never lost), `SESSION_LOG.md`, and these walkthroughs.
- **`README.md`** — the public pitch.
- **`LICENSE`** (Apache-2.0) + **`NOTICE`** — open-source licensing.
- **`.github/workflows/ci.yml`** — CI: every push, a robot runs format-check + build + tests
  (like a GitHub Action running `npm ci && npm test`).
- **`Makefile`** — shortcuts: `make test`, `make build`, `make run`, `make proto`.

---

## 6. Try it yourself

```sh
make test                          # run the suite (watch it pass)
go run ./cmd/workflowd --demo      # open a real store, write+read an event
go run ./cmd/workflowd --demo      # run again → events_on_disk goes 1 → 2 (durable!)
go run ./cmd/workflowd --version   # prints the version
cat data/demo-workflow.history     # the raw binary log (mostly unreadable — that's the point)
```

**VS Code tip:** install the Go extension's tools when it prompts (or run the command palette →
"Go: Install/Update Tools"). That gives you hover-docs, go-to-definition, and inline error
squiggles — hugely helpful for reading Go as a newcomer.

---

## 7. What's next (Session 3 → Week 2)

The **state machine** (`internal/state`): the "brain" that reads a workflow's history and
decides what to do next — schedule the next step, complete the workflow, or fail it. It's a
*pure function* of the history (same history in → same decision out), which is exactly what
makes crash recovery just "replay the log." That property is the whole ballgame, and we'll
build it next.

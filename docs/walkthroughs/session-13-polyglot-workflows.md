# Session 13 Walkthrough — Polyglot Workflows (TypeScript, Python & Java)

*Plain-English tour of what was built and why, with Node/TS analogies. Written for
someone who knows Express/NestJS/Mongo but is new to Go and durable execution.*

## The change of plan

This session started on Raft (Phase 4 — making the engine survive losing a whole
machine). About 30% in, you said workflows shouldn't be Go-only. That's a
product decision that beats a reliability one for a project with no users yet, so
we parked Raft and did this instead.

Parking cost almost nothing: the Raft *protocol design* (`proto/raft.proto`) is
still in the repo as groundwork, and the half-written implementation file was
deleted rather than left broken.

## First, the thing that was already true

"Multi-language support" sounds like one feature. It's actually two, and one of
them has worked since Week 3:

| Layer | What it does | Language support |
|---|---|---|
| **Activities** | The real work — charge a card, send an email | **Always any language.** It's just an API call. |
| **Workflows** | The orchestration logic — "charge, then ship, and if charging fails, release" | Was Go only. |

Activities were never the problem. A worker just asks "got any work?", does it,
and reports back — no replay, no state. Any language that can make an HTTP or
gRPC call could always do that.

The *workflow function* was the Go-locked part, because replay meant literally
re-executing Go code inside the engine.

## The trick: the engine already had the right seam

Here's the piece of luck that made this session small. Since Week 6, the engine
has driven every workflow through one interface:

```go
type Planner interface {
    Decide(events []*HistoryEvent) (Decision, error)
}
```

Read that as: *"given everything that has happened, what should happen next?"*

Two implementations existed, both Go, both in-process. But nothing about that
interface says the answer has to come from inside the process. So we added a
third implementation that answers **over the network**:

```
engine → "here is workflow W's history. what next?"
SDK    → replays your TypeScript/Python function against that history
SDK    → "schedule the activity called 'charge'"     (exactly one command)
engine → records it durably, dispatches it, and repeats
```

The history store, crash recovery, timers, retries, leases and the dashboard
were **not touched**. A TypeScript workflow and a Go workflow travel the exact
same path through the engine — they just differ in who answers the question.

In NestJS terms: this is swapping a local service for one behind an HTTP call,
because the code depended on an interface rather than a concrete class.

## The one genuinely hard problem

There's a subtle constraint. `Decide` used to be called while the engine held its
global lock — fine for a Go function call, fatal for a network round-trip (the
whole engine would freeze while waiting for a Node process to answer).

So the remote path is **asynchronous**: the engine puts a task on a queue,
releases the lock, and applies the command whenever it comes back. That opens a
race: what if history changes while the SDK is thinking?

The fix is **optimistic concurrency**, the same idea as a version number on a
Mongo document. Every task records how many events it was based on:

```json
{ "workflow_id": "order-42", "history_length": 4, "steps": [...] }
```

When the command comes back, the engine checks the history is still 4 events
long. If a timer fired or a signal landed in the meantime, the command is stale
and gets rejected — the SDK just polls again.

This one check buys something valuable: **it makes re-dispatching safe**. If an
SDK worker dies holding a task, the engine can hand the same work to another
worker without worrying, because even if both eventually answer, only one command
can apply. Replay is a pure function of history, so deciding twice is harmless.

## Two bugs worth telling you about

Both were found by running things for real, not by reasoning.

### 1. A rejected command stranded the workflow forever

The engine validates commands before writing them (a bad step number would
produce a history it could never read again). My first version *cleared the
pending decision* and then rejected the command — leaving the workflow with no
queued task and nothing to re-dispatch it. Permanently frozen.

My own test caught it. The fix: a rejected command leaves the task **pending**,
so the timeout re-offers it. A buggy SDK now spins slowly and visibly instead of
killing workflows, and deploying a fix heals them automatically.

I verified the fix has teeth by temporarily restoring the bug: the test hangs.
(You then saw this work for real — a workflow stranded by bug #2 below completed
by itself once the fixed SDK connected.)

### 2. JavaScript cannot use exceptions for this

This is the interesting one, and it's a genuinely language-specific trap.

When replay reaches a step that hasn't run yet, the workflow function has to
*stop*. Go does this with `panic`/`recover`. Python uses an exception class
derived from `BaseException`, which `except Exception` deliberately doesn't
catch. Java uses `Error` for the same reason.

I assumed JavaScript would work the same way. It doesn't — **`catch (err)` in JS
catches literally everything**. So this perfectly reasonable workflow:

```js
try {
  await ctx.activity('charge', reserved);
} catch (err) {
  await ctx.activity('release', reserved);   // compensate
}
```

...swallowed the stop signal and ran compensation on *every replay pass*.

The symptom in testing was stark: the Python workflow completed the full order
flow first try, while the TypeScript one silently froze at exactly the step
wrapped in `try/catch`.

The fix is to stop using exceptions in JS at all. When replay hits an unrecorded
step, the SDK records the command and returns **a promise that never settles**.
The function suspends at that `await` and can never observe it, no matter how it
handles errors:

```js
const FOREVER = () => new Promise(() => {});
```

`decide()` then races the workflow function against a suspend signal. Workflow
code cannot intercept it, because there's nothing to intercept.

I fixed the SDK rather than the example, because users should never have to know
this.

## Why the SDKs have zero dependencies

The gRPC protocol ships raw protobuf history. That's the right choice for a
language with good protobuf tooling, but it means every new SDK must reimplement
the history fold before running a single workflow — plus install a protobuf
compiler.

So the HTTP transport ships the **already-reconstructed** view instead: the
workflow's input, its steps in order with their results, and any buffered
signals. That's exactly what replay needs, so the SDK is reduced to "walk my
function against this list".

The result: a complete SDK is ~250 lines using only built-ins —
Node's `fetch`, Python's `urllib`, Java's `HttpClient`. No `npm install`, no
`pip install`. Run `node example.mjs` and it works.

## What was proven, and what wasn't

I ran the TypeScript and Python workers against a live engine and drove real
workflows through them: an activity ran, a **durable timer** fired, the workflow
**parked on a signal** until it was sent, and **Saga compensation** ran exactly
once on failure (`charge` failed → `release` ran → `ship` never did).

Java is different, and I want to be clear about it: **there is no JDK on this
machine**, so that SDK has never been compiled, let alone run. `java` and
`javac` exist as macOS stubs that only print an install prompt. It's shipped as a
clearly-labelled reference implementation, not as something I've verified.

## Go concepts you touched

- **Interfaces as seams** — `Planner` is why this was a small change. Depending
  on an interface rather than a concrete type let a network implementation slot
  in with no changes to the machinery around it. Same idea as TypeScript's
  structural typing.
- **Cross-package protobuf imports** — `service.proto` now imports `event.proto`
  so a workflow task can carry history events.
- **`slices.DeleteFunc` / `slices.Contains`** — Go's generic slice helpers
  (roughly `Array.prototype.filter` / `.includes`).
- **Broadcast with channels** — the workflow queue wakes *every* waiting poller
  by closing a channel and replacing it, since each one needs to re-check whether
  the new task matches the workflow types it handles.

## Where we are now

Weeks 1–10 plus this. The engine is durable, worker-crash-safe, retries with
backoff, observable, has a dashboard — and now workflow logic can be written in
Go, TypeScript or Python.

Next is either resuming **Raft** (surviving the loss of a machine, not just a
process) or polishing the SDKs — publishing them to npm/PyPI, richer payload
types, and getting that Java SDK actually compiled.

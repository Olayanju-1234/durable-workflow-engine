# SDKs — write workflows in any language

The engine owns durability (history, replay position, timers, retries, leases).
An SDK owns **your workflow code**. They talk over a tiny protocol, so workflow
logic can live in any language.

| SDK | Path | Status |
|-----|------|--------|
| Go | [`../workflow/`](../workflow/), [`../worker/`](../worker/), [`../client/`](../client/) | ✅ tested end to end, including leader failover (gRPC) |
| TypeScript / Node | [`typescript/`](typescript/) | ✅ verified end-to-end against a live engine |
| Python | [`python/`](python/) | ✅ verified end-to-end against a live engine |
| Java | [`java/`](java/) | ⚠️ **written but never compiled** — no JDK was available on the machine it was authored on. Treat as a reference implementation until someone runs it. |

Every SDK here is **dependency-free**: Node's `fetch`, Python's `urllib`, and
Java's `HttpClient` are all built in. No npm install, no pip install, no
protobuf toolchain.

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

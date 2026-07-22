# Session 6 — Crash recovery & The Kill Test (plain-English walkthrough)

This is the payoff session. Everything we've built — the append-only log, the state
machine, the task queue — was setup for one moment: **the engine dies mid-workflow, comes
back, and finishes the job as if nothing happened.** This session makes that real, and then
proves it by literally `kill -9`-ing the engine and watching the workflow complete anyway.

## The mental model: the queue is a lie, the log is the truth

Remember the task queue from Session 4? It's an in-memory Go channel — think of it as a
JavaScript array that lives in RAM. Fast, but it has one fatal property: **when the process
dies, it's gone.** Every pending "please run step 3" message vanishes.

That sounds like a disaster for durability. It isn't — because the queue was never the
source of truth. The *history log on disk* is. The queue is just a fast cache of "work a
worker should pick up right now." We can always rebuild it by reading the logs.

Analogy: imagine a restaurant kitchen. The tickets clipped to the rail (the queue) are how
cooks know what to make next. If the power flickers and the rail clears, you don't lose the
orders — you re-print the tickets from the order log. The order log is the truth; the rail
is a convenience.

So crash recovery is exactly that re-print step: **on startup, read every workflow's log,
figure out where each one was, and re-clip the tickets that were in flight.**

## Piece 1 — finding every workflow (`ListWorkflows`)

Before we can replay all workflows, we need to know which workflows exist. Each workflow's
history is a file named `<workflow-id>.history` in the data directory. So "list all
workflows" is just "list all `.history` files and strip the suffix."

```go
func (s *HistoryStore) ListWorkflows() ([]string, error) {
    entries, err := os.ReadDir(s.dir)      // like fs.readdirSync(dir)
    ...
    for _, e := range entries {
        if strings.HasSuffix(name, ".history") {
            ids = append(ids, strings.TrimSuffix(name, ".history"))
        }
    }
}
```

`os.ReadDir` ≈ Node's `fs.readdirSync`. We skip anything that isn't a `.history` file (so a
stray `README.txt` in the data dir doesn't get mistaken for a workflow). The data directory
*is* our index of all workflows — no separate database needed.

## Piece 2 — putting each workflow back where it was (`Recover`)

For each workflow, we read its log and `Reconstruct` its state (that pure fold from
Session 3). Then we branch on the status:

| Status on disk | What it means | What recovery does |
|---|---|---|
| **WAITING** | a step was scheduled and handed to a worker when we died | re-enqueue that step (re-clip the ticket) |
| **RUNNING** | we crashed *between* recording a result and scheduling the next step | make one decision → schedules the next step |
| **COMPLETED / FAILED** | already finished | nothing to do |
| unregistered type | we don't have this workflow's code loaded | skip it, log a warning |

The subtle one is **RUNNING**. Think about the exact instant of the crash. The engine's job
after a worker finishes is a two-step dance:
1. record `StepCompleted` (durable)
2. decide + record `StepScheduled` for the next step (durable), then enqueue

If we crash *between* those, the log says "step 2 done, nothing scheduled." That's RUNNING.
Recovery just runs the decision loop one more time — which looks at the log, sees step 2 is
done, and schedules step 3. The workflow didn't lose its place; it just needed a nudge.

### The at-least-once caveat (say it out loud, don't hide it)

When we re-enqueue a WAITING step, there's a case we can't distinguish: what if the worker
*actually finished* step 3, but the engine crashed before recording `StepCompleted`? Then on
recovery we re-run step 3. That's **at-least-once** execution — a step can run more than once
after a crash.

This is fine and expected for Phase 1. We are **engine-crash-safe**, not **exactly-once**.
Exactly-once needs worker leases and idempotency, which land in Phase 3. Overclaiming here
would be a lie, and the whole project's value is *not lying about durability*.

Recovery does exactly **one** decision per workflow at startup — not a loop. Phase 1 runs one
step at a time, so one nudge is enough; the rest schedule themselves as workers report back,
same as normal operation.

We wired `Recover()` into the daemon so it runs on boot, after registering workflow types
but *before* the server accepts any traffic:

```go
engine.Register(demoDef)
if err := engine.Recover(); err != nil { ... }  // rebuild from disk first
lis, err := net.Listen("tcp", addr)             // ...then open for business
```

## Piece 3 — The Kill Test (the whole point)

A durability claim you haven't tested by actually killing the process is a wish, not a
feature. So we wrote a test that:

1. **Starts a real second process** running our engine (a 5-step workflow).
2. Lets it get to **step 3 in flight** (steps 1–2 done and on disk).
3. **`SIGKILL`s it** — signal 9, the uncatchable one. No cleanup, no "save your work"
   handler, no graceful shutdown. The OS just stops the process dead.
4. Opens a **fresh engine over the same data directory**, calls `Recover()`.
5. Drives steps 3, 4, 5 to completion and checks the final result.

### How do you kill a process from inside a Go test?

The neat trick is the **re-exec pattern**. A Go test binary can run *itself* as a
subprocess. We launch `os.Args[0]` (the compiled test binary) but tell it to run only one
special test, `TestKillHelperVictim`, and set an environment variable as a secret handshake:

```go
child := exec.Command(os.Args[0], "-test.run=^TestKillHelperVictim$")
child.Env = append(os.Environ(), "WORKFLOW_KILL_HELPER=1", ...)
child.Start()
```

`TestKillHelperVictim` skips instantly in a normal `go test` run (the env var isn't set), so
it only does real work when it's the child. It's like an `if (process.env.CHILD) { ... }`
guard in a Node script that re-spawns itself. `exec.Command` ≈ Node's `child_process.spawn`.

The child drives the workflow to step 3, writes a little "ready" file so the parent knows
it's safe to kill, then blocks forever waiting to die. The parent watches for that file,
then calls `child.Process.Kill()` — which on Unix is `SIGKILL`.

### What we assert (and why it proves durability)

After recovery finishes the workflow, we check the final log has **exactly 12 events**:

```
WorkflowStarted
step1 scheduled, step1 completed
step2 scheduled, step2 completed
step3 scheduled, step3 completed
step4 scheduled, step4 completed
step5 scheduled, step5 completed
WorkflowCompleted
```

Twelve, not more. If recovery had wrongly re-run steps 1 and 2, we'd see extra events (or
the `Reconstruct` validation would reject the log outright, because sequence numbers must be
contiguous). So "12 events, each step completed once, in order" is a strong proof of **no
duplicate execution of already-finished work** — and steps 4–5 being present proves the
workflow genuinely made progress *after* the kill.

### Does the test actually test anything? (teeth-checking)

A passing test that would pass even when the code is broken is worthless. So we did a
one-off experiment: we deleted the re-enqueue line from `Recover` and re-ran the Kill Test.
It **failed** — the workflow stalled at step 3 forever, because nothing re-clipped the
ticket, and the test timed out. Then we put the line back. That's how you know the test has
"teeth": break the feature, the test goes red.

## Why `SIGKILL` durability comes for free from Session 5

The child gets *zero* chance to flush anything on `SIGKILL`. So the only reason any of its
work survives is that every `Append` already called `fsync` before returning — the promise
from Session 5. The Kill Test is really the end-to-end proof that all that fussy fsync /
torn-tail / directory-sync plumbing actually pays off: pull the power, the log is still
correct, and the engine picks up exactly where it left off.

## Where this leaves us

**Phase 1 is complete.** We have a single-node durable workflow engine that survives a hard
crash: it persists every event, rebuilds its runtime state from disk on boot, and resumes
in-flight workflows without re-running finished work. That's the core Temporal-style promise,
in a single binary with zero dependencies.

Next up (Phase 2, Week 5): **timers and signals** — letting a workflow `Sleep("24h")` or wait
for an external event, durably, so a timer set before a crash still fires after restart.
That builds directly on the recovery machinery we just wrote.

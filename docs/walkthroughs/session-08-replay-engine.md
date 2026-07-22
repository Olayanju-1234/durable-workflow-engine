# Session 8 — The deterministic replay engine (plain-English walkthrough)

This is the session where the project becomes *magic* — the feature that makes people
say "wait, how does that work?" about Temporal. Until now, a workflow was a list of
step names (data). Now you write a workflow as a **normal Go function**:

```go
func orderWorkflow(ctx sdk.Context, input []byte) ([]byte, error) {
    reserved, err := ctx.ExecuteActivity("reserve", input)
    if err != nil {
        return nil, err
    }
    ctx.Sleep("cooldown", 40*time.Millisecond)
    approval := ctx.WaitForSignal("await-approval", "approve")
    return ctx.ExecuteActivity("ship", append(reserved, approval...))
}
```

This function might take **a month** to run (that `Sleep` could be `30*24*time.Hour`).
The process will restart many times during that month. Yet the function "resumes"
exactly where it left off, every time. How? It doesn't actually pause. It gets
**re-run from the top**, and the magic is in what the `ctx` calls do.

## The core trick: replay by re-execution

Here's the mental model. Think of the workflow function as a **recipe** and the
history log as a **checklist** of steps you've already done.

Every time the engine needs to decide "what's next for this workflow?", it runs the
function again from line 1. But each `ctx.` call checks the checklist:

- `ctx.ExecuteActivity("reserve", ...)` — "Is 'reserve' already done in history?"
  - **Yes** → return the recorded result *instantly*. Don't actually run it again.
  - **No** → "reserve" is what we should do next. Emit that decision and **stop**.

So on the very first run, `ExecuteActivity("reserve")` finds nothing recorded → the
engine schedules "reserve" and stops. A worker runs it; the result is written to
history. Next decision cycle, the function runs from the top *again*: this time
`ExecuteActivity("reserve")` finds the result and returns it immediately, execution
flows to `Sleep("cooldown")`, which finds no timer recorded → the engine starts the
timer and stops. And so on.

**The function never actually blocks or sleeps.** It races from the top to the first
unfinished step, emits one decision, and unwinds. The illusion of a long-running,
pausing function is built entirely out of fast replays over the durable log.

Analogy for a Node dev: imagine an `async` function, but instead of `await` suspending
the call stack (which can't survive a process restart), each "await point" is a
*checkpoint written to disk*. To resume, you re-run the function and fast-forward
through every checkpoint that already has an answer. The stack is rebuilt by
re-execution, not by serializing a paused coroutine.

## "Stop" = a controlled panic

How does a `ctx` call make the function *stop* when it hits an unfinished step? We use
a `panic` with a private sentinel value, and `recover` it in the engine:

```go
// inside ctx.ExecuteActivity, when the step isn't in history yet:
panic(yield{decision: scheduleReserve})
```

```go
// in the replay driver:
defer func() {
    if r := recover(); r != nil {
        if y, ok := r.(yield); ok {
            decision = y.decision   // caught our sentinel: here's what to do next
            return
        }
        panic(r)                    // a real bug in workflow code: let it crash
    }
}()
out, err := fn(ctx, input)          // if it returns instead, the workflow is done
```

Using `panic`/`recover` as control flow is unusual, but it's the clean way to unwind
the stack from deep inside the function back to the driver. (If the function *returns*
normally instead of yielding, that means every step replayed and there's nothing left
to do → the workflow completes, or fails if it returned an error.)

## Why "deterministic" is not optional

The whole trick relies on one assumption: **running the function again produces the
exact same sequence of `ctx` calls.** If it didn't, the checklist wouldn't line up.

Imagine the workflow did `if time.Now().Hour() < 12 { ctx.ExecuteActivity("morning") }
else { ctx.ExecuteActivity("evening") }`. Run it at 11am, it records "morning." The
process restarts, replay runs at 1pm — now the code wants "evening," but history says
"morning." The checklist and the recipe have diverged. State is corrupt.

That's why workflow code must be **deterministic**: no `time.Now()`, no `rand`, no
reading a database, no goroutines — nothing whose result could differ between runs.
Anything with a real-world result must go through `ctx` (an activity), because activity
*results are recorded*, so they're the same on replay.

We **enforce** a piece of this: on each `ctx` call, the replay engine checks that the
step the code is asking for matches the step history recorded at that position (same
kind, same name). If they differ, it refuses with a `non-determinism` error rather
than corrupting the workflow. `TestReplayDetectsNonDeterminism` proves it: give it a
workflow that renamed a step, and `Decide` errors out.

(We don't catch *every* kind of non-determinism yet — e.g. a `time.Now()` that doesn't
change the step order slips through. Full protection is what `workflow.Now/Random/Go`
in Phase 5 provide: deterministic replacements the engine records and replays.)

## The `Planner` seam (keeping the old model alive)

We didn't throw away the Phase-1 "list of steps" model. Instead we introduced a small
interface both models satisfy:

```go
type Planner interface {
    Decide(events []*history.HistoryEvent) (state.Decision, error)
}
```

- The old `state.WorkflowDefinition` got a `Decide` method (it just calls the existing
  logic).
- The new `sdk.Workflow` has a `Decide` method that *is* the replay engine.

The engine stores `map[string]Planner` and calls `planner.Decide(...)`. It has **no
idea** whether a workflow is a data list or Go code — and every downstream piece
(scheduling, timers, signals, recovery, the whole Kill Test) works unchanged for
both. That's the payoff of a good interface: one new capability, near-zero disruption.
(Go interfaces are *structural* — `sdk.Workflow` satisfies `Planner` just by having
the right method, so the `sdk` package doesn't even import the engine. No dependency
cycle.)

## One more cleanup: the step's "kind" now lives in history

Previously, to know whether a waiting step was an activity, a timer, or a signal, the
engine looked it up in the definition. But a code-based workflow has no step
list to look up. So we moved that fact **into the log**: a signal-wait step records a
flag and the signal name; a timer step is opened by a `TimerStarted` event. Now the
engine reconstructs each step's kind straight from history, and recovery / signal
delivery / timers all work without ever consulting "the definition." The runtime is
driven purely by what happened, which is exactly the event-sourcing ideal.

## Where this leaves us

You can now write durable workflows as plain Go. The engine replays your function over
the event log to figure out what to do next, enforces that your code is deterministic
enough to replay safely, and — because it all rests on the same history + recovery
machinery from Phases 1–2 — your month-long workflow survives every crash and deploy
in between.

Next up (Week 7): **retries and Saga/compensation.** The replay model already makes
compensation natural — a failed `ExecuteActivity` returns a normal Go `error` your
workflow can `if err != nil { ... }` on, running "undo" activities. We'll add
configurable retry policies (with backoff, built on the timer service) so transient
failures retry automatically before the workflow ever sees an error.

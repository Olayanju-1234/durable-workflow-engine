# Walkthrough — Session 3: The Workflow State Machine

> For someone who knows backend engineering (Node/TypeScript/NestJS/Mongo) but is new to Go.
> Everything here is explained in plain terms, with analogies to the stack you already know.

---

## 1. Where we are

Last session we built the **history store** (`internal/history`): an append-only log on disk
that records everything that happens to a workflow, fsync'd so it survives a crash. It can
write events and read them all back. But it's just a notebook — it has no opinion about what
the events *mean* or what should happen next.

This session we built the **brain**: `internal/state`. It reads the notebook and answers two
questions:

1. **"Where are we right now?"** — `Reconstruct(events)`
2. **"So what should we do next?"** — `Decide(definition, events)`

Both are **pure functions**. That word is the whole point of this session, so let's nail it.

---

## 2. The big idea: a pure function

A **pure function** is one that, given the same input, *always* returns the same output, and
does nothing else — no database calls, no `Date.now()`, no random numbers, no network, no
writing files. In TypeScript:

```ts
// PURE: same input → same output, no side effects
const add = (a: number, b: number) => a + b;

// IMPURE: reads the clock, talks to a DB, result changes run-to-run
const impure = async (id: string) => {
  const now = Date.now();              // ← different every call
  return db.users.findOne({ id });     // ← side effect + external state
};
```

Why does this matter for a workflow engine? Because of **crash recovery**. The whole promise
is: "if the process dies, restart it and it continues exactly where it left off." How do we
make that work? We make "where it left off" a *pure function of the history on disk*.

The history is the only memory. To recover, we re-read the log and re-run the pure function.
Same log in → same answer out → same decision → the engine resumes as if nothing happened. No
"current step" column to keep in sync, no cron-job reconciliation, no double-charging. The
purity is not a stylistic nicety — **it is the recovery mechanism.**

This is also why the project's CLAUDE.md bans `time.Now()` and `rand` inside workflow code
(Phase 2). Anything non-deterministic would make replay produce a *different* answer the
second time, and recovery would break. We're not enforcing that on workflow code yet, but the
state machine itself is held to that standard starting now.

---

## 3. Part one: `Reconstruct` — "where are we?"

`Reconstruct` takes the list of events and **folds** them into a single `State` struct. If
you've used `Array.prototype.reduce` in JS, this is exactly that: walk the list left to right,
carrying an accumulator, and at the end the accumulator *is* the answer.

```ts
// The JS mental model:
const state = events.reduce((acc, event) => applyOneEvent(acc, event), emptyState);
```

In Go it's a `for` loop over the events with a `switch` on the event type. Here's the shape
(simplified from [internal/state/state.go](../../internal/state/state.go)):

```go
func Reconstruct(events []*history.HistoryEvent) (*State, error) {
    st := &State{Status: StatusNotStarted}
    openIdx := -1        // which step is "in flight" right now (-1 = none)
    lastSeq := int64(0)  // highest step number we've scheduled

    for i, ev := range events {
        switch attr := ev.GetAttributes().(type) {
        case *history.HistoryEvent_WorkflowStarted:
            st.Status = StatusRunning
            // ...record the workflow type and input...
        case *history.HistoryEvent_StepScheduled:
            // append a Step in "SCHEDULED" status, remember it's in flight
        case *history.HistoryEvent_StepCompleted:
            // mark the in-flight step "COMPLETED", store its output
        // ... StepFailed, WorkflowCompleted, WorkflowFailed ...
        }
    }
    return st, nil
}
```

A few Go things worth pointing out, mapped to what you know:

- **`switch attr := ev.GetAttributes().(type)`** is a *type switch*. Our events are a protobuf
  `oneof` — like a TypeScript discriminated union (`type Event = Started | Scheduled | ...`).
  The type switch is the Go equivalent of `switch (event.kind)` followed by narrowing. Each
  `case` gives you `attr` already typed as that specific event.
- **`for i, ev := range events`** is `for (const [i, ev] of events.entries())`.
- **`(*State, error)`** return — Go functions return errors as a second value instead of
  throwing. `if err != nil { return ... }` is the `try/catch` you'd write in Node, just
  spelled out at each call.

### The output: a `State`

```go
type State struct {
    Status       Status   // RUNNING / WAITING / COMPLETED / FAILED / ...
    WorkflowType string
    Input        []byte
    Steps        []Step   // each step + its result so far
    Output       []byte   // the final answer, once completed
    Error        string
    LastEventID  int64
}
```

The two interesting statuses to distinguish:

- **`WAITING`** — a step was scheduled but we haven't heard back from a worker yet. The
  engine should sit tight.
- **`RUNNING`** — nothing is in flight; the engine *owes a decision* (schedule the next step,
  or finish).

So `Reconstruct` doesn't just classify — it tells the engine whether it's that engine's turn
to act.

### `Reconstruct` is also a bouncer

A durable engine's log is sacred. If the log is somehow inconsistent — a "step completed"
event for a step that was never scheduled, or events recorded *after* the workflow already
finished — we must **refuse to proceed**, not silently invent a plausible state. So
`Reconstruct` validates as it folds and returns an `error` on nonsense. Rules it enforces:

- `WorkflowStarted` must be the very first event.
- No events allowed after a terminal (`WorkflowCompleted`/`WorkflowFailed`) event.
- A step result must match the step that's actually in flight — same sequence number *and*
  same name.
- Step numbers go 1, 2, 3… with no gaps, and you can't schedule a new step while one is still
  in flight (Phase 1 runs steps one at a time).

There's a whole test (`TestReconstructRejectsBadHistory`) that feeds it eight different
corrupt logs and checks each one is rejected with a clear message.

---

## 4. Part two: `Decide` — "so what's next?"

`Reconstruct` tells us where we are. `Decide` turns that into a single next action.

But there's a missing piece: to know the *next* step, the engine needs to know the **plan** —
the ordered list of steps this workflow is supposed to run. In the real product (Phase 2),
the plan comes from *running your workflow code*. We're not there yet. So for Phase 1 we use a
deliberately simple stand-in:

```go
type WorkflowDefinition struct {
    Type  string
    Steps []string   // e.g. ["reserve", "charge", "ship"]
}
```

Think of it as a hardcoded recipe. Later, "run the recipe" becomes "execute the user's Go
function and watch which steps it asks for" — but the machinery that figures out *where we are
in the recipe* (`Reconstruct`) doesn't change at all. That's the payoff of building it this
way: none of today's work gets thrown away.

`Decide` is short because `Reconstruct` did the hard part:

```go
func Decide(def WorkflowDefinition, events []*history.HistoryEvent) (Decision, error) {
    st, err := Reconstruct(events)         // where are we?
    if err != nil { return Decision{}, err }

    if st.Status != StatusRunning {        // terminal, waiting, or not started:
        return Decision{Type: DecideNoop}  // → nothing for the engine to do
    }

    tail := st.Tail()                      // the most recent step
    if tail != nil && tail.Status == StepFailed {
        return Decision{Type: DecideFailWorkflow, Error: tail.Error}  // no retries yet
    }

    done := int64(0)
    if tail != nil { done = tail.Sequence }      // how many steps are finished
    if done >= int64(len(def.Steps)) {           // ran the whole recipe?
        return Decision{Type: DecideCompleteWorkflow, Output: lastOutput}
    }

    return Decision{                              // otherwise: schedule the next step
        Type:     DecideScheduleStep,
        StepName: def.Steps[done],
        Sequence: done + 1,
        Input:    previousStepOutput,             // chain output → next input
    }
}
```

The four possible answers (`DecisionType`):

| Decision            | When                                          | What the engine will do |
|---------------------|-----------------------------------------------|-------------------------|
| `SCHEDULE_STEP`     | a step is finished (or none run yet) and more remain | record `StepScheduled`, hand the task to a worker |
| `COMPLETE_WORKFLOW` | every step in the plan is done                | record `WorkflowCompleted` |
| `FAIL_WORKFLOW`     | the last step failed                          | record `WorkflowFailed` |
| `NOOP`              | a step is mid-flight, or it's already over    | wait / do nothing |

Two Phase-1 simplifications worth knowing (both will get richer later):

- **A failed step fails the whole workflow.** No automatic retry yet — retry policies arrive
  in Week 7. The data model already supports retries (there's a `retry_count` field); we just
  don't act on it.
- **Steps run strictly one at a time, and each step's input is the previous step's output** —
  a simple pipeline. Parallel "fan-out" steps come with the replay engine in Phase 2.

### Decide does NOT touch the disk

Notice `Decide` returns a `Decision` — a plain description of what to do — but it doesn't write
anything or send anything. That's deliberate: it stays a pure function. The *caller* (the gRPC
server we build next week) is responsible for the side effects: take the `Decision`, build the
matching history event, stamp it with an id and timestamp, append it to the store, and enqueue
the task for a worker.

This is the classic **"functional core, imperative shell"** split: the brain (`state`) is pure
and trivially testable; the messy I/O lives in a thin outer layer. It's the same instinct as
keeping your business logic out of your Express route handlers.

---

## 5. Seeing it actually run

To prove the brain works before the gRPC server exists, the demo command now drives a full
workflow through the loop, playing the role of the (not-yet-built) worker itself:

```sh
go run ./cmd/workflowd --demo --data-dir /tmp/wfdemo
```

Output (trimmed):

```
msg="workflow started"        type=OrderWorkflow
msg="engine decided"          decision=SCHEDULE_STEP step=reserve
msg="worker completed step"   step=reserve sequence=1
msg="engine decided"          decision=SCHEDULE_STEP step=charge
msg="worker completed step"   step=charge sequence=2
msg="engine decided"          decision=SCHEDULE_STEP step=ship
msg="worker completed step"   step=ship sequence=3
msg="engine decided"          decision=COMPLETE_WORKFLOW
msg="workflow completed"      output=done:ship events_on_disk=8
```

Read that top to bottom and you can see the entire engine: **replay the history → decide →
record the result → repeat, until done.** Eight events landed durably on disk. That loop is
the literal template for next week's server — the only change will be that a *real, external*
worker does the steps instead of the demo doing them inline.

---

## 6. What's in the tests (the real deliverable)

The roadmap's Week-2 goal was: *"hand-built history → assert decisions."* That's
`TestDecide`: a table of little fake histories, each paired with the decision we expect.

```go
{
    name:     "first step done → schedule second, chaining its output as input",
    events:   withIDs(started("Order", "in"),
                      scheduled("reserve", 1, "in"),
                      completed("reserve", 1, "reserved")),
    wantType: DecideScheduleStep, wantName: "charge", wantSeq: 2, wantData: "reserved",
},
```

This reads like a spec: "given the workflow started and `reserve` finished with output
`reserved`, the engine should schedule `charge` next, passing `reserved` as its input." No
disk, no mocks, no setup — because the thing under test is a pure function. (`withIDs`,
`started`, `scheduled`… are tiny helpers that build fake events so the tests stay readable.)

On top of that:

- `TestDriveToCompletion` runs the whole loop in-memory and checks it finishes correctly.
- `TestDriveToFailure` checks a failed step ends the workflow.
- `TestReconstruct*` cover the fold and the eight corrupt-log rejections.

All green under Go's race detector (`go test ./... -race`).

---

## 7. One Go gotcha we hit

The protobuf-generated events use a `oneof`, and the interface that ties the variants together
(`isHistoryEvent_Attributes`) is **unexported** — lowercase, so it's private to the `history`
package. I first tried to write a helper in `cmd/workflowd` that accepted "any event
attribute" as a parameter, and the compiler refused: code outside `package history` literally
cannot name that private interface.

The fix is simple once you know it: build the whole event value and pass *that* around —
`&history.HistoryEvent{Attributes: &history.HistoryEvent_StepScheduled{...}}` — instead of
trying to pass the inner attribute by its interface type. (In TS terms: the union's internal
"brand" is module-private, so you hand over the fully-constructed object, not the bare arm.)

---

## 8. What's next — Week 3

The gRPC **task queue** in `internal/server`: the three public methods
`StartWorkflow`, `PollTask`, `CompleteTask`. This is where the pure brain meets the real
world. `StartWorkflow` records `WorkflowStarted` and runs `Decide`; `PollTask` lets an
external worker pull the next scheduled step; `CompleteTask` records the result and runs
`Decide` again to schedule what follows. The `runDemo` loop you saw above is the blueprint —
we're just swapping the inline fake worker for real ones talking over the network.

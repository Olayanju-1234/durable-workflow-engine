# Session 7 — Durable timers & signals (plain-English walkthrough)

Phase 1 gave us a workflow that survives a crash. But so far a workflow could only do
one thing: run activities back-to-back. Real workflows need to **wait** — "sleep 24
hours, then send a reminder," or "pause until a human clicks Approve." This session
adds both, and the interesting part is making the *waiting itself* survive a crash.

## The one idea to hold onto

A durable engine can't keep anything important only in memory, because memory dies
with the process. We already learned this with the task queue (Session 6): it's a
convenience cache, and the log is the truth.

Timers are the same story. If you did `setTimeout(fn, 24*3600*1000)` in Node and the
process restarted, that timer is gone — `fn` never runs. Our timers can't work that
way. So the rule is:

> **The deadline is written to the log; the in-memory timer is disposable.** After a
> crash we read the deadline back and set a fresh timer.

Signals follow the same rule: every signal is written to the log, so "did this
workflow already receive its approval?" is answered by replaying history, not by
remembering.

## First, a model change: steps got types

Until now a workflow was just a list of activity names:

```go
Steps: []string{"reserve", "charge", "ship"}
```

That can't express "sleep here" or "wait for a signal here." So a step is now a small
typed struct, and there are three kinds:

```go
state.Activity("reserve")           // run on a worker (what we had)
state.Sleep("cooldown", time.Hour)  // pause on a durable timer
state.AwaitSignal("approval", "approve") // pause until an "approve" signal arrives
```

For the common all-activities workflow, `state.Activities("reserve", "charge", "ship")`
is shorthand. (Analogy: it's like turning a `string[]` into a `Step[]` where `Step`
is a discriminated union — TypeScript would call it `{ kind: 'activity' | 'timer' |
'signal', ... }`.) The zero value is an activity, so nothing about existing workflows
changed.

## Timers

### Three new log events

- `TimerStarted` — "a timer for step N is now running; it should fire at absolute
  time T." We store the **absolute** fire time (a Unix nanosecond timestamp), not the
  duration. That's deliberate: after a restart we want the timer to fire at the same
  wall-clock moment, whether we were down for 1 second or 1 hour.
- `TimerFired` — "the timer for step N elapsed; resume the workflow."
- (Signals get `SignalReceived`, below.)

A timer step is WAITING (paused) between `TimerStarted` and `TimerFired`, just like an
activity is WAITING between "scheduled" and "completed." The symmetry keeps the state
machine simple: **something opens the step, something closes it.**

### The min-heap timer service

How does one engine efficiently manage thousands of pending timers, each with a
different deadline? You don't want a goroutine per timer (wasteful) or to scan them
all every second (slow). The classic answer is a **min-heap** — a tree that always
keeps the *smallest* element (here, the earliest deadline) at the top, cheap to read
(O(1)) and cheap to insert/remove (O(log n)).

Go ships one in `container/heap`. Our `internal/timer.Service` wraps it with a single
background goroutine that does exactly this loop:

```
forever:
  look at the earliest deadline (top of the heap)
  sleep until then
  when you wake: pop and fire every timer whose deadline has passed
```

Two subtleties, both tested:
1. **Adding an earlier timer must interrupt the sleep.** If the loop is asleep until
   "5pm" and you add a timer for "in 40ms," it must wake up and re-plan — otherwise
   your 40ms timer waits until 5pm. We do this with a `wake` channel: `Add` nudges the
   loop to recompute. (In Go, `select` over "wake channel" vs "timer channel" is the
   idiomatic way to say "sleep, but be interruptible.")
2. **An already-expired deadline fires immediately.** This is the crash-recovery case:
   we were down for an hour, a 10-minute timer's deadline is long past, so on restart
   it fires right away.

The service is **ephemeral** — it holds no durable state. On restart it starts empty,
exactly like the task queue.

### Recovery re-arms timers (the payoff)

Remember `Recover()` from Session 6? It walked every workflow and put WAITING ones
back to work. We taught it a new trick. When it finds a workflow WAITING, it now looks
at what *kind* of step is waiting:

- activity → re-enqueue the task (Week 4 behavior)
- **timer → re-arm it in the timer service, using the `fire_at` from the log**
- signal → re-check the buffer in case a signal was already waiting

So a timer set at 2:00pm for 2:05pm, with a crash at 2:01pm, is re-armed on restart
and still fires at 2:05pm. The test `TestTimerSurvivesRestart` proves it: engine 1
arms the timer and "crashes" before it fires; engine 2 recovers and the workflow
completes. We even checked the test has *teeth* — delete the re-arm line and the
workflow hangs forever, failing the test.

## Signals

A signal is an external nudge: `SignalWorkflow(id, "approve", payload)`. Think of it
as an inbox message for a running workflow.

The tricky part is **timing**. What if the signal arrives *before* the workflow gets
to the step that waits for it? (Very common: you approve an order the instant it's
placed, before the "await approval" step is even reached.) A naive engine would drop
the signal and wait forever. That's a classic distributed-systems footgun.

Our answer is **buffering**, and it falls out of event sourcing for free: every
`SignalWorkflow` call writes a `SignalReceived` event to the log, *always*, whether or
not anyone is waiting. Then:

- When a workflow reaches an `AwaitSignal` step, it checks the log for a matching
  signal that hasn't been used yet. If one is buffered, the wait completes instantly.
- If none is buffered, the step just stays WAITING until a matching `SignalWorkflow`
  arrives later.

To keep it correct when the same signal name is used twice, we match by order: the
1st wait for "approve" consumes the 1st received "approve," the 2nd wait the 2nd, and
so on. `TestSignalBufferedBeforeWait` sends the signal while an earlier step is still
running and proves the later wait picks it up with no second send.

Implementation note: a signal-wait step has **no worker task** — no cook is assigned,
because there's nothing to *do*, only something to wait for. So it records a
"scheduled" marker (to be WAITING) but enqueues nothing, and it completes when the
signal is delivered (recorded as a normal step completion, with the signal's payload
as the step's output so downstream steps can use it).

## A note on purity (why `Decide` never calls `time.Now()`)

`Decide` — the function that looks at history and says "do this next" — must stay a
**pure function**: same history in, same decision out, every time. That's what makes
replay safe. But a timer decision needs a deadline, which needs the current time,
which is *not* pure.

We resolve this by splitting responsibilities: `Decide` returns "start a timer with
delay = 1 hour" (pure — just the duration from the plan). The **engine** — which is
allowed to be impure — reads the clock once, computes `fire_at = now + 1 hour`, and
writes that absolute time into the log. Replaying that log later is fully
deterministic because the deadline is already baked in. This clock-reads-live-outside-
the-pure-core discipline is exactly what the full replay engine (next session) will
enforce for *all* workflow code.

## Where this leaves us

Workflows can now pause on time and on external events, durably. Combined with Phase
1's crash safety, that's the substance of what makes an engine "durable": a workflow
can sleep for a month across dozens of deploys and crashes and still wake up and
finish.

Next up (Week 6): the **deterministic replay engine** — replacing our list-of-steps
"plan" with an actual Go function you write, whose execution the engine replays
against history to decide what's next. That's the feature that turns this from "a
config-driven pipeline runner" into "write normal-looking Go, get durability."

*(Deferred: `CronSchedule` — running a workflow on a repeating schedule. It's a thin
layer on the timer service we just built, and slots in more naturally once the replay
engine exists. Noted in the roadmap so it isn't forgotten.)*

# Session 9 — Retries & Saga/compensation (plain-English walkthrough)

Real systems fail in two annoying ways: things fail *transiently* (a network blip; try
again and it works), and things fail *permanently* (the card is declined; no amount of
retrying helps, and now you need to undo what you already did). This session handles
both — **retries** for the first, **Saga/compensation** for the second — and finishes
Phase 2.

## Part 1 — Retries: failing quietly and trying again

The idea: if an activity fails, don't bother the workflow with it. Just run it again,
a few times, with a growing pause between attempts. Only if it *keeps* failing does the
workflow find out.

### The policy

```go
type RetryPolicy struct {
    MaxAttempts int           // total tries, including the first (1 = no retry)
    Backoff     time.Duration // wait before the first retry
    MaxBackoff  time.Duration // cap
}
```

You register one per activity: `engine.SetRetryPolicy("charge", RetryPolicy{MaxAttempts:
3, Backoff: 200ms})`. "Backoff" grows exponentially — 200ms, 400ms, 800ms… — so a
struggling downstream service gets breathing room instead of a hammering. (This is
exactly what you'd do with `axios-retry` in Node, but durable.)

### Where retries happen

Retries are an **engine** concern, invisible to workflow code. When a worker reports an
activity failed:

1. The engine records `StepFailed` in history (with the attempt number).
2. It checks the policy. If there are attempts left, it **arms a backoff timer** and
   *stops* — it does **not** tell the workflow anything yet.
3. When the timer fires, the engine re-schedules the *same* activity and hands it to a
   worker again.
4. If attempts run out, *now* the failure flows to the workflow (which fails, or
   compensates — Part 2).

We reused the min-heap timer service from Week 5 for the backoff — in fact we run a
**second** timer service dedicated to retries, separate from the one that runs `Sleep`
steps. Same data structure, different job.

### The subtle bit: retries reuse the same "slot"

Here's a design decision that matters. When "charge" is retried, is it a *new* step or
the *same* step tried again?

It has to be the **same** step. Remember the replay engine (Week 6): the workflow
function calls `ctx.ExecuteActivity("charge")` exactly once, and that call maps to *one*
position in history. If a retry created a brand-new step, the workflow's single
`charge` call and history's two `charge` entries would get out of sync, and replay
would break.

So a retry **re-opens the same sequence number**. In the history you see:

```
charge scheduled (attempt 1)
charge failed
charge scheduled (attempt 2)   ← same sequence, re-opened
charge completed
```

`Reconstruct` folds all of that into a *single* step with `Attempts: 2` and final
status `Completed`. The workflow function, replaying, calls `ctx.ExecuteActivity("charge")`
once, sees that single step is `Completed`, and gets the result — completely unaware
that it took two tries. (Analogy: it's one line item on the receipt that happens to
have been rung up twice; the customer sees one charge.)

### Retries survive crashes too

What if the engine dies *during* a backoff pause? The backoff timer was in memory —
gone. But the important fact is on disk: "charge failed, and it's only had 1 of 3
attempts." So recovery, walking the log on restart, sees a failed activity with retries
left and re-arms the retry. `TestRetrySurvivesRestart` proves it: fail on engine 1,
crash, and engine 2 finishes the retry. Durable all the way down.

## Part 2 — Saga: undoing work when you give up

Some failures are final. If the credit card is declined after 3 tries, you can't just
retry forever — and you may have *already* reserved inventory that now needs releasing.
That "undo" is called **compensation**, and the pattern of "do a series of steps, and if
one fails, undo the earlier ones" is a **Saga**.

Here's the beautiful part: **we didn't build anything for this.** It falls straight out
of the replay engine from Week 6. Because a failed activity returns a normal Go `error`,
your workflow just… handles it, like any Go code:

```go
func orderSaga(ctx sdk.Context, input []byte) ([]byte, error) {
    reserved, err := ctx.ExecuteActivity("reserve", input)
    if err != nil {
        return nil, err
    }
    if _, err := ctx.ExecuteActivity("charge", reserved); err != nil {
        ctx.ExecuteActivity("release", reserved)          // compensate: undo the reservation
        return nil, fmt.Errorf("charge failed, released: %w", err)
    }
    return ctx.ExecuteActivity("ship", reserved)
}
```

If `charge` fails (after its retries are exhausted), the `if err != nil` runs, the
`release` compensating activity executes, and the workflow fails with a clear error.
There's no special "compensation API" — compensation is *just another activity you
call in your error branch*. This is the payoff of "workflows are real code": Saga, which
is a whole framework feature in some systems, is a plain `if err != nil` here.

And it's durable and deterministic like everything else: on replay, `charge` returns
the same recorded error every time, so the workflow always takes the same compensation
branch. `TestSagaCompensationOnFailure` checks the history ends up exactly as
`reserve✓ → charge✗ → release✓ → FAILED`.

### Retries + Saga together

They compose naturally: give `charge` a retry policy *and* handle its error. Transient
failures get retried silently; only a *persistent* failure trips the compensation
branch. That's the real-world sequence: "try the payment a few times; if it truly won't
go through, release the hold and tell the customer." The `orderSaga` demo in
`workflowd --serve` wires exactly this up.

## Where this leaves us — Phase 2 is done

Step back and look at what the engine can do now. You can write a workflow as ordinary
Go that:

- runs activities on a pool of workers,
- **sleeps** for arbitrary durations,
- **waits** for external signals,
- **retries** transient failures with backoff,
- **compensates** (undoes) on permanent failure,
- and **survives a hard crash** at any point in all of the above, resuming exactly
  where it left off with no duplicated work.

That is the full durable-execution feature set — the core of what Temporal offers — in a
single Go binary with zero external dependencies.

## What's next (Phase 3)

The big remaining gap is **worker** failure. Today, if a *worker* dies mid-task (not the
engine — the worker), that task is stranded until an engine restart. Phase 3 Week 8 adds
**leases / visibility timeouts**: a worker "leases" a task for a while and must
heartbeat; if it goes silent, the engine reassigns the task to another worker. That's
what upgrades our guarantee from "engine-crash-safe" toward true "exactly-once-ish"
execution across a fleet of workers. After that: metrics and a visibility API (Week 9),
an embedded web dashboard (Week 10), and eventually multi-node replication with Raft
(Phase 4).

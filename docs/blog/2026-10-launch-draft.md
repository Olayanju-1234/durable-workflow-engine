<!--
DRAFT. Not published anywhere. For Joseph to review and rewrite in his own words.

Notes for review:
- Every number below comes from the repo or docs/SESSION_LOG.md, and the source is in
  the comment next to it. Change or cut anything that doesn't match how you remember it.
- Lines in [square brackets] are prompts only you can answer (the real moment, how it
  felt). I have not guessed them. Delete the prompt and write yours, or cut the line.
- No em dashes, on purpose. Please keep it that way when editing.
- Links use the github.io share link so "ojusentinel" doesn't show.
-->

# I built a small Temporal in Go, and it's one binary

[JOSEPH: the real moment this started. For example, the first time you saw a payment
flow or onboarding drip held together with a queue, a cron job and a status column, and
what broke. Only keep it if it happened.]

<!-- confirm: true of the backends you have worked on? -->
Every backend I have worked on eventually grows the same thing. A payment flow, an
onboarding drip, a billing cycle, an approval chain. It starts as a few lines in a
controller, then it needs to wait for something, so it becomes a queue. Then it needs to
wait for a day, so it becomes a cron job and a `status` column. Then a deploy lands at the
wrong moment and you are writing a script at night to find the orders that got charged but
never shipped.

The proper fix for this is durable execution, and the tool everybody points to is
Temporal. It is very good. It is also a cluster of services and a database to run, which is
a lot to ask of a small team that just wants its checkout flow to survive a restart.

So I wanted to see how small the idea could get. The result is **workflowd**: a durable
workflow engine in Go that ships as one binary, keeps its data in a directory of files, and
lets you write workflows as ordinary functions in Go, TypeScript or Python.

## What it looks like

```go
func Checkout(ctx workflow.Context, order []byte) ([]byte, error) {
	if _, err := ctx.ExecuteActivity("reserve-stock", order); err != nil {
		return nil, err
	}
	if string(ctx.WaitForSignal("manager-approval", "approval")) != "approve" {
		ctx.ExecuteActivity("release-stock", order)
		return nil, errors.New("rejected")
	}
	receipt, err := ctx.ExecuteActivity("charge-card", order)
	if err != nil {
		ctx.ExecuteActivity("release-stock", order) // compensate
		return nil, err
	}
	return ctx.ExecuteActivity("ship-order", receipt)
}
```

That `WaitForSignal` can wait for a manager for a week. You can kill the server in the
middle of it, deploy a new version, restart the machine, and when it comes back the
workflow is still sitting at that line, waiting. When the approval arrives, it charges
the card once.

## The one idea

Every workflow is an append-only log of events in a file. "Step 1 scheduled", "step 1
completed with this output", "timer started, fires at this time". Each record is framed with
a length and a CRC and fsync'd before the engine moves on.

To find out where a workflow is, the engine doesn't load some saved state. It runs your
function again from the top. Every call that already has a result in the log returns that
result instantly, and the first call with no result is what happens next. So the function
runs many times, but each side effect happens once.

That is the whole trick, and it is also where all the hard parts come from.

## The parts that were harder than they looked

**Replay has to be stoppable, and your code must not be able to stop the stop.** When replay
reaches a step with no result yet, the function has to halt right there. In Go I first did
that with `panic` and `recover`. Then I realised a workflow that wraps a call in
`defer func() { recover() }()` swallows it and keeps running with half a history. It now runs
each replay on its own goroutine and halts it with `runtime.Goexit`, which `recover` can't
catch. The TypeScript SDK had the JavaScript version of the same bug: the first version threw
a special error to stop replay, and a `try/catch` around an activity quietly stalled the
workflow. (JavaScript's `catch` catches everything, so the fix there is a promise that never
settles.)
<!-- sources: internal/replay/replay.go, sdks/README.md "The one hard part" -->

**Raft was its own semester.** High availability is opt-in. Give three nodes a peer list and
every history write goes through a hand-written Raft log before it counts. I wrote a checker
for the five safety properties from the Raft paper and ran it against 200 random failure
schedules, with switches that break one rule at a time to prove the checker can actually
fail. The strangest bug only showed up on CI: on a slow runner each commit on a new
leader took about a second, because heartbeats were re-sending entries that were already in
flight and the duplicates piled up until a 1024-slot queue was full.
<!-- sources: SESSION_LOG Sessions 20-21 ("960 ms ≈ 1024 × 0.94 ms") -->

[JOSEPH: honest admission here, if you want one. For example how long the Raft weeks took
compared to the plan, or the moment the CI-only bug made sense.]

In a live test, I killed the leader with `kill -9` while an order workflow was sleeping. The
next leader was serving 1.2 seconds later and the order finished 5.8 seconds after the kill.
<!-- source: SESSION_LOG Session 21 "Live check" -->

**The last phase found two bugs in the engine itself.** Writing the public Go SDK meant
running the engine the way a real user would: worker in its own process, engine restarting
underneath it. A workflow whose worker hadn't reconnected yet after a restart could get stuck
forever, and a signal that arrived while a worker was still deciding could strand a
workflow. The second one only failed on CI, and only some of the time. With the fix removed,
the end-to-end test hung in 9 of 20 runs. With it, none.
<!-- source: PR #13 description -->

## Where it is now

All 17 weeks of the plan I wrote at the start are done.
<!-- source: docs/ROADMAP.md; first commit 2026-07-22 -->
You can import it as a Go module and run workflows in your own process, test them without a
server in virtual time (a week-long workflow finishes in milliseconds), and change workflow
code while executions are in flight with `GetVersion`. There are about 20,000 lines of Go,
roughly half of them tests.
<!-- source: wc -l over *.go excluding generated code: 20,332 total, 9,907 in _test.go -->

From a fresh clone with an empty Go cache, the quickstart took 42 seconds on my laptop.
<!-- source: timed in Session 22 -->

```sh
git clone https://github.com/Olayanju-1234/durable-workflow-engine.git
cd durable-workflow-engine
go run ./cmd/workflowd --serve
go run ./examples/hello Ada      # in another terminal
```

It is pre-alpha and I want to be honest about that. It has not carried real production
traffic. There is one engine-wide lock, there is no TLS, and it has not been load-tested. The
limitations are written down in the docs, not hidden.

[JOSEPH: what you learned, in your words. The engineer framing from your Substack would fit
here.]

If you have ever written the cron-job-and-status-column version of this and wondered if there
is a smaller way, I would love for you to try it and tell me where it breaks.

Repo: https://github.com/Olayanju-1234/durable-workflow-engine
Docs and live demo: https://olayanju-1234.github.io/durable-workflow-engine/

[JOSEPH: thank-you line, if you want one.]

# Session 5 — Code review & durability hardening (plain-English walkthrough)

No new features this session. We stopped and asked: *of everything we've built, what's
quietly broken or will bite us later?* Then fixed the real ones. Here's what we found and
why it matters, in terms of the stack you already know.

## The mental model first

Our engine's whole promise is: **"once `Append` returns, that event survived — even if the
machine loses power one nanosecond later."** Every durability bug is a place where that
promise was a lie. Two of them were.

Think of the history file like an append-only log file you're writing with
`fs.appendFile` + `fs.fsync`. We write each event as a framed record:

```
[4 bytes: length N][4 bytes: CRC checksum][N bytes: the event]
```

The length tells the reader how many bytes to read next; the CRC is a fingerprint that
catches corruption (a flipped bit makes the fingerprint not match).

## Bug 1 — the torn tail (the serious one)

**The crash scenario.** A write is not atomic. If we crash *while* writing a record, the
file ends with a half-written record — a "torn tail." Say only 3 bytes of an 8-byte header
made it to disk.

Our reader already handled this on *read*: it sees a partial record at the end, shrugs, and
treats it as "end of log." Good. **But it never deleted those 3 junk bytes.** And here's the
trap: our write handle is opened in *append mode* (`O_APPEND`), which always writes at the
true physical end of the file — *after* the junk.

So the next time the engine appended an event:

```
[good record][3 bytes of junk][brand new good record]
                ^^^^^^^^^^^^^^
                framing is now wrong for everything after here
```

The reader reads the first good record fine. Then it hits the 3 junk bytes, reads them
*plus* the first 5 bytes of the next record as if they were a `[length][CRC]` header, and
computes a garbage length. In our test the junk `0xDEADBE…` was interpreted as a record
length of **3.7 gigabytes**. The whole rest of the log becomes unreadable. The workflow is
bricked.

**Node analogy:** imagine a JSON-lines file where one line got cut off mid-write. If your
parser just appends a *new* full line after the broken one without a newline in between, now
two lines are glued together and `JSON.parse` chokes — not just on the broken line, but on
the good one stuck to it. You have to clean up the broken line *before* writing more.

**The fix (`recoverTornTail` in `internal/history/store.go`).** The first time we open an
existing history file, we scan it to find the byte offset right after the last *complete,
valid* record, then `Truncate` the file to exactly that length — chopping off any torn tail
— and fsync that truncation. Now the next append lands on a clean boundary. We do this
*before* writing anything new, which is the whole point.

Important nuance: we only chop off a torn *tail*. If a record in the *middle* of the file
fails its CRC, that's real corruption (bit rot, a bad disk) — we do **not** silently delete
it; we return a hard error. Losing the tail of an interrupted write is safe (that event
never finished committing). Losing data from the middle is not.

This matters *right now* because Week 4 is "crash recovery": restart the engine and continue
workflows. The moment we do that, we'll be appending to files that may have torn tails. So
this fix is the foundation Week 4 stands on — it had to come first.

## Bug 2 — the missing directory fsync

`fsync` is the syscall that says "actually flush this to the physical disk, don't just leave
it in a memory buffer." We were calling it on the *file* after every append. Good. But on
Unix, a file and its *name* are two different things: the file's bytes live in the file, but
the entry that says "this name points to this file" lives in the **directory**, which is
itself a thing on disk that needs its own fsync.

So when we created a *brand-new* workflow's history file and wrote its first event
(`WorkflowStarted`), we fsync'd the file's bytes — but never the directory. On power loss,
you could end up with the bytes written but the *filename* never durably recorded: the file
is effectively lost, even though `Append` told the caller "it's safe." The very first event
of a workflow is exactly the one you can least afford to lose.

**The fix (`syncDir`).** When we create a new history file, we also open the data directory
and fsync *it* once. (Only on creation — appends to an existing file don't need it.)

**Node analogy:** there isn't a clean one, because this is a layer below where Node usually
operates — it's the kind of thing databases like SQLite and Postgres do religiously and most
app code never thinks about. That's the point: "durable" means caring about it.

## The smaller guardrails

- **Shutdown could hang forever** (`cmd/workflowd/main.go`). On `Ctrl-C`/SIGTERM we called
  gRPC's `GracefulStop`, which politely waits for in-flight requests to finish. But workers
  *long-poll* — they call `PollTask` and it just *parks*, holding the request open, waiting
  for work that may never come. `GracefulStop` waits for that parked call... which never
  returns... so shutdown hangs forever. Fix: wait at most 10 seconds, then force-stop. (Think
  `Promise.race([gracefulDrain, timeout(10s)])`.)

- **"Definition drift"** (`internal/state/decision.go`). A workflow's *plan* (the ordered
  list of steps) lives in code today. If you deploy a new version that renames or reorders
  steps while old workflows are mid-flight, the engine would happily replay the old history
  against the new plan and silently run *a different workflow* — the worst kind of bug,
  because nothing errors. We now check that the step names already in the history match the
  registered plan and refuse to continue if they don't. (A real fix — versioning — is a
  Phase-5 feature; this is a cheap seatbelt until then.)

- **Workflow-ID collisions** (`internal/server/engine.go`). Auto-generated IDs were
  `type-<timestamp-in-nanoseconds>`. On some machines the clock resolution is coarse enough
  that two rapid starts get the *same* nanosecond → same ID → spurious "already exists"
  error. Added a random suffix so that effectively can't happen. (Like adding a few random
  chars to a generated slug.)

- **A comment, not a code change**: `Reconstruct` deliberately allows a workflow to be marked
  *failed* while a step is still running (think timeout/cancellation — the step never reports
  back), but does *not* allow it to be marked *completed* in that situation. That asymmetry
  looked like a bug, so we wrote down that it's on purpose.

## What we deliberately did NOT fix (and why)

Two things are real but not worth fixing *today*:

1. **One big lock + re-reading the whole file on every operation.** Right now every workflow
   operation across the whole engine takes a single global lock and re-reads + re-parses the
   entire history file from disk. That's O(n²) work for a long workflow and caps how many
   workflows we can run at once. The fix is architectural (a lock *per workflow* + keeping
   reconstructed state in memory instead of re-reading). We logged it so it's a deliberate
   decision later, not a surprise — bolting it on now would be premature.

2. **Worker crashes strand workflows.** If a worker grabs a task and then dies before
   reporting back, that workflow waits forever (until an engine restart re-queues it). The
   proper fix is "leases" (a task you don't finish in time gets handed to someone else),
   which is a Phase-3 feature. For now the honest move is to *say so*: our Phase-1 durability
   protects against the *engine* crashing, not against *workers* crashing. That's a
   documentation fix, not a code one.

## How we proved the big fix actually works

We added `TestAppendAfterTornTailRecovers`: write an event, manually scribble 3 junk bytes on
the end (simulating a crash mid-write), reopen, append a second event, and assert both events
read back cleanly. Then — the important part — we **temporarily disabled the fix and re-ran
it** to confirm the test *fails* without the fix (it blew up with the 3.7 GB length we
described). A test that passes whether or not the code is correct proves nothing; we checked
that this one has teeth, then restored the fix.

Everything green: `go build`, `go vet`, `go test -race`, `gofmt`.

## Where this leaves us

Week 3 (the gRPC server) is done. This session made the storage layer actually trustworthy,
which is precisely what Week 4 (crash recovery & "The Kill Test" — kill the process
mid-workflow and prove it resumes) needs underneath it. Next session builds that.

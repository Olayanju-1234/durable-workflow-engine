# Session 17: When a write fails halfway

*Plain-English walkthrough for a Node/TS developer who is new to Go. Another
short one: the follow-up bug from Session 16, one fix, one table-driven test,
one deploy.*

## The bug

Every workflow's history is a file of records, each framed as
`[length][checksum][payload]`. The store keeps one open handle per running
workflow in a map, so it doesn't reopen the file on every event (Session 16
covered that cache).

Writing a record is one `write` call, but the operating system is allowed to
write only part of it. If the disk fills up (ENOSPC) or the disk reports an I/O
error (EIO) halfway, you end up with half a record at the end of the file, and
`Append` returns an error.

That part was already handled for crashes: when the store *opens* a file, it
scans to the last complete record and trims anything after it
(`recoverTornTail`). The gap was the live process. The handle was still in the
map, so nothing reopened the file. The engine retries failed work (timer
callbacks re-arm, retry backoffs fire, the next HTTP request comes in), and the
retry wrote a full record **after** the half one:

```
[record 1][half of record 2][record 2]
```

Reading that back, the reader trusts record 2's length header, reads that many
bytes (the rest of the half record plus the start of the full one), and the
checksum doesn't match. `ReadAll` returns `ErrCorruptRecord` and the workflow
can't be loaded. Worse, a restart can't fix it any more: the garbage is now in
the middle of the log, and recovery only ever trims the *tail*. It refuses to
throw away data in the middle, on purpose.

Node analogy: a `fs.appendFile` that died halfway through a line of NDJSON, and
your code kept appending to the same handle. Every later line is now glued to a
broken one and `JSON.parse` fails on that line forever.

## The fix

When the write or the fsync fails, drop the cached handle:

```go
// os.File.Write errors whenever it writes short, so err alone catches a torn
// record. Dropping the handle makes the next Append reopen and truncate it.
if _, err := s.writeFile(f, record); err != nil {
    s.releaseLocked(workflowID)
    return fmt.Errorf("history: write record: %w", err)
}
if err := f.Sync(); err != nil {
    s.releaseLocked(workflowID)
    return fmt.Errorf("history: fsync: %w", err)
}
```

The caller still gets the original error. The difference is what happens on the
retry: the map no longer has a handle, so `Append` goes through `fileLocked`,
which opens the file fresh and runs the same trim used after a crash. The half
record is cut off and the retry lands on a clean boundary.

### Why `releaseLocked` and not `Release`?

`Release` (from Session 16) takes the store's mutex. `Append` already holds that
mutex when the write fails. Go's `sync.Mutex` is **not re-entrant**: if a
goroutine that holds the lock calls `Lock()` again, it waits for itself forever
(a deadlock). Node has no equivalent because your JS never blocks on a lock, but
think of `await`ing a promise that only resolves after the current function
returns.

So the body of `Release` moved into `releaseLocked`, which assumes the lock is
already held. `Release` is now two lines: take the lock, call `releaseLocked`.
`Delete` had its own copy of the same close-and-forget code, so it calls
`releaseLocked` too. The `...Locked` suffix is the convention in this codebase
for "caller must hold `s.mu`" (`fileLocked` and `appendLocked` follow it too).

### Can a write be short *without* an error?

That was worth checking, because if it could, `if err != nil` would miss it. In
Go it can't: `os.File.Write` documents "Write returns a non-nil error when
n != len(b)", and the standard library enforces it. Under the hood it keeps
calling the OS until everything is written or the OS returns an error, and if it
still comes up short it returns `io.ErrShortWrite`. I read the stdlib source to
confirm this. So the error check alone is enough, and the comment above the
write says so.

### A subtlety with fsync

If `write` succeeds but `fsync` fails, the full record is already in the file
(in the OS cache at least). When the file is reopened, the trim sees a complete,
valid record and keeps it. So `Append` returned an error, but the event shows up
on the next read. That's fine for this engine, because it re-reads the history
before every decision and just carries on from there. It's also how it behaved
before this change. Dropping the handle on an fsync error is still the right
move, because after a failed fsync nobody can say what state the handle is in.

## The test seam

To test this, a write has to fail halfway on demand. You can't easily fill a
disk in a unit test, so the store got a tiny **seam**:

```go
// writeFile is (*os.File).Write; tests swap it to inject a torn write.
writeFile func(f *os.File, b []byte) (int, error)
```

`(*os.File).Write` is a **method expression**: it turns the method into a plain
function whose first argument is the file, like
`FileHandle.prototype.write.call(fh, buf)` in JS. Production always uses the real
one. The test, which lives in the same package and can see lowercase fields,
swaps in a function that writes the first half of the record to the real file
and then returns `syscall.ENOSPC`. On disk that's exactly what a full disk
leaves behind. It's the Go version of `jest.spyOn(fs, 'writeSync')`, except
explicit: there's no monkey-patching in Go, so you leave a hook on purpose.

## The test

`TestAppendAfterFailedWriteRecovers` is **table-driven**: a slice of cases, each
with a name, a way to break things, and the error we expect. `t.Run` runs each
as a named subtest, like a `describe.each` / `it.each` table in Jest.

1. **Partial write then ENOSPC**: the seam above.
2. **Torn bytes and a dead descriptor**: write 3 stray bytes through a second
   handle, then close the store's cached handle under it so the next `Write`
   gets a real `os.ErrClosed` from the standard library, with no seam involved.

Each case appends a good event, breaks the store, checks that the next `Append`
fails with the expected error and that no handle is left cached, then retries.
The retry must succeed, and `ReadAll` must return exactly the two good events.

As in Session 16, I ran the test *before* writing the fix to make sure it fails:

- ENOSPC case: the retry "succeeded" and then `ReadAll` failed with
  `record at offset 57: corrupt record (CRC mismatch)`. That's the production
  bug exactly.
- Dead-descriptor case: every retry kept failing with `file already closed`. The
  workflow would have been stuck until a restart.

With the fix, both pass.

## Deploy

Same runbook as before: merge the green PR (#7, `966b991`), build an arm64
binary on the laptop from a clean checkout of `main`, take a fresh data backup,
ship the binary to the VPS keeping the old one as `workflowd.prev`, and restart.
The restart took well under a second and recovery replayed all 4 workflows on
disk. `/api/health` now reports `966b991`. A test OrderSaga ran to completion
(10 events) and held one history file open while it ran and none once it
finished, so Session 16's fix still holds.

## What's next

- Still pending your OK: `--retention` in production (suggested: a week).
- Then Phase 4, Raft.

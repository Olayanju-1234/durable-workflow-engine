# Session 16: Releasing file handles when a workflow finishes

*Plain-English walkthrough for a Node/TS developer who is new to Go. A short,
focused session: one leak, one fix, three tests, one deploy.*

## The leak

Every time the engine writes an event to a workflow's history file, it needs an
open file to write into. Opening a file is not free. The store checks for a
half-written record left by a crash, and fsyncs the folder for brand-new files.
So the store opens each workflow's file once and keeps the handle in a map:

```go
files map[string]*os.File // workflowID -> open append handle
```

In Node terms that's a `Map<string, FileHandle>` from `fs.promises.open()`, kept
around so the next `appendFile` skips the open. Fine for a workflow that's still
running, since it's about to write again.

The catch: nothing ever removed entries from the map, except `Delete`
(retention) and `Close` (shutdown). A finished workflow will never write again,
but its handle stayed open for as long as the process lived. It's the classic
Node cache that never evicts, except each entry holds an operating-system file
descriptor (fd) instead of memory.

The server limits how many fds one process can hold. On the VPS that limit is
about 524,000, so this would have taken a long time to hurt. But it only ever grew. Before the fix, production had 3 workflows, all
finished, and the process was still holding all 3 history files open.

## The fix, part 1: `Release` in the store

```go
func (s *HistoryStore) Release(workflowID string) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    f, ok := s.files[workflowID]
    if !ok {
        return nil
    }
    delete(s.files, workflowID)
    if err := f.Close(); err != nil {
        return fmt.Errorf("history: release %s: %w", workflowID, err)
    }
    return nil
}
```

Read it as:

```ts
release(id: string) {
  const fh = this.files.get(id);
  if (!fh) return;
  this.files.delete(id);
  await fh.close();
}
```

Two details:

- `f, ok := s.files[workflowID]` is Go's "get with a found flag". A plain
  `s.files[id]` on a missing key returns `nil`, not an error, so `ok` plays the
  part of `map.has(id)`.
- We remove the entry *before* closing. If `Close` fails, the handle is unusable
  anyway, so keeping it in the map would only hand a broken handle to the next
  write.

The file itself is untouched. If anything ever appends to that workflow again,
the store just opens it fresh, and that reopen runs the crash-safety check
again. Nothing special is needed for that case.

## The fix, part 2: calling it at the right moment

Where does a workflow "finish"? There are several paths: the workflow returns,
it fails, an operator terminates it, or it finishes while the engine is
recovering after a restart. Calling `Release` in each of those would be easy to
miss the next time someone adds a path.

But every event, on every path, is written through one function,
`appendLocked`. So the release goes there:

```go
if isTerminalEvent(ev) {
    if err := e.store.Release(workflowID); err != nil {
        e.log.Warn("release history handle", "workflow_id", workflowID, "err", err)
    }
}
```

and

```go
func isTerminalEvent(ev *history.HistoryEvent) bool {
    switch ev.GetAttributes().(type) {
    case *history.HistoryEvent_WorkflowCompleted, *history.HistoryEvent_WorkflowFailed:
        return true
    }
    return false
}
```

`switch x.(type)` is a **type switch**. A history event holds exactly one kind
of payload (a protobuf `oneof`), and the type switch checks which one, like
narrowing a TypeScript discriminated union with `switch (ev.kind)`. Terminate
writes a `WorkflowFailed` with `terminated: true`, so it's covered too.

Why log the error instead of returning it? By this point the event is already
safely on disk. Returning an error would tell the caller "the write failed",
which isn't true. The worst outcome of a failed close is one leaked fd, so a
warning in the logs is the honest response.

## What about recovery?

The task asked to check this. On startup, `Recover` reads every workflow to work
out where it stopped. It reads with `ReadAll`, which opens the file, reads it,
and closes it again (`defer f.Close()`, Go's `finally`). It never touches the
cached handles. So finished workflows cost nothing at startup. A workflow that
*finishes during* recovery writes its final event through `appendLocked`, so it
gets released like any other. A test now pins this down so it can't quietly
change.

## The tests

- **Store:** append, release, append again, then read. Both events must be
  there, and the handle count goes 1 → 0 → 1.
- **Engine:** run one workflow to completion, one to failure, terminate a third,
  and keep a fourth running. Only the running one should still have a handle.
- **Recovery:** restart over one finished workflow and one that finishes during
  recovery. Zero handles afterwards.

To read the handle count from the engine's tests I added
`HistoryStore.OpenHandles()`. A capitalised name in Go means it's exported (like
`export`), but the package lives under `internal/`, which Go only lets code in
this module import. So it's public to us and invisible to anyone else.

One habit worth copying: after the tests passed, I disabled the `Release` call
and ran them again. Both engine tests failed ("open history handles after
completion = 1, want 0"). A test that still passes with the fix removed proves
nothing, so this checks that the tests actually test the fix.

## A related bug spotted on the way (not fixed here)

If a write fails halfway (say the disk fills up), part of a record is left at
the end of the file. The cached handle stays in the map, so the engine's next
retry writes a full record *after* that partial one. Reads then misread the
boundary and report the log as corrupt. The crash-safety trim only runs when a
file is opened, and a cached handle never gets reopened. The likely fix reuses
today's work: drop the cached handle when a write fails, so the next append
reopens the file and trims the partial record first. It's logged as a
follow-up.

## What's next

- The torn-write follow-up above.
- Still pending your OK: `--retention` in production (suggested: a week).
- Then Phase 4, Raft.

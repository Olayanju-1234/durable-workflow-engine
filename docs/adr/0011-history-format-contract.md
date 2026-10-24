# ADR-0011: History formats and the upgrade/rollback contract

- **Status:** Accepted
- **Date:** 2026-10-24

## Context

workflowd's history files are its database, and each release adds things a history can hold.
Three times in one week a release note had to warn "an older binary refuses histories that
contain X": version markers (Phase 5), two steps in flight (#25), and a step closed between
retries (0.2.0). Each warning was prose an operator had to find, read and apply by hand before
a rollback, and nothing stopped a rollback that ignored it. The failure was also poor when it
came: an older binary met the first history it could not fold and stopped with
`event 5 has unknown or empty attributes`, or misread a flag it did not know and carried on.

Forces:

- **New binaries must keep reading every old history.** That is non-negotiable and already true.
- **Old binaries cannot be changed.** Whatever we add helps rollbacks to binaries that have it,
  so it has to start now, and it has to classify the changes that came before it.
- **Most new features are flags on existing events** (ADR-0008, 0009), which older binaries
  read without complaint but may misread. Unknown event types at least fail; unknown flags
  fail silently.
- **The format must rise only when a feature is used.** An operator who upgrades and changes
  nothing else should still be able to roll back.

## Decision

1. **Formats are numbered.** A format names a change an older binary would refuse or misread.
   `history.FormatVersion` is the newest a binary reads; every binary reads all older ones.
   The formats so far: 1 base (Phase 1–4), 2 markers (`14a1fa0`), 3 two steps in flight
   (`f7c18c1`), 4 a cancelled step or a step closed between retries (0.2.0), 5 continue-as-new
   and graceful cancellation (0.3.0). `docs/UPGRADING.md` keeps the table.
2. **Every event says the oldest format that reads it.** `HistoryEvent.min_reader_format`,
   stamped by the engine when it appends (only when above the base, so most events carry
   nothing). The fold computes the same for events written before stamps existed, from the
   feature they use, so old directories are classified too.
3. **The data directory records its format** in a `FORMAT` file (`{"history_format":N}`),
   written atomically. The store raises it *before* writing the first event that needs more,
   so a crash can leave it too high, never too low. Every writer goes through the store: the
   single-node engine, a cluster node's apply loop, and a snapshot install. The first recovery
   of a directory written before this ADR records the format its histories need.
4. **A binary refuses a directory it cannot read, at startup, before reading any history**:
   `history: format too new for this binary: data directory /x needs history format 6 (...),
   but this binary reads up to format 5; upgrade the binary, or restore a backup taken before
   the upgrade`. Recovery refuses a single history the same way (a stamp it does not know, or
   an event kind it has no field for), naming the workflow.
5. **Operators can ask.** `workflowd --check-format --data-dir D` reads the record and scans
   every history read-only, printing what they need and whether this binary serves them;
   `--rescan-format` also records the scanned value, which is how the format comes back down
   once the histories that needed a newer one are gone (finished and purged). `/api/health`
   reports `history_format` and `reads_history_format`; `--version` prints the format too.

## Consequences

- From 0.3.0 on, a rollback that would misread history is impossible: the old binary does not
  start. Rolling back to a binary before 0.3.0 is still a manual check, but a 0.3.0 binary's
  `--check-format` gives the number to compare with the table.
- In a cluster, a node too old for an event it is asked to apply stops with the same error,
  rather than diverging from the others. Upgrade every node before using new features.
- Every new feature must answer "what format does this need?" in its ADR, and stamp the events
  that need it. A test in each feature checks the stamp.
- Stamps cost a few bytes on events that need them. The directory scan for an unrecorded
  directory happens once, inside the recovery pass that already reads every history.

## Alternatives considered

- **A format version per history file** (a header record). Precise per workflow, but an old
  binary would still have to read each file to refuse it, and a header changes the file layout
  older binaries expect.
- **A single version bumped by every release.** Simple, but it would forbid every rollback,
  even when nothing new was written. Raising the format on first use keeps rollbacks open until
  they would actually be unsafe.
- **New event types for every feature, so old binaries fail loudly.** Loud but late (at the
  first unreadable workflow, after others have started running), and it gives up the cheap
  compatibility flags buy.
- **Lowering the record automatically after retention.** It would need every startup to scan
  archived runs too; an explicit `--rescan-format` keeps startup cost unchanged.

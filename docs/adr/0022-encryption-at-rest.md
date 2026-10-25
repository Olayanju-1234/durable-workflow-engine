# ADR-0022: Encrypting history at rest

- **Status:** Accepted
- **Date:** 2026-10-25

## Context

A history holds everything a workflow saw: its input, every activity's input and output, every
signal's payload, every error message. liftoff's webhook histories carry tenant data (URLs,
payloads, response bodies in errors), and so do its backups, its archived runs and the private
engine's disk. Anyone who can read `/opt/workflowd-liftoff/data`, a backup tarball or an archive
bucket reads all of it in plain text.

Forces:

- **Optional.** Most users of a single-binary engine on their own disk do not need it, and key
  handling is an operator's decision; nothing changes until they turn it on.
- **Every existing history stays readable**, and a directory that turns encryption on keeps its
  plaintext histories working beside the new, encrypted events.
- **Keys rotate.** A new key encrypts new events; old events stay readable with their old key
  until they are rewritten or purged.
- **No new dependencies**: Go's standard library has AES-GCM.
- **The format contract** (ADR-0011): a binary that cannot read encrypted events must refuse to
  start, not misread them.
- **What the engine needs without a key**: format checks, CRC framing, cluster fences
  (`raft_index`), backups and archive copies all work on raw records.

## Decision

1. **The attributes of each event are sealed; its header is not.** An encrypted event keeps
   `event_id`, `timestamp`, `raft_index` and `min_reader_format` in the clear and carries its
   attributes (the event kind with every name, payload and error) in a new `sealed` field,
   encrypted with AES-256-GCM, plus the `key_id` that sealed them. The attributes oneof is empty.
   The associated data binds the ciphertext to its key id and event id, so an event cannot be
   moved to another position in a history unnoticed, and any change to a byte fails
   authentication.
2. **History format 7.** A sealed event is stamped `min_reader_format = 7`, so the data directory
   records format 7 before the first one is written; a binary before 0.5.0 refuses the directory
   at startup (it would see an event with no attributes). Plaintext events written before, or
   with encryption off, keep their format.
3. **Keys come from a file**, `--encryption-keys FILE` (JSON): a list of `{"id", "key"}` (32
   random bytes, base64) and the `active` id that seals new events. Every key in the file opens
   events sealed with it. Rotation is adding a key and making it active; a running engine picks
   the file up again on `SIGHUP` (ADR-0023). Retiring a key needs its events gone: purged by
   retention, or rewritten with `workflowd --encrypt-all`, which reseals every history in the data
   directory with the active key (with the engine stopped). `--decrypt-all` writes them back as
   plaintext, which is the way back to an older binary.
4. **The store seals and opens.** Every write path seals: the engine seals an event before
   handing it to its history log, so a cluster's Raft log holds ciphertext too, and the store
   seals anything that reaches it unsealed. Reads open: every history read, the cluster's
   position fence, an archived run, a backup listing. In memory the engine works with plaintext,
   so replay, describe, the dashboard and the SDKs see exactly what they saw before.
5. **A missing key is fatal for that history**: reading an event sealed with a key the file does
   not have fails with an error naming the key id and the workflow. Recovery stops on it, like on
   any unreadable history, so the engine does not start with workflows it cannot run.
   `--check-format --encryption-keys FILE` lists how many events each key sealed (from the clear
   headers, without opening them) and whether the file has every key.

## Consequences

- **Losing a key loses every event it sealed.** There is no recovery: keep the key file out of
  the data directory and out of its backups, back it up separately, and keep old keys until
  `--check-format` says nothing uses them. To start an engine whose key is gone, the affected
  histories must be moved out of the data directory (they are lost).
- Backups, archive copies and snapshots carry ciphertext: restoring or reading them needs the
  same keys. Every node of a cluster needs the same key file.
- Event ids, timestamps and the number of events stay visible on disk, and so does each event's
  size (to within padding the GCM tag adds): enough to see that a workflow ran and how long, not
  what it did.
- Turning encryption on raises the directory to format 7, which a binary before 0.5.0 refuses;
  `--decrypt-all` (then `--rescan-format`) brings it back.
- Each event costs 12 bytes of nonce, 16 of tag and the key id more on disk, and an AES-GCM pass
  on every write and history read (microseconds).
- History size limits are measured on the plaintext events.

## Alternatives considered

- **Encrypt each payload field** (inputs, outputs, signals), leaving step names and errors in
  the clear. Errors routinely quote what failed (a URL, a response body), so they would leak; and
  an older binary would read ciphertext as payload instead of refusing it.
- **Encrypt whole records in the store**, header included. Simpler, but format checks, the
  cluster's index fence and restore validation would all need keys, and a lost key would make even
  "which workflows exist" unanswerable.
- **Client-side encryption in the SDKs** (Temporal's payload codecs). The engine never sees
  plaintext, which is stronger, but every SDK needs codecs, the dashboard can no longer show
  payloads, and errors and names stay in the clear. A codec in the SDKs can still be added on top.
- **A KMS or envelope keys.** No dependency-free way to reach one; a key file is what a
  single-binary engine can promise. Envelope encryption per history can come later behind the same
  key id.

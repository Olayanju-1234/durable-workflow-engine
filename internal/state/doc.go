// Package state implements the workflow state machine: it replays a workflow's
// recorded history to reconstruct the current state and decide what happens next
// — schedule a step, complete the workflow, or fail it. Decisions are a pure
// function of the history, which is what makes crash recovery just a replay.
//
// Two entry points:
//
//   - Reconstruct(events) folds the whole history into a State (status + per-step
//     progress), validating the log's structural invariants along the way.
//   - Decide(def, events) replays history and returns the single next Decision.
//
// Both are pure: no clock, no disk, no network. The gRPC server (Phase 1, Week 3)
// is what records WorkflowStarted, calls Decide, persists the resulting event,
// and enqueues work for a worker.
//
// Status: implemented in Phase 1, Week 2. See docs/ROADMAP.md and docs/ARCHITECTURE.md.
package state

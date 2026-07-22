// Package sdk lets you write a workflow as an ordinary Go function whose execution
// is deterministically replayed against history to drive it durably. A WorkflowFunc
// calls the Context's primitives — ExecuteActivity, Sleep, WaitForSignal — and the
// replay engine (Workflow.Decide) turns each call into the engine's next decision.
//
// Status: the replay core landed in Phase 2 (Week 6). The ergonomic public surface
// — a fuller Context (Now, Random, Go for deterministic time/rng/concurrency), the
// client and worker entrypoints, and workflow.Version — is Phase 5. See
// docs/ROADMAP.md.
package sdk

// Package server implements the gRPC API and task queue that bridge the engine
// and external workers: StartWorkflow, PollTask, CompleteTask, SignalWorkflow,
// and Heartbeat — plus the visibility API (ListWorkflows, GetWorkflow,
// TerminateWorkflow) for operators.
//
// The Engine owns the history store and an in-memory task queue and runs the
// decision loop in response to RPCs; GRPCServer is a thin adapter from the
// generated WorkflowServiceServer interface to the Engine. Every dispatched
// task carries a lease (see lease.go): a worker must complete or heartbeat
// before the lease expires, or the task is reassigned — so a dead worker can
// no longer strand a workflow. The engine also carries a Prometheus metrics
// registry (see metrics.go, internal/metrics); workflowd serves it at /metrics.
//
// Status: Phase 3, Week 9 — observability (metrics + visibility) implemented.
package server

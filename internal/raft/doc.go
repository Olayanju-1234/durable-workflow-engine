// Package raft is a hand-rolled implementation of the Raft consensus algorithm
// (Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm").
// It lets a small cluster of workflowd nodes agree on one ordered log, so that
// any node can take over when the leader dies without losing committed work.
//
// The package is split in three layers, each testable on its own:
//
//   - core: the algorithm itself, as a deterministic state machine. It never
//     touches the clock, the disk or the network. The driver feeds it ticks,
//     received messages and proposals; it answers with a Ready batch: state to
//     persist, messages to send, entries that became committed. Given the same
//     inputs it always produces the same outputs, which is what lets tests
//     replay elections and network partitions step by step.
//   - Node: the driver. One goroutine owns the core; it turns real time into
//     ticks, persists every Ready before sending its messages (so a node never
//     tells a peer something it could forget in a crash), and publishes status.
//   - Transport: delivers messages between nodes. The gRPC transport sends each
//     message as its own one-way Send call; tests use an in-memory network.
package raft

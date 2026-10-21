// Package workflow is the API workflow code is written against.
//
// A workflow is an ordinary Go function that calls the durable primitives on its
// Context. workflowd records each call's result in the workflow's history and, to
// resume after a crash, a deploy or a long sleep, runs the function again from the
// top: calls that already completed return their recorded result instantly, and
// the first call with no result yet is what happens next.
//
//	func Order(ctx workflow.Context, input []byte) ([]byte, error) {
//		reserved, err := ctx.ExecuteActivity("reserve", input)
//		if err != nil {
//			return nil, err
//		}
//		ctx.Sleep("settle", time.Minute) // durable: survives restarts
//		if _, err := ctx.ExecuteActivity("charge", reserved); err != nil {
//			ctx.ExecuteActivity("release", reserved) // compensate
//			return nil, err
//		}
//		return ctx.ExecuteActivity("ship", reserved)
//	}
//
// Because the function is re-run, it must be deterministic: given the same
// history it must make the same calls in the same order. Do side effects in
// activities, use ctx.Now instead of time.Now, ctx.Sleep instead of time.Sleep,
// and do not start goroutines, read files or call the network from workflow code.
// A workflow that changes the order or names of its calls while executions are in
// flight fails those executions with a non-determinism error.
//
// Register workflows with a worker (package worker); start them with a client
// (package client).
package workflow

import (
	"log/slog"
	"time"
)

// Func is a workflow: orchestration logic over the Context's durable primitives.
// Its return value is the workflow's output; a returned error fails the workflow.
type Func func(ctx Context, input []byte) ([]byte, error)

// Context is the durable API a workflow function calls. Every durable call
// (ExecuteActivity, Sleep, WaitForSignal) is one step in the workflow's history,
// named by its first argument; names make the dashboard and history readable and
// are how replay checks that the code still matches the recorded run.
//
// Context is implemented by workflowd; methods may be added in later releases.
type Context interface {
	// ExecuteActivity runs a named activity on a worker and returns its output.
	// It returns an error if the activity failed (after any engine-side retries);
	// the workflow can handle it, for example by compensating, or return it.
	ExecuteActivity(name string, input []byte) ([]byte, error)

	// Sleep pauses the workflow durably for d. The deadline is recorded, so the
	// timer survives engine restarts and fires on time afterwards.
	Sleep(name string, d time.Duration)

	// WaitForSignal blocks until a signal with this name is sent to the workflow
	// and returns its payload. A signal sent before the workflow waits is
	// buffered: the k-th wait for a signal receives the k-th one sent.
	WaitForSignal(name, signal string) []byte

	// Input is the workflow's start input.
	Input() []byte

	// Now is the workflow's deterministic clock: the time of the latest history
	// event the code has observed (the start, then each step's result). It moves
	// forward only when a step resolves and reads the same on every replay.
	Now() time.Time

	// Info describes the running workflow.
	Info() Info

	// Logger logs from workflow code without repeating itself: records are
	// dropped while the function replays steps it has already run.
	Logger() *slog.Logger
}

// Info identifies a workflow execution. WorkflowID is stable for the life of the
// execution, which makes it a good basis for idempotency keys.
type Info struct {
	WorkflowID   string
	WorkflowType string
}

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
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// ErrCancelled is what Future.Get returns for a step that was cancelled.
var ErrCancelled = errors.New("workflow: step cancelled")

// ErrWorkflowCancelled is what Future.Get returns for the steps a workflow was
// waiting on when a graceful cancel was requested (client.CancelWorkflow with
// client.Graceful). It matches ErrCancelled too. The code may compensate (new
// steps run normally) and then return an error matching it to end CANCELLED;
// returning normally ends it COMPLETED.
var ErrWorkflowCancelled = fmt.Errorf("%w: the workflow is being cancelled", ErrCancelled)

// ContinueAsNewError, returned by a workflow function, ends this run and
// starts a fresh one under the same workflow id, with an empty history and
// Input as its input. A workflow that loops forever (polling, a subscription,
// an entity that lives for months) uses it to keep its history small. Build
// one with ContinueAsNew.
type ContinueAsNewError struct {
	Input []byte
	// WorkflowType runs the next run as another type; empty keeps this one's.
	WorkflowType string
	// ExecutionTimeout bounds the next run; zero keeps this run's.
	ExecutionTimeout time.Duration
	// TaskQueue runs the next run on another task queue; empty keeps this one's.
	TaskQueue string
}

func (e *ContinueAsNewError) Error() string { return "workflow: continue as new" }

// ContinueAsNew returns the error that ends this run and starts the next:
//
//	if ctx.ContinueAsNewSuggested() {
//		return nil, workflow.ContinueAsNew(state)
//	}
//
// Signals the run has not consumed are not carried over.
func ContinueAsNew(input []byte, opts ...ContinueAsNewOption) error {
	e := &ContinueAsNewError{Input: input}
	for _, o := range opts {
		o(e)
	}
	return e
}

// ContinueAsNewOption customises ContinueAsNew.
type ContinueAsNewOption func(*ContinueAsNewError)

// WithNewWorkflowType runs the next run as another registered workflow type.
func WithNewWorkflowType(workflowType string) ContinueAsNewOption {
	return func(e *ContinueAsNewError) { e.WorkflowType = workflowType }
}

// WithNewExecutionTimeout gives the next run its own execution timeout.
func WithNewExecutionTimeout(d time.Duration) ContinueAsNewOption {
	return func(e *ContinueAsNewError) { e.ExecutionTimeout = d }
}

// WithNewTaskQueue runs the next run on another task queue.
func WithNewTaskQueue(q string) ContinueAsNewOption {
	return func(e *ContinueAsNewError) { e.TaskQueue = q }
}

// Func is a workflow: orchestration logic over the Context's durable primitives.
// Its return value is the workflow's output; a returned error fails the workflow.
type Func func(ctx Context, input []byte) ([]byte, error)

// Context is the durable API a workflow function calls. Every durable call
// (ExecuteActivity, Sleep, WaitForSignal and their Async forms) is one step in
// the workflow's history,
// named by its first argument; names make the dashboard and history readable and
// are how replay checks that the code still matches the recorded run.
//
// Context is implemented by workflowd; methods may be added in later releases.
type Context interface {
	// ExecuteActivity runs a named activity on a worker and returns its output.
	// It returns an error if the activity failed (after any retries); the
	// workflow can handle it, for example by compensating, or return it.
	// WithRetryPolicy makes the engine retry a failed attempt first;
	// WithStartToCloseTimeout and WithScheduleToCloseTimeout bound its time.
	ExecuteActivity(name string, input []byte, opts ...ActivityOption) ([]byte, error)

	// Sleep pauses the workflow durably for d. The deadline is recorded, so the
	// timer survives engine restarts and fires on time afterwards.
	Sleep(name string, d time.Duration)

	// WaitForSignal blocks until a signal with this name is sent to the workflow
	// and returns its payload. A signal sent before the workflow waits is
	// buffered: the k-th wait for a signal receives the k-th one sent.
	WaitForSignal(name, signal string) []byte

	// ExecuteActivityAsync starts an activity and returns at once, so several
	// can run in parallel; Future.Get waits for the result.
	//
	//	charge := ctx.ExecuteActivityAsync("charge", order)
	//	email := ctx.ExecuteActivityAsync("email", order)
	//	if _, err := charge.Get(); err != nil { ... }
	//	email.Get()
	ExecuteActivityAsync(name string, input []byte, opts ...ActivityOption) Future

	// NewTimer starts a durable timer and returns at once; Get returns when it
	// fires. Sleep is NewTimer(...).Get().
	NewTimer(name string, d time.Duration) Future

	// WaitForSignalAsync starts waiting for a signal and returns at once; Get
	// returns its payload.
	WaitForSignalAsync(name, signal string) Future

	// WaitAny blocks until one of the futures has a result and returns its
	// index. When several have one, it picks the step whose result history
	// recorded first, so the choice is the same on every replay. Use it for
	// races such as a reply against a deadline:
	//
	//	reply := ctx.WaitForSignalAsync("reply", "approved")
	//	deadline := ctx.NewTimer("deadline", 48*time.Hour)
	//	if ctx.WaitAny(reply, deadline) == 1 { ... timed out ... }
	WaitAny(futures ...Future) int

	// CancelStep closes a step the workflow no longer wants, such as the loser
	// of a WaitAny race: a timer stops mattering, a signal wait stops taking
	// signals (a later wait for the same signal gets the next one), and an
	// activity's worker is told to stop. Its Get returns an error matching
	// ErrCancelled. A step that already has a result is left alone.
	//
	//	i := ctx.WaitAny(reply, deadline)
	//	ctx.CancelStep([]workflow.Future{reply, deadline}[1-i])
	CancelStep(f Future)

	// Input is the workflow's start input.
	Input() []byte

	// Now is the workflow's deterministic clock: the time of the latest history
	// event the code has observed (the start, then each step's result), in UTC.
	// It moves forward only when a step resolves and reads the same on every
	// replay, on any worker.
	Now() time.Time

	// Info describes the running workflow.
	Info() Info

	// Logger logs from workflow code without repeating itself: records are
	// dropped while the function replays steps it has already run.
	Logger() *slog.Logger

	// GetVersion lets you change workflow code while executions are in flight.
	// Wrap the change in a branch on the returned version:
	//
	//	if ctx.GetVersion("add-fraud-check", workflow.DefaultVersion, 1) == 1 {
	//		ctx.ExecuteActivity("fraud-check", order)
	//	}
	//
	// An execution that reaches this point for the first time records
	// maxSupported in its history and gets it on every replay. One that already
	// ran past this point under the old code has no record of it and gets
	// DefaultVersion, so it replays the old branch. Repeated calls with the same
	// changeID return the same version. Once no execution can still need the
	// old branch, raise minSupported and delete it; a recorded version outside
	// [minSupported, maxSupported] fails the execution as non-deterministic.
	GetVersion(changeID string, minSupported, maxSupported Version) Version

	// SideEffect runs fn once, records its result in history, and returns the
	// recorded result on every replay. Use it for small non-deterministic values
	// such as a random id. It must not fail or do I/O; use an activity for that.
	SideEffect(name string, fn func() []byte) []byte

	// ContinueAsNewSuggested reports whether the history has grown past the
	// engine's warning share of its limits, as of the latest result the code
	// has observed, so every replay gives the same answer at the same point.
	// Then return ContinueAsNew: a history that reaches its limit fails the
	// workflow.
	ContinueAsNewSuggested() bool

	// CancelRequested reports whether a graceful cancel has reached the code:
	// the steps it was waiting on then failed with ErrWorkflowCancelled. Sleep
	// and WaitForSignal return no error, so check it after them:
	//
	//	ctx.Sleep("settle", time.Hour)
	//	if ctx.CancelRequested() {
	//		ctx.ExecuteActivity("release", reserved) // compensate
	//		return nil, workflow.ErrWorkflowCancelled
	//	}
	//
	// It turns true at the same point of the code on every replay.
	CancelRequested() bool
}

// Future is the result of a durable call started without waiting for it. Get
// blocks the workflow until the step has a result and returns it (for a timer,
// nil; for a signal wait, the payload). A future must only be used with the
// Context that created it. There is deliberately no non-blocking "is it done
// yet": that would depend on when a replay happens, not on history.
type Future interface {
	Get() ([]byte, error)
}

// RetryPolicy retries a failed activity before the workflow sees its error.
// MaxAttempts counts the first attempt (1 means no retries). The wait before
// each retry starts at InitialBackoff and doubles, capped at MaxBackoff (zero
// means no cap). A failure whose type (worker.Typed, or one of the engine's:
// "LeaseExpired", "StartToCloseTimeout", "ScheduleToCloseTimeout") is in
// NonRetryableErrorTypes is not retried. The policy is recorded with the step,
// so retries continue with it across engine restarts and failovers.
type RetryPolicy struct {
	MaxAttempts            int
	InitialBackoff         time.Duration
	MaxBackoff             time.Duration
	NonRetryableErrorTypes []string
}

// ActivityError is the error ExecuteActivity (or Future.Get) returns when an
// activity failed for good. Its message is the last attempt's error.
//
//	var ae *workflow.ActivityError
//	if errors.As(err, &ae) && ae.Type == "BadRequest" { ... }
type ActivityError struct {
	StepName string
	Message  string
	// Type is the failure's kind, as the worker or engine reported it.
	Type string
	// NonRetryable: the worker said the failure was not worth retrying.
	NonRetryable bool
}

func (e *ActivityError) Error() string { return e.Message }

// ActivityOption customises one ExecuteActivity call.
type ActivityOption func(*ActivityOptions)

// ActivityOptions is what the ActivityOption values of a call add up to.
type ActivityOptions struct {
	RetryPolicy            *RetryPolicy
	StartToCloseTimeout    time.Duration
	ScheduleToCloseTimeout time.Duration
	TaskQueue              string
}

// WithTaskQueue runs the activity on a named task queue instead of the
// workflow's own, so only workers polling that queue receive it.
func WithTaskQueue(q string) ActivityOption {
	return func(o *ActivityOptions) { o.TaskQueue = q }
}

// WithRetryPolicy retries the activity per p, overriding any policy the
// engine has for the step name.
func WithRetryPolicy(p RetryPolicy) ActivityOption {
	return func(o *ActivityOptions) { o.RetryPolicy = &p }
}

// WithStartToCloseTimeout limits one attempt: the engine fails it d after a
// worker picks it up, even if the worker is still heartbeating, and the retry
// policy decides whether another attempt runs. The worker's ctx ends then too.
func WithStartToCloseTimeout(d time.Duration) ActivityOption {
	return func(o *ActivityOptions) { o.StartToCloseTimeout = d }
}

// WithScheduleToCloseTimeout limits the whole step, every attempt and backoff
// included, from when it is first scheduled. When it passes the step fails
// for good, and no retry is planned that could not start before it.
func WithScheduleToCloseTimeout(d time.Duration) ActivityOption {
	return func(o *ActivityOptions) { o.ScheduleToCloseTimeout = d }
}

// Version identifies one variant of changed workflow code; see GetVersion.
type Version int

// DefaultVersion is the version of code that ran before a GetVersion call was
// added.
const DefaultVersion Version = -1

// Info identifies a workflow execution. WorkflowID is stable for the life of the
// execution, which makes it a good basis for idempotency keys.
type Info struct {
	WorkflowID   string
	WorkflowType string
	// RunID names this run: a workflow id that continues as new gets a new one.
	RunID string
	// ContinuedFromRunID is the run this one continued from, if any.
	ContinuedFromRunID string
	// TaskQueue is the task queue the run is on ("default" for the default one).
	TaskQueue string
}

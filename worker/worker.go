// Package worker runs Go workflows and activities in your own process against a
// workflowd engine.
//
//	c, _ := client.Dial(client.Options{Addrs: []string{"localhost:7233"}})
//	w := worker.New(c, worker.Options{})
//	w.RegisterWorkflow("Order", Order)
//	w.RegisterActivity("charge", Charge)
//	err := w.Start(ctx) // returns once registered; w.Stop() to end
//
// The engine owns durability. The worker replays workflow code against the
// history the engine sends and answers with the next command, and it executes
// activities and reports their results. It keeps no state of its own, so a
// crashed worker costs only a re-dispatch, and you can run several.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/olayanju-1234/durable-workflow-engine/client"
	"github.com/olayanju-1234/durable-workflow-engine/internal/replay"
	"github.com/olayanju-1234/durable-workflow-engine/internal/rpc"
	"github.com/olayanju-1234/durable-workflow-engine/internal/sdkctx"
	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

// sdkName is reported to the engine when the worker registers.
const sdkName = "go/0.1.0"

// ActivityFunc does the real work of a step: call an API, write a row, send an
// email. It runs at least once per scheduled step (a worker can die after the
// work but before reporting it), so make it idempotent; ActivityInfo gives a
// stable key. ctx is cancelled if the worker loses the task's lease.
type ActivityFunc func(ctx context.Context, input []byte) ([]byte, error)

// Causes an activity's ctx is cancelled with (read them with context.Cause):
// the work is no longer wanted (its step was cancelled, or its workflow
// cancelled, timed out or finished without it), or the engine gave the task to
// another worker because this one missed its lease.
var (
	ErrActivityCancelled = errors.New("worker: activity cancelled")
	ErrLeaseLost         = errors.New("worker: activity lease lost")
)

// ActivityInfo identifies the step an activity is running for.
type ActivityInfo struct {
	WorkflowID string
	StepName   string
	Sequence   int64 // with WorkflowID, unique per step: a good idempotency key
}

// GetActivityInfo returns the step an activity's ctx belongs to.
func GetActivityInfo(ctx context.Context) (ActivityInfo, bool) {
	info, ok := ctx.Value(sdkctx.ActivityInfoKey{}).(ActivityInfo)
	return info, ok
}

// Options configures a Worker. The zero value is usable.
type Options struct {
	// ID names this worker in engine logs and leases. Default: host-pid.
	ID string
	// ActivityConcurrency is how many activities run at once. Default 4.
	ActivityConcurrency int
	// WorkflowConcurrency is how many workflow tasks are decided at once. Default 2.
	WorkflowConcurrency int
	// PollTimeout bounds each long poll. Default 30s.
	PollTimeout time.Duration
	// Logger receives worker logs and is the base of workflow.Context.Logger.
	// Default slog.Default().
	Logger *slog.Logger
}

// Worker hosts registered workflows and activities. Register everything before
// calling Start or Run.
type Worker struct {
	conn *rpc.Conn
	opts Options
	log  *slog.Logger

	mu         sync.Mutex
	workflows  map[string]workflow.Func
	activities map[string]ActivityFunc
	running    bool
	cancel     context.CancelFunc
	wg         sync.WaitGroup

	regMu        sync.Mutex
	registered   bool
	registeredAt uint64 // conn epoch when the types were last registered
}

// New creates a worker that uses c's connection, so it follows the same leader.
func New(c *client.Client, opts Options) *Worker {
	if opts.ID == "" {
		host, _ := os.Hostname()
		opts.ID = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	if opts.ActivityConcurrency <= 0 {
		opts.ActivityConcurrency = 4
	}
	if opts.WorkflowConcurrency <= 0 {
		opts.WorkflowConcurrency = 2
	}
	if opts.PollTimeout <= 0 {
		opts.PollTimeout = 30 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Worker{
		conn:       rpc.ConnOf(c),
		opts:       opts,
		log:        opts.Logger.With("worker_id", opts.ID),
		workflows:  make(map[string]workflow.Func),
		activities: make(map[string]ActivityFunc),
	}
}

// RegisterWorkflow makes this worker run workflows of the given type.
func (w *Worker) RegisterWorkflow(workflowType string, fn workflow.Func) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.mustNotRun()
	w.workflows[workflowType] = fn
}

// RegisterActivity makes this worker execute steps with the given name.
func (w *Worker) RegisterActivity(name string, fn ActivityFunc) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.mustNotRun()
	w.activities[name] = fn
}

func (w *Worker) mustNotRun() {
	if w.running {
		panic("worker: register before calling Start or Run")
	}
}

// Start registers the worker's workflow types with the engine and starts
// polling in the background. It returns once the engine knows the types, so a
// workflow started right after is dispatched; it fails if the engine is
// unreachable or refuses a type (for example, one compiled into the engine).
// Stop ends the polling.
func (w *Worker) Start(ctx context.Context) error {
	types, acts, err := w.begin()
	if err != nil {
		return err
	}
	if len(types) > 0 {
		if err := w.register(ctx, types); err != nil {
			w.mu.Lock()
			w.running = false
			w.mu.Unlock()
			return err
		}
	}
	w.launch(types, acts)
	return nil
}

// Run is Start that tolerates an engine that is not up yet (it keeps trying),
// then blocks until ctx is cancelled and stops. It returns an error only if
// nothing is registered, the engine refuses a type, or it rejects the token.
func (w *Worker) Run(ctx context.Context) error {
	types, acts, err := w.begin()
	if err != nil {
		return err
	}
	if len(types) > 0 {
		err := w.register(ctx, types)
		if code := status.Code(errors.Unwrap(err)); code == codes.InvalidArgument || code == codes.Unauthenticated {
			return err
		}
		if err != nil && ctx.Err() == nil {
			w.log.Warn("engine not reachable yet; will keep trying", "err", err)
		}
	}
	w.launch(types, acts)
	<-ctx.Done()
	w.Stop()
	return nil
}

// Stop ends polling and waits for in-flight workflow tasks and activities to
// finish and report.
func (w *Worker) Stop() {
	w.mu.Lock()
	cancel := w.cancel
	w.mu.Unlock()
	if cancel != nil {
		cancel()
		w.wg.Wait()
		w.log.Info("worker stopped")
	}
}

func (w *Worker) begin() (types, acts []string, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.running {
		return nil, nil, errors.New("worker: already running")
	}
	types, acts = sortedKeys(w.workflows), sortedKeys(w.activities)
	if len(types) == 0 && len(acts) == 0 {
		return nil, nil, errors.New("worker: no workflows or activities registered")
	}
	w.running = true
	return types, acts, nil
}

func (w *Worker) launch(types, acts []string) {
	ctx, cancel := context.WithCancel(context.Background())
	w.mu.Lock()
	w.cancel = cancel
	w.mu.Unlock()
	if len(types) > 0 {
		for range w.opts.WorkflowConcurrency {
			w.wg.Go(func() { w.pollWorkflowTasks(ctx, types) })
		}
	}
	if len(acts) > 0 {
		for range w.opts.ActivityConcurrency {
			w.wg.Go(func() { w.pollActivityTasks(ctx, acts) })
		}
	}
	w.log.Info("worker started", "workflows", types, "activities", acts)
}

// register (re-)announces the workflow types to whichever engine serves now.
// An engine forgets registrations when it restarts or a new leader takes over,
// and it never dispatches a type nobody registered.
func (w *Worker) register(ctx context.Context, types []string) error {
	w.regMu.Lock()
	defer w.regMu.Unlock()
	epoch := w.conn.Epoch()
	if w.registered && epoch == w.registeredAt {
		return nil
	}
	req := &server.RegisterWorkflowTypeRequest{WorkflowTypes: types, WorkerId: w.opts.ID, Sdk: sdkName}
	_, err := w.conn.Call(ctx, func(ctx context.Context, s server.WorkflowServiceClient, opt grpc.CallOption) error {
		_, err := s.RegisterWorkflowType(ctx, req, opt)
		return err
	})
	if err != nil {
		return fmt.Errorf("worker: register workflow types %v: %w", types, err)
	}
	// Record the epoch from before the call: if the call itself was redirected,
	// the epoch moved and the next poll registers again, which is harmless.
	w.registered, w.registeredAt = true, epoch
	return nil
}

func (w *Worker) pollWorkflowTasks(ctx context.Context, types []string) {
	req := &server.PollWorkflowTaskRequest{WorkerId: w.opts.ID, WorkflowTypes: types}
	for ctx.Err() == nil {
		if err := w.register(ctx, types); err != nil {
			w.pause(ctx, err)
			continue
		}
		var task *server.WorkflowTask
		err := w.poll(ctx, func(ctx context.Context, s server.WorkflowServiceClient, opt grpc.CallOption) error {
			resp, err := s.PollWorkflowTask(ctx, req, opt)
			task = resp.GetTask()
			return err
		})
		if err != nil {
			w.pause(ctx, err)
			continue
		}
		if task != nil {
			w.decide(ctx, task)
		}
	}
}

func (w *Worker) decide(ctx context.Context, task *server.WorkflowTask) {
	log := w.log.With("workflow_id", task.GetWorkflowId(), "workflow_type", task.GetWorkflowType())
	w.mu.Lock()
	fn := w.workflows[task.GetWorkflowType()]
	w.mu.Unlock()

	req := &server.CompleteWorkflowTaskRequest{WorkflowId: task.GetWorkflowId(), TaskToken: task.GetTaskToken()}
	decisions, err := replay.DecideAll(fn, task.GetHistory(), w.opts.Logger)
	switch {
	case err != nil:
		// Non-determinism or a panic: the code can never replay this history, so
		// the engine fails the workflow instead of re-dispatching it forever.
		log.Error("workflow replay failed", "err", err)
		req.ReplayError = err.Error()
	case len(decisions) == 1:
		// One command travels in the field every engine version understands.
		req.Command = server.CommandFromDecision(decisions[0])
	default:
		for _, d := range decisions {
			req.Commands = append(req.Commands, server.CommandFromDecision(d))
		}
	}
	cctx, cancel := completionContext(ctx)
	defer cancel()
	_, err = w.conn.Call(cctx, func(ctx context.Context, s server.WorkflowServiceClient, opt grpc.CallOption) error {
		_, err := s.CompleteWorkflowTask(ctx, req, opt)
		return err
	})
	switch status.Code(err) {
	case codes.OK:
	case codes.Aborted:
		log.Debug("workflow task was stale; the engine re-dispatched it", "err", err)
	default:
		log.Warn("complete workflow task", "err", err)
	}
}

func (w *Worker) pollActivityTasks(ctx context.Context, names []string) {
	req := &server.PollTaskRequest{WorkerId: w.opts.ID, StepNames: names}
	for ctx.Err() == nil {
		var task *server.Task
		err := w.poll(ctx, func(ctx context.Context, s server.WorkflowServiceClient, opt grpc.CallOption) error {
			resp, err := s.PollTask(ctx, req, opt)
			task = resp.GetTask()
			return err
		})
		if err != nil {
			w.pause(ctx, err)
			continue
		}
		if task != nil {
			w.execute(ctx, task)
		}
	}
}

// execute runs one activity, heartbeating its lease, and reports the result.
func (w *Worker) execute(ctx context.Context, task *server.Task) {
	log := w.log.With("workflow_id", task.GetWorkflowId(), "step", task.GetStepName(), "sequence", task.GetSequence())
	w.mu.Lock()
	fn := w.activities[task.GetStepName()]
	w.mu.Unlock()

	// The activity keeps running through worker shutdown so its result can be
	// reported; only a lost lease, a cancellation or its start-to-close timeout
	// ends it.
	actx, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	defer cancel(nil)
	if d := time.Duration(task.GetStartToCloseTimeoutMs()) * time.Millisecond; d > 0 {
		var stop context.CancelFunc
		actx, stop = context.WithTimeout(actx, d)
		defer stop()
	}
	actx = context.WithValue(actx, sdkctx.ActivityInfoKey{}, ActivityInfo{
		WorkflowID: task.GetWorkflowId(), StepName: task.GetStepName(), Sequence: task.GetSequence(),
	})
	stopBeats := w.heartbeat(actx, cancel, task, log)
	output, actErr := runActivity(actx, fn, task.GetInput())
	stopBeats()

	req := &server.CompleteTaskRequest{
		WorkflowId: task.GetWorkflowId(), StepName: task.GetStepName(),
		Sequence: task.GetSequence(), LeaseToken: task.GetLeaseToken(),
	}
	if actErr != nil {
		req.Result = &server.CompleteTaskRequest_Error{Error: actErr.Error()}
	} else {
		req.Result = &server.CompleteTaskRequest_Output{Output: output}
	}
	cctx, cancelComplete := completionContext(ctx)
	defer cancelComplete()
	_, err := w.conn.Call(cctx, func(ctx context.Context, s server.WorkflowServiceClient, opt grpc.CallOption) error {
		_, err := s.CompleteTask(ctx, req, opt)
		return err
	})
	switch {
	case err == nil:
	case cancelled(err):
		log.Info("activity result not needed: the step was cancelled", "err", err)
	default:
		// Usually a lost lease: the task was reassigned.
		log.Warn("complete activity task rejected", "err", err)
	}
}

// cancelled reports whether the engine turned a worker away because the work
// is no longer wanted, rather than because its lease was lost.
func cancelled(err error) bool {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		return false
	}
	for _, d := range st.Details() {
		if _, ok := d.(*server.ActivityCancelled); ok {
			return true
		}
	}
	return false
}

// heartbeat extends the task's lease every third of its duration until stopped.
// A rejected heartbeat means the lease is gone, so the activity is cancelled,
// with ErrActivityCancelled or ErrLeaseLost as the cause.
func (w *Worker) heartbeat(ctx context.Context, cancel context.CancelCauseFunc, task *server.Task, log *slog.Logger) (stop func()) {
	every := time.Duration(task.GetLeaseDurationMs()) * time.Millisecond / 3
	if every <= 0 {
		return func() {}
	}
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		t := time.NewTicker(every)
		defer t.Stop()
		req := &server.HeartbeatRequest{WorkflowId: task.GetWorkflowId(), LeaseToken: task.GetLeaseToken(), WorkerId: w.opts.ID}
		for {
			select {
			case <-done:
				return
			case <-t.C:
			}
			_, err := w.conn.Call(ctx, func(ctx context.Context, s server.WorkflowServiceClient, opt grpc.CallOption) error {
				_, err := s.Heartbeat(ctx, req, opt)
				return err
			})
			if status.Code(err) == codes.FailedPrecondition {
				if cancelled(err) {
					log.Info("activity cancelled; stopping it", "err", err)
					cancel(ErrActivityCancelled)
				} else {
					log.Warn("lease lost; cancelling activity", "err", err)
					cancel(ErrLeaseLost)
				}
				return
			}
		}
	})
	return func() { close(done); wg.Wait() }
}

// completionContext outlives worker shutdown, so work already done is reported
// rather than redone after a lease or decision timeout.
func completionContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
}

// runActivity turns a panicking activity into a failed step instead of a
// crashed worker.
func runActivity(ctx context.Context, fn ActivityFunc, input []byte) (out []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("activity panicked: %v", r)
		}
	}()
	return fn(ctx, input)
}

// poll runs one long poll. It is not retried inside the connection: a poll that
// was turned away returns, so the loop re-registers before polling again.
func (w *Worker) poll(ctx context.Context, fn rpc.CallFunc) error {
	pctx, cancel := context.WithTimeout(ctx, w.opts.PollTimeout)
	defer cancel()
	err := w.conn.Once(pctx, fn)
	if ctx.Err() == nil && status.Code(err) == codes.DeadlineExceeded {
		return nil // an idle poll
	}
	return err
}

// pause backs off after a failed call, unless the worker is stopping.
func (w *Worker) pause(ctx context.Context, err error) {
	if ctx.Err() != nil {
		return
	}
	w.log.Debug("poll failed; retrying", "err", err)
	select {
	case <-ctx.Done():
	case <-time.After(200 * time.Millisecond):
	}
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

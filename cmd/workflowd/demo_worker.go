package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
)

// demoSteps is every activity the built-in demo workflows schedule. The demo
// worker polls for these only, so a visitor's own SDK worker keeps its tasks.
var demoSteps = []string{"reserve", "charge", "ship", "release"}

// demoWorker executes the demo activities in-process, so a public demo shows
// workflows actually moving without a separate worker deployment. Activities,
// unlike workflow code, may use randomness and wall-clock time freely: their
// results are recorded once and never replayed.
type demoWorker struct {
	engine *server.Engine
	log    *slog.Logger
	// pause simulates how long real work takes, so each step is visible in the
	// dashboard. Tests make it zero.
	pause func() time.Duration
	// flakyRate is the chance a charge's first attempt fails with a transient
	// error, so visitors see the retry policy without asking for it.
	flakyRate float64

	mu sync.Mutex
	// failedOnce marks charges whose first attempt was failed on purpose, so
	// the retry always succeeds and only "decline" inputs fail for good.
	failedOnce map[string]bool
}

func newDemoWorker(engine *server.Engine, logger *slog.Logger) *demoWorker {
	return &demoWorker{
		engine:     engine,
		log:        logger.With("worker_id", "demo-worker"),
		pause:      func() time.Duration { return 800*time.Millisecond + rand.N(1200*time.Millisecond) },
		flakyRate:  0.25,
		failedOnce: make(map[string]bool),
	}
}

// run serves demo activities on n concurrent pollers until ctx ends.
func (w *demoWorker) run(ctx context.Context, n int) {
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.loop(ctx, fmt.Sprintf("demo-worker-%d", i))
		}()
	}
	wg.Wait()
}

func (w *demoWorker) loop(ctx context.Context, workerID string) {
	for {
		task, err := w.engine.PollTask(ctx, workerID, demoSteps...)
		if err != nil {
			return // ctx ended: shutting down
		}
		log := w.log.With("workflow_id", task.GetWorkflowId(), "step", task.GetStepName())

		select {
		case <-time.After(w.pause()):
		case <-ctx.Done():
			return // the lease expires and recovery re-runs the step
		}

		output, stepErr := w.execute(task)
		errMsg := ""
		if stepErr != nil {
			errMsg = stepErr.Error()
		}
		if err := w.engine.CompleteTask(task.GetWorkflowId(), task.GetStepName(), task.GetSequence(),
			task.GetLeaseToken(), output, errMsg, stepErr != nil); err != nil {
			// Usually a lost lease (the workflow was terminated or the task reassigned).
			log.Warn("demo worker: complete rejected", "err", err)
		}
	}
}

func (w *demoWorker) execute(task *server.Task) ([]byte, error) {
	order := orderRef(task.GetInput())
	switch task.GetStepName() {
	case "reserve":
		return []byte("reserved:" + order), nil
	case "charge":
		if strings.Contains(strings.ToLower(order), "decline") {
			return nil, errors.New("card declined")
		}
		key := fmt.Sprintf("%s/%d", task.GetWorkflowId(), task.GetSequence())
		w.mu.Lock()
		defer w.mu.Unlock()
		if w.failedOnce[key] {
			delete(w.failedOnce, key)
		} else if rand.Float64() < w.flakyRate {
			w.failedOnce[key] = true
			return nil, errors.New("payment gateway timeout")
		}
		return []byte("charged:" + order), nil
	case "ship":
		return []byte("shipped:" + order), nil
	case "release":
		return []byte("released:" + order), nil
	default:
		// Unreachable while PollTask filters by demoSteps.
		return nil, fmt.Errorf("demo worker has no activity %q", task.GetStepName())
	}
}

// orderRef recovers the order reference from a step input. OrderSaga passes
// reserve's output to every later step, while OrderWorkflow chains each step's
// output into the next, so strip whichever of our own prefixes is present.
func orderRef(input []byte) string {
	ref := string(input)
	for _, prefix := range []string{"reserved:", "charged:"} {
		ref = strings.TrimPrefix(ref, prefix)
	}
	return ref
}

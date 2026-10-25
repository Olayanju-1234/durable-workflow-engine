package server

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// bufferedLog skips the per-event fsync, so a benchmark measures the engine
// (locking, reads, replay) rather than the disk.
type bufferedLog struct{ s *history.HistoryStore }

func (l bufferedLog) Append(id string, ev *history.HistoryEvent) error {
	return l.s.AppendBuffered(id, ev)
}
func (l bufferedLog) Delete(id string) error { return l.s.Delete(id) }
func (l bufferedLog) NewRun(id, archive string, ev *history.HistoryEvent, more ...*history.HistoryEvent) error {
	return l.s.NewRun(id, archive, ev, more...)
}

func benchEngine(b *testing.B, fsync bool, def state.WorkflowDefinition) *Engine {
	b.Helper()
	store, err := history.NewHistoryStore(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { store.Close() })
	var opts []Option
	if !fsync {
		opts = append(opts, WithHistoryLog(bufferedLog{store}))
	}
	e := NewEngine(store, discardLogger(), opts...)
	b.Cleanup(e.Close)
	e.Register(def)
	return e
}

// drain completes tasks until n of them are done.
func drain(b *testing.B, e *Engine, n int) {
	for range n {
		t, err := e.PollTask(context.Background(), "bench")
		if err != nil {
			b.Error(err)
			return
		}
		if err := e.CompleteTask(t.GetWorkflowId(), t.GetStepName(), t.GetSequence(), t.GetLeaseToken(), []byte("ok"), "", false); err != nil {
			b.Error(err)
			return
		}
	}
}

// BenchmarkLongWorkflow runs one workflow of many sequential steps: the cost of
// a step as its history grows. Reading and replaying the whole history on
// every operation makes this quadratic.
func BenchmarkLongWorkflow(b *testing.B) {
	for _, steps := range []int{100, 1000} {
		b.Run(fmt.Sprintf("steps=%d", steps), func(b *testing.B) {
			names := make([]string, steps)
			for i := range names {
				names[i] = fmt.Sprintf("s%d", i)
			}
			e := benchEngine(b, false, state.WorkflowDefinition{Type: "Long", Steps: state.Activities(names...)})
			b.ResetTimer()
			for range b.N {
				if _, err := e.StartWorkflow("Long", nil, ""); err != nil {
					b.Fatal(err)
				}
				drain(b, e, steps)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*steps), "ns/step")
		})
	}
}

// BenchmarkConcurrentWorkflows runs 64 workflows of 8 steps with 16 workers: how
// well independent workflows proceed in parallel, with and without fsync.
func BenchmarkConcurrentWorkflows(b *testing.B) {
	const workflows, steps, workers = 64, 8, 16
	for _, fsync := range []bool{false, true} {
		b.Run(fmt.Sprintf("fsync=%v", fsync), func(b *testing.B) {
			names := make([]string, steps)
			for i := range names {
				names[i] = fmt.Sprintf("s%d", i)
			}
			e := benchEngine(b, fsync, state.WorkflowDefinition{Type: "Short", Steps: state.Activities(names...)})
			b.ResetTimer()
			for range b.N {
				var wg sync.WaitGroup
				for range workers {
					wg.Go(func() { drain(b, e, workflows*steps/workers) })
				}
				for range workflows {
					if _, err := e.StartWorkflow("Short", nil, ""); err != nil {
						b.Fatal(err)
					}
				}
				wg.Wait()
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*workflows*steps), "ns/step")
		})
	}
}

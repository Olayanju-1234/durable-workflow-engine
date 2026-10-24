package server

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

func reuseEngine(t *testing.T) *Engine {
	t.Helper()
	return timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{
		"Quick": func(ctx workflow.Context, in []byte) ([]byte, error) {
			if string(in) == "fail" {
				return nil, errors.New("failed on purpose")
			}
			return in, nil
		},
		"Slow": waitsForever,
	})
}

func TestIDReusePolicies(t *testing.T) {
	const (
		reject     = IdReusePolicy_ID_REUSE_POLICY_REJECT_DUPLICATE
		allow      = IdReusePolicy_ID_REUSE_POLICY_ALLOW_DUPLICATE
		failedOnly = IdReusePolicy_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY
		useExist   = IdConflictPolicy_ID_CONFLICT_POLICY_USE_EXISTING
	)
	tests := []struct {
		name      string
		firstType string
		first     string // the first run's input ("fail" fails it)
		reuse     IdReusePolicy
		conflict  IdConflictPolicy
		want      string // "new", "existing", or an error to contain
	}{
		{"completed, default", "Quick", "ok", 0, 0, "has a finished run (COMPLETED)"},
		{"completed, reject", "Quick", "ok", reject, 0, "has a finished run"},
		{"completed, allow", "Quick", "ok", allow, 0, "new"},
		{"completed, failed only", "Quick", "ok", failedOnly, 0, "allows a new run only after a failure"},
		{"failed, failed only", "Quick", "fail", failedOnly, 0, "new"},
		{"completed, reject, use existing", "Quick", "ok", reject, useExist, "existing"},
		{"running, allow", "Slow", "", allow, 0, "is already running"},
		{"running, use existing", "Slow", "", allow, useExist, "existing"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := reuseEngine(t)
			first, err := e.Start(tt.firstType, []byte(tt.first), "order-7")
			if err != nil {
				t.Fatal(err)
			}
			if tt.firstType == "Quick" {
				waitFinished(t, e, "order-7", 2*time.Second)
			}
			got, err := e.Start("Quick", []byte("again"), "order-7", WithIDReusePolicy(tt.reuse), WithIDConflictPolicy(tt.conflict))
			runs, lerr := e.ListWorkflowRuns("order-7")
			if lerr != nil {
				t.Fatal(lerr)
			}
			switch tt.want {
			case "new":
				if err != nil || got.Existing || got.RunID == first.RunID || len(runs) != 2 || runs[0].GetRunId() != got.RunID {
					t.Fatalf("start: %+v %v; runs %v", got, err, runs)
				}
				if out := waitFinished(t, e, "order-7", 2*time.Second).GetOutput(); string(out) != "again" {
					t.Fatalf("the new run's output %q", out)
				}
			case "existing":
				if err != nil || !got.Existing || got.RunID != first.RunID || len(runs) != 1 {
					t.Fatalf("start: %+v %v, want the first run back; runs %v", got, err, runs)
				}
			default:
				if !errors.Is(err, ErrAlreadyExists) || !strings.Contains(err.Error(), tt.want) || len(runs) != 1 {
					t.Fatalf("start: %+v %v, want ErrAlreadyExists %q; runs %v", got, err, tt.want, runs)
				}
			}
		})
	}
}

// Many starts racing for one finished id: exactly one begins a run, and with
// USE_EXISTING every other gets that same run back, never a second one.
func TestIDReuseRace(t *testing.T) {
	for _, conflict := range []IdConflictPolicy{IdConflictPolicy_ID_CONFLICT_POLICY_USE_EXISTING, IdConflictPolicy_ID_CONFLICT_POLICY_FAIL} {
		t.Run(conflict.String(), func(t *testing.T) {
			e := reuseEngine(t)
			if _, err := e.Start("Quick", []byte("ok"), "race"); err != nil {
				t.Fatal(err)
			}
			waitFinished(t, e, "race", 2*time.Second)

			const n = 32
			var (
				wg      sync.WaitGroup
				mu      sync.Mutex
				started []string
				runIDs  = map[string]bool{}
				errs    []error
			)
			for range n {
				wg.Add(1)
				go func() {
					defer wg.Done()
					got, err := e.Start("Slow", nil, "race",
						WithIDReusePolicy(IdReusePolicy_ID_REUSE_POLICY_ALLOW_DUPLICATE), WithIDConflictPolicy(conflict))
					mu.Lock()
					defer mu.Unlock()
					switch {
					case err != nil:
						errs = append(errs, err)
					case !got.Existing:
						started = append(started, got.RunID)
						runIDs[got.RunID] = true
					default:
						runIDs[got.RunID] = true
					}
				}()
			}
			wg.Wait()
			if len(started) != 1 || len(runIDs) != 1 {
				t.Fatalf("%d starts began a run (%v), %d run ids returned", len(started), started, len(runIDs))
			}
			for _, err := range errs {
				if conflict == IdConflictPolicy_ID_CONFLICT_POLICY_USE_EXISTING || !errors.Is(err, ErrAlreadyExists) {
					t.Fatalf("unexpected error: %v", err)
				}
			}
			if conflict == IdConflictPolicy_ID_CONFLICT_POLICY_FAIL && len(errs) != n-1 {
				t.Fatalf("%d starts refused, want %d", len(errs), n-1)
			}
			runs, err := e.ListWorkflowRuns("race")
			if err != nil || len(runs) != 2 || runs[0].GetRunId() != started[0] || runs[0].GetStatus() != WorkflowStatus_WORKFLOW_STATUS_WAITING {
				t.Fatalf("runs %v %v, want the new run then the finished one", runs, err)
			}
		})
	}
}

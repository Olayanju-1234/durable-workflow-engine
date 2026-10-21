package server

import (
	"context"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/sdk"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

// In-process planners record markers too: each marker is applied and the
// workflow decided again in the same call, until a step needs a worker.
func TestInProcessMarkersChainIntoNextStep(t *testing.T) {
	wf := sdk.Workflow{Type: "Versioned", Fn: func(ctx workflow.Context, in []byte) ([]byte, error) {
		v := ctx.GetVersion("change", workflow.DefaultVersion, 2)
		id := ctx.SideEffect("id", func() []byte { return []byte("id-1") })
		return ctx.ExecuteActivity("work", append(id, byte('0'+v)))
	}}
	e := newFuncEngine(t, t.TempDir(), wf)
	id, err := e.StartWorkflow("Versioned", nil, "wf-1")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	task, err := e.PollTask(ctx, "w", "work")
	if err != nil || string(task.GetInput()) != "id-12" || task.GetSequence() != 3 {
		t.Fatalf("task %v %v, want work/3 with input id-12", task, err)
	}
	evs, _ := e.store.ReadAll(id)
	st, err := state.Reconstruct(evs)
	if err != nil {
		t.Fatal(err)
	}
	if st.Steps[0].Marker != state.MarkerVersion || st.Steps[1].Marker != state.MarkerSideEffect || st.Steps[2].Kind != state.KindActivity {
		t.Fatalf("steps %+v", st.Steps)
	}
}

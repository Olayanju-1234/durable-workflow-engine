package server

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/replay"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

var flowDef = state.WorkflowDefinition{Type: "Flow", Steps: state.Activities("work")}

// nsEngine runs Flow in the default namespace and in acme and beta.
func nsEngine(t *testing.T, dir string, opts ...Option) *Engine {
	t.Helper()
	store, err := history.NewHistoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	e := NewEngine(store, discardLogger(), append([]Option{WithLeaseDuration(time.Minute)}, opts...)...)
	t.Cleanup(e.Close)
	e.Register(flowDef)
	e.In("acme").RegisterWorkflow(flowDef.Type, flowDef)
	e.In("beta").RegisterWorkflow(flowDef.Type, flowDef)
	return e
}

func pollIn(t *testing.T, sc *Scope, d time.Duration, steps ...string) (*Task, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return sc.PollTask(ctx, "worker-"+sc.Namespace(), "", steps...)
}

func statusIn(t *testing.T, sc *Scope, id string) string {
	t.Helper()
	resp, err := sc.GetWorkflowRun(id, "")
	if err != nil {
		t.Fatalf("%s: get %s: %v", sc.Namespace(), id, err)
	}
	return statusName(resp.GetSummary().GetStatus())
}

// The same workflow id in three namespaces is three workflows: each namespace
// lists, describes, polls, completes, signals and ends only its own.
func TestNamespacesIsolateWorkflows(t *testing.T) {
	e := nsEngine(t, t.TempDir())
	def, acme, beta := e.In(""), e.In("acme"), e.In("beta")
	for _, sc := range []*Scope{def, acme, beta} {
		if _, err := sc.Start("Flow", []byte(sc.Namespace()), "order-1"); err != nil {
			t.Fatalf("%s: start: %v", sc.Namespace(), err)
		}
	}
	for _, sc := range []*Scope{def, acme, beta} {
		sums, err := sc.ListWorkflows(WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED)
		if err != nil || len(sums) != 1 || sums[0].GetWorkflowId() != "order-1" {
			t.Fatalf("%s: list = %v, %v; want only order-1", sc.Namespace(), sums, err)
		}
		resp, err := sc.GetWorkflowRun("order-1", "")
		if err != nil || string(resp.GetInput()) != sc.Namespace() || resp.GetSummary().GetWorkflowId() != "order-1" {
			t.Fatalf("%s: describe = %v, %v", sc.Namespace(), resp, err)
		}
	}

	// A poller asking for any activity still only sees its own namespace.
	bt, err := pollIn(t, beta, time.Second)
	if err != nil || string(bt.GetInput()) != "beta" || bt.GetWorkflowId() != "order-1" || bt.GetActivityId() != "order-1/1" {
		t.Fatalf("beta poll = %v, %v; want beta's task, named locally", bt, err)
	}
	if extra, err := pollIn(t, beta, 100*time.Millisecond); err == nil {
		t.Fatalf("beta polled a second task, %q's: namespaces leak", extra.GetInput())
	}
	at, err := pollIn(t, acme, time.Second, "work")
	if err != nil || string(at.GetInput()) != "acme" {
		t.Fatalf("acme poll = %v, %v", at, err)
	}

	// acme's lease cannot be used from beta, even though the ids match.
	if err := beta.CompleteTask(at.GetWorkflowId(), at.GetStepName(), at.GetSequence(), at.GetLeaseToken(), []byte("x")); err == nil {
		t.Fatal("beta completed acme's task with acme's lease token")
	}
	if err := beta.Heartbeat(at.GetWorkflowId(), at.GetLeaseToken()); err == nil {
		t.Fatal("beta heartbeated acme's lease")
	}
	if got := statusIn(t, beta, "order-1"); got != "WAITING" {
		t.Fatalf("beta's workflow is %s after the cross-namespace completion, want WAITING", got)
	}
	if err := acme.CompleteTask(at.GetWorkflowId(), at.GetStepName(), at.GetSequence(), at.GetLeaseToken(), []byte("x")); err != nil {
		t.Fatalf("acme completes its own task: %v", err)
	}
	if got := statusIn(t, acme, "order-1"); got != "COMPLETED" {
		t.Fatalf("acme's workflow is %s, want COMPLETED", got)
	}

	// Ending one namespace's workflow leaves the others alone.
	if err := beta.TerminateWorkflow("order-1", "test"); err != nil {
		t.Fatal(err)
	}
	if got := statusIn(t, def, "order-1"); got != "WAITING" {
		t.Fatalf("default's workflow is %s after beta's terminate, want WAITING", got)
	}
	if err := def.SignalWorkflowOnce("order-1", "nudge", "", nil); err != nil {
		t.Fatalf("signal default: %v", err)
	}
	// Started, scheduled, terminated: nothing of default's signal.
	if resp, _ := beta.GetWorkflowRun("order-1", ""); resp.GetSummary().GetHistoryLength() != 3 {
		t.Fatalf("beta's history has %d events, want 3", resp.GetSummary().GetHistoryLength())
	}
	if resp, _ := def.GetWorkflowRun("order-1", ""); resp.GetSummary().GetHistoryLength() != 3 {
		t.Fatalf("default's history has %d events, want 3 (its signal)", resp.GetSummary().GetHistoryLength())
	}
}

// A client's id can never name another namespace's workflow, from any scope.
func TestNamespaceIDsCannotEscape(t *testing.T) {
	e := nsEngine(t, t.TempDir())
	if _, err := e.In("acme").Start("Flow", nil, "order-1"); err != nil {
		t.Fatal(err)
	}
	for _, sc := range []*Scope{e.In(""), e.In("beta")} {
		for _, id := range []string{"acme/order-1", "../acme/order-1", `acme\order-1`} {
			if _, err := sc.GetWorkflowRun(id, ""); !errors.Is(err, ErrNotFound) {
				t.Errorf("%s: describe %q: %v, want ErrNotFound", sc.Namespace(), id, err)
			}
			if err := sc.TerminateWorkflow(id, ""); !errors.Is(err, ErrNotFound) {
				t.Errorf("%s: terminate %q: %v, want ErrNotFound", sc.Namespace(), id, err)
			}
			if err := sc.SignalWorkflowOnce(id, "s", "", nil); !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("%s: signal %q: %v, want ErrInvalidArgument", sc.Namespace(), id, err)
			}
			if _, err := sc.Start("Flow", nil, id); !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("%s: start %q: %v, want ErrInvalidArgument", sc.Namespace(), id, err)
			}
		}
	}
	if got := statusIn(t, e.In("acme"), "order-1"); got != "WAITING" {
		t.Fatalf("acme's workflow is %s", got)
	}
	for _, bad := range []string{"Acme", "default/x", "a_b", "-a", ""} {
		if bad == "" {
			continue // "" is the default namespace
		}
		if _, err := e.In(bad).ListWorkflows(0); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("namespace %q: %v, want ErrInvalidArgument", bad, err)
		}
	}
}

// Workflow types, SDK registrations and their decisions belong to a namespace.
func TestNamespacesIsolateWorkflowTypes(t *testing.T) {
	e := nsEngine(t, t.TempDir())
	if _, err := e.In("gamma").Start("Flow", nil, "x"); !errors.Is(err, ErrUnknownType) {
		t.Fatalf("start Flow in gamma, where it is not registered: %v", err)
	}
	if err := e.In("acme").RegisterWorkflowTypes([]string{"Remote"}, "sdk-acme", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.In("beta").Start("Remote", nil, "r1"); !errors.Is(err, ErrUnknownType) {
		t.Fatalf("start acme's Remote in beta: %v, want ErrUnknownType", err)
	}
	// An SDK may register a type the default namespace runs in-process.
	if err := e.In("acme").RegisterWorkflowTypes([]string{"Flow2"}, "sdk-acme", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.In("acme").Start("Remote", nil, "r1"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if task, err := e.In("beta").PollWorkflowTask(ctx, "sdk-beta", "", []string{"Remote"}); err == nil {
		t.Fatalf("beta received acme's decision for %s", task.GetWorkflowId())
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	task, err := e.In("acme").PollWorkflowTask(ctx2, "sdk-acme", "", nil)
	if err != nil || task.GetWorkflowId() != "r1" {
		t.Fatalf("acme's decision = %v, %v", task, err)
	}
	if err := e.In("beta").CompleteWorkflowTask("r1", task.GetTaskToken(), nil, ""); !errors.Is(err, ErrStaleDecision) {
		t.Fatalf("beta answered acme's decision: %v, want ErrStaleDecision", err)
	}
}

// Schedules are per namespace, and survive a restart there.
func TestNamespacesIsolateSchedules(t *testing.T) {
	dir := t.TempDir()
	e := nsEngine(t, dir)
	spec := &Schedule{ScheduleId: "nightly", Cron: "@every 1h", WorkflowType: "Flow"}
	if err := e.In("acme").CreateSchedule(spec); err != nil {
		t.Fatal(err)
	}
	for _, sc := range []*Scope{e.In(""), e.In("beta")} {
		if got, _ := sc.ListSchedules(); len(got) != 0 {
			t.Fatalf("%s lists acme's schedule: %v", sc.Namespace(), got)
		}
		if err := sc.DeleteSchedule("nightly"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("%s deleted acme's schedule: %v", sc.Namespace(), err)
		}
	}
	e.Close()

	e2 := nsEngine(t, dir)
	if err := e2.Recover(); err != nil {
		t.Fatal(err)
	}
	got, err := e2.In("acme").ListSchedules()
	if err != nil || len(got) != 1 || got[0].GetSchedule().GetScheduleId() != "nightly" {
		t.Fatalf("acme's schedules after restart = %v, %v", got, err)
	}
	if got, _ := e2.ListSchedules(); len(got) != 0 {
		t.Fatalf("default lists acme's schedule after restart: %v", got)
	}
}

// An activity in flight in a namespace comes back there after a restart, and
// only there.
func TestNamespaceWorkflowRecovers(t *testing.T) {
	dir := t.TempDir()
	e := nsEngine(t, dir)
	if _, err := e.In("acme").Start("Flow", []byte("acme"), "order-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := pollIn(t, e.In("acme"), time.Second); err != nil {
		t.Fatal(err)
	}
	e.Close()

	e2 := nsEngine(t, dir)
	if err := e2.Recover(); err != nil {
		t.Fatal(err)
	}
	if _, err := pollIn(t, e2.In(""), 100*time.Millisecond); err == nil {
		t.Fatal("the default namespace received acme's recovered task")
	}
	task, err := pollIn(t, e2.In("acme"), time.Second)
	if err != nil || task.GetWorkflowId() != "order-1" || string(task.GetInput()) != "acme" {
		t.Fatalf("acme's recovered task = %v, %v", task, err)
	}
	if e2.store.Format() < history.FormatNamespaces {
		t.Fatalf("data dir format %d, want at least %d", e2.store.Format(), history.FormatNamespaces)
	}
}

// Retention follows each namespace's own setting.
func TestNamespaceRetention(t *testing.T) {
	short := time.Millisecond
	set, err := NewNamespaceSet([]NamespaceConfig{{Name: "acme", Retention: &short}})
	if err != nil {
		t.Fatal(err)
	}
	e := nsEngine(t, t.TempDir(), WithNamespaces(set))
	for _, sc := range []*Scope{e.In(""), e.In("acme"), e.In("beta")} {
		if _, err := sc.Start("Flow", nil, "done"); err != nil {
			t.Fatal(err)
		}
		if err := sc.TerminateWorkflow("done", "finish it"); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(10 * time.Millisecond)
	// default and beta inherit an engine retention of an hour; acme's is 1ms.
	n, err := e.purge(e.retentionCutoffs(time.Now(), time.Hour))
	if err != nil || n != 1 {
		t.Fatalf("purged %d (%v), want 1", n, err)
	}
	if _, err := e.In("acme").GetWorkflowRun("done", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("acme's finished workflow survived its retention: %v", err)
	}
	for _, sc := range []*Scope{e.In(""), e.In("beta")} {
		if _, err := sc.GetWorkflowRun("done", ""); err != nil {
			t.Fatalf("%s's workflow was purged under acme's retention: %v", sc.Namespace(), err)
		}
	}
	// Zero engine retention keeps everything but acme's.
	if n, _ := e.purge(e.retentionCutoffs(time.Now(), 0)); n != 0 {
		t.Fatalf("purged %d with retention off, want 0", n)
	}
}

// Continue-as-new and id reuse keep a namespaced workflow's earlier runs in
// its namespace.
func TestNamespaceRunsStayInNamespace(t *testing.T) {
	dir := t.TempDir()
	e := nsEngine(t, dir)
	acme := e.In("acme")
	if _, err := acme.Start("Flow", nil, "order-1"); err != nil {
		t.Fatal(err)
	}
	if err := acme.TerminateWorkflow("order-1", "first run"); err != nil {
		t.Fatal(err)
	}
	if _, err := acme.Start("Flow", nil, "order-1", WithIDReusePolicy(IdReusePolicy_ID_REUSE_POLICY_ALLOW_DUPLICATE)); err != nil {
		t.Fatal(err)
	}
	runs, err := acme.ListWorkflowRuns("order-1")
	if err != nil || len(runs) != 2 {
		t.Fatalf("acme's runs = %v, %v; want 2", runs, err)
	}
	if _, err := e.In("").ListWorkflowRuns("order-1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("default sees acme's runs: %v", err)
	}
	if old, err := acme.GetWorkflowRun("order-1", runs[1].GetRunId()); err != nil || old.GetSummary().GetWorkflowId() != "order-1" {
		t.Fatalf("acme's earlier run = %v, %v", old, err)
	}
	entries, err := os.ReadDir(filepath.Join(dir, "namespaces", "acme"))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, en := range entries {
		names = append(names, en.Name())
	}
	if len(names) != 2 || !slices.Contains(names, "order-1.history") {
		t.Fatalf("acme's directory holds %v, want the current run and one archived run", names)
	}
}

// Workflow code sees its own id, never the namespace's key, in every run.
func TestNamespacedWorkflowSeesItsOwnID(t *testing.T) {
	e := nsEngine(t, t.TempDir())
	acme := e.In("acme")
	acme.RegisterWorkflow("WhoAmI", replay.Planner{Fn: func(ctx workflow.Context, in []byte) ([]byte, error) {
		if string(in) == "again" {
			return nil, workflow.ContinueAsNew([]byte("done"))
		}
		return []byte(ctx.Info().WorkflowID), nil
	}})
	if _, err := acme.Start("WhoAmI", []byte("again"), "me"); err != nil {
		t.Fatal(err)
	}
	resp, err := acme.GetWorkflowRun("me", "")
	if err != nil || string(resp.GetOutput()) != "me" {
		t.Fatalf("the continued run saw its id as %q (%v), want \"me\"", resp.GetOutput(), err)
	}
	evs, err := e.store.ReadAll("acme/me")
	if err != nil || evs[0].GetWorkflowStarted().GetWorkflowId() != "me" {
		t.Fatalf("the run's started event names %q", evs[0].GetWorkflowStarted().GetWorkflowId())
	}
	if _, err := acme.Start("WhoAmI", []byte("first"), "me2"); err != nil {
		t.Fatal(err)
	}
	if resp, _ := acme.GetWorkflowRun("me2", ""); string(resp.GetOutput()) != "me2" {
		t.Fatalf("a first run saw its id as %q", resp.GetOutput())
	}
}

// Recovery decides a namespaced workflow with its namespace's planner, even
// when the type exists nowhere else.
func TestNamespaceRecoveryDecidesInNamespace(t *testing.T) {
	dir := t.TempDir()
	writeRaw(t, dir, "acme/r1",
		&history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowStarted{WorkflowStarted: &history.WorkflowStartedEvent{
			WorkflowId: "r1", WorkflowType: "AcmeOnly", RunId: "run"}}},
		&history.HistoryEvent{Attributes: &history.HistoryEvent_StepScheduled{StepScheduled: &history.StepScheduledEvent{StepName: "one", Sequence: 1}}},
		&history.HistoryEvent{Attributes: &history.HistoryEvent_StepCompleted{StepCompleted: &history.StepCompletedEvent{StepName: "one", Sequence: 1}}})
	e := nsEngine(t, dir)
	e.In("acme").RegisterWorkflow("AcmeOnly", state.WorkflowDefinition{Type: "AcmeOnly", Steps: state.Activities("one", "two")})
	if err := e.Recover(); err != nil {
		t.Fatal(err)
	}
	task, err := pollIn(t, e.In("acme"), time.Second, "two")
	if err != nil || task.GetWorkflowId() != "r1" {
		t.Fatalf("acme's next step after recovery = %v, %v", task, err)
	}
}

// A namespace's schedule starts its runs in that namespace.
func TestNamespaceScheduleFiresInNamespace(t *testing.T) {
	e := nsEngine(t, t.TempDir())
	if err := e.In("acme").CreateSchedule(&Schedule{ScheduleId: "tick", Cron: "@every 1s", WorkflowType: "Flow"}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if sums, _ := e.In("acme").ListWorkflows(0); len(sums) > 0 {
			if !strings.HasPrefix(sums[0].GetWorkflowId(), "tick-") {
				t.Fatalf("acme's run is %q", sums[0].GetWorkflowId())
			}
			if def, _ := e.ListWorkflows(0); len(def) != 0 {
				t.Fatalf("the default namespace holds %v", def)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("acme's schedule never started a run in acme")
}

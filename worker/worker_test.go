package worker_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/olayanju-1234/durable-workflow-engine/client"
	"github.com/olayanju-1234/durable-workflow-engine/embedded"
	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
	"github.com/olayanju-1234/durable-workflow-engine/worker"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// order reserves, sleeps, waits for approval, then charges; a failed charge
// releases the reservation (Saga compensation).
func order(ctx workflow.Context, input []byte) ([]byte, error) {
	reserved, err := ctx.ExecuteActivity("reserve", input)
	if err != nil {
		return nil, err
	}
	ctx.Sleep("settle", 20*time.Millisecond)
	if approval := ctx.WaitForSignal("approval", "approve"); string(approval) != "yes" {
		return nil, errors.New("not approved")
	}
	if _, err := ctx.ExecuteActivity("charge", reserved); err != nil {
		ctx.ExecuteActivity("release", reserved)
		return nil, err
	}
	return ctx.ExecuteActivity("ship", reserved)
}

func registerOrder(w *worker.Worker) {
	w.RegisterWorkflow("Order", order)
	w.RegisterActivity("reserve", func(_ context.Context, in []byte) ([]byte, error) { return append([]byte("reserved:"), in...), nil })
	w.RegisterActivity("charge", func(_ context.Context, in []byte) ([]byte, error) {
		if strings.Contains(string(in), "decline") {
			return nil, errors.New("card declined")
		}
		return []byte("charged"), nil
	})
	w.RegisterActivity("release", func(_ context.Context, in []byte) ([]byte, error) { return []byte("released"), nil })
	w.RegisterActivity("ship", func(_ context.Context, in []byte) ([]byte, error) { return []byte("shipped:" + string(in)), nil })
}

func startEmbedded(t *testing.T, opts embedded.Options) *embedded.Server {
	t.Helper()
	if opts.DataDir == "" {
		opts.DataDir = t.TempDir()
	}
	srv, err := embedded.Start(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	return srv
}

// run starts w and stops it when the test ends.
func run(t *testing.T, w *worker.Worker) {
	t.Helper()
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)
}

func testCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestOrderWorkflowEndToEnd(t *testing.T) {
	srv := startEmbedded(t, embedded.Options{})
	c := srv.Client()
	w := worker.New(c, worker.Options{Logger: quiet})
	registerOrder(w)
	run(t, w)
	ctx := testCtx(t)

	okID, err := c.StartWorkflow(ctx, "Order", []byte("o-1"))
	if err != nil {
		t.Fatal(err)
	}
	declineID, err := c.StartWorkflow(ctx, "Order", []byte("decline-2"), client.WithWorkflowID("order-decline-2"))
	if err != nil || declineID != "order-decline-2" {
		t.Fatalf("start with id: %q %v", declineID, err)
	}
	for _, id := range []string{okID, declineID} {
		if err := c.SignalWorkflow(ctx, id, "approve", []byte("yes")); err != nil {
			t.Fatal(err)
		}
	}

	out, err := c.WaitForResult(ctx, okID)
	if err != nil || string(out) != "shipped:reserved:o-1" {
		t.Fatalf("happy path: %q %v", out, err)
	}
	_, err = c.WaitForResult(ctx, declineID)
	if !errors.Is(err, client.ErrWorkflowFailed) || !strings.Contains(err.Error(), "card declined") {
		t.Fatalf("declined order: want ErrWorkflowFailed with the charge error, got %v", err)
	}
	d, err := c.DescribeWorkflow(ctx, declineID)
	if err != nil {
		t.Fatal(err)
	}
	var steps []string
	for _, s := range d.Steps {
		steps = append(steps, s.Name+"/"+s.Kind+"/"+s.Status)
	}
	want := "reserve/ACTIVITY/COMPLETED settle/TIMER/COMPLETED approval/SIGNAL/COMPLETED charge/ACTIVITY/FAILED release/ACTIVITY/COMPLETED"
	if got := strings.Join(steps, " "); got != want || d.Status != client.StatusFailed {
		t.Fatalf("steps %q status %s\nwant %q FAILED", got, d.Status, want)
	}
}

// An activity that runs longer than its lease keeps it by heartbeating, so it
// completes on the first attempt; it can also see which step it serves.
func TestLongActivityHeartbeats(t *testing.T) {
	srv := startEmbedded(t, embedded.Options{LeaseDuration: 300 * time.Millisecond})
	c := srv.Client()
	w := worker.New(c, worker.Options{Logger: quiet})
	w.RegisterWorkflow("Slow", func(ctx workflow.Context, in []byte) ([]byte, error) {
		return ctx.ExecuteActivity("slow", in)
	})
	w.RegisterActivity("slow", func(ctx context.Context, in []byte) ([]byte, error) {
		info, _ := worker.GetActivityInfo(ctx)
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return []byte(info.WorkflowID + "/" + info.StepName), nil
	})
	run(t, w)
	ctx := testCtx(t)

	id, err := c.StartWorkflow(ctx, "Slow", nil, client.WithWorkflowID("slow-1"))
	if err != nil {
		t.Fatal(err)
	}
	out, err := c.WaitForResult(ctx, id)
	if err != nil || string(out) != "slow-1/slow" {
		t.Fatalf("got %q %v", out, err)
	}
	d, _ := c.DescribeWorkflow(ctx, id)
	if d.Steps[0].Attempts != 1 {
		t.Fatalf("activity took %d attempts; heartbeats should have kept its lease", d.Steps[0].Attempts)
	}
}

func TestPanickingWorkflowFails(t *testing.T) {
	srv := startEmbedded(t, embedded.Options{})
	c := srv.Client()
	w := worker.New(c, worker.Options{Logger: quiet})
	w.RegisterWorkflow("Buggy", func(ctx workflow.Context, in []byte) ([]byte, error) {
		var p *struct{ x int }
		return []byte{byte(p.x)}, nil
	})
	run(t, w)
	ctx := testCtx(t)

	id, err := c.StartWorkflow(ctx, "Buggy", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.WaitForResult(ctx, id); !errors.Is(err, client.ErrWorkflowFailed) || !strings.Contains(err.Error(), "panicked") {
		t.Fatalf("got %v, want the workflow failed with the panic", err)
	}
}

func TestClientErrors(t *testing.T) {
	srv := startEmbedded(t, embedded.Options{})
	c := srv.Client()
	w := worker.New(c, worker.Options{Logger: quiet})
	registerOrder(w)
	run(t, w)
	ctx := testCtx(t)

	if _, err := c.DescribeWorkflow(ctx, "nope"); !errors.Is(err, client.ErrNotFound) {
		t.Errorf("describe unknown: %v", err)
	}
	if _, err := c.StartWorkflow(ctx, "NoSuchType", nil); !errors.Is(err, client.ErrInvalid) {
		t.Errorf("start unknown type: %v", err)
	}
	if _, err := c.StartWorkflow(ctx, "Order", nil, client.WithWorkflowID("dup")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.StartWorkflow(ctx, "Order", nil, client.WithWorkflowID("dup")); !errors.Is(err, client.ErrAlreadyExists) {
		t.Errorf("duplicate id: %v", err)
	}
	if err := c.TerminateWorkflow(ctx, "dup", "test"); err != nil {
		t.Fatal(err)
	}
	if err := c.SignalWorkflow(ctx, "dup", "approve", nil); !errors.Is(err, client.ErrNotRunning) {
		t.Errorf("signal terminated workflow: %v", err)
	}
	if _, err := c.WaitForResult(ctx, "dup"); !errors.Is(err, client.ErrWorkflowFailed) || !strings.Contains(err.Error(), "TERMINATED") {
		t.Errorf("result of terminated workflow: %v", err)
	}
}

func TestStartErrors(t *testing.T) {
	srv := startEmbedded(t, embedded.Options{})
	if err := worker.New(srv.Client(), worker.Options{Logger: quiet}).Start(context.Background()); err == nil {
		t.Error("Start with nothing registered should fail")
	}

	// Run keeps going while the engine is unreachable, and stops with its ctx.
	c, err := client.Dial(client.Options{Addrs: []string{"127.0.0.1:1"}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	w := worker.New(c, worker.Options{Logger: quiet})
	w.RegisterWorkflow("Order", order)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := w.Run(ctx); err != nil {
		t.Errorf("Run against an unreachable engine: %v", err)
	}
}

// tcpEngine serves a real engine on addr, as workflowd does.
type tcpEngine struct {
	store  *history.HistoryStore
	engine *server.Engine
	gs     *grpc.Server
}

func startTCPEngine(t *testing.T, dir, addr string) *tcpEngine {
	t.Helper()
	store, err := history.NewHistoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	e := server.NewEngine(store, quiet)
	if err := e.Recover(); err != nil {
		t.Fatal(err)
	}
	var lis net.Listener
	for range 50 { // the old listener's port can take a moment to free
		if lis, err = net.Listen("tcp", addr); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	server.RegisterWorkflowServiceServer(gs, server.NewGRPCServer(e, quiet))
	go gs.Serve(lis)
	return &tcpEngine{store: store, engine: e, gs: gs}
}

func (te *tcpEngine) stop() {
	te.gs.Stop()
	te.engine.Close()
	te.store.Close()
}

// The engine forgets SDK registrations when it restarts. The worker must notice
// the dropped connection and register again, and the workflow must finish
// even though its signal arrives before the worker is back.
func TestWorkerReregistersAfterEngineRestart(t *testing.T) {
	dir := t.TempDir()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()

	te := startTCPEngine(t, dir, addr)
	c, err := client.Dial(client.Options{Addrs: []string{addr}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	w := worker.New(c, worker.Options{Logger: quiet, PollTimeout: time.Second})
	registerOrder(w)
	run(t, w)
	ctx := testCtx(t)

	id, err := c.StartWorkflow(ctx, "Order", []byte("o-9"))
	if err != nil {
		t.Fatal(err)
	}
	for { // wait until it is parked on the approval signal
		d, err := c.DescribeWorkflow(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if n := len(d.Steps); n == 3 && d.Steps[2].Kind == "SIGNAL" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	te.stop()
	te = startTCPEngine(t, dir, addr)
	defer te.stop()
	if err := c.SignalWorkflow(ctx, id, "approve", []byte("yes")); err != nil {
		t.Fatal(err)
	}
	out, err := c.WaitForResult(ctx, id)
	if err != nil || string(out) != "shipped:reserved:o-9" {
		t.Fatalf("after restart: %q %v", out, err)
	}
}

// A deploy that changes workflow code mid-flight: v1 parks an execution on a
// signal, v2 (which inserts a fraud check before the signal, behind GetVersion)
// replaces the worker. The parked execution finishes on the old branch; a new
// one takes the new branch.
func TestVersionedDeploy(t *testing.T) {
	srv := startEmbedded(t, embedded.Options{})
	c := srv.Client()
	ctx := testCtx(t)
	activities := func(w *worker.Worker) {
		for _, name := range []string{"reserve", "fraud-check", "charge"} {
			w.RegisterActivity(name, func(_ context.Context, in []byte) ([]byte, error) { return append(in, '+'), nil })
		}
	}
	v1 := func(ctx workflow.Context, in []byte) ([]byte, error) {
		r, _ := ctx.ExecuteActivity("reserve", in)
		ctx.WaitForSignal("approval", "approve")
		return ctx.ExecuteActivity("charge", r)
	}
	v2 := func(ctx workflow.Context, in []byte) ([]byte, error) {
		r, _ := ctx.ExecuteActivity("reserve", in)
		if ctx.GetVersion("fraud-check", workflow.DefaultVersion, 1) == 1 {
			r, _ = ctx.ExecuteActivity("fraud-check", r)
		}
		ctx.WaitForSignal("approval", "approve")
		return ctx.ExecuteActivity("charge", r)
	}

	w1 := worker.New(c, worker.Options{Logger: quiet})
	w1.RegisterWorkflow("Pay", v1)
	activities(w1)
	if err := w1.Start(ctx); err != nil {
		t.Fatal(err)
	}
	old, err := c.StartWorkflow(ctx, "Pay", []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	for {
		d, err := c.DescribeWorkflow(ctx, old)
		if err != nil {
			t.Fatal(err)
		}
		if d.Status == client.StatusWaiting && len(d.Steps) == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	w1.Stop()

	w2 := worker.New(c, worker.Options{Logger: quiet})
	w2.RegisterWorkflow("Pay", v2)
	activities(w2)
	run(t, w2)
	fresh, err := c.StartWorkflow(ctx, "Pay", []byte("new"))
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{old, fresh} {
		if err := c.SignalWorkflow(ctx, id, "approve", nil); err != nil {
			t.Fatal(err)
		}
	}

	for id, want := range map[string]string{old: "old++", fresh: "new+++"} {
		out, err := c.WaitForResult(ctx, id)
		if err != nil || string(out) != want {
			t.Fatalf("%s: %q %v, want %q", id, out, err, want)
		}
	}
	d, _ := c.DescribeWorkflow(ctx, fresh)
	if d.Steps[1].Kind != "MARKER" || d.Steps[1].Name != "fraud-check" || string(d.Steps[1].Output) != "1" {
		t.Fatalf("new execution step 2 = %+v, want the version marker", d.Steps[1])
	}
}

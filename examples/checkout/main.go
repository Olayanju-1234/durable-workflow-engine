// Command checkout runs an order through stock, payment and shipping as a Saga:
// large orders wait for a manager's approval (a signal), and if payment fails
// the reserved stock is released (compensation).
//
//	go run ./cmd/workflowd --serve   # terminal 1: the engine
//	go run ./examples/checkout       # terminal 2: three orders, three outcomes
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/client"
	"github.com/olayanju-1234/durable-workflow-engine/worker"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

// Order is the workflow input. Payloads are bytes on the wire; JSON is the
// usual choice for anything structured.
type Order struct {
	ID          string `json:"id"`
	AmountCents int    `json:"amount_cents"`
	Card        string `json:"card"`
}

// approvalThreshold is the order size, in cents, that needs a manager.
const approvalThreshold = 100_000

// Checkout is the workflow.
func Checkout(ctx workflow.Context, input []byte) ([]byte, error) {
	var o Order
	if err := json.Unmarshal(input, &o); err != nil {
		return nil, fmt.Errorf("bad order: %w", err)
	}
	if _, err := ctx.ExecuteActivity("reserve-stock", input); err != nil {
		return nil, err
	}
	if o.AmountCents >= approvalThreshold {
		ctx.Logger().Info("waiting for a manager", "order", o.ID)
		// Parks durably until someone sends the "approval" signal: seconds or weeks.
		if decision := ctx.WaitForSignal("manager-approval", "approval"); string(decision) != "approve" {
			ctx.ExecuteActivity("release-stock", input)
			return nil, fmt.Errorf("order %s rejected by a manager", o.ID)
		}
	}
	receipt, err := ctx.ExecuteActivity("charge-card", input)
	if err != nil {
		ctx.ExecuteActivity("release-stock", input) // compensate the reservation
		return nil, fmt.Errorf("payment failed, stock released: %w", err)
	}
	return ctx.ExecuteActivity("ship-order", receipt)
}

// shop is a stand-in for the services the activities would call.
type shop struct {
	mu       sync.Mutex
	reserved map[string]bool
	released []string
}

func (s *shop) reserve(_ context.Context, in []byte) ([]byte, error) {
	o, err := decode(in)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reserved[o.ID] = true
	return nil, nil
}

func (s *shop) release(_ context.Context, in []byte) ([]byte, error) {
	o, err := decode(in)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.reserved, o.ID)
	s.released = append(s.released, o.ID)
	return nil, nil
}

func (s *shop) charge(ctx context.Context, in []byte) ([]byte, error) {
	o, err := decode(in)
	if err != nil {
		return nil, err
	}
	if o.Card == "declined" {
		return nil, errors.New("card declined")
	}
	// Activities run at least once; the step's id makes a safe idempotency key.
	info, _ := worker.GetActivityInfo(ctx)
	return fmt.Appendf(nil, "receipt for %s (key %s/%d)", o.ID, info.WorkflowID, info.Sequence), nil
}

func ship(_ context.Context, receipt []byte) ([]byte, error) {
	return append([]byte("shipped, "), receipt...), nil
}

func decode(in []byte) (Order, error) {
	var o Order
	err := json.Unmarshal(in, &o)
	return o, err
}

func run(ctx context.Context, c *client.Client, out io.Writer) error {
	s := &shop{reserved: map[string]bool{}}
	w := worker.New(c, worker.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	w.RegisterWorkflow("Checkout", Checkout)
	w.RegisterActivity("reserve-stock", s.reserve)
	w.RegisterActivity("release-stock", s.release)
	w.RegisterActivity("charge-card", s.charge)
	w.RegisterActivity("ship-order", ship)
	if err := w.Start(ctx); err != nil {
		return err
	}
	defer w.Stop()

	orders := []Order{
		{ID: "small", AmountCents: 2_500, Card: "ok"},
		{ID: "large", AmountCents: 250_000, Card: "ok"},
		{ID: "declined", AmountCents: 4_000, Card: "declined"},
	}
	ids := make([]string, len(orders))
	for i, o := range orders {
		input, _ := json.Marshal(o)
		id, err := c.StartWorkflow(ctx, "Checkout", input)
		if err != nil {
			return err
		}
		ids[i] = id
	}

	// The large order is parked on its signal; a manager approves it.
	if err := waitForSignalWait(ctx, c, ids[1]); err != nil {
		return err
	}
	fmt.Fprintln(out, "large: waiting for a manager... approving")
	if err := c.SignalWorkflow(ctx, ids[1], "approval", []byte("approve")); err != nil {
		return err
	}

	for i, id := range ids {
		result, err := c.WaitForResult(ctx, id)
		if err != nil {
			fmt.Fprintf(out, "%s: %v\n", orders[i].ID, err)
			continue
		}
		fmt.Fprintf(out, "%s: %s\n", orders[i].ID, result)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	fmt.Fprintf(out, "reservations released by compensation: %v\n", s.released)
	return nil
}

func waitForSignalWait(ctx context.Context, c *client.Client, id string) error {
	for {
		d, err := c.DescribeWorkflow(ctx, id)
		if err != nil {
			return err
		}
		if n := len(d.Steps); n > 0 && d.Steps[n-1].Kind == "SIGNAL" {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func main() {
	addr := flag.String("addr", "localhost:7233", "workflowd gRPC address")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	c, err := client.Dial(client.Options{Addrs: []string{*addr}})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	if err := run(ctx, c, os.Stdout); err != nil {
		log.Fatal(err)
	}
}

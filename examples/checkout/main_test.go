package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/embedded"
	"github.com/olayanju-1234/durable-workflow-engine/testsuite"
)

func TestCheckoutAgainstARealEngine(t *testing.T) {
	srv, err := embedded.Start(embedded.Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var out bytes.Buffer
	if err := run(ctx, srv.Client(), &out); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"small: shipped, receipt for small",
		"large: shipped, receipt for large",
		"declined: client: workflow did not complete: Checkout-",
		"payment failed, stock released: card declined",
		"reservations released by compensation: [declined]",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, out.String())
		}
	}
}

// A manager's rejection a week later, with no server and no waiting.
func TestRejectedLargeOrderReleasesStock(t *testing.T) {
	env := testsuite.NewEnv()
	s := &shop{reserved: map[string]bool{}}
	env.RegisterActivity("reserve-stock", s.reserve)
	env.RegisterActivity("release-stock", s.release)
	env.SignalAfter(7*24*time.Hour, "approval", []byte("reject"))

	input, _ := json.Marshal(Order{ID: "big", AmountCents: 500_000, Card: "ok"})
	_, err := env.ExecuteWorkflow(Checkout, input)
	if !errors.Is(err, testsuite.ErrWorkflowFailed) || !strings.Contains(err.Error(), "rejected by a manager") {
		t.Fatalf("got %v", err)
	}
	if len(s.reserved) != 0 || len(s.released) != 1 || env.Elapsed() != 7*24*time.Hour {
		t.Fatalf("reserved %v, released %v after %s", s.reserved, s.released, env.Elapsed())
	}
}

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/embedded"
	"github.com/olayanju-1234/durable-workflow-engine/testsuite"
)

// Three real days of drip, tested in milliseconds.
func TestDripOverThreeDays(t *testing.T) {
	env := testsuite.NewEnv()
	m := &mailer{active: map[string]bool{}, out: io.Discard}
	m.register(env)
	input, _ := json.Marshal(Signup{Email: "ada@example.com", Day: 24 * time.Hour})

	out, err := env.ExecuteWorkflow(Onboarding, input)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"welcome→ada@example.com", "tips→ada@example.com", "check-in→ada@example.com"}
	if !slices.Equal(m.sent, want) || env.Elapsed() != 72*time.Hour {
		t.Fatalf("sent %v over %s", m.sent, env.Elapsed())
	}
	if string(out) != "drip finished at 2026-01-04T00:00:00Z" {
		t.Fatalf("result %q", out)
	}
}

func TestActiveUserStopsEarly(t *testing.T) {
	env := testsuite.NewEnv()
	m := &mailer{active: map[string]bool{"grace@example.com": true}, out: io.Discard}
	m.register(env)
	input, _ := json.Marshal(Signup{Email: "grace@example.com", Day: 24 * time.Hour})

	out, err := env.ExecuteWorkflow(Onboarding, input)
	if err != nil || !strings.Contains(string(out), "stopped early") || len(m.sent) != 1 {
		t.Fatalf("%q %v, sent %v", out, err, m.sent)
	}
}

func TestOnboardingAgainstARealEngine(t *testing.T) {
	srv, err := embedded.Start(embedded.Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var out bytes.Buffer
	if err := run(ctx, srv.Client(), Signup{Email: "ada@example.com", Day: 50 * time.Millisecond}, &out); err != nil {
		t.Fatal(err)
	}
	if strings.Count(out.String(), "sent ") != 3 || !strings.Contains(out.String(), "result: drip finished at") {
		t.Fatalf("output:\n%s", out.String())
	}
}

package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/embedded"
	"github.com/olayanju-1234/durable-workflow-engine/testsuite"
)

func TestHelloAgainstARealEngine(t *testing.T) {
	srv, err := embedded.Start(embedded.Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var out bytes.Buffer
	if err := run(ctx, srv.Client(), "Ada", &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "result: Hello, Ada! 👋") {
		t.Fatalf("output:\n%s", out.String())
	}
}

func TestGreetingUnit(t *testing.T) {
	env := testsuite.NewEnv()
	env.RegisterActivity("compose-greeting", ComposeGreeting)
	env.RegisterActivity("shout", Shout)
	out, err := env.ExecuteWorkflow(Greeting, []byte("Grace"))
	if err != nil || string(out) != "Hello, Grace! 👋" {
		t.Fatalf("%q %v", out, err)
	}
	if env.Elapsed() != time.Second {
		t.Fatalf("virtual time %s, want the 1s pause", env.Elapsed())
	}
}

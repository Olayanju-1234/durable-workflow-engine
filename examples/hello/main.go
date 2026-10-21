// Command hello is the smallest complete workflowd program: one workflow, one
// activity, a worker that runs both, and a client that starts the workflow and
// prints its result.
//
//	go run ./cmd/workflowd --serve   # terminal 1: the engine
//	go run ./examples/hello Ada      # terminal 2: this program
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/client"
	"github.com/olayanju-1234/durable-workflow-engine/worker"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

// Greeting is the workflow: orchestration only. It runs again from the top on
// every decision, so it must be deterministic; real work goes in activities.
func Greeting(ctx workflow.Context, name []byte) ([]byte, error) {
	greeting, err := ctx.ExecuteActivity("compose-greeting", name)
	if err != nil {
		return nil, err
	}
	ctx.Sleep("pause", time.Second) // a durable timer: it survives an engine restart
	return ctx.ExecuteActivity("shout", greeting)
}

// ComposeGreeting and Shout are activities: ordinary Go that may do anything.
// Each runs once per step and its result is recorded in history.
func ComposeGreeting(_ context.Context, name []byte) ([]byte, error) {
	return fmt.Appendf(nil, "Hello, %s!", name), nil
}

func Shout(_ context.Context, s []byte) ([]byte, error) {
	return append(s, " 👋"...), nil
}

func run(ctx context.Context, c *client.Client, name string, out io.Writer) error {
	w := worker.New(c, worker.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	w.RegisterWorkflow("Greeting", Greeting)
	w.RegisterActivity("compose-greeting", ComposeGreeting)
	w.RegisterActivity("shout", Shout)
	if err := w.Start(ctx); err != nil {
		return err
	}
	defer w.Stop()

	id, err := c.StartWorkflow(ctx, "Greeting", []byte(name))
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "started workflow %s\n", id)
	result, err := c.WaitForResult(ctx, id)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "result: %s\n", result)
	return nil
}

func main() {
	addr := flag.String("addr", "localhost:7233", "workflowd gRPC address")
	flag.Parse()
	name := "world"
	if flag.NArg() > 0 {
		name = flag.Arg(0)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	c, err := client.Dial(client.Options{Addrs: []string{*addr}})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	if err := run(ctx, c, name, os.Stdout); err != nil {
		log.Fatal(err)
	}
}

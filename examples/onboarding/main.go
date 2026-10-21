// Command onboarding sends a new user a drip of emails over several days: the
// kind of flow usually built from a queue, a cron job and a status column. Here
// it is one function whose sleeps are durable, so a deploy or crash mid-drip
// loses nothing.
//
//	go run ./cmd/workflowd --serve                  # terminal 1: the engine
//	go run ./examples/onboarding -day 3s ada@x.dev  # terminal 2: a "day" is 3s
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/client"
	"github.com/olayanju-1234/durable-workflow-engine/worker"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

// Signup is the workflow input. Day is part of the input, not a global, so
// every replay sees the same value.
type Signup struct {
	Email string        `json:"email"`
	Day   time.Duration `json:"day"`
}

// Onboarding is the workflow.
func Onboarding(ctx workflow.Context, input []byte) ([]byte, error) {
	var s Signup
	if err := json.Unmarshal(input, &s); err != nil {
		return nil, fmt.Errorf("bad signup: %w", err)
	}
	if _, err := send(ctx, s.Email, "welcome"); err != nil {
		return nil, err
	}
	ctx.Sleep("wait-1-day", s.Day)

	// Added after launch. Drips already past this point when it shipped keep
	// the old path (DefaultVersion); every later one checks activation first.
	if ctx.GetVersion("skip-tips-if-active", workflow.DefaultVersion, 1) == 1 {
		active, err := ctx.ExecuteActivity("is-active", []byte(s.Email))
		if err != nil {
			return nil, err
		}
		if string(active) == "yes" {
			return []byte("active after day 1; stopped early"), nil
		}
	}
	if _, err := send(ctx, s.Email, "tips"); err != nil {
		return nil, err
	}
	ctx.Sleep("wait-2-days", 2*s.Day)
	if _, err := send(ctx, s.Email, "check-in"); err != nil {
		return nil, err
	}
	// ctx.Now is the workflow's own clock: the same on every replay.
	return fmt.Appendf(nil, "drip finished at %s", ctx.Now().Format(time.RFC3339)), nil
}

func send(ctx workflow.Context, email, template string) ([]byte, error) {
	return ctx.ExecuteActivity("send-email:"+template, []byte(email))
}

// mailer stands in for an email provider and the product's activity check.
type mailer struct {
	mu     sync.Mutex
	sent   []string
	active map[string]bool
	out    io.Writer
}

func (m *mailer) sender(template string) worker.ActivityFunc {
	return func(_ context.Context, email []byte) ([]byte, error) {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.sent = append(m.sent, template+"→"+string(email))
		fmt.Fprintf(m.out, "%s  sent %q to %s\n", time.Now().Format("15:04:05"), template, email)
		return nil, nil
	}
}

func (m *mailer) isActive(_ context.Context, email []byte) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active[string(email)] {
		return []byte("yes"), nil
	}
	return []byte("no"), nil
}

func (m *mailer) register(r interface {
	RegisterActivity(string, worker.ActivityFunc)
}) {
	for _, t := range []string{"welcome", "tips", "check-in"} {
		r.RegisterActivity("send-email:"+t, m.sender(t))
	}
	r.RegisterActivity("is-active", m.isActive)
}

func run(ctx context.Context, c *client.Client, s Signup, out io.Writer) error {
	m := &mailer{active: map[string]bool{}, out: out}
	w := worker.New(c, worker.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	w.RegisterWorkflow("Onboarding", Onboarding)
	m.register(w)
	if err := w.Start(ctx); err != nil {
		return err
	}
	defer w.Stop()

	input, _ := json.Marshal(s)
	id, err := c.StartWorkflow(ctx, "Onboarding", input, client.WithWorkflowID("onboarding-"+strings.ToLower(s.Email)))
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "started %s; one day = %s\n", id, s.Day)
	result, err := c.WaitForResult(ctx, id)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "result: %s\n", result)
	return nil
}

func main() {
	addr := flag.String("addr", "localhost:7233", "workflowd gRPC address")
	day := flag.Duration("day", 3*time.Second, "how long a drip \"day\" lasts")
	flag.Parse()
	email := "ada@example.com"
	if flag.NArg() > 0 {
		email = flag.Arg(0)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	c, err := client.Dial(client.Options{Addrs: []string{*addr}})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	if err := run(ctx, c, Signup{Email: email, Day: *day}, os.Stdout); err != nil {
		log.Fatal(err)
	}
}

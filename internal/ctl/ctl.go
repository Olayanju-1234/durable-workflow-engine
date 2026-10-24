// Package ctl is `workflowd ctl`, the operator's command line. It talks to an
// engine over its JSON API, with a token from a file or the environment, in
// one namespace, and prints tables for people or JSON (--json) for scripts.
// Backup can also read a data directory directly, and restore only does.
package ctl

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

// Environment variables the global flags default to.
const (
	envAddr      = "WORKFLOWD_ADDR"
	envNamespace = "WORKFLOWD_NAMESPACE"
	envTokenFile = "WORKFLOWD_TOKEN_FILE"
	envToken     = "WORKFLOWD_TOKEN"
	envAuthToken = "WORKFLOWD_AUTH_TOKEN" // what the SDK examples read
	envCAFile    = "WORKFLOWD_CA_FILE"
)

// cli is one invocation: the global flags, resolved, and where output goes.
type cli struct {
	addr, namespace, tokenFile, caFile string
	json                               bool
	timeout                            time.Duration
	getenv                             func(string) string
	stdin                              io.Reader
	out, errOut                        io.Writer

	token string
	http  *http.Client
}

// usageError is a mistake on the command line (exit status 2).
type usageError struct{ msg string }

func (e usageError) Error() string { return e.msg }

func usagef(format string, a ...any) error { return usageError{fmt.Sprintf(format, a...)} }

// apiError is an engine's refusal, with its HTTP status.
type apiError struct {
	status int
	msg    string
}

func (e *apiError) Error() string { return fmt.Sprintf("%s (HTTP %d)", e.msg, e.status) }

// command is one `workflowd ctl` command. setup registers its flags and
// returns what runs it with the positional arguments.
type command struct {
	name, args, summary string
	setup               func(fs *flag.FlagSet) func(c *cli, pos []string) error
}

// Main runs `workflowd ctl args...` and returns the process's exit status.
func Main(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string) int {
	if len(args) == 0 || args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		printUsage(stdout)
		return 0
	}
	name, rest := args[0], args[1:]
	if name == "schedule" || name == "schedules" || name == "archive" {
		if len(rest) == 0 {
			printUsage(stderr)
			return 2
		}
		name, rest = name+" "+rest[0], rest[1:]
		name = strings.Replace(name, "schedules ", "schedule ", 1)
	}
	cmd, ok := commands()[name]
	if !ok {
		fmt.Fprintf(stderr, "workflowd ctl: unknown command %q\n\n", name)
		printUsage(stderr)
		return 2
	}
	c := &cli{getenv: getenv, stdin: stdin, out: stdout, errOut: stderr}
	fs := flag.NewFlagSet("workflowd ctl "+name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	c.globalFlags(fs)
	run := cmd.setup(fs)
	pos, err := parseInterleaved(fs, rest)
	if err == nil {
		err = c.resolve()
	}
	if err == nil {
		err = run(c, pos)
	}
	switch {
	case err == nil:
		return 0
	case errors.Is(err, flag.ErrHelp):
		printCommandUsage(stdout, cmd, fs)
		return 0
	case isUsage(err):
		fmt.Fprintf(stderr, "workflowd ctl %s: %v\n\n", name, err)
		printCommandUsage(stderr, cmd, fs)
		return 2
	default:
		fmt.Fprintf(stderr, "workflowd ctl %s: %v\n", name, err)
		return 1
	}
}

func isUsage(err error) bool {
	var u usageError
	return errors.As(err, &u) || strings.HasPrefix(err.Error(), "flag provided but not defined") ||
		strings.HasPrefix(err.Error(), "invalid value") || strings.HasPrefix(err.Error(), "flag needs an argument")
}

func (c *cli) globalFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.addr, "addr", "", "the engine's HTTP address (default $"+envAddr+", else http://localhost:8233)")
	fs.StringVar(&c.namespace, "namespace", "", "the namespace to work in (default $"+envNamespace+", else the token's)")
	fs.StringVar(&c.namespace, "n", "", "short for --namespace")
	fs.StringVar(&c.tokenFile, "token-file", "", "read the bearer token from this file (default $"+envTokenFile+", else $"+envToken+")")
	fs.StringVar(&c.caFile, "ca-file", "", "trust this PEM CA for an https address (default $"+envCAFile+")")
	fs.BoolVar(&c.json, "json", false, "print the engine's JSON instead of a table")
	fs.DurationVar(&c.timeout, "timeout", 30*time.Second, "give up on a request after this long")
}

// resolve fills in what the flags left to the environment.
func (c *cli) resolve() error {
	if c.addr == "" {
		c.addr = c.getenv(envAddr)
	}
	if c.addr == "" {
		c.addr = "http://localhost:8233"
	}
	if !strings.Contains(c.addr, "://") {
		c.addr = "http://" + c.addr
	}
	c.addr = strings.TrimRight(c.addr, "/")
	if c.namespace == "" {
		c.namespace = c.getenv(envNamespace)
	}
	if c.tokenFile == "" {
		c.tokenFile = c.getenv(envTokenFile)
	}
	if c.tokenFile != "" {
		b, err := os.ReadFile(c.tokenFile)
		if err != nil {
			return fmt.Errorf("token file: %w", err)
		}
		c.token = strings.TrimSpace(string(b))
	} else if c.token = strings.TrimSpace(c.getenv(envToken)); c.token == "" {
		c.token = strings.TrimSpace(c.getenv(envAuthToken))
	}
	if c.caFile == "" {
		c.caFile = c.getenv(envCAFile)
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if c.caFile != "" {
		pem, err := os.ReadFile(c.caFile)
		if err != nil {
			return fmt.Errorf("CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return fmt.Errorf("CA file %s holds no PEM certificate", c.caFile)
		}
		tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	c.http = &http.Client{Transport: tr, Timeout: c.timeout}
	return nil
}

// parseInterleaved parses flags wherever they appear among the positional
// arguments (`describe order-1 --json` as well as `describe --json order-1`).
func parseInterleaved(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		args = fs.Args()
		if len(args) == 0 {
			return pos, nil
		}
		pos, args = append(pos, args[0]), args[1:]
	}
}

// request calls the API. With out nil the body is dropped; with *[]byte it is
// kept raw; with an io.Writer it is streamed there; anything else is decoded
// as JSON. A 503 that names the cluster's leader is retried there.
func (c *cli) request(method, path string, query url.Values, body any, out any) error {
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return err
		}
	}
	base := c.addr
	for hop := 0; ; hop++ {
		u := base + path
		if len(query) > 0 {
			u += "?" + query.Encode()
		}
		req, err := http.NewRequest(method, u, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if c.token != "" {
			req.Header.Set("Authorization", "Bearer "+c.token)
		}
		if c.namespace != "" {
			req.Header.Set("X-Workflowd-Namespace", c.namespace)
		}
		res, err := c.http.Do(req)
		if err != nil {
			return err
		}
		if w, ok := out.(io.Writer); ok && res.StatusCode < 400 {
			_, err := io.Copy(w, res.Body)
			res.Body.Close()
			return err
		}
		data, err := io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil {
			return err
		}
		if res.StatusCode >= 400 {
			var e struct {
				Error      string `json:"error"`
				LeaderHTTP string `json:"leader_http"`
			}
			_ = json.Unmarshal(data, &e)
			if res.StatusCode == http.StatusServiceUnavailable && e.LeaderHTTP != "" && hop < 3 {
				scheme, _, _ := strings.Cut(base, "://")
				base = scheme + "://" + e.LeaderHTTP
				continue
			}
			if e.Error == "" {
				e.Error = strings.TrimSpace(string(data))
			}
			return &apiError{status: res.StatusCode, msg: e.Error}
		}
		switch o := out.(type) {
		case nil:
		case *[]byte:
			*o = data
		default:
			if res.StatusCode != http.StatusNoContent && len(data) > 0 {
				if err := json.Unmarshal(data, out); err != nil {
					return fmt.Errorf("decode the engine's reply: %w", err)
				}
			}
		}
		return nil
	}
}

// getJSON fetches path and either prints its JSON (--json) or decodes it into
// out for a table.
func (c *cli) getJSON(path string, query url.Values, out any) (printed bool, err error) {
	var raw []byte
	if err := c.request(http.MethodGet, path, query, nil, &raw); err != nil {
		return false, err
	}
	if c.json {
		c.printJSON(raw)
		return true, nil
	}
	return false, json.Unmarshal(raw, out)
}

// printJSON writes an engine reply on one line.
func (c *cli) printJSON(raw []byte) {
	fmt.Fprintln(c.out, strings.TrimSpace(string(raw)))
}

func printUsage(w io.Writer) {
	fmt.Fprint(w, `Usage: workflowd ctl <command> [arguments] [flags]

Talks to a workflowd engine over its JSON API. The address, token and namespace
come from flags or the environment (WORKFLOWD_ADDR, WORKFLOWD_TOKEN_FILE or
WORKFLOWD_TOKEN, WORKFLOWD_NAMESPACE). Add --json for the engine's JSON.

Commands:
`)
	cmds := commands()
	names := make([]string, 0, len(cmds))
	for n := range cmds {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		cmd := cmds[n]
		fmt.Fprintf(w, "  %-34s %s\n", strings.TrimSpace(cmd.name+" "+cmd.args), cmd.summary)
	}
	fmt.Fprint(w, `
Run "workflowd ctl <command> --help" for its flags.
`)
}

func printCommandUsage(w io.Writer, cmd command, fs *flag.FlagSet) {
	fmt.Fprintf(w, "Usage: workflowd ctl %s %s\n\n%s\n\nFlags:\n", cmd.name, cmd.args, cmd.summary)
	fs.SetOutput(w)
	fs.PrintDefaults()
	fs.SetOutput(io.Discard)
}

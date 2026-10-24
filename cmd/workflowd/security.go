package main

import (
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/netip"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/olayanju-1234/durable-workflow-engine/internal/archive"
	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
	"github.com/olayanju-1234/durable-workflow-engine/internal/tlsconfig"
)

// Environment variables that can carry secrets, so they never have to appear
// in argv (visible to every user via ps) or in a unit file.
const (
	envAuthToken        = "WORKFLOWD_AUTH_TOKEN"
	envMetricsAuthToken = "WORKFLOWD_METRICS_AUTH_TOKEN"
)

// securityFlags are the opt-in auth and TLS flags. With none set, every
// listener is plaintext and unauthenticated, as before they existed.
type securityFlags struct {
	authToken, authTokenFile, metricsTokenFile string
	namespacesFile                             string
	guardrails                                 server.Policy
	trustedProxies                             string
	publicReads                                bool
	tlsCert, tlsKey                            string
	raftCert, raftKey, raftCA                  string
}

func registerSecurityFlags(fs *flag.FlagSet) *securityFlags {
	f := &securityFlags{}
	fs.StringVar(&f.authToken, "auth-token", "", "require this bearer token on the JSON API and gRPC (visible in ps: prefer --auth-token-file or $"+envAuthToken+")")
	fs.StringVar(&f.authTokenFile, "auth-token-file", "", "read the API/gRPC bearer token from this file (default: $"+envAuthToken+")")
	fs.BoolVar(&f.publicReads, "auth-public-reads", false, "with a token set, let anyone list and describe workflows; starting, signalling, terminating and workers still need the token")
	fs.StringVar(&f.namespacesFile, "namespaces", "", "JSON file of namespaces: each one's tokens, retention and limits (docs: Namespaces). The default namespace always exists; --auth-token reaches every namespace")
	fs.BoolVar(&f.guardrails.ReadOnly, "read-only", false, "refuse every client write in every namespace: starts, signals, cancels, terminations, schedule changes (workers keep running what exists)")
	fs.BoolVar(&f.guardrails.ReadOnlySchedules, "read-only-schedules", false, "refuse schedule changes in every namespace that does not say otherwise")
	fs.IntVar(&f.guardrails.RateLimitPerMinute, "rate-limit", 0, "client writes per minute per token (or per IP without one) in each namespace; the engine token is not limited (0: no limit)")
	fs.IntVar(&f.guardrails.RateLimitBurst, "rate-limit-burst", 0, "client writes allowed at once before --rate-limit applies (default: the rate)")
	fs.StringVar(&f.trustedProxies, "trusted-proxies", "", "addresses or CIDR ranges of proxies whose X-Forwarded-For names the client, e.g. 127.0.0.1,::1")
	fs.IntVar(&f.guardrails.MaxActiveWorkflows, "max-active-workflows", 0, "workflows running or waiting at once in each namespace (0: no limit)")
	fs.IntVar(&f.guardrails.MaxStartsPerMinute, "max-starts-per-minute", 0, "workflow starts per minute in each namespace, schedule fires included (0: no limit)")
	fs.StringVar(&f.metricsTokenFile, "metrics-auth-token-file", "", "require the bearer token in this file on /metrics (default: $"+envMetricsAuthToken+"; unset leaves /metrics open)")
	fs.StringVar(&f.tlsCert, "tls-cert", "", "PEM certificate for TLS on the gRPC, dashboard/API and metrics listeners (reloaded when the file changes)")
	fs.StringVar(&f.tlsKey, "tls-key", "", "PEM private key for --tls-cert")
	fs.StringVar(&f.raftCert, "raft-tls-cert", "", "PEM certificate this node presents to its Raft peers (cluster mode; enables mutual TLS)")
	fs.StringVar(&f.raftKey, "raft-tls-key", "", "PEM private key for --raft-tls-cert")
	fs.StringVar(&f.raftCA, "raft-tls-ca", "", "PEM CA bundle that signs every peer's --raft-tls-cert; peers without one are refused")
	return f
}

// security is the resolved form of securityFlags.
type security struct {
	authToken, metricsToken string
	publicReads             bool
	namespaces              *server.NamespaceSet // nil: only default
	guardrails              server.Policy        // defaults for every namespace
	trusted                 []netip.Prefix
	guard                   *server.Guard // built with the engine's metrics; nil admits all
	tls                     *tls.Config   // client-facing listeners; nil = plaintext
	peerTLS                 *tls.Config   // Raft peers; nil = plaintext
}

func (f *securityFlags) resolve(logger *slog.Logger) (security, error) {
	var s security
	var err error
	s.authToken, err = secret("--auth-token", f.authToken, "--auth-token-file", f.authTokenFile, envAuthToken, logger)
	if err != nil {
		return s, err
	}
	s.metricsToken, err = secret("", "", "--metrics-auth-token-file", f.metricsTokenFile, envMetricsAuthToken, logger)
	if err != nil {
		return s, err
	}
	if f.publicReads && s.authToken == "" {
		return s, errors.New("--auth-public-reads needs a token (--auth-token-file or $" + envAuthToken + ")")
	}
	s.publicReads = f.publicReads
	if f.namespacesFile != "" {
		if s.namespaces, err = server.LoadNamespaces(f.namespacesFile); err != nil {
			return s, err
		}
	}
	g := f.guardrails
	if g.RateLimitPerMinute < 0 || g.RateLimitBurst < 0 || g.MaxActiveWorkflows < 0 || g.MaxStartsPerMinute < 0 {
		return s, errors.New("--rate-limit, --rate-limit-burst, --max-active-workflows and --max-starts-per-minute must not be negative (0 means no limit)")
	}
	s.guardrails = g
	if s.trusted, err = server.ParseTrustedProxies(f.trustedProxies); err != nil {
		return s, err
	}

	if f.tlsCert != "" || f.tlsKey != "" {
		if s.tls, err = tlsconfig.Server(f.tlsCert, f.tlsKey); err != nil {
			return s, fmt.Errorf("--tls-cert/--tls-key: %w", err)
		}
	}
	if f.raftCert != "" || f.raftKey != "" || f.raftCA != "" {
		if f.raftCert == "" || f.raftKey == "" || f.raftCA == "" {
			return s, errors.New("--raft-tls-cert, --raft-tls-key and --raft-tls-ca go together")
		}
		if s.peerTLS, err = tlsconfig.Peer(f.raftCert, f.raftKey, f.raftCA); err != nil {
			return s, fmt.Errorf("--raft-tls-*: %w", err)
		}
	}
	return s, nil
}

// secret resolves a token from, in order, its file flag, its plain flag and
// its environment variable. Setting both flags is an error, not a silent pick.
func secret(flagName, flagVal, fileFlag, file, env string, logger *slog.Logger) (string, error) {
	if flagVal != "" && file != "" {
		return "", fmt.Errorf("set %s or %s, not both", flagName, fileFlag)
	}
	if file != "" {
		return readSecretFile(fileFlag, file, logger)
	}
	if flagVal != "" {
		logger.Warn(flagName+" is visible to other users in the process list", "hint", "use "+fileFlag+" or $"+env)
		return flagVal, nil
	}
	return strings.TrimSpace(os.Getenv(env)), nil
}

func readSecretFile(fileFlag, path string, logger *slog.Logger) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("%s: %w", fileFlag, err)
	}
	tok := strings.TrimSpace(string(b))
	if tok == "" {
		return "", fmt.Errorf("%s: %s is empty", fileFlag, path)
	}
	if st, err := os.Stat(path); err == nil && st.Mode().Perm()&0o077 != 0 {
		logger.Warn("token file is readable by other users", "file", path, "mode", st.Mode().Perm().String(), "hint", "chmod 600")
	}
	return tok, nil
}

// authorizer decides every call's namespace and token, on both transports.
func (s security) authorizer() *server.Authorizer {
	return server.NewAuthorizer(s.authToken, s.publicReads, s.namespaces)
}

// archiveCredentials reads S3 credentials for --archive: from the file, else
// the environment. Like every secret here, never from a flag's value.
func archiveCredentials(file string, getenv func(string) string) (archive.Credentials, error) {
	var c archive.Credentials
	if file == "" {
		c.AccessKeyID, c.SecretAccessKey = getenv("WORKFLOWD_ARCHIVE_ACCESS_KEY_ID"), getenv("WORKFLOWD_ARCHIVE_SECRET_ACCESS_KEY")
		if c.AccessKeyID == "" && c.SecretAccessKey == "" {
			c.AccessKeyID, c.SecretAccessKey = getenv("AWS_ACCESS_KEY_ID"), getenv("AWS_SECRET_ACCESS_KEY")
		}
		return c, nil
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return c, fmt.Errorf("--archive-credentials-file: %w", err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		k, v, ok := strings.Cut(strings.TrimSpace(line), "=")
		if !ok || strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		switch strings.TrimSpace(k) {
		case "access_key_id":
			c.AccessKeyID = strings.TrimSpace(v)
		case "secret_access_key":
			c.SecretAccessKey = strings.TrimSpace(v)
		}
	}
	if c.AccessKeyID == "" || c.SecretAccessKey == "" {
		return c, fmt.Errorf("--archive-credentials-file: %s needs access_key_id= and secret_access_key= lines", file)
	}
	return c, nil
}

// grpcServerOptions are the TLS and auth options for the client-facing gRPC server.
func (s security) grpcServerOptions() []grpc.ServerOption {
	opts := server.GRPCAuthorize(s.authorizer(), s.guard)
	if s.tls != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(s.tls)))
	}
	return opts
}

// httpOptions are the auth options for the JSON API.
func (s security) httpOptions() []server.HTTPOption {
	opts := []server.HTTPOption{server.WithAuthToken(s.authToken), server.WithNamespaceSet(s.namespaces), server.WithGuard(s.guard)}
	if s.publicReads {
		opts = append(opts, server.WithPublicReads())
	}
	return opts
}

// metricsHandler guards /metrics with its own token, if one is set.
func (s security) metricsHandler(h http.Handler) http.Handler {
	return server.BearerAuth(s.metricsToken, h)
}

// logPosture says once at startup what is and isn't protected.
func (s security) logPosture(logger *slog.Logger, httpOn bool) {
	if s.authToken == "" && httpOn && s.authorizer().Open() {
		logger.Warn("API auth is disabled for the default namespace; anyone who can reach the HTTP or gRPC port can start, signal and terminate its workflows",
			"hint", "set --auth-token-file or $"+envAuthToken)
	}
	if names := s.namespaces.Names(); len(names) > 1 {
		logger.Info("namespaces configured", "namespaces", names)
	}
	if g := s.guardrails; g != (server.Policy{}) {
		logger.Info("guardrails", "read_only", g.ReadOnly, "read_only_schedules", g.ReadOnlySchedules,
			"rate_limit_per_minute", g.RateLimitPerMinute, "rate_limit_burst", g.RateLimitBurst,
			"max_active_workflows", g.MaxActiveWorkflows, "max_starts_per_minute", g.MaxStartsPerMinute,
			"trusted_proxies", len(s.trusted))
	}
	if s.tls == nil {
		logger.Info("client listeners are plaintext", "hint", "set --tls-cert/--tls-key, or terminate TLS in a proxy")
	}
}

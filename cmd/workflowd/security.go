package main

import (
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

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
	publicReads                                bool
	tlsCert, tlsKey                            string
	raftCert, raftKey, raftCA                  string
}

func registerSecurityFlags(fs *flag.FlagSet) *securityFlags {
	f := &securityFlags{}
	fs.StringVar(&f.authToken, "auth-token", "", "require this bearer token on the JSON API and gRPC (visible in ps: prefer --auth-token-file or $"+envAuthToken+")")
	fs.StringVar(&f.authTokenFile, "auth-token-file", "", "read the API/gRPC bearer token from this file (default: $"+envAuthToken+")")
	fs.BoolVar(&f.publicReads, "auth-public-reads", false, "with a token set, let anyone list and describe workflows; starting, signalling, terminating and workers still need the token")
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
	tls                     *tls.Config // client-facing listeners; nil = plaintext
	peerTLS                 *tls.Config // Raft peers; nil = plaintext
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

// grpcServerOptions are the TLS and auth options for the client-facing gRPC server.
func (s security) grpcServerOptions() []grpc.ServerOption {
	opts := server.GRPCAuth(s.authToken, s.publicReads)
	if s.tls != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(s.tls)))
	}
	return opts
}

// httpOptions are the auth options for the JSON API.
func (s security) httpOptions() []server.HTTPOption {
	opts := []server.HTTPOption{server.WithAuthToken(s.authToken)}
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
	if s.authToken == "" && httpOn {
		logger.Warn("API auth is disabled; anyone who can reach the HTTP or gRPC port can start, signal and terminate workflows",
			"hint", "set --auth-token-file or $"+envAuthToken)
	}
	if s.tls == nil {
		logger.Info("client listeners are plaintext", "hint", "set --tls-cert/--tls-key, or terminate TLS in a proxy")
	}
}

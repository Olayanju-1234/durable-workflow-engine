package main

import (
	"context"
	"crypto/tls"
	"flag"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/tlsconfig"
	"github.com/olayanju-1234/durable-workflow-engine/internal/tlsconfig/tlstest"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func writeFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func resolveArgs(t *testing.T, args ...string) (security, error) {
	t.Helper()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	f := registerSecurityFlags(fs)
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	return f.resolve(quiet)
}

func TestTokenSources(t *testing.T) {
	t.Setenv(envAuthToken, "")
	t.Setenv(envMetricsAuthToken, "")

	s, err := resolveArgs(t)
	if err != nil || s.authToken != "" || s.metricsToken != "" || s.tls != nil || s.peerTLS != nil {
		t.Fatalf("no flags: %+v, %v; want everything off", s, err)
	}

	// A trailing newline (echo > file) is not part of the token.
	s, err = resolveArgs(t, "--auth-token-file", writeFile(t, "from-file\n"), "--metrics-auth-token-file", writeFile(t, "  scrape \n"))
	if err != nil || s.authToken != "from-file" || s.metricsToken != "scrape" {
		t.Fatalf("token files: %q %q %v", s.authToken, s.metricsToken, err)
	}

	t.Setenv(envAuthToken, "from-env")
	t.Setenv(envMetricsAuthToken, "metrics-env")
	if s, err = resolveArgs(t); err != nil || s.authToken != "from-env" || s.metricsToken != "metrics-env" {
		t.Fatalf("env: %q %q %v", s.authToken, s.metricsToken, err)
	}
	if s, err = resolveArgs(t, "--auth-token-file", writeFile(t, "file-wins")); err != nil || s.authToken != "file-wins" {
		t.Fatalf("a flag must beat the environment: %q %v", s.authToken, err)
	}
	if s, err = resolveArgs(t, "--auth-token", "argv"); err != nil || s.authToken != "argv" {
		t.Fatalf("--auth-token still works: %q %v", s.authToken, err)
	}
}

func TestSecurityFlagErrors(t *testing.T) {
	t.Setenv(envAuthToken, "")
	ca, err := tlstest.NewCA(t.TempDir(), "flags")
	if err != nil {
		t.Fatal(err)
	}
	peer, err := ca.Issue(t.TempDir(), "n1")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{"token twice", []string{"--auth-token", "a", "--auth-token-file", writeFile(t, "b")}, "not both"},
		{"missing token file", []string{"--auth-token-file", "/does/not/exist"}, "no such file"},
		{"empty token file", []string{"--auth-token-file", writeFile(t, " \n")}, "is empty"},
		{"public reads without a token", []string{"--auth-public-reads"}, "needs a token"},
		{"cert without key", []string{"--tls-cert", peer.CertFile}, "both a certificate and a key"},
		{"unreadable cert", []string{"--tls-cert", "/nope.pem", "--tls-key", "/nope.key"}, "load key pair"},
		{"raft cert without CA", []string{"--raft-tls-cert", peer.CertFile, "--raft-tls-key", peer.KeyFile}, "go together"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := resolveArgs(t, tt.args...)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("got %v, want an error containing %q", err, tt.wantErr)
			}
		})
	}
	s, err := resolveArgs(t, "--raft-tls-cert", peer.CertFile, "--raft-tls-key", peer.KeyFile, "--raft-tls-ca", ca.CertFile)
	if err != nil || s.peerTLS == nil || s.peerTLS.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("raft mTLS flags: %v", err)
	}
}

// --tls-cert serves the dashboard/API and metrics over HTTPS only.
func TestHTTPListenerServesTLS(t *testing.T) {
	dir := t.TempDir()
	ca, err := tlstest.NewCA(dir, "http")
	if err != nil {
		t.Fatal(err)
	}
	srvCert, err := ca.Issue(dir, "server")
	if err != nil {
		t.Fatal(err)
	}
	s, err := resolveArgs(t, "--tls-cert", srvCert.CertFile, "--tls-key", srvCert.KeyFile)
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := startHTTP(ctx, addr, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }), "test", s.tls, quiet)
	defer srv.Close()

	pool, err := tlsconfig.LoadCAs(ca.CertFile)
	if err != nil {
		t.Fatal(err)
	}
	trusting := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}
	var res *http.Response
	for i := 0; i < 50; i++ { // the listener starts in the background
		if res, err = trusting.Get("https://" + addr + "/"); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("https with the CA: %v", err)
	}
	res.Body.Close()

	if res, err := (&http.Client{Timeout: 2 * time.Second}).Get("http://" + addr + "/"); err == nil {
		res.Body.Close()
		if res.StatusCode == http.StatusOK {
			t.Fatal("plaintext request served on a TLS listener")
		}
	}
	if _, err := (&http.Client{Timeout: 2 * time.Second}).Get("https://" + addr + "/"); err == nil {
		t.Fatal("client without the CA trusted the certificate")
	}
}

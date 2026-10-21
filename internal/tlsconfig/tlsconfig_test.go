package tlsconfig_test

import (
	"crypto/tls"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/tlsconfig"
	"github.com/olayanju-1234/durable-workflow-engine/internal/tlsconfig/tlstest"
)

func newCA(t *testing.T, name string) *tlstest.CA {
	t.Helper()
	ca, err := tlstest.NewCA(t.TempDir(), name)
	if err != nil {
		t.Fatal(err)
	}
	return ca
}

func issue(t *testing.T, ca *tlstest.CA, name string) tlstest.Files {
	t.Helper()
	f, err := ca.Issue(t.TempDir(), name)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// serve accepts TLS connections and echoes one byte, which forces the
// handshake to finish on both ends.
func serve(t *testing.T, cfg *tls.Config) string {
	t.Helper()
	lis, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lis.Close() })
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				buf := make([]byte, 1)
				if _, err := io.ReadFull(c, buf); err == nil {
					_, _ = c.Write(buf)
				}
			}()
		}
	}()
	return lis.Addr().String()
}

func roundTrip(addr string, cfg *tls.Config) error {
	c, err := tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp", addr, cfg)
	if err != nil {
		return err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Write([]byte{1}); err != nil {
		return err
	}
	_, err = io.ReadFull(c, make([]byte, 1))
	return err
}

func TestServerTLS(t *testing.T) {
	ca := newCA(t, "srv")
	f := issue(t, ca, "server")
	cfg, err := tlsconfig.Server(f.CertFile, f.KeyFile)
	if err != nil {
		t.Fatal(err)
	}
	addr := serve(t, cfg)
	pool, err := tlsconfig.LoadCAs(ca.CertFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := roundTrip(addr, &tls.Config{RootCAs: pool, ServerName: "localhost"}); err != nil {
		t.Fatalf("client trusting the CA: %v", err)
	}
	if err := roundTrip(addr, &tls.Config{ServerName: "localhost"}); err == nil {
		t.Fatal("client without the CA accepted an untrusted certificate")
	}
}

// Peers accept each other and nobody else: no certificate, or one from another
// CA, fails the handshake.
func TestPeerMutualTLS(t *testing.T) {
	ca := newCA(t, "cluster")
	a, b := issue(t, ca, "n1"), issue(t, ca, "n2")
	serverCfg, err := tlsconfig.Peer(a.CertFile, a.KeyFile, ca.CertFile)
	if err != nil {
		t.Fatal(err)
	}
	addr := serve(t, serverCfg)

	clientCfg, err := tlsconfig.Peer(b.CertFile, b.KeyFile, ca.CertFile)
	if err != nil {
		t.Fatal(err)
	}
	clientCfg.ServerName = "127.0.0.1"
	if err := roundTrip(addr, clientCfg); err != nil {
		t.Fatalf("valid peer rejected: %v", err)
	}

	pool, _ := tlsconfig.LoadCAs(ca.CertFile)
	if err := roundTrip(addr, &tls.Config{RootCAs: pool, ServerName: "127.0.0.1"}); err == nil {
		t.Fatal("peer without a client certificate was accepted")
	}

	rogueCA := newCA(t, "rogue")
	r := issue(t, rogueCA, "n3")
	rogueCert, err := tls.LoadX509KeyPair(r.CertFile, r.KeyFile)
	if err != nil {
		t.Fatal(err)
	}
	rogue := &tls.Config{RootCAs: pool, ServerName: "127.0.0.1", Certificates: []tls.Certificate{rogueCert}}
	if err := roundTrip(addr, rogue); err == nil {
		t.Fatal("peer with a certificate from another CA was accepted")
	}
}

// A renewed certificate is served without a restart.
func TestCertificateReload(t *testing.T) {
	dir := t.TempDir()
	ca := newCA(t, "reload")
	first, err := ca.Issue(dir, "server")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := tlsconfig.Server(first.CertFile, first.KeyFile)
	if err != nil {
		t.Fatal(err)
	}
	served := func() string {
		c, err := cfg.GetCertificate(nil)
		if err != nil {
			t.Fatal(err)
		}
		return string(c.Certificate[0])
	}
	before := served()

	if _, err := ca.Issue(dir, "server"); err != nil { // same paths, new key pair
		t.Fatal(err)
	}
	later := time.Now().Add(time.Minute)
	for _, f := range []string{first.CertFile, first.KeyFile} {
		if err := os.Chtimes(f, later, later); err != nil {
			t.Fatal(err)
		}
	}
	if served() == before {
		t.Fatal("certificate was not reloaded after the files changed")
	}

	// A broken renewal keeps the last good certificate.
	current := served()
	if err := os.WriteFile(first.CertFile, []byte("not a cert"), 0o600); err != nil {
		t.Fatal(err)
	}
	evenLater := later.Add(time.Minute)
	_ = os.Chtimes(first.CertFile, evenLater, evenLater)
	if served() != current {
		t.Fatal("a broken certificate file replaced the last good one")
	}
}

func TestConfigErrors(t *testing.T) {
	if _, err := tlsconfig.Server("", ""); err == nil {
		t.Error("missing files accepted")
	}
	if _, err := tlsconfig.Server("/nope.pem", "/nope-key.pem"); err == nil {
		t.Error("nonexistent files accepted")
	}
	f := issue(t, newCA(t, "x"), "n1")
	if _, err := tlsconfig.Peer(f.CertFile, f.KeyFile, f.KeyFile); err == nil {
		t.Error("a CA file with no certificates accepted")
	}
}

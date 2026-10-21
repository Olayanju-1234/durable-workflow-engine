// Package tlsconfig builds the *tls.Config values workflowd serves and dials
// with: server TLS for the client-facing listeners, and mutual TLS between
// Raft peers. Certificates are re-read when their files change, so a renewed
// certificate takes effect without a restart.
package tlsconfig

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

// Server returns a TLS config presenting the certificate in certFile/keyFile.
// Clients are not asked for certificates.
func Server(certFile, keyFile string) (*tls.Config, error) {
	kp, err := newKeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:     tls.VersionTLS12,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return kp.get() },
	}, nil
}

// Peer returns a mutual-TLS config for Raft traffic, usable on both ends of a
// connection: as a server it demands a client certificate signed by caFile,
// and as a client it presents its own and trusts only caFile.
func Peer(certFile, keyFile, caFile string) (*tls.Config, error) {
	kp, err := newKeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	pool, err := LoadCAs(caFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:           tls.VersionTLS12,
		GetCertificate:       func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return kp.get() },
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return kp.get() },
		ClientAuth:           tls.RequireAndVerifyClientCert,
		ClientCAs:            pool,
		RootCAs:              pool,
	}, nil
}

// LoadCAs reads PEM certificates from file into a pool.
func LoadCAs(file string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("tlsconfig: read CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("tlsconfig: no PEM certificates in %s", file)
	}
	return pool, nil
}

// keyPair serves a certificate, reloading it when either file's modification
// time changes. A reload that fails keeps the previous certificate, so a
// half-written renewal never takes a listener down.
type keyPair struct {
	certFile, keyFile string

	mu      sync.Mutex
	cert    *tls.Certificate
	modTime [2]time.Time
}

func newKeyPair(certFile, keyFile string) (*keyPair, error) {
	if certFile == "" || keyFile == "" {
		return nil, errors.New("tlsconfig: both a certificate and a key file are required")
	}
	kp := &keyPair{certFile: certFile, keyFile: keyFile}
	if _, err := kp.get(); err != nil {
		return nil, err
	}
	return kp, nil
}

func (kp *keyPair) get() (*tls.Certificate, error) {
	mods, statErr := kp.modTimes()
	kp.mu.Lock()
	defer kp.mu.Unlock()
	if kp.cert != nil && (statErr != nil || mods == kp.modTime) {
		return kp.cert, nil
	}
	cert, err := tls.LoadX509KeyPair(kp.certFile, kp.keyFile)
	if err != nil {
		if kp.cert != nil {
			return kp.cert, nil
		}
		return nil, fmt.Errorf("tlsconfig: load key pair: %w", err)
	}
	kp.cert, kp.modTime = &cert, mods
	return kp.cert, nil
}

func (kp *keyPair) modTimes() ([2]time.Time, error) {
	var mods [2]time.Time
	for i, f := range []string{kp.certFile, kp.keyFile} {
		st, err := os.Stat(f)
		if err != nil {
			return mods, err
		}
		mods[i] = st.ModTime()
	}
	return mods, nil
}

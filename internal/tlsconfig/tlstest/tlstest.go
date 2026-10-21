// Package tlstest issues throwaway certificates for tests: a CA, and leaf
// certificates for 127.0.0.1 and localhost that serve as both server and
// client certificates.
package tlstest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// CA is a self-signed certificate authority.
type CA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	// CertFile is the CA certificate, PEM-encoded.
	CertFile string
}

// Files is one issued certificate and its key, PEM-encoded on disk.
type Files struct{ CertFile, KeyFile string }

// NewCA creates a CA and writes its certificate to dir/<name>-ca.pem.
func NewCA(dir, name string) (*CA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial(),
		Subject:               pkix.Name{CommonName: name + " test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	ca := &CA{cert: cert, key: key, CertFile: filepath.Join(dir, name+"-ca.pem")}
	return ca, writePEM(ca.CertFile, "CERTIFICATE", der)
}

// Issue signs a certificate for 127.0.0.1, ::1 and localhost, usable by a
// server and a client, and writes it to dir/<name>.pem and dir/<name>-key.pem.
func (ca *CA) Issue(dir, name string) (Files, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return Files{}, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial(),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		return Files{}, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return Files{}, err
	}
	f := Files{CertFile: filepath.Join(dir, name+".pem"), KeyFile: filepath.Join(dir, name+"-key.pem")}
	if err := writePEM(f.CertFile, "CERTIFICATE", der); err != nil {
		return Files{}, err
	}
	return f, writePEM(f.KeyFile, "PRIVATE KEY", keyDER)
}

func serial() *big.Int {
	n, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	return n
}

func writePEM(path, kind string, der []byte) error {
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}), 0o600); err != nil {
		return fmt.Errorf("tlstest: %w", err)
	}
	return nil
}

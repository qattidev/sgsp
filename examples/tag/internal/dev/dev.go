// Package dev supplies local-only TLS and JWT material. Sharing a signing key
// with clients is convenient for this demo, not a production login mechanism.
package dev

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"

	library "github.com/golang-jwt/jwt/v5"
	"qattidev/sgsp"
	identity "qattidev/sgsp/auth/jwt"
	"qattidev/sgsp/examples/tag/internal/protocol"
)

const issuer = "sgsp-tag-dev"

type Material struct {
	certificate tls.Certificate
	roots       *x509.CertPool
	identity    ed25519.PrivateKey
}

type bundle struct {
	CA, Certificate, PrivateKey, IdentityKey []byte
}

// Ensure is called only by the server. Clients never create or replace assets.
func Ensure(dir string) (Material, error) {
	path := filepath.Join(dir, "credentials.json")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := generate(dir, path); err != nil {
			return Material{}, err
		}
	} else if err != nil {
		return Material{}, err
	}
	return Load(dir)
}

func Load(dir string) (Material, error) {
	path := filepath.Join(dir, "credentials.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return Material{}, fmt.Errorf("read development credentials: %w; start the server first using the same -dev-dir", err)
	}
	var b bundle
	if err := json.Unmarshal(data, &b); err != nil {
		return Material{}, fmt.Errorf("invalid %s: %w", path, err)
	}
	certificate, err := tls.X509KeyPair(b.Certificate, b.PrivateKey)
	if err != nil {
		return Material{}, fmt.Errorf("invalid development TLS material: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(b.CA) || len(b.IdentityKey) != ed25519.PrivateKeySize {
		return Material{}, fmt.Errorf("invalid development CA or identity key in %s", path)
	}
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		return Material{}, err
	}
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, DNSName: "localhost"}); err != nil {
		return Material{}, fmt.Errorf("development certificate invalid or expired: %w; stop all Tag processes, remove %s, and restart the server", err, path)
	}
	return Material{certificate: certificate, roots: roots, identity: ed25519.PrivateKey(b.IdentityKey)}, nil
}

func (m Material) ServerTLS() *tls.Config {
	return &tls.Config{Certificates: []tls.Certificate{m.certificate}, MinVersion: tls.VersionTLS13}
}

func (m Material) ClientTLS() *tls.Config {
	return &tls.Config{RootCAs: m.roots, MinVersion: tls.VersionTLS13}
}

func (m Material) Authenticator() (sgsp.Authenticator, error) {
	return identity.NewIdentityVerifier(identity.IdentityConfig{Issuer: issuer, Audience: protocol.AppID,
		Keys: map[string]ed25519.PublicKey{"dev": m.identity.Public().(ed25519.PublicKey)}})
}

// Credentials preserves one subject across reconnects, while each new client
// invocation gets a different identity. Tokens are freshly minted on reconnect.
func (m Material) Credentials() (sgsp.CredentialProvider, error) {
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return nil, err
	}
	subject := "player-" + hex.EncodeToString(id)
	return func(context.Context) (sgsp.Credential, error) {
		now := time.Now()
		claims := library.RegisteredClaims{Issuer: issuer, Subject: subject, Audience: library.ClaimStrings{protocol.AppID},
			ExpiresAt: library.NewNumericDate(now.Add(24 * time.Hour)), IssuedAt: library.NewNumericDate(now), NotBefore: library.NewNumericDate(now)}
		token := library.NewWithClaims(library.SigningMethodEdDSA, claims)
		token.Header["typ"], token.Header["kid"] = "sgsp-access+jwt", "dev"
		encoded, err := token.SignedString(m.identity)
		return sgsp.Credential{Scheme: "jwt", Data: []byte(encoded)}, err
	}, nil
}

func generate(dir, path string) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	caPublic, caPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "SGSP Tag Development CA"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(7 * 24 * time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, caPublic, caPrivate)
	if err != nil {
		return err
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		return err
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "localhost"},
		DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	certDER, err := x509.CreateCertificate(rand.Reader, cert, ca, public, caPrivate)
	if err != nil {
		return err
	}
	_, signingKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	data, err := json.Marshal(bundle{
		CA:          pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		Certificate: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}),
		PrivateKey:  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), IdentityKey: signingKey,
	})
	if err != nil {
		return err
	}
	// Publish one complete bundle, so clients cannot observe half-written keys.
	f, err := os.CreateTemp(dir, ".credentials-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

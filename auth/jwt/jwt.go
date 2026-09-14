// Package jwt implements SGSP's deliberately narrow Ed25519 JWT profile.
package jwt

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	library "github.com/golang-jwt/jwt/v5"

	"qattidev/sgsp"
)

const (
	identityType  = "sgsp-access+jwt"
	admissionType = "sgsp-admission+jwt"
)

type IdentityConfig struct {
	Issuer, Audience string
	Keys             map[string]ed25519.PublicKey
	Clock            func() time.Time
}
type AdmissionConfig struct {
	Issuer, Audience string
	Keys             map[string]ed25519.PublicKey
	Clock            func() time.Time
}
type SignerConfig struct {
	Issuer, Audience, KeyID string
	PrivateKey              ed25519.PrivateKey
	Clock                   func() time.Time
}

type IdentityVerifier struct {
	issuer, audience string
	clock            func() time.Time
	mu               sync.RWMutex
	keys             map[string]ed25519.PublicKey
}
type AdmissionVerifier struct {
	issuer, audience string
	clock            func() time.Time
	mu               sync.RWMutex
	keys             map[string]ed25519.PublicKey
}
type AdmissionSigner struct {
	issuer, audience, keyID string
	private                 ed25519.PrivateKey
	clock                   func() time.Time
}

var ErrInvalidConfig = errors.New("sgsp jwt: invalid configuration")

func NewIdentityVerifier(config IdentityConfig) (*IdentityVerifier, error) {
	keys, err := copyKeys(config.Keys)
	if err != nil || config.Issuer == "" || config.Audience == "" {
		return nil, ErrInvalidConfig
	}
	return &IdentityVerifier{issuer: config.Issuer, audience: config.Audience, clock: clockOrNow(config.Clock), keys: keys}, nil
}
func NewAdmissionVerifier(config AdmissionConfig) (*AdmissionVerifier, error) {
	keys, err := copyKeys(config.Keys)
	if err != nil || config.Issuer == "" || config.Audience == "" {
		return nil, ErrInvalidConfig
	}
	return &AdmissionVerifier{issuer: config.Issuer, audience: config.Audience, clock: clockOrNow(config.Clock), keys: keys}, nil
}
func NewAdmissionSigner(config SignerConfig) (*AdmissionSigner, error) {
	if config.Issuer == "" || config.Audience == "" || config.KeyID == "" || len(config.PrivateKey) != ed25519.PrivateKeySize {
		return nil, ErrInvalidConfig
	}
	return &AdmissionSigner{issuer: config.Issuer, audience: config.Audience, keyID: config.KeyID, private: append(ed25519.PrivateKey(nil), config.PrivateKey...), clock: clockOrNow(config.Clock)}, nil
}
func (v *IdentityVerifier) ReplaceKeys(keys map[string]ed25519.PublicKey) error {
	copied, err := copyKeys(keys)
	if err != nil {
		return ErrInvalidConfig
	}
	v.mu.Lock()
	v.keys = copied
	v.mu.Unlock()
	return nil
}
func (v *AdmissionVerifier) ReplaceKeys(keys map[string]ed25519.PublicKey) error {
	copied, err := copyKeys(keys)
	if err != nil {
		return ErrInvalidConfig
	}
	v.mu.Lock()
	v.keys = copied
	v.mu.Unlock()
	return nil
}

func (v *IdentityVerifier) Authenticate(_ context.Context, credential sgsp.Credential) (sgsp.Principal, error) {
	if credential.Scheme != "jwt" {
		return sgsp.Principal{}, authError(errors.New("unsupported credential scheme"))
	}
	claims := identityClaims{}
	if _, err := parse(string(credential.Data), identityType, v.issuer, v.audience, v.clock(), v.keyFor, &claims); err != nil {
		return sgsp.Principal{}, authError(err)
	}
	return sgsp.Principal{Issuer: claims.Issuer, Subject: claims.Subject, ExpiresAt: claims.ExpiresAt.Time, Attributes: cloneAttributes(claims.Attributes)}, nil
}
func (v *AdmissionVerifier) Verify(_ context.Context, token string) (sgsp.Admission, error) {
	claims := admissionClaims{}
	if _, err := parse(token, admissionType, v.issuer, v.audience, v.clock(), v.keyFor, &claims); err != nil {
		return sgsp.Admission{}, admissionError(err)
	}
	if claims.PrincipalIssuer == "" || claims.AppVersion == "" || claims.OwnerID == "" || claims.Incarnation == "" || claims.Address == "" || claims.ServerName == "" || claims.ID == "" {
		return sgsp.Admission{}, admissionError(errors.New("missing admission claim"))
	}
	incarnation, err := parseID(claims.Incarnation)
	if err != nil {
		return sgsp.Admission{}, admissionError(err)
	}
	return sgsp.Admission{App: sgsp.AppIdentity{ID: claims.Audience[0], Version: claims.AppVersion}, PrincipalIssuer: claims.PrincipalIssuer, Subject: claims.Subject, GroupKey: claims.Group, Owner: sgsp.Owner{ID: claims.OwnerID, Incarnation: incarnation, Endpoint: sgsp.Endpoint{Address: claims.Address, ServerName: claims.ServerName}}, ExpiresAt: claims.ExpiresAt.Time}, nil
}
func (s *AdmissionSigner) Sign(_ context.Context, admission sgsp.Admission) (string, error) {
	now := s.clock()
	if admission.App.ID == "" || admission.App.ID != s.audience || admission.App.Version == "" || admission.PrincipalIssuer == "" || admission.Subject == "" || admission.Owner.ID == "" || admission.Owner.Endpoint.Address == "" || admission.Owner.Endpoint.ServerName == "" || admission.ExpiresAt.After(now.Add(30*time.Second)) || !admission.ExpiresAt.After(now) {
		return "", ErrInvalidConfig
	}
	jti := make([]byte, 16)
	if _, err := rand.Read(jti); err != nil {
		return "", err
	}
	claims := admissionClaims{RegisteredClaims: library.RegisteredClaims{Issuer: s.issuer, Subject: admission.Subject, Audience: library.ClaimStrings{s.audience}, ExpiresAt: library.NewNumericDate(admission.ExpiresAt), IssuedAt: library.NewNumericDate(now), NotBefore: library.NewNumericDate(now), ID: hex.EncodeToString(jti)}, PrincipalIssuer: admission.PrincipalIssuer, AppVersion: admission.App.Version, Group: admission.GroupKey, OwnerID: admission.Owner.ID, Incarnation: hex.EncodeToString(admission.Owner.Incarnation[:]), Address: admission.Owner.Endpoint.Address, ServerName: admission.Owner.Endpoint.ServerName}
	token := library.NewWithClaims(library.SigningMethodEdDSA, claims)
	token.Header["typ"], token.Header["kid"] = admissionType, s.keyID
	return token.SignedString(s.private)
}

type identityClaims struct {
	library.RegisteredClaims
	Attributes map[string]string `json:"attrs,omitempty"`
}
type admissionClaims struct {
	library.RegisteredClaims
	PrincipalIssuer string `json:"principal_issuer"`
	AppVersion      string `json:"app_version"`
	Group           string `json:"group"`
	OwnerID         string `json:"owner_id"`
	Incarnation     string `json:"incarnation"`
	Address         string `json:"address"`
	ServerName      string `json:"server_name"`
}
type keyLookup func(string) (ed25519.PublicKey, bool)

func (v *IdentityVerifier) keyFor(kid string) (ed25519.PublicKey, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	key, ok := v.keys[kid]
	return key, ok
}
func (v *AdmissionVerifier) keyFor(kid string) (ed25519.PublicKey, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	key, ok := v.keys[kid]
	return key, ok
}
func parse(tokenText, typ, issuer, audience string, now time.Time, lookup keyLookup, claims library.Claims) (*library.Token, error) {
	options := []library.ParserOption{library.WithValidMethods([]string{library.SigningMethodEdDSA.Alg()}), library.WithIssuer(issuer), library.WithAudience(audience), library.WithIssuedAt(), library.WithExpirationRequired(), library.WithLeeway(5 * time.Second), library.WithTimeFunc(func() time.Time { return now }), library.WithStrictDecoding()}
	token, err := library.ParseWithClaims(tokenText, claims, func(token *library.Token) (any, error) {
		if token.Method.Alg() != library.SigningMethodEdDSA.Alg() {
			return nil, errors.New("unexpected signing algorithm")
		}
		actualType, _ := token.Header["typ"].(string)
		kid, _ := token.Header["kid"].(string)
		if actualType != typ || kid == "" {
			return nil, errors.New("invalid token header")
		}
		key, ok := lookup(kid)
		if !ok {
			return nil, errors.New("unknown signing key")
		}
		return key, nil
	}, options...)
	if err != nil || token == nil || !token.Valid {
		if err == nil {
			err = errors.New("invalid token")
		}
		return nil, err
	}
	registered, ok := claims.(interface {
		registered() library.RegisteredClaims
	})
	if !ok {
		return nil, errors.New("invalid claim implementation")
	}
	if err := validateRegistered(registered.registered(), audience, now); err != nil {
		return nil, err
	}
	return token, nil
}
func (c identityClaims) registered() library.RegisteredClaims  { return c.RegisteredClaims }
func (c admissionClaims) registered() library.RegisteredClaims { return c.RegisteredClaims }
func validateRegistered(claims library.RegisteredClaims, audience string, now time.Time) error {
	if claims.Subject == "" || claims.ExpiresAt == nil || claims.IssuedAt == nil || claims.NotBefore == nil || !claims.ExpiresAt.Time.After(now) || !claims.ExpiresAt.Time.After(claims.IssuedAt.Time) || claims.IssuedAt.Time.After(now.Add(5*time.Second)) || claims.NotBefore.Time.After(now.Add(5*time.Second)) || len(claims.Audience) != 1 || claims.Audience[0] != audience {
		return errors.New("invalid registered claims")
	}
	return nil
}
func copyKeys(keys map[string]ed25519.PublicKey) (map[string]ed25519.PublicKey, error) {
	if len(keys) == 0 {
		return nil, ErrInvalidConfig
	}
	copied := make(map[string]ed25519.PublicKey, len(keys))
	for kid, key := range keys {
		if kid == "" || len(key) != ed25519.PublicKeySize {
			return nil, ErrInvalidConfig
		}
		copied[kid] = append(ed25519.PublicKey(nil), key...)
	}
	return copied, nil
}
func clockOrNow(clock func() time.Time) func() time.Time {
	if clock != nil {
		return clock
	}
	return time.Now
}
func cloneAttributes(attributes map[string]string) map[string]string {
	if attributes == nil {
		return nil
	}
	copied := make(map[string]string, len(attributes))
	for key, value := range attributes {
		copied[key] = value
	}
	return copied
}
func parseID(value string) (sgsp.Incarnation, error) {
	var id sgsp.Incarnation
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(id) {
		return id, errors.New("invalid incarnation")
	}
	copy(id[:], decoded)
	return id, nil
}
func authError(err error) error {
	return &sgsp.Error{Code: sgsp.Unauthenticated, Message: "authentication failed", Cause: err}
}
func admissionError(err error) error { return fmt.Errorf("%w: %v", sgsp.ErrForbidden, err) }

package jwt

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	library "github.com/golang-jwt/jwt/v5"

	"qattidev/sgsp"
)

func identityToken(t *testing.T, private ed25519.PrivateKey, kid string, claims identityClaims, typ string) string {
	t.Helper()
	token := library.NewWithClaims(library.SigningMethodEdDSA, claims)
	token.Header["kid"], token.Header["typ"] = kid, typ
	encoded, err := token.SignedString(private)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestJWTValidation(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_700_000_000, 0)
	base := func() identityClaims {
		return identityClaims{RegisteredClaims: library.RegisteredClaims{Issuer: "issuer", Subject: "player", Audience: library.ClaimStrings{"app"}, ExpiresAt: library.NewNumericDate(now.Add(time.Minute)), IssuedAt: library.NewNumericDate(now), NotBefore: library.NewNumericDate(now)}}
	}
	verifier, err := NewIdentityVerifier(IdentityConfig{Issuer: "issuer", Audience: "app", Keys: map[string]ed25519.PublicKey{"key": public}, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	valid := base()
	valid.Attributes = map[string]string{"rank": "gold"}
	principal, err := verifier.Authenticate(context.Background(), sgsp.Credential{Scheme: "jwt", Data: []byte(identityToken(t, private, "key", valid, identityType))})
	if err != nil || principal.Subject != "player" || principal.Attributes["rank"] != "gold" {
		t.Fatalf("valid identity = %#v, %v", principal, err)
	}
	for name, mutate := range map[string]func(*identityClaims, *string){
		"wrong issuer":   func(c *identityClaims, _ *string) { c.Issuer = "other" },
		"wrong audience": func(c *identityClaims, _ *string) { c.Audience = library.ClaimStrings{"other"} },
		"extra audience": func(c *identityClaims, _ *string) { c.Audience = library.ClaimStrings{"app", "other"} },
		"expired":        func(c *identityClaims, _ *string) { c.ExpiresAt = library.NewNumericDate(now.Add(-time.Second)) },
		"future nbf":     func(c *identityClaims, _ *string) { c.NotBefore = library.NewNumericDate(now.Add(6 * time.Second)) },
		"future iat":     func(c *identityClaims, _ *string) { c.IssuedAt = library.NewNumericDate(now.Add(6 * time.Second)) },
		"wrong type":     func(_ *identityClaims, typ *string) { *typ = admissionType },
	} {
		t.Run(name, func(t *testing.T) {
			claims, typ := base(), identityType
			mutate(&claims, &typ)
			_, err := verifier.Authenticate(context.Background(), sgsp.Credential{Scheme: "jwt", Data: []byte(identityToken(t, private, "key", claims, typ))})
			if !errors.Is(err, sgsp.ErrUnauthenticated) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	unknownKid := identityToken(t, private, "unknown", base(), identityType)
	if _, err := verifier.Authenticate(context.Background(), sgsp.Credential{Scheme: "jwt", Data: []byte(unknownKid)}); !errors.Is(err, sgsp.ErrUnauthenticated) {
		t.Fatalf("unknown kid = %v", err)
	}
	if err := verifier.ReplaceKeys(map[string]ed25519.PublicKey{"other": public}); err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.Authenticate(context.Background(), sgsp.Credential{Scheme: "jwt", Data: []byte(identityToken(t, private, "key", base(), identityType))}); !errors.Is(err, sgsp.ErrUnauthenticated) {
		t.Fatalf("old key still trusted: %v", err)
	}
	if err := verifier.ReplaceKeys(map[string]ed25519.PublicKey{"key": public}); err != nil {
		t.Fatal(err)
	}
}

func TestAdmissionSigningAndVerification(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_700_000_000, 0)
	signer, err := NewAdmissionSigner(SignerConfig{Issuer: "bootstrap", Audience: "app", KeyID: "ticket-key", PrivateKey: private, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := NewAdmissionVerifier(AdmissionConfig{Issuer: "bootstrap", Audience: "app", Keys: map[string]ed25519.PublicKey{"ticket-key": public}, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	incarnation := sgsp.Incarnation{1}
	admission := sgsp.Admission{App: sgsp.AppIdentity{ID: "app", Version: "1"}, PrincipalIssuer: "identity", Subject: "player", GroupKey: "match", Owner: sgsp.Owner{ID: "owner", Incarnation: incarnation, Endpoint: sgsp.Endpoint{Address: "127.0.0.1:9000", ServerName: "game.test"}}, ExpiresAt: now.Add(30 * time.Second)}
	token, err := signer.Sign(context.Background(), admission)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := verifier.Verify(context.Background(), token)
	if err != nil || decoded.Owner != admission.Owner || decoded.PrincipalIssuer != admission.PrincipalIssuer || decoded.GroupKey != admission.GroupKey || !decoded.ExpiresAt.Equal(admission.ExpiresAt) {
		t.Fatalf("decoded admission = %#v, %v", decoded, err)
	}
	identityClaims := identityClaims{RegisteredClaims: library.RegisteredClaims{Issuer: "bootstrap", Subject: "player", Audience: library.ClaimStrings{"app"}, ExpiresAt: library.NewNumericDate(now.Add(time.Minute)), IssuedAt: library.NewNumericDate(now), NotBefore: library.NewNumericDate(now)}}
	identity := identityToken(t, private, "ticket-key", identityClaims, identityType)
	if _, err := verifier.Verify(context.Background(), identity); !errors.Is(err, sgsp.ErrForbidden) {
		t.Fatalf("identity accepted as ticket: %v", err)
	}
	admission.ExpiresAt = now.Add(31 * time.Second)
	if _, err := signer.Sign(context.Background(), admission); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("long ticket = %v", err)
	}
	admission.ExpiresAt, admission.App.ID = now.Add(time.Second), "other"
	if _, err := signer.Sign(context.Background(), admission); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("wrong app = %v", err)
	}
}

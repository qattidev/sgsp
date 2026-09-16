package sgsp

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"math/big"
	"net"
	goruntime "runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"qattidev/sgsp/internal/quictransport"
	"qattidev/sgsp/internal/runtime"
	"qattidev/sgsp/internal/transport"
	"qattidev/sgsp/internal/udprelay"
	"qattidev/sgsp/internal/wire"
)

type testAuthenticator struct {
	calls          atomic.Int32
	expires        time.Duration
	refreshExpires time.Duration
}

type principalAuthenticator struct{}

type authenticatorFunc func(context.Context, Credential) (Principal, error)

func (f authenticatorFunc) Authenticate(ctx context.Context, credential Credential) (Principal, error) {
	return f(ctx, credential)
}

func (principalAuthenticator) Authenticate(_ context.Context, credential Credential) (Principal, error) {
	switch string(credential.Data) {
	case "issuer-a":
		return Principal{Issuer: "issuer-a", Subject: "player", ExpiresAt: time.Now().Add(time.Minute)}, nil
	case "issuer-b":
		return Principal{Issuer: "issuer-b", Subject: "player", ExpiresAt: time.Now().Add(time.Minute)}, nil
	default:
		return Principal{}, ErrUnauthenticated
	}
}

type countedReceiveStream struct {
	reader bytes.Reader
	reads  atomic.Int64
}

func newCountedReceiveStream(payload []byte) *countedReceiveStream {
	stream := &countedReceiveStream{}
	stream.reader.Reset(payload)
	return stream
}

func (s *countedReceiveStream) Read(payload []byte) (int, error) {
	count, err := s.reader.Read(payload)
	s.reads.Add(int64(count))
	return count, err
}
func (*countedReceiveStream) CloseRead()                      {}
func (*countedReceiveStream) SetReadDeadline(time.Time) error { return nil }

type countedBidiStream struct {
	*countedReceiveStream
	writes bytes.Buffer
}

func (s *countedBidiStream) Write(payload []byte) (int, error) { return s.writes.Write(payload) }
func (*countedBidiStream) Close() error                        { return nil }
func (*countedBidiStream) CloseWrite() error                   { return nil }
func (*countedBidiStream) Abort(uint64)                        {}
func (*countedBidiStream) SetDeadline(time.Time) error         { return nil }

type deadlineBlockingControlStream struct {
	started       chan struct{}
	unblock       chan struct{}
	deadlines     []time.Time
	deadlineCalls int
}

func newDeadlineBlockingControlStream() *deadlineBlockingControlStream {
	return &deadlineBlockingControlStream{started: make(chan struct{}), unblock: make(chan struct{})}
}
func (s *deadlineBlockingControlStream) Read([]byte) (int, error) {
	close(s.started)
	<-s.unblock
	return 0, context.DeadlineExceeded
}
func (*deadlineBlockingControlStream) Write(payload []byte) (int, error) { return len(payload), nil }
func (s *deadlineBlockingControlStream) SetDeadline(deadline time.Time) error {
	s.deadlineCalls++
	s.deadlines = append(s.deadlines, deadline)
	if !deadline.IsZero() {
		close(s.unblock)
	}
	return nil
}

type blockingControlWriteStream struct {
	started chan struct{}
	unblock chan struct{}
}

func newBlockingControlWriteStream() *blockingControlWriteStream {
	return &blockingControlWriteStream{started: make(chan struct{}, 1), unblock: make(chan struct{})}
}
func (*blockingControlWriteStream) Read([]byte) (int, error) { return 0, io.EOF }
func (s *blockingControlWriteStream) Write(payload []byte) (int, error) {
	select {
	case s.started <- struct{}{}:
	default:
	}
	<-s.unblock
	return len(payload), nil
}

func (a *testAuthenticator) Authenticate(_ context.Context, credential Credential) (Principal, error) {
	a.calls.Add(1)
	if string(credential.Data) != "valid" && string(credential.Data) != "refresh" {
		return Principal{}, ErrUnauthenticated
	}
	expires := a.expires
	if string(credential.Data) == "refresh" && a.refreshExpires != 0 {
		expires = a.refreshExpires
	}
	if expires == 0 {
		expires = time.Minute
	}
	return Principal{Issuer: "test", Subject: "player", ExpiresAt: time.Now().Add(expires)}, nil
}

type testAdmissionVerifier struct{ admission Admission }

func (v *testAdmissionVerifier) Verify(_ context.Context, ticket string) (Admission, error) {
	if ticket != "ticket" {
		return Admission{}, ErrForbidden
	}
	return v.admission, nil
}

func endpointCertificate(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(parsed)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: private}, pool
}

func TestAuthenticationBarrier(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	authenticator := &testAuthenticator{}
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: authenticator, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	clientConfig := ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	if client.Session().State() != Active || client.Session().Principal().Subject != "player" {
		t.Fatalf("unauthenticated session state: %#v", client.Session())
	}
	if authenticator.calls.Load() != 1 || len(server.Sessions()) != 1 {
		t.Fatalf("authentication/session commit = %d/%d", authenticator.calls.Load(), len(server.Sessions()))
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestRejectedCredentialsNeverCreateSession(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	observations := &recordingObserver{values: make(chan Observation, 8)}
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: &testAuthenticator{}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}, Observer: observations})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("invalid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err == nil {
		t.Fatal("invalid credentials connected")
	}
	if len(server.Sessions()) != 0 {
		t.Fatal("rejected credentials created an active session")
	}
	observationDeadline := time.NewTimer(2 * time.Second)
	defer observationDeadline.Stop()
	for {
		select {
		case observation := <-observations.values:
			if observation.Name == "handshakes" && observation.Kind == "result" && observation.Code == Unauthenticated && observation.Value == 1 {
				goto observed
			}
		case <-observationDeadline.C:
			t.Fatal("missing unauthenticated handshake observation")
		}
	}
observed:
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestAdmissionBinding(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	verifier := &testAdmissionVerifier{}
	var authorized atomic.Int32
	var committed atomic.Int32
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: &testAuthenticator{}, Admission: verifier, AuthorizeGroup: func(_ context.Context, principal Principal, group string) error {
		if principal.Subject != "player" || group != "match" {
			return ErrForbidden
		}
		authorized.Add(1)
		return nil
	}, CommitGroupClose: func(_ context.Context, app AppIdentity, group string, owner Owner) error {
		if app != (AppIdentity{ID: "app", Version: "1"}) || group != "match" || owner.ID != "server" {
			return ErrInvalidArgument
		}
		committed.Add(1)
		return nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	owner := server.Owner()
	verifier.admission = Admission{App: AppIdentity{ID: "app", Version: "1"}, PrincipalIssuer: "test", Subject: "player", GroupKey: "match", Owner: owner, ExpiresAt: time.Now().Add(time.Minute)}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, GroupKey: "match", AdmissionTicket: "ticket", Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	if authorized.Load() != 1 {
		t.Fatalf("authorizer calls = %d", authorized.Load())
	}
	if err := server.CloseGroup(ctx, "match"); err != nil {
		t.Fatal(err)
	}
	if committed.Load() != 1 {
		t.Fatalf("group close commits = %d", committed.Load())
	}
	select {
	case <-client.Session().Context().Done():
	case <-time.After(time.Second):
		t.Fatal("group close did not close session")
	}
	_ = client.Close(context.Background())
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestGroupCloseRace(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	verifier := &testAdmissionVerifier{}
	commitStarted := make(chan struct{})
	releaseCommit := make(chan struct{})
	persistFailure := errors.New("durable group close unavailable")
	var commits atomic.Int32
	server, err := NewServer(ServerConfig{
		TLS:       &tls.Config{Certificates: []tls.Certificate{certificate}},
		App:       AppIdentity{ID: "app", Version: "1"},
		Owner:     Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}},
		Auth:      &testAuthenticator{},
		Admission: verifier,
		AuthorizeGroup: func(_ context.Context, principal Principal, group string) error {
			if principal.Subject != "player" || group != "match" {
				return ErrForbidden
			}
			return nil
		},
		CommitGroupClose: func(_ context.Context, _ AppIdentity, group string, _ Owner) error {
			if group != "match" {
				return ErrInvalidArgument
			}
			if commits.Add(1) == 1 {
				close(commitStarted)
				<-releaseCommit
				return persistFailure
			}
			return nil
		},
		Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()},
	})
	if err != nil {
		t.Fatal(err)
	}
	owner := server.Owner()
	verifier.admission = Admission{App: AppIdentity{ID: "app", Version: "1"}, PrincipalIssuer: "test", Subject: "player", GroupKey: "match", Owner: owner, ExpiresAt: time.Now().Add(time.Minute)}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	clientConfig := ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, GroupKey: "match", AdmissionTicket: "ticket", Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}}
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	closeResult := make(chan error, 1)
	go func() { closeResult <- server.CloseGroup(ctx, "match") }()
	select {
	case <-commitStarted:
	case <-ctx.Done():
		t.Fatal("group close did not reach durable commit")
	}
	// The closure flag is set before persistence. An admission racing the
	// unavailable durable store must be rejected locally, not admitted and then
	// later moved to another owner.
	if raced, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, clientConfig); err == nil {
		_ = raced.Close(context.Background())
		t.Fatal("group admission succeeded while group closure was pending")
	}
	close(releaseCommit)
	select {
	case err := <-closeResult:
		if !errors.Is(err, persistFailure) {
			t.Fatalf("first group close = %v, want persistence error", err)
		}
	case <-ctx.Done():
		t.Fatal("first group close did not return")
	}
	endpoint.groupMu.Lock()
	group := endpoint.groups["match"]
	closed, persisted := group != nil && group.closed, group != nil && group.persisted
	endpoint.groupMu.Unlock()
	if !closed || persisted {
		t.Fatalf("failed durable close group state = closed:%t persisted:%t", closed, persisted)
	}
	if err := server.CloseGroup(ctx, "match"); err != nil {
		t.Fatalf("group close retry = %v", err)
	}
	if commits.Load() != 2 {
		t.Fatalf("durable close attempts = %d, want 2", commits.Load())
	}
	endpoint.groupMu.Lock()
	persisted = endpoint.groups["match"].persisted
	endpoint.groupMu.Unlock()
	if !persisted {
		t.Fatal("successful durable close was not retained")
	}
	select {
	case <-client.Session().Context().Done():
	case <-ctx.Done():
		t.Fatal("group close did not terminate the existing session")
	}
	_ = client.Close(context.Background())
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestRevocation(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: principalAuthenticator{}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	dial := func(identity string) Client {
		client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
			return Credential{Scheme: "test", Data: []byte(identity)}, nil
		}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
		if err != nil {
			t.Fatal(err)
		}
		return client
	}
	first, second, other := dial("issuer-a"), dial("issuer-a"), dial("issuer-b")
	if err := server.Revoke(ctx, "issuer-a", "player"); err != nil {
		t.Fatal(err)
	}
	for _, client := range []Client{first, second} {
		select {
		case <-client.Session().Context().Done():
		case <-time.After(time.Second):
			t.Fatal("revoked session remained active")
		}
		if client.Session().State() != Closed {
			t.Fatalf("revoked session state = %v", client.Session().State())
		}
	}
	if other.Session().State() != Active || other.Session().Principal().Issuer != "issuer-b" {
		t.Fatalf("unrelated identity was revoked: %#v", other.Session())
	}
	if sessions := server.Sessions(); len(sessions) != 1 || sessions[0].Principal().Issuer != "issuer-b" {
		t.Fatalf("remaining sessions = %#v", sessions)
	}
	_ = first.Close(context.Background())
	_ = second.Close(context.Background())
	_ = other.Close(context.Background())
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestAuthenticationExpirationClosesSession(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: &testAuthenticator{expires: 25 * time.Millisecond}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-client.Session().Context().Done():
	case <-time.After(time.Second):
		t.Fatal("expired session remained active")
	}
	if client.Session().State() != Closed {
		t.Fatalf("expired state = %v", client.Session().State())
	}
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestAuthRefresh(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	authenticator := &testAuthenticator{expires: time.Second, refreshExpires: 2 * time.Second}
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: authenticator, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	before := client.Session().Principal().ExpiresAt
	if err := client.Session().RefreshAuth(ctx, Credential{Scheme: "test", Data: []byte("refresh")}); err != nil {
		t.Fatal(err)
	}
	if after := client.Session().Principal().ExpiresAt; !after.After(before) {
		t.Fatalf("expiration did not advance: %v <= %v", after, before)
	}
	if got := server.Sessions()[0].Principal().ExpiresAt; !got.After(before) {
		t.Fatalf("server expiration did not advance: %v", got)
	}
	_ = client.Close(context.Background())
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestLogicalCloseAcknowledgedAndTerminalLifecycleDrains(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: &testAuthenticator{}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Polling}})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatalf("Close = %v", err)
	}
	terminal, err := client.Next(ctx)
	if err != nil {
		t.Fatalf("terminal Next = %v", err)
	}
	defer terminal.Release()
	if terminal.Kind != LifecycleMessage || terminal.Lifecycle == nil || terminal.Lifecycle.Kind != SessionEnded || terminal.Lifecycle.Reason != Normal {
		t.Fatalf("terminal lifecycle = %#v", terminal)
	}
	if _, err := client.Next(ctx); err != ErrSessionClosed {
		t.Fatalf("Next after terminal = %v", err)
	}
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestAuthenticatedEventDelivery(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	events := make(chan string, 3)
	router := NewRouter()
	if err := router.OnEvent(7, func(_ context.Context, incoming *Incoming) {
		events <- string(incoming.Payload) + ":" + map[Delivery]string{ReliableOrdered: "reliable", Unreliable: "unreliable"}[incoming.Delivery]
	}); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: &testAuthenticator{}, Dispatch: DispatchConfig{Mode: Handlers, Router: router}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Session().Send(ctx, 7, []byte("command"), SendOptions{Delivery: ReliableOrdered}); err != nil {
		t.Fatal(err)
	}
	select {
	case value := <-events:
		if value != "command:reliable" {
			t.Fatalf("reliable event = %q", value)
		}
	case <-time.After(time.Second):
		t.Fatal("reliable event was not delivered")
	}
	if err := client.Session().Send(ctx, 7, []byte("command-two"), SendOptions{Delivery: ReliableOrdered}); err != nil {
		t.Fatal(err)
	}
	select {
	case value := <-events:
		if value != "command-two:reliable" {
			t.Fatalf("second reliable event = %q", value)
		}
	case <-time.After(time.Second):
		t.Fatal("second reliable event was not delivered")
	}
	if err := client.Session().Send(ctx, 7, []byte("input"), SendOptions{Channel: 1, Delivery: Unreliable}); err != nil {
		t.Fatal(err)
	}
	select {
	case value := <-events:
		if value != "input:unreliable" {
			t.Fatalf("datagram event = %q", value)
		}
	case <-time.After(time.Second):
		t.Fatal("datagram event was not delivered")
	}
	_ = client.Close(context.Background())
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestAuthenticatedRequestReply(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	router := NewRouter()
	if err := router.OnRequest(8, func(ctx context.Context, incoming *Incoming) {
		if err := incoming.Reply(ctx, append([]byte("echo:"), incoming.Payload...)); err != nil {
			t.Errorf("reply: %v", err)
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := router.OnRequest(9, func(ctx context.Context, incoming *Incoming) {
		if err := incoming.Fail(ctx, Forbidden, "not permitted"); err != nil {
			t.Errorf("fail: %v", err)
		}
	}); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: &testAuthenticator{}, Dispatch: DispatchConfig{Mode: Handlers, Router: router}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Session().Call(ctx, 8, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if string(response) != "echo:hello" {
		t.Fatalf("response = %q", response)
	}
	_, err = client.Session().Call(ctx, 9, nil)
	if remote, ok := err.(*Error); !ok || remote.Code != Forbidden || remote.Message != "not permitted" {
		t.Fatalf("remote failure = %#v", err)
	}
	_ = client.Close(context.Background())
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestAuthenticatedRequestLossHasUnknownOutcome(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	router := NewRouter()
	if err := router.OnRequest(23, func(_ context.Context, incoming *Incoming) {
		_ = incoming.Session.Close(context.Background(), Internal)
	}); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: &testAuthenticator{}, Dispatch: DispatchConfig{Mode: Handlers, Router: router}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Session().Call(ctx, 23, []byte("operation"))
	var protocol *Error
	if !errors.As(err, &protocol) || !protocol.OutcomeUnknown {
		t.Fatalf("Call error = %#v", err)
	}
	_ = client.Close(context.Background())
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestPeerReceiveLimitsBoundOutboundMessages(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	limits := DefaultLimits()
	limits.MessageBytes = 10
	limits.QueueBytes = 2 * (limits.MessageBytes + 32)
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: &testAuthenticator{}, Limits: limits, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	err = client.Session().Send(ctx, 1, make([]byte, limits.MessageBytes+1), SendOptions{Delivery: ReliableOrdered})
	var protocol *Error
	if !errors.As(err, &protocol) || protocol.Code != TooLarge || protocol.MaxPayload != limits.MessageBytes {
		t.Fatalf("Send error = %#v", err)
	}
	_ = client.Close(context.Background())
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestFullJitterStaysWithinBackoffCap(t *testing.T) {
	if delay := fullJitter(0); delay != 0 {
		t.Fatalf("zero jitter = %v", delay)
	}
	maximum := 5 * time.Millisecond
	for range 100 {
		if delay := fullJitter(maximum); delay < 0 || delay > maximum {
			t.Fatalf("jitter delay = %v, maximum = %v", delay, maximum)
		}
	}
}

func TestClientWelcomeValidation(t *testing.T) {
	secret := make([]byte, 32)
	welcome := welcomeMessage{Epoch: "1", Principal: principalMessage{ExpiresMS: time.Now().Add(time.Minute).UnixMilli()}, Resumable: true, ResumeGraceMS: 1000, ResumeSecret: base64.RawURLEncoding.EncodeToString(secret)}
	if err := validateClientWelcome(welcome, GameRole, false); err != nil {
		t.Fatalf("valid game welcome = %v", err)
	}
	bad := welcome
	bad.ResumeSecret = ""
	if err := validateClientWelcome(bad, GameRole, false); err == nil {
		t.Fatal("missing game resume secret accepted")
	}
	resumed := welcome
	resumed.Resumed, resumed.Epoch, resumed.ResumeSecret = true, "2", ""
	if err := validateClientWelcome(resumed, GameRole, true); err != nil {
		t.Fatalf("valid resumed welcome = %v", err)
	}
	resumed.ResumeSecret = welcome.ResumeSecret
	if err := validateClientWelcome(resumed, GameRole, true); err == nil {
		t.Fatal("resumed welcome rotated secret")
	}
	bootstrap := welcomeMessage{Epoch: "1", Principal: principalMessage{ExpiresMS: time.Now().Add(time.Minute).UnixMilli()}}
	if err := validateClientWelcome(bootstrap, BootstrapRole, false); err != nil {
		t.Fatalf("valid bootstrap welcome = %v", err)
	}
}

func TestAuthenticatedCustomStream(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	router := NewRouter()
	streamStarted := make(chan struct{}, 1)
	events := make(chan struct{}, 1)
	if err := router.OnEvent(11, func(context.Context, *Incoming) { events <- struct{}{} }); err != nil {
		t.Fatal(err)
	}
	if err := router.OnStream(10, func(_ context.Context, incoming *Incoming) {
		streamStarted <- struct{}{}
		payload, err := io.ReadAll(incoming.Stream)
		if err != nil {
			t.Errorf("read stream: %v", err)
			return
		}
		if _, err := incoming.Stream.Write(append([]byte("echo:"), payload...)); err != nil {
			t.Errorf("write stream: %v", err)
			return
		}
		if err := incoming.Stream.CloseWrite(); err != nil {
			t.Errorf("close response: %v", err)
		}
	}); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: &testAuthenticator{}, Dispatch: DispatchConfig{Mode: Handlers, Router: router}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := client.Session().OpenStream(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-streamStarted:
	case <-time.After(time.Second):
		t.Fatal("stream handler did not start")
	}
	if err := client.Session().Send(ctx, 11, []byte("ordinary"), SendOptions{Delivery: ReliableOrdered}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-events:
	case <-time.After(time.Second):
		t.Fatal("ordinary event was blocked behind active custom stream")
	}
	if _, err := stream.Write([]byte("bytes")); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	response, err := io.ReadAll(stream)
	if err != nil {
		t.Fatal(err)
	}
	if string(response) != "echo:bytes" {
		t.Fatalf("stream response = %q", response)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	_ = client.Close(context.Background())
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestPollingEndpointDispatch(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: &testAuthenticator{}, Dispatch: DispatchConfig{Mode: Polling}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Polling}})
	if err != nil {
		t.Fatal(err)
	}
	serverOpened, err := server.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if serverOpened.Kind != LifecycleMessage || serverOpened.Lifecycle.Kind != Opened {
		t.Fatalf("server opened = %#v", serverOpened)
	}
	serverOpened.Release()
	clientOpened, err := client.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if clientOpened.Kind != LifecycleMessage || clientOpened.Lifecycle.Kind != Opened {
		t.Fatalf("client opened = %#v", clientOpened)
	}
	clientOpened.Release()
	if err := client.Session().Send(ctx, 11, []byte("to-server"), SendOptions{Delivery: ReliableOrdered}); err != nil {
		t.Fatal(err)
	}
	serverEvent, err := server.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if serverEvent.Kind != Event || string(serverEvent.Payload) != "to-server" {
		t.Fatalf("server event = %#v", serverEvent)
	}
	serverEvent.Release()
	if err := server.Sessions()[0].Send(ctx, 12, []byte("to-client"), SendOptions{Delivery: ReliableOrdered}); err != nil {
		t.Fatal(err)
	}
	clientEvent, err := client.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if clientEvent.Kind != Event || string(clientEvent.Payload) != "to-client" {
		t.Fatalf("client event = %#v", clientEvent)
	}
	clientEvent.Release()
	callResult := make(chan struct {
		payload []byte
		err     error
	}, 1)
	go func() {
		payload, err := client.Session().Call(ctx, 13, []byte("request"))
		callResult <- struct {
			payload []byte
			err     error
		}{payload, err}
	}()
	request, err := server.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if request.Kind != RequestMessage || string(request.Payload) != "request" {
		t.Fatalf("server request = %#v", request)
	}
	if err := request.Reply(ctx, []byte("response")); err != nil {
		t.Fatal(err)
	}
	request.Release()
	result := <-callResult
	if result.err != nil || string(result.payload) != "response" {
		t.Fatalf("Call = %q, %v", result.payload, result.err)
	}
	_ = client.Close(context.Background())
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestResumeIdentity(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	observations := &recordingObserver{values: make(chan Observation, 64)}
	lifecycles := make(chan LifecycleKind, 3)
	router := NewRouter()
	if err := router.OnLifecycle(func(_ context.Context, incoming *Incoming) { lifecycles <- incoming.Lifecycle.Kind }); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: &testAuthenticator{}, Dispatch: DispatchConfig{Mode: Handlers, Router: router}, Observer: observations})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case kind := <-lifecycles:
		if kind != Opened {
			t.Fatalf("initial lifecycle = %v", kind)
		}
	case <-time.After(time.Second):
		t.Fatal("missing Opened lifecycle")
	}
	clientSession := client.Session()
	clientSession.SetAttachment("retained")
	beforeID, beforeEpoch := clientSession.ID(), clientSession.Epoch()
	serverSession := endpoint.Sessions()[0].(*sessionRecord)
	if err := closeCurrentTransport(serverSession, "forced loss"); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(4 * time.Second)
	defer deadline.Stop()
	for clientSession.State() != Active || clientSession.Epoch() <= beforeEpoch || len(endpoint.Sessions()) != 1 || endpoint.Sessions()[0].Epoch() <= beforeEpoch {
		select {
		case <-time.After(10 * time.Millisecond):
		case <-deadline.C:
			t.Fatalf("resume state client=%v/%d server=%d sessions=%d", clientSession.State(), clientSession.Epoch(), func() uint64 {
				if len(endpoint.Sessions()) == 0 {
					return 0
				}
				return endpoint.Sessions()[0].Epoch()
			}(), len(endpoint.Sessions()))
		}
	}
	if clientSession.ID() != beforeID || clientSession.Attachment() != "retained" {
		t.Fatalf("resume did not retain identity/attachment: %x %#v", clientSession.ID(), clientSession.Attachment())
	}
	select {
	case kind := <-lifecycles:
		if kind != ConnectionLost {
			t.Fatalf("loss lifecycle = %v", kind)
		}
	case <-time.After(time.Second):
		t.Fatal("missing ConnectionLost lifecycle")
	}
	select {
	case kind := <-lifecycles:
		if kind != Resumed {
			t.Fatalf("resumed lifecycle = %v", kind)
		}
	case <-time.After(time.Second):
		t.Fatal("missing Resumed lifecycle")
	}
	type observationKey struct {
		kind string
		code Code
	}
	resumeObservations := make(map[observationKey]float64)
	handshakeResults := make(map[Code]float64)
	observationDeadline := time.NewTimer(3 * time.Second)
	defer observationDeadline.Stop()
	for handshakeResults[Normal] < 2 ||
		resumeObservations[observationKey{"attempt", Normal}] < 1 ||
		resumeObservations[observationKey{"success", Normal}] < 1 ||
		resumeObservations[observationKey{"superseded", SessionSuperseded}] < 1 {
		select {
		case observation := <-observations.values:
			if observation.Name == "resumes" {
				resumeObservations[observationKey{observation.Kind, observation.Code}] += observation.Value
			}
			if observation.Name == "handshakes" && observation.Kind == "result" {
				handshakeResults[observation.Code] += observation.Value
			}
		case <-observationDeadline.C:
			t.Fatalf("resume observations = %#v; handshake results = %#v", resumeObservations, handshakeResults)
		}
	}
	_ = client.Close(context.Background())
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestResumeCredentialBinding(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: principalAuthenticator{}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("issuer-a")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	concrete := client.(*clientEndpoint)
	serverSession := endpoint.Sessions()[0].(*sessionRecord)
	serverSession.mu.RLock()
	originalOperations := serverSession.operations
	serverSession.mu.RUnlock()

	attempt := func(name string, credential []byte, app AppIdentity, limits limitMessage, mutate func(*resumeMessage)) {
		t.Helper()
		resumeCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		attemptPacket, err := net.ListenPacket("udp", "[::]:0")
		if err != nil {
			t.Fatal(err)
		}
		defer attemptPacket.Close()
		remote, err := net.ResolveUDPAddr("udp", concrete.endpoint.Address)
		if err != nil {
			t.Fatal(err)
		}
		tlsConfig := concrete.config.TLS.Clone()
		tlsConfig.ServerName = concrete.endpoint.ServerName
		connection, err := quictransport.Dial(resumeCtx, attemptPacket, remote, transportConfig(tlsConfig, concrete.limits))
		if err != nil {
			t.Fatal(err)
		}
		defer connection.Close(uint64(Internal), "resume binding test")
		stream, err := connection.OpenBidi(resumeCtx)
		if err != nil {
			t.Fatal(err)
		}
		id, owner := concrete.session.ID(), concrete.session.Owner()
		resume := resumeMessage{SessionID: hex.EncodeToString(id[:]), OwnerID: owner.ID, Incarnation: hex.EncodeToString(owner.Incarnation[:]), Secret: concrete.resumeSecret}
		mutate(&resume)
		channel := newControlChannel(stream)
		hello := helloMessage{Op: "hello", Role: roleName(concrete.config.Role), App: app, Required: []string{"datagrams"}, Limits: limits, Credential: credentialMessage{Scheme: "test", Data: base64.RawURLEncoding.EncodeToString(credential)}, Resume: &resume}
		if err := channel.write(hello); err != nil {
			t.Fatal(err)
		}
		if _, err := channel.read(resumeCtx, concrete.limits.ControlBytes); err == nil {
			t.Fatalf("%s resume unexpectedly received a welcome", name)
		}
		serverSession.mu.RLock()
		state, epoch, operations := serverSession.state, serverSession.epoch, serverSession.operations
		serverSession.mu.RUnlock()
		if state != Active || epoch != 1 || operations != originalOperations {
			t.Fatalf("%s changed active session to state=%v epoch=%d operations=%T", name, state, epoch, operations)
		}
	}

	validLimits := limitMessageFrom(concrete.limits)
	attempt("subject", []byte("issuer-b"), concrete.config.App, validLimits, func(*resumeMessage) {})
	attempt("secret", []byte("issuer-a"), concrete.config.App, validLimits, func(resume *resumeMessage) {
		resume.Secret = base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	})
	attempt("owner", []byte("issuer-a"), concrete.config.App, validLimits, func(resume *resumeMessage) { resume.OwnerID = "other" })
	attempt("incarnation", []byte("issuer-a"), concrete.config.App, validLimits, func(resume *resumeMessage) {
		resume.Incarnation = hex.EncodeToString(make([]byte, len(Incarnation{})))
	})
	attempt("app", []byte("issuer-a"), AppIdentity{ID: "app", Version: "2"}, validLimits, func(*resumeMessage) {})
	changedLimits := validLimits
	changedLimits.MessageBytes--
	attempt("limits", []byte("issuer-a"), concrete.config.App, changedLimits, func(*resumeMessage) {})

	_ = client.Close(context.Background())
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestNATRebinding(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverRouter := NewRouter()
	if err := serverRouter.OnEvent(51, func(ctx context.Context, incoming *Incoming) {
		_ = incoming.Session.Send(ctx, 52, append([]byte(nil), incoming.Payload...), SendOptions{Channel: 2, Delivery: UnreliableSequenced})
	}); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: &testAuthenticator{}, Dispatch: DispatchConfig{Mode: Handlers, Router: serverRouter}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	upstream, err := net.ResolveUDPAddr("udp", packet.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	forwarded := make(chan udprelay.Direction, 256)
	relay, err := udprelay.New("127.0.0.1:0", "127.0.0.1:0", upstream, udprelay.Config{Seed: 51, OnForward: func(direction udprelay.Direction) {
		select {
		case forwarded <- direction:
		default:
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	updates := make(chan string, 2)
	clientRouter := NewRouter()
	if err := clientRouter.OnEvent(52, func(_ context.Context, incoming *Incoming) { updates <- string(incoming.Payload) }); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: relay.ClientAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, DisableReconnect: true, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: clientRouter}})
	if err != nil {
		t.Fatal(err)
	}
	beforeEpoch := client.Session().Epoch()
	exchange := func(payload string) {
		t.Helper()
		if err := client.Session().Send(ctx, 51, []byte(payload), SendOptions{Channel: 1, Delivery: UnreliableSequenced}); err != nil {
			t.Fatal(err)
		}
		select {
		case update := <-updates:
			if update != payload {
				t.Fatalf("echo update = %q, want %q", update, payload)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	exchange("before")
	for {
		select {
		case <-forwarded:
		default:
			goto drainedForwarding
		}
	}

drainedForwarding:
	if err := relay.RebindUpstream("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	// The first non-probing packet on the new source path causes the server to
	// send PATH_CHALLENGE. QUIC switches only after receiving PATH_RESPONSE and
	// a subsequent non-probing packet on that validated path. The probe's
	// unreliable echo is intentionally not awaited because it can be addressed
	// to the old path while validation is in progress. The relay's direction-only
	// forwarding hook makes the validation sequence a barrier instead of a
	// timing-dependent sleep.
	if err := client.Session().Send(ctx, 51, []byte("rebinding-probe"), SendOptions{Channel: 1, Delivery: UnreliableSequenced}); err != nil {
		t.Fatal(err)
	}
	waitForward := func(want udprelay.Direction) {
		t.Helper()
		for {
			select {
			case direction := <-forwarded:
				if direction == want {
					return
				}
			case <-ctx.Done():
				t.Fatalf("waiting for %v relay forwarding: %v", want, ctx.Err())
			}
		}
	}
	waitForward(udprelay.ClientToServer) // non-probing packet on the new path
	waitForward(udprelay.ServerToClient) // server PATH_CHALLENGE
	waitForward(udprelay.ClientToServer) // client PATH_RESPONSE
	exchange("after")
	if client.Session().State() != Active || client.Session().Epoch() != beforeEpoch {
		t.Fatalf("client rebinding state/epoch = %v/%d, want active/%d", client.Session().State(), client.Session().Epoch(), beforeEpoch)
	}
	serverSession := endpoint.Sessions()[0]
	if serverSession.State() != Active || serverSession.Epoch() != beforeEpoch {
		t.Fatalf("server rebinding state/epoch = %v/%d, want active/%d", serverSession.State(), serverSession.Epoch(), beforeEpoch)
	}
	_ = client.Close(context.Background())
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestOwnerRestart(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := packet.LocalAddr().String()
	config := ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: address, ServerName: "localhost"}}, Auth: &testAuthenticator{}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}}
	first, err := NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	firstEndpoint := first.(*serverEndpoint)
	firstCtx, stopFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.Serve(firstCtx, packet) }()
	<-firstEndpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: address, ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, DisableReconnect: true, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	concrete := client.(*clientEndpoint)
	firstOwner := first.Owner()
	if firstOwner.Incarnation == (Incarnation{}) {
		t.Fatal("first owner has no process incarnation")
	}
	stopFirst()
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	restartedPacket, err := net.ListenPacket("udp", address)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewServer(config)
	if err != nil {
		_ = restartedPacket.Close()
		t.Fatal(err)
	}
	secondOwner := second.Owner()
	if secondOwner.ID != firstOwner.ID || secondOwner.Incarnation == firstOwner.Incarnation {
		_ = restartedPacket.Close()
		t.Fatalf("restart owner = %#v, want same ID and a fresh incarnation from %#v", secondOwner, firstOwner)
	}
	secondEndpoint := second.(*serverEndpoint)
	secondCtx, stopSecond := context.WithCancel(context.Background())
	defer stopSecond()
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.Serve(secondCtx, restartedPacket) }()
	<-secondEndpoint.started
	if _, err := concrete.openResume(ctx); err == nil {
		t.Fatal("old process resume unexpectedly attached to restarted owner")
	}
	_ = client.Close(context.Background())
	stopSecond()
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
}

func TestClientReconnectLoop(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var authentications atomic.Int32
	resumeAuthentication := make(chan struct{}, 1)
	authenticator := authenticatorFunc(func(_ context.Context, credential Credential) (Principal, error) {
		if string(credential.Data) != "valid" {
			return Principal{}, ErrUnauthenticated
		}
		if authentications.Add(1) == 1 {
			return Principal{Issuer: "test", Subject: "player", ExpiresAt: time.Now().Add(time.Minute)}, nil
		}
		select {
		case resumeAuthentication <- struct{}{}:
		default:
		}
		return Principal{}, ErrUnauthenticated
	})
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: authenticator, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	serverSession := endpoint.Sessions()[0].(*sessionRecord)
	if err := closeCurrentTransport(serverSession, "force reconnect"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-resumeAuthentication:
	case <-ctx.Done():
		t.Fatal("client did not make a resume authentication attempt")
	}
	select {
	case <-client.Session().Context().Done():
	case <-ctx.Done():
		t.Fatal("terminal resume rejection did not end reconnect loop")
	}
	if code := client.(*clientEndpoint).session.terminalCode(); code != Unauthenticated {
		t.Fatalf("terminal reconnect code = %v, want %v", code, Unauthenticated)
	}
	if calls := authentications.Load(); calls != 2 {
		t.Fatalf("authentication calls = %d, want initial plus one terminal resume", calls)
	}
	_ = client.Close(context.Background())
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentResume(t *testing.T) {
	const candidates = 100
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, candidates)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAuth := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAuth()
	authenticator := authenticatorFunc(func(ctx context.Context, credential Credential) (Principal, error) {
		switch string(credential.Data) {
		case "initial":
			return Principal{Issuer: "test", Subject: "player", ExpiresAt: time.Now().Add(time.Minute)}, nil
		case "resume":
			select {
			case entered <- struct{}{}:
			case <-ctx.Done():
				return Principal{}, ctx.Err()
			}
			select {
			case <-release:
				return Principal{Issuer: "test", Subject: "player", ExpiresAt: time.Now().Add(time.Minute)}, nil
			case <-ctx.Done():
				return Principal{}, ctx.Err()
			}
		default:
			return Principal{}, ErrUnauthenticated
		}
	})
	limits := DefaultLimits()
	limits.MaxPendingHandshakes = candidates
	limits.HandshakesPerIPPerSecond = 100_000
	limits.HandshakeBurstPerIP = 100_000
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: authenticator, Limits: limits, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	defer func() {
		releaseAuth()
		stop()
		if err := <-serveDone; err != nil {
			t.Error(err)
		}
	}()
	var useResumeCredential atomic.Bool
	credentials := func(context.Context) (Credential, error) {
		if useResumeCredential.Load() {
			return Credential{Scheme: "test", Data: []byte("resume")}, nil
		}
		return Credential{Scheme: "test", Data: []byte("initial")}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: credentials, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	concrete := client.(*clientEndpoint)
	concrete.mu.Lock()
	concrete.reconnecting = true // Candidate handshakes below deliberately replace this epoch.
	concrete.mu.Unlock()
	serverSession := endpoint.Sessions()[0].(*sessionRecord)
	useResumeCredential.Store(true)
	type candidate struct {
		packet     net.PacketConn
		connection transport.Conn
		err        error
	}
	results := make(chan candidate, candidates)
	start := make(chan struct{})
	for range candidates {
		go func() {
			<-start
			candidatePacket, connection, err := sendResumeAndDiscardWelcome(ctx, concrete)
			results <- candidate{packet: candidatePacket, connection: connection, err: err}
		}()
	}
	close(start)
	connections := make([]candidate, 0, candidates)
	for range candidates {
		result := <-results
		if result.err != nil {
			t.Fatalf("candidate resume setup: %v", result.err)
		}
		connections = append(connections, result)
	}
	defer func() {
		for _, candidate := range connections {
			_ = candidate.connection.Close(uint64(SessionClosed), "test complete")
			_ = candidate.packet.Close()
		}
	}()
	for range candidates {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatalf("authenticated resume barrier: %v", ctx.Err())
		}
	}
	releaseAuth()
	waitForSessionState(t, ctx, serverSession, Active, candidates+1)
	if got, want := serverSession.Epoch(), uint64(candidates+1); got != want {
		t.Fatalf("committed resume epoch = %d, want %d", got, want)
	}
	if sessions := endpoint.Sessions(); len(sessions) != 1 || sessions[0] != serverSession {
		t.Fatalf("server sessions after concurrent resumes = %#v", sessions)
	}
}

func TestSessionAdmissionLimit(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	limits := DefaultLimits()
	limits.MaxSessions = 1
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: &testAuthenticator{}, Limits: limits, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	clientConfig := ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}}
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, clientConfig); !errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("new session at capacity = %v, want ResourceExhausted", err)
	}
	concrete := client.(*clientEndpoint)
	concrete.mu.Lock()
	concrete.reconnecting = true // The explicit resume supersedes this transport.
	concrete.mu.Unlock()
	attachment, err := concrete.openResume(ctx)
	if err != nil {
		t.Fatalf("resume at session capacity = %v", err)
	}
	if err := concrete.attachResume(attachment); err != nil {
		t.Fatalf("attach at session capacity = %v", err)
	}
	concrete.mu.Lock()
	concrete.reconnecting = false
	concrete.mu.Unlock()
	waitForSessionState(t, ctx, concrete.session, Active, 2)
	sessions := endpoint.Sessions()
	if len(sessions) != 1 || sessions[0].Epoch() != 2 {
		t.Fatalf("resume at capacity sessions = %#v", sessions)
	}
	_ = client.Close(context.Background())
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestLostResumeWelcome(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: &testAuthenticator{}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	concrete := client.(*clientEndpoint)
	concrete.mu.Lock()
	concrete.reconnecting = true // Test drives retries explicitly.
	concrete.mu.Unlock()
	serverSession := endpoint.Sessions()[0].(*sessionRecord)
	if err := closeCurrentTransport(serverSession, "force suspended"); err != nil {
		t.Fatal(err)
	}
	waitForSessionState(t, ctx, concrete.session, Suspended, 1)
	waitForSessionState(t, ctx, serverSession, Suspended, 1)
	secret := concrete.resumeSecret
	discardedPacket, discardedConnection, err := sendResumeAndDiscardWelcome(ctx, concrete)
	if err != nil {
		t.Fatal(err)
	}
	waitForSessionState(t, ctx, serverSession, Active, 2)
	_ = discardedConnection.Close(uint64(Internal), "discard resume welcome")
	_ = discardedPacket.Close()
	waitForSessionState(t, ctx, serverSession, Suspended, 2)
	attachment, err := concrete.openResume(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := concrete.attachResume(attachment); err != nil {
		t.Fatal(err)
	}
	concrete.mu.Lock()
	concrete.reconnecting = false
	concrete.mu.Unlock()
	waitForSessionState(t, ctx, concrete.session, Active, 3)
	if concrete.resumeSecret != secret || concrete.session.ID() != serverSession.ID() {
		t.Fatal("lost WELCOME retry rotated or replaced resume identity")
	}
	_ = client.Close(context.Background())
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestResumeGrace(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	limits := DefaultLimits()
	limits.ResumeGrace = 75 * time.Millisecond
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: &testAuthenticator{}, Limits: limits, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Limits: limits, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	concrete := client.(*clientEndpoint)
	concrete.mu.Lock()
	concrete.reconnecting = true // Exercise grace manually.
	concrete.mu.Unlock()
	serverSession := endpoint.Sessions()[0].(*sessionRecord)
	if err := closeCurrentTransport(serverSession, "first loss"); err != nil {
		t.Fatal(err)
	}
	waitForSessionState(t, ctx, serverSession, Suspended, 1)
	attachment, err := concrete.openResume(ctx)
	if err != nil {
		t.Fatalf("resume before grace = %v", err)
	}
	if err := concrete.attachResume(attachment); err != nil {
		t.Fatalf("attach before grace = %v", err)
	}
	waitForSessionState(t, ctx, concrete.session, Active, 2)
	if err := closeCurrentTransport(serverSession, "second loss"); err != nil {
		t.Fatal(err)
	}
	waitForSessionState(t, ctx, serverSession, Suspended, 2)
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		_, code := endpoint.lookupSession(serverSession.ID())
		if code == SessionExpired {
			break
		}
		select {
		case <-time.After(time.Millisecond):
		case <-deadline.C:
			t.Fatal("suspended session remained resumable after grace")
		}
	}
	_ = client.Close(context.Background())
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestEpochFence(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: &testAuthenticator{}, Dispatch: DispatchConfig{Mode: Polling}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	// Hold reconnection so this test controls the suspended owner explicitly.
	clientEndpoint := client.(*clientEndpoint)
	clientEndpoint.mu.Lock()
	clientEndpoint.reconnecting = true
	clientEndpoint.mu.Unlock()
	opened, err := server.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if opened.Kind != LifecycleMessage || opened.Lifecycle == nil || opened.Lifecycle.Kind != Opened {
		t.Fatalf("initial polling item = %#v", opened)
	}
	opened.Release()
	if err := client.Session().Send(ctx, 91, []byte("queued-before-loss"), SendOptions{Delivery: ReliableOrdered}); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for endpoint.applicationBudget.Used() == 0 {
		select {
		case <-time.After(time.Millisecond):
		case <-deadline.C:
			t.Fatal("reliable event did not enter the polling queue")
		}
	}
	serverSession := endpoint.Sessions()[0].(*sessionRecord)
	if err := closeCurrentTransport(serverSession, "force polling loss"); err != nil {
		t.Fatal(err)
	}
	waitForSessionState(t, ctx, serverSession, Suspended, 1)
	loss, err := server.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if loss.Kind != LifecycleMessage || loss.Lifecycle == nil || loss.Lifecycle.Kind != ConnectionLost {
		t.Fatalf("post-loss polling item = %#v", loss)
	}
	loss.Release()
	if used := endpoint.applicationBudget.Used(); used != 0 {
		t.Fatalf("discarded polling payload remains charged: %d bytes", used)
	}
	empty, emptyCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer emptyCancel()
	if _, err := server.Next(empty); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("discarded epoch was still deliverable: %v", err)
	}
	attachment, err := clientEndpoint.openResume(ctx)
	if err != nil {
		t.Fatalf("resume after epoch fence = %v", err)
	}
	if err := clientEndpoint.attachResume(attachment); err != nil {
		t.Fatalf("attach after epoch fence = %v", err)
	}
	clientEndpoint.mu.Lock()
	clientEndpoint.reconnecting = false
	clientEndpoint.mu.Unlock()
	waitForSessionState(t, ctx, clientEndpoint.session, Active, 2)
	waitForSessionState(t, ctx, serverSession, Active, 2)
	resumed, err := server.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Kind != LifecycleMessage || resumed.Lifecycle == nil || resumed.Lifecycle.Kind != Resumed || resumed.Epoch != 2 {
		t.Fatalf("resumed polling item = %#v", resumed)
	}
	resumed.Release()
	if err := client.Session().Send(ctx, 91, []byte("current-epoch"), SendOptions{Delivery: ReliableOrdered}); err != nil {
		t.Fatal(err)
	}
	current, err := server.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if current.Kind != Event || current.Epoch != 2 || string(current.Payload) != "current-epoch" {
		t.Fatalf("current epoch polling item = %#v", current)
	}
	current.Release()
	_ = client.Close(context.Background())
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestEpochFenceRejectsOldReplyAndStream(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	requestStarted := make(chan *Incoming, 1)
	streamStarted := make(chan *Incoming, 1)
	releaseRequest := make(chan struct{})
	releaseStream := make(chan struct{})
	replyResult := make(chan error, 1)
	streamResult := make(chan error, 1)
	router := NewRouter()
	if err := router.OnRequest(96, func(_ context.Context, incoming *Incoming) {
		requestStarted <- incoming
		<-releaseRequest
		replyResult <- incoming.Reply(context.Background(), []byte("stale reply"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := router.OnStream(97, func(_ context.Context, incoming *Incoming) {
		streamStarted <- incoming
		<-releaseStream
		_, err := incoming.Stream.Write([]byte("stale stream"))
		streamResult <- err
	}); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: &testAuthenticator{}, Dispatch: DispatchConfig{Mode: Handlers, Router: router}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	concrete := client.(*clientEndpoint)
	concrete.mu.Lock()
	concrete.reconnecting = true // The test commits the replacement epoch itself.
	concrete.mu.Unlock()
	stream, err := client.Session().OpenStream(ctx, 97)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-streamStarted:
	case <-ctx.Done():
		t.Fatal("custom stream did not reach server")
	}
	callDone := make(chan error, 1)
	go func() {
		_, err := client.Session().Call(ctx, 96, []byte("operation"))
		callDone <- err
	}()
	select {
	case <-requestStarted:
	case <-ctx.Done():
		t.Fatal("request did not reach server")
	}
	serverSession := endpoint.Sessions()[0].(*sessionRecord)
	if err := closeCurrentTransport(serverSession, "fence old application work"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-callDone:
		var outcome *Error
		if !errors.As(err, &outcome) || !outcome.OutcomeUnknown {
			t.Fatalf("lost old-epoch call = %v, want OutcomeUnknown", err)
		}
	case <-ctx.Done():
		t.Fatal("old-epoch call did not unblock")
	}
	waitForSessionState(t, ctx, concrete.session, Suspended, 1)
	waitForSessionState(t, ctx, serverSession, Suspended, 1)
	attachment, err := concrete.openResume(ctx)
	if err != nil {
		t.Fatalf("resume for epoch fence = %v", err)
	}
	if err := concrete.attachResume(attachment); err != nil {
		t.Fatalf("attach for epoch fence = %v", err)
	}
	concrete.mu.Lock()
	concrete.reconnecting = false
	concrete.mu.Unlock()
	waitForSessionState(t, ctx, concrete.session, Active, 2)
	waitForSessionState(t, ctx, serverSession, Active, 2)
	if _, err := stream.Write([]byte("stale client stream")); !errors.Is(err, ErrSessionSuperseded) {
		t.Fatalf("old client stream write = %v, want SessionSuperseded", err)
	}
	close(releaseRequest)
	close(releaseStream)
	select {
	case err := <-replyResult:
		if !errors.Is(err, ErrSessionSuperseded) {
			t.Fatalf("old server reply = %v, want SessionSuperseded", err)
		}
	case <-ctx.Done():
		t.Fatal("old request handler did not return")
	}
	select {
	case err := <-streamResult:
		if !errors.Is(err, ErrSessionSuperseded) {
			t.Fatalf("old server stream write = %v, want SessionSuperseded", err)
		}
	case <-ctx.Done():
		t.Fatal("old stream handler did not return")
	}
	_ = stream.Close()
	_ = client.Close(context.Background())
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestUnknownRequestOutcome(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var committed atomic.Int32
	started := make(chan struct{}, 1)
	router := NewRouter()
	if err := router.OnRequest(94, func(ctx context.Context, _ *Incoming) {
		committed.Add(1) // The application commit happens before response delivery.
		started <- struct{}{}
		<-ctx.Done()
	}); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: &testAuthenticator{}, Dispatch: DispatchConfig{Mode: Handlers, Router: router}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	concrete := client.(*clientEndpoint)
	concrete.mu.Lock()
	concrete.reconnecting = true // The test, rather than jitter, drives the resume.
	concrete.mu.Unlock()
	callDone := make(chan error, 1)
	go func() {
		_, err := client.Session().Call(ctx, 94, []byte("counted-operation"))
		callDone <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	serverSession := endpoint.Sessions()[0].(*sessionRecord)
	if err := closeCurrentTransport(serverSession, "lose response after commit"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-callDone:
		var outcome *Error
		if !errors.As(err, &outcome) || !outcome.OutcomeUnknown || outcome.Cause == nil {
			t.Fatalf("Call after committed transport loss = %#v, want OutcomeUnknown with a cause", err)
		}
	case <-ctx.Done():
		t.Fatal("Call remained blocked after transport loss")
	}
	waitForSessionState(t, ctx, concrete.session, Suspended, 1)
	waitForSessionState(t, ctx, serverSession, Suspended, 1)
	attachment, err := concrete.openResume(ctx)
	if err != nil {
		t.Fatalf("resume after unknown outcome = %v", err)
	}
	if err := concrete.attachResume(attachment); err != nil {
		t.Fatalf("attach after unknown outcome = %v", err)
	}
	concrete.mu.Lock()
	concrete.reconnecting = false
	concrete.mu.Unlock()
	waitForSessionState(t, ctx, concrete.session, Active, 2)
	waitForSessionState(t, ctx, serverSession, Active, 2)
	if got := committed.Load(); got != 1 {
		t.Fatalf("committed operation executions = %d, want 1; Call must not replay after resume", got)
	}
	_ = client.Close(context.Background())
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestTransportLossCleanup(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	requestStarted := make(chan struct{}, 1)
	streamStarted := make(chan struct{}, 1)
	router := NewRouter()
	if err := router.OnRequest(92, func(ctx context.Context, _ *Incoming) {
		requestStarted <- struct{}{}
		<-ctx.Done()
	}); err != nil {
		t.Fatal(err)
	}
	if err := router.OnStream(93, func(_ context.Context, incoming *Incoming) {
		streamStarted <- struct{}{}
		buffer := make([]byte, 1)
		_, _ = incoming.Stream.Read(buffer)
	}); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: &testAuthenticator{}, Dispatch: DispatchConfig{Mode: Handlers, Router: router}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, DisableReconnect: true, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := client.Session().OpenStream(ctx, 93)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-streamStarted:
	case <-time.After(time.Second):
		t.Fatal("custom stream did not reach server")
	}
	readDone := make(chan error, 1)
	go func() {
		_, err := stream.Read(make([]byte, 1))
		readDone <- err
	}()
	callDone := make(chan error, 1)
	go func() {
		_, err := client.Session().Call(ctx, 92, []byte("pending"))
		callDone <- err
	}()
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("request did not reach server")
	}
	serverSession := endpoint.Sessions()[0].(*sessionRecord)
	if err := closeCurrentTransport(serverSession, "force pending cleanup"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-callDone:
		var outcome *Error
		if !errors.As(err, &outcome) || !outcome.OutcomeUnknown {
			t.Fatalf("pending call result = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending call remained blocked after transport loss")
	}
	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("blocked custom stream read completed without transport error")
		}
	case <-time.After(time.Second):
		t.Fatal("custom stream read remained blocked after transport loss")
	}
	waitForSessionState(t, ctx, serverSession, Suspended, 1)
	_ = client.Close(context.Background())
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestSlowConsumer(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	limits := DefaultLimits()
	limits.QueueMessages = 2
	limits.SlowConsumerTimeout = 50 * time.Millisecond
	started := make(chan struct{}, 1)
	router := NewRouter()
	if err := router.OnEvent(95, func(ctx context.Context, _ *Incoming) {
		started <- struct{}{}
		<-ctx.Done()
	}); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: &testAuthenticator{}, Limits: limits, Dispatch: DispatchConfig{Mode: Handlers, Router: router}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Limits: limits, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	serverSession := endpoint.Sessions()[0].(*sessionRecord)
	payload := []byte("queued")
	if err := client.Session().Send(ctx, 95, payload, SendOptions{Delivery: ReliableOrdered}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first reliable handler did not start")
	}
	if err := client.Session().Send(ctx, 95, payload, SendOptions{Delivery: ReliableOrdered}); err != nil {
		t.Fatal(err)
	}
	charge := int64(len(payload) + 32)
	wantQueuedBytes := 2 * charge
	for endpoint.applicationBudget.Used() < wantQueuedBytes {
		select {
		case <-ctx.Done():
			t.Fatalf("reliable queue charge = %d, want at least %d", endpoint.applicationBudget.Used(), wantQueuedBytes)
		case <-time.After(time.Millisecond):
		}
	}
	if err := client.Session().Send(ctx, 95, payload, SendOptions{Delivery: ReliableOrdered}); err != nil {
		t.Fatal(err)
	}
	// The running callback retains its payload charge, while two more messages
	// fill the bounded handler queue. A fourth reliable frame must wait only
	// through SlowConsumerTimeout and then close the session.
	wantQueuedBytes = 3 * charge
	for endpoint.applicationBudget.Used() < wantQueuedBytes {
		select {
		case <-ctx.Done():
			t.Fatalf("reliable queue charge = %d, want at least %d", endpoint.applicationBudget.Used(), wantQueuedBytes)
		case <-time.After(time.Millisecond):
		}
	}
	if err := client.Session().Send(ctx, 95, payload, SendOptions{Delivery: ReliableOrdered}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-client.Session().Context().Done():
	case <-ctx.Done():
		t.Fatal("slow consumer did not close the peer session")
	}
	if code := client.(*clientEndpoint).session.terminalCode(); code != SlowConsumer {
		t.Fatalf("client slow-consumer terminal code = %v, want %v", code, SlowConsumer)
	}
	if code := serverSession.terminalCode(); code != SlowConsumer {
		t.Fatalf("server slow-consumer terminal code = %v, want %v", code, SlowConsumer)
	}
	for len(endpoint.Sessions()) != 0 {
		select {
		case <-endpoint.changed:
		case <-ctx.Done():
			t.Fatalf("slow consumer retained %d active sessions", len(endpoint.Sessions()))
		}
	}
	_ = client.Close(context.Background())
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestReliableReaderReservesBeforeReadingBody(t *testing.T) {
	limits := DefaultLimits()
	limits.QueueBytes, limits.QueueMessages = 1024, 1
	operations := &connectionOperations{mode: Polling}
	session := newSessionRecord(SessionID{1}, Owner{}, "", Principal{ExpiresAt: time.Now().Add(time.Minute)}, limits, false, operations)
	operations.session, operations.epoch = session, 1
	operations.incoming = newIncomingQueue(int64(limits.QueueBytes), limits.QueueMessages, nil)
	operations.startEpoch()
	held := &Incoming{Kind: Event, Session: session, Epoch: 1, Type: 1, Payload: make([]byte, 32)}
	if !operations.incoming.Push(held, incomingCharge(held)) {
		t.Fatal("could not fill polling queue")
	}
	channel, err := wire.EncodeReliableChannelHeaderFor(1)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := wire.EncodeReliableEvent(wire.Event{Channel: 1, MessageType: 2, Payload: []byte("body-remains-unread")}, limits.MessageBytes)
	if err != nil {
		t.Fatal(err)
	}
	payloadBytes := len("body-remains-unread")
	stream := newCountedReceiveStream(append(channel, frame...))
	go operations.readReliableStream(stream)
	headerBytes := len(channel) + len(frame) - payloadBytes
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for stream.reads.Load() < int64(headerBytes) {
		select {
		case <-time.After(time.Millisecond):
		case <-deadline.C:
			t.Fatalf("reader did not consume reliable header: %d/%d", stream.reads.Load(), headerBytes)
		}
	}
	if got := stream.reads.Load(); got != int64(headerBytes) {
		t.Fatalf("reader consumed %d body bytes before capacity was available", got-int64(headerBytes))
	}
	item, err := operations.incoming.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	item.Release()
	result, err := operations.incoming.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()
	if result.Type != 2 || string(result.Payload) != "body-remains-unread" {
		t.Fatalf("delivered reliable event = %#v", result)
	}
}

func TestRequestReaderReservesBeforeReadingBody(t *testing.T) {
	limits := DefaultLimits()
	limits.QueueBytes, limits.QueueMessages = 1024, 1
	operations := &connectionOperations{mode: Polling}
	session := newSessionRecord(SessionID{2}, Owner{}, "", Principal{ExpiresAt: time.Now().Add(time.Minute)}, limits, false, operations)
	operations.session, operations.epoch = session, 1
	operations.incoming = newIncomingQueue(int64(limits.QueueBytes), limits.QueueMessages, nil)
	operations.startEpoch()
	held := &Incoming{Kind: Event, Session: session, Epoch: 1, Type: 1, Payload: make([]byte, 32)}
	if !operations.incoming.Push(held, incomingCharge(held)) {
		t.Fatal("could not fill polling queue")
	}
	requestPayload := []byte("request-body-remains-unread")
	frame, err := wire.EncodeRequest(wire.Request{MessageType: 3, TimeoutMS: 1_000, Payload: requestPayload}, limits.MessageBytes)
	if err != nil {
		t.Fatal(err)
	}
	stream := &countedBidiStream{countedReceiveStream: newCountedReceiveStream(frame)}
	go operations.readApplicationStream(stream)
	headerBytes := len(frame) - len(requestPayload)
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for stream.reads.Load() < int64(headerBytes) {
		select {
		case <-time.After(time.Millisecond):
		case <-deadline.C:
			t.Fatalf("reader did not consume request header: %d/%d", stream.reads.Load(), headerBytes)
		}
	}
	if got := stream.reads.Load(); got != int64(headerBytes) {
		t.Fatalf("reader consumed %d request-body bytes before capacity was available", got-int64(headerBytes))
	}
	item, err := operations.incoming.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	item.Release()
	request, err := operations.incoming.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if request.Kind != RequestMessage || request.Type != 3 || string(request.Payload) != string(requestPayload) {
		t.Fatalf("delivered request = %#v", request)
	}
	if err := request.Fail(context.Background(), Canceled, "test complete"); err != nil {
		t.Fatal(err)
	}
	request.Release()
}

func TestMalformedApplicationStreamClosesSession(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: &testAuthenticator{}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	serverSession := endpoint.Sessions()[0].(*sessionRecord)
	concrete := client.(*clientEndpoint)
	stream, err := concrete.connection.OpenBidi(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte{99}); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-serverSession.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("malformed application stream did not close server session")
	}
	if serverSession.terminalCode() != ProtocolViolation {
		t.Fatalf("malformed stream terminal code = %v", serverSession.terminalCode())
	}
	_ = client.Close(context.Background())
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestTruncatedReliableStreamClosesSession(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: &testAuthenticator{}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	serverSession := endpoint.Sessions()[0].(*sessionRecord)
	concrete := client.(*clientEndpoint)
	stream, err := concrete.connection.OpenUni(ctx)
	if err != nil {
		t.Fatal(err)
	}
	header, err := wire.EncodeReliableChannelHeaderFor(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write(header); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-serverSession.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("truncated reliable stream did not close server session")
	}
	if serverSession.terminalCode() != ProtocolViolation {
		t.Fatalf("truncated reliable stream terminal code = %v", serverSession.terminalCode())
	}
	_ = client.Close(context.Background())
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestOutgoingFrameReservationUsesGlobalBudget(t *testing.T) {
	budget := runtime.NewBudget(64)
	operations := &connectionOperations{applicationBudget: budget}
	if _, err := operations.reserveOutgoing(65); err != ErrBackpressure {
		t.Fatalf("oversized outgoing reservation = %v", err)
	}
	release, err := operations.reserveOutgoing(64)
	if err != nil {
		t.Fatal(err)
	}
	if used := budget.Used(); used != 64 {
		t.Fatalf("reserved outgoing bytes = %d", used)
	}
	release()
	release()
	if used := budget.Used(); used != 0 {
		t.Fatalf("outgoing release retained %d bytes", used)
	}
}

func TestControlReadUsesTransportDeadline(t *testing.T) {
	stream := newDeadlineBlockingControlStream()
	deadline := time.Now().Add(time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	_, err := newControlChannel(stream).read(ctx, wire.PreNegotiationControlBytes)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("control read error = %v", err)
	}
	if stream.deadlineCalls != 2 || len(stream.deadlines) != 2 || stream.deadlines[0].IsZero() || !stream.deadlines[1].IsZero() {
		t.Fatalf("control deadlines = %#v", stream.deadlines)
	}
}

func TestControlWriterBoundsQueuedFrames(t *testing.T) {
	stream := newBlockingControlWriteStream()
	channel := newControlChannel(stream)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var exhausted atomic.Int32
	channel.startWriter(ctx, 64, 1, func() { exhausted.Add(1) })
	defer channel.stopWriter()

	first := make(chan error, 1)
	go func() { first <- channel.write(closeAckMessage{Op: "close_ack"}) }()
	select {
	case <-stream.started:
	case <-time.After(time.Second):
		t.Fatal("control writer did not begin the first frame")
	}
	second := make(chan error, 1)
	go func() { second <- channel.write(closeAckMessage{Op: "close_ack"}) }()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		items, _ := channel.queue.Len()
		if items == 1 {
			break
		}
		select {
		case <-time.After(time.Millisecond):
		case <-deadline.C:
			t.Fatal("second control frame was not queued")
		}
	}
	if err := channel.write(closeAckMessage{Op: "close_ack"}); err != ErrBackpressure {
		t.Fatalf("full control queue write = %v", err)
	}
	if exhausted.Load() != 1 {
		t.Fatalf("control exhaustion callback count = %d", exhausted.Load())
	}
	close(stream.unblock)
	for _, result := range []<-chan error{first, second} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("queued control write = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("queued control write remained blocked")
		}
	}
}

func TestFrameReservationsUsePersistentDirectionalBudgets(t *testing.T) {
	limits := DefaultLimits()
	limits.QueueBytes = 64
	session := newSessionRecord(SessionID{3}, Owner{}, "", Principal{}, limits, true, nil)
	global := runtime.NewBudget(128)
	operations := &connectionOperations{session: session, applicationBudget: global}

	releaseOutgoing, err := operations.reserveOutgoing(64)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := operations.reserveOutgoing(1); err != ErrBackpressure {
		t.Fatalf("outgoing directional limit = %v", err)
	}
	releaseIncoming, err := operations.reserveApplicationBytes(64)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := operations.reserveApplicationBytes(1); err != ErrBackpressure {
		t.Fatalf("incoming directional limit = %v", err)
	}
	if used := global.Used(); used != 128 {
		t.Fatalf("combined global charge = %d", used)
	}
	releaseOutgoing()
	releaseIncoming()
	if used := session.outgoingBudget.Used(); used != 0 {
		t.Fatalf("outgoing directional release retained %d bytes", used)
	}
	if used := session.incomingBudget.Used(); used != 0 {
		t.Fatalf("incoming directional release retained %d bytes", used)
	}
	if used := global.Used(); used != 0 {
		t.Fatalf("global release retained %d bytes", used)
	}

	if !global.Acquire(128) {
		t.Fatal("occupy global budget")
	}
	if _, err := operations.reserveApplicationBytes(1); err != ErrBackpressure {
		t.Fatalf("global limit = %v", err)
	}
	if used := session.incomingBudget.Used(); used != 0 {
		t.Fatalf("failed global reservation retained incoming charge: %d", used)
	}
	global.Release(128)
}

func TestReplyReservationSurvivesOrdinaryDirectionalPressure(t *testing.T) {
	limits := DefaultLimits()
	reserve := limits.MessageBytes + 32
	limits.QueueBytes = 2 * reserve
	session := newSessionRecord(SessionID{4}, Owner{}, "", Principal{}, limits, true, nil)
	operations := &connectionOperations{session: session, applicationBudget: runtime.NewBudget(int64(2 * limits.QueueBytes))}

	releaseOrdinary, err := operations.reserveApplicationBytes(reserve)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := operations.reserveApplicationBytes(1); err != ErrBackpressure {
		t.Fatalf("ordinary incoming capacity = %v", err)
	}
	releaseReply, err := operations.reserveReplyApplicationBytes(reserve)
	if err != nil {
		t.Fatalf("reply reservation was unavailable behind ordinary pressure: %v", err)
	}
	if _, err := operations.reserveReplyApplicationBytes(1); err != ErrBackpressure {
		t.Fatalf("reply reservation exceeded directional capacity: %v", err)
	}
	releaseReply()
	releaseOrdinary()
	if used := session.incomingBudget.Used(); used != 0 {
		t.Fatalf("ordinary incoming charge retained %d bytes", used)
	}
	if used := session.incomingReplyBudget.Used(); used != 0 {
		t.Fatalf("reply incoming charge retained %d bytes", used)
	}
}

func waitForSessionState(t *testing.T, ctx context.Context, session Session, state State, minimumEpoch uint64) {
	t.Helper()
	for session.State() != state || session.Epoch() < minimumEpoch {
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			terminal := Normal
			if record, ok := session.(*sessionRecord); ok {
				terminal = record.terminalCode()
			}
			t.Fatalf("session state/epoch/terminal = %v/%d/%v, want %v/>=%d: %v", session.State(), session.Epoch(), terminal, state, minimumEpoch, ctx.Err())
		}
	}
}

func closeCurrentTransport(session *sessionRecord, message string) error {
	session.mu.RLock()
	operations, _ := session.operations.(*connectionOperations)
	session.mu.RUnlock()
	if operations == nil || operations.connection == nil {
		return ErrSessionClosed
	}
	return operations.connection.Close(uint64(Internal), message)
}

func sendResumeAndDiscardWelcome(ctx context.Context, client *clientEndpoint) (net.PacketConn, transport.Conn, error) {
	credential, err := client.config.Credentials(ctx)
	if err != nil {
		return nil, nil, err
	}
	remote, err := net.ResolveUDPAddr("udp", client.endpoint.Address)
	if err != nil {
		return nil, nil, err
	}
	packet, err := net.ListenPacket("udp", "[::]:0")
	if err != nil {
		return nil, nil, err
	}
	fail := func(err error) (net.PacketConn, transport.Conn, error) {
		_ = packet.Close()
		return nil, nil, err
	}
	tlsConfig := client.config.TLS.Clone()
	tlsConfig.ServerName = client.endpoint.ServerName
	connection, err := quictransport.Dial(ctx, packet, remote, transportConfig(tlsConfig, client.limits))
	if err != nil {
		return fail(err)
	}
	stream, err := connection.OpenBidi(ctx)
	if err != nil {
		_ = connection.Close(uint64(Internal), "resume control stream")
		return fail(err)
	}
	id, owner := client.session.ID(), client.session.Owner()
	hello := helloMessage{Op: "hello", Role: roleName(client.config.Role), App: client.config.App, Required: []string{"datagrams"}, Limits: limitMessageFrom(client.limits), Credential: credentialMessage{Scheme: credential.Scheme, Data: base64.RawURLEncoding.EncodeToString(credential.Data)}, Resume: &resumeMessage{SessionID: hex.EncodeToString(id[:]), OwnerID: owner.ID, Incarnation: hex.EncodeToString(owner.Incarnation[:]), Secret: client.resumeSecret}}
	if err := newControlChannel(stream).write(hello); err != nil {
		_ = connection.Close(uint64(Internal), "resume hello failed")
		return fail(err)
	}
	return packet, connection, nil
}

func TestDraining(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: &testAuthenticator{}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	concrete := client.(*clientEndpoint)
	concrete.mu.Lock()
	concrete.reconnecting = true // Keep the suspended session available for the drain test's explicit resume.
	concrete.mu.Unlock()
	serverSession := endpoint.Sessions()[0].(*sessionRecord)
	if err := closeCurrentTransport(serverSession, "suspend before drain"); err != nil {
		t.Fatal(err)
	}
	waitForSessionState(t, ctx, concrete.session, Suspended, 1)
	waitForSessionState(t, ctx, serverSession, Suspended, 1)
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer drainCancel()
	drainDone := make(chan error, 1)
	go func() { drainDone <- server.Drain(drainCtx) }()
	for {
		endpoint.mu.Lock()
		draining := endpoint.draining
		endpoint.mu.Unlock()
		if draining {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("server did not enter draining state")
		case <-time.After(time.Millisecond):
		}
	}
	newAdmissionCtx, cancelNewAdmission := context.WithTimeout(context.Background(), time.Second)
	defer cancelNewAdmission()
	if extra, err := Dial(newAdmissionCtx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}}); err == nil {
		_ = extra.Close(context.Background())
		t.Fatal("new admission unexpectedly succeeded while draining")
	}
	attachment, err := concrete.openResume(ctx)
	if err != nil {
		t.Fatalf("resume during drain = %v", err)
	}
	if err := concrete.attachResume(attachment); err != nil {
		t.Fatalf("attach during drain = %v", err)
	}
	concrete.mu.Lock()
	concrete.reconnecting = false
	concrete.mu.Unlock()
	waitForSessionState(t, ctx, concrete.session, Active, 2)
	waitForSessionState(t, ctx, serverSession, Active, 2)
	select {
	case err := <-drainDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Drain = %v, want deadline exceeded", err)
		}
	case <-ctx.Done():
		t.Fatal("drain did not reach its deadline")
	}
	select {
	case <-client.Session().Context().Done():
	case <-ctx.Done():
		t.Fatal("drain did not close resumed active session")
	}
	emptyCtx, emptyCancel := context.WithTimeout(context.Background(), time.Second)
	defer emptyCancel()
	if err := server.Drain(emptyCtx); err != nil {
		t.Fatalf("empty Drain = %v", err)
	}
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestShutdownCleanup(t *testing.T) {
	const cycles = 1_000
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	limits := DefaultLimits()
	// The stress loop deliberately creates a fresh admission and resume from
	// one local source per cycle; avoid making the source-IP limiter the thing
	// under test here.
	limits.HandshakesPerIPPerSecond = 100_000
	limits.HandshakeBurstPerIP = 100_000
	router := NewRouter()
	if err := router.OnRequest(98, func(ctx context.Context, incoming *Incoming) {
		_ = incoming.Reply(ctx, []byte("ok"))
	}); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: &testAuthenticator{}, Limits: limits, Dispatch: DispatchConfig{Mode: Handlers, Router: router}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			stop()
			<-serveDone
		}
	})
	config := ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}}
	call := func(ctx context.Context, session Session) {
		t.Helper()
		response, err := session.Call(ctx, 98, []byte("request"))
		if err != nil || string(response) != "ok" {
			t.Fatalf("request response = %q, %v", response, err)
		}
	}
	waitForNoSessions := func(ctx context.Context) {
		t.Helper()
		for len(endpoint.Sessions()) != 0 {
			select {
			case <-endpoint.changed:
			case <-ctx.Done():
				t.Fatalf("sessions remained after close: %v", ctx.Err())
			}
		}
	}
	runCycle := func(index int) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, config)
		if err != nil {
			t.Fatalf("cycle %d Dial: %v", index, err)
		}
		concrete := client.(*clientEndpoint)
		call(ctx, client.Session())
		concrete.mu.Lock()
		concrete.reconnecting = true // Drive the resume below without a jitter race.
		concrete.mu.Unlock()
		sessions := endpoint.Sessions()
		if len(sessions) != 1 {
			t.Fatalf("cycle %d server sessions = %d, want 1", index, len(sessions))
		}
		serverSession := sessions[0].(*sessionRecord)
		if err := closeCurrentTransport(serverSession, "shutdown cleanup transport loss"); err != nil {
			t.Fatalf("cycle %d transport loss: %v", index, err)
		}
		waitForSessionState(t, ctx, concrete.session, Suspended, 1)
		waitForSessionState(t, ctx, serverSession, Suspended, 1)
		attachment, err := concrete.openResume(ctx)
		if err != nil {
			t.Fatalf("cycle %d resume: %v", index, err)
		}
		if err := concrete.attachResume(attachment); err != nil {
			t.Fatalf("cycle %d attach resume: %v", index, err)
		}
		concrete.mu.Lock()
		concrete.reconnecting = false
		concrete.mu.Unlock()
		waitForSessionState(t, ctx, concrete.session, Active, 2)
		waitForSessionState(t, ctx, serverSession, Active, 2)
		call(ctx, client.Session())
		if err := client.Close(ctx); err != nil {
			t.Fatalf("cycle %d Close: %v", index, err)
		}
		waitForNoSessions(ctx)
		if used := endpoint.applicationBudget.Used(); used != 0 {
			t.Fatalf("cycle %d server application budget retained %d bytes", index, used)
		}
	}

	// Establish the warmed baseline after a complete cycle, then ensure that
	// 999 more cycles do not retain endpoint work or material heap growth.
	runCycle(0)
	goruntime.GC()
	var baseline goruntime.MemStats
	goruntime.ReadMemStats(&baseline)
	for index := 1; index < cycles; index++ {
		runCycle(index)
	}
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
	stopped = true
	goruntime.GC()
	var after goruntime.MemStats
	goruntime.ReadMemStats(&after)
	if after.HeapAlloc > baseline.HeapAlloc+2<<20 {
		t.Fatalf("heap retained after %d cleanup cycles: baseline=%d after=%d", cycles, baseline.HeapAlloc, after.HeapAlloc)
	}
}

func TestAdmissionAndRateLimits(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	limits := DefaultLimits()
	limits.MessagesPerSessionPerSecond, limits.MessageBurstPerSession = 1, 1
	delivered := make(chan struct{}, 1)
	router := NewRouter()
	if err := router.OnEvent(21, func(context.Context, *Incoming) { delivered <- struct{}{} }); err != nil {
		t.Fatal(err)
	}
	if err := router.OnRequest(22, func(ctx context.Context, incoming *Incoming) { _ = incoming.Reply(ctx, []byte("ok")) }); err != nil {
		t.Fatal(err)
	}
	if err := router.OnStream(23, func(_ context.Context, incoming *Incoming) { _ = incoming.Stream.Close() }); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: &testAuthenticator{}, Limits: limits, Dispatch: DispatchConfig{Mode: Handlers, Router: router}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Session().Send(ctx, 21, []byte("first"), SendOptions{Delivery: ReliableOrdered}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("first event was not delivered")
	}
	if err := client.Session().Send(ctx, 21, []byte("second"), SendOptions{Delivery: ReliableOrdered}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-client.Session().Context().Done():
	case <-time.After(time.Second):
		t.Fatal("rate limit did not close session")
	}
	if client.Session().State() != Closed {
		t.Fatalf("client state = %v", client.Session().State())
	}
	requestClient, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Limits: limits, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	if response, err := requestClient.Session().Call(ctx, 22, []byte("first")); err != nil || string(response) != "ok" {
		t.Fatalf("first request = %q, %v", response, err)
	}
	_, _ = requestClient.Session().Call(ctx, 22, []byte("second"))
	select {
	case <-requestClient.Session().Context().Done():
	case <-time.After(time.Second):
		t.Fatal("request rate limit did not close session")
	}
	streamClient, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Limits: limits, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := streamClient.Session().OpenStream(ctx, 23)
	if err != nil {
		t.Fatalf("first custom stream = %v", err)
	}
	_ = stream.Close()
	_, _ = streamClient.Session().OpenStream(ctx, 23)
	select {
	case <-streamClient.Session().Context().Done():
	case <-time.After(time.Second):
		t.Fatal("custom-stream rate limit did not close session")
	}
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func TestRequestSlots(t *testing.T) {
	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}, 1), make(chan struct{})
	router := NewRouter()
	if err := router.OnRequest(22, func(ctx context.Context, incoming *Incoming) {
		started <- struct{}{}
		<-release
		_ = incoming.Reply(ctx, []byte("ok"))
	}); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{TLS: &tls.Config{Certificates: []tls.Certificate{certificate}}, App: AppIdentity{ID: "app", Version: "1"}, Owner: Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}}, Auth: &testAuthenticator{}, Dispatch: DispatchConfig{Mode: Handlers, Router: router}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveCtx, stop := context.WithCancel(context.Background())
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, packet) }()
	<-endpoint.started
	limits := DefaultLimits()
	limits.Requests = 1
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}, ClientConfig{TLS: &tls.Config{RootCAs: roots}, App: AppIdentity{ID: "app", Version: "1"}, Limits: limits, Credentials: func(context.Context) (Credential, error) {
		return Credential{Scheme: "test", Data: []byte("valid")}, nil
	}, Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()}})
	if err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	go func() { _, err := client.Session().Call(ctx, 22, nil); first <- err }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first request did not reach handler")
	}
	if _, err := client.Session().Call(ctx, 22, nil); err != ErrResourceExhausted {
		t.Fatalf("second call = %v", err)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatalf("first call = %v", err)
	}
	_ = client.Close(context.Background())
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

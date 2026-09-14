package quictransport

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	"testing"
	"time"

	"qattidev/sgsp/internal/transport"
)

func testCertificate(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
		DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		t.Fatal(err)
	}
	certificate := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: private}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(parsed)
	return certificate, pool
}

func testConfig(t *testing.T, server bool, roots *x509.CertPool, certificate tls.Certificate) Config {
	t.Helper()
	tlsConfig := &tls.Config{ServerName: "localhost", RootCAs: roots}
	if server {
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}
	return Config{TLS: tlsConfig, HandshakeTimeout: time.Second, IdleTimeout: 5 * time.Second, KeepAlive: time.Second, StreamReceiveWindow: 64 << 10, ConnectionReceiveWindow: 8 << 20, MaxIncomingBidi: 4, MaxIncomingUni: 2, EnableDatagrams: true}
}

func connectedPair(t *testing.T) (context.Context, transport.Conn, transport.Conn, func()) {
	return connectedPairWithBidiLimit(t, 4)
}

func connectedPairWithBidiLimit(t *testing.T, maxBidi int64) (context.Context, transport.Conn, transport.Conn, func()) {
	t.Helper()
	certificate, roots := testCertificate(t)
	serverPacket, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverConfig := testConfig(t, true, nil, certificate)
	serverConfig.MaxIncomingBidi = maxBidi
	listener, err := Listen(serverPacket, serverConfig)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	accepted := make(chan struct {
		connection transport.Conn
		err        error
	}, 1)
	go func() {
		connection, err := listener.Accept(ctx)
		accepted <- struct {
			connection transport.Conn
			err        error
		}{connection, err}
	}()
	clientPacket, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	client, err := Dial(ctx, clientPacket, listener.Addr(), testConfig(t, false, roots, tls.Certificate{}))
	if err != nil {
		t.Fatal(err)
	}
	result := <-accepted
	if result.err != nil {
		t.Fatal(result.err)
	}
	cleanup := func() {
		_ = client.Close(0, "test done")
		_ = result.connection.Close(0, "test done")
		_ = listener.Close()
		_ = clientPacket.Close()
		_ = serverPacket.Close()
		cancel()
	}
	return ctx, client, result.connection, cleanup
}

func TestQUICChannels(t *testing.T) {
	ctx, client, server, cleanup := connectedPair(t)
	defer cleanup()
	if !client.Stats().DatagramsEnabled || !server.Stats().DatagramsEnabled {
		t.Fatal("datagram capability was not negotiated")
	}

	stream, err := client.OpenBidi(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	peer, err := server.AcceptBidi(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(peer)
	if err != nil || string(got) != "hi" {
		t.Fatalf("bidi received %q, %v", got, err)
	}
	if _, err := peer.Write([]byte("ok")); err != nil {
		t.Fatal(err)
	}
	if err := peer.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	got, err = io.ReadAll(stream)
	if err != nil || string(got) != "ok" {
		t.Fatalf("bidi reply %q, %v", got, err)
	}
	if stats := client.Stats(); stats.BytesSent == 0 || stats.BytesReceived == 0 {
		t.Fatalf("client byte stats = %#v", stats)
	}
	if stats := server.Stats(); stats.BytesSent == 0 || stats.BytesReceived == 0 {
		t.Fatalf("server byte stats = %#v", stats)
	}

	uni, err := client.OpenUni(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := uni.Write([]byte("uni")); err != nil {
		t.Fatal(err)
	}
	if err := uni.Close(); err != nil {
		t.Fatal(err)
	}
	receivedUni, err := server.AcceptUni(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got, err = io.ReadAll(receivedUni)
	if err != nil || string(got) != "uni" {
		t.Fatalf("uni received %q, %v", got, err)
	}

	if err := client.SendDatagram(ctx, []byte("input")); err != nil {
		t.Fatal(err)
	}
	datagram, err := server.ReceiveDatagram(ctx)
	if err != nil || string(datagram.Payload) != "input" {
		t.Fatalf("datagram %q, %v", datagram.Payload, err)
	}
}

func TestStreamCancellation(t *testing.T) {
	ctx, client, server, cleanup := connectedPairWithBidiLimit(t, 1)
	defer cleanup()
	stream, err := client.OpenBidi(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte{'x'}); err != nil {
		t.Fatal(err)
	}
	peer, err := server.AcceptBidi(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var first [1]byte
	if _, err := io.ReadFull(peer, first[:]); err != nil {
		t.Fatal(err)
	}
	readDone := make(chan error, 1)
	go func() { _, err := peer.Read(make([]byte, 1)); readDone <- err }()
	stream.Abort(42)
	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("blocked read unexpectedly succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("stream reset did not unblock reader")
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := server.AcceptBidi(canceled); err == nil {
		t.Fatal("canceled accept succeeded")
	}
}

func TestTLSVerificationAndConfiguration(t *testing.T) {
	certificate, _ := testCertificate(t)
	if _, _, err := (Config{TLS: &tls.Config{InsecureSkipVerify: true}, StreamReceiveWindow: 1, ConnectionReceiveWindow: 1}).quicConfig(); err == nil {
		t.Fatal("insecure TLS was accepted")
	}
	if _, _, err := (Config{TLS: &tls.Config{MinVersion: tls.VersionTLS12}, StreamReceiveWindow: 1, ConnectionReceiveWindow: 1}).quicConfig(); err == nil {
		t.Fatal("TLS 1.2 was accepted")
	}
	configuredTLS, configuredQUIC, err := testConfig(t, false, x509.NewCertPool(), tls.Certificate{}).quicConfig()
	if err != nil {
		t.Fatal(err)
	}
	if configuredTLS.MinVersion != tls.VersionTLS13 || configuredTLS.MaxVersion != tls.VersionTLS13 || len(configuredTLS.NextProtos) != 1 || configuredTLS.NextProtos[0] != ALPN {
		t.Fatal("TLS/ALPN policy was not enforced")
	}
	if configuredQUIC.Allow0RTT || !configuredQUIC.EnableDatagrams {
		t.Fatal("QUIC early-data or datagram policy was not enforced")
	}
	serverPacket, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := Listen(serverPacket, testConfig(t, true, nil, certificate))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close(); _ = serverPacket.Close() }()
	clientPacket, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer clientPacket.Close()
	wrongRoots := x509.NewCertPool()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := Dial(ctx, clientPacket, listener.Addr(), testConfig(t, false, wrongRoots, tls.Certificate{})); err == nil {
		t.Fatal("connection with an untrusted server certificate succeeded")
	}
}

func TestCapabilityNegotiation(t *testing.T) {
	certificate, roots := testCertificate(t)
	serverPacket, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listenerConfig := testConfig(t, true, nil, certificate)
	listenerConfig.EnableDatagrams = false
	listener, err := Listen(serverPacket, listenerConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close(); _ = serverPacket.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	accepted := make(chan transport.Conn, 1)
	go func() {
		connection, acceptErr := listener.Accept(ctx)
		if acceptErr == nil {
			accepted <- connection
		}
	}()
	clientPacket, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer clientPacket.Close()
	client, err := Dial(ctx, clientPacket, listener.Addr(), testConfig(t, false, roots, tls.Certificate{}))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(0, "test done")
	server := <-accepted
	defer server.Close(0, "test done")
	if client.Stats().DatagramsEnabled || server.Stats().DatagramsEnabled {
		t.Fatal("datagrams were reported despite peer capability being disabled")
	}
	if err := client.SendDatagram(ctx, []byte("must not become reliable")); err == nil {
		t.Fatal("datagram send succeeded without negotiated capability")
	}
}

func TestConfiguredStreamCredit(t *testing.T) {
	ctx, client, server, cleanup := connectedPairWithBidiLimit(t, 1)
	defer cleanup()
	_ = server
	first, err := client.OpenBidi(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Abort(0)
	limitedCtx, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
	defer cancel()
	if _, err := client.OpenBidi(limitedCtx); err == nil {
		t.Fatal("opened stream past advertised limit")
	}
}

func TestStreamDeadline(t *testing.T) {
	ctx, client, server, cleanup := connectedPair(t)
	defer cleanup()
	stream, err := client.OpenBidi(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	peer, err := server.AcceptBidi(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var first [1]byte
	if _, err := io.ReadFull(peer, first[:]); err != nil {
		t.Fatal(err)
	}
	if err := peer.SetDeadline(time.Now().Add(15 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.Read(make([]byte, 1)); err == nil {
		t.Fatal("read without payload ignored its deadline")
	}
}

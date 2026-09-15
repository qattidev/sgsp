// Package quictransport adapts quic-go to SGSP's private transport boundary.
// No quic-go type escapes this package.
package quictransport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"time"

	quic "github.com/quic-go/quic-go"

	"qattidev/sgsp/internal/transport"
)

const ALPN = "sgsp/1"

var ErrInvalidTLS = errors.New("sgsp quic transport: invalid TLS configuration")

// Config supplies transport-only limits. SGSP validates the broader endpoint
// configuration before constructing this adapter.
type Config struct {
	TLS                     *tls.Config
	HandshakeTimeout        time.Duration
	IdleTimeout             time.Duration
	KeepAlive               time.Duration
	StreamReceiveWindow     uint64
	ConnectionReceiveWindow uint64
	MaxIncomingBidi         int64
	MaxIncomingUni          int64
	EnableDatagrams         bool
}

func (c Config) quicConfig() (*tls.Config, *quic.Config, error) {
	if c.TLS == nil || c.TLS.InsecureSkipVerify {
		return nil, nil, ErrInvalidTLS
	}
	tlsConfig := c.TLS.Clone()
	if tlsConfig.MinVersion > 0 && tlsConfig.MinVersion < tls.VersionTLS13 || tlsConfig.MaxVersion > 0 && tlsConfig.MaxVersion < tls.VersionTLS13 {
		return nil, nil, fmt.Errorf("%w: TLS 1.3 required", ErrInvalidTLS)
	}
	tlsConfig.MinVersion = tls.VersionTLS13
	tlsConfig.MaxVersion = tls.VersionTLS13
	tlsConfig.NextProtos = []string{ALPN}
	if c.StreamReceiveWindow == 0 || c.ConnectionReceiveWindow == 0 || c.MaxIncomingBidi < 0 || c.MaxIncomingUni < 0 {
		return nil, nil, fmt.Errorf("%w: invalid transport limit", ErrInvalidTLS)
	}
	return tlsConfig, &quic.Config{
		HandshakeIdleTimeout:           c.HandshakeTimeout,
		MaxIdleTimeout:                 c.IdleTimeout,
		KeepAlivePeriod:                c.KeepAlive,
		InitialStreamReceiveWindow:     c.StreamReceiveWindow,
		MaxStreamReceiveWindow:         c.StreamReceiveWindow,
		InitialConnectionReceiveWindow: c.ConnectionReceiveWindow,
		MaxConnectionReceiveWindow:     c.ConnectionReceiveWindow,
		MaxIncomingStreams:             c.MaxIncomingBidi,
		MaxIncomingUniStreams:          c.MaxIncomingUni,
		EnableDatagrams:                c.EnableDatagrams,
		Allow0RTT:                      false,
		AllowConnectionWindowIncrease:  func(*quic.Conn, uint64) bool { return false },
	}, nil
}

// Listen binds QUIC to packetConn. Closing the returned Listener closes the
// underlying QUIC listener but leaves packetConn ownership with the caller.
func Listen(packetConn net.PacketConn, config Config) (*Listener, error) {
	tlsConfig, quicConfig, err := config.quicConfig()
	if err != nil {
		return nil, err
	}
	listener, err := quic.Listen(packetConn, tlsConfig, quicConfig)
	if err != nil {
		return nil, err
	}
	return &Listener{listener: listener}, nil
}

type Listener struct{ listener *quic.Listener }

func (l *Listener) Accept(ctx context.Context) (transport.Conn, error) {
	raw, err := l.listener.Accept(ctx)
	if err != nil {
		return nil, err
	}
	return &conn{conn: raw}, nil
}
func (l *Listener) Close() error   { return l.listener.Close() }
func (l *Listener) Addr() net.Addr { return l.listener.Addr() }

// Dial opens one normal (never early-data) QUIC connection over packetConn.
func Dial(ctx context.Context, packetConn net.PacketConn, remote net.Addr, config Config) (transport.Conn, error) {
	tlsConfig, quicConfig, err := config.quicConfig()
	if err != nil {
		return nil, err
	}
	raw, err := quic.Dial(ctx, packetConn, remote, tlsConfig, quicConfig)
	if err != nil {
		return nil, err
	}
	return &conn{conn: raw}, nil
}

type conn struct{ conn *quic.Conn }

func (c *conn) Context() context.Context { return c.conn.Context() }
func (c *conn) OpenBidi(ctx context.Context) (transport.BidiStream, error) {
	stream, err := c.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return bidi{stream}, nil
}
func (c *conn) AcceptBidi(ctx context.Context) (transport.BidiStream, error) {
	stream, err := c.conn.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}
	return bidi{stream}, nil
}
func (c *conn) OpenUni(ctx context.Context) (transport.SendStream, error) {
	stream, err := c.conn.OpenUniStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return send{stream}, nil
}
func (c *conn) AcceptUni(ctx context.Context) (transport.ReceiveStream, error) {
	stream, err := c.conn.AcceptUniStream(ctx)
	if err != nil {
		return nil, err
	}
	return receive{stream}, nil
}
func (c *conn) SendDatagram(ctx context.Context, payload []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return c.conn.SendDatagram(payload)
}
func (c *conn) ReceiveDatagram(ctx context.Context) (transport.Datagram, error) {
	payload, err := c.conn.ReceiveDatagram(ctx)
	if err != nil {
		return transport.Datagram{}, err
	}
	return transport.Datagram{Payload: append([]byte(nil), payload...)}, nil
}
func (c *conn) Close(code uint64, message string) error {
	return c.conn.CloseWithError(quic.ApplicationErrorCode(code), message)
}
func (c *conn) CloseCode() (uint64, bool) {
	var application *quic.ApplicationError
	if errors.As(context.Cause(c.conn.Context()), &application) {
		return uint64(application.ErrorCode), true
	}
	return 0, false
}
func (c *conn) LocalAddr() net.Addr  { return c.conn.LocalAddr() }
func (c *conn) RemoteAddr() net.Addr { return c.conn.RemoteAddr() }
func (c *conn) Stats() transport.Stats {
	state := c.conn.ConnectionState()
	stats := c.conn.ConnectionStats()
	return transport.Stats{RTT: stats.SmoothedRTT, DatagramsEnabled: state.SupportsDatagrams.Local && state.SupportsDatagrams.Remote, BytesSent: stats.BytesSent, BytesReceived: stats.BytesReceived, PacketsSent: stats.PacketsSent, PacketsReceived: stats.PacketsReceived, PacketsLost: stats.PacketsLost}
}

type bidi struct{ *quic.Stream }

func (s bidi) CloseWrite() error { return s.Stream.Close() }
func (s bidi) CloseRead()        { s.Stream.CancelRead(0) }
func (s bidi) Abort(code uint64) {
	s.Stream.CancelRead(quic.StreamErrorCode(code))
	s.Stream.CancelWrite(quic.StreamErrorCode(code))
}

type send struct{ *quic.SendStream }

func (s send) Abort(code uint64) { s.SendStream.CancelWrite(quic.StreamErrorCode(code)) }

type receive struct{ *quic.ReceiveStream }

func (s receive) CloseRead() { s.ReceiveStream.CancelRead(0) }

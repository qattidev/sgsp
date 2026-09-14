// Package transport defines the private adapter boundary used by SGSP. Public
// packages never expose a concrete QUIC implementation through this boundary.
package transport

import (
	"context"
	"io"
	"net"
	"time"
)

// Datagram is an adapter-neutral received unreliable payload.
type Datagram struct{ Payload []byte }

type Stats struct {
	RTT              time.Duration
	DatagramsEnabled bool
	BytesSent        uint64
	BytesReceived    uint64
}

type BidiStream interface {
	io.Reader
	io.Writer
	Close() error
	CloseWrite() error
	CloseRead()
	Abort(uint64)
	SetDeadline(time.Time) error
}

type SendStream interface {
	io.Writer
	Close() error
	Abort(uint64)
	SetWriteDeadline(time.Time) error
}

type ReceiveStream interface {
	io.Reader
	CloseRead()
	SetReadDeadline(time.Time) error
}

type Conn interface {
	Context() context.Context
	OpenBidi(context.Context) (BidiStream, error)
	AcceptBidi(context.Context) (BidiStream, error)
	OpenUni(context.Context) (SendStream, error)
	AcceptUni(context.Context) (ReceiveStream, error)
	SendDatagram(context.Context, []byte) error
	ReceiveDatagram(context.Context) (Datagram, error)
	Close(code uint64, message string) error
	CloseCode() (uint64, bool)
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	Stats() Stats
}

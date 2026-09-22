// Package sgsp provides the public API for the Simple Game Server Protocol.
//
// This M0 package deliberately establishes API ownership only. Networking,
// framing, authentication, and dispatch are implemented in later milestones.
package sgsp

import (
	"context"
	"io"
	"time"
)

type SessionID [16]byte
type Incarnation [16]byte
type MessageType uint32
type ChannelID uint16
type Code uint32
type Delivery uint8
type State uint8
type DispatchMode uint8
type Role uint8

const (
	GameRole Role = iota
	BootstrapRole
)

const (
	ReliableOrdered Delivery = iota
	Unreliable
	UnreliableSequenced
)

const (
	Connecting State = iota + 1
	Authenticating
	Active
	Suspended
	Closed
)

const (
	Handlers DispatchMode = iota
	Polling
)

type AppIdentity struct{ ID, Version string }
type Endpoint struct{ Address, ServerName string }
type Owner struct {
	ID          string
	Incarnation Incarnation
	Endpoint    Endpoint
}
type Credential struct {
	Scheme string
	Data   []byte
}
type Principal struct {
	Issuer, Subject string
	ExpiresAt       time.Time
	Attributes      map[string]string
}
type Authenticator interface {
	Authenticate(context.Context, Credential) (Principal, error)
}
type CredentialProvider func(context.Context) (Credential, error)
type GroupAuthorizer func(context.Context, Principal, string) error
type Admission struct {
	App                                AppIdentity
	PrincipalIssuer, Subject, GroupKey string
	Owner                              Owner
	ExpiresAt                          time.Time
}
type AdmissionVerifier interface {
	Verify(context.Context, string) (Admission, error)
}

type SendOptions struct {
	Channel  ChannelID
	Delivery Delivery
}

type Session interface {
	ID() SessionID
	Owner() Owner
	GroupKey() string
	State() State
	Epoch() uint64
	Principal() Principal
	Context() context.Context
	Attachment() any
	SetAttachment(any)
	Send(context.Context, MessageType, []byte, SendOptions) error
	TrySend(MessageType, []byte, SendOptions) error
	Call(context.Context, MessageType, []byte) ([]byte, error)
	OpenStream(context.Context, MessageType) (Stream, error)
	RefreshAuth(context.Context, Credential) error
	Close(context.Context, Code) error
	Stats() Stats
}

type Stream interface {
	io.Reader
	io.Writer
	io.Closer
	CloseWrite() error
	CloseRead() error
	SetDeadline(time.Time) error
	Abort(Code)
}

type Stats struct {
	RTT                                                                   time.Duration
	TransportStatsAvailable                                               bool
	BytesSent, BytesReceived                                              uint64
	SendQueuedBytes, ReceiveQueuedBytes                                   int64
	LocalDatagramsDropped, CoalescedDatagramsDropped, StaleUpdatesDropped uint64
}

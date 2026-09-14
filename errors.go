package sgsp

import "fmt"

const (
	Normal Code = iota
	ProtocolViolation
	UnsupportedVersion
	UnsupportedCapability
	Unauthenticated
	Forbidden
	AuthExpired
	SessionNotFound
	SessionExpired
	ServerUnavailable
	ServerDraining
	Backpressure
	TooLarge
	UnsupportedMessage
	Canceled
	DeadlineExceeded
	Internal
	SessionSuperseded
	SlowConsumer
	GroupClosed
	ResourceExhausted
	SessionClosed
	SessionSuspended
	OutcomeUnknown
	InvalidArgument
	WrongMode
)

// Error is the protocol-shaped error returned by the public API.
// Error mapping behavior is completed in M1.
type Error struct {
	Code           Code
	Message        string
	OutcomeUnknown bool
	MaxPayload     int
	Cause          error
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Message != "" {
		return e.Message
	}
	return fmt.Sprintf("sgsp: %s", codeName(e.Code))
}

func (e *Error) Unwrap() error { return e.Cause }

func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && e != nil && e.Code != Normal && e.Code == t.Code
}

func codeError(code Code) *Error { return &Error{Code: code, Message: "sgsp: " + codeName(code)} }

var (
	ErrProtocolViolation     = codeError(ProtocolViolation)
	ErrUnsupportedVersion    = codeError(UnsupportedVersion)
	ErrUnsupportedCapability = codeError(UnsupportedCapability)
	ErrUnauthenticated       = codeError(Unauthenticated)
	ErrForbidden             = codeError(Forbidden)
	ErrAuthExpired           = codeError(AuthExpired)
	ErrSessionNotFound       = codeError(SessionNotFound)
	ErrSessionExpired        = codeError(SessionExpired)
	ErrServerUnavailable     = codeError(ServerUnavailable)
	ErrServerDraining        = codeError(ServerDraining)
	ErrBackpressure          = codeError(Backpressure)
	ErrTooLarge              = codeError(TooLarge)
	ErrUnsupportedMessage    = codeError(UnsupportedMessage)
	ErrCanceled              = codeError(Canceled)
	ErrDeadlineExceeded      = codeError(DeadlineExceeded)
	ErrInternal              = codeError(Internal)
	ErrSessionSuperseded     = codeError(SessionSuperseded)
	ErrSlowConsumer          = codeError(SlowConsumer)
	ErrGroupClosed           = codeError(GroupClosed)
	ErrResourceExhausted     = codeError(ResourceExhausted)
	ErrSessionClosed         = codeError(SessionClosed)
	ErrSessionSuspended      = codeError(SessionSuspended)
	ErrOutcomeUnknown        = codeError(OutcomeUnknown)
	ErrInvalidArgument       = codeError(InvalidArgument)
	ErrWrongMode             = codeError(WrongMode)
)

func codeName(code Code) string {
	if names, ok := map[Code]string{
		ProtocolViolation: "protocol violation", UnsupportedVersion: "unsupported version",
		UnsupportedCapability: "unsupported capability", Unauthenticated: "unauthenticated",
		Forbidden: "forbidden", AuthExpired: "authentication expired", SessionNotFound: "session not found",
		SessionExpired: "session expired", ServerUnavailable: "server unavailable", ServerDraining: "server draining",
		Backpressure: "backpressure", TooLarge: "too large", UnsupportedMessage: "unsupported message",
		Canceled: "canceled", DeadlineExceeded: "deadline exceeded", Internal: "internal",
		SessionSuperseded: "session superseded", SlowConsumer: "slow consumer", GroupClosed: "group closed",
		ResourceExhausted: "resource exhausted", SessionClosed: "session closed", SessionSuspended: "session suspended",
		OutcomeUnknown: "outcome unknown", InvalidArgument: "invalid argument", WrongMode: "wrong mode",
	}[code]; ok {
		return names
	}
	return "unknown error"
}

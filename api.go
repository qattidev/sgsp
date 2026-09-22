package sgsp

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"time"
)

type Limits struct {
	ControlBytes, MessageBytes, DatagramBytes             int
	QueueBytes, QueueMessages                             int
	ReliableChannels, DatagramChannels, Requests, Streams int
	MaxSessions, MaxPendingHandshakes                     int
	GlobalApplicationBytes                                int64
	AuthTimeout, KeepAlive, IdleTimeout, CloseTimeout     time.Duration
	ResumeGrace, SlowConsumerTimeout, RequestTimeout      time.Duration
	ReconnectMin, ReconnectMax                            time.Duration
	ControlQueueBytes, ControlQueueMessages               int
	StreamReceiveWindow, ConnectionReceiveWindow          uint64
	PreAuthBytes, MaxActiveGroups, MaxGroupKeyBytes       int
	HandshakesPerIPPerSecond, HandshakeBurstPerIP         int
	MessagesPerSessionPerSecond, MessageBurstPerSession   int
}

func DefaultLimits() Limits {
	return Limits{
		ControlBytes: 16 << 10, MessageBytes: 64 << 10, DatagramBytes: 1000,
		QueueBytes: 256 << 10, QueueMessages: 256, ReliableChannels: 16, DatagramChannels: 64,
		Requests: 32, Streams: 8, MaxSessions: 128, MaxPendingHandshakes: 64,
		GlobalApplicationBytes: 256 << 20, AuthTimeout: 5 * time.Second, KeepAlive: time.Second,
		IdleTimeout: 5 * time.Second, CloseTimeout: time.Second, ResumeGrace: 30 * time.Second,
		SlowConsumerTimeout: 5 * time.Second, RequestTimeout: 5 * time.Second,
		ReconnectMin: 250 * time.Millisecond, ReconnectMax: 2 * time.Second,
		ControlQueueBytes: 64 << 10, ControlQueueMessages: 16, StreamReceiveWindow: 64 << 10,
		ConnectionReceiveWindow: 8 << 20, PreAuthBytes: 32 << 10, MaxActiveGroups: 10_000,
		MaxGroupKeyBytes: 256, HandshakesPerIPPerSecond: 20, HandshakeBurstPerIP: 40,
		MessagesPerSessionPerSecond: 2_000, MessageBurstPerSession: 4_000,
	}
}

// normalizeLimits applies the all-defaults rule and rejects partial or
// internally impossible budgets before an endpoint allocates any resources.
func normalizeLimits(limits Limits) (Limits, error) {
	if limits == (Limits{}) {
		return DefaultLimits(), nil
	}
	ints := []int{limits.ControlBytes, limits.MessageBytes, limits.DatagramBytes, limits.QueueBytes, limits.QueueMessages, limits.ReliableChannels, limits.DatagramChannels, limits.Requests, limits.Streams, limits.MaxSessions, limits.MaxPendingHandshakes, limits.ControlQueueBytes, limits.ControlQueueMessages, limits.PreAuthBytes, limits.MaxActiveGroups, limits.MaxGroupKeyBytes, limits.HandshakesPerIPPerSecond, limits.HandshakeBurstPerIP, limits.MessagesPerSessionPerSecond, limits.MessageBurstPerSession}
	for _, value := range ints {
		if value <= 0 {
			return Limits{}, ErrInvalidArgument
		}
	}
	durations := []time.Duration{limits.AuthTimeout, limits.KeepAlive, limits.IdleTimeout, limits.CloseTimeout, limits.ResumeGrace, limits.SlowConsumerTimeout, limits.RequestTimeout, limits.ReconnectMin, limits.ReconnectMax}
	for _, value := range durations {
		if value <= 0 {
			return Limits{}, ErrInvalidArgument
		}
	}
	if limits.GlobalApplicationBytes <= 0 || limits.StreamReceiveWindow == 0 || limits.ConnectionReceiveWindow == 0 || limits.ReconnectMin > limits.ReconnectMax {
		return Limits{}, ErrInvalidArgument
	}
	maxInt := int(^uint(0) >> 1)
	if limits.QueueMessages < 2 || limits.MessageBytes > maxInt-32 || limits.QueueBytes < 2*(limits.MessageBytes+32) {
		return Limits{}, ErrInvalidArgument
	}
	streams := uint64(limits.ReliableChannels) + 2*(uint64(limits.Requests)+uint64(limits.Streams)) + 1
	if streams > ^uint64(0)/limits.StreamReceiveWindow || streams*limits.StreamReceiveWindow > limits.ConnectionReceiveWindow {
		return Limits{}, ErrInvalidArgument
	}
	return limits, nil
}

type IncomingKind uint8

const (
	Event IncomingKind = iota + 1
	RequestMessage
	StreamMessage
	LifecycleMessage
)

type LifecycleKind uint8

const (
	Opened LifecycleKind = iota + 1
	ConnectionLost
	Resumed
	SessionEnded
	AuthRefreshed
)

type Lifecycle struct {
	Kind   LifecycleKind
	Epoch  uint64
	Reason Code
}
type Incoming struct {
	Kind      IncomingKind
	Session   Session
	Epoch     uint64
	Type      MessageType
	Channel   ChannelID
	Delivery  Delivery
	Sequence  uint64
	Payload   []byte
	Stream    Stream
	Lifecycle *Lifecycle

	ctx        context.Context
	enqueuedAt time.Time
	decodedAt  time.Time
	release    func()
	after      func()
	reply      func(context.Context, []byte) error
	fail       func(context.Context, Code, string) error
	released   sync.Once
	responseMu sync.Mutex
	responded  bool
}

func (i *Incoming) Context() context.Context {
	if i != nil && i.ctx != nil {
		return i.ctx
	}
	return context.Background()
}

// DecodedAt reports the local monotonic timestamp captured after SGSP has
// completely decoded an incoming frame and before it enters dispatch. It is
// zero for envelopes constructed without a transport frame, such as lifecycle
// notifications. Applications normally do not need it; it permits benchmark
// and profiling code to measure queue/dispatch latency without inferring a
// one-way transport delay from wall clocks.
func (i *Incoming) DecodedAt() time.Time {
	if i == nil {
		return time.Time{}
	}
	return i.decodedAt
}
func (i *Incoming) Reply(ctx context.Context, payload []byte) error {
	if i == nil || i.Kind != RequestMessage || i.reply == nil {
		return ErrInvalidArgument
	}
	return i.respond(func() error { return i.reply(ctx, append([]byte(nil), payload...)) })
}
func (i *Incoming) Fail(ctx context.Context, code Code, message string) error {
	if i == nil || i.Kind != RequestMessage || i.fail == nil || code == Normal {
		return ErrInvalidArgument
	}
	return i.respond(func() error { return i.fail(ctx, code, message) })
}
func (i *Incoming) respond(call func() error) error {
	i.responseMu.Lock()
	defer i.responseMu.Unlock()
	if i.responded {
		return ErrSessionClosed
	}
	if err := call(); err != nil {
		return err
	}
	i.responded = true
	return nil
}
func (i *Incoming) hasResponded() bool {
	if i == nil {
		return false
	}
	i.responseMu.Lock()
	defer i.responseMu.Unlock()
	return i.responded
}
func (i *Incoming) Release() {
	if i == nil {
		return
	}
	i.released.Do(func() {
		i.Payload = nil
		if i.release != nil {
			i.release()
		}
	})
}

type Handler func(context.Context, *Incoming)
type routeKey struct {
	kind IncomingKind
	typ  MessageType
}
type Router struct {
	mu        sync.RWMutex
	routes    map[routeKey]Handler
	unknown   Handler
	lifecycle Handler
}

func NewRouter() *Router { return &Router{routes: make(map[routeKey]Handler)} }
func (r *Router) OnEvent(typ MessageType, handler Handler) error {
	return r.register(Event, typ, handler)
}
func (r *Router) OnRequest(typ MessageType, handler Handler) error {
	return r.register(RequestMessage, typ, handler)
}
func (r *Router) OnStream(typ MessageType, handler Handler) error {
	return r.register(StreamMessage, typ, handler)
}
func (r *Router) OnUnknown(handler Handler) error {
	if r == nil || handler == nil {
		return ErrInvalidArgument
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.unknown != nil {
		return ErrInvalidArgument
	}
	r.unknown = handler
	return nil
}
func (r *Router) OnLifecycle(handler Handler) error {
	if r == nil || handler == nil {
		return ErrInvalidArgument
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lifecycle != nil {
		return ErrInvalidArgument
	}
	r.lifecycle = handler
	return nil
}
func (r *Router) register(kind IncomingKind, typ MessageType, handler Handler) error {
	if r == nil || typ == 0 || handler == nil {
		return ErrInvalidArgument
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := routeKey{kind, typ}
	if _, exists := r.routes[key]; exists {
		return ErrInvalidArgument
	}
	r.routes[key] = handler
	return nil
}
func (r *Router) handler(in *Incoming) Handler {
	if r == nil || in == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if in.Kind == LifecycleMessage {
		return r.lifecycle
	}
	if h := r.routes[routeKey{in.Kind, in.Type}]; h != nil {
		return h
	}
	return r.unknown
}

// snapshot makes a configuration-owned, immutable registration copy. It is
// used by endpoint construction so later calls on the application's Router
// cannot race or silently change a running endpoint's dispatch behavior.
func (r *Router) snapshot() *Router {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	copyRoutes := make(map[routeKey]Handler, len(r.routes))
	for key, handler := range r.routes {
		copyRoutes[key] = handler
	}
	return &Router{routes: copyRoutes, unknown: r.unknown, lifecycle: r.lifecycle}
}

type Observation struct {
	Name, Kind string
	Code       Code
	Value      float64
}
type Observer interface{ Observe(Observation) }
type DispatchConfig struct {
	Mode   DispatchMode
	Router *Router
}
type ServerConfig struct {
	Role             Role
	TLS              *tls.Config
	App              AppIdentity
	Owner            Owner
	Auth             Authenticator
	AuthorizeGroup   GroupAuthorizer
	Admission        AdmissionVerifier
	RequireAdmission bool
	CommitGroupClose func(context.Context, AppIdentity, string, Owner) error
	Dispatch         DispatchConfig
	Limits           Limits
	Observer         Observer
	// clock is an internal deterministic-test seam. Public callers always use
	// the system clock; keeping it unexported prevents a production clock from
	// becoming part of the API contract.
	clock endpointClock
}
type ClientConfig struct {
	Role                      Role
	TLS                       *tls.Config
	App                       AppIdentity
	Credentials               CredentialProvider
	ExpectedOwner             *Owner
	AdmissionTicket, GroupKey string
	Dispatch                  DispatchConfig
	Limits                    Limits
	Observer                  Observer
	DisableReconnect          bool
}
type Client interface {
	Session() Session
	Next(context.Context) (*Incoming, error)
	Close(context.Context) error
}
type Server interface {
	Owner() Owner
	Serve(context.Context, net.PacketConn) error
	Next(context.Context) (*Incoming, error)
	Sessions() []Session
	Revoke(context.Context, string, string) error
	CloseGroup(context.Context, string) error
	Drain(context.Context) error
}

// NewServer is intentionally unavailable until the endpoint implementation lands.

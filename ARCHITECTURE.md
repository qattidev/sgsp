# SGSP v1 — architecture and implementation contract

Status: specification awaiting implementation. No implementation test, race check, fuzz campaign, or performance benchmark is claimed as passed by this document.

Read [DESIGN.md](DESIGN.md) for intent. This file defines the technical contract, implementation order, and evidence required to complete the library. If the two documents disagree about a technical detail, this file is authoritative and the design document MUST be corrected in the same change.

## 1. Instructions for the implementing model

MUST means required for v1 conformance. SHOULD permits a documented deviation supported by evidence. MAY denotes an optional extension. Implement the numbered milestones in section 14 in order. Complete each gate before advancing; a compiling skeleton or skipped integration test is not a passed gate.

Do not implement game simulation, account registration, automatic failover, or a second transport. Do not replace QUIC with reliable TCP while claiming unreliable support. Do not retry application requests automatically. Use the deterministic test harness before adding network fault tests.

When reporting a milestone, name the changed behavior, list the actual commands run and their exit results, identify skipped/unavailable checks, and link the evidence. Fix contradictions in this specification explicitly instead of making an undocumented wire or lifecycle change.

### 1.1 Workspace and dependencies

The documentation directory is the intended future library root: `/home/sarpojoh/Projects/sgsp`. At documentation creation, `/home/sarpojoh/Projects/go.mod` declares `qattidev/sgsp` with Go `1.27.1`, and the library directory has no module or source code.

Milestone M0 MUST create `sgsp/go.mod` declaring `qattidev/sgsp` and Go `1.27.1`. Run all subsequent Go commands from that directory. Do not move or delete the parent module as part of library implementation. This explicit nested module prevents accidentally including unrelated workspace projects in package discovery.

Use these initial dependency baselines and commit `go.sum`:

- `github.com/quic-go/quic-go v0.62.0` for the QUIC adapter.
- `github.com/golang-jwt/jwt/v5 v5.3.0` for the optional JWT package.
- `database/sql` for the PostgreSQL store; the host supplies an already-open `*sql.DB` and its driver. The PostgreSQL integration test module uses `github.com/jackc/pgx/v5/stdlib`, with the exact resolved version committed in that test module.

These versions are reproducible starting points, not a claim that they are the newest releases or have completed security review. Verify Go compatibility and dependency advisories at M0; a necessary version change MUST be recorded here with its reason and revalidated adapter behavior. Do not expose external transport or JWT types in the core API. [quic-go baseline API](https://pkg.go.dev/github.com/quic-go/quic-go@v0.62.0), [JWT baseline release](https://github.com/golang-jwt/jwt/releases/tag/v5.3.0)

### 1.2 Vocabulary and invariants

| Term | Meaning |
| --- | --- |
| Connection | One QUIC transport connection; losing it does not immediately destroy a logical session. |
| Session | An authenticated logical peer with a stable random ID and exactly one owning process. |
| Owner | Server ID plus process incarnation and a specific reachable endpoint. |
| Epoch | Owner-assigned, monotonically increasing connection generation within a session. |
| Group key | Application-scoped unique identifier for one match/room instance; it is never reused. |
| Assignment | Durable group-to-owner record with terminal closure. |
| Admission ticket | Bootstrap-signed permission to attempt a new connection to a specific owner/group. |
| Resume secret | Opaque credential proving possession of an existing session, used alongside fresh authentication. |

The implementation MUST preserve these invariants:

1. A logical session and an open group assignment never change owners.
2. At most one epoch can dispatch current work for a session.
3. Authentication and admission complete before gameplay dispatch.
4. Transport delivery is not proof of application execution.
5. Closing a session is terminal; resumption cannot resurrect it.
6. A changed process incarnation invalidates previous-process sessions and tickets.
7. Every library-managed queue, parser allocation, pending-operation set, and admission pool has a finite bound.

## 2. Packages and dependency direction

| Package | Owns | Permitted dependencies |
| --- | --- | --- |
| `sgsp` (module root) | Public API, client/server orchestration, sessions, dispatch, codecs | Internal wire, transport, quictransport, and runtime packages; standard library |
| `internal/wire` | Pure framing, JSON control decoding, constants and error mapping | Standard library only |
| `internal/transport` | Private transport interfaces | Standard library only |
| `internal/quictransport` | quic-go adaptation and transport metrics | Private transport interfaces, quic-go |
| `internal/runtime` | Budgets, queues, timers, reusable scheduling primitives | Standard library only; no public session types |
| `auth/jwt` | Optional identity verifier and admission signing/verification | `sgsp`, golang-jwt, standard crypto |
| `placement` | Bootstrap handler/client, ownership policy, store and registry interfaces | `sgsp`; no dependency on a concrete store |
| `placement/memory` | Development assignment store and registry | `placement` |
| `placement/postgres` | Durable assignment adapter and explicit migration SQL | `placement`, `database/sql` |
| `internal/testutil` | Fake clock, deterministic transport, barriers and bounded observers | Private interfaces; used by tests only |

Keep the session state machine in the root package to avoid circular imports with public types. Put a test-only PostgreSQL driver dependency in `integration/postgres`, a nested module with a local replacement for `qattidev/sgsp`. Examples and the benchmark command import the public APIs.

## 3. Public API contract

The following Go blocks declare the intended public surface. Function declarations are signatures, not implementations. Internal representation can change while preserving these contracts.

### 3.1 Core types

```go
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

type AppIdentity struct { ID, Version string }
type Endpoint struct { Address, ServerName string }
type Owner struct {
    ID string
    Incarnation Incarnation
    Endpoint Endpoint
}
type Credential struct {
    Scheme string
    Data []byte
}
type Principal struct {
    Issuer, Subject string
    ExpiresAt time.Time
    Attributes map[string]string
}
type Authenticator interface {
    Authenticate(context.Context, Credential) (Principal, error)
}
type CredentialProvider func(context.Context) (Credential, error)
type GroupAuthorizer func(context.Context, Principal, string) error
type Admission struct {
    App AppIdentity
    PrincipalIssuer, Subject, GroupKey string
    Owner Owner
    ExpiresAt time.Time
}
type AdmissionVerifier interface {
    Verify(context.Context, string) (Admission, error)
}

type SendOptions struct {
    Channel ChannelID
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
    RTT time.Duration
    TransportStatsAvailable bool
    BytesSent, BytesReceived uint64
    SendQueuedBytes, ReceiveQueuedBytes int64
    LocalDatagramsDropped, StaleUpdatesDropped uint64
}
type Error struct {
    Code Code
    Message string
    OutcomeUnknown bool
    MaxPayload int
    Cause error
}
```

`Error` MUST implement `error`, `Unwrap`, and `Is`. Export one `Err<Name>` sentinel for each nonzero code in section 6; `errors.Is` compares SGSP codes. Context-related errors MUST also unwrap to the corresponding `context` error. `OutcomeUnknown` is meaningful for requests and is conservative as specified in section 6.

`Session` methods are concurrency-safe. `Principal` returns a defensive copy. Attachment replacement is atomic; the application is responsible for synchronization inside the attached value. `Session.Context` is canceled only on logical closure. A receive item's context is canceled on its request deadline, epoch supersession, transport loss, or logical closure.

Stats transport fields describe the current active connection and are zero/unavailable while suspended or closed. Transport byte counters exclude outer UDP/IP framing. Queue gauges describe library-owned session data, including still-borrowed items, and drop counters accumulate across epochs. Do not present a previous epoch's RTT as a current measurement.

A `Stream` supports one concurrent reader and one concurrent writer. Deadline and close operations are concurrency-safe. `CloseWrite` sends FIN; `CloseRead` cancels further reads. `Close` performs both, while `Abort` resets both directions with the supplied code. Closing one custom stream does not close its session.

### 3.2 Endpoints, configuration, and dispatch

```go
package sgsp

import (
    "context"
    "crypto/tls"
    "net"
    "time"
)

type Limits struct {
    ControlBytes, MessageBytes, DatagramBytes int
    QueueBytes, QueueMessages int
    ReliableChannels, DatagramChannels int
    Requests, Streams int
    MaxSessions, MaxPendingHandshakes int
    GlobalApplicationBytes int64
    AuthTimeout, KeepAlive, IdleTimeout, CloseTimeout time.Duration
    ResumeGrace, SlowConsumerTimeout, RequestTimeout time.Duration
    ReconnectMin, ReconnectMax time.Duration
    ControlQueueBytes, ControlQueueMessages int
    StreamReceiveWindow, ConnectionReceiveWindow uint64
    PreAuthBytes int
    MaxActiveGroups, MaxGroupKeyBytes int
    HandshakesPerIPPerSecond, HandshakeBurstPerIP int
    MessagesPerSessionPerSecond, MessageBurstPerSession int
}
func DefaultLimits() Limits

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
    Kind LifecycleKind
    Epoch uint64
    Reason Code
}
type Incoming struct {
    Kind IncomingKind
    Session Session
    Epoch uint64
    Type MessageType
    Channel ChannelID
    Delivery Delivery
    Sequence uint64
    Payload []byte
    Stream Stream
    Lifecycle *Lifecycle
}
func (*Incoming) Context() context.Context
func (*Incoming) Reply(context.Context, []byte) error
func (*Incoming) Fail(context.Context, Code, string) error
func (*Incoming) Release()

type Handler func(context.Context, *Incoming)
type Router struct { /* private fields */ }
func NewRouter() *Router
func (*Router) OnEvent(MessageType, Handler) error
func (*Router) OnRequest(MessageType, Handler) error
func (*Router) OnStream(MessageType, Handler) error
func (*Router) OnUnknown(Handler) error
func (*Router) OnLifecycle(Handler) error

type Observation struct {
    Name string
    Kind string
    Code Code
    Value float64
}
type Observer interface { Observe(Observation) }
type DispatchConfig struct {
    Mode DispatchMode
    Router *Router
}
type ServerConfig struct {
    Role Role
    TLS *tls.Config
    App AppIdentity
    Owner Owner
    Auth Authenticator
    AuthorizeGroup GroupAuthorizer
    Admission AdmissionVerifier
    RequireAdmission bool
    CommitGroupClose func(context.Context, AppIdentity, string, Owner) error
    Dispatch DispatchConfig
    Limits Limits
    Observer Observer
}
type ClientConfig struct {
    Role Role
    TLS *tls.Config
    App AppIdentity
    Credentials CredentialProvider
    ExpectedOwner *Owner
    AdmissionTicket string
    GroupKey string
    Dispatch DispatchConfig
    Limits Limits
    Observer Observer
    DisableReconnect bool
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
func NewServer(ServerConfig) (Server, error)
func Dial(context.Context, Endpoint, ClientConfig) (Client, error)
```

Use `DefaultLimits()` when constructing configuration. A completely zero `Limits` value selects all defaults; a nonzero value MUST be fully valid. Zero does not mean unlimited. Constructors validate ranges and reject impossible budgets. Require QueueMessages >= 2 and QueueBytes >= 2 * (MessageBytes + 32) so replies have reserved capacity; also enforce the transport-window formula in section 10.3. Check additions/multiplications for overflow before comparing bounds.

`NewServer` freezes a copy of router registrations; duplicate `(kind, type)` routes are errors. Type IDs MUST be in `1..2^32-1`; channel ID zero is valid. Missing authenticators, credential providers, or handler routers are configuration errors. In polling mode, `Router` MUST be nil. `Next` in handler mode returns `ErrWrongMode`.

Both endpoint constructors require a TLS configuration and clone it before use, require TLS 1.3, set ALPN to sgsp/1, and reject InsecureSkipVerify. Server configuration must supply a certificate or certificate callback. Client trust uses configured RootCAs or system roots and the supplied Endpoint.ServerName. Endpoint.Address is a host:port for a specific owner; ServerName is its expected TLS DNS identity.

Configure Owner.ID and Owner.Endpoint, leaving Owner.Incarnation zero. NewServer MUST reject a supplied nonzero incarnation and generate a fresh one with crypto/rand. Server.Owner returns the resulting owner snapshot; register that snapshot with placement after construction. Tests inject randomness through a private constructor rather than weakening this rule. A listener can Serve only once.

`Serve` transfers ownership of the supplied packet socket when it starts successfully, and MUST close it before returning. Its context cancellation force-closes sessions and joins internal workers. `Drain` requires a deadline: it changes admission policy immediately, waits for logical sessions to close, and force-closes remaining ones at that deadline. It continues accepting handshakes needed for resumes while draining.

`Dial`'s context covers initial connection establishment, not the returned client's lifetime. `Client.Close` disables reconnect, closes its session, and joins workers. The initial `Client.Session()` object is retained across reconnects. Persistence of resume credentials across client process restarts is outside v1; store them only in SDK memory.

An endpoint permits one `Next` consumer. Payload memory is borrowed through handler return or `Incoming.Release` in polling mode. `Release` MUST be idempotent and MUST NOT invalidate the request responder state: a retained item can still reply before its deadline, but its old payload MUST NOT be read. Normal replies copy accepted bytes. Each request accepts at most one successful reply/failure. A handler that returns without replying fails the request with `Internal`; polling callers can release the payload and reply later within the request deadline. A stream item's `Release` releases its envelope, not its stream.

Session closure cancels a borrowed item's context but does not invalidate its memory before Release. It remains charged until released. Reply/Fail on a non-request or Fail with status Normal returns InvalidArgument; a second completed response returns SessionClosed for that responder, not for the enclosing session. OpenStream uses the smaller of RequestTimeout and its caller deadline for opening/acceptance; accepted stream I/O subsequently uses its own deadlines and session lifetime. RefreshAuth is a client operation; invoking it on a server-side Session returns WrongMode.

Session.Close MUST reject SessionSuperseded as InvalidArgument: that code is reserved for the library's internal replacement of a transport epoch and cannot be used as a public logical-close reason.

`OnUnknown` can handle unregistered events and requests. Unhandled events are discarded and counted; unhandled requests and streams receive `UnsupportedMessage`. Lifecycle events arrive in state-transition order for each session. `Opened` precedes its ordinary messages and stream-handler launch. A handler panic is contained at the callback boundary, counted, and closes the affected session with `Internal`; it MUST NOT crash the server.

### 3.3 Typed helpers

```go
package sgsp

import "context"

type Codec[T any] interface {
    Encode(T) ([]byte, error)
    Decode([]byte) (T, error)
}
type Message[T any] struct {
    ID MessageType
    Codec Codec[T]
}
type Request[I, O any] struct {
    ID MessageType
    Input Codec[I]
    Output Codec[O]
}
func JSON[T any]() Codec[T]
func Emit[T any](context.Context, Session, Message[T], T, SendOptions) error
func Call[I, O any](context.Context, Session, Request[I, O], I) (O, error)
func OnMessage[T any](*Router, Message[T], func(context.Context, Session, T) error) error
func OnCall[I, O any](*Router, Request[I, O], func(context.Context, Session, I) (O, error)) error
```

These MUST delegate to the raw API. Encoding or decoding failures are application errors, not malformed SGSP frames. Typed event decode failures discard the event with a diagnostic. Typed request decode failures return `InvalidArgument` before invoking the handler. The JSON codec owns any reference-backed values it returns; custom codecs MUST document if their decoded values borrow input memory.

A typed event handler's returned error is observed as an application failure without closing its session. OnCall returns a typed sgsp.Error's allowed nonzero code or Internal for an arbitrary error; an arbitrary error's text is replaced with a generic diagnostic. Failure to encode a handler's result also returns Internal. An error response describes the result observed by the caller and never promises that earlier handler side effects were rolled back.

## 4. Wire encoding

### 4.1 Common encoding rules

- The ALPN identifier is exactly `sgsp/1`. TLS server identity verification is mandatory. Disable application 0-RTT on both ends.
- `V` denotes the QUIC unsigned variable-length integer encoding: 1, 2, 4, or 8 bytes, with two high bits selecting width and 62 payload bits. Senders MUST use the shortest encoding. Receivers MUST reject a non-minimal encoding in SGSP framing, even if QUIC itself permits it.
- `B(n)` denotes exactly `n` uninterpreted bytes. Every length counts bytes, not characters. Validate lengths against configured bounds before converting to int or allocating; reserve budget before reading bodies. This also applies on 32-bit targets.
- Control binary values use base64url without padding. Session and incarnation IDs are 16 random bytes represented as 32 lowercase hexadecimal characters in JSON. JSON integers are limited to exact interoperable values; epochs and values that can exceed `2^53-1` are decimal strings.
- All JSON is UTF-8 with an object at its root. Reject duplicate keys, invalid types, invalid UTF-8, trailing values, and nesting deeper than 8. Ignore unknown fields within the negotiated major version, subject to the size and nesting limits. Reject unknown required capabilities or control operations.
- Application ID, version, owner ID, and credential scheme are nonempty UTF-8 strings of at most 128 bytes. Group keys are nonempty UTF-8 strings of at most 256 bytes when present. Treat strings as exact byte sequences; do not normalize them.

### 4.2 Stream identification and frames

The client opens the first bidirectional stream (QUIC stream 0) as the control stream. It has no SGSP stream-kind prefix. All subsequent streams start with one `V` stream kind:

| Stream kind | Value | Direction | Following data |
| --- | --- | --- | --- |
| Reliable event channel | 1 | Unidirectional | `channel:V`, then zero or more reliable event frames |
| Request | 2 | Bidirectional | One request followed by one response on the reverse direction |
| Custom stream | 3 | Bidirectional | Stream type and acceptance, then raw bytes |

An event-channel frame is:

```text
frame_length:V | kind:V (=1) | channel:V | message_type:V | payload:B(remaining)
```

`frame_length` counts everything after itself. The channel MUST equal the stream's declared channel. A payload is at most the negotiated reliable-message cap; total frame length is additionally bounded to payload cap plus 32 bytes. No reliable sequence field is present: the stream supplies ordering.

A datagram contains exactly one event and no outer length:

```text
kind:V (=2) | channel:V | message_type:V | payload:B(remaining)
kind:V (=3) | channel:V | message_type:V | sequence:V | payload:B(remaining)
```

Kind 2 is `Unreliable`; kind 3 is `UnreliableSequenced`. The encoded datagram, including envelope, is bounded by the receiver's advertised datagram cap and the transport's current smaller cap. Do not fragment, pack multiple SGSP events into one datagram, or add a session ID to every event. The authenticated connection identifies its session and epoch.

A channel's delivery mode binds on first use within a sending direction and epoch. A sender changing that mode returns `InvalidArgument`; a peer doing so is a protocol violation. The same numeric channel may use a different mode in the opposite direction. Reliable and datagram channel count limits apply independently.

Channel bindings and their count slots last for the epoch. Event channels have no public close/reopen operation in v1. A duplicate channel stream, or an unsolicited event-stream FIN/reset while its session connection is otherwise active, is ProtocolViolation. Ordinary connection-loss cleanup takes precedence over interpreting interrupted channel reads as malformed framing. New application streams must supply their complete SGSP header within AuthTimeout; an incomplete header cannot occupy a slot indefinitely.

Sequenced channel numbers begin at 1 per epoch and increase at successful queue admission. Gaps are allowed. They MUST NOT wrap; reaching `2^62-1` closes with `ResourceExhausted` before another send. Track the largest sequence admitted to the receive queue per channel; discard lower or equal values. If a new message cannot acquire budget, drop it without advancing this high-water mark. Keep at most the latest undispatched sequenced event per channel. Unreliable events have no SGSP sequence field.

### 4.3 Requests and custom streams

A request stream carries, after kind 2:

```text
message_type:V | timeout_ms:V | payload_length:V | payload:B(payload_length) | FIN
```

`timeout_ms` is a relative processing budget, starting when the receiver reads the request header. It MUST be in `1..300000`. The sender uses the smaller of its remaining context deadline, configured request timeout (default 5 seconds), and 300 seconds. This is not clock synchronization; the caller's own context remains authoritative. A canceled stream cancels its receiver-side request context.

The reverse direction contains exactly one response:

```text
status:V | payload_length:V | payload:B(payload_length) | FIN
```

Status 0 is success with application bytes. A nonzero status is a code from section 6, with a UTF-8 diagnostic body of at most 256 bytes. The normal message cap applies to request and success-response payloads. The stream ID provides correlation; do not create a separate request-ID map spanning connections. Extra frames, extra bytes after a complete exchange, or an event frame on a request stream are violations. After reading the declared body, the receiver MUST require FIN; use the request deadline to bound this wait.

`Call` returns a caller-owned response slice. Remote cancellation is advisory with respect to game execution: already committed game actions are not rolled back.

A custom stream carries, after kind 3:

```text
stream_type:V
```

The receiver responds with `status:V`. On nonzero status it sends `diagnostic_length:V | diagnostic:B(length) | FIN` with at most 256 UTF-8 bytes. On zero, the remainder of both directions is raw application data. The initiator MUST wait for acceptance before writing application bytes. The receiver reserves a raw-stream slot and accepts a registered handler (or polling item) before sending status 0. An accepted raw stream has no SGSP message-size cap, but remains subject to flow control, concurrent-stream limits, deadlines, and session closure.

### 4.4 Control stream

Each control message is `json_length:V | json:B(json_length)`, bounded to 16 KiB by default. Before negotiation, always enforce the fixed 16 KiB ceiling; subsequent control size is the minimum of the peers' advertised caps. The following schemas use required fields unless explicitly marked optional.

`HELLO` fields:

| Field | Type and rule |
| --- | --- |
| `op` | String `hello` |
| `role` | `game` or `bootstrap`; role is fixed by the endpoint configuration |
| `app`, `app_version` | Application identity; exact match required in v1 |
| `required` | Unique string array; game SDK default is `["datagrams"]`, bootstrap uses `[]` |
| `limits` | Object with positive integer `control_bytes`, `message_bytes`, `datagram_bytes`, `reliable_channels`, `datagram_channels`, `requests`, `streams` |
| `credential` | Object with `scheme` and base64url `data`; decoded data at most 4,096 bytes |
| `group` | Optional group key; required to exactly match a grouped admission ticket |
| `admission` | Optional signed admission-ticket string; at most 4,096 bytes |
| `resume` | Optional object with `session_id`, `owner_id`, `incarnation`, and base64url `secret` (exactly 32 bytes) |

An initial connection has no `resume`. A resume has no `admission` and uses the existing session's group, app, limits, and delivery capabilities. Changed receive limits or app version during resume are rejected; a new logical session is needed to renegotiate them. `HELLO` never carries trusted principal claims supplied independently by the authenticator.

`WELCOME` fields:

| Field | Type and rule |
| --- | --- |
| `op` | String `welcome` |
| `session_id` | Session ID |
| `epoch` | Positive decimal string, starting at `"1"` |
| `owner` | Object with `id`, `incarnation`, `address`, `server_name` |
| `principal` | Object with `issuer`, `subject`, `expires_unix_ms`; do not copy private authenticator attributes onto the wire |
| `limits` | Server's receive limits in the same shape as `HELLO.limits` |
| `capabilities` | Enabled capability strings; v1 defines `datagrams` |
| `resumed` | Boolean |
| `resumable` | Boolean; false only for bootstrap role |
| `resume_grace_ms` | Positive integer for game role; 0 for bootstrap |
| `resume_secret` | Present only for a new resumable session, exactly 32 bytes encoded base64url |

No new secret is issued on resume. This avoids losing the only valid credential when a resume response is lost. The server holds only its hash after initially constructing `WELCOME`. The client checks expected owner ID/incarnation and the TLS server name before accepting the result. The client keeps its original secret after a resumed `WELCOME`.

Other control messages:

| Operation | Fields beyond `op` | Rule |
| --- | --- | --- |
| `reject` | `code`, `message` | Handshake rejected; no session dispatch; message at most 256 UTF-8 bytes |
| `refresh` | `credential` | Client-to-server only; one outstanding refresh allowed |
| `refresh_result` | `code`, `message`, optional `expires_unix_ms` on success | Successful refresh preserves issuer/subject |
| `close` | `code`, `message` | Terminal logical closure, initiated by either peer |
| `close_ack` | No additional fields | Confirms that the peer committed logical closure |

Unexpected direction, duplicate handshake, unknown operation, or unsupported required capability is a rejection/violation, never an application message. Transport keepalive uses QUIC PING rather than additional SGSP heartbeat messages.

### 4.5 Golden vectors

Check these bytes literally; hex values exclude QUIC packet framing. Request/custom examples include their stream-kind prefix. Reliable examples include the channel-stream prefix where stated.

| Input | Expected hex |
| --- | --- |
| `V(0)`, `V(63)`, `V(64)`, `V(16383)`, `V(16384)` | `00`, `3f`, `40 40`, `7f ff`, `80 00 40 00` |
| Channel 1 stream with reliable type 10, payload `aa bb` | `01 01 05 01 01 0a aa bb` |
| Unreliable channel 1, type 10, payload `aa bb` | `02 01 0a aa bb` |
| Sequenced channel 1, type 10, sequence 2, payload `aa bb` | `03 01 0a 02 aa bb` |
| Request type 20, timeout 1,000 ms, payload `aa` | `02 14 43 e8 01 aa`, then FIN |
| Success response with payload `bb` | `00 01 bb`, then FIN |
| Unknown-message response with empty diagnostic | `0d 00`, then FIN |
| Custom stream type 30 / accepted response | `03 1e` / `00` |
| Control JSON `{"op":"close_ack"}` | `12 7b 22 6f 70 22 3a 22 63 6c 6f 73 65 5f 61 63 6b 22 7d` |

The final control JSON is 18 UTF-8 bytes. Golden-vector tests MUST also cover empty application payloads, largest allowed IDs, each integer-width boundary, non-minimal encodings, and truncation at every byte offset.

## 5. Establishment and capability negotiation

1. Complete a normal QUIC/TLS handshake with `sgsp/1`; keep early application data disabled.
2. Accept stream 0 and bounded `HELLO` within the authentication timeout. Reserve a pre-authentication slot, not a gameplay session.
3. Validate syntax, role, app/version, required capabilities, and receive limits. The adapter's actual datagram negotiation MUST agree with SGSP capability negotiation.
4. Invoke the authenticator under the same overall authentication deadline. Require nonempty issuer/subject and an expiration strictly in the future.
5. For a new assigned session, verify the ticket and owner binding and invoke group authorization. For resume, verify identity and resume ownership and reauthorize its existing group before mutating the session. A locally closed group rejects a resume even with valid credentials.
6. Reserve session/group capacity and commit the new session or resumed epoch. Enqueue its lifecycle event before ordinary messages. Send `WELCOME` on control.
7. The client becomes active only after validating `WELCOME`. The owner accepts gameplay only after committing admission. Game data cannot bypass the authentication barrier.

A peer's receive limits constrain the opposite sender; retain both directional limit sets. Do not substitute one peer's settings for the other's. Reliable streams received by the client before its `WELCOME` are left unread in bounded transport buffers; pre-`WELCOME` datagrams are discarded. At the owner, pre-admission datagrams are discarded and application streams are rejected. Because separate streams/datagrams can arrive before a control message, early arrival alone after an accepted handshake MUST NOT be treated as an authentication bypass.

A new-session `WELCOME` lost before the client learns its secret can leave an orphan session; normal transport-loss detection and grace expiry remove it. Do not grant unauthenticated recovery of that session. A lost resumed `WELCOME` is handled by the existing secret. Callback invocation itself MUST be bounded by context; an application authenticator that ignores cancellation is a host bug and must not release its concurrency slot until it returns.

## 6. Error and execution-outcome contract

These numeric codes are stable across control messages, request statuses, stream resets, and local SGSP errors. Codes `1024..2^32-1` are reserved for application-defined request/custom-stream failures. Values `26..1023` are reserved for future SGSP versions.

| Value | Name | Meaning |
| --- | --- | --- |
| 0 | Normal | Success or clean logical close |
| 1 | ProtocolViolation | Invalid framing, state, or stream use |
| 2 | UnsupportedVersion | ALPN/application compatibility rejected |
| 3 | UnsupportedCapability | Required transport feature unavailable |
| 4 | Unauthenticated | Identity or credential verification failed |
| 5 | Forbidden | Authenticated principal is not permitted |
| 6 | AuthExpired | Logical authentication lifetime ended |
| 7 | SessionNotFound | Owner has no matching session |
| 8 | SessionExpired | Reconnect grace expired where a retained tombstone distinguishes it |
| 9 | ServerUnavailable | Placement/owner cannot currently serve the request |
| 10 | ServerDraining | New admission rejected while draining |
| 11 | Backpressure | Local queue lacks capacity |
| 12 | TooLarge | Payload or frame exceeds the permitted bound |
| 13 | UnsupportedMessage | No handler for this message/stream type |
| 14 | Canceled | Operation canceled |
| 15 | DeadlineExceeded | Operation deadline ended |
| 16 | Internal | Library or contained handler failure |
| 17 | SessionSuperseded | Another connection epoch replaced this transport |
| 18 | SlowConsumer | Reliable application work stalled beyond its bound |
| 19 | GroupClosed | This group instance is terminal |
| 20 | ResourceExhausted | Admission/stream/request/rate limit reached |
| 21 | SessionClosed | Logical session is terminal |
| 22 | SessionSuspended | Logical session currently lacks an active transport |
| 23 | OutcomeUnknown | Request may have executed without an observed result |
| 24 | InvalidArgument | Locally invalid API input or rejected application payload |
| 25 | WrongMode | Polling/handler API mismatch |

Malformed reliable/control framing closes the logical session with `ProtocolViolation`. A malformed datagram is discarded and counted; repeated abuse is handled by the rate limiter. A well-formed request exceeding a concurrency budget receives `ResourceExhausted` without running a handler. An oversized incoming reliable body closes before allocation; a locally oversized send returns `TooLarge` and leaves the session usable.

`Send` waits only for local queue admission. Once admitted, later context cancellation does not recall an event. `TrySend` never waits. FIFO on a reliable channel is queue-admission order, including concurrent callers. Sender payloads are copied before success is returned. Sequenced datagram replacement can discard an earlier successful send; that is part of unreliable delivery.

For `Call`, an error before any request bytes are handed to the transport has `OutcomeUnknown=false`. Once transmission starts, loss, reset, cancellation, or deadline expiration before a complete response MUST return `OutcomeUnknown=true` (code 23 with its underlying cause). A complete remote response supplies the observed result. An application failure response does not imply rollback. Cancellation MUST NOT cause automatic retry or replay after reconnect. Request handlers may execute once for each separately initiated call, even if payloads are identical.

`Close` commits local logical closure and waits for close_ack for at most CloseTimeout (one second by default), or a shorter caller deadline. The control writer remains alive for this bounded notification phase even though the logical context is canceled. If delivery of `close` fails, the remote owner may temporarily suspend until its grace expires; the initiating SDK MUST NOT reconnect. At the owner, SessionSuperseded terminates only the replaced epoch. A client ignores that error from an already-obsolete local transport; if its current transport is superseded by a different client attempt, it ends that local SDK handle without reconnecting and fighting the winning connection. The owner's logical session continues on the winner. All other explicit SGSP terminal close codes close the session; unexpected network transport loss suspends it.

## 7. Authentication and credential handling

### 7.1 Identity hooks

The authenticator runs on every new connection, resume, and credential refresh. Issuer and subject together identify a principal; subject alone is insufficient. Authentication failure on a candidate connection MUST NOT alter an existing session or its expiration timer.

The optional JWT helper has separate identity-verifier and admission-verifier constructors. Both take explicit trusted issuer, audience, and `kid -> ed25519.PublicKey` maps. V1's built-in signing/verifying profile is Ed25519 with JWT algorithm `EdDSA`; other schemes use the generic authenticator hook. Use the JWT library's parser and signature validation rather than a handwritten JWT implementation.

Identity JWTs MUST have `typ=sgsp-access+jwt`, an explicitly trusted `kid`, algorithm `EdDSA`, and required `iss`, `sub`, `aud`, `exp`, `iat`, and `nbf` claims. The audience is the configured application ID. Time claims are integer Unix seconds. Require `exp > now`, `exp > iat`, `nbf <= now+5s`, and `iat <= now+5s`. The five-second skew allowance MUST NOT extend the session beyond `exp`. Require nonempty subject and exact issuer/audience matches; reject extra audiences in the built-in profile. Reject token-supplied key URLs/keys and unknown key IDs. Key refresh is an explicit, atomic replacement of a host-provided trusted key map, not network lookup directed by a token.

Provide `auth/jwt.NewIdentityVerifier(IdentityConfig)`, `NewAdmissionVerifier(AdmissionConfig)`, and `NewAdmissionSigner(SignerConfig)`, each returning its concrete helper plus error. IdentityConfig and AdmissionConfig each contain Issuer, Audience, and a trusted Keys map. SignerConfig contains Issuer, Audience, KeyID, and an Ed25519 PrivateKey. Configurations own copies of their key material. Each verifier exposes ReplaceKeys for an atomic validated replacement.

The identity verifier implements `sgsp.Authenticator` and accepts credential scheme `jwt`; the admission verifier implements `sgsp.AdmissionVerifier`. Admission.PrincipalIssuer always means the client's identity issuer, while JWT iss is the bootstrap signer's configured issuer. The signer must never substitute one for the other. Bootstrap sets Admission.ExpiresAt to its current Unix second plus 30 seconds; the signer uses that exact expiry, requires it to be in the future and no more than 30 seconds after signing time, and generates a fresh token ID. Placement.ExpiresAt returns the same expiry. Constructors reject missing issuer/audience/keys and mismatched key types. Identity minting/account login belongs to the game or its identity provider.

The profile is intentionally distinct from arbitrary identity-provider tokens. An application integrating another provider implements the authenticator or exchanges that provider's credential for this profile outside SGSP. No REST exchange is required by the library. [JWT validation guidance](https://www.rfc-editor.org/rfc/rfc8725.html)

### 7.2 Admission tickets

Admission JWTs use `typ=sgsp-admission+jwt` and a separate trusted signing-key map. They contain standard issuer/subject/audience/time claims plus `principal_issuer`, `app_version`, `group` (empty for an ungrouped placement), `owner_id`, `incarnation`, `address`, `server_name`, and a random 128-bit `jti`.

The target owner MUST check signature, ticket type, issuer, audience/application, application version, subject and principal issuer against fresh authentication, owner ID/incarnation against itself, endpoint binding against its configured endpoint, and group against `HELLO`. Require `exp <= iat+30s` and apply the strict expiration rule. A ticket authorizes an admission attempt, not unauthenticated identity or gameplay actions. It is reusable during its short lifetime; rate/session admission limits bound repeated attempts. It is never a resume credential.

In single-server mode `RequireAdmission=false` permits ungrouped direct sessions. A group key always requires a valid admission ticket, group-authorizer hook, and group-close integration. With `RequireAdmission=true`, every new game session requires a ticket, including ungrouped sessions. A valid existing-session resume does not need an admission ticket.

### 7.3 Refresh, revocation, and secrets

Only the client sends `refresh`; the server reauthenticates the supplied credential and requires the same issuer and subject. An unsuccessful refresh leaves the current unexpired principal intact. Successful refresh atomically replaces the principal snapshot and its expiration timer, and emits `AuthRefreshed`. The client updates its expiration from `refresh_result`. Credential acquisition is host code; automatic login or token minting is not implemented.

An expiration closes Active or Suspended sessions and destroys their resume credential. A session that already expired cannot be recovered by supplying a later credential. The game SHOULD refresh before its expiration deadline.

`Server.Revoke(ctx, issuer, subject)` closes matching sessions present at its linearization point and cancels their work. To also reject future new sessions, the authenticator MUST consult the host's revocation/ban policy. Offline JWT signature verification alone cannot detect an external revocation. This distinction MUST appear in public API documentation.

Use a short admission-commit mutex to serialize insertion of new sessions with Revoke's snapshot of matching sessions. This mutex is never used for gameplay dispatch. Revoke releases it before closing its captured session references or performing any I/O. A new admission that linearizes after this snapshot is subject to the host authenticator's policy, not an implicit permanent library ban.

Generate session IDs, process incarnations, resume secrets, and token IDs with `crypto/rand`. Hash resume secrets with SHA-256 and compare fixed-size hashes with constant-time comparison. Use separate maps for sessions and token keys. Never log credential data, resume secrets/hashes, admission tokens, raw HELLO JSON, or raw authenticator errors. Observer output uses sanitized numeric codes. Do not persist TLS application 0-RTT data. [QUIC TLS security](https://www.rfc-editor.org/rfc/rfc9001.html#section-9.2)

## 8. Session state machine and reconnect algorithm

### 8.1 Authoritative session record

Each owner maintains a bounded, sharded session registry. A session record contains ID, owner, app, optional group, principal snapshot, negotiated directional limits/capabilities, resume-secret hash, current epoch and connection, state, attachment, expiration/grace deadlines, queues, pending requests, and custom-stream handles.

Each record has one mutex governing state/epoch changes. Registry locks protect lookup/insertion/removal only. Do not hold a registry or session mutex during I/O, authentication, application callbacks, or assignment-store calls. Increment epochs under the record mutex. Epoch 1 belongs to initial admission; refuse another resume at `2^64-1` with `ResourceExhausted` rather than wrapping.

### 8.2 Transition table

| Current state | Trigger | Next state | Required effects |
| --- | --- | --- | --- |
| Connecting | TLS succeeds | Authenticating | Begin bounded SGSP handshake; no game dispatch |
| Connecting/Authenticating | Failure or timeout | Closed candidate | Free candidate resources; existing session, if any, remains untouched |
| Authenticating | New admission succeeds | Active | Allocate epoch 1, store secret hash, enqueue Opened, send WELCOME |
| Active | Unexpected transport loss | Suspended | Detach epoch; cancel its contexts/streams/requests; clear its payload queues; start grace |
| Suspended | Valid resume before both deadlines | Active | Attach new epoch, retain identity/attachment, enqueue Resumed |
| Active | Valid resume from a new transport | Active | Atomically supersede previous epoch, cancel old work, enqueue Resumed |
| Active/Suspended | Explicit close, revocation, authentication expiration, or forced shutdown | Closed | Cancel logical context; invalidate hash; remove from active registry; enqueue SessionEnded |
| Suspended | Grace expires | Closed | Same terminal cleanup, with SessionExpired |
| Closed | Any attempted resume | Closed | Reject; never recreate this session ID |

Only the owner assigns epochs. A resume credential and authenticated principal authorize replacing an apparently active transport; this is needed when the original connection is blackholed but not yet timed out. Authentication/secret checks occur before acquiring the state lock, and the final identity, expiration, and state predicates MUST be checked again under it before commit.

### 8.3 Fencing old work

Every transport read loop and incoming item captures its epoch. Enqueue MUST compare that epoch with the session's current Active epoch. Dispatch MUST compare it again before invoking a message callback. Superseding a connection cancels its incoming request contexts and closes its custom streams. Replies from an old epoch cannot write onto a new connection.

Already-running callbacks cannot be safely preempted in Go. Do not claim that SGSP undoes their game-state changes. Their context is canceled, and a resumed callback MUST NOT overlap an old ordinary callback under handler mode's serial dispatcher. Game-loop integrations MUST check epoch/context at their own command-commit boundary. An action committed before supersession remains committed; retry safety requires an application operation ID.

Lifecycle events are ordered records of transitions and are not discarded merely because their recorded epoch is old. Ordinary stale work is discarded. The Opened/Resumed marker precedes ordinary work from its epoch. A bounded lifecycle queue holds at most 32 records per session; a host that cannot consume it is closed as a slow consumer. Terminal state remains observable via `Session.State()` even if a terminal notification cannot be queued.

### 8.4 Grace and client reconnect

Default loss detection uses one-second keepalive and five-second QUIC idle timeout. An explicit network error can detect loss earlier. Start the 30-second grace when the owner transitions to Suspended, not at the last game message. The effective retention deadline is the earlier of grace and principal expiration. Failed unauthenticated resume attempts do not change either deadline.

The SDK retains the original owner endpoint and incarnation. Its credential provider is invoked under a five-second attempt deadline for every resume attempt. Reconnect uses one loop per session with full-jitter delays uniformly sampled from `[0, min(2s, 250ms * 2^attempt)]`. The local overall retry deadline is the earlier of the client's observed disconnect time plus the advertised grace and its known principal expiration. An owner's terminal rejection ends retries earlier. Success resets the attempt counter.

Retry network-unavailable errors while within the deadline. An explicit authentication, forbidden, version, owner-incarnation, expired/not-found, or closed rejection is terminal to that SDK session. Do not fall back to bootstrap to turn failure into a silently new session. The application may explicitly request a new session.

Committing a resume and failing to deliver its WELCOME MUST NOT roll back the epoch or invalidate the existing secret. A later attempt with that secret may supersede the failed attempt. Successful authenticated attachment can reset the suspension deadline if that new transport subsequently fails; unrelated failed attempts cannot extend retention.

On suspension, pending `Call` results resolve according to section 6, all application payload queues are discarded, custom streams are reset, and further sends return `SessionSuspended`. Logical attachment remains reachable until closure. Session closure clears the attachment reference after dispatching terminal cleanup (or immediate cleanup if the dispatcher is stalled). Host-side maps/copies are the host's responsibility.

Maintain a bounded terminal-ID cache for up to 30 seconds, capped at `2 * MaxSessions`. It can distinguish expired from unknown sessions. Eviction changes rejection from `SessionExpired` to `SessionNotFound`; both are terminal and MUST NOT permit recreation. Resume-secret hashes MUST be removed immediately rather than retained in this cache.

QUIC path migration or NAT rebinding that preserves a connection does not create a new SGSP epoch or emit Resumed. New-connection resume is an application-level operation distinct from path migration. [quic-go connection migration](https://quic-go.net/docs/quic/connection-migration/)

## 9. Placement, group lifecycle, and database contract

### 9.1 Interfaces and bootstrap exchange

`placement` exposes these contracts (names prefixed with their defining package in this signature summary):

```text
GroupID { App sgsp.AppIdentity; Key string }
Assignment { Group GroupID; Owner sgsp.Owner; Closed bool }
OwnerStatus { Owner sgsp.Owner; Healthy bool; Draining bool; HasCapacity bool }

AssignmentStore.Assign(ctx, group, candidate) -> (Assignment, error)
AssignmentStore.Get(ctx, group) -> (Assignment, error)
AssignmentStore.Close(ctx, group, expectedOwner) -> error
Registry.Snapshot(ctx) -> ([]OwnerStatus, error)
SelectOwner(ctx, principal, group, candidates) -> (sgsp.Owner, error)
AdmissionSigner.Sign(ctx, sgsp.Admission) -> (string, error)
Bootstrap.Resolve(ctx, principal, groupKey) -> (Placement, error)
placement.Resolve(ctx, bootstrapEndpoint, ResolveConfig) -> (Placement, error)

Placement { Owner sgsp.Owner; GroupKey string; AdmissionTicket string; ExpiresAt time.Time }
ResolveConfig { App sgsp.AppIdentity; TLS *tls.Config; Credentials sgsp.CredentialProvider; GroupKey string }
```

Use concrete exported Go structs/interfaces matching this summary. Store missing records return a placement-specific `ErrAssignmentNotFound`; closed records return their closed state from Get and `sgsp.ErrGroupClosed` from Assign. Every operation accepts context cancellation. The in-memory implementation MUST obey the same atomic behavior as PostgreSQL.

The bootstrap service is an SGSP endpoint with role `bootstrap`, authenticated ephemeral sessions, no resumption, and request type 1 reserved for Resolve in that role. Resolve's payload is JSON `{"group":"<key>"}`; empty group requests an ungrouped placement. The reply is JSON with `owner`, `group`, `admission`, and `expires_unix_ms`, using the same owner shape as WELCOME. It is bounded by the reliable-message cap. `placement.Resolve` authenticates, makes one Call, then closes its bootstrap session. Game role's type 1 remains application-defined.

The bootstrap wrapper explicitly selects BootstrapRole in ServerConfig and ClientConfig; ordinary endpoints default to GameRole. HELLO.role MUST match the configured listener role. The wrapper otherwise uses the same public constructors, framing, request, authentication, and dispatch machinery. A bootstrap connection loss closes its ephemeral session immediately.

Bootstrap application wiring supplies a registry, assignment store, selector, signer, authenticator, and group authorizer. The memory registry exposes trusted `Register`, `SetStatus`, and `Remove` methods; service discovery and health-probe scheduling are host responsibilities. Registry changes MUST be visible consistently to one Snapshot call. Client-supplied server registrations are never accepted.

The default selector filters to healthy, non-draining owners with capacity. For a new group, rank eligible owners by descending SHA-256 of `V(app byte length) || app bytes || V(group byte length) || group bytes || V(owner ID byte length) || owner ID bytes || incarnation bytes`; compare digests as unsigned big-endian integers, with ties broken by ascending owner ID then incarnation. This is deterministic rendezvous selection, used only before assignment creation. For an ungrouped request, use a fresh 16-byte random key as the group-byte selection input. A host selector can implement regional/capacity policy without changing the wire contract.

Resolve algorithm:

1. Authenticate the bootstrap session and authorize the requested group if nonempty.
2. For a group, Get its existing assignment first. A closed record fails; an existing owner is never replaced by a newly ranked owner.
3. For a missing group, choose a candidate and call Assign. Use the returned winner, including when another caller won the race.
4. Check that the recorded winning owner/incarnation is currently healthy and admissible. If unavailable, return ServerUnavailable/ServerDraining and retain its assignment.
5. Sign an admission ticket for that owner and principal and return the endpoint. An ungrouped placement bypasses the assignment store but still uses a signed ticket.

An empty eligible set fails without creating a mapping. A winner that becomes unavailable after assignment remains the owner. If a selected server fills before admission, report failure; do not rewrite the group's record. The game can explicitly close that never-started instance and create a new group key.

### 9.2 PostgreSQL schema and atomicity

The PostgreSQL adapter ships a reviewed, explicit migration file; constructors MUST NOT run migrations automatically. Initial schema:

```sql
CREATE TABLE sgsp_group_assignments (
    app_id text NOT NULL,
    group_key text NOT NULL,
    app_version text NOT NULL,
    owner_id text NOT NULL,
    incarnation bytea NOT NULL CHECK (octet_length(incarnation) = 16),
    endpoint_address text NOT NULL,
    endpoint_server_name text NOT NULL,
    closed boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    closed_at timestamptz,
    PRIMARY KEY (app_id, group_key),
    CHECK (closed = (closed_at IS NOT NULL))
);
```

`Assign` MUST use a READ COMMITTED transaction: parameterized `INSERT ... ON CONFLICT (app_id, group_key) DO NOTHING`, followed by a separate SELECT of that key and COMMIT. The second statement must see the committed conflict winner. Do not replace this with a single insert/union-select CTE whose statement snapshot can miss a concurrent winner. Return the stored owner, never the proposed owner unless it won. A stored application-version mismatch returns UnsupportedVersion. [PostgreSQL INSERT conflict behavior](https://www.postgresql.org/docs/current/sql-insert.html)

`Close` verifies the expected owner ID/incarnation and sets `closed=true, closed_at=now()` conditionally. Closing the same record again with the same owner is successful. A different expected owner fails and cannot close the record. `Get` reads closed records too. No operation updates an owner, deletes a closed record to reuse its key, or expires ownership by a health-check TTL.

The memory store defaults to 100,000 total assignment records and returns ResourceExhausted when full; it never evicts an open record or reopens a closed key. It is for development only: losing its process loses assignments, so it MUST NOT be described as restart-safe placement for surviving game servers. Use the persistent adapter for that deployment. Durable closed records can later be archived only into a store that continues to reject reused group keys; archival is outside v1.

### 9.3 Owner-side groups and closure

The owner keeps a bounded local group-admission table keyed by app/group. The first valid ticket reserves the group entry under a lock; concurrent admissions share it. Game group creation belongs to host code, which MUST perform it once per group. Membership and session-to-game attachment are also host code.

The library still tracks which session records belong to each group for admission and closure. Grouped admission and resume MUST hold that group's lock from the final closed-state check through committing session membership/activation. CloseGroup marks the group closed and snapshots its session references under the same lock, then releases the lock before canceling sessions or calling the store. This prevents an admission from slipping between a closed-state check and the closure snapshot. Where both are needed, acquire group lock before session lock; terminal session cleanup releases its session lock before removing itself from the group index. The admission-commit mutex described in section 7 is acquired inside the group lock and released before callbacks; Revoke never acquires a group lock while holding that mutex.

Group-enabled server configuration MUST supply the `CommitGroupClose` callback declared in ServerConfig. The host wires this to `AssignmentStore.Close`. A grouped admission is rejected as misconfigured if either group authorization or close integration is missing.

`Server.CloseGroup` first marks the owner-side group closed under its group lock, rejecting new admissions and resumes, and closes all its sessions. It then calls CommitGroupClose outside the lock. A failure is returned to the caller and leaves the local group blocked. Repeating CloseGroup retries the idempotent persistence step. Retain the local tombstone until durable closure succeeds and 35 seconds pass from that success (ticket lifetime plus clock allowance), then release its slot. A blocked, uncommitted tombstone counts toward MaxActiveGroups and cannot be evicted to permit unsafe admission.

If closure races with ticket issuance, local closure rejects the ticket. Once the assignment is durably closed, bootstrap issues no more tickets. Delayed tickets have expired before local tombstone removal. A process crash during closure leaves its previous incarnation unavailable; another process cannot resurrect the group by taking its owner name.

The application MUST call CloseGroup explicitly when an instance ends. Session count reaching zero does not release ownership. A group can allow new members during owner draining only if those are actual resumes of existing sessions; all new session admissions, even for an existing group, are rejected.

## 10. Runtime, ownership, and bounded resource use

### 10.1 Scheduling and dispatch

Use a small fixed set of connection tasks: control reader/writer, datagram reader/writer, stream accept loops, one reader per active reliable input channel, and one writer per active reliable output channel. Request/custom-stream tasks are bounded by their separate concurrency slots. Never spawn a goroutine for every event or datagram.

Per-session queues hold parsed work. Handler mode uses a server-wide worker pool sized to `GOMAXPROCS` with a minimum of two, and a ready-session queue bounded to MaxSessions. A session holds a single scheduling token; a worker handles up to 32 ordinary items before requeueing a still-ready session for fairness. The token remains held during a callback, preserving serial execution. Client handler mode uses the same mechanism with one logical session. Incoming replies and control never use this ordinary callback pool.

Custom streams launch bounded handlers after their lifecycle marker has been delivered. They can execute concurrently with ordinary callbacks and MUST obey the stream concurrency limit. A blocking game callback can delay its session; the library cannot safely kill it. Do not launch replacement callbacks for the same session to work around a stuck callback. Host callbacks MUST observe cancellation; internal shutdown joins only library workers, and reports a deadline if host callbacks fail to return.

Polling mode preserves per-session ordering but makes no cross-session ordering guarantee. Ready items are round-robin across sessions. `Next` honors its context, and a closed client can drain queued terminal lifecycle information before returning SessionClosed. Polling applications MUST Release every acquired item even after decoding errors.

### 10.2 Budgets and queue behavior

Charge encoded outgoing payloads and allocated incoming payloads to per-direction session byte/count budgets and the global application byte budget before allocating/copying. Request and reply bodies are included; custom raw streams use transport flow control and bounded copy buffers. A pending request still consumes its request slot even after its outgoing payload is transmitted. Received Call results become application-owned and leave the library budget when returned.

Within each direction's application budget, reserve MessageBytes + 32 bytes and one item exclusively for request replies. Ordinary events and request bodies cannot consume this reserve; replies can use it and any other free capacity. This prevents a synchronous Call from deadlocking behind ordinary messages waiting for the same session's handler. If the global byte budget still prevents allocating an incoming reply, reset that request and resolve its Call with OutcomeUnknown wrapping ResourceExhausted instead of indefinitely waiting for its own blocked dispatcher. This is an observable failure, not a successful reply. No path exceeds the global budget to make progress.

For reliable receive pressure, leave the body unread until budget is available. If no budget becomes available within SlowConsumerTimeout, close the session with SlowConsumer. A declared length exceeding the message limit is a protocol error immediately, not a reason to wait for budget. Incoming unreliable payloads are discarded if they cannot acquire budget. A new sequenced event replaces the older queued event for its channel atomically, releasing the old charge; if the larger replacement cannot fit, retain the old item and count a dropped new event.

Outgoing sequenced events similarly replace the older SGSP-queued event for their channel; messages already handed to QUIC cannot be recalled. Ordinary unreliable events are FIFO in the local queue but gain no remote ordering guarantee. A successful replacement still consumes a new sequence number.

The datagram adapter maintains a conservative current encoded-size bound, initially the smaller negotiated cap. Enqueue returns TooLarge when the frame exceeds a known bound; MaxPayload reports the remaining application bytes for that specific envelope. If quic-go later rejects an accepted datagram because its packet limit changed, drop it, update the cached bound, and emit a local-drop/size observation. Do not retroactively claim that an earlier successful Send failed, and do not claim to know the exact current PMTU from an SGSP handshake. [Datagram size behavior](https://pkg.go.dev/github.com/quic-go/quic-go@v0.62.0#Conn.SendDatagram)

The control queue has its own finite reserve. Persistent control-queue exhaustion closes the connection; never drop a close/refresh result and pretend it succeeded. Session cleanup releases queued buffers, stops timers, cancels readers, and joins internal transport workers. Pooled idle buffers also count against the global budget; evict pooled buffers before rejecting active work solely because the pool retained unused memory.

### 10.3 Transport flow-control reservation

Cap each reliable stream's receive window at 64 KiB and the connection's receive window at 8 MiB by default; disable auto-growth above those caps. A connection may receive on 16 event streams plus both directions' 32 request and 8 custom-stream slots: at most 96 application receive streams. Keep at least one additional stream window available for control.

Configuration MUST satisfy:

```text
(ReliableChannels + 2 * (Requests + Streams) + 1) * StreamReceiveWindow
    <= ConnectionReceiveWindow
```

Set QUIC incoming unidirectional-stream credit to ReliableChannels, and incoming bidirectional credit to Requests + Streams + 1. SGSP still enforces separate request/custom budgets; the extra incoming slot is reserved for control where applicable and cannot be consumed by a 41st application stream. These bounds reserve flow-control capacity, not packet priority or bandwidth. QUIC receive-credit limits are not exact heap limits; include transport overhead and kernel buffers in memory measurements.

### 10.4 Defaults and admission

| Limit | Default | Enforcement |
| --- | --- | --- |
| ControlBytes | 16 KiB | Control JSON body |
| MessageBytes | 64 KiB | Reliable event/request/success body |
| DatagramBytes | 1,000 bytes | Complete encoded SGSP datagram |
| QueueBytes / QueueMessages | 256 KiB / 256 | Per session and direction; whichever fills first |
| ControlQueueBytes / ControlQueueMessages | 64 KiB / 16 | Separate control reserve per direction |
| ReliableChannels / DatagramChannels | 16 / 64 | Per direction and epoch |
| Requests / Streams | 32 / 8 | Per initiator and peer; both local and remote quotas |
| MaxSessions | 128 | Includes suspended sessions; configurable capacity, not a claim of maximum throughput |
| MaxPendingHandshakes | 64 | Includes authenticator calls still running |
| GlobalApplicationBytes | 256 MiB | Queued, borrowed, and pooled library payload bytes |
| PreAuthBytes | 32 KiB | SGSP reads per candidate, within 16 KiB control-message bound |
| StreamReceiveWindow / ConnectionReceiveWindow | 64 KiB / 8 MiB | Bounded QUIC receive credit |
| MaxActiveGroups / MaxGroupKeyBytes | 10,000 / 256 | Includes blocked local closure records |
| AuthTimeout | 5 seconds | Whole SGSP admission attempt |
| KeepAlive / IdleTimeout | 1 second / 5 seconds | QUIC liveness |
| CloseTimeout | 1 second | Logical close acknowledgment and notification bound |
| ResumeGrace | 30 seconds | From owner transport-loss detection |
| SlowConsumerTimeout | 5 seconds | Sustained reliable budget starvation |
| RequestTimeout | 5 seconds | Default call processing budget; wire upper bound 300 seconds |
| ReconnectMin / ReconnectMax | 250 ms / 2 seconds | Exponential full-jitter reconnect delay |
| HandshakesPerIPPerSecond / HandshakeBurstPerIP | 20 / 40 | Before authentication; IP is a rate key, never identity or affinity |
| MessagesPerSessionPerSecond / MessageBurstPerSession | 2,000 / 4,000 | Aggregate incoming events/requests/stream opens |

Use a bounded 4,096-entry source-IP token-bucket table with idle eviction after 60 seconds. If it is full and no idle entry can be evicted, reject new unknown-IP admissions temporarily; do not grow the table. The handshake concurrency bound also applies regardless of IP. A depleted authenticated-session rate bucket discards unreliable events; repeated reliable/request/stream excess closes with ResourceExhausted. The host can enforce stricter per-principal policies in authentication/admission hooks.

Do not preallocate the maximum transport window or application queue per session. MaxSessions and the receive-window formula together bound potential transport credit; the global application budget does not include the QUIC stack's independent buffers. A deployment MUST size session admission using measured total memory and CPU rather than treating 256 MiB as a complete process-memory ceiling.

## 11. Observability and shutdown verification

Required bounded-label observations:

| Observation | Measurement |
| --- | --- |
| `sessions` | Gauges by state, including suspended |
| `handshakes` | Attempts/results by numeric rejection code |
| `resumes` | Attempts, successes, terminal failures, superseded transports |
| `auth` | Verification/refresh duration and sanitized result code |
| `messages` | Counts/bytes by direction and delivery kind |
| `queue_bytes`, `queue_items`, `queue_age` | Current gauges and age histograms by direction/kind |
| `datagram_drop` | Local size/pressure drops and stale-sequence/coalescing drops |
| `requests` | Active gauge, latency, result codes, and unknown outcomes |
| `streams` | Active request/custom counts by initiator |
| `protocol_errors`, `handler_panics` | Counts by bounded reason code |
| `transport` | RTT, aggregate bytes, transport packet loss when available |
| `placement` | Assignment/read/close results and latency |

Do not label metrics with session IDs, principal identities, group keys, arbitrary application message types, or endpoint strings. Per-session Stats is explicitly requested local data, not a metric-label template. Transport packet-loss statistics are not equivalent to confirmed SGSP message delivery or loss. Do not expose a precise per-datagram remote-delivery counter without transport evidence.

Update counters/histograms without executing observers on the gameplay hot path. Use fixed histogram buckets from 1 microsecond to 1 second in powers of two, plus an overflow bucket. Flush aggregate observations once per second through a 4,096-item bounded observer queue and one observer task. A slow observer loses observations and increments a local counter; it cannot stall gameplay. Observers MUST return on endpoint shutdown; the host contract for a blocking observer is the same as for a blocking game callback.

Graceful shutdown phases are: set draining; reject new logical admissions; continue existing traffic and resumes; wait for session count zero or drain deadline; invalidate remaining sessions and attempt bounded close notification; cancel all transport tasks; close the UDP socket; release budgets/timers; return. Never advertise a clean shutdown if owned internal workers remain alive. Database outage during group-close persistence must be reported; it does not authorize moving a group to another owner.

## 12. Performance and network verification protocol

### 12.1 Reproducible benchmark command

M8 MUST implement `cmd/sgspbench` with these flags:

```text
--implementation sgsp|quic
--clients N
--hz 60|128
--input-bytes N
--update-bytes N
--rpc-per-second N
--bulk-bytes-per-second N
--warmup DURATION
--duration DURATION
--rtt DURATION
--jitter DURATION
--loss FRACTION
--reorder FRACTION
--seed INTEGER
--output PATH
```

The baseline sends a 64-byte input per client per tick, a 512-byte update back to each client per tick, two 128-byte request/reply exchanges per client per second, and 64 KiB/s of reliable bulk data per client. Inputs and updates each use a separate sequenced-unreliable channel. Bulk streams stay open and are paced; do not send unlimited data in the background. Payload byte counts exclude SGSP framing. The example protocol embeds send tick and sequence in the fixed-size payload to observe application update age without adding protocol ACKs.

Use 10 seconds warmup, 60 seconds measurement, and five trials per acceptance point with seeds 1..5. Run at 60 and 128 Hz. Start at 1, then 8, 32, 64, and 128 clients; continue doubling only after increasing admission limits and confirming memory/CPU headroom. Never bypass budgets to obtain a higher throughput number.

The `quic` mode MUST use the same pinned transport, certificates, packet sizes, channel/request topology, congestion configuration, message framing bytes, and workload generator. Bypass only SGSP session/dispatch/codec wrappers that are being measured. Give the baseline equivalent bounded application queues so memory growth is not mistaken for performance. Include a second codec microbenchmark for raw bytes versus the supplied JSON codec; do not conflate codec cost with transport overhead.

### 12.2 Fault injection

Use two actual UDP relay sockets per proxy path, forwarding packets rather than decoding QUIC. Apply seeded Bernoulli loss to each direction, base delay RTT/2 per direction, and uniform jitter in `[-jitter,+jitter]` clamped at zero delay. For the selected reorder fraction, add one extra base one-way delay to that packet. Jitter/reordering operate on the relay's bounded scheduled-packet heap, not on application messages. Keep independent random streams for the two directions.

The relay MUST cap pending packets and bytes (65,536 packets / 64 MiB); exceeding either fails the experiment with `harness_overload`, rather than silently attributing its additional drops to SGSP. The generator must similarly flag inability to produce the scheduled workload. Capture relay CPU and scheduled backlog so its bottleneck is visible. This harness needs ordinary UDP sockets, not privileged system-wide traffic shaping.

Required matrix: RTT 20/50/100 ms, loss 0/1/5%, and both 60/128 Hz. Use zero jitter/reordering for the full base matrix, then repeat 50 ms RTT with 10 ms jitter and 2% reorder at each loss level. Test the healthy baseline at the highest passing capacity; run the full impairment matrix at half that capacity to separate loss response from intentional overload. Reconnect tests separately blackhole an established connection for 2, 8, and 40 seconds: test short transport recovery, resume within grace, and expiry beyond idle detection plus grace. Use authentication lifetimes longer than the experiment so they do not confound the result.

### 12.3 Measurements and gates

Record per trial: UTC timestamp; OS/kernel; CPU model and available cores; RAM; Go version; dependency versions; GOMAXPROCS; build flags; configuration; payload sizes; offered/accepted/delivered counts; CPU; RSS and Go live heap; allocations and bytes per operation; goroutines; queue occupancy/age; local/coalesced/observed network drops; RTT; request outcomes; and reconnect timing.

Measure SGSP send overhead from API entry to handoff to the transport adapter, and receive overhead from a complete decoded frame to handler invocation/poll return. Report both distributions independently and the combined per-direction local budget (send plus receive) using correlated synthetic samples. Exclude time spent inside transport transmission/retransmission, network transit, and the game's own tick queue. The polling benchmark must continuously drain items for the library-overhead measure; report a separate tick-drained result for application update age. A same-host relay harness uses one monotonic clock domain; cross-host one-way timings require clock synchronization and must not be inferred from unsynchronized wall clocks.

Correlate samples by client index, direction, and the application sequence embedded in the payload. Benchmark-only adapter instrumentation records timestamps in preallocated bounded buffers; it does not use the aggregate Observer callback or send extra protocol acknowledgments. Compute summed local durations only for matched deliveries and publish the unmatched/dropped fraction beside that distribution. Any sample-buffer overflow invalidates the run. Decoder/codec microbenchmarks separately account for costs excluded by the queue-and-dispatch timing boundary.

Initial release gates:

1. At least the 32-client, 128 Hz healthy profile must complete on a documented reference machine with at least four physical cores, GOMAXPROCS=4, and 8 GiB RAM. This is a target to verify, not an existing capacity claim.
2. At the published healthy operating capacity, p99 local SGSP send-plus-receive queue/dispatch overhead is below 1 ms in every trial. Use the same-host correlated workload to compute the sum; do not add uncorrelated p99 values and label that a p99 distribution.
3. No healthy-profile drops from SGSP queue overflow, rate limits, or harness overload; sequenced replacement is reported separately and must be below 0.1% of offered updates. Both wire and application throughput are reported.
4. Go live heap and library-owned goroutines reach a plateau under a ten-minute slow-consumer/overload run, and return to the documented warmed baseline after teardown and GC. Budgets stay within their configured values throughout.
5. Loss/jitter/reordering produce no panic, stale-epoch execution, broken channel ordering, or silent request replay. Report delivery age and unknown outcomes instead of asserting perfect datagram delivery.
6. Publish SGSP versus bare-QUIC CPU, memory, allocation, and latency results even when the acceptance target fails. A failed target is an unpassed gate, not permission to relabel a smaller workload as the promised profile.

quic-go's documentation flags datagram performance as an optimization area. If it prevents these gates, profile first and document the bottleneck; do not claim the selected library meets the action-game target based on an echo demo. [Datagram performance notes](https://quic-go.net/docs/quic/datagrams/)

## 13. Required test scenarios

Place deterministic behavior tests beside the owning package. Keep real-network tests in `integration` and PostgreSQL tests in their separate module. The names below are required test names so milestone commands cannot quietly select zero tests. Additional table cases should retain these top-level names.

### 13.1 Protocol and adapter

| Test | Setup and action | Required result |
| --- | --- | --- |
| `TestGoldenVectors` | Encode and decode every vector in 4.5 using independent expected literals. | Exact bytes and decoded fields; no encode-then-decode-only oracle. |
| `TestFrameBoundaries` | Feed one byte at a time; truncate at every byte; send multiple control frames in one read; advertise lengths above caps. | Correct incremental parsing; truncation fails; oversized lengths reject before body allocation. |
| `TestMalformedControl` | Send duplicate keys, wrong types, extra roots, invalid UTF-8, excessive nesting, and unknown required capability. | Explicit rejection; authenticator and game handlers are not called. |
| `TestStreamRoles` | Open wrong-direction streams, duplicate a reliable channel, send an unknown stream kind, and send extra request bytes. | Specified reset/ProtocolViolation; no misclassification as game data. |
| `TestDatagramValidation` | Inject bad varints, oversized channel/type values, missing sequence, and oversized datagrams. | Bounded drop and counters; session remains usable unless rate policy closes it. |
| `TestCapabilityNegotiation` | Connect a peer with datagrams disabled to a game client requiring them. | UnsupportedCapability before gameplay; no reliable substitution. |
| `TestQUICChannels` | With ample flow-control credit, stall reliable channel A and send on B; exchange datagrams and requests in both directions. | B makes progress independently of A's byte order; all declared modes work on actual QUIC. |
| `TestStreamCancellation` | Reset a request/custom stream while its peer is blocked reading or writing. | Both tasks unblock by deadline; session remains usable; slots are released. |

### 13.2 API and runtime

| Test | Setup and action | Required result |
| --- | --- | --- |
| `TestSendOwnership` | Send a pooled byte slice; mutate/reuse the caller slice immediately after success. | Receiver observes the original bytes; race detector is clean. |
| `TestReceiveOwnership` | Deliver borrowed data in handler and polling modes; retain only explicit copies; Release twice. | Borrowed lifetime is documented; copies survive reuse; double Release does not double-free a budget. |
| `TestBidirectionalRequests` | Client and server concurrently call registered handlers; reply out of order; saturate ordinary event queues before a reply arrives. | Each Call receives its own response; incoming replies bypass the busy ordinary dispatcher and use their reserved budget. |
| `TestUnknownRequestOutcome` | Block the response after the handler commits a counted operation; cancel/lose transport and then resume. | OutcomeUnknown=true and underlying cause preserved; commit count stays one without a new explicit call. |
| `TestPreSendFailure` | Fill request slots or cancel before transport handoff. | No handler execution; OutcomeUnknown=false. |
| `TestUnknownRoutes` | Send unregistered events/requests/streams with and without OnUnknown. | Fallback sees events/requests; absent fallback drops events and returns UnsupportedMessage for requests/streams. |
| `TestTypedCodecs` | Exercise raw and JSON codecs; inject a decoder error. | Typed helpers preserve raw delivery semantics; failed decode never invokes typed handler. |
| `TestQueueBackpressure` | Stop consumption, fill byte and count budgets independently, TrySend and Send with deadline. | Immediate Backpressure versus bounded context error; budgets never exceed limits. |
| `TestSequencedCoalescing` | Hold dispatch, enqueue sequences 1, 3, 2, duplicate 3, then 4; repeat with a larger replacement that cannot fit. | Only current accepted update is delivered; stale/duplicate counters correct; failed larger replacement preserves the old charge and watermark. |
| `TestWrongDispatchMode` | Call Next in handler mode and configure a router in polling mode. | Explicit WrongMode/configuration errors. |
| `TestDispatchOrdering` | Use multiple workers and sessions with barriers around callbacks and lifecycle transitions. | Ordinary callbacks never overlap within a session; other sessions progress; Opened/Resumed ordering holds. |
| `TestHandlerPanic` | Panic from a handler while another session exchanges messages. | Affected session closes; other session and server remain live; worker resources return. |
| `TestSlowConsumer` | Exhaust reliable receive budget and advance fake time past the configured timeout. | SlowConsumer closure; control cleanup works; no unbounded extra reader allocation. |
| `TestGlobalBudget` | Use many sessions and retained polling items/pool buffers to reach the aggregate budget. | Allocation stops at the application budget; releasing items permits progress. |

### 13.3 Authentication, sessions, and fencing

| Test | Setup and action | Required result |
| --- | --- | --- |
| `TestAuthenticationBarrier` | Pause authenticator; send events, datagrams, requests, and streams before release. | Zero gameplay callbacks before successful commit; rejected credentials never allocate an active session. |
| `TestJWTValidation` | Test missing/wrong issuer, audience, type, kid, algorithm, signature, expired exp, and future nbf/iat. | Every invalid case fails for the expected category; valid EdDSA succeeds; admission JWT cannot be used as identity JWT. |
| `TestAuthRefresh` | Refresh to same principal, different principal, invalid token, and near-expiry valid token. | Only valid same-principal refresh changes expiration; failed attempts retain current identity; expired sessions close. |
| `TestRevocation` | Create several sessions for the same issuer/subject and one different issuer with the same subject; revoke the first identity. | Matching sessions close and cannot resume; the other issuer is unaffected. |
| `TestResumeIdentity` | Set attachment, lose transport, resume before grace. | Same ID/owner/attachment, larger epoch, one Resumed event per committed epoch. |
| `TestResumeCredentialBinding` | Try wrong subject/issuer, secret, owner, incarnation, app/version, or changed receive limits. | Rejection leaves an existing active session untouched. |
| `TestConcurrentResume` | Authenticate 100 candidate resumes, release their commit barrier together. | Exactly one current epoch/connection at a time; previous winners are superseded; stale readers cannot dispatch. |
| `TestLostResumeWelcome` | Commit resume, discard response, kill that connection, retry with original secret. | Retry succeeds before deadlines; no secret-rotation lockout. |
| `TestResumeGrace` | Use fake clock at grace-minus-one-tick and at/after grace; repeat with earlier auth expiration. | Only eligible resume succeeds; terminal session never reopens; failed auth does not extend grace. |
| `TestEpochFence` | Queue epoch-1 input, commit epoch 2 before dispatch, then try old reply/stream write and consume a game inbox. | Queued old input is dropped, old reply/stream fails, game inbox rejects old epoch; already committed work is not rolled back. |
| `TestTransportLossCleanup` | Disconnect with queued events, pending requests, streams, and timers. | Session suspends; queued payloads release; requests resolve; streams unblock; attachment remains. |
| `TestClientReconnectLoop` | Inject deterministic RNG/clock and repeated network failures followed by success or terminal rejection. | One loop, bounded jitter/backoff/deadline, fresh credential calls, no bootstrap fallback. |
| `TestNATRebinding` | With actual QUIC, change the relay's upstream source port while preserving forwarding. | Connection either survives with the same epoch, or follows documented transport-loss resume; the test separately records which behavior the adapter supports. |

For `TestNATRebinding`, capability acceptance requires the pinned adapter to demonstrate same-epoch recovery in a supported NAT-rebinding setup; merely falling back to resume verifies cleanup but does not pass the adapter's migration case.

### 13.4 Placement and operations

| Test | Setup and action | Required result |
| --- | --- | --- |
| `TestConcurrentAssignment` | Start 100 Assign calls for the same key with at least three candidates. Run against memory and real PostgreSQL. | One stored owner; every success returns that owner; no empty result from an insertion race. |
| `TestAssignmentVersionAndClose` | Assign, request a conflicting version, close with wrong/right owner, close again, then Assign again. | Version mismatch fails; wrong owner cannot close; same-owner close is idempotent; closed key never reopens. |
| `TestAdmissionBinding` | Modify each ticket binding and attempt grouped/ungrouped admission. | Wrong identity/app/group/owner/incarnation/endpoint/expiry fails; original valid ticket succeeds only on its target. |
| `TestPlacementOutage` | Establish gameplay; make bootstrap/store fail; attempt new group placement and an existing direct resume. | New placement fails explicitly; gameplay/direct resume need no store call. |
| `TestUnhealthyOwner` | Assign a group to A, mark A unavailable, then add healthy B and resolve the group. | No reassignment; explicit unavailable result; new unrelated groups can use B. |
| `TestOwnerRestart` | Restart server ID A with a new incarnation and try old ticket/resume. | Neither can recover previous sessions or groups under the new process. |
| `TestGroupCloseRace` | Race ticket issuance/admission with CloseGroup; fail persistence, retry closure, advance ticket expiry. | Local admission stays blocked; persistence failure is reported; safe tombstone eviction occurs only after commit and retention. |
| `TestDraining` | Drain with a deadline while one session is active and another suspended; attempt new and resumed admissions. | New sessions fail; eligible resume succeeds before deadline; forced closure happens at deadline. |
| `TestShutdownCleanup` | Repeat connect/request/disconnect/resume/close 1,000 times with no blocking host callbacks. | Internal task/budget/timer counts return to zero; warmed heap/goroutine measurements return to baseline tolerance. |
| `TestSlowObserver` | Block observer consumption and exceed observer queue capacity. | Gameplay continues; queue remains bounded; lost-observation counter increases. |
| `TestAdmissionAndRateLimits` | Exhaust session, pre-auth, source-IP-table, stream/request, and message-rate limits. | Each bound rejects/drops according to its documented rule; resume never creates a second logical session slot. |

Use explicit barriers and fake time for races and timeout tests; do not rely on arbitrary sleeps to make a race "likely." Add randomized schedules with recorded seeds after deterministic cases pass. Test the actual SQL transaction, not a mock returning the proposed owner.

## 14. Ordered implementation milestones

### M0 — establish the module and test harness

Prerequisite: read both documents. Create the nested module and package skeleton from section 2, public API declarations with behavior still clearly unimplemented, and deterministic clock/transport test utilities. Pin dependency baselines and record any required compatibility adjustment. The fake transport must independently block writes, signal delivery, reset streams, drop datagrams, reorder messages, and expose current live-task/budget counters.

Gate: `go env GOMOD` points to the library module, package discovery excludes sibling workspace projects, the harness's own clock/barrier tests pass, and example API signatures compile. Do not count placeholder implementations as feature completion.

### M1 — implement framing and errors

Prerequisite: M0. Implement pure codecs, control validation, error mapping, and the literal vectors before networking. Commit protocol testdata files consumed by independent decoders.

Gate: all protocol tests in 13.1 that do not require QUIC pass. Fuzz control, event, and request decoders separately with bounded allocations and no panics. The decoder MUST reject oversized declarations before acquiring body memory.

### M2 — implement the QUIC adapter

Prerequisite: M1. Map stream kinds, actual negotiated datagram capabilities, TLS verification, deadlines, flow-control limits, resets, and statistics. Add a localhost test certificate issued by a test CA; the client trusts that CA explicitly. Never set InsecureSkipVerify in examples or integration tests to avoid implementing certificate setup.

Gate: real-QUIC channel/cancellation/capability tests pass, a wrong server certificate fails, zero-RTT application delivery is impossible, and configured stream/credit limits are observed. Record the adapter's NAT-rebinding behavior.

### M3 — implement bounded messaging and APIs

Prerequisite: M2. Implement queues, raw and typed messaging, request/response, streaming, ownership rules, handler/polling dispatch, and observers. Until M4, use an explicitly test-only accepted-session fixture; do not ship a public unauthenticated mode as a shortcut.

Gate: every API/runtime test in 13.2 passes under the race detector except the explicitly deferred resumed branch of TestUnknownRequestOutcome and supersession branch of TestDispatchOrdering, which require M5. Use accepted-session fixtures for the M3 branches. Queue count, byte, and global budgets have separate tests; Call cancellation distinguishes known pre-send failure from unknown outcome. Record deferred branches as pending, not passed.

### M4 — implement authentication and initial admission

Prerequisite: M3. Implement TLS-to-HELLO-to-WELCOME flow, authenticator hooks, JWT helpers, credential refresh, expiration, admission-ticket verification, and Revoke. Add the generic hooks without forcing JWT onto host applications.

Gate: authentication/JWT/refresh tests and the immediate-closure branch of revocation pass, authentication cannot be bypassed with an early stream/datagram, and sensitive credential fixtures are absent from captured logs/observations. The resume-rejection branch of revocation is explicitly pending until M5.

### M5 — implement continuity and terminal cleanup

Prerequisite: M4. Implement the state machine, stable resume secret, epochs, grace timers, client reconnect loop, closure acknowledgment, and shutdown. A reconnect must reattach the same session object rather than allocate a new logical session.

Gate: all session/fencing/reconnect tests in 13.3 and all M3/M4 deferred branches pass, including deterministic lost-response and simultaneous-resume races. Run shutdown cleanup repeatedly under `-race`; pending operations and budgets must resolve on every exit path.

### M6 — implement placement and group lifecycle

Prerequisite: M5. Implement bootstrap Resolve, memory registry/store, default selector, PostgreSQL store/migration, admission signing, and group-close callback wiring. Keep the placement service off the direct resume path.

Gate: run placement tests against both the in-memory adapter and a disposable PostgreSQL instance. Multiple bootstrap instances must share one winner for concurrent group joins. Prove closure, owner restart, and database outage behavior. An unavailable PostgreSQL service means the PostgreSQL gate is incomplete, even if unit tests pass.

### M7 — build an executable integration example

Prerequisite: M6. Create `examples/action` using a 128 Hz authoritative Go tick loop, sequenced input/update channels, one reliable command type, typed inventory request/reply, and a raw stream. Use operation IDs for a state-changing command and explicit game-inbox epoch checks. Provide direct mode and two-owner bootstrap mode, with local CA/certificate generation and an explicit development identity issuer.

Gate: documented commands start the example from a fresh checkout. A client can authenticate, join a group, exchange all communication kinds, lose its connection, resume the same session, and reconcile state. Two clients joining the same group reach one owner. Killing that owner ends the session and does not cause hidden recovery on another process.

### M8 — establish release evidence

Prerequisite: M7. Implement the benchmark/UDP-relay harness and run section 12. Publish machine-readable trial results and a short human-readable report covering healthy capacity, impairment results, transport comparison, and identified bottlenecks.

Gate: all tests, race checks, fuzz runs, SQL integration, shutdown/resource checks, and performance targets have actual results. If a performance gate fails, preserve its output and report the feature as not release-ready; do not mark M8 complete based on functionality alone.

## 15. Verification commands and evidence format

These commands are requirements for the future implementation. They have not been run as library checks during documentation creation. Run from the SGSP module root after the named milestone creates the corresponding packages/tests.

### 15.1 Routine checks

```sh
go env GOMOD
go list ./...
go test -count=1 ./...
go vet ./...
go test -race -count=1 ./...
gofmt -l .
```

Expected: correct nested module, only SGSP packages, all selected tests executed and passed, no vet/race diagnostics, and no files printed by gofmt. A test output saying `[no tests to run]` for an expected gate is failure of the gate setup, not success.

### 15.2 Focused stress and fuzzing

```sh
go test -race -count=100 -run 'Test(ConcurrentResume|EpochFence|LostResumeWelcome|DispatchOrdering)$' .
go test -race -count=20 -run 'Test(ConcurrentAssignment|AssignmentVersionAndClose)$' ./placement/...
go test -count=10 -run 'Test(ShutdownCleanup|SlowConsumer|GlobalBudget)$' ./...
go test ./internal/wire -run '^$' -fuzz '^FuzzControl$' -fuzztime=60s
go test ./internal/wire -run '^$' -fuzz '^FuzzEvent$' -fuzztime=60s
go test ./internal/wire -run '^$' -fuzz '^FuzzRequest$' -fuzztime=60s
```

Commit every reproducible fuzz failure as a regression seed after fixing it. Record the Go/toolchain version and duration. Extend to ten minutes per fuzz target for M8. A decoder timeout/large allocation is a failure even if it does not panic.

### 15.3 PostgreSQL integration

The nested integration module MUST accept `SGSP_TEST_DATABASE_URL`, create a unique disposable schema for each test run, apply the migration there, and remove only its own schema afterward. It MUST reject a missing URL with a clear incomplete-check result and MUST NOT run migrations in the database's default public schema. Configure search_path on every connection through the pgx connection configuration before opening the pool; setting it once through a pooled DB.Exec is insufficient. Generate schema names from a fixed test prefix and random hexadecimal suffix, and quote identifiers correctly. Never print the connection string, which can contain credentials.

```sh
go -C integration/postgres test -race -count=20 -v ./...
```

Pass the environment variable through the environment rather than embedding secrets in command text or committed files. Require TestConcurrentAssignment and TestAssignmentVersionAndClose to report actual executed cases. Test migration reapplication separately: applying the same version through the migration runner twice is a recorded no-op, while schema drift/version mismatch is a reported error. The host-triggered migration runner owns a schema-local migration-version table and a transaction/advisory lock so simultaneous runners cannot apply the same migration twice.

### 15.4 Example benchmark invocation

```sh
go run ./cmd/sgspbench --implementation sgsp --clients 32 --hz 128 --input-bytes 64 --update-bytes 512 --rpc-per-second 2 --bulk-bytes-per-second 65536 --warmup 10s --duration 60s --rtt 20ms --jitter 0ms --loss 0 --reorder 0 --seed 1 --output artifacts/sgsp-32-128-seed1.json
go run ./cmd/sgspbench --implementation quic --clients 32 --hz 128 --input-bytes 64 --update-bytes 512 --rpc-per-second 2 --bulk-bytes-per-second 65536 --warmup 10s --duration 60s --rtt 20ms --jitter 0ms --loss 0 --reorder 0 --seed 1 --output artifacts/quic-32-128-seed1.json
```

The benchmark command MUST create its output parent directory, return nonzero for harness overload or an invalid/incomplete run, and serialize its full configuration with the result. Repeat the specified seeds and matrix; two successful invocations do not satisfy M8. Use separately built benchmark binaries for final capacity measurements and report their exact build command; `go run` invocations above verify the CLI and workload.

### 15.5 Completion evidence

For each milestone, retain a report with: commit/revision, requirement and test IDs covered, exact command, exit code, actual selected tests and counts, start/duration, environment, artifact paths, and any failure/skip. Do not infer SQL conformance from a memory-store test or performance from a functional echo test.

| Design requirement | Implementation contract | Required evidence |
| --- | --- | --- |
| R1 | Section 3 API and typed helpers | M3 API examples compile; typed/raw ownership and dispatch tests |
| R2 | Sections 3–4 messages, requests, and streams | Bidirectional request and custom-stream tests; M7 example |
| R3 | Sections 4–6 delivery semantics | Golden vectors, channel/capability tests, loss and sequencing cases |
| R4 | Section 8 ownership and resume | Resume identity, grace, lost-WELCOME, epoch-fencing, and concurrent-resume tests |
| R5 | Section 9 atomic placement | Actual PostgreSQL concurrency, closure, outage, and restart tests |
| R6 | Sections 5 and 7 authentication | Barrier, JWT, refresh, revocation, and ticket-binding tests |
| R7 | Sections 10–11 bounded runtime | Independent byte/count/global caps, slow-consumer, rate, shutdown, and observer tests |
| R8 | Section 12 performance protocol | M8 five-trial results, impairment matrix, bare-QUIC comparison, and plateau evidence |
| R9 | Sections 2 and 9 optional integrations | M7 direct mode with no bootstrap/database; assigned mode without a REST dependency |
| R10 | Sections 4, 13–15 implementation handoff | Literal vectors, executed named tests, and complete milestone reports |

The documentation task is complete when DESIGN.md and this file agree, links and examples are structurally valid, and all requirements have explicit verification procedures. The library task is complete only after M0–M8 have passed. Neither status implies the other.

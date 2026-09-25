# SGSP — Simple Game Server Protocol

SGSP is a Go library for authenticated, session-based communication between game
clients and authoritative servers over QUIC. It combines reliable messages,
unreliable state updates, request/reply, and application streams with session
resumption and optional group-to-owner placement.

The application owns its simulation, message schemas, identity policy, and game
state. SGSP supplies the connection and session machinery. The implementation
uses `quic-go` for transport and exposes byte-oriented APIs plus generic typed
helpers. See [implementation status](docs/IMPLEMENTATION_STATUS.md) for the
verification history and [benchmarking](docs/BENCHMARKING.md) for performance
methodology.

## Getting started

This checkout requires **Go 1.27.1 or newer**. QUIC uses UDP, so the selected
server port must be reachable over UDP.

```sh
git clone git@github.com:qattidev/sgsp.git
cd sgsp
go test ./...
```

The module currently declares `qattidev/sgsp`, and source imports use that path.
For a separate application using a local checkout, add a replacement to its
`go.mod`:

```go
require qattidev/sgsp v0.0.0

replace qattidev/sgsp => ../sgsp
```

Adjust the relative path to your checkout and run `go mod tidy` after adding
imports. The database submodules are optional; the core library and development
examples work without initializing them.

### Run a client and server

From the repository root, start the action server:

```sh
go run ./examples/action -mode server -listen 127.0.0.1:4444
```

In another terminal at the same directory, start a client:

```sh
go run ./examples/action -mode client -server 127.0.0.1:4444
```

The example generates local development certificates and identity keys in
`.sgsp-action-dev/`. It demonstrates sequenced input and snapshots, a reliable
command, a typed inventory request, and a raw stream against a 128 Hz server
loop. See its [README](examples/action/README.md) for bootstrap mode and setup
details. Use application-managed certificates and credentials when deploying.

| Example | Purpose |
| --- | --- |
| [Pong](examples/pong/README.md) | Playable two-client terminal game with authoritative state and reconnects |
| [Terminal Tag](examples/tag/README.md) | Two-player game with auto play, sequenced updates, and live statistics |
| [Action](examples/action/README.md) | Scripted protocol exercise, simulation inbox, and optional placement |
| [API](examples/api/main.go) | Minimal typed message and request declarations |

Pong and Tag have separate Go modules to isolate their terminal UI dependencies.

## Implementation

### Transport and framing

`internal/quictransport` adapts `quic-go` to the internal transport interfaces.
Connections use TLS 1.3 and ALPN `sgsp/1`; certificate verification is required,
`InsecureSkipVerify` is rejected, and 0-RTT is disabled. SGSP's binary frame
encoding and validation live in `internal/wire`.

A control stream handles session establishment and lifecycle operations.
Reliable event channels use QUIC streams; unreliable events use QUIC datagrams.
Requests and custom streams use separate bidirectional streams. Receive windows,
stream counts, frame sizes, and application queues are bounded. Peers advertise
receive limits, and outbound payload limits account for the remote receiver.

### Endpoint setup

Create a server with `sgsp.NewServer(ServerConfig)` and call
`server.Serve(ctx, packetConn)` with an application-owned `net.PacketConn`.
Supply TLS credentials, an application ID/version, owner ID and advertised
endpoint, an `Authenticator`, and a dispatch configuration. Leave the configured
owner incarnation zero: construction generates it, and `server.Owner()` returns
the actual identity to publish to a registry.

Clients connect with `sgsp.Dial(ctx, endpoint, ClientConfig)`, supplying trusted
TLS configuration, the same application identity, a credential provider, and
dispatch configuration. The returned `Client` exposes its logical `Session`.
For placement-based connections, also supply the resolved owner, group key, and
admission ticket. `GameRole` and `BootstrapRole` distinguish endpoint roles.

The server exposes session enumeration, identity revocation, group closure, and
draining. The application owns the server's packet socket and serving context;
it also owns any databases or registries it supplies.

### Authentication and admission

`Authenticator` maps a credential to a `Principal` containing issuer, subject,
expiry, and optional attributes. Authentication completes before application
traffic is accepted. `GroupAuthorizer` supplies application-specific access
checks. Session authentication can be refreshed; expiry and revocation end
access.

The optional `auth/jwt` package implements an Ed25519 JWT profile with separate
identity and admission token types, issuer/audience validation, key IDs, and
replaceable verification keys. Admission tickets bind the principal, application
version, group, and complete owner identity, including its process incarnation.
Configure `RequireAdmission` and an admission verifier on owners that require
bootstrap-issued tickets.

### Delivery modes

| Mode | Behavior | Typical use |
| --- | --- | --- |
| `ReliableOrdered` | Reliable, ordered delivery within a channel on the current connection | Commands and discrete events |
| `Unreliable` | Datagram delivery without retransmission or ordering guarantees | Transient notifications |
| `UnreliableSequenced` | Datagram delivery with stale sequence rejection and queued-update coalescing | Inputs and state snapshots |

Send events with `Session.Send` or `Session.TrySend`, choosing a channel and
mode through `SendOptions`. Reliable ordering is per channel; there is no global
ordering across channels. Datagram payloads must fit the negotiated limit.
Queue admission can fail with `ErrBackpressure`, and oversized payloads return
`ErrTooLarge`. Successful send admission is not an application acknowledgment.

`Session.Call` performs request/reply. An incoming request can `Reply` or `Fail`.
A connection loss after a request may leave its result unknown; inspect
`ErrOutcomeUnknown` rather than assuming the operation did not execute. Use
application operation IDs and deduplication for retryable mutations.

`Session.OpenStream` creates an application byte stream identified by message
type. Streams support deadlines, half-close, and abort. The application owns the
stream's payload format and must close or abort it when finished.

### Typed messages

`Message[T]`, `Request[I, O]`, `Emit`, `Call`, `OnMessage`, and `OnCall` wrap the
byte-oriented API. `JSON[T]()` is provided; implement `Codec[T]` for another
encoding. Both peers must agree on message IDs and schemas.

```go
package game

import (
    "context"

    "qattidev/sgsp"
)

type Input struct {
    Tick uint64  `json:"tick"`
    Move float32 `json:"move"`
}

var Inputs = sgsp.Message[Input]{ID: 10, Codec: sgsp.JSON[Input]()}

func RegisterInput(router *sgsp.Router, enqueue func(sgsp.Session, Input)) error {
    return sgsp.OnMessage(router, Inputs,
        func(ctx context.Context, session sgsp.Session, input Input) error {
            enqueue(session, input)
            return nil
        })
}

func SendInput(ctx context.Context, session sgsp.Session, input Input) error {
    return sgsp.Emit(ctx, session, Inputs, input, sgsp.SendOptions{
        Channel:  1,
        Delivery: sgsp.UnreliableSequenced,
    })
}
```

Keep the simulation inbox bounded and handle its overload policy in the
application. For deferred game-state mutations, capture the session epoch and
recheck it before committing work; the [action example](examples/action/main.go)
shows that integration.

### Dispatch and memory ownership

Choose one dispatch mode when constructing an endpoint:

- **Handlers:** provide a `Router` with event, request, stream, and lifecycle
  callbacks. A bounded worker pool allows different sessions to run concurrently
  while serializing callbacks for each session. Register routes before endpoint
  construction; the endpoint snapshots the router.
- **Polling:** omit the router and consume `Incoming` values with `Client.Next`
  or `Server.Next`. Call `Incoming.Release()` after processing each value to
  return its buffer and queue budget.

Handler dispatch releases incoming envelopes after callbacks return. Copy any
payload you need to retain. Avoid blocking callbacks on lengthy simulation or
storage work. `Incoming.Context()` and `Incoming.Epoch` identify the work's
connection lifetime; session attachments can hold application-owned state.

### Sessions and reconnection

A logical session progresses through connecting, authenticating, active,
suspended, and closed states. An interrupted transport can suspend the session,
and the client automatically attempts reconnect/resume unless
`DisableReconnect` is set. A successful resume advances the epoch, fencing work
from the old connection. Lifecycle notifications include opened, connection
lost, resumed, authentication refreshed, and session ended.

Resume is bounded by the grace period and credential expiry, and remains tied
to the same owner incarnation. It does not recover game state after an owner
process restarts or migrate a session to another owner. Messages, requests, and
streams are not transparently replayed as application transactions. Applications
must decide how to resynchronize state after resumption.

### Limits and observability

A zero `Limits` value selects all defaults. To customize limits, start with
`sgsp.DefaultLimits()` and change individual fields; partially populated limits
are rejected.

Selected defaults in the current implementation:

| Limit | Default |
| --- | --- |
| Reliable message payload | 64 KiB |
| Datagram payload | 1,000 bytes |
| Per-session queue budget | 256 KiB / 256 messages |
| Sessions / pending handshakes | 128 / 64 |
| Outstanding requests / custom streams | 32 / 8 |
| Global application byte budget | 256 MiB |
| Authentication / idle timeout | 5 seconds / 5 seconds |
| Resume grace | 30 seconds |
| Reconnect delay bounds | 250 milliseconds–2 seconds |

There are also control-queue, receive-window, group, handshake-rate, and
message-rate limits. See [`DefaultLimits`](api.go) for the full configuration.
Slow consumers and exhausted budgets produce explicit errors or session closure;
sequenced datagrams can be coalesced to keep more recent updates.

`Session.Stats()` exposes transport availability, RTT, byte counts, queue sizes,
and datagram drop/coalescing counters. An optional `Observer` receives bounded,
aggregated observations through a separate queue. A slow observer can lose
observations without blocking gameplay traffic. Placement has its own optional
observation hooks.

## Placement and database adapters

The optional `placement` package resolves groups to owners and signs admission
tickets. A registry supplies current owner health, draining, and capacity
information. An injected `AssignmentStore` persists the winning owner for each
application/group key. Version mismatches and closed groups are rejected;
unavailable owners do not cause automatic reassignment.

`placement/memory` remains in the core repository for development and tests.
Durable stores are independent Git submodules and Go modules:

| Backend | Repository | Local checkout |
| --- | --- | --- |
| PostgreSQL | [sgsp-adapters-postgres](https://github.com/qattidev/sgsp-adapters-postgres) | `adapters/postgres` |
| SQLite | [sgsp-adapters-sqlite](https://github.com/qattidev/sgsp-adapters-sqlite) | `adapters/sqlite` |

```sh
git submodule update --init --recursive
```

Both adapters own their SQLC queries and generated code, Goose migrations,
database configuration, and integration tests. Applications open the database,
apply migrations explicitly, construct a store, and inject it into
`placement.BootstrapConfig.Store`. Core SGSP imports neither adapter nor its
database driver. Wire `ServerConfig.CommitGroupClose` to durable storage when
owner-side group closure must survive restarts.

See the [placement contract](placement/README.md) and
[adapter development guide](adapters/README.md). Persistence stores assignments;
owner discovery, game-state persistence, and recovery policy remain application
responsibilities.

## Repository layout

| Path | Contents |
| --- | --- |
| Root Go package | Public API, endpoint/session implementation, typed helpers, dispatch, limits, observations |
| `auth/jwt` | Identity verification and admission signing/verification |
| `placement` | Bootstrap resolution, interfaces, and memory implementations |
| `internal/wire` | Binary protocol framing and validation |
| `internal/quictransport` | QUIC transport implementation |
| `internal/runtime` | Bounded runtime primitives |
| `internal/testutil`, `internal/udprelay` | Test fixtures and network fault injection |
| `adapters` | Separate durable-store repositories |
| `examples` | Runnable games and API examples |
| `cmd/sgspbench`, `cmd/sgspbencheval`, `cmd/sgspbenchreport` | Benchmark execution, evaluation, and reporting |
| `scripts`, `docs` | Verification campaigns, benchmark guidance, and milestone evidence |

## Development and verification

```sh
go test ./...
go test -race ./...
go vet ./...
```

Networking tests bind local UDP sockets. The suite exercises authentication,
protocol validation, delivery, bounded dispatch, reconnect/resume, epoch
fencing, group closure, and resource behavior. Longer fault, fuzz, and resource
campaigns have runners under `scripts/`.

Root Go commands exclude nested modules. Check adapters separately:

```sh
go -C adapters/sqlite test -race ./...
SGSP_TEST_DATABASE_URL='postgres://...' go -C adapters/postgres test -race ./...
go -C adapters/sqlite generate ./...
go -C adapters/postgres generate ./...
```

SQLite tests use temporary database files and the CGO `mattn/go-sqlite3` driver,
so a C compiler is required. PostgreSQL tests require a disposable database;
they create isolated schemas and fail if the database URL is missing. SQLC is
pinned by each adapter's generation directive; generated code is committed.
Run Pong and Tag's checks from their own module directories as needed.

## License

Copyright 2026 Johannes Sarpola.

SGSP is licensed under the [Apache License, Version 2.0](LICENSE).
Dependencies and separately maintained adapter repositories carry their own
license terms.

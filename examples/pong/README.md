# Two-player terminal Pong

A small Go game demonstrating an SGSP client-server application. One server
owns the ball, paddles, scores, and player slots. Two Bubble Tea clients send
paddle targets and display the same authoritative snapshots. The server uses
plain logs; the clients use Bubbles help/key and spinner components, with Lip
Gloss styling.

## Run in three terminals

Use Go **1.27.1 or newer**, the version required by this checkout. In **each**
terminal, start in `examples/pong`:

```sh
cd examples/pong
```

Terminal 1 — start the server and wait for its listening message:

```sh
go run ./server
```

Terminal 2 — the first client gets the left paddle:

```sh
go run ./client
```

Terminal 3 — the second client gets the right paddle:

```sh
go run ./client
```

Each client needs at least **64 columns × 28 rows**. The game starts automatically
when both players join. A third client gets a `game full` error.

| Key | Action |
| --- | --- |
| ↑ / W | Move your paddle target up one row |
| ↓ / S | Move your paddle target down one row |
| Q / Ctrl-C | Leave the game and restore the terminal |

Hold a movement key to use your terminal's normal key repeat. Key-release
reporting is not required. The server caps paddle speed, and clients render only
server-confirmed positions. Resize a small terminal to return to the court.

Scores continue indefinitely, with a one-second serve delay after each point.
The game pauses when either player disconnects. SGSP automatically tries to
resume a lost connection, preserving the player's slot for its 30-second resume
window. Loss detection can take about five seconds. A clean client exit frees
its slot immediately; another client can take that side. Scores remain while
at least one player remains, and reset when both leave. After a terminal session
error or a server restart, start the client again.

Stop the server with Ctrl-C.

## Addresses and development credentials

The default endpoint is `127.0.0.1:4444` over UDP. For another local port:

```sh
go run ./server -listen 127.0.0.1:4555
go run ./client -server 127.0.0.1:4555
```

On first launch the server creates `.sgsp-pong-dev/credentials.json`, containing
a local CA, server certificate/key, and development JWT signing key. Both
clients read the same directory and generate distinct player identities. TLS
certificate verification remains enabled. Clients never generate missing keys:
start the server first and use the same working directory or `-dev-dir` path.
Client transport diagnostics go to `client.log` in that directory, keeping log
messages out of the game display. Startup and terminal session errors still
print after the terminal is restored.

This is a **single-computer development demo**. The shared signing key lets any
local client mint credentials; it is not a production login system. Private
material is written with mode `0600` under a `0700` directory and ignored by Git.
Certificates last seven days. If they expire, stop all Pong processes, remove
the generated `credentials.json`, and restart the server before restarting the
clients. A client session's development JWT lasts 24 hours; restart the client
if it expires.

Use a distinct `-dev-dir` and UDP port for each independent server. All three
processes belonging to a game must share that directory. Remote credential
distribution and matchmaking are outside this example.

## Follow the client-server flow

```text
client (left)             SGSP server                  client (right)
     |--- join request ------>|<------ join request ---------|
     |<-- side + snapshot ----|------- side + snapshot ----->|
     |--- paddle target ----->|<------ paddle target --------|
     |                        |                             |
     |                 simulate at 60 Hz                    |
     |<-- full state, 30 Hz --|------- full state, 30 Hz ---->|
```

| Traffic | SGSP API / delivery | Purpose |
| --- | --- | --- |
| Join | Typed `Request` + `Call` | Assign a side and return an initial snapshot; repeated joins retain the assignment |
| Input | Typed `Message` + `Emit`, channel 1, `UnreliableSequenced` | Send the latest desired paddle position at 30 Hz |
| State | Typed `Message` + `Emit`, channel 2, `UnreliableSequenced` | Broadcast a complete authoritative snapshot at 30 Hz |
| Connection lifecycle | `Router.OnLifecycle` and session state | Pause, reserve/free slots, and resume |

Sending absolute paddle targets repeatedly means losing an input packet does
not lose a movement permanently. Complete snapshots mean losing a state packet
does not require replaying earlier frames. Each sequenced channel discards stale
network updates. The application additionally captures `Incoming.Epoch` and
checks it at the simulation boundary, so queued input from an old connection
cannot change the game after resuming.

The simulation lives in one goroutine. Join/lifecycle work uses a bounded inbox;
input is coalesced to one latest target per session. The client similarly keeps
only the latest pending snapshot, and passes it into Bubble Tea as a message.
Each joined session has one sender goroutine and one replaceable outgoing
snapshot, so a full QUIC send queue cannot stall the simulation.
No network callback mutates the UI or the simulation directly.

Read the code in this order:

1. `internal/protocol`: application messages, delivery options, and shared dimensions.
2. `internal/game`: deterministic physics with no terminal or networking code.
3. `internal/server`: SGSP handlers, player ownership, and the authoritative tick loop.
4. `internal/netclient`: dial, join, input, and snapshot reception.
5. `internal/tui`: Bubble Tea update/view loop and Bubbles components.
6. `internal/dev`: local TLS and development identity setup.

The `server` and `client` directories contain the executable entrypoints.

## Test

From `examples/pong`:

```sh
go test -race ./...
```

Tests cover deterministic physics, bounds, scoring, pause/resume, stale input
epochs, TUI states and controls, and actual loopback SGSP connections. The
reconnect test temporarily drops UDP packets through a local relay and takes
several seconds. These tests need permission to bind local UDP sockets.

From the repository root, run the library's tests separately:

```sh
go test ./...
```

Pong is a separate module with `replace qattidev/sgsp => ../..`. Root-level
`go test ./...` does not include its tests, and the TUI dependencies do not enter
the library's `go.mod`.

# Terminal Tag — SGSP at 120 Hz

One authoritative server, two terminal clients, and a small 60 × 20 arena.
The chaser scores on contact; roles swap and both players immediately respawn.
Play continues indefinitely. The chaser moves at 20 cells/s and the runner at
16 cells/s so tags remain frequent. Positions and collisions use fractional cells.

Requires Go 1.27.1 or newer. From **examples/tag**, run in three terminals:

```sh
go run ./server
go run ./client --auto
go run ./client --auto
```

Omit `--auto` on either or both clients for manual play. Use **WASD / arrow
keys** to move and **Q / Ctrl-C** to quit. Hold a direction for continuous
movement. Terminals supporting the Kitty keyboard protocol report releases
precisely. Other terminals use key repeat with a 600 ms inactivity timeout;
movement can linger briefly after release or pause if repeat is unusually slow.
Clients request release reporting automatically. Allow at least 90 × 30 cells.

The first client is A, the second B; a third receives a game-full error.
Both clients show scores, chaser, elapsed play time, player modes, shared tick,
measured server Hz and received state updates/s. Rendering is capped at 60 FPS.

## Protocol and timing

- Simulation, movement and collision detection run at **120 Hz** (8.33 ms steps).
- Complete snapshots are broadcast every tick using SGSP `UnreliableSequenced`
  channel 2. Logs report measured simulation Hz once per second.
- Clients repeatedly send their latest movement direction on sequenced channel 1
  at a target of 120 Hz. Input sending and snapshot reception continue independently
  of the terminal's 60 FPS render cap. Actual rates depend on host scheduling.
- Auto decisions run in each client using received authoritative positions.
  Chasers pursue the runner. Runners choose X and Y uniformly and independently
  inside a three-cell wall margin, rejecting targets less than three cells away.
  They keep that destination until within one cell, then immediately choose
  another. Tags also reset the destination.
  Auto and manual use the same messages.
- Typed SGSP join requests allocate two slots. Authentication, TLS, session
  resumption, and transport are provided by SGSP; no alternative game socket exists.
- One goroutine owns the simulation. Inputs and outbound snapshots are coalesced;
  slow networking cannot build an unbounded queue or block the simulation.
  Epoch checks reject input queued before a reconnect.

The game pauses if either client disconnects. A suspended session retains its
slot during SGSP's resume window (30 seconds). A clean quit frees the slot.
Scores reset when both players leave. Restart clients after terminal session
failure or server restart.

## Local credentials

The server creates `.sgsp-tag-dev/credentials.json` with a local TLS CA,
server certificate, and shared development JWT key. Start the server first;
clients read that same directory and mint distinct identities. TLS verification
is enabled. This shared-key setup is for a local demo, not production login.
Keys are stored with private permissions. Certificates expire after seven days;
stop the processes, remove the credentials file, and restart to regenerate.

Defaults: UDP `127.0.0.1:4445`. Override with server `-listen`, client `-server`,
and matching `-dev-dir` paths for all processes. Transport diagnostics go to
`client.log` in that directory.

## Verification

```sh
go test -race ./...
```

Tests cover scoring, role swaps, respawns, bounds, normalized speed, pause,
sustained autonomous motion, keyboard controls, and two real SGSP clients.
The loopback test measures update rates and matching snapshots, rejects a third
player, and checks departure and slot replacement. It needs local UDP permission.

This is a separate Go module with `replace qattidev/sgsp => ../..`; repository-root
tests do not include it. Start reading at `internal/protocol`, `internal/game`,
`internal/server`, `internal/netclient`, `internal/bot`, and `internal/tui`.

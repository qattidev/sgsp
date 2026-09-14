# SGSP action example

This is a direct-owner development example. It creates a local CA, server
certificate, and development identity signing key in `.sgsp-action-dev/` on
first use. Those files are deliberately for local development only.

From the module root, start the owner in one terminal:

```sh
go run ./examples/action -mode server -listen 127.0.0.1:4444
```

Then run a client in another terminal:

```sh
go run ./examples/action -mode client -server 127.0.0.1:4444
```

The owner runs a 128 Hz authoritative tick loop. The client sends a sequenced
input on channel 1, a reliable `collect` command with a random operation ID on
channel 3, receives sequenced state updates on channel 2, performs a typed
inventory request/reply, and exchanges a raw snapshot stream. The server only
commits inbox entries whose captured session epoch is still current; stale work
after a reconnect is ignored at the game-state boundary. The client waits for
an authoritative update showing the `collect` command before requesting its
inventory, then prints the update, typed reply, and stream echo.

The game inbox is intentionally bounded. This demonstrates one appropriate
application policy for the handler integration: network callbacks do not
mutate simulation state directly or create one goroutine per input.

Use a different `-dev-dir` for independent local environments. Never reuse
the generated development identity key in a deployed service.

## Bootstrap placement mode

The example can also demonstrate a grouped placement flow. Start an owner
that writes its process-incarnation snapshot:

```sh
go run ./examples/action -mode owner -listen 127.0.0.1:45556 \
  -owner-file .sgsp-action-dev/owner.json
```

Start bootstrap using that snapshot (pass a comma-separated list to `-owners`
to experiment with multiple live owner snapshots):

```sh
go run ./examples/action -mode bootstrap -listen 127.0.0.1:45557 \
  -owners .sgsp-action-dev/owner.json
```

For two owners, use distinct snapshot files (the shared development directory
keeps their local CA and development identity consistent):

```sh
go run ./examples/action -mode owner -listen 127.0.0.1:45556 \
  -owner-file .sgsp-action-dev/owner-a.json
go run ./examples/action -mode owner -listen 127.0.0.1:45558 \
  -owner-file .sgsp-action-dev/owner-b.json
go run ./examples/action -mode bootstrap -listen 127.0.0.1:45557 \
  -owners .sgsp-action-dev/owner-a.json,.sgsp-action-dev/owner-b.json
```

Resolve and join a group through bootstrap:

```sh
go run ./examples/action -mode client -bootstrap 127.0.0.1:45557 \
  -group match-1
```

This mode uses the in-memory placement store, so restart the bootstrap process
to intentionally discard development assignments. The development setup is a
single-machine demonstration, not a production service-discovery mechanism.

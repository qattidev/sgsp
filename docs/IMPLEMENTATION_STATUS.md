# SGSP implementation status

This file is an evidence log, not a release claim. The library has working
initial admission (including source-IP handshake limits), JWT helpers,
directionally negotiated message limits, messages, request/reply, custom
streams, polling dispatch, and bounded fixed-worker handler dispatch (with
per-session serialization and queued sequenced-update coalescing), logical close acknowledgement,
reconnect/resume, bounded terminal-ID and group-tombstone retention, placement
interfaces, memory and PostgreSQL adapters with an explicit migration runner,
one-second bounded observer aggregation for session-state, handshake/resume,
authentication, message, queue, request/stream, protocol, transport, and
optional Bootstrap placement signals, and the direct/bootstrap action example
with a 128 Hz authoritative loop.
Control-frame reads use the fixed control-reader task and transport deadlines;
they do not spawn a helper goroutine per frame.

Verified on 2026-09-15:

```text
GOCACHE=/tmp/sgsp-go-build go test -count=1 ./...        PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=1 ./...  PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=20 \
  -run 'Test(ConcurrentAssignment|AssignmentVersionAndClose)$' ./placement/...  PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=20 ./placement ./placement/memory PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=10 ./cmd/sgspbench            PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=5 -run '^TestNATRebinding$' . PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=5 -run '^TestUnknownRequestOutcome' . PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=5 -run '^TestEpochFence$' .  PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=5 -run '^TestDraining$' .    PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=10 -run '^TestSlowConsumer$' . PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=5 -run '^TestOwnerRestart$' . PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=5 -run '^TestGroupCloseRace$' . PASS
go vet ./...                                                                   PASS
test -z "$(gofmt -l -- *.go internal/**/*.go examples/**/*.go cmd/**/*.go \
  placement/**/*.go 2>/dev/null)"                                             PASS
```

The action example has automated local-QUIC direct and bootstrap coverage:
the 128 Hz update, operation-ID command, typed inventory response, raw stream,
two concurrent grouped clients resolving to one of two live owners, a
UDP-blackhole reconnect/resume, and terminal selected-owner shutdown. Its
README contains the reproducible commands.

`TestNATRebinding` uses the two-socket UDP relay to change only its
server-facing source port. It observes the QUIC probe, `PATH_CHALLENGE`, and
`PATH_RESPONSE` forwarding sequence without decoding packets, then verifies
that traffic continues with the original SGSP session epoch. This records
same-epoch NAT-rebinding support for the pinned QUIC adapter.

The following specification gates are still incomplete and must not be
reported as passed:

- Reliable event and request stream readers reserve queue and
  global-body capacity before consuming a declared payload, and `Call`
  response bodies reserve global capacity before allocation. Short-lived
  outbound frames and decoded `Call` bodies also reserve their separate,
  resume-persistent per-session directional `QueueBytes` budgets; one maximum
  request-reply body per direction has an exclusive reserve, while raw custom
  streams avoid library-owned copy buffers. Control writes use their own
  bounded per-connection queue and close with
  `ResourceExhausted` on queue exhaustion. Both dispatch modes wait through
  `SlowConsumerTimeout` for queued reliable work and then close, but this does
  not complete the full gate;
- broader terminal cleanup coverage from section 8. Barrier-driven
  100-candidate concurrent commit, real lost-WELCOME retry with an unseen
  epoch, positive and credential-bound resume cases, direct revocation,
  pre-grace resume/post-grace expiry, a real-QUIC polling epoch-fencing test
  that discards epoch-1 work and delivers only resumed epoch-2 work, a
  real-QUIC pending request/custom-stream loss cleanup test, a
  real-QUIC committed-request loss/resume test that records `OutcomeUnknown`
  without replaying the committed operation,
  interrupted reliable-stream loss handoff (which must not be relabeled as a
  protocol violation), and bounded expired-ID cache tests are present;
- full 60-second decoder fuzz campaigns. One-worker five-second diagnostics
  completed successfully for `FuzzControl`, `FuzzEvent`, and `FuzzRequest`.
  An attempted 60-second `FuzzControl` campaign made 142,731 executions and
  then stopped making progress before the execution environment ended without
  a `PASS` result, so it is not recorded as the required M1 fuzz evidence;
- PostgreSQL integration execution against a disposable service; the explicit
  migration runner and its local integrity checks compile, but no service was
  available here;
- the M8 impairment matrix and published performance trial results.
  `cmd/sgspbench` now runs a bounded single SGSP or bare-QUIC trial through
  per-client deterministic two-socket UDP relays, exercising sequenced
  input/update datagrams, request/reply, and paced raw-stream bulk data.
  `scripts/run-benchmark-matrix.sh` builds both binaries, captures environment
  metadata, runs the configured five-seed matrix, and invokes
  `cmd/sgspbenchreport` to index the JSON results. A 100 ms, one-client,
  five-seed tooling smoke passed locally, but the required 60-second matrix,
  plateau campaign, and published report remain incomplete.

The UDP receive-buffer warning emitted by quic-go on this host (416 KiB versus
its 7 MiB desired buffer) is environmental and remains recorded as a
performance/benchmark concern.

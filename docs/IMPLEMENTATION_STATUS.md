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

Verified through 2026-09-16:

```text
GOCACHE=/tmp/sgsp-go-build go test -count=1 ./...        PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=1 ./...  PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=20 \
  -run 'Test(ConcurrentAssignment|AssignmentVersionAndClose)$' ./placement/...  PASS
GOCACHE=/tmp/sgsp-go-build go -C integration/postgres test -race -count=20 \
  -v ./...                                                                  PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=20 ./placement ./placement/memory PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=10 ./cmd/sgspbench            PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=5 -run '^TestNATRebinding$' . PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=5 -run '^TestUnknownRequestOutcome' . PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=5 -run '^TestEpochFence' .   PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=5 -run '^TestDraining$' .    PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=10 -run '^TestSlowConsumer$' . PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=5 -run '^TestOwnerRestart$' . PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=100 \
  -run 'Test(ConcurrentResume|EpochFence|LostResumeWelcome|DispatchOrdering)$' . PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=5 -run '^TestGroupCloseRace$' . PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=10 -run '^TestClientReconnectLoop$' . PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=5 -run '^TestSessionAdmissionLimit$' . PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=20 -run '^TestTerminalIDCache' . PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=1 -run '^TestShutdownCleanup$' . PASS
GOCACHE=/tmp/sgsp-go-build go test -count=10 \
  -run 'Test(ShutdownCleanup|SlowConsumer|GlobalBudget)$' ./...              PASS
GOCACHE=/tmp/sgsp-go-build go test -run '^$' -bench '^BenchmarkCodec' \
  -benchmem .                                                                 PASS
GOMAXPROCS=1 GOCACHE=/tmp/sgsp-go-build go test ./internal/wire -run '^$' \
  -fuzz '^FuzzControl$' -fuzztime=60s -parallel=1                         PASS
GOMAXPROCS=1 GOCACHE=/tmp/sgsp-go-build go test ./internal/wire -run '^$' \
  -fuzz '^FuzzControl$' -fuzztime=10m -parallel=1                         PASS
GOMAXPROCS=1 GOCACHE=/tmp/sgsp-go-build go test ./internal/wire -run '^$' \
  -fuzz '^FuzzEvent$' -fuzztime=60s -parallel=1                           PASS
GOMAXPROCS=1 GOCACHE=/tmp/sgsp-go-build go test ./internal/wire -run '^$' \
  -fuzz '^FuzzEvent$' -fuzztime=10m -parallel=1                           PASS
GOMAXPROCS=1 GOCACHE=/tmp/sgsp-go-build go test ./internal/wire -run '^$' \
  -fuzz '^FuzzRequest$' -fuzztime=60s -parallel=1                         PASS
GOMAXPROCS=1 GOCACHE=/tmp/sgsp-go-build go test ./internal/wire -run '^$' \
  -fuzz '^FuzzRequest$' -fuzztime=10m -parallel=1                         PASS
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
  pre-grace resume/post-grace expiry, a real-QUIC epoch-fencing test that
  discards epoch-1 work, rejects stale reply/custom-stream writes, and
  delivers only resumed epoch-2 work, a
  real-QUIC pending request/custom-stream loss cleanup test, a
  real-QUIC committed-request loss/resume test that records `OutcomeUnknown`
  without replaying the committed operation,
  interrupted reliable-stream loss handoff (which must not be relabeled as a
  protocol violation), bounded expired-ID cache tests, and a real-QUIC
  1,000-cycle shutdown-cleanup test (request, loss, explicit resume, close,
  no retained session/application budget, and post-GC heap tolerance) are
  present;
- the M8 impairment matrix and published performance trial results.
  `cmd/sgspbench` now runs a bounded single SGSP or bare-QUIC trial through
  per-client deterministic two-socket UDP relays, exercising sequenced
  input/update datagrams, request/reply, and paced raw-stream bulk data. Its
  JSON uses bounded fixed-memory histograms for local input-send API-call
  duration, successful request round trip, and same-process update age;
  percentile fields are upper bounds rather than retained per-operation
  samples.
  `scripts/run-benchmark-matrix.sh` builds both binaries, captures environment
  metadata, runs the configured five-seed matrix, and invokes
  `cmd/sgspbenchreport` to index the JSON results. A 100 ms, one-client,
  five-seed tooling smoke passed locally. On 2026-09-16, a second 50 ms,
  one-client script smoke completed all 20 trials (both implementations,
  60/128 Hz, five seeds) and rendered timing, relay, runtime, and build-flag
  metadata into temporary local artifacts. A paired full 60-second pilot
  (one client, 60 Hz, 20 ms RTT, seed 1, `GOMAXPROCS=1`) completed for both
  implementations with no failed operations, relay drops, or relay overload:
  SGSP accepted 3,601 inputs and delivered 3,600, accepted/delivered 3,600
  updates, delivered 120 requests, and delivered 3,774,874 bulk bytes; bare
  QUIC accepted/delivered 3,600 inputs and updates, delivered 120 requests,
  and delivered 3,774,874 bulk bytes. Offered-but-unmatched work at the
  measurement cutoff is deliberately not counted as a failure. These are
  local pilot artifacts only, not a published result. They cannot establish
  capacity or variance, so the required five-seed 60-second matrix, plateau
  campaign, and published report remain incomplete. A subsequent full
  one-client healthy sweep (both implementations, 60/128 Hz, five seeds,
  10-second warmup and 60-second measurement, `GOMAXPROCS=4`) completed all
  20 of 20 recorded trials. It establishes only the configured one-client
  sweep. The corresponding eight-client healthy sweep also completed all
  20 of 20 trials under the same timing, rate, seed, and implementation
  matrix, as did the 32-, 64-, and 128-client healthy sweeps. These local
  points do not establish a multi-client capacity plateau; capacity doubling
  beyond 128, the impairment sweep, and published report remain incomplete.

The UDP receive-buffer warning emitted by quic-go on this host (416 KiB versus
its 7 MiB desired buffer) is environmental and remains recorded as a
performance/benchmark concern.

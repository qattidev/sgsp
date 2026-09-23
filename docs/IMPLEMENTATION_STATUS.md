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

Historical verification through 2026-09-16:

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

Additional verification on the current worktree, 2026-09-22:

```text
go test -count=1 ./...                                             PASS (82.818s)
go test -race -count=1 -run '^(TestResumeGrace|TestResumeGraceHonorsEarlierAuthenticationExpiry)$' .
                                                                      PASS (1.113s)
go test -count=1 ./cmd/sgspbench ./cmd/sgspbenchreport              PASS (3.704s)
go test -race -count=1 ./...                                        PASS (root 106.339s)
go vet ./...                                                        PASS
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
- M8 capacity, impairment, plateau, and published comparison evidence. The
  benchmark harness now runs SGSP and bare-QUIC trials through deterministic
  per-client two-socket UDP relays with bounded timing histograms, correlated
  directional local budgets, queue/drop and relay-backlog observations, and
  explicit known versus unknown request failures. Bare-QUIC correctly renders
  SGSP-only queue/drop measures unavailable. The runner accepts only local
  `artifacts/` paths, preserving its exact binaries, manifests, raw JSON, and
  report; benchmark matrices are never retained under `/tmp`.

Warmup and measurement are separate workload generations. Phase-marked
datagrams, requests, and bulk streams prevent late warmup work from appearing
in measured offered, accepted, or delivered counts. The benchmark also records
scheduled and missed input/request/bulk ticks, marking a trial invalid when
the generator cannot sustain its configured schedule.

The retained `artifacts/healthy-1c-full-20260922/` matrix predates this
phase-aware accounting and is diagnostic only, not capacity evidence. It also
records 11--22 local/coalesced SGSP datagram drops at 128 Hz (about
0.15--0.29%), above the architecture's below-0.1% criterion. The retained
boundary-validation matrices
`artifacts/measurement-boundary-smoke-20260922/` and
`artifacts/measurement-boundary-readiness-smoke-20260922/` have no
offered/accepted/delivered boundary inversion; the latter correctly invalidates
all 20 short trials on this constrained host because scheduled input and/or
bulk ticks were missed. The subsequent
`artifacts/measurement-nonzero-smoke-20260923/` additionally confirms that
all 20 invalid JSON records and the report are retained before the runner
returns a nonzero failed-gate exit. These are harness diagnostics, not
performance results. The five-seed 60-second capacity and plateau campaign,
full impairment matrix at half that capacity, and published comparison remain
incomplete.

`TestReconnectBlackholeDurations` now performs the required real UDP-relay
blackholes for 2, 8, and 40 seconds. The locally retained JSON record at
`artifacts/reconnect-blackhole-20260922-rerun/test.json` passed all three: the
two-second outage kept epoch 1 active, the eight-second outage resumed at
epoch 2 after 1.023 seconds, and the 40-second outage expired the session
after 36.001 seconds.
This is reconnect regression evidence, not a five-seed capacity or impairment
matrix result.

The UDP receive-buffer warning emitted by quic-go on this host (416 KiB versus
its 7 MiB desired buffer) is environmental and remains recorded as a
performance/benchmark concern.

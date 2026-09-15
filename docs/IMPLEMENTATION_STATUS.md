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
GOCACHE=/tmp/sgsp-go-build go test -race -count=10 ./cmd/sgspbench            PASS
go vet ./...                                                                   PASS
test -z "$(gofmt -l -- *.go internal/**/*.go examples/**/*.go cmd/**/*.go \
  placement/**/*.go 2>/dev/null)"                                             PASS
```

The action example has automated local-QUIC direct and bootstrap coverage:
the 128 Hz update, operation-ID command, typed inventory response, raw stream,
two concurrent grouped clients resolving to one of two live owners, a
UDP-blackhole reconnect/resume, and terminal selected-owner shutdown. Its
README contains the reproducible commands.

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
  pre-grace resume/post-grace expiry, a real-QUIC polling epoch-queue cleanup
  test, a real-QUIC pending request/custom-stream loss cleanup test,
  interrupted reliable-stream loss handoff (which must not be relabeled as a
  protocol violation), and bounded expired-ID cache tests are present;
- full 60-second decoder fuzz campaigns. A one-worker five-second
  `FuzzControl` diagnostic completed successfully, but this runner stalled
  before reaching a 60-second fuzz-time budget, so it is not recorded as the
  required M1 fuzz evidence;
- PostgreSQL integration execution against a disposable service; the explicit
  migration runner and its local integrity checks compile, but no service was
  available here;
- the M8 impairment matrix and published performance trial results.
  `cmd/sgspbench` now runs a bounded single SGSP or bare-QUIC trial through
  per-client deterministic two-socket UDP relays, exercising sequenced
  input/update datagrams, request/reply, and paced raw-stream bulk data. It
  records configuration, operation counts, relay state, and Go runtime data,
  but the required five-trial matrix, plateau campaign, and published report
  remain incomplete.

The UDP receive-buffer warning emitted by quic-go on this host (416 KiB versus
its 7 MiB desired buffer) is environmental and remains recorded as a
performance/benchmark concern.

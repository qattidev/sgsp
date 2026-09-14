# SGSP implementation status

This file is an evidence log, not a release claim. The library has working
initial admission (including source-IP handshake limits), JWT helpers,
directionally negotiated message limits, messages, request/reply, custom
streams, polling dispatch, and bounded fixed-worker handler dispatch (with
per-session serialization and queued sequenced-update coalescing), logical close acknowledgement,
reconnect/resume, bounded terminal-ID and group-tombstone retention, placement
interfaces, memory and PostgreSQL adapters with an explicit migration runner,
one-second bounded observer aggregation for session and two-way event-message signals,
and the direct/bootstrap action example with a 128 Hz authoritative loop.

Verified on 2026-09-14:

```text
GOCACHE=/tmp/sgsp-go-build go test -count=1 ./...        PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=1 ./...  PASS
GOCACHE=/tmp/sgsp-go-build go test -race -count=20 \
  -run 'Test(ConcurrentAssignment|AssignmentVersionAndClose)$' ./placement/...  PASS
```

The action example has automated local-QUIC direct and bootstrap coverage:
the 128 Hz update, operation-ID command, typed inventory response, raw stream,
two concurrent grouped clients resolving to one of two live owners, a
UDP-blackhole reconnect/resume, and terminal selected-owner shutdown. Its
README contains the reproducible commands.

The following specification gates are still incomplete and must not be
reported as passed:

- complete aggregate application-budget accounting (outgoing/stream buffers),
  reserved reply/control queue capacity, and the remaining rate-limit behavior
  from section 10. Reliable stream readers now reserve queue and global-body
  capacity before consuming a declared payload; both dispatch modes wait
  through `SlowConsumerTimeout` for queued reliable work and then close, but
  this does not complete the full gate;
- broader terminal cleanup coverage from section 8. Barrier-driven
  100-candidate concurrent commit, real lost-WELCOME retry with an unseen
  epoch, pre-grace resume/post-grace expiry, a real-QUIC polling epoch-queue
  cleanup test, and bounded expired-ID cache tests are present;
- PostgreSQL integration execution against a disposable service; the explicit
  migration runner and its local integrity checks compile, but no service was
  available here;
- full section-11 observation coverage: fixed histogram buckets and the
  remaining authentication, queue, request, stream, placement, and transport
  signals are not yet instrumented;
- the M8 workload engine, impairment matrix, and published performance trial
  results. A bounded deterministic two-socket UDP relay now exists for tests,
  but `cmd/sgspbench` still writes an explicit incomplete result and exits
  nonzero until it drives and records the required measurements.

The UDP receive-buffer warning emitted by quic-go on this host (416 KiB versus
its 7 MiB desired buffer) is environmental and remains recorded as a
performance/benchmark concern.

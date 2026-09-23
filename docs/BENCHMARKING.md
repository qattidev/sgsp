# Benchmark evidence

`scripts/run-benchmark-matrix.sh` runs the section-12 SGSP and bare-QUIC
matrix using separately built binaries. It writes one JSON result per trial,
an `environment.txt` manifest, and a generated `summary.md` index.

Each trial also records fixed-memory binary-histogram timing summaries. They
include input and update API-to-adapter handoff durations, plus decoded-frame
to continuous-poll/handler-return overhead at each receiving endpoint. The
report correlates client index and input sequence to publish two true
per-direction distributions: client input send plus server input receive, and
server update send plus client update receive. Each carries its own
unmatched-send fraction. Percentiles are explicit upper bounds, not exact
samples; a timestamp-buffer overflow in either direction marks the trial
invalid rather than silently dropping samples. Update age is a separate
end-to-end application measure, so transport transit is not misrepresented as
local API overhead.

The JSON and report also retain transport RTT, local/coalesced/stale SGSP
datagram drops, explicit unknown request outcomes, queue occupancy maxima and
bounded queue-age buckets, relay forwarding, and final and peak scheduled
backlog state, plus interval CPU time, live heap/RSS, allocation, and
goroutine data. Relay drops are observed network loss; they are not reported
as local SGSP drops. The bare-QUIC baseline marks SGSP-only local-drop and
dispatch-queue fields unavailable rather than reporting a fabricated zero;
its request failures are still classified as known pre-send or unknown
post-write outcomes. Warmup and measured generators are separate phases:
measurement inputs and requests carry a phase marker and measured bulk uses a
separate stream type, so late warmup traffic cannot increase measured offered,
accepted, or delivered counts. Measured bulk streams are prepared before the
timer starts and released together with the other generators. The result also
records scheduled and missed input, request, and bulk ticks. A missed tick
marks the trial invalid rather than hiding generator shortfall behind a lower
offered rate. On Linux CPU time is user plus system process time; on other
platforms a zero value means that this dependency-free collector is
unavailable.

The matrix runner also executes and retains the codec microbenchmark alongside
its transport trials, in `codec-benchmark-*.txt` and
`codec-benchmark-*.status.json` under the campaign directory. Run it by itself
only when iterating on codecs:

```sh
go test -run '^$' -bench '^BenchmarkCodec' -benchmem .
```

`BenchmarkCodecRawBytes` is the pre-encoded payload path, while
`BenchmarkCodecJSON` is the public `sgsp.JSON` encode/decode path. Neither
result is added to a transport-overhead percentile.

Run it on the reference host, not on a development laptop or a constrained
CI runner:

```sh
SGSPBENCH_GOMAXPROCS=4 SGSPBENCH_REFERENCE_HOST=1 \
  scripts/run-benchmark-matrix.sh artifacts/$(date -u +%Y%m%dT%H%M%SZ)
```

The healthy sweep defaults to 1, 8, 32, 64, and 128 clients at 60 and 128 Hz,
with 10-second warmup, 60-second measurement, and five seeds. After a healthy
capacity is demonstrated, rerun with half that value to include the full
impairment matrix:

```sh
SGSPBENCH_GOMAXPROCS=4 SGSPBENCH_REFERENCE_HOST=1 \
  SGSPBENCH_IMPAIRED_CLIENTS=16 \
  scripts/run-benchmark-matrix.sh artifacts/$(date -u +%Y%m%dT%H%M%SZ)
```

The matrix runner rejects `/tmp` and other destinations outside this
checkout's `artifacts/` directory. Keep the resulting directory as the local
machine-readable record for the run; it contains the environment manifest,
exact binaries, JSON trials, and rendered summary.

For release-capacity evidence, `SGSPBENCH_REFERENCE_HOST=1` refuses to start
unless the machine has at least four physical CPU cores, 8 GiB RAM, and
`SGSPBENCH_GOMAXPROCS=4`. Its environment manifest records the validated core
and memory values. Leave this setting unset only for constrained-host harness
diagnostics; those runs cannot establish the architecture's capacity gate.

For an interrupted campaign, rerun the identical command with
`SGSPBENCH_RESUME=1`. The runner creates `campaign.json` on the first
invocation and refuses to resume if its benchmark settings, Go version, or
source snapshot differs. It retains the original manifest and writes a
separate environment manifest for the resumed invocation.

An invalid or failed trial writes its JSON before `sgspbench` exits nonzero.
The matrix runner continues through the remaining trials, renders `summary.md`,
and then exits nonzero when any result is non-completed. This preserves the
failure evidence without allowing a failed performance gate to appear
successful.

The reconnect cases are a separate real-QUIC regression test, rather than
synthetic benchmark samples. It blackholes both directions of an established
UDP relay for 2, 8, and 40 seconds, verifies continuity, resume within grace,
and expiry beyond transport idle detection plus grace. Pair it with the
race-tested ordering, epoch, and loss invariants through the retained M8
functional-evidence runner:

```sh
scripts/run-m8-functional-evidence.sh \
  artifacts/m8-functional-$(date -u +%Y%m%dT%H%M%SZ)
```

It preserves a source manifest, environment, raw Go JSON event streams, run
log, status, and summary locally below `artifacts/`. This is functional
reconnect/protocol evidence, not a substitute for the five-seed capacity
matrix.

The generated report is a trial index, not a gate verdict. Its results must be
evaluated against the p99 local-overhead, drop, plateau, reconnect, and
capacity criteria in [ARCHITECTURE.md](../ARCHITECTURE.md). A short run may be
used to test the tooling, but never reported as a 60-second matrix result.

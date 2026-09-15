# Benchmark evidence

`scripts/run-benchmark-matrix.sh` runs the section-12 SGSP and bare-QUIC
matrix using separately built binaries. It writes one JSON result per trial,
an `environment.txt` manifest, and a generated `summary.md` index.

Run it on the reference host, not on a development laptop or a constrained
CI runner:

```sh
SGSPBENCH_GOMAXPROCS=4 \
  scripts/run-benchmark-matrix.sh artifacts/$(date -u +%Y%m%dT%H%M%SZ)
```

The healthy sweep defaults to 1, 8, 32, 64, and 128 clients at 60 and 128 Hz,
with 10-second warmup, 60-second measurement, and five seeds. After a healthy
capacity is demonstrated, rerun with half that value to include the full
impairment matrix:

```sh
SGSPBENCH_GOMAXPROCS=4 SGSPBENCH_IMPAIRED_CLIENTS=16 \
  scripts/run-benchmark-matrix.sh artifacts/$(date -u +%Y%m%dT%H%M%SZ)
```

The generated report is a trial index, not a gate verdict. Its results must be
evaluated against the p99 local-overhead, drop, plateau, reconnect, and
capacity criteria in [ARCHITECTURE.md](../ARCHITECTURE.md). A short run may be
used to test the tooling, but never reported as a 60-second matrix result.

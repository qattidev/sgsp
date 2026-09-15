#!/usr/bin/env bash
# Run the section-12 SGSP/bare-QUIC benchmark matrix and render its trial
# index. Defaults intentionally match ARCHITECTURE.md; no result is treated as
# a release-gate pass by this script.
set -euo pipefail

if [[ ${1:-} == "--help" ]]; then
  cat <<'USAGE'
Usage: scripts/run-benchmark-matrix.sh [artifact-directory]

Environment overrides:
  SGSPBENCH_WARMUP=10s             warmup per trial
  SGSPBENCH_DURATION=60s           measurement duration per trial
  SGSPBENCH_HEALTHY_CLIENTS="1 8 32 64 128"
  SGSPBENCH_IMPAIRED_CLIENTS=16    half of a proven healthy capacity
  SGSPBENCH_GOMAXPROCS=4

The script builds reproducible local binaries, runs five seeds for each
healthy and impairment point, stores JSON under raw/, and writes summary.md.
Set SGSPBENCH_IMPAIRED_CLIENTS only after identifying a healthy capacity.
USAGE
  exit 0
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root=$(cd -- "$script_dir/.." && pwd)
cd "$root"
artifacts=${1:-"$root/artifacts/$(date -u +%Y%m%dT%H%M%SZ)"}
warmup=${SGSPBENCH_WARMUP:-10s}
duration=${SGSPBENCH_DURATION:-60s}
healthy_clients=${SGSPBENCH_HEALTHY_CLIENTS:-"1 8 32 64 128"}
impaired_clients=${SGSPBENCH_IMPAIRED_CLIENTS:-}
gomaxprocs=${SGSPBENCH_GOMAXPROCS:-4}
bin_dir="$artifacts/bin"
raw_dir="$artifacts/raw"
mkdir -p "$bin_dir" "$raw_dir"

{
  echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "gomaxprocs=$gomaxprocs"
  echo "build_flags=-trimpath"
  echo "go_version=$(go version)"
  echo "go_env=$(go env GOOS GOARCH GOVERSION)"
  echo "kernel=$(uname -sr)"
  if command -v lscpu >/dev/null 2>&1; then
    lscpu
  fi
  if command -v free >/dev/null 2>&1; then
    free -b
  fi
  echo "modules:"
  go list -m all
} > "$artifacts/environment.txt"

GOMAXPROCS="$gomaxprocs" go build -trimpath -o "$bin_dir/sgspbench" ./cmd/sgspbench
GOMAXPROCS="$gomaxprocs" go build -trimpath -o "$bin_dir/sgspbenchreport" ./cmd/sgspbenchreport

run_trial() {
  local implementation=$1 clients=$2 hz=$3 rtt=$4 loss=$5 jitter=$6 reorder=$7 seed=$8 profile=$9
  local file="$raw_dir/${profile}-${implementation}-c${clients}-hz${hz}-rtt${rtt}-loss${loss}-j${jitter}-r${reorder}-seed${seed}.json"
  GOMAXPROCS="$gomaxprocs" "$bin_dir/sgspbench" \
    --implementation "$implementation" --clients "$clients" --hz "$hz" \
    --input-bytes 64 --update-bytes 512 --rpc-per-second 2 --bulk-bytes-per-second 65536 \
    --warmup "$warmup" --duration "$duration" --rtt "$rtt" --jitter "$jitter" \
    --loss "$loss" --reorder "$reorder" --seed "$seed" --output "$file"
}

run_point() {
  local clients=$1 hz=$2 rtt=$3 loss=$4 jitter=$5 reorder=$6 profile=$7
  for implementation in sgsp quic; do
    for seed in 1 2 3 4 5; do
      run_trial "$implementation" "$clients" "$hz" "$rtt" "$loss" "$jitter" "$reorder" "$seed" "$profile"
    done
  done
}

for clients in $healthy_clients; do
  for hz in 60 128; do
    run_point "$clients" "$hz" 20ms 0 0ms 0 healthy
  done
done

if [[ -z $impaired_clients ]]; then
  echo "Healthy sweep complete. Set SGSPBENCH_IMPAIRED_CLIENTS to half the proven healthy capacity, then rerun to include impairment trials." >&2
else
  for hz in 60 128; do
    for rtt in 20ms 50ms 100ms; do
      for loss in 0 0.01 0.05; do
        run_point "$impaired_clients" "$hz" "$rtt" "$loss" 0ms 0 impairment
      done
    done
    for loss in 0 0.01 0.05; do
      run_point "$impaired_clients" "$hz" 50ms "$loss" 10ms 0.02 impairment-jitter
    done
  done
fi

"$bin_dir/sgspbenchreport" --input "$raw_dir" --output "$artifacts/summary.md"
echo "Artifacts written to $artifacts" >&2

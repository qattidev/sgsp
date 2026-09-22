#!/usr/bin/env bash
# Run the section-12 SGSP/bare-QUIC benchmark matrix and render its trial
# index. Defaults intentionally match ARCHITECTURE.md; no result is treated as
# a release-gate pass by this script.
set -euo pipefail

if [[ ${1:-} == "--help" ]]; then
  cat <<'USAGE'
Usage: scripts/run-benchmark-matrix.sh [artifacts/subdirectory]

Environment overrides:
  SGSPBENCH_WARMUP=10s             warmup per trial
  SGSPBENCH_DURATION=60s           measurement duration per trial
  SGSPBENCH_HEALTHY_CLIENTS="1 8 32 64 128"
  SGSPBENCH_IMPAIRED_CLIENTS=16    half of a proven healthy capacity
  SGSPBENCH_GOMAXPROCS=4
  SGSPBENCH_RESUME=1               continue a matching interrupted campaign

The script builds reproducible local binaries, runs five seeds for each
healthy and impairment point, stores JSON under raw/, and writes summary.md.
Artifacts always remain below this checkout's artifacts/ directory; /tmp is
not a valid destination. Set SGSPBENCH_IMPAIRED_CLIENTS only after
identifying a healthy capacity. Resume only continues a campaign whose
configuration and source snapshot match its campaign.json manifest.
USAGE
  exit 0
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root=$(cd -- "$script_dir/.." && pwd)
cd "$root"
artifact_argument=${1:-"artifacts/$(date -u +%Y%m%dT%H%M%SZ)"}
case "$artifact_argument" in
  artifacts|artifacts/*)
    ;;
  *)
    echo "artifact directory must be below artifacts/ in this checkout; refusing: $artifact_argument" >&2
    exit 2
    ;;
esac
artifact_root=$(realpath -m "$root/artifacts")
artifacts=$(realpath -m "$root/$artifact_argument")
case "$artifacts" in
  "$artifact_root"|"$artifact_root"/*)
    ;;
  *)
    echo "artifact directory escapes artifacts/ in this checkout; refusing: $artifact_argument" >&2
    exit 2
    ;;
esac
warmup=${SGSPBENCH_WARMUP:-10s}
duration=${SGSPBENCH_DURATION:-60s}
healthy_clients=${SGSPBENCH_HEALTHY_CLIENTS:-"1 8 32 64 128"}
impaired_clients=${SGSPBENCH_IMPAIRED_CLIENTS:-}
gomaxprocs=${SGSPBENCH_GOMAXPROCS:-4}
resume=${SGSPBENCH_RESUME:-0}
bin_dir="$artifacts/bin"
raw_dir="$artifacts/raw"
mkdir -p "$bin_dir" "$raw_dir"
if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to create and validate the local campaign manifest" >&2
  exit 2
fi
source_revision=$(git rev-parse HEAD 2>/dev/null || printf 'unknown')
# Compare the complete worktree to HEAD so both staged and unstaged source
# edits prevent a resumed run from mixing binaries. The checkout revision alone
# cannot distinguish a staged build from its unmodified parent.
source_dirty_hash=$(git diff --no-ext-diff HEAD | sha256sum | awk '{print $1}')
go_version=$(go version)
campaign_file="$artifacts/campaign.json"
campaign_matches() {
  jq -e \
    --arg warmup "$warmup" \
    --arg duration "$duration" \
    --arg healthy_clients "$healthy_clients" \
    --arg impaired_clients "$impaired_clients" \
    --arg gomaxprocs "$gomaxprocs" \
    --arg source_revision "$source_revision" \
    --arg source_dirty_hash "$source_dirty_hash" \
    --arg go_version "$go_version" \
    '.schema == 1 and
     .warmup == $warmup and
     .duration == $duration and
     .healthy_clients == $healthy_clients and
     .impaired_clients == $impaired_clients and
     .gomaxprocs == $gomaxprocs and
     .source_revision == $source_revision and
     .source_dirty_hash == $source_dirty_hash and
     .go_version == $go_version' "$campaign_file" >/dev/null
}
if [[ -e $campaign_file ]]; then
  if ! campaign_matches; then
    echo "campaign manifest does not match this configuration or source snapshot; use a new artifacts/ directory" >&2
    exit 2
  fi
elif find "$raw_dir" -type f -print -quit | grep -q .; then
  echo "raw trial files exist without a campaign manifest; refusing to mix evidence" >&2
  exit 2
else
  jq -n \
    --arg warmup "$warmup" \
    --arg duration "$duration" \
    --arg healthy_clients "$healthy_clients" \
    --arg impaired_clients "$impaired_clients" \
    --arg gomaxprocs "$gomaxprocs" \
    --arg source_revision "$source_revision" \
    --arg source_dirty_hash "$source_dirty_hash" \
    --arg go_version "$go_version" \
    '{schema: 1, warmup: $warmup, duration: $duration,
      healthy_clients: $healthy_clients, impaired_clients: $impaired_clients,
      gomaxprocs: $gomaxprocs, source_revision: $source_revision,
      source_dirty_hash: $source_dirty_hash, go_version: $go_version}' > "$campaign_file"
fi
run_log="$artifacts/run.log"
exec > >(tee -a "$run_log") 2>&1
echo "matrix_command=$0 $artifact_argument"
run_started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
run_stamp=$(date -u +%Y%m%dT%H%M%SZ)
echo "matrix_started_utc=$run_started_utc"

environment_file="$artifacts/environment.txt"
if [[ -e $environment_file ]]; then
  environment_file="$artifacts/environment-$run_stamp.txt"
fi
{
  echo "timestamp_utc=$run_started_utc"
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
} > "$environment_file"
echo "environment_manifest=$environment_file"

GOMAXPROCS="$gomaxprocs" go build -trimpath -o "$bin_dir/sgspbench" ./cmd/sgspbench
GOMAXPROCS="$gomaxprocs" go build -trimpath -o "$bin_dir/sgspbenchreport" ./cmd/sgspbenchreport

run_trial() {
  local implementation=$1 clients=$2 hz=$3 rtt=$4 loss=$5 jitter=$6 reorder=$7 seed=$8 profile=$9
  local file="$raw_dir/${profile}-${implementation}-c${clients}-hz${hz}-rtt${rtt}-loss${loss}-j${jitter}-r${reorder}-seed${seed}.json"
  if [[ $resume == 1 ]] && jq -e \
    --arg implementation "$implementation" \
    --argjson clients "$clients" \
    --argjson hz "$hz" \
    --argjson seed "$seed" \
    '.status == "completed" and
     .config.implementation == $implementation and
     .config.clients == $clients and
     .config.hz == $hz and
     .config.seed == $seed' "$file" >/dev/null 2>&1; then
    echo "resume_skip=$file"
    return
  fi
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

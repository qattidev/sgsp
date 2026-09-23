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
  SGSPBENCH_REFERENCE_HOST=1       require the release-gate host minimums
  SGSPBENCH_RESUME=1               continue a matching interrupted campaign

The script builds reproducible local binaries, runs five seeds for each
healthy and impairment point, stores JSON under raw/, and writes summary.md.
Artifacts always remain below this checkout's artifacts/ directory; /tmp is
not a valid destination. Set SGSPBENCH_IMPAIRED_CLIENTS only after
identifying a healthy capacity. Resume only continues a campaign whose
configuration and source snapshot match its campaign.json manifest.

Set SGSPBENCH_REFERENCE_HOST=1 for release capacity evidence. It requires
GOMAXPROCS=4, at least four physical CPU cores, and at least 8 GiB of RAM
before creating a campaign. Leave it unset for constrained-host tooling
diagnostics, which are not release capacity evidence.

The script renders and retains all trial results even when a trial is invalid
or fails, then exits nonzero if any recorded trial is non-completed.
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
reference_host=${SGSPBENCH_REFERENCE_HOST:-0}
resume=${SGSPBENCH_RESUME:-0}
case "$reference_host" in
  0|1)
    ;;
  *)
    echo "SGSPBENCH_REFERENCE_HOST must be 0 or 1" >&2
    exit 2
    ;;
esac
reference_host_validation=not_requested
reference_host_physical_cores=unknown
reference_host_memory_kib=unknown
verify_reference_host() {
  if [[ $reference_host != 1 ]]; then
    return
  fi
  if [[ $gomaxprocs != 4 ]]; then
    echo "reference-host capacity evidence requires SGSPBENCH_GOMAXPROCS=4; got $gomaxprocs" >&2
    exit 2
  fi
  if command -v lscpu >/dev/null 2>&1; then
    reference_host_physical_cores=$(lscpu -p=CORE,SOCKET | awk -F, '$1 !~ /^#/ { seen[$1 "," $2] = 1 } END { for (key in seen) { count++ } print count }')
  elif [[ $(uname -s) == Darwin ]] && command -v sysctl >/dev/null 2>&1; then
    reference_host_physical_cores=$(sysctl -n hw.physicalcpu)
  else
    echo "reference-host capacity evidence requires lscpu or macOS sysctl to verify physical cores" >&2
    exit 2
  fi
  if ! [[ $reference_host_physical_cores =~ ^[0-9]+$ ]] || (( reference_host_physical_cores < 4 )); then
    echo "reference-host capacity evidence requires at least four physical cores; got $reference_host_physical_cores" >&2
    exit 2
  fi
  if [[ -r /proc/meminfo ]]; then
    reference_host_memory_kib=$(awk '/^MemTotal:/ { print $2; exit }' /proc/meminfo)
  elif [[ $(uname -s) == Darwin ]] && command -v sysctl >/dev/null 2>&1; then
    reference_host_memory_bytes=$(sysctl -n hw.memsize)
    if [[ $reference_host_memory_bytes =~ ^[0-9]+$ ]]; then
      reference_host_memory_kib=$((reference_host_memory_bytes / 1024))
    fi
  else
    echo "reference-host capacity evidence requires /proc/meminfo or macOS sysctl to verify RAM" >&2
    exit 2
  fi
  if ! [[ $reference_host_memory_kib =~ ^[0-9]+$ ]] || (( reference_host_memory_kib < 8388608 )); then
    echo "reference-host capacity evidence requires at least 8 GiB RAM; got ${reference_host_memory_kib:-unknown} KiB" >&2
    exit 2
  fi
  reference_host_validation=passed
}
verify_reference_host
bin_dir="$artifacts/bin"
raw_dir="$artifacts/raw"
mkdir -p "$bin_dir" "$raw_dir"
if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to create and validate the local campaign manifest" >&2
  exit 2
fi
source_revision=$(git rev-parse HEAD 2>/dev/null || printf 'unknown')
# Hash every tracked and non-ignored untracked input's current contents. A
# git-diff hash omits untracked Go sources, which can still change the binaries
# built below and would otherwise let a resumed campaign mix source snapshots.
source_snapshot_hash() {
  git ls-files -co --exclude-standard -z | while IFS= read -r -d '' path; do
    printf '%s\0' "$path"
    if [[ -f $path ]]; then
      sha256sum -- "$path"
    else
      printf 'missing\n'
    fi
  done | sha256sum | awk '{print $1}'
}
source_dirty_hash=$(source_snapshot_hash)
go_version=$(go version)
campaign_file="$artifacts/campaign.json"
campaign_matches() {
  jq -e \
    --arg warmup "$warmup" \
    --arg duration "$duration" \
    --arg healthy_clients "$healthy_clients" \
    --arg impaired_clients "$impaired_clients" \
    --arg gomaxprocs "$gomaxprocs" \
    --arg reference_host "$reference_host" \
    --arg source_revision "$source_revision" \
    --arg source_dirty_hash "$source_dirty_hash" \
    --arg go_version "$go_version" \
    '.schema == 1 and
     .warmup == $warmup and
     .duration == $duration and
     .healthy_clients == $healthy_clients and
     .impaired_clients == $impaired_clients and
     .gomaxprocs == $gomaxprocs and
     .reference_host == $reference_host and
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
    --arg reference_host "$reference_host" \
    --arg source_revision "$source_revision" \
    --arg source_dirty_hash "$source_dirty_hash" \
    --arg go_version "$go_version" \
    '{schema: 1, warmup: $warmup, duration: $duration,
      healthy_clients: $healthy_clients, impaired_clients: $impaired_clients,
      gomaxprocs: $gomaxprocs, reference_host: $reference_host,
      source_revision: $source_revision,
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
  echo "reference_host_required=$reference_host"
  echo "reference_host_validation=$reference_host_validation"
  echo "reference_host_physical_cores=$reference_host_physical_cores"
  echo "reference_host_memory_kib=$reference_host_memory_kib"
  echo "build_flags=-trimpath"
  # Keep copy-and-pasteable commands with the evidence rather than requiring
  # an auditor to reconstruct them from flags and binary paths.
  printf 'build_sgspbench_command=GOMAXPROCS=%q go build -trimpath -o %q ./cmd/sgspbench\n' \
    "$gomaxprocs" "$bin_dir/sgspbench"
  printf 'build_sgspbenchreport_command=GOMAXPROCS=%q go build -trimpath -o %q ./cmd/sgspbenchreport\n' \
    "$gomaxprocs" "$bin_dir/sgspbenchreport"
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
non_completed_trials=0
expected_trials_file="$artifacts/expected-trials.tsv"
: > "$expected_trials_file"

run_trial() {
  local implementation=$1 clients=$2 hz=$3 rtt=$4 loss=$5 jitter=$6 reorder=$7 seed=$8 profile=$9
  local file="$raw_dir/${profile}-${implementation}-c${clients}-hz${hz}-rtt${rtt}-loss${loss}-j${jitter}-r${reorder}-seed${seed}.json"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$implementation" "$clients" "$hz" "$rtt" "$loss" "$jitter" "$reorder" "$seed" "$profile" "$file" >> "$expected_trials_file"
  if [[ $resume == 1 ]] && jq -e \
    --arg implementation "$implementation" \
    --argjson clients "$clients" \
    --argjson hz "$hz" \
    --argjson loss "$loss" \
    --argjson reorder "$reorder" \
    --argjson seed "$seed" \
    '.status == "completed" and
     .config.implementation == $implementation and
     .config.clients == $clients and
     .config.hz == $hz and
     .config.loss == $loss and
     .config.reorder == $reorder and
     .config.seed == $seed' "$file" >/dev/null 2>&1; then
    echo "resume_skip=$file"
    return
  fi
  if GOMAXPROCS="$gomaxprocs" "$bin_dir/sgspbench" \
    --implementation "$implementation" --clients "$clients" --hz "$hz" \
    --input-bytes 64 --update-bytes 512 --rpc-per-second 2 --bulk-bytes-per-second 65536 \
    --warmup "$warmup" --duration "$duration" --rtt "$rtt" --jitter "$jitter" \
    --loss "$loss" --reorder "$reorder" --seed "$seed" --output "$file"; then
    return
  fi
  non_completed_trials=$((non_completed_trials + 1))
  echo "trial_non_completed=$file"
  if [[ ! -s $file ]]; then
    echo "trial failed without a result file: $file" >&2
  fi
  return 0
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

matrix_coverage_failures=0
expected_trials=0
while IFS=$'\t' read -r implementation clients hz rtt loss jitter reorder seed profile file; do
  expected_trials=$((expected_trials + 1))
  if [[ ! -s $file ]]; then
    matrix_coverage_failures=$((matrix_coverage_failures + 1))
    echo "matrix_missing_trial=$file" >&2
    continue
  fi
  if ! jq -e \
    --arg implementation "$implementation" \
    --argjson clients "$clients" \
    --argjson hz "$hz" \
    --argjson loss "$loss" \
    --argjson reorder "$reorder" \
    --argjson seed "$seed" \
    '.status == "completed" and
     .config.implementation == $implementation and
     .config.clients == $clients and
     .config.hz == $hz and
     .config.loss == $loss and
     .config.reorder == $reorder and
     .config.seed == $seed' "$file" >/dev/null; then
    matrix_coverage_failures=$((matrix_coverage_failures + 1))
    echo "matrix_incomplete_or_mismatched_trial=$file" >&2
  fi
done < "$expected_trials_file"
if (( expected_trials == 0 )); then
  matrix_coverage_failures=1
  echo "matrix_expected_trial_plan_is_empty" >&2
fi
echo "matrix_expected_trials=$expected_trials"
echo "matrix_coverage_failures=$matrix_coverage_failures"

summary_file="$artifacts/summary.md"
"$bin_dir/sgspbenchreport" --input "$raw_dir" --output "$summary_file"
{
  echo
  echo '## Campaign context'
  echo
  echo '- This report is a measurement index, not a release-gate verdict.'
  echo '- Campaign manifest: `campaign.json`'
  echo "- Latest environment manifest: \`$(basename "$environment_file")\`"
  echo '- Expected trial plan: `expected-trials.tsv`'
  echo "- Source revision: \`$source_revision\`"
  echo "- Source snapshot hash: \`$source_dirty_hash\`"
  echo "- Reference-host validation: \`$reference_host_validation\`"
  echo "- Expected trials: $expected_trials"
  echo "- Coverage failures: $matrix_coverage_failures"
  echo "- Invocation non-completed trials: $non_completed_trials"
} >> "$summary_file"
echo "Artifacts written to $artifacts" >&2
if (( non_completed_trials > 0 || matrix_coverage_failures > 0 )); then
  echo "matrix_non_completed_trials=$non_completed_trials matrix_coverage_failures=$matrix_coverage_failures" >&2
  exit 1
fi

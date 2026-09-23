#!/usr/bin/env bash
# Run the architecture's extended wire-codec fuzz gate with locally retained
# evidence. A non-completed target never appears as a successful fuzz gate.
set -euo pipefail

if [[ ${1:-} == "--help" ]]; then
  cat <<'USAGE'
Usage: scripts/run-fuzz-campaign.sh [artifacts/subdirectory]

Environment overrides:
  SGSP_FUZZ_DURATION=10m
  SGSP_FUZZ_SLICE_DURATION=10m
  SGSP_FUZZ_SLICES=1
  SGSP_FUZZ_MAX_SLICES_PER_INVOCATION=1
  SGSP_FUZZ_TEST_TIMEOUT=12m
  SGSP_FUZZ_GOMAXPROCS=1
  SGSP_FUZZ_TARGETS="FuzzControl FuzzEvent FuzzRequest"
  SGSP_FUZZ_RESUME=1

The default run executes each Architecture.md wire fuzz target for one
continuous ten-minute slice. Set SGSP_FUZZ_SLICE_DURATION and
SGSP_FUZZ_SLICES to record an explicit aggregate duration as resumable slices
(their product must equal SGSP_FUZZ_DURATION); all duration values must use
whole ns, us, ms, s, m, or h units. SGSP_FUZZ_MAX_SLICES_PER_INVOCATION bounds
the work done by one invocation.
It retains a campaign manifest, a local Go cache, per-target logs and status
JSON, and summary.md below this checkout's artifacts/ directory. /tmp and
other destinations are refused. Resume continues incomplete targets in a
matching source and configuration snapshot; failed or interrupted targets are
run again and retain appended output.
USAGE
  exit 0
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root=$(cd -- "$script_dir/.." && pwd)
cd "$root"

artifact_argument=${1:-"artifacts/fuzz-$(date -u +%Y%m%dT%H%M%SZ)"}
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

duration=${SGSP_FUZZ_DURATION:-10m}
slice_duration=${SGSP_FUZZ_SLICE_DURATION:-$duration}
slices=${SGSP_FUZZ_SLICES:-1}
max_slices_per_invocation=${SGSP_FUZZ_MAX_SLICES_PER_INVOCATION:-$slices}
test_timeout=${SGSP_FUZZ_TEST_TIMEOUT:-12m}
gomaxprocs=${SGSP_FUZZ_GOMAXPROCS:-1}
targets=${SGSP_FUZZ_TARGETS:-"FuzzControl FuzzEvent FuzzRequest"}
resume=${SGSP_FUZZ_RESUME:-0}
case "$resume" in
  0|1)
    ;;
  *)
    echo "SGSP_FUZZ_RESUME must be 0 or 1" >&2
    exit 2
    ;;
esac
duration_to_ns() {
  local remaining=$1
  local value
  local unit
  local multiplier
  local total=0
  while [[ -n $remaining ]]; do
    if ! [[ $remaining =~ ^([0-9]+)(ns|us|µs|ms|s|m|h)(.*)$ ]]; then
      return 1
    fi
    value=${BASH_REMATCH[1]}
    unit=${BASH_REMATCH[2]}
    remaining=${BASH_REMATCH[3]}
    case "$unit" in
      ns) multiplier=1 ;;
      us|µs) multiplier=1000 ;;
      ms) multiplier=1000000 ;;
      s) multiplier=1000000000 ;;
      m) multiplier=60000000000 ;;
      h) multiplier=3600000000000 ;;
    esac
    total=$((total + value * multiplier))
  done
  printf '%s\n' "$total"
}
if ! duration_ns=$(duration_to_ns "$duration"); then
  echo "SGSP_FUZZ_DURATION must use whole ns, us, ms, s, m, or h units" >&2
  exit 2
fi
if ! slice_duration_ns=$(duration_to_ns "$slice_duration"); then
  echo "SGSP_FUZZ_SLICE_DURATION must use whole ns, us, ms, s, m, or h units" >&2
  exit 2
fi
if (( duration_ns <= 0 || slice_duration_ns <= 0 )); then
  echo "SGSP_FUZZ_DURATION and SGSP_FUZZ_SLICE_DURATION must be positive" >&2
  exit 2
fi
if ! [[ $slices =~ ^[1-9][0-9]*$ ]]; then
  echo "SGSP_FUZZ_SLICES must be a positive integer" >&2
  exit 2
fi
if ! [[ $max_slices_per_invocation =~ ^[1-9][0-9]*$ ]]; then
  echo "SGSP_FUZZ_MAX_SLICES_PER_INVOCATION must be a positive integer" >&2
  exit 2
fi
if (( slice_duration_ns * slices != duration_ns )); then
  echo "SGSP_FUZZ_SLICE_DURATION multiplied by SGSP_FUZZ_SLICES must equal SGSP_FUZZ_DURATION" >&2
  exit 2
fi
if [[ -z ${targets//[[:space:]]/} ]]; then
  echo "SGSP_FUZZ_TARGETS must name at least one supported target" >&2
  exit 2
fi
for target in $targets; do
  case "$target" in
    FuzzControl|FuzzEvent|FuzzRequest)
      ;;
    *)
      echo "unsupported fuzz target: $target" >&2
      exit 2
      ;;
  esac
done
if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to create and validate local fuzz evidence" >&2
  exit 2
fi

logs_dir="$artifacts/logs"
status_dir="$artifacts/status"
cache_dir="$artifacts/go-cache"
mkdir -p "$logs_dir" "$status_dir" "$cache_dir"

source_revision=$(git rev-parse HEAD 2>/dev/null || printf 'unknown')
source_dirty_hash=$(git diff --no-ext-diff HEAD | sha256sum | awk '{print $1}')
go_version=$(go version)
campaign_file="$artifacts/campaign.json"
campaign_matches() {
  jq -e \
    --arg duration "$duration" \
    --arg slice_duration "$slice_duration" \
    --arg slices "$slices" \
    --arg test_timeout "$test_timeout" \
    --arg gomaxprocs "$gomaxprocs" \
    --arg targets "$targets" \
    --arg source_revision "$source_revision" \
    --arg source_dirty_hash "$source_dirty_hash" \
    --arg go_version "$go_version" \
    '.schema == 2 and
     .duration == $duration and
     .slice_duration == $slice_duration and
     .slices == $slices and
     .test_timeout == $test_timeout and
     .gomaxprocs == $gomaxprocs and
     .targets == $targets and
     .source_revision == $source_revision and
     .source_dirty_hash == $source_dirty_hash and
     .go_version == $go_version' "$campaign_file" >/dev/null
}
if [[ -e $campaign_file ]]; then
  if ! campaign_matches; then
    echo "campaign manifest does not match this configuration or source snapshot; use a new artifacts/ directory" >&2
    exit 2
  fi
elif find "$logs_dir" "$status_dir" -type f -print -quit | grep -q .; then
  echo "fuzz evidence exists without a campaign manifest; refusing to mix evidence" >&2
  exit 2
else
  jq -n \
    --arg duration "$duration" \
    --arg slice_duration "$slice_duration" \
    --arg slices "$slices" \
    --arg test_timeout "$test_timeout" \
    --arg gomaxprocs "$gomaxprocs" \
    --arg targets "$targets" \
    --arg source_revision "$source_revision" \
    --arg source_dirty_hash "$source_dirty_hash" \
    --arg go_version "$go_version" \
    '{schema: 2, duration: $duration, slice_duration: $slice_duration,
      slices: $slices, test_timeout: $test_timeout, gomaxprocs: $gomaxprocs,
      targets: $targets,
      source_revision: $source_revision, source_dirty_hash: $source_dirty_hash,
      go_version: $go_version}' > "$campaign_file"
fi

run_started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
run_log="$artifacts/run.log"
printf 'fuzz_campaign_started_utc=%s\n' "$run_started_utc" | tee -a "$run_log"
printf 'fuzz_campaign_manifest=%s\n' "$campaign_file" | tee -a "$run_log"

non_completed_targets=0
write_target_status() {
  local target=$1
  local status_file=$2
  local outcome=$3
  local target_started_utc=$4
  local exit_code=$5
  local completed_slices=$6
  local elapsed_seconds=$7
  local log_file=$8
  jq -n \
    --arg target "$target" \
    --arg status "$outcome" \
    --arg started_utc "$target_started_utc" \
    --arg updated_utc "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg duration "$duration" \
    --arg slice_duration "$slice_duration" \
    --arg slices "$slices" \
    --arg test_timeout "$test_timeout" \
    --arg gomaxprocs "$gomaxprocs" \
    --arg go_version "$go_version" \
    --arg log "$log_file" \
    --argjson exit_code "$exit_code" \
    --argjson completed_slices "$completed_slices" \
    --argjson elapsed_seconds "$elapsed_seconds" \
    '{schema: 2, target: $target, status: $status, exit_code: $exit_code,
      started_utc: $started_utc, updated_utc: $updated_utc,
      requested_duration: $duration, slice_duration: $slice_duration,
      requested_slices: $slices, completed_slices: $completed_slices,
      test_timeout: $test_timeout, gomaxprocs: $gomaxprocs,
      go_version: $go_version, elapsed_seconds: $elapsed_seconds, log: $log}' \
      > "$status_file"
}

run_target() {
  local target=$1
  local status_file="$status_dir/$target.json"
  local log_file="$logs_dir/$target.log"
  local target_started_utc
  local elapsed_seconds=0
  local exit_code
  local started_seconds
  local outcome
  local completed_slices=0
  local slice_number
  local last_slice
  local slices_this_invocation

  if [[ $resume == 1 ]] && jq -e \
    --arg target "$target" \
    --argjson requested_slices "$slices" \
    '.schema == 2 and .target == $target and .completed_slices == $requested_slices and
     .status == "completed" and .exit_code == 0' \
    "$status_file" >/dev/null 2>&1; then
    printf 'fuzz_resume_skip=%s\n' "$target" | tee -a "$run_log"
    return
  fi

  target_started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  if [[ $resume == 1 ]] && [[ -e $status_file ]] && jq -e \
    --arg target "$target" \
    '.schema == 2 and .target == $target and (.completed_slices | type == "number") and
     (.elapsed_seconds | type == "number")' "$status_file" >/dev/null 2>&1; then
    completed_slices=$(jq -r '.completed_slices' "$status_file")
    elapsed_seconds=$(jq -r '.elapsed_seconds' "$status_file")
    target_started_utc=$(jq -r '.started_utc' "$status_file")
  fi
  if (( completed_slices > slices )); then
    echo "fuzz status has more completed slices than this campaign permits: $target" >&2
    exit 2
  fi
  slices_this_invocation=$((slices - completed_slices))
  if (( slices_this_invocation > max_slices_per_invocation )); then
    slices_this_invocation=$max_slices_per_invocation
  fi
  last_slice=$((completed_slices + slices_this_invocation))

  for ((slice_number = completed_slices + 1; slice_number <= last_slice; slice_number++)); do
    started_seconds=$SECONDS
    {
      printf 'target=%s\n' "$target"
      printf 'slice=%s/%s\n' "$slice_number" "$slices"
      printf 'started_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      printf 'command=GOCACHE=%q GOMAXPROCS=%q go test ./internal/wire -run %q -fuzz %q -fuzztime %q -parallel=1 -timeout %q\n' \
        "$cache_dir" "$gomaxprocs" '^$' "^$target$" "$slice_duration" "$test_timeout"
    } | tee -a "$log_file" "$run_log"

    set +e
    GOCACHE="$cache_dir" GOMAXPROCS="$gomaxprocs" \
      go test ./internal/wire -run '^$' -fuzz "^$target$" -fuzztime "$slice_duration" \
        -parallel=1 -timeout "$test_timeout" 2>&1 | tee -a "$log_file" "$run_log"
    exit_code=${PIPESTATUS[0]}
    set -e

    elapsed_seconds=$((elapsed_seconds + SECONDS - started_seconds))
    if (( exit_code != 0 )); then
      write_target_status "$target" "$status_file" failed "$target_started_utc" \
        "$exit_code" "$completed_slices" "$elapsed_seconds" "$log_file"
      printf 'fuzz_target_status=%s status=failed exit_code=%s completed_slices=%s elapsed_seconds=%s\n' \
        "$target" "$exit_code" "$completed_slices" "$elapsed_seconds" | tee -a "$run_log"
      non_completed_targets=$((non_completed_targets + 1))
      return
    fi

    completed_slices=$slice_number
    if (( completed_slices == slices )); then
      outcome=completed
    else
      outcome=partial
    fi
    write_target_status "$target" "$status_file" "$outcome" "$target_started_utc" \
      0 "$completed_slices" "$elapsed_seconds" "$log_file"
    printf 'fuzz_target_status=%s status=%s exit_code=0 completed_slices=%s elapsed_seconds=%s\n' \
      "$target" "$outcome" "$completed_slices" "$elapsed_seconds" | tee -a "$run_log"
  done

  if (( completed_slices < slices )); then
    non_completed_targets=$((non_completed_targets + 1))
  fi
}

for target in $targets; do
  run_target "$target"
done

summary_file="$artifacts/summary.md"
{
  echo '# Wire fuzz campaign'
  echo
  echo "- Started: $run_started_utc"
  echo "- Requested duration per target: $duration"
  echo "- Requested slices per target: $slices × $slice_duration"
  echo "- Maximum slices this invocation: $max_slices_per_invocation"
  echo "- Test timeout: $test_timeout"
  echo "- Go toolchain: $go_version"
  echo "- Campaign manifest: \`campaign.json\`"
  echo "- Local Go cache: \`go-cache/\`"
  echo
  echo '## Targets'
  echo
  for target in $targets; do
    if [[ -e $status_dir/$target.json ]]; then
      jq -r '"- `\(.target)`: \(.status), slices \(.completed_slices)/\(.requested_slices), exit \(.exit_code), elapsed \(.elapsed_seconds)s; log `logs/\(.target).log`"' \
        "$status_dir/$target.json"
    else
      echo "- \`$target\`: interrupted before a terminal result; inspect \`logs/$target.log\`."
    fi
  done
} > "$summary_file"
printf 'fuzz_campaign_summary=%s\n' "$summary_file" | tee -a "$run_log"
if (( non_completed_targets > 0 )); then
  printf 'fuzz_campaign_non_completed_targets=%s\n' "$non_completed_targets" | tee -a "$run_log" >&2
  exit 1
fi

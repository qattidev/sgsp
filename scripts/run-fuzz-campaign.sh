#!/usr/bin/env bash
# Run the architecture's extended wire-codec fuzz gate with locally retained
# evidence. A non-completed target never appears as a successful fuzz gate.
set -euo pipefail

if [[ ${1:-} == "--help" ]]; then
  cat <<'USAGE'
Usage: scripts/run-fuzz-campaign.sh [artifacts/subdirectory]

Environment overrides:
  SGSP_FUZZ_DURATION=10m
  SGSP_FUZZ_TEST_TIMEOUT=12m
  SGSP_FUZZ_GOMAXPROCS=1
  SGSP_FUZZ_TARGETS="FuzzControl FuzzEvent FuzzRequest"
  SGSP_FUZZ_RESUME=1

The default run executes each Architecture.md wire fuzz target for ten minutes.
It retains a campaign manifest, a local Go cache, per-target logs and status
JSON, and summary.md below this checkout's artifacts/ directory. /tmp and
other destinations are refused. Resume only skips targets completed by a
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
    --arg test_timeout "$test_timeout" \
    --arg gomaxprocs "$gomaxprocs" \
    --arg targets "$targets" \
    --arg source_revision "$source_revision" \
    --arg source_dirty_hash "$source_dirty_hash" \
    --arg go_version "$go_version" \
    '.schema == 1 and
     .duration == $duration and
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
    --arg test_timeout "$test_timeout" \
    --arg gomaxprocs "$gomaxprocs" \
    --arg targets "$targets" \
    --arg source_revision "$source_revision" \
    --arg source_dirty_hash "$source_dirty_hash" \
    --arg go_version "$go_version" \
    '{schema: 1, duration: $duration, test_timeout: $test_timeout,
      gomaxprocs: $gomaxprocs, targets: $targets,
      source_revision: $source_revision, source_dirty_hash: $source_dirty_hash,
      go_version: $go_version}' > "$campaign_file"
fi

run_started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
run_log="$artifacts/run.log"
printf 'fuzz_campaign_started_utc=%s\n' "$run_started_utc" | tee -a "$run_log"
printf 'fuzz_campaign_manifest=%s\n' "$campaign_file" | tee -a "$run_log"

non_completed_targets=0
run_target() {
  local target=$1
  local status_file="$status_dir/$target.json"
  local log_file="$logs_dir/$target.log"
  local target_started_utc
  local elapsed_seconds
  local exit_code
  local started_seconds
  local outcome

  if [[ $resume == 1 ]] && jq -e \
    --arg target "$target" \
    '.status == "completed" and .target == $target and .exit_code == 0' \
    "$status_file" >/dev/null 2>&1; then
    printf 'fuzz_resume_skip=%s\n' "$target" | tee -a "$run_log"
    return
  fi

  target_started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  started_seconds=$SECONDS
  {
    printf 'target=%s\n' "$target"
    printf 'started_utc=%s\n' "$target_started_utc"
    printf 'command=GOCACHE=%q GOMAXPROCS=%q go test ./internal/wire -run %q -fuzz %q -fuzztime %q -parallel=1 -timeout %q\n' \
      "$cache_dir" "$gomaxprocs" '^$' "^$target$" "$duration" "$test_timeout"
  } | tee -a "$log_file" "$run_log"

  set +e
  GOCACHE="$cache_dir" GOMAXPROCS="$gomaxprocs" \
    go test ./internal/wire -run '^$' -fuzz "^$target$" -fuzztime "$duration" \
      -parallel=1 -timeout "$test_timeout" 2>&1 | tee -a "$log_file" "$run_log"
  exit_code=${PIPESTATUS[0]}
  set -e

  elapsed_seconds=$((SECONDS - started_seconds))
  if (( exit_code == 0 )); then
    outcome=completed
  else
    outcome=failed
    non_completed_targets=$((non_completed_targets + 1))
  fi
  jq -n \
    --arg target "$target" \
    --arg status "$outcome" \
    --arg started_utc "$target_started_utc" \
    --arg completed_utc "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg duration "$duration" \
    --arg test_timeout "$test_timeout" \
    --arg gomaxprocs "$gomaxprocs" \
    --arg go_version "$go_version" \
    --arg log "$log_file" \
    --argjson exit_code "$exit_code" \
    --argjson elapsed_seconds "$elapsed_seconds" \
    '{schema: 1, target: $target, status: $status, exit_code: $exit_code,
      started_utc: $started_utc, completed_utc: $completed_utc,
      requested_duration: $duration, test_timeout: $test_timeout,
      gomaxprocs: $gomaxprocs, go_version: $go_version,
      elapsed_seconds: $elapsed_seconds, log: $log}' > "$status_file"
  printf 'fuzz_target_status=%s status=%s exit_code=%s elapsed_seconds=%s\n' \
    "$target" "$outcome" "$exit_code" "$elapsed_seconds" | tee -a "$run_log"
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
  echo "- Test timeout: $test_timeout"
  echo "- Go toolchain: $go_version"
  echo "- Campaign manifest: \`campaign.json\`"
  echo "- Local Go cache: \`go-cache/\`"
  echo
  echo '## Targets'
  echo
  for target in $targets; do
    if [[ -e $status_dir/$target.json ]]; then
      jq -r '"- `\(.target)`: \(.status), exit \(.exit_code), elapsed \(.elapsed_seconds)s; log `logs/\(.target).log`"' \
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

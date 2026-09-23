#!/usr/bin/env bash
# Run the M8 slow-consumer resource-plateau gate with retained local evidence.
set -euo pipefail

if [[ ${1:-} == "--help" ]]; then
  cat <<'USAGE'
Usage: scripts/run-resource-plateau.sh [artifacts/subdirectory]

Environment overrides:
  SGSP_RESOURCE_PLATEAU_DURATION=10m
  SGSP_RESOURCE_PLATEAU_TEST_TIMEOUT=12m

The default run drives real QUIC slow-consumer closures for ten minutes. It
retains a campaign manifest, local Go cache, environment manifest, Go test
log, machine-readable result.json, and summary.md below this checkout's
artifacts/ directory. /tmp and other destinations are refused. A nonzero test
exit or missing/incomplete result makes the runner exit nonzero after its log
and status evidence are retained.
USAGE
  exit 0
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root=$(cd -- "$script_dir/.." && pwd)
cd "$root"

artifact_argument=${1:-"artifacts/resource-plateau-$(date -u +%Y%m%dT%H%M%SZ)"}
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

duration=${SGSP_RESOURCE_PLATEAU_DURATION:-10m}
test_timeout=${SGSP_RESOURCE_PLATEAU_TEST_TIMEOUT:-12m}
if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to create and validate local plateau evidence" >&2
  exit 2
fi
source_revision=$(git rev-parse HEAD 2>/dev/null || printf 'unknown')
source_dirty_hash=$(git diff --no-ext-diff HEAD | sha256sum | awk '{print $1}')
go_version=$(go version)
campaign_file="$artifacts/campaign.json"
if [[ -e $campaign_file ]]; then
  echo "resource plateau campaign already exists; use a new artifacts/ directory to avoid mixing evidence" >&2
  exit 2
fi
if [[ -e $artifacts ]] && find "$artifacts" -mindepth 1 -maxdepth 1 -print -quit | grep -q .; then
  echo "resource plateau artifacts already exist without a campaign manifest; refusing to mix evidence" >&2
  exit 2
fi
mkdir -p "$artifacts/go-cache"
jq -n \
  --arg duration "$duration" \
  --arg test_timeout "$test_timeout" \
  --arg source_revision "$source_revision" \
  --arg source_dirty_hash "$source_dirty_hash" \
  --arg go_version "$go_version" \
  '{schema: 1, duration: $duration, test_timeout: $test_timeout, source_revision: $source_revision,
    source_dirty_hash: $source_dirty_hash, go_version: $go_version}' > "$campaign_file"

run_log="$artifacts/run.log"
exec > >(tee -a "$run_log") 2>&1
run_started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "resource_plateau_started_utc=$run_started_utc"
echo "resource_plateau_manifest=$campaign_file"

environment_file="$artifacts/environment.txt"
{
  echo "timestamp_utc=$run_started_utc"
  echo "requested_duration=$duration"
  echo "test_timeout=$test_timeout"
  echo "go_version=$go_version"
  echo "go_env=$(go env GOOS GOARCH GOVERSION)"
  echo "kernel=$(uname -sr)"
  if command -v lscpu >/dev/null 2>&1; then
    lscpu
  fi
  if command -v free >/dev/null 2>&1; then
    free -b
  fi
} > "$environment_file"
echo "resource_plateau_environment=$environment_file"

result_file="$artifacts/result.json"
test_log="$artifacts/test.log"
printf 'resource_plateau_command=GOCACHE=%q SGSP_RESOURCE_PLATEAU_DURATION=%q SGSP_RESOURCE_PLATEAU_OUTPUT=%q go test -count=1 -timeout=%q -run %q .\n' \
  "$artifacts/go-cache" "$duration" "$result_file" "$test_timeout" '^TestResourcePlateau$' | tee -a "$test_log"
set +e
GOCACHE="$artifacts/go-cache" SGSP_RESOURCE_PLATEAU_DURATION="$duration" \
  SGSP_RESOURCE_PLATEAU_OUTPUT="$result_file" \
  go test -count=1 -timeout="$test_timeout" -run '^TestResourcePlateau$' . 2>&1 | tee -a "$test_log"
test_status=${PIPESTATUS[0]}
set -e

status=failed
if (( test_status == 0 )) && jq -e --arg duration "$duration" \
  '.schema == 1 and .status == "completed" and .requested_duration == $duration and
   (.cycles | type == "number" and . > 0) and
   (.samples | type == "array" and length > 0)' "$result_file" >/dev/null 2>&1; then
  status=completed
fi
status_file="$artifacts/status.json"
jq -n \
  --arg status "$status" \
  --arg duration "$duration" \
  --arg test_timeout "$test_timeout" \
  --arg started_utc "$run_started_utc" \
  --arg completed_utc "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg result "$result_file" \
  --arg log "$test_log" \
  --argjson exit_code "$test_status" \
  '{schema: 1, status: $status, requested_duration: $duration, test_timeout: $test_timeout,
    started_utc: $started_utc, completed_utc: $completed_utc,
    exit_code: $exit_code, result: $result, log: $log}' > "$status_file"
summary_file="$artifacts/summary.md"
{
  echo '# Slow-consumer resource plateau'
  echo
  echo "- Requested duration: $duration"
  echo "- Go test timeout: $test_timeout"
  echo "- Campaign manifest: \`campaign.json\`"
  echo "- Environment: \`environment.txt\`"
  echo "- Test log: \`test.log\`"
  echo "- Terminal status: \`status.json\`"
  if [[ $status == completed ]]; then
    jq -r '"- Cycles: \(.cycles)\n- Warmed heap: \(.warmed_baseline.heap_alloc) B\n- Peak heap: \(.peak.heap_alloc) B\n- Teardown heap: \(.teardown.heap_alloc) B\n- Warmed goroutines: \(.warmed_baseline.goroutines)\n- Peak goroutines: \(.peak.goroutines)\n- Teardown goroutines: \(.teardown.goroutines)"' "$result_file"
  else
    echo '- Result: incomplete or failed; inspect `test.log` and `status.json`.'
  fi
} > "$summary_file"
echo "resource_plateau_summary=$summary_file"
if [[ $status != completed ]]; then
  echo "resource_plateau_status=failed exit_code=$test_status" >&2
  exit 1
fi

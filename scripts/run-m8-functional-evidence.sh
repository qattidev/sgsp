#!/usr/bin/env bash
# Retain the section-12 protocol-invariant and reconnect evidence alongside
# benchmark artifacts. This is a functional complement to, not a substitute
# for, the reference-host capacity/impairment matrix.
set -euo pipefail

if [[ ${1:-} == "--help" ]]; then
  cat <<'USAGE'
Usage: scripts/run-m8-functional-evidence.sh [artifacts/subdirectory]

Environment overrides:
  SGSP_M8_FUNCTIONAL_RACE_COUNT=20     repetitions for protocol race tests
  SGSP_M8_FUNCTIONAL_TEST_TIMEOUT=4m   per go-test invocation timeout

The runner retains JSON test events, a manifest, host environment, log,
summary, and final status only below this checkout's artifacts/ directory.
It refuses /tmp and non-empty evidence directories. The protocol tests cover
ordering, epoch fencing, loss/unknown-outcome behavior, and cleanup; the
reconnect test covers bounded blackhole reconnection. This does not establish
benchmark capacity or impairment performance.
USAGE
  exit 0
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root=$(cd -- "$script_dir/.." && pwd)
cd "$root"
artifact_argument=${1:-"artifacts/m8-functional-$(date -u +%Y%m%dT%H%M%SZ)"}
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
if [[ -e $artifacts ]] && find "$artifacts" -mindepth 1 -print -quit | grep -q .; then
  echo "functional evidence directory is non-empty; refusing to overwrite: $artifact_argument" >&2
  exit 2
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to retain and validate functional evidence" >&2
  exit 2
fi

race_count=${SGSP_M8_FUNCTIONAL_RACE_COUNT:-20}
test_timeout=${SGSP_M8_FUNCTIONAL_TEST_TIMEOUT:-4m}
if ! [[ $race_count =~ ^[1-9][0-9]*$ ]]; then
  echo "SGSP_M8_FUNCTIONAL_RACE_COUNT must be a positive integer; got $race_count" >&2
  exit 2
fi

mkdir -p "$artifacts"
go_cache_dir="$artifacts/go-cache"
mkdir -p "$go_cache_dir"
source_snapshot_hash() {
  git ls-files -co --exclude-standard -z | while IFS= read -r -d '' path; do
    printf '%s\\0' "$path"
    if [[ -f $path ]]; then
      sha256sum -- "$path"
    else
      printf 'missing\\n'
    fi
  done | sha256sum | awk '{print $1}'
}
started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
source_revision=$(git rev-parse HEAD 2>/dev/null || printf 'unknown')
source_dirty_hash=$(source_snapshot_hash)
protocol_pattern='^(TestSequencedCoalescing|TestDispatchOrdering|TestEpochFence|TestEpochFenceRejectsOldReplyAndStream|TestAuthenticatedRequestLossHasUnknownOutcome|TestUnknownRequestOutcome|TestTransportLossCleanup)$'
protocol_tests=(
  TestSequencedCoalescing
  TestDispatchOrdering
  TestEpochFence
  TestEpochFenceRejectsOldReplyAndStream
  TestAuthenticatedRequestLossHasUnknownOutcome
  TestUnknownRequestOutcome
  TestTransportLossCleanup
)
reconnect_test=TestReconnectBlackholeDurations
campaign_file="$artifacts/campaign.json"
jq -n \
  --arg started_utc "$started_utc" \
  --arg source_revision "$source_revision" \
  --arg source_dirty_hash "$source_dirty_hash" \
  --arg protocol_pattern "$protocol_pattern" \
  --arg reconnect_test "$reconnect_test" \
  --arg test_timeout "$test_timeout" \
  --argjson race_count "$race_count" \
  '{schema: 1, started_utc: $started_utc, source_revision: $source_revision,
    source_dirty_hash: $source_dirty_hash, protocol_pattern: $protocol_pattern,
    reconnect_test: $reconnect_test, race_count: $race_count,
    test_timeout: $test_timeout}' > "$campaign_file"

run_log="$artifacts/run.log"
environment_file="$artifacts/environment.txt"
{
  echo "started_utc=$started_utc"
  echo "go_cache=$go_cache_dir"
  echo "go_version=$(go version)"
  echo "go_env=$(go env GOOS GOARCH GOVERSION)"
  echo "kernel=$(uname -sr)"
  echo "race_count=$race_count"
  echo "test_timeout=$test_timeout"
  printf 'protocol_command=GOCACHE=%q go test -race -count=%q -timeout=%q -json -run %q .\n' \
    "$go_cache_dir" "$race_count" "$test_timeout" "$protocol_pattern"
  printf 'reconnect_command=GOCACHE=%q go test -count=1 -timeout=%q -json -run %q .\n' \
    "$go_cache_dir" "$test_timeout" "^${reconnect_test}$"
} > "$environment_file"
exec > >(tee -a "$run_log") 2>&1
echo "functional_evidence_command=$0 $artifact_argument"
echo "functional_evidence_started_utc=$started_utc"
echo "campaign_manifest=$campaign_file"
echo "environment_manifest=$environment_file"

protocol_json="$artifacts/protocol-race.json"
echo "running protocol invariants with race detector"
set +e
GOCACHE="$go_cache_dir" go test -race -count="$race_count" -timeout="$test_timeout" -json \
  -run "$protocol_pattern" . > "$protocol_json"
protocol_exit_code=$?
set -e
protocol_passes=0
protocol_missing=0
for test_name in "${protocol_tests[@]}"; do
  pass_count=$(jq -r --arg test_name "$test_name" \
    'select(.Action == "pass" and .Test == $test_name) | .Test' "$protocol_json" | wc -l | tr -d ' ')
  echo "protocol_test=$test_name pass_count=$pass_count expected=$race_count"
  protocol_passes=$((protocol_passes + pass_count))
  if [[ $pass_count != "$race_count" ]]; then
    protocol_missing=$((protocol_missing + 1))
  fi
done

reconnect_json="$artifacts/reconnect-blackhole.json"
echo "running bounded reconnect blackhole evidence"
set +e
GOCACHE="$go_cache_dir" go test -count=1 -timeout="$test_timeout" -json \
  -run "^${reconnect_test}$" . > "$reconnect_json"
reconnect_exit_code=$?
set -e
reconnect_pass_count=$(jq -r --arg test_name "$reconnect_test" \
  'select(.Action == "pass" and .Test == $test_name) | .Test' "$reconnect_json" | wc -l | tr -d ' ')
echo "reconnect_test=$reconnect_test pass_count=$reconnect_pass_count expected=1"

completed_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
evidence_status=completed
if [[ $protocol_exit_code -ne 0 || $protocol_missing -ne 0 || $reconnect_exit_code -ne 0 || $reconnect_pass_count != 1 ]]; then
  evidence_status=failed
fi
status_file="$artifacts/status.json"
jq -n \
  --arg status "$evidence_status" \
  --arg started_utc "$started_utc" \
  --arg completed_utc "$completed_utc" \
  --arg protocol_json "$protocol_json" \
  --arg reconnect_json "$reconnect_json" \
  --argjson race_count "$race_count" \
  --argjson protocol_exit_code "$protocol_exit_code" \
  --argjson protocol_missing "$protocol_missing" \
  --argjson protocol_passes "$protocol_passes" \
  --argjson reconnect_exit_code "$reconnect_exit_code" \
  --argjson reconnect_pass_count "$reconnect_pass_count" \
  '{schema: 1, status: $status, started_utc: $started_utc, completed_utc: $completed_utc,
    protocol_json: $protocol_json, reconnect_json: $reconnect_json,
    race_count: $race_count, protocol_exit_code: $protocol_exit_code,
    protocol_missing_tests: $protocol_missing, protocol_passes: $protocol_passes,
    reconnect_exit_code: $reconnect_exit_code, reconnect_pass_count: $reconnect_pass_count}' > "$status_file"
summary_file="$artifacts/summary.md"
cat > "$summary_file" <<SUMMARY
# M8 functional evidence

- status: $evidence_status
- protocol race repetitions: $race_count
- protocol pass events: $protocol_passes / $((race_count * ${#protocol_tests[@]}))
- protocol tests missing complete repetition coverage: $protocol_missing
- reconnect blackhole pass events: $reconnect_pass_count / 1
- protocol JSON: `protocol-race.json`
- reconnect JSON: `reconnect-blackhole.json`

This validates protocol invariants and bounded reconnect behavior. It does not
establish the section-12 reference-host capacity, impairment, or SGSP-vs-QUIC
performance gates; those require a qualifying benchmark matrix campaign.
SUMMARY
echo "functional_evidence_status=$status_file status=$evidence_status"
if [[ $evidence_status != completed ]]; then
  exit 1
fi

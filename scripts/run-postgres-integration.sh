#!/usr/bin/env bash
# Run the M6 disposable-PostgreSQL gate with retained, repository-local evidence.
set -euo pipefail

if [[ ${1:-} == "--help" ]]; then
  cat <<'USAGE'
Usage: scripts/run-postgres-integration.sh [artifacts/subdirectory]

Required environment:
  SGSP_TEST_DATABASE_URL  Disposable PostgreSQL database URL; never logged.

Optional environment:
  SGSP_POSTGRES_TEST_COUNT=20
  SGSP_POSTGRES_TEST_TIMEOUT=12m

The runner retains a redacted environment manifest, source revision, local Go
cache, complete test log, status JSON, and summary below this checkout's
artifacts/ directory. It refuses /tmp and any existing campaign directory.
USAGE
  exit 0
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root=$(cd -- "$script_dir/.." && pwd)
cd "$root"

artifact_argument=${1:-"artifacts/postgres-integration-$(date -u +%Y%m%dT%H%M%SZ)"}
case "$artifact_argument" in
  artifacts|artifacts/*) ;;
  *)
    echo "artifact directory must be below artifacts/ in this checkout; refusing: $artifact_argument" >&2
    exit 2
    ;;
esac
artifacts="$root/$artifact_argument"
if [[ -z ${SGSP_TEST_DATABASE_URL:-} ]]; then
  echo "SGSP_TEST_DATABASE_URL is required for PostgreSQL integration; this is an incomplete check" >&2
  exit 2
fi
count=${SGSP_POSTGRES_TEST_COUNT:-20}
if ! [[ $count =~ ^[1-9][0-9]*$ ]]; then
  echo "SGSP_POSTGRES_TEST_COUNT must be a positive integer" >&2
  exit 2
fi
test_timeout=${SGSP_POSTGRES_TEST_TIMEOUT:-12m}
if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to create and validate local PostgreSQL evidence" >&2
  exit 2
fi
if [[ -e $artifacts/campaign.json ]]; then
  echo "PostgreSQL integration campaign already exists; use a new artifacts/ directory to avoid mixing evidence" >&2
  exit 2
fi
if [[ -e $artifacts ]] && find "$artifacts" -mindepth 1 -maxdepth 1 -print -quit | grep -q .; then
  echo "PostgreSQL integration artifacts already exist without a campaign manifest; refusing to mix evidence" >&2
  exit 2
fi

source_revision=$(git rev-parse HEAD 2>/dev/null || printf 'unknown')
source_dirty_hash=$(git diff --no-ext-diff HEAD | sha256sum | awk '{print $1}')
go_version=$(go version)
mkdir -p "$artifacts/go-cache"
campaign_file="$artifacts/campaign.json"
jq -n \
  --arg source_revision "$source_revision" \
  --arg source_dirty_hash "$source_dirty_hash" \
  --arg go_version "$go_version" \
  --argjson count "$count" \
  --arg test_timeout "$test_timeout" \
  '{schema: 1, source_revision: $source_revision, source_dirty_hash: $source_dirty_hash,
    go_version: $go_version, count: $count, test_timeout: $test_timeout}' > "$campaign_file"

run_log="$artifacts/run.log"
exec > >(tee -a "$run_log") 2>&1
started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "postgres_integration_started_utc=$started_utc"
echo "postgres_integration_manifest=$campaign_file"

environment_file="$artifacts/environment.txt"
{
  echo "timestamp_utc=$started_utc"
  echo "database_url=provided_redacted"
  echo "test_count=$count"
  echo "test_timeout=$test_timeout"
  echo "go_version=$go_version"
  go env GOOS GOARCH GOVERSION
  uname -a
  if command -v lscpu >/dev/null 2>&1; then
    lscpu
  fi
  if command -v free >/dev/null 2>&1; then
    free -b
  fi
} > "$environment_file"
echo "postgres_integration_environment=$environment_file"

test_log="$artifacts/test.log"
printf 'postgres_integration_command=GOCACHE=%q SGSP_TEST_DATABASE_URL=REDACTED go -C integration/postgres test -race -count=%q -timeout=%q -v ./...\n' \
  "$artifacts/go-cache" "$count" "$test_timeout" | tee -a "$test_log"
set +e
GOCACHE="$artifacts/go-cache" SGSP_TEST_DATABASE_URL="$SGSP_TEST_DATABASE_URL" \
  go -C integration/postgres test -race -count="$count" -timeout="$test_timeout" -v ./... 2>&1 | tee -a "$test_log"
test_status=${PIPESTATUS[0]}
set -e
completed_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
status=completed
if [[ $test_status -ne 0 ]]; then
  status=failed
fi
status_file="$artifacts/status.json"
jq -n \
  --arg status "$status" \
  --arg started_utc "$started_utc" \
  --arg completed_utc "$completed_utc" \
  --arg test_log "$test_log" \
  --arg environment "$environment_file" \
  --argjson exit_code "$test_status" \
  '{schema: 1, status: $status, started_utc: $started_utc, completed_utc: $completed_utc,
    exit_code: $exit_code, test_log: $test_log, environment: $environment}' > "$status_file"

summary_file="$artifacts/summary.md"
{
  echo '# PostgreSQL integration gate'
  echo
  echo "- Test count: $count"
  echo "- Go test timeout: $test_timeout"
  echo '- Database URL: provided and redacted'
  echo '- Campaign manifest: `campaign.json`'
  echo '- Environment: `environment.txt`'
  echo '- Test log: `test.log`'
  echo '- Terminal status: `status.json`'
  echo "- Exit code: $test_status"
} > "$summary_file"
echo "postgres_integration_summary=$summary_file"
if [[ $status != completed ]]; then
  echo "postgres_integration_status=failed exit_code=$test_status" >&2
  exit 1
fi

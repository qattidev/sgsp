#!/usr/bin/env bash
# Exercise the documented action-example commands from a clean local clone and
# retain the operator evidence. The resulting artifacts are deliberately local
# and never use /tmp as an evidence destination.
set -euo pipefail

if [[ ${1:-} == "--help" ]]; then
  cat <<'USAGE'
Usage: scripts/run-action-operator-evidence.sh [artifacts/subdirectory]

The runner requires a clean committed source worktree. It clones that revision
beneath the selected repository-local artifacts/ directory, runs direct mode
and the two-owner bootstrap mode on loopback, and retains logs, manifests,
status, and a local Go cache. It refuses /tmp and non-empty destinations.
USAGE
  exit 0
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root=$(cd -- "$script_dir/.." && pwd)
cd "$root"
artifact_argument=${1:-"artifacts/action-operator-$(date -u +%Y%m%dT%H%M%SZ)"}
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
if [[ -e $artifacts ]] && find "$artifacts" -mindepth 1 -print -quit | rg -q .; then
  echo "operator evidence directory is non-empty; refusing to overwrite: $artifact_argument" >&2
  exit 2
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to retain operator evidence" >&2
  exit 2
fi
if ! command -v setsid >/dev/null 2>&1; then
  echo "setsid is required to cleanly stop documented go run services" >&2
  exit 2
fi
if [[ -n $(git status --porcelain) ]]; then
  echo "source worktree must be clean before creating fresh-checkout evidence" >&2
  exit 2
fi

direct_port=${SGSP_ACTION_DIRECT_PORT:-45761}
owner_a_port=${SGSP_ACTION_OWNER_A_PORT:-45762}
owner_b_port=${SGSP_ACTION_OWNER_B_PORT:-45763}
bootstrap_port=${SGSP_ACTION_BOOTSTRAP_PORT:-45764}
for port in "$direct_port" "$owner_a_port" "$owner_b_port" "$bootstrap_port"; do
  if ! [[ $port =~ ^[0-9]+$ ]] || (( port < 1024 || port > 65535 )); then
    echo "action example ports must be in 1024..65535; got $port" >&2
    exit 2
  fi
done
if [[ $direct_port == "$owner_a_port" || $direct_port == "$owner_b_port" || $direct_port == "$bootstrap_port" || $owner_a_port == "$owner_b_port" || $owner_a_port == "$bootstrap_port" || $owner_b_port == "$bootstrap_port" ]]; then
  echo "action example ports must be distinct" >&2
  exit 2
fi

mkdir -p "$artifacts"
checkout="$artifacts/checkout"
go_cache_dir="$artifacts/go-cache"
mkdir -p "$go_cache_dir"
clone_log="$artifacts/clone.log"
if ! git clone --no-local "$root" "$checkout" > "$clone_log" 2>&1; then
  echo "fresh checkout clone failed; see $clone_log" >&2
  exit 1
fi
source_revision=$(git -C "$checkout" rev-parse HEAD)
started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
run_log="$artifacts/run.log"
exec > >(tee -a "$run_log") 2>&1
echo "action_operator_command=$0 $artifact_argument"
echo "action_operator_started_utc=$started_utc"
echo "fresh_checkout=$checkout"
echo "source_revision=$source_revision"

server_pids=()
servers_stopped=false
direct_client_exit=null
bootstrap_client_exit=null
direct_ok=false
bootstrap_ok=false
status_written=false

stop_servers() {
  local pid attempt
  for pid in "${server_pids[@]:-}"; do
    if kill -0 -- "-$pid" 2>/dev/null; then
      kill -INT -- "-$pid" 2>/dev/null || true
      for ((attempt = 1; attempt <= 50; attempt++)); do
        if ! kill -0 -- "-$pid" 2>/dev/null; then
          break
        fi
        sleep 0.1
      done
      if kill -0 -- "-$pid" 2>/dev/null; then
        kill -TERM -- "-$pid" 2>/dev/null || true
      fi
      for ((attempt = 1; attempt <= 20; attempt++)); do
        if ! kill -0 -- "-$pid" 2>/dev/null; then
          break
        fi
        sleep 0.1
      done
      if kill -0 -- "-$pid" 2>/dev/null; then
        kill -KILL -- "-$pid" 2>/dev/null || true
      fi
    fi
    wait "$pid" 2>/dev/null || true
  done
  server_pids=()
  servers_stopped=true
}

write_evidence() {
  local evidence_status=$1
  local failure=${2:-}
  local completed_utc
  completed_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  local environment_file="$artifacts/environment.txt"
  {
    echo "started_utc=$started_utc"
    echo "source_revision=$source_revision"
    echo "go_version=$(go version)"
    echo "go_cache=$go_cache_dir"
    echo "direct_port=$direct_port"
    echo "owner_a_port=$owner_a_port"
    echo "owner_b_port=$owner_b_port"
    echo "bootstrap_port=$bootstrap_port"
    echo "kernel=$(uname -sr)"
    echo "service_process_isolation=setsid process group per documented go run command"
    echo "direct_server_command=go run ./examples/action -mode server -listen 127.0.0.1:$direct_port -dev-dir $direct_dev_dir -owner-file $direct_owner_file"
    echo "direct_client_command=go run ./examples/action -mode client -server 127.0.0.1:$direct_port -dev-dir $direct_dev_dir"
    echo "owner_a_command=go run ./examples/action -mode owner -listen 127.0.0.1:$owner_a_port -dev-dir $bootstrap_dev_dir -owner-file $owner_a_file"
    echo "owner_b_command=go run ./examples/action -mode owner -listen 127.0.0.1:$owner_b_port -dev-dir $bootstrap_dev_dir -owner-file $owner_b_file"
    echo "bootstrap_command=go run ./examples/action -mode bootstrap -listen 127.0.0.1:$bootstrap_port -dev-dir $bootstrap_dev_dir -owners $owner_a_file,$owner_b_file"
    echo "bootstrap_client_command=go run ./examples/action -mode client -bootstrap 127.0.0.1:$bootstrap_port -group match-1 -dev-dir $bootstrap_dev_dir"
  } > "$environment_file"
  local status_file="$artifacts/status.json"
  jq -n \
    --arg status "$evidence_status" \
    --arg failure "$failure" \
    --arg started_utc "$started_utc" \
    --arg completed_utc "$completed_utc" \
    --arg source_revision "$source_revision" \
    --arg checkout "$checkout" \
    --argjson direct_client_exit "$direct_client_exit" \
    --argjson bootstrap_client_exit "$bootstrap_client_exit" \
    --argjson direct_ok "$direct_ok" \
    --argjson bootstrap_ok "$bootstrap_ok" \
    --argjson servers_stopped "$servers_stopped" \
    '{schema: 1, status: $status, failure: $failure, started_utc: $started_utc, completed_utc: $completed_utc,
      source_revision: $source_revision, fresh_checkout: $checkout,
      direct_client_exit: $direct_client_exit, bootstrap_client_exit: $bootstrap_client_exit,
      direct_ok: $direct_ok, bootstrap_ok: $bootstrap_ok, servers_stopped: $servers_stopped}' > "$status_file"
  local summary_file="$artifacts/summary.md"
  {
    printf '%s\n\n' '# M7 fresh-checkout operator evidence'
    printf '%s\n' "- status: $evidence_status"
    printf '%s\n' "- source revision: $source_revision"
    if [[ -n $failure ]]; then
      printf '%s\n' "- failure: $failure"
    fi
    printf '%s\n' "- direct client exit: $direct_client_exit; observed full exchange: $direct_ok"
    printf '%s\n' "- bootstrap client exit: $bootstrap_client_exit; observed full exchange: $bootstrap_ok"
    printf '%s\n' "- loopback service cleanup completed: $servers_stopped"
    printf '%s\n\n' '- retained logs: `direct-server.log`, `direct-client.log`, `bootstrap-owner-a.log`, `bootstrap-owner-b.log`, `bootstrap.log`, and `bootstrap-client.log`'
    printf '%s\n' 'The clone was created from a clean committed checkout. All service addresses'
    printf '%s\n' 'are loopback-only and all output stays beneath this repository’s artifacts/.'
  } > "$summary_file"
  status_written=true
  echo "action_operator_status=$status_file status=$evidence_status"
}

fail_evidence() {
  local failure=$1
  stop_servers
  write_evidence failed "$failure"
  trap - EXIT
  exit 1
}

on_exit() {
  local code=$?
  if [[ $status_written != true ]]; then
    stop_servers
    write_evidence failed "runner exited unexpectedly with status $code"
  fi
}
trap on_exit EXIT

wait_for_ready() {
  local pid=$1
  local log=$2
  local expected=$3
  local attempt
  for ((attempt = 1; attempt <= 300; attempt++)); do
    if [[ -f $log ]] && rg -q "$expected" "$log"; then
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "service exited before readiness; see $log" >&2
      return 1
    fi
    sleep 0.1
  done
  echo "service did not become ready; see $log" >&2
  return 1
}

start_action() {
  local log=$1
  shift
  (
    cd "$checkout"
    exec setsid env GOCACHE="$go_cache_dir" go run ./examples/action "$@"
  ) > "$log" 2>&1 &
  server_pids+=("$!")
  started_pid=$!
}

run_client() {
  local log=$1
  shift
  set +e
  (
    cd "$checkout"
    env GOCACHE="$go_cache_dir" go run ./examples/action "$@"
  ) > "$log" 2>&1
  local code=$?
  set -e
  printf '%s\n' "$code"
}

direct_server_log="$artifacts/direct-server.log"
direct_client_log="$artifacts/direct-client.log"
direct_dev_dir="artifacts/direct-dev"
direct_owner_file="$direct_dev_dir/owner.json"
start_action "$direct_server_log" -mode server -listen "127.0.0.1:$direct_port" -dev-dir "$direct_dev_dir" -owner-file "$direct_owner_file"
direct_pid=$started_pid
if ! wait_for_ready "$direct_pid" "$direct_server_log" 'action owner listening on'; then
  fail_evidence "direct owner did not become ready"
fi
direct_client_exit=$(run_client "$direct_client_log" -mode client -server "127.0.0.1:$direct_port" -dev-dir "$direct_dev_dir")
if [[ $direct_client_exit == 0 ]] && rg -q 'update=.*inventory=.*stream=echo:snapshot bytes' "$direct_client_log"; then
  direct_ok=true
fi

owner_a_log="$artifacts/bootstrap-owner-a.log"
owner_b_log="$artifacts/bootstrap-owner-b.log"
bootstrap_log="$artifacts/bootstrap.log"
bootstrap_client_log="$artifacts/bootstrap-client.log"
bootstrap_dev_dir="artifacts/bootstrap-dev"
owner_a_file="$bootstrap_dev_dir/owner-a.json"
owner_b_file="$bootstrap_dev_dir/owner-b.json"
start_action "$owner_a_log" -mode owner -listen "127.0.0.1:$owner_a_port" -dev-dir "$bootstrap_dev_dir" -owner-file "$owner_a_file"
owner_a_pid=$started_pid
if ! wait_for_ready "$owner_a_pid" "$owner_a_log" 'action owner listening on'; then
  fail_evidence "bootstrap owner A did not become ready"
fi
start_action "$owner_b_log" -mode owner -listen "127.0.0.1:$owner_b_port" -dev-dir "$bootstrap_dev_dir" -owner-file "$owner_b_file"
owner_b_pid=$started_pid
if ! wait_for_ready "$owner_b_pid" "$owner_b_log" 'action owner listening on'; then
  fail_evidence "bootstrap owner B did not become ready"
fi
start_action "$bootstrap_log" -mode bootstrap -listen "127.0.0.1:$bootstrap_port" -dev-dir "$bootstrap_dev_dir" -owners "$owner_a_file,$owner_b_file"
bootstrap_pid=$started_pid
if ! wait_for_ready "$bootstrap_pid" "$bootstrap_log" 'action bootstrap listening on'; then
  fail_evidence "bootstrap service did not become ready"
fi
bootstrap_client_exit=$(run_client "$bootstrap_client_log" -mode client -bootstrap "127.0.0.1:$bootstrap_port" -group match-1 -dev-dir "$bootstrap_dev_dir")
if [[ $bootstrap_client_exit == 0 ]] && rg -q 'update=.*inventory=.*stream=echo:snapshot bytes' "$bootstrap_client_log"; then
  bootstrap_ok=true
fi

stop_servers
evidence_status=completed
if [[ $direct_ok != true || $bootstrap_ok != true ]]; then
  evidence_status=failed
fi
write_evidence "$evidence_status"
trap - EXIT
if [[ $evidence_status != completed ]]; then
  exit 1
fi

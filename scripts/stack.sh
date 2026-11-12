#!/usr/bin/env bash
# Starts, tests and stops Module 28's lab stack: one Kafka broker in KRaft mode, the topic setup,
# the booking, inventory and payment PostgreSQL databases and the payment provider's stub, under
# the Compose project name seatwave-saga, every port on 127.0.0.1 only.
#
# Usage: scripts/stack.sh <up|down|test|status> [--run] [--yes]
#   default  a dry run: print the plan, change nothing (and need no Docker)
#   --run    do it; asks for confirmation on the terminal first
#   --yes    skip the question
#
#   up      generate the database password into .secrets, build the stub's image if it is missing,
#           start everything, wait until every healthcheck passes and kafka-init has created the
#           four topics
#   down    remove the project's containers, network and volumes (down -v) and the password of the
#           database that is now gone, then check nothing is left
#   test    up, ./gradlew stackTest stackCoverage, then down, on every path
#   status  list the project's containers (read-only)
set -euo pipefail

readonly PROJECT="seatwave-saga"
readonly STUB_IMAGE="seatwave-saga/payment-provider-stub:1.0.0"
readonly WAIT_SECONDS=180
readonly DOCKER_TIMEOUT_SECONDS=10
readonly PASSWORD_BYTES=24

usage() {
  echo "usage: $0 <up|down|test|status> [--run] [--yes]" >&2
  exit 2
}

[[ $# -ge 1 ]] || usage
action="$1"
shift
mode="dry-run"
confirmed="no"
for option in "$@"; do
  case "$option" in
    --run) mode="run" ;;
    --yes) confirmed="yes" ;;
    *) usage ;;
  esac
done
case "$action" in up|down|test|status) ;; *) usage ;; esac

cd "$(dirname "$0")/.."
compose=(docker compose -p "$PROJECT")

echo "plan: $action the Compose project $PROJECT"
case "$action" in
  up) echo "  write .secrets/postgres.password if missing; docker build $STUB_IMAGE; ${compose[*]} up -d --wait; wait for kafka-init to exit 0" ;;
  down) echo "  ${compose[*]} down -v --remove-orphans; remove .secrets/postgres.password; check nothing of $PROJECT is left" ;;
  test) echo "  up, then ./gradlew stackTest stackCoverage, then down -v (always)" ;;
  status) echo "  ${compose[*]} ps --all" ;;
esac
# The dry run comes before the Docker check: it changes nothing, so it needs nothing.
if [[ "$mode" == "dry-run" && "$action" != "status" ]]; then
  echo "dry run: nothing changed. Add --run to do it."
  exit 0
fi

# Docker answers only if "docker info" prints a server version: it exits 0 with empty output when
# the engine is down. macOS has no timeout command, so the check runs in the background and is
# killed if it hangs.
docker_answers() {
  local answer_file="build/stack/docker-version"
  mkdir -p build/stack
  rm -f "$answer_file"
  docker info --format '{{.ServerVersion}}' > "$answer_file" 2> /dev/null &
  local checker=$!
  local waited
  for (( waited = 0; waited < DOCKER_TIMEOUT_SECONDS; waited++ )); do
    if ! kill -0 "$checker" 2> /dev/null; then
      wait "$checker" || return 1
      grep -q "[0-9]" "$answer_file"
      return $?
    fi
    sleep 1
  done
  kill "$checker" 2> /dev/null || true
  return 1
}
if ! command -v docker > /dev/null || ! docker_answers; then
  echo "Docker is not running (or does not answer within ${DOCKER_TIMEOUT_SECONDS} s); nothing was changed." >&2
  exit 1
fi
if [[ "$action" == "status" ]]; then
  "${compose[@]}" ps --all
  exit 0
fi
if [[ "$confirmed" != "yes" ]]; then
  read -r -p "Go ahead with '$action' for $PROJECT? [y/N] " answer < /dev/tty
  [[ "$answer" == "y" ]] || { echo "not confirmed; nothing changed" >&2; exit 1; }
fi

mkdir -p build/stack
audit() {
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $1" >> build/stack/audit.log
}

up() {
  mkdir -p .secrets
  if [[ ! -s .secrets/postgres.password ]]; then
    # Readable by all, not owner-only: on a Linux host a Compose secret is a bind mount that keeps
    # the file's owner and mode, and the container's postgres user reads it. Any account on this
    # machine can read this throwaway lab password; down deletes it.
    ( umask 022; openssl rand -hex "$PASSWORD_BYTES" > .secrets/postgres.password )
  fi
  # --wait returns when every healthcheck passes; kafka-init has no healthcheck, so its exit
  # status is read below.
  # The stub's image is built here, not by Compose, from the one source file .dockerignore lets in.
  docker build --quiet -t "$STUB_IMAGE" -f stub/Dockerfile . > /dev/null
  "${compose[@]}" up -d --wait --wait-timeout "$WAIT_SECONDS" --quiet-pull kafka booking-db inventory-db payment-db payment-provider
  "${compose[@]}" up --quiet-pull --exit-code-from kafka-init kafka-init
  audit "up: started $PROJECT"
}

down() {
  local status=0
  "${compose[@]}" down -v --remove-orphans || status=$?
  rm -f .secrets/postgres.password
  audit "down: removed $PROJECT and its password (compose exit $status)"
  local left=""
  left+="$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT") "
  left+="$(docker network ls -q --filter "label=com.docker.compose.project=$PROJECT") "
  left+="$(docker volume ls -q --filter "label=com.docker.compose.project=$PROJECT")"
  if [[ "$status" -ne 0 || -n "${left// /}" ]]; then
    echo "down failed (compose exit $status) or left behind by $PROJECT: $left" >&2
    return 1
  fi
  echo "nothing of $PROJECT is left (containers, networks, volumes)"
}

case "$action" in
  up) up ;;
  down) down ;;
  test)
    status=0
    up || status=$?
    if [[ "$status" -eq 0 ]]; then
      ./gradlew stackTest stackCoverage || status=$?
    fi
    down || status=$?
    exit "$status"
    ;;
esac

#!/usr/bin/env bash
# The drills of Module 28 against the real stack of compose.yaml. Each run starts a fresh stack with
# scripts/stack.sh, does one experiment, prints its numbers and brings the stack down (down -v), on
# every path. The three services and the relays run as plain java processes on this machine, so a
# drill can kill one with SIGKILL by its process id at a crash point the program announces with a
# "PAUSED pid=<id>" line.
#
# Usage: scripts/drill.sh <experiment> [--runs N] [--run] [--yes]
#   dual-write  the confirm written three ways (commit then publish, publish then commit, outbox)
#               and broken at each point, N runs a point; then the relay killed between the
#               broker's acknowledgement and its mark, N times; every topic audited against the
#               bookings table, with Module 27's claim counting the emails
#   saga        the orchestrated saga through the three services over HTTP: 100 bookings one by one
#               (the happy path's time at the stub's 200 ms), 1,000 with 1 in 100 declined, 10
#               lost answers; the booking service killed in each of the six open states, N times
#               each, and resumed; the semantic lock against a slow payment (2.5 s against a 1 s
#               hold, another fan at 2 s), with and without it;
#               every saga judged by the databases' rows and the provider's ledger
#   stuck       the orchestrator killed after the charge and left down for 310 s: the stuck-saga
#               query of Section 10, then the restart that finishes it
#   relay       relay delay from commit to arrival at 20 and at 1,000 rows a second, the relay's
#               throughput on 100,000 waiting rows, the outbox's size and its clean-up; N runs each
#   rebuild     bookings_by_customer rebuilt from a topic of 11,220 events, N times, then from the
#               same events shuffled, with and without the version rule
#   two-phase   PREPARE TRANSACTION in inventory and payment, the coordinator gone for 120 s while
#               another transaction tries the seat every 30 s (inventory's database restarted in
#               the middle), then the decision; and the booking database's default
#   lab1        Lab 1's outage: 500 holds a second for 60 s, the broker stopped from 20 s to 40 s
#   command8    Section 8's Command 8: 10,000 holds, the relay killed after its first acknowledged
#               batch and before its mark, restarted with the next token; records counted
#   default     a dry run: print the plan, change nothing; --run does it, --yes skips the question
set -euo pipefail

readonly PROJECT="seatwave-saga"
readonly SETTINGS="config/stack.properties"
readonly SAGA_STATES="SEAT_HELD PAYMENT_REQUESTED PAYMENT_SUCCEEDED PAYMENT_FAILED REFUND_REQUESTED PAYMENT_REFUNDED"

usage() {
  echo "usage: $0 <dual-write|saga|stuck|relay|rebuild|two-phase|lab1|command8> [--runs N] [--run] [--yes]" >&2
  exit 2
}

[[ $# -ge 1 ]] || usage
experiment="$1"
shift
case "$experiment" in dual-write|saga|stuck|relay|rebuild|two-phase|lab1|command8) ;; *) usage ;; esac
runs=3
execute="no"
confirmed="no"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --runs)
      [[ $# -ge 2 && "$2" =~ ^[1-9][0-9]?$ ]] || { echo "--runs takes a number from 1 to 99" >&2; exit 2; }
      runs=$((10#$2))
      shift
      ;;
    --run) execute="yes" ;;
    --yes) confirmed="yes" ;;
    *) usage ;;
  esac
  shift
done
cd "$(dirname "$0")/.."

echo "plan ($experiment): $runs run(s) a point, on a fresh stack (scripts/stack.sh up, then down -v)"
if [[ "$execute" != "yes" ]]; then
  echo "dry run: nothing changed. Add --run to do it."
  exit 0
fi
if [[ "$confirmed" != "yes" ]]; then
  read -r -p "Go ahead with '$experiment' for $PROJECT? [y/N] " answer < /dev/tty
  [[ "$answer" == "y" ]] || { echo "not confirmed; nothing changed" >&2; exit 1; }
fi

./gradlew -q drillClasspath
classpath="$(head -n 1 build/drill/runtime.classpath)"
work="build/drill/$experiment"
rm -rf "$work"
mkdir -p "$work"
compose=(docker compose -p "$PROJECT")
started_pids=()

# Every process this drill started is stopped on every path, then the stack goes down.
finish() {
  local status=$?
  local pid
  for pid in "${started_pids[@]}"; do
    kill "$pid" 2> /dev/null || true
  done
  wait 2> /dev/null || true
  scripts/stack.sh down --run --yes > "$work/stack-down.txt" 2>&1 || status=1
  tail -n 1 "$work/stack-down.txt"
  exit "$status"
}
trap finish EXIT

tool() {
  java -Xmx512m -cp "$classpath" -Dseatwave.stackSettings="$PWD/$SETTINGS" "io.plainround.seatwave.saga.stack.$1" "${@:2}"
}

# Starts a program in the background and sets started_pid to its process id; exec makes the
# subshell the java process itself, so the id is the program's own.
start() {
  local name="$1"
  shift
  ( exec java -Xmx256m -cp "$classpath" -Dseatwave.stackSettings="$PWD/$SETTINGS" \
      "io.plainround.seatwave.saga.stack.$1" "${@:2}" > "$work/$name.out" 2> "$work/$name.err" ) &
  started_pid=$!
  started_pids+=("$started_pid")
}

wait_for_line() {
  local file="$1" pattern="$2" seconds="$3" waited
  for (( waited = 0; waited < seconds * 10; waited++ )); do
    grep -q "$pattern" "$file" 2> /dev/null && return 0
    sleep 0.1
  done
  echo "no '$pattern' in $file after $seconds s" >&2
  return 1
}

# Kills the program that printed PAUSED, after checking the id it printed is the one started.
kill_paused() {
  local name="$1" pid="$2" printed
  wait_for_line "$work/$name.out" "^PAUSED" 120
  printed="$(sed -n 's/^PAUSED pid=\([0-9]*\) .*/\1/p' "$work/$name.out" | head -n 1)"
  [[ "$printed" == "$pid" ]] || { echo "paused process $printed is not $name ($pid)" >&2; return 1; }
  kill -9 "$pid"
  wait "$pid" 2> /dev/null || true
  echo "  killed $name (pid $pid, SIGKILL) at: $(sed -n 's/^PAUSED pid=[0-9]* //p' "$work/$name.out" | head -n 1)"
}

sql() {
  "${compose[@]}" exec -T "$1-db" psql -U seatwave -d seatwave -v ON_ERROR_STOP=1 -Atc "$2"
}

stub() {
  curl -sS -X POST "http://127.0.0.1:22820$1" > /dev/null
}

load_average() {
  sysctl -n vm.loadavg | awk '{print $2}'
}

start_services() {
  local hold_seconds="$1" protection="$2"
  start inventory InventoryServiceCli --hold-seconds "$hold_seconds"
  inventory_pid="$started_pid"
  start payment PaymentServiceCli
  payment_pid="$started_pid"
  start booking BookingServiceCli --protection "$protection"
  booking_pid="$started_pid"
  wait_for_line "$work/inventory.out" "^inventory service on port" 60
  wait_for_line "$work/payment.out" "^payment service on port" 60
  wait_for_line "$work/booking.out" "^booking service on port" 60
}

stop_services() {
  local pid
  for pid in "$inventory_pid" "$payment_pid" "$booking_pid"; do
    kill "$pid" 2> /dev/null || true
    wait "$pid" 2> /dev/null || true
  done
}

dual_write_run() {
  local token=1 mode failure run topic
  for mode in commit-then-publish publish-then-commit outbox; do
    for failure in none crash-before-first crash-between second-refused; do
      [[ "$mode" == "outbox" && "$failure" == "second-refused" ]] && continue
      topic="seatwave.drill.$mode.$failure"
      sql booking "TRUNCATE bookings, processed_messages, outbox" > /dev/null
      tool StackCli create-topic --topic "$topic" > /dev/null
      for (( run = 1; run <= runs; run++ )); do
        start writer DualWriteDrillCli --mode "$mode" --failure "$failure" --bookings 3 --topic "$topic"
        if [[ "$failure" == crash-* ]]; then
          kill_paused writer "$started_pid" > /dev/null
        else
          wait "$started_pid"
        fi
      done
      if [[ "$mode" == "outbox" ]]; then
        token=$((token + 1))
        tool RelayCli --database booking --topic "$topic" --fencing-token "$token" --until-drained yes > /dev/null
      fi
      echo "$mode / $failure, $runs runs of 3 bookings (load $(load_average)):"
      echo "  $(tool TopicAuditCli --topic "$topic" --target bookings)"
    done
  done
  topic="seatwave.drill.outbox.relay-crash"
  sql booking "TRUNCATE bookings, processed_messages, outbox" > /dev/null
  tool StackCli create-topic --topic "$topic" > /dev/null
  for (( run = 1; run <= runs; run++ )); do
    start writer DualWriteDrillCli --mode outbox --failure none --bookings 3 --topic "$topic"
    wait "$started_pid"
    token=$((token + 1))
    start relay RelayCli --database booking --topic "$topic" --fencing-token "$token" --pause-on-pass 1
    kill_paused relay "$started_pid" > /dev/null
  done
  token=$((token + 1))
  tool RelayCli --database booking --topic "$topic" --fencing-token "$token" --until-drained yes > /dev/null
  echo "outbox / relay killed after the acknowledgement and before the mark, $runs times (load $(load_average)):"
  echo "  $(tool TopicAuditCli --topic "$topic" --target bookings)"
}

saga_run() {
  local state run protection behavior
  start_services 300 SEMANTIC_LOCK
  echo "happy path, 100 bookings one at a time, provider latency 200 ms (load $(load_average)):"
  echo "  $(tool SagaDrillCli book --run happy --count 100 --first-seat 1)"
  echo "1,000 bookings, 8 at a time, 1 in 100 cards declined (load $(load_average)):"
  echo "  $(tool SagaDrillCli book --run declines --count 1000 --first-seat 101 --concurrency 8 --decline-every 100)"
  echo "10 lost answers: the provider captures, then answers after 10 s; the payment service waits 3 s (load $(load_average)):"
  echo "  $(tool SagaDrillCli book --run lost --count 10 --first-seat 1101 --concurrency 10 --behavior CAPTURE_THEN_TIMEOUT)"
  echo "  $(tool SagaDrillCli verify)"
  stop_services
  # The refund path needs a hold that ends during a slow payment and another fan on the seat.
  local seat=2001
  for state in $SAGA_STATES; do
    case "$state" in
      SEAT_HELD|PAYMENT_REQUESTED|PAYMENT_SUCCEEDED) protection=SEMANTIC_LOCK; behavior=CAPTURE; latency=200; hold=300; fan=0 ;;
      PAYMENT_FAILED) protection=SEMANTIC_LOCK; behavior=DECLINE; latency=200; hold=300; fan=0 ;;
      *) protection=NONE; behavior=CAPTURE; latency=2500; hold=1; fan=2000 ;;
    esac
    stub "/admin/latency?millis=$latency"
    start inventory InventoryServiceCli --hold-seconds "$hold"
    inventory_pid="$started_pid"
    start payment PaymentServiceCli
    payment_pid="$started_pid"
    wait_for_line "$work/inventory.out" "^inventory service on port" 60
    wait_for_line "$work/payment.out" "^payment service on port" 60
    for (( run = 1; run <= runs; run++ )); do
      start booking BookingServiceCli --protection "$protection" --pause-in "$state"
      booking_pid="$started_pid"
      wait_for_line "$work/booking.out" "^booking service on port" 60
      start client SagaDrillCli book --run "kill-${state//_/-}-$run" --first-seat "$seat" --behavior "$behavior" --second-fan-after-millis "$fan"
      kill_paused booking "$booking_pid"
      start booking BookingServiceCli --protection "$protection"
      booking_pid="$started_pid"
      wait_for_line "$work/booking.out" "^booking service on port" 60
      echo "  resumed: $(tool SagaDrillCli book --run "kill-${state//_/-}-$run" --first-seat "$seat" --behavior "$behavior")"
      kill "$booking_pid"
      wait "$booking_pid" 2> /dev/null || true
      seat=$((seat + 1))
    done
    kill "$inventory_pid" "$payment_pid"
    wait "$inventory_pid" "$payment_pid" 2> /dev/null || true
  done
  echo "after every kill (load $(load_average)):"
  echo "  $(tool SagaDrillCli verify)"
  for protection in SEMANTIC_LOCK NONE; do
    stub "/admin/latency?millis=2500"
    start_services 1 "$protection"
    echo "slow payment (2.5 s) against a 1 s hold, another fan at 2 s, $protection, 10 bookings (load $(load_average)):"
    echo "  $(tool SagaDrillCli book --run "slow-${protection//_/-}" --count 10 --first-seat "$seat" --concurrency 10 --second-fan-after-millis 2000)"
    stop_services
    seat=$((seat + 10))
  done
  echo "every saga of the drill, judged by the rows and the provider's ledger:"
  echo "  $(tool SagaDrillCli verify)"
}

stuck_run() {
  start inventory InventoryServiceCli
  inventory_pid="$started_pid"
  start payment PaymentServiceCli
  payment_pid="$started_pid"
  start booking BookingServiceCli --pause-in PAYMENT_REQUESTED
  booking_pid="$started_pid"
  wait_for_line "$work/booking.out" "^booking service on port" 60
  start client SagaDrillCli book --run stuck --first-seat 1
  kill_paused booking "$booking_pid"
  echo "  waiting 310 s with the orchestrator down"
  sleep 310
  echo "  the stuck-saga query of Section 10 at $(date -u +%H:%M:%SZ):"
  sql booking "SELECT booking_id, state, attempts, now() - updated_at AS age FROM booking_sagas WHERE state NOT IN ('COMPLETED', 'COMPENSATED') AND updated_at < now() - interval '300 seconds' ORDER BY updated_at LIMIT 100;" | sed 's/^/    /'
  echo "  seat A-1 is $(sql inventory "SELECT state FROM holds WHERE seat_label = 'A-1'") (the lock held through the outage)"
  start booking BookingServiceCli
  booking_pid="$started_pid"
  wait_for_line "$work/booking.out" "^booking service on port" 60
  echo "  restarted: $(head -n 1 "$work/booking.out")"
  echo "  $(tool SagaDrillCli book --run stuck --first-seat 1)"
  echo "  $(tool SagaDrillCli verify)"
}

relay_run() {
  local run rate token=1
  tool StackCli create-topic --topic seatwave.drill.load > /dev/null
  for (( run = 1; run <= runs; run++ )); do
    for rate in 20 1000; do
      token=$((token + 1))
      start relay RelayCli --database booking --topic seatwave.drill.load --fencing-token "$token"
      wait_for_line "$work/relay.out" "started" 60
      echo "run $run, $rate rows a second for 30 s (load $(load_average)):"
      echo "  $(tool OutboxLoadCli delay --rate "$rate" --seconds 30 --topic seatwave.drill.load)"
      kill "$started_pid"
      wait "$started_pid" 2> /dev/null || true
    done
    echo "run $run, throughput on 100,000 waiting rows, batch 1,000 (load $(load_average)):"
    echo "  $(tool OutboxLoadCli fill --rows 100000)"
    token=$((token + 1))
    echo "  $(tool RelayCli --database booking --topic seatwave.drill.load --fencing-token "$token" --until-drained yes | tail -n 1)"
  done
  echo "the outbox after the runs, and its clean-up (load $(load_average)):"
  tool OutboxLoadCli size --delete-published yes | sed 's/^/  /'
}

rebuild_run() {
  local run rule
  # Both topics first, so neither is published to in the moment after it was created.
  tool StackCli create-topic --topic seatwave.drill.rebuild > /dev/null
  tool StackCli create-topic --topic seatwave.drill.shuffled > /dev/null
  tool ReadModelRebuildCli publish --topic seatwave.drill.rebuild --order relay | sed 's/^/  /'
  for (( run = 1; run <= runs; run++ )); do
    echo "run $run (load $(load_average)):"
    tool ReadModelRebuildCli rebuild --topic seatwave.drill.rebuild --rule HIGHER_VERSION_WINS | sed 's/^/  /'
  done
  tool ReadModelRebuildCli publish --topic seatwave.drill.shuffled --order shuffled | sed 's/^/  /'
  for rule in HIGHER_VERSION_WINS LAST_APPLIED_WINS; do
    echo "shuffled delivery (load $(load_average)):"
    tool ReadModelRebuildCli rebuild --topic seatwave.drill.shuffled --rule "$rule" | sed 's/^/  /'
  done
}

two_phase_run() {
  local second
  tool TwoPhaseCommitDrillCli booking-db | sed 's/^/  /'
  echo "coordinator crash after both votes (load $(load_average)):"
  tool TwoPhaseCommitDrillCli prepare --gid b-1004 --crash after-votes | sed 's/^/  /'
  for second in 0 30 60 90; do
    [[ "$second" -gt 0 ]] && sleep 30
    if [[ "$second" -eq 60 ]]; then
      "${compose[@]}" restart inventory-db > /dev/null 2>&1
      "${compose[@]}" up -d --wait inventory-db > /dev/null 2>&1
      echo "  inventory-db restarted at $(date -u +%H:%M:%SZ)"
    fi
    tool TwoPhaseCommitDrillCli probe --gid b-1004 | sed 's/^/  /'
  done
  sleep 30
  echo "  the coordinator is back at 120 s; it finds no decision for b-1004: presumed abort"
  tool TwoPhaseCommitDrillCli decide --gid b-1004 --decision rollback | sed 's/^/  /'
  tool TwoPhaseCommitDrillCli probe --gid b-1004 | sed 's/^/  /'
  echo "coordinator crash after telling payment COMMIT (load $(load_average)):"
  tool TwoPhaseCommitDrillCli prepare --gid b-1005 --crash after-first-commit | sed 's/^/  /'
  tool TwoPhaseCommitDrillCli probe --gid b-1005 | sed 's/^/  /'
  sleep 30
  echo "  the coordinator is back; its log says COMMIT, so it finishes the commit"
  tool TwoPhaseCommitDrillCli decide --gid b-1005 --decision commit | sed 's/^/  /'
  tool TwoPhaseCommitDrillCli probe --gid b-1005 | sed 's/^/  /'
}

lab1_run() {
  start inventory InventoryServiceCli
  inventory_pid="$started_pid"
  wait_for_line "$work/inventory.out" "^inventory service on port" 60
  start load HoldLoadCli --rate 500 --seconds 60 --drain-seconds 30
  local load_pid="$started_pid"
  sleep 20
  "${compose[@]}" stop kafka > /dev/null 2>&1
  echo "  broker stopped at $(date -u +%H:%M:%SZ) (load $(load_average))"
  sleep 20
  "${compose[@]}" start kafka > /dev/null 2>&1
  echo "  broker started at $(date -u +%H:%M:%SZ)"
  wait "$load_pid"
  sed 's/^/  /' "$work/load.out"
  echo "  $(tool TopicAuditCli --topic seatwave.inventory --target holds)"
}

command8_run() {
  echo "  $(tool HoldLoadCli --rate 2000 --seconds 5 --drain-seconds 0 | tail -n 1)"
  echo "  unpublished before the relay: $(sql inventory "SELECT count(*) FROM outbox WHERE published_at IS NULL")"
  start relay RelayCli --database inventory --topic seatwave.inventory --fencing-token 33 --pause-on-pass 1
  kill_paused relay "$started_pid"
  echo "  $(tool RelayCli --database inventory --topic seatwave.inventory --fencing-token 34 --until-drained yes | tail -n 1)"
  echo "  \$ kafka-console-consumer.sh --bootstrap-server localhost:22800 --topic seatwave.inventory --from-beginning --timeout-ms 10000 | wc -l"
  echo "  $("${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic seatwave.inventory --from-beginning --timeout-ms 10000 2> /dev/null | wc -l | tr -d ' ')"
  echo "  $(tool TopicAuditCli --topic seatwave.inventory --target holds)"
  echo "  \$ SELECT count(*) FROM outbox WHERE published_at IS NULL"
  echo "  $(sql inventory "SELECT count(*) FROM outbox WHERE published_at IS NULL")"
}

scripts/stack.sh up --run --yes > "$work/stack-up.txt" 2>&1 || { cat "$work/stack-up.txt" >&2; exit 1; }
echo "stack up at $(date -u +%Y-%m-%dT%H:%M:%SZ); load average $(load_average)"
case "$experiment" in
  dual-write) dual_write_run ;;
  saga) saga_run ;;
  stuck) stuck_run ;;
  relay) relay_run ;;
  rebuild) rebuild_run ;;
  two-phase) two_phase_run ;;
  lab1) lab1_run ;;
  command8) command8_run ;;
esac

# seatwave-saga

The project of Module 28 of the course *System Design Fundamentals* ("Events and transactions": the outbox, sagas, two-phase commit, read models). Running case: SeatWave, a ticketing system.

## What it shows

- **The dual write against the outbox**, on the real stack: the booking service's confirm written as commit-then-publish, publish-then-commit, and as one commit of the booking with its outbox row, killed by process id at each crash point; every topic audited against the `bookings` table, with Module 27's processed-message claim counting the emails.
- **The orchestrated booking saga** across three services, each with its own PostgreSQL and outbox, over HTTP and Kafka, in front of a payment provider's stub: the happy path, a declined card compensated, a lost payment answer resolved with exactly one capture, the orchestrator killed in every open state and resumed, the semantic lock (CONFIRMING) keeping a seat during a slow payment; every end judged by the databases' rows and the provider's own ledger.
- **The relay**: its delay from commit to arrival, its throughput, the outbox's size and clean-up, a relay killed between the broker's acknowledgement and its mark, and the broker stopped for 20 seconds.
- **The read model** `bookings_by_customer` rebuilt from a topic, with and without the version rule, in order and shuffled.
- **Two-phase commit** with PostgreSQL's `PREPARE TRANSACTION`: a coordinator that is gone holds the seat's lock for as long as it is gone, and survives a database restart.
- The JDK-only teaching programs of the module (in-process broker and databases, virtual time), unchanged from the seed: `dualWriteRun`, `sagaRun`, `choreographyRun`, `twoPhaseCommitRun`, `readModelRun`.

## Prerequisites

- JDK 25 (the build uses a toolchain; Eclipse Temurin 25.0.2 on the verification machine, the newest is 25.0.5).
- Docker for the stack (Docker 24.0.6 and Compose v2.23.0 on the verification machine). `./gradlew check` needs no Docker.
- `bash`, `curl` and `openssl` for the scripts.

## Dependencies

Every version is in `gradle/libs.versions.toml`, checked on Maven Central on 12 November 2026, and every resolved jar is pinned in `gradle.lockfile`.

| Coordinates | License | Why the JDK alone is not enough |
|---|---|---|
| `org.apache.kafka:kafka-clients:4.4.0` | Apache-2.0 | Kafka's protocol (producer, consumer, admin) |
| `at.yawk.lz4:lz4-java:1.14.0`, `org.xerial.snappy:snappy-java:1.1.10.12` (constraints) | Apache-2.0 | kafka-clients 4.4.0 asks for 1.11.2 and 1.1.10.7, which OSV lists with advisories |
| `org.postgresql:postgresql:42.7.14` | BSD-2-Clause | PostgreSQL's wire protocol |
| `com.zaxxer:HikariCP:7.1.0` | Apache-2.0 | a bounded connection pool per service |
| `com.fasterxml.jackson.core:jackson-databind` (BOM 2.23.0) | Apache-2.0 | JSON for outbox payloads and HTTP bodies |
| `org.slf4j:slf4j-api:2.0.20`, `ch.qos.logback:logback-classic:1.6.6` | MIT, EPL-1.0/LGPL-2.1 | service logs |
| JUnit BOM 6.1.3, AssertJ 3.27.7, Awaitility 4.3.0 | EPL-2.0, Apache-2.0, Apache-2.0 | tests |
| Spotless 8.11.0 with google-java-format 2.0.0, JaCoCo 0.8.16 | Apache-2.0, Apache-2.0, EPL-2.0 | format and coverage gates |

Images (Docker Hub, checked 12 November 2026, each with an arm64 variant): `apache/kafka:4.4.0@sha256:833b867a9585a141da455dc2b57431c65cbccb27f11c91f694444cca9f777e0f`, `postgres:18.6@sha256:e3d24f6906d0760bf4da0dae872415391ededefad23fd471c47dc53b0ddf3c42`, and for the stub `eclipse-temurin:25.0.5_8-jdk-noble@sha256:80f67fb3d6f76e2d817d98a4cc2a9cf05f054d4e6adc5e2144bde671d722e63f` and `eclipse-temurin:25.0.5_8-jre-noble@sha256:e3b6f7887a50f20d8f07abf279d8a81a5953fb398a9555bd9edd56b9bd15968c`.

OSV, every coordinate of both lockfiles, 12 November 2026:

```text
org.xerial.snappy:snappy-java:1.1.10.12  0 advisories
checked 36 coordinates against OSV; 0 with advisories
```

## Commands and what they printed

Build and test, without Docker (Temurin 25.0.2):

```text
PaymentProviderStubTest > aLostAnswerHasMovedTheMoneyAndTheRetryFindsIt() PASSED
PaymentProviderStubTest > refusesARefundWithoutACaptureAndRequestsWithoutAKey() PASSED
PaymentProviderStubTest > aRepeatedChargeReturnsTheFirstAnswerAndCapturesOnce() PASSED
PaymentProviderStubTest > aRepeatedRefundRefundsOnceAndAFailingRefundAnswers503() PASSED
PaymentProviderStubTest > aDeclinedCardIsDeclinedAgainAndMovesNoMoney() PASSED
> Task :jacocoTestCoverageVerification
> Task :spotlessJava
> Task :spotlessJavaCheck
> Task :spotlessCheck
> Task :check
> Task :jacocoTestReport
BUILD SUCCESSFUL in 15s
10 actionable tasks: 10 executed
Consider enabling configuration cache to speed up this build: https://docs.gradle.org/9.9.1/userguide/configuration_cache_enabling.html
tests run by ./gradlew check: 31 in 8 classes, 0 failed
line coverage checked by ./gradlew check: 712 of 771 lines, 0.923 (floor 0.80; adapters and programs excluded, measured by the stack run)
```

```text
$ ./gradlew spotlessCheck --rerun-tasks
> Task :spotlessJava
> Task :spotlessJavaCheck
> Task :spotlessCheck
BUILD SUCCESSFUL in 683ms
```

The five programs, each run twice with identical output (their output is in the module and in `src/test/resources/golden`):

```text
dualWriteRun: second run identical
sagaRun: second run identical
choreographyRun: second run identical
twoPhaseCommitRun: second run identical
readModelRun: second run identical
```

The stack (`scripts/stack.sh up --run --yes`), then the tables of each database:

```text
$ scripts/stack.sh up --run --yes
plan: up the Compose project seatwave-saga
  write .secrets/postgres.password if missing; docker build seatwave-saga/payment-provider-stub:1.0.0; docker compose -p seatwave-saga up -d --wait; wait for kafka-init to exit 0
 Network seatwave-saga_default  Creating
 Network seatwave-saga_default  Created
 Volume "seatwave-saga_booking-data"  Creating
 Volume "seatwave-saga_booking-data"  Created
 Volume "seatwave-saga_inventory-data"  Creating
 Volume "seatwave-saga_inventory-data"  Created
 Volume "seatwave-saga_payment-data"  Creating
 Volume "seatwave-saga_payment-data"  Created
 Volume "seatwave-saga_kafka-data"  Creating
 Volume "seatwave-saga_kafka-data"  Created
 Container seatwave-saga-payment-db-1  Creating
 Container seatwave-saga-kafka-1  Creating
 Container seatwave-saga-inventory-db-1  Creating
 Container seatwave-saga-booking-db-1  Creating
 Container seatwave-saga-payment-provider-1  Creating
 Container seatwave-saga-payment-provider-1  Created
 Container seatwave-saga-booking-db-1  Created
 Container seatwave-saga-payment-db-1  Created
 Container seatwave-saga-inventory-db-1  Created
 Container seatwave-saga-kafka-1  Created
 Container seatwave-saga-booking-db-1  Starting
 Container seatwave-saga-payment-db-1  Starting
 Container seatwave-saga-inventory-db-1  Starting
 Container seatwave-saga-payment-provider-1  Starting
 Container seatwave-saga-kafka-1  Starting
 Container seatwave-saga-payment-provider-1  Started
 Container seatwave-saga-payment-db-1  Started
 Container seatwave-saga-inventory-db-1  Started
 Container seatwave-saga-booking-db-1  Started
 Container seatwave-saga-kafka-1  Started
 Container seatwave-saga-payment-provider-1  Waiting
 Container seatwave-saga-kafka-1  Waiting
 Container seatwave-saga-booking-db-1  Waiting
 Container seatwave-saga-inventory-db-1  Waiting
 Container seatwave-saga-payment-db-1  Waiting
 Container seatwave-saga-payment-db-1  Healthy
 Container seatwave-saga-booking-db-1  Healthy
 Container seatwave-saga-kafka-1  Healthy
 Container seatwave-saga-inventory-db-1  Healthy
 Container seatwave-saga-payment-provider-1  Healthy
 Container seatwave-saga-kafka-1  Running
 Container seatwave-saga-kafka-init-1  Creating
 Container seatwave-saga-kafka-init-1  Created
Attaching to seatwave-saga-kafka-init-1
seatwave-saga-kafka-init-1  | Created topic seatwave.bookings.
seatwave-saga-kafka-init-1  | Created topic seatwave.inventory.
seatwave-saga-kafka-init-1  | Created topic seatwave.payments.
seatwave-saga-kafka-init-1  | Created topic seatwave.saga.commands.
seatwave-saga-kafka-init-1 exited with code 0
Aborting on container exit...
 Container seatwave-saga-kafka-init-1  Stopping
 Container seatwave-saga-kafka-init-1  Stopped
$ docker compose -p seatwave-saga exec -T booking-db psql -U seatwave -d seatwave -Atc "SELECT string_agg(tablename, ', ' ORDER BY tablename) FROM pg_tables WHERE schemaname = 'public'"
booking_sagas, bookings, bookings_by_customer, outbox, outbox_relay_fence, processed_messages
$ docker compose -p seatwave-saga exec -T inventory-db psql -U seatwave -d seatwave -Atc "SELECT string_agg(tablename, ', ' ORDER BY tablename) FROM pg_tables WHERE schemaname = 'public'"
customers, events, holds, outbox, outbox_relay_fence, seats, sections, venues
$ docker compose -p seatwave-saga exec -T payment-db psql -U seatwave -d seatwave -Atc "SELECT string_agg(tablename, ', ' ORDER BY tablename) FROM pg_tables WHERE schemaname = 'public'"
outbox, outbox_relay_fence, payments
$ docker stats --no-stream (this project)
seatwave-saga-booking-db-1 34.97MiB / 384MiB
seatwave-saga-inventory-db-1 36.02MiB / 384MiB
seatwave-saga-kafka-1 336.3MiB / 640MiB
seatwave-saga-payment-db-1 35.29MiB / 384MiB
seatwave-saga-payment-provider-1 31.42MiB / 160MiB
$ scripts/stack.sh down --run --yes
nothing of seatwave-saga is left (containers, networks, volumes)
```

The stack tests (`scripts/stack.sh test --run --yes`: up, `./gradlew stackTest stackCoverage`, down):

```text
$ scripts/stack.sh test --run --yes
JdbcBookingsByCustomerStackTest > anOlderEventAfterANewerOneLeavesTheNewerRow() PASSED
JdbcBookingsByCustomerStackTest > aDuplicateChangesNothingAndUnknownTypesAreSkipped() PASSED
JdbcInventoryStackTest > anUnlockedHoldPastItsEndIsExpiredAndTheSeatFreed() PASSED
JdbcInventoryStackTest > theDatabaseRefusesAHoldChangeCommittedWithoutItsEvent() PASSED
JdbcInventoryStackTest > aRepeatedLockAndARepeatedReleaseAnswerAlreadyDone() PASSED
JdbcInventoryStackTest > aLockedSeatIsNeitherExpiredNorHeldByAnotherFanAfterItsHoldEnds() PASSED
JdbcSagaStoreStackTest > everyTransitionWritesItsOutboxRowAndACompletedSagaItsBooking() PASSED
JdbcSagaStoreStackTest > aFinalSagaCannotMoveAgain() PASSED
JdbcSagaStoreStackTest > listsOnlyOpenSagasOlderThanTheThreshold() PASSED
PostgresKafkaRelayStackTest > publishesOneAggregatesEventsInTheOrderTheyWereCommitted() PASSED
PostgresKafkaRelayStackTest > marksNothingWhenTheBrokerDoesNotAcknowledge() PASSED
PostgresKafkaRelayStackTest > aStaleRelayIsFencedAndTheRepublishCarriesTheSameMessageId() PASSED
SagaOverHttpStackTest > anOrchestratorThatCrashedAfterTheChargeResumesWithoutASecondCapture() PASSED
SagaOverHttpStackTest > aDeclinedCardIsCompensatedAndTheSeatReleased() PASSED
SagaOverHttpStackTest > aLostPaymentAnswerIsResolvedWithExactlyOneCapture() PASSED
SagaOverHttpStackTest > aBookingCompletesWithOneCaptureAndAConfirmedSeat() PASSED
CompensationTest > aReleaseOfAnExpiredHoldDoesNotFreeTheSeatAnotherFanHoldsNow() PASSED
CompensationTest > aRepeatedReleaseAnswersAlreadyDoneAndWritesNoSecondEvent() PASSED
CompensationTest > aSoldSeatIsNeverReleasedByACompensation() PASSED
OutboxRelayTest > publishesOneAggregatesEventsInTheOrderTheyWereCommitted() PASSED
OutboxRelayTest > aStaleRelayIsFencedAndMarksNothing() PASSED
OutboxRelayTest > marksNothingAndKeepsEveryRowWhenTheBrokerIsDown() PASSED
OutboxRelayTest > aRelayThatCrashesBeforeMarkingRepublishesTheSameMessageIds() PASSED
ProgramOutputTest > printsTheOutputTheModuleShows(String) > [1] "DualWriteCli" PASSED
ProgramOutputTest > printsTheOutputTheModuleShows(String) > [2] "SagaCli" PASSED
ProgramOutputTest > printsTheOutputTheModuleShows(String) > [3] "ChoreographyCli" PASSED
ProgramOutputTest > printsTheOutputTheModuleShows(String) > [4] "TwoPhaseCommitCli" PASSED
ProgramOutputTest > printsTheOutputTheModuleShows(String) > [5] "ReadModelCli" PASSED
ReadModelTest > aMalformedLineDecodesToNothing() PASSED
ReadModelTest > anOlderEventArrivingAfterANewerOneChangesNothing() PASSED
ReadModelTest > addingAnOptionalFieldIsCompatibleBothWaysAndRenamingIsNot() PASSED
ReadModelTest > theVersionRuleRebuildsTheSameRowsInAnyOrderAndLastAppliedDoesNot() PASSED
SagaInvariantTest > aSeatBeingPaidForIsNotFreedByExpiryNorHeldByAnotherFan() PASSED
SagaInvariantTest > aRepeatedRefundRefundsOnce() PASSED
SagaInvariantTest > refusesAStateChangeCommittedWithoutItsEvent() PASSED
SagaInvariantTest > everySagaEndsConfirmedAndPaidOrReleasedAndNotChargedWhateverCrashes() PASSED
HttpCommandsTest > theProviderClientReadsCapturesDeclinesRefundsAndLostAnswers() PASSED
HttpCommandsTest > aParticipantThatDoesNotAnswerIsUnavailable() PASSED
HttpCommandsTest > mapsEachAnswerToItsReplyAndASlowAnswerToUnavailable() PASSED
StackSettingsTest > reportsEveryProblemAtOnce(Path) PASSED
StackSettingsTest > theProjectsSettingsFileHasEveryKeyInRange(Path) PASSED
PaymentProviderStubTest > refusesSettingsOutOfRange() PASSED
PaymentProviderStubTest > aLostAnswerHasMovedTheMoneyAndTheRetryFindsIt() PASSED
PaymentProviderStubTest > refusesARefundWithoutACaptureAndRequestsWithoutAKey() PASSED
PaymentProviderStubTest > aRepeatedChargeReturnsTheFirstAnswerAndCapturesOnce() PASSED
PaymentProviderStubTest > aRepeatedRefundRefundsOnceAndAFailingRefundAnswers503() PASSED
PaymentProviderStubTest > aDeclinedCardIsDeclinedAgainAndMovesNoMoney() PASSED
BUILD SUCCESSFUL in 24s
nothing of seatwave-saga is left (containers, networks, volumes)
16 stack tests, 0 failed; stack coverage 0.848 of the main code but the programs, over both test runs (1266 of 1493 lines, floor 0.80)
```

The drills (`scripts/drill.sh <experiment> --runs 3 --run --yes`), each on a fresh stack, timings on Docker Desktop's VM with the services on the host:

```text
$ scripts/drill.sh dual-write --runs 5 --run --yes
plan (dual-write): 5 run(s) a point, on a fresh stack (scripts/stack.sh up, then down -v)
stack up at 2026-11-12T20:33:22Z; load average 9.04
commit-then-publish / none, 5 runs of 3 bookings (load 10.39):
  seatwave.drill.commit-then-publish.none: 15 records, 15 distinct booking_id; bookings committed 15; committed without an event 0; events for no row 0; emails 15, duplicates skipped by the claim 0
commit-then-publish / crash-before-first, 5 runs of 3 bookings (load 10.91):
  seatwave.drill.commit-then-publish.crash-before-first: 10 records, 10 distinct booking_id; bookings committed 10; committed without an event 0; events for no row 0; emails 10, duplicates skipped by the claim 0
commit-then-publish / crash-between, 5 runs of 3 bookings (load 10.52):
  seatwave.drill.commit-then-publish.crash-between: 10 records, 10 distinct booking_id; bookings committed 15; committed without an event 5; events for no row 0; emails 10, duplicates skipped by the claim 0
commit-then-publish / second-refused, 5 runs of 3 bookings (load 10.77):
  seatwave.drill.commit-then-publish.second-refused: 10 records, 10 distinct booking_id; bookings committed 15; committed without an event 5; events for no row 0; emails 10, duplicates skipped by the claim 0
publish-then-commit / none, 5 runs of 3 bookings (load 11.07):
  seatwave.drill.publish-then-commit.none: 15 records, 15 distinct booking_id; bookings committed 15; committed without an event 0; events for no row 0; emails 15, duplicates skipped by the claim 0
publish-then-commit / crash-before-first, 5 runs of 3 bookings (load 11.14):
  seatwave.drill.publish-then-commit.crash-before-first: 10 records, 10 distinct booking_id; bookings committed 10; committed without an event 0; events for no row 0; emails 10, duplicates skipped by the claim 0
publish-then-commit / crash-between, 5 runs of 3 bookings (load 10.75):
  seatwave.drill.publish-then-commit.crash-between: 15 records, 15 distinct booking_id; bookings committed 10; committed without an event 0; events for no row 5; emails 15, duplicates skipped by the claim 0
publish-then-commit / second-refused, 5 runs of 3 bookings (load 10.45):
  seatwave.drill.publish-then-commit.second-refused: 15 records, 15 distinct booking_id; bookings committed 10; committed without an event 0; events for no row 5; emails 15, duplicates skipped by the claim 0
outbox / none, 5 runs of 3 bookings (load 10.61):
  seatwave.drill.outbox.none: 15 records, 15 distinct booking_id; bookings committed 15; committed without an event 0; events for no row 0; emails 15, duplicates skipped by the claim 0
outbox / crash-before-first, 5 runs of 3 bookings (load 10.97):
  seatwave.drill.outbox.crash-before-first: 10 records, 10 distinct booking_id; bookings committed 10; committed without an event 0; events for no row 0; emails 10, duplicates skipped by the claim 0
outbox / crash-between, 5 runs of 3 bookings (load 10.81):
  seatwave.drill.outbox.crash-between: 15 records, 15 distinct booking_id; bookings committed 15; committed without an event 0; events for no row 0; emails 15, duplicates skipped by the claim 0
outbox / relay killed after the acknowledgement and before the mark, 5 times (load 19.97):
  seatwave.drill.outbox.relay-crash: 60 records, 15 distinct booking_id; bookings committed 15; committed without an event 0; events for no row 0; emails 15, duplicates skipped by the claim 45
nothing of seatwave-saga is left (containers, networks, volumes)
```

```text
$ scripts/drill.sh saga --runs 3 --run --yes
plan (saga): 3 run(s) a point, on a fresh stack (scripts/stack.sh up, then down -v)
stack up at 2026-11-12T20:44:09Z; load average 13.99
happy path, 100 bookings one at a time, provider latency 200 ms (load 14.63):
  run happy: 100 booking(s), ends {COMPLETED=100}; booking time p50 252 ms, p99 847 ms, max 1044 ms
1,000 bookings, 8 at a time, 1 in 100 cards declined (load 16.30):
  run declines: 1000 booking(s), ends {COMPENSATED=10, COMPLETED=990}; booking time p50 273 ms, p99 586 ms, max 714 ms
10 lost answers: the provider captures, then answers after 10 s; the payment service waits 3 s (load 18.48):
  run lost: 10 booking(s), ends {COMPLETED=10}; booking time p50 3779 ms, p99 3799 ms, max 3799 ms
  sagas 1110: {COMPENSATED=10, COMPLETED=1100}; provider ledger 1100 capture(s), 0 refund(s) for them; bookings rows 1100; wrong ends 0
  killed booking (pid 50838, SIGKILL) at: booking 1c025ec0-a23d-3ae2-9f6f-f4adf55a1560 in SEAT_HELD, reply received, not recorded
  resumed: run kill-SEAT-HELD-1: 1 booking(s), ends {COMPLETED=1}; booking time p50 40 ms, p99 40 ms, max 40 ms
  killed booking (pid 50939, SIGKILL) at: booking b138c31f-40d8-3f47-9c83-e390d8b3626b in SEAT_HELD, reply received, not recorded
  resumed: run kill-SEAT-HELD-2: 1 booking(s), ends {COMPLETED=1}; booking time p50 55 ms, p99 55 ms, max 55 ms
  killed booking (pid 51046, SIGKILL) at: booking 70f77d6a-d8ff-345b-9a16-19423baec5cf in SEAT_HELD, reply received, not recorded
  resumed: run kill-SEAT-HELD-3: 1 booking(s), ends {COMPLETED=1}; booking time p50 27 ms, p99 27 ms, max 27 ms
  killed booking (pid 51197, SIGKILL) at: booking 2d348ec4-26aa-321b-8a81-12deb7ef3a48 in PAYMENT_REQUESTED, reply received, not recorded
  resumed: run kill-PAYMENT-REQUESTED-1: 1 booking(s), ends {COMPLETED=1}; booking time p50 67 ms, p99 67 ms, max 67 ms
  killed booking (pid 51582, SIGKILL) at: booking 3b1b08d0-88b1-3382-aac0-41c8b0614c85 in PAYMENT_REQUESTED, reply received, not recorded
  resumed: run kill-PAYMENT-REQUESTED-2: 1 booking(s), ends {COMPLETED=1}; booking time p50 40 ms, p99 40 ms, max 40 ms
  killed booking (pid 51986, SIGKILL) at: booking d86bde85-28b0-326b-b063-87fd54cc8e50 in PAYMENT_REQUESTED, reply received, not recorded
  resumed: run kill-PAYMENT-REQUESTED-3: 1 booking(s), ends {COMPLETED=1}; booking time p50 35 ms, p99 35 ms, max 35 ms
  killed booking (pid 52582, SIGKILL) at: booking 0653c03b-fe1b-3ad7-9212-d0c18b164531 in PAYMENT_SUCCEEDED, reply received, not recorded
  resumed: run kill-PAYMENT-SUCCEEDED-1: 1 booking(s), ends {COMPLETED=1}; booking time p50 25 ms, p99 25 ms, max 25 ms
  killed booking (pid 52721, SIGKILL) at: booking 82bf8517-ac7d-3703-8a18-1afb5bd9ff8b in PAYMENT_SUCCEEDED, reply received, not recorded
  resumed: run kill-PAYMENT-SUCCEEDED-2: 1 booking(s), ends {COMPLETED=1}; booking time p50 25 ms, p99 25 ms, max 25 ms
  killed booking (pid 52805, SIGKILL) at: booking 62f7f574-aa4a-3b69-8148-411a893fa7d5 in PAYMENT_SUCCEEDED, reply received, not recorded
  resumed: run kill-PAYMENT-SUCCEEDED-3: 1 booking(s), ends {COMPLETED=1}; booking time p50 25 ms, p99 25 ms, max 25 ms
  killed booking (pid 52931, SIGKILL) at: booking 95052cb6-c86f-34ed-a9a4-5c6ed508134a in PAYMENT_FAILED, reply received, not recorded
  resumed: run kill-PAYMENT-FAILED-1: 1 booking(s), ends {COMPENSATED=1}; booking time p50 40 ms, p99 40 ms, max 40 ms
  killed booking (pid 53258, SIGKILL) at: booking e3533739-14fa-35ae-8b19-06a8d2a38b39 in PAYMENT_FAILED, reply received, not recorded
  resumed: run kill-PAYMENT-FAILED-2: 1 booking(s), ends {COMPENSATED=1}; booking time p50 28 ms, p99 28 ms, max 28 ms
  killed booking (pid 53398, SIGKILL) at: booking 964dd34b-3251-32e5-b79f-ac8a9c996b7e in PAYMENT_FAILED, reply received, not recorded
  resumed: run kill-PAYMENT-FAILED-3: 1 booking(s), ends {COMPENSATED=1}; booking time p50 31 ms, p99 31 ms, max 31 ms
  killed booking (pid 53495, SIGKILL) at: booking 0504843f-7195-33cb-8af9-e8d838c0c0a1 in REFUND_REQUESTED, reply received, not recorded
  resumed: run kill-REFUND-REQUESTED-1: 1 booking(s), ends {COMPENSATED=1}; booking time p50 69 ms, p99 69 ms, max 69 ms
  killed booking (pid 53839, SIGKILL) at: booking 78a92b48-9978-3c04-9c54-f0ae204bb759 in REFUND_REQUESTED, reply received, not recorded
  resumed: run kill-REFUND-REQUESTED-2: 1 booking(s), ends {COMPENSATED=1}; booking time p50 27 ms, p99 27 ms, max 27 ms
  killed booking (pid 54521, SIGKILL) at: booking 0110856d-05f9-3ef2-ba41-551f063ccfe4 in REFUND_REQUESTED, reply received, not recorded
  resumed: run kill-REFUND-REQUESTED-3: 1 booking(s), ends {COMPENSATED=1}; booking time p50 30 ms, p99 30 ms, max 30 ms
  killed booking (pid 55075, SIGKILL) at: booking 05c44f5b-3372-39bd-a2bc-b76247821929 in PAYMENT_REFUNDED, reply received, not recorded
  resumed: run kill-PAYMENT-REFUNDED-1: 1 booking(s), ends {COMPENSATED=1}; booking time p50 45 ms, p99 45 ms, max 45 ms
  killed booking (pid 55468, SIGKILL) at: booking c02cd681-6599-3fcf-a19a-8d1f26d57ef7 in PAYMENT_REFUNDED, reply received, not recorded
  resumed: run kill-PAYMENT-REFUNDED-2: 1 booking(s), ends {COMPENSATED=1}; booking time p50 26 ms, p99 26 ms, max 26 ms
  killed booking (pid 55629, SIGKILL) at: booking d7545e74-8930-3931-ac67-cbc28bcdb0a0 in PAYMENT_REFUNDED, reply received, not recorded
  resumed: run kill-PAYMENT-REFUNDED-3: 1 booking(s), ends {COMPENSATED=1}; booking time p50 39 ms, p99 39 ms, max 39 ms
after every kill (load 17.27):
  sagas 1128: {COMPENSATED=19, COMPLETED=1109}; provider ledger 1115 capture(s), 6 refund(s) for them; bookings rows 1109; wrong ends 0
slow payment (2.5 s) against a 1 s hold, another fan at 2 s, SEMANTIC_LOCK, 10 bookings (load 17.27):
  run slow-SEMANTIC-LOCK: 10 booking(s), ends {COMPLETED=10}; booking time p50 2929 ms, p99 2937 ms, max 2937 ms; second fan's holds: {REFUSED=10}
slow payment (2.5 s) against a 1 s hold, another fan at 2 s, NONE, 10 bookings (load 18.89):
  run slow-NONE: 10 booking(s), ends {COMPENSATED=10}; booking time p50 2886 ms, p99 2891 ms, max 2891 ms; second fan's holds: {SUCCEEDED=10}
every saga of the drill, judged by the rows and the provider's ledger:
  sagas 1148: {COMPENSATED=29, COMPLETED=1119}; provider ledger 1135 capture(s), 16 refund(s) for them; bookings rows 1119; wrong ends 0
nothing of seatwave-saga is left (containers, networks, volumes)
```

```text
$ scripts/drill.sh stuck --runs 1 --run --yes
plan (stuck): 1 run(s) a point, on a fresh stack (scripts/stack.sh up, then down -v)
stack up at 2026-11-12T21:12:36Z; load average 10.85
  killed booking (pid 50852, SIGKILL) at: booking 9d71e42a-79da-3115-a3ca-8c8e63c3b652 in PAYMENT_REQUESTED, reply received, not recorded
  waiting 310 s with the orchestrator down
  the stuck-saga query of Section 10 at 21:17:48Z:
    9d71e42a-79da-3115-a3ca-8c8e63c3b652|PAYMENT_REQUESTED|0|00:05:10.933854
  seat A-1 is CONFIRMING (the lock held through the outage)
  restarted: booking service resuming 1 open saga(s)
  run stuck: 1 booking(s), ends {COMPLETED=1}; booking time p50 29 ms, p99 29 ms, max 29 ms
  sagas 1: {COMPLETED=1}; provider ledger 1 capture(s), 0 refund(s) for them; bookings rows 1; wrong ends 0
nothing of seatwave-saga is left (containers, networks, volumes)
```

```text
$ scripts/drill.sh relay --runs 3 --run --yes
plan (relay): 3 run(s) a point, on a fresh stack (scripts/stack.sh up, then down -v)
stack up at 2026-11-12T21:18:23Z; load average 9.18
run 1, 20 rows a second for 30 s (load 9.18):
  offered 600 rows at 20 a second for 30 s; committed 600 (0 failed); arrived 600; commit p50 2.80 ms; relay delay commit to arrival p50 70.9 ms, p99 150.1 ms, max 308.4 ms
run 1, 1000 rows a second for 30 s (load 8.98):
  offered 30,000 rows at 1,000 a second for 30 s; committed 30,000 (0 failed); arrived 30,000; commit p50 1.02 ms; relay delay commit to arrival p50 71.7 ms, p99 184.4 ms, max 341.6 ms
run 1, throughput on 100,000 waiting rows, batch 1,000 (load 9.42):
  filled 100,000 unpublished outbox rows in 0.86 s
  relay token 4: 101 passes, 100,000 rows published, 100,000 marked, in 2.854 s (35,039 rows a second)
run 2, 20 rows a second for 30 s (load 9.47):
  offered 600 rows at 20 a second for 30 s; committed 600 (0 failed); arrived 600; commit p50 2.83 ms; relay delay commit to arrival p50 70.9 ms, p99 129.3 ms, max 210.7 ms
run 2, 1000 rows a second for 30 s (load 10.42):
  offered 30,000 rows at 1,000 a second for 30 s; committed 30,000 (0 failed); arrived 30,000; commit p50 0.83 ms; relay delay commit to arrival p50 68.3 ms, p99 132.0 ms, max 220.0 ms
run 2, throughput on 100,000 waiting rows, batch 1,000 (load 10.00):
  filled 100,000 unpublished outbox rows in 0.82 s
  relay token 7: 101 passes, 100,000 rows published, 100,000 marked, in 2.608 s (38,342 rows a second)
run 3, 20 rows a second for 30 s (load 9.44):
  offered 600 rows at 20 a second for 30 s; committed 600 (0 failed); arrived 600; commit p50 3.16 ms; relay delay commit to arrival p50 68.7 ms, p99 129.2 ms, max 162.7 ms
run 3, 1000 rows a second for 30 s (load 8.50):
  offered 30,000 rows at 1,000 a second for 30 s; committed 30,000 (0 failed); arrived 30,000; commit p50 0.92 ms; relay delay commit to arrival p50 70.3 ms, p99 151.7 ms, max 217.2 ms
run 3, throughput on 100,000 waiting rows, batch 1,000 (load 8.95):
  filled 100,000 unpublished outbox rows in 0.82 s
  relay token 10: 101 passes, 100,000 rows published, 100,000 marked, in 2.878 s (34,741 rows a second)
the outbox after the runs, and its clean-up (load 8.72):
  before: 391,800 rows (0 unpublished); table and indexes 125,091,840 bytes (319 a row); heap 65,904,640 bytes; partial index outbox_unpublished 4,808,704 bytes
  deleted 391,800 published rows in 0.17 s; VACUUM in 0.12 s
  after: 0 rows (0 unpublished); table and indexes 59,162,624 bytes (0 a row); heap 0 bytes; partial index outbox_unpublished 4,808,704 bytes
nothing of seatwave-saga is left (containers, networks, volumes)
```

```text
$ scripts/drill.sh rebuild --runs 3 --run --yes
plan (rebuild): 3 run(s) a point, on a fresh stack (scripts/stack.sh up, then down -v)
stack up at 2026-11-12T21:22:45Z; load average 7.17
  published 11,220 events to seatwave.drill.rebuild in relay order
run 1 (load 7.08):
  HIGHER_VERSION_WINS: rebuilt from 11,220 records in 1.17 s (0.93 s reading the topic; 9,600 events a second); rows changed 11,000; rows 10,000; differences from the booking table 0
run 2 (load 7.08):
  HIGHER_VERSION_WINS: rebuilt from 11,220 records in 1.12 s (0.87 s reading the topic; 9,975 events a second); rows changed 11,000; rows 10,000; differences from the booking table 0
run 3 (load 7.08):
  HIGHER_VERSION_WINS: rebuilt from 11,220 records in 1.15 s (0.89 s reading the topic; 9,795 events a second); rows changed 11,000; rows 10,000; differences from the booking table 0
  published 11,220 events to seatwave.drill.shuffled in shuffled order
shuffled delivery (load 6.91):
  HIGHER_VERSION_WINS: rebuilt from 11,220 records in 1.20 s (0.86 s reading the topic; 9,332 events a second); rows changed 10,500; rows 10,000; differences from the booking table 0
shuffled delivery (load 6.91):
  LAST_APPLIED_WINS: rebuilt from 11,220 records in 1.13 s (0.88 s reading the topic; 9,933 events a second); rows changed 11,220; rows 10,000; differences from the booking table 500
nothing of seatwave-saga is left (containers, networks, volumes)
```

```text
$ scripts/drill.sh two-phase --runs 1 --run --yes
plan (two-phase): 1 run(s) a point, on a fresh stack (scripts/stack.sh up, then down -v)
stack up at 2026-11-12T21:02:00Z; load average 17.61
  booking-db max_prepared_transactions = 0
  booking-db PREPARE TRANSACTION: 55000 ERROR: prepared transactions are disabled
coordinator crash after both votes (load 17.61):
  2026-11-12T21:02:01.076480Z both prepared: seat A-7 locked in inventory, charge row in payment
  2026-11-12T21:02:01.082869Z coordinator exits without a decision for inventory (crash)
  2026-11-12T21:02:02.536966Z seat A-7: LOCKED: 55P03 after 1 s (ERROR: canceling statement due to lock timeout)
    pg_prepared_xacts: gid b-1004-inventory, prepared 2026-11-12T21:02:01.072574Z, age 00:00:01.467493; locks held by no session 4
    pg_prepared_xacts: gid b-1004-payment, prepared 2026-11-12T21:02:01.075897Z, age 00:00:01.470537; locks held by no session 8
  2026-11-12T21:02:34.035589Z seat A-7: LOCKED: 55P03 after 1 s (ERROR: canceling statement due to lock timeout)
    pg_prepared_xacts: gid b-1004-inventory, prepared 2026-11-12T21:02:01.072574Z, age 00:00:32.96704; locks held by no session 4
    pg_prepared_xacts: gid b-1004-payment, prepared 2026-11-12T21:02:01.075897Z, age 00:00:32.968527; locks held by no session 8
  inventory-db restarted at 21:03:06Z
  2026-11-12T21:03:08.347795Z seat A-7: LOCKED: 55P03 after 1 s (ERROR: canceling statement due to lock timeout)
    pg_prepared_xacts: gid b-1004-inventory, prepared 2026-11-12T21:02:01.072574Z, age 00:01:07.28278; locks held by no session 4
    pg_prepared_xacts: gid b-1004-payment, prepared 2026-11-12T21:02:01.075897Z, age 00:01:07.288785; locks held by no session 8
  2026-11-12T21:03:39.791223Z seat A-7: LOCKED: 55P03 after 1 s (ERROR: canceling statement due to lock timeout)
    pg_prepared_xacts: gid b-1004-inventory, prepared 2026-11-12T21:02:01.072574Z, age 00:01:38.721856; locks held by no session 4
    pg_prepared_xacts: gid b-1004-payment, prepared 2026-11-12T21:02:01.075897Z, age 00:01:38.722894; locks held by no session 8
  the coordinator is back at 120 s; it finds no decision for b-1004: presumed abort
  2026-11-12T21:04:10.321162Z ROLLBACK PREPARED b-1004-payment
  2026-11-12T21:04:10.332959Z ROLLBACK PREPARED b-1004-inventory
  2026-11-12T21:04:10.846150Z seat A-7: free: another transaction locked it at once
    pg_prepared_xacts: no row for b-1004 in seatwave at jdbc:postgresql://127.0.0.1:22802/seatwave
    pg_prepared_xacts: no row for b-1004 in seatwave at jdbc:postgresql://127.0.0.1:22803/seatwave
coordinator crash after telling payment COMMIT (load 12.87):
  2026-11-12T21:04:11.432009Z both prepared: seat A-7 locked in inventory, charge row in payment
  2026-11-12T21:04:11.434478Z COMMIT sent to payment only: the fan is charged
  2026-11-12T21:04:11.441589Z coordinator exits without a decision for inventory (crash)
  2026-11-12T21:04:12.924759Z seat A-7: LOCKED: 55P03 after 1 s (ERROR: canceling statement due to lock timeout)
    pg_prepared_xacts: gid b-1005-inventory, prepared 2026-11-12T21:04:11.428462Z, age 00:00:01.502864; locks held by no session 4
    pg_prepared_xacts: no row for b-1005 in seatwave at jdbc:postgresql://127.0.0.1:22803/seatwave
  the coordinator is back; its log says COMMIT, so it finishes the commit
  2026-11-12T21:04:43.549320Z b-1005-payment: nothing prepared (already decided)
  2026-11-12T21:04:43.571516Z COMMIT PREPARED b-1005-inventory
  2026-11-12T21:04:44.158524Z seat A-7: free: another transaction locked it at once
    pg_prepared_xacts: no row for b-1005 in seatwave at jdbc:postgresql://127.0.0.1:22802/seatwave
    pg_prepared_xacts: no row for b-1005 in seatwave at jdbc:postgresql://127.0.0.1:22803/seatwave
nothing of seatwave-saga is left (containers, networks, volumes)
```

```text
$ scripts/drill.sh lab1 --runs 1 --run --yes
plan (lab1): 1 run(s) a point, on a fresh stack (scripts/stack.sh up, then down -v)
stack up at 2026-11-12T21:05:38Z; load average 14.35
  broker stopped at 21:06:02Z (load 15.30)
  broker started at 21:06:23Z
  t=  1 s  holds committed    448  unsent outbox rows    130  relay lag   0.1 s
  t=  2 s  holds committed  1,001  unsent outbox rows     29  relay lag   0.1 s
  t=  3 s  holds committed  1,499  unsent outbox rows     34  relay lag   0.1 s
  t=  4 s  holds committed  1,998  unsent outbox rows     17  relay lag   0.0 s
  t=  5 s  holds committed  2,501  unsent outbox rows     44  relay lag   0.1 s
  t=  6 s  holds committed  3,002  unsent outbox rows     21  relay lag   0.0 s
  t=  7 s  holds committed  3,500  unsent outbox rows     53  relay lag   0.1 s
  t=  8 s  holds committed  3,961  unsent outbox rows     31  relay lag   0.2 s
  t=  9 s  holds committed  4,499  unsent outbox rows     61  relay lag   0.1 s
  t= 10 s  holds committed  4,998  unsent outbox rows     47  relay lag   0.1 s
  t= 11 s  holds committed  5,500  unsent outbox rows     60  relay lag   0.1 s
  t= 12 s  holds committed  6,001  unsent outbox rows     32  relay lag   0.1 s
  t= 13 s  holds committed  6,500  unsent outbox rows      7  relay lag   0.0 s
  t= 14 s  holds committed  7,001  unsent outbox rows     33  relay lag   0.1 s
  t= 15 s  holds committed  7,435  unsent outbox rows     20  relay lag   0.1 s
  t= 16 s  holds committed  7,987  unsent outbox rows     14  relay lag   0.1 s
  t= 17 s  holds committed  8,499  unsent outbox rows     11  relay lag   0.0 s
  t= 18 s  holds committed  8,999  unsent outbox rows    109  relay lag   0.2 s
  t= 19 s  holds committed  9,500  unsent outbox rows     45  relay lag   0.1 s
  t= 20 s  holds committed 10,000  unsent outbox rows    315  relay lag   0.6 s
  t= 21 s  holds committed 10,499  unsent outbox rows    813  relay lag   1.6 s
  t= 22 s  holds committed 10,997  unsent outbox rows  1,311  relay lag   2.6 s
  t= 23 s  holds committed 11,455  unsent outbox rows  1,770  relay lag   3.6 s
  t= 24 s  holds committed 11,879  unsent outbox rows  2,195  relay lag   4.6 s
  t= 25 s  holds committed 12,498  unsent outbox rows  2,813  relay lag   5.6 s
  t= 26 s  holds committed 13,000  unsent outbox rows  3,313  relay lag   6.6 s
  t= 27 s  holds committed 13,503  unsent outbox rows  3,817  relay lag   7.6 s
  t= 28 s  holds committed 14,001  unsent outbox rows  4,315  relay lag   8.6 s
  t= 29 s  holds committed 14,502  unsent outbox rows  4,817  relay lag   9.6 s
  t= 30 s  holds committed 15,000  unsent outbox rows  5,313  relay lag  10.6 s
  t= 31 s  holds committed 15,500  unsent outbox rows  5,814  relay lag  11.6 s
  t= 32 s  holds committed 16,001  unsent outbox rows  6,313  relay lag  12.6 s
  t= 33 s  holds committed 16,502  unsent outbox rows  6,817  relay lag  13.6 s
  t= 34 s  holds committed 17,002  unsent outbox rows  7,316  relay lag  14.6 s
  t= 35 s  holds committed 17,502  unsent outbox rows  7,815  relay lag  15.6 s
  t= 36 s  holds committed 17,997  unsent outbox rows  8,312  relay lag  16.6 s
  t= 37 s  holds committed 18,500  unsent outbox rows  8,814  relay lag  17.6 s
  t= 38 s  holds committed 19,002  unsent outbox rows  9,316  relay lag  18.6 s
  t= 39 s  holds committed 19,502  unsent outbox rows  9,816  relay lag  19.6 s
  t= 40 s  holds committed 20,003  unsent outbox rows 10,317  relay lag  20.6 s
  t= 41 s  holds committed 20,502  unsent outbox rows 10,816  relay lag  21.6 s
  t= 42 s  holds committed 20,993  unsent outbox rows 11,309  relay lag  22.6 s
  t= 43 s  holds committed 21,499  unsent outbox rows 11,813  relay lag  23.6 s
  t= 44 s  holds committed 22,001  unsent outbox rows 12,315  relay lag  24.6 s
  t= 45 s  holds committed 22,457  unsent outbox rows 12,765  relay lag  25.6 s
  t= 46 s  holds committed 23,003  unsent outbox rows 13,317  relay lag  26.6 s
  t= 47 s  holds committed 23,500  unsent outbox rows     24  relay lag   0.0 s
  t= 48 s  holds committed 23,909  unsent outbox rows     43  relay lag   0.2 s
  t= 49 s  holds committed 24,499  unsent outbox rows     60  relay lag   0.1 s
  t= 50 s  holds committed 24,999  unsent outbox rows     15  relay lag   0.0 s
  t= 51 s  holds committed 25,500  unsent outbox rows     43  relay lag   0.1 s
  t= 52 s  holds committed 26,001  unsent outbox rows     25  relay lag   0.1 s
  t= 53 s  holds committed 26,502  unsent outbox rows     14  relay lag   0.0 s
  t= 54 s  holds committed 27,002  unsent outbox rows     10  relay lag   0.0 s
  t= 55 s  holds committed 27,500  unsent outbox rows     49  relay lag   0.1 s
  t= 56 s  holds committed 28,001  unsent outbox rows     27  relay lag   0.1 s
  t= 57 s  holds committed 28,500  unsent outbox rows      9  relay lag   0.0 s
  t= 58 s  holds committed 29,002  unsent outbox rows     47  relay lag   0.1 s
  t= 59 s  holds committed 29,501  unsent outbox rows     23  relay lag   0.1 s
  t= 60 s  holds committed 30,000  unsent outbox rows     60  relay lag   0.1 s
  t= 61 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 62 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 63 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 64 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 65 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 66 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 67 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 68 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 69 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 70 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 71 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 72 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 73 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 74 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 75 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 76 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 77 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 78 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 79 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 80 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 81 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 82 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 83 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 84 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 85 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 86 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 87 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 88 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 89 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  t= 90 s  holds committed 30,000  unsent outbox rows      0  relay lag   0.0 s
  holds committed 30,000, refused or failed 0, offered 30,000 at 500 a second
  seatwave.inventory: 31,066 records, 30,000 distinct hold_id; holds committed 30,000; committed without an event 0; events for no row 0
nothing of seatwave-saga is left (containers, networks, volumes)
```

```text
$ scripts/drill.sh command8 --runs 1 --run --yes
plan (command8): 1 run(s) a point, on a fresh stack (scripts/stack.sh up, then down -v)
stack up at 2026-11-12T21:11:11Z; load average 16.25
  holds committed 10,000, refused or failed 0, offered 10,000 at 2,000 a second
  unpublished before the relay: 10000
  killed relay (pid 44853, SIGKILL) at: relay token 33 pass 1: 1000 rows acknowledged by the broker, not marked
  relay token 34: 11 passes, 10,000 rows published, 10,000 marked, in 0.806 s (12,409 rows a second)
  $ kafka-console-consumer.sh --bootstrap-server localhost:22800 --topic seatwave.inventory --from-beginning --timeout-ms 10000 | wc -l
  11000
  seatwave.inventory: 11,000 records, 10,000 distinct hold_id; holds committed 10,000; committed without an event 0; events for no row 0
  $ SELECT count(*) FROM outbox WHERE published_at IS NULL
  0
nothing of seatwave-saga is left (containers, networks, volumes)
```

Mutants: each changes one rule in a copy of the project and runs the tests that guard it.

```text
mutant refundtwice (test): a refund with no memory of the first (in-memory payment service)
  failed: SagaInvariantTest > aRepeatedRefundRefundsOnce() FAILED
  failed: SagaInvariantTest > everySagaEndsConfirmedAndPaidOrReleasedAndNotChargedWhateverCrashes() FAILED
mutant expirylock (test): the expiry sweep frees a CONFIRMING seat (in-memory inventory)
  failed: ProgramOutputTest > printsTheOutputTheModuleShows(String) > [2] "SagaCli" FAILED
  failed: SagaInvariantTest > aSeatBeingPaidForIsNotFreedByExpiryNorHeldByAnotherFan() FAILED
mutant noeventrule (test): a commit is accepted without its event (in-memory database)
  failed: SagaInvariantTest > refusesAStateChangeCommittedWithoutItsEvent() FAILED
mutant releasetwice (test): a release that is not idempotent: it releases an ended hold again and frees the seat
  failed: CompensationTest > aReleaseOfAnExpiredHoldDoesNotFreeTheSeatAnotherFanHoldsNow() FAILED
  failed: CompensationTest > aRepeatedReleaseAnswersAlreadyDoneAndWritesNoSecondEvent() FAILED
  failed: ProgramOutputTest > printsTheOutputTheModuleShows(String) > [2] "SagaCli" FAILED
mutant olderwins (test): the in-memory read model applies an older version over a newer one
  failed: ProgramOutputTest > printsTheOutputTheModuleShows(String) > [5] "ReadModelCli" FAILED
  failed: ReadModelTest > anOlderEventArrivingAfterANewerOneChangesNothing() FAILED
  failed: ReadModelTest > theVersionRuleRebuildsTheSameRowsInAnyOrderAndLastAppliedDoesNot() FAILED
mutant markbeforeack (stack): the relay marks rows published before the broker's acknowledgement
  failed: PostgresKafkaRelayStackTest > marksNothingWhenTheBrokerDoesNotAcknowledge() FAILED
mutant relayreorders (stack): the relay publishes one aggregate's rows out of commit order
  failed: PostgresKafkaRelayStackTest > publishesOneAggregatesEventsInTheOrderTheyWereCommitted() FAILED
mutant sqlolderwins (stack): the PostgreSQL read model applies an older version over a newer one
  failed: JdbcBookingsByCustomerStackTest > aDuplicateChangesNothingAndUnknownTypesAreSkipped() FAILED
  failed: JdbcBookingsByCustomerStackTest > anOlderEventAfterANewerOneLeavesTheNewerRow() FAILED
mutant sqlexpirylock (stack): the PostgreSQL expiry sweep frees a CONFIRMING seat
  failed: JdbcInventoryStackTest > aLockedSeatIsNeitherExpiredNorHeldByAnotherFanAfterItsHoldEnds() FAILED
mutant notrigger (stack): PostgreSQL accepts a hold change without its outbox row
  failed: JdbcInventoryStackTest > theDatabaseRefusesAHoldChangeCommittedWithoutItsEvent() FAILED
```

The test task 40 times beside busy loops, and `./gradlew check` from a long path and from a folder with a space:

```text
$ for run in $(seq 1 40); do ./gradlew test --rerun-tasks; done   (two busy loops beside it)
40 runs of the test task, 0 failed
(a first loop, before HttpCommandsTest's client timeouts were raised from 200 ms to 2 s, failed once in 40)
$ ./gradlew check   (from a path of 376 characters)
BUILD SUCCESSFUL in 15s
$ ./gradlew check   (from a folder named 'folder with space')
BUILD SUCCESSFUL in 15s
```

Nothing of the project is left after the last run:

```text
$ docker ps -a --filter label=com.docker.compose.project=seatwave-saga --quiet | wc -l
0
$ docker network ls --filter label=com.docker.compose.project=seatwave-saga --quiet | wc -l
0
$ docker volume ls --filter label=com.docker.compose.project=seatwave-saga --quiet | wc -l
0
$ docker images seatwave-saga/payment-provider-stub --quiet | wc -l   (the stub image scripts/stack.sh up builds; kept: no image is deleted)
1
```

## Ports

All on 127.0.0.1, from Module 28's range: Kafka 22800; PostgreSQL booking 22801, inventory 22802, payment 22803; the services (host processes) booking 22810, inventory 22811, payment 22812; the provider's stub 22820.

## Project structure

```text
compose.yaml, stub/Dockerfile, .dockerignore   the stack and the stub's image
config/stack.properties                        where the stack answers, timeouts, relay and retry settings
db/common/V1__outbox.sql                       every service database: outbox, fence, event rule
db/inventory/V2..V4                            Module 13's catalog and holds, the semantic lock, the lab's seats
db/payment/V2__payments.sql, db/booking/V2__booking_sagas.sql
scripts/stack.sh, scripts/drill.sh             up, down, test; the drills
src/main/java/io/plainround/seatwave/saga/     the teaching model and the five programs (JDK only)
  stub/                                        the payment provider's stub (JDK only)
  jdbc/  kafka/  stack/                        PostgreSQL adapters, the relay, HTTP, services and drill tools
src/test/java/...                              unit tests (./gradlew check) and stack tests (tag "stack")
```

## What is simplified

- One Kafka broker: replication, `acks=all` waiting for followers and leader failover are not shown here (Module 27 shows them on three).
- The services run on the host, not in containers, so a drill can kill one by its process id; their calls to the databases and the broker cross Docker Desktop's VM boundary.
- No lease service: a relay's fencing token is given on its command line, and the booking service is the only orchestrator.
- Every seat costs Module 2's $80.00; the provider's stub keeps its ledger in memory.
- The read model, the email claim and the bookings live in the booking database, so the lab needs three databases, not five.
- Commands go to participants by direct HTTP calls; `seatwave.saga.commands` receives them through the outbox as the saga's log, and nothing consumes it here.

## Known issue

One unit test has failed twice for a reason that is not known. On 12 November 2026 the builder's 40-run loop of the test task failed once without keeping the test's name, and a later loop of 25 runs failed once in `PaymentProviderStubTest.aDeclinedCardIsDeclinedAgainAndMovesNoMoney`: a request the stub answers at once got no answer within the test's 5-second wait. A search for the cause then ran the test class 300 times, the whole test task 36 times and a copy of that test's steps 3,000 times in one process without one failure, so the rate is low and the cause was not found; reading the JDK's HTTP server ruled out a lost wake-up after a response, the client's upgrade headers and the lock in the charge path. Until it is explained, treat a single such timeout as this issue and not as a change in behavior. The test now writes a dump of every thread, virtual ones included, to `build/reports/stub-timeouts/` when a long wait times out, so the next time leaves evidence. In the stack drills the stub runs in a container and was judged by its ledger and the database rows: no drill showed a charge that went unanswered without being scripted to.

## No CI on purpose

The course keeps its projects free of CI configuration; every gate (`./gradlew check`, the stack tests, the drills, the mutants) is run locally and its output is above.

## The Compose path

`scripts/stack.sh up --run --yes` generates the database password into the ignored `.secrets/`, builds the stub's image from one source file, starts the stack and waits for every healthcheck and for `kafka-init`; `scripts/stack.sh down --run --yes` removes the containers, network and volumes (`down -v`) and the password, then checks nothing of `seatwave-saga` is left. Without `--run` both print their plan and change nothing.

## License

MIT, see `LICENSE`.

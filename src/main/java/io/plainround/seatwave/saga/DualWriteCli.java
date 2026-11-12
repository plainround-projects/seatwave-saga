package io.plainround.seatwave.saga;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Chapter 28.7 made visible. Part 1 crashes a service that writes its database and publishes to the
 * broker as two separate steps, at every point, in both orders, and prints which effects survived.
 * Part 2 does the same to the transactional outbox, restarts the relay, and counts what the email
 * consumer of Module 27 did. One booking, one event, deterministic.
 */
final class DualWriteCli {

  private static final String TOPIC = "seatwave.bookings";
  private static final int PARTITIONS = 24;
  private static final String BOOKING_ID = "b-1004";
  private static final int RELAY_BATCH = 100;
  private static final long FIRST_FENCING_TOKEN = 33;
  private static final long NOW_MILLIS = 0;

  /** What goes wrong in a run of the two-step service. */
  private enum Failure {
    NONE,
    CRASH_BEFORE_FIRST_STEP,
    CRASH_BETWEEN_THE_STEPS,
    SECOND_STEP_REFUSED
  }

  /** What goes wrong in a run of the outbox path. */
  private enum OutboxCase {
    NO_CRASH("no crash"),
    CRASH_BEFORE_COMMIT("service crashes before the commit"),
    BROKER_DOWN("broker down while the relay runs, back later"),
    RELAY_CRASH("relay crashes after publishing, before marking");

    private final String description;

    OutboxCase(String description) {
      this.description = description;
    }
  }

  private DualWriteCli() {}

  public static void main(String[] arguments) {
    System.out.println(
        "Part 1. Two writes to two systems: commit, then publish (Module 27's confirm)");
    for (Failure failure : Failure.values()) {
      printTwoStep("commit then publish", failure, commitThenPublish(failure));
    }
    System.out.println();
    System.out.println("Part 1b. The other order: publish, then commit");
    for (Failure failure : Failure.values()) {
      printTwoStep("publish then commit", failure, publishThenCommit(failure));
    }
    System.out.println();
    System.out.println("Part 2. Transactional outbox: booking row and event row in one commit");
    for (OutboxCase outboxCase : OutboxCase.values()) {
      outbox(outboxCase);
    }
  }

  private record TwoStepResult(boolean isStateSaved, boolean isEventPublished) {}

  private static TwoStepResult commitThenPublish(Failure failure) {
    Map<String, String> bookingTable = new HashMap<>();
    EventLog eventLog = newLog();
    if (failure == Failure.CRASH_BEFORE_FIRST_STEP) {
      return new TwoStepResult(false, false);
    }
    bookingTable.put(BOOKING_ID, "CONFIRMED");
    if (failure == Failure.CRASH_BETWEEN_THE_STEPS) {
      return new TwoStepResult(true, false);
    }
    if (failure == Failure.SECOND_STEP_REFUSED) {
      // The broker refuses; the commit cannot be taken back, so the event is simply missing.
      eventLog.goDown();
    }
    boolean isPublished = eventLog.append(TOPIC, BOOKING_ID, confirmedEvent()).isPresent();
    return new TwoStepResult(bookingTable.containsKey(BOOKING_ID), isPublished);
  }

  private static TwoStepResult publishThenCommit(Failure failure) {
    Map<String, String> bookingTable = new HashMap<>();
    EventLog eventLog = newLog();
    if (failure == Failure.CRASH_BEFORE_FIRST_STEP) {
      return new TwoStepResult(false, false);
    }
    boolean isPublished = eventLog.append(TOPIC, BOOKING_ID, confirmedEvent()).isPresent();
    if (failure == Failure.CRASH_BETWEEN_THE_STEPS) {
      return new TwoStepResult(false, isPublished);
    }
    if (failure == Failure.SECOND_STEP_REFUSED) {
      // The commit is refused (another fan's booking took the seat first) and rolls back, but the
      // event is already on the broker and consumers will act on it.
      return new TwoStepResult(false, isPublished);
    }
    bookingTable.put(BOOKING_ID, "CONFIRMED");
    return new TwoStepResult(true, isPublished);
  }

  private static void outbox(OutboxCase outboxCase) {
    ServiceDatabase bookingDatabase = new ServiceDatabase("booking");
    EventLog eventLog = newLog();
    if (outboxCase != OutboxCase.CRASH_BEFORE_COMMIT) {
      bookingDatabase.commit(
          bookingDatabase
              .begin()
              .put("bookings", BOOKING_ID, Map.of("state", "CONFIRMED"))
              .addEvent(BOOKING_ID, "BookingConfirmed", Map.of("booking_id", BOOKING_ID)),
          NOW_MILLIS);
    }
    if (outboxCase == OutboxCase.BROKER_DOWN) {
      eventLog.goDown();
    }
    OutboxRelay firstRelay = relay(bookingDatabase, eventLog, FIRST_FENCING_TOKEN);
    OutboxRelay.Pass firstPass =
        outboxCase == OutboxCase.RELAY_CRASH
            ? firstRelay.relayOnceAndCrashBeforeMarking()
            : firstRelay.relayOnce();
    eventLog.comeBack();
    // The restarted relay is a new lease holder, so it carries the next fencing token.
    OutboxRelay.Pass secondPass =
        relay(bookingDatabase, eventLog, FIRST_FENCING_TOKEN + 1).relayOnce();
    List<EventLog.Record> published = eventLog.readFromStart(TOPIC);
    EmailCount emails = consumeOnce(published);
    System.out.println(
        String.format(
            Locale.ROOT,
            "  %-48s state saved: %-3s | first pass published %d, marked %d%s | restart published"
                + " %d | events on the topic: %d | emails: %d, duplicates skipped: %d",
            outboxCase.description,
            bookingDatabase.read("bookings", BOOKING_ID).isPresent() ? "yes" : "no",
            firstPass.published(),
            firstPass.marked(),
            firstPass.wasBrokerDown() ? " (broker down)" : "",
            secondPass.published(),
            published.size(),
            emails.sent(),
            emails.skipped()));
  }

  private record EmailCount(int sent, int skipped) {}

  /** Module 27's idempotent consumer in one loop: the effect's key is claimed once. */
  private static EmailCount consumeOnce(List<EventLog.Record> records) {
    Set<String> processedMessages = new HashSet<>();
    int sent = 0;
    int skipped = 0;
    for (EventLog.Record record : records) {
      String bookingId =
          EventFields.decode(record.value()).orElseThrow().getOrDefault("booking_id", "");
      if (processedMessages.add("confirmation/" + bookingId)) {
        sent++;
      } else {
        skipped++;
      }
    }
    return new EmailCount(sent, skipped);
  }

  private static void printTwoStep(String order, Failure failure, TwoStepResult result) {
    String verdict;
    if (result.isStateSaved() == result.isEventPublished()) {
      verdict = result.isStateSaved() ? "consistent: both" : "consistent: neither";
    } else {
      verdict =
          result.isStateSaved()
              ? "BROKEN: booking saved, event lost (no email, no index row, ever)"
              : "BROKEN: event sent for a booking that does not exist";
    }
    System.out.println(
        String.format(
            Locale.ROOT,
            "  %-20s %-25s state saved: %-3s event published: %-3s -> %s",
            order,
            failure.name().toLowerCase(Locale.ROOT),
            result.isStateSaved() ? "yes" : "no",
            result.isEventPublished() ? "yes" : "no",
            verdict));
  }

  private static EventLog newLog() {
    EventLog eventLog = new EventLog();
    eventLog.createTopic(TOPIC, PARTITIONS);
    return eventLog;
  }

  private static OutboxRelay relay(ServiceDatabase database, EventLog eventLog, long token) {
    return new OutboxRelay(new OutboxRelay.Settings(TOPIC, token, RELAY_BATCH), database, eventLog);
  }

  private static String confirmedEvent() {
    return EventFields.encode(Map.of("type", "BookingConfirmed", "booking_id", BOOKING_ID));
  }
}

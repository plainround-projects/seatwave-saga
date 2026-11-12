package io.plainround.seatwave.saga;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** The in-memory relay's three rules: publish before mark, keep order, obey the fence. */
class OutboxRelayTest {

  private static final String TOPIC = "seatwave.inventory";
  private static final int PARTITIONS = 24;
  private static final int BATCH = 100;
  private static final long FIRST_TOKEN = 33;

  @Test
  void marksNothingAndKeepsEveryRowWhenTheBrokerIsDown() {
    ServiceDatabase database = databaseWithEvents(3);
    EventLog eventLog = log();
    eventLog.goDown();

    OutboxRelay.Pass pass = relay(database, eventLog, FIRST_TOKEN).relayOnce();

    assertThat(pass.marked()).isZero();
    assertThat(pass.wasBrokerDown()).isTrue();
    assertThat(database.unsentCount()).isEqualTo(3);
  }

  @Test
  void aRelayThatCrashesBeforeMarkingRepublishesTheSameMessageIds() {
    ServiceDatabase database = databaseWithEvents(2);
    EventLog eventLog = log();

    relay(database, eventLog, FIRST_TOKEN).relayOnceAndCrashBeforeMarking();
    relay(database, eventLog, FIRST_TOKEN + 1).relayOnce();

    List<String> messageIds =
        eventLog.readFromStart(TOPIC).stream()
            .map(record -> EventFields.decode(record.value()).orElseThrow().get("message_id"))
            .sorted()
            .toList();
    assertThat(messageIds)
        .containsExactly("inventory-1", "inventory-1", "inventory-2", "inventory-2");
    assertThat(database.unsentCount()).isZero();
  }

  @Test
  void aStaleRelayIsFencedAndMarksNothing() {
    ServiceDatabase database = databaseWithEvents(1);
    EventLog eventLog = log();
    relay(database, eventLog, FIRST_TOKEN + 1).relayOnceAndCrashBeforeMarking();
    database.markSent(List.of(), FIRST_TOKEN + 1);

    OutboxRelay.Pass stale = relay(database, eventLog, FIRST_TOKEN).relayOnce();

    assertThat(stale.wasFenced()).isTrue();
    assertThat(database.unsentCount()).isEqualTo(1);
  }

  @Test
  void publishesOneAggregatesEventsInTheOrderTheyWereCommitted() {
    ServiceDatabase database = databaseWithEvents(4);
    EventLog eventLog = log();

    relay(database, eventLog, FIRST_TOKEN).relayOnce();

    List<String> types =
        eventLog.readFromStart(TOPIC).stream()
            .map(record -> EventFields.decode(record.value()).orElseThrow().get("type"))
            .toList();
    assertThat(types).containsExactly("Event1", "Event2", "Event3", "Event4");
  }

  private static ServiceDatabase databaseWithEvents(int count) {
    ServiceDatabase database = new ServiceDatabase("inventory");
    for (int eventIndex = 1; eventIndex <= count; eventIndex++) {
      database.commit(
          database
              .begin()
              .put("holds", "h-1", Map.of("state", "ACTIVE"))
              .addEvent("h-1", "Event" + eventIndex, Map.of("hold_id", "h-1")),
          eventIndex);
    }
    return database;
  }

  private static EventLog log() {
    EventLog eventLog = new EventLog();
    eventLog.createTopic(TOPIC, PARTITIONS);
    return eventLog;
  }

  private static OutboxRelay relay(ServiceDatabase database, EventLog eventLog, long token) {
    return new OutboxRelay(new OutboxRelay.Settings(TOPIC, token, BATCH), database, eventLog);
  }
}

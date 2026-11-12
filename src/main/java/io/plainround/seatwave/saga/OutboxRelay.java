package io.plainround.seatwave.saga;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The polling relay of one service's outbox: read the oldest unsent rows, publish each to the
 * broker keyed by its aggregate id, then mark the published ones sent. Publishing comes before
 * marking, so a crash between the two publishes those rows again on the next pass: the relay
 * delivers at least once, and consumers must be idempotent (Module 27).
 *
 * <p>One relay per outbox at a time, holding a fencing token from Module 23's lease; a relay whose
 * token is older than one the database has seen is told so and stops. Not thread-safe.
 */
final class OutboxRelay {

  private final ServiceDatabase database;
  private final EventLog eventLog;
  private final String topic;
  private final long fencingToken;
  private final int batchSize;

  /** What one pass did. */
  record Pass(int published, int marked, boolean wasBrokerDown, boolean wasFenced) {}

  OutboxRelay(Settings settings, ServiceDatabase database, EventLog eventLog) {
    this.database = Objects.requireNonNull(database, "database");
    this.eventLog = Objects.requireNonNull(eventLog, "eventLog");
    this.topic = settings.topic();
    this.fencingToken = settings.fencingToken();
    this.batchSize = settings.batchSize();
  }

  /** The relay's settings: where it publishes, its fencing token, and rows per pass. */
  record Settings(String topic, long fencingToken, int batchSize) {
    Settings {
      Objects.requireNonNull(topic, "topic");
      if (fencingToken < 1 || batchSize < 1) {
        throw new IllegalArgumentException("fencingToken and batchSize must be at least 1");
      }
    }
  }

  /** One normal pass: publish a batch, then mark it sent. */
  Pass relayOnce() {
    return relay(true);
  }

  /** A pass that dies after publishing and before marking, the relay's one dangerous moment. */
  Pass relayOnceAndCrashBeforeMarking() {
    return relay(false);
  }

  private Pass relay(boolean shouldMark) {
    List<Long> publishedIds = new ArrayList<>();
    boolean wasBrokerDown = false;
    for (ServiceDatabase.OutboxRow row : database.unsent(batchSize)) {
      if (eventLog.append(topic, row.aggregateId(), row.value()).isEmpty()) {
        // Stop at the first refusal: publishing a later row of the same booking first would
        // reorder its events.
        wasBrokerDown = true;
        break;
      }
      publishedIds.add(row.outboxId());
    }
    if (!shouldMark || publishedIds.isEmpty()) {
      return new Pass(publishedIds.size(), 0, wasBrokerDown, false);
    }
    return switch (database.markSent(publishedIds, fencingToken)) {
      case ServiceDatabase.MarkResult.Marked marked ->
          new Pass(publishedIds.size(), marked.rowCount(), wasBrokerDown, false);
      case ServiceDatabase.MarkResult.Fenced fenced ->
          new Pass(publishedIds.size(), 0, wasBrokerDown, true);
    };
  }
}

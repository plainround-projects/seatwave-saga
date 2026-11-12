package io.plainround.seatwave.saga;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * One service's own database, in memory: rows by table and key, and the service's outbox table. A
 * transaction's row writes and outbox rows become visible together or not at all, which is the one
 * property of a real database this module relies on. The database outlives a crash of the service
 * that uses it; a transaction that was never committed leaves no trace.
 *
 * <p>The rule "every state change has its event" is enforced here, where no service can bypass it:
 * {@link #commit} refuses a transaction that changes a row and writes no outbox row. The PostgreSQL
 * form is the trigger of {@code db/V1__outbox_and_sagas.sql}.
 *
 * <p>Not thread-safe: the programs drive it from one thread.
 */
final class ServiceDatabase {

  private final String serviceName;
  private final Map<String, Map<String, String>> rows = new TreeMap<>();
  private final List<OutboxRow> outbox = new ArrayList<>();
  private final Set<Long> sentOutboxIds = new HashSet<>();
  private long nextOutboxId = 1;
  private long highestFencingToken;

  /** One row of the outbox table; {@code value} is the full event as the relay will publish it. */
  record OutboxRow(long outboxId, String aggregateId, String value, long createdAtMillis) {
    OutboxRow {
      Objects.requireNonNull(aggregateId, "aggregateId");
      Objects.requireNonNull(value, "value");
    }
  }

  /** The answer to a relay that marks rows as sent. */
  sealed interface MarkResult {
    /** The rows are marked; the relay may move on. */
    record Marked(int rowCount) implements MarkResult {}

    /** A relay with a newer fencing token exists, so this one is a stale leader and must stop. */
    record Fenced(long highestTokenSeen) implements MarkResult {}
  }

  /** Row writes and outbox rows that commit together. */
  static final class Transaction {
    private final Map<String, Map<String, String>> writes = new LinkedHashMap<>();
    private final List<PendingEvent> events = new ArrayList<>();

    /** Writes (inserts or replaces) one row. */
    Transaction put(String table, String key, Map<String, String> row) {
      writes.put(table + "/" + key, Map.copyOf(row));
      return this;
    }

    /** Adds one outbox row, the event that describes the change, for the booking or hold named. */
    Transaction addEvent(String aggregateId, String eventType, Map<String, String> payload) {
      events.add(new PendingEvent(aggregateId, eventType, new LinkedHashMap<>(payload)));
      return this;
    }
  }

  private record PendingEvent(String aggregateId, String eventType, Map<String, String> payload) {}

  ServiceDatabase(String serviceName) {
    this.serviceName = Objects.requireNonNull(serviceName, "serviceName");
  }

  Transaction begin() {
    return new Transaction();
  }

  /**
   * Applies every write and inserts every outbox row at once, giving each row the next outbox id
   * and a message id made from it. Refuses a transaction that changes a row without an event.
   */
  List<OutboxRow> commit(Transaction transaction, long nowMillis) {
    if (!transaction.writes.isEmpty() && transaction.events.isEmpty()) {
      throw new IllegalStateException(serviceName + ": a state change without its event");
    }
    rows.putAll(transaction.writes);
    List<OutboxRow> inserted = new ArrayList<>();
    for (PendingEvent event : transaction.events) {
      long outboxId = nextOutboxId++;
      Map<String, String> fields = new LinkedHashMap<>();
      fields.put("type", event.eventType());
      // The message id is fixed when the row is written, so a relay that publishes the row twice
      // publishes the same id twice, and a consumer can tell a republish from a new fact.
      fields.put("message_id", serviceName + "-" + outboxId);
      fields.putAll(event.payload());
      OutboxRow row =
          new OutboxRow(outboxId, event.aggregateId(), EventFields.encode(fields), nowMillis);
      outbox.add(row);
      inserted.add(row);
    }
    return List.copyOf(inserted);
  }

  Optional<Map<String, String>> read(String table, String key) {
    return Optional.ofNullable(rows.get(table + "/" + key));
  }

  /** Every row of one table, by key in key order. */
  Map<String, Map<String, String>> readAll(String table) {
    Map<String, Map<String, String>> tableRows = new TreeMap<>();
    rows.forEach((tableAndKey, row) -> {
      if (tableAndKey.startsWith(table + "/")) {
        tableRows.put(tableAndKey.substring(table.length() + 1), row);
      }
    });
    return tableRows;
  }

  /** Up to {@code maxRows} outbox rows not yet marked sent, oldest first. */
  List<OutboxRow> unsent(int maxRows) {
    return outbox.stream()
        .filter(row -> !sentOutboxIds.contains(row.outboxId()))
        .limit(maxRows)
        .toList();
  }

  /** Marks rows sent, unless a relay with a newer fencing token has marked rows before. */
  MarkResult markSent(List<Long> outboxIds, long fencingToken) {
    if (fencingToken < highestFencingToken) {
      return new MarkResult.Fenced(highestFencingToken);
    }
    highestFencingToken = fencingToken;
    sentOutboxIds.addAll(outboxIds);
    return new MarkResult.Marked(outboxIds.size());
  }

  int unsentCount() {
    return outbox.size() - sentOutboxIds.size();
  }

  /** Every outbox row ever written, sent or not, oldest first. */
  List<OutboxRow> outbox() {
    return List.copyOf(outbox);
  }

  String serviceName() {
    return serviceName;
  }
}

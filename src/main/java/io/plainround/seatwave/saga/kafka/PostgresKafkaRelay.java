package io.plainround.seatwave.saga.kafka;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.sql.DataSource;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * The production form of {@code OutboxRelay}: polls one service's PostgreSQL outbox, publishes each
 * row to Kafka keyed by its aggregate id, waits for the broker's acknowledgements, then marks the
 * rows published under its fencing token. A crash between the acknowledgement and the mark
 * publishes those rows again: at least once, absorbed by idempotent consumers.
 *
 * <p>Each record's value is the row's payload with {@code type}, {@code schema_version}, {@code
 * aggregate_version} and a {@code message_id} made from the service's name and the outbox id, all
 * fixed by the row, so a republished row carries the same id. One instance per outbox runs at a
 * time, as Module 23's lease holder; its fencing token comes from the lease. The producer must be
 * built with {@code acks=all} and {@code enable.idempotence=true} ({@link Producers}). Not
 * thread-safe: one thread calls {@link #relayOnce}.
 */
public final class PostgresKafkaRelay {

  // The oldest unpublished rows first. Order matters for one aggregate: its rows are committed one
  // after the other (each waits for the aggregate row's lock), so their ids grow in commit order,
  // and the producer keeps that order within the key's partition.
  private static final String SELECT_UNPUBLISHED =
      """
      SELECT outbox_id, aggregate_id, event_type,
        (payload || jsonb_build_object('type', event_type, 'schema_version', schema_version,
          'aggregate_version', aggregate_version, 'message_id', ? || '-' || outbox_id))::text
      FROM outbox
      WHERE published_at IS NULL
      ORDER BY outbox_id
      LIMIT ?
      """;

  // The fence row moves up only for a token at least as new as the highest seen; the outbox rows
  // are marked only if it did, so a stale relay's mark changes nothing.
  private static final String MARK_PUBLISHED =
      """
      WITH fence AS (
        UPDATE outbox_relay_fence SET highest_token = ? WHERE highest_token <= ?
        RETURNING highest_token)
      UPDATE outbox SET published_at = now()
      WHERE outbox_id = ANY (?) AND EXISTS (SELECT 1 FROM fence)
      """;

  private final DataSource outboxDatabase;
  private final Producer<String, String> producer;
  private final Settings settings;
  private final BeforeMark beforeMark;

  /**
   * Where to publish, the lease's fencing token, rows per pass, and the wait for the broker.
   *
   * @param serviceName the outbox's owner, the first part of every message id
   * @param defaultTopic the topic of every event type not in {@code topicByEventType}
   * @param topicByEventType the event types that go elsewhere (the booking outbox sends its final
   *     events to {@code seatwave.bookings} and its commands to the saga's command log)
   * @param fencingToken this relay's token from the lease
   * @param batchSize rows a pass at most
   * @param sendTimeout how long a pass waits for the broker's acknowledgements
   */
  public record Settings(
      String serviceName,
      String defaultTopic,
      Map<String, String> topicByEventType,
      long fencingToken,
      int batchSize,
      Duration sendTimeout) {
    /** Refuses a missing value and a token, batch size or timeout that is not positive. */
    public Settings {
      Objects.requireNonNull(serviceName, "serviceName");
      Objects.requireNonNull(defaultTopic, "defaultTopic");
      topicByEventType = Map.copyOf(topicByEventType);
      Objects.requireNonNull(sendTimeout, "sendTimeout");
      if (fencingToken < 1 || batchSize < 1 || sendTimeout.isNegative() || sendTimeout.isZero()) {
        throw new IllegalArgumentException("token, batch size and send timeout must be positive");
      }
    }

    String topicFor(String eventType) {
      return topicByEventType.getOrDefault(eventType, defaultTopic);
    }
  }

  /**
   * What runs between the broker's acknowledgement and the mark. Production passes nothing; the
   * drills pass one that blocks, so that a kill lands exactly in the relay's dangerous moment.
   */
  @FunctionalInterface
  public interface BeforeMark {
    /** Called with the number of rows the broker acknowledged, before any is marked. */
    void acknowledged(int rowCount) throws InterruptedException;
  }

  /**
   * What one pass did.
   *
   * @param published rows sent and acknowledged
   * @param marked rows marked published; 0 with {@code published} above 0 means a newer relay holds
   *     the fence, and this one must stop
   */
  public record Pass(int published, int marked) {
    /** True when a newer relay holds the fence. */
    public boolean isFenced() {
      return published > 0 && marked == 0;
    }
  }

  /** The relay; the data source and the producer are long-lived and shared, never closed here. */
  public PostgresKafkaRelay(
      DataSource outboxDatabase, Producer<String, String> producer, Settings settings) {
    this(outboxDatabase, producer, settings, rowCount -> {});
  }

  /** The relay with a step between acknowledgement and mark (for the drills). */
  public PostgresKafkaRelay(
      DataSource outboxDatabase,
      Producer<String, String> producer,
      Settings settings,
      BeforeMark beforeMark) {
    this.outboxDatabase = Objects.requireNonNull(outboxDatabase, "outboxDatabase");
    this.producer = Objects.requireNonNull(producer, "producer");
    this.settings = Objects.requireNonNull(settings, "settings");
    this.beforeMark = Objects.requireNonNull(beforeMark, "beforeMark");
  }

  /**
   * One pass: read up to a batch of unpublished rows, send them all, wait for every
   * acknowledgement, then mark them. Throws when the broker refused a row or did not acknowledge in
   * time; nothing is marked, so the next pass publishes the same rows again.
   */
  public Pass relayOnce() throws SQLException, InterruptedException, TimeoutException {
    try (Connection connection = outboxDatabase.getConnection()) {
      List<Long> outboxIds = new ArrayList<>();
      List<Future<RecordMetadata>> acknowledgements = new ArrayList<>();
      try (PreparedStatement select = connection.prepareStatement(SELECT_UNPUBLISHED)) {
        select.setString(1, settings.serviceName());
        select.setInt(2, settings.batchSize());
        try (ResultSet rows = select.executeQuery()) {
          while (rows.next()) {
            outboxIds.add(rows.getLong(1));
            // Sending every row before waiting lets the producer batch them.
            acknowledgements.add(
                producer.send(
                    new ProducerRecord<>(
                        settings.topicFor(rows.getString(3)),
                        rows.getString(2),
                        rows.getString(4))));
          }
        }
      }
      if (outboxIds.isEmpty()) {
        return new Pass(0, 0);
      }
      awaitAll(acknowledgements);
      beforeMark.acknowledged(outboxIds.size());
      return new Pass(outboxIds.size(), mark(connection, outboxIds));
    }
  }

  private void awaitAll(List<Future<RecordMetadata>> acknowledgements)
      throws InterruptedException, TimeoutException {
    long deadlineNanos = System.nanoTime() + settings.sendTimeout().toNanos();
    for (Future<RecordMetadata> acknowledgement : acknowledgements) {
      try {
        acknowledgement.get(Math.max(0, deadlineNanos - System.nanoTime()), TimeUnit.NANOSECONDS);
      } catch (ExecutionException brokerRefused) {
        // Not stored: stop before marking anything; the rows stay unpublished and are retried.
        throw new IllegalStateException("broker refused an outbox row", brokerRefused.getCause());
      }
    }
  }

  private int mark(Connection connection, List<Long> outboxIds) throws SQLException {
    try (PreparedStatement update = connection.prepareStatement(MARK_PUBLISHED)) {
      Array ids = connection.createArrayOf("bigint", outboxIds.toArray());
      update.setLong(1, settings.fencingToken());
      update.setLong(2, settings.fencingToken());
      update.setArray(3, ids);
      return update.executeUpdate();
    }
  }
}

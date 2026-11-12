package io.plainround.seatwave.saga.stack;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.zaxxer.hikari.HikariDataSource;
import io.plainround.seatwave.saga.jdbc.Outbox;
import io.plainround.seatwave.saga.kafka.Producers;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;

/**
 * Chapter 28.7 on the real stack: the booking service's confirm, written three ways, and broken at
 * one point. It confirms {@code --bookings} bookings into the booking database's {@code bookings}
 * table and announces each with {@code BookingConfirmed}; the last booking meets {@code --failure}.
 *
 * <ul>
 *   <li>{@code commit-then-publish} and {@code publish-then-commit}: the dual write, a commit and a
 *       Kafka send as two steps;
 *   <li>{@code outbox}: the booking row and its outbox row in one commit; a relay (RelayCli)
 *       publishes later.
 * </ul>
 *
 * Failures: {@code crash-before-first} and {@code crash-between} pause with a {@code PAUSED} line
 * for the drill to kill the process; {@code second-refused} makes the second step fail (a topic
 * that does not exist for a publish, a second booking for the same hold for a commit).
 *
 * <p>Usage: {@code DualWriteDrillCli --mode M --failure F --bookings N --topic T}
 */
public final class DualWriteDrillCli {

  private static final ObjectMapper JSON = JsonMapper.builder().build();
  private static final long MAX_BOOKINGS = 100_000;
  private static final Duration REFUSED_SEND_TIMEOUT = Duration.ofSeconds(2);

  private final HikariDataSource database;
  private final KafkaProducer<String, String> producer;
  private final String topic;
  private final Duration sendTimeout;

  private DualWriteDrillCli(
      HikariDataSource database,
      KafkaProducer<String, String> producer,
      String topic,
      Duration sendTimeout) {
    this.database = database;
    this.producer = producer;
    this.topic = topic;
    this.sendTimeout = sendTimeout;
  }

  /** Writes the bookings, then exits, unless a crash point paused it first. */
  public static void main(String[] arguments) throws Exception {
    StackSettings settings = StackSettings.load();
    Arguments options =
        Arguments.parse(
            List.of(arguments),
            Map.of("mode", "", "failure", "none", "bookings", "1", "topic", ""));
    String mode =
        options.choice("mode", Set.of("commit-then-publish", "publish-then-commit", "outbox"));
    String failure =
        options.choice(
            "failure", Set.of("none", "crash-before-first", "crash-between", "second-refused"));
    int bookings = Math.toIntExact(options.number("bookings", 1, MAX_BOOKINGS));
    String topic = options.name("topic");
    if (failure.equals("second-refused") && bookings < 2) {
      throw new IllegalArgumentException("second-refused needs --bookings 2 or more");
    }
    if (mode.equals("outbox") && failure.equals("second-refused")) {
      throw new IllegalArgumentException(
          "the outbox has no second step to refuse; the relay's crash is RelayCli --pause-on-pass");
    }
    try (HikariDataSource database =
            settings.openPool("dual-write", settings.databases().bookingUrl());
        KafkaProducer<String, String> producer =
            Producers.create(
                settings.kafkaBootstrap(), "dual-write", settings.timeouts().stack())) {
      DualWriteDrillCli writer =
          new DualWriteDrillCli(database, producer, topic, settings.timeouts().stack());
      UUID firstHold = UUID.randomUUID();
      for (int bookingIndex = 1; bookingIndex <= bookings; bookingIndex++) {
        boolean isLast = bookingIndex == bookings;
        UUID bookingId = UUID.randomUUID();
        // The refused commit reuses the first booking's hold: one_booking_per_hold says no.
        boolean isRefusedCommit =
            isLast
                && failure.equals("second-refused")
                && mode.equals("publish-then-commit")
                && bookingIndex > 1;
        UUID holdId = bookingIndex == 1 || isRefusedCommit ? firstHold : UUID.randomUUID();
        String outcome = writer.confirm(mode, isLast ? failure : "none", bookingId, holdId);
        if (isLast) {
          System.out.println("last booking " + bookingId + ": " + outcome);
        }
      }
      System.out.println(mode + "/" + failure + ": " + bookings + " booking(s) written");
    }
  }

  private String confirm(String mode, String failure, UUID bookingId, UUID holdId)
      throws Exception {
    return switch (mode) {
      case "commit-then-publish" -> commitThenPublish(failure, bookingId, holdId);
      case "publish-then-commit" -> publishThenCommit(failure, bookingId, holdId);
      default -> outbox(failure, bookingId, holdId);
    };
  }

  private String commitThenPublish(String failure, UUID bookingId, UUID holdId) throws Exception {
    if (failure.equals("crash-before-first")) {
      Pause.untilKilled("before the commit of " + bookingId);
    }
    commit(bookingId, holdId, false);
    if (failure.equals("crash-between")) {
      Pause.untilKilled("after the commit of " + bookingId + ", before its publish");
    }
    if (failure.equals("second-refused")) {
      return publish(topic + ".missing", bookingId, REFUSED_SEND_TIMEOUT)
          ? "published"
          : "committed, publish refused (no such topic)";
    }
    return publish(topic, bookingId, sendTimeout) ? "committed and published" : "publish failed";
  }

  private String publishThenCommit(String failure, UUID bookingId, UUID holdId) throws Exception {
    if (failure.equals("crash-before-first")) {
      Pause.untilKilled("before the publish of " + bookingId);
    }
    if (!publish(topic, bookingId, sendTimeout)) {
      return "publish failed";
    }
    if (failure.equals("crash-between")) {
      Pause.untilKilled("after the publish of " + bookingId + ", before its commit");
    }
    try {
      commit(bookingId, holdId, false);
      return "published and committed";
    } catch (SQLException refused) {
      return "published, commit refused (" + refused.getSQLState() + ")";
    }
  }

  private String outbox(String failure, UUID bookingId, UUID holdId) throws Exception {
    if (failure.equals("crash-before-first")) {
      // The rows are written but not committed: the kill makes PostgreSQL roll them back.
      try (Connection transaction = database.getConnection()) {
        transaction.setAutoCommit(false);
        insert(transaction, bookingId, holdId, true);
        Pause.untilKilled("before the commit of " + bookingId + " and its outbox row");
      }
    }
    commit(bookingId, holdId, true);
    if (failure.equals("crash-between")) {
      Pause.untilKilled("after the commit of " + bookingId + " and its outbox row");
    }
    return "committed with its outbox row";
  }

  private void commit(UUID bookingId, UUID holdId, boolean withOutboxRow) throws SQLException {
    try (Connection transaction = database.getConnection()) {
      transaction.setAutoCommit(false);
      try {
        insert(transaction, bookingId, holdId, withOutboxRow);
        transaction.commit();
      } catch (SQLException refused) {
        transaction.rollback();
        throw refused;
      }
    }
  }

  private static void insert(
      Connection transaction, UUID bookingId, UUID holdId, boolean withOutboxRow)
      throws SQLException {
    try (PreparedStatement insert =
        transaction.prepareStatement(
            "INSERT INTO bookings (booking_id, hold_id, price_paid, currency, confirmed_at)"
                + " VALUES (?, ?, 80.00, 'USD', now())")) {
      insert.setObject(1, bookingId);
      insert.setObject(2, holdId);
      insert.executeUpdate();
    }
    if (withOutboxRow) {
      Outbox.insert(
          transaction,
          bookingId.toString(),
          1,
          "BookingConfirmed",
          Map.of("booking_id", bookingId.toString(), "hold_id", holdId.toString()));
    }
  }

  /** True when the broker acknowledged; false when it refused or did not answer in time. */
  private boolean publish(String targetTopic, UUID bookingId, Duration timeout) throws Exception {
    Map<String, Object> event = new LinkedHashMap<>();
    event.put("type", "BookingConfirmed");
    event.put("schema_version", 1);
    event.put("message_id", "dual-" + bookingId);
    event.put("booking_id", bookingId.toString());
    try {
      producer
          .send(
              new ProducerRecord<>(
                  targetTopic, bookingId.toString(), JSON.writeValueAsString(event)))
          .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
      return true;
    } catch (ExecutionException | TimeoutException | KafkaException refused) {
      return false;
    }
  }
}

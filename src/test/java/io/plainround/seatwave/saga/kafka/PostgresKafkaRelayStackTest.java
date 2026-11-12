package io.plainround.seatwave.saga.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.zaxxer.hikari.HikariDataSource;
import io.plainround.seatwave.saga.jdbc.Outbox;
import io.plainround.seatwave.saga.stack.StackClients;
import io.plainround.seatwave.saga.stack.StackSettings;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The relay on PostgreSQL and Kafka: an aggregate's events in commit order, nothing marked without
 * the broker's acknowledgement, a stale relay fenced, and the same message id on a republish. Runs
 * on the payment database, whose service is not running during the stack tests.
 */
@Tag("stack")
class PostgresKafkaRelayStackTest {

  private static final Duration SHORT_TIMEOUT = Duration.ofSeconds(2);
  private static final int BATCH = 1_000;
  private static final int VERSIONS = 5;

  /**
   * Every relay of these tests takes a newer token than the last, so only the fence test's stale
   * relay is ever fenced, whatever order the tests run in.
   */
  private static final AtomicLong TOKENS = new AtomicLong();

  private static StackSettings settings;
  private static HikariDataSource database;
  private static KafkaProducer<String, String> producer;

  @BeforeAll
  static void open() throws Exception {
    settings = StackSettings.load();
    database = settings.openPool("relay-test", settings.databases().paymentUrl());
    producer = Producers.create(settings.kafkaBootstrap(), "relay-test", SHORT_TIMEOUT);
  }

  @AfterAll
  static void close() {
    producer.close();
    database.close();
  }

  @Test
  void publishesOneAggregatesEventsInTheOrderTheyWereCommitted() throws Exception {
    String topic = newTopic();
    String aggregate = "order-" + UUID.randomUUID();
    for (int version = 1; version <= VERSIONS; version++) {
      commitEvent(aggregate, version);
    }

    relay(topic, TOKENS.incrementAndGet()).relayOnce();

    List<Long> versions = new ArrayList<>();
    for (ConsumerRecord<String, String> record :
        StackClients.readAll(settings.kafkaBootstrap(), topic)) {
      if (record.key().equals(aggregate)) {
        versions.add(
            JsonMapper.builder()
                .build()
                .readTree(record.value())
                .path("aggregate_version")
                .asLong());
      }
    }
    assertThat(versions).containsExactly(1L, 2L, 3L, 4L, 5L);
  }

  @Test
  void marksNothingWhenTheBrokerDoesNotAcknowledge() throws Exception {
    String aggregate = "unacknowledged-" + UUID.randomUUID();
    commitEvent(aggregate, 1);

    assertThatThrownBy(() ->
            relay("seatwave.no-such-topic-" + UUID.randomUUID(), TOKENS.incrementAndGet())
                .relayOnce())
        .isInstanceOfAny(
            IllegalStateException.class,
            java.util.concurrent.TimeoutException.class,
            org.apache.kafka.common.KafkaException.class);

    assertThat(isPublished(aggregate)).isFalse();
  }

  @Test
  void aStaleRelayIsFencedAndTheRepublishCarriesTheSameMessageId() throws Exception {
    String topic = newTopic();
    String aggregate = "fenced-" + UUID.randomUUID();
    commitEvent(aggregate, 1);
    long newer = TOKENS.addAndGet(2);
    relay(topic, newer).relayOnce();
    String second = "fenced-" + UUID.randomUUID();
    commitEvent(second, 1);

    PostgresKafkaRelay.Pass stale = relay(topic, newer - 1).relayOnce();
    PostgresKafkaRelay.Pass current = relay(topic, TOKENS.incrementAndGet()).relayOnce();

    assertThat(stale.isFenced()).isTrue();
    assertThat(current.marked()).isPositive();
    List<String> messageIds = new ArrayList<>();
    for (ConsumerRecord<String, String> record :
        StackClients.readAll(settings.kafkaBootstrap(), topic)) {
      if (record.key().equals(second)) {
        JsonNode value = JsonMapper.builder().build().readTree(record.value());
        messageIds.add(value.path("message_id").asText());
      }
    }
    assertThat(messageIds)
        .hasSize(2)
        .allMatch(messageIds.get(0)::equals)
        .allMatch(id -> id.startsWith("payment-"));
  }

  private static String newTopic() throws Exception {
    String topic = "seatwave.test.relay-" + UUID.randomUUID();
    StackClients.createTopic(settings.kafkaBootstrap(), topic, settings.timeouts().stack());
    return topic;
  }

  private static PostgresKafkaRelay relay(String topic, long token) {
    return new PostgresKafkaRelay(
        database,
        producer,
        new PostgresKafkaRelay.Settings("payment", topic, Map.of(), token, BATCH, SHORT_TIMEOUT));
  }

  private static void commitEvent(String aggregate, int version) throws SQLException {
    try (Connection transaction = database.getConnection()) {
      transaction.setAutoCommit(false);
      Outbox.insert(transaction, aggregate, version, "TestEvent", Map.of("aggregate", aggregate));
      transaction.commit();
    }
  }

  private static boolean isPublished(String aggregate) throws SQLException {
    try (Connection connection = database.getConnection();
        PreparedStatement select =
            connection.prepareStatement(
                "SELECT published_at IS NOT NULL FROM outbox WHERE aggregate_id = ?")) {
      select.setString(1, aggregate);
      try (ResultSet rows = select.executeQuery()) {
        rows.next();
        return rows.getBoolean(1);
      }
    }
  }
}

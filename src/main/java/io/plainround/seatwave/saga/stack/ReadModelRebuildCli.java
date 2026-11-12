package io.plainround.seatwave.saga.stack;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.zaxxer.hikari.HikariDataSource;
import io.plainround.seatwave.saga.BookingHistory;
import io.plainround.seatwave.saga.BookingsByCustomer;
import io.plainround.seatwave.saga.jdbc.JdbcBookingsByCustomer;
import io.plainround.seatwave.saga.kafka.Producers;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Chapter 28.3 on the real stack: publishes {@link BookingHistory}'s events to a Kafka topic as
 * JSON (in the relay's order, or shuffled with a fixed seed so a booking's cancel can arrive before
 * its confirm), then rebuilds {@code bookings_by_customer} in the booking database from the topic's
 * first record, in batches of one transaction each, and reconciles the table with the booking
 * history's truth.
 *
 * <p>Usage: {@code ReadModelRebuildCli publish --topic T --order relay|shuffled} and {@code
 * ReadModelRebuildCli rebuild --topic T --rule HIGHER_VERSION_WINS|LAST_APPLIED_WINS}
 */
public final class ReadModelRebuildCli {

  private static final ObjectMapper JSON = JsonMapper.builder().build();
  private static final TypeReference<Map<String, String>> EVENT = new TypeReference<>() {};
  private static final long SHUFFLE_SEED = 28;
  private static final int BATCH = 500;
  private static final double NANOS_PER_SECOND = 1e9;

  private ReadModelRebuildCli() {}

  /** Runs {@code publish} or {@code rebuild}. */
  public static void main(String[] arguments) throws Exception {
    StackSettings settings = StackSettings.load();
    if (arguments.length < 1) {
      throw new IllegalArgumentException("usage: ReadModelRebuildCli publish|rebuild [options]");
    }
    List<String> rest = List.of(arguments).subList(1, arguments.length);
    switch (arguments[0]) {
      case "publish" -> {
        Arguments options = Arguments.parse(rest, Map.of("topic", "", "order", "relay"));
        publish(
            settings, options.name("topic"), options.choice("order", Set.of("relay", "shuffled")));
      }
      case "rebuild" -> {
        Arguments options =
            Arguments.parse(rest, Map.of("topic", "", "rule", "HIGHER_VERSION_WINS"));
        rebuild(
            settings,
            options.name("topic"),
            BookingsByCustomer.UpdateRule.valueOf(
                options.choice("rule", Set.of("HIGHER_VERSION_WINS", "LAST_APPLIED_WINS"))));
      }
      default -> throw new IllegalArgumentException("unknown command " + arguments[0]);
    }
  }

  private static void publish(StackSettings settings, String topic, String order) throws Exception {
    List<Map<String, String>> events = new ArrayList<>(BookingHistory.generate().events());
    if (order.equals("shuffled")) {
      Collections.shuffle(events, new Random(SHUFFLE_SEED));
    }
    StackClients.createTopic(settings.kafkaBootstrap(), topic, settings.timeouts().stack());
    try (KafkaProducer<String, String> producer =
        Producers.create(
            settings.kafkaBootstrap(), "rebuild-publisher", settings.timeouts().stack(), 1)) {
      List<Future<RecordMetadata>> sent = new ArrayList<>();
      for (Map<String, String> event : events) {
        sent.add(
            producer.send(
                new ProducerRecord<>(
                    topic, event.get("booking_id"), JSON.writeValueAsString(event))));
      }
      for (Future<RecordMetadata> acknowledgement : sent) {
        acknowledgement.get();
      }
    }
    System.out.println(
        String.format(
            Locale.ROOT, "published %,d events to %s in %s order", events.size(), topic, order));
  }

  private static void rebuild(
      StackSettings settings, String topic, BookingsByCustomer.UpdateRule rule) throws Exception {
    BookingHistory history = BookingHistory.generate();
    try (HikariDataSource database =
        settings.openPool("rebuild", settings.databases().bookingUrl())) {
      JdbcBookingsByCustomer projector = new JdbcBookingsByCustomer(database, rule);
      projector.clear();
      long startNanos = System.nanoTime();
      List<ConsumerRecord<String, String>> records =
          StackClients.readAll(settings.kafkaBootstrap(), topic);
      double readSeconds = (System.nanoTime() - startNanos) / NANOS_PER_SECOND;
      int changed = 0;
      List<Map<String, String>> batch = new ArrayList<>();
      for (ConsumerRecord<String, String> record : records) {
        batch.add(JSON.readValue(record.value(), EVENT));
        if (batch.size() == BATCH) {
          changed += projector.apply(batch);
          batch.clear();
        }
      }
      if (!batch.isEmpty()) {
        changed += projector.apply(batch);
      }
      double seconds = (System.nanoTime() - startNanos) / NANOS_PER_SECOND;
      Map<String, BookingsByCustomer.Row> rows = projector.readAll();
      System.out.println(
          String.format(
              Locale.ROOT,
              "%s: rebuilt from %,d records in %.2f s (%.2f s reading the topic; %,.0f events a"
                  + " second); rows changed %,d; rows %,d; differences from the booking table %,d",
              rule,
              records.size(),
              seconds,
              readSeconds,
              records.size() / seconds,
              changed,
              rows.size(),
              BookingsByCustomer.differences(rows, history.truth())));
    }
  }
}

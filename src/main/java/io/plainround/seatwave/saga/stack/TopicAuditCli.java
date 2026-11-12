package io.plainround.seatwave.saga.stack;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Compares what a service committed with what reached a topic, the reconciliation of Section 9's
 * first failure mode. For {@code --target bookings}: every record on the topic is handed to Module
 * 27's idempotent email consumer, which claims {@code confirmation/<booking_id>} in {@code
 * processed_messages} (one email per claim); then the booking database's {@code bookings} rows are
 * compared with the bookings the topic announced. For {@code --target holds}: the inventory
 * database's {@code holds} with the holds on the topic. Read-only except for the claims.
 *
 * <p>Usage: {@code TopicAuditCli --topic T --target bookings|holds [--group G]}
 */
public final class TopicAuditCli {

  private static final ObjectMapper JSON = JsonMapper.builder().build();

  private TopicAuditCli() {}

  /** Prints one line of counts. */
  public static void main(String[] arguments) throws Exception {
    StackSettings settings = StackSettings.load();
    Arguments options =
        Arguments.parse(
            List.of(arguments),
            Map.of("topic", "", "target", "bookings", "group", "notifications-email"));
    String topic = options.name("topic");
    String target = options.choice("target", Set.of("bookings", "holds"));
    String group = options.name("group");
    boolean isBookings = target.equals("bookings");
    String keyField = isBookings ? "booking_id" : "hold_id";
    List<ConsumerRecord<String, String>> records =
        StackClients.readAll(settings.kafkaBootstrap(), topic);
    Set<String> announced = new HashSet<>();
    int emails = 0;
    int skipped = 0;
    try (HikariDataSource database =
        settings.openPool(
            "audit",
            isBookings ? settings.databases().bookingUrl() : settings.databases().inventoryUrl())) {
      try (Connection connection = database.getConnection();
          PreparedStatement claim =
              connection.prepareStatement(
                  "INSERT INTO processed_messages (consumer_group, dedupe_key, message_id, status,"
                      + " claimed_by, claimed_at, done_at)"
                      + " VALUES (?, ?, ?, 'DONE', 'audit', now(), now()) ON CONFLICT DO NOTHING")) {
        for (ConsumerRecord<String, String> record : records) {
          JsonNode event = JSON.readTree(record.value());
          String id = event.path(keyField).asText("");
          announced.add(id);
          if (!isBookings) {
            continue;
          }
          claim.setString(1, group);
          claim.setString(2, "confirmation/" + id);
          claim.setString(3, event.path("message_id").asText(""));
          // One row inserted: this delivery claimed the email. None: an earlier one did.
          if (claim.executeUpdate() == 1) {
            emails++;
          } else {
            skipped++;
          }
        }
      }
      Set<String> committed = committedIds(database, isBookings);
      Set<String> withoutEvent = new HashSet<>(committed);
      withoutEvent.removeAll(announced);
      Set<String> withoutRow = new HashSet<>(announced);
      withoutRow.removeAll(committed);
      System.out.println(
          String.format(
              Locale.ROOT,
              "%s: %,d records, %,d distinct %s; %s committed %,d; committed without an event %,d;"
                  + " events for no row %,d%s",
              topic,
              records.size(),
              announced.size(),
              keyField,
              target,
              committed.size(),
              withoutEvent.size(),
              withoutRow.size(),
              isBookings
                  ? String.format(
                      Locale.ROOT,
                      "; emails %,d, duplicates skipped by the claim %,d",
                      emails,
                      skipped)
                  : ""));
    }
  }

  private static Set<String> committedIds(HikariDataSource database, boolean isBookings)
      throws java.sql.SQLException {
    String select =
        isBookings ? "SELECT booking_id::text FROM bookings" : "SELECT hold_id::text FROM holds";
    Set<String> ids = new HashSet<>();
    try (Connection connection = database.getConnection();
        PreparedStatement statement = connection.prepareStatement(select);
        ResultSet rows = statement.executeQuery()) {
      while (rows.next()) {
        ids.add(rows.getString(1));
      }
    }
    return ids;
  }
}

package io.plainround.seatwave.saga.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;

/**
 * Writes one outbox row inside the caller's transaction: the event that describes the change the
 * same transaction makes. The deferred trigger of each service's tables checks at commit that the
 * row exists for the aggregate's new version, so a caller that forgets this cannot commit.
 *
 * <p>Thread-safe: no state but the JSON writer, which is safe to share once built.
 */
public final class Outbox {

  /** Every event is schema version 1 of its type here; {@code BookingConfirmed} v2 is Lab 3's. */
  public static final int SCHEMA_VERSION = 1;

  private static final ObjectMapper JSON = JsonMapper.builder().build();

  private static final String INSERT =
      """
      INSERT INTO outbox (aggregate_id, aggregate_version, event_type, schema_version, payload)
      VALUES (?, ?, ?, ?, ?::jsonb)
      """;

  private Outbox() {}

  /** Inserts the row; throws when the aggregate already has an event for this version. */
  public static void insert(
      Connection transaction,
      String aggregateId,
      long aggregateVersion,
      String eventType,
      Map<String, ?> payload)
      throws SQLException {
    Objects.requireNonNull(aggregateId, "aggregateId");
    Objects.requireNonNull(eventType, "eventType");
    try (PreparedStatement insert = transaction.prepareStatement(INSERT)) {
      insert.setString(1, aggregateId);
      insert.setLong(2, aggregateVersion);
      insert.setString(3, eventType);
      insert.setInt(4, SCHEMA_VERSION);
      insert.setString(5, JSON.writeValueAsString(payload));
      insert.executeUpdate();
    } catch (JsonProcessingException notJson) {
      throw new IllegalArgumentException("payload of " + eventType + " is not JSON", notJson);
    }
  }
}

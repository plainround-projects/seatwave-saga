package io.plainround.seatwave.saga.jdbc;

import io.plainround.seatwave.saga.BookingsByCustomer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import javax.sql.DataSource;

/**
 * The read model {@code bookings_by_customer} on PostgreSQL, written only from booking events. With
 * HIGHER_VERSION_WINS the upsert changes a row only when the event's {@code booking_version} is
 * higher than the row's: the rule is in the statement's {@code WHERE}, so a duplicate or an older
 * event changes nothing and the table comes out the same in any order of delivery.
 * LAST_APPLIED_WINS exists only to show the drift without the rule.
 *
 * <p>Thread-safe: no state but the data source.
 */
public final class JdbcBookingsByCustomer {

  private static final String UPSERT =
      """
      INSERT INTO bookings_by_customer (booking_id, customer_id, seat_label, status,
        booking_version)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT (booking_id) DO UPDATE SET customer_id = EXCLUDED.customer_id,
        seat_label = EXCLUDED.seat_label, status = EXCLUDED.status,
        booking_version = EXCLUDED.booking_version
      """;

  private static final String ONLY_IF_NEWER =
      " WHERE bookings_by_customer.booking_version < EXCLUDED.booking_version";

  private final DataSource database;
  private final BookingsByCustomer.UpdateRule rule;

  /** A projector with one update rule. */
  public JdbcBookingsByCustomer(DataSource database, BookingsByCustomer.UpdateRule rule) {
    this.database = Objects.requireNonNull(database, "database");
    this.rule = Objects.requireNonNull(rule, "rule");
  }

  /**
   * Applies a batch of decoded booking events in one transaction and returns the rows it changed.
   * Only {@code BookingConfirmed} and {@code BookingCanceled} are read; fields it does not know are
   * ignored (the tolerant reader).
   */
  public int apply(List<Map<String, String>> events) throws SQLException {
    String statement =
        rule == BookingsByCustomer.UpdateRule.HIGHER_VERSION_WINS ? UPSERT + ONLY_IF_NEWER : UPSERT;
    return Transactions.inTransaction(
        database,
        transaction -> {
          try (PreparedStatement upsert = transaction.prepareStatement(statement)) {
            for (Map<String, String> event : events) {
              String type = event.getOrDefault("type", "");
              if (!type.equals("BookingConfirmed") && !type.equals("BookingCanceled")) {
                continue;
              }
              upsert.setString(1, event.get("booking_id"));
              upsert.setString(2, event.get("customer_id"));
              upsert.setString(3, event.getOrDefault("seat_label", "?"));
              upsert.setString(4, type.equals("BookingCanceled") ? "CANCELED" : "CONFIRMED");
              upsert.setLong(5, Long.parseLong(event.get("booking_version")));
              upsert.addBatch();
            }
            int changed = 0;
            for (int count : upsert.executeBatch()) {
              changed += count;
            }
            return changed;
          }
        });
  }

  /** Empties the table, the first step of a rebuild from offset 0. */
  public void clear() throws SQLException {
    Transactions.inTransaction(
        database,
        transaction -> {
          try (PreparedStatement truncate =
              transaction.prepareStatement("TRUNCATE bookings_by_customer")) {
            return truncate.executeUpdate();
          }
        });
  }

  /** Every row, by booking id. */
  public Map<String, BookingsByCustomer.Row> readAll() throws SQLException {
    return Transactions.inTransaction(
        database,
        transaction -> {
          Map<String, BookingsByCustomer.Row> rows = new TreeMap<>();
          try (PreparedStatement select =
                  transaction.prepareStatement(
                      "SELECT booking_id, customer_id, seat_label, status, booking_version"
                          + " FROM bookings_by_customer");
              ResultSet result = select.executeQuery()) {
            while (result.next()) {
              rows.put(
                  result.getString(1),
                  new BookingsByCustomer.Row(
                      result.getString(2),
                      result.getString(3),
                      result.getString(4),
                      result.getLong(5)));
            }
          }
          return rows;
        });
  }
}

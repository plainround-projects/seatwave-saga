package io.plainround.seatwave.saga.jdbc;

import io.plainround.seatwave.saga.BookingSaga;
import io.plainround.seatwave.saga.SagaRecord;
import io.plainround.seatwave.saga.SagaStore;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;

/**
 * {@code booking_sagas} on the booking service's PostgreSQL. Every {@link #record} is one
 * transaction: the saga's row at its next version, the outbox row for that version, and, when the
 * saga completes, its {@code bookings} row. The deferred trigger {@code saga_change_has_event}
 * refuses a commit without the outbox row and {@code final_states_are_final} refuses to move a
 * final saga, whatever this code does.
 *
 * <p>Thread-safe: no state but the data source. One orchestrator drives a saga at a time.
 */
public final class JdbcSagaStore implements SagaStore {

  private static final String CURRENCY = "USD";
  private static final int MAX_STUCK_ROWS = 100;
  private static final double MILLIS_PER_SECOND = 1_000.0;

  private static final String SELECT_COLUMNS =
      "SELECT booking_id::text, hold_id::text, provider_key, state, attempts,"
          + " (extract(epoch FROM updated_at) * 1000)::bigint FROM booking_sagas";

  private final DataSource database;
  private final BigDecimal seatPrice;

  /** A store whose completed bookings record {@code seatPrice} as the price paid. */
  public JdbcSagaStore(DataSource database, BigDecimal seatPrice) {
    this.database = Objects.requireNonNull(database, "database");
    this.seatPrice = Objects.requireNonNull(seatPrice, "seatPrice");
  }

  @Override
  public Optional<SagaRecord> find(String bookingId) {
    try {
      return Transactions.inTransaction(
          database,
          transaction -> {
            try (PreparedStatement select =
                transaction.prepareStatement(SELECT_COLUMNS + " WHERE booking_id = ?")) {
              select.setObject(1, UUID.fromString(bookingId));
              try (ResultSet rows = select.executeQuery()) {
                return rows.next() ? Optional.of(saga(rows)) : Optional.<SagaRecord>empty();
              }
            }
          });
    } catch (SQLException failure) {
      throw new IllegalStateException("cannot read saga " + bookingId, failure);
    }
  }

  @Override
  public void record(SagaRecord saga, String message, long nowMillis) {
    try {
      Transactions.inTransaction(database, transaction -> write(transaction, saga, message));
    } catch (SQLException failure) {
      throw new IllegalStateException(
          "cannot record saga " + saga.bookingId() + " in " + saga.state(), failure);
    }
  }

  @Override
  public List<SagaRecord> openOlderThan(long nowMillis, long olderThanMillis) {
    String select =
        SELECT_COLUMNS
            + " WHERE state NOT IN ('COMPLETED', 'COMPENSATED') AND updated_at < to_timestamp(?)"
            + " ORDER BY updated_at LIMIT "
            + MAX_STUCK_ROWS;
    try {
      return Transactions.inTransaction(
          database,
          transaction -> {
            try (PreparedStatement statement = transaction.prepareStatement(select)) {
              statement.setDouble(1, (nowMillis - olderThanMillis) / MILLIS_PER_SECOND);
              List<SagaRecord> stuck = new ArrayList<>();
              try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                  stuck.add(saga(rows));
                }
              }
              return stuck;
            }
          });
    } catch (SQLException failure) {
      throw new IllegalStateException("cannot list open sagas", failure);
    }
  }

  private long write(Connection transaction, SagaRecord saga, String message) throws SQLException {
    String upsert =
        """
        INSERT INTO booking_sagas (booking_id, hold_id, provider_key, state, attempts, updated_at,
          version)
        VALUES (?, ?, ?, ?, ?, to_timestamp(?), 1)
        ON CONFLICT (booking_id) DO UPDATE SET state = EXCLUDED.state,
          attempts = EXCLUDED.attempts, updated_at = EXCLUDED.updated_at,
          version = booking_sagas.version + 1
        RETURNING version
        """;
    long version;
    try (PreparedStatement statement = transaction.prepareStatement(upsert)) {
      statement.setObject(1, UUID.fromString(saga.bookingId()));
      statement.setObject(2, UUID.fromString(saga.holdId()));
      statement.setString(3, saga.providerKey());
      statement.setString(4, saga.state().name());
      statement.setInt(5, saga.attempts());
      statement.setDouble(6, saga.updatedAtMillis() / MILLIS_PER_SECOND);
      try (ResultSet rows = statement.executeQuery()) {
        rows.next();
        version = rows.getLong(1);
      }
    }
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("booking_id", saga.bookingId());
    payload.put("hold_id", saga.holdId());
    payload.put("state", saga.state().name());
    payload.put("attempts", saga.attempts());
    Outbox.insert(transaction, saga.bookingId(), version, message, payload);
    if (saga.state() == BookingSaga.State.COMPLETED) {
      String booking =
          "INSERT INTO bookings (booking_id, hold_id, price_paid, currency, confirmed_at)"
              + " VALUES (?, ?, ?, ?, now()) ON CONFLICT DO NOTHING";
      try (PreparedStatement statement = transaction.prepareStatement(booking)) {
        statement.setObject(1, UUID.fromString(saga.bookingId()));
        statement.setObject(2, UUID.fromString(saga.holdId()));
        statement.setBigDecimal(3, seatPrice);
        statement.setString(4, CURRENCY);
        statement.executeUpdate();
      }
    }
    return version;
  }

  private static SagaRecord saga(ResultSet rows) throws SQLException {
    return new SagaRecord(
        rows.getString(1),
        rows.getString(2),
        rows.getString(3),
        BookingSaga.State.valueOf(rows.getString(4)),
        rows.getInt(5),
        rows.getLong(6));
  }
}

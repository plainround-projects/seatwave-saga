package io.plainround.seatwave.saga.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariDataSource;
import io.plainround.seatwave.saga.BookingSaga;
import io.plainround.seatwave.saga.SagaRecord;
import io.plainround.seatwave.saga.stack.StackSettings;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** The orchestrator's table on PostgreSQL: one row change, one outbox row, final means final. */
@Tag("stack")
class JdbcSagaStoreStackTest {

  private static final BigDecimal PRICE = new BigDecimal("80.00");
  private static final long LONG_AGO_MILLIS = 1_000;

  private static HikariDataSource database;

  @BeforeAll
  static void openDatabase() throws Exception {
    StackSettings settings = StackSettings.load();
    database = settings.openPool("saga-test", settings.databases().bookingUrl());
  }

  @AfterAll
  static void closeDatabase() {
    database.close();
  }

  @Test
  void everyTransitionWritesItsOutboxRowAndACompletedSagaItsBooking() throws Exception {
    JdbcSagaStore store = new JdbcSagaStore(database, PRICE);
    SagaRecord started = saga(BookingSaga.State.SEAT_HELD);

    store.record(started, "LockSeat", 0);
    store.record(withState(started, BookingSaga.State.COMPLETED), "BookingConfirmed", 0);

    assertThat(count("SELECT count(*) FROM outbox WHERE aggregate_id = ?", started.bookingId()))
        .isEqualTo(2);
    assertThat(
            count("SELECT count(*) FROM bookings WHERE booking_id::text = ?", started.bookingId()))
        .isEqualTo(1);
    assertThat(store.find(started.bookingId()).orElseThrow().state())
        .isEqualTo(BookingSaga.State.COMPLETED);
  }

  @Test
  void aFinalSagaCannotMoveAgain() {
    JdbcSagaStore store = new JdbcSagaStore(database, PRICE);
    SagaRecord completed = saga(BookingSaga.State.COMPLETED);
    store.record(completed, "BookingConfirmed", 0);

    assertThatThrownBy(() ->
            store.record(
                withState(completed, BookingSaga.State.REFUND_REQUESTED), "RefundPayment", 0))
        .rootCause()
        .hasMessageContaining("is COMPLETED and final");
  }

  @Test
  void listsOnlyOpenSagasOlderThanTheThreshold() {
    JdbcSagaStore store = new JdbcSagaStore(database, PRICE);
    SagaRecord open = saga(BookingSaga.State.PAYMENT_REQUESTED);
    store.record(open, "ChargePayment", 0);
    SagaRecord done = saga(BookingSaga.State.COMPENSATED);
    store.record(done, "BookingFailed", 0);

    assertThat(store.openOlderThan(LONG_AGO_MILLIS * 2, LONG_AGO_MILLIS / 2))
        .extracting(SagaRecord::bookingId)
        .contains(open.bookingId())
        .doesNotContain(done.bookingId());
  }

  private static SagaRecord saga(BookingSaga.State state) {
    String hold = UUID.randomUUID().toString();
    return new SagaRecord(
        UUID.randomUUID().toString(),
        hold,
        SagaRecord.PROVIDER_KEY_PREFIX + hold,
        state,
        0,
        LONG_AGO_MILLIS);
  }

  private static SagaRecord withState(SagaRecord saga, BookingSaga.State state) {
    return new SagaRecord(
        saga.bookingId(), saga.holdId(), saga.providerKey(), state, 0, LONG_AGO_MILLIS);
  }

  private static long count(String sql, String id) throws SQLException {
    try (Connection connection = database.getConnection();
        PreparedStatement select = connection.prepareStatement(sql)) {
      select.setString(1, id);
      try (ResultSet rows = select.executeQuery()) {
        rows.next();
        return rows.getLong(1);
      }
    }
  }
}

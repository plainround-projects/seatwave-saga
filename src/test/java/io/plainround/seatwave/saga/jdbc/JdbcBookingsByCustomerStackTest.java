package io.plainround.seatwave.saga.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariDataSource;
import io.plainround.seatwave.saga.BookingsByCustomer;
import io.plainround.seatwave.saga.stack.StackSettings;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** The read model's version rule, in the SQL statement itself. */
@Tag("stack")
class JdbcBookingsByCustomerStackTest {

  private static HikariDataSource database;

  @BeforeAll
  static void openDatabase() throws Exception {
    StackSettings settings = StackSettings.load();
    database = settings.openPool("read-model-test", settings.databases().bookingUrl());
  }

  @AfterAll
  static void closeDatabase() {
    database.close();
  }

  @Test
  void anOlderEventAfterANewerOneLeavesTheNewerRow() throws Exception {
    JdbcBookingsByCustomer projector =
        new JdbcBookingsByCustomer(database, BookingsByCustomer.UpdateRule.HIGHER_VERSION_WINS);
    String booking = "test-" + UUID.randomUUID();

    projector.apply(List.of(event(booking, "BookingCanceled", 2)));
    int changed = projector.apply(List.of(event(booking, "BookingConfirmed", 1)));

    assertThat(changed).isZero();
    assertThat(projector.readAll().get(booking).status()).isEqualTo("CANCELED");
  }

  @Test
  void aDuplicateChangesNothingAndUnknownTypesAreSkipped() throws Exception {
    JdbcBookingsByCustomer projector =
        new JdbcBookingsByCustomer(database, BookingsByCustomer.UpdateRule.HIGHER_VERSION_WINS);
    String booking = "test-" + UUID.randomUUID();

    int changed =
        projector.apply(
            List.of(
                event(booking, "BookingConfirmed", 1),
                event(booking, "BookingConfirmed", 1),
                event(booking, "SeatHeld", 3)));

    assertThat(changed).isEqualTo(1);
    assertThat(projector.readAll().get(booking).bookingVersion()).isEqualTo(1);
  }

  private static Map<String, String> event(String booking, String type, int version) {
    return Map.of(
        "type",
        type,
        "booking_id",
        booking,
        "booking_version",
        Integer.toString(version),
        "customer_id",
        "1042",
        "seat_label",
        "S0-42");
  }
}

package io.plainround.seatwave.saga.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariDataSource;
import io.plainround.seatwave.saga.StepReply;
import io.plainround.seatwave.saga.stack.StackSettings;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Inventory's rules on PostgreSQL 18: the semantic lock against the expiry sweep and another fan,
 * the idempotent release, and the deferred trigger that refuses a hold change without its event.
 */
@Tag("stack")
class JdbcInventoryStackTest {

  private static final Duration SHORT_HOLD = Duration.ofSeconds(1);
  private static final Duration WAIT = Duration.ofSeconds(10);

  /** Seats 39,001 to 39,999 are kept for this test class; drills use lower ones. */
  private static final int FIRST_SEAT = 39_001;

  private static HikariDataSource database;
  private static int nextSeat = FIRST_SEAT;

  @BeforeAll
  static void openDatabase() throws Exception {
    StackSettings settings = StackSettings.load();
    database = settings.openPool("inventory-test", settings.databases().inventoryUrl());
  }

  @AfterAll
  static void closeDatabase() {
    database.close();
  }

  @Test
  void aLockedSeatIsNeitherExpiredNorHeldByAnotherFanAfterItsHoldEnds() throws Exception {
    JdbcInventory inventory = new JdbcInventory(database, SHORT_HOLD);
    int seat = nextSeat++;
    UUID hold = UUID.randomUUID();
    inventory.hold(hold, "A-" + seat, seat);
    inventory.lockForPayment(hold, UUID.randomUUID());

    Awaitility.await().atMost(WAIT).until(() -> isPastItsEnd(hold));
    inventory.expireDue();
    StepReply secondFan = inventory.hold(UUID.randomUUID(), "A-" + seat, seat + 1);

    assertThat(inventory.holdState(hold)).isEqualTo("CONFIRMING");
    assertThat(secondFan).isInstanceOf(StepReply.Refused.class);
  }

  @Test
  void anUnlockedHoldPastItsEndIsExpiredAndTheSeatFreed() throws Exception {
    JdbcInventory inventory = new JdbcInventory(database, SHORT_HOLD);
    int seat = nextSeat++;
    UUID hold = UUID.randomUUID();
    inventory.hold(hold, "A-" + seat, seat);

    Awaitility.await().atMost(WAIT).until(() -> isPastItsEnd(hold));
    inventory.expireDue();

    assertThat(inventory.holdState(hold)).isEqualTo("EXPIRED");
    assertThat(inventory.hold(UUID.randomUUID(), "A-" + seat, seat + 1))
        .isInstanceOf(StepReply.Succeeded.class);
  }

  @Test
  void aRepeatedLockAndARepeatedReleaseAnswerAlreadyDone() throws Exception {
    JdbcInventory inventory = new JdbcInventory(database, Duration.ofMinutes(5));
    int seat = nextSeat++;
    UUID hold = UUID.randomUUID();
    UUID saga = UUID.randomUUID();
    inventory.hold(hold, "A-" + seat, seat);

    inventory.lockForPayment(hold, saga);
    StepReply secondLock = inventory.lockForPayment(hold, saga);
    StepReply otherSaga = inventory.lockForPayment(hold, UUID.randomUUID());
    inventory.release(hold);
    StepReply secondRelease = inventory.release(hold);

    assertThat(secondLock).isInstanceOf(StepReply.AlreadyDone.class);
    assertThat(otherSaga).isInstanceOf(StepReply.Refused.class);
    assertThat(secondRelease).isInstanceOf(StepReply.AlreadyDone.class);
    assertThat(inventory.holdState(hold)).isEqualTo("RELEASED");
  }

  @Test
  void theDatabaseRefusesAHoldChangeCommittedWithoutItsEvent() throws Exception {
    JdbcInventory inventory = new JdbcInventory(database, Duration.ofMinutes(5));
    int seat = nextSeat++;
    UUID hold = UUID.randomUUID();
    inventory.hold(hold, "A-" + seat, seat);

    assertThatThrownBy(() -> releaseWithoutEvent(hold))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("changed without its outbox row");
    assertThat(inventory.holdState(hold)).isEqualTo("ACTIVE");
  }

  private static void releaseWithoutEvent(UUID hold) throws SQLException {
    try (Connection transaction = database.getConnection()) {
      transaction.setAutoCommit(false);
      try (PreparedStatement update =
          transaction.prepareStatement(
              "UPDATE holds SET state = 'RELEASED', ended_at = now(), version = version + 1"
                  + " WHERE hold_id = ?")) {
        update.setObject(1, hold);
        update.executeUpdate();
      }
      transaction.commit();
    }
  }

  private static boolean isPastItsEnd(UUID hold) throws SQLException {
    try (Connection connection = database.getConnection();
        PreparedStatement select =
            connection.prepareStatement(
                "SELECT expires_at <= now() FROM holds WHERE hold_id = ?")) {
      select.setObject(1, hold);
      try (var rows = select.executeQuery()) {
        return rows.next() && rows.getBoolean(1);
      }
    }
  }
}

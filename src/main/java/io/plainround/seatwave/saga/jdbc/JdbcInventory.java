package io.plainround.seatwave.saga.jdbc;

import io.plainround.seatwave.saga.StepReply;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;

/**
 * The inventory service on its own PostgreSQL: Module 13's holds with the saga's semantic lock,
 * every change with its outbox row in the same transaction. The rules live in the database where no
 * code path can bypass them: {@code one_claim_per_seat} counts ACTIVE, CONFIRMING and CONFIRMED,
 * and the deferred trigger {@code hold_change_has_event} refuses a commit without its event. Every
 * command is one conditional {@code UPDATE}, so a repeated command finds the work done and answers
 * {@code AlreadyDone}. Times come from the database's clock, {@code now()}.
 *
 * <p>Thread-safe: no state but the data source; each call is its own transaction.
 */
public final class JdbcInventory {

  /** The lab's one event (db/inventory/V4__lab_catalog.sql). */
  public static final long EVENT_ID = 1;

  private static final String CHECK_VIOLATION = "23514";
  private static final String FOREIGN_KEY_VIOLATION = "23503";

  private final DataSource database;
  private final Duration holdDuration;

  /** A service whose holds last {@code holdDuration} (Module 2's 300 s; a drill may shorten it). */
  public JdbcInventory(DataSource database, Duration holdDuration) {
    this.database = Objects.requireNonNull(database, "database");
    this.holdDuration = Objects.requireNonNull(holdDuration, "holdDuration");
  }

  /**
   * Holds a free seat for one customer. Refused when another hold or booking claims the seat, the
   * seat or customer does not exist, or the customer has four active holds (Module 13's limit).
   * Repeating a hold id that exists answers {@code AlreadyDone}.
   */
  public StepReply hold(UUID holdId, String seatLabel, long customerId) throws SQLException {
    Objects.requireNonNull(holdId, "holdId");
    Objects.requireNonNull(seatLabel, "seatLabel");
    try {
      return Transactions.inTransaction(
          database, transaction -> holdIn(transaction, holdId, seatLabel, customerId));
    } catch (SQLException refused) {
      String state = Objects.requireNonNullElse(refused.getSQLState(), "");
      if (state.equals(CHECK_VIOLATION) || state.equals(FOREIGN_KEY_VIOLATION)) {
        return new StepReply.Refused("hold refused: " + firstLine(refused));
      }
      throw refused;
    }
  }

  /** Saga step 1: ACTIVE and not past its end becomes CONFIRMING for this saga. */
  public StepReply lockForPayment(UUID holdId, UUID sagaId) throws SQLException {
    String lock =
        """
        UPDATE holds SET state = 'CONFIRMING', saga_id = ?, version = version + 1
        WHERE hold_id = ? AND state = 'ACTIVE' AND expires_at > now()
        RETURNING seat_label, version
        """;
    return Transactions.inTransaction(
        database,
        transaction -> {
          Optional<StepReply> changed =
              change(transaction, lock, holdId, sagaId, "SeatLocked", "CONFIRMING");
          if (changed.isPresent()) {
            return changed.get();
          }
          HoldRow hold = read(transaction, holdId);
          if (hold.state().equals("CONFIRMING") && sagaId.toString().equals(hold.sagaId())) {
            return new StepReply.AlreadyDone("locked by " + sagaId);
          }
          return new StepReply.Refused(
              "hold " + holdId + " is " + hold.describe() + ", not an active hold");
        });
  }

  /** Saga step 1 with no lock: only checks that the hold is ACTIVE and not past its end. */
  public StepReply checkActive(UUID holdId) throws SQLException {
    return Transactions.inTransaction(
        database,
        transaction -> {
          HoldRow hold = read(transaction, holdId);
          return hold.isLiveActive()
              ? new StepReply.Succeeded("hold is ACTIVE (no lock taken)")
              : new StepReply.Refused("hold " + holdId + " is not an active hold");
        });
  }

  /** Saga step 3: CONFIRMING (or, with no lock, a live ACTIVE hold) becomes CONFIRMED. */
  public StepReply confirm(UUID holdId) throws SQLException {
    String confirm =
        """
        UPDATE holds SET state = 'CONFIRMED', ended_at = now(), version = version + 1
        WHERE hold_id = ? AND (state = 'CONFIRMING' OR (state = 'ACTIVE' AND expires_at > now()))
        RETURNING seat_label, version
        """;
    return Transactions.inTransaction(
        database,
        transaction -> {
          Optional<StepReply> changed =
              change(transaction, confirm, holdId, null, "SeatConfirmed", "CONFIRMED");
          if (changed.isPresent()) {
            return changed.get();
          }
          HoldRow hold = read(transaction, holdId);
          return hold.state().equals("CONFIRMED")
              ? new StepReply.AlreadyDone("CONFIRMED")
              : new StepReply.Refused("seat lost: hold " + holdId + " is " + hold.describe());
        });
  }

  /**
   * Compensation of step 1: an ACTIVE or CONFIRMING hold becomes RELEASED. A hold that has ended
   * already (released, expired, or missing) answers {@code AlreadyDone} and changes nothing, so a
   * repeated release can never free a seat that another fan holds by now; a CONFIRMED hold is
   * refused, because selling a seat back is a cancellation, a new saga.
   */
  public StepReply release(UUID holdId) throws SQLException {
    String release =
        """
        UPDATE holds SET state = 'RELEASED', ended_at = now(), version = version + 1
        WHERE hold_id = ? AND state IN ('ACTIVE', 'CONFIRMING')
        RETURNING seat_label, version
        """;
    return Transactions.inTransaction(
        database,
        transaction -> {
          Optional<StepReply> changed =
              change(transaction, release, holdId, null, "SeatReleased", "RELEASED");
          if (changed.isPresent()) {
            return changed.get();
          }
          HoldRow hold = read(transaction, holdId);
          return hold.state().equals("CONFIRMED")
              ? new StepReply.Refused("hold " + holdId + " is CONFIRMED")
              : new StepReply.AlreadyDone("hold already " + hold.state().toLowerCase(Locale.ROOT));
        });
  }

  /**
   * The expiry sweep: every ACTIVE hold past its end becomes EXPIRED, each with its event, in one
   * transaction. CONFIRMING is not touched: that one condition is the semantic lock.
   */
  public int expireDue() throws SQLException {
    String expire =
        """
        UPDATE holds SET state = 'EXPIRED', ended_at = now(), version = version + 1
        WHERE state = 'ACTIVE' AND expires_at <= now()
        RETURNING hold_id, seat_label, version
        """;
    return Transactions.inTransaction(
        database,
        transaction -> {
          int expired = 0;
          try (PreparedStatement update = transaction.prepareStatement(expire);
              ResultSet rows = update.executeQuery()) {
            while (rows.next()) {
              writeEvent(
                  transaction,
                  rows.getString(1),
                  rows.getString(2),
                  rows.getLong(3),
                  "SeatReleased",
                  "EXPIRED");
              expired++;
            }
          }
          return expired;
        });
  }

  /** The state of one hold, or MISSING. */
  public String holdState(UUID holdId) throws SQLException {
    return Transactions.inTransaction(database, transaction -> read(transaction, holdId).state());
  }

  private StepReply holdIn(Connection transaction, UUID holdId, String seatLabel, long customerId)
      throws SQLException {
    // Expiry is lazy, as in Module 13: a new hold on the seat first ends an ACTIVE hold that is
    // due, in the same transaction, with its event.
    String expireSeat =
        """
        UPDATE holds SET state = 'EXPIRED', ended_at = now(), version = version + 1
        WHERE event_id = ? AND seat_label = ? AND state = 'ACTIVE' AND expires_at <= now()
        RETURNING hold_id, version
        """;
    try (PreparedStatement update = transaction.prepareStatement(expireSeat)) {
      update.setLong(1, EVENT_ID);
      update.setString(2, seatLabel);
      try (ResultSet rows = update.executeQuery()) {
        while (rows.next()) {
          writeEvent(
              transaction,
              rows.getString(1),
              seatLabel,
              rows.getLong(2),
              "SeatReleased",
              "EXPIRED");
        }
      }
    }
    String insert =
        """
        INSERT INTO holds (hold_id, event_id, seat_label, customer_id, state, created_at,
          expires_at, version)
        VALUES (?, ?, ?, ?, 'ACTIVE', now(), now() + make_interval(secs => ?), 1)
        ON CONFLICT DO NOTHING
        """;
    try (PreparedStatement statement = transaction.prepareStatement(insert)) {
      statement.setObject(1, holdId);
      statement.setLong(2, EVENT_ID);
      statement.setString(3, seatLabel);
      statement.setLong(4, customerId);
      statement.setDouble(5, holdDuration.toMillis() / (double) Duration.ofSeconds(1).toMillis());
      if (statement.executeUpdate() == 0) {
        // Either this hold id exists (a repeat) or the seat is claimed: the unique indexes
        // decided, not this code.
        HoldRow existing = read(transaction, holdId);
        return existing.state().equals("MISSING")
            ? new StepReply.Refused("seat " + seatLabel + " is claimed")
            : new StepReply.AlreadyDone("hold " + holdId + " is " + existing.state());
      }
    }
    writeEvent(transaction, holdId.toString(), seatLabel, 1, "SeatHeld", "ACTIVE");
    return new StepReply.Succeeded("ACTIVE for " + holdDuration.toSeconds() + " s");
  }

  /** Runs one conditional update; on a change writes its event and answers Succeeded. */
  private Optional<StepReply> change(
      Connection transaction,
      String update,
      UUID holdId,
      UUID sagaId,
      String eventType,
      String newState)
      throws SQLException {
    try (PreparedStatement statement = transaction.prepareStatement(update)) {
      int parameter = 1;
      if (sagaId != null) {
        statement.setObject(parameter++, sagaId);
      }
      statement.setObject(parameter, holdId);
      try (ResultSet rows = statement.executeQuery()) {
        if (!rows.next()) {
          return Optional.empty();
        }
        writeEvent(
            transaction,
            holdId.toString(),
            rows.getString(1),
            rows.getLong(2),
            eventType,
            newState);
        return Optional.of(new StepReply.Succeeded(newState));
      }
    }
  }

  private static void writeEvent(
      Connection transaction,
      String holdId,
      String seatLabel,
      long version,
      String eventType,
      String state)
      throws SQLException {
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("hold_id", holdId);
    payload.put("seat_label", seatLabel);
    payload.put("state", state);
    Outbox.insert(transaction, holdId, version, eventType, payload);
  }

  private record HoldRow(String state, String sagaId, boolean isLiveActive) {
    String describe() {
      return state.equals("ACTIVE") && !isLiveActive ? "ACTIVE past its end" : state;
    }
  }

  private static HoldRow read(Connection transaction, UUID holdId) throws SQLException {
    String select =
        "SELECT state, saga_id::text, state = 'ACTIVE' AND expires_at > now() FROM holds"
            + " WHERE hold_id = ?";
    try (PreparedStatement statement = transaction.prepareStatement(select)) {
      statement.setObject(1, holdId);
      try (ResultSet rows = statement.executeQuery()) {
        if (!rows.next()) {
          return new HoldRow("MISSING", "", false);
        }
        return new HoldRow(
            rows.getString(1),
            Objects.requireNonNullElse(rows.getString(2), ""),
            rows.getBoolean(3));
      }
    }
  }

  private static String firstLine(SQLException failure) {
    String message = Objects.requireNonNullElse(failure.getMessage(), failure.getSQLState());
    int newline = message.indexOf('\n');
    return newline < 0 ? message : message.substring(0, newline);
  }
}

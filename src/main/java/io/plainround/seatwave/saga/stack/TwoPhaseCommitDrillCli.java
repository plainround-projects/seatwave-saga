package io.plainround.seatwave.saga.stack;

import com.zaxxer.hikari.HikariDataSource;
import io.plainround.seatwave.saga.jdbc.Outbox;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Chapter 28.5 on two real PostgreSQL databases, with PostgreSQL's own two-phase commit ({@code
 * PREPARE TRANSACTION}). This program is the coordinator, and its crash is real: {@code prepare}
 * locks seat A-7's row in inventory and inserts the charge's row in payment, prepares both, and
 * exits without deciding (or, with {@code --crash after-first-commit}, after committing payment
 * only). The prepared transactions outlive the coordinator, its connections, and a restart of the
 * database. {@code probe} tries to lock the seat as another transaction would (waiting at most 1 s)
 * and prints what an operator sees in {@code pg_prepared_xacts}; {@code decide} is the coordinator
 * back: it finishes what it finds. {@code booking-db} shows the default, prepared transactions
 * disabled.
 *
 * <p>Usage: {@code TwoPhaseCommitDrillCli prepare --gid G --crash after-votes|after-first-commit},
 * {@code probe --gid G}, {@code decide --gid G --decision commit|rollback}, {@code booking-db}
 */
public final class TwoPhaseCommitDrillCli {

  private static final String SEAT_LOCK =
      "SELECT seat_label FROM seats WHERE event_id = 1 AND seat_label = 'A-7' FOR UPDATE";

  private TwoPhaseCommitDrillCli() {}

  /** Runs one coordinator command. */
  public static void main(String[] arguments) throws Exception {
    StackSettings settings = StackSettings.load();
    if (arguments.length < 1) {
      throw new IllegalArgumentException(
          "usage: TwoPhaseCommitDrillCli prepare|probe|decide|booking-db");
    }
    List<String> rest = List.of(arguments).subList(1, arguments.length);
    try (HikariDataSource inventory =
            settings.openPool("2pc-inventory", settings.databases().inventoryUrl());
        HikariDataSource payment =
            settings.openPool("2pc-payment", settings.databases().paymentUrl())) {
      switch (arguments[0]) {
        case "prepare" -> {
          Arguments options = Arguments.parse(rest, Map.of("gid", "", "crash", "after-votes"));
          prepare(
              inventory,
              payment,
              options.name("gid"),
              options.choice("crash", Set.of("after-votes", "after-first-commit")));
        }
        case "probe" ->
            probe(inventory, payment, Arguments.parse(rest, Map.of("gid", "")).name("gid"));
        case "decide" -> {
          Arguments options = Arguments.parse(rest, Map.of("gid", "", "decision", "rollback"));
          decide(
              inventory,
              payment,
              options.name("gid"),
              options.choice("decision", Set.of("commit", "rollback")));
        }
        case "booking-db" -> bookingDatabase(settings);
        default -> throw new IllegalArgumentException("unknown command " + arguments[0]);
      }
    }
  }

  private static void prepare(
      HikariDataSource inventory, HikariDataSource payment, String gid, String crash)
      throws SQLException {
    try (Connection seat = inventory.getConnection();
        Connection charge = payment.getConnection()) {
      seat.setAutoCommit(false);
      charge.setAutoCommit(false);
      execute(seat, SEAT_LOCK);
      try (PreparedStatement insert =
          charge.prepareStatement(
              "INSERT INTO payments (provider_key, state, amount, currency, version)"
                  + " VALUES (?, 'CAPTURED', 80.00, 'USD', 1)")) {
        insert.setString(1, "seatwave-hold-" + gid);
        insert.executeUpdate();
      }
      Outbox.insert(
          charge,
          "seatwave-hold-" + gid,
          1,
          "PaymentCaptured",
          Map.of("provider_key", "seatwave-hold-" + gid));
      // PREPARE TRANSACTION takes no bind parameter; the gid is safe to splice because Arguments
      // allowed only letters, digits, dots and dashes.
      // Phase 1: each participant forces a prepare record and votes yes; its locks stay held.
      execute(seat, "PREPARE TRANSACTION '" + gid + "-inventory'");
      execute(charge, "PREPARE TRANSACTION '" + gid + "-payment'");
      System.out.println(
          Instant.now() + " both prepared: seat A-7 locked in inventory, charge row in payment");
      if (crash.equals("after-first-commit")) {
        // COMMIT PREPARED runs outside a transaction block, like the coordinator's own session.
        charge.setAutoCommit(true);
        execute(charge, "COMMIT PREPARED '" + gid + "-payment'");
        System.out.println(Instant.now() + " COMMIT sent to payment only: the fan is charged");
      }
    }
    System.out.println(
        Instant.now() + " coordinator exits without a decision for inventory (crash)");
  }

  private static void probe(HikariDataSource inventory, HikariDataSource payment, String gid)
      throws SQLException {
    String seat;
    try (Connection other = inventory.getConnection()) {
      other.setAutoCommit(false);
      execute(other, "SET LOCAL lock_timeout = '1s'");
      try {
        execute(other, SEAT_LOCK);
        seat = "free: another transaction locked it at once";
      } catch (SQLException blocked) {
        seat = "LOCKED: " + blocked.getSQLState() + " after 1 s (" + firstLine(blocked) + ")";
      }
      other.rollback();
    }
    System.out.println(Instant.now() + " seat A-7: " + seat);
    for (HikariDataSource database : List.of(inventory, payment)) {
      try (Connection connection = database.getConnection();
          PreparedStatement statement =
              connection.prepareStatement(
                  "SELECT gid, prepared, now() - prepared, (SELECT count(*) FROM pg_locks l"
                      + " WHERE l.pid IS NULL AND l.granted) FROM pg_prepared_xacts p"
                      + " WHERE gid LIKE ? ORDER BY gid")) {
        statement.setString(1, gid + "-%");
        try (ResultSet rows = statement.executeQuery()) {
          boolean isAny = false;
          while (rows.next()) {
            isAny = true;
            System.out.println(
                String.format(
                    Locale.ROOT,
                    "  pg_prepared_xacts: gid %s, prepared %s, age %s; locks held by no session %d",
                    rows.getString(1),
                    rows.getTimestamp(2).toInstant(),
                    rows.getString(3),
                    rows.getLong(4)));
          }
          if (!isAny) {
            System.out.println(
                "  pg_prepared_xacts: no row for "
                    + gid
                    + " in "
                    + connection.getCatalog()
                    + " at "
                    + database.getJdbcUrl());
          }
        }
      }
    }
  }

  private static void decide(
      HikariDataSource inventory, HikariDataSource payment, String gid, String decision)
      throws SQLException {
    String verb = decision.equals("commit") ? "COMMIT PREPARED" : "ROLLBACK PREPARED";
    for (Map.Entry<String, HikariDataSource> participant :
        Map.of("inventory", inventory, "payment", payment).entrySet()) {
      String prepared = gid + "-" + participant.getKey();
      try (Connection connection = participant.getValue().getConnection()) {
        if (isPrepared(connection, prepared)) {
          execute(connection, verb + " '" + prepared + "'");
          System.out.println(Instant.now() + " " + verb + " " + prepared);
        } else {
          System.out.println(
              Instant.now() + " " + prepared + ": nothing prepared (already decided)");
        }
      }
    }
  }

  private static void bookingDatabase(StackSettings settings) throws SQLException {
    try (HikariDataSource booking =
            settings.openPool("2pc-booking", settings.databases().bookingUrl());
        Connection connection = booking.getConnection();
        Statement statement = connection.createStatement();
        ResultSet setting = statement.executeQuery("SHOW max_prepared_transactions")) {
      setting.next();
      System.out.println("booking-db max_prepared_transactions = " + setting.getString(1));
      connection.setAutoCommit(false);
      try {
        execute(connection, "PREPARE TRANSACTION 'refused-here'");
      } catch (SQLException refused) {
        System.out.println(
            "booking-db PREPARE TRANSACTION: " + refused.getSQLState() + " " + firstLine(refused));
      }
      connection.rollback();
    }
  }

  private static boolean isPrepared(Connection connection, String gid) throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement("SELECT 1 FROM pg_prepared_xacts WHERE gid = ?")) {
      statement.setString(1, gid);
      try (ResultSet rows = statement.executeQuery()) {
        return rows.next();
      }
    }
  }

  private static void execute(Connection connection, String sql) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private static String firstLine(SQLException failure) {
    String message = String.valueOf(failure.getMessage());
    int newline = message.indexOf('\n');
    return newline < 0 ? message : message.substring(0, newline);
  }
}

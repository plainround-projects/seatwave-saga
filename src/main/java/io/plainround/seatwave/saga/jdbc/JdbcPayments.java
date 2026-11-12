package io.plainround.seatwave.saga.jdbc;

import io.plainround.seatwave.saga.StepReply;
import io.plainround.seatwave.saga.stack.ProviderClient;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import javax.sql.DataSource;

/**
 * The payment service on its own PostgreSQL, in front of the provider: one row of {@code payments}
 * per provider key, written with its outbox row after the provider answered. The call to the
 * provider happens outside any transaction (code guidelines, rule 69); the provider's own
 * idempotency key makes a repeated call after a crash or a timeout return the first result, so both
 * commands are idempotent end to end. The row is written with {@code ON CONFLICT DO NOTHING}, so
 * two services that both heard "captured" record it once.
 *
 * <p>Thread-safe: no state but the data source and the shared client.
 */
public final class JdbcPayments {

  private static final String CURRENCY = "USD";

  private final DataSource database;
  private final ProviderClient provider;
  private final BigDecimal seatPrice;

  /** A service that charges {@code seatPrice} per booking through {@code provider}. */
  public JdbcPayments(DataSource database, ProviderClient provider, BigDecimal seatPrice) {
    this.database = Objects.requireNonNull(database, "database");
    this.provider = Objects.requireNonNull(provider, "provider");
    this.seatPrice = Objects.requireNonNull(seatPrice, "seatPrice");
  }

  /** Saga step 2, the pivot: charges once per provider key, whatever the number of calls. */
  public StepReply charge(String providerKey) throws SQLException, InterruptedException {
    String state = state(providerKey);
    if (state.equals("CAPTURED") || state.equals("REFUNDED")) {
      return new StepReply.AlreadyDone("captured earlier under " + providerKey);
    }
    if (state.equals("DECLINED")) {
      return new StepReply.Refused("card declined");
    }
    return switch (provider.charge(providerKey, seatPrice)) {
      case CAPTURED -> {
        record(providerKey, "CAPTURED", "PaymentCaptured");
        yield new StepReply.Succeeded("captured " + seatPrice.toPlainString());
      }
      case DECLINED -> {
        record(providerKey, "DECLINED", "PaymentFailed");
        yield new StepReply.Refused("card declined");
      }
      case REFUNDED, NOTHING_TO_REFUND, NO_ANSWER ->
          new StepReply.Unavailable("no answer from the provider, outcome unknown");
    };
  }

  /** Compensation of the charge: refunds a capture once; nothing to do for no capture. */
  public StepReply refund(String providerKey) throws SQLException, InterruptedException {
    String state = state(providerKey);
    if (state.equals("REFUNDED")) {
      return new StepReply.AlreadyDone("refunded earlier");
    }
    if (!state.equals("CAPTURED")) {
      return new StepReply.AlreadyDone("nothing captured, nothing to refund");
    }
    return switch (provider.refund(providerKey)) {
      case REFUNDED -> {
        record(providerKey, "REFUNDED", "PaymentRefunded");
        yield new StepReply.Succeeded("refunded " + seatPrice.toPlainString());
      }
      case NOTHING_TO_REFUND -> new StepReply.Refused("the provider has no capture to refund");
      case CAPTURED, DECLINED, NO_ANSWER ->
          new StepReply.Unavailable("no answer from the provider, outcome unknown");
    };
  }

  /** The payment row's state for one key, or NONE. */
  public String state(String providerKey) throws SQLException {
    return Transactions.inTransaction(
        database,
        transaction -> {
          try (PreparedStatement select =
              transaction.prepareStatement("SELECT state FROM payments WHERE provider_key = ?")) {
            select.setString(1, providerKey);
            try (ResultSet rows = select.executeQuery()) {
              return rows.next() ? rows.getString(1) : "NONE";
            }
          }
        });
  }

  /**
   * Writes the provider's answer and its event in one transaction. A new key is inserted at version
   * 1; a capture becomes a refund at version 2 only from CAPTURED.
   */
  private void record(String providerKey, String state, String eventType) throws SQLException {
    Transactions.inTransaction(
        database,
        transaction -> {
          long version = write(transaction, providerKey, state);
          if (version > 0) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("provider_key", providerKey);
            payload.put("amount", seatPrice.toPlainString());
            payload.put("currency", CURRENCY);
            payload.put("state", state);
            Outbox.insert(transaction, providerKey, version, eventType, payload);
          }
          return version;
        });
  }

  /** The new version, or 0 when another call recorded the same answer first. */
  private long write(Connection transaction, String providerKey, String state) throws SQLException {
    String sql =
        state.equals("REFUNDED")
            ? "UPDATE payments SET state = 'REFUNDED', version = version + 1, updated_at = now()"
                + " WHERE provider_key = ? AND state = 'CAPTURED' RETURNING version"
            : "INSERT INTO payments (provider_key, state, amount, currency, version)"
                + " VALUES (?, ?, ?, ?, 1) ON CONFLICT DO NOTHING RETURNING version";
    try (PreparedStatement statement = transaction.prepareStatement(sql)) {
      statement.setString(1, providerKey);
      if (!state.equals("REFUNDED")) {
        statement.setString(2, state);
        statement.setBigDecimal(3, seatPrice);
        statement.setString(4, CURRENCY);
      }
      try (ResultSet rows = statement.executeQuery()) {
        return rows.next() ? rows.getLong(1) : 0;
      }
    }
  }
}

package io.plainround.seatwave.saga;

import java.util.Objects;

/**
 * One row of {@code booking_sagas}: the saga's state, how often its current command was retried,
 * and when it last moved. The saga's id is the booking's id: the saga is how a booking comes to
 * exist.
 *
 * @param bookingId the saga's and the booking's id
 * @param holdId the hold the booking is for (one saga per hold)
 * @param providerKey Module 7's {@code seatwave-hold-<holdId>}, the key of the charge and refund
 * @param state where the saga is
 * @param attempts how many times the current state's command was sent without an answer
 * @param updatedAtMillis the last transition; a retry does not move it
 */
public record SagaRecord(
    String bookingId,
    String holdId,
    String providerKey,
    BookingSaga.State state,
    int attempts,
    long updatedAtMillis) {

  /** Module 7's provider key is made from the hold id. */
  public static final String PROVIDER_KEY_PREFIX = "seatwave-hold-";

  /** Rejects a missing field or a negative attempt count. */
  public SagaRecord {
    Objects.requireNonNull(bookingId, "bookingId");
    Objects.requireNonNull(holdId, "holdId");
    Objects.requireNonNull(providerKey, "providerKey");
    Objects.requireNonNull(state, "state");
    if (attempts < 0) {
      throw new IllegalArgumentException("attempts must not be negative: " + attempts);
    }
  }

  SagaRecord moved(BookingSaga.State next, long nowMillis) {
    return new SagaRecord(bookingId, holdId, providerKey, next, 0, nowMillis);
  }

  SagaRecord retried() {
    return new SagaRecord(
        bookingId, holdId, providerKey, state, Math.addExact(attempts, 1), updatedAtMillis);
  }
}

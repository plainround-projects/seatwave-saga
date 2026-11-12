package io.plainround.seatwave.saga;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Three services, each with its own database and outbox, and the orchestrator, wired the same way
 * for every program, test and lab: the booking service (orchestrator and {@code booking_sagas}),
 * inventory (holds and seats) and payment (charges and refunds). Time is virtual milliseconds
 * passed in by the caller.
 */
record SagaWorld(
    ServiceDatabase bookingDatabase,
    ServiceDatabase inventoryDatabase,
    ServiceDatabase paymentDatabase,
    InventoryService inventory,
    PaymentService payment,
    BookingSaga saga) {

  /** Module 2's hold: 300 seconds. */
  static final long HOLD_MILLIS = 300_000;

  /** Module 2's average ticket. */
  static final BigDecimal SEAT_PRICE = new BigDecimal("80.00");

  /**
   * First retry after 1 s, doubling, capped at 60 s (the saga's own backoff, Module 12's shape).
   */
  static final long FIRST_RETRY_MILLIS = 1_000;

  static final long MAX_RETRY_MILLIS = 60_000;

  /** A bound on the steps of one saga, far above the six a saga can take without retries. */
  private static final int MAX_STEPS = 50;

  SagaWorld {
    Objects.requireNonNull(saga, "saga");
  }

  /** A fresh world: empty databases, a provider that captures unless told otherwise. */
  static SagaWorld create(BookingSaga.SeatProtection protection) {
    ServiceDatabase bookingDatabase = new ServiceDatabase("booking");
    ServiceDatabase inventoryDatabase = new ServiceDatabase("inventory");
    ServiceDatabase paymentDatabase = new ServiceDatabase("payment");
    InventoryService inventory = new InventoryService(inventoryDatabase, HOLD_MILLIS);
    PaymentService payment = new PaymentService(paymentDatabase, SEAT_PRICE);
    BookingSaga saga =
        new BookingSaga(
            new ServiceDatabaseSagaStore(bookingDatabase),
            new LocalParticipants(inventory, payment),
            new BookingSaga.Settings(protection, FIRST_RETRY_MILLIS, MAX_RETRY_MILLIS));
    return new SagaWorld(
        bookingDatabase, inventoryDatabase, paymentDatabase, inventory, payment, saga);
  }

  /** Steps one saga until it is final, each step at the time the last one allowed. */
  long runToEnd(String sagaId, long startMillis) {
    long nowMillis = startMillis;
    for (int stepIndex = 0; stepIndex < MAX_STEPS && !saga.state(sagaId).isFinal(); stepIndex++) {
      nowMillis = saga.step(sagaId, nowMillis).retryAtMillis();
    }
    return nowMillis;
  }

  /** The end state of one saga, as the invariant judges it. */
  record Outcome(BookingSaga.State state, BigDecimal netCharged, String holdState) {
    /** Seat confirmed and paid, or seat not held by this saga and nothing kept from the fan. */
    boolean isCorrect() {
      return switch (state) {
        case COMPLETED -> holdState.equals("CONFIRMED") && netCharged.compareTo(SEAT_PRICE) == 0;
        case COMPENSATED ->
            !holdState.equals("CONFIRMING")
                && !holdState.equals("CONFIRMED")
                && netCharged.signum() == 0;
        case SEAT_HELD,
            PAYMENT_REQUESTED,
            PAYMENT_SUCCEEDED,
            PAYMENT_FAILED,
            REFUND_REQUESTED,
            PAYMENT_REFUNDED ->
            false;
      };
    }
  }

  Outcome outcome(String sagaId, String holdId) {
    return new Outcome(
        saga.state(sagaId),
        payment.netCharged("seatwave-hold-" + holdId),
        inventory.holdState(holdId));
  }
}

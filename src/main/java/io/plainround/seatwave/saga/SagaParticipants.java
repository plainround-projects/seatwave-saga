package io.plainround.seatwave.saga;

/**
 * The six commands the booking saga sends, to inventory and to payment. Every command is idempotent
 * under the key it names (the hold id with the saga id, or Module 7's provider key), so the
 * orchestrator may send any of them again after a crash and get the first answer back.
 *
 * <p>Two implementations: {@link LocalParticipants}, the in-memory services every program and
 * {@code ./gradlew check} use, and {@code stack.HttpParticipants}, which calls the inventory and
 * payment services of the Compose stack over HTTP. Times are the caller's clock in milliseconds;
 * the services of the stack read their database's clock instead and ignore it.
 */
public interface SagaParticipants {

  /** Step 1: hold ACTIVE becomes CONFIRMING for this saga (the semantic lock). */
  StepReply lockSeat(String holdId, String sagaId, long nowMillis);

  /** Step 1 without a lock: only checks that the hold is ACTIVE and not past its end. */
  StepReply checkSeatActive(String holdId, long nowMillis);

  /** Step 2, the pivot: charges once per provider key. */
  StepReply chargePayment(String providerKey, long nowMillis);

  /** Step 3: the locked (or still live) hold becomes CONFIRMED. */
  StepReply confirmSeat(String holdId, long nowMillis);

  /** Compensation of step 1: frees the seat of a hold that is still claimed. */
  StepReply releaseSeat(String holdId, long nowMillis);

  /** Compensation of step 2: refunds a capture once. */
  StepReply refundPayment(String providerKey, long nowMillis);
}

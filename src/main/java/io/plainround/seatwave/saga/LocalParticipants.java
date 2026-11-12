package io.plainround.seatwave.saga;

import java.util.Objects;

/** The saga's commands answered by the in-memory services of {@link SagaWorld}. Not thread-safe. */
final class LocalParticipants implements SagaParticipants {

  private final InventoryService inventory;
  private final PaymentService payment;

  LocalParticipants(InventoryService inventory, PaymentService payment) {
    this.inventory = Objects.requireNonNull(inventory, "inventory");
    this.payment = Objects.requireNonNull(payment, "payment");
  }

  @Override
  public StepReply lockSeat(String holdId, String sagaId, long nowMillis) {
    return inventory.lockForPayment(holdId, sagaId, nowMillis);
  }

  @Override
  public StepReply checkSeatActive(String holdId, long nowMillis) {
    return inventory.checkActive(holdId, nowMillis);
  }

  @Override
  public StepReply chargePayment(String providerKey, long nowMillis) {
    return payment.charge(providerKey, nowMillis);
  }

  @Override
  public StepReply confirmSeat(String holdId, long nowMillis) {
    return inventory.confirm(holdId, nowMillis);
  }

  @Override
  public StepReply releaseSeat(String holdId, long nowMillis) {
    return inventory.release(holdId, nowMillis);
  }

  @Override
  public StepReply refundPayment(String providerKey, long nowMillis) {
    return payment.refund(providerKey, nowMillis);
  }
}

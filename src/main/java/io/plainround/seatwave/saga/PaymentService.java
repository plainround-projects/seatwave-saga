package io.plainround.seatwave.saga;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The payment service in front of a stand-in provider, with its own database and outbox. A charge
 * is keyed by Module 7's provider key {@code seatwave-hold-<holdId>}, so a repeated charge returns
 * the first one; a refund is keyed by the same charge, so a repeated refund returns the first
 * refund. Both commands are therefore idempotent, which is what lets the orchestrator repeat any
 * command after a crash.
 *
 * <p>The stand-in can be told, per provider key, to decline, or to capture and lose the answer (a
 * timeout after the money moved, Module 7's case), and to fail the next refunds with a 503. Not
 * thread-safe.
 */
final class PaymentService {

  private static final String PAYMENTS = "payments";

  private final ServiceDatabase database;
  private final BigDecimal seatPrice;
  private final Map<String, Behavior> behaviors = new HashMap<>();
  private final Map<String, Integer> captureCount = new HashMap<>();
  private final Map<String, Integer> refundCount = new HashMap<>();
  private int refundFailuresLeft;

  /** What the stand-in provider does with a charge. */
  enum Behavior {
    CAPTURE,
    DECLINE,
    CAPTURE_THEN_TIMEOUT
  }

  /** A service that charges {@code seatPrice} (Module 2's average ticket, $80.00) per booking. */
  PaymentService(ServiceDatabase database, BigDecimal seatPrice) {
    this.database = Objects.requireNonNull(database, "database");
    this.seatPrice = Objects.requireNonNull(seatPrice, "seatPrice");
  }

  /** Scripts the provider's answer for one provider key; CAPTURE when nothing is scripted. */
  void script(String providerKey, Behavior behavior) {
    behaviors.put(providerKey, behavior);
  }

  /** The next {@code count} refund calls fail with 503 before reaching the provider. */
  void failNextRefunds(int count) {
    refundFailuresLeft = count;
  }

  /** Saga step 2, the pivot: charges once per provider key, whatever the number of calls. */
  StepReply charge(String providerKey, long nowMillis) {
    String state = state(providerKey);
    if (state.equals("CAPTURED") || state.equals("REFUNDED")) {
      return new StepReply.AlreadyDone("captured earlier under " + providerKey);
    }
    if (state.equals("DECLINED")) {
      return new StepReply.Refused("card declined");
    }
    Behavior behavior = behaviors.getOrDefault(providerKey, Behavior.CAPTURE);
    if (behavior == Behavior.DECLINE) {
      record(providerKey, "DECLINED", "PaymentFailed", nowMillis);
      return new StepReply.Refused("card declined");
    }
    captureCount.merge(providerKey, 1, Integer::sum);
    record(providerKey, "CAPTURED", "PaymentCaptured", nowMillis);
    if (behavior == Behavior.CAPTURE_THEN_TIMEOUT) {
      // The money moved, but the answer is lost: the caller sees a timeout and must ask again.
      behaviors.put(providerKey, Behavior.CAPTURE);
      return new StepReply.Unavailable("timeout, outcome unknown");
    }
    return new StepReply.Succeeded("captured " + seatPrice.toPlainString());
  }

  /** Compensation of the charge: refunds a capture once; nothing to do for no capture. */
  StepReply refund(String providerKey, long nowMillis) {
    if (refundFailuresLeft > 0) {
      refundFailuresLeft--;
      return new StepReply.Unavailable("provider answered 503");
    }
    String state = state(providerKey);
    if (state.equals("REFUNDED")) {
      return new StepReply.AlreadyDone("refunded earlier");
    }
    if (!state.equals("CAPTURED")) {
      return new StepReply.AlreadyDone("nothing captured, nothing to refund");
    }
    refundCount.merge(providerKey, 1, Integer::sum);
    record(providerKey, "REFUNDED", "PaymentRefunded", nowMillis);
    return new StepReply.Succeeded("refunded " + seatPrice.toPlainString());
  }

  /** Money kept from the fan for this key: price times (captures minus refunds). */
  BigDecimal netCharged(String providerKey) {
    int kept = captureCount.getOrDefault(providerKey, 0) - refundCount.getOrDefault(providerKey, 0);
    return seatPrice.multiply(BigDecimal.valueOf(kept));
  }

  /** How many captures and refunds the provider made for this key, for the idempotency check. */
  String ledger(String providerKey) {
    return captureCount.getOrDefault(providerKey, 0)
        + " capture(s), "
        + refundCount.getOrDefault(providerKey, 0)
        + " refund(s)";
  }

  private String state(String providerKey) {
    return database.read(PAYMENTS, providerKey).map(row -> row.get("state")).orElse("NONE");
  }

  private void record(String providerKey, String state, String eventType, long nowMillis) {
    Map<String, String> event = new LinkedHashMap<>();
    event.put("provider_key", providerKey);
    event.put("amount", seatPrice.toPlainString());
    event.put("state", state);
    database.commit(
        database
            .begin()
            .put(PAYMENTS, providerKey, Map.of("state", state))
            .addEvent(providerKey, eventType, event),
        nowMillis);
  }
}

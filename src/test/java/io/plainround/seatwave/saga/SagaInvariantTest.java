package io.plainround.seatwave.saga;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The module's one invariant: every saga ends in exactly one of two states (seat confirmed and
 * paid, or seat released and not charged or refunded), and every state change has its event in the
 * outbox. Failure injection: every payment behavior, with and without the semantic lock, a refund
 * that fails twice, and an orchestrator crash before recording at every step.
 */
class SagaInvariantTest {

  private static final long CONFIRM_AT_MILLIS = 10_000;
  private static final long LATE_CONFIRM_AT_MILLIS = 299_000;
  private static final long SECOND_FAN_AT_MILLIS = 300_500;
  private static final long RESUME_AFTER_MILLIS = 30_000;
  private static final int MAX_CRASH_STEP = 8;
  private static final int REFUND_FAILURES = 2;

  @Test
  void everySagaEndsConfirmedAndPaidOrReleasedAndNotChargedWhateverCrashes() {
    List<String> wrongEnds = new ArrayList<>();
    int sagasRun = 0;
    for (BookingSaga.SeatProtection protection : BookingSaga.SeatProtection.values()) {
      for (PaymentService.Behavior behavior : PaymentService.Behavior.values()) {
        for (boolean isLate : new boolean[] {false, true}) {
          for (int crashStep = 0; crashStep < MAX_CRASH_STEP; crashStep++) {
            SagaWorld world = SagaWorld.create(protection);
            SagaWorld.Outcome outcome = runWithCrash(world, behavior, isLate, crashStep);
            sagasRun++;
            if (!outcome.isCorrect()) {
              wrongEnds.add(
                  protection
                      + "/"
                      + behavior
                      + "/late="
                      + isLate
                      + "/crash="
                      + crashStep
                      + ": "
                      + outcome);
            }
          }
        }
      }
    }

    assertThat(sagasRun).isEqualTo(96);
    assertThat(wrongEnds).isEmpty();
  }

  @Test
  void refusesAStateChangeCommittedWithoutItsEvent() {
    ServiceDatabase inventoryDatabase = new ServiceDatabase("inventory");

    ServiceDatabase.Transaction withoutEvent =
        inventoryDatabase.begin().put("holds", "h-1", Map.of("state", "RELEASED"));

    assertThatThrownBy(() -> inventoryDatabase.commit(withoutEvent, 0))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("without its event");
    assertThat(inventoryDatabase.read("holds", "h-1")).isEmpty();
  }

  @Test
  void aRepeatedRefundRefundsOnce() {
    SagaWorld world = SagaWorld.create(BookingSaga.SeatProtection.SEMANTIC_LOCK);
    world.payment().charge("seatwave-hold-h-1", 0);

    world.payment().refund("seatwave-hold-h-1", 0);
    StepReply second = world.payment().refund("seatwave-hold-h-1", 0);

    assertThat(second).isInstanceOf(StepReply.AlreadyDone.class);
    assertThat(world.payment().netCharged("seatwave-hold-h-1").signum()).isZero();
  }

  @Test
  void aSeatBeingPaidForIsNotFreedByExpiryNorHeldByAnotherFan() {
    SagaWorld world = SagaWorld.create(BookingSaga.SeatProtection.SEMANTIC_LOCK);
    world.inventory().hold(new InventoryService.HoldRequest("h-1", "A-7", "1000"), 0);
    world.saga().start("b-1004", "h-1", LATE_CONFIRM_AT_MILLIS);
    world.saga().step("b-1004", LATE_CONFIRM_AT_MILLIS);

    int expired = world.inventory().expireDue(SagaWorld.HOLD_MILLIS);
    StepReply secondFan =
        world
            .inventory()
            .hold(new InventoryService.HoldRequest("h-2", "A-7", "2000"), SECOND_FAN_AT_MILLIS);

    assertThat(expired).isZero();
    assertThat(secondFan).isInstanceOf(StepReply.Refused.class);
    assertThat(world.inventory().holdState("h-1")).isEqualTo("CONFIRMING");
  }

  /**
   * One saga for hold h-1 on seat A-7, crashed once before recording at step {@code crashStep} (if
   * it gets that far) and resumed 30 s later. A late saga confirms at 299 s; the hold expires at
   * 300 s and a second fan asks for the seat at 300.5 s, and its refunds fail twice.
   */
  private static SagaWorld.Outcome runWithCrash(
      SagaWorld world, PaymentService.Behavior behavior, boolean isLate, int crashStep) {
    world.inventory().hold(new InventoryService.HoldRequest("h-1", "A-7", "1000"), 0);
    world.payment().script("seatwave-hold-h-1", behavior);
    world.payment().failNextRefunds(REFUND_FAILURES);
    long nowMillis = isLate ? LATE_CONFIRM_AT_MILLIS : CONFIRM_AT_MILLIS;
    world.saga().start("b-1004", "h-1", nowMillis);
    for (int stepIndex = 0; stepIndex <= crashStep; stepIndex++) {
      if (world.saga().state("b-1004").isFinal()) {
        break;
      }
      if (stepIndex == crashStep) {
        world.saga().stepAndCrashBeforeRecording("b-1004", nowMillis);
        nowMillis += RESUME_AFTER_MILLIS;
        break;
      }
      nowMillis = world.saga().step("b-1004", nowMillis).retryAtMillis();
      if (isLate && stepIndex == 0) {
        world.inventory().expireDue(SagaWorld.HOLD_MILLIS);
        world
            .inventory()
            .hold(new InventoryService.HoldRequest("h-2", "A-7", "2000"), SECOND_FAN_AT_MILLIS);
        nowMillis = Math.max(nowMillis, SECOND_FAN_AT_MILLIS);
      }
    }
    world.runToEnd("b-1004", nowMillis);
    return world.outcome("b-1004", "h-1");
  }
}

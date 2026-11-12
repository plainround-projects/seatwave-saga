package io.plainround.seatwave.saga;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** A compensation repeated, or arriving late, must change nothing it does not own. */
class CompensationTest {

  private static final long AFTER_EXPIRY_MILLIS = SagaWorld.HOLD_MILLIS + 1;

  @Test
  void aReleaseOfAnExpiredHoldDoesNotFreeTheSeatAnotherFanHoldsNow() {
    SagaWorld world = SagaWorld.create(BookingSaga.SeatProtection.NONE);
    world.inventory().hold(new InventoryService.HoldRequest("h-1", "A-7", "1000"), 0);
    world.inventory().expireDue(AFTER_EXPIRY_MILLIS);
    world
        .inventory()
        .hold(new InventoryService.HoldRequest("h-2", "A-7", "2000"), AFTER_EXPIRY_MILLIS);

    StepReply release = world.inventory().release("h-1", AFTER_EXPIRY_MILLIS);

    assertThat(release).isInstanceOf(StepReply.AlreadyDone.class);
    assertThat(world.inventory().seatState("A-7")).isEqualTo("ACTIVE by h-2");
  }

  @Test
  void aRepeatedReleaseAnswersAlreadyDoneAndWritesNoSecondEvent() {
    SagaWorld world = SagaWorld.create(BookingSaga.SeatProtection.SEMANTIC_LOCK);
    world.inventory().hold(new InventoryService.HoldRequest("h-1", "A-7", "1000"), 0);
    world.inventory().release("h-1", 1);
    int rowsAfterFirst = world.inventoryDatabase().outbox().size();

    StepReply second = world.inventory().release("h-1", 2);

    assertThat(second).isInstanceOf(StepReply.AlreadyDone.class);
    assertThat(world.inventoryDatabase().outbox()).hasSize(rowsAfterFirst);
  }

  @Test
  void aSoldSeatIsNeverReleasedByACompensation() {
    SagaWorld world = SagaWorld.create(BookingSaga.SeatProtection.SEMANTIC_LOCK);
    world.inventory().hold(new InventoryService.HoldRequest("h-1", "A-7", "1000"), 0);
    world.inventory().confirm("h-1", 1);

    StepReply release = world.inventory().release("h-1", 2);

    assertThat(release).isInstanceOf(StepReply.Refused.class);
    assertThat(world.inventory().holdState("h-1")).isEqualTo("CONFIRMED");
  }
}

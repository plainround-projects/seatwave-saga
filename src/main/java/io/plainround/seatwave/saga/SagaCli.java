package io.plainround.seatwave.saga;

import java.util.Locale;
import java.util.Map;

/**
 * Chapter 28.6 made visible: the orchestrated booking saga, printed step by step, for a success, a
 * declined card, a lost payment answer, the walkthrough's late confirm with and without the
 * semantic lock (with a refund that fails twice), a crash before recording at every step, and a
 * stuck saga found by its age. Virtual time; payment answers 2,000 ms after the request (Section
 * 7's p99), every other participant 5 ms (an assumption for in-region calls).
 */
final class SagaCli {

  private static final long PAYMENT_MILLIS = 2_000;
  private static final long LOCAL_CALL_MILLIS = 5;
  private static final long HOLD_AT_MILLIS = 0;
  private static final long EARLY_CONFIRM_MILLIS = 10_000;
  private static final long LATE_CONFIRM_MILLIS = 299_000;
  private static final long SECOND_FAN_MILLIS = 300_500;
  private static final long ORCHESTRATOR_DOWN_UNTIL_MILLIS = 400_000;
  private static final int REFUND_FAILURES = 2;
  private static final double MILLIS_PER_SECOND = 1_000.0;
  private static final long RESUME_DELAY_MILLIS = 30_000;

  /** More steps than any saga of Part 6 takes, so every step gets its crash. */
  private static final int MAX_CRASH_STEP = 6;

  private SagaCli() {}

  public static void main(String[] arguments) {
    System.out.println("Part 1. Success: hold at 0 s, confirm at 10 s");
    SagaWorld world = startedSaga(BookingSaga.SeatProtection.SEMANTIC_LOCK, EARLY_CONFIRM_MILLIS);
    runPrinting(world, EARLY_CONFIRM_MILLIS);
    printOutboxes(world);

    System.out.println();
    System.out.println("Part 2. Card declined: the compensation releases the seat");
    world = startedSaga(BookingSaga.SeatProtection.SEMANTIC_LOCK, EARLY_CONFIRM_MILLIS);
    world.payment().script("seatwave-hold-h-1", PaymentService.Behavior.DECLINE);
    runPrinting(world, EARLY_CONFIRM_MILLIS);

    System.out.println();
    System.out.println("Part 3. The provider captures but the answer is lost (Module 7's timeout)");
    world = startedSaga(BookingSaga.SeatProtection.SEMANTIC_LOCK, EARLY_CONFIRM_MILLIS);
    world.payment().script("seatwave-hold-h-1", PaymentService.Behavior.CAPTURE_THEN_TIMEOUT);
    runPrinting(world, EARLY_CONFIRM_MILLIS);
    System.out.println("  provider ledger: " + world.payment().ledger("seatwave-hold-h-1"));

    System.out.println();
    System.out.println("Part 4. Confirm at 299 s, hold expires at 300 s, NO semantic lock");
    lateConfirm(BookingSaga.SeatProtection.NONE);

    System.out.println();
    System.out.println("Part 5. The same timing WITH the semantic lock (CONFIRMING)");
    lateConfirm(BookingSaga.SeatProtection.SEMANTIC_LOCK);

    System.out.println();
    System.out.println(
        "Part 6. The orchestrator crashes before recording, at every step; resumed 30 s later");
    crashAtEveryStep();

    System.out.println();
    System.out.println(
        "Part 7. A stuck saga: the orchestrator dies after the charge, before recording it, and"
            + " stays down");
    stuckSaga();
  }

  private static SagaWorld startedSaga(BookingSaga.SeatProtection protection, long confirmMillis) {
    SagaWorld world = SagaWorld.create(protection);
    world.inventory().hold(new InventoryService.HoldRequest("h-1", "A-7", "1000"), HOLD_AT_MILLIS);
    world.saga().start("b-1004", "h-1", confirmMillis);
    return world;
  }

  /** Steps to the end; a payment step takes 2,000 ms, others 5 ms, retries wait their backoff. */
  private static void runPrinting(SagaWorld world, long startMillis) {
    long nowMillis = startMillis;
    while (!world.saga().state("b-1004").isFinal()) {
      BookingSaga.State before = world.saga().state("b-1004");
      long answeredAtMillis = nowMillis + durationOf(before);
      BookingSaga.StepResult result = world.saga().step("b-1004", answeredAtMillis);
      printStep(answeredAtMillis, result);
      nowMillis = result.from() == result.to() ? result.retryAtMillis() : answeredAtMillis;
    }
    printOutcome(world);
  }

  private static void lateConfirm(BookingSaga.SeatProtection protection) {
    SagaWorld world = startedSaga(protection, LATE_CONFIRM_MILLIS);
    world.payment().failNextRefunds(REFUND_FAILURES);
    BookingSaga.StepResult first = world.saga().step("b-1004", LATE_CONFIRM_MILLIS);
    printStep(LATE_CONFIRM_MILLIS, first);
    long expiryMillis = SagaWorld.HOLD_MILLIS;
    int expired = world.inventory().expireDue(expiryMillis);
    System.out.println(
        String.format(
            Locale.ROOT,
            "  %8.3f s  expiry sweep ends %d hold(s); seat A-7 is %s",
            expiryMillis / MILLIS_PER_SECOND,
            expired,
            world.inventory().seatState("A-7")));
    StepReply secondFan =
        world
            .inventory()
            .hold(new InventoryService.HoldRequest("h-2", "A-7", "2000"), SECOND_FAN_MILLIS);
    System.out.println(
        String.format(
            Locale.ROOT,
            "  %8.3f s  fan 2000 asks to hold A-7: %s; seat A-7 is %s",
            SECOND_FAN_MILLIS / MILLIS_PER_SECOND,
            describe(secondFan),
            world.inventory().seatState("A-7")));
    runPrinting(world, LATE_CONFIRM_MILLIS);
    System.out.println("  provider ledger: " + world.payment().ledger("seatwave-hold-h-1"));
  }

  private static void crashAtEveryStep() {
    for (PaymentService.Behavior behavior : PaymentService.Behavior.values()) {
      for (int crashStep = 0; crashStep < MAX_CRASH_STEP; crashStep++) {
        SagaWorld world =
            startedSaga(BookingSaga.SeatProtection.SEMANTIC_LOCK, EARLY_CONFIRM_MILLIS);
        world.payment().script("seatwave-hold-h-1", behavior);
        long nowMillis = EARLY_CONFIRM_MILLIS;
        for (int stepIndex = 0; stepIndex < crashStep; stepIndex++) {
          nowMillis = world.saga().step("b-1004", nowMillis).retryAtMillis();
        }
        BookingSaga.State crashedIn = world.saga().state("b-1004");
        if (crashedIn.isFinal()) {
          break;
        }
        world.saga().stepAndCrashBeforeRecording("b-1004", nowMillis);
        world.runToEnd("b-1004", nowMillis + RESUME_DELAY_MILLIS);
        SagaWorld.Outcome outcome = world.outcome("b-1004", "h-1");
        System.out.println(
            String.format(
                Locale.ROOT,
                "  %-20s crash at step %d (%-17s) -> %-11s hold %-9s kept %5s  %s  %s",
                behavior.name().toLowerCase(Locale.ROOT),
                crashStep + 1,
                crashedIn,
                outcome.state(),
                outcome.holdState(),
                outcome.netCharged().toPlainString(),
                world.payment().ledger("seatwave-hold-h-1"),
                outcome.isCorrect() ? "correct" : "WRONG"));
      }
    }
  }

  private static void stuckSaga() {
    SagaWorld world = startedSaga(BookingSaga.SeatProtection.SEMANTIC_LOCK, EARLY_CONFIRM_MILLIS);
    world.saga().step("b-1004", EARLY_CONFIRM_MILLIS);
    world.saga().stepAndCrashBeforeRecording("b-1004", EARLY_CONFIRM_MILLIS + PAYMENT_MILLIS);
    for (long checkMillis : new long[] {SagaWorld.HOLD_MILLIS, ORCHESTRATOR_DOWN_UNTIL_MILLIS}) {
      System.out.println(
          String.format(
              Locale.ROOT,
              "  %8.3f s  sagas older than the 300 s hold: %s; seat A-7 is %s; expiry sweep ends"
                  + " %d",
              checkMillis / MILLIS_PER_SECOND,
              world.saga().stuckSagas(checkMillis, SagaWorld.HOLD_MILLIS),
              world.inventory().seatState("A-7"),
              world.inventory().expireDue(checkMillis)));
    }
    System.out.println(
        "  the sweeper resumes b-1004 (the provider's key returns the first charge):");
    runPrinting(world, ORCHESTRATOR_DOWN_UNTIL_MILLIS);
    System.out.println("  provider ledger: " + world.payment().ledger("seatwave-hold-h-1"));
  }

  private static long durationOf(BookingSaga.State state) {
    return state == BookingSaga.State.PAYMENT_REQUESTED
            || state == BookingSaga.State.REFUND_REQUESTED
        ? PAYMENT_MILLIS
        : LOCAL_CALL_MILLIS;
  }

  private static void printStep(long atMillis, BookingSaga.StepResult result) {
    String retry =
        result.from() == result.to()
            ? String.format(
                Locale.ROOT, " (retry at %.3f s)", result.retryAtMillis() / MILLIS_PER_SECOND)
            : "";
    System.out.println(
        String.format(
            Locale.ROOT,
            "  %8.3f s  %-17s -> %-17s %s%s",
            atMillis / MILLIS_PER_SECOND,
            result.from(),
            result.to(),
            describe(result.reply()),
            retry));
  }

  private static void printOutcome(SagaWorld world) {
    SagaWorld.Outcome outcome = world.outcome("b-1004", "h-1");
    System.out.println(
        String.format(
            Locale.ROOT,
            "  end: %s, hold %s, kept from the fan %s, %s",
            outcome.state(),
            outcome.holdState(),
            outcome.netCharged().toPlainString(),
            outcome.isCorrect() ? "correct" : "WRONG"));
  }

  private static void printOutboxes(SagaWorld world) {
    for (ServiceDatabase database :
        new ServiceDatabase[] {
          world.bookingDatabase(), world.inventoryDatabase(), world.paymentDatabase()
        }) {
      StringBuilder types = new StringBuilder();
      for (ServiceDatabase.OutboxRow row : database.outbox()) {
        Map<String, String> fields = EventFields.decode(row.value()).orElseThrow();
        types.append(types.isEmpty() ? "" : ", ").append(fields.get("type"));
      }
      System.out.println(
          String.format(
              Locale.ROOT,
              "  outbox of %-9s %d row(s): %s",
              database.serviceName(),
              database.outbox().size(),
              types));
    }
  }

  private static String describe(StepReply reply) {
    return switch (reply) {
      case StepReply.Succeeded succeeded -> "ok: " + succeeded.detail();
      case StepReply.AlreadyDone alreadyDone -> "already done: " + alreadyDone.detail();
      case StepReply.Refused refused -> "refused: " + refused.reason();
      case StepReply.Unavailable unavailable -> "unavailable: " + unavailable.reason();
    };
  }
}

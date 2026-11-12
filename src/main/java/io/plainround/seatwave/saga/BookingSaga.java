package io.plainround.seatwave.saga;

import java.util.List;
import java.util.Objects;

/**
 * The booking saga's orchestrator, in the booking service. Each saga is one row of {@code
 * booking_sagas}; every transition rewrites the row and, in the same commit, writes the next
 * command (or the final event) to the booking service's outbox. A step delivers the command of the
 * current state to its participant, then commits the next state. A crash between the two leaves the
 * old state, so the step runs again and the participant, being idempotent, answers as before. Every
 * saga therefore ends in COMPLETED (seat confirmed and paid) or COMPENSATED (seat released, not
 * charged or refunded), whatever crashes in between.
 *
 * <p>The same class drives the in-memory services of every program and test and the HTTP services
 * of the Compose stack: only the {@link SagaStore} and the {@link SagaParticipants} differ. Not
 * thread-safe for one saga: one orchestrator drives a saga at a time (in production, the single
 * writer of Module 23 per saga partition).
 */
public final class BookingSaga {

  private final SagaStore store;
  private final SagaParticipants participants;
  private final Settings settings;

  /** The saga's states; the two last are final. */
  public enum State {
    SEAT_HELD,
    PAYMENT_REQUESTED,
    PAYMENT_SUCCEEDED,
    PAYMENT_FAILED,
    REFUND_REQUESTED,
    PAYMENT_REFUNDED,
    COMPLETED,
    COMPENSATED;

    /** COMPLETED or COMPENSATED: nothing moves the saga again. */
    public boolean isFinal() {
      return this == COMPLETED || this == COMPENSATED;
    }
  }

  /** Whether step 1 takes the semantic lock (CONFIRMING) or only checks the hold is alive. */
  public enum SeatProtection {
    SEMANTIC_LOCK,
    NONE
  }

  /**
   * How the orchestrator protects the seat, and how it backs off a participant that is down.
   *
   * @param protection the semantic lock, or none (only to show what goes wrong without it)
   * @param firstRetryMillis the wait after the first unanswered command
   * @param maxRetryMillis the cap of the doubling wait
   */
  public record Settings(SeatProtection protection, long firstRetryMillis, long maxRetryMillis) {
    /** Refuses a missing protection, a non-positive first wait or a cap below it. */
    public Settings {
      Objects.requireNonNull(protection, "protection");
      if (firstRetryMillis < 1 || maxRetryMillis < firstRetryMillis) {
        throw new IllegalArgumentException("retry delays must be positive and capped above start");
      }
    }
  }

  /**
   * What one step did.
   *
   * @param from the state before
   * @param to the state after (equal to {@code from} when the command must be sent again)
   * @param reply the participant's answer
   * @param retryAtMillis when the next step may run
   */
  public record StepResult(State from, State to, StepReply reply, long retryAtMillis) {}

  /** An orchestrator over one store and one set of participants. */
  public BookingSaga(SagaStore store, SagaParticipants participants, Settings settings) {
    this.store = Objects.requireNonNull(store, "store");
    this.participants = Objects.requireNonNull(participants, "participants");
    this.settings = Objects.requireNonNull(settings, "settings");
  }

  /** Starts a saga for a hold: state SEAT_HELD, and the command LockSeat in the outbox. */
  public void start(String sagaId, String holdId, long nowMillis) {
    SagaRecord saga =
        new SagaRecord(
            sagaId, holdId, SagaRecord.PROVIDER_KEY_PREFIX + holdId, State.SEAT_HELD, 0, nowMillis);
    store.record(saga, messageFor(State.SEAT_HELD), nowMillis);
  }

  /** Delivers the current state's command and commits what follows. */
  public StepResult step(String sagaId, long nowMillis) {
    return deliverAndMaybeRecord(sagaId, nowMillis, true);
  }

  /** Delivers the current state's command, then crashes before recording the reply. */
  public StepResult stepAndCrashBeforeRecording(String sagaId, long nowMillis) {
    return deliverAndMaybeRecord(sagaId, nowMillis, false);
  }

  /** Whether a saga with this id was started. */
  public boolean exists(String sagaId) {
    return store.find(sagaId).isPresent();
  }

  /** The saga's current state; refuses an unknown saga id. */
  public State state(String sagaId) {
    return saga(sagaId).state();
  }

  /** Sagas not final whose last change is older than {@code olderThanMillis}: stuck. */
  public List<String> stuckSagas(long nowMillis, long olderThanMillis) {
    return store.openOlderThan(nowMillis, olderThanMillis).stream()
        .map(SagaRecord::bookingId)
        .toList();
  }

  private StepResult deliverAndMaybeRecord(String sagaId, long nowMillis, boolean shouldRecord) {
    SagaRecord saga = saga(sagaId);
    State current = saga.state();
    if (current.isFinal()) {
      return new StepResult(current, current, new StepReply.AlreadyDone("final"), nowMillis);
    }
    StepReply reply = deliver(saga, nowMillis);
    State next = next(current, reply);
    long retryAtMillis = nowMillis;
    if (next == current) {
      retryAtMillis = nowMillis + backoffMillis(saga.attempts());
    }
    if (shouldRecord) {
      if (next != current) {
        store.record(saga.moved(next, nowMillis), messageFor(next), nowMillis);
      } else {
        // A retry changes the row (the attempt count), so it carries its event too. The row's
        // updated_at stays, so a saga that keeps retrying ages into the stuck-saga alert.
        store.record(saga.retried(), "SagaStepRetried", nowMillis);
      }
    }
    return new StepResult(current, next, reply, retryAtMillis);
  }

  /** The command each state sends, to whom. */
  private StepReply deliver(SagaRecord saga, long nowMillis) {
    String holdId = saga.holdId();
    String providerKey = saga.providerKey();
    return switch (saga.state()) {
      case SEAT_HELD ->
          settings.protection() == SeatProtection.SEMANTIC_LOCK
              ? participants.lockSeat(holdId, saga.bookingId(), nowMillis)
              : participants.checkSeatActive(holdId, nowMillis);
      case PAYMENT_REQUESTED -> participants.chargePayment(providerKey, nowMillis);
      case PAYMENT_SUCCEEDED -> participants.confirmSeat(holdId, nowMillis);
      case PAYMENT_FAILED, PAYMENT_REFUNDED -> participants.releaseSeat(holdId, nowMillis);
      case REFUND_REQUESTED -> participants.refundPayment(providerKey, nowMillis);
      case COMPLETED, COMPENSATED -> new StepReply.AlreadyDone("final");
    };
  }

  /** The transition table. A compensation is never refused for good: it is retried. */
  private static State next(State current, StepReply reply) {
    return switch (reply) {
      case StepReply.Succeeded _, StepReply.AlreadyDone _ -> afterDone(current);
      case StepReply.Refused _ -> afterRefused(current);
      // Outcome unknown: stay, and send the same command again later.
      case StepReply.Unavailable _ -> current;
    };
  }

  private static State afterDone(State current) {
    return switch (current) {
      case SEAT_HELD -> State.PAYMENT_REQUESTED;
      case PAYMENT_REQUESTED -> State.PAYMENT_SUCCEEDED;
      case PAYMENT_SUCCEEDED -> State.COMPLETED;
      case REFUND_REQUESTED -> State.PAYMENT_REFUNDED;
      case PAYMENT_FAILED, PAYMENT_REFUNDED -> State.COMPENSATED;
      case COMPLETED, COMPENSATED -> current;
    };
  }

  private static State afterRefused(State current) {
    return switch (current) {
      // Nothing was done yet, so nothing needs undoing.
      case SEAT_HELD -> State.COMPENSATED;
      case PAYMENT_REQUESTED -> State.PAYMENT_FAILED;
      // Paid, and then the seat is gone: the interview question; compensate the payment.
      case PAYMENT_SUCCEEDED -> State.REFUND_REQUESTED;
      // A compensation must end in success, so a refusal is retried and shows as a stuck saga.
      case PAYMENT_FAILED, REFUND_REQUESTED, PAYMENT_REFUNDED, COMPLETED, COMPENSATED -> current;
    };
  }

  /** The event or command written with each state: what the outbox carries next. */
  private static String messageFor(State state) {
    return switch (state) {
      case SEAT_HELD -> "LockSeat";
      case PAYMENT_REQUESTED -> "ChargePayment";
      case PAYMENT_SUCCEEDED -> "ConfirmSeat";
      case PAYMENT_FAILED, PAYMENT_REFUNDED -> "ReleaseSeat";
      case REFUND_REQUESTED -> "RefundPayment";
      // The notification step: the email consumer of Module 27 sends one email for this event.
      // It is last because an email cannot be taken back.
      case COMPLETED -> "BookingConfirmed";
      case COMPENSATED -> "BookingFailed";
    };
  }

  private long backoffMillis(int attempts) {
    long delay = settings.firstRetryMillis();
    for (int attemptIndex = 0;
        attemptIndex < attempts && delay < settings.maxRetryMillis();
        attemptIndex++) {
      delay = Math.multiplyExact(delay, 2);
    }
    return Math.min(delay, settings.maxRetryMillis());
  }

  private SagaRecord saga(String sagaId) {
    return store.find(sagaId).orElseThrow(() -> new IllegalArgumentException("no saga " + sagaId));
  }
}

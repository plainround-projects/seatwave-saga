package io.plainround.seatwave.saga.stack;

import io.plainround.seatwave.saga.BookingSaga;
import io.plainround.seatwave.saga.SagaRecord;
import io.plainround.seatwave.saga.SagaStore;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The drills' crash point for the orchestrator: the first time a saga in {@code pauseIn} is about
 * to record what its participant answered, it blocks with a {@code PAUSED} line instead, so the
 * drill kills the booking service after the command was delivered and before the reply was stored.
 * Every other call goes straight to the real store.
 *
 * <p>Thread-safe if the store is.
 */
final class PausingSagaStore implements SagaStore {

  private final SagaStore store;
  private final BookingSaga.State pauseIn;

  PausingSagaStore(SagaStore store, BookingSaga.State pauseIn) {
    this.store = Objects.requireNonNull(store, "store");
    this.pauseIn = Objects.requireNonNull(pauseIn, "pauseIn");
  }

  @Override
  public Optional<SagaRecord> find(String bookingId) {
    return store.find(bookingId);
  }

  @Override
  public void record(SagaRecord saga, String message, long nowMillis) {
    Optional<SagaRecord> before = store.find(saga.bookingId());
    if (before.isPresent() && before.get().state() == pauseIn) {
      try {
        Pause.untilKilled(
            "booking " + saga.bookingId() + " in " + pauseIn + ", reply received, not recorded");
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("interrupted at the drill's crash point", interrupted);
      }
    }
    store.record(saga, message, nowMillis);
  }

  @Override
  public List<SagaRecord> openOlderThan(long nowMillis, long olderThanMillis) {
    return store.openOlderThan(nowMillis, olderThanMillis);
  }
}

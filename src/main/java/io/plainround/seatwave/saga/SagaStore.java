package io.plainround.seatwave.saga;

import java.util.List;
import java.util.Optional;

/**
 * Where the orchestrator keeps its sagas: the booking service's own database. Every {@link #record}
 * writes the saga's row and one outbox row in the same commit, and a completed saga's booking row
 * with them, so a decision and its record cannot be separated.
 *
 * <p>Two implementations: {@link ServiceDatabaseSagaStore} on the in-memory {@link
 * ServiceDatabase}, and {@code jdbc.JdbcSagaStore} on the booking service's PostgreSQL, where the
 * deferred trigger {@code saga_change_has_event} refuses a commit without its outbox row.
 */
public interface SagaStore {

  /** The saga with this id, or empty. */
  Optional<SagaRecord> find(String bookingId);

  /**
   * Writes the saga's new row and, in the same commit, the outbox row {@code message} (the next
   * command, the final event, or {@code SagaStepRetried}). Throws when another writer moved the
   * saga first or when the commit is refused; then nothing of it is stored.
   */
  void record(SagaRecord saga, String message, long nowMillis);

  /** Sagas not final whose last transition is older than {@code olderThanMillis}, oldest first. */
  List<SagaRecord> openOlderThan(long nowMillis, long olderThanMillis);
}

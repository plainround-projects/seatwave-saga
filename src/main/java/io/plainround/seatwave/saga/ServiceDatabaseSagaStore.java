package io.plainround.seatwave.saga;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** {@code booking_sagas} as rows of the in-memory {@link ServiceDatabase}. Not thread-safe. */
final class ServiceDatabaseSagaStore implements SagaStore {

  private static final String SAGAS = "booking_sagas";

  private final ServiceDatabase database;

  ServiceDatabaseSagaStore(ServiceDatabase database) {
    this.database = Objects.requireNonNull(database, "database");
  }

  @Override
  public Optional<SagaRecord> find(String bookingId) {
    return database.read(SAGAS, bookingId).map(row -> fromRow(bookingId, row));
  }

  @Override
  public void record(SagaRecord saga, String message, long nowMillis) {
    Map<String, String> row = new LinkedHashMap<>();
    row.put("hold_id", saga.holdId());
    row.put("provider_key", saga.providerKey());
    row.put("state", saga.state().name());
    row.put("attempts", Integer.toString(saga.attempts()));
    row.put("updated_at_ms", Long.toString(saga.updatedAtMillis()));
    Map<String, String> payload = new LinkedHashMap<>();
    payload.put("booking_id", saga.bookingId());
    payload.put("hold_id", saga.holdId());
    payload.put("state", saga.state().name());
    ServiceDatabase.Transaction transaction = database.begin().put(SAGAS, saga.bookingId(), row);
    if (saga.state() == BookingSaga.State.COMPLETED) {
      transaction.put("bookings", saga.bookingId(), Map.of("hold_id", saga.holdId()));
    }
    database.commit(transaction.addEvent(saga.bookingId(), message, payload), nowMillis);
  }

  @Override
  public List<SagaRecord> openOlderThan(long nowMillis, long olderThanMillis) {
    return database.readAll(SAGAS).entrySet().stream()
        .map(entry -> fromRow(entry.getKey(), entry.getValue()))
        .filter(saga -> !saga.state().isFinal())
        .filter(saga -> nowMillis - saga.updatedAtMillis() > olderThanMillis)
        .sorted(Comparator.comparingLong(SagaRecord::updatedAtMillis))
        .toList();
  }

  private static SagaRecord fromRow(String bookingId, Map<String, String> row) {
    return new SagaRecord(
        bookingId,
        row.get("hold_id"),
        row.get("provider_key"),
        BookingSaga.State.valueOf(row.get("state")),
        Integer.parseInt(row.get("attempts")),
        Long.parseLong(row.get("updated_at_ms")));
  }
}

package io.plainround.seatwave.saga;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * The read model {@code bookings_by_customer} (Module 18's global index, now a CQRS projection):
 * one row per booking, found by customer, built only from booking events. With the rule
 * HIGHER_VERSION_WINS an event changes a row only when its {@code booking_version} is higher than
 * the row's, so a duplicate or an older event changes nothing and the rows come out the same in any
 * order of delivery (Module 25's idempotent merge). Not thread-safe.
 */
public final class BookingsByCustomer {

  private final UpdateRule updateRule;
  private final Set<String> handledTypes;
  private final Map<String, Row> rowsByBooking = new TreeMap<>();

  /** One row of the read model. */
  public record Row(String customerId, String seatLabel, String status, long bookingVersion) {
    /** Rejects a missing customer or status. */
    public Row {
      Objects.requireNonNull(customerId, "customerId");
      Objects.requireNonNull(status, "status");
    }
  }

  /** How an event decides whether it may overwrite a row. */
  public enum UpdateRule {
    HIGHER_VERSION_WINS,
    LAST_APPLIED_WINS
  }

  /** What one event did to the read model. */
  enum Applied {
    CHANGED,
    SKIPPED_NOT_NEWER,
    SKIPPED_TYPE_NOT_HANDLED
  }

  /** A projection with an update rule and the event types its code handles. */
  BookingsByCustomer(UpdateRule updateRule, Set<String> handledTypes) {
    this.updateRule = Objects.requireNonNull(updateRule, "updateRule");
    this.handledTypes = Set.copyOf(handledTypes);
  }

  /** Applies one decoded event; fields it does not know are ignored (the tolerant reader). */
  Applied apply(Map<String, String> event) {
    String type = event.get("type");
    if (!handledTypes.contains(type)) {
      return Applied.SKIPPED_TYPE_NOT_HANDLED;
    }
    String bookingId = event.get("booking_id");
    long version = Long.parseLong(event.get("booking_version"));
    Row existing = rowsByBooking.get(bookingId);
    if (updateRule == UpdateRule.HIGHER_VERSION_WINS
        && existing != null
        && existing.bookingVersion() >= version) {
      return Applied.SKIPPED_NOT_NEWER;
    }
    String status = type.equals("BookingCanceled") ? "CANCELED" : "CONFIRMED";
    rowsByBooking.put(
        bookingId,
        new Row(event.get("customer_id"), event.getOrDefault("seat_label", "?"), status, version));
    return Applied.CHANGED;
  }

  /** Rows that differ from the source of truth, or are missing from either side. */
  int differencesFrom(Map<String, Row> truth) {
    return differences(rowsByBooking, truth);
  }

  /**
   * Rows of {@code model} that differ from {@code truth}, plus rows missing from either side: the
   * reconciliation of a read model, in memory or read back from PostgreSQL.
   */
  public static int differences(Map<String, Row> model, Map<String, Row> truth) {
    int differences = 0;
    for (Map.Entry<String, Row> entry : truth.entrySet()) {
      if (!entry.getValue().equals(model.get(entry.getKey()))) {
        differences++;
      }
    }
    for (String bookingId : model.keySet()) {
      if (!truth.containsKey(bookingId)) {
        differences++;
      }
    }
    return differences;
  }

  /** "My tickets": the confirmed bookings of one customer, the query this model exists for. */
  long confirmedCount(String customerId) {
    return rowsByBooking.values().stream()
        .filter(row -> row.customerId().equals(customerId) && row.status().equals("CONFIRMED"))
        .count();
  }

  int size() {
    return rowsByBooking.size();
  }
}

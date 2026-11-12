package io.plainround.seatwave.saga;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * The booking events of Chapters 28.3 and 28.4, the same on every run: 10,000 bookings of 1,000
 * customers, one in ten later canceled, one event in fifty published twice (a relay that crashed
 * before marking), the last 3,000 bookings written with schema version 2. {@code ReadModelCli}
 * appends them to the in-process log; the stack's rebuild drill publishes the same events to Kafka.
 *
 * @param events every event in the order the relay published it, duplicates included
 * @param truth the booking table, the source of truth, by booking id
 */
public record BookingHistory(
    List<Map<String, String>> events, Map<String, BookingsByCustomer.Row> truth) {

  private static final int BOOKINGS = 10_000;
  private static final int CUSTOMERS = 1_000;
  private static final int FIRST_CUSTOMER = 1_000;
  private static final int CANCEL_EVERY = 10;
  private static final int DUPLICATE_EVERY = 50;
  private static final int FIRST_V2_BOOKING = 7_000;

  /** Copies both, so the history cannot change after it is made. */
  public BookingHistory {
    events = List.copyOf(events);
    truth = Map.copyOf(truth);
  }

  /** The history; every call returns equal values. */
  public static BookingHistory generate() {
    List<Map<String, String>> events = new ArrayList<>();
    Map<String, BookingsByCustomer.Row> truth = new TreeMap<>();
    int eventIndex = 0;
    for (int bookingIndex = 0; bookingIndex < BOOKINGS; bookingIndex++) {
      String bookingId = String.format(Locale.ROOT, "b-%05d", bookingIndex);
      // Customers 1000 to 1999, numbered like Module 27's customer 1000.
      String customerId = Integer.toString(FIRST_CUSTOMER + bookingIndex % CUSTOMERS);
      String seatLabel = "S" + bookingIndex / CUSTOMERS + "-" + bookingIndex % CUSTOMERS;
      boolean isCanceled = bookingIndex % CANCEL_EVERY == CANCEL_EVERY - 1;
      int versions = isCanceled ? 2 : 1;
      for (int version = 1; version <= versions; version++) {
        Map<String, String> event = new LinkedHashMap<>();
        event.put("type", version == 1 ? "BookingConfirmed" : "BookingCanceled");
        event.put("schema_version", bookingIndex >= FIRST_V2_BOOKING ? "2" : "1");
        event.put("message_id", "booking-" + (eventIndex + 1));
        event.put("booking_id", bookingId);
        event.put("booking_version", Integer.toString(version));
        event.put("customer_id", customerId);
        event.put("event_id", "1");
        event.put("seat_label", seatLabel);
        event.put("occurred_at", "2026-11-20T19:00:00Z");
        event.put("show_starts_at", "2026-12-05T20:00:00Z");
        if (bookingIndex >= FIRST_V2_BOOKING) {
          event.put("price", "80.00");
          event.put("currency", "USD");
        }
        // Insertion order is kept, so every encoding of the event lists its fields the same way.
        Map<String, String> frozen = Collections.unmodifiableMap(event);
        events.add(frozen);
        if (eventIndex % DUPLICATE_EVERY == DUPLICATE_EVERY - 1) {
          // A relay that crashed before marking publishes the same row, same message id, again.
          events.add(frozen);
        }
        eventIndex++;
      }
      truth.put(
          bookingId,
          new BookingsByCustomer.Row(
              customerId, seatLabel, isCanceled ? "CANCELED" : "CONFIRMED", versions));
    }
    return new BookingHistory(events, truth);
  }
}

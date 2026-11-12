package io.plainround.seatwave.saga;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

/**
 * Chapters 28.3 and 28.4 made visible. 10,000 bookings of 1,000 customers flow as events through
 * {@code seatwave.bookings} (one in ten later canceled, one event in fifty published twice by a
 * relay, the last 3,000 written with schema version 2). The read model {@code bookings_by_customer}
 * is built, drifts under a bug, is rebuilt from offset 0, is rebuilt in shuffled order, and the
 * schema versions are checked for compatibility. Deterministic: fixed counts and a fixed seed.
 */
final class ReadModelCli {

  private static final String TOPIC = "seatwave.bookings";
  private static final int PARTITIONS = 24;
  private static final long SHUFFLE_SEED = 28;

  /**
   * One write per event on Module 1's SSD volume of 3,000 IOPS (an assumption for the estimate).
   */
  private static final double PROJECTED_EVENTS_PER_SECOND = 3_000;

  /** Module 15's estimate, quoted through Module 18. */
  private static final double BOOKINGS_PER_YEAR = 50_000_000;

  private static final double SECONDS_PER_HOUR = 3_600;

  private static final EventSchema V1 =
      new EventSchema(
          "BookingConfirmed",
          1,
          required(
              "message_id",
              "booking_id",
              "booking_version",
              "customer_id",
              "event_id",
              "seat_label",
              "occurred_at",
              "show_starts_at"));

  private ReadModelCli() {}

  public static void main(String[] arguments) {
    EventLog eventLog = new EventLog();
    eventLog.createTopic(TOPIC, PARTITIONS);
    BookingHistory history = BookingHistory.generate();
    history
        .events()
        .forEach(
            event -> eventLog.append(TOPIC, event.get("booking_id"), EventFields.encode(event)));
    Map<String, BookingsByCustomer.Row> truth = history.truth();
    List<Map<String, String>> events = new ArrayList<>();
    eventLog.readFromStart(TOPIC).forEach(record -> events.add(decode(record)));

    System.out.println("Part 1. Build bookings_by_customer from the topic");
    BookingsByCustomer correct =
        project(
            events,
            BookingsByCustomer.UpdateRule.HIGHER_VERSION_WINS,
            Set.of("BookingConfirmed", "BookingCanceled"),
            true);
    System.out.println(
        String.format(
            Locale.ROOT,
            "  %,d bookings, %,d events (%.2f per booking); rows %,d; differences from the booking"
                + " table: %d; customer 1042 has %d confirmed",
            truth.size(),
            events.size(),
            events.size() / (double) truth.size(),
            correct.size(),
            correct.differencesFrom(truth),
            correct.confirmedCount("1042")));

    System.out.println("Part 2. A bug: the projector's code ignores BookingCanceled");
    BookingsByCustomer buggy =
        project(
            events,
            BookingsByCustomer.UpdateRule.HIGHER_VERSION_WINS,
            Set.of("BookingConfirmed"),
            false);
    System.out.println(
        String.format(
            Locale.ROOT,
            "  reconciliation against the booking table:"
                + " %,d rows differ (drift); fixed code rebuilt from offset 0: %d differ",
            buggy.differencesFrom(truth),
            correct.differencesFrom(truth)));
    double rebuildSeconds = events.size() / PROJECTED_EVENTS_PER_SECOND;
    double yearHours =
        BOOKINGS_PER_YEAR
            * events.size()
            / truth.size()
            / PROJECTED_EVENTS_PER_SECOND
            / SECONDS_PER_HOUR;
    System.out.println(
        String.format(
            Locale.ROOT,
            "  estimated rebuild at 3,000 events a second:"
                + " %.1f s for these %,d events; %.1f hours for a year of 50,000,000 bookings",
            rebuildSeconds,
            events.size(),
            yearHours));

    System.out.println("Part 3. Rebuild with the events in a shuffled order");
    List<Map<String, String>> shuffled = new ArrayList<>(events);
    Collections.shuffle(shuffled, new Random(SHUFFLE_SEED));
    for (BookingsByCustomer.UpdateRule rule : BookingsByCustomer.UpdateRule.values()) {
      BookingsByCustomer model =
          project(shuffled, rule, Set.of("BookingConfirmed", "BookingCanceled"), false);
      System.out.println(
          String.format(
              Locale.ROOT, "  %-19s differences: %,d", rule, model.differencesFrom(truth)));
    }

    System.out.println("Part 4. Schema versions of BookingConfirmed");
    EventSchema v2 =
        V1.plus(2, "price", EventSchema.Presence.OPTIONAL)
            .plus(2, "currency", EventSchema.Presence.OPTIONAL);
    EventSchema renamed = v2.renamed(3, "seat_label", "seat");
    EventSchema priceRequired = v2.plus(3, "price", EventSchema.Presence.REQUIRED);
    compare("v1 -> v2, add optional price and currency", V1, v2);
    compare("v2 -> v3, rename seat_label to seat", v2, renamed);
    compare("v2 -> v3, make price required", v2, priceRequired);
    Map<String, String> renamedEvent = new LinkedHashMap<>(events.get(0));
    renamedEvent.put("seat", renamedEvent.remove("seat_label"));
    System.out.println(
        "  a v1 reader given one v3 (renamed) event: missing "
            + V1.missingFrom(renamedEvent)
            + " -> park it on seatwave.notifications.dlq (Module 27's poison message)");
  }

  private static BookingsByCustomer project(
      List<Map<String, String>> events,
      BookingsByCustomer.UpdateRule rule,
      Set<String> handledTypes,
      boolean isPrinting) {
    BookingsByCustomer model = new BookingsByCustomer(rule, handledTypes);
    Map<BookingsByCustomer.Applied, Integer> counts = new TreeMap<>();
    events.forEach(event -> counts.merge(model.apply(event), 1, Integer::sum));
    if (isPrinting) {
      System.out.println("  applied: " + counts);
    }
    return model;
  }

  private static void compare(String change, EventSchema older, EventSchema newer) {
    List<String> backward = newer.problemsReading(older);
    List<String> forward = older.problemsReading(newer);
    System.out.println(
        String.format(
            Locale.ROOT,
            "  %-44s backward (new reader, old events): %s;"
                + " forward (old reader, new events): %s",
            change,
            backward.isEmpty() ? "ok" : backward,
            forward.isEmpty() ? "ok" : forward));
  }

  private static Map<String, EventSchema.Presence> required(String... names) {
    Map<String, EventSchema.Presence> fields = new LinkedHashMap<>();
    for (String name : names) {
      fields.put(name, EventSchema.Presence.REQUIRED);
    }
    return fields;
  }

  private static Map<String, String> decode(EventLog.Record record) {
    return EventFields.decode(record.value())
        .orElseThrow(
            () -> new IllegalStateException("malformed event at offset " + record.offset()));
  }
}

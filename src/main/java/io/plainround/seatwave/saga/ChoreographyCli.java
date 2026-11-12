package io.plainround.seatwave.saga;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The same booking saga with no orchestrator: each service reacts to the others' events and
 * publishes its own through its outbox and relay. Run twice (card captured, card declined), with
 * the inventory relay crashing once before marking, so one event arrives twice. Prints the event
 * trail, the effects, and what it takes to answer "where is booking b-1004?".
 */
final class ChoreographyCli {

  private static final String BOOKINGS = "seatwave.bookings";
  private static final String INVENTORY = "seatwave.inventory";
  private static final String PAYMENTS = "seatwave.payments";
  private static final int PARTITIONS = 24;
  private static final int RELAY_BATCH = 100;
  private static final long FENCING_TOKEN = 33;
  private static final int MAX_ROUNDS = 10;
  private static final long ROUND_MILLIS = 100;

  /** How the booking service ends a booking: its new state and the event that says so. */
  private record Ending(String state, String eventType) {}

  private static final Ending CONFIRMED = new Ending("CONFIRMED", "BookingConfirmed");
  private static final Ending FAILED = new Ending("FAILED", "BookingFailed");

  private ChoreographyCli() {}

  public static void main(String[] arguments) {
    for (PaymentService.Behavior behavior :
        List.of(PaymentService.Behavior.CAPTURE, PaymentService.Behavior.DECLINE)) {
      System.out.println("Card " + behavior.name().toLowerCase(Locale.ROOT) + ":");
      run(behavior);
      System.out.println();
    }
  }

  private static void run(PaymentService.Behavior behavior) {
    SagaWorld world = SagaWorld.create(BookingSaga.SeatProtection.SEMANTIC_LOCK);
    EventLog eventLog = new EventLog();
    for (String topic : List.of(BOOKINGS, INVENTORY, PAYMENTS)) {
      eventLog.createTopic(topic, PARTITIONS);
    }
    world.payment().script("seatwave-hold-h-1", behavior);
    world.inventory().hold(new InventoryService.HoldRequest("h-1", "A-7", "1000"), 0);
    ServiceDatabase bookings = world.bookingDatabase();
    // The booking service's own step: a PENDING booking and its event, in one commit.
    bookings.commit(
        bookings
            .begin()
            .put("bookings", "b-1004", Map.of("state", "PENDING", "hold_id", "h-1"))
            .put("booking_by_hold", "h-1", Map.of("booking_id", "b-1004"))
            .addEvent(
                "b-1004", "BookingRequested", Map.of("booking_id", "b-1004", "hold_id", "h-1")),
        0);
    Map<String, Long> positions = new HashMap<>();
    List<String> trail = new ArrayList<>();
    for (int round = 1; round <= MAX_ROUNDS; round++) {
      long nowMillis = round * ROUND_MILLIS;
      relay(world.bookingDatabase(), eventLog, BOOKINGS).relayOnce();
      OutboxRelay inventoryRelay = relay(world.inventoryDatabase(), eventLog, INVENTORY);
      // Round 2: the inventory relay dies after publishing and before marking, once.
      OutboxRelay.Pass pass =
          round == 2 ? inventoryRelay.relayOnceAndCrashBeforeMarking() : inventoryRelay.relayOnce();
      relay(world.paymentDatabase(), eventLog, PAYMENTS).relayOnce();
      int handled = 0;
      for (String topic : List.of(BOOKINGS, INVENTORY, PAYMENTS)) {
        for (EventLog.Record record : newRecords(eventLog, topic, positions)) {
          Map<String, String> event = EventFields.decode(record.value()).orElseThrow();
          trail.add(
              String.format(
                  Locale.ROOT,
                  "    round %d  %-18s %-17s %s%s",
                  round,
                  topic,
                  event.get("type"),
                  event.get("message_id"),
                  react(world, event, nowMillis)));
          handled++;
        }
      }
      if (handled == 0 && pass.published() == 0) {
        break;
      }
    }
    System.out.println("  event trail (topic, type, message id, reaction):");
    trail.forEach(System.out::println);
    System.out.println(
        String.format(
            Locale.ROOT,
            "  end: booking %s, hold %s, kept %s, provider ledger %s",
            bookings.read("bookings", "b-1004").orElseThrow().get("state"),
            world.inventory().holdState("h-1"),
            world.payment().netCharged("seatwave-hold-h-1").toPlainString(),
            world.payment().ledger("seatwave-hold-h-1")));
    System.out.println(
        "  while it ran, the booking row said only PENDING; its progress was spread over "
            + eventLog.readFromStart(BOOKINGS).size()
            + " + "
            + eventLog.readFromStart(INVENTORY).size()
            + " + "
            + eventLog.readFromStart(PAYMENTS).size()
            + " events on three topics, joined by hold id");
  }

  /** Each service's handler: which event it listens to, and the local step it takes. */
  private static String react(SagaWorld world, Map<String, String> event, long nowMillis) {
    String holdId = event.getOrDefault("hold_id", "");
    String providerKey = event.getOrDefault("provider_key", "seatwave-hold-" + holdId);
    String paidHold = providerKey.substring("seatwave-hold-".length());
    return switch (event.get("type")) {
      case "BookingRequested" ->
          " -> inventory locks: "
              + text(world.inventory().lockForPayment(holdId, "b-1004", nowMillis));
      case "SeatLocked" ->
          " -> payment charges: " + text(world.payment().charge(providerKey, nowMillis));
      case "PaymentCaptured" ->
          " -> inventory confirms: " + text(world.inventory().confirm(paidHold, nowMillis));
      case "PaymentFailed" ->
          " -> inventory releases: " + text(world.inventory().release(paidHold, nowMillis));
      case "SeatConfirmed" -> " -> booking: " + finish(world, holdId, CONFIRMED, nowMillis);
      case "SeatReleased" -> " -> booking: " + finish(world, holdId, FAILED, nowMillis);
      case "BookingConfirmed" -> " -> notifications-email sends one email (Module 27)";
      default -> "";
    };
  }

  /** The booking service ends its booking once, whatever the number of deliveries. */
  private static String finish(SagaWorld world, String holdId, Ending ending, long nowMillis) {
    ServiceDatabase bookings = world.bookingDatabase();
    String bookingId = bookings.read("booking_by_hold", holdId).orElseThrow().get("booking_id");
    Map<String, String> booking = bookings.read("bookings", bookingId).orElseThrow();
    if (!booking.get("state").equals("PENDING")) {
      return "already " + booking.get("state");
    }
    Map<String, String> changed = new LinkedHashMap<>(booking);
    changed.put("state", ending.state());
    bookings.commit(
        bookings
            .begin()
            .put("bookings", bookingId, changed)
            .addEvent(
                bookingId, ending.eventType(), Map.of("booking_id", bookingId, "hold_id", holdId)),
        nowMillis);
    return ending.state();
  }

  private static List<EventLog.Record> newRecords(
      EventLog eventLog, String topic, Map<String, Long> positions) {
    List<EventLog.Record> fresh = new ArrayList<>();
    for (int partition = 0; partition < eventLog.partitionCount(topic); partition++) {
      String position = topic + "|" + partition;
      long fromOffset = positions.getOrDefault(position, 0L);
      List<EventLog.Record> records =
          eventLog.fetch(topic, partition, fromOffset, (int) eventLog.endOffset(topic, partition));
      fresh.addAll(records);
      positions.put(position, fromOffset + records.size());
    }
    return fresh;
  }

  private static OutboxRelay relay(ServiceDatabase database, EventLog eventLog, String topic) {
    return new OutboxRelay(
        new OutboxRelay.Settings(topic, FENCING_TOKEN, RELAY_BATCH), database, eventLog);
  }

  private static String text(StepReply reply) {
    return switch (reply) {
      case StepReply.Succeeded succeeded -> succeeded.detail();
      case StepReply.AlreadyDone alreadyDone -> "already done (" + alreadyDone.detail() + ")";
      case StepReply.Refused refused -> "refused: " + refused.reason();
      case StepReply.Unavailable unavailable -> "unavailable: " + unavailable.reason();
    };
  }
}

package io.plainround.seatwave.saga;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * The inventory service: seats and holds in its own database, every change with its event in the
 * same commit. A hold is ACTIVE (Module 13), then CONFIRMING while a saga pays for it, then
 * CONFIRMED, or RELEASED or EXPIRED. CONFIRMING is the saga's semantic lock: the expiry sweep frees
 * only ACTIVE holds, so a seat being paid for cannot be sold to anyone else meanwhile.
 *
 * <p>Every command is idempotent: repeating one that already happened answers {@code AlreadyDone}
 * and changes nothing. Not thread-safe; in PostgreSQL the partial unique index {@code
 * one_claim_per_seat} enforces one claim per seat whatever the code does.
 */
final class InventoryService {

  private static final String HOLDS = "holds";
  private static final String SEATS = "seats";
  private static final String FREE = "";
  private static final long MILLIS_PER_SECOND = 1_000;

  private final ServiceDatabase database;
  private final long holdMillis;

  /** A fan's request to hold one seat. */
  record HoldRequest(String holdId, String seatLabel, String customerId) {
    HoldRequest {
      Objects.requireNonNull(holdId, "holdId");
      Objects.requireNonNull(seatLabel, "seatLabel");
      Objects.requireNonNull(customerId, "customerId");
    }
  }

  /** A service whose holds last {@code holdMillis} (Module 2's 300 seconds in every program). */
  InventoryService(ServiceDatabase database, long holdMillis) {
    this.database = Objects.requireNonNull(database, "database");
    this.holdMillis = holdMillis;
  }

  /** Holds a free seat for 300 seconds; refused when another hold or booking claims it. */
  StepReply hold(HoldRequest request, long nowMillis) {
    String claimedBy = seatClaim(request.seatLabel());
    // Expiry is lazy, as in Module 13: a new hold on the seat first ends an ACTIVE hold that is
    // due.
    if (!claimedBy.equals(FREE)) {
      Map<String, String> claimant = database.read(HOLDS, claimedBy).orElseThrow();
      if (claimant.get("state").equals("ACTIVE")
          && Long.parseLong(claimant.get("expires_at_ms")) <= nowMillis) {
        commit(
            new HoldChange(claimedBy, with(claimant, "EXPIRED"), FREE, "SeatReleased"), nowMillis);
        claimedBy = FREE;
      }
    }
    if (!claimedBy.equals(FREE)) {
      return new StepReply.Refused("seat " + request.seatLabel() + " is claimed by " + claimedBy);
    }
    Map<String, String> hold =
        Map.of(
            "state", "ACTIVE",
            "seat_label", request.seatLabel(),
            "customer_id", request.customerId(),
            "expires_at_ms", Long.toString(nowMillis + holdMillis),
            "saga_id", FREE);
    commit(new HoldChange(request.holdId(), hold, request.holdId(), "SeatHeld"), nowMillis);
    return new StepReply.Succeeded("ACTIVE for " + holdMillis / MILLIS_PER_SECOND + " s");
  }

  /** Saga step 1: takes the semantic lock, ACTIVE to CONFIRMING, if the hold has not expired. */
  StepReply lockForPayment(String holdId, String sagaId, long nowMillis) {
    Map<String, String> hold = database.read(HOLDS, holdId).orElse(Map.of("state", "MISSING"));
    String state = hold.get("state");
    if (state.equals("CONFIRMING") && sagaId.equals(hold.get("saga_id"))) {
      return new StepReply.AlreadyDone("locked by " + sagaId);
    }
    if (!state.equals("ACTIVE") || Long.parseLong(hold.get("expires_at_ms")) <= nowMillis) {
      return new StepReply.Refused("hold " + holdId + " is " + state + ", not an active hold");
    }
    Map<String, String> locked = with(hold, "CONFIRMING");
    locked.put("saga_id", sagaId);
    commit(new HoldChange(holdId, locked, holdId, "SeatLocked"), nowMillis);
    return new StepReply.Succeeded("CONFIRMING, expiry no longer applies");
  }

  /** Saga step 1 with no lock: only checks that the hold is ACTIVE and not past its end. */
  StepReply checkActive(String holdId, long nowMillis) {
    Map<String, String> hold = database.read(HOLDS, holdId).orElse(Map.of("state", "MISSING"));
    boolean isLiveActive =
        hold.get("state").equals("ACTIVE") && Long.parseLong(hold.get("expires_at_ms")) > nowMillis;
    return isLiveActive
        ? new StepReply.Succeeded("hold is ACTIVE (no lock taken)")
        : new StepReply.Refused("hold " + holdId + " is not an active hold");
  }

  /** Saga step 3: CONFIRMING (or, with no lock, a live ACTIVE hold) becomes CONFIRMED. */
  StepReply confirm(String holdId, long nowMillis) {
    Map<String, String> hold = database.read(HOLDS, holdId).orElse(Map.of("state", "MISSING"));
    String state = hold.get("state");
    if (state.equals("CONFIRMED")) {
      return new StepReply.AlreadyDone("CONFIRMED");
    }
    boolean isLiveActive =
        state.equals("ACTIVE") && Long.parseLong(hold.get("expires_at_ms")) > nowMillis;
    if (!state.equals("CONFIRMING") && !isLiveActive) {
      return new StepReply.Refused("seat lost: hold " + holdId + " is " + state);
    }
    commit(new HoldChange(holdId, with(hold, "CONFIRMED"), holdId, "SeatConfirmed"), nowMillis);
    return new StepReply.Succeeded("CONFIRMED");
  }

  /** Compensation: frees the seat of a hold that is still claimed; a no-op when already ended. */
  StepReply release(String holdId, long nowMillis) {
    Map<String, String> hold = database.read(HOLDS, holdId).orElse(Map.of("state", "MISSING"));
    String state = hold.get("state");
    if (state.equals("RELEASED") || state.equals("EXPIRED") || state.equals("MISSING")) {
      return new StepReply.AlreadyDone("hold already " + state.toLowerCase(Locale.ROOT));
    }
    if (state.equals("CONFIRMED")) {
      // A sold seat is never released by a compensation; only a cancellation (a new saga) may.
      return new StepReply.Refused("hold " + holdId + " is CONFIRMED");
    }
    commit(new HoldChange(holdId, with(hold, "RELEASED"), FREE, "SeatReleased"), nowMillis);
    return new StepReply.Succeeded("RELEASED, seat free");
  }

  /** The expiry sweep: every ACTIVE hold past its end becomes EXPIRED. CONFIRMING is skipped. */
  int expireDue(long nowMillis) {
    int expired = 0;
    for (Map.Entry<String, Map<String, String>> entry : database.readAll(HOLDS).entrySet()) {
      Map<String, String> hold = entry.getValue();
      if (hold.get("state").equals("ACTIVE")
          && Long.parseLong(hold.get("expires_at_ms")) <= nowMillis) {
        commit(
            new HoldChange(entry.getKey(), with(hold, "EXPIRED"), FREE, "SeatReleased"), nowMillis);
        expired++;
      }
    }
    return expired;
  }

  /** What a reader of the seat map sees: AVAILABLE, or the state of the hold that claims it. */
  String seatState(String seatLabel) {
    String claimedBy = seatClaim(seatLabel);
    if (claimedBy.equals(FREE)) {
      return "AVAILABLE";
    }
    return database.read(HOLDS, claimedBy).orElseThrow().get("state") + " by " + claimedBy;
  }

  /** The state of one hold, or MISSING. */
  String holdState(String holdId) {
    return database.read(HOLDS, holdId).map(hold -> hold.get("state")).orElse("MISSING");
  }

  private String seatClaim(String seatLabel) {
    return database.read(SEATS, seatLabel).map(seat -> seat.get("claimed_by")).orElse(FREE);
  }

  private static Map<String, String> with(Map<String, String> hold, String state) {
    Map<String, String> changed = new HashMap<>(hold);
    changed.put("state", state);
    return changed;
  }

  /** One change of one hold: its new row, who claims its seat afterwards, and the event's type. */
  private record HoldChange(
      String holdId, Map<String, String> hold, String claimedBy, String eventType) {}

  /** One local transaction: the hold, the seat's claim and the event, together. */
  private void commit(HoldChange change, long nowMillis) {
    String seatLabel = change.hold().get("seat_label");
    Map<String, String> event = new LinkedHashMap<>();
    event.put("hold_id", change.holdId());
    event.put("seat_label", seatLabel);
    event.put("state", change.hold().get("state"));
    database.commit(
        database
            .begin()
            .put(HOLDS, change.holdId(), change.hold())
            .put(SEATS, seatLabel, Map.of("claimed_by", change.claimedBy()))
            .addEvent(change.holdId(), change.eventType(), event),
        nowMillis);
  }
}

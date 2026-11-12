package io.plainround.seatwave.saga;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** The read model's version rule, and the schema check of Section 6e. */
class ReadModelTest {

  private static final Set<String> BOTH_TYPES = Set.of("BookingConfirmed", "BookingCanceled");
  private static final long SHUFFLE_SEED = 28;

  @Test
  void anOlderEventArrivingAfterANewerOneChangesNothing() {
    BookingsByCustomer model =
        new BookingsByCustomer(BookingsByCustomer.UpdateRule.HIGHER_VERSION_WINS, BOTH_TYPES);
    model.apply(event("BookingCanceled", 2));

    BookingsByCustomer.Applied older = model.apply(event("BookingConfirmed", 1));

    assertThat(older).isEqualTo(BookingsByCustomer.Applied.SKIPPED_NOT_NEWER);
    assertThat(model.confirmedCount("1042")).isZero();
  }

  @Test
  void theVersionRuleRebuildsTheSameRowsInAnyOrderAndLastAppliedDoesNot() {
    BookingHistory history = BookingHistory.generate();
    List<Map<String, String>> shuffled = new ArrayList<>(history.events());
    Collections.shuffle(shuffled, new Random(SHUFFLE_SEED));

    int withRule =
        differences(shuffled, BookingsByCustomer.UpdateRule.HIGHER_VERSION_WINS, history);
    int withoutRule =
        differences(shuffled, BookingsByCustomer.UpdateRule.LAST_APPLIED_WINS, history);

    assertThat(withRule).isZero();
    assertThat(withoutRule).isPositive();
  }

  @Test
  void addingAnOptionalFieldIsCompatibleBothWaysAndRenamingIsNot() {
    EventSchema v1 =
        new EventSchema("BookingConfirmed", 1, Map.of("booking_id", EventSchema.Presence.REQUIRED));
    EventSchema v2 = v1.plus(2, "price", EventSchema.Presence.OPTIONAL);
    EventSchema renamed = v1.renamed(2, "booking_id", "id");

    assertThat(v2.problemsReading(v1)).isEmpty();
    assertThat(v1.problemsReading(v2)).isEmpty();
    assertThat(renamed.problemsReading(v1)).hasSize(1);
    assertThat(v1.problemsReading(renamed))
        .containsExactly("v1 reader requires 'booking_id', v2 writer never writes it");
  }

  @Test
  void aMalformedLineDecodesToNothing() {
    assertThat(EventFields.decode("type=SeatHeld;broken")).isEmpty();
    assertThat(EventFields.decode("type=SeatHeld;hold_id=h-1"))
        .contains(Map.of("type", "SeatHeld", "hold_id", "h-1"));
  }

  private static int differences(
      List<Map<String, String>> events,
      BookingsByCustomer.UpdateRule rule,
      BookingHistory history) {
    BookingsByCustomer model = new BookingsByCustomer(rule, BOTH_TYPES);
    events.forEach(model::apply);
    return model.differencesFrom(history.truth());
  }

  private static Map<String, String> event(String type, int version) {
    return Map.of(
        "type", type,
        "booking_id", "b-1",
        "booking_version", Integer.toString(version),
        "customer_id", "1042",
        "seat_label", "A-7");
  }
}

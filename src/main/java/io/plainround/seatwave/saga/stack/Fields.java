package io.plainround.seatwave.saga.stack;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Reads the fields of a command's JSON body against an allow-list: each field is required, of the
 * stated form, and anything else is refused with {@link IllegalArgumentException}, which {@link
 * CommandServer} answers with 400.
 */
final class Fields {

  /** Module 7's provider key: {@code seatwave-hold-} and a hold id. */
  private static final Pattern PROVIDER_KEY =
      Pattern.compile("seatwave-hold-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");

  /** The lab's seat labels: section A, seats 1 to 40,000 (db/inventory/V4__lab_catalog.sql). */
  private static final Pattern SEAT_LABEL = Pattern.compile("A-[1-9][0-9]{0,4}");

  private static final long MAX_CUSTOMER_ID = 1_000_000;

  private Fields() {}

  static UUID uuid(JsonNode body, String name) {
    String value = body.path(name).asText("");
    try {
      UUID parsed = UUID.fromString(value);
      if (parsed.toString().equals(value)) {
        return parsed;
      }
    } catch (IllegalArgumentException notUuid) {
      // Reported below with the field's name.
    }
    throw new IllegalArgumentException(name + " must be a UUID in lower-case hex");
  }

  static String providerKey(JsonNode body) {
    String value = body.path("provider_key").asText("");
    if (!PROVIDER_KEY.matcher(value).matches()) {
      throw new IllegalArgumentException("provider_key must be seatwave-hold-<hold id>");
    }
    return value;
  }

  static String seatLabel(JsonNode body) {
    String value = body.path("seat_label").asText("");
    if (!SEAT_LABEL.matcher(value).matches()) {
      throw new IllegalArgumentException("seat_label must be A-1 to A-99999");
    }
    return value;
  }

  static long customerId(JsonNode body) {
    JsonNode value = body.path("customer_id");
    if (!value.isIntegralNumber() || value.asLong() < 1 || value.asLong() > MAX_CUSTOMER_ID) {
      throw new IllegalArgumentException("customer_id must be a whole number from 1 to 1000000");
    }
    return value.asLong();
  }
}

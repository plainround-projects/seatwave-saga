package io.plainround.seatwave.saga;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The teaching wire format of every event and row in this package: {@code name=value} pairs joined
 * by {@code ;}, in insertion order, for example {@code format=v2;type=SeatLocked;hold_id=h-1}. A
 * value may not contain {@code ;} or {@code =}; production code uses JSON or Avro with a schema
 * (Section 6e) instead.
 */
final class EventFields {

  private static final String PAIR_SEPARATOR = ";";
  private static final String NAME_VALUE_SEPARATOR = "=";

  private EventFields() {}

  /** Joins the pairs; refuses a name or value that would break the format. */
  static String encode(Map<String, String> fields) {
    StringBuilder line = new StringBuilder();
    fields.forEach((name, value) -> {
      if (name.contains(PAIR_SEPARATOR)
          || value.contains(PAIR_SEPARATOR)
          || name.contains(NAME_VALUE_SEPARATOR)
          || value.contains(NAME_VALUE_SEPARATOR)) {
        throw new IllegalArgumentException("separator inside field " + name);
      }
      if (!line.isEmpty()) {
        line.append(PAIR_SEPARATOR);
      }
      line.append(name).append(NAME_VALUE_SEPARATOR).append(value);
    });
    return line.toString();
  }

  /** Splits a line into its pairs; empty when a pair has no {@code =}, a malformed message. */
  static Optional<Map<String, String>> decode(String line) {
    Map<String, String> fields = new LinkedHashMap<>();
    for (String pair : line.split(PAIR_SEPARATOR, -1)) {
      int separatorAt = pair.indexOf(NAME_VALUE_SEPARATOR);
      if (separatorAt <= 0) {
        return Optional.empty();
      }
      fields.put(pair.substring(0, separatorAt), pair.substring(separatorAt + 1));
    }
    return Optional.of(fields);
  }
}

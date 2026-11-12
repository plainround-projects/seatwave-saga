package io.plainround.seatwave.saga.stack;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The {@code --name value} options of a program, checked against an allow-list with their defaults.
 * Every unknown name and every value out of range is reported together.
 */
final class Arguments {

  private final Map<String, String> values;

  private Arguments(Map<String, String> values) {
    this.values = Map.copyOf(values);
  }

  /** Parses {@code arguments}; {@code defaults} names every allowed option and its default. */
  static Arguments parse(List<String> arguments, Map<String, String> defaults) {
    Map<String, String> values = new HashMap<>(defaults);
    StringBuilder problems = new StringBuilder();
    for (int index = 0; index < arguments.size(); index += 2) {
      String name = arguments.get(index);
      if (!name.startsWith("--") || !defaults.containsKey(name.substring(2))) {
        problems.append(" unknown option '").append(name).append("';");
        continue;
      }
      if (index + 1 >= arguments.size()) {
        problems.append(" ").append(name).append(" needs a value;");
        continue;
      }
      values.put(name.substring(2), arguments.get(index + 1));
    }
    if (!problems.isEmpty()) {
      throw new IllegalArgumentException(
          "bad arguments:" + problems + " allowed: " + Set.copyOf(defaults.keySet()));
    }
    return new Arguments(values);
  }

  /** A whole number between {@code minimum} and {@code maximum}. */
  long number(String name, long minimum, long maximum) {
    String value = values.get(name);
    boolean isWholeNumber = value.matches("-?[0-9]{1,18}");
    long parsed = isWholeNumber ? Long.parseLong(value) : minimum - 1;
    if (isWholeNumber && parsed >= minimum && parsed <= maximum) {
      return parsed;
    }
    throw new IllegalArgumentException(
        String.format(
            Locale.ROOT,
            "--%s must be a whole number from %d to %d, not '%s'",
            name,
            minimum,
            maximum,
            value));
  }

  /** One of {@code allowed}. */
  String choice(String name, Set<String> allowed) {
    String value = values.get(name);
    if (!allowed.contains(value)) {
      throw new IllegalArgumentException(
          "--" + name + " must be one of " + allowed + ", not '" + value + "'");
    }
    return value;
  }

  /** Text of 1 to 64 letters, digits, dots and dashes. */
  String name(String name) {
    String value = values.get(name);
    if (!value.matches("[A-Za-z0-9.-]{1,64}")) {
      throw new IllegalArgumentException(
          "--" + name + " must be 1 to 64 letters, digits, dots or dashes");
    }
    return value;
  }
}

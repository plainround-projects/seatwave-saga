package io.plainround.seatwave.saga;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * One version of one event type's schema: its fields, each required or optional. A reader built on
 * one version can read an event written with another when every field the reader requires is one
 * the writer always writes. Fields the reader does not know are ignored (the tolerant reader), so
 * adding an optional field breaks no one, and renaming or newly requiring a field does.
 */
record EventSchema(String eventType, int version, Map<String, Presence> fields) {

  /** Whether a writer always writes a field, or only sometimes. */
  enum Presence {
    REQUIRED,
    OPTIONAL
  }

  EventSchema {
    Objects.requireNonNull(eventType, "eventType");
    if (version < 1) {
      throw new IllegalArgumentException("version must be at least 1: " + version);
    }
    // Insertion order is kept, so problems print in the schema's own field order on every run.
    fields = Collections.unmodifiableMap(new LinkedHashMap<>(fields));
  }

  /** A new version with one more field; the rest unchanged. */
  EventSchema plus(int newVersion, String field, Presence presence) {
    Map<String, Presence> changed = new LinkedHashMap<>(fields);
    changed.put(field, presence);
    return new EventSchema(eventType, newVersion, changed);
  }

  /** A new version with a field renamed, keeping its presence. */
  EventSchema renamed(int newVersion, String oldName, String newName) {
    Map<String, Presence> changed = new LinkedHashMap<>(fields);
    changed.put(newName, changed.remove(oldName));
    return new EventSchema(eventType, newVersion, changed);
  }

  /**
   * What breaks when a reader built on this schema reads events written with {@code writer}: every
   * field this reader requires that the writer does not always write. Empty means it works.
   */
  List<String> problemsReading(EventSchema writer) {
    List<String> problems = new ArrayList<>();
    fields.forEach((field, presence) -> {
      if (presence == Presence.REQUIRED && writer.fields().get(field) != Presence.REQUIRED) {
        problems.add(
            "v"
                + version
                + " reader requires '"
                + field
                + "', v"
                + writer.version()
                + " writer "
                + (writer.fields().containsKey(field) ? "may omit it" : "never writes it"));
      }
    });
    return problems;
  }

  /** The required fields missing from one decoded event: empty means this reader can use it. */
  List<String> missingFrom(Map<String, String> event) {
    return fields.entrySet().stream()
        .filter(
            entry -> entry.getValue() == Presence.REQUIRED && !event.containsKey(entry.getKey()))
        .map(Map.Entry::getKey)
        .sorted()
        .toList();
  }
}

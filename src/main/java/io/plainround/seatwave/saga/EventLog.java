package io.plainround.seatwave.saga;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * An in-process broker: named topics split into partitions, each partition an append-only list of
 * records numbered by offset, plus each consumer group's committed offset. Adapted from Module 27's
 * {@code PartitionedLog} (code seed {@code _tools/module-27-code/}): topics, partitions chosen by
 * key, offsets and committed offsets are kept; headers, the idempotent append and the murmur2 hash
 * are dropped, and a switch that takes the broker down is added.
 *
 * <p>Not thread-safe: every program drives it from one thread, which keeps the runs deterministic.
 */
final class EventLog {

  private final Map<String, List<List<Record>>> topics = new HashMap<>();
  private final Map<String, Long> committedOffsets = new HashMap<>();
  private boolean isDown;

  /** One stored message: where it lives (topic, partition, offset), its key and its value. */
  record Record(String topic, int partition, long offset, String key, String value) {
    Record {
      Objects.requireNonNull(topic, "topic");
      Objects.requireNonNull(key, "key");
      Objects.requireNonNull(value, "value");
    }
  }

  /** Creates a topic; refuses a name that exists or a partition count below 1. */
  void createTopic(String topic, int partitionCount) {
    if (partitionCount < 1 || topics.containsKey(topic)) {
      throw new IllegalArgumentException("bad topic or partition count: " + topic);
    }
    List<List<Record>> partitions = new ArrayList<>();
    for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
      partitions.add(new ArrayList<>());
    }
    topics.put(topic, partitions);
  }

  /** From now on every append is refused, as when no broker answers. */
  void goDown() {
    isDown = true;
  }

  /** Appends are accepted again. */
  void comeBack() {
    isDown = false;
  }

  /**
   * Appends to the key's partition. Empty when the broker is down: the caller learns that nothing
   * was stored and must keep the message to try again.
   */
  Optional<Record> append(String topic, String key, String value) {
    if (isDown) {
      return Optional.empty();
    }
    List<List<Record>> partitions = partitionsOf(topic);
    // The same key always lands in the same partition, which is all that per-key order needs;
    // Kafka hashes with murmur2 instead of String.hashCode.
    int partition = Math.floorMod(key.hashCode(), partitions.size());
    List<Record> partitionRecords = partitions.get(partition);
    Record record = new Record(topic, partition, partitionRecords.size(), key, value);
    partitionRecords.add(record);
    return Optional.of(record);
  }

  int partitionCount(String topic) {
    return partitionsOf(topic).size();
  }

  /** Up to {@code maxRecords} records of one partition, starting at {@code fromOffset}. */
  List<Record> fetch(String topic, int partition, long fromOffset, int maxRecords) {
    List<Record> partitionRecords = partitionsOf(topic).get(partition);
    int start = (int) Math.min(fromOffset, partitionRecords.size());
    int end = (int) Math.min((long) start + maxRecords, partitionRecords.size());
    return List.copyOf(partitionRecords.subList(start, end));
  }

  /** The log end offset: the offset the next record of this partition will get. */
  long endOffset(String topic, int partition) {
    return partitionsOf(topic).get(partition).size();
  }

  /** Every record of a topic, partition by partition, each partition in offset order. */
  List<Record> readFromStart(String topic) {
    List<Record> everything = new ArrayList<>();
    partitionsOf(topic).forEach(everything::addAll);
    return List.copyOf(everything);
  }

  /** Stores a group's position: the offset of the next record it will read. */
  void commit(String group, String topic, int partition, long nextOffset) {
    if (nextOffset < 0 || nextOffset > endOffset(topic, partition)) {
      throw new IllegalArgumentException("offset outside the partition: " + nextOffset);
    }
    committedOffsets.put(group + "|" + topic + "|" + partition, nextOffset);
  }

  /** The group's committed offset; 0 (the earliest record) when it never committed. */
  long committed(String group, String topic, int partition) {
    return committedOffsets.getOrDefault(group + "|" + topic + "|" + partition, 0L);
  }

  private List<List<Record>> partitionsOf(String topic) {
    List<List<Record>> partitions = topics.get(topic);
    if (partitions == null) {
      throw new IllegalArgumentException("no such topic: " + topic);
    }
    return partitions;
  }
}

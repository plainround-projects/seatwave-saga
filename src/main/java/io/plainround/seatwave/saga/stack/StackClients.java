package io.plainround.seatwave.saga.stack;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * The Kafka side of the drills: create a topic on purpose (auto-creation is off), and read a whole
 * topic from its first record to the end offsets it had when the read started.
 */
public final class StackClients {

  private static final Duration POLL = Duration.ofMillis(200);
  private static final int TOPIC_PARTITIONS = 24;
  private static final short ONE_REPLICA = 1;

  private StackClients() {}

  /** Creates a topic of 24 partitions and one replica if it does not exist. */
  public static void createTopic(String bootstrap, String topic, Duration timeout)
      throws InterruptedException, TimeoutException {
    try (Admin admin =
        Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
      admin
          .createTopics(List.of(new NewTopic(topic, TOPIC_PARTITIONS, ONE_REPLICA)))
          .all()
          .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (ExecutionException failed) {
      if (!(failed.getCause() instanceof TopicExistsException)) {
        throw new IllegalStateException("cannot create topic " + topic, failed.getCause());
      }
    }
    awaitLeaders(bootstrap, topic, timeout);
  }

  /**
   * Waits until every partition of a new topic has a leader: a producer that sends earlier gets
   * NOT_LEADER_OR_FOLLOWER and retries, which a drill would count in its timings.
   */
  private static void awaitLeaders(String bootstrap, String topic, Duration timeout)
      throws InterruptedException, TimeoutException {
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    try (KafkaConsumer<String, String> consumer = reader(bootstrap)) {
      while (true) {
        List<PartitionInfo> partitions = consumer.partitionsFor(topic, timeout);
        if (partitions.size() == TOPIC_PARTITIONS
            && partitions.stream().allMatch(partition -> partition.leader() != null)) {
          return;
        }
        if (System.nanoTime() > deadlineNanos) {
          throw new TimeoutException("topic " + topic + " has partitions without a leader");
        }
        Thread.sleep(POLL);
      }
    }
  }

  /** A consumer with no group: it reads by assignment and commits nothing. The caller closes it. */
  public static KafkaConsumer<String, String> reader(String bootstrap) {
    Map<String, Object> config = new HashMap<>();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    return new KafkaConsumer<>(config);
  }

  /** Every record of {@code topic}, partition by partition, up to the end offsets of now. */
  public static List<ConsumerRecord<String, String>> readAll(String bootstrap, String topic) {
    try (KafkaConsumer<String, String> consumer = reader(bootstrap)) {
      List<TopicPartition> partitions = new ArrayList<>();
      for (PartitionInfo partition : consumer.partitionsFor(topic)) {
        partitions.add(new TopicPartition(topic, partition.partition()));
      }
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      Map<TopicPartition, Long> ends = new HashMap<>(consumer.endOffsets(partitions));
      List<ConsumerRecord<String, String>> records = new ArrayList<>();
      while (ends.entrySet().stream()
          .anyMatch(end -> consumer.position(end.getKey()) < end.getValue())) {
        consumer.poll(POLL).forEach(records::add);
      }
      return records;
    }
  }
}

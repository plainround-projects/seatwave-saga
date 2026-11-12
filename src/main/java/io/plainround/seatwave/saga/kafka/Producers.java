package io.plainround.seatwave.saga.kafka;

import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * The one way this project builds a Kafka producer: {@code acks=all} and idempotence on, as Module
 * 27 set them, so a retried send is stored once and keeps its order within a partition, and a send
 * that cannot reach the broker fails within {@code timeout} instead of blocking.
 */
public final class Producers {

  /** Module 27's linger: a pass's rows leave in a few batches rather than one record each. */
  private static final int LINGER_MILLIS = 5;

  private Producers() {}

  /** Kafka's default and its limit with idempotence: five requests in flight a connection. */
  private static final int MAX_IN_FLIGHT = 5;

  /** A producer the caller owns and closes once, at shutdown. */
  public static KafkaProducer<String, String> create(
      String bootstrap, String clientId, Duration timeout) {
    return create(bootstrap, clientId, timeout, MAX_IN_FLIGHT);
  }

  /**
   * The same producer with at most {@code maxInFlight} requests in flight a connection. The rebuild
   * drill's loader uses 1: it publishes a whole topic right after creating it, when the one broker
   * still answers NOT_LEADER_OR_FOLLOWER for a moment, and with several batches in flight the
   * refused first one leaves the later ones out of sequence, refused again and again.
   */
  public static KafkaProducer<String, String> create(
      String bootstrap, String clientId, Duration timeout, int maxInFlight) {
    int timeoutMillis = Math.toIntExact(timeout.toMillis());
    Map<String, Object> config =
        Map.ofEntries(
            Map.entry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap),
            Map.entry(ProducerConfig.CLIENT_ID_CONFIG, clientId),
            Map.entry(ProducerConfig.ACKS_CONFIG, "all"),
            Map.entry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true),
            Map.entry(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlight),
            Map.entry(ProducerConfig.LINGER_MS_CONFIG, LINGER_MILLIS),
            // A send waits at most this long for metadata or buffer space. Requests and
            // deliveries keep Kafka's defaults (30 s and 120 s): a batch that expires early leaves
            // a gap in the idempotent producer's sequence, which the broker then refuses again.
            Map.entry(ProducerConfig.MAX_BLOCK_MS_CONFIG, timeoutMillis),
            Map.entry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class),
            Map.entry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
    return new KafkaProducer<>(config);
  }
}

package io.plainround.seatwave.saga.stack;

import com.zaxxer.hikari.HikariDataSource;
import io.plainround.seatwave.saga.kafka.PostgresKafkaRelay;
import io.plainround.seatwave.saga.kafka.Producers;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * One relay on its own, for the drills: polls one service database's outbox and publishes every row
 * to one topic. With {@code --pause-on-pass N} it stops between the broker's acknowledgement and
 * the mark of its Nth pass that published anything, with a {@code PAUSED} line, so the drill kills
 * it exactly there and restarts it with the next fencing token. With {@code --until-drained yes} it
 * exits after a pass that found nothing, and prints what it did and how long it took.
 *
 * <p>Usage: {@code RelayCli --database booking|inventory|payment --topic T [--fencing-token N]
 * [--pause-on-pass N] [--until-drained yes|no] [--batch-size N]}
 */
public final class RelayCli {

  private static final long MAX_TOKEN = 1_000_000;
  private static final long MAX_PASS = 1_000_000;
  private static final long MAX_BATCH = 10_000;
  private static final double NANOS_PER_SECOND = 1e9;

  private RelayCli() {}

  /** Runs the relay. */
  public static void main(String[] arguments) throws Exception {
    StackSettings settings = StackSettings.load();
    Arguments options =
        Arguments.parse(
            List.of(arguments),
            Map.of(
                "database", "",
                "topic", "",
                "fencing-token", "1",
                "pause-on-pass", "0",
                "until-drained", "no",
                "batch-size", Integer.toString(settings.relay().batchSize())));
    String service = options.choice("database", Set.of("booking", "inventory", "payment"));
    String topic = options.name("topic");
    long token = options.number("fencing-token", 1, MAX_TOKEN);
    long pauseOnPass = options.number("pause-on-pass", 0, MAX_PASS);
    boolean isUntilDrained = options.choice("until-drained", Set.of("yes", "no")).equals("yes");
    int batchSize = Math.toIntExact(options.number("batch-size", 1, MAX_BATCH));
    String url =
        switch (service) {
          case "booking" -> settings.databases().bookingUrl();
          case "inventory" -> settings.databases().inventoryUrl();
          default -> settings.databases().paymentUrl();
        };
    AtomicInteger publishingPasses = new AtomicInteger();
    try (HikariDataSource database = settings.openPool(service + "-relay", url);
        KafkaProducer<String, String> producer =
            Producers.create(
                settings.kafkaBootstrap(),
                service + "-relay-" + token,
                settings.timeouts().stack())) {
      PostgresKafkaRelay relay =
          new PostgresKafkaRelay(
              database,
              producer,
              new PostgresKafkaRelay.Settings(
                  service, topic, Map.of(), token, batchSize, settings.timeouts().stack()),
              rowCount -> {
                if (publishingPasses.incrementAndGet() == pauseOnPass) {
                  Pause.untilKilled(
                      "relay token "
                          + token
                          + " pass "
                          + pauseOnPass
                          + ": "
                          + rowCount
                          + " rows acknowledged by the broker, not marked");
                }
              });
      run(relay, settings, isUntilDrained, token);
    }
  }

  private static void run(
      PostgresKafkaRelay relay, StackSettings settings, boolean isUntilDrained, long token)
      throws Exception {
    long published = 0;
    long marked = 0;
    int passes = 0;
    long startNanos = System.nanoTime();
    long lastPublishNanos = startNanos;
    System.out.println("relay token " + token + " started, pid " + ProcessHandle.current().pid());
    while (true) {
      PostgresKafkaRelay.Pass pass = relay.relayOnce();
      passes++;
      published += pass.published();
      marked += pass.marked();
      if (pass.published() > 0) {
        lastPublishNanos = System.nanoTime();
      }
      if (pass.isFenced()) {
        System.out.println("relay token " + token + " is fenced by a newer relay: stopping");
        break;
      }
      if (pass.published() == 0 && isUntilDrained) {
        break;
      }
      if (pass.published() < settings.relay().batchSize()) {
        Thread.sleep(settings.relay().pollInterval());
      }
    }
    double seconds = (lastPublishNanos - startNanos) / NANOS_PER_SECOND;
    System.out.println(
        String.format(
            Locale.ROOT,
            "relay token %d: %d passes, %,d rows published, %,d marked, in %.3f s (%,.0f rows a"
                + " second)",
            token,
            passes,
            published,
            marked,
            seconds,
            seconds > 0 ? published / seconds : 0));
  }
}

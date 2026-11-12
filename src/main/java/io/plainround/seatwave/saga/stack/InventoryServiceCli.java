package io.plainround.seatwave.saga.stack;

import com.zaxxer.hikari.HikariDataSource;
import io.plainround.seatwave.saga.jdbc.JdbcInventory;
import io.plainround.seatwave.saga.kafka.PostgresKafkaRelay;
import io.plainround.seatwave.saga.kafka.Producers;
import io.plainround.seatwave.saga.kafka.RelayLoop;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The inventory service of the stack: the saga's seat commands over HTTP on {@link JdbcInventory},
 * its outbox relay to {@code seatwave.inventory}, and the expiry sweep every second. Runs until
 * killed.
 *
 * <p>Usage: {@code InventoryServiceCli [--hold-seconds N] [--fencing-token N]}
 */
public final class InventoryServiceCli {

  private static final Logger LOG = LoggerFactory.getLogger(InventoryServiceCli.class);
  private static final Duration SWEEP_INTERVAL = Duration.ofSeconds(1);
  private static final long MAX_TOKEN = 1_000_000;
  private static final long MAX_HOLD_SECONDS = 3_600;

  private InventoryServiceCli() {}

  /** Starts the service. */
  public static void main(String[] arguments) throws Exception {
    StackSettings settings = StackSettings.load();
    Arguments options =
        Arguments.parse(
            List.of(arguments),
            Map.of(
                "hold-seconds",
                Long.toString(settings.holdDuration().toSeconds()),
                "fencing-token",
                "1"));
    Duration holdDuration = Duration.ofSeconds(options.number("hold-seconds", 1, MAX_HOLD_SECONDS));
    long token = options.number("fencing-token", 1, MAX_TOKEN);
    try (HikariDataSource database =
            settings.openPool("inventory", settings.databases().inventoryUrl());
        KafkaProducer<String, String> producer =
            Producers.create(
                settings.kafkaBootstrap(), "inventory-relay", settings.timeouts().stack())) {
      JdbcInventory inventory = new JdbcInventory(database, holdDuration);
      RelayLoop relay =
          new RelayLoop(
              new PostgresKafkaRelay(
                  database,
                  producer,
                  new PostgresKafkaRelay.Settings(
                      "inventory",
                      "seatwave.inventory",
                      Map.of(),
                      token,
                      settings.relay().batchSize(),
                      settings.timeouts().stack())),
              settings.relay().pollInterval(),
              settings.relay().batchSize());
      Thread.ofVirtual().name("inventory-relay").start(relay);
      Thread.ofVirtual().name("expiry-sweep").start(() -> sweep(inventory));
      try (CommandServer server =
          CommandServer.start(
              settings.services().inventory().getPort(),
              Map.of(
                  "/commands/hold",
                  body ->
                      inventory.hold(
                          Fields.uuid(body, "hold_id"),
                          Fields.seatLabel(body),
                          Fields.customerId(body)),
                  "/commands/lock-seat",
                  body ->
                      inventory.lockForPayment(
                          Fields.uuid(body, "hold_id"), Fields.uuid(body, "saga_id")),
                  "/commands/check-seat",
                  body -> inventory.checkActive(Fields.uuid(body, "hold_id")),
                  "/commands/confirm-seat",
                  body -> inventory.confirm(Fields.uuid(body, "hold_id")),
                  "/commands/release-seat",
                  body -> inventory.release(Fields.uuid(body, "hold_id"))))) {
        System.out.println(
            "inventory service on port "
                + server.port()
                + ", holds of "
                + holdDuration.toSeconds()
                + " s, relay token "
                + token
                + ", pid "
                + ProcessHandle.current().pid());
        Thread.currentThread().join();
      }
    }
  }

  private static void sweep(JdbcInventory inventory) {
    while (!Thread.currentThread().isInterrupted()) {
      try {
        int expired = inventory.expireDue();
        if (expired > 0) {
          LOG.info("expiry sweep ended {} hold(s)", expired);
        }
        Thread.sleep(SWEEP_INTERVAL);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      } catch (java.sql.SQLException failed) {
        LOG.warn("expiry sweep failed, retrying: {}", failed.toString());
      }
    }
  }
}

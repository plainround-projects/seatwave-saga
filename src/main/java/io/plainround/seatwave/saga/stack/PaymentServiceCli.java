package io.plainround.seatwave.saga.stack;

import com.zaxxer.hikari.HikariDataSource;
import io.plainround.seatwave.saga.jdbc.JdbcPayments;
import io.plainround.seatwave.saga.kafka.PostgresKafkaRelay;
import io.plainround.seatwave.saga.kafka.Producers;
import io.plainround.seatwave.saga.kafka.RelayLoop;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * The payment service of the stack: the saga's charge and refund over HTTP on {@link JdbcPayments},
 * which calls the provider's stub, and its outbox relay to {@code seatwave.payments}. Runs until
 * killed.
 *
 * <p>Usage: {@code PaymentServiceCli [--fencing-token N]}
 */
public final class PaymentServiceCli {

  private static final long MAX_TOKEN = 1_000_000;

  private PaymentServiceCli() {}

  /** Starts the service. */
  public static void main(String[] arguments) throws Exception {
    StackSettings settings = StackSettings.load();
    Arguments options = Arguments.parse(List.of(arguments), Map.of("fencing-token", "1"));
    long token = options.number("fencing-token", 1, MAX_TOKEN);
    try (HikariDataSource database =
            settings.openPool("payment", settings.databases().paymentUrl());
        KafkaProducer<String, String> producer =
            Producers.create(
                settings.kafkaBootstrap(), "payment-relay", settings.timeouts().stack());
        ProviderClient provider =
            new ProviderClient(settings.services().provider(), settings.timeouts().provider())) {
      JdbcPayments payments = new JdbcPayments(database, provider, settings.seatPrice());
      RelayLoop relay =
          new RelayLoop(
              new PostgresKafkaRelay(
                  database,
                  producer,
                  new PostgresKafkaRelay.Settings(
                      "payment",
                      "seatwave.payments",
                      Map.of(),
                      token,
                      settings.relay().batchSize(),
                      settings.timeouts().stack())),
              settings.relay().pollInterval(),
              settings.relay().batchSize());
      Thread.ofVirtual().name("payment-relay").start(relay);
      try (CommandServer server =
          CommandServer.start(
              settings.services().payment().getPort(),
              Map.of(
                  "/commands/charge", body -> payments.charge(Fields.providerKey(body)),
                  "/commands/refund", body -> payments.refund(Fields.providerKey(body))))) {
        System.out.println(
            "payment service on port "
                + server.port()
                + ", provider "
                + settings.services().provider()
                + ", relay token "
                + token
                + ", pid "
                + ProcessHandle.current().pid());
        Thread.currentThread().join();
      }
    }
  }
}

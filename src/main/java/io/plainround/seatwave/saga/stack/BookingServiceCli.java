package io.plainround.seatwave.saga.stack;

import com.zaxxer.hikari.HikariDataSource;
import io.plainround.seatwave.saga.BookingSaga;
import io.plainround.seatwave.saga.SagaRecord;
import io.plainround.seatwave.saga.SagaStore;
import io.plainround.seatwave.saga.StepReply;
import io.plainround.seatwave.saga.jdbc.JdbcSagaStore;
import io.plainround.seatwave.saga.kafka.PostgresKafkaRelay;
import io.plainround.seatwave.saga.kafka.Producers;
import io.plainround.seatwave.saga.kafka.RelayLoop;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The booking service of the stack: the orchestrator ({@link BookingSaga} on {@link JdbcSagaStore},
 * talking to inventory and payment over HTTP), its outbox relay (final events to {@code
 * seatwave.bookings}, commands to {@code seatwave.saga.commands}), and the sweeper. On start it
 * resumes every saga that is not final, which is how a killed orchestrator finishes its work; the
 * sweeper resumes any saga older than the stuck-saga threshold. {@code POST /commands/book} with
 * {@code booking_id} and {@code hold_id} starts a saga (or resumes the one with that id) and
 * answers when it is final: 200 with COMPLETED, or 409 with COMPENSATED. Runs until killed.
 *
 * <p>Usage: {@code BookingServiceCli [--protection SEMANTIC_LOCK|NONE] [--pause-in STATE|NONE]
 * [--fencing-token N] [--sweep-seconds N] [--stuck-seconds N]}
 */
public final class BookingServiceCli {

  private static final Logger LOG = LoggerFactory.getLogger(BookingServiceCli.class);
  private static final long MAX_TOKEN = 1_000_000;
  private static final long MAX_SECONDS = 3_600;
  private static final Set<String> FINAL_EVENTS = Set.of("BookingConfirmed", "BookingFailed");

  private final BookingSaga saga;
  private final Clock clock;
  private final Map<String, Object> sagaLocks = new ConcurrentHashMap<>();

  private BookingServiceCli(BookingSaga saga, Clock clock) {
    this.saga = Objects.requireNonNull(saga, "saga");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  /** Starts the service. */
  public static void main(String[] arguments) throws Exception {
    StackSettings settings = StackSettings.load();
    Arguments options =
        Arguments.parse(
            List.of(arguments),
            Map.of(
                "protection", "SEMANTIC_LOCK",
                "pause-in", "NONE",
                "fencing-token", "1",
                "sweep-seconds", "60",
                "stuck-seconds", Long.toString(settings.holdDuration().toSeconds())));
    BookingSaga.SeatProtection protection =
        BookingSaga.SeatProtection.valueOf(
            options.choice(
                "protection",
                Arrays.stream(BookingSaga.SeatProtection.values())
                    .map(Enum::name)
                    .collect(Collectors.toSet())));
    Set<String> pausable =
        Arrays.stream(BookingSaga.State.values()).map(Enum::name).collect(Collectors.toSet());
    pausable.add("NONE");
    String pauseIn = options.choice("pause-in", pausable);
    long token = options.number("fencing-token", 1, MAX_TOKEN);
    Duration sweepInterval = Duration.ofSeconds(options.number("sweep-seconds", 1, MAX_SECONDS));
    Duration stuckAfter = Duration.ofSeconds(options.number("stuck-seconds", 1, MAX_SECONDS));
    try (HikariDataSource database =
            settings.openPool("booking", settings.databases().bookingUrl());
        KafkaProducer<String, String> producer =
            Producers.create(
                settings.kafkaBootstrap(), "booking-relay", settings.timeouts().stack());
        HttpParticipants participants =
            new HttpParticipants(
                settings.services().inventory(),
                settings.services().payment(),
                settings.timeouts().stack(),
                settings.timeouts().paymentStep())) {
      SagaStore store = new JdbcSagaStore(database, settings.seatPrice());
      if (!pauseIn.equals("NONE")) {
        store = new PausingSagaStore(store, BookingSaga.State.valueOf(pauseIn));
      }
      Map<String, String> finalEventsToBookings =
          FINAL_EVENTS.stream()
              .collect(Collectors.toMap(type -> type, type -> "seatwave.bookings"));
      RelayLoop relay =
          new RelayLoop(
              new PostgresKafkaRelay(
                  database,
                  producer,
                  new PostgresKafkaRelay.Settings(
                      "booking",
                      "seatwave.saga.commands",
                      finalEventsToBookings,
                      token,
                      settings.relay().batchSize(),
                      settings.timeouts().stack())),
              settings.relay().pollInterval(),
              settings.relay().batchSize());
      Thread.ofVirtual().name("booking-relay").start(relay);
      BookingServiceCli service =
          new BookingServiceCli(
              new BookingSaga(
                  store,
                  participants,
                  new BookingSaga.Settings(
                      protection,
                      settings.retry().firstRetryMillis(),
                      settings.retry().maxRetryMillis())),
              Clock.systemUTC());
      SagaStore sweptStore = store;
      List<SagaRecord> open = store.openOlderThan(service.clock.millis(), 0);
      System.out.println("booking service resuming " + open.size() + " open saga(s)");
      for (SagaRecord saga : open) {
        Thread.ofVirtual().start(() -> service.driveQuietly(saga.bookingId()));
      }
      Thread.ofVirtual()
          .name("sweeper")
          .start(() -> service.sweep(sweptStore, sweepInterval, stuckAfter));
      try (CommandServer server =
          CommandServer.start(
              settings.services().booking().getPort(),
              Map.of(
                  "/commands/book",
                  body ->
                      service.book(
                          Fields.uuid(body, "booking_id"), Fields.uuid(body, "hold_id"))))) {
        System.out.println(
            "booking service on port "
                + server.port()
                + ", "
                + protection
                + ", pause in "
                + pauseIn
                + ", relay token "
                + token
                + ", pid "
                + ProcessHandle.current().pid());
        Thread.currentThread().join();
      }
    }
  }

  /** Starts the saga if it is new, then drives it to its end. */
  private StepReply book(UUID bookingId, UUID holdId) throws InterruptedException {
    String sagaId = bookingId.toString();
    synchronized (lockFor(sagaId)) {
      // A client that sends the same booking again resumes it rather than starting a second saga.
      if (!saga.exists(sagaId)) {
        saga.start(sagaId, holdId.toString(), clock.millis());
      }
    }
    BookingSaga.State end = drive(sagaId);
    return end == BookingSaga.State.COMPLETED
        ? new StepReply.Succeeded(end.name())
        : new StepReply.Refused(end.name());
  }

  /** Steps one saga until it is final, waiting each retry's backoff; one driver per saga. */
  private BookingSaga.State drive(String sagaId) throws InterruptedException {
    synchronized (lockFor(sagaId)) {
      while (!saga.state(sagaId).isFinal()) {
        BookingSaga.StepResult result = saga.step(sagaId, clock.millis());
        if (result.from() == result.to()) {
          LOG.info("saga {} stays in {}: {}", sagaId, result.from(), result.reply());
          Thread.sleep(Math.max(0, result.retryAtMillis() - clock.millis()));
        }
      }
      return saga.state(sagaId);
    }
  }

  private void driveQuietly(String sagaId) {
    try {
      LOG.info("saga {} resumed, ended {}", sagaId, drive(sagaId));
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    } catch (RuntimeException failed) {
      LOG.warn("saga {} could not be resumed now: {}", sagaId, failed.toString());
    }
  }

  private void sweep(SagaStore store, Duration interval, Duration stuckAfter) {
    while (!Thread.currentThread().isInterrupted()) {
      try {
        Thread.sleep(interval);
        for (SagaRecord stuck : store.openOlderThan(clock.millis(), stuckAfter.toMillis())) {
          LOG.warn("stuck saga {} in {}: resuming", stuck.bookingId(), stuck.state());
          Thread.ofVirtual().start(() -> driveQuietly(stuck.bookingId()));
        }
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      } catch (RuntimeException failed) {
        LOG.warn("sweep failed, retrying: {}", failed.toString());
      }
    }
  }

  private Object lockFor(String sagaId) {
    return sagaLocks.computeIfAbsent(sagaId, unused -> new Object());
  }
}

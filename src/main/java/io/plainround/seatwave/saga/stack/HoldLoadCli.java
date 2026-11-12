package io.plainround.seatwave.saga.stack;

import com.zaxxer.hikari.HikariDataSource;
import io.plainround.seatwave.saga.StepReply;
import io.plainround.seatwave.saga.jdbc.JdbcInventory;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lab 1 on the real stack: holds new seats through {@link JdbcInventory}, each hold with its {@code
 * SeatHeld} outbox row in one commit, at {@code --rate} holds a second for {@code --seconds}, one
 * seat and one customer per hold, while the inventory service's relay publishes. Every second it
 * prints the holds committed, the unsent outbox rows and the relay lag (the age of the oldest
 * unsent row, Section 6b's query), and it keeps sampling for {@code --drain-seconds} after the last
 * hold. scripts/drill.sh stops the broker in the middle.
 *
 * <p>Usage: {@code HoldLoadCli --rate R --seconds S --drain-seconds D}
 */
public final class HoldLoadCli {

  private static final long MAX_RATE = 5_000;
  private static final long MAX_SECONDS = 600;
  private static final int MAX_IN_FLIGHT = 32;
  private static final double NANOS_PER_SECOND = 1e9;
  private static final long MAX_SEATS = 40_000;

  /** Section 6b's lag query, verbatim. */
  private static final String LAG =
      "SELECT count(*), coalesce(extract(epoch FROM now() - min(created_at)), 0)"
          + " FROM outbox WHERE published_at IS NULL";

  private HoldLoadCli() {}

  /** Runs the load and prints one line a second. */
  public static void main(String[] arguments) throws Exception {
    StackSettings settings = StackSettings.load();
    Arguments options =
        Arguments.parse(
            List.of(arguments), Map.of("rate", "500", "seconds", "60", "drain-seconds", "20"));
    long rate = options.number("rate", 1, MAX_RATE);
    long seconds = options.number("seconds", 1, MAX_SECONDS);
    long drainSeconds = options.number("drain-seconds", 0, MAX_SECONDS);
    if (rate * seconds > MAX_SEATS) {
      throw new IllegalArgumentException("rate x seconds must be at most the lab's 40,000 seats");
    }
    AtomicInteger committed = new AtomicInteger();
    AtomicInteger refused = new AtomicInteger();
    try (HikariDataSource database =
        settings.openPool("hold-load", settings.databases().inventoryUrl())) {
      JdbcInventory inventory = new JdbcInventory(database, settings.holdDuration());
      long startNanos = System.nanoTime();
      Thread sampler =
          Thread.ofPlatform()
              .start(() -> sample(database, startNanos, seconds + drainSeconds, committed));
      Semaphore inFlight = new Semaphore(MAX_IN_FLIGHT);
      long intervalNanos = (long) (NANOS_PER_SECOND / rate);
      try (ExecutorService holds = Executors.newVirtualThreadPerTaskExecutor()) {
        for (long holdIndex = 0; holdIndex < rate * seconds; holdIndex++) {
          long waitNanos = startNanos + holdIndex * intervalNanos - System.nanoTime();
          if (waitNanos > 0) {
            TimeUnit.NANOSECONDS.sleep(waitNanos);
          }
          long seatNumber = holdIndex + 1;
          inFlight.acquire();
          holds.submit(() -> {
            try {
              StepReply reply = inventory.hold(UUID.randomUUID(), "A-" + seatNumber, seatNumber);
              (reply instanceof StepReply.Succeeded ? committed : refused).incrementAndGet();
            } catch (SQLException failed) {
              refused.incrementAndGet();
            } finally {
              inFlight.release();
            }
          });
        }
      }
      sampler.join();
      System.out.println(
          String.format(
              Locale.ROOT,
              "holds committed %,d, refused or failed %,d, offered %,d at %,d a second",
              committed.get(),
              refused.get(),
              rate * seconds,
              rate));
    }
  }

  private static void sample(
      HikariDataSource database, long startNanos, long totalSeconds, AtomicInteger committed) {
    try (Connection connection = database.getConnection()) {
      for (long second = 1; second <= totalSeconds; second++) {
        long waitNanos = startNanos + second * (long) NANOS_PER_SECOND - System.nanoTime();
        if (waitNanos > 0) {
          TimeUnit.NANOSECONDS.sleep(waitNanos);
        }
        try (Statement statement = connection.createStatement();
            ResultSet row = statement.executeQuery(LAG)) {
          row.next();
          System.out.println(
              String.format(
                  Locale.ROOT,
                  "t=%3d s  holds committed %,6d  unsent outbox rows %,6d  relay lag %5.1f s",
                  second,
                  committed.get(),
                  row.getLong(1),
                  row.getDouble(2)));
        }
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    } catch (SQLException failed) {
      System.out.println("sampling stopped: " + failed.getMessage());
    }
  }
}

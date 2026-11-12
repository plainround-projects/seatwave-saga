package io.plainround.seatwave.saga.stack;

import com.zaxxer.hikari.HikariDataSource;
import io.plainround.seatwave.saga.jdbc.Outbox;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

/**
 * Measures the outbox and its relay on the booking database (Section 6b's numbers), with a relay
 * (RelayCli) running beside it:
 *
 * <ul>
 *   <li>{@code delay --rate R --seconds S --topic T}: commits one outbox row per transaction at R a
 *       second (open loop: each row's time is fixed in advance, so a slow commit does not slow the
 *       offered rate), and a consumer on T records when each arrives; prints the delay from the
 *       commit's return to the record's arrival, p50, p99 and the largest, both clocks being this
 *       process's.
 *   <li>{@code fill --rows N}: inserts N unpublished rows in one statement, for the relay's
 *       throughput (RelayCli --until-drained prints rows a second).
 *   <li>{@code size [--delete-published yes]}: the outbox's rows and bytes, and the clean-up of
 *       published rows with its time.
 * </ul>
 */
public final class OutboxLoadCli {

  private static final long MAX_RATE = 20_000;
  private static final long MAX_SECONDS = 600;
  private static final long MAX_ROWS = 10_000_000;
  private static final int MAX_IN_FLIGHT = 64;
  private static final Duration POLL = Duration.ofMillis(50);
  private static final Duration DRAIN = Duration.ofSeconds(15);
  private static final double NANOS_PER_MILLI = 1e6;
  private static final double NANOS_PER_SECOND = 1e9;
  private static final double P99 = 0.99;
  private static final double P50 = 0.5;

  private OutboxLoadCli() {}

  /** Runs one measurement. */
  public static void main(String[] arguments) throws Exception {
    StackSettings settings = StackSettings.load();
    if (arguments.length < 1) {
      throw new IllegalArgumentException("usage: OutboxLoadCli delay|fill|size [options]");
    }
    List<String> rest = List.of(arguments).subList(1, arguments.length);
    try (HikariDataSource database =
        settings.openPool("outbox-load", settings.databases().bookingUrl())) {
      switch (arguments[0]) {
        case "delay" -> {
          Arguments options =
              Arguments.parse(rest, Map.of("rate", "100", "seconds", "30", "topic", ""));
          delay(
              settings,
              database,
              options.number("rate", 1, MAX_RATE),
              options.number("seconds", 1, MAX_SECONDS),
              options.name("topic"));
        }
        case "fill" ->
            fill(
                database,
                Arguments.parse(rest, Map.of("rows", "100000")).number("rows", 1, MAX_ROWS));
        case "size" ->
            size(
                database,
                Arguments.parse(rest, Map.of("delete-published", "no"))
                    .choice("delete-published", Set.of("yes", "no"))
                    .equals("yes"));
        default -> throw new IllegalArgumentException("unknown command " + arguments[0]);
      }
    }
  }

  private static void delay(
      StackSettings settings, HikariDataSource database, long rate, long seconds, String topic)
      throws Exception {
    // Both times are kept and joined at the end: a fast relay can deliver a row before its writer
    // has noted the commit's return, and such a row must still count.
    Map<String, Long> committedAtNanos = new ConcurrentHashMap<>();
    Map<String, Long> arrivedAtNanos = new ConcurrentHashMap<>();
    List<Long> delaysNanos = new ArrayList<>();
    List<Long> commitNanos = Collections.synchronizedList(new ArrayList<>());
    AtomicInteger failures = new AtomicInteger();
    long total = rate * seconds;
    String runTag = Long.toString(System.currentTimeMillis(), Character.MAX_RADIX);
    try (KafkaConsumer<String, String> consumer = StackClients.reader(settings.kafkaBootstrap())) {
      List<TopicPartition> partitions = new ArrayList<>();
      for (PartitionInfo partition : consumer.partitionsFor(topic)) {
        partitions.add(new TopicPartition(topic, partition.partition()));
      }
      consumer.assign(partitions);
      consumer.seekToEnd(partitions);
      partitions.forEach(consumer::position);
      Thread writer =
          Thread.ofPlatform()
              .start(() ->
                  write(database, rate, total, runTag, committedAtNanos, commitNanos, failures));
      long drainUntil = Long.MAX_VALUE;
      while (System.nanoTime() < drainUntil && arrivedAtNanos.size() < total - failures.get()) {
        for (ConsumerRecord<String, String> record : consumer.poll(POLL)) {
          if (record.key().startsWith("load-" + runTag + "-")) {
            arrivedAtNanos.putIfAbsent(record.key(), System.nanoTime());
          }
        }
        if (!writer.isAlive() && drainUntil == Long.MAX_VALUE) {
          drainUntil = System.nanoTime() + DRAIN.toNanos();
        }
      }
      writer.join();
    }
    committedAtNanos.forEach((aggregateId, committed) -> {
      Long arrived = arrivedAtNanos.get(aggregateId);
      if (arrived != null) {
        delaysNanos.add(arrived - committed);
      }
    });
    System.out.println(
        String.format(
            Locale.ROOT,
            "offered %,d rows at %,d a second for %d s; committed %,d (%d failed); arrived %,d;"
                + " commit p50 %.2f ms; relay delay commit to arrival p50 %.1f ms, p99 %.1f ms, max"
                + " %.1f ms",
            total,
            rate,
            seconds,
            commitNanos.size(),
            failures.get(),
            delaysNanos.size(),
            percentile(commitNanos, P50) / NANOS_PER_MILLI,
            percentile(delaysNanos, P50) / NANOS_PER_MILLI,
            percentile(delaysNanos, P99) / NANOS_PER_MILLI,
            percentile(delaysNanos, 1.0) / NANOS_PER_MILLI));
  }

  private static void write(
      HikariDataSource database,
      long rate,
      long total,
      String runTag,
      Map<String, Long> committedAtNanos,
      List<Long> commitNanos,
      AtomicInteger failures) {
    long intervalNanos = (long) (NANOS_PER_SECOND / rate);
    long startNanos = System.nanoTime();
    Semaphore inFlight = new Semaphore(MAX_IN_FLIGHT);
    try (ExecutorService commits = Executors.newVirtualThreadPerTaskExecutor()) {
      for (long rowIndex = 0; rowIndex < total; rowIndex++) {
        long dueNanos = startNanos + rowIndex * intervalNanos;
        long waitNanos = dueNanos - System.nanoTime();
        if (waitNanos > 0) {
          TimeUnit.NANOSECONDS.sleep(waitNanos);
        }
        String aggregateId = "load-" + runTag + "-" + rowIndex;
        inFlight.acquire();
        commits.submit(() -> {
          try {
            long beganNanos = System.nanoTime();
            commitOne(database, aggregateId);
            long doneNanos = System.nanoTime();
            commitNanos.add(doneNanos - beganNanos);
            committedAtNanos.put(aggregateId, doneNanos);
          } catch (SQLException failed) {
            failures.incrementAndGet();
          } finally {
            inFlight.release();
          }
        });
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private static void commitOne(HikariDataSource database, String aggregateId) throws SQLException {
    try (Connection transaction = database.getConnection()) {
      transaction.setAutoCommit(false);
      Outbox.insert(transaction, aggregateId, 1, "LoadProbe", Map.of("probe", aggregateId));
      transaction.commit();
    }
  }

  private static void fill(HikariDataSource database, long rows) throws SQLException {
    String insert =
        "INSERT INTO outbox (aggregate_id, aggregate_version, event_type, schema_version, payload)"
            + " SELECT ? || row_number, 1, 'LoadProbe', 1,"
            + " jsonb_build_object('probe', ? || row_number)"
            + " FROM generate_series(1, ?) AS row_number";
    // Each fill names its rows apart, so a second fill on the same outbox adds rows, not conflicts.
    String prefix = "fill-" + Long.toString(System.nanoTime(), Character.MAX_RADIX) + "-";
    long startNanos = System.nanoTime();
    try (Connection connection = database.getConnection();
        PreparedStatement statement = connection.prepareStatement(insert)) {
      statement.setString(1, prefix);
      statement.setString(2, prefix);
      statement.setLong(3, rows);
      statement.executeUpdate();
    }
    System.out.println(
        String.format(
            Locale.ROOT,
            "filled %,d unpublished outbox rows in %.2f s",
            rows,
            (System.nanoTime() - startNanos) / NANOS_PER_SECOND));
  }

  private static void size(HikariDataSource database, boolean isDeletingPublished)
      throws SQLException {
    try (Connection connection = database.getConnection()) {
      printSize(connection, "before");
      if (!isDeletingPublished) {
        return;
      }
      long startNanos = System.nanoTime();
      int deleted;
      try (Statement statement = connection.createStatement()) {
        // The nightly job's rule is "published more than 7 days ago"; the drill deletes every
        // published row at once, the same statement with the interval at 0.
        deleted =
            statement.executeUpdate(
                "DELETE FROM outbox WHERE published_at < now() - interval '0 seconds'");
      }
      double deleteSeconds = (System.nanoTime() - startNanos) / NANOS_PER_SECOND;
      startNanos = System.nanoTime();
      try (Statement statement = connection.createStatement()) {
        statement.execute("VACUUM outbox");
      }
      System.out.println(
          String.format(
              Locale.ROOT,
              "deleted %,d published rows in %.2f s; VACUUM in %.2f s",
              deleted,
              deleteSeconds,
              (System.nanoTime() - startNanos) / NANOS_PER_SECOND));
      printSize(connection, "after");
    }
  }

  private static void printSize(Connection connection, String when) throws SQLException {
    String query =
        "SELECT count(*), count(*) FILTER (WHERE published_at IS NULL),"
            + " pg_total_relation_size('outbox'), pg_relation_size('outbox'),"
            + " pg_relation_size('outbox_unpublished') FROM outbox";
    try (Statement statement = connection.createStatement();
        ResultSet row = statement.executeQuery(query)) {
      row.next();
      long rows = row.getLong(1);
      System.out.println(
          String.format(
              Locale.ROOT,
              "%s: %,d rows (%,d unpublished); table and indexes %,d bytes (%,.0f a row); heap %,d"
                  + " bytes; partial index outbox_unpublished %,d bytes",
              when,
              rows,
              row.getLong(2),
              row.getLong(3),
              rows == 0 ? 0.0 : row.getLong(3) / (double) rows,
              row.getLong(4),
              row.getLong(5)));
    }
  }

  private static double percentile(List<Long> values, double fraction) {
    List<Long> sorted;
    synchronized (values) {
      sorted = new ArrayList<>(values);
    }
    if (sorted.isEmpty()) {
      return 0;
    }
    Collections.sort(sorted);
    int index = (int) Math.ceil(fraction * sorted.size()) - 1;
    return sorted.get(Math.clamp(index, 0, sorted.size() - 1));
  }
}

package io.plainround.seatwave.saga.stack;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * The saga drills' client and judge, against the running services.
 *
 * <ul>
 *   <li>{@code book}: for each of {@code --count} bookings, holds seat {@code A-<first-seat + n>}
 *       through inventory, scripts the provider for its key ({@code --behavior}, or a decline for
 *       every {@code --decline-every}th), and books it through the booking service, which answers
 *       when the saga is final; prints the ends and the durations. With {@code
 *       --second-fan-after-millis}, another fan asks for each seat that long after its booking
 *       started. Ids come from {@code --run}, so the same command sent again after a kill names the
 *       same bookings and resumes them.
 *   <li>{@code verify}: judges every saga in the booking database by the records of the three
 *       databases and of the provider's own ledger, never by logs.
 * </ul>
 */
public final class SagaDrillCli {

  private static final ObjectMapper JSON = JsonMapper.builder().build();
  private static final Duration BOOK_TIMEOUT = Duration.ofSeconds(180);
  private static final long MAX_COUNT = 10_000;
  private static final long MAX_SEAT = 40_000;
  private static final long MAX_MILLIS = 600_000;
  private static final long MAX_CONCURRENCY = 64;
  private static final long SECOND_FAN_OFFSET = 20_000;
  private static final int OK = 200;
  private static final double NANOS_PER_MILLI = 1e6;

  private final StackSettings settings;
  private final HttpClient client;

  private SagaDrillCli(StackSettings settings, HttpClient client) {
    this.settings = settings;
    this.client = client;
  }

  /** Runs {@code book} or {@code verify}. */
  public static void main(String[] arguments) throws Exception {
    StackSettings settings = StackSettings.load();
    if (arguments.length < 1) {
      throw new IllegalArgumentException("usage: SagaDrillCli book|verify [options]");
    }
    List<String> rest = List.of(arguments).subList(1, arguments.length);
    try (HttpClient client =
        HttpClient.newBuilder().connectTimeout(settings.timeouts().stack()).build()) {
      SagaDrillCli drill = new SagaDrillCli(settings, client);
      switch (arguments[0]) {
        case "book" -> drill.book(rest);
        case "verify" -> drill.verify();
        default -> throw new IllegalArgumentException("unknown command " + arguments[0]);
      }
    }
  }

  private record Booking(UUID bookingId, UUID holdId, String seatLabel, long customerId) {}

  private record Result(String end, long millis, String secondFan) {}

  private void book(List<String> rest) throws Exception {
    Arguments options =
        Arguments.parse(
            rest,
            Map.of(
                "run", "",
                "count", "1",
                "first-seat", "1",
                "concurrency", "1",
                "behavior", "CAPTURE",
                "decline-every", "0",
                "second-fan-after-millis", "0"));
    String run = options.name("run");
    long count = options.number("count", 1, MAX_COUNT);
    long firstSeat = options.number("first-seat", 1, MAX_SEAT);
    long concurrency = options.number("concurrency", 1, MAX_CONCURRENCY);
    String behavior =
        options.choice("behavior", Set.of("CAPTURE", "DECLINE", "CAPTURE_THEN_TIMEOUT"));
    long declineEvery = options.number("decline-every", 0, MAX_COUNT);
    long secondFanAfterMillis = options.number("second-fan-after-millis", 0, MAX_MILLIS);
    if (firstSeat + count - 1 > MAX_SEAT || firstSeat + count - 1 + SECOND_FAN_OFFSET > MAX_SEAT) {
      throw new IllegalArgumentException("first-seat + count must stay within the lab's seats");
    }
    List<Future<Result>> results = new ArrayList<>();
    java.util.concurrent.Semaphore slots = new java.util.concurrent.Semaphore((int) concurrency);
    try (ExecutorService bookings = Executors.newVirtualThreadPerTaskExecutor()) {
      for (long index = 0; index < count; index++) {
        long seatNumber = firstSeat + index;
        Booking booking =
            new Booking(
                UUID.nameUUIDFromBytes(
                    ("booking/" + run + "/" + index).getBytes(StandardCharsets.UTF_8)),
                UUID.nameUUIDFromBytes(
                    ("hold/" + run + "/" + index).getBytes(StandardCharsets.UTF_8)),
                "A-" + seatNumber,
                seatNumber);
        boolean isDeclined = declineEvery > 0 && (index + 1) % declineEvery == 0;
        String keyBehavior = isDeclined ? "DECLINE" : behavior;
        slots.acquire();
        results.add(
            bookings.submit(() -> {
              try {
                return bookOne(booking, keyBehavior, secondFanAfterMillis);
              } finally {
                slots.release();
              }
            }));
      }
    }
    Map<String, Integer> ends = new TreeMap<>();
    Map<String, Integer> secondFans = new TreeMap<>();
    List<Long> millis = new ArrayList<>();
    for (Future<Result> future : results) {
      Result result = future.get();
      ends.merge(result.end(), 1, Integer::sum);
      if (!result.secondFan().isEmpty()) {
        secondFans.merge(result.secondFan(), 1, Integer::sum);
      }
      millis.add(result.millis());
    }
    Collections.sort(millis);
    System.out.println(
        String.format(
            Locale.ROOT,
            "run %s: %d booking(s), ends %s; booking time p50 %d ms, p99 %d ms, max %d ms%s",
            run,
            count,
            ends,
            millis.get((int) Math.ceil(0.5 * millis.size()) - 1),
            millis.get((int) Math.ceil(0.99 * millis.size()) - 1),
            millis.get(millis.size() - 1),
            secondFans.isEmpty() ? "" : "; second fan's holds: " + secondFans));
  }

  private Result bookOne(Booking booking, String behavior, long secondFanAfterMillis)
      throws IOException, InterruptedException {
    post(
        settings.services().inventory(),
        "/commands/hold",
        Map.of(
            "hold_id", booking.holdId().toString(),
            "seat_label", booking.seatLabel(),
            "customer_id", booking.customerId()));
    String providerKey = "seatwave-hold-" + booking.holdId();
    HttpResponse<String> scripted =
        post(
            settings.services().provider(),
            "/admin/script?key=" + providerKey + "&behavior=" + behavior,
            Map.of());
    if (!isAccepted(scripted)) {
      throw new IllegalStateException("the provider refused the script: " + scripted.body());
    }
    long startNanos = System.nanoTime();
    Future<String> secondFan = null;
    try (ExecutorService fan = Executors.newVirtualThreadPerTaskExecutor()) {
      if (secondFanAfterMillis > 0) {
        secondFan =
            fan.submit(() -> {
              Thread.sleep(secondFanAfterMillis);
              return outcome(
                  post(
                      settings.services().inventory(),
                      "/commands/hold",
                      Map.of(
                          "hold_id", UUID.randomUUID().toString(),
                          "seat_label", booking.seatLabel(),
                          "customer_id", booking.customerId() + SECOND_FAN_OFFSET)));
            });
      }
      String end =
          outcome(
              post(
                  settings.services().booking(),
                  "/commands/book",
                  Map.of(
                      "booking_id", booking.bookingId().toString(),
                      "hold_id", booking.holdId().toString())));
      long elapsedMillis = (long) ((System.nanoTime() - startNanos) / NANOS_PER_MILLI);
      String fanResult = "";
      if (secondFan != null) {
        try {
          fanResult = secondFan.get();
        } catch (java.util.concurrent.ExecutionException failed) {
          fanResult = "ERROR";
        }
      }
      return new Result(end, elapsedMillis, fanResult);
    }
  }

  private static String outcome(HttpResponse<String> response) throws IOException {
    JsonNode body = JSON.readTree(response.body());
    String detail = body.path("detail").asText("");
    String outcome = body.path("outcome").asText("HTTP " + response.statusCode());
    // A booking answers with its end state as the detail; a hold with its outcome.
    return detail.equals("COMPLETED") || detail.equals("COMPENSATED") ? detail : outcome;
  }

  private HttpResponse<String> post(URI service, String path, Map<String, Object> body)
      throws IOException, InterruptedException {
    HttpRequest request =
        HttpRequest.newBuilder(service.resolve(path))
            .timeout(BOOK_TIMEOUT)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofByteArray(JSON.writeValueAsBytes(body)))
            .build();
    return client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
  }

  private record Saga(String bookingId, String holdId, String providerKey, String state) {}

  private void verify() throws Exception {
    List<Saga> sagas = new ArrayList<>();
    Map<String, String> holdStates = new HashMap<>();
    Map<String, String> paymentStates = new HashMap<>();
    java.util.Set<String> bookings = new java.util.HashSet<>();
    try (HikariDataSource booking =
            settings.openPool("verify-booking", settings.databases().bookingUrl());
        HikariDataSource inventory =
            settings.openPool("verify-inventory", settings.databases().inventoryUrl());
        HikariDataSource payment =
            settings.openPool("verify-payment", settings.databases().paymentUrl())) {
      query(
          booking,
          "SELECT booking_id::text, hold_id::text, provider_key, state FROM booking_sagas",
          rows ->
              sagas.add(
                  new Saga(
                      rows.getString(1), rows.getString(2), rows.getString(3), rows.getString(4))));
      query(
          booking,
          "SELECT booking_id::text FROM bookings",
          rows -> bookings.add(rows.getString(1)));
      query(
          inventory,
          "SELECT hold_id::text, state FROM holds",
          rows -> holdStates.put(rows.getString(1), rows.getString(2)));
      query(
          payment,
          "SELECT provider_key, state FROM payments",
          rows -> paymentStates.put(rows.getString(1), rows.getString(2)));
    }
    Map<String, Integer> ends = new TreeMap<>();
    List<String> wrong = new ArrayList<>();
    int captures = 0;
    int refunds = 0;
    for (Saga saga : sagas) {
      ends.merge(saga.state(), 1, Integer::sum);
      JsonNode ledger =
          JSON.readTree(
              get(settings.services().provider(), "/admin/ledger?key=" + saga.providerKey())
                  .body());
      int sagaCaptures = ledger.path("captures").asInt();
      int sagaRefunds = ledger.path("refunds").asInt();
      captures += sagaCaptures;
      refunds += sagaRefunds;
      int kept = sagaCaptures - sagaRefunds;
      String hold = holdStates.getOrDefault(saga.holdId(), "MISSING");
      boolean isCorrect =
          switch (saga.state()) {
            case "COMPLETED" ->
                hold.equals("CONFIRMED") && kept == 1 && bookings.contains(saga.bookingId());
            case "COMPENSATED" ->
                !hold.equals("CONFIRMING")
                    && !hold.equals("CONFIRMED")
                    && kept == 0
                    && !bookings.contains(saga.bookingId());
            default -> false;
          };
      if (!isCorrect) {
        wrong.add(
            saga.bookingId()
                + " "
                + saga.state()
                + " hold "
                + hold
                + " payment "
                + paymentStates.getOrDefault(saga.providerKey(), "NONE")
                + " provider "
                + sagaCaptures
                + " capture(s) "
                + sagaRefunds
                + " refund(s)");
      }
    }
    System.out.println(
        String.format(
            Locale.ROOT,
            "sagas %d: %s; provider ledger %d capture(s), %d refund(s) for them; bookings rows %d;"
                + " wrong ends %d",
            sagas.size(),
            ends,
            captures,
            refunds,
            bookings.size(),
            wrong.size()));
    wrong.forEach(line -> System.out.println("  WRONG " + line));
  }

  private HttpResponse<String> get(URI service, String path)
      throws IOException, InterruptedException {
    return client.send(
        HttpRequest.newBuilder(service.resolve(path)).timeout(settings.timeouts().stack()).build(),
        HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
  }

  @FunctionalInterface
  private interface RowReader {
    void read(ResultSet rows) throws SQLException;
  }

  private static void query(HikariDataSource database, String sql, RowReader reader)
      throws SQLException {
    try (Connection connection = database.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql);
        ResultSet rows = statement.executeQuery()) {
      while (rows.next()) {
        reader.read(rows);
      }
    }
  }

  /** True when the stub accepted an admin call. */
  private static boolean isAccepted(HttpResponse<String> response) {
    return response.statusCode() == OK;
  }
}

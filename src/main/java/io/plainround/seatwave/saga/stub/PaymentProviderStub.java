package io.plainround.seatwave.saga.stub;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * Module 7's stand-in for the card provider, as a small HTTP service: a charge and a refund, each
 * keyed by the caller's {@code Idempotency-Key}, so a repeated request returns the first answer and
 * moves no more money. Its own ledger (captures and refunds per key) is what the drills compare
 * with the saga's tables: the money is judged by the provider, not by the saga's opinion of it.
 *
 * <p>An admin API scripts what the provider does per key: decline, or capture and then answer only
 * after the caller has timed out (the lost answer), and sets the latency of a new charge (a repeat
 * and a refund answer at once) and a number of refunds to fail with 503. JDK only, one file, so
 * {@code stub/Dockerfile} compiles it alone. Thread-safe: one lock per key makes concurrent
 * requests for one key capture once.
 *
 * <pre>
 * POST /charges?amount=80.00     Idempotency-Key: seatwave-hold-h-1   201 captured, 402 declined
 * POST /refunds                  Idempotency-Key: seatwave-hold-h-1   201 refunded, 404, 503
 * POST /admin/script?key=K&amp;behavior=CAPTURE|DECLINE|CAPTURE_THEN_TIMEOUT
 * POST /admin/latency?millis=N   POST /admin/fail-refunds?count=N
 * GET  /admin/ledger?key=K       GET  /admin/totals                   GET /health
 * </pre>
 */
public final class PaymentProviderStub implements AutoCloseable {

  private static final int CREATED = 201;
  private static final int OK = 200;
  private static final int BAD_REQUEST = 400;
  private static final int PAYMENT_REQUIRED = 402;
  private static final int NOT_FOUND = 404;
  private static final int METHOD_NOT_ALLOWED = 405;
  private static final int UNAVAILABLE = 503;
  private static final int MAX_PORT = 65_535;
  private static final long MAX_DELAY_MILLIS = 600_000;
  private static final int MAX_FAILURES = 1_000;
  private static final int BACKLOG = 128;
  private static final Duration CHECK_TIMEOUT = Duration.ofSeconds(2);

  /** A key the caller may use: letters, digits, dashes and underscores, 1 to 80 of them. */
  private static final Pattern KEY = Pattern.compile("[A-Za-z0-9_-]{1,80}");

  /** An amount with at most two decimals, below a million. */
  private static final Pattern AMOUNT = Pattern.compile("[0-9]{1,6}\\.[0-9]{2}");

  private final HttpServer server;
  private final ExecutorService executor;
  private final Map<String, Behavior> behaviors = new ConcurrentHashMap<>();
  private final Map<String, Charge> charges = new ConcurrentHashMap<>();
  private final Map<String, Object> keyLocks = new ConcurrentHashMap<>();
  private final AtomicLong latencyMillis;
  private final long lostAnswerMillis;
  private final AtomicInteger refundFailuresLeft = new AtomicInteger();
  private final AtomicLong chargeIds = new AtomicLong();

  /** What the provider does with a new charge for one key. */
  public enum Behavior {
    CAPTURE,
    DECLINE,
    CAPTURE_THEN_TIMEOUT
  }

  /** What the provider recorded for one key: its answer, and the money it moved. */
  private record Charge(String chargeId, boolean isCaptured, int captures, int refunds) {}

  /**
   * The stub's settings.
   *
   * @param port where it listens (0 for any free port, in tests)
   * @param latencyMillis how long a new charge takes (a repeated charge and a refund answer at
   *     once)
   * @param lostAnswerMillis how long a CAPTURE_THEN_TIMEOUT charge waits before it answers
   */
  public record Settings(int port, long latencyMillis, long lostAnswerMillis) {
    /** Refuses a port or delay outside its range. */
    public Settings {
      if (port < 0 || port > MAX_PORT) {
        throw new IllegalArgumentException("port must be 0 to " + MAX_PORT + ", not " + port);
      }
      if (latencyMillis < 0 || latencyMillis > MAX_DELAY_MILLIS) {
        throw new IllegalArgumentException("latency must be 0 to " + MAX_DELAY_MILLIS + " ms");
      }
      if (lostAnswerMillis < 0 || lostAnswerMillis > MAX_DELAY_MILLIS) {
        throw new IllegalArgumentException("lost-answer delay must be 0 to " + MAX_DELAY_MILLIS);
      }
    }

    /** Reads STUB_PORT, STUB_LATENCY_MILLIS and STUB_LOST_ANSWER_MILLIS; all are required. */
    static Settings fromEnvironment(Map<String, String> environment) {
      return new Settings(
          Math.toIntExact(number(environment, "STUB_PORT")),
          number(environment, "STUB_LATENCY_MILLIS"),
          number(environment, "STUB_LOST_ANSWER_MILLIS"));
    }

    private static long number(Map<String, String> environment, String name) {
      String value = environment.getOrDefault(name, "");
      if (!value.matches("[0-9]{1,9}")) {
        throw new IllegalArgumentException(name + " must be a whole number, not '" + value + "'");
      }
      return Long.parseLong(value);
    }
  }

  private PaymentProviderStub(Settings settings) throws IOException {
    this.latencyMillis = new AtomicLong(settings.latencyMillis());
    this.lostAnswerMillis = settings.lostAnswerMillis();
    this.server = HttpServer.create(new InetSocketAddress(settings.port()), BACKLOG);
    // One virtual thread per request: a lost answer waits 10 s without holding a platform thread.
    this.executor = Executors.newVirtualThreadPerTaskExecutor();
    server.setExecutor(executor);
    server.createContext("/", this::handle);
  }

  /** Starts a stub; the caller closes it. */
  public static PaymentProviderStub start(Settings settings) throws IOException {
    PaymentProviderStub stub =
        new PaymentProviderStub(Objects.requireNonNull(settings, "settings"));
    stub.server.start();
    return stub;
  }

  /**
   * Runs the stub until the process is stopped, or with {@code --check}, the container's
   * healthcheck, asks a running stub for {@code /health} and exits 0 if it answers 200.
   */
  public static void main(String[] arguments) throws IOException, InterruptedException {
    Settings settings = Settings.fromEnvironment(System.getenv());
    if (arguments.length == 1 && arguments[0].equals("--check")) {
      System.exit(isHealthy(settings.port()) ? 0 : 1);
    }
    PaymentProviderStub stub = start(settings);
    System.out.println("payment provider stub listening on port " + stub.port());
    Thread.currentThread().join();
  }

  /** The port the stub listens on. */
  public int port() {
    return server.getAddress().getPort();
  }

  @Override
  public void close() {
    server.stop(0);
    executor.close();
  }

  private static boolean isHealthy(int port) throws InterruptedException {
    try (HttpClient client = HttpClient.newBuilder().connectTimeout(CHECK_TIMEOUT).build()) {
      HttpRequest request =
          HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/health"))
              .timeout(CHECK_TIMEOUT)
              .build();
      return client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode() == OK;
    } catch (IOException notAnswering) {
      return false;
    }
  }

  private void handle(HttpExchange exchange) throws IOException {
    try (exchange) {
      Answer answer = route(exchange);
      byte[] body = answer.json().getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().set("Content-Type", "application/json");
      exchange.sendResponseHeaders(answer.status(), body.length);
      try (OutputStream output = exchange.getResponseBody()) {
        output.write(body);
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private record Answer(int status, String json) {}

  private Answer route(HttpExchange exchange) throws InterruptedException {
    String path = exchange.getRequestURI().getPath();
    String method = exchange.getRequestMethod();
    Map<String, String> query = parseQuery(exchange.getRequestURI().getRawQuery());
    boolean isPost = method.equals("POST");
    return switch (path) {
      case "/health" -> new Answer(OK, "{\"status\":\"up\"}");
      case "/charges" -> isPost ? charge(idempotencyKey(exchange), query) : notAllowed();
      case "/refunds" -> isPost ? refund(idempotencyKey(exchange)) : notAllowed();
      case "/admin/script" -> isPost ? script(query) : notAllowed();
      case "/admin/latency" -> isPost ? setLatency(query) : notAllowed();
      case "/admin/fail-refunds" -> isPost ? failRefunds(query) : notAllowed();
      case "/admin/ledger" -> ledger(query.getOrDefault("key", ""));
      case "/admin/totals" -> totals();
      default -> new Answer(NOT_FOUND, "{\"error\":\"no such path\"}");
    };
  }

  private Answer charge(Optional<String> key, Map<String, String> query)
      throws InterruptedException {
    String amount = query.getOrDefault("amount", "");
    if (key.isEmpty() || !AMOUNT.matcher(amount).matches()) {
      return badRequest("an Idempotency-Key of 1 to 80 letters, digits or dashes, and an amount");
    }
    String providerKey = key.get();
    Charge earlier = charges.get(providerKey);
    if (earlier != null) {
      // The idempotency key: the first answer again, at once, and no money moves.
      return chargeAnswer(earlier, new BigDecimal(amount));
    }
    // Only a new charge takes the provider's time: that is the call that moves money.
    Thread.sleep(latencyMillis.get());
    boolean isLostAnswer;
    Charge charge;
    synchronized (lockFor(providerKey)) {
      Charge concurrent = charges.get(providerKey);
      if (concurrent != null) {
        // A second request for the key arrived while the first waited: it gets the first answer.
        return chargeAnswer(concurrent, new BigDecimal(amount));
      }
      Behavior behavior = behaviors.getOrDefault(providerKey, Behavior.CAPTURE);
      boolean isCaptured = behavior != Behavior.DECLINE;
      charge = new Charge("ch-" + chargeIds.incrementAndGet(), isCaptured, isCaptured ? 1 : 0, 0);
      charges.put(providerKey, charge);
      isLostAnswer = behavior == Behavior.CAPTURE_THEN_TIMEOUT;
    }
    if (isLostAnswer) {
      // The money moved; the answer comes after the caller gave up waiting.
      Thread.sleep(lostAnswerMillis);
    }
    return chargeAnswer(charge, new BigDecimal(amount));
  }

  private static Answer chargeAnswer(Charge charge, BigDecimal amount) {
    if (!charge.isCaptured()) {
      return new Answer(PAYMENT_REQUIRED, "{\"status\":\"declined\"}");
    }
    return new Answer(
        CREATED,
        String.format(
            Locale.ROOT,
            "{\"status\":\"captured\",\"charge_id\":\"%s\",\"amount\":\"%s\"}",
            charge.chargeId(),
            amount.toPlainString()));
  }

  private Answer refund(Optional<String> key) {
    if (key.isEmpty()) {
      return badRequest("an Idempotency-Key of 1 to 80 letters, digits or dashes");
    }
    if (refundFailuresLeft.getAndUpdate(left -> Math.max(0, left - 1)) > 0) {
      return new Answer(UNAVAILABLE, "{\"error\":\"try again later\"}");
    }
    String providerKey = key.get();
    synchronized (lockFor(providerKey)) {
      Charge charge = charges.get(providerKey);
      if (charge == null || !charge.isCaptured()) {
        return new Answer(NOT_FOUND, "{\"error\":\"no capture under this key\"}");
      }
      if (charge.refunds() == 0) {
        charges.put(
            providerKey,
            new Charge(charge.chargeId(), true, charge.captures(), charge.refunds() + 1));
      }
    }
    return new Answer(CREATED, "{\"status\":\"refunded\"}");
  }

  private Answer script(Map<String, String> query) {
    String key = query.getOrDefault("key", "");
    String behavior = query.getOrDefault("behavior", "");
    boolean isKnown =
        behavior.equals("CAPTURE")
            || behavior.equals("DECLINE")
            || behavior.equals("CAPTURE_THEN_TIMEOUT");
    if (!KEY.matcher(key).matches() || !isKnown) {
      return badRequest("key and behavior CAPTURE, DECLINE or CAPTURE_THEN_TIMEOUT");
    }
    behaviors.put(key, Behavior.valueOf(behavior));
    return new Answer(OK, "{\"scripted\":\"" + key + "\"}");
  }

  private Answer setLatency(Map<String, String> query) {
    String millis = query.getOrDefault("millis", "");
    if (!millis.matches("[0-9]{1,6}") || Long.parseLong(millis) > MAX_DELAY_MILLIS) {
      return badRequest("millis from 0 to " + MAX_DELAY_MILLIS);
    }
    latencyMillis.set(Long.parseLong(millis));
    return new Answer(OK, "{\"latency_millis\":" + millis + "}");
  }

  private Answer failRefunds(Map<String, String> query) {
    String count = query.getOrDefault("count", "");
    if (!count.matches("[0-9]{1,4}") || Integer.parseInt(count) > MAX_FAILURES) {
      return badRequest("count from 0 to " + MAX_FAILURES);
    }
    refundFailuresLeft.set(Integer.parseInt(count));
    return new Answer(OK, "{\"failing_refunds\":" + count + "}");
  }

  private Answer ledger(String key) {
    if (!KEY.matcher(key).matches()) {
      return badRequest("a key");
    }
    Charge charge = charges.get(key);
    int captures = charge == null ? 0 : charge.captures();
    int refunds = charge == null ? 0 : charge.refunds();
    return new Answer(
        OK,
        String.format(
            Locale.ROOT,
            "{\"key\":\"%s\",\"captures\":%d,\"refunds\":%d}",
            key,
            captures,
            refunds));
  }

  private Answer totals() {
    int captures = 0;
    int refunds = 0;
    int declines = 0;
    for (Charge charge : charges.values()) {
      captures += charge.captures();
      refunds += charge.refunds();
      declines += charge.isCaptured() ? 0 : 1;
    }
    return new Answer(
        OK,
        String.format(
            Locale.ROOT,
            "{\"keys\":%d,\"captures\":%d,\"refunds\":%d,\"declines\":%d}",
            charges.size(),
            captures,
            refunds,
            declines));
  }

  private Object lockFor(String providerKey) {
    return keyLocks.computeIfAbsent(providerKey, unused -> new Object());
  }

  private static Optional<String> idempotencyKey(HttpExchange exchange) {
    String key = exchange.getRequestHeaders().getFirst("Idempotency-Key");
    return key != null && KEY.matcher(key).matches() ? Optional.of(key) : Optional.empty();
  }

  private static Answer badRequest(String expected) {
    return new Answer(BAD_REQUEST, "{\"error\":\"expected " + expected + "\"}");
  }

  private static Answer notAllowed() {
    return new Answer(METHOD_NOT_ALLOWED, "{\"error\":\"method not allowed\"}");
  }

  private static Map<String, String> parseQuery(String rawQuery) {
    Map<String, String> query = new HashMap<>();
    if (rawQuery == null || rawQuery.isEmpty()) {
      return query;
    }
    for (String pair : rawQuery.split("&", -1)) {
      int separatorAt = pair.indexOf('=');
      if (separatorAt > 0) {
        query.put(
            URLDecoder.decode(pair.substring(0, separatorAt), StandardCharsets.UTF_8),
            URLDecoder.decode(pair.substring(separatorAt + 1), StandardCharsets.UTF_8));
      }
    }
    return query;
  }
}

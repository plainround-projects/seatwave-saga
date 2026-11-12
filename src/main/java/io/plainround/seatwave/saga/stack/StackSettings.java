package io.plainround.seatwave.saga.stack;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;

/**
 * Where the lab stack of {@code compose.yaml} and the three host services answer, and the limits
 * every program uses with them, read from {@code config/stack.properties}. Every problem in the
 * file is reported together, as one settings problem, never as a stack trace from the first bad
 * value.
 *
 * @param kafkaBootstrap the broker's host listener
 * @param databases the three service databases
 * @param services the three services' and the stub's base URLs
 * @param timeouts the stack's general, payment-step and provider timeouts
 * @param holdDuration how long a hold lasts (Module 2's 300 s)
 * @param seatPrice the price of every seat of the lab (Module 2's $80.00)
 * @param relay the relay's poll interval and batch size
 * @param retry the saga's first retry wait and its cap
 */
public record StackSettings(
    String kafkaBootstrap,
    Databases databases,
    Services services,
    Timeouts timeouts,
    Duration holdDuration,
    BigDecimal seatPrice,
    Relay relay,
    Retry retry) {

  /** The system property that names the settings file; the Gradle tasks and drills set it. */
  public static final String LOCATION_PROPERTY = "seatwave.stackSettings";

  private static final int MAX_POOL_SIZE = 64;
  private static final long MAX_TIMEOUT_MILLIS = 120_000;
  private static final long MAX_HOLD_SECONDS = 3_600;
  private static final long MAX_POLL_MILLIS = 60_000;
  private static final long MAX_BATCH = 10_000;
  private static final long MAX_RETRY_MILLIS = 600_000;

  /**
   * The three service databases: their URLs, one login and its password, and the pool size.
   *
   * @param bookingUrl the booking service's database (the orchestrator's)
   * @param inventoryUrl inventory's
   * @param paymentUrl payment's
   * @param user the login of all three
   * @param password read from the ignored {@code .secrets} folder at start, never logged
   * @param poolSize connections per pool
   */
  public record Databases(
      String bookingUrl,
      String inventoryUrl,
      String paymentUrl,
      String user,
      String password,
      int poolSize) {
    /** Rejects a missing value. */
    public Databases {
      Objects.requireNonNull(bookingUrl, "bookingUrl");
      Objects.requireNonNull(inventoryUrl, "inventoryUrl");
      Objects.requireNonNull(paymentUrl, "paymentUrl");
      Objects.requireNonNull(user, "user");
      Objects.requireNonNull(password, "password");
    }

    /** Never prints the password. */
    @Override
    public String toString() {
      return "Databases[" + bookingUrl + ", " + inventoryUrl + ", " + paymentUrl + "]";
    }
  }

  /**
   * Base URLs.
   *
   * @param booking the booking service (the orchestrator's HTTP side)
   * @param inventory the inventory service
   * @param payment the payment service
   * @param provider the payment provider's stub
   */
  public record Services(URI booking, URI inventory, URI payment, URI provider) {
    /** Rejects a missing URL. */
    public Services {
      Objects.requireNonNull(booking, "booking");
      Objects.requireNonNull(inventory, "inventory");
      Objects.requireNonNull(payment, "payment");
      Objects.requireNonNull(provider, "provider");
    }
  }

  /**
   * Timeouts.
   *
   * @param stack connect and request timeout of every call but the two below
   * @param paymentStep the orchestrator's wait for the payment service
   * @param provider the payment service's wait for the provider
   */
  public record Timeouts(Duration stack, Duration paymentStep, Duration provider) {
    /** Rejects a missing timeout. */
    public Timeouts {
      Objects.requireNonNull(stack, "stack");
      Objects.requireNonNull(paymentStep, "paymentStep");
      Objects.requireNonNull(provider, "provider");
    }
  }

  /**
   * The relay's settings.
   *
   * @param pollInterval the wait after a pass that found fewer rows than a batch
   * @param batchSize rows a pass at most
   */
  public record Relay(Duration pollInterval, int batchSize) {
    /** Rejects a missing interval. */
    public Relay {
      Objects.requireNonNull(pollInterval, "pollInterval");
    }
  }

  /**
   * The saga's retry wait.
   *
   * @param firstRetryMillis the first wait
   * @param maxRetryMillis its cap
   */
  public record Retry(long firstRetryMillis, long maxRetryMillis) {}

  /** Rejects missing values; ranges are checked by {@link #load}, all at once. */
  public StackSettings {
    Objects.requireNonNull(kafkaBootstrap, "kafkaBootstrap");
    Objects.requireNonNull(databases, "databases");
    Objects.requireNonNull(services, "services");
    Objects.requireNonNull(timeouts, "timeouts");
    Objects.requireNonNull(holdDuration, "holdDuration");
    Objects.requireNonNull(seatPrice, "seatPrice");
    Objects.requireNonNull(relay, "relay");
    Objects.requireNonNull(retry, "retry");
  }

  /** Reads the file named by {@link #LOCATION_PROPERTY}, or {@code config/stack.properties}. */
  public static StackSettings load() throws IOException {
    return load(Path.of(System.getProperty(LOCATION_PROPERTY, "config/stack.properties")));
  }

  /**
   * Reads one settings file. The password file is relative to the project folder, the parent of the
   * settings file's folder, so the programs work from any working directory.
   */
  public static StackSettings load(Path settingsFile) throws IOException {
    Properties file = new Properties();
    try (Reader reader = Files.newBufferedReader(settingsFile, StandardCharsets.UTF_8)) {
      file.load(reader);
    }
    List<String> problems = new ArrayList<>();
    Values values = new Values(file, problems);
    String bootstrap = values.text("kafka.bootstrap");
    String bookingUrl = values.jdbcUrl("booking.db.url");
    String inventoryUrl = values.jdbcUrl("inventory.db.url");
    String paymentUrl = values.jdbcUrl("payment.db.url");
    String user = values.text("db.user");
    String passwordFile = values.text("db.passwordFile");
    long poolSize = values.number("db.poolSize", 1, MAX_POOL_SIZE);
    URI booking = values.httpUrl("booking.url");
    URI inventory = values.httpUrl("inventory.url");
    URI payment = values.httpUrl("payment.url");
    URI provider = values.httpUrl("provider.url");
    long stackMillis = values.number("stack.timeoutMillis", 1, MAX_TIMEOUT_MILLIS);
    long paymentStepMillis = values.number("payment.stepTimeoutMillis", 1, MAX_TIMEOUT_MILLIS);
    long providerMillis = values.number("provider.timeoutMillis", 1, MAX_TIMEOUT_MILLIS);
    long holdSeconds = values.number("hold.seconds", 1, MAX_HOLD_SECONDS);
    BigDecimal seatPrice = values.price("seat.price");
    long pollMillis = values.number("relay.pollMillis", 1, MAX_POLL_MILLIS);
    long batchSize = values.number("relay.batchSize", 1, MAX_BATCH);
    long firstRetryMillis = values.number("saga.firstRetryMillis", 1, MAX_RETRY_MILLIS);
    long maxRetryMillis = values.number("saga.maxRetryMillis", 1, MAX_RETRY_MILLIS);
    if (providerMillis >= paymentStepMillis) {
      problems.add(
          "provider.timeoutMillis must be below payment.stepTimeoutMillis, so the payment service"
              + " answers before the orchestrator stops waiting");
    }
    if (maxRetryMillis < firstRetryMillis) {
      problems.add("saga.maxRetryMillis must be at least saga.firstRetryMillis");
    }
    Path projectFolder = settingsFile.toAbsolutePath().getParent().getParent();
    Path password = projectFolder.resolve(passwordFile);
    if (!passwordFile.isEmpty() && !Files.isReadable(password)) {
      problems.add("db.passwordFile: cannot read " + password + " (scripts/stack.sh up writes it)");
    }
    if (!problems.isEmpty()) {
      throw new IllegalStateException(
          settingsFile + " has " + problems.size() + " problem(s): " + String.join("; ", problems));
    }
    return new StackSettings(
        bootstrap,
        new Databases(
            bookingUrl,
            inventoryUrl,
            paymentUrl,
            user,
            Files.readString(password, StandardCharsets.UTF_8).strip(),
            Math.toIntExact(poolSize)),
        new Services(booking, inventory, payment, provider),
        new Timeouts(
            Duration.ofMillis(stackMillis),
            Duration.ofMillis(paymentStepMillis),
            Duration.ofMillis(providerMillis)),
        Duration.ofSeconds(holdSeconds),
        seatPrice,
        new Relay(Duration.ofMillis(pollMillis), Math.toIntExact(batchSize)),
        new Retry(firstRetryMillis, maxRetryMillis));
  }

  /** The same settings with another hold duration, for a drill that needs a short hold. */
  public StackSettings withHoldDuration(Duration otherHoldDuration) {
    return new StackSettings(
        kafkaBootstrap, databases, services, timeouts, otherHoldDuration, seatPrice, relay, retry);
  }

  /** A pool on one of the three databases; the caller closes it. */
  public HikariDataSource openPool(String poolName, String jdbcUrl) {
    HikariConfig config = new HikariConfig();
    config.setPoolName(poolName);
    config.setJdbcUrl(jdbcUrl);
    config.setUsername(databases.user());
    config.setPassword(databases.password());
    config.setMaximumPoolSize(databases.poolSize());
    config.setConnectionTimeout(timeouts.stack().toMillis());
    return new HikariDataSource(config);
  }

  /** Never prints the password. */
  @Override
  public String toString() {
    return String.format(
        Locale.ROOT, "StackSettings[kafka=%s, %s, %s]", kafkaBootstrap, databases, services);
  }

  /** Reads keys and collects what is wrong with them instead of stopping at the first. */
  private record Values(Properties file, List<String> problems) {
    String text(String key) {
      String value = file.getProperty(key, "").strip();
      if (value.isEmpty()) {
        problems.add(key + ": missing");
      }
      return value;
    }

    String jdbcUrl(String key) {
      String value = text(key);
      if (!value.isEmpty() && !value.startsWith("jdbc:postgresql://")) {
        problems.add(key + ": '" + value + "' is not a jdbc:postgresql:// URL");
      }
      return value;
    }

    URI httpUrl(String key) {
      String value = text(key);
      if (!value.isEmpty() && !value.matches("http://[A-Za-z0-9.-]+:[0-9]{1,5}")) {
        problems.add(key + ": '" + value + "' is not http://host:port");
        return URI.create("http://invalid:1");
      }
      return URI.create(value.isEmpty() ? "http://invalid:1" : value);
    }

    BigDecimal price(String key) {
      String value = text(key);
      if (!value.matches("[0-9]{1,6}\\.[0-9]{2}")) {
        if (!value.isEmpty()) {
          problems.add(key + ": '" + value + "' is not an amount such as 80.00");
        }
        return BigDecimal.ZERO;
      }
      return new BigDecimal(value);
    }

    long number(String key, long minimum, long maximum) {
      String value = text(key);
      if (value.isEmpty()) {
        return minimum;
      }
      try {
        long parsed = Long.parseLong(value);
        if (parsed < minimum || parsed > maximum) {
          problems.add(
              String.format(
                  Locale.ROOT, "%s: %d is outside %d to %d", key, parsed, minimum, maximum));
        }
        return Math.clamp(parsed, minimum, maximum);
      } catch (NumberFormatException notANumber) {
        problems.add(key + ": '" + value + "' is not a whole number");
        return minimum;
      }
    }
  }
}

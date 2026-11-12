package io.plainround.seatwave.saga.stack;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;

/**
 * The payment service's client of the card provider (the stub of compose.yaml): a charge and a
 * refund, each sent with the provider key as its {@code Idempotency-Key}, so sending one again
 * after a timeout moves no more money (Module 7). It does not retry: the saga's orchestrator is the
 * one layer that retries (code guidelines, rule 66).
 *
 * <p>Thread-safe: the HTTP client is shared and immutable.
 */
public final class ProviderClient implements AutoCloseable {

  private static final int CREATED = 201;
  private static final int PAYMENT_REQUIRED = 402;
  private static final int NOT_FOUND = 404;

  private final HttpClient client;
  private final URI provider;
  private final Duration timeout;

  /** What the provider said. */
  public enum Answer {
    CAPTURED,
    DECLINED,
    REFUNDED,
    NOTHING_TO_REFUND,
    NO_ANSWER
  }

  /** A client of the provider at {@code provider}, waiting at most {@code timeout} a call. */
  public ProviderClient(URI provider, Duration timeout) {
    this.provider = Objects.requireNonNull(provider, "provider");
    this.timeout = Objects.requireNonNull(timeout, "timeout");
    this.client = HttpClient.newBuilder().connectTimeout(timeout).build();
  }

  /**
   * Charges {@code amount} once under {@code providerKey}; NO_ANSWER when the outcome is unknown.
   */
  public Answer charge(String providerKey, BigDecimal amount) throws InterruptedException {
    int status = post("/charges?amount=" + amount.toPlainString(), providerKey);
    return switch (status) {
      case CREATED -> Answer.CAPTURED;
      case PAYMENT_REQUIRED -> Answer.DECLINED;
      default -> Answer.NO_ANSWER;
    };
  }

  /** Refunds the capture under {@code providerKey} once; NO_ANSWER when the outcome is unknown. */
  public Answer refund(String providerKey) throws InterruptedException {
    int status = post("/refunds", providerKey);
    return switch (status) {
      case CREATED -> Answer.REFUNDED;
      case NOT_FOUND -> Answer.NOTHING_TO_REFUND;
      default -> Answer.NO_ANSWER;
    };
  }

  @Override
  public void close() {
    client.close();
  }

  /** The status code, or 0 when no answer came in time (timeout or connection failure). */
  private int post(String pathAndQuery, String providerKey) throws InterruptedException {
    HttpRequest request =
        HttpRequest.newBuilder(provider.resolve(pathAndQuery))
            .timeout(timeout)
            .header("Idempotency-Key", providerKey)
            .POST(HttpRequest.BodyPublishers.noBody())
            .build();
    try {
      return client
          .send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
          .statusCode();
    } catch (HttpTimeoutException timedOut) {
      // The provider may have moved the money: the outcome is unknown, never "failed".
      return 0;
    } catch (IOException noAnswer) {
      return 0;
    }
  }
}

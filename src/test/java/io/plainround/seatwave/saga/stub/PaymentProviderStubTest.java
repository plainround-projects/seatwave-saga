package io.plainround.seatwave.saga.stub;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.management.HotSpotDiagnosticMXBean;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The provider's stub keeps Module 7's promise: one key, one capture, one refund. */
class PaymentProviderStubTest {

  private static final long LOST_ANSWER_MILLIS = 300;
  private static final Duration SHORT_WAIT = Duration.ofMillis(100);
  private static final Duration LONG_WAIT = Duration.ofSeconds(5);
  private static final String KEY = "seatwave-hold-h-1";

  private PaymentProviderStub stub;
  private HttpClient client;

  @BeforeEach
  void startStub() throws IOException {
    stub = PaymentProviderStub.start(new PaymentProviderStub.Settings(0, 0, LOST_ANSWER_MILLIS));
    client = HttpClient.newHttpClient();
  }

  @AfterEach
  void stopStub() {
    client.close();
    stub.close();
  }

  @Test
  void aRepeatedChargeReturnsTheFirstAnswerAndCapturesOnce() throws Exception {
    HttpResponse<String> first = post("/charges?amount=80.00", KEY, LONG_WAIT);
    HttpResponse<String> second = post("/charges?amount=80.00", KEY, LONG_WAIT);

    assertThat(first.statusCode()).isEqualTo(201);
    assertThat(second.body()).isEqualTo(first.body());
    assertThat(get("/admin/ledger?key=" + KEY)).contains("\"captures\":1", "\"refunds\":0");
  }

  @Test
  void aLostAnswerHasMovedTheMoneyAndTheRetryFindsIt() throws Exception {
    post("/admin/script?key=" + KEY + "&behavior=CAPTURE_THEN_TIMEOUT", "admin", LONG_WAIT);

    boolean isTimedOut = false;
    try {
      post("/charges?amount=80.00", KEY, SHORT_WAIT);
    } catch (HttpTimeoutException timedOut) {
      isTimedOut = true;
    }
    HttpResponse<String> retry = post("/charges?amount=80.00", KEY, LONG_WAIT);

    assertThat(isTimedOut).isTrue();
    assertThat(retry.statusCode()).isEqualTo(201);
    assertThat(get("/admin/ledger?key=" + KEY)).contains("\"captures\":1");
  }

  @Test
  void aDeclinedCardIsDeclinedAgainAndMovesNoMoney() throws Exception {
    post("/admin/script?key=" + KEY + "&behavior=DECLINE", "admin", LONG_WAIT);

    HttpResponse<String> first = post("/charges?amount=80.00", KEY, LONG_WAIT);
    HttpResponse<String> second = post("/charges?amount=80.00", KEY, LONG_WAIT);

    assertThat(first.statusCode()).isEqualTo(402);
    assertThat(second.statusCode()).isEqualTo(402);
    assertThat(get("/admin/totals")).contains("\"captures\":0", "\"declines\":1");
  }

  @Test
  void aRepeatedRefundRefundsOnceAndAFailingRefundAnswers503() throws Exception {
    post("/charges?amount=80.00", KEY, LONG_WAIT);
    post("/admin/fail-refunds?count=1", "admin", LONG_WAIT);

    HttpResponse<String> failing = post("/refunds", KEY, LONG_WAIT);
    post("/refunds", KEY, LONG_WAIT);
    HttpResponse<String> repeated = post("/refunds", KEY, LONG_WAIT);

    assertThat(failing.statusCode()).isEqualTo(503);
    assertThat(repeated.statusCode()).isEqualTo(201);
    assertThat(get("/admin/ledger?key=" + KEY)).contains("\"captures\":1", "\"refunds\":1");
  }

  @Test
  void refusesARefundWithoutACaptureAndRequestsWithoutAKey() throws Exception {
    assertThat(post("/refunds", "seatwave-hold-never", LONG_WAIT).statusCode()).isEqualTo(404);
    assertThat(post("/charges?amount=80.00", "", LONG_WAIT).statusCode()).isEqualTo(400);
    assertThat(post("/charges?amount=eighty", KEY, LONG_WAIT).statusCode()).isEqualTo(400);
    assertThat(
            post("/admin/script?key=" + KEY + "&behavior=MAYBE", "admin", LONG_WAIT).statusCode())
        .isEqualTo(400);
  }

  @Test
  void refusesSettingsOutOfRange() {
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> new PaymentProviderStub.Settings(70_000, 0, 0))
        .hasMessageContaining("port");
    org.assertj.core.api.Assertions.assertThatThrownBy(() ->
            PaymentProviderStub.Settings.fromEnvironment(
                java.util.Map.of("STUB_PORT", "8080", "STUB_LATENCY_MILLIS", "fast")))
        .hasMessageContaining("STUB_LATENCY_MILLIS");
  }

  private HttpResponse<String> post(String path, String key, Duration timeout) throws Exception {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + stub.port() + path))
            .timeout(timeout)
            .POST(HttpRequest.BodyPublishers.noBody());
    if (!key.isEmpty()) {
      request.header("Idempotency-Key", key);
    }
    try {
      return client.send(
          request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    } catch (HttpTimeoutException timedOut) {
      if (timeout.equals(LONG_WAIT)) {
        saveThreadDump();
      }
      throw timedOut;
    }
  }

  /**
   * A request the stub answers at once has, rarely, gone unanswered for the whole long wait on a
   * developer machine, and the cause is not known (the README's "Known issue"). The dump shows what
   * every thread, virtual ones included, was doing at that moment, so the next time leaves
   * evidence.
   */
  private static void saveThreadDump() throws IOException {
    Path folder = Path.of("build", "reports", "stub-timeouts");
    Files.createDirectories(folder);
    Path dump = folder.resolve("threads-" + System.nanoTime() + ".txt").toAbsolutePath();
    ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class)
        .dumpThreads(dump.toString(), HotSpotDiagnosticMXBean.ThreadDumpFormat.TEXT_PLAIN);
  }

  private String get(String path) throws Exception {
    return client
        .send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + stub.port() + path)).build(),
            HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
        .body();
  }
}

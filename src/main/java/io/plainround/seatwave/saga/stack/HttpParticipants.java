package io.plainround.seatwave.saga.stack;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.plainround.seatwave.saga.SagaParticipants;
import io.plainround.seatwave.saga.StepReply;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/**
 * The saga's commands sent over HTTP to the inventory and payment services of the stack ({@link
 * CommandServer}). Every failure to get an answer (a timeout, a refused connection, a 5xx, a body
 * that is not the expected shape) is {@code Unavailable}: the outcome is unknown, so the
 * orchestrator sends the same command again later, which is safe because every command is
 * idempotent. The orchestrator is the only layer that retries.
 *
 * <p>Thread-safe: the HTTP client is shared and immutable.
 */
public final class HttpParticipants implements SagaParticipants, AutoCloseable {

  private static final ObjectMapper JSON = JsonMapper.builder().build();
  private static final int OK = 200;
  private static final int CONFLICT = 409;

  private final HttpClient client;
  private final URI inventory;
  private final URI payment;
  private final Duration stepTimeout;
  private final Duration paymentStepTimeout;

  /**
   * Where the two services are and how long to wait: {@code paymentStepTimeout} for the charge and
   * the refund (the provider is slow), {@code stepTimeout} for every other command.
   */
  public HttpParticipants(
      URI inventory, URI payment, Duration stepTimeout, Duration paymentStepTimeout) {
    this.inventory = Objects.requireNonNull(inventory, "inventory");
    this.payment = Objects.requireNonNull(payment, "payment");
    this.stepTimeout = Objects.requireNonNull(stepTimeout, "stepTimeout");
    this.paymentStepTimeout = Objects.requireNonNull(paymentStepTimeout, "paymentStepTimeout");
    this.client = HttpClient.newBuilder().connectTimeout(stepTimeout).build();
  }

  @Override
  public StepReply lockSeat(String holdId, String sagaId, long nowMillis) {
    return send(inventory, "lock-seat", Map.of("hold_id", holdId, "saga_id", sagaId), stepTimeout);
  }

  @Override
  public StepReply checkSeatActive(String holdId, long nowMillis) {
    return send(inventory, "check-seat", Map.of("hold_id", holdId), stepTimeout);
  }

  @Override
  public StepReply chargePayment(String providerKey, long nowMillis) {
    return send(payment, "charge", Map.of("provider_key", providerKey), paymentStepTimeout);
  }

  @Override
  public StepReply confirmSeat(String holdId, long nowMillis) {
    return send(inventory, "confirm-seat", Map.of("hold_id", holdId), stepTimeout);
  }

  @Override
  public StepReply releaseSeat(String holdId, long nowMillis) {
    return send(inventory, "release-seat", Map.of("hold_id", holdId), stepTimeout);
  }

  @Override
  public StepReply refundPayment(String providerKey, long nowMillis) {
    return send(payment, "refund", Map.of("provider_key", providerKey), paymentStepTimeout);
  }

  @Override
  public void close() {
    client.close();
  }

  private StepReply send(URI service, String command, Map<String, String> body, Duration timeout) {
    try {
      HttpRequest request =
          HttpRequest.newBuilder(service.resolve("/commands/" + command))
              .timeout(timeout)
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofByteArray(JSON.writeValueAsBytes(body)))
              .build();
      HttpResponse<String> response =
          client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
      return replyOf(response.statusCode(), response.body(), command);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      return new StepReply.Unavailable(command + " interrupted");
    } catch (IOException noAnswer) {
      // A timeout included: the participant may have done the step, so the outcome is unknown.
      return new StepReply.Unavailable(
          command + ": no answer (" + noAnswer.getClass().getSimpleName() + ")");
    }
  }

  private static StepReply replyOf(int status, String body, String command) throws IOException {
    if (status != OK && status != CONFLICT) {
      return new StepReply.Unavailable(command + " answered " + status);
    }
    JsonNode answer = JSON.readTree(body);
    String outcome = answer.path("outcome").asText("");
    String detail = answer.path("detail").asText("");
    return switch (outcome) {
      case "SUCCEEDED" -> new StepReply.Succeeded(detail);
      case "ALREADY_DONE" -> new StepReply.AlreadyDone(detail);
      case "REFUSED" -> new StepReply.Refused(detail);
      default -> new StepReply.Unavailable(command + " answered an unknown outcome");
    };
  }
}

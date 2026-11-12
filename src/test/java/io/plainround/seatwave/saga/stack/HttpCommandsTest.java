package io.plainround.seatwave.saga.stack;

import static org.assertj.core.api.Assertions.assertThat;

import io.plainround.seatwave.saga.StepReply;
import io.plainround.seatwave.saga.stub.PaymentProviderStub;
import java.math.BigDecimal;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The HTTP edges without Docker: a participant's answers map to the four replies, a timeout is
 * "unavailable", never "failed", and the provider client reads the stub's answers.
 */
class HttpCommandsTest {

  /** Generous for a call on this machine even beside busy loops; the slow answers take longer. */
  private static final Duration SHORT = Duration.ofSeconds(2);

  private static final Duration TIMEOUT = Duration.ofSeconds(6);
  private static final String HOLD = "0190f3c8-1111-7000-8000-000000000001";
  private static final String KEY = "seatwave-hold-" + HOLD;

  @Test
  void mapsEachAnswerToItsReplyAndASlowAnswerToUnavailable() throws Exception {
    Map<String, CommandServer.Command> commands =
        Map.of(
            "/commands/lock-seat", body -> new StepReply.Succeeded("CONFIRMING"),
            "/commands/check-seat", body -> new StepReply.Refused("not active"),
            "/commands/confirm-seat", body -> new StepReply.AlreadyDone("CONFIRMED"),
            "/commands/release-seat",
                body -> {
                  Thread.sleep(TIMEOUT.toMillis());
                  return new StepReply.Succeeded("late");
                },
            "/commands/charge",
                body -> {
                  throw new java.sql.SQLException("database down");
                },
            "/commands/refund", body -> new StepReply.Succeeded(Fields.providerKey(body)));
    try (CommandServer server = CommandServer.start(0, commands);
        HttpParticipants participants =
            new HttpParticipants(
                URI.create("http://127.0.0.1:" + server.port()),
                URI.create("http://127.0.0.1:" + server.port()),
                SHORT,
                TIMEOUT)) {
      assertThat(participants.lockSeat(HOLD, HOLD, 0)).isInstanceOf(StepReply.Succeeded.class);
      assertThat(participants.checkSeatActive(HOLD, 0)).isInstanceOf(StepReply.Refused.class);
      assertThat(participants.confirmSeat(HOLD, 0)).isInstanceOf(StepReply.AlreadyDone.class);
      assertThat(participants.releaseSeat(HOLD, 0)).isInstanceOf(StepReply.Unavailable.class);
      assertThat(participants.chargePayment(KEY, 0)).isInstanceOf(StepReply.Unavailable.class);
      assertThat(participants.refundPayment("not-a-key", 0))
          .isInstanceOf(StepReply.Unavailable.class);
      assertThat(participants.refundPayment(KEY, 0)).isEqualTo(new StepReply.Succeeded(KEY));
    }
  }

  @Test
  void aParticipantThatDoesNotAnswerIsUnavailable() {
    try (HttpParticipants participants =
        new HttpParticipants(
            URI.create("http://127.0.0.1:1"), URI.create("http://127.0.0.1:1"), SHORT, SHORT)) {
      assertThat(participants.lockSeat(HOLD, HOLD, 0)).isInstanceOf(StepReply.Unavailable.class);
    }
  }

  @Test
  void theProviderClientReadsCapturesDeclinesRefundsAndLostAnswers() throws Exception {
    try (PaymentProviderStub stub =
            PaymentProviderStub.start(new PaymentProviderStub.Settings(0, 0, TIMEOUT.toMillis()));
        ProviderClient provider =
            new ProviderClient(URI.create("http://127.0.0.1:" + stub.port()), SHORT);
        ProviderClient admin =
            new ProviderClient(URI.create("http://127.0.0.1:" + stub.port()), TIMEOUT)) {
      BigDecimal price = new BigDecimal("80.00");
      assertThat(provider.charge(KEY, price)).isEqualTo(ProviderClient.Answer.CAPTURED);
      assertThat(provider.refund(KEY)).isEqualTo(ProviderClient.Answer.REFUNDED);
      assertThat(provider.refund("seatwave-hold-other"))
          .isEqualTo(ProviderClient.Answer.NOTHING_TO_REFUND);
      java.net.http.HttpClient.newHttpClient()
          .send(
              java.net.http.HttpRequest.newBuilder(
                      URI.create(
                          "http://127.0.0.1:"
                              + stub.port()
                              + "/admin/script?key=lost&behavior=CAPTURE_THEN_TIMEOUT"))
                  .POST(java.net.http.HttpRequest.BodyPublishers.noBody())
                  .build(),
              java.net.http.HttpResponse.BodyHandlers.discarding());
      assertThat(provider.charge("lost", price)).isEqualTo(ProviderClient.Answer.NO_ANSWER);
      assertThat(admin.charge("lost", price)).isEqualTo(ProviderClient.Answer.CAPTURED);
    }
  }
}

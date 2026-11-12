package io.plainround.seatwave.saga.stack;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariDataSource;
import io.plainround.seatwave.saga.BookingSaga;
import io.plainround.seatwave.saga.SagaRecord;
import io.plainround.seatwave.saga.StepReply;
import io.plainround.seatwave.saga.jdbc.JdbcInventory;
import io.plainround.seatwave.saga.jdbc.JdbcPayments;
import io.plainround.seatwave.saga.jdbc.JdbcSagaStore;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The orchestrated saga over HTTP against the three real databases and the provider's stub: the
 * same {@link BookingSaga} as every program, now with {@link JdbcSagaStore} and {@link
 * HttpParticipants}, and the two participants served in this JVM by {@link CommandServer}. Each end
 * is judged by the hold's row and the provider's own ledger.
 */
@Tag("stack")
class SagaOverHttpStackTest {

  /** Seats 38,001 to 38,999 are this class's. */
  private static final AtomicInteger NEXT_SEAT = new AtomicInteger(38_001);

  private static final Duration HOLD = Duration.ofMinutes(5);
  private static final long FIRST_RETRY_MILLIS = 200;
  private static final long MAX_RETRY_MILLIS = 2_000;
  private static final int MAX_STEPS = 50;

  private static StackSettings settings;
  private static HikariDataSource booking;
  private static HikariDataSource inventoryDatabase;
  private static HikariDataSource paymentDatabase;
  private static ProviderClient provider;
  private static CommandServer inventoryServer;
  private static CommandServer paymentServer;
  private static HttpParticipants participants;
  private static JdbcInventory inventory;
  private static HttpClient admin;

  @BeforeAll
  static void start() throws Exception {
    settings = StackSettings.load();
    booking = settings.openPool("saga-booking", settings.databases().bookingUrl());
    inventoryDatabase = settings.openPool("saga-inventory", settings.databases().inventoryUrl());
    paymentDatabase = settings.openPool("saga-payment", settings.databases().paymentUrl());
    provider = new ProviderClient(settings.services().provider(), settings.timeouts().provider());
    inventory = new JdbcInventory(inventoryDatabase, HOLD);
    JdbcPayments payments = new JdbcPayments(paymentDatabase, provider, settings.seatPrice());
    inventoryServer =
        CommandServer.start(
            0,
            Map.of(
                "/commands/lock-seat",
                body ->
                    inventory.lockForPayment(
                        Fields.uuid(body, "hold_id"), Fields.uuid(body, "saga_id")),
                "/commands/confirm-seat",
                body -> inventory.confirm(Fields.uuid(body, "hold_id")),
                "/commands/release-seat",
                body -> inventory.release(Fields.uuid(body, "hold_id"))));
    paymentServer =
        CommandServer.start(
            0,
            Map.of(
                "/commands/charge", body -> payments.charge(Fields.providerKey(body)),
                "/commands/refund", body -> payments.refund(Fields.providerKey(body))));
    participants =
        new HttpParticipants(
            URI.create("http://127.0.0.1:" + inventoryServer.port()),
            URI.create("http://127.0.0.1:" + paymentServer.port()),
            settings.timeouts().stack(),
            settings.timeouts().paymentStep());
    admin = HttpClient.newHttpClient();
  }

  @AfterAll
  static void stop() {
    admin.close();
    participants.close();
    paymentServer.close();
    inventoryServer.close();
    provider.close();
    paymentDatabase.close();
    inventoryDatabase.close();
    booking.close();
  }

  @Test
  void aBookingCompletesWithOneCaptureAndAConfirmedSeat() throws Exception {
    Booking run = newBooking("CAPTURE");

    BookingSaga.State end = runToEnd(saga(), run);

    assertThat(end).isEqualTo(BookingSaga.State.COMPLETED);
    assertThat(inventory.holdState(run.hold())).isEqualTo("CONFIRMED");
    assertThat(ledger(run)).contains("\"captures\":1", "\"refunds\":0");
  }

  @Test
  void aDeclinedCardIsCompensatedAndTheSeatReleased() throws Exception {
    Booking run = newBooking("DECLINE");

    BookingSaga.State end = runToEnd(saga(), run);

    assertThat(end).isEqualTo(BookingSaga.State.COMPENSATED);
    assertThat(inventory.holdState(run.hold())).isEqualTo("RELEASED");
    assertThat(ledger(run)).contains("\"captures\":0");
  }

  @Test
  void aLostPaymentAnswerIsResolvedWithExactlyOneCapture() throws Exception {
    Booking run = newBooking("CAPTURE_THEN_TIMEOUT");
    BookingSaga saga = saga();
    saga.start(run.booking().toString(), run.hold().toString(), System.currentTimeMillis());
    saga.step(run.booking().toString(), System.currentTimeMillis());

    BookingSaga.StepResult charge = saga.step(run.booking().toString(), System.currentTimeMillis());
    BookingSaga.State end = driveToEnd(saga, run);

    assertThat(charge.reply()).isInstanceOf(StepReply.Unavailable.class);
    assertThat(end).isEqualTo(BookingSaga.State.COMPLETED);
    assertThat(ledger(run)).contains("\"captures\":1", "\"refunds\":0");
  }

  @Test
  void anOrchestratorThatCrashedAfterTheChargeResumesWithoutASecondCapture() throws Exception {
    Booking run = newBooking("CAPTURE");
    BookingSaga saga = saga();
    saga.start(run.booking().toString(), run.hold().toString(), System.currentTimeMillis());
    saga.step(run.booking().toString(), System.currentTimeMillis());
    saga.stepAndCrashBeforeRecording(run.booking().toString(), System.currentTimeMillis());

    BookingSaga.State end = driveToEnd(saga(), run);

    assertThat(end).isEqualTo(BookingSaga.State.COMPLETED);
    assertThat(ledger(run)).contains("\"captures\":1");
  }

  private record Booking(UUID booking, UUID hold) {
    String providerKey() {
      return SagaRecord.PROVIDER_KEY_PREFIX + hold;
    }
  }

  private static Booking newBooking(String behavior) throws Exception {
    int seat = NEXT_SEAT.getAndIncrement();
    Booking run = new Booking(UUID.randomUUID(), UUID.randomUUID());
    assertThat(inventory.hold(run.hold(), "A-" + seat, seat))
        .isInstanceOf(StepReply.Succeeded.class);
    HttpResponse<String> scripted =
        admin.send(
            HttpRequest.newBuilder(
                    settings
                        .services()
                        .provider()
                        .resolve(
                            "/admin/script?key=" + run.providerKey() + "&behavior=" + behavior))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build(),
            HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    assertThat(scripted.statusCode()).isEqualTo(200);
    return run;
  }

  private static BookingSaga saga() {
    return new BookingSaga(
        new JdbcSagaStore(booking, settings.seatPrice()),
        participants,
        new BookingSaga.Settings(
            BookingSaga.SeatProtection.SEMANTIC_LOCK, FIRST_RETRY_MILLIS, MAX_RETRY_MILLIS));
  }

  private static BookingSaga.State runToEnd(BookingSaga saga, Booking run) throws Exception {
    saga.start(run.booking().toString(), run.hold().toString(), System.currentTimeMillis());
    return driveToEnd(saga, run);
  }

  private static BookingSaga.State driveToEnd(BookingSaga saga, Booking run) throws Exception {
    String id = run.booking().toString();
    for (int stepIndex = 0; stepIndex < MAX_STEPS && !saga.state(id).isFinal(); stepIndex++) {
      BookingSaga.StepResult result = saga.step(id, System.currentTimeMillis());
      if (result.from() == result.to()) {
        Thread.sleep(Math.max(0, result.retryAtMillis() - System.currentTimeMillis()));
      }
    }
    return saga.state(id);
  }

  private static String ledger(Booking run) throws Exception {
    return admin
        .send(
            HttpRequest.newBuilder(
                    settings
                        .services()
                        .provider()
                        .resolve("/admin/ledger?key=" + run.providerKey()))
                .build(),
            HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
        .body();
  }
}

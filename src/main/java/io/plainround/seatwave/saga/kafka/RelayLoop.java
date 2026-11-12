package io.plainround.seatwave.saga.kafka;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs one relay until it is stopped or fenced: a pass, then a pause of one poll interval when the
 * pass found fewer rows than a batch (the outbox is drained), at once otherwise (a backlog). A pass
 * that fails (broker or database unavailable) is logged and retried after the interval; nothing is
 * lost, because nothing was marked.
 *
 * <p>Thread-safe: {@link #stop} may be called from any thread; {@link #run} runs on one.
 */
public final class RelayLoop implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(RelayLoop.class);

  private final PostgresKafkaRelay relay;
  private final Duration pollInterval;
  private final int batchSize;
  private final AtomicBoolean isRunning = new AtomicBoolean(true);

  /**
   * A loop over {@code relay}, pausing {@code pollInterval} when a pass is below {@code batchSize}.
   */
  public RelayLoop(PostgresKafkaRelay relay, Duration pollInterval, int batchSize) {
    this.relay = Objects.requireNonNull(relay, "relay");
    this.pollInterval = Objects.requireNonNull(pollInterval, "pollInterval");
    this.batchSize = batchSize;
  }

  @Override
  public void run() {
    while (isRunning.get()) {
      try {
        PostgresKafkaRelay.Pass pass = relay.relayOnce();
        if (pass.isFenced()) {
          LOG.warn(
              "relay fenced by a newer token: stopping; {} rows will be published again",
              pass.published());
          return;
        }
        if (pass.published() < batchSize) {
          Thread.sleep(pollInterval);
        }
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        return;
      } catch (SQLException | TimeoutException | RuntimeException failedPass) {
        LOG.warn(
            "relay pass failed, retrying in {} ms: {}",
            pollInterval.toMillis(),
            failedPass.toString());
        if (!pause()) {
          return;
        }
      }
    }
  }

  /** Asks the loop to stop after its current pass. */
  public void stop() {
    isRunning.set(false);
  }

  private boolean pause() {
    try {
      Thread.sleep(pollInterval);
      return true;
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      return false;
    }
  }
}

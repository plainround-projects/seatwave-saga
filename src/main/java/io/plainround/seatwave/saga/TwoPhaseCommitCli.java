package io.plainround.seatwave.saga;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Chapter 28.5 as a model: a coordinator and two participants (inventory holds seat A-7's row lock,
 * payment holds the fan's authorization) run two-phase commit. Three runs: no failure; the
 * coordinator crashes after both votes and before deciding; the coordinator crashes after telling
 * payment "commit" and before telling inventory. Prints what each party knows, how long the lock is
 * held, and what a participant that decided alone would break. Virtual time.
 */
final class TwoPhaseCommitCli {

  /** One message on the network inside the region (an assumption, Module 5's order of size). */
  private static final long MESSAGE_MILLIS = 1;

  /** One force of a log record to disk (an assumption, Module 26's order of size). */
  private static final long FORCE_MILLIS = 2;

  /** How long the coordinator's machine takes to come back (an assumption for the run). */
  private static final long COORDINATOR_DOWN_MILLIS = 120_000;

  /** A prepared participant that hears nothing asks again this often (an assumption). */
  private static final long PARTICIPANT_TIMEOUT_MILLIS = 30_000;

  /** Module 7 and Section 7's peak: confirms a second, each one a 2PC transaction here. */
  private static final long CONFIRMS_PER_SECOND = 1_500;

  private static final double MILLIS_PER_SECOND = 1_000.0;

  private enum Crash {
    NONE,
    AFTER_VOTES_BEFORE_DECISION,
    AFTER_FIRST_COMMIT_MESSAGE
  }

  private enum ParticipantState {
    WORKING,
    PREPARED,
    COMMITTED,
    ABORTED
  }

  private TwoPhaseCommitCli() {}

  public static void main(String[] arguments) {
    for (Crash crash : Crash.values()) {
      System.out.println("Run: coordinator crash " + crash.name().toLowerCase(Locale.ROOT));
      run(crash).forEach(System.out::println);
      System.out.println();
    }
    long preparedWindowMillis = MESSAGE_MILLIS + FORCE_MILLIS + MESSAGE_MILLIS + FORCE_MILLIS;
    System.out.println(
        String.format(
            Locale.ROOT,
            "At %,d confirms a second, a coordinator down for %.0f s leaves about %d transactions"
                + " in doubt (rate x the %d ms from prepare to decision), each holding its seat"
                + " lock for the whole outage, and refuses %,d new confirms.",
            CONFIRMS_PER_SECOND,
            COORDINATOR_DOWN_MILLIS / MILLIS_PER_SECOND,
            Math.max(1, CONFIRMS_PER_SECOND * preparedWindowMillis / (long) MILLIS_PER_SECOND),
            preparedWindowMillis,
            CONFIRMS_PER_SECOND * COORDINATOR_DOWN_MILLIS / (long) MILLIS_PER_SECOND));
  }

  private static List<String> run(Crash crash) {
    List<String> lines = new ArrayList<>();
    ParticipantState inventory = ParticipantState.WORKING;
    ParticipantState payment = ParticipantState.WORKING;
    long nowMillis = 0;
    lines.add(line(nowMillis, "coordinator -> both: PREPARE", inventory, payment));
    nowMillis += MESSAGE_MILLIS + FORCE_MILLIS;
    inventory = ParticipantState.PREPARED;
    payment = ParticipantState.PREPARED;
    long lockTakenAtMillis = nowMillis;
    lines.add(
        line(
            nowMillis,
            "both forced a prepare record and voted YES; seat A-7 locked",
            inventory,
            payment));
    nowMillis += MESSAGE_MILLIS;
    if (crash == Crash.AFTER_VOTES_BEFORE_DECISION) {
      lines.add(
          line(nowMillis, "coordinator crashes before forcing its decision", inventory, payment));
      for (long askedAtMillis = nowMillis + PARTICIPANT_TIMEOUT_MILLIS;
          askedAtMillis < nowMillis + COORDINATOR_DOWN_MILLIS;
          askedAtMillis += PARTICIPANT_TIMEOUT_MILLIS) {
        lines.add(
            line(
                askedAtMillis,
                "inventory asks payment: 'prepared too, no decision'; both wait",
                inventory,
                payment));
      }
      nowMillis += COORDINATOR_DOWN_MILLIS;
      inventory = ParticipantState.ABORTED;
      payment = ParticipantState.ABORTED;
      lines.add(
          line(
              nowMillis,
              "coordinator restarts, finds no decision, aborts (presumed abort)",
              inventory,
              payment));
    } else {
      nowMillis += FORCE_MILLIS;
      lines.add(
          line(
              nowMillis,
              "coordinator forced COMMIT: the decision exists from here",
              inventory,
              payment));
      nowMillis += MESSAGE_MILLIS;
      payment = ParticipantState.COMMITTED;
      lines.add(line(nowMillis, "payment got COMMIT: the fan is charged", inventory, payment));
      if (crash == Crash.AFTER_FIRST_COMMIT_MESSAGE) {
        lines.add(
            line(nowMillis, "coordinator crashes before telling inventory", inventory, payment));
        lines.add(
            "      if inventory gave up and aborted alone now: charged, and seat A-7 released"
                + " (the walkthrough's incident); so it must wait, holding the lock");
        nowMillis += COORDINATOR_DOWN_MILLIS;
        lines.add(
            line(
                nowMillis,
                "coordinator restarts, reads COMMIT in its log, resends it",
                inventory,
                payment));
      }
      inventory = ParticipantState.COMMITTED;
      lines.add(line(nowMillis, "inventory got COMMIT: seat A-7 sold", inventory, payment));
    }
    lines.add(
        String.format(
            Locale.ROOT,
            "      seat A-7 locked for %.3f s",
            (nowMillis - lockTakenAtMillis) / MILLIS_PER_SECOND));
    return lines;
  }

  private static String line(
      long atMillis, String what, ParticipantState inventory, ParticipantState payment) {
    return String.format(
        Locale.ROOT,
        "  %9.3f s  inventory %-9s payment %-9s %s",
        atMillis / MILLIS_PER_SECOND,
        inventory,
        payment,
        what);
  }
}

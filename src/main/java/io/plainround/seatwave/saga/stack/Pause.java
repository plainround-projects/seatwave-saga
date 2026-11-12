package io.plainround.seatwave.saga.stack;

/**
 * A drill's crash point: prints {@code PAUSED pid=<id> <where>} and blocks until the process is
 * killed. scripts/drill.sh waits for the line, checks that the id is the process it started, and
 * kills it with SIGKILL, so the kill lands exactly at the point named, never a little before or
 * after.
 */
final class Pause {

  private Pause() {}

  static void untilKilled(String where) throws InterruptedException {
    System.out.println("PAUSED pid=" + ProcessHandle.current().pid() + " " + where);
    System.out.flush();
    Thread.currentThread().join();
  }
}

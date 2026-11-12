package io.plainround.seatwave.saga;

import java.util.Objects;

/**
 * A participant's answer to one saga command. The four cases are the four things a caller must
 * treat differently, so a switch over them has no default branch. They play the part of result4j's
 * {@code Result} with a sealed error, with two success cases instead of one, because the saga must
 * tell "done now" from "done before" only when it prints, and must tell a final refusal from an
 * unknown outcome always.
 */
public sealed interface StepReply {

  /** The step happened now. */
  record Succeeded(String detail) implements StepReply {
    public Succeeded {
      Objects.requireNonNull(detail, "detail");
    }
  }

  /** The step had already happened: a repeated command, answered as the first one was. */
  record AlreadyDone(String detail) implements StepReply {
    public AlreadyDone {
      Objects.requireNonNull(detail, "detail");
    }
  }

  /** A business rule says no (card declined, hold expired): final, retrying will not help. */
  record Refused(String reason) implements StepReply {
    public Refused {
      Objects.requireNonNull(reason, "reason");
    }
  }

  /** No answer or a transient failure (timeout, 503): the outcome is unknown, so retry. */
  record Unavailable(String reason) implements StepReply {
    public Unavailable {
      Objects.requireNonNull(reason, "reason");
    }
  }
}

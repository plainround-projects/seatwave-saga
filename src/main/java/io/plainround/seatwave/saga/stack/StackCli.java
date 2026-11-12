package io.plainround.seatwave.saga.stack;

import java.util.List;
import java.util.Map;

/**
 * Small helpers for scripts/drill.sh: {@code create-topic --topic T} creates a topic of 24
 * partitions and one replica on purpose (the broker does not create topics on its own).
 */
public final class StackCli {

  private StackCli() {}

  /** Runs one helper. */
  public static void main(String[] arguments) throws Exception {
    StackSettings settings = StackSettings.load();
    if (arguments.length < 1 || !arguments[0].equals("create-topic")) {
      throw new IllegalArgumentException("usage: StackCli create-topic --topic T");
    }
    String topic =
        Arguments.parse(List.of(arguments).subList(1, arguments.length), Map.of("topic", ""))
            .name("topic");
    StackClients.createTopic(settings.kafkaBootstrap(), topic, settings.timeouts().stack());
    System.out.println("topic " + topic + " exists");
  }
}

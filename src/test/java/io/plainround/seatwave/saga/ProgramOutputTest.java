package io.plainround.seatwave.saga;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The five programs are deterministic (fixed seed, virtual time), so each must print exactly the
 * output the module embeds, kept in {@code src/test/resources/golden}. A change to any class they
 * drive that changes a number fails here before it reaches the text.
 */
class ProgramOutputTest {

  @ParameterizedTest
  @ValueSource(
      strings = {"DualWriteCli", "SagaCli", "ChoreographyCli", "TwoPhaseCommitCli", "ReadModelCli"})
  @ResourceLock(Resources.SYSTEM_OUT)
  void printsTheOutputTheModuleShows(String program) throws Exception {
    String expected;
    try (InputStream golden = getClass().getResourceAsStream("/golden/" + program + ".txt")) {
      assertThat(golden).as("golden output of %s", program).isNotNull();
      expected = new String(golden.readAllBytes(), StandardCharsets.UTF_8);
    }

    String printed = run(program);

    assertThat(printed).isEqualTo(expected);
  }

  private static String run(String program) throws ReflectiveOperationException, IOException {
    Method main =
        Class.forName("io.plainround.seatwave.saga." + program).getMethod("main", String[].class);
    PrintStream original = System.out;
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    try (PrintStream capture = new PrintStream(captured, true, StandardCharsets.UTF_8)) {
      System.setOut(capture);
      main.invoke(null, (Object) new String[0]);
    } finally {
      System.setOut(original);
    }
    return captured.toString(StandardCharsets.UTF_8);
  }
}

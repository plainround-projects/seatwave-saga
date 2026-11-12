package io.plainround.seatwave.saga.stack;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Every problem of the settings file is reported together, and the real file is complete. */
class StackSettingsTest {

  @Test
  void theProjectsSettingsFileHasEveryKeyInRange(@TempDir Path project) throws Exception {
    Path config = Files.createDirectories(project.resolve("config"));
    Files.copy(Path.of("config/stack.properties"), config.resolve("stack.properties"));
    Files.createDirectories(project.resolve(".secrets"));
    Files.writeString(
        project.resolve(".secrets/postgres.password"), "lab\n", StandardCharsets.UTF_8);

    StackSettings settings = StackSettings.load(config.resolve("stack.properties"));

    assertThat(settings.databases().password()).isEqualTo("lab");
    assertThat(settings.toString()).doesNotContain("lab\"").doesNotContain("password");
    assertThat(settings.timeouts().provider()).isLessThan(settings.timeouts().paymentStep());
  }

  @Test
  void reportsEveryProblemAtOnce(@TempDir Path project) throws Exception {
    Path config = Files.createDirectories(project.resolve("config"));
    String broken =
        Files.readString(Path.of("config/stack.properties"), StandardCharsets.UTF_8)
            .replace("relay.batchSize=1000", "relay.batchSize=0")
            .replace("provider.timeoutMillis=3000", "provider.timeoutMillis=9000")
            .replace("seat.price=80.00", "seat.price=eighty")
            .replace("booking.url=http://127.0.0.1:22810", "booking.url=ftp://somewhere");
    Files.writeString(config.resolve("stack.properties"), broken, StandardCharsets.UTF_8);

    assertThatThrownBy(() -> StackSettings.load(config.resolve("stack.properties")))
        .hasMessageContaining("5 problem(s)")
        .hasMessageContaining("relay.batchSize: 0 is outside 1 to 10000")
        .hasMessageContaining("provider.timeoutMillis must be below")
        .hasMessageContaining("seat.price: 'eighty'")
        .hasMessageContaining("booking.url")
        .hasMessageContaining("cannot read");
  }
}

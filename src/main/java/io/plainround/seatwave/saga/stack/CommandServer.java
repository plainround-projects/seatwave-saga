package io.plainround.seatwave.saga.stack;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.plainround.seatwave.saga.StepReply;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A participant's HTTP side: {@code POST /commands/<name>} with a small JSON body, answered with
 * the command's {@link StepReply} as JSON. The status code says what the orchestrator must do: 200
 * for done (now or before), 409 for refused, 503 when the outcome is unknown (the database did not
 * answer), 400 for a body that does not fit, in RFC 9457's problem shape. Listens on the loopback
 * address only.
 *
 * <p>Thread-safe: one virtual thread per request; the commands must be thread-safe themselves.
 */
public final class CommandServer implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(CommandServer.class);
  private static final ObjectMapper JSON = JsonMapper.builder().build();
  private static final int OK = 200;
  private static final int BAD_REQUEST = 400;
  private static final int NOT_FOUND = 404;
  private static final int CONFLICT = 409;
  private static final int UNAVAILABLE = 503;
  private static final int BACKLOG = 256;
  private static final int MAX_BODY_BYTES = 4_096;

  /** One command: reads its fields from the body and answers. */
  @FunctionalInterface
  public interface Command {
    /** Throws {@link IllegalArgumentException} for a bad body; any other exception means 503. */
    StepReply run(JsonNode body) throws Exception;
  }

  private final HttpServer server;
  private final ExecutorService executor;

  private CommandServer(int port, Map<String, Command> commands) throws IOException {
    server =
        HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), BACKLOG);
    executor = Executors.newVirtualThreadPerTaskExecutor();
    server.setExecutor(executor);
    Map<String, Command> byPath = Map.copyOf(commands);
    server.createContext("/", exchange -> handle(exchange, byPath));
  }

  /** Starts a server on {@code port} (0 for any free one) with commands by path. */
  public static CommandServer start(int port, Map<String, Command> commands) throws IOException {
    CommandServer commandServer = new CommandServer(port, Objects.requireNonNull(commands));
    commandServer.server.start();
    return commandServer;
  }

  /** The port it listens on. */
  public int port() {
    return server.getAddress().getPort();
  }

  /** The body of an answer: outcome and detail. Shared with {@link HttpParticipants}. */
  static String outcomeOf(StepReply reply) {
    return switch (reply) {
      case StepReply.Succeeded _ -> "SUCCEEDED";
      case StepReply.AlreadyDone _ -> "ALREADY_DONE";
      case StepReply.Refused _ -> "REFUSED";
      case StepReply.Unavailable _ -> "UNAVAILABLE";
    };
  }

  @Override
  public void close() {
    server.stop(0);
    executor.close();
  }

  private static void handle(HttpExchange exchange, Map<String, Command> commands)
      throws IOException {
    try (exchange) {
      Command command = commands.get(exchange.getRequestURI().getPath());
      if (command == null || !exchange.getRequestMethod().equals("POST")) {
        problem(exchange, NOT_FOUND, "no such command");
        return;
      }
      JsonNode body;
      try (InputStream input = exchange.getRequestBody()) {
        byte[] bytes = input.readNBytes(MAX_BODY_BYTES + 1);
        if (bytes.length > MAX_BODY_BYTES) {
          problem(exchange, BAD_REQUEST, "body over " + MAX_BODY_BYTES + " bytes");
          return;
        }
        body = JSON.readTree(bytes);
      } catch (IOException notJson) {
        problem(exchange, BAD_REQUEST, "body is not JSON");
        return;
      }
      StepReply reply;
      try {
        reply = run(command, body);
      } catch (IllegalArgumentException badField) {
        problem(exchange, BAD_REQUEST, badField.getMessage());
        return;
      }
      answer(exchange, reply);
    }
  }

  private static StepReply run(Command command, JsonNode body) {
    try {
      return command.run(body);
    } catch (IllegalArgumentException badField) {
      // A body that does not fit: the caller's mistake, answered 400 by handle.
      throw badField;
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      return new StepReply.Unavailable("interrupted");
    } catch (Exception failure) {
      // The database or the provider did not answer: nothing was committed, so the orchestrator
      // may send the same command again.
      LOG.warn("command failed, answering 503: {}", failure.toString());
      return new StepReply.Unavailable("service could not complete the command");
    }
  }

  private static void answer(HttpExchange exchange, StepReply reply) throws IOException {
    int status =
        switch (reply) {
          case StepReply.Succeeded _, StepReply.AlreadyDone _ -> OK;
          case StepReply.Refused _ -> CONFLICT;
          case StepReply.Unavailable _ -> UNAVAILABLE;
        };
    String detail =
        switch (reply) {
          case StepReply.Succeeded succeeded -> succeeded.detail();
          case StepReply.AlreadyDone alreadyDone -> alreadyDone.detail();
          case StepReply.Refused refused -> refused.reason();
          case StepReply.Unavailable unavailable -> unavailable.reason();
        };
    Map<String, String> body = new LinkedHashMap<>();
    body.put("outcome", outcomeOf(reply));
    body.put("detail", detail);
    send(exchange, status, "application/json", JSON.writeValueAsBytes(body));
  }

  private static void problem(HttpExchange exchange, int status, String detail) throws IOException {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("type", "about:blank");
    body.put("title", status == NOT_FOUND ? "Not Found" : "Bad Request");
    body.put("status", status);
    body.put("detail", detail);
    send(exchange, status, "application/problem+json", JSON.writeValueAsBytes(body));
  }

  private static void send(HttpExchange exchange, int status, String type, byte[] body)
      throws IOException {
    exchange.getResponseHeaders().set("Content-Type", type);
    exchange.sendResponseHeaders(status, body.length);
    try (OutputStream output = exchange.getResponseBody()) {
      output.write(body);
    }
  }
}

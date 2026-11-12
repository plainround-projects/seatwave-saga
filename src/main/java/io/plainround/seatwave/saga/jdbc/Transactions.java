package io.plainround.seatwave.saga.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;

/**
 * Runs one unit of work in one database transaction: commit if it returns, roll back if it throws.
 * Read committed, PostgreSQL's default, is the level of every transaction here: each statement is a
 * conditional update on one row, which takes the row's lock, so two transactions on one hold or
 * saga run one after the other and the second sees the first's result.
 */
final class Transactions {

  /** The work of one transaction. */
  @FunctionalInterface
  interface Work<T> {
    T run(Connection transaction) throws SQLException;
  }

  private Transactions() {}

  static <T> T inTransaction(DataSource database, Work<T> work) throws SQLException {
    try (Connection connection = database.getConnection()) {
      connection.setAutoCommit(false);
      try {
        T result = work.run(connection);
        connection.commit();
        return result;
      } catch (SQLException | RuntimeException failure) {
        connection.rollback();
        throw failure;
      }
    }
  }
}

package io.vertx.starter;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBMySqlVerticle extends DBVerticle {
  static final String queueName = "mysql.queue";
  private HikariDataSource ds;
  private final int MAX_POOL_SIZE = 4;

  @Override
  public void start(Promise<Void> promise) throws Exception {
    HikariConfig config = new HikariConfig();
    String osName = System.getProperty("os.name");
    if (osName.contains("Mac OS")) {
      config.setJdbcUrl("jdbc:mysql://34.201.19.193/twitter?useSSL=false");
    } else {
      config.setJdbcUrl("jdbc:mysql://localhost/twitter?useSSL=false");
    }
    config.setUsername("og");
    config.setPassword("123456");
    config.setMaximumPoolSize(MAX_POOL_SIZE);
    config.addDataSourceProperty("cachePrepStmts", "true");
    config.addDataSourceProperty("prepStmtCacheSize", "250");
    config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
    ds = new HikariDataSource(config);
    vertx.eventBus().consumer(queueName, this::onMessage);
    promise.complete();
  }

  @Override
  void fetchQ2(Message<JsonObject> message) {
    JsonObject request = message.body();
    long reqUser = request.getLong("user");
    String sql =
        String.format(
            "select description, screen_name, hashtags, records from q2_tweets where uid = %d;",
            reqUser);

    try (Connection conn = ds.getConnection()) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      JsonObject result = new JsonObject();
      if (rs.next()) {
        result.put("description", rs.getString("description"));
        result.put("screen_name", rs.getString("screen_name"));
        result.put("hashtags", rs.getString("hashtags"));
        result.put("records", rs.getString("records"));
      }
      message.reply(result);
    } catch (SQLException e) {
      LOGGER.error(e);
    }
  }

  @Override
  void fetchQ3Words(Message<JsonObject> message) {
    JsonObject request = message.body();

    String sql =
        String.format(
            "SELECT id, words "
                + "FROM tweets "
                + "WHERE (user_id BETWEEN %s AND %s) AND (created_at BETWEEN %s AND %s)",
            request.getString("uidStart"),
            request.getString("uidEnd"),
            request.getString("timeStart"),
            request.getString("timeEnd"));
    try (Connection conn = ds.getConnection()) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      message.reply(rs);
    } catch (SQLException e) {
      LOGGER.error(e);
    }
  }

  @Override
  void fetchQ3Texts(Message<JsonObject> message) {
    JsonObject request = message.body();
    String sql =
        String.format(
            "SELECT impact_score, id, censored_text "
                + "FROM tweets "
                + "WHERE id IN %s "
                + "ORDER BY impact_score DESC, id DESC LIMIT %s",
            request.getString("ids"), request.getString("n2"));
    try (Connection conn = ds.getConnection()) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      message.reply(rs);
    } catch (SQLException e) {
      LOGGER.error(e);
    }
  }
}

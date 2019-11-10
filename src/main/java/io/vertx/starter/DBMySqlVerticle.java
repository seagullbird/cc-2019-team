package io.vertx.starter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.asyncsql.MySQLClient;
import io.vertx.ext.sql.SQLClient;

public class DBMySqlVerticle extends AbstractVerticle {
  static final String queueName = "mysql.queue";

  public enum ErrorCodes {
    NO_ACTION_SPECIFIED,
    BAD_ACTION,
    DB_ERROR
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(DBMySqlVerticle.class);
  private SQLClient dbClient;

  @Override
  public void start(Promise<Void> promise) throws Exception {
    String host = "localhost";
    if (System.getProperty("os.name").contains("Mac OS")) {
      host = "54.144.244.86";
    }
    dbClient =
        MySQLClient.createShared(
            vertx,
            new JsonObject()
                .put("host", host)
                .put("database", "twitter")
                .put("username", "og")
                .put("password", "123456")
                .put("sslMode", "disable")
                .put("max_pool_size", 4));
    vertx.eventBus().consumer(queueName, this::onMessage);
    promise.complete();
  }

  private void onMessage(Message<JsonObject> message) {
    if (!message.headers().contains("action")) {
      LOGGER.error(
          "No action header specified for message with headers {} and body {}",
          message.headers(),
          message.body().encodePrettily());
      message.fail(ErrorCodes.NO_ACTION_SPECIFIED.ordinal(), "No action header specified");
      return;
    }
    String action = message.headers().get("action");

    switch (action) {
      case "words":
        fetchQ3Words(message);
        break;
      case "texts":
        fetchQ3Texts(message);
        break;
      case "q2":
        fetchQ2(message);
        break;
      default:
        message.fail(ErrorCodes.BAD_ACTION.ordinal(), "Bad action: " + action);
    }
  }

  private void fetchQ2(Message<JsonObject> message) {
    JsonObject request = message.body();
    long reqUser = request.getLong("user");
    String sql =
      String.format(
        "select description, screen_name, hashtags, records from q2_tweets where uid = %d;", reqUser);
    dbClient.query(
      sql,
      fetch -> {
        if (fetch.succeeded()) {
          JsonArray response = new JsonArray(fetch.result().getResults());
          message.reply(response);
        } else {
          reportQueryError(message, fetch.cause());
        }
      });
  }

  private void fetchQ3Words(Message<JsonObject> message) {
    JsonObject request = message.body();
    JsonArray params =
        new JsonArray()
            .add(request.getString("uidStart"))
            .add(request.getString("uidEnd"))
            .add(request.getString("timeStart"))
            .add(request.getString("timeEnd"));

    String sql =
        "SELECT id, words "
            + "FROM tweets "
            + "WHERE (user_id BETWEEN ? AND ?) AND (created_at BETWEEN ? AND ?)";
    dbClient.queryWithParams(
        sql,
        params,
        fetch -> {
          if (fetch.succeeded()) {
            JsonArray response = new JsonArray(fetch.result().getResults());
            message.reply(response);
          } else {
            reportQueryError(message, fetch.cause());
          }
        });
  }

  private void fetchQ3Texts(Message<JsonObject> message) {
    JsonObject request = message.body();
    String sql =
        String.format(
            "SELECT impact_score, id, censored_text "
                + "FROM tweets "
                + "WHERE id IN %s "
                + "ORDER BY impact_score DESC, id DESC LIMIT %s",
            request.getString("ids"), request.getString("n2"));
    dbClient.query(
        sql,
        fetch -> {
          if (fetch.succeeded()) {
            JsonArray response = new JsonArray(fetch.result().getResults());
            message.reply(response);
          } else {
            reportQueryError(message, fetch.cause());
          }
        });
  }

  private void reportQueryError(Message<JsonObject> message, Throwable cause) {
    LOGGER.error("Database query error", cause);
    message.fail(ErrorCodes.DB_ERROR.ordinal(), cause.getMessage());
  }
}

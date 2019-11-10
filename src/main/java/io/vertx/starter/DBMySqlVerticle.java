package io.vertx.starter;

import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.asyncsql.MySQLClient;
import io.vertx.ext.sql.SQLClient;

public class DBMySqlVerticle extends DBVerticle {
  static final String queueName = "mysql.queue";
  private SQLClient dbClient;

  @Override
  public void start(Promise<Void> promise) throws Exception {
    String host = "localhost";
    if (System.getProperty("os.name").contains("Mac OS")) {
      host = "34.201.19.193";
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

  @Override
  void fetchQ2(Message<JsonObject> message) {
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

  @Override
  void fetchQ3Words(Message<JsonObject> message) {
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
}

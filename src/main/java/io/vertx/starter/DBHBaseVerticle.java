package io.vertx.starter;

import ch.qos.logback.classic.db.names.TableName;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

public class DBHBaseVerticle extends DBVerticle {
  static final String queueName = "hbase.queue";
  private static final TableName tableName = TableName.valueOf("twitter");

  @Override
  public void start(Promise<Void> promise) throws Exception {
    promise.complete();
  }

  @Override
  void fetchQ2(Message<JsonObject> message) {
    JsonObject request = message.body();
    ;
  }

  @Override
  void fetchQ3Words(Message<JsonObject> message) {
    JsonObject request = message.body();
  }

  @Override
  void fetchQ3Texts(Message<JsonObject> message) {
    JsonObject request = message.body();
  }
}

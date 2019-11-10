package io.vertx.starter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

abstract class DBVerticle extends AbstractVerticle {
  enum ErrorCodes {
    NO_ACTION_SPECIFIED,
    BAD_ACTION,
    DB_ERROR
  }
  static final Logger LOGGER = LoggerFactory.getLogger(DBMySqlVerticle.class);
  void onMessage(Message<JsonObject> message) {
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

  void reportQueryError(Message<JsonObject> message, Throwable cause) {
    LOGGER.error("Database query error", cause);
    message.fail(DBMySqlVerticle.ErrorCodes.DB_ERROR.ordinal(), cause.getMessage());
  }

  abstract void fetchQ2(Message<JsonObject> message);
  abstract void fetchQ3Texts(Message<JsonObject> message);
  abstract void fetchQ3Words(Message<JsonObject> message);
}

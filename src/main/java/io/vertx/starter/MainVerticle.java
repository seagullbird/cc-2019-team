package io.vertx.starter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.asyncsql.MySQLClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class MainVerticle extends AbstractVerticle {
  private SQLClient dbClient;
  private final int PORT = 80;
  private static final Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class);

  private Future<Void> prepareDatabase() {
    Promise<Void> promise = Promise.promise();
    dbClient =
        MySQLClient.createShared(
            vertx,
            new JsonObject()
                .put("host", "3.83.130.255")
                .put("database", "twitter")
                .put("username", "iniog")
                .put("password", "Iniog_201911")
                .put("sslMode", "disable")
                .put("max_pool_size", 4));
    promise.complete();
    return promise.future();
  }

  private Future<Void> startHttpServer() {
    Promise<Void> promise = Promise.promise();
    HttpServer server = vertx.createHttpServer();

    Router router = Router.router(vertx);
    router.get("/healthcheck").handler(this::indexHandler);
    router.get("/q3*").handler(this::q3Handler);

    server
        .requestHandler(router)
        .listen(
            PORT,
            ar -> {
              if (ar.succeeded()) {
                LOGGER.info("HTTP server running on port " + PORT);
                promise.complete();
              } else {
                LOGGER.error("Could not start a HTTP server", ar.cause());
                promise.fail(ar.cause());
              }
            });

    return promise.future();
  }

  private void indexHandler(RoutingContext routingContext) {
    routingContext
      .response()
      .setStatusCode(200)
      .putHeader(HttpHeaders.CONTENT_TYPE, "text/plain")
      .end("healthy");
  }

  private void q3Handler(RoutingContext routingContext) {
    HttpServerRequest request = routingContext.request();
    String uidStart = request.getParam("uid_start");
    String uidEnd = request.getParam("uid_end");
    String timeStart = request.getParam("time_start");
    String timeEnd = request.getParam("time_end");
    String n1 = request.getParam("n1");
    String n2 = request.getParam("n2");

    String msg = String.join("\t", uidStart, uidEnd, timeStart, timeEnd, n1, n2);
    routingContext
      .response()
      .setStatusCode(200)
      .putHeader(HttpHeaders.CONTENT_TYPE, "text/plain")
      .end(msg);
  }

  @Override
  public void start(Promise<Void> promise) {
    prepareDatabase()
        .compose(v -> startHttpServer())
        .setHandler(
            // AsyncResult<Void>
            ar -> {
              if (ar.succeeded()) {
                promise.complete();
              } else {
                promise.fail(ar.cause());
              }
            });
  }
}

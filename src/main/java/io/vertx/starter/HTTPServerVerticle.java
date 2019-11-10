package io.vertx.starter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class HTTPServerVerticle extends AbstractVerticle {
  static final String header = "INI OG,163064922485";
  private final int PORT = 80;
  private static final Logger LOGGER = LoggerFactory.getLogger(DBMySqlVerticle.class);


  @Override
  public void start(Promise<Void> promise) throws Exception {
    HttpServer server = vertx.createHttpServer();

    Router router = Router.router(vertx);
    router.get("/healthcheck").handler(context -> respondMsg(context, "healthy"));
    router.get("/q1*").handler(HTTPServerVerticle::q1handler);
    router.get("/q2*").handler(context -> Query2.run(vertx, context));
    router.get("/q3*").handler(context -> Query3.run(vertx, context));

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
  }

  static void q1handler(RoutingContext context) {
    String input = context.request().getParam("cc");
    OGCoin ogCoin = new OGCoin(input);
    String res = ogCoin.run();
    respondMsg(context, res);
  }

  static void respondMsg(RoutingContext context, String msg) {
    context
      .response()
      .setStatusCode(200)
      .putHeader(HttpHeaders.CONTENT_TYPE, "text/plain")
      .end(msg);
  }
}

package io.vertx.starter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;

public class MainVerticle extends AbstractVerticle {
  static final String dbQueue = DBMySqlVerticle.queueName;
  @Override
  public void start(Promise<Void> promise) {

    Promise<String> dbVerticleDeployment = Promise.promise();
    vertx.deployVerticle(
        new DBMySqlVerticle(), dbVerticleDeployment);

    dbVerticleDeployment
        .future()
        .compose(
            id -> {
              Promise<String> httpVerticleDeployment = Promise.promise();
              vertx.deployVerticle(
                  "io.vertx.starter.HTTPServerVerticle",
                  new DeploymentOptions().setInstances(2),
                  httpVerticleDeployment);

              return httpVerticleDeployment.future();
            })
        .setHandler(
            ar -> {
              if (ar.succeeded()) {
                promise.complete();
              } else {
                promise.fail(ar.cause());
              }
            });
  }
}

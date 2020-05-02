package io.vertx.guides.wiki;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.guides.wiki.db.WikiDatabaseVerticle;
import io.vertx.guides.wiki.http.HttpServerVerticle;

public class MainVerticle extends AbstractVerticle {

    @Override
    public void start(Promise<Void> promise) {
        Promise<String> dbVerticleDeployment = Promise.promise();
        vertx.deployVerticle(new WikiDatabaseVerticle(), dbVerticleDeployment);

        dbVerticleDeployment.future()
            .compose(id -> {
                Promise<String> httpVerticleDeployment = Promise.promise();
                vertx.deployVerticle(HttpServerVerticle.class,
                    new DeploymentOptions().setInstances(2),
                    httpVerticleDeployment);

                return httpVerticleDeployment.future();
            })
            .setHandler(ar -> {
                if (ar.succeeded()) {
                    promise.complete();
                } else {
                    promise.fail(ar.cause());
                }
            });
    }
}

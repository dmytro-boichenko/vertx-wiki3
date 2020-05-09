package io.vertx.guides.wiki;

import io.reactivex.disposables.Disposable;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.guides.wiki.database.WikiDatabaseVerticle;
import io.vertx.guides.wiki.http.HttpServerVerticle;
import io.vertx.reactivex.config.ConfigRetriever;
import io.vertx.reactivex.core.AbstractVerticle;

public class MainVerticle extends AbstractVerticle {

    private Disposable init;

    @Override
    public void start(Promise<Void> promise) {
        ConfigRetriever retriever = ConfigRetriever.create(vertx);
        retriever.getConfig(c -> {
            JsonObject config = c.result();
            JsonObject dbConfig = config.getJsonObject("wikidb");
            JsonObject httpConfig = config.getJsonObject("http");

            init = vertx
                .rxDeployVerticle(new WikiDatabaseVerticle(),
                    new DeploymentOptions()
                        .setConfig(dbConfig))
                .flatMap(id -> vertx.rxDeployVerticle(HttpServerVerticle.class.getCanonicalName(),
                    new DeploymentOptions()
                        .setInstances(2)
                        .setConfig(httpConfig)))
                .subscribe(id -> promise.complete(), promise::fail);
        });


    }

    @Override
    public void stop(Promise<Void> promise) {
        init.dispose();
    }
}

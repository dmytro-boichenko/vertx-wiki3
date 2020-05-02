package io.vertx.guides.wiki.db;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class WikiDatabaseServiceImpl implements WikiDatabaseService {

    private static final Logger LOGGER = LoggerFactory.getLogger(WikiDatabaseServiceImpl.class);

    private final Map<SqlQuery, String> sqlQueries;
    private final JDBCClient dbClient;

    public WikiDatabaseServiceImpl(JDBCClient dbClient,
                                   Map<SqlQuery, String> sqlQueries,
                                   Handler<AsyncResult<WikiDatabaseService>> readyHandler) {
        this.sqlQueries = sqlQueries;
        this.dbClient = dbClient;

        dbClient.getConnection(ar -> {
            if (ar.failed()) {
                LOGGER.error("Could not open database connection", ar.cause());
                readyHandler.handle(Future.failedFuture(ar.cause()));
            } else {
                SQLConnection connection = ar.result();
                connection.execute(sqlQueries.get(SqlQuery.CREATE_PAGES_TABLE), create -> {
                    connection.close();
                    if (create.failed()) {
                        LOGGER.error("Database preparation error", create.cause());
                        readyHandler.handle(Future.failedFuture(create.cause()));
                    } else {
                        LOGGER.info("Database successfully prepared");
                        readyHandler.handle(Future.succeededFuture(this));
                    }
                });
            }
        });
    }

    @Override
    public WikiDatabaseService fetchAllPages(Handler<AsyncResult<JsonArray>> resultHandler) {
        dbClient.query(sqlQueries.get(SqlQuery.ALL_PAGES), res -> {
            if (res.succeeded()) {
                JsonArray pages = new JsonArray(res.result()
                    .getResults()
                    .stream()
                    .map(json -> json.getString(0))
                    .sorted()
                    .collect(Collectors.toList()));
                resultHandler.handle(Future.succeededFuture(pages));
            } else {
                LOGGER.error("Database query error", res.cause());
                resultHandler.handle(Future.failedFuture(res.cause()));
            }
        });
        return this;
    }

    @Override
    public WikiDatabaseService fetchAllPagesData(Handler<AsyncResult<List<JsonObject>>> resultHandler) {
        dbClient.query(sqlQueries.get(SqlQuery.ALL_PAGES_DATA), queryResult -> {
            if (queryResult.succeeded()) {
                resultHandler.handle(Future.succeededFuture(queryResult.result().getRows()));
            } else {
                LOGGER.error("Database query error", queryResult.cause());
                resultHandler.handle(Future.failedFuture(queryResult.cause()));
            }
        });
        return this;
    }

    @Override
    public WikiDatabaseService fetchPage(String name, Handler<AsyncResult<JsonObject>> resultHandler) {
        JsonArray params = new JsonArray().add(name);

        dbClient.queryWithParams(sqlQueries.get(SqlQuery.GET_PAGE), params, fetch -> {
            if (fetch.succeeded()) {
                JsonObject response = new JsonObject();
                ResultSet resultSet = fetch.result();
                if (resultSet.getNumRows() == 0) {
                    response.put("found", false);
                } else {
                    response.put("found", true);
                    JsonArray row = resultSet.getResults().get(0);
                    response.put("id", row.getInteger(0));
                    response.put("rawContent", row.getString(1));
                }
                resultHandler.handle(Future.succeededFuture(response));
            } else {
                LOGGER.error("Database query error", fetch.cause());
                resultHandler.handle(Future.failedFuture(fetch.cause()));
            }
        });
        return this;
    }

    @Override
    public WikiDatabaseService fetchPageById(int id, Handler<AsyncResult<JsonObject>> resultHandler) {
        JsonArray params = new JsonArray().add(id);

        dbClient.querySingleWithParams(sqlQueries.get(SqlQuery.GET_PAGE_BY_ID), params, fetch -> {
            if (fetch.succeeded()) {
                JsonObject response = new JsonObject();
                JsonArray row = fetch.result();
                if (row == null) {
                    response.put("found", false);
                } else {
                    response.put("found", true);
                    response.put("id", id);
                    response.put("name", row.getString(0));
                    response.put("content", row.getString(1));
                }
                resultHandler.handle(Future.succeededFuture(response));
            } else {
                LOGGER.error("Database query error", fetch.cause());
                resultHandler.handle(Future.failedFuture(fetch.cause()));
            }
        });
        return this;
    }

    @Override
    public WikiDatabaseService createPage(String title, String markdown, Handler<AsyncResult<Void>> resultHandler) {
        JsonArray data = new JsonArray()
            .add(title)
            .add(markdown);

        dbClient.updateWithParams(sqlQueries.get(SqlQuery.CREATE_PAGE), data, res -> {
            if (res.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                LOGGER.error("Database create page error", res.cause());
                resultHandler.handle(Future.failedFuture(res.cause()));
            }
        });
        return this;
    }

    @Override
    public WikiDatabaseService savePage(int id, String markdown, Handler<AsyncResult<Void>> resultHandler) {
        JsonArray data = new JsonArray()
            .add(markdown)
            .add(id);

        dbClient.updateWithParams(sqlQueries.get(SqlQuery.SAVE_PAGE), data, res -> {
            if (res.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                LOGGER.error("Database saving page error", res.cause());
                resultHandler.handle(Future.failedFuture(res.cause()));
            }
        });
        return this;
    }

    @Override
    public WikiDatabaseService deletePage(int id, Handler<AsyncResult<Void>> resultHandler) {
        JsonArray data = new JsonArray().add(id);

        dbClient.updateWithParams(sqlQueries.get(SqlQuery.DELETE_PAGE), data, res -> {
            if (res.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                LOGGER.error("Database deletion page error", res.cause());
                resultHandler.handle(Future.failedFuture(res.cause()));
            }
        });
        return this;
    }
}

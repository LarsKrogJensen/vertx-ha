package se.lars;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApiVerticle extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(ApiVerticle.class);
    private final int httpPort;

    public ApiVerticle(int httpPort) {
        this.httpPort = httpPort;
    }

    @Override
    public void start(Promise<Void> startPromise) {
        vertx.createHttpServer()
                .requestHandler(router())
                .listen(httpPort, ar -> {
                    if (ar.succeeded()) {
                        log.info("HttpServer listening at port {}", ar.result().actualPort());
                        startPromise.complete();
                    } else {
                        log.error("Failed to start http server on port {}", httpPort, ar.cause());
                        startPromise.fail(ar.cause());
                    }
                });
    }

    private Router router() {
        Router router = Router.router(vertx);
        router.route("/login/:id").handler(this::login);
        router.route("/logout/:id").handler(this::logout);

        return router;
    }

    private void login(RoutingContext rc) {
        vertx.eventBus().send("login", Integer.parseInt(rc.request().getParam("id")));
        rc.response().end("OK");
    }

    private void logout(RoutingContext rc) {
        vertx.eventBus().send("logout", Integer.parseInt(rc.request().getParam("id")));
        rc.response().end("OK");

    }
}

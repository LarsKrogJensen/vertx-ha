package se.lars;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.lang.Integer.parseInt;

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
        router.route("/login/:ids").handler(this::login);
        router.route("/logout/:ids").handler(this::logout);
        router.errorHandler(500, (rc) -> {
            log.error("Opps", rc.failure());
        });

        return router;
    }

    private void login(RoutingContext rc) {
        idStream(rc.request().getParam("ids")).forEach(id -> {
            vertx.eventBus().send("login", id);
        });
        rc.response().end("OK");
    }

    private void logout(RoutingContext rc) {
        idStream(rc.request().getParam("ids")).forEach(id -> {
            vertx.eventBus().send("logout", id);
        });
        rc.response().end("OK");
    }

    private Stream<Integer> idStream(String ids) {
        String[] range = ids.split("-");
        if (range.length == 1) {
            return Stream.of(parseInt(range[0]));
        } else {
            return IntStream.range(parseInt(range[0]), parseInt(range[1])).boxed();
        }

    }
}

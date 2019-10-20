package se.lars;

import com.hazelcast.core.HazelcastInstance;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static se.lars.VertxUtil.deployVerticle;

public class Application {
    private static final Logger log = LoggerFactory.getLogger(Application.class);
    private final AppConfig config;
    private Vertx vertx;

    public Application(AppConfig config) {
        this.config = config;
    }

    public void start() {
        startVertx()
                .compose(hazelcast -> deployVerticle(vertx, new PunterManagerActor(hazelcast)))
                .compose(__ -> deployVerticle(vertx, new ApiVerticle(config.httpPort)))
                .setHandler(ar -> {
                    if (ar.succeeded()) {
                        log.info("App started successfully");
                    } else {
                        log.error("Failed to startup application", ar.cause());
                        stop();
                    }
                });
    }

    public void stop() {
        log.info("Shuuting down");
        if (vertx != null) {
            vertx.close(ar -> log.info("Vertx shutdown completed"));
        }
    }

    private Future<HazelcastInstance> startVertx() {
        return Future.future(promise -> {
            HazelcastClusterManager clusterManager = new HazelcastClusterManager();
            VertxOptions vertxOptions = new VertxOptions().setClusterManager(clusterManager);
            Vertx.clusteredVertx(vertxOptions, ar -> {
                if (ar.succeeded()) {
                    vertx = ar.result();
                    EventBusCodec.registerEventBusMessages(vertx.eventBus(), "se.lars.events");
                    promise.complete(clusterManager.getHazelcastInstance());
                } else {
                    promise.fail(ar.cause());
                }
            });
        });
    }
}

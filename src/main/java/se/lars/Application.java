package se.lars;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import io.reactivex.Single;
import io.vertx.core.VertxOptions;
import io.vertx.reactivex.core.Vertx;
import io.vertx.spi.cluster.hazelcast.ConfigUtil;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static se.lars.EventBusCodec.registerEventBusMessages;

public class Application {
  private static final Logger log = LoggerFactory.getLogger(Application.class);
  private final AppConfig config;
  private Vertx vertx;

  public Application(AppConfig config) {
    this.config = config;
  }

  public void start() {
    startVertx()
      .flatMap(hazelcast -> vertx.rxDeployVerticle(new PunterManagerActor(hazelcast)))
      .flatMap(__ -> vertx.rxDeployVerticle(new ApiVerticle(config.httpPort)))
      .doOnSuccess(__ -> log.info("App started successfully"))
      .doOnError(e -> log.error("Failed to startup application", e))
      .doOnError(__ -> stop())
      .subscribe();

    Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
  }

  public void stop() {
    log.info("Shutting down");
    if (vertx != null) {
      vertx.rxClose()
        .doOnComplete(() -> log.info("Vertx shutdown completed"))
        .subscribe();
    }
  }

  private Single<HazelcastInstance> startVertx() {
    Config hazelcastConfig = ConfigUtil.loadConfig();
    hazelcastConfig.setClusterName("mob-cluster");
    hazelcastConfig.getNetworkConfig().getJoin().getMulticastConfig().setMulticastGroup("224.0.0.1").setEnabled(true);

    HazelcastClusterManager clusterManager = new HazelcastClusterManager(hazelcastConfig);
    VertxOptions vertxOptions = new VertxOptions()
//      .setMetricsOptions(new DropwizardMetricsOptions().setEnabled(true))
      .setClusterManager(clusterManager);

    return Vertx.rxClusteredVertx(vertxOptions)
      .doOnSuccess(vertx -> this.vertx = vertx)
      .doOnSuccess(vertx -> registerEventBusMessages(vertx.eventBus(), "se.lars.events"))
      .map(__ -> clusterManager.getHazelcastInstance());
  }
}

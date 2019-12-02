package se.lars;

import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
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
    Config config = ConfigUtil.loadConfig();
    config.setGroupConfig(new GroupConfig().setName("mob-cluster"));
    MapConfig punterConfig = new MapConfig("punters");
    punterConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
    config.addMapConfig(punterConfig);
    HazelcastClusterManager clusterManager = new HazelcastClusterManager(config);
    VertxOptions vertxOptions = new VertxOptions()
//      .setMetricsOptions(new DropwizardMetricsOptions().setEnabled(true).setJmxEnabled(true))
      .setClusterManager(clusterManager);

    return Vertx.rxClusteredVertx(vertxOptions)
      .doOnSuccess(vertx -> this.vertx = vertx)
      .doOnSuccess(vertx -> registerEventBusMessages(vertx.eventBus(), "se.lars.events"))
      .map(__ -> clusterManager.getHazelcastInstance());
  }
}

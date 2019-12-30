package se.lars;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.dropwizard.DropwizardMetricsOptions;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.IntStream;

import static java.lang.System.setProperty;

public class DummyCluster extends AbstractVerticle {
  private static final Logger log = LoggerFactory.getLogger(DummyCluster.class);
  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    vertx.eventBus().consumer("dummy", this::handleMessage);
    IntStream.range(0, 100).forEach(i -> vertx.eventBus().send("dummy", "dummy-msg-" +i));
    vertx.setTimer(2_000, __ -> startPromise.complete());
  }

  private void handleMessage(Message<String> msg) {
    log.info("message received: {}",msg.body());
  }


  public static void main(String[] args) {
    setProperty(
      "vertx.logger-delegate-factory-class-name",
      "io.vertx.core.logging.SLF4JLogDelegateFactory"
    );
    setProperty("hazelcast.logging.type", "slf4j");

    ClusterManager mgr = new HazelcastClusterManager();
    VertxOptions options = new VertxOptions()
      .setClusterManager(mgr)
      .setMetricsOptions(new DropwizardMetricsOptions().setEnabled(true).setJmxEnabled(true));
    Vertx.clusteredVertx(options, ar -> {
      ar.result().deployVerticle(new DummyCluster(), ar2 -> {
        log.info("dummy started");
      });

    });
  }
}

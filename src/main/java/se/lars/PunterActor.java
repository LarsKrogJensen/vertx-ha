package se.lars;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.lars.events.PunterActorEvent;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.Duration.ofSeconds;
import static se.lars.events.PunterActorEvent.PunterActorStatus.COMPLETED;
import static se.lars.events.PunterActorEvent.PunterActorStatus.STARTED;

public class PunterActor extends AbstractVerticle {
    private static AtomicInteger chaos = new AtomicInteger(1);

    private static final Logger log = LoggerFactory.getLogger(PunterActor.class);
    private final int punterId;
    private final String managerId;

    public PunterActor(int punterId, String managerId) {
        this.punterId = punterId;
        this.managerId = managerId;
    }

    @Override
    public void start(Promise<Void> startPromise) {
        if (punterId == 101) {
            // simulate delay
            vertx.setTimer(ofSeconds(101).toMillis(), __ -> {
                log.info("PunterActor started, id: {}", punterId);
                vertx.eventBus().send(managerId, new PunterActorEvent(deploymentID(), managerId, punterId, STARTED));
                startPromise.complete();
            });
        }
        else if (chaos.incrementAndGet() % 5 == 0) {
            log.error("PunterActor failed, id: {}", punterId);
            startPromise.fail("Oh noes");
        } else {
            log.info("PunterActor started, id: {}", punterId);
            vertx.eventBus().send(managerId, new PunterActorEvent(deploymentID(), managerId, punterId, STARTED));
            startPromise.complete();
        }
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        log.info("PunterActor stopped, id: {}", punterId);
        vertx.eventBus().send(managerId, new PunterActorEvent(deploymentID(), managerId, punterId, COMPLETED));
        stopPromise.complete();
    }
}

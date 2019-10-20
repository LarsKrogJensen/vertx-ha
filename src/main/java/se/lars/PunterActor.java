package se.lars;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.lars.events.PunterActorEvent;

import static se.lars.events.PunterActorEvent.PunterActorStatus.COMPLETED;
import static se.lars.events.PunterActorEvent.PunterActorStatus.STARTED;

public class PunterActor extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(PunterActor.class);
    private final int punterId;
    private final String managerId;

    public PunterActor(int punterId, String managerId) {
        this.punterId = punterId;
        this.managerId = managerId;
    }

    @Override
    public void start(Promise<Void> startPromise) {
        log.info("PunterActor started, id: {}", punterId);
        vertx.eventBus().send(managerId, new PunterActorEvent(deploymentID(), managerId, punterId, STARTED));
        startPromise.complete();
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        log.info("PunterActor stopped, id: {}", punterId);
        vertx.eventBus().send(managerId, new PunterActorEvent(deploymentID(), managerId, punterId, COMPLETED));
        stopPromise.complete();
    }
}

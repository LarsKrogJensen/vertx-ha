package se.lars;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.lars.events.PunterActorEvent;

import java.util.HashSet;
import java.util.Set;

import static io.vertx.reactivex.RxHelper.scheduler;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static se.lars.VertxUtil.deployVerticleRx;
import static se.lars.VertxUtil.undeployVerticleRx;
import static se.lars.events.PunterActorEvent.PunterActorStatus.COMPLETED;
import static se.lars.events.PunterActorEvent.PunterActorStatus.STARTED;

public class PunterManagerActor extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(PunterManagerActor.class);
    private final HazelcastInstance hazelcast;

    // hazelcast distributed map with all punters
    private final IMap<Integer, Integer> distributedPunters;

    // book keeping of punter actors managed,  deploymentId -> punterId
    private final BiMap<Integer, String> managedPunters = HashBiMap.create();

    // used to prevent concurrent local deployments of punters
    private final Set<Integer> pendingDeployments = new HashSet<>();

    public PunterManagerActor(HazelcastInstance hazelcast) {
        this.distributedPunters = hazelcast.getMap("punters");
        this.hazelcast = hazelcast;
    }

    @Override
    public void start(Promise<Void> startPromise) {
        log.info("Punter Manager starting...");
        // listen for changes to locally owned punters.Locallity is very important here
        // all actions taken (start/stop is a reaction to changes to the distributed map)
        Context context = vertx.getOrCreateContext();
        distributedPunters.addLocalEntryListener((EntryAddedListener<Integer, Integer>) event -> {
            context.runOnContext((__) -> startPunterActor(event.getKey()).subscribe());
        });
        distributedPunters.addLocalEntryListener((EntryRemovedListener<Integer, Integer>) event -> {
            context.runOnContext((__) -> stopPunterActor(event.getKey()).subscribe());
        });

        // handle migration that happens when nodes comes and goes
        HazelcastMigrationAdapter migrationListener = new HazelcastMigrationAdapter(hazelcast);
        migrationListener.observable()
                .filter(me -> me.getStatus() == MigrationEvent.MigrationStatus.COMPLETED)
                .debounce(1, SECONDS, scheduler(context))
                .doOnNext(__ -> log.info("Migrating nodes"))
                .flatMapCompletable(__ -> syncPunters(distributedPunters.localKeySet()))
                .subscribe();

        // login/logout is updating the backing distributed map and let hazelcast decide which node
        // will own the punter
        vertx.eventBus().consumer("login", this::handleLogin);
        vertx.eventBus().consumer("logout", this::handleLogout);
        vertx.eventBus().localConsumer(deploymentID(), this::handlePunterActorEvent);

        // consistency supervisor
        vertx.setPeriodic(ofSeconds(5).toMillis(), (__) -> ensureConsistency());

        // initialize by asking for locally owned punters
        syncPunters(distributedPunters.localKeySet())
                .doOnComplete(startPromise::complete)
                .doOnError(startPromise::fail)
                .subscribe();
    }

    private Completable syncPunters(Set<Integer> wantedPunters) {
        var added = Observable.fromIterable(wantedPunters)
                .filter(id -> !managedPunters.containsKey(id))
                .flatMapCompletable(this::startPunterActor);
        var removed = Observable.fromIterable(managedPunters.keySet())
                .filter(id -> !wantedPunters.contains(id))
                .flatMapCompletable(this::stopPunterActor);

        return Completable.concatArray(added, removed);
    }

    private Completable startPunterActor(int punterId) {
        if (managedPunters.containsKey(punterId)) {
            log.info("Punter {} already logged in on node", punterId);
            return Completable.complete();
        }
        if (pendingDeployments.contains(punterId)) {
            log.info("Punter {} already being logged in on node", punterId);
            return Completable.complete();
        }

        pendingDeployments.add(punterId);

        // deploy and ignore errors, cause consistency check will repair
        return deployVerticleRx(vertx, new PunterActor(punterId, deploymentID()))
                .ignoreElement()
                .doOnError(e -> log.warn("Failed to deploy punter {} actor, caused by {}", punterId, e.getMessage()))
                .onErrorComplete()
                .doOnComplete(() -> pendingDeployments.remove(punterId));
    }

    private Completable stopPunterActor(int punterId) {
        // undeploy and ignore errors, cause consistency check will repair
        if (!managedPunters.containsKey(punterId)) {
            log.info("Trying to stopping punter {} actor that is not deployed", punterId);
            return Completable.complete();
        }
        return undeployVerticleRx(vertx, managedPunters.get(punterId))
                .doOnError(e -> log.warn("Failed to undeploy punter {} caused by: {}", punterId, e.getMessage()))
                .onErrorComplete();
    }

    private void handleLogin(Message<Integer> msg) {
        distributedPunters.put(msg.body(), msg.body());
    }

    private void handleLogout(Message<Integer> msg) {
        distributedPunters.remove(msg.body(), msg.body());
    }

    private void handlePunterActorEvent(Message<PunterActorEvent> message) {
        PunterActorEvent event = message.body();
        if (event.status == STARTED) {
            this.managedPunters.put(event.punterId, event.deploymentId);
        } else if (event.status == COMPLETED) {
            this.managedPunters.inverse().remove(event.deploymentId);
        }
    }

    private void ensureConsistency() {
        log.info("Managed punters {}, locally owned punters {}, pending: {}", managedPunters.size(), distributedPunters.localKeySet().size(), pendingDeployments.size());
        if (!managedPunters.keySet().equals(distributedPunters.localKeySet())) {
            log.warn("Managed punters not in sync to locally owned ones, synchronizing");
            syncPunters(distributedPunters.localKeySet()).subscribe();
        }
    }
}

package se.lars;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.subjects.PublishSubject;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.lars.events.PunterActorEvent;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static io.vertx.reactivex.RxHelper.scheduler;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static se.lars.events.PunterActorEvent.PunterActorStatus.COMPLETED;
import static se.lars.events.PunterActorEvent.PunterActorStatus.STARTED;

public class PunterManagerActor extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(PunterManagerActor.class);
    private final HazelcastInstance hazelcast;

    // hazelcast distributed map with all punters
    private final IMap<Integer, Integer> distributedPunters;

    // book keeping of punter actors managed deploymentId -> punterId
    private final Map<String, Integer> punterActors = new ConcurrentHashMap<>(1000, 0.75f, 1);
    // reverse book keeping map punterId -> deploymentId
    private final Map<Integer, String> managedPunters = new ConcurrentHashMap<>(1000, 0.75f, 1);
    
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
        MigrationListenerAdapter migrationListener = new MigrationListenerAdapter();
        hazelcast.getPartitionService().addMigrationListener(migrationListener);
        migrationListener.observable()
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

    @Override
    public void stop(Promise<Void> stopPromise) {
        stopPromise.complete();
    }

    private Completable syncPunters(Set<Integer> wantedPunters) {
        var added = Observable.fromIterable(wantedPunters)
                .filter(id -> !managedPunters.containsKey(id))
//                .doOnNext(id -> log.info("Trying to start punter actor {}", id))
                .flatMapCompletable(this::startPunterActor);
        var removed = Observable.fromIterable(managedPunters.keySet())
                .filter(id -> !wantedPunters.contains(id))
//                .doOnNext(id -> log.info("Trying to stop punter actor {}", id))
                .flatMapCompletable(this::stopPunterActor);

        return Completable.concatArray(added, removed);
    }

    private Completable startPunterActor(int punterId) {
        if (managedPunters.containsKey(punterId)) {
            log.info("Punter {} already logged in on node", punterId);
            return Completable.complete();
        }

        return VertxUtil.deployVerticleRx(vertx, new PunterActor(punterId, deploymentID()))
                .doOnSuccess(deploymentId -> managedPunters.put(punterId, deploymentId))
                .ignoreElement();
    }

    private void handleLogin(Message<Integer> msg) {
        log.info("Punter login {}", msg.body());
        distributedPunters.put(msg.body(), msg.body());
    }

    private void handleLogout(Message<Integer> msg) {
        log.info("Punter logout {}", msg.body());
        distributedPunters.remove(msg.body(), msg.body());
    }

    private void handlePunterActorEvent(Message<PunterActorEvent> message) {
        PunterActorEvent event = message.body();
        if (event.status == STARTED) {
            this.punterActors.put(event.deploymentId, event.punterId);
        } else if (event.status == COMPLETED) {
            this.punterActors.remove(event.deploymentId);
        }
    }

    private void ensureConsistency() {
//        log.info("Managed punters {}, locally own punters {}, actors: {}", managedPunters.size(), distributedPunters.localKeySet().size(), punterActors.size());
//        if (!managedPunters.keySet().equals(distributedPunters.localKeySet())) {
//            log.warn("Managed punters not in sync to locally owned ones");
//        } else if (punterActors.size() != managedPunters.size()) {
//            log.warn("Punter actors not in sync");
//        }
    }

    private Completable stopPunterActor(int punterId) {
        return VertxUtil.undeployVerticleRx(vertx, managedPunters.get(punterId))
                .doOnComplete(() -> managedPunters.remove(punterId));
    }

    private static class MigrationListenerAdapter implements MigrationListener {
        private final PublishSubject<MigrationEvent> subject = PublishSubject.create();

        @Override
        public void migrationStarted(MigrationEvent migrationEvent) {
        }

        @Override
        public void migrationCompleted(MigrationEvent migrationEvent) {
            // when migration completed (called once for each partition) makes sure add and remove punters
            // to reflect changes
            subject.onNext(migrationEvent);
        }

        @Override
        public void migrationFailed(MigrationEvent migrationEvent) {

        }

        public Observable<MigrationEvent> observable() {
            return subject;
        }
    }
}

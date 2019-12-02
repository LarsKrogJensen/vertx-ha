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
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.Context;
import io.vertx.reactivex.core.eventbus.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.lars.events.PunterActorEvent;
import se.lars.events.PunterLoginEvent;
import se.lars.events.PunterLogoutEvent;
import se.lars.events.PunterRequest;

import java.util.HashSet;
import java.util.OptionalLong;
import java.util.Set;

import static io.vertx.reactivex.core.RxHelper.scheduler;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static se.lars.events.PunterActorEvent.PunterActorStatus.*;

public class PunterManagerActor extends AbstractVerticle {
  private static final Logger log = LoggerFactory.getLogger(PunterManagerActor.class);
  private final HazelcastInstance hazelcast;

  // hazelcast distributed map with all punters
  private final IMap<Integer, Integer> distributedPunters;

  // book keeping of punter actors managed,  deploymentId -> punterId
  private final BiMap<Integer, String> managedPunters = HashBiMap.create();

  // used to prevent concurrent local deployments of punters
  private final Set<Integer> pendingDeployments = new HashSet<>();
  private OptionalLong supervisorTimer = OptionalLong.empty();

  public PunterManagerActor(HazelcastInstance hazelcast) {
    this.distributedPunters = hazelcast.getMap("punters");
    this.hazelcast = hazelcast;
  }

  @Override
  public Completable rxStart() {
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
    migrationListener.asObservable()
      .filter(me -> me.getStatus() == MigrationEvent.MigrationStatus.COMPLETED)
      .debounce(1, SECONDS, scheduler(context))
      .doOnNext(__ -> log.info("Migrating nodes"))
      .doOnNext(__ -> stopSupervisorTimer())
      .flatMapCompletable(__ -> syncPunters())
      .doOnComplete(this::startSupervisorTimer)
      .subscribe();

    vertx.eventBus().consumer(PunterLoginEvent.class.getName(), this::handleLogin);
    vertx.eventBus().consumer(PunterLogoutEvent.class.getName(), this::handleLogout);
    vertx.eventBus().localConsumer(PunterRequest.class.getName(), this::routePunterRequest);
    vertx.eventBus().<PunterActorEvent>consumer(PunterActorEvent.class.getName()).toObservable()
      .map(Message::body)
      .filter(evt -> evt.managerId.equals(deploymentID()))
      .subscribe(this::handlePunterActorEvent);

    // initialize by asking for locally owned punters
    return syncPunters().doOnComplete(this::startSupervisorTimer);
//    startSupervisorTimer();
//    return Completable.complete();
  }

  private void startSupervisorTimer() {
    supervisorTimer = OptionalLong.of(vertx.setTimer(ofSeconds(10).toMillis(), (__) -> ensureConsistency()));
  }

  private void stopSupervisorTimer() {
    supervisorTimer.ifPresent(timerId -> {
      vertx.cancelTimer(timerId);
      supervisorTimer = OptionalLong.empty();
    });
  }

  private Completable syncPunters() {
    var wantedPunters = Set.copyOf(distributedPunters.localKeySet());
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
    return vertx.rxDeployVerticle(new PunterActor(punterId, deploymentID()))
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
    return vertx.rxUndeploy(managedPunters.get(punterId))
      .doOnError(e -> log.warn("Failed to undeploy punter {} caused by: {}", punterId, e.getMessage()))
      .onErrorComplete();
  }

  private void handleLogin(Message<PunterLoginEvent> msg) {
    distributedPunters.put(msg.body().punterId, msg.body().punterId);
  }

  private void handleLogout(Message<PunterLogoutEvent> msg) {
    distributedPunters.remove(msg.body().punterId, msg.body().punterId);
  }

  private void routePunterRequest(Message<PunterRequest> msg) {
    // routing request
    if (distributedPunters.containsKey(msg.body().punterId)) {
      forwardMessage(msg);
    } else {
      vertx.eventBus().<PunterActorEvent>consumer(PunterActorEvent.class.getName())
        .toObservable()
        .map(Message::body)
        .filter(evt -> evt.punterId == msg.body().punterId)
        .firstOrError()
        .subscribe(evt -> forwardMessage(msg), ex -> msg.fail(0, ex.getMessage()));

      distributedPunters.put(msg.body().punterId, msg.body().punterId);
    }
  }

  private void forwardMessage(Message<PunterRequest> msg) {
    vertx.eventBus().rxRequest(msg.body().topic(), msg.body())
      .map(Message::body)
      .subscribe(msg::reply, msg::reply);
  }

  private void handlePunterActorEvent(PunterActorEvent event) {
    if (event.status == STARTED) {
      this.managedPunters.put(event.punterId, event.deploymentId);
    } else if (event.status == COMPLETED || event.status == FAILED) {
      this.managedPunters.inverse().remove(event.deploymentId);
    }
  }

  private void ensureConsistency() {
    Set<Integer> wantedPunters = Set.copyOf(distributedPunters.localKeySet());
    log.info("Managed punters {}, locally owned punters {}, pending: {}", managedPunters.size(), wantedPunters.size(), pendingDeployments.size());
    if (!managedPunters.keySet().equals(wantedPunters) && pendingDeployments.isEmpty()) {
      log.warn("Managed punters not in sync to locally owned ones, synchronizing");
      syncPunters()
        .doOnComplete(this::startSupervisorTimer)
        .doOnError(e -> log.info("Punter synchronization failed, cause: {}", e.getMessage()))
        .onErrorComplete()
        .subscribe();
    } else {
      startSupervisorTimer();
    }
  }
}

package se.lars;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.MigrationState;
import com.hazelcast.partition.ReplicaMigrationEvent;
import io.reactivex.Observable;
import io.reactivex.subjects.PublishSubject;

public class HazelcastMigrationAdapter implements MigrationListener {
  private final PublishSubject<MigrationState> subject = PublishSubject.create();

  public HazelcastMigrationAdapter(HazelcastInstance hazelcast) {
    hazelcast.getPartitionService().addMigrationListener(this);
  }

  @Override
  public void migrationStarted(MigrationState migrationEvent) {
//    subject.onNext(migrationEvent);
  }

  @Override
  public void migrationFinished(MigrationState state) {
    subject.onNext(state);
  }

  @Override
  public void replicaMigrationCompleted(ReplicaMigrationEvent event) {
  }

  @Override
  public void replicaMigrationFailed(ReplicaMigrationEvent event) {
  }

  public Observable<MigrationState> asObservable() {
    return subject;
  }
}

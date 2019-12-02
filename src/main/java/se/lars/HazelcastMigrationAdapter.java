package se.lars;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationListener;
import io.reactivex.Observable;
import io.reactivex.subjects.PublishSubject;

public class HazelcastMigrationAdapter implements MigrationListener {
    private final PublishSubject<MigrationEvent> subject = PublishSubject.create();

    public HazelcastMigrationAdapter(HazelcastInstance hazelcast) {
        hazelcast.getPartitionService().addMigrationListener(this);
    }

    @Override
    public void migrationStarted(MigrationEvent migrationEvent) {
        subject.onNext(migrationEvent);
    }

    @Override
    public void migrationCompleted(MigrationEvent migrationEvent) {
        subject.onNext(migrationEvent);
    }

    @Override
    public void migrationFailed(MigrationEvent migrationEvent) {
        subject.onNext(migrationEvent);
    }

    public Observable<MigrationEvent> asObservable() {
        return subject;
    }
}
package se.lars;

import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import io.vertx.core.Future;

import java.util.Optional;

public class Rx {
    public static <T> Single<T> toSingle(Future<T> f) {
        return Single.create(s -> {
            f.setHandler(ar -> {
                if (ar.succeeded()) {
                    s.onSuccess(ar.result());
                } else {
                    s.onError(ar.cause());
                }
            });
        });
    }

    public static Completable toCompletable(Future<?> f) {
        return Completable.create(s -> {
            f.setHandler(ar -> {
                if (ar.succeeded()) {
                    s.onComplete();
                } else {
                    s.onError(ar.cause());
                }
            });
        });
    }

    public static <T> Maybe<T> toMaybe(Future<Optional<T>> f) {
        return Maybe.create(s -> {
            f.setHandler(ar -> {
                if (ar.succeeded()) {
                    ar.result().ifPresentOrElse(s::onSuccess, s::onComplete);
                } else {
                    s.onError(ar.cause());
                }
            });
        });
    }
}
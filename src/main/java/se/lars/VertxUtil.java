package se.lars;

import io.reactivex.Completable;
import io.reactivex.Single;
import io.vertx.core.Future;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;

public class VertxUtil {

    public static Future<String> deployVerticle(Vertx vertx, Verticle verticle) {
        return Future.future(promise -> vertx.deployVerticle(verticle, promise));
    }

    public static Future<Void> undeployVerticle(Vertx vertx, String deploymentId) {
        return Future.future(promise -> vertx.undeploy(deploymentId, promise));
    }

    public static Single<String> deployVerticleRx(Vertx vertx, Verticle verticle) {
        return Rx.toSingle(deployVerticle(vertx, verticle));
    }

    public static Completable undeployVerticleRx(Vertx vertx, String deploymentId) {
        return Rx.toCompletable(undeployVerticle(vertx, deploymentId));
    }

}

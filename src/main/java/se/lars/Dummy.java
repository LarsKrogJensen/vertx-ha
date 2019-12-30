package se.lars;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;

public class Dummy extends AbstractVerticle {

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
       vertx.eventBus().consumer("dummy", this::handleMessage);
       vertx.eventBus().send("dummy", "dummy-msg");
       vertx.setTimer(2_000, __ -> startPromise.complete());
  }

  private void handleMessage(Message<String> msg) {
    System.out.println("message received: " + msg.body());
  }


  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(new Dummy(), ar -> {
      System.out.println("dummy started");
    });
  }
}

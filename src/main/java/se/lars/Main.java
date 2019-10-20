package se.lars;

import static java.lang.System.setProperty;

public class Main {

    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);

        setProperty(
                "vertx.logger-delegate-factory-class-name",
                "io.vertx.core.logging.SLF4JLogDelegateFactory"
        );
        setProperty("hazelcast.logging.type", "slf4j");

        var app = new Application(new AppConfig(port));
        app.start();
    }


}

package se.lars;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.reactivex.core.eventbus.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.lars.kryo.KryoCodec;


public class EventBusCodec<T> implements MessageCodec<T, T> {
    private final static Logger log = LoggerFactory.getLogger(EventBusCodec.class);

    private Class<T> type;

    public EventBusCodec(Class<T> type) {
        this.type = type;
    }

    @Override
    public void encodeToWire(Buffer buffer, T obj) {
        KryoCodec.encode(buffer, obj);
    }

    @Override
    public T decodeFromWire(int pos, Buffer buffer) {
        return KryoCodec.decode(pos, buffer, type);
    }

    @Override
    public T transform(T obj) {
        return obj;
    }

    @Override
    public String name() {
        return type.getName();
    }

    @Override
    public byte systemCodecID() {
        return -1;
    }

    public static <T> void registerCodec(EventBus eventBus, Class<T> cls) {
        log.info("Registering event bus codec for class {}", cls.getName());
        eventBus.getDelegate().registerDefaultCodec(cls, new EventBusCodec<>(cls));
    }

    public static void registerEventBusMessages(EventBus eventBus, String... packages) {
        new ClassGraph().acceptPackages(packages)
                .scan().getAllClasses().stream()
                .filter(EventBusCodec::nonTestClasses)
                .forEach(cls -> registerCodec(eventBus, cls.loadClass()));
    }

    private static boolean nonTestClasses(ClassInfo c) {
        boolean isTest = c.getSimpleName().endsWith("Test")
                         || c.getSimpleName().endsWith("TestBuilder")
                         || c.getSimpleName().endsWith("Spec");
        if (isTest) {
            log.warn("ignoring test-class: {}", c.getName());
        }
        return !isTest;
    }
}
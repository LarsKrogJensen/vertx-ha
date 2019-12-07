package se.lars.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.Pool;
import io.vertx.core.buffer.Buffer;

public class KryoCodec {
    private static final Pool<KryoContext> pool = new Pool<>(true, false, Runtime.getRuntime().availableProcessors()) {
        @Override
        protected KryoContext create() {
            return new KryoContext();
        }
    };

    public static <T> void encode(Buffer buffer, T object) {
        KryoContext context = pool.obtain();

        try {
            Kryo kryo = context.kryo;
            Output output = context.output;
            output.reset();
            kryo.writeObject(output, object);
            byte[] bytes = output.getBuffer();
            buffer.appendInt(bytes.length);
            buffer.appendBytes(bytes);
        } finally {
            pool.free(context);
        }
    }

    public static <T> T decode(int pos, Buffer buffer, Class<T> type) {
        KryoContext context = pool.obtain();
        try {
            Kryo kryo = context.kryo;
            Input input = context.input;
            int length = buffer.getInt(pos);
            pos += 4;
            input.setBuffer(buffer.getBytes(pos, pos + length));
            T instance = kryo.readObject(input, type);
            input.reset();
            return instance;
        } finally {
            pool.free(context);
        }
    }
}

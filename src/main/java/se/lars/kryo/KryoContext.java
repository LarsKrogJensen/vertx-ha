package se.lars.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.unsafe.UnsafeInput;
import com.esotericsoftware.kryo.unsafe.UnsafeOutput;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import org.objenesis.strategy.StdInstantiatorStrategy;

class KryoContext {
    private static final int BUFFER_SIZE = 16 * 1024; // 16 kB
    final Kryo kryo;
    final Output output;
    final Input input;

    KryoContext() {
        kryo = new Kryo();
        kryo.setRegistrationRequired(false);
        // use DefaultInstantiatorStrategy to first try with constructors
        kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
        input = new UnsafeInput(BUFFER_SIZE);
        output = new UnsafeOutput(BUFFER_SIZE); // -1 -> allow grow buffer
    }
}
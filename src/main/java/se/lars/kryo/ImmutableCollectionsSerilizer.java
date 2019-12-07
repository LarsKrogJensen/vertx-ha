package se.lars.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ImmutableCollectionsSerilizer {

  public static void register(Kryo kryo) {
    kryo.register(List.of().getClass(), new ListN());
    kryo.register(List.of(1).getClass(), new ListN());

    kryo.register(Set.of().getClass(), new SetN());
    kryo.register(Set.of(1).getClass(), new SetN());

    kryo.register(Map.of().getClass(), new MapN());
    kryo.register(Map.of(1,1).getClass(), new MapN());
  }

  private static class ListN extends Serializer<List<?>> {

    public ListN() {
      super(true, true);
    }

    @Override
    public void write(Kryo kryo, Output output, List<?> list) {
      output.writeInt(list.size());
      for (Object elm : list) {
        kryo.writeClassAndObject(output, elm);
      }
    }

    @Override
    public List<?> read(Kryo kryo, Input input, Class<? extends List<?>> type) {
      int size = input.readInt();
      if (size == 0) return List.of();

      Object[] arr = new Object[size];
      for (int i = 0; i < arr.length; i++) {
        arr[i] = kryo.readClassAndObject(input);
      }

      return List.of(arr);
    }
  }

  private static class SetN extends Serializer<Set<?>> {

    public SetN() {
      super(true, true);
    }

    @Override
    public void write(Kryo kryo, Output output, Set<?> set) {
      output.writeInt(set.size());
      for (Object elm : set) {
        kryo.writeClassAndObject(output, elm);
      }
    }

    @Override
    public Set<?> read(Kryo kryo, Input input, Class<? extends Set<?>> type) {
      int size = input.readInt();
      if (size == 0) return Set.of();

      Object[] arr = new Object[size];
      for (int i = 0; i < arr.length; i++) {
        arr[i] = kryo.readClassAndObject(input);
      }

      return Set.of(arr);
    }
  }

  private static class MapN extends Serializer<Map<?,?>> {

    public MapN() {
      super(true, true);
    }

    @Override
    public void write(Kryo kryo, Output output, Map<?,?> map) {
      output.writeInt(map.size());
      for (Map.Entry<?,?> elm : map.entrySet()) {
        kryo.writeClassAndObject(output, elm.getKey());
        kryo.writeClassAndObject(output, elm.getValue());
      }
    }

    @Override
    public Map<?,?> read(Kryo kryo, Input input, Class<? extends Map<?,?>> type) {
      int size = input.readInt();
      if (size == 0) return Map.of();

      Map.Entry<?,?>[] arr = new Map.Entry[size];
      for (int i = 0; i < arr.length; i++) {
        arr[i] = Map.entry(
          kryo.readClassAndObject(input),
          kryo.readClassAndObject(input)
        );
      }

      return Map.ofEntries(arr);
    }
  }
}

package se.lars.kryo

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import org.objenesis.strategy.StdInstantiatorStrategy
import spock.lang.Specification

class ImmutableCollectionsSerilizerSpec extends Specification {
  Kryo kryo

  void setup() {
    kryo = new Kryo();
    kryo.setRegistrationRequired(false)
    kryo.setInstantiatorStrategy(new StdInstantiatorStrategy())
    ImmutableCollectionsSerilizer.register(kryo)
  }

  def "should serialize and then deserialize immutable collections"() {
    expect:
    endocdeDecode(collection) == collection

    where:
    collection               | _
    List.of()                | _
    List.of(1)               | _
    List.of(1, 2)            | _
    List.of(1, 2, 3)         | _
    Set.of()                 | _
    Set.of(1)                | _
    Set.of(1, 2)             | _
    Set.of(1, 2, 3)          | _
    Map.of()                 | _
    Map.of(1, 1)             | _
    Map.of(1, 1, 2, 2)       | _
    Map.of(1, 1, 2, 2, 3, 3) | _
  }


  def endocdeDecode(Object value) {
    def output = new Output(16 * 1024)
    kryo.writeClassAndObject(output, value)

    def input = new Input(output.buffer)
    return kryo.readClassAndObject(input)
  }
}

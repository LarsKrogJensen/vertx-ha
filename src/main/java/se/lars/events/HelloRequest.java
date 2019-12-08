package se.lars.events;

public class HelloRequest extends PunterRequest{

  public HelloRequest(int punterId) {
    super(punterId);
  }

  @Override
  public String toString() {
    return "HelloRequest{" +
      "punterId=" + punterId +
      '}';
  }
}

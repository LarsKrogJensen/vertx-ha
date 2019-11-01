package se.lars.events;

import static java.lang.String.format;

public abstract class PunterRequest {
  public final int punterId;

  public PunterRequest(int punterId) {
    this.punterId = punterId;
  }

  public String topic() {
    return topic(this.getClass(), punterId);
  }

  public static <T> String topic(Class<T> clz, int punterId) {
    return format("%s-%d", clz.getName(), punterId);
  }
}

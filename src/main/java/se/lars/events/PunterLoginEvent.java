package se.lars.events;

public class PunterLoginEvent {
    public final int punterId;

    public PunterLoginEvent(int punterId) {
        this.punterId = punterId;
    }

    @Override
    public String toString() {
        return "PunterLoginEvent{" +
               "punterId=" + punterId +
               '}';
    }
}

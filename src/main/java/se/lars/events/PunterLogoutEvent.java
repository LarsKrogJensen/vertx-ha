package se.lars.events;

public class PunterLogoutEvent {
    public final int punterId;

    public PunterLogoutEvent(int punterId) {
        this.punterId = punterId;
    }
}

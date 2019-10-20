package se.lars.events;

public class PunterActorEvent {
    public final String deploymentId;
    public final String managerId;
    public final int punterId;
    public final PunterActorStatus status;

    public PunterActorEvent(String deploymentId, String managerId, int punterId, PunterActorStatus status) {
        this.deploymentId = deploymentId;
        this.managerId = managerId;
        this.punterId = punterId;
        this.status = status;
    }

    public enum PunterActorStatus {STARTED, COMPLETED, FAILED}
}

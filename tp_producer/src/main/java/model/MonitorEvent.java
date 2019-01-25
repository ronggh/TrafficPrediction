package model;

public class MonitorEvent {
    private String monitorId;
    private String speed;

    public MonitorEvent() {
    }

    public MonitorEvent(String monitorId, String speed) {
        this.monitorId = monitorId;
        this.speed = speed;
    }

    public String getMonitorId() {
        return monitorId;
    }

    public void setMonitorId(String monitorId) {
        this.monitorId = monitorId;
    }

    public String getSpeed() {
        return speed;
    }

    public void setSpeed(String speed) {
        this.speed = speed;
    }
}

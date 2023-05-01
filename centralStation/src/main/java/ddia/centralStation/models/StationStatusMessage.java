package ddia.centralStation.models;

public class StationStatusMessage {
    long stationId, sNo, statusTimestamp;
    String batteryStatus;
    WeatherStatus weather;

    public long getStationId() {
        return stationId;
    }

    public void setStationId(long stationId) {
        this.stationId = stationId;
    }

    public long getsNo() {
        return sNo;
    }

    public void setsNo(long sNo) {
        this.sNo = sNo;
    }

    public long getStatusTimestamp() {
        return statusTimestamp;
    }

    public void setStatusTimestamp(long statusTimestamp) {
        this.statusTimestamp = statusTimestamp;
    }

    public String getBatteryStatus() {
        return batteryStatus;
    }

    public void setBatteryStatus(String batteryStatus) {
        this.batteryStatus = batteryStatus;
    }

    public WeatherStatus getWeather() {
        return weather;
    }

    public void setWeather(WeatherStatus weather) {
        this.weather = weather;
    }
}


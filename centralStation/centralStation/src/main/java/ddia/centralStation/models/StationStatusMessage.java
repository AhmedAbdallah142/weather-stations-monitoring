package ddia.centralStation.models;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class StationStatusMessage {
    @JsonProperty("station_id")
    long stationId;
    @JsonProperty("s_no")
    long sNo;
    @JsonProperty("status_timestamp")
    long statusTimestamp;
    @JsonProperty("battery_status")
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

    public String getStatusDayDate() {
        LocalDateTime dateTime = Instant.ofEpochMilli(statusTimestamp)
                .atZone(ZoneId.systemDefault())
                .toLocalDateTime();

        // format LocalDateTime to a year-month-day string
        return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
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

    @Override
    public String toString() {
        return "StationStatusMessage{" +
                "stationId=" + stationId +
                ", sNo=" + sNo +
                ", statusTimestamp=" + statusTimestamp +
                ", batteryStatus='" + batteryStatus + '\'' +
                ", weather=" + weather +
                '}';
    }
}


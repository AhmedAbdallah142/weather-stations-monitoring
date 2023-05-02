package ddia.centralStation.models;

import org.codehaus.jackson.annotate.JsonProperty;

public class WeatherStatus {
    int humidity, temperature;
    @JsonProperty("wind_speed")
    int windSpeed;

    public int getHumidity() {
        return humidity;
    }

    public void setHumidity(int humidity) {
        this.humidity = humidity;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }

    public int getWindSpeed() {
        return windSpeed;
    }

    public void setWindSpeed(int windSpeed) {
        this.windSpeed = windSpeed;
    }

    @Override
    public String toString() {
        return "WeatherStatus{" +
                "humidity=" + humidity +
                ", temperature=" + temperature +
                ", windSpeed=" + windSpeed +
                '}';
    }
}

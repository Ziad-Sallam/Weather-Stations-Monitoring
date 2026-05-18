package com.example.repo;

public class WeatherMessage {
    public long station_id;
    public long s_no;
    public String battery_status;
    public long status_timestamp;
    public Weather weather;

    public static class Weather {
        public int humidity;
        public int temperature;
        public int wind_speed;
    }

    public boolean isValid() {
        if (station_id <= 0) return false;
        if (s_no <= 0) return false;
        if (battery_status == null ||
            !battery_status.equals("low") &&
            !battery_status.equals("medium") &&
            !battery_status.equals("high")) return false;
        if (status_timestamp <= 0) return false;
        if (weather == null) return false;
        if (weather.humidity < 0 || weather.humidity > 100) return false;
        if (weather.temperature < 0) return false;
        if (weather.wind_speed < 0) return false;
        return true;
    }
}
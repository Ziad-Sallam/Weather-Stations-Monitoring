package com.example.consumer.channel;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.example.repo.WeatherMessage;
import com.google.gson.Gson;

import java.util.Properties;

public class AlertStreamsPipeline {

    private static final String INPUT_TOPIC        = "weather-valid-topic";
    private static final String RAIN_TOPIC         = "weather-rain-alert-topic";
    private static final String LOW_BATTERY_TOPIC  = "weather-low-battery-topic";

    private final KafkaStreams streams;
    private final Gson gson = new Gson();

    public AlertStreamsPipeline() {
        Properties props = new Properties();
        // different application ID from routing pipeline
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,    "alert-streams-pipeline");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> validMessages = builder.stream(INPUT_TOPIC);

        validMessages
            .filter((key, value) -> isRaining(value))
            .to(RAIN_TOPIC);

        validMessages
            .filter((key, value) -> isLowBattery(value))
            .to(LOW_BATTERY_TOPIC);

        this.streams = new KafkaStreams(builder.build(), props);
    }

    public void start() {
        streams.start();
        System.out.println("Alert streams pipeline started...");
        Runtime.getRuntime().addShutdownHook(
            new Thread(streams::close)
        );
    }

    private boolean isRaining(String raw) {
        try {
            WeatherMessage msg = gson.fromJson(raw, WeatherMessage.class);
            return msg != null && msg.weather != null && msg.weather.humidity > 70;
        } catch (Exception e) {
            return false;
        }
    }

    private boolean isLowBattery(String raw) {
        try {
            WeatherMessage msg = gson.fromJson(raw, WeatherMessage.class);
            return msg != null && "low".equals(msg.battery_status);
        } catch (Exception e) {
            return false;
        }
    }
}
package com.example.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

class WeatherMessage {

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
}



public class SimpleProducerMock {

    public static void main(String[] args) throws Exception {

        String topic = "weather-station-topic";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Create object
        WeatherMessage msg = new WeatherMessage();
        msg.station_id = 1;
        msg.s_no = 1;
        msg.battery_status = "low";
        msg.status_timestamp = 1681521224;

        msg.weather = new WeatherMessage.Weather();
        msg.weather.humidity = 35;
        msg.weather.temperature = 100;
        msg.weather.wind_speed = 13;

        // Convert to JSON
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(msg);

        ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, json);

        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                System.out.println("Sent JSON successfully!");
                System.out.println(json);
            } else {
                exception.printStackTrace();
            }
        });

        producer.flush();
        producer.close();
    }
}
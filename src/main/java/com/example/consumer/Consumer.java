package com.example.consumer;

import com.example.repo.WeatherMessage;
import com.google.gson.Gson;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.example.consumer.channel.MessageRoutingPipeline;
import com.example.consumer.channel.AlertStreamsPipeline;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

    private final KafkaConsumer<String, String> kafkaConsumer;
    private final Gson gson = new Gson();
    public Consumer() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "central-station-group");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        // start from earliest message if no offset exists
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        this.kafkaConsumer = new KafkaConsumer<>(props);
        this.kafkaConsumer.subscribe(Collections.singletonList("weather-valid-topic"));
    }

    public void start() {
        System.out.println("Central Station Consumer started...");

        while (true) {
            ConsumerRecords<String, String> records =
                    kafkaConsumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                processRecord(record);
            }
        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        String raw = record.value();

        // 1. try to parse JSON
        WeatherMessage message;
        try {
            message = gson.fromJson(raw, WeatherMessage.class);
        } catch (Exception e) {
            // 2. if parsing fails, log and skip (message will be routed to invalid-messages topic by pipeline)
            System.err.println("[ERROR] Failed to parse message: " + raw);
            return;
        }

        // 3. valid message — print it
        System.out.printf(
            "[VALID] station=%d | s_no=%d | battery=%s | " +
            "humidity=%d | temp=%d | wind=%d%n",
            message.station_id,
            message.s_no,
            message.battery_status,
            message.weather.humidity,
            message.weather.temperature,
            message.weather.wind_speed
        );

        // 4. store latest status
        // TODO: bitcask.write(message.station_id, message)

        // 6. archive to parquet (buffer managed internally)
        // TODO: parquetWriter.add(message)
    }

    public static void main(String[] args) {
        new MessageRoutingPipeline().start();
        new AlertStreamsPipeline().start();
        new Consumer().start();
    }
}
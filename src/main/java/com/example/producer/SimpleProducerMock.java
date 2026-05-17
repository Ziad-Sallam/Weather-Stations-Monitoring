package com.example.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.Scanner;

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

    private static final Random random = new Random();

    public static void main(String[] args) throws Exception {

        String topic = "weather-station-topic";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(props);

        ObjectMapper mapper = new ObjectMapper();
        Scanner scanner = new Scanner(System.in);

        while (true) {

            System.out.println("\n===== MENU =====");
            System.out.println("1 -> Send Hardcoded Valid Message");
            System.out.println("2 -> Send Random Valid Message");
            System.out.println("3 -> Send Invalid Message");
            System.out.println("4 -> Send Dead Letter Message");
            System.out.println("5 -> Exit");
            System.out.print("Choose option: ");

            int choice = scanner.nextInt();

            String json = "";
            String selectedTopic = topic;

            if (choice == 1) {

                // Hardcoded valid message
                WeatherMessage msg = new WeatherMessage();
                msg.station_id = 1;
                msg.s_no = 1;
                msg.battery_status = "low";
                msg.status_timestamp = System.currentTimeMillis();

                msg.weather = new WeatherMessage.Weather();
                msg.weather.humidity = 35;
                msg.weather.temperature = 100;
                msg.weather.wind_speed = 13;

                json = mapper.writeValueAsString(msg);

            } else if (choice == 2) {

                // Random valid message
                WeatherMessage msg = new WeatherMessage();

                msg.station_id = random.nextInt(10) + 1;
                msg.s_no = random.nextInt(1000);
                msg.status_timestamp = System.currentTimeMillis();

                String[] batteryStatuses = {
                        "low",
                        "medium",
                        "high"
                };

                msg.battery_status =
                        batteryStatuses[random.nextInt(batteryStatuses.length)];

                msg.weather = new WeatherMessage.Weather();
                msg.weather.humidity = random.nextInt(101);
                msg.weather.temperature = random.nextInt(61) - 10;
                msg.weather.wind_speed = random.nextInt(151);

                json = mapper.writeValueAsString(msg);

            } else if (choice == 3) {

                // Invalid message
                json = "{ invalid json message }";

            } else if (choice == 4) {

                // Dead letter message
                WeatherMessage msg = new WeatherMessage();

                msg.station_id = -1;
                msg.s_no = -999;
                msg.battery_status = "dead";
                msg.status_timestamp = System.currentTimeMillis();

                msg.weather = new WeatherMessage.Weather();
                msg.weather.humidity = -1;
                msg.weather.temperature = 9999;
                msg.weather.wind_speed = -50;

                json = mapper.writeValueAsString(msg);

                selectedTopic = topic;  // Send to main topic, consumer will route it to dead letter

            } else if (choice == 5) {

                System.out.println("Exiting...");
                break;

            } else {

                System.out.println("Invalid choice!");
                continue;
            }

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(selectedTopic, json);
            final String payload = json;

            producer.send(record, (metadata, exception) -> {

                if (exception == null) {

                    System.out.println("\nMessage sent successfully!");
                    System.out.println("Topic: " + metadata.topic());
                    System.out.println("Partition: " + metadata.partition());
                    System.out.println("Offset: " + metadata.offset());

                    System.out.println("Payload:");
                    System.out.println(payload);

                } else {

                    System.out.println("Error sending message!");
                    exception.printStackTrace();
                }
            });

            producer.flush();
        }

        producer.close();
        scanner.close();
    }
}
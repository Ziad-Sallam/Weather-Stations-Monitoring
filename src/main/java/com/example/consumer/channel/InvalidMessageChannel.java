package com.example.consumer.channel;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class InvalidMessageChannel implements MessageChannel {

    private static final String TOPIC = "weather-invalid-message-topic";
    private final KafkaProducer<String, String> producer;

    public InvalidMessageChannel() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public void send(String rawMessage) {
        producer.send(
            new ProducerRecord<>(TOPIC, rawMessage),
            (metadata, ex) -> {
                if (ex != null) {
                    System.err.println("[InvalidChannel] Failed to forward: " + ex.getMessage());
                }
            }
        );
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }
}
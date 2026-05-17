package com.example.consumer.channel;

import com.example.repo.WeatherMessage;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class MessageRoutingPipeline {

    private static final String INPUT_TOPIC       = "weather-station-topic";
    private static final String VALID_TOPIC        = "weather-valid-topic";
    private static final String INVALID_TOPIC      = "weather-invalid-message-topic";
    private static final String DEAD_LETTER_TOPIC  = "weather-dead-letter-topic";

    private final KafkaStreams streams;
    private final Gson gson = new Gson();

    public MessageRoutingPipeline() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,    "message-routing-pipeline");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream(INPUT_TOPIC);

        source
            .split()
            // invalid-messages
            .branch(
                (key, value) -> !isParseable(value),
                Branched.withConsumer(stream ->
                    stream.to(INVALID_TOPIC))
            )
            // dead-letter
            .branch(
                (key, value) -> !isValid(value),
                Branched.withConsumer(stream ->
                    stream.to(DEAD_LETTER_TOPIC))
            )
            // valid
            .defaultBranch(
                Branched.withConsumer(stream ->
                    stream.to(VALID_TOPIC))
            );

        this.streams = new KafkaStreams(builder.build(), props);
    }

    public void start() {
        streams.start();
        System.out.println("Message routing pipeline started...");
        Runtime.getRuntime().addShutdownHook(
            new Thread(streams::close)
        );
    }

    private boolean isParseable(String raw) {
        try {
            gson.fromJson(raw, WeatherMessage.class);
            return true;
        } catch (JsonSyntaxException e) {
            return false;
        }
    }

    private boolean isValid(String raw) {
        try {
            WeatherMessage msg = gson.fromJson(raw, WeatherMessage.class);
            return msg != null && msg.isValid();
        } catch (JsonSyntaxException e) {
            return false;
        }
    }
}
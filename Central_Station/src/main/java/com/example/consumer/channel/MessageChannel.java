package com.example.consumer.channel;

public interface MessageChannel {
    void send(String rawMessage);
    void close();
}
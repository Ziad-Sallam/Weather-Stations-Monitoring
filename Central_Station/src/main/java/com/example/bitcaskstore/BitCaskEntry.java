package com.example.bitcaskstore;

public class BitCaskEntry {

    public static final int HEADER_SIZE = 16; 

    public final long   timestamp;
    public final String key;
    public final String value;

    public BitCaskEntry(long timestamp, String key, String value) {
        this.timestamp = timestamp;
        this.key       = key;
        this.value     = value;
    }

    public int totalSize() {
        return HEADER_SIZE
             + key.getBytes(java.nio.charset.StandardCharsets.UTF_8).length
             + value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
    }
}
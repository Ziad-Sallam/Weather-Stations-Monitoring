package com.example.bitcaskstore;

public class KeyDirEntry {

    public final long fileId;
    public final long valueOffset;
    public final int valueSize;
    public final long timestamp;

    public KeyDirEntry(long fileId, long valueOffset, int valueSize, long timestamp) {
        this.fileId      = fileId;
        this.valueOffset = valueOffset;
        this.valueSize   = valueSize;
        this.timestamp   = timestamp;
    }

    @Override
    public String toString() {
        return String.format("KeyDirEntry{fileId=%d, valueOffset=%d, valueSize=%d, ts=%d}",
                fileId, valueOffset, valueSize, timestamp);
    }
}
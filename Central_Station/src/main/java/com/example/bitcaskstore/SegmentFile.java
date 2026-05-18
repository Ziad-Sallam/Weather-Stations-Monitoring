package com.example.bitcaskstore;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;


public class SegmentFile implements Closeable {

    public static final String DATA_EXT = ".data";
    public static final String HINT_EXT = ".hint";

    private static final int HINT_HEADER_SIZE = 24;


    private final long   fileId;          
    private final Path   dataPath;
    private final Path   hintPath;

    private RandomAccessFile dataWriter;
    private DataOutputStream hintWriter;

    private long currentOffset;          

    public SegmentFile(Path dir, long fileId) throws IOException {
        this.fileId   = fileId;
        this.dataPath = dir.resolve(fileId + DATA_EXT);
        this.hintPath = dir.resolve(fileId + HINT_EXT);

        this.dataWriter = new RandomAccessFile(dataPath.toFile(), "rw");
        this.dataWriter.seek(dataWriter.length());
        this.currentOffset = dataWriter.length();

        this.hintWriter = new DataOutputStream(
                new BufferedOutputStream(
                        new FileOutputStream(hintPath.toFile(), /*append=*/true)));
    }


    public synchronized KeyDirEntry write(String key, String value) throws IOException {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valBytes = value.getBytes(StandardCharsets.UTF_8);
        long   ts       = System.currentTimeMillis();

        ByteBuffer header = ByteBuffer.allocate(BitCaskEntry.HEADER_SIZE);
        header.putLong(ts);
        header.putInt(keyBytes.length);
        header.putInt(valBytes.length);

        dataWriter.write(header.array());
        dataWriter.write(keyBytes);

        long valueOffset = currentOffset + BitCaskEntry.HEADER_SIZE + keyBytes.length;
        dataWriter.write(valBytes);

        currentOffset += BitCaskEntry.HEADER_SIZE + keyBytes.length + valBytes.length;

        hintWriter.writeLong(ts);
        hintWriter.writeInt(keyBytes.length);
        hintWriter.writeInt(valBytes.length);
        hintWriter.writeLong(valueOffset);
        hintWriter.write(keyBytes);
        hintWriter.flush();

        return new KeyDirEntry(fileId, valueOffset, valBytes.length, ts);
    }


    public String readValue(long valueOffset, int valueSize) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(dataPath.toFile(), "r")) {
            raf.seek(valueOffset);
            byte[] buf = new byte[valueSize];
            raf.readFully(buf);
            return new String(buf, StandardCharsets.UTF_8);
        }
    }


    public void replayHint(java.util.Map<String, KeyDirEntry> keyDir) throws IOException {
        if (!Files.exists(hintPath)) return;

        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(new FileInputStream(hintPath.toFile())))) {

            while (dis.available() >= HINT_HEADER_SIZE) {
                long ts          = dis.readLong();
                int  keySize     = dis.readInt();
                int  valSize     = dis.readInt();
                long valueOffset = dis.readLong();

                if (dis.available() < keySize) break; 

                byte[] keyBytes = new byte[keySize];
                dis.readFully(keyBytes);
                String key = new String(keyBytes, StandardCharsets.UTF_8);

                KeyDirEntry existing = keyDir.get(key);
                if (existing == null || ts > existing.timestamp) {
                    keyDir.put(key, new KeyDirEntry(fileId, valueOffset, valSize, ts));
                }
            }
        } catch (EOFException ignored) {
        }
    }


    public void replayData(java.util.Map<String, KeyDirEntry> keyDir) throws IOException {
        if (!Files.exists(dataPath)) return;

        try (RandomAccessFile raf = new RandomAccessFile(dataPath.toFile(), "r")) {
            long pos = 0;
            while (pos + BitCaskEntry.HEADER_SIZE <= raf.length()) {
                raf.seek(pos);
                long ts      = raf.readLong();
                int  keySize = raf.readInt();
                int  valSize = raf.readInt();

                if (pos + BitCaskEntry.HEADER_SIZE + keySize + valSize > raf.length()) break;

                byte[] keyBytes = new byte[keySize];
                raf.readFully(keyBytes);
                String key = new String(keyBytes, StandardCharsets.UTF_8);

                long valueOffset = pos + BitCaskEntry.HEADER_SIZE + keySize;

                KeyDirEntry existing = keyDir.get(key);
                if (existing == null || ts > existing.timestamp) {
                    keyDir.put(key, new KeyDirEntry(fileId, valueOffset, valSize, ts));
                }

                pos += BitCaskEntry.HEADER_SIZE + keySize + valSize;
            }
        } catch (EOFException ignored) { /* truncated tail – ok */ }
    }

    public long getFileId()    { return fileId; }
    public Path getDataPath()  { return dataPath; }
    public Path getHintPath()  { return hintPath; }
    public long size()         { return currentOffset; }

    @Override
    public void close() throws IOException {
        if (dataWriter != null) { dataWriter.close(); dataWriter = null; }
        if (hintWriter != null) { hintWriter.close(); hintWriter = null; }
    }
}
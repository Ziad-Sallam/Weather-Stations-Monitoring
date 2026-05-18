package com.example.bitcaskstore;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class BitCaskStore implements Closeable {

    private static final long MAX_SEGMENT_SIZE = 64L * 1024 * 1024;
    private static final long COMPACTION_INTERVAL_SECONDS = 60;
    private static final Logger LOG = Logger.getLogger(BitCaskStore.class.getName());

    private final Path dir;
    private final ConcurrentHashMap<String, KeyDirEntry> keyDir = new ConcurrentHashMap<>();
    private final List<SegmentFile> immutableSegments = new CopyOnWriteArrayList<>();
    private volatile SegmentFile activeSegment;
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "bitcask-compaction");
                t.setDaemon(true);
                return t;
            });


    public BitCaskStore(String directoryPath) throws IOException {
        this.dir = Paths.get(directoryPath);
        Files.createDirectories(dir);

        recover();          // rebuild keyDir from hint / data files
        openActiveSegment();

        // schedule background compaction
        scheduler.scheduleAtFixedRate(
                this::compactSafely,
                COMPACTION_INTERVAL_SECONDS,
                COMPACTION_INTERVAL_SECONDS,
                TimeUnit.SECONDS);
    }


    public void put(String key, String value) throws IOException {
        rwLock.readLock().lock();  
        try {
            KeyDirEntry entry = activeSegment.write(key, value);
            keyDir.put(key, entry);

            if (activeSegment.size() >= MAX_SEGMENT_SIZE) {
                rollOver();
            }
        } finally {
            rwLock.readLock().unlock();
        }
    }


    public String get(String key) throws IOException {
        KeyDirEntry entry = keyDir.get(key);
        if (entry == null) return null;

        SegmentFile seg = findSegment(entry.fileId);
        if (seg == null) return null;

        return seg.readValue(entry.valueOffset, entry.valueSize);
    }

    public Set<String> keys() {
        return Collections.unmodifiableSet(keyDir.keySet());
    }

    public Map<String, String> getAll() throws IOException {
        Map<String, String> result = new LinkedHashMap<>();
        for (String key : keyDir.keySet()) {
            String val = get(key);
            if (val != null) result.put(key, val);
        }
        return result;
    }

    public int size() { return keyDir.size(); }

    private void recover() throws IOException {
        List<Path> dataFiles = Files.list(dir)
                .filter(p -> p.toString().endsWith(SegmentFile.DATA_EXT))
                .sorted(Comparator.comparing(p -> fileIdFromPath(p)))
                .collect(Collectors.toList());

        for (Path dataPath : dataFiles) {
            long   fileId  = fileIdFromPath(dataPath);
            SegmentFile seg = new SegmentFile(dir, fileId);

            Path hintPath = dir.resolve(fileId + SegmentFile.HINT_EXT);
            if (Files.exists(hintPath)) {
                LOG.info("BitCask Replaying hint file: " + hintPath.getFileName());
                seg.replayHint(keyDir);
            } else {
                LOG.warning("BitCask No hint file for " + fileId + " – scanning data file");
                seg.replayData(keyDir);
            }

            immutableSegments.add(seg);
        }

        LOG.info("BitCask Recovery complete. Keys loaded: " + keyDir.size());
    }

    private void rollOver() throws IOException {
        rwLock.writeLock().lock();
        try {
            if (activeSegment.size() < MAX_SEGMENT_SIZE) return;

            activeSegment.close();
            immutableSegments.add(activeSegment);
            openActiveSegment();
            LOG.info("BitCask Rolled over to new segment: " + activeSegment.getFileId());
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private void openActiveSegment() throws IOException {
        long newId = System.currentTimeMillis();
        this.activeSegment = new SegmentFile(dir, newId);
    }

    private void compactSafely() {
        try {
            compact();
        } catch (Exception e) {
            LOG.warning("BitCask Compaction failed: " + e.getMessage());
        }
    }

    private synchronized void compact() throws IOException {
        if (immutableSegments.isEmpty()) return;

        LOG.info("BitCask Starting compaction of " + immutableSegments.size() + " segments...");

        List<SegmentFile> toCompact = new ArrayList<>(immutableSegments);
        Set<Long> compactedIds = new HashSet<>();
        for (SegmentFile s : toCompact) compactedIds.add(s.getFileId());

        long compactedId = System.currentTimeMillis() - 1; 
        SegmentFile compacted = new SegmentFile(dir, compactedId);

        Map<String, KeyDirEntry> updatedEntries = new HashMap<>();

        for (Map.Entry<String, KeyDirEntry> e : keyDir.entrySet()) {
            KeyDirEntry kd = e.getValue();
            if (!compactedIds.contains(kd.fileId)) continue; 

            SegmentFile src = findSegment(kd.fileId);
            if (src == null) continue;

            String value    = src.readValue(kd.valueOffset, kd.valueSize);
            KeyDirEntry neo = compacted.write(e.getKey(), value);
            updatedEntries.put(e.getKey(), neo);
        }

        compacted.close();

        rwLock.writeLock().lock();
        try {
            for (Map.Entry<String, KeyDirEntry> e : updatedEntries.entrySet()) {
                keyDir.compute(e.getKey(), (k, existing) -> {
                    if (existing != null && !compactedIds.contains(existing.fileId)) {
                        return existing; // a newer write happened – keep it
                    }
                    return e.getValue();
                });
            }

            immutableSegments.removeAll(toCompact);

            immutableSegments.add(new SegmentFile(dir, compactedId));
        } finally {
            rwLock.writeLock().unlock();
        }

        for (SegmentFile old : toCompact) {
            try { old.close(); } catch (IOException ignored) {}
            Files.deleteIfExists(old.getDataPath());
            Files.deleteIfExists(old.getHintPath());
        }

        LOG.info("BitCask Compaction done. Merged " + toCompact.size()
                + " segments → " + compactedId + SegmentFile.DATA_EXT);
    }

    private SegmentFile findSegment(long fileId) {
        if (activeSegment != null && activeSegment.getFileId() == fileId) {
            return activeSegment;
        }
        for (SegmentFile s : immutableSegments) {
            if (s.getFileId() == fileId) return s;
        }
        return null;
    }

    private static long fileIdFromPath(Path p) {
        String name = p.getFileName().toString();
        int dot = name.indexOf('.');
        try {
            return Long.parseLong(dot >= 0 ? name.substring(0, dot) : name);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public void close() throws IOException {
        scheduler.shutdownNow();
        rwLock.writeLock().lock();
        try {
            if (activeSegment != null) activeSegment.close();
            for (SegmentFile s : immutableSegments) s.close();
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
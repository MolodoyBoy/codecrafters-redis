package com.my.redis.data_storage.stream;

import com.my.redis.data_storage.key_space.KeySpaceStorage;
import com.my.redis.data_storage.key_space.Storage;
import com.my.redis.exception.ValidationException;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.my.redis.data_storage.stream.StreamEntries.initStreamEntry;
import static java.lang.Thread.currentThread;

public class StreamDataStorage {

    private static final Class<Stream> CLASS = Stream.class;

    private final Condition condition;
    private final ReadWriteLock readWriteLock;
    private final KeySpaceStorage keySpaceStorage;

    public StreamDataStorage(KeySpaceStorage keySpaceStorage) {
        this.keySpaceStorage = keySpaceStorage;
        this.readWriteLock = new ReentrantReadWriteLock();
        this.condition = readWriteLock.writeLock().newCondition();
    }

    public NavigableMap<StreamId, StreamEntries> getInRange(String key,
                                                            StreamId start,
                                                            StreamId end) {
        Lock readLock = readWriteLock.readLock();
        readLock.lock();

        try {
            Stream stream = keySpaceStorage.get(key, CLASS);

            var streamValue = stream.value();

            if (start == null && end == null) {
                return streamValue;
            }

            if (start == null) {
                return streamValue.headMap(end, end.isInclusive());
            }

            if (end == null) {
               return streamValue.tailMap(start, start.isInclusive());
            }

            return streamValue.subMap(start, start.isInclusive(), end, end.isInclusive());
        } finally {
            readLock.unlock();
        }
    }

    //Todo add support for concurrent blocking reads
    public Map<String, NavigableMap<StreamId, StreamEntries>> getInRange(Map<String, StreamId> keys, Duration duration) {
        Lock readLock = readWriteLock.writeLock();
        readLock.lock();

        boolean needToGenerate = keys.values().stream()
            .anyMatch(StreamId::needToBeGenerated);

        if (needToGenerate) {
            Map<String, StreamId> refactoredKeys = new LinkedHashMap<>();
            keys.forEach((key, streamId) -> {
                if (streamId.needToBeGenerated()) {
                    Stream stream = keySpaceStorage.get(key, CLASS);
                    if (streamId.needToBeGenerated()) {
                        streamId = getLatestStreamId(stream);
                    }
                }

                refactoredKeys.put(key, streamId);
            });

            keys = refactoredKeys;
        }

        try {
            if (duration == null) {
                var result = new LinkedHashMap<String, NavigableMap<StreamId, StreamEntries>>();

                keys.forEach((key, streamId) -> {
                    Stream stream = keySpaceStorage.get(key, CLASS);

                    var streamValue = stream.value();
                    result.put(key, streamValue.tailMap(streamId, streamId.isInclusive()));
                });

                return result;
            } else {
                long timeout = duration.toMillis();
                long nanosRemaining = duration.toNanos();

                Map<String, NavigableMap<StreamId, StreamEntries>> result;
                while ((result = findAnyNotEmptyStream(keys)) == null) {
                    if (timeout == 0) {
                        condition.await();
                    } else {
                        nanosRemaining = condition.awaitNanos(nanosRemaining);
                        if (nanosRemaining <= 0) {
                            return null;
                        }
                    }
                }

                return result;
            }
        } catch (InterruptedException e) {
            currentThread().interrupt();
            return null;
        } finally {
            readLock.unlock();
        }
    }

    private Map<String, NavigableMap<StreamId, StreamEntries>> findAnyNotEmptyStream(Map<String, StreamId> keys) {
        for (Map.Entry<String, StreamId> entry : keys.entrySet()) {
            String key = entry.getKey();
            StreamId streamId = entry.getValue();

            Stream stream = keySpaceStorage.get(key, CLASS);
            if (stream != null && !stream.isEmpty()) {
                var streamValue = stream.value;
                NavigableMap<StreamId, StreamEntries> result = streamValue.tailMap(streamId, streamId.isInclusive());
                if (result != null && !result.isEmpty()) {
                    return Map.of(key, result);
                }
            }
        }

        return null;
    }

    public StreamId addEntries(String key, StreamId streamId, List<StreamEntry> keyValuePairs) {
        Lock writeLock = readWriteLock.writeLock();
        writeLock.lock();

        try {
            Stream stream = keySpaceStorage.computeIfAbsent(key, new Stream(), CLASS);
            var streamValue = stream.value;

            if (streamId.needToBeGenerated()) {
                streamId = generateNextStreamId(stream, streamId.timestampMillis(), streamId.isInclusive());
            } else {
                validateStream(streamId, streamValue);
            }

            StreamEntries streamEntries = streamValue.get(streamId);
            if (streamEntries == null) {
                streamEntries = initStreamEntry();
                streamValue.put(streamId, streamEntries);
            }

            streamEntries.addAll(keyValuePairs);

            condition.signalAll();

            return streamId;
        } finally {
            writeLock.unlock();
        }
    }

    private StreamId getLatestStreamId(Stream stream) {
        if (stream == null || stream.isEmpty()) {
            return new StreamId(0L, 0L, true);
        }

        var streamValue = stream.value;
        StreamId streamId = streamValue.lastKey();

        // Return a non-inclusive ID to start reading from the next entry
        return new StreamId(streamId.timestampMillis(), streamId.sequence(), false);
    }

    private StreamId generateNextStreamId(Stream stream, Long timestampMillis, boolean isInclusive) {
        if (timestampMillis == null) {
            timestampMillis = System.currentTimeMillis();
        }

        if (stream == null || stream.isEmpty()) {
            return new StreamId(timestampMillis, getDefaultSequence(timestampMillis), isInclusive);
        }

        var streamValue = stream.value;
        StreamId latestStreamId = streamValue.lastKey();
        if (timestampMillis > latestStreamId.timestampMillis()) {
            return new StreamId(timestampMillis, 0L, isInclusive);
        } else if (timestampMillis.equals(latestStreamId.timestampMillis())) {
            return new StreamId(timestampMillis, latestStreamId.sequence() + 1, isInclusive);
        } else {
            throw new ValidationException("ERR The ID specified in XADD is equal or smaller than the target stream top item");
        }
    }

    private long getDefaultSequence(long timestampMillis) {
        if (timestampMillis == 0) {
            return 1;
        } else {
            return 0;
        }
    }

    private void validateStream(StreamId streamId, NavigableMap<StreamId, StreamEntries> streamValue) {
        if (!streamValue.isEmpty()) {
            StreamId topKey = streamValue.lastKey();
            if (streamId.compareTo(topKey) <= 0) {
                throw new ValidationException("ERR The ID specified in XADD is equal or smaller than the target stream top item");
            }
        }
    }

    private record Stream(NavigableMap<StreamId, StreamEntries> value) implements Storage {

        public Stream() {
            this(new TreeMap<>());
        }

        public boolean isEmpty() {
            return value.isEmpty();
        }

        @Override
        public String type() {
            return "stream";
        }
    }
}
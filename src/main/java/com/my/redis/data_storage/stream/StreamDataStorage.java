package com.my.redis.data_storage.stream;

import com.my.redis.data_storage.key_space.KeySpaceStorage;
import com.my.redis.data_storage.key_space.Storage;
import com.my.redis.exception.ValidationException;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.my.redis.data_storage.stream.StreamEntries.initStreamEntry;

public class StreamDataStorage {

    private static final Class<Stream> CLASS = Stream.class;

    private final ReadWriteLock readWriteLock;
    private final KeySpaceStorage keySpaceStorage;

    public StreamDataStorage(KeySpaceStorage keySpaceStorage) {
        this.keySpaceStorage = keySpaceStorage;
        this.readWriteLock = new ReentrantReadWriteLock();
    }

    public NavigableMap<StreamId, StreamEntries> getInRange(String key, StreamId start, StreamId end) {
        Lock readLock = readWriteLock.readLock();
        readLock.lock();

        try {
            Stream stream = keySpaceStorage.get(key, CLASS);

            var streamValue = stream.value();

            return streamValue.subMap(start, true, end, true);
        } finally {
            readLock.unlock();
        }
    }

    public StreamId addEntries(String key, StreamId streamId, List<StreamEntry> keyValuePairs) {
        Lock writeLock = readWriteLock.writeLock();
        writeLock.lock();

        try {
            Stream stream = keySpaceStorage.computeIfAbsent(key, new Stream(), CLASS);
            var streamValue = stream.value;

            if (streamId.needToBeGenerated()) {
                streamId = generateNextStreamId(stream, streamId.timestampMillis());
            } else {
                validateStream(streamId, streamValue);
            }

            StreamEntries streamEntries = streamValue.get(streamId);
            if (streamEntries == null) {
                streamEntries = initStreamEntry();
                streamValue.put(streamId, streamEntries);
            }

            streamEntries.addAll(keyValuePairs);

            return streamId;
        } finally {
            writeLock.unlock();
        }
    }

    private StreamId generateNextStreamId(Stream stream, Long timestampMillis) {
        if (timestampMillis == null) {
            timestampMillis = System.currentTimeMillis();
        }

        if (stream == null || stream.isEmpty()) {
            return new StreamId(timestampMillis, getDefaultSequence(timestampMillis));
        }

        var streamValue = stream.value;
        StreamId latestStreamId = streamValue.lastKey();
        if (timestampMillis > latestStreamId.timestampMillis()) {
            return new StreamId(timestampMillis, 0L);
        } else if (timestampMillis.equals(latestStreamId.timestampMillis())) {
            return new StreamId(timestampMillis, latestStreamId.sequence() + 1);
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
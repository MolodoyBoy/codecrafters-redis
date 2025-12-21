package com.my.redis.data_storage;

import com.my.redis.exception.ValidationException;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.my.redis.data_storage.StreamEntries.initStreamEntry;

public class StreamDataStorage {

    private static final Class<Stream> CLASS = Stream.class;

    private final ReadWriteLock readWriteLock;
    private final KeySpaceStorage keySpaceStorage;

    public StreamDataStorage(KeySpaceStorage keySpaceStorage) {
        this.keySpaceStorage = keySpaceStorage;
        this.readWriteLock = new ReentrantReadWriteLock();
    }

    public StreamId addEntries(String key, StreamId streamId, List<StreamKeyValuePair> keyValuePairs) {
        Lock writeLock = readWriteLock.writeLock();
        writeLock.lock();

        try {
            var stream = keySpaceStorage.computeIfAbsent(key, new Stream(), CLASS);
            var streamValue = stream.value;

            validateStream(streamId, streamValue);

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

    private void validateStream(StreamId streamId, NavigableMap<StreamId, StreamEntries> streamValue) {
        if (!streamValue.isEmpty()) {
            StreamId topKey = streamValue.lastKey();
            if (streamId.compareTo(topKey) <= 0) {
                throw new ValidationException("ERR The ID specified in XADD is equal or smaller than the target stream top item");
            }
        }
    }

    private record Stream(NavigableMap<StreamId, StreamEntries> value) implements StorageValue {

        public Stream() {
            this(new TreeMap<>());
        }

        @Override
        public String type() {
            return "stream";
        }
    }
}